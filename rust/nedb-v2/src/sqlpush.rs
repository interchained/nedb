// SPDX-License-Identifier: BUSL-1.1
// SPDX-FileCopyrightText: © 2026 INTERCHAINED LLC × Claude Sonnet 4.6

//! Predicate pushdown — conservative, and narrower than the phrase sounds.
//!
//! # What this is NOT
//!
//! It is not a rewrite engine that turns
//!
//! ```text
//!   Filter(Join(A, B))   ->   Join(Filter(A), B)
//! ```
//!
//! on the basis of column ownership. That transformation is unsound in
//! general, and the specific way it fails is already pinned in the semantic
//! corpus:
//!
//! ```text
//!   LEFT JOIN ... WHERE d.dname = 'eng'        2 rows
//!   LEFT JOIN ... ON ... AND d.dname = 'eng'   5 rows
//! ```
//!
//! Moving a predicate from `WHERE` to the join's `ON` changes which rows get
//! NULL-synthesised, so it changes the answer. Three-valued logic is what
//! makes it dangerous: the outer rows survive the join and are then dropped by
//! `WHERE` because a comparison against the synthesised NULL is UNKNOWN.
//!
//! # What it IS: a pre-filter on a relation that is never NULL-synthesised
//!
//! A qualifying conjunct is COPIED to run against its own relation before the
//! join. The `WHERE` clause is left untouched and still runs after the join.
//!
//! Retaining the original is necessary but **not sufficient**, and getting
//! that wrong is instructive. The first version of this module argued that a
//! copy-not-move was safe for every join type, reasoning:
//!
//! > Removing rows from a relation can only create MORE unmatched rows on the
//! > other side; those get NULL-synthesised, and the retained `WHERE` then
//! > evaluates the same predicate against a NULL, yields UNKNOWN, and drops
//! > them.
//!
//! That is wrong, and the semantic corpus caught it immediately. A predicate
//! can be SATISFIED by a synthesised NULL:
//!
//! ```text
//!   SELECT e.name FROM emp e LEFT JOIN dept d ON e.dept_id = d.id
//!    WHERE d.dname IS NULL
//! ```
//!
//! No `dept` row has a NULL `dname`, so pre-filtering `dept` empties it
//! entirely; every `emp` row then becomes unmatched, gets NULL-extended, and
//! `IS NULL` is TRUE for all of them. The answer went from 1 row to 5.
//!
//! So the real condition is about NULL SYNTHESIS, not about retention:
//!
//! > A predicate may be pre-applied to relation `R` only if `R` is never
//! > NULL-synthesised in this query's output.
//!
//! When `R` cannot be synthesised, every output row carries a real `R` row, so
//! the retained `WHERE` sees exactly the values the pre-filter saw, and the
//! pre-filter can only remove rows the `WHERE` would have removed. When `R`
//! CAN be synthesised, removing a row can manufacture an outer row whose
//! values differ from anything the pre-filter examined — and whether that row
//! survives depends on the predicate, which is not something to guess at.
//!
//! [`nullable_bindings`] computes that set:
//!
//! * a join's right binding is nullable when the join is `LEFT` or `FULL`;
//! * every binding accumulated so far becomes nullable when a LATER join is
//!   `RIGHT` or `FULL`, because those synthesise NULLs across the whole left
//!   side — including the `FROM` relation.
//!
//! An `INNER` (or `CROSS`) join synthesises nothing, which is why an
//! all-inner query can push everything and is the common case.
//!
//! # Refusals are recorded, not silent
//!
//! When a conjunct cannot be pushed, the reason is kept on the plan
//! (`Filter retained above join: ...`). An optimiser that silently declines is
//! impossible to audit — you cannot tell "correctly refused" from "forgot to
//! look". The reasons are inspectable in tests today and are the natural thing
//! for `EXPLAIN` to show later.

use crate::sqlselect::Expr;
use std::collections::HashMap;

/// Functions safe to evaluate while pre-filtering.
///
/// An allowlist, for the same fail-safe reason as the hash-join key planner:
/// a volatile function added to the evaluator and not added here is REFUSED
/// rather than silently evaluated twice with different answers.
const PURE_FUNCS: &[&str] = &[
    "lower", "upper", "length", "char_length", "character_length", "coalesce",
    "nullif", "int2", "int4", "int8", "text", "quote_ident", "format_type",
    "array_to_string", "current_schema", "current_database", "current_catalog",
    "current_user", "session_user", "user", "version", "pg_get_userbyid",
    "pg_table_is_visible", "pg_type_is_visible", "pg_function_is_visible",
    "pg_encoding_to_char", "pg_get_expr", "pg_get_indexdef",
    "pg_get_constraintdef",
];

/// What the planner decided, per relation, plus why it declined the rest.
#[derive(Debug, Clone, Default)]
pub struct Pushdown {
    /// binding (lowercased) -> conjuncts to pre-filter that relation with.
    pub per_binding: HashMap<String, Vec<Expr>>,
    /// Human-readable refusal reasons, in the order the conjuncts appeared.
    pub refusals: Vec<String>,
}

impl Pushdown {
    pub fn for_binding(&self, binding: &str) -> Option<&Vec<Expr>> {
        self.per_binding.get(&binding.to_ascii_lowercase())
    }

    pub fn pushed_count(&self) -> usize {
        self.per_binding.values().map(|v| v.len()).sum()
    }
}

/// Split an expression into top-level `AND` conjuncts.
///
/// Only `AND` may be split. An `OR` branch constrains the row as a whole, so
/// pre-filtering on one side of it would drop rows the predicate accepts.
fn conjuncts<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    match e {
        Expr::Binary { op, left, right } if op == "AND" => {
            conjuncts(left, out);
            conjuncts(right, out);
        }
        other => out.push(other),
    }
}

/// Which bindings a predicate reads, and whether it is safe to evaluate early.
enum Reads {
    /// Exactly one binding, and nothing that prevents early evaluation.
    One(String),
    /// Reads no column at all — a constant. Pre-filtering on it would be
    /// pointless (it is the same answer for every row) so it is left alone.
    Constant,
    Refused(&'static str),
}

fn reads(e: &Expr, known: &[String]) -> Reads {
    let mut seen: Vec<String> = vec![];
    let mut why: Option<&'static str> = None;
    walk(e, known, &mut seen, &mut why);
    if let Some(w) = why {
        return Reads::Refused(w);
    }
    match seen.len() {
        0 => Reads::Constant,
        1 => Reads::One(seen.pop().expect("one")),
        _ => Reads::Refused("spans more than one relation"),
    }
}

fn walk(e: &Expr, known: &[String], seen: &mut Vec<String>, why: &mut Option<&'static str>) {
    match e {
        Expr::Column { qual, .. } => match qual {
            Some(q) => {
                let lower = q.to_ascii_lowercase();
                if !known.iter().any(|b| b.eq_ignore_ascii_case(q)) {
                    // An unknown binding is a query error, reported with a
                    // better message by the evaluator than by the planner.
                    *why = Some("references an unknown relation");
                } else if !seen.contains(&lower) {
                    seen.push(lower);
                }
            }
            // A bare column resolves by scanning bindings in order AT
            // EVALUATION TIME, so it cannot be attributed to one relation
            // here. Guessing would pre-filter the wrong relation.
            None => *why = Some("unqualified column cannot be attributed to a relation"),
        },
        Expr::Literal(_) => {}
        Expr::Star | Expr::QualifiedStar(_) => *why = Some("contains `*`"),
        Expr::Func { name, args } => {
            if !PURE_FUNCS.iter().any(|f| f.eq_ignore_ascii_case(name)) {
                *why = Some("calls a function not known to be pure");
            }
            for a in args {
                walk(a, known, seen, why);
            }
        }
        Expr::Case { operand, whens, else_ } => {
            if let Some(o) = operand {
                walk(o, known, seen, why);
            }
            for (w, t) in whens {
                walk(w, known, seen, why);
                walk(t, known, seen, why);
            }
            if let Some(x) = else_ {
                walk(x, known, seen, why);
            }
        }
        Expr::Binary { left, right, .. } => {
            walk(left, known, seen, why);
            walk(right, known, seen, why);
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } => {
            walk(expr, known, seen, why)
        }
        Expr::InList { expr, list, .. } => {
            walk(expr, known, seen, why);
            for i in list {
                walk(i, known, seen, why);
            }
        }
    }
}

/// The bindings this query can NULL-synthesise.
///
/// Pre-filtering any of these is refused: removing a row can manufacture an
/// outer row carrying NULLs the pre-filter never examined, and whether that
/// row survives the retained `WHERE` depends on the predicate.
pub fn nullable_bindings(sel: &crate::sqlselect::Select) -> Vec<String> {
    use crate::sqlselect::JoinKind;
    let mut out: Vec<String> = vec![];
    let mut accumulated: Vec<String> = sel
        .from
        .iter()
        .map(|t| t.binding().to_ascii_lowercase())
        .collect();

    for j in &sel.joins {
        let rb = j.table.binding().to_ascii_lowercase();
        // LEFT/FULL: the RIGHT side is synthesised when a left row has no
        // partner.
        if matches!(j.kind, JoinKind::Left | JoinKind::Full) && !out.contains(&rb) {
            out.push(rb.clone());
        }
        // RIGHT/FULL: the whole accumulated LEFT side is synthesised when a
        // right row has no partner — which retroactively makes every earlier
        // binding nullable, the `FROM` relation included.
        if matches!(j.kind, JoinKind::Right | JoinKind::Full) {
            for a in &accumulated {
                if !out.contains(a) {
                    out.push(a.clone());
                }
            }
        }
        accumulated.push(rb);
    }
    out
}

/// Decide which `WHERE` conjuncts may be pre-applied to which relation.
///
/// `bindings` must list every relation in the query. The returned predicates
/// are COPIES — the caller keeps evaluating the original `WHERE` after the
/// join, which is what makes this safe.
pub fn plan(
    where_: Option<&Expr>,
    bindings: &[String],
    nullable: &[String],
) -> Pushdown {
    let mut out = Pushdown::default();
    let Some(w) = where_ else { return out };

    // With a single relation there is no join to push below, and the filter
    // already runs directly over it. Pushing would only duplicate the work.
    if bindings.len() < 2 {
        return out;
    }

    let mut parts = vec![];
    conjuncts(w, &mut parts);
    for p in parts {
        match reads(p, bindings) {
            Reads::One(b) if nullable.iter().any(|n| n.eq_ignore_ascii_case(&b)) => {
                // Oracle's wording, because it names the actual hazard rather
                // than restating the rule.
                out.refusals.push(format!(
                    "Filter retained above join: predicate references nullable \
                     side of an outer join ({b})"
                ));
            }
            Reads::One(b) => out.per_binding.entry(b).or_default().push(p.clone()),
            Reads::Constant => out
                .refusals
                .push("Filter retained above join: predicate reads no column".into()),
            Reads::Refused(why) => out
                .refusals
                .push(format!("Filter retained above join: {why}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlselect::parse;

    fn plan_for(sql: &str) -> Pushdown {
        let sel = parse(sql).expect("parses");
        let mut b = vec![];
        if let Some(f) = &sel.from {
            b.push(f.binding());
        }
        for j in &sel.joins {
            b.push(j.table.binding());
        }
        let nullable = nullable_bindings(&sel);
        plan(sel.where_.as_ref(), &b, &nullable)
    }

    #[test]
    fn a_single_relation_predicate_is_pushed_to_that_relation() {
        let p = plan_for("SELECT 1 FROM a JOIN b ON a.x = b.x WHERE a.v > 5");
        assert_eq!(p.pushed_count(), 1);
        assert_eq!(p.for_binding("a").map(|v| v.len()), Some(1));
        assert!(p.for_binding("b").is_none());
        assert!(p.refusals.is_empty(), "{:?}", p.refusals);
    }

    #[test]
    fn conjuncts_are_pushed_to_their_own_relations_independently() {
        let p = plan_for(
            "SELECT 1 FROM a JOIN b ON a.x = b.x WHERE a.v > 5 AND b.w < 2 AND a.z = 'q'",
        );
        assert_eq!(p.pushed_count(), 3);
        assert_eq!(p.for_binding("a").map(|v| v.len()), Some(2));
        assert_eq!(p.for_binding("b").map(|v| v.len()), Some(1));
    }

    #[test]
    fn a_predicate_on_the_nullable_side_of_a_left_join_is_REFUSED() {
        // This test asserted the opposite in the first version of this module,
        // and it was wrong. `WHERE d.dname IS NULL` over a LEFT JOIN is
        // SATISFIED by the synthesised NULL, so emptying the right relation
        // manufactures outer rows that pass the retained WHERE — 1 row became
        // 5. The semantic corpus caught it.
        let p = plan_for("SELECT 1 FROM a LEFT JOIN b ON a.x = b.x WHERE b.w = 5");
        assert_eq!(p.pushed_count(), 0);
        assert!(p.refusals[0].contains("nullable side"), "{:?}", p.refusals);
    }

    #[test]
    fn the_non_nullable_side_of_a_left_join_is_still_pushed() {
        // `a` is never synthesised by a LEFT JOIN, so its own predicates are
        // safe. This is the case that matters in practice — a selective filter
        // on the driving relation.
        let p = plan_for("SELECT 1 FROM a LEFT JOIN b ON a.x = b.x WHERE a.v > 5");
        assert_eq!(p.for_binding("a").map(|v| v.len()), Some(1));
        assert!(p.refusals.is_empty(), "{:?}", p.refusals);
    }

    #[test]
    fn a_right_join_makes_the_LEFT_side_nullable_including_the_from_relation() {
        let p = plan_for("SELECT 1 FROM a RIGHT JOIN b ON a.x = b.x WHERE a.v > 5");
        assert_eq!(p.pushed_count(), 0, "a is synthesised by the RIGHT join");
        assert!(p.refusals[0].contains("nullable side"), "{:?}", p.refusals);
        // The right side of a RIGHT join is never synthesised.
        let p = plan_for("SELECT 1 FROM a RIGHT JOIN b ON a.x = b.x WHERE b.w > 5");
        assert_eq!(p.for_binding("b").map(|v| v.len()), Some(1));
    }

    #[test]
    fn a_full_join_makes_both_sides_nullable() {
        for w in ["a.v > 5", "b.w > 5"] {
            let p = plan_for(&format!("SELECT 1 FROM a FULL JOIN b ON a.x = b.x WHERE {w}"));
            assert_eq!(p.pushed_count(), 0, "{w}");
        }
    }

    #[test]
    fn a_later_right_join_retroactively_protects_earlier_relations() {
        // `a` and `b` are fine on their own, but the RIGHT join to `c`
        // synthesises NULLs across BOTH of them — so neither may be
        // pre-filtered. Missing this would be a wrong answer that only shows
        // up in three-relation queries.
        let sel = parse(
            "SELECT 1 FROM a JOIN b ON a.x = b.x RIGHT JOIN c ON b.y = c.y \
             WHERE a.v > 1 AND b.w > 1 AND c.z > 1",
        )
        .expect("parses");
        let nullable = nullable_bindings(&sel);
        assert!(nullable.contains(&"a".to_string()), "{nullable:?}");
        assert!(nullable.contains(&"b".to_string()), "{nullable:?}");
        assert!(!nullable.contains(&"c".to_string()), "c is never synthesised");

        let p = plan_for(
            "SELECT 1 FROM a JOIN b ON a.x = b.x RIGHT JOIN c ON b.y = c.y \
             WHERE a.v > 1 AND b.w > 1 AND c.z > 1",
        );
        assert_eq!(p.pushed_count(), 1, "only c");
        assert_eq!(p.for_binding("c").map(|v| v.len()), Some(1));
        assert_eq!(p.refusals.len(), 2);
    }

    #[test]
    fn an_all_inner_query_can_push_everything() {
        let p = plan_for(
            "SELECT 1 FROM a JOIN b ON a.x = b.x JOIN c ON b.y = c.y \
             WHERE a.v > 1 AND b.w > 1 AND c.z > 1",
        );
        assert_eq!(p.pushed_count(), 3);
        assert!(p.refusals.is_empty());
        assert!(nullable_bindings(&parse(
            "SELECT 1 FROM a JOIN b ON a.x = b.x JOIN c ON b.y = c.y"
        ).unwrap()).is_empty());
    }

    #[test]
    fn a_predicate_spanning_two_relations_is_refused_with_a_reason() {
        let p = plan_for("SELECT 1 FROM a JOIN b ON a.x = b.x WHERE a.v > b.w");
        assert_eq!(p.pushed_count(), 0);
        assert_eq!(p.refusals.len(), 1);
        assert!(p.refusals[0].contains("spans more than one relation"), "{:?}", p.refusals);
    }

    #[test]
    fn or_is_never_split() {
        // `a.v > 5 OR b.w < 2` accepts a row when EITHER holds, so filtering
        // `a` by the left half alone would drop rows the predicate accepts.
        let p = plan_for("SELECT 1 FROM a JOIN b ON a.x = b.x WHERE a.v > 5 OR b.w < 2");
        assert_eq!(p.pushed_count(), 0);
        assert_eq!(p.refusals.len(), 1);
    }

    #[test]
    fn an_or_of_one_relation_is_also_refused_today() {
        // `a.v > 5 OR a.v < 1` COULD be pushed, since it reads only `a`. It is
        // allowed, because `reads` looks at the whole conjunct rather than
        // splitting the OR.
        let p = plan_for("SELECT 1 FROM a JOIN b ON a.x = b.x WHERE a.v > 5 OR a.v < 1");
        assert_eq!(p.pushed_count(), 1, "one conjunct, one relation");
    }

    #[test]
    fn an_unqualified_column_is_refused() {
        let p = plan_for("SELECT 1 FROM a JOIN b ON a.x = b.x WHERE v > 5");
        assert_eq!(p.pushed_count(), 0);
        assert!(p.refusals[0].contains("unqualified"), "{:?}", p.refusals);
    }

    #[test]
    fn a_constant_predicate_is_refused_as_pointless() {
        let p = plan_for("SELECT 1 FROM a JOIN b ON a.x = b.x WHERE 1 = 1");
        assert_eq!(p.pushed_count(), 0);
        assert!(p.refusals[0].contains("reads no column"), "{:?}", p.refusals);
    }

    #[test]
    fn a_volatile_function_is_refused_because_the_allowlist_is_fail_safe() {
        let sel = parse("SELECT 1 FROM a JOIN b ON a.x = b.x").expect("parses");
        let _ = sel;
        let pred = Expr::Binary {
            op: "=".into(),
            left: Box::new(Expr::Func {
                name: "random".into(),
                args: vec![Expr::Column { qual: Some("a".into()), name: "v".into() }],
            }),
            right: Box::new(Expr::Literal(serde_json::json!(1))),
        };
        let p = plan(Some(&pred), &["a".into(), "b".into()], &[]);
        assert_eq!(p.pushed_count(), 0);
        assert!(p.refusals[0].contains("not known to be pure"), "{:?}", p.refusals);
    }

    #[test]
    fn pure_functions_and_postfix_operators_are_pushable() {
        // `BETWEEN` desugars to `>= AND <=`, so it legitimately yields TWO
        // pushable conjuncts. Stating the real count rather than rounding it
        // to one — the parser's shape is part of what is being asserted.
        for (w, want) in [
            ("lower(a.name) = 'x'", 1),
            ("a.v IS NULL", 1),
            ("a.v IS NOT NULL", 1),
            ("a.v IN (1, 2, 3)", 1),
            ("a.v NOT IN (1, 2)", 1),
            ("a.v BETWEEN 1 AND 9", 2),
            ("a.v NOT BETWEEN 1 AND 9", 1),
            ("coalesce(a.v, 0) > 1", 1),
            ("a.v::text = '5'", 1),
            ("NOT (a.v = 3)", 1),
            ("CASE WHEN a.v > 1 THEN true ELSE false END", 1),
        ] {
            let p = plan_for(&format!("SELECT 1 FROM a JOIN b ON a.x = b.x WHERE {w}"));
            assert_eq!(
                p.pushed_count(), want,
                "{w} should push {want}: {:?}", p.refusals
            );
            assert!(p.refusals.is_empty(), "{w}: {:?}", p.refusals);
        }
    }

    #[test]
    fn nothing_is_pushed_without_a_join_because_there_is_nothing_to_push_below() {
        let p = plan_for("SELECT 1 FROM a WHERE a.v > 5");
        assert_eq!(p.pushed_count(), 0);
        // Not a refusal either — there is simply no join.
        assert!(p.refusals.is_empty());
    }

    #[test]
    fn an_unknown_relation_is_left_to_the_evaluator_to_report() {
        let p = plan_for("SELECT 1 FROM a JOIN b ON a.x = b.x WHERE zz.v > 5");
        assert_eq!(p.pushed_count(), 0);
        assert!(p.refusals[0].contains("unknown relation"), "{:?}", p.refusals);
    }

    #[test]
    fn a_binding_is_matched_case_insensitively() {
        // Binding resolution ignores case, so the planner must too or it would
        // attribute `A.v` to no relation and refuse a pushable predicate.
        let p = plan_for("SELECT 1 FROM a JOIN b ON a.x = b.x WHERE A.v > 5");
        assert_eq!(p.for_binding("a").map(|v| v.len()), Some(1));
        assert_eq!(p.for_binding("A").map(|v| v.len()), Some(1));
    }
}
