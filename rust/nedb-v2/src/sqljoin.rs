// SPDX-License-Identifier: BUSL-1.1
// SPDX-FileCopyrightText: © 2026 INTERCHAINED LLC × Claude Sonnet 4.6

//! Join strategy: nested loop (the reference) and hash (the fast path).
//!
//! # Division of responsibility
//!
//! The evaluator in [`crate::sqlselect`] decides what a query MEANS. This
//! module decides only HOW the join is executed. Those are kept apart
//! deliberately: an optimisation that can change an answer is not an
//! optimisation, it is a bug with better throughput.
//!
//! # Why the hash table is not allowed to decide anything
//!
//! A hash join works by partitioning rows into buckets, which assumes equality
//! is an equivalence relation. In this engine it is NOT, because comparison is
//! dynamically typed:
//!
//! ```text
//!   1   =  '1'     TRUE    (number vs numeric string -> compared numerically)
//!   1   =  '1.0'   TRUE    (same)
//!  '1'  =  '1.0'   FALSE   (string vs string -> compared exactly)
//! ```
//!
//! Equality is therefore not transitive, and no bucketing scheme can reproduce
//! nested-loop results by bucketing alone. So this module does not try.
//!
//! [`hkey`] maps a value to a bucket, and the ONLY property it must have is:
//!
//! > if `a = b` evaluates to TRUE, then `hkey(a) == hkey(b)`
//!
//! That is, no FALSE NEGATIVES. Collisions are harmless and expected — every
//! candidate pair that survives the bucket lookup is then re-checked against
//! the complete, unmodified `ON` expression by the evaluator itself. The hash
//! table shrinks the candidate set; the evaluator still decides the answer.
//!
//! That is what makes equivalence with the nested loop provable rather than
//! merely tested: both paths end up asking the same question of the same
//! expression, and the fast path only skips pairs that the invariant above
//! guarantees would have answered "no".
//!
//! The asymmetry is the whole safety argument, and it was checked by mutation
//! rather than assumed. Breaking the invariant — bucketing numeric strings as
//! text, so `1 = '1'` is no longer found — fails seven differential tests.
//! Adding false POSITIVES, by giving `NULL` an ordinary bucket, changes no
//! answer at all. Only one direction can be wrong, which is why this module
//! is allowed to be approximate and the evaluator is not.
//!
//! # Row order
//!
//! Buckets hold right-hand row INDICES in ascending order, and probing walks
//! left rows in order. A nested loop over the same inputs emits pairs in
//! exactly that order too, so the two strategies agree row-for-row — not just
//! as sets. Differential tests can compare ordered lists, which is a far
//! sharper assertion than comparing sorted ones.

use crate::sqlselect::{Expr, JoinKind};
use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;

/// Which execution strategy to use for joins.
///
/// `Auto` is what production uses. The two forced variants exist so that
/// differential tests can run the SAME query down BOTH paths and compare —
/// without them, a test believing it exercised the hash path could silently be
/// measuring the nested loop, and the equivalence suite would prove nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinExec {
    Auto,
    NestedLoop,
    Hash,
}

/// What actually ran, per join, for `EXPLAIN` and for benchmark honesty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinChoice {
    pub kind: JoinKind,
    pub table: String,
    pub strategy: Strategy,
    /// How many equality key pairs the planner could prove usable. Zero means
    /// the hash path was not available at all.
    pub keys: usize,
    pub left_rows: usize,
    pub right_rows: usize,
    pub out_rows: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    NestedLoop,
    Hash,
}

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Strategy::NestedLoop => "Nested Loop",
            Strategy::Hash => "Hash Join",
        })
    }
}

/// Below this many candidate pairs, a nested loop is simply cheaper — building
/// a hash table costs an allocation per distinct key and a clone of every
/// right row's key values, which a handful of comparisons does not repay.
///
/// Catalogue queries (psql's `\dt` and friends) join a few dozen rows and stay
/// on the reference path, which is a welcome side effect: the code path that
/// real clients exercise most is the one whose semantics are most thoroughly
/// tested.
pub const AUTO_HASH_MIN_PAIRS: usize = 64;

// ─────────────────────────────────────────────────────────────────────────────
// Bucket keys
// ─────────────────────────────────────────────────────────────────────────────

/// A bucket identity for one value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HKey {
    /// Canonical bits of the numeric interpretation.
    Num(u64),
    /// Canonical text.
    Text(String),
}

/// `-0.0` and `0.0` are numerically equal and so must share a bucket; their
/// bit patterns differ, so the sign is normalised away first.
fn canon(f: f64) -> u64 {
    let f = if f == 0.0 { 0.0 } else { f };
    f.to_bits()
}

/// The bucket a value belongs to, or `None` when the value can never match.
///
/// `NULL` returns `None`: every comparison against `NULL` is UNKNOWN, and
/// UNKNOWN does not join, so a NULL-keyed row is set aside rather than
/// bucketed.
///
/// Worth being precise about what that buys, because it is NOT correctness.
/// Giving `NULL` an ordinary bucket was tried as a deliberate mutation and the
/// differential suite still passed — the confirm step evaluates `NULL = NULL`
/// to UNKNOWN and drops the pair regardless. Setting NULLs aside avoids
/// building one enormous bucket of rows that can never match. Correctness
/// rests on the no-false-negatives property, not on this.
///
/// A numeric-looking STRING deliberately buckets with numbers, because
/// `1 = '1'` is TRUE here. Two different numeric strings therefore share a
/// bucket even though they are not equal to each other — the confirm step
/// rejects that pair, and no correct match is lost.
pub fn hkey(v: &Value) -> Option<HKey> {
    match v {
        Value::Null => None,
        Value::Number(n) => Some(match n.as_f64() {
            Some(f) => HKey::Num(canon(f)),
            // Only reachable with arbitrary-precision numbers enabled; text is
            // a safe over-approximation.
            None => HKey::Text(n.to_string()),
        }),
        Value::String(s) => match s.parse::<f64>() {
            Ok(f) => Some(HKey::Num(canon(f))),
            Err(_) => Some(HKey::Text(s.clone())),
        },
        // Mirrors the engine's textual rendering of a boolean, because
        // `true = 't'` is TRUE here and the two must share a bucket.
        Value::Bool(b) => Some(HKey::Text(if *b { "t" } else { "f" }.to_string())),
        // Arrays and objects compare as their JSON text, so they bucket by it —
        // which also puts a string holding that same text in the same bucket,
        // exactly as the comparison requires.
        other => Some(HKey::Text(other.to_string())),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Planning
// ─────────────────────────────────────────────────────────────────────────────

/// Functions the planner will evaluate while building a hash key.
///
/// This is an ALLOWLIST rather than a list of forbidden functions, and that
/// direction is the whole point. A hash key is computed once per row and then
/// trusted; a function whose value varies between the build and probe passes
/// would silently drop matching rows. Every function below is pure. If a
/// volatile one (`random()`, `clock_timestamp()`) is ever added to the
/// evaluator and not added here, the planner refuses to use it as a key and
/// the join falls back to the nested loop — wrong-but-slow is not a failure
/// mode this list can produce, which is why it is written this way round.
const PURE_FUNCS: &[&str] = &[
    "lower",
    "upper",
    "length",
    "char_length",
    "character_length",
    "coalesce",
    "nullif",
    "int2",
    "int4",
    "int8",
    "text",
    "quote_ident",
    "format_type",
    "array_to_string",
    "current_schema",
    "current_database",
    "current_catalog",
    "current_user",
    "session_user",
    "user",
    "version",
    "pg_get_userbyid",
    "pg_table_is_visible",
    "pg_type_is_visible",
    "pg_function_is_visible",
    "pg_encoding_to_char",
    "pg_get_expr",
    "pg_get_indexdef",
    "pg_get_constraintdef",
];

/// Which relation an expression reads from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    /// Reads only left-hand bindings, and at least one of them.
    Left,
    /// Reads only the right-hand binding, and does read it.
    Right,
    /// Reads no columns at all — a constant. Usable as neither key side,
    /// because `ON a.x = 5` is a filter rather than a join condition.
    Const,
    /// Spans both sides, mentions an unqualified column (whose binding is
    /// resolved at evaluation time and so cannot be attributed statically), or
    /// contains something the planner will not evaluate early.
    Unusable,
}

fn side_of(e: &Expr, left: &[String], right: &str) -> Side {
    let mut saw_left = false;
    let mut saw_right = false;
    let mut usable = true;
    walk(e, left, right, &mut saw_left, &mut saw_right, &mut usable);
    if !usable || (saw_left && saw_right) {
        return Side::Unusable;
    }
    match (saw_left, saw_right) {
        (true, false) => Side::Left,
        (false, true) => Side::Right,
        (false, false) => Side::Const,
        (true, true) => unreachable!("handled above"),
    }
}

fn walk(
    e: &Expr,
    left: &[String],
    right: &str,
    saw_left: &mut bool,
    saw_right: &mut bool,
    usable: &mut bool,
) {
    match e {
        Expr::Column { qual, .. } => match qual {
            Some(q) => {
                if q.eq_ignore_ascii_case(right) {
                    *saw_right = true;
                } else if left.iter().any(|b| b.eq_ignore_ascii_case(q)) {
                    *saw_left = true;
                } else {
                    // An unknown binding is a query error, which the evaluator
                    // reports with a better message than the planner could.
                    *usable = false;
                }
            }
            // A bare column resolves by scanning bindings in order AT
            // EVALUATION TIME, so which relation it reads depends on the row.
            // It cannot be attributed to a side here, and guessing would build
            // a key from the wrong relation.
            None => *usable = false,
        },
        Expr::Literal(_) => {}
        Expr::Star | Expr::QualifiedStar(_) => *usable = false,
        Expr::Func { name, args } => {
            if !PURE_FUNCS.iter().any(|f| f.eq_ignore_ascii_case(name)) {
                *usable = false;
            }
            for a in args {
                walk(a, left, right, saw_left, saw_right, usable);
            }
        }
        Expr::Case { operand, whens, else_ } => {
            if let Some(o) = operand {
                walk(o, left, right, saw_left, saw_right, usable);
            }
            for (w, t) in whens {
                walk(w, left, right, saw_left, saw_right, usable);
                walk(t, left, right, saw_left, saw_right, usable);
            }
            if let Some(x) = else_ {
                walk(x, left, right, saw_left, saw_right, usable);
            }
        }
        Expr::Binary { left: l, right: r, .. } => {
            walk(l, left, right, saw_left, saw_right, usable);
            walk(r, left, right, saw_left, saw_right, usable);
        }
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } => {
            walk(expr, left, right, saw_left, saw_right, usable);
        }
        Expr::InList { expr, list, .. } => {
            walk(expr, left, right, saw_left, saw_right, usable);
            for i in list {
                walk(i, left, right, saw_left, saw_right, usable);
            }
        }
    }
}

/// Split an expression into top-level `AND` conjuncts.
///
/// Only `AND` may be split. An `OR` branch constrains the pair as a whole, so
/// treating either side as an independent key would match pairs that the
/// predicate rejects.
fn conjuncts<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    match e {
        Expr::Binary { op, left, right } if op == "AND" => {
            conjuncts(left, out);
            conjuncts(right, out);
        }
        other => out.push(other),
    }
}

/// The equality key pairs a hash join can use, as `(left expr, right expr)`.
///
/// Returns an empty vector when the hash path is unavailable, in which case
/// the caller uses the nested loop. Note that non-equality conjuncts are NOT
/// extracted or rewritten — they stay in the `ON` expression and are evaluated
/// by the confirm step, so nothing here has to reason about their semantics.
pub fn hash_keys(on: Option<&Expr>, left: &[String], right: &str) -> Vec<(Expr, Expr)> {
    let Some(on) = on else { return vec![] };
    let mut parts = vec![];
    conjuncts(on, &mut parts);
    let mut keys = vec![];
    for p in parts {
        let Expr::Binary { op, left: l, right: r } = p else { continue };
        // Only `=`. `IS NOT DISTINCT FROM` would need NULL keys to match each
        // other, and no other operator partitions rows at all.
        if op != "=" {
            continue;
        }
        match (side_of(l, left, right), side_of(r, left, right)) {
            (Side::Left, Side::Right) => keys.push(((**l).clone(), (**r).clone())),
            (Side::Right, Side::Left) => keys.push(((**r).clone(), (**l).clone())),
            _ => {}
        }
    }
    keys
}

/// The strategy to use, given a plan and the actual relation sizes.
pub fn choose(exec: JoinExec, keys: usize, left_rows: usize, right_rows: usize) -> Strategy {
    if keys == 0 {
        // Not a matter of preference: with no provable equality key there is
        // nothing to hash on.
        return Strategy::NestedLoop;
    }
    match exec {
        JoinExec::NestedLoop => Strategy::NestedLoop,
        JoinExec::Hash => Strategy::Hash,
        JoinExec::Auto => {
            if left_rows.saturating_mul(right_rows) > AUTO_HASH_MIN_PAIRS {
                Strategy::Hash
            } else {
                Strategy::NestedLoop
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The hash table
// ─────────────────────────────────────────────────────────────────────────────

/// Right-hand rows indexed by their key bucket.
pub struct HashSide {
    buckets: HashMap<Vec<HKey>, Vec<usize>>,
    /// Right rows whose key contains a NULL. They match nothing, but a
    /// `RIGHT`/`FULL` join still has to emit them as unmatched, so they cannot
    /// simply be dropped.
    pub null_keyed: Vec<usize>,
}

impl HashSide {
    /// Build the probe side. `key_of` yields one row's key values, or `None`
    /// when any of them is NULL.
    pub fn build(
        n: usize,
        mut key_of: impl FnMut(usize) -> Result<Option<Vec<HKey>>>,
    ) -> Result<Self> {
        let mut buckets: HashMap<Vec<HKey>, Vec<usize>> = HashMap::new();
        let mut null_keyed = vec![];
        for i in 0..n {
            match key_of(i)? {
                // Insertion order is ascending `i`, which is what keeps output
                // row order identical to the nested loop's.
                Some(k) => buckets.entry(k).or_default().push(i),
                None => null_keyed.push(i),
            }
        }
        Ok(Self { buckets, null_keyed })
    }

    /// Candidate right-row indices for a left key, in ascending order.
    pub fn probe(&self, key: &[HKey]) -> &[usize] {
        self.buckets.get(key).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn distinct_keys(&self) -> usize {
        self.buckets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlselect::parse;
    use serde_json::json;

    // ── the no-false-negatives invariant ─────────────────────────────────────

    /// Every value the engine can hold, including the pairs that make equality
    /// non-transitive. Kept deliberately hostile.
    fn corpus() -> Vec<Value> {
        vec![
            Value::Null,
            json!(0),
            json!(-0.0),
            json!(0.0),
            json!(1),
            json!(1.0),
            json!(-1),
            json!(1000),
            json!(0.1),
            json!(9007199254740993i64),
            json!(9007199254740992i64),
            json!("0"),
            json!("1"),
            json!("1.0"),
            json!("1.00"),
            json!("01"),
            json!("1e3"),
            json!(" 1"),
            json!("1abc"),
            json!(""),
            json!("t"),
            json!("f"),
            json!("true"),
            json!("nan"),
            json!("inf"),
            json!("-0"),
            json!("abc"),
            json!("ABC"),
            json!(true),
            json!(false),
            json!([1, 2]),
            json!("[1,2]"),
            json!({"a": 1}),
            json!(r#"{"a":1}"#),
        ]
    }

    /// Ask the real evaluator whether `a = b`, so the invariant is checked
    /// against the actual semantics rather than a restatement of them.
    fn equals(a: &Value, b: &Value) -> bool {
        let sel = parse("SELECT l.v = r.v AS eq FROM l JOIN r ON 1 = 1").expect("parses");
        let (la, lb) = (a.clone(), b.clone());
        let resolve = move |t: &str| -> Result<Option<Box<dyn crate::sqlselect::Relation>>> {
            Ok(Some(crate::sqlselect::from_vec(match t {
                "l" => vec![json!({"v": la})],
                _ => vec![json!({"v": lb})],
            })))
        };
        let (_, rows) = crate::sqlselect::execute(&sel, &resolve).expect("runs");
        rows.first().and_then(|r| r.get("eq")).and_then(|v| v.as_bool()) == Some(true)
    }

    #[test]
    fn equality_implies_same_bucket() {
        let c = corpus();
        let mut equal_pairs = 0;
        for a in &c {
            for b in &c {
                if !equals(a, b) {
                    continue;
                }
                equal_pairs += 1;
                let (ka, kb) = (hkey(a), hkey(b));
                assert!(
                    ka.is_some() && kb.is_some(),
                    "{a:?} = {b:?} is TRUE but a key is unhashable"
                );
                assert_eq!(
                    ka, kb,
                    "{a:?} = {b:?} is TRUE but they bucket apart — the hash \
                     join would LOSE this match"
                );
            }
        }
        // Guards against the corpus silently degenerating into values that are
        // never equal, which would make the assertion above vacuous.
        assert!(equal_pairs > 40, "corpus proved too little: {equal_pairs} equal pairs");
    }

    #[test]
    fn null_never_hashes() {
        assert_eq!(hkey(&Value::Null), None);
        // And NULL is equal to nothing, including itself.
        for v in corpus() {
            assert!(!equals(&Value::Null, &v));
            assert!(!equals(&v, &Value::Null));
        }
    }

    #[test]
    fn the_non_transitive_case_is_real_and_survives() {
        // The reason bucketing alone cannot be trusted.
        assert!(equals(&json!(1), &json!("1")));
        assert!(equals(&json!(1), &json!("1.0")));
        assert!(!equals(&json!("1"), &json!("1.0")));
        // All three share a bucket, so no match is lost; the confirm step is
        // what keeps '1' from joining '1.0'.
        assert_eq!(hkey(&json!(1)), hkey(&json!("1")));
        assert_eq!(hkey(&json!(1)), hkey(&json!("1.0")));
        assert_eq!(hkey(&json!("1")), hkey(&json!("1.0")));
    }

    #[test]
    fn signed_zero_shares_a_bucket() {
        assert_eq!(hkey(&json!(0.0)), hkey(&json!(-0.0)));
        assert_eq!(hkey(&json!(0)), hkey(&json!(-0.0)));
    }

    #[test]
    fn bool_and_its_text_share_a_bucket() {
        assert!(equals(&json!(true), &json!("t")));
        assert_eq!(hkey(&json!(true)), hkey(&json!("t")));
        assert_eq!(hkey(&json!(false)), hkey(&json!("f")));
    }

    #[test]
    fn composite_and_its_json_text_share_a_bucket() {
        assert_eq!(hkey(&json!([1, 2])), hkey(&json!("[1,2]")));
    }

    // ── planning ─────────────────────────────────────────────────────────────

    fn keys_for(sql: &str) -> Vec<(Expr, Expr)> {
        let s = parse(sql).expect("parses");
        let left = vec![s.from.as_ref().unwrap().binding()];
        let j = &s.joins[0];
        hash_keys(j.on.as_ref(), &left, &j.table.binding())
    }

    #[test]
    fn simple_equijoin_yields_one_key() {
        assert_eq!(keys_for("SELECT 1 FROM a JOIN b ON a.x = b.y").len(), 1);
    }

    #[test]
    fn key_pairs_are_normalised_left_then_right() {
        // Written right-side-first; the planner must still order the pair
        // (left, right) or the probe would look up the wrong relation's value.
        let k = keys_for("SELECT 1 FROM a JOIN b ON b.y = a.x");
        assert_eq!(k.len(), 1);
        assert_eq!(k[0].0, Expr::Column { qual: Some("a".into()), name: "x".into() });
        assert_eq!(k[0].1, Expr::Column { qual: Some("b".into()), name: "y".into() });
    }

    #[test]
    fn multiple_equality_conjuncts_all_become_keys() {
        assert_eq!(keys_for("SELECT 1 FROM a JOIN b ON a.x = b.x AND a.y = b.y").len(), 2);
    }

    #[test]
    fn non_equality_conjuncts_are_left_to_the_evaluator() {
        // One usable key; the `>` stays in the ON expression, where the confirm
        // step applies it.
        assert_eq!(keys_for("SELECT 1 FROM a JOIN b ON a.x = b.x AND a.n > b.n").len(), 1);
    }

    #[test]
    fn or_is_never_split() {
        assert!(keys_for("SELECT 1 FROM a JOIN b ON a.x = b.x OR a.y = b.y").is_empty());
    }

    #[test]
    fn a_constant_side_is_not_a_key() {
        assert!(keys_for("SELECT 1 FROM a JOIN b ON a.x = 5").is_empty());
        assert!(keys_for("SELECT 1 FROM a JOIN b ON 1 = 1").is_empty());
    }

    #[test]
    fn same_side_equality_is_not_a_key() {
        assert!(keys_for("SELECT 1 FROM a JOIN b ON a.x = a.y").is_empty());
    }

    #[test]
    fn a_bare_column_is_refused() {
        // `x` resolves by scanning bindings at evaluation time, so it cannot be
        // attributed to a relation here.
        assert!(keys_for("SELECT 1 FROM a JOIN b ON x = b.y").is_empty());
        assert!(keys_for("SELECT 1 FROM a JOIN b ON a.x = y").is_empty());
    }

    #[test]
    fn an_expression_key_is_allowed_when_it_reads_one_side() {
        assert_eq!(keys_for("SELECT 1 FROM a JOIN b ON lower(a.x) = lower(b.y)").len(), 1);
        assert_eq!(keys_for("SELECT 1 FROM a JOIN b ON a.x = b.y::text").len(), 1);
    }

    #[test]
    fn a_key_spanning_both_sides_is_refused() {
        assert!(keys_for("SELECT 1 FROM a JOIN b ON coalesce(a.x, b.y) = b.z").is_empty());
    }

    #[test]
    fn an_unknown_function_is_refused() {
        // Not on the allowlist, so it cannot be trusted to be pure.
        let s = parse("SELECT 1 FROM a JOIN b ON a.x = b.y").expect("parses");
        let left = vec!["a".to_string()];
        let on = Expr::Binary {
            op: "=".into(),
            left: Box::new(Expr::Column { qual: Some("a".into()), name: "x".into() }),
            right: Box::new(Expr::Func {
                name: "random".into(),
                args: vec![Expr::Column { qual: Some("b".into()), name: "y".into() }],
            }),
        };
        assert!(hash_keys(Some(&on), &left, &s.joins[0].table.binding()).is_empty());
    }

    #[test]
    fn cross_join_has_no_keys() {
        assert!(keys_for("SELECT 1 FROM a CROSS JOIN b").is_empty());
    }

    #[test]
    fn a_second_join_may_key_off_either_earlier_relation() {
        let s = parse("SELECT 1 FROM a JOIN b ON a.x = b.x JOIN c ON b.y = c.y").expect("parses");
        let left = vec!["a".to_string(), "b".to_string()];
        let j = &s.joins[1];
        assert_eq!(hash_keys(j.on.as_ref(), &left, &j.table.binding()).len(), 1);
    }

    // ── strategy choice ──────────────────────────────────────────────────────

    #[test]
    fn no_keys_forces_the_nested_loop_even_when_hash_is_requested() {
        assert_eq!(choose(JoinExec::Hash, 0, 1000, 1000), Strategy::NestedLoop);
    }

    #[test]
    fn auto_stays_on_the_reference_path_for_small_inputs() {
        assert_eq!(choose(JoinExec::Auto, 1, 4, 4), Strategy::NestedLoop);
        assert_eq!(choose(JoinExec::Auto, 1, 8, 8), Strategy::NestedLoop);
        assert_eq!(choose(JoinExec::Auto, 1, 8, 9), Strategy::Hash);
    }

    #[test]
    fn forcing_is_honoured_so_differential_tests_mean_something() {
        assert_eq!(choose(JoinExec::NestedLoop, 2, 10_000, 10_000), Strategy::NestedLoop);
        assert_eq!(choose(JoinExec::Hash, 2, 1, 1), Strategy::Hash);
    }

    // ── the table ────────────────────────────────────────────────────────────

    #[test]
    fn build_preserves_ascending_row_order_within_a_bucket() {
        let vals = vec![json!("a"), json!("b"), json!("a"), json!("a")];
        let side = HashSide::build(vals.len(), |i| Ok(hkey(&vals[i]).map(|k| vec![k])))
            .expect("builds");
        let k = vec![hkey(&json!("a")).unwrap()];
        assert_eq!(side.probe(&k), &[0, 2, 3]);
        assert_eq!(side.distinct_keys(), 2);
    }

    #[test]
    fn null_keyed_rows_are_set_aside_not_dropped() {
        let vals = vec![json!("a"), Value::Null, json!("b")];
        let side = HashSide::build(vals.len(), |i| Ok(hkey(&vals[i]).map(|k| vec![k])))
            .expect("builds");
        assert_eq!(side.null_keyed, vec![1]);
        assert!(side.probe(&[HKey::Text("zzz".into())]).is_empty());
        // Still reachable, which is what a RIGHT/FULL join needs.
        assert_eq!(side.distinct_keys(), 2);
    }

    #[test]
    fn a_compound_key_matches_only_on_every_column() {
        let rows = vec![(json!(1), json!("x")), (json!(1), json!("y"))];
        let side = HashSide::build(rows.len(), |i| {
            Ok(match (hkey(&rows[i].0), hkey(&rows[i].1)) {
                (Some(a), Some(b)) => Some(vec![a, b]),
                _ => None,
            })
        })
        .expect("builds");
        let want = vec![hkey(&json!(1)).unwrap(), hkey(&json!("x")).unwrap()];
        assert_eq!(side.probe(&want), &[0]);
    }
}
