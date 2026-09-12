// SPDX-License-Identifier: BUSL-1.1
// SPDX-FileCopyrightText: © 2026 INTERCHAINED LLC × Claude Sonnet 4.6

//! Differential tests: predicate pushdown on, against pushdown off.
//!
//! # The invariant
//!
//! Pushdown must not change any answer — same rows, same order, for every
//! query. It is an optimisation, so its only permitted effect is on how much
//! work happens.
//!
//! Both join strategies are exercised too, because pushdown changes relation
//! SIZES and the `Auto` planner picks a strategy from those sizes. An
//! optimisation that quietly moves a query onto a different execution path is
//! exactly the situation where a latent difference between the paths would
//! surface, so all four combinations are compared.
//!
//! # Why this needed its own harness
//!
//! The first version of `sqlpush` argued that copying a predicate (rather than
//! moving it) made pre-filtering safe for every join type. The semantic corpus
//! disproved that in one case: `WHERE d.dname IS NULL` over a `LEFT JOIN` is
//! SATISFIED by the synthesised NULL, so emptying the right relation
//! manufactures outer rows that pass the retained filter — 1 row became 5.
//!
//! The rule is therefore about NULL synthesis, not retention, and the fixture
//! below is built to attack it: predicates on both sides of every join kind,
//! including predicates that a NULL satisfies (`IS NULL`, `NOT IN`, negations)
//! and predicates a NULL makes UNKNOWN.

use nedb_engine::sqljoin::JoinExec;
use nedb_engine::sqlplan::Stage;
use nedb_engine::sqlselect::{execute_with, parse};
use serde_json::{json, Value};

fn relation(name: &str) -> Option<Vec<Value>> {
    Some(match name {
        "l" => vec![
            json!({"k": 1, "v": 10, "tag": "a"}),
            json!({"k": 2, "v": 20, "tag": "b"}),
            json!({"k": 3, "v": 30, "tag": null}),
            json!({"k": null, "v": 40, "tag": "c"}),
            json!({"k": 1, "v": 50, "tag": "a"}),
        ],
        "r" => vec![
            json!({"k": 1, "w": 100, "label": "x"}),
            json!({"k": 2, "w": 200, "label": null}),
            json!({"k": 9, "w": 300, "label": "orphan"}),
            json!({"k": null, "w": 400, "label": "limbo"}),
        ],
        "m" => vec![
            json!({"k": 1, "z": 7}),
            json!({"k": 2, "z": 8}),
        ],
        _ => return None,
    })
}

fn resolve(name: &str) -> anyhow::Result<Option<Box<dyn nedb_engine::sqlselect::Relation>>> {
    Ok(relation(name).map(nedb_engine::sqlselect::from_vec))
}

/// Run one query with pushdown off and on, under both join strategies, and
/// assert every combination agrees. Returns how many of the four runs actually
/// pre-filtered something.
#[track_caller]
fn agree(sql: &str) -> usize {
    let sel = parse(sql).unwrap_or_else(|e| panic!("{sql}\n  failed to parse: {e:#}"));

    let mut baseline: Option<(Vec<String>, Vec<Value>)> = None;
    let mut prefiltered = 0usize;

    for exec in [JoinExec::NestedLoop, JoinExec::Hash] {
        for pushdown in [false, true] {
            let (cols, rows, plan) = execute_with(&sel, &resolve, exec, pushdown)
                .unwrap_or_else(|e| {
                    panic!("{sql}\n  failed with exec={exec:?} pushdown={pushdown}: {e:#}")
                });
            let names: Vec<String> = cols.into_iter().map(|c| c.name).collect();

            if plan.stages.iter().any(|s| matches!(s, Stage::Prefilter { .. })) {
                prefiltered += 1;
                assert!(pushdown, "{sql}\n  pre-filtered with pushdown DISABLED");
            }

            match &baseline {
                None => baseline = Some((names, rows)),
                Some((bn, br)) => {
                    assert_eq!(bn, &names, "{sql}\n  columns differ (exec={exec:?} pushdown={pushdown})");
                    assert_eq!(
                        br, &rows,
                        "{sql}\n  ROWS DIFFER with exec={exec:?} pushdown={pushdown}\n  \
                         baseline = {}\n  got      = {}\n  plan:\n{}",
                        json!(br),
                        json!(rows),
                        plan.render().join("\n")
                    );
                }
            }
        }
    }
    prefiltered
}

/// Every predicate shape worth attacking, including the ones a NULL satisfies.
const PREDICATES: &[&str] = &[
    // UNKNOWN over a synthesised NULL — these were always safe.
    "l.v > 15",
    "r.w > 150",
    "l.tag = 'a'",
    "r.label = 'x'",
    // SATISFIED by a synthesised NULL — the class that disproved the first
    // safety argument.
    "l.tag IS NULL",
    "r.label IS NULL",
    "l.k IS NULL",
    "r.k IS NULL",
    // Negations and NOT IN, where three-valued logic is easiest to get wrong.
    "NOT (r.label = 'x')",
    "r.label NOT IN ('x')",
    "r.w NOT BETWEEN 150 AND 250",
    "coalesce(r.label, 'none') = 'none'",
    // Spans both relations — must never be pushed.
    "l.v > r.w",
    // Compound.
    "l.v > 15 AND r.w > 150",
    "l.tag IS NULL AND r.label IS NULL",
    "l.v > 15 OR r.w > 150",
];

const KINDS: &[&str] = &["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"];

#[test]
fn pushdown_never_changes_an_answer() {
    let mut total_prefiltered = 0;
    for kind in KINDS {
        for pred in PREDICATES {
            for order in ["", " ORDER BY 1, 2"] {
                let sql = format!(
                    "SELECT l.v, r.w FROM l {kind} r ON l.k = r.k WHERE {pred}{order}"
                );
                total_prefiltered += agree(&sql);
            }
        }
    }
    // An inner join can push most of these, so the suite must be doing real
    // work rather than refusing everything and comparing nothing.
    assert!(
        total_prefiltered > 40,
        "only {total_prefiltered} runs pre-filtered — the suite is not exercising pushdown"
    );
}

#[test]
fn pushdown_never_changes_an_answer_in_a_three_relation_query() {
    // The retroactive case: a later RIGHT join makes the earlier relations
    // nullable, so predicates on them must stop being pushed.
    for second in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"] {
        for pred in [
            "l.v > 15",
            "r.w > 150",
            "m.z > 7",
            "l.tag IS NULL",
            "r.label IS NULL",
            "l.v > 15 AND m.z > 7",
        ] {
            let sql = format!(
                "SELECT l.v, r.w, m.z FROM l JOIN r ON l.k = r.k \
                 {second} m ON r.k = m.k WHERE {pred} ORDER BY 1, 2, 3"
            );
            agree(&sql);
        }
    }
}

#[test]
fn the_case_that_disproved_the_first_safety_argument() {
    // Kept as its own test because it is the reason the rule is what it is.
    // Pre-filtering `r` on `label IS NULL` empties it of everything except the
    // one NULL-labelled row; without the nullable guard, the LEFT JOIN then
    // manufactures outer rows that SATISFY the retained filter.
    let sql = "SELECT l.v, r.w FROM l LEFT JOIN r ON l.k = r.k \
               WHERE r.label IS NULL ORDER BY 1";
    let prefiltered = agree(sql);
    assert_eq!(prefiltered, 0, "r is nullable here and must not be pre-filtered");

    let sel = parse(sql).unwrap();
    let (_, rows, plan) = execute_with(&sel, &resolve, JoinExec::Auto, true).unwrap();
    // l.k=2 matches r.w=200 whose label IS NULL; l.k=3 and l.k=null match
    // nothing and are NULL-extended, so their r.label IS NULL too.
    assert_eq!(rows.len(), 3, "{}", json!(rows));
    assert!(
        plan.refusals.iter().any(|r| r.contains("nullable side")),
        "the refusal must be recorded, not silent: {:?}",
        plan.refusals
    );
}

#[test]
fn an_inner_join_pushes_and_says_so_in_the_plan() {
    let sel = parse(
        "SELECT l.v, r.w FROM l JOIN r ON l.k = r.k WHERE l.v > 15 AND r.w > 150",
    )
    .unwrap();
    let (_, _, plan) = execute_with(&sel, &resolve, JoinExec::Auto, true).unwrap();

    let pre: Vec<&Stage> = plan
        .stages
        .iter()
        .filter(|s| matches!(s, Stage::Prefilter { .. }))
        .collect();
    assert_eq!(pre.len(), 2, "one per relation: {:?}", plan.stages);
    assert!(plan.refusals.is_empty(), "{:?}", plan.refusals);

    // And the pre-filter really shrank the inputs.
    for s in pre {
        if let Stage::Prefilter { in_rows, out_rows, .. } = s {
            assert!(out_rows < in_rows, "{s:?} removed nothing");
        }
    }
}

#[test]
fn a_predicate_spanning_both_relations_is_refused_with_a_reason() {
    let sel = parse("SELECT l.v FROM l JOIN r ON l.k = r.k WHERE l.v > r.w").unwrap();
    let (_, _, plan) = execute_with(&sel, &resolve, JoinExec::Auto, true).unwrap();
    assert!(!plan.stages.iter().any(|s| matches!(s, Stage::Prefilter { .. })));
    assert!(
        plan.refusals.iter().any(|r| r.contains("spans more than one relation")),
        "{:?}",
        plan.refusals
    );
}

#[test]
fn the_plan_tree_survives_a_prefilter_above_a_scan() {
    // A Prefilter sits between a scan and the join that consumes it, so the
    // tree builder has to look past it to find the join's right input.
    // Getting this wrong flattens the tree and loses the sibling structure.
    let sel = parse(
        "SELECT l.v, r.w FROM l JOIN r ON l.k = r.k WHERE l.v > 15 AND r.w > 150",
    )
    .unwrap();
    let (_, _, plan) = execute_with(&sel, &resolve, JoinExec::Auto, true).unwrap();
    let t = plan.tree().expect("a tree");

    // Walk down the unary postfix stages to the join.
    let mut node = &t;
    while node.children().len() == 1 {
        node = node.children()[0];
    }
    assert_eq!(node.children().len(), 2, "the join still has two inputs:\n{:#?}", t);
    for c in node.children() {
        // Each input is a Prefilter wrapping its Scan.
        assert!(matches!(c.stage(), Stage::Prefilter { .. }), "{:?}", c.stage());
        assert_eq!(c.children().len(), 1);
        assert!(matches!(c.children()[0].stage(), Stage::Scan { .. }));
    }
}
