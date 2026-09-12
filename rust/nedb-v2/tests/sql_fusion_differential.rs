// SPDX-License-Identifier: BUSL-1.1
// SPDX-FileCopyrightText: © 2026 INTERCHAINED LLC × Claude Sonnet 4.6

//! Differential tests: the `WHERE` clause fused into the join, against the
//! `WHERE` clause as a separate pass.
//!
//! # The invariant
//!
//! Evaluating the filter inside the join loop must not change any answer.
//! It is a physical change only.
//!
//! # The one thing that makes it a physical change and not a semantic one
//!
//! An `ON` predicate and a post-join `WHERE` predicate mean different things:
//!
//! ```text
//!   LEFT JOIN ... ON a.k = b.k AND b.tag = 'q'     keeps every left row
//!   LEFT JOIN ... ON a.k = b.k WHERE b.tag = 'q'   discards the outer rows
//! ```
//!
//! One loop may evaluate both, but the loop must keep them apart. Concretely:
//! whether a row counts as MATCHED is decided by `ON` alone. If the filter
//! were allowed to influence it, a left row whose only partner fails the
//! filter would be treated as unmatched, get NULL-extended, and a filter like
//! `WHERE b.tag IS NULL` would then ACCEPT that synthesised row — inventing
//! output the unfused pipeline never produces.
//!
//! [`the_filter_must_not_decide_what_counts_as_matched`] is that case,
//! isolated to one left row and one right row so the failure is unmistakable.

use nedb_engine::sqljoin::JoinExec;
use nedb_engine::sqlplan::Stage;
use nedb_engine::sqlselect::{execute_opts, parse, Opts};
use serde_json::{json, Value};

fn relation(name: &str) -> Option<Vec<Value>> {
    Some(match name {
        "l" => vec![
            json!({"k": 1, "v": 10, "tag": "a"}),
            json!({"k": 2, "v": 20, "tag": null}),
            json!({"k": 3, "v": 30, "tag": "c"}),
            json!({"k": null, "v": 40, "tag": "d"}),
            json!({"k": 1, "v": 50, "tag": "a"}),
        ],
        "r" => vec![
            json!({"k": 1, "w": 100, "label": "x"}),
            json!({"k": 2, "w": 200, "label": null}),
            json!({"k": 9, "w": 300, "label": "orphan"}),
            json!({"k": null, "w": 400, "label": "limbo"}),
        ],
        "m" => vec![json!({"k": 1, "z": 7}), json!({"k": 2, "z": 8})],
        _ => return None,
    })
}

fn resolve(name: &str) -> anyhow::Result<Option<Box<dyn nedb_engine::sqlselect::Relation>>> {
    Ok(relation(name).map(nedb_engine::sqlselect::from_vec))
}

/// Run with the filter fused and unfused, under both join strategies, and
/// assert all four agree. Returns how many runs actually fused.
#[track_caller]
fn agree(sql: &str) -> usize {
    let sel = parse(sql).unwrap_or_else(|e| panic!("{sql}\n  failed to parse: {e:#}"));
    let mut baseline: Option<(Vec<String>, Vec<Value>)> = None;
    let mut fused_runs = 0usize;

    for exec in [JoinExec::NestedLoop, JoinExec::Hash] {
        for fuse_filter in [false, true] {
            let opts = Opts { exec, pushdown: true, fuse_filter };
            let (cols, rows, plan) = execute_opts(&sel, &resolve, opts)
                .unwrap_or_else(|e| panic!("{sql}\n  failed {opts:?}: {e:#}"));
            let names: Vec<String> = cols.into_iter().map(|c| c.name).collect();

            let fused = plan.stages.iter().any(
                |s| matches!(s, Stage::Join { post_filter_removed: Some(_), .. }),
            );
            if fused {
                fused_runs += 1;
                assert!(fuse_filter, "{sql}\n  fused with fusion DISABLED");
                // The separate Filter stage must be gone, or the filter ran
                // twice and the plan is lying about one of them.
                assert!(
                    !plan.stages.iter().any(|s| matches!(s, Stage::Filter { .. })),
                    "{sql}\n  filter both fused AND run as a stage:\n{}",
                    plan.render().join("\n")
                );
            }

            match &baseline {
                None => baseline = Some((names, rows)),
                Some((bn, br)) => {
                    assert_eq!(bn, &names, "{sql}\n  columns differ under {opts:?}");
                    assert_eq!(
                        br, &rows,
                        "{sql}\n  ROWS DIFFER under {opts:?}\n  baseline = {}\n  \
                         got      = {}\n  plan:\n{}",
                        json!(br),
                        json!(rows),
                        plan.render().join("\n")
                    );
                }
            }
        }
    }
    fused_runs
}

const KINDS: &[&str] = &["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"];

/// Predicates chosen to attack NULL synthesis from both directions: some are
/// UNKNOWN over a synthesised NULL, others are SATISFIED by one.
const PREDICATES: &[&str] = &[
    "l.v > 15",
    "r.w > 150",
    "r.label = 'x'",
    "r.label IS NULL",
    "l.tag IS NULL",
    "r.k IS NULL",
    "NOT (r.label = 'x')",
    "r.label NOT IN ('x')",
    "coalesce(r.label, 'none') = 'none'",
    "l.v > r.w",
    "l.v > 15 AND r.w > 150",
    "l.tag IS NULL AND r.label IS NULL",
    "l.v > 15 OR r.label IS NULL",
];

#[test]
fn fusing_the_filter_never_changes_an_answer() {
    let mut fused = 0;
    for kind in KINDS {
        for pred in PREDICATES {
            for tail in ["", " ORDER BY 1, 2", " LIMIT 3", " LIMIT 2 OFFSET 1"] {
                let sql = format!(
                    "SELECT l.v, r.w FROM l {kind} r ON l.k = r.k WHERE {pred}{tail}"
                );
                fused += agree(&sql);
            }
        }
    }
    assert!(
        fused > 100,
        "only {fused} runs fused — the suite is not exercising the fusion"
    );
}

#[test]
fn fusing_never_changes_an_answer_across_two_joins() {
    // The filter fuses into the FINAL join only, because that is the first
    // point at which every binding it might read is bound.
    for second in KINDS {
        for pred in [
            "l.v > 15",
            "m.z > 7",
            "r.label IS NULL",
            "m.z IS NULL",
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
fn the_filter_must_not_decide_what_counts_as_matched() {
    // One left row, one right row, and they DO join. The filter then rejects
    // the pair. The correct answer is no rows.
    //
    // If `matched` were influenced by the filter, the left row would be
    // treated as unmatched, NULL-extended, and `r.label IS NULL` would be TRUE
    // for the synthesised row — inventing a row from nothing.
    fn one(name: &str) -> anyhow::Result<Option<Box<dyn nedb_engine::sqlselect::Relation>>> {
        Ok(Some(nedb_engine::sqlselect::from_vec(match name {
            "l" => vec![json!({"k": 1, "v": 10})],
            "r" => vec![json!({"k": 1, "label": "q"})],
            _ => return Ok(None),
        })))
    }
    let sel = parse(
        "SELECT l.v FROM l LEFT JOIN r ON l.k = r.k WHERE r.label IS NULL",
    )
    .unwrap();

    for fuse_filter in [false, true] {
        for exec in [JoinExec::NestedLoop, JoinExec::Hash] {
            let (_, rows, plan) =
                execute_opts(&sel, &one, Opts { exec, pushdown: true, fuse_filter }).unwrap();
            assert!(
                rows.is_empty(),
                "the pair joined and was then filtered out, so there is nothing to \
                 return — a NULL-extended row here is invented\n  fuse={fuse_filter} \
                 exec={exec:?}\n  got {}\n  plan:\n{}",
                json!(rows),
                plan.render().join("\n")
            );
        }
    }
}

#[test]
fn the_same_predicate_in_on_still_keeps_the_outer_rows() {
    // The distinction the fusion must not erase. Same predicate, same data,
    // different clause, different answer.
    let in_where = parse(
        "SELECT l.v, r.w FROM l LEFT JOIN r ON l.k = r.k WHERE r.label = 'x' ORDER BY 1",
    )
    .unwrap();
    let in_on = parse(
        "SELECT l.v, r.w FROM l LEFT JOIN r ON l.k = r.k AND r.label = 'x' ORDER BY 1",
    )
    .unwrap();

    let (_, w, _) = execute_opts(&in_where, &resolve, Opts::default()).unwrap();
    let (_, o, _) = execute_opts(&in_on, &resolve, Opts::default()).unwrap();

    assert_eq!(w.len(), 2, "WHERE discards the outer rows: {}", json!(w));
    assert_eq!(o.len(), 5, "ON keeps every left row: {}", json!(o));
    assert_ne!(w, o, "if these ever agree, the fusion has merged ON and WHERE");
}

#[test]
fn right_outer_rows_face_the_post_join_filter_too() {
    // A `WHERE` applies to every row the join produced, including the rows it
    // synthesised for unmatched right-hand rows. Skipping them inside the
    // fused loop would leak rows the unfused pipeline drops.
    let sql = "SELECT l.v, r.w FROM l RIGHT JOIN r ON l.k = r.k \
               WHERE l.v IS NULL ORDER BY 2";
    agree(sql);
    let sel = parse(sql).unwrap();
    let (_, rows, _) = execute_opts(&sel, &resolve, Opts::default()).unwrap();
    // r.k = 9 and r.k = NULL match nothing, so their `l.v` is NULL.
    assert_eq!(rows.len(), 2, "{}", json!(rows));
}

// ─────────────────────────────────────────────────────────────────────────────
// The point of the exercise: a filtered join can now stop early
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn a_filtered_join_with_a_limit_is_now_budget_eligible() {
    let sel = parse(
        "SELECT l.v, r.w FROM l JOIN r ON l.k = r.k WHERE r.w > 50 LIMIT 1",
    )
    .unwrap();
    let (_, rows, plan) = execute_opts(&sel, &resolve, Opts::default()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(plan.budget, Some(1), "a WHERE no longer disqualifies the budget");
    assert!(
        plan.joins().iter().any(|s| matches!(
            s,
            Stage::Join { early_stopped: true, post_filter_removed: Some(_), .. }
        )),
        "the join should have filtered AND stopped early:\n{}",
        plan.render().join("\n")
    );
}

#[test]
fn a_limit_over_a_filtered_join_is_still_a_prefix() {
    // The property that makes early termination legitimate, now checked for
    // the filtered case as well.
    for kind in KINDS {
        for pred in ["r.w > 150", "r.label IS NULL", "l.v > 15"] {
            let base = format!(
                "SELECT l.v, r.w FROM l {kind} r ON l.k = r.k WHERE {pred}"
            );
            let sel = parse(&base).unwrap();
            let (_, all, _) = execute_opts(&sel, &resolve, Opts::default()).unwrap();

            for n in 0..=all.len() + 2 {
                let s2 = parse(&format!("{base} LIMIT {n}")).unwrap();
                let (_, got, _) = execute_opts(&s2, &resolve, Opts::default()).unwrap();
                let want: Vec<Value> = all.iter().take(n).cloned().collect();
                assert_eq!(got, want, "{base} LIMIT {n}");
            }
        }
    }
}

#[test]
fn ordering_and_distinct_still_disqualify_the_budget() {
    for (sql, why) in [
        (
            "SELECT l.v FROM l JOIN r ON l.k = r.k WHERE r.w > 50 ORDER BY 1 LIMIT 1",
            "ORDER BY decides the prefix",
        ),
        (
            "SELECT DISTINCT l.v FROM l JOIN r ON l.k = r.k WHERE r.w > 50 LIMIT 1",
            "DISTINCT can shrink the row count",
        ),
    ] {
        let sel = parse(sql).unwrap();
        let (_, _, plan) = execute_opts(&sel, &resolve, Opts::default()).unwrap();
        assert_eq!(plan.budget, None, "{why}: {sql}");
    }
}

#[test]
fn the_plan_reports_the_fused_filter_separately_from_the_join() {
    // An `ON` predicate and a post-join `WHERE` predicate mean different
    // things, so the plan reports them as different numbers even though one
    // loop evaluated both.
    let sel = parse(
        "SELECT l.v, r.w FROM l LEFT JOIN r ON l.k = r.k WHERE r.label IS NULL",
    )
    .unwrap();
    let (_, _, plan) = execute_opts(&sel, &resolve, Opts::default()).unwrap();
    let text = plan.render().join("\n");
    assert!(text.contains("post-join filter removed"), "{text}");
    // And the join's own output count is still reported.
    assert!(text.contains("actual rows="), "{text}");
}
