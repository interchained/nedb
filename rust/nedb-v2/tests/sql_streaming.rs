// SPDX-License-Identifier: BUSL-1.1
// SPDX-FileCopyrightText: © 2026 INTERCHAINED LLC × Claude Sonnet 4.6

//! Does the evaluator actually stop asking for rows?
//!
//! # The claim under test
//!
//! `LIMIT 20` over a join of 8000 rows must not pull 8000 source rows.
//!
//! That claim cannot be checked by timing — a fast run proves nothing about
//! how many rows were requested, and a slow one could be noise. So the source
//! itself counts every row handed out, and the assertions are about that
//! count. A demand-driven executor is a property of the CALL PATTERN, so the
//! call pattern is what gets measured.
//!
//! # What is deliberately NOT streamed
//!
//! The INNER side of every join is pulled whole, on purpose: a hash join has
//! to build its table before it can probe, and a nested loop re-scans the
//! inner side once per left row. Streaming it would save nothing and would
//! require rewinding. [`the_inner_side_is_pulled_whole_on_purpose`] pins that,
//! so the limitation is recorded rather than discovered later and mistaken for
//! a bug.

use nedb_engine::sqljoin::JoinExec;
use nedb_engine::sqlplan::Stage;
use nedb_engine::sqlselect::{execute_opts, parse, Opts, Relation};
use serde_json::{json, Value};
use std::cell::Cell;
use std::rc::Rc;

/// A relation that reports how many rows it was asked for.
struct Counting {
    rows: Vec<Value>,
    at: usize,
    pulled: Rc<Cell<usize>>,
    /// Reported to the planner. Kept honest — the point is to stream, not to
    /// hide the size from the strategy chooser.
    hint: Option<usize>,
}

impl Relation for Counting {
    fn next_row(&mut self) -> anyhow::Result<Option<Value>> {
        if self.at >= self.rows.len() {
            return Ok(None);
        }
        let v = self.rows[self.at].clone();
        self.at += 1;
        self.pulled.set(self.pulled.get() + 1);
        Ok(Some(v))
    }
    fn size_hint(&self) -> Option<usize> {
        self.hint
    }
}

const N_LEFT: usize = 8_000;
const N_RIGHT: usize = 1_500;

fn left_rows() -> Vec<Value> {
    (0..N_LEFT)
        .map(|i| json!({"k": i % 1_500, "v": i, "amount": (i * 7) % 1000}))
        .collect()
}

fn right_rows() -> Vec<Value> {
    (0..N_RIGHT).map(|i| json!({"k": i, "w": i * 10})).collect()
}

struct Counters {
    left: Rc<Cell<usize>>,
    right: Rc<Cell<usize>>,
}

impl Counters {
    fn new() -> Self {
        Counters { left: Rc::new(Cell::new(0)), right: Rc::new(Cell::new(0)) }
    }
}

/// Run a query against counting sources and report `(left pulled, right
/// pulled, rows out, plan)`.
fn run(sql: &str, exec: JoinExec) -> (usize, usize, usize, nedb_engine::sqlplan::Plan) {
    let c = Counters::new();
    let (lc, rc) = (Rc::clone(&c.left), Rc::clone(&c.right));
    let resolve = move |name: &str| -> anyhow::Result<Option<Box<dyn Relation>>> {
        Ok(match name {
            "l" => Some(Box::new(Counting {
                rows: left_rows(),
                at: 0,
                pulled: Rc::clone(&lc),
                hint: Some(N_LEFT),
            }) as Box<dyn Relation>),
            "r" => Some(Box::new(Counting {
                rows: right_rows(),
                at: 0,
                pulled: Rc::clone(&rc),
                hint: Some(N_RIGHT),
            }) as Box<dyn Relation>),
            _ => None,
        })
    };
    let sel = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e:#}"));
    let (_, rows, plan) =
        execute_opts(&sel, &resolve, Opts::exec(exec)).unwrap_or_else(|e| panic!("{sql}: {e:#}"));
    (c.left.get(), c.right.get(), rows.len(), plan)
}

// ─────────────────────────────────────────────────────────────────────────────
// The claim
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn a_limit_over_a_join_pulls_exactly_the_rows_it_needs() {
    // EXACTLY 20, not "fewer than some comfortable bound".
    //
    // A loose assertion here would pass at 99 pulls and hide a real
    // inefficiency, which is the same decorative-test failure as checking a
    // column's name without checking its value. The count is knowable, so it
    // is asserted.
    //
    // Why 20 is the right number: `l.k` is `i % 1500` and `r.k` is `0..1500`,
    // so every left row matches exactly ONE right row. Twenty left rows
    // therefore produce twenty output rows, the budget is met, and the join
    // stops asking.
    for exec in [JoinExec::NestedLoop, JoinExec::Hash] {
        let (left, right, out, plan) =
            run("SELECT l.v, r.w FROM l JOIN r ON l.k = r.k LIMIT 20", exec);

        assert_eq!(out, 20, "{exec:?}");
        assert_eq!(
            left, 20,
            "{exec:?}: pulled {left} of {N_LEFT} left rows to return 20\n{}",
            plan.render().join("\n")
        );
        // The inner side is whole by design — see the module docs.
        assert_eq!(right, N_RIGHT, "{exec:?}: the inner side is materialised");
    }
}

#[test]
fn an_outer_join_with_a_limit_is_equally_tight() {
    // A LEFT JOIN emits one row per left row regardless of matching, so the
    // budget is met after exactly 20 either way.
    for exec in [JoinExec::NestedLoop, JoinExec::Hash] {
        let (left, _, out, _) =
            run("SELECT l.v, r.w FROM l LEFT JOIN r ON l.k = r.k LIMIT 20", exec);
        assert_eq!(out, 20, "{exec:?}");
        assert_eq!(left, 20, "{exec:?}");
    }
}

#[test]
fn a_filtered_limit_over_a_join_also_stops_early() {
    // #120 made this eligible by fusing the filter into the join. Before that
    // a WHERE disqualified the budget and the whole source had to be read.
    for exec in [JoinExec::NestedLoop, JoinExec::Hash] {
        let (left, _, out, plan) = run(
            "SELECT l.v, r.w FROM l JOIN r ON l.k = r.k WHERE l.amount > 500 LIMIT 20",
            exec,
        );
        assert_eq!(out, 20, "{exec:?}");
        // EXACTLY 92, and the arithmetic is worth writing down because the
        // number looks arbitrary and is not.
        //
        // `amount` is `(7 * i) % 1000`. For `i` in 0..=71 that is `7i`, which
        // tops out at 497 — so the first 72 rows ALL fail `> 500`. From
        // `i = 72` (504) onward they all pass, so rows 72..=91 supply the 20
        // survivors. 72 rejected + 20 kept = 92 pulled, which is minimal for
        // this data.
        assert_eq!(
            left, 92,
            "{exec:?}: pulled {left} of {N_LEFT} with a filter + LIMIT 20\n{}",
            plan.render().join("\n")
        );
    }
}

#[test]
fn an_offset_is_added_to_the_budget_rather_than_ignored() {
    let (left, _, out, _) =
        run("SELECT l.v FROM l JOIN r ON l.k = r.k LIMIT 5 OFFSET 40", JoinExec::Hash);
    assert_eq!(out, 5);
    // EXACTLY 45 = 40 skipped + 5 returned. The offset rows still have to be
    // produced, so they are charged to the budget rather than wished away —
    // and not one row beyond them is read.
    assert_eq!(left, 45, "pulled {left}, expected offset + limit");
}

// ─────────────────────────────────────────────────────────────────────────────
// The same assertions from the other side, so none of the above is vacuous
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn without_a_limit_the_whole_source_is_read() {
    // If this ever passes with a small count, the tests above are measuring
    // nothing — a source that simply never gets asked would satisfy them too.
    let (left, right, out, _) =
        run("SELECT l.v, r.w FROM l JOIN r ON l.k = r.k", JoinExec::Hash);
    assert_eq!(left, N_LEFT, "no LIMIT means every left row is needed");
    assert_eq!(right, N_RIGHT);
    // `l.k` is `i % 1500` and `r.k` is `0..1500`, so every left row matches
    // exactly ONE right row — output equals the left count rather than
    // exceeding it.
    assert_eq!(out, N_LEFT, "one match per left row");
}

#[test]
fn an_order_by_still_reads_everything_because_it_must() {
    // The prefix depends on the sort, not on emission order, so the budget is
    // correctly refused. Stated as a test so the limitation is deliberate and
    // visible rather than an accident of the implementation.
    let (left, _, out, plan) = run(
        "SELECT l.v FROM l JOIN r ON l.k = r.k ORDER BY l.v DESC LIMIT 20",
        JoinExec::Hash,
    );
    assert_eq!(out, 20);
    assert_eq!(left, N_LEFT, "sorting needs every row first");
    assert_eq!(plan.budget, None);
}

#[test]
fn distinct_also_reads_everything_because_it_must() {
    let (left, _, _, plan) = run(
        "SELECT DISTINCT l.k FROM l JOIN r ON l.k = r.k LIMIT 20",
        JoinExec::Hash,
    );
    assert_eq!(left, N_LEFT, "dedup can shrink the count, so nothing may be skipped");
    assert_eq!(plan.budget, None);
}

#[test]
fn the_inner_side_is_pulled_whole_on_purpose() {
    // A hash join builds its table before probing; a nested loop re-scans the
    // inner side per left row. Neither can work from a one-shot stream, so
    // this is a designed limit rather than an oversight.
    for exec in [JoinExec::NestedLoop, JoinExec::Hash] {
        let (_, right, _, _) =
            run("SELECT l.v FROM l JOIN r ON l.k = r.k LIMIT 1", exec);
        assert_eq!(right, N_RIGHT, "{exec:?}");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// The plan must not claim rows it never read
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn the_plan_reports_rows_pulled_not_rows_available() {
    // `EXPLAIN` says "actual rows", and for a streamed relation the actual
    // number is what was requested. Printing the source's full size would be
    // a plausible number that describes work the engine did not do.
    let (left, _, _, plan) =
        run("SELECT l.v, r.w FROM l JOIN r ON l.k = r.k LIMIT 20", JoinExec::Hash);

    let scan_l = plan
        .stages
        .iter()
        .find_map(|s| match s {
            Stage::Scan { binding, rows, .. } if binding == "l" => Some(*rows),
            _ => None,
        })
        .expect("a scan of l");

    assert_eq!(scan_l, left, "the plan must report what was pulled");
    assert!(scan_l < 100, "and that must be the small number: {scan_l}");
}

#[test]
fn a_prefilter_on_a_streamed_relation_reports_honest_counts() {
    let (left, _, _, plan) = run(
        "SELECT l.v, r.w FROM l JOIN r ON l.k = r.k WHERE l.amount > 500 LIMIT 20",
        JoinExec::Hash,
    );
    let pre = plan.stages.iter().find_map(|s| match s {
        Stage::Prefilter { binding, in_rows, out_rows, .. } if binding == "l" => {
            Some((*in_rows, *out_rows))
        }
        _ => None,
    });
    let (in_rows, out_rows) = pre.expect("a prefilter on l");
    assert_eq!(in_rows, left, "in_rows is what the source delivered");
    assert!(out_rows <= in_rows);
    assert!(out_rows >= 20, "at least the budget survived the filter");
}

// ─────────────────────────────────────────────────────────────────────────────
// Streaming must not change answers
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn the_early_stopped_answer_is_the_prefix_of_the_full_one() {
    // Cheap to get wrong and impossible to notice without asserting it: the
    // rows returned must be the FIRST 20, not any 20.
    let sel_all = parse("SELECT l.v, r.w FROM l JOIN r ON l.k = r.k").unwrap();
    let sel_lim = parse("SELECT l.v, r.w FROM l JOIN r ON l.k = r.k LIMIT 20").unwrap();

    let resolve = |name: &str| -> anyhow::Result<Option<Box<dyn Relation>>> {
        Ok(match name {
            "l" => Some(nedb_engine::sqlselect::from_vec(left_rows())),
            "r" => Some(nedb_engine::sqlselect::from_vec(right_rows())),
            _ => None,
        })
    };

    for exec in [JoinExec::NestedLoop, JoinExec::Hash] {
        let (_, all, _) = execute_opts(&sel_all, &resolve, Opts::exec(exec)).unwrap();
        let (_, lim, _) = execute_opts(&sel_lim, &resolve, Opts::exec(exec)).unwrap();
        assert_eq!(lim, all[..20].to_vec(), "{exec:?}");
    }
}

#[test]
fn a_source_that_does_not_know_its_size_still_works() {
    // A real storage scan will not know its length up front. The planner has
    // to cope without a hint rather than assuming one is always available.
    let resolve = |name: &str| -> anyhow::Result<Option<Box<dyn Relation>>> {
        Ok(match name {
            "l" => Some(Box::new(Counting {
                rows: left_rows(),
                at: 0,
                pulled: Rc::new(Cell::new(0)),
                hint: None,
            }) as Box<dyn Relation>),
            "r" => Some(nedb_engine::sqlselect::from_vec(right_rows())),
            _ => None,
        })
    };
    let sel = parse("SELECT l.v, r.w FROM l JOIN r ON l.k = r.k LIMIT 20").unwrap();
    let (_, rows, plan) = execute_opts(&sel, &resolve, Opts::default()).unwrap();
    assert_eq!(rows.len(), 20);
    // With no hint the planner cannot size the left side, and picks the hash
    // path because a key exists. Deterministic, and both paths are proven
    // equivalent, so the choice costs time at worst.
    assert_eq!(
        plan.join_strategy(0),
        Some(nedb_engine::sqljoin::Strategy::Hash),
        "{}",
        plan.render().join("\n")
    );
}
