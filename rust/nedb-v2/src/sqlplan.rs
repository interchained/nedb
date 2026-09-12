// SPDX-License-Identifier: BUSL-1.1
// SPDX-FileCopyrightText: © 2026 INTERCHAINED LLC × Claude Sonnet 4.6

//! The execution plan — what the evaluator actually did, and how to render it.
//!
//! # Why this exists
//!
//! Before this, parsing, semantics, optimisation and execution all lived in one
//! growing function. That is survivable while there is one execution strategy
//! and no rewrites; it stops being survivable the moment a query can be run
//! more than one way, because there is then no place to record WHICH way was
//! chosen or WHY.
//!
//! This is deliberately not a PostgreSQL planner. There is no cost model, no
//! statistics, and no search over join orders. It is a record of the pipeline
//! that ran, with the real row counts it moved.
//!
//! # The plan is EMITTED by execution, never written alongside it
//!
//! Every node here is appended by the executor as it does the work, and the
//! row counts are the counts it actually observed. That is a design constraint,
//! not an implementation detail: a plan assembled independently of the executor
//! can drift out of agreement with it, and an `EXPLAIN` that confidently
//! describes a pipeline the engine did not run is worse than having no
//! `EXPLAIN` at all — it sends the reader to optimise a query shape that never
//! existed.
//!
//! For the same reason it is stored as a PIPELINE (a `Vec` of stages) rather
//! than a tree: the executor is a pipeline — materialise, join, filter,
//! project, sort, paginate — and a tree structure would imply a generality it
//! does not have.
//!
//! The one exception is a join, which genuinely has two inputs, and
//! [`Plan::render`] accounts for that by printing them as siblings. Getting
//! that wrong is not cosmetic: the first version indented the two scans
//! differently, which reads as "the left relation was scanned inside the scan
//! of the right one" — a claim about the execution that was simply false.
//!
//! # Consequently, `EXPLAIN` here always reports actual rows
//!
//! PostgreSQL's bare `EXPLAIN` estimates without executing, and `EXPLAIN
//! ANALYZE` executes and reports reality. NEDB has no statistics to estimate
//! from, so an estimate would be a guess dressed as a number. It executes and
//! reports what happened. Stated in the output so nobody mistakes one for the
//! other.

use crate::sqljoin::Strategy;
use crate::sqlselect::JoinKind;

/// One stage of the pipeline that ran.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stage {
    /// A base relation was materialised.
    Scan {
        table: String,
        /// The name its columns are addressed by — the alias when one was
        /// given. Shown because a plan that does not name its bindings is
        /// unreadable for a self-join.
        binding: String,
        rows: usize,
    },
    /// Two inputs were joined.
    Join {
        kind: JoinKind,
        table: String,
        binding: String,
        strategy: Strategy,
        /// Equality key pairs the planner could PROVE usable. Zero means the
        /// hash path was unavailable, which is the single most useful number
        /// in the plan when a join is unexpectedly slow.
        keys: usize,
        left_rows: usize,
        right_rows: usize,
        out_rows: usize,
        /// The join stopped early because the row budget was already met.
        early_stopped: bool,
        /// Rows this join produced and then discarded because the `WHERE`
        /// clause was evaluated inside it. `None` means the filter ran as a
        /// separate stage.
        ///
        /// Reported separately from the join's own row count precisely so the
        /// two stay distinguishable: an `ON` predicate and a post-join
        /// `WHERE` predicate mean different things, and the plan should not
        /// blur them just because one loop evaluates both.
        post_filter_removed: Option<usize>,
    },
    /// A `WHERE` clause was applied.
    Filter { in_rows: usize, out_rows: usize },
    /// The select list was evaluated.
    Project { columns: usize, out_rows: usize },
    /// `DISTINCT` removed duplicates.
    Distinct { in_rows: usize, out_rows: usize },
    /// `ORDER BY` sorted the rows.
    Sort { keys: usize, rows: usize },
    /// A `WHERE` conjunct was pre-applied to a base relation before the join.
    ///
    /// The original `WHERE` still runs afterwards — this is a copy, not a
    /// move, which is what makes it safe for every join type.
    Prefilter {
        binding: String,
        predicates: usize,
        in_rows: usize,
        out_rows: usize,
    },
    /// `LIMIT` / `OFFSET` were applied.
    Limit {
        limit: Option<usize>,
        offset: Option<usize>,
        in_rows: usize,
        out_rows: usize,
    },
}

/// The pipeline as a TREE, which is what it actually is.
///
/// Every stage has one input except a join, which has two. Tests assert over
/// this rather than over rendered text, because the bug that shipped in the
/// first `EXPLAIN` was a TOPOLOGY bug: a join's two scans were printed at
/// different depths, so `pg_class` read as a child of the scan of
/// `pg_namespace`. Every assertion at the time checked content — which
/// relation, how many rows — and content was correct. Structure was not.
///
/// ```text
///   Join            is NOT        Join
///   ├── Scan A                    └── Scan A
///   └── Scan B                        └── Scan B
/// ```
///
/// A string assertion can be made to pass by either shape. A tree assertion
/// cannot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanTree {
    Leaf(Stage),
    Unary { stage: Stage, input: Box<PlanTree> },
    Binary { stage: Stage, left: Box<PlanTree>, right: Box<PlanTree> },
}

impl PlanTree {
    pub fn stage(&self) -> &Stage {
        match self {
            PlanTree::Leaf(s) => s,
            PlanTree::Unary { stage, .. } => stage,
            PlanTree::Binary { stage, .. } => stage,
        }
    }

    /// Inputs, in the order PostgreSQL prints them: outer side first.
    pub fn children(&self) -> Vec<&PlanTree> {
        match self {
            PlanTree::Leaf(_) => vec![],
            PlanTree::Unary { input, .. } => vec![input],
            PlanTree::Binary { left, right, .. } => vec![left, right],
        }
    }

    /// Total node count, so a test can assert nothing was dropped.
    pub fn size(&self) -> usize {
        1 + self.children().iter().map(|c| c.size()).sum::<usize>()
    }

    /// The deepest path length, which is what distinguishes siblings from
    /// nesting: two scans under a join give depth 2, one nested under the
    /// other gives depth 3.
    pub fn depth(&self) -> usize {
        1 + self.children().iter().map(|c| c.depth()).max().unwrap_or(0)
    }
}

/// What ran, in the order it ran.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Plan {
    pub stages: Vec<Stage>,
    /// Set when the row budget let a stage stop before consuming its input.
    pub budget: Option<usize>,
    /// Why the optimiser declined to push a predicate. Recorded rather than
    /// silent: you cannot tell "correctly refused" from "forgot to look" if
    /// the decision leaves no trace.
    pub refusals: Vec<String>,
}

impl Plan {
    pub fn push(&mut self, s: Stage) {
        self.stages.push(s);
    }

    /// The join stages, for callers that only care how joins were executed.
    ///
    /// Keeps the differential tests and the benchmark reading the same report
    /// the plan is built from, rather than a second source of truth.
    pub fn joins(&self) -> Vec<&Stage> {
        self.stages
            .iter()
            .filter(|s| matches!(s, Stage::Join { .. }))
            .collect()
    }

    /// The strategy of the nth join, if there is one.
    pub fn join_strategy(&self, n: usize) -> Option<Strategy> {
        match self.joins().get(n) {
            Some(Stage::Join { strategy, .. }) => Some(*strategy),
            _ => None,
        }
    }

    /// The strategy of every join, in execution order.
    pub fn join_strategies(&self) -> Vec<Strategy> {
        self.stages
            .iter()
            .filter_map(|s| match s {
                Stage::Join { strategy, .. } => Some(*strategy),
                _ => None,
            })
            .collect()
    }

    /// Proven equality key count of the nth join.
    pub fn join_keys(&self, n: usize) -> Option<usize> {
        match self.joins().get(n) {
            Some(Stage::Join { keys, .. }) => Some(*keys),
            _ => None,
        }
    }

    /// The pipeline as a tree. `None` when nothing ran (`SELECT 1`).
    ///
    /// Stages are recorded as: the base scan, then per join its right-hand
    /// scan followed by the join itself, then the postfix stages. That shape
    /// is what makes the reconstruction unambiguous.
    pub fn tree(&self) -> Option<PlanTree> {
        let mut it = self.stages.iter();
        let mut node = PlanTree::Leaf(it.next()?.clone());
        let rest: Vec<&Stage> = it.collect();
        let mut i = 0usize;
        while i < rest.len() {
            // A right-hand input is a Scan, optionally wrapped in the
            // Prefilter that was pushed into it. The join therefore sits one
            // or two stages after the scan, and the pair detection has to look
            // past the prefilter or it would mistake the join for a unary
            // stage and flatten the tree.
            let right_len = match (rest.get(i), rest.get(i + 1), rest.get(i + 2)) {
                (Some(Stage::Scan { .. }), Some(Stage::Join { .. }), _) => Some(1),
                (
                    Some(Stage::Scan { .. }),
                    Some(Stage::Prefilter { .. }),
                    Some(Stage::Join { .. }),
                ) => Some(2),
                _ => None,
            };
            if let Some(n) = right_len {
                let mut right = PlanTree::Leaf(rest[i].clone());
                if n == 2 {
                    right = PlanTree::Unary {
                        stage: rest[i + 1].clone(),
                        input: Box::new(right),
                    };
                }
                node = PlanTree::Binary {
                    stage: rest[i + n].clone(),
                    left: Box::new(node),
                    right: Box::new(right),
                };
                i += n + 1;
            } else {
                node = PlanTree::Unary {
                    stage: rest[i].clone(),
                    input: Box::new(node),
                };
                i += 1;
            }
        }
        Some(node)
    }

    /// Render as `EXPLAIN` output: one string per line, innermost first, the
    /// way PostgreSQL nests its plan tree.
    ///
    /// The pipeline is linear, so indentation grows monotonically. A reader
    /// familiar with PostgreSQL's output will read this correctly; a reader who
    /// is not still sees the order things happened in.
    pub fn render(&self) -> Vec<String> {
        let mut out = vec![];
        if let Some(t) = self.tree() {
            render_node(&t, 0, &mut out);
        }

        for r in &self.refusals {
            out.push(r.clone());
        }
        if let Some(b) = self.budget {
            out.push(format!(
                "Row budget: {b} — the join was allowed to stop once this many \
                 rows existed"
            ));
        }
        out.push(
            "NEDB reports ACTUAL rows, never estimates: it has no statistics to \
             estimate from, and a guess printed as a number is worse than the truth."
                .to_string(),
        );
        out
    }
}

/// Walk the tree, outermost first, each input indented under the stage that
/// consumes it.
///
/// Recursing over the tree is what makes a join's two inputs siblings without
/// a special case: they are children of the same node, so they get the same
/// depth by construction. The first version of this walked a flat list and
/// tried to patch the sibling case by hand, which is how it got the topology
/// wrong.
fn render_node(n: &PlanTree, depth: usize, out: &mut Vec<String>) {
    let indent = "  ".repeat(depth);
    let arrow = if depth == 0 { String::new() } else { format!("{indent}-> ") };
    let line = match n.stage() {
        Stage::Scan { table, binding, rows } => {
            format!("{arrow}Seq Scan on {}  (actual rows={rows})", named(table, binding))
        }
        Stage::Join {
            kind,
            table,
            binding,
            strategy,
            keys,
            left_rows,
            right_rows,
            out_rows,
            early_stopped,
            post_filter_removed,
        } => {
            let k = match keys {
                0 => "no equality key".to_string(),
                1 => "1 hash key".to_string(),
                n => format!("{n} hash keys"),
            };
            let stop = if *early_stopped { ", stopped early" } else { "" };
            let filt = match post_filter_removed {
                Some(n) => format!(", post-join filter removed {n}"),
                None => String::new(),
            };
            format!(
                "{arrow}{strategy} {} Join on {} \
                 ({k}, left={left_rows}, right={right_rows}{stop}{filt}) \
                 (actual rows={out_rows})",
                kind_name(*kind),
                named(table, binding)
            )
        }
        Stage::Filter { in_rows, out_rows } => format!(
            "{arrow}Filter  (removed {}) (actual rows={out_rows})",
            in_rows.saturating_sub(*out_rows)
        ),
        Stage::Project { columns, out_rows } => {
            format!("{arrow}Project  ({columns} columns) (actual rows={out_rows})")
        }
        Stage::Distinct { in_rows, out_rows } => format!(
            "{arrow}Unique  (removed {}) (actual rows={out_rows})",
            in_rows.saturating_sub(*out_rows)
        ),
        Stage::Sort { keys, rows } => {
            format!("{arrow}Sort  ({keys} key(s)) (actual rows={rows})")
        }
        Stage::Prefilter { binding, predicates, in_rows, out_rows } => format!(
            "{arrow}Prefilter on {binding}  ({predicates} pushed, removed {}) \
             (actual rows={out_rows})",
            in_rows.saturating_sub(*out_rows)
        ),
        Stage::Limit { limit, offset, in_rows, out_rows } => {
            let l = limit.map(|n| n.to_string()).unwrap_or_else(|| "ALL".into());
            let o = offset.map(|n| format!(", offset {n}")).unwrap_or_default();
            format!("{arrow}Limit  ({l}{o}, from {in_rows}) (actual rows={out_rows})")
        }
    };
    out.push(line);
    for c in n.children() {
        render_node(c, depth + 1, out);
    }
}

/// `orders` or `orders o` — a redundant alias is not repeated.
fn named(table: &str, binding: &str) -> String {
    if table == binding {
        table.to_string()
    } else {
        format!("{table} {binding}")
    }
}

fn kind_name(k: JoinKind) -> &'static str {
    match k {
        JoinKind::Inner => "Inner",
        JoinKind::Left => "Left",
        JoinKind::Right => "Right",
        JoinKind::Full => "Full",
        JoinKind::Cross => "Cross",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(t: &str, rows: usize) -> Stage {
        Stage::Scan { table: t.into(), binding: t.into(), rows }
    }

    #[test]
    fn an_empty_plan_still_explains_itself() {
        let p = Plan::default();
        let r = p.render();
        assert_eq!(r.len(), 1);
        assert!(r[0].contains("ACTUAL rows"));
    }

    #[test]
    fn a_scan_renders_with_actual_rows() {
        let mut p = Plan::default();
        p.push(scan("orders", 1000));
        let r = p.render();
        assert!(r[0].starts_with("Seq Scan on orders"), "{:?}", r[0]);
        assert!(r[0].contains("actual rows=1000"));
    }

    #[test]
    fn an_alias_is_shown_but_a_redundant_one_is_not() {
        let mut p = Plan::default();
        p.push(Stage::Scan { table: "orders".into(), binding: "o".into(), rows: 1 });
        assert!(p.render()[0].contains("orders o"));

        let mut p = Plan::default();
        p.push(scan("orders", 1));
        assert!(!p.render()[0].contains("orders orders"));
    }

    #[test]
    fn the_outermost_stage_is_printed_first() {
        let mut p = Plan::default();
        p.push(scan("orders", 100));
        p.push(Stage::Limit { limit: Some(5), offset: None, in_rows: 100, out_rows: 5 });
        let r = p.render();
        assert!(r[0].starts_with("Limit"), "{r:?}");
        assert!(r[1].contains("Seq Scan"), "{r:?}");
        // The input is indented under the operation that consumes it.
        assert!(r[1].starts_with("  -> "), "{:?}", r[1]);
    }

    #[test]
    fn a_join_names_its_strategy_and_key_count() {
        let mut p = Plan::default();
        p.push(scan("orders", 1000));
        p.push(Stage::Join {
            kind: JoinKind::Inner,
            table: "customers".into(),
            binding: "c".into(),
            strategy: Strategy::Hash,
            keys: 2,
            left_rows: 1000,
            right_rows: 500,
            out_rows: 1922,
            early_stopped: false,
            post_filter_removed: None,
        });
        let r = p.render();
        assert!(r[0].contains("Hash Join"), "{:?}", r[0]);
        assert!(r[0].contains("Inner"));
        assert!(r[0].contains("2 hash keys"));
        assert!(r[0].contains("customers c"));
        assert!(r[0].contains("actual rows=1922"));
    }

    #[test]
    fn a_join_with_no_key_says_so_because_that_is_why_it_is_slow() {
        let mut p = Plan::default();
        p.push(Stage::Join {
            kind: JoinKind::Inner,
            table: "customers".into(),
            binding: "customers".into(),
            strategy: Strategy::NestedLoop,
            keys: 0,
            left_rows: 1000,
            right_rows: 500,
            out_rows: 2500,
            early_stopped: false,
            post_filter_removed: None,
        });
        let r = p.render();
        assert!(r[0].contains("Nested Loop"));
        assert!(r[0].contains("no equality key"), "{:?}", r[0]);
    }

    #[test]
    fn early_termination_is_visible() {
        let mut p = Plan::default();
        p.push(Stage::Join {
            kind: JoinKind::Inner,
            table: "c".into(),
            binding: "c".into(),
            strategy: Strategy::Hash,
            keys: 1,
            left_rows: 1000,
            right_rows: 500,
            out_rows: 20,
            early_stopped: true,
            post_filter_removed: None,
        });
        p.budget = Some(20);
        let r = p.render();
        assert!(r[0].contains("stopped early"), "{:?}", r[0]);
        assert!(r.iter().any(|l| l.contains("Row budget: 20")));
    }

    #[test]
    fn filter_and_unique_report_what_they_removed() {
        let mut p = Plan::default();
        p.push(Stage::Filter { in_rows: 1000, out_rows: 117 });
        p.push(Stage::Distinct { in_rows: 117, out_rows: 4 });
        let r = p.render();
        assert!(r.iter().any(|l| l.contains("Unique") && l.contains("removed 113")), "{r:?}");
        assert!(r.iter().any(|l| l.contains("Filter") && l.contains("removed 883")), "{r:?}");
    }

    #[test]
    fn a_joins_two_inputs_are_siblings_not_nested() {
        // A join is the one stage with two inputs. Printing them at different
        // depths reads as "the left relation was scanned INSIDE the scan of
        // the right one", which is not what happened.
        let mut p = Plan::default();
        p.push(scan("orders", 10));
        p.push(scan("customers", 5));
        p.push(Stage::Join {
            kind: JoinKind::Inner,
            table: "customers".into(),
            binding: "customers".into(),
            strategy: Strategy::Hash,
            keys: 1,
            left_rows: 10,
            right_rows: 5,
            out_rows: 7,
            early_stopped: false,
            post_filter_removed: None,
        });
        let r = p.render();
        assert!(r[0].contains("Hash Join"), "{r:?}");
        let orders = r.iter().find(|l| l.contains("orders")).expect("orders scanned");
        let custs = r.iter().find(|l| l.contains("customers  (actual")).expect("customers");
        let depth = |l: &str| l.len() - l.trim_start().len();
        assert_eq!(
            depth(orders), depth(custs),
            "the two inputs of a join must be at the same depth\n{r:#?}"
        );
        assert!(depth(orders) > depth(&r[0]), "both are nested under the join");
    }

    // ── structural assertions: topology, not rendered text ──────────────────

    fn join_stage(table: &str) -> Stage {
        Stage::Join {
            kind: JoinKind::Inner,
            table: table.into(),
            binding: table.into(),
            strategy: Strategy::Hash,
            keys: 1,
            left_rows: 1,
            right_rows: 1,
            out_rows: 1,
            early_stopped: false,
            post_filter_removed: None,
        }
    }

    #[test]
    fn a_join_node_has_exactly_two_children() {
        // The distinction the rendered text could not express:
        //   Join            is NOT     Join
        //   ├── Scan a                 └── Scan a
        //   └── Scan b                     └── Scan b
        let mut p = Plan::default();
        p.push(scan("a", 1));
        p.push(scan("b", 1));
        p.push(join_stage("b"));

        let t = p.tree().expect("a tree");
        assert!(matches!(t, PlanTree::Binary { .. }), "a join is binary");
        assert_eq!(t.children().len(), 2, "two inputs, not one nested in the other");
        assert_eq!(t.size(), 3, "join + two scans");
        // Two scans as SIBLINGS is depth 2. One nested under the other is 3.
        assert_eq!(t.depth(), 2, "the inputs are siblings\n{t:#?}");
        for c in t.children() {
            assert!(matches!(c, PlanTree::Leaf(Stage::Scan { .. })));
            assert_eq!(c.children().len(), 0, "a scan consumes nothing");
        }
    }

    #[test]
    fn the_outer_side_is_the_left_child() {
        // `a` is the FROM relation, `b` is joined to it. Getting these the
        // wrong way round would make EXPLAIN describe the build and probe
        // sides backwards.
        let mut p = Plan::default();
        p.push(scan("a", 10));
        p.push(scan("b", 5));
        p.push(join_stage("b"));
        let t = p.tree().unwrap();
        let kids = t.children();
        assert_eq!(kids[0].stage(), &scan("a", 10), "outer side first");
        assert_eq!(kids[1].stage(), &scan("b", 5), "inner side second");
    }

    #[test]
    fn a_chained_join_nests_on_the_left() {
        // `FROM a JOIN b JOIN c` — the second join's outer side is the FIRST
        // join, so the tree leans left and depth grows by one per join.
        let mut p = Plan::default();
        p.push(scan("a", 1));
        p.push(scan("b", 1));
        p.push(join_stage("b"));
        p.push(scan("c", 1));
        p.push(join_stage("c"));

        let t = p.tree().unwrap();
        assert_eq!(t.size(), 5, "3 scans + 2 joins");
        assert_eq!(t.depth(), 3, "left-deep: join -> join -> scan");
        let kids = t.children();
        assert!(matches!(kids[0], PlanTree::Binary { .. }), "outer side is the first join");
        assert!(matches!(kids[1], PlanTree::Leaf(_)), "inner side is c");
        assert_eq!(kids[0].children().len(), 2);
    }

    #[test]
    fn unary_stages_wrap_the_whole_tree_below_them() {
        let mut p = Plan::default();
        p.push(scan("a", 100));
        p.push(scan("b", 5));
        p.push(join_stage("b"));
        p.push(Stage::Filter { in_rows: 100, out_rows: 7 });
        p.push(Stage::Limit { limit: Some(2), offset: None, in_rows: 7, out_rows: 2 });

        let t = p.tree().unwrap();
        assert!(matches!(t.stage(), Stage::Limit { .. }), "the last stage is outermost");
        assert_eq!(t.children().len(), 1, "a unary stage has one input");
        let filter = t.children()[0];
        assert!(matches!(filter.stage(), Stage::Filter { .. }));
        assert_eq!(filter.children().len(), 1);
        let join = filter.children()[0];
        assert_eq!(join.children().len(), 2, "and the join below still has two");
        assert_eq!(t.size(), 5);
    }

    #[test]
    fn a_plan_with_no_stages_has_no_tree() {
        // `SELECT 1` touches no relation.
        assert_eq!(Plan::default().tree(), None);
    }

    #[test]
    fn the_rendered_depth_agrees_with_the_tree_depth() {
        // Ties the text back to the structure, so the two cannot drift: if the
        // renderer ever flattens the tree again, this fails.
        let mut p = Plan::default();
        p.push(scan("a", 1));
        p.push(scan("b", 1));
        p.push(join_stage("b"));
        p.push(scan("c", 1));
        p.push(join_stage("c"));
        let t = p.tree().unwrap();

        let lines = p.render();
        let plan_lines: Vec<&String> = lines
            .iter()
            .filter(|l| !l.starts_with("NEDB reports") && !l.starts_with("Row budget"))
            .collect();
        assert_eq!(plan_lines.len(), t.size(), "every node is rendered once");

        let max_indent = plan_lines
            .iter()
            .map(|l| (l.len() - l.trim_start().len()) / 2)
            .max()
            .unwrap();
        assert_eq!(max_indent + 1, t.depth(), "rendered nesting matches the tree");
    }

    #[test]
    fn joins_and_join_strategy_read_the_same_report() {
        let mut p = Plan::default();
        p.push(scan("a", 1));
        for s in [Strategy::NestedLoop, Strategy::Hash] {
            p.push(Stage::Join {
                kind: JoinKind::Left,
                table: "b".into(),
                binding: "b".into(),
                strategy: s,
                keys: 1,
                left_rows: 1,
                right_rows: 1,
                out_rows: 1,
                early_stopped: false,
            post_filter_removed: None,
            });
        }
        assert_eq!(p.joins().len(), 2);
        assert_eq!(p.join_strategy(0), Some(Strategy::NestedLoop));
        assert_eq!(p.join_strategy(1), Some(Strategy::Hash));
        assert_eq!(p.join_strategy(2), None);
    }
}
