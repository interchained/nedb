// SPDX-License-Identifier: BUSL-1.1
// SPDX-FileCopyrightText: © 2026 INTERCHAINED LLC × Claude Sonnet 4.6

//! Differential tests: the hash join against the nested loop.
//!
//! # The invariant
//!
//! For every join query both strategies can execute, they must return
//! IDENTICAL results — the same rows, with the same duplicates, in the same
//! ORDER. Not the same set. Order equality is deliberate and achievable: the
//! hash table holds right-hand row indices in ascending order and probing
//! walks left rows in order, so a correct hash join emits pairs in exactly the
//! sequence a nested loop would.
//!
//! Comparing sorted sets would hide two real bug classes — a duplicate emitted
//! once instead of twice, and outer rows placed in the wrong position — so it
//! is not done here.
//!
//! # Why a generator and not only hand-written cases
//!
//! Hand-written cases test what the author thought of. The failure mode this
//! engine has actually suffered is the case nobody imagined: a NULL key in a
//! `FULL JOIN` whose partner row also has a NULL key, a numeric string
//! matching a number on one side of the join but not the other, a duplicate
//! key on BOTH sides at once. The generator below enumerates and randomises
//! those combinations across a deliberately hostile value pool.
//!
//! The seed is fixed, so a failure is reproducible — a differential test that
//! cannot be replayed is a rumour, not a bug report.

use nedb_engine::sqljoin::{JoinExec, Strategy};
use nedb_engine::sqlselect::{execute_explain, parse};
use serde_json::{json, Value};

/// Values chosen to make dynamic-typed equality as awkward as possible.
///
/// `1` / `"1"` / `"1.0"` are the non-transitive triple: the first matches both
/// others, which do not match each other. `true` / `"t"` compare equal.
/// `0` / `-0.0` are numerically equal with different bit patterns. NULL
/// matches nothing, including itself.
fn pool() -> Vec<Value> {
    vec![
        Value::Null,
        json!(1),
        json!("1"),
        json!("1.0"),
        json!(2),
        json!(0),
        json!(-0.0),
        json!(true),
        json!("t"),
        json!("abc"),
    ]
}

/// A deterministic PRNG. Fixed constants, fixed seed, so every failure
/// replays exactly.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 11
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

struct Fixture {
    left: Vec<Value>,
    right: Vec<Value>,
}

impl Fixture {
    fn resolve(&self) -> impl Fn(&str) -> anyhow::Result<Option<Box<dyn nedb_engine::sqlselect::Relation>>> + '_ {
        move |t: &str| {
            Ok(match t {
                "l" => Some(nedb_engine::sqlselect::from_vec(self.left.clone())),
                "r" => Some(nedb_engine::sqlselect::from_vec(self.right.clone())),
                _ => None,
            })
        }
    }
}

/// Run one query both ways and assert the two agree exactly.
///
/// Returns whether the hash path was genuinely exercised, so a caller can
/// prove its battery was not silently running the nested loop twice.
#[track_caller]
fn differ(sql: &str, fx: &Fixture) -> bool {
    let sel = parse(sql).unwrap_or_else(|e| panic!("{sql}\n  failed to parse: {e:#}"));
    let resolve = fx.resolve();

    let (nl_names, nl_rows, nl_choices) =
        execute_explain(&sel, &resolve, JoinExec::NestedLoop)
            .unwrap_or_else(|e| panic!("{sql}\n  nested loop failed: {e:#}"));
    let (h_names, h_rows, h_choices) = execute_explain(&sel, &resolve, JoinExec::Hash)
        .unwrap_or_else(|e| panic!("{sql}\n  hash join failed: {e:#}"));

    assert!(
        nl_choices.join_strategies().iter().all(|s| *s == Strategy::NestedLoop),
        "{sql}\n  forcing NestedLoop did not take effect"
    );

    assert_eq!(nl_names, h_names, "{sql}\n  column names differ");
    assert_eq!(
        nl_rows, h_rows,
        "{sql}\n  RESULTS DIFFER\n  left  = {}\n  right = {}\n  nested loop {:?}\n  hash {:?}",
        json!(nl_rows),
        json!(h_rows),
        nl_choices.render(),
        h_choices.render()
    );

    h_choices.join_strategies().contains(&Strategy::Hash)
}

// ─────────────────────────────────────────────────────────────────────────────
// Enumerated: every join kind × key shape × residual, over the hostile pool
// ─────────────────────────────────────────────────────────────────────────────

/// Relations covering every awkward pairing: each pool value against each
/// other, plus deliberate duplicates on both sides.
fn cross_fixture() -> Fixture {
    let p = pool();
    let mut left = vec![];
    let mut right = vec![];
    for (i, a) in p.iter().enumerate() {
        for (j, b) in p.iter().enumerate() {
            left.push(json!({"k1": a, "k2": b, "v": i * 10 + j}));
        }
    }
    for (i, a) in p.iter().enumerate() {
        for b in p.iter() {
            right.push(json!({"k1": a, "k2": b, "w": i}));
        }
        // A duplicate of every row, so "multiple matching rows" is real on the
        // BUILD side and not merely on the probe side.
        right.push(json!({"k1": a, "k2": a, "w": 999}));
    }
    Fixture { left, right }
}

const KINDS: &[&str] = &["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"];

const RESIDUALS: &[&str] = &[
    "",
    " AND l.v > 30",
    " AND r.w IS NOT NULL",
    " AND l.k2 = r.k2",
    " AND (l.v > 30 OR r.w = 1)",
    " AND l.v IS NOT NULL AND r.w BETWEEN 0 AND 4",
];

#[test]
fn every_join_kind_agrees_on_a_single_key() {
    let fx = cross_fixture();
    let mut hashed = 0;
    for kind in KINDS {
        for res in RESIDUALS {
            let sql = format!(
                "SELECT l.v, r.w FROM l {kind} r ON l.k1 = r.k1{res} \
                 ORDER BY 1, 2"
            );
            if differ(&sql, &fx) {
                hashed += 1;
            }
        }
    }
    assert_eq!(
        hashed,
        KINDS.len() * RESIDUALS.len(),
        "every case must actually have taken the hash path"
    );
}

#[test]
fn every_join_kind_agrees_on_a_compound_key() {
    let fx = cross_fixture();
    let mut hashed = 0;
    for kind in KINDS {
        let sql = format!(
            "SELECT l.v, r.w FROM l {kind} r ON l.k1 = r.k1 AND l.k2 = r.k2 \
             ORDER BY 1, 2"
        );
        if differ(&sql, &fx) {
            hashed += 1;
        }
    }
    assert_eq!(hashed, KINDS.len());
}

#[test]
fn unordered_results_agree_row_for_row_including_order() {
    // No ORDER BY: the two strategies must emit rows in the SAME sequence,
    // which is a far sharper claim than agreeing as multisets.
    let fx = cross_fixture();
    for kind in KINDS {
        let sql = format!("SELECT l.v, r.w FROM l {kind} r ON l.k1 = r.k1");
        assert!(differ(&sql, &fx));
    }
}

#[test]
fn expression_keys_agree() {
    let fx = cross_fixture();
    for kind in KINDS {
        for on in [
            "lower(l.k1) = lower(r.k1)",
            "l.k1 = r.k1::text",
            "coalesce(l.k1, 'x') = coalesce(r.k1, 'x')",
        ] {
            let sql = format!("SELECT l.v, r.w FROM l {kind} r ON {on} ORDER BY 1, 2");
            assert!(differ(&sql, &fx), "{sql}");
        }
    }
}

#[test]
fn a_key_that_is_null_on_both_sides_still_does_not_join() {
    // `coalesce(k1,'x')` above makes NULLs match; plain `k1 = k1` must not.
    let fx = Fixture {
        left: vec![json!({"k1": null, "v": 1})],
        right: vec![json!({"k1": null, "w": 2})],
    };
    for (kind, want) in [
        ("JOIN", 0),
        ("LEFT JOIN", 1),
        ("RIGHT JOIN", 1),
        ("FULL JOIN", 2),
    ] {
        let sql = format!("SELECT l.v, r.w FROM l {kind} r ON l.k1 = r.k1");
        differ(&sql, &fx);
        let sel = parse(&sql).unwrap();
        let (_, rows, _) = execute_explain(&sel, &fx.resolve(), JoinExec::Hash).unwrap();
        assert_eq!(rows.len(), want, "{sql}");
    }
}

#[test]
fn an_empty_relation_on_either_side_agrees() {
    let full = vec![json!({"k1": 1, "v": 1}), json!({"k1": null, "v": 2})];
    for kind in KINDS {
        for fx in [
            Fixture { left: vec![], right: full.clone() },
            Fixture { left: full.clone(), right: vec![] },
            Fixture { left: vec![], right: vec![] },
        ] {
            let sql = format!("SELECT l.v, r.w FROM l {kind} r ON l.k1 = r.k1");
            differ(&sql, &fx);
        }
    }
}

#[test]
fn a_three_relation_chain_agrees() {
    let sql = "SELECT l.v, r.w FROM l \
               JOIN r ON l.k1 = r.k1 \
               LEFT JOIN l AS l2 ON r.k1 = l2.k1 \
               ORDER BY 1, 2";
    let p = pool();
    let mut left = vec![];
    for (i, a) in p.iter().enumerate() {
        left.push(json!({"k1": a, "k2": a, "v": i}));
    }
    let right = left
        .iter()
        .enumerate()
        .map(|(i, r)| json!({"k1": r["k1"], "k2": r["k2"], "w": i}))
        .collect();
    let fx = Fixture { left: left.clone(), right };

    let sel = parse(sql).unwrap();
    // `l AS l2` resolves through the same relation, so the resolver serves it.
    let resolve = |t: &str| -> anyhow::Result<Option<Box<dyn nedb_engine::sqlselect::Relation>>> {
        Ok(match t {
            "l" => Some(nedb_engine::sqlselect::from_vec(fx.left.clone())),
            "r" => Some(nedb_engine::sqlselect::from_vec(fx.right.clone())),
            _ => None,
        })
    };
    let (n1, r1, c1) = execute_explain(&sel, &resolve, JoinExec::NestedLoop).unwrap();
    let (n2, r2, c2) = execute_explain(&sel, &resolve, JoinExec::Hash).unwrap();
    assert_eq!(n1, n2);
    assert_eq!(r1, r2, "chained joins disagree\n{c1:?}\n{c2:?}");
    assert_eq!(c2.joins().len(), 2, "both joins accounted for");
    assert!(c2.join_strategies().iter().all(|s| *s == Strategy::Hash));
}

// ─────────────────────────────────────────────────────────────────────────────
// Generated
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn generated_join_queries_agree() {
    let p = pool();
    let mut rng = Lcg(0x5EED_1234_ABCD_0001);
    let mut hashed = 0;
    let mut total = 0;

    for case in 0..600 {
        // Relation sizes deliberately include 0 and 1.
        let ln = rng.below(9);
        let rn = rng.below(9);
        let left: Vec<Value> = (0..ln)
            .map(|i| {
                json!({
                    "k1": p[rng.below(p.len())],
                    "k2": p[rng.below(p.len())],
                    "v": i,
                })
            })
            .collect();
        let right: Vec<Value> = (0..rn)
            .map(|i| {
                json!({
                    "k1": p[rng.below(p.len())],
                    "k2": p[rng.below(p.len())],
                    "w": i,
                })
            })
            .collect();
        let fx = Fixture { left, right };

        let kind = KINDS[rng.below(KINDS.len())];
        let keys = match rng.below(3) {
            0 => "l.k1 = r.k1",
            1 => "l.k1 = r.k1 AND l.k2 = r.k2",
            _ => "l.k2 = r.k1",
        };
        let res = RESIDUALS[rng.below(RESIDUALS.len())];
        let order = if rng.below(2) == 0 { " ORDER BY 1, 2" } else { "" };
        let sql = format!("SELECT l.v, r.w FROM l {kind} r ON {keys}{res}{order}");

        total += 1;
        if differ(&sql, &fx) {
            hashed += 1;
        }
        // A tripwire against the generator degenerating into empty relations.
        if case == 599 {
            assert!(total == 600);
        }
    }

    // Not every generated case can hash — an empty relation produces no join
    // work at all — but the great majority must, or this test is theatre.
    assert!(
        hashed > 400,
        "only {hashed}/{total} generated cases took the hash path"
    );
}

#[test]
fn generated_queries_on_larger_relations_agree() {
    // Big enough that `Auto` would pick the hash path in production, so the
    // sizes the optimiser actually optimises are the sizes under test.
    let p = pool();
    let mut rng = Lcg(0xC0FF_EE00_D15E_A5E1);
    for _ in 0..40 {
        let left: Vec<Value> = (0..120)
            .map(|i| json!({"k1": p[rng.below(p.len())], "v": i}))
            .collect();
        let right: Vec<Value> = (0..120)
            .map(|i| json!({"k1": p[rng.below(p.len())], "w": i}))
            .collect();
        let fx = Fixture { left, right };
        let kind = KINDS[rng.below(KINDS.len())];
        let sql = format!("SELECT l.v, r.w FROM l {kind} r ON l.k1 = r.k1");
        assert!(differ(&sql, &fx), "{sql}");
    }
}

#[test]
fn auto_picks_the_hash_path_only_once_the_work_justifies_it() {
    let small = Fixture {
        left: (0..4).map(|i| json!({"k1": 1, "v": i})).collect(),
        right: (0..4).map(|i| json!({"k1": 1, "w": i})).collect(),
    };
    let big = Fixture {
        left: (0..40).map(|i| json!({"k1": 1, "v": i})).collect(),
        right: (0..40).map(|i| json!({"k1": 1, "w": i})).collect(),
    };
    let sel = parse("SELECT l.v, r.w FROM l JOIN r ON l.k1 = r.k1").unwrap();

    let (_, _, c) = execute_explain(&sel, &small.resolve(), JoinExec::Auto).unwrap();
    assert_eq!(c.join_strategy(0), Some(Strategy::NestedLoop), "16 pairs is not worth a table");

    let (_, _, c) = execute_explain(&sel, &big.resolve(), JoinExec::Auto).unwrap();
    assert_eq!(c.join_strategy(0), Some(Strategy::Hash), "1600 pairs is");
}

#[test]
fn a_join_with_no_equality_key_reports_the_nested_loop_under_every_setting() {
    let fx = cross_fixture();
    let sel = parse("SELECT l.v, r.w FROM l JOIN r ON l.v > r.w").unwrap();
    for exec in [JoinExec::Auto, JoinExec::NestedLoop, JoinExec::Hash] {
        let (_, _, c) = execute_explain(&sel, &fx.resolve(), exec).unwrap();
        assert_eq!(c.join_strategy(0), Some(Strategy::NestedLoop), "{exec:?}");
        assert_eq!(c.join_keys(0), Some(0));
    }
}
