// SPDX-License-Identifier: BUSL-1.1
// SPDX-FileCopyrightText: © 2026 INTERCHAINED LLC × Claude Sonnet 4.6

//! Baseline workloads for the SQL evaluator, nested loop vs hash join.
//!
//! Run with:
//!
//! ```text
//!   cargo run --release --example sqlbench
//! ```
//!
//! # What this is for
//!
//! Not marketing. This exists so that a claim about the optimiser can be
//! checked. Every input is generated from a FIXED SEED by the generator in
//! this file, so a number measured today is comparable with one measured after
//! the next change — which is the only thing that makes "faster" a fact rather
//! than an impression.
//!
//! Debug builds are dominated by bounds checks and `serde_json` cloning, so
//! `--release` is required for the numbers to mean anything. The harness says
//! so rather than silently reporting debug timings as if they were real.
//!
//! # What it measures honestly
//!
//! Each row reports the strategy that ACTUALLY ran, read back from the
//! execution report rather than from the flag that was requested. A benchmark
//! that believes it measured a hash join while timing a nested loop twice is
//! worse than no benchmark, because it produces a confident speedup number
//! from nothing.
//!
//! Row counts are printed too. Two strategies returning different row counts
//! means the comparison is meaningless, and the harness flags it loudly rather
//! than reporting the faster of two different questions.

use nedb_engine::sqljoin::{JoinExec, Strategy};
use nedb_engine::sqlplan::Plan;
use nedb_engine::sqlselect::{execute_explain, parse};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

/// Deterministic PRNG — fixed constants, fixed seed, reproducible inputs.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 11
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// The generated fixture.
///
/// `orders.customer_id` points into `customers.id`, with a deliberate 5% of
/// NULLs so outer joins and UNKNOWN filtering are exercised at scale rather
/// than only in unit tests. Key cardinality is `keyspace`, so bucket depth is
/// a controlled variable instead of an accident.
fn build(n_orders: usize, n_customers: usize, keyspace: u64, seed: u64) -> (Vec<Value>, Vec<Value>) {
    let mut rng = Lcg(seed);
    let orders = (0..n_orders)
        .map(|i| {
            let cid = rng.below(keyspace);
            let null_key = rng.below(100) < 5;
            json!({
                "id": i,
                "customer_id": if null_key { Value::Null } else { json!(cid) },
                "amount": rng.below(1000),
                "region": match rng.below(4) { 0 => "us", 1 => "eu", 2 => "apac", _ => "latam" },
            })
        })
        .collect();
    let customers = (0..n_customers)
        .map(|i| {
            json!({
                "id": (i as u64) % keyspace,
                "name": format!("cust-{i}"),
                "tier": match rng.below(3) { 0 => "gold", 1 => "silver", _ => "bronze" },
            })
        })
        .collect();
    (orders, customers)
}

fn median(mut d: Vec<Duration>) -> Duration {
    d.sort();
    d[d.len() / 2]
}

struct Outcome {
    elapsed: Duration,
    rows: usize,
    strategy: Option<Strategy>,
}

fn time_it(sql: &str, orders: &[Value], customers: &[Value], exec: JoinExec, reps: usize) -> Outcome {
    let sel = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e:#}"));
    let resolve = |t: &str| -> anyhow::Result<Option<Box<dyn nedb_engine::sqlselect::Relation>>> {
        Ok(match t {
            "orders" => Some(nedb_engine::sqlselect::from_vec(orders.to_vec())),
            "customers" => Some(nedb_engine::sqlselect::from_vec(customers.to_vec())),
            _ => None,
        })
    };

    // One untimed pass, so allocator warm-up is not charged to the first
    // strategy measured.
    let (_, warm, _): (_, _, Plan) = execute_explain(&sel, &resolve, exec).unwrap();

    let mut samples = vec![];
    let mut strategy = None;
    for _ in 0..reps {
        let t = Instant::now();
        let (_, rows, choices) = execute_explain(&sel, &resolve, exec).unwrap();
        samples.push(t.elapsed());
        strategy = choices.join_strategy(0);
        std::hint::black_box(rows);
    }
    Outcome { elapsed: median(samples), rows: warm.len(), strategy }
}

fn ms(d: Duration) -> String {
    format!("{:>9.2}", d.as_secs_f64() * 1000.0)
}

fn main() {
    if cfg!(debug_assertions) {
        eprintln!(
            "REFUSING to report numbers from a debug build — they are dominated by \
             bounds checks and would not be comparable.\n\
             Run: cargo run --release --example sqlbench"
        );
        std::process::exit(2);
    }

    // (orders, customers, distinct keys, repetitions)
    // Sizes are bounded by the NESTED LOOP, which is quadratic. That is the
    // point of the comparison, but it also means the largest shape has to stay
    // small enough to finish: 8000 x 1500 is 12M candidate pairs, already tens
    // of seconds on the reference path.
    let shapes: &[(usize, usize, u64, usize)] = &[
        (1_000, 500, 250, 5),
        (3_000, 1_000, 500, 3),
        (8_000, 1_500, 1_500, 1),
    ];

    // (label, sql, joins?)
    let workloads: &[(&str, &str)] = &[
        ("scan", "SELECT o.id, o.amount FROM orders o"),
        ("filtered scan", "SELECT o.id FROM orders o WHERE o.amount > 900"),
        ("equality join", "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"),
        ("left join", "SELECT o.id, c.name FROM orders o LEFT JOIN customers c ON o.customer_id = c.id"),
        ("join + selective pred", "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.amount > 990"),
        ("join + broad pred", "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.amount > 100"),
        ("join + sort", "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id ORDER BY c.name, o.id"),
        ("join + limit", "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id LIMIT 20"),
        ("join + pred + limit", "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.amount > 500 LIMIT 20"),
        ("non-equality join", "SELECT o.id FROM orders o JOIN customers c ON o.amount > 995"),
    ];

    println!("nedb sqlselect baselines — engine {}", env!("CARGO_PKG_VERSION"));
    println!("deterministic inputs, seed 0x51C0_FFEE_0000_0001, median of n reps\n");

    for (no, nc, keyspace, reps) in shapes.iter().copied() {
        let (orders, customers) = build(no, nc, keyspace, 0x51C0_FFEE_0000_0001);
        println!(
            "── orders={no} customers={nc} distinct keys={keyspace} reps={reps} \
             ────────────────"
        );
        println!(
            "{:<24} {:>11} {:>11} {:>8}  {:>7}  {}",
            "workload", "nested(ms)", "hash(ms)", "speedup", "rows", "note"
        );

        for (label, sql) in workloads {
            let nl = time_it(sql, &orders, &customers, JoinExec::NestedLoop, reps);
            let h = time_it(sql, &orders, &customers, JoinExec::Hash, reps);

            // The comparison is only meaningful if both answered the same
            // question.
            let note = if nl.rows != h.rows {
                format!("!! ROW COUNT DIFFERS {} vs {}", nl.rows, h.rows)
            } else {
                match (nl.strategy, h.strategy) {
                    (None, None) => "no join".to_string(),
                    (Some(Strategy::NestedLoop), Some(Strategy::Hash)) => String::new(),
                    (Some(a), Some(b)) if a == b => {
                        format!("both ran {a} — no hash path available")
                    }
                    (a, b) => format!("strategies {a:?} / {b:?}"),
                }
            };

            let speedup = if h.elapsed.as_nanos() > 0 {
                format!("{:>7.2}x", nl.elapsed.as_secs_f64() / h.elapsed.as_secs_f64())
            } else {
                "     n/a".to_string()
            };

            println!(
                "{:<24} {} {} {}  {:>7}  {}",
                label,
                ms(nl.elapsed),
                ms(h.elapsed),
                speedup,
                nl.rows,
                note
            );
        }
        println!();
    }

    println!(
        "Read the speedup column only on rows with an empty note. A row saying \
         \"both ran Nested Loop\"\nis a case with no provable equality key, where \
         the two columns time the SAME code twice and\nthe ratio is measurement \
         noise — reporting it as a speedup would be a lie."
    );
}
