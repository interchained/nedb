// SPDX-License-Identifier: BUSL-1.1
// SPDX-FileCopyrightText: © 2026 INTERCHAINED LLC × Claude Sonnet 4.6
//! The fusion win, measured directly: the SAME query with `fuse_filter` off
//! and on. Same generator and seed as `sqlbench`, so these numbers are
//! comparable with the baselines in docs/BENCH-sqlselect.md.
//!
//! Measured rather than inferred. Without fusion a `WHERE` disqualifies the
//! row budget entirely, so the unfused column is the cost of materialising
//! the whole join before filtering — but stating that from reasoning alone
//! would be a guess, and a guess printed as a number is worse than the truth.
use nedb_engine::sqljoin::{JoinExec, Strategy};
use nedb_engine::sqlselect::{execute_opts, parse, Opts};
use serde_json::{json, Value};
use std::time::Instant;

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 11
    }
    fn below(&mut self, n: u64) -> u64 { self.next() % n }
}

fn main() {
    if cfg!(debug_assertions) { eprintln!("run with --release"); std::process::exit(2); }
    let sql = "SELECT o.id, c.name FROM orders o JOIN customers c \
               ON o.customer_id = c.id WHERE o.amount > 500 LIMIT 20";
    let sel = parse(sql).unwrap();
    println!("{sql}\n");
    println!("{:<28} {:>12} {:>12} {:>9}  {}", "shape", "unfused(ms)", "fused(ms)", "gain", "rows");

    for (no, nc, ks) in [(1_000usize, 500usize, 250u64), (3_000, 1_000, 500), (8_000, 1_500, 1_500)] {
        let mut rng = Lcg(0x51C0_FFEE_0000_0001);
        let orders: Vec<Value> = (0..no).map(|i| {
            let cid = rng.below(ks); let nul = rng.below(100) < 5;
            json!({"id": i, "customer_id": if nul { Value::Null } else { json!(cid) },
                   "amount": rng.below(1000),
                   "region": match rng.below(4) {0=>"us",1=>"eu",2=>"apac",_=>"latam"}})
        }).collect();
        let customers: Vec<Value> = (0..nc).map(|i| json!({
            "id": (i as u64) % ks, "name": format!("cust-{i}"),
            "tier": match rng.below(3) {0=>"gold",1=>"silver",_=>"bronze"}})).collect();
        let resolve = |t: &str| -> anyhow::Result<Option<Box<dyn nedb_engine::sqlselect::Relation>>> {
            Ok(match t { "orders" => Some(nedb_engine::sqlselect::from_vec(orders.clone())), "customers" => Some(nedb_engine::sqlselect::from_vec(customers.clone())), _ => None })
        };

        for exec in [Strategy::NestedLoop, Strategy::Hash] {
            let e = if exec == Strategy::NestedLoop { JoinExec::NestedLoop } else { JoinExec::Hash };
            let mut t = [0f64; 2]; let mut rows = [0usize; 2];
            for (i, fuse) in [false, true].into_iter().enumerate() {
                let o = Opts { exec: e, pushdown: true, fuse_filter: fuse };
                let _ = execute_opts(&sel, &resolve, o).unwrap();   // warm
                let st = Instant::now();
                let (_, r, _) = execute_opts(&sel, &resolve, o).unwrap();
                t[i] = st.elapsed().as_secs_f64() * 1000.0;
                rows[i] = r.len();
            }
            assert_eq!(rows[0], rows[1], "fused and unfused disagree on row count");
            println!("{:<28} {:>12.2} {:>12.2} {:>8.1}x  {}",
                format!("{no}x{nc} {exec}"), t[0], t[1], t[0] / t[1], rows[0]);
        }
    }
}
