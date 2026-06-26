//! NEDB v3 benchmark — v2 loose objects vs v3 segment packs, head to head.
//!
//! Run it:
//!   cargo run --release -p nedb-engine --example v3_bench
//!   cargo run --release -p nedb-engine --example v3_bench -- 50000   # custom object count
//!
//! It runs the SAME workload twice in-process — once with the v3 segment store
//! OFF (NEDB_DAG_V3 unset → v2 one-file-per-object) and once ON — and prints a
//! side-by-side of write throughput, read throughput, flush time, cold-start
//! time, on-disk file count + bytes, a full verify, and (v3) compaction reclaim.
//!
//! The headline number is writes/sec: v3 should clear the spec's >=10x gate on a
//! real disk, because it turns N file-creates+renames into batched sequential
//! appends with one fsync per batch.

use std::fs;
use std::path::Path;
use std::time::Instant;

use nedb_engine::Db;
use serde_json::json;

struct Stats {
    label: &'static str,
    objects: usize,
    write_secs: f64,
    writes_per_sec: f64,
    reads: usize,
    reads_per_sec: f64,
    cold_start_ms: f64,
    object_files: usize,
    total_files: usize,
    total_bytes: u64,
    verify_ok: usize,
    verify_bad: usize,
    compact_dropped: usize,
    compact_reclaimed: u64,
    compacted: bool,
}

fn dir_stats(root: &Path) -> (usize, u64) {
    let mut files = 0usize;
    let mut bytes = 0u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        if let Ok(rd) = fs::read_dir(&d) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if let Ok(m) = e.metadata() {
                    files += 1;
                    bytes += m.len();
                }
            }
        }
    }
    (files, bytes)
}

fn count_files(dir: &Path) -> usize {
    dir_stats(dir).0
}

fn run(label: &'static str, base: &Path, v3: bool, n: usize) -> Stats {
    if v3 {
        std::env::set_var("NEDB_DAG_V3", "1");
    } else {
        std::env::remove_var("NEDB_DAG_V3");
    }

    let dir = base.join(if v3 { "v3" } else { "v2" });
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    let db = Db::open(&dir, None).expect("open db");

    // ── WRITE: n coins in batches of 1000 (mirrors chainstate flush batching) ──
    let batch = 1000usize;
    let t = Instant::now();
    let mut written = 0usize;
    while written < n {
        let end = (written + batch).min(n);
        let ops: Vec<(String, String, serde_json::Value, Vec<String>, Option<String>, Option<String>)> =
            (written..end)
                .map(|i| (
                    "coins".to_string(),
                    format!("utxo-{:09}", i),
                    json!({ "v": format!("{:064x}", i) }),
                    Vec::new(),
                    None,
                    None,
                ))
                .collect();
        db.put_batch(ops).expect("put_batch");
        db.flush_all();
        written = end;
    }
    let write_secs = t.elapsed().as_secs_f64();
    let writes_per_sec = n as f64 / write_secs.max(1e-9);

    // ── READ: pseudo-random point lookups ──────────────────────────────────────
    let reads = 20_000usize.min(n);
    let t = Instant::now();
    for k in 0..reads {
        let i = (k.wrapping_mul(7919)) % n;
        let _ = db.get("coins", &format!("utxo-{:09}", i));
    }
    let read_secs = t.elapsed().as_secs_f64();
    let reads_per_sec = reads as f64 / read_secs.max(1e-9);

    // ── on-disk footprint ──────────────────────────────────────────────────────
    let object_files = count_files(&dir.join("objects"));
    let (total_files, total_bytes) = dir_stats(&dir);

    // ── verify (tamper-evidence over everything) ───────────────────────────────
    // db.verify() returns (ok_count, bad_hashes); we report the count of bad.
    let (verify_ok, verify_bad_hashes) = db.verify();
    let verify_bad = verify_bad_hashes.len();

    drop(db);

    // ── cold start: reopen + first read ─────────────────────────────────────────
    let t = Instant::now();
    let db2 = Db::open(&dir, None).expect("reopen db");
    let _ = db2.get("coins", "utxo-000000000");
    let cold_start_ms = t.elapsed().as_secs_f64() * 1000.0;

    // ── compaction: overwrite a slice to create dead versions, then prune ───────
    // (v2 compact is a no-op; v3 reclaims the superseded objects.)
    let overwrite = n / 2;
    let ops: Vec<(String, String, serde_json::Value, Vec<String>, Option<String>, Option<String>)> =
        (0..overwrite)
            .map(|i| (
                "coins".to_string(),
                format!("utxo-{:09}", i),
                json!({ "v": format!("{:064x}", i ^ 0xABCD) }),
                Vec::new(),
                None,
                None,
            ))
            .collect();
    db2.put_batch(ops).expect("overwrite put_batch");
    db2.flush_all();
    let cstats = db2.compact().expect("compact");
    let compacted = v3; // Db::compact is a no-op unless v3

    Stats {
        label,
        objects: n,
        write_secs,
        writes_per_sec,
        reads,
        reads_per_sec,
        cold_start_ms,
        object_files,
        total_files,
        total_bytes,
        verify_ok,
        verify_bad,
        compact_dropped: cstats.dropped_objects,
        compact_reclaimed: cstats.bytes_reclaimed,
        compacted,
    }
}

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);

    let base = std::env::temp_dir().join(format!("nedb_v3_bench_{}", std::process::id()));
    let _ = fs::remove_dir_all(&base);
    fs::create_dir_all(&base).unwrap();

    println!("\nNEDB v3 benchmark — {} objects, writes batched x1000, one flush per batch\n", n);
    println!("  running v2 (loose objects) baseline — this is the slow one...");
    let v2 = run("v2 loose", &base, false, n);
    println!("  running v3 (segment packs)...");
    let v3 = run("v3 segments", &base, true, n);

    let speedup = v3.writes_per_sec / v2.writes_per_sec.max(1e-9);
    let cold_speedup = v2.cold_start_ms / v3.cold_start_ms.max(1e-9);

    println!("\n{:<24}{:>18}{:>18}", "metric", "v2 (loose)", "v3 (segments)");
    println!("{}", "-".repeat(60));
    println!("{:<24}{:>18.0}{:>18.0}", "writes / sec", v2.writes_per_sec, v3.writes_per_sec);
    println!("{:<24}{:>18.2}{:>18.2}", "write total (s)", v2.write_secs, v3.write_secs);
    println!("{:<24}{:>18.0}{:>18.0}", "reads / sec", v2.reads_per_sec, v3.reads_per_sec);
    println!("{:<24}{:>18.1}{:>18.1}", "cold start (ms)", v2.cold_start_ms, v3.cold_start_ms);
    println!("{:<24}{:>18}{:>18}", "object files", v2.object_files, v3.object_files);
    println!("{:<24}{:>18}{:>18}", "total files", v2.total_files, v3.total_files);
    println!("{:<24}{:>18.1}{:>18.1}", "on-disk MiB",
             v2.total_bytes as f64 / 1048576.0, v3.total_bytes as f64 / 1048576.0);
    println!("{:<24}{:>11} ok/{:<5}{:>11} ok/{:<5}", "verify (ok/bad)",
             v2.verify_ok, v2.verify_bad, v3.verify_ok, v3.verify_bad);
    let v2c = if v2.compacted { format!("{} obj", v2.compact_dropped) } else { "n/a".to_string() };
    let v3c = format!("{} obj / {:.1} MiB", v3.compact_dropped, v3.compact_reclaimed as f64 / 1048576.0);
    println!("{:<24}{:>18}{:>18}", "compaction reclaim", v2c, v3c);
    println!("{}", "-".repeat(60));
    println!("\n  >>> write throughput: v3 is {:.1}x v2", speedup);
    println!("  >>> cold start:       v3 is {:.1}x faster", cold_speedup);
    println!("  >>> object-file count: {} -> {}  ({}x fewer inodes)\n",
             v2.object_files, v3.object_files,
             if v3.object_files > 0 { v2.object_files / v3.object_files } else { v2.object_files });

    let _ = fs::remove_dir_all(&base);

    if v2.verify_bad != 0 || v3.verify_bad != 0 {
        eprintln!("WARNING: verify found corruption — investigate before trusting these numbers");
    }
    if speedup < 10.0 {
        eprintln!("NOTE: write speedup {:.1}x is under the 10x gate on this disk/size — try a larger object count, e.g. -- 50000", speedup);
    }
}
