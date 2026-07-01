//! `nedb-cli` — the engine's companion CLI/sidecar for a NEDB v2 store on disk.
//!
//! `nedbd` *serves* a store over HTTP; `nedb-cli` operates on a store directory
//! directly, offline, with no server running.
//!
//!   nedb-cli <command> <path> [args]
//!
//!   head    <path>                     the tamper-evident Merkle head
//!   status  <path>                     readiness snapshot (scan_status + seq + head)
//!   verify  <path>                     re-hash every node; report any tamper
//!   get     <path> <coll> <id> [seq]   one document (optionally AS OF seq), as JSON
//!   scan    <path> [after] [limit]     changefeed page since `after` (NDJSON) + envelope
//!   flush   <path>                     make buffered writes durable now (WAL + MANIFEST)
//!   repair  <path>                     rebuild the index from objects, then verify + flush
//!   export  <path> [coll]              dump live documents (all collections, or one) as NDJSON
//!
//! Reads are always safe. Prefer running write commands (`flush`/`repair`)
//! against a STOPPED store (or use the nedbd HTTP API on a live one) — two
//! writers to one directory is not supported.
//!
//! Exit code: 0 ok · 1 error/tamper/not-found · 2 usage.
//!
//! © INTERCHAINED LLC × Claude Opus 4.8

use std::env;
use std::path::Path;
use std::process::exit;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use nedb_engine::Db;

fn main() {
    let args: Vec<String> = env::args().collect();
    let cmd = args.get(1).map(String::as_str).unwrap_or("");
    let rest: &[String] = if args.len() > 2 { &args[2..] } else { &[] };
    let code = match cmd {
        "head" => cmd_head(rest),
        "status" => cmd_status(rest),
        "verify" => cmd_verify(rest),
        "get" => cmd_get(rest),
        "scan" => cmd_scan(rest),
        "flush" => cmd_flush(rest),
        "repair" => cmd_repair(rest),
        "export" => cmd_export(rest),
        "" | "-h" | "--help" | "help" => { usage(); 0 }
        other => { eprintln!("nedb-cli: unknown command '{other}'\n"); usage(); 2 }
    };
    exit(code);
}

fn usage() {
    eprintln!(
        "nedb-cli — NEDB v2 store CLI\n\n\
         USAGE:\n  \
         nedb-cli head    <path>\n  \
         nedb-cli status  <path>\n  \
         nedb-cli verify  <path>\n  \
         nedb-cli get     <path> <coll> <id> [seq]\n  \
         nedb-cli scan    <path> [after_seq] [limit]\n  \
         nedb-cli flush   <path>\n  \
         nedb-cli repair  <path>\n  \
         nedb-cli export  <path> [coll]\n\n\
         Exit: 0 ok · 1 error/tamper/not-found · 2 usage."
    );
}

/// Open the store or exit(1) with a clear message.
fn open(path: &str) -> Db {
    match Db::open(Path::new(path), None) {
        Ok(db) => db,
        Err(e) => { eprintln!("nedb-cli: cannot open '{path}': {e}"); exit(1); }
    }
}

fn need_path(rest: &[String]) -> &str {
    match rest.first() {
        Some(p) => p.as_str(),
        None => { eprintln!("nedb-cli: missing <path>\n"); usage(); exit(2); }
    }
}

fn cmd_head(rest: &[String]) -> i32 {
    let db = open(need_path(rest));
    println!("{}", db.head());
    0
}

fn cmd_status(rest: &[String]) -> i32 {
    let db = open(need_path(rest));
    let s = db.scan_status();
    let out = serde_json::json!({
        "head": db.head(),
        "seq": db.seq.load(Ordering::SeqCst),
        "scan_complete": s.scan_complete,
        "tip_seq": s.tip_seq,
        "indexed_seq_min": s.indexed_seq_min,
        "indexed_seq_max": s.indexed_seq_max,
        "indexed_count": s.indexed_count,
    });
    println!("{}", serde_json::to_string_pretty(&out).unwrap());
    0
}

fn cmd_verify(rest: &[String]) -> i32 {
    let db = open(need_path(rest));
    let (checked, tampered) = db.verify();
    if tampered.is_empty() {
        println!("ok: {checked} node(s) verified, no tamper");
        0
    } else {
        eprintln!("TAMPER DETECTED: {} of {} node(s) failed hash check:", tampered.len(), checked);
        for h in &tampered { println!("{h}"); }
        1
    }
}

fn cmd_get(rest: &[String]) -> i32 {
    let path = need_path(rest);
    let (coll, id) = match (rest.get(1), rest.get(2)) {
        (Some(c), Some(i)) => (c.as_str(), i.as_str()),
        _ => { eprintln!("nedb-cli: get needs <coll> <id>\n"); usage(); return 2; }
    };
    let db = open(path);
    let node = match rest.get(3).and_then(|s| s.parse::<u64>().ok()) {
        Some(seq) => db.get_as_of(coll, id, seq),
        None => db.get(coll, id),
    };
    match node {
        Some(n) => { println!("{}", serde_json::to_string_pretty(&n).unwrap()); 0 }
        None => { eprintln!("nedb-cli: {coll}/{id} not found"); 1 }
    }
}

fn cmd_scan(rest: &[String]) -> i32 {
    let path = need_path(rest);
    let after = rest.get(1).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
    let limit = rest.get(2).and_then(|s| s.parse::<usize>().ok()).unwrap_or(0); // 0 → engine default cap
    let db = open(path);
    let batch = db.since(after, limit);
    for n in &batch.nodes {
        println!("{}", serde_json::to_string(n).unwrap());
    }
    // Envelope on stderr so stdout stays clean NDJSON for piping.
    eprintln!(
        "-- from_seq={} to_seq={} head_seq={} has_more={} ({} node(s))",
        batch.from_seq, batch.to_seq, batch.head_seq, batch.has_more, batch.nodes.len()
    );
    0
}

fn cmd_flush(rest: &[String]) -> i32 {
    let db = open(need_path(rest));
    db.flush_all();
    println!("flushed: WAL + MANIFEST durable");
    0
}

fn cmd_repair(rest: &[String]) -> i32 {
    let db = Arc::new(open(need_path(rest)));
    // Rebuild the seq/id index from the content-addressed objects (idempotent —
    // a no-op on a warm store, a full self-heal on a stale/missing MANIFEST).
    Db::start_cold_scan(Arc::clone(&db));
    let mut waited = 0u64;
    while !db.scan_status().scan_complete {
        std::thread::sleep(std::time::Duration::from_millis(50));
        waited += 50;
        if waited > 120_000 { eprintln!("nedb-cli: repair scan still running after 120s — aborting wait"); break; }
    }
    let (checked, tampered) = db.verify();
    db.flush_all();
    if tampered.is_empty() {
        println!("repaired: index rebuilt, {checked} node(s) verified, flushed");
        0
    } else {
        eprintln!("repaired index + flushed, but {} of {} node(s) FAILED verify:", tampered.len(), checked);
        for h in &tampered { println!("{h}"); }
        1
    }
}

fn cmd_export(rest: &[String]) -> i32 {
    let db = open(need_path(rest));
    let colls: Vec<String> = match rest.get(1) {
        Some(c) => vec![c.clone()],
        None => db.id_index.collections(),
    };
    let mut total = 0usize;
    for coll in &colls {
        for node in db.list(coll) {
            println!("{}", serde_json::to_string(&node).unwrap());
            total += 1;
        }
    }
    eprintln!("-- exported {} live document(s) from {} collection(s)", total, colls.len());
    0
}
