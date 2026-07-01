//! End-to-end proof for durable-mode auto-flush-on-exit (`Db::install_exit_flush`).
//!
//! The in-process unit tests in `src/exit.rs` prove the flush path makes writes
//! durable, but they cannot prove the SIGINT/SIGTERM wiring fires — a successful
//! in-process signal test would terminate the test runner. This example is that
//! missing proof, driven by a REAL signal:
//!
//! ```text
//! DIR=$(mktemp -d)
//!
//! # 1. Start the writer. It writes a doc into the id-index WAL (NOT yet flushed
//! #    to disk), prints READY, then blocks. No manifest ticker, no explicit
//! #    flush — exactly the situation where a hard exit would normally lose data.
//! cargo run -q --example exit_flush_demo -- write "$DIR" &
//! PID=$!
//! sleep 1
//!
//! # 2. Kill it the way an orchestrator / Ctrl+C would. The installed handler
//! #    flushes on the way out, then re-raises so the exit status is 143.
//! kill -TERM "$PID"; wait "$PID"; echo "writer exit: $?"   # expect 143
//!
//! # 3. Reopen the same directory in a fresh process. The write must be there.
//! cargo run -q --example exit_flush_demo -- check "$DIR"    # expect: OK
//! ```
//!
//! Run the same sequence with `install_exit_flush` commented out and step 3
//! prints `MISSING` — that is the bug this feature fixes.

use std::sync::Arc;
use std::time::Duration;

use nedb_engine::Db;

const COLL: &str = "demo";
const ID: &str = "sentinel";

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("write");
    let dir = args.get(2).cloned().unwrap_or_else(|| "/tmp/nedb_exit_flush_demo".into());

    match mode {
        "write" => write_and_wait(&dir),
        "check" => check(&dir),
        other => {
            eprintln!("usage: exit_flush_demo <write|check> <dir>  (got mode '{other}')");
            std::process::exit(2);
        }
    }
}

/// Open durable, arm exit-flush, stage a write, then block until signalled.
fn write_and_wait(dir: &str) {
    let db = Arc::new(Db::open(std::path::Path::new(dir), None).expect("open durable db"));

    // The one line under test. Comment it out to observe data loss on SIGTERM.
    Db::install_exit_flush(Arc::clone(&db));

    db.put(
        COLL,
        ID,
        serde_json::json!({ "armed": true, "pid": std::process::id() }),
        vec![],
        None,
        None,
    )
    .expect("stage write");

    // Signal readiness to the driving script AFTER the write is staged but BEFORE
    // any flush — so a SIGTERM now can only survive via install_exit_flush.
    println!("READY pid={} dir={}", std::process::id(), dir);
    use std::io::Write;
    let _ = std::io::stdout().flush();

    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}

/// Reopen and report whether the sentinel survived. Exit 0 = OK, 1 = MISSING.
fn check(dir: &str) {
    let db = Db::open(std::path::Path::new(dir), None).expect("reopen durable db");
    match db.get(COLL, ID) {
        Some(node) if node.data.get("armed").and_then(|v| v.as_bool()) == Some(true) => {
            println!("OK — sentinel survived exit flush (seq={})", node.seq);
        }
        _ => {
            println!("MISSING — write was lost (no exit flush)");
            std::process::exit(1);
        }
    }
}
