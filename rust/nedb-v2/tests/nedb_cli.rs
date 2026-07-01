//! Integration tests for the `nedb-cli` binary. Builds a real store via the lib,
//! then drives the compiled CLI as a subprocess and checks its behavior.
//!
//! © INTERCHAINED LLC × Claude Opus 4.8

use std::process::{Command, Output};

use nedb_engine::Db;

const BIN: &str = env!("CARGO_BIN_EXE_nedb-cli");

/// Create a durable store with one document and return its directory.
fn store_with_doc() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(dir.path(), None).unwrap();
        db.put("users", "alice", serde_json::json!({ "name": "Ann" }), vec![], None, None)
            .unwrap();
        db.flush_all();
    } // drop → flush again; store is durable on disk
    dir
}

fn run(args: &[&str]) -> Output {
    Command::new(BIN).args(args).output().expect("spawn nedb-cli")
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).to_string()
}

#[test]
fn head_prints_a_nonempty_merkle_head() {
    let dir = store_with_doc();
    let o = run(&["head", dir.path().to_str().unwrap()]);
    assert!(o.status.success(), "head should exit 0");
    assert!(!stdout(&o).trim().is_empty(), "head should print the Merkle head");
}

#[test]
fn status_reports_scan_state() {
    let dir = store_with_doc();
    let o = run(&["status", dir.path().to_str().unwrap()]);
    assert!(o.status.success());
    assert!(stdout(&o).contains("scan_complete"), "status is a JSON readiness snapshot");
}

#[test]
fn get_returns_the_document() {
    let dir = store_with_doc();
    let o = run(&["get", dir.path().to_str().unwrap(), "users", "alice"]);
    assert!(o.status.success());
    let out = stdout(&o);
    assert!(out.contains("alice"), "the id should be present");
    assert!(out.contains("Ann"), "the document body should be present");
}

#[test]
fn get_missing_exits_nonzero() {
    let dir = store_with_doc();
    let o = run(&["get", dir.path().to_str().unwrap(), "users", "nobody"]);
    assert_eq!(o.status.code(), Some(1), "a missing doc is exit 1");
}

#[test]
fn verify_reports_clean() {
    let dir = store_with_doc();
    let o = run(&["verify", dir.path().to_str().unwrap()]);
    assert!(o.status.success());
    assert!(stdout(&o).contains("verified"), "verify prints a clean report");
}

#[test]
fn export_dumps_ndjson() {
    let dir = store_with_doc();
    let o = run(&["export", dir.path().to_str().unwrap(), "users"]);
    assert!(o.status.success());
    assert!(stdout(&o).contains("Ann"), "export includes the live document");
}

#[test]
fn flush_succeeds() {
    let dir = store_with_doc();
    let o = run(&["flush", dir.path().to_str().unwrap()]);
    assert!(o.status.success());
}

#[test]
fn unknown_command_is_usage_error() {
    let o = run(&["frobnicate", "/tmp/whatever"]);
    assert_eq!(o.status.code(), Some(2), "unknown command → exit 2");
}
