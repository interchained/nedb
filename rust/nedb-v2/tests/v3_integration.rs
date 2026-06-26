//! NEDB v3 integration tests — Db-level behavior of the segment substrate.
//!
//! These exercise the public `Db` API end to end: v3 write/read/reopen,
//! compaction/pruning, dual-read migration from v2 loose objects, and the
//! default-off (v2) path. Kept in a SINGLE test fn because the v3 switch is the
//! process-global `NEDB_DAG_V3` env var read at `Db::open`; running the
//! scenarios sequentially avoids racing it across parallel test threads.

use std::fs;
use std::path::Path;

use nedb_engine::Db;
use serde_json::json;

fn put_one(db: &Db, coll: &str, id: &str, v: &str) {
    db.put_batch(vec![(
        coll.to_string(),
        id.to_string(),
        json!({ "v": v }),
        Vec::new(),
        None,
        None,
    )])
    .expect("put_batch");
    db.flush_all();
}

fn value(db: &Db, coll: &str, id: &str) -> Option<String> {
    db.get(coll, id)
        .and_then(|n| n.data.get("v").and_then(|x| x.as_str().map(|s| s.to_string())))
}

#[test]
fn v3_segment_substrate_end_to_end() {
    let tmp = std::env::temp_dir().join(format!("nedb_v3_it_{}", std::process::id()));
    let _ = fs::remove_dir_all(&tmp);
    fs::create_dir_all(&tmp).unwrap();

    // ── 1. v3 mode: write / read / reopen ──────────────────────────────────────
    std::env::set_var("NEDB_DAG_V3", "1");
    let d1 = tmp.join("v3");
    {
        let db = Db::open(&d1, None).unwrap();
        for i in 0..200 {
            put_one(&db, "coins", &format!("u-{:04}", i), &format!("val-{}", i));
        }
        assert_eq!(value(&db, "coins", "u-0000").as_deref(), Some("val-0"));
        assert_eq!(value(&db, "coins", "u-0199").as_deref(), Some("val-199"));
        let (ok, bad) = db.verify();
        assert!(bad.is_empty(), "v3 verify must be clean: {:?}", bad);
        assert!(ok >= 200, "v3 verify should cover all objects");
    }
    // segment files exist; reopen rebuilds the index (via scan/.idx) and reads.
    assert!(d1.join("objects").join("segments").is_dir(), "v3 must create objects/segments/");
    {
        let db = Db::open(&d1, None).unwrap();
        assert_eq!(value(&db, "coins", "u-0123").as_deref(), Some("val-123"));

        // ── 2. compaction prunes superseded versions but keeps current ─────────
        put_one(&db, "coins", "u-0001", "val-1-NEW"); // supersede one → old version dead
        let stats = db.compact().expect("compact");
        assert!(stats.dropped_objects >= 1, "compaction should drop the dead old version");
        assert_eq!(value(&db, "coins", "u-0001").as_deref(), Some("val-1-NEW"), "current version survives compaction");
        assert_eq!(value(&db, "coins", "u-0150").as_deref(), Some("val-150"), "untouched current version survives");
        let (_ok, bad) = db.verify();
        assert!(bad.is_empty(), "verify clean after compaction: {:?}", bad);
    }
    // survives a reopen after compaction
    {
        let db = Db::open(&d1, None).unwrap();
        assert_eq!(value(&db, "coins", "u-0001").as_deref(), Some("val-1-NEW"));
        assert_eq!(value(&db, "coins", "u-0150").as_deref(), Some("val-150"));
    }

    // ── 3. dual-read migration: v2 loose objects readable in v3 mode ──────────
    let d2 = tmp.join("migrate");
    std::env::remove_var("NEDB_DAG_V3"); // write as v2 loose objects first
    {
        let db = Db::open(&d2, None).unwrap();
        put_one(&db, "coins", "old-1", "loose-1");
        put_one(&db, "coins", "old-2", "loose-2");
    }
    assert!(!d2.join("objects").join("segments").is_dir(), "v2 mode must NOT create segments/");
    std::env::set_var("NEDB_DAG_V3", "1"); // reopen in v3 mode
    {
        let db = Db::open(&d2, None).unwrap();
        // old loose objects still readable via dual-read fallback
        assert_eq!(value(&db, "coins", "old-1").as_deref(), Some("loose-1"));
        assert_eq!(value(&db, "coins", "old-2").as_deref(), Some("loose-2"));
        // new writes go to segments
        put_one(&db, "coins", "new-1", "seg-1");
        assert_eq!(value(&db, "coins", "new-1").as_deref(), Some("seg-1"));
        assert!(d2.join("objects").join("segments").is_dir(), "new writes create segments/");
        // both old (loose) and new (segment) survive a reopen
    }
    {
        let db = Db::open(&d2, None).unwrap();
        assert_eq!(value(&db, "coins", "old-1").as_deref(), Some("loose-1"));
        assert_eq!(value(&db, "coins", "new-1").as_deref(), Some("seg-1"));
    }

    // ── 4. default OFF == v2 loose objects, no segments ────────────────────────
    std::env::remove_var("NEDB_DAG_V3");
    let d3 = tmp.join("default");
    {
        let db = Db::open(&d3, None).unwrap();
        put_one(&db, "coins", "x", "y");
        assert_eq!(value(&db, "coins", "x").as_deref(), Some("y"));
    }
    assert!(!d3.join("objects").join("segments").is_dir(), "default (no flag) must stay v2 loose");

    let _ = fs::remove_dir_all(&tmp);
}

#[allow(dead_code)]
fn _unused(_p: &Path) {}
