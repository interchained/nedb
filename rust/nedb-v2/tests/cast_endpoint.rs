// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

//! Integration tests for the `cast` feature — natural-language query planning.
//!
//! Run:
//! ```sh
//!   cargo test --features cast --test cast_endpoint -- --nocapture
//! ```
//!
//! Most tests need a `model.cast` container. Get one from
//! <https://github.com/aiassistsecure/nedb-cast-slm/releases> and point at it:
//! ```sh
//!   export NEDBD_CAST_MODEL=/path/to/model.cast
//! ```
//! Without it, model-dependent tests SKIP rather than fail, so a plain
//! `cargo test --features cast` stays green on a machine that has no weights.
//! Set `CAST_REQUIRE_MODEL=1` (CI does) to turn a missing model into a failure —
//! otherwise a broken setup silently looks like a pass.
//!
//! What these actually guard:
//!
//!   * `nql::parse` accepts what `nql::execute` accepts. If validation and
//!     execution ever disagree about well-formedness, `/cast` would greenlight a
//!     query that then blows up — so they share one code path and this proves it.
//!   * A generated plan runs against a REAL database and returns the right rows.
//!     Not "it parsed" — it actually filtered correctly.
//!   * Collection checking catches a plan naming a collection that doesn't exist.
//!     That is the model's known failure mode on unfamiliar schemas, and it must
//!     surface as an explicit error, never as zero rows.

use nedb_engine::Db;
use nedb_engine::nql;
use serde_json::json;

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "nedb-cast-test-{}-{}",
        tag,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// A small real database: three orders, two of them over 100.
///
/// The `flush_all()` here is convenience, not realism — see
/// `collections_visible_without_explicit_flush` below, which deliberately does
/// NOT flush, because that flush was hiding a real bug.
fn seeded_db(tag: &str) -> (Db, std::path::PathBuf) {
    let dir = tmpdir(tag);
    let db = seeded_db_unflushed_at(&dir);
    db.flush_all();
    (db, dir)
}

fn seeded_db_unflushed_at(dir: &std::path::Path) -> Db {
    let db = Db::open(dir, None).expect("open db");
    db.put("orders", "o1", json!({"total": 150, "status": "paid"}), vec![], None, None)
        .expect("put o1");
    db.put("orders", "o2", json!({"total": 40, "status": "pending"}), vec![], None, None)
        .expect("put o2");
    db.put("orders", "o3", json!({"total": 900, "status": "paid"}), vec![], None, None)
        .expect("put o3");
    db
}

/// Regression: `/cast` validates the model's collection against
/// `db.id_index.collections()`. That list must reflect a write IMMEDIATELY.
///
/// The bug: `collections()` was a bare `read_dir` of the object root, while every
/// other read path overlaid the WAL write buffer. A brand-new collection has no
/// directory until the 1s flush ticker fires, so for up to a second after a
/// successful PUT the engine reported it as absent — and `/cast` answered
/// `422 collection "orders" does not exist` for a collection holding three rows.
///
/// Why the rest of this file missed it: `seeded_db` calls `flush_all()`. That one
/// line made the directory exist and the assertion pass. The test helper was
/// hiding the defect it was supposed to expose — which is why this test seeds
/// WITHOUT flushing, exactly as the live server does between a PUT and a cast.
///
/// Do not "tidy" a `flush_all()` into this test. It would still pass, and it
/// would stop testing anything.
#[test]
fn collections_visible_without_explicit_flush() {
    let dir = tmpdir("noflush");
    let db = seeded_db_unflushed_at(&dir);

    let colls = db.id_index.collections();
    assert!(
        colls.contains(&"orders".to_string()),
        "collection invisible before flush — /cast would 422 a collection that \
         exists. got {colls:?}"
    );

    // And the data really is readable at the same instant, so this is a pure
    // visibility bug rather than the write not having happened.
    let (_rows, count) = nql::query(&db, "FROM orders").expect("query orders");
    assert_eq!(count, 3, "expected 3 seeded orders, got {count}");

    let _ = std::fs::remove_dir_all(dir);
}

// ── parse/execute agreement — no model needed ─────────────────────────────────

#[test]
fn parse_accepts_what_execute_accepts() {
    let (db, dir) = seeded_db("agree");
    let good = [
        "FROM orders",
        "FROM orders WHERE total > 100",
        "FROM orders WHERE status = \"paid\" AND total >= 100",
        "FROM orders ORDER BY total DESC LIMIT 2",
        "FROM orders GROUP BY status COUNT",
        "FROM orders AS OF 1",
    ];
    for q in good {
        let parsed = nql::parse(q);
        let executed = nql::execute(&db, q);
        assert_eq!(
            parsed.is_ok(),
            executed.is_ok(),
            "parse and execute disagree on {q:?}: parse={:?} execute={:?}",
            parsed.as_ref().err().map(|e| e.to_string()),
            executed.as_ref().err().map(|e| e.to_string()),
        );
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn parse_rejects_garbage() {
    // A validator that accepts anything validates nothing.
    for bad in ["FROM orders WHERE bogus ~~ 3", "SELECT * FROM orders", "WHERE x = 1"] {
        assert!(nql::parse(bad).is_err(), "should have rejected {bad:?}");
    }
}

#[test]
fn parse_has_no_side_effects() {
    // /cast calls parse() on untrusted model output before deciding to run it.
    // If parse could mutate, that would be a write path with no auth check.
    let (db, dir) = seeded_db("noside");
    let before = db.seq.load(std::sync::atomic::Ordering::SeqCst);
    let _ = nql::parse("FROM orders WHERE total > 100");
    let _ = nql::parse("garbage that will not parse");
    let after = db.seq.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(before, after, "parse() must not touch the database");
    let _ = std::fs::remove_dir_all(dir);
}

// ── the model itself ──────────────────────────────────────────────────────────

#[cfg(feature = "cast")]
mod with_model {
    use super::*;
    use nedb_engine::cast::Caster;

    fn require_model() -> bool {
        std::env::var("CAST_REQUIRE_MODEL").map(|v| v == "1").unwrap_or(false)
    }

    /// Load the planner, or None if no weights are available.
    fn caster(dir: &std::path::Path) -> Option<Caster> {
        match Caster::load(dir) {
            Ok(c) => Some(c),
            Err(e) => {
                if require_model() {
                    panic!("CAST_REQUIRE_MODEL=1 but no model could be loaded: {e}");
                }
                eprintln!("skip: {e}");
                None
            }
        }
    }

    #[test]
    fn model_loads_and_reports_shape() {
        let dir = tmpdir("load");
        let Some(c) = caster(&dir) else { return };
        eprintln!(
            "  loaded {:.2}M params, vocab {}, from {}",
            c.n_params() as f64 / 1e6,
            c.vocab_size(),
            c.source()
        );
        assert!(c.vocab_size() > 100, "implausible vocab {}", c.vocab_size());
        assert!(c.n_params() > 1_000_000, "implausible params {}", c.n_params());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn generated_nql_parses() {
        let dir = tmpdir("parses");
        let Some(c) = caster(&dir) else { return };
        let prompts = [
            "show me all orders",
            "orders over 100",
            "paid orders sorted by total descending",
            "top 5 orders",
        ];
        let mut invalid = Vec::new();
        for p in prompts {
            let nqls = c.cast(p);
            eprintln!("  {p:?} -> {nqls}");
            if nql::parse(&nqls).is_err() {
                invalid.push(format!("{p:?} -> {nqls:?}"));
            }
        }
        assert!(invalid.is_empty(), "unparseable output:\n  {}", invalid.join("\n  "));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// The real test: a prompt becomes a plan that runs and returns correct rows.
    #[test]
    fn cast_then_execute_returns_correct_rows() {
        let (db, dir) = seeded_db("e2e");
        let Some(c) = caster(&dir) else {
            let _ = std::fs::remove_dir_all(dir);
            return;
        };

        let nqls = c.cast("orders over 100");
        eprintln!("  cast -> {nqls}");
        let parsed = nql::parse(&nqls);
        assert!(parsed.is_ok(), "generated NQL did not parse: {nqls:?}");

        let (rows, count) = nql::query(&db, &nqls).expect("execute generated NQL");
        eprintln!("  executed -> {count} rows");

        // o1 (150) and o3 (900) qualify; o2 (40) must not. We assert the SEMANTICS,
        // not the row count alone — a query that returned 2 wrong rows would pass
        // a count check.
        for r in &rows {
            let total = r.get("total").and_then(|v| v.as_f64()).unwrap_or(0.0);
            assert!(total > 100.0, "row leaked through the filter: {r}");
        }
        assert_eq!(count, 2, "expected o1 and o3, got {count} rows: {rows:?}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn unknown_collection_is_detected_not_silently_empty() {
        // The model's known weakness on unfamiliar schemas is naming a collection
        // that does not exist. Returning zero rows would read as "no matching
        // data", which is a lie. It must be reported.
        let (db, dir) = seeded_db("unknown");
        let Some(c) = caster(&dir) else {
            let _ = std::fs::remove_dir_all(dir);
            return;
        };
        let collections = db.id_index.collections();
        assert!(collections.contains(&"orders".to_string()));

        // A prompt about a collection this db does not have.
        let res = c.cast_checked("show me all stylists", &collections);
        eprintln!(
            "  cast -> {} | collection={:?} known={}",
            res.nql, res.collection, res.collection_known
        );
        if res.collection.as_deref() == Some("stylists") {
            assert!(
                !res.collection_known,
                "stylists is not in {collections:?} but was reported as known"
            );
        }

        // And the positive case must still be recognised.
        let res2 = c.cast_checked("show me all orders", &collections);
        if res2.collection.as_deref() == Some("orders") {
            assert!(res2.collection_known, "orders IS in {collections:?}");
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Drift must be reported through the same `cast_checked` path the endpoint
    /// uses, so every client gets it without opting in.
    #[test]
    fn drift_surfaces_through_cast_checked() {
        let (db, dir) = seeded_db("drift");
        let Some(c) = caster(&dir) else {
            let _ = std::fs::remove_dir_all(dir);
            return;
        };
        let collections = db.id_index.collections();

        // "pricing" is outside the 581-token vocabulary. On the released
        // checkpoint this yields SEARCH "handoff" -- a valid query answering a
        // different question. Asserted loosely because the substituted literal
        // is a property of the checkpoint: what MUST hold is that a literal
        // absent from the prompt is reported.
        let res = c.cast_checked("memories about pricing", &collections);
        eprintln!("  cast -> {} | drift={:?}", res.nql, res.drift);
        if res.nql.contains("SEARCH") {
            let lit = res.nql.split('"').nth(1).unwrap_or("");
            if !lit.is_empty() && !"memories about pricing".contains(lit) {
                assert!(
                    res.drift.is_some(),
                    "emitted literal {lit:?} absent from the prompt but drift was None"
                );
            }
        }

        // And a plan with no invented literal must stay quiet -- a check that
        // fires on correct output is a check people learn to ignore.
        let clean = c.cast_checked("show me all orders", &collections);
        eprintln!("  cast -> {} | drift={:?}", clean.nql, clean.drift);
        assert!(clean.drift.is_none(), "false positive on {:?}", clean.nql);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn decoding_is_deterministic() {
        // Greedy decoding. Same prompt must give the same plan every time, or the
        // endpoint is not safe to cache or reason about.
        let dir = tmpdir("determinism");
        let Some(c) = caster(&dir) else { return };
        let a = c.cast("orders over 100");
        let b = c.cast("orders over 100");
        assert_eq!(a, b, "greedy decode is not deterministic");
        let _ = std::fs::remove_dir_all(dir);
    }
}
