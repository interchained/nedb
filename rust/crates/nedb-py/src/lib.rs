//! PyO3 bindings: expose the v2 DAG Db to Python as the accelerated `nedb._native`.
//! Built into a wheel with maturin. The pure-Python package is the always-works fallback.
//!
//! API surface is identical to the v1 bindings so existing Python code works unchanged.
//! Under the hood, all operations go through nedb_engine::Db (content-addressed DAG).

// pyo3::prelude::* must come first so proc-macro attributes are in scope.
use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use pyo3::types::PyCFunction;
use std::sync::{Arc, Weak};
use nedb_engine::{Db, nql};
use serde_json::Value;

/// Register a Python `atexit` hook that flushes this durable database on
/// interpreter shutdown — durable-mode auto-flush-on-exit for the native module.
///
/// Cooperative with CPython: `atexit` runs on normal exit AND on Ctrl+C (SIGINT →
/// `KeyboardInterrupt` → interpreter exit), so no C-level `sigaction` is needed —
/// which is exactly what we want, since seizing SIGINT would break
/// `KeyboardInterrupt`. Holds only a `Weak<Db>`, so it never keeps the database
/// alive. Best-effort: the caller ignores registration errors so a hook hiccup
/// never fails `open()` (a clean shutdown still flushes via `Drop`).
fn register_atexit_flush(py: Python<'_>, weak: Weak<Db>) -> PyResult<()> {
    let flush = PyCFunction::new_closure_bound(py, None, None, move |_args, _kwargs| {
        if let Some(db) = weak.upgrade() {
            db.flush_all();
        }
    })?;
    py.import_bound("atexit")?.call_method1("register", (flush,))?;
    Ok(())
}

fn jerr(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

fn node_to_json_str(node: &nedb_engine::store::Node) -> String {
    let mut obj = if let Value::Object(m) = &node.data { m.clone() } else { Default::default() };
    obj.insert("_id".into(),   Value::String(node.id.clone()));
    obj.insert("_hash".into(), Value::String(node.hash.clone()));
    obj.insert("_seq".into(),  serde_json::json!(node.seq));
    obj.insert("_coll".into(), Value::String(node.coll.clone()));
    Value::Object(obj).to_string()
}

#[pyclass]
struct NedbCore {
    inner: Arc<Db>,
}

#[allow(unused_variables)]
#[pymethods]
impl NedbCore {
    /// Create an in-memory v2 DAG database — zero disk I/O.
    #[new]
    fn new() -> Self {
        Self { inner: Arc::new(Db::in_memory()) }
    }

    /// Open a durable v2 DAG database at `path`.
    ///
    /// Durable-mode auto-flush-on-exit is armed here by registering a Python
    /// `atexit` hook (see `register_atexit_flush`) — the CPython-cooperative path,
    /// NOT a C-level signal handler, which would seize `SIGINT` and break
    /// `KeyboardInterrupt`. The in-memory constructor never arms it.
    #[staticmethod]
    fn open(py: Python<'_>, path: &str) -> PyResult<Self> {
        let db = Db::open(std::path::Path::new(path), None)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let inner = Arc::new(db);
        // Best-effort: a hook-registration hiccup must not fail open().
        let _ = register_atexit_flush(py, Arc::downgrade(&inner));
        Ok(Self { inner })
    }

    // ── Indexes ────────────────────────────────────────────────────────────────

    fn create_index(&self, coll: &str, field: &str, kind: &str) {
        self.inner.create_sorted_index(coll, field);
    }

    // ── Writes ─────────────────────────────────────────────────────────────────

    #[pyo3(signature = (coll, id, doc_json, client=None, nonce=None, idem=None))]
    fn put(
        &self,
        coll: &str, id: &str, doc_json: &str,
        client: Option<&str>, nonce: Option<u64>, idem: Option<String>,
    ) -> PyResult<String> {
        let doc: Value = serde_json::from_str(doc_json)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let caused_by: Vec<String> = doc.get("caused_by")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect())
            .unwrap_or_default();
        let valid_from = doc.get("valid_from").and_then(|v| v.as_str()).map(str::to_string);
        let valid_to   = doc.get("valid_to").and_then(|v| v.as_str()).map(str::to_string);
        self.inner.put(coll, id, doc, caused_by, valid_from, valid_to)
            .map(|node| node_to_json_str(&node))
            .map_err(jerr)
    }

    #[pyo3(signature = (coll, id, client=None, nonce=None, idem=None))]
    fn delete(
        &self,
        coll: &str, id: &str,
        client: Option<&str>, nonce: Option<u64>, idem: Option<String>,
    ) -> PyResult<()> {
        self.inner.delete(coll, id).map(|_| ()).map_err(jerr)
    }

    #[pyo3(signature = (frm, rel, to, client=None, nonce=None))]
    fn link(
        &self,
        frm: &str, rel: &str, to: &str,
        client: Option<&str>, nonce: Option<u64>,
    ) -> PyResult<()> {
        let link_id = format!("{}|{}|{}", frm, rel, to);
        let doc = serde_json::json!({"_from": frm, "_rel": rel, "_to": to});
        self.inner.put("__links__", &link_id, doc, vec![], None, None)
            .map(|_| ()).map_err(jerr)
    }

    #[pyo3(signature = (frm, rel, to, client=None, nonce=None))]
    fn unlink(
        &self,
        frm: &str, rel: &str, to: &str,
        client: Option<&str>, nonce: Option<u64>,
    ) -> PyResult<()> {
        let link_id = format!("{}|{}|{}", frm, rel, to);
        self.inner.delete("__links__", &link_id).map(|_| ()).map_err(jerr)
    }

    // ── Reads ──────────────────────────────────────────────────────────────────

    #[pyo3(signature = (coll, id, as_of=None))]
    fn get(&self, coll: &str, id: &str, as_of: Option<u64>) -> Option<String> {
        let node = if let Some(seq) = as_of {
            self.inner.get_as_of(coll, id, seq)
        } else {
            self.inner.get(coll, id)
        };
        node.as_ref().map(node_to_json_str)
    }

    #[pyo3(signature = (nql))]
    fn query(&self, nql: &str) -> PyResult<Vec<String>> {
        nql::query(&self.inner, nql)
            .map(|(rows, _)| rows.into_iter().map(|v| v.to_string()).collect())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    #[pyo3(signature = (frm, rel, as_of=None))]
    fn neighbors(&self, frm: &str, rel: &str, as_of: Option<u64>) -> Vec<String> {
        let nql_str = format!(r#"FROM __links__ WHERE _from = "{}" AND _rel = "{}""#, frm, rel);
        nql::query(&self.inner, &nql_str)
            .map(|(rows, _)| rows.iter()
                .filter_map(|r| r.get("_to").and_then(|v| v.as_str()).map(str::to_string))
                .collect())
            .unwrap_or_default()
    }

    #[pyo3(signature = (to, rel, as_of=None))]
    fn inbound(&self, to: &str, rel: &str, as_of: Option<u64>) -> Vec<String> {
        let nql_str = format!(r#"FROM __links__ WHERE _to = "{}" AND _rel = "{}""#, to, rel);
        nql::query(&self.inner, &nql_str)
            .map(|(rows, _)| rows.iter()
                .filter_map(|r| r.get("_from").and_then(|v| v.as_str()).map(str::to_string))
                .collect())
            .unwrap_or_default()
    }

    // ── Integrity ──────────────────────────────────────────────────────────────

    fn verify(&self) -> bool {
        let (_, tampered) = self.inner.verify();
        tampered.is_empty()
    }

    fn head(&self) -> String { self.inner.head() }

    fn seq(&self) -> u64 {
        self.inner.seq.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn flush(&self) { self.inner.flush_all(); }

    // ── tip / changefeed ─────────────────────────────────────────────────────────

    /// The tip — the most recent write (latest node) as a JSON string, or None if
    /// the database is empty. The cheap "give me the latest write" primitive.
    fn tip(&self) -> Option<String> {
        self.inner.tip().as_ref().map(node_to_json_str)
    }

    /// Collection-local tip — the most recent write into `coll`, or None. Lets a
    /// consumer resume one chain (blocks / tx / utxo) without filtering global tip.
    fn tip_collection(&self, coll: &str) -> Option<String> {
        self.inner.tip_collection(coll).as_ref().map(node_to_json_str)
    }

    /// Changefeed page after `after_seq` (exclusive), up to `limit` nodes (0 = the
    /// engine default cap), as a JSON envelope string:
    /// `{nodes, from_seq, to_seq, head_seq, has_more}`. Page while `has_more`,
    /// advancing your cursor to `to_seq`, then attach to the live subscribe edge.
    #[pyo3(signature = (after_seq, limit=0))]
    fn since(&self, after_seq: u64, limit: usize) -> String {
        let b = self.inner.since(after_seq, limit);
        let nodes: Vec<Value> = b.nodes.iter()
            .filter_map(|n| serde_json::from_str::<Value>(&node_to_json_str(n)).ok())
            .collect();
        serde_json::json!({
            "nodes": nodes, "from_seq": b.from_seq, "to_seq": b.to_seq,
            "head_seq": b.head_seq, "has_more": b.has_more
        }).to_string()
    }

    /// Replication readiness as a JSON string: `{scan_complete, tip_seq,
    /// indexed_seq_min, indexed_seq_max, indexed_count}`. Wait for
    /// `scan_complete == true` before trusting historical `since()` catch-up.
    fn scan_status(&self) -> String {
        let s = self.inner.scan_status();
        serde_json::json!({
            "scan_complete": s.scan_complete, "tip_seq": s.tip_seq,
            "indexed_seq_min": s.indexed_seq_min, "indexed_seq_max": s.indexed_seq_max,
            "indexed_count": s.indexed_count
        }).to_string()
    }
}

/// Flush index WAL and MANIFEST when the Python object is freed.
/// Ensures id_index and MANIFEST are written to disk on `del db` or
/// when the object goes out of scope — no explicit flush() call needed.
impl Drop for NedbCore {
    fn drop(&mut self) {
        // Only flush durable databases (no-op for in-memory)
        self.inner.flush_all();
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NedbCore>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
