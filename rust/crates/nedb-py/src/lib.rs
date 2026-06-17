//! PyO3 bindings: expose the v2 DAG Db to Python as the accelerated `nedb._native`.
//! Built into a wheel with maturin. The pure-Python package is the always-works fallback.
//!
//! API surface is identical to the v1 bindings so existing Python code works unchanged.
//! Under the hood, all operations go through nedb_core_v2::Db (content-addressed DAG).

// pyo3::prelude::* must come first — it exports the proc-macro attributes
// (#[pyclass], #[pymethods], #[pyo3(signature=...)]) that maturin needs.
use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use std::sync::Arc;
use nedb_core_v2::{Db, nql};
use serde_json::Value;

fn jerr(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

fn node_to_json_str(node: &nedb_core_v2::store::Node) -> String {
    // Return the node's data merged with metadata fields (_id, _hash, _seq, _coll)
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

#[pymethods]
impl NedbCore {
    /// Create an in-memory v2 DAG database — zero disk I/O.
    #[new]
    fn new() -> Self {
        Self { inner: Arc::new(Db::in_memory()) }
    }

    /// Open a durable v2 DAG database at `path`.
    /// Automatically migrates v1 AOF → v2 DAG on first open.
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        Db::open(std::path::Path::new(path), None)
            .map(|db| Self { inner: Arc::new(db) })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    // ── Indexes ────────────────────────────────────────────────────────────────

    fn create_index(&self, coll: &str, field: &str, kind: &str) {
        // v2 supports sorted indexes; eq/search map to sorted for NQL compatibility
        match kind {
            "eq" | "ordered" | "sorted" | "search" => {
                self.inner.create_sorted_index(coll, field);
            }
            _ => {}
        }
    }

    // ── Writes ─────────────────────────────────────────────────────────────────

    /// Put a document. client/nonce/idem are accepted for API compatibility but
    /// v2 DAG uses caused_by for provenance — pass caused_by as a JSON array in doc
    /// or via the `caused_by` key if present.
    #[pyo3(signature = (coll, id, doc_json, client=None, nonce=None, idem=None))]
    fn put(
        &self,
        coll: &str, id: &str, doc_json: &str,
        _client: Option<&str>, _nonce: Option<u64>, _idem: Option<String>,
    ) -> PyResult<String> {
        let doc: Value = serde_json::from_str(doc_json)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        // Extract caused_by from doc if present (list of hash strings)
        let caused_by: Vec<String> = doc.get("caused_by")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect())
            .unwrap_or_default();
        let valid_from = doc.get("valid_from").and_then(|v| v.as_str()).map(str::to_string);
        let valid_to   = doc.get("valid_to").and_then(|v| v.as_str()).map(str::to_string);

        self.inner.put(coll, id, doc, caused_by, valid_from, valid_to)
            .map(|node| node_to_json_str(&node))
            .map_err(|e| jerr(e))
    }

    #[pyo3(signature = (coll, id, client=None, nonce=None, idem=None))]
    fn delete(
        &self,
        coll: &str, id: &str,
        _client: Option<&str>, _nonce: Option<u64>, _idem: Option<String>,
    ) -> PyResult<()> {
        self.inner.delete(coll, id).map(|_| ()).map_err(|e| jerr(e))
    }

    /// Link: stored as a doc in __links__ collection for NQL traversal.
    #[pyo3(signature = (frm, rel, to, client=None, nonce=None))]
    fn link(
        &self,
        frm: &str, rel: &str, to: &str,
        _client: Option<&str>, _nonce: Option<u64>,
    ) -> PyResult<()> {
        let link_id = format!("{}|{}|{}", frm, rel, to);
        let doc = serde_json::json!({"_from": frm, "_rel": rel, "_to": to});
        self.inner.put("__links__", &link_id, doc, vec![], None, None)
            .map(|_| ())
            .map_err(|e| jerr(e))
    }

    #[pyo3(signature = (frm, rel, to, client=None, nonce=None))]
    fn unlink(
        &self,
        frm: &str, rel: &str, to: &str,
        _client: Option<&str>, _nonce: Option<u64>,
    ) -> PyResult<()> {
        let link_id = format!("{}|{}|{}", frm, rel, to);
        self.inner.delete("__links__", &link_id).map(|_| ()).map_err(|e| jerr(e))
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

    #[pyo3(signature = (nql_str))]
    fn query(&self, nql_str: &str) -> PyResult<Vec<String>> {
        nql::query(&self.inner, nql_str)
            .map(|(rows, _)| rows.into_iter().map(|v| v.to_string()).collect())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Traverse links stored in __links__ collection.
    #[pyo3(signature = (frm, rel, as_of=None))]
    fn neighbors(&self, frm: &str, rel: &str, _as_of: Option<u64>) -> Vec<String> {
        let nql = format!(
            r#"FROM __links__ WHERE _from = "{}" AND _rel = "{}""#,
            frm, rel
        );
        nql::query(&self.inner, &nql)
            .map(|(rows, _)| rows.iter()
                .filter_map(|r| r.get("_to").and_then(|v| v.as_str()).map(str::to_string))
                .collect())
            .unwrap_or_default()
    }

    #[pyo3(signature = (to, rel, as_of=None))]
    fn inbound(&self, to: &str, rel: &str, _as_of: Option<u64>) -> Vec<String> {
        let nql = format!(
            r#"FROM __links__ WHERE _to = "{}" AND _rel = "{}""#,
            to, rel
        );
        nql::query(&self.inner, &nql)
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

    /// Flush WAL and MANIFEST — v2 equivalent of v1 flush().
    fn flush(&self) { self.inner.flush_all(); }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NedbCore>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
