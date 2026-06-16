//! PyO3 bindings: expose the full Rust Db to Python as the accelerated `nedb._native`.
//! Built into a wheel with maturin. The pure-Python package is the always-works fallback.

use nedb_core::{Db, IndexKind};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use serde_json::Value;

fn jval(s: &str) -> PyResult<Value> {
    serde_json::from_str(s).map_err(|e| PyValueError::new_err(e.to_string()))
}
fn jerr(e: nedb_core::LogError) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}
fn str_or_null(v: Option<Value>) -> Option<String> {
    v.map(|v| v.to_string())
}

#[pyclass]
struct NedbCore {
    inner: Db,
}

#[pymethods]
impl NedbCore {
    /// Create an in-memory database.
    #[new]
    fn new() -> Self {
        Self { inner: Db::new() }
    }

    /// Open a durable database at `path` (AOF persistence).
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        Db::open(path)
            .map(|db| Self { inner: db })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    // ── Indexes ────────────────────────────────────────────────────────────────

    fn create_index(&mut self, coll: &str, field: &str, kind: &str) {
        let k = match kind {
            "eq"      => IndexKind::Eq,
            "ordered" => IndexKind::Ordered,
            "sorted"  => IndexKind::Sorted,
            "search"  => IndexKind::Search,
            _         => IndexKind::Eq,
        };
        self.inner.create_index(coll, field, k);
    }

    // ── Writes ─────────────────────────────────────────────────────────────────

    #[pyo3(signature = (coll, id, doc_json, client=None, nonce=None, idem=None))]
    fn put(
        &mut self,
        coll: &str, id: &str, doc_json: &str,
        client: Option<&str>, nonce: Option<u64>, idem: Option<String>,
    ) -> PyResult<String> {
        let doc = jval(doc_json)?;
        self.inner
            .put(coll, id, doc, client, nonce, idem)
            .map(|v| v.to_string())
            .map_err(jerr)
    }

    #[pyo3(signature = (coll, id, client=None, nonce=None, idem=None))]
    fn delete(
        &mut self,
        coll: &str, id: &str,
        client: Option<&str>, nonce: Option<u64>, idem: Option<String>,
    ) -> PyResult<()> {
        self.inner.delete(coll, id, client, nonce, idem).map_err(jerr)
    }

    #[pyo3(signature = (frm, rel, to, client=None, nonce=None))]
    fn link(
        &mut self,
        frm: &str, rel: &str, to: &str,
        client: Option<&str>, nonce: Option<u64>,
    ) -> PyResult<()> {
        self.inner.link(frm, rel, to, client, nonce).map_err(jerr)
    }

    #[pyo3(signature = (frm, rel, to, client=None, nonce=None))]
    fn unlink(
        &mut self,
        frm: &str, rel: &str, to: &str,
        client: Option<&str>, nonce: Option<u64>,
    ) -> PyResult<()> {
        self.inner.unlink(frm, rel, to, client, nonce).map_err(jerr)
    }

    // ── Reads ──────────────────────────────────────────────────────────────────

    #[pyo3(signature = (coll, id, as_of=None))]
    fn get(&self, coll: &str, id: &str, as_of: Option<u64>) -> Option<String> {
        str_or_null(self.inner.get(coll, id, as_of))
    }

    #[pyo3(signature = (nql))]
    fn query(&self, nql: &str) -> PyResult<Vec<String>> {
        self.inner.query(nql)
            .map(|rows| rows.into_iter().map(|v| v.to_string()).collect())
            .map_err(|e| PyRuntimeError::new_err(e))
    }

    #[pyo3(signature = (frm, rel, as_of=None))]
    fn neighbors(&self, frm: &str, rel: &str, as_of: Option<u64>) -> Vec<String> {
        self.inner.neighbors(frm, rel, as_of)
    }

    #[pyo3(signature = (to, rel, as_of=None))]
    fn inbound(&self, to: &str, rel: &str, as_of: Option<u64>) -> Vec<String> {
        self.inner.inbound(to, rel, as_of)
    }

    // ── Integrity ──────────────────────────────────────────────────────────────

    fn verify(&self) -> bool { self.inner.verify() }
    fn head(&self)   -> String { self.inner.head() }
    fn seq(&self)    -> u64    { self.inner.seq()  }
    fn flush(&mut self) { self.inner.flush(); }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NedbCore>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
