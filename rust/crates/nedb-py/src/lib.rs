//! PyO3 bindings: expose the Rust `Db` to Python as the accelerated backend.
//! Built into a wheel with maturin; published to PyPI for all platforms from this
//! one source. The pure-Python package (../../nedb) is the reference/fallback.

use nedb_core::Db;
use pyo3::exceptions::{PyValueError, PyRuntimeError};
use pyo3::prelude::*;
use serde_json::Value;

#[pyclass]
struct NedbCore {
    inner: Db,
}

#[pymethods]
impl NedbCore {
    #[new]
    fn new() -> Self {
        Self { inner: Db::new() }
    }

    /// Auto-nonce put. `doc_json` is a JSON object string.
    fn put(&mut self, coll: &str, id: &str, doc_json: &str) -> PyResult<u64> {
        let v: Value = serde_json::from_str(doc_json)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        self.inner
            .put(coll, id, v)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Replay-protected, idempotent put with explicit client/nonce/idem.
    #[pyo3(signature = (coll, id, doc_json, client, nonce, idem=None))]
    fn put_checked(
        &mut self,
        coll: &str,
        id: &str,
        doc_json: &str,
        client: &str,
        nonce: u64,
        idem: Option<String>,
    ) -> PyResult<u64> {
        let v: Value = serde_json::from_str(doc_json)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        self.inner
            .put_checked(coll, id, v, client, nonce, idem)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn get(&self, coll: &str, id: &str) -> Option<String> {
        self.inner.get(coll, id).map(|v| v.to_string())
    }

    fn get_as_of(&self, coll: &str, id: &str, seq: u64) -> Option<String> {
        self.inner.get_as_of(coll, id, seq).map(|v| v.to_string())
    }

    fn head(&self) -> String {
        self.inner.head().to_string()
    }

    fn verify(&self) -> bool {
        self.inner.verify()
    }
}

#[pymodule]
fn nedb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NedbCore>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
