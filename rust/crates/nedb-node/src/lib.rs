//! napi-rs bindings: expose the full Rust `Db` to Node.js as the accelerated
//! nedb-engine native addon. Built with @napi-rs/cli into prebuilt per-platform
//! binaries and published to npm as `nedb-engine`.
//!
//! API surface mirrors the Python PyO3 binding (nedb-py) so the same engine
//! contract holds across both runtimes.
//!
//! © INTERCHAINED LLC × Claude Sonnet 4.6

#![deny(clippy::all)]

use napi::bindgen_prelude::*;
use napi_derive::napi;
use nedb_core::{Db, IndexKind};
use serde_json::Value;

fn jval(s: &str) -> Result<Value> {
    serde_json::from_str(s).map_err(|e| Error::from_reason(e.to_string()))
}
fn jerr(e: nedb_core::LogError) -> Error {
    Error::from_reason(e.to_string())
}
fn str_or_null(v: Option<Value>) -> Option<String> {
    v.map(|v| v.to_string())
}

#[napi(js_name = "NedbCore")]
pub struct NedbCore {
    inner: Db,
}

#[napi]
impl NedbCore {
    /// Create an in-memory database.
    #[napi(constructor)]
    pub fn new() -> Self {
        Self { inner: Db::new() }
    }

    /// Open a durable database at `path` (AOF persistence).
    #[napi(factory)]
    pub fn open(path: String) -> Result<Self> {
        Db::open(&path)
            .map(|db| Self { inner: db })
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // ── Indexes ────────────────────────────────────────────────────────────────

    #[napi]
    pub fn create_index(&mut self, coll: String, field: String, kind: String) {
        let k = match kind.as_str() {
            "ordered" => IndexKind::Ordered,
            "sorted"  => IndexKind::Sorted,
            "search"  => IndexKind::Search,
            _         => IndexKind::Eq,
        };
        self.inner.create_index(&coll, &field, k);
    }

    // ── Writes ─────────────────────────────────────────────────────────────────

    /// Auto-nonce put. `doc_json` is a JSON object string. Returns the stored doc.
    #[napi]
    pub fn put(&mut self, coll: String, id: String, doc_json: String) -> Result<String> {
        let doc = jval(&doc_json)?;
        self.inner
            .put(&coll, &id, doc, None, None, None)
            .map(|v| v.to_string())
            .map_err(jerr)
    }

    /// Full put with optional client / nonce / idempotency key.
    #[napi]
    pub fn put_ex(
        &mut self,
        coll: String,
        id: String,
        doc_json: String,
        client: Option<String>,
        nonce: Option<BigInt>,
        idem: Option<String>,
    ) -> Result<String> {
        let doc = jval(&doc_json)?;
        let cl  = client.as_deref();
        let n: Option<u64> = nonce.map(|b| b.get_u64().1);
        self.inner.put(&coll, &id, doc, cl, n, idem)
            .map(|v| v.to_string())
            .map_err(jerr)
    }

    #[napi]
    pub fn delete(&mut self, coll: String, id: String) -> Result<()> {
        self.inner.delete(&coll, &id, None, None, None).map_err(jerr)
    }

    #[napi]
    pub fn delete_ex(
        &mut self,
        coll: String,
        id: String,
        client: Option<String>,
        nonce: Option<BigInt>,
        idem: Option<String>,
    ) -> Result<()> {
        let cl = client.as_deref();
        let n: Option<u64> = nonce.map(|b| b.get_u64().1);
        self.inner.delete(&coll, &id, cl, n, idem).map_err(jerr)
    }

    #[napi]
    pub fn link(&mut self, frm: String, rel: String, to: String) -> Result<()> {
        self.inner.link(&frm, &rel, &to, None, None).map_err(jerr)
    }

    #[napi]
    pub fn unlink(&mut self, frm: String, rel: String, to: String) -> Result<()> {
        self.inner.unlink(&frm, &rel, &to, None, None).map_err(jerr)
    }

    // ── Reads ──────────────────────────────────────────────────────────────────

    #[napi]
    pub fn get(&self, coll: String, id: String) -> Option<String> {
        str_or_null(self.inner.get(&coll, &id, None))
    }

    /// Time-travel: return the document as it was at sequence `as_of`.
    #[napi]
    pub fn get_as_of(&self, coll: String, id: String, as_of: BigInt) -> Option<String> {
        str_or_null(self.inner.get(&coll, &id, Some(as_of.get_u64().1)))
    }

    /// Execute an NQL query string. Returns an array of JSON document strings.
    /// Full grammar supported: WHERE, ORDER BY, LIMIT, GROUP BY, TRAVERSE,
    /// SEARCH, AS OF, VALID AS OF, TRACE caused_by.
    #[napi]
    pub fn query(&self, nql: String) -> Result<Vec<String>> {
        self.inner.query(&nql)
            .map(|rows| rows.into_iter().map(|v| v.to_string()).collect())
            .map_err(|e| Error::from_reason(e))
    }

    #[napi]
    pub fn neighbors(&self, frm: String, rel: String) -> Vec<String> {
        self.inner.neighbors(&frm, &rel, None)
    }

    #[napi]
    pub fn neighbors_as_of(&self, frm: String, rel: String, as_of: BigInt) -> Vec<String> {
        self.inner.neighbors(&frm, &rel, Some(as_of.get_u64().1))
    }

    #[napi]
    pub fn inbound(&self, to: String, rel: String) -> Vec<String> {
        self.inner.inbound(&to, &rel, None)
    }

    #[napi]
    pub fn inbound_as_of(&self, to: String, rel: String, as_of: BigInt) -> Vec<String> {
        self.inner.inbound(&to, &rel, Some(as_of.get_u64().1))
    }

    // ── Integrity ──────────────────────────────────────────────────────────────

    #[napi]
    pub fn verify(&self) -> bool { self.inner.verify() }

    #[napi]
    pub fn head(&self) -> String { self.inner.head() }

    #[napi]
    pub fn seq(&self) -> BigInt { BigInt::from(self.inner.seq()) }

    #[napi]
    pub fn flush(&mut self) { self.inner.flush(); }
}
