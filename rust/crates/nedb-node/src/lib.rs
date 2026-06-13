//! napi-rs bindings: expose the Rust `Db` to Node.js as a native addon.
//! Built with @napi-rs/cli into prebuilt per-platform binaries; published to npm
//! as @interchained/nedb from this one source.

#![deny(clippy::all)]

use napi::bindgen_prelude::*;
use napi_derive::napi;
use nedb_core::Db;
use serde_json::Value;

#[napi(js_name = "NedbCore")]
pub struct NedbCore {
    inner: Db,
}

#[napi]
impl NedbCore {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self { inner: Db::new() }
    }

    /// Auto-nonce put. `doc_json` is a JSON object string.
    #[napi]
    pub fn put(&mut self, coll: String, id: String, doc_json: String) -> Result<i64> {
        let v: Value =
            serde_json::from_str(&doc_json).map_err(|e| Error::from_reason(e.to_string()))?;
        self.inner
            .put(&coll, &id, v)
            .map(|s| s as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Replay-protected, idempotent put with explicit client/nonce/idem.
    #[napi]
    pub fn put_checked(
        &mut self,
        coll: String,
        id: String,
        doc_json: String,
        client: String,
        nonce: i64,
        idem: Option<String>,
    ) -> Result<i64> {
        let v: Value =
            serde_json::from_str(&doc_json).map_err(|e| Error::from_reason(e.to_string()))?;
        self.inner
            .put_checked(&coll, &id, v, &client, nonce as u64, idem)
            .map(|s| s as i64)
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    #[napi]
    pub fn get(&self, coll: String, id: String) -> Option<String> {
        self.inner.get(&coll, &id).map(|v| v.to_string())
    }

    #[napi]
    pub fn get_as_of(&self, coll: String, id: String, seq: i64) -> Option<String> {
        self.inner.get_as_of(&coll, &id, seq as u64).map(|v| v.to_string())
    }

    #[napi]
    pub fn head(&self) -> String {
        self.inner.head().to_string()
    }

    #[napi]
    pub fn verify(&self) -> bool {
        self.inner.verify()
    }
}
