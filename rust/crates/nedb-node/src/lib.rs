// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

//! napi-rs bindings: expose the v2 DAG Db to Node.js as the accelerated
//! nedb-engine native addon. Built with @napi-rs/cli into prebuilt per-platform
//! binaries and published to npm as `nedb-engine`.
//!
//! API surface mirrors the Python PyO3 binding (nedb-py) so the same engine
//! contract holds across both runtimes.
//!
//! © INTERCHAINED LLC × Vex (Interchained AI fleet: GLM · Claude · Opus · Fable · GPT-6)

#![deny(clippy::all)]

use std::sync::Arc;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use nedb_engine::{Db, nql};
use serde_json::Value;

fn jerr(e: impl std::fmt::Display) -> Error {
    Error::from_reason(e.to_string())
}

fn node_to_json_str(node: &nedb_engine::store::Node) -> String {
    let mut obj = if let Value::Object(m) = &node.data { m.clone() } else { Default::default() };
    obj.insert("_id".into(),   Value::String(node.id.clone()));
    obj.insert("_hash".into(), Value::String(node.hash.clone()));
    obj.insert("_seq".into(),  serde_json::json!(node.seq));
    obj.insert("_coll".into(), Value::String(node.coll.clone()));
    // Project the causal + bitemporal lanes exactly as the daemon's NQL
    // serializer (nedb-v2/src/nql.rs node_to_json) does, so an embedded
    // (napi) consumer sees the same node shape as an HTTP one. Without this,
    // `_caused_by` is invisible to embedded readers even though the causal
    // edge exists and TRACE resolves it — a silent read-projection gap that
    // forced downstreams (e.g. mantel) to reconstruct it from residual data.
    if !node.caused_by.is_empty() {
        obj.insert("_caused_by".into(), Value::Array(
            node.caused_by.iter().map(|h| Value::String(h.clone())).collect()
        ));
    }
    if let Some(ref vf) = node.valid_from {
        obj.insert("_valid_from".into(), Value::String(vf.clone()));
    }
    if let Some(ref vt) = node.valid_to {
        obj.insert("_valid_to".into(), Value::String(vt.clone()));
    }
    Value::Object(obj).to_string()
}

#[napi(js_name = "NedbCore")]
pub struct NedbCore {
    inner: Arc<Db>,
}

#[napi]
impl NedbCore {
    /// Create an in-memory v2 DAG database — zero disk I/O.
    #[napi(constructor)]
    pub fn new() -> Self {
        Self { inner: Arc::new(Db::in_memory()) }
    }

    /// Open a durable v2 DAG database at `path`.
    /// Automatically migrates v1 AOF → v2 DAG on first open.
    ///
    /// Durable-mode auto-flush-on-exit is wired in the JS wrapper via
    /// `process.on('SIGTERM'|'SIGINT'|'beforeExit', () => db.flush())` — the
    /// libuv-cooperative hook — NOT a C-level signal handler here, which would
    /// clobber libuv's own signal machinery.
    #[napi(factory)]
    pub fn open(path: String) -> Result<Self> {
        let db = Db::open(std::path::Path::new(&path), None)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        let inner = Arc::new(db);
        // Durability parity with nedbd (2.8.5): flush the id-index WAL + MANIFEST on a cadence, not
        // only on exit. Without this an embedded app killed with SIGKILL lost every write since open.
        if let Some(ms) = Db::embedded_flush_interval_ms() {
            Db::start_manifest_ticker(Arc::clone(&inner), ms);
        }
        Ok(Self { inner })
    }

    // ── Indexes ────────────────────────────────────────────────────────────────

    #[napi]
    pub fn create_index(&self, coll: String, field: String, _kind: String) {
        // v2 supports sorted indexes; all kinds map to sorted for NQL compatibility
        self.inner.create_sorted_index(&coll, &field);
    }

    // ── Writes ─────────────────────────────────────────────────────────────────

    /// Put a document. Returns the stored doc as a JSON string.
    #[napi]
    pub fn put(&self, coll: String, id: String, doc_json: String) -> Result<String> {
        let doc: Value = serde_json::from_str(&doc_json)
            .map_err(|e| Error::from_reason(e.to_string()))?;
        let caused_by: Vec<String> = doc.get("caused_by")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect())
            .unwrap_or_default();
        let valid_from = doc.get("valid_from").and_then(|v| v.as_str()).map(str::to_string);
        let valid_to   = doc.get("valid_to").and_then(|v| v.as_str()).map(str::to_string);
        self.inner.put(&coll, &id, doc, caused_by, valid_from, valid_to)
            .map(|n| node_to_json_str(&n))
            .map_err(|e| jerr(e))
    }

    /// Full put with optional client / nonce — API compat, v2 ignores these.
    #[napi]
    pub fn put_ex(
        &self,
        coll: String, id: String, doc_json: String,
        _client: Option<String>, _nonce: Option<BigInt>, _idem: Option<String>,
    ) -> Result<String> {
        self.put(coll, id, doc_json)
    }

    #[napi]
    pub fn delete(&self, coll: String, id: String) -> Result<()> {
        self.inner.delete(&coll, &id).map(|_| ()).map_err(|e| jerr(e))
    }

    #[napi]
    pub fn delete_ex(
        &self, coll: String, id: String,
        _client: Option<String>, _nonce: Option<BigInt>, _idem: Option<String>,
    ) -> Result<()> {
        self.delete(coll, id)
    }

    /// Link: stored as a doc in __links__ collection for NQL traversal.
    #[napi]
    pub fn link(&self, frm: String, rel: String, to: String) -> Result<()> {
        let link_id = format!("{}|{}|{}", frm, rel, to);
        let doc = serde_json::json!({"_from": frm, "_rel": rel, "_to": to});
        self.inner.put("__links__", &link_id, doc, vec![], None, None)
            .map(|_| ()).map_err(|e| jerr(e))
    }

    #[napi]
    pub fn unlink(&self, frm: String, rel: String, to: String) -> Result<()> {
        let link_id = format!("{}|{}|{}", frm, rel, to);
        self.inner.delete("__links__", &link_id).map(|_| ()).map_err(|e| jerr(e))
    }

    // ── Reads ──────────────────────────────────────────────────────────────────

    #[napi]
    pub fn get(&self, coll: String, id: String) -> Option<String> {
        self.inner.get(&coll, &id).as_ref().map(node_to_json_str)
    }

    #[napi]
    pub fn get_as_of(&self, coll: String, id: String, as_of: BigInt) -> Option<String> {
        self.inner.get_as_of(&coll, &id, as_of.get_u64().1)
            .as_ref().map(node_to_json_str)
    }

    #[napi]
    pub fn query(&self, nql_str: String) -> Result<Vec<String>> {
        nql::query(&self.inner, &nql_str)
            .map(|(rows, _)| rows.into_iter().map(|v| v.to_string()).collect())
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    #[napi]
    pub fn neighbors(&self, frm: String, rel: String) -> Vec<String> {
        let nql_str = format!(r#"FROM __links__ WHERE _from = "{}" AND _rel = "{}""#, frm, rel);
        nql::query(&self.inner, &nql_str)
            .map(|(rows, _)| rows.iter()
                .filter_map(|r| r.get("_to").and_then(|v| v.as_str()).map(str::to_string))
                .collect())
            .unwrap_or_default()
    }

    #[napi]
    pub fn neighbors_as_of(&self, frm: String, rel: String, as_of: BigInt) -> Vec<String> {
        // Time-travel the causal DAG: edges live as docs in __links__, so an NQL
        // AS OF query returns only the edges live at `as_of` — an edge linked at
        // a later seq is excluded, and one unlinked since is restored. Mirrors
        // neighbors() with `AS OF {seq}`. (Verified: AS OF before the link seq
        // returns [], AS OF at/after returns the edge.)
        let seq = as_of.get_u64().1;
        let nql_str = format!(
            r#"FROM __links__ AS OF {} WHERE _from = "{}" AND _rel = "{}""#,
            seq, frm, rel);
        nql::query(&self.inner, &nql_str)
            .map(|(rows, _)| rows.iter()
                .filter_map(|r| r.get("_to").and_then(|v| v.as_str()).map(str::to_string))
                .collect())
            .unwrap_or_default()
    }

    #[napi]
    pub fn inbound(&self, to: String, rel: String) -> Vec<String> {
        let nql_str = format!(r#"FROM __links__ WHERE _to = "{}" AND _rel = "{}""#, to, rel);
        nql::query(&self.inner, &nql_str)
            .map(|(rows, _)| rows.iter()
                .filter_map(|r| r.get("_from").and_then(|v| v.as_str()).map(str::to_string))
                .collect())
            .unwrap_or_default()
    }

    #[napi]
    pub fn inbound_as_of(&self, to: String, rel: String, as_of: BigInt) -> Vec<String> {
        // Time-travel inbound edges — see neighbors_as_of. Mirrors inbound() with
        // `AS OF {seq}` so only edges live at `as_of` are returned.
        let seq = as_of.get_u64().1;
        let nql_str = format!(
            r#"FROM __links__ AS OF {} WHERE _to = "{}" AND _rel = "{}""#,
            seq, to, rel);
        nql::query(&self.inner, &nql_str)
            .map(|(rows, _)| rows.iter()
                .filter_map(|r| r.get("_from").and_then(|v| v.as_str()).map(str::to_string))
                .collect())
            .unwrap_or_default()
    }

    // ── Integrity ──────────────────────────────────────────────────────────────

    #[napi]
    pub fn verify(&self) -> bool {
        let (_, tampered) = self.inner.verify();
        tampered.is_empty()
    }

    #[napi]
    pub fn head(&self) -> String { self.inner.head() }

    #[napi]
    pub fn seq(&self) -> BigInt {
        BigInt::from(self.inner.seq.load(std::sync::atomic::Ordering::SeqCst))
    }

    /// Flush WAL and MANIFEST — v2 equivalent of v1 flush().
    #[napi]
    pub fn flush(&self) { self.inner.flush_all(); }

    /// The tip — the most recent write (latest node) as a JSON string, or null if
    /// the database is empty. The cheap "give me the latest write" primitive.
    #[napi]
    pub fn tip(&self) -> Option<String> {
        self.inner.tip().as_ref().map(node_to_json_str)
    }

    /// Collection-local tip — the most recent write into `coll` as a JSON string,
    /// or null if the collection has no writes. Resume one chain without filtering.
    #[napi]
    pub fn tip_collection(&self, coll: String) -> Option<String> {
        self.inner.tip_collection(&coll).as_ref().map(node_to_json_str)
    }

    /// Changefeed page after `after_seq` (exclusive), up to `limit` nodes (0 = the
    /// engine default cap), as a JSON envelope string:
    /// `{nodes, from_seq, to_seq, head_seq, has_more}`. Page while `has_more`,
    /// advancing your cursor to `to_seq`, then attach to the live subscribe edge.
    #[napi]
    pub fn since(&self, after_seq: BigInt, limit: i64) -> String {
        let (_, after, _) = after_seq.get_u64();
        let b = self.inner.since(after, limit.max(0) as usize);
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
    #[napi]
    pub fn scan_status(&self) -> String {
        let s = self.inner.scan_status();
        serde_json::json!({
            "scan_complete": s.scan_complete, "seq_index_ready": s.seq_index_ready, "tip_seq": s.tip_seq,
            "indexed_seq_min": s.indexed_seq_min, "indexed_seq_max": s.indexed_seq_max,
            "indexed_count": s.indexed_count
        }).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::node_to_json_str;
    use nedb_engine::store::Node;
    use serde_json::Value;

    fn node() -> Node {
        Node {
            id: "sig1".into(),
            coll: "signals".into(),
            seq: 7,
            data: serde_json::json!({ "claim": "fits in 24GB" }),
            prev: None,
            caused_by: vec!["a".into(), "b".into()],
            ts: 1_718_400_000.0,
            valid_from: Some("2026-01-01".into()),
            valid_to: None,
            hash: "deadbeef".into(),
        }
    }

    #[test]
    fn serializes_causal_and_bitemporal_lanes_like_the_daemon() {
        let v: Value = serde_json::from_str(&node_to_json_str(&node())).unwrap();
        // The bug this fix closes: _caused_by must be present on the embedded
        // read projection, matching nedb-v2/src/nql.rs node_to_json.
        assert_eq!(
            v["_caused_by"],
            serde_json::json!(["a", "b"]),
            "embedded serializer must project _caused_by"
        );
        assert_eq!(v["_valid_from"], serde_json::json!("2026-01-01"));
        // valid_to is None -> the key is omitted, not null (daemon parity).
        assert!(v.get("_valid_to").is_none(), "absent valid_to omits the key");
        // The pre-existing lanes still hold.
        assert_eq!(v["_id"], serde_json::json!("sig1"));
        assert_eq!(v["_hash"], serde_json::json!("deadbeef"));
        assert_eq!(v["_seq"], serde_json::json!(7));
        assert_eq!(v["_coll"], serde_json::json!("signals"));
        assert_eq!(v["claim"], serde_json::json!("fits in 24GB"));
    }

    #[test]
    fn omits_caused_by_when_empty() {
        let mut n = node();
        n.caused_by = vec![];
        let v: Value = serde_json::from_str(&node_to_json_str(&n)).unwrap();
        // An empty causal set omits the key entirely — a root write is not
        // "caused by nothing", it simply has no _caused_by field.
        assert!(v.get("_caused_by").is_none(), "empty caused_by omits the key");
    }
}
