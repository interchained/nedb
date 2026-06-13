//! nedb-core — the production speed core for NEDB.
//!
//! The OpLog is the source of truth; the MVCC store is a materialized view folded
//! from the log. State is a pure function of the log, which is what gives us crash
//! recovery, deterministic replay, and `AS OF seq` time-travel.
//!
//! This crate is consumed by:
//!   * `nedb-py`   — PyO3 bindings → PyPI wheels (one source, all platforms)
//!   * `nedb-node` — napi-rs bindings → npm prebuilt binaries
//!   * a future `nedbd` server speaking a Redis-compatible (RESP) wire protocol.

pub mod log;
pub mod store;

pub use log::{LogError, Op, OpLog, GENESIS};
pub use store::MvccStore;

use std::collections::HashMap;

use serde_json::Value;

/// High-level embedded database tying the log to the materialized store.
#[derive(Default)]
pub struct Db {
    pub log: OpLog,
    pub store: MvccStore,
    nonce: HashMap<String, u64>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            log: OpLog::new(),
            store: MvccStore::default(),
            nonce: HashMap::new(),
        }
    }

    fn next_nonce(&mut self, client: &str) -> u64 {
        let n = self.nonce.get(client).copied().unwrap_or(0) + 1;
        self.nonce.insert(client.to_string(), n);
        n
    }

    fn apply(&mut self, op: &Op) {
        match op.op.as_str() {
            "put" => {
                if let (Some(key), Some(doc)) =
                    (op.payload.get("key").and_then(|v| v.as_str()), op.payload.get("doc"))
                {
                    self.store.put(key, doc.clone(), op.seq);
                }
            }
            "delete" => {
                if let Some(key) = op.payload.get("key").and_then(|v| v.as_str()) {
                    self.store.delete(key, op.seq);
                }
            }
            _ => {}
        }
    }

    pub fn put(&mut self, coll: &str, id: &str, doc: Value) -> Result<u64, LogError> {
        let key = format!("{coll}:{id}");
        let nonce = self.next_nonce("local");
        let payload = serde_json::json!({"key": key, "coll": coll, "id": id, "doc": doc});
        let (op, created) = self.log.append("local", nonce, "put", payload, None)?;
        if created {
            self.apply(&op);
        }
        Ok(op.seq)
    }

    /// Replay-protected, idempotent write with an explicit client/nonce/idem key.
    pub fn put_checked(
        &mut self,
        coll: &str,
        id: &str,
        doc: Value,
        client: &str,
        nonce: u64,
        idem: Option<String>,
    ) -> Result<u64, LogError> {
        let key = format!("{coll}:{id}");
        let payload = serde_json::json!({"key": key, "coll": coll, "id": id, "doc": doc});
        let (op, created) = self.log.append(client, nonce, "put", payload, idem)?;
        if created {
            self.apply(&op);
        }
        Ok(op.seq)
    }

    pub fn get(&self, coll: &str, id: &str) -> Option<Value> {
        self.store.get(&format!("{coll}:{id}"), None).cloned()
    }

    pub fn get_as_of(&self, coll: &str, id: &str, seq: u64) -> Option<Value> {
        self.store.get(&format!("{coll}:{id}"), Some(seq)).cloned()
    }

    pub fn head(&self) -> &str {
        self.log.head()
    }

    pub fn verify(&self) -> bool {
        self.log.verify()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_and_time_travel() {
        let mut db = Db::new();
        let s = db.put("users", "alice", serde_json::json!({"city": "Austin"})).unwrap();
        db.put("users", "alice", serde_json::json!({"city": "Lisbon"})).unwrap();
        assert_eq!(db.get("users", "alice").unwrap()["city"], "Lisbon");
        assert_eq!(db.get_as_of("users", "alice", s).unwrap()["city"], "Austin");
        assert!(db.verify());
    }
}
