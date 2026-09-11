// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

//! The layer-2 surface — register → backfill → shadow → full NEDB API.
//!
//! Embeds a `nedb_engine::Db` (the v2/v3 DAG core) and mirrors the Python/JS
//! wrap contract one-for-one.

use nedb_engine::Db;
use serde_json::{json, Value};

use crate::mapping::CollectionMapping;

/// The `.nedb` surface. Clone-friendly: the engine handle is `Arc`-backed.
#[derive(Clone)]
pub struct Surface {
    db: std::sync::Arc<Db>,
    mappings: std::sync::Arc<std::sync::RwLock<Vec<CollectionMapping>>>,
    /// When true, `shadow*` calls chain into the DAG. (Kept as a field so a
    /// host adapter can flip it at runtime, exactly like the Python/JS ports.)
    pub shadow_writes: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// A shadowed write outcome: the collection + id it landed on and the new seq.
#[derive(Debug, Clone)]
pub struct Shadowed {
    pub coll: String,
    pub id: String,
    pub seq: u64,
}

impl Surface {
    /// In-memory DAG (zero disk I/O) — tests and ephemeral use.
    pub fn in_memory() -> Self {
        Self {
            db: std::sync::Arc::new(Db::in_memory()),
            mappings: std::sync::Arc::new(std::sync::RwLock::new(Vec::new())),
            shadow_writes: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Durable DAG store at `path` (v2 loose layout; v3 via env/flag parity
    /// with the engine — `NEDB_DAG_V3`).
    pub fn open(path: &std::path::Path) -> anyhow::Result<Self> {
        Ok(Self {
            db: std::sync::Arc::new(Db::open(path, None)?),
            mappings: std::sync::Arc::new(std::sync::RwLock::new(Vec::new())),
            shadow_writes: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Durable + AES-256-GCM encrypted with an explicit 32-byte TMK. Key
    /// derivation matches `nedbd-v2` (SHA-256(TMK ‖ basename(path))).
    pub fn open_encrypted(path: &std::path::Path, tmk: [u8; 32]) -> anyhow::Result<Self> {
        let name = path.file_name().map(|n| n.as_encoded_bytes().to_vec())
            .unwrap_or_else(|| b"nedb-wrap".to_vec());
        let dek = nedb_engine::Dek::from_tmk(&tmk, &name);
        Ok(Self {
            db: std::sync::Arc::new(Db::open(path, Some(dek))?),
            mappings: std::sync::Arc::new(std::sync::RwLock::new(Vec::new())),
            shadow_writes: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Register a host key pattern as a NEDB collection.
    pub fn register(&self, pattern: &str, collection: &str) -> &Self {
        self.mappings.write().unwrap().push(CollectionMapping::new(pattern, collection));
        self
    }

    fn mapping_for(&self, key: &str) -> Option<CollectionMapping> {
        self.mappings.read().unwrap().iter().rev().find(|m| m.matches(key)).cloned()
    }

    /// Chain one host write into the DAG.
    ///
    /// `value` is the post-write host value (JSON text or JSON `Value`).
    /// `replace` selects full-replace (SET semantics) vs merge over the
    /// existing doc (HSET/field-update semantics). Returns `None` when the
    /// key has no registered mapping (raw ops are not auto-chained here —
    /// use [`Surface::chain_raw`] for the tamper-evidence-only path).
    pub fn shadow(
        &self,
        key: &str,
        value: Value,
        replace: bool,
    ) -> anyhow::Result<Option<Shadowed>> {
        if !self.shadow_writes.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(None);
        }
        let Some(m) = self.mapping_for(key) else { return Ok(None) };
        let id = m.extract_id(key).to_string();

        let mut doc = match value {
            Value::String(txt) => serde_json::from_str::<Value>(&txt)
                .unwrap_or_else(|_| json!({ "_v": txt })),
            v @ Value::Object(_) => v,
            other => json!({ "_v": other }),
        };
        if let Value::Object(map) = &mut doc {
            map.entry("_source".to_string())
                .or_insert_with(|| json!("shadow"));
        }

        let merged = if replace {
            doc
        } else if let Some(prev) = self.db.get(&m.collection, &id) {
            let mut both = prev.data.clone();
            if let (Value::Object(prev_map), Value::Object(new_map)) =
                (&mut both, &doc)
            {
                for (k, v) in new_map {
                    prev_map.insert(k.clone(), v.clone());
                }
            }
            both
        } else {
            doc
        };

        let node = self.db.put(&m.collection, &id, merged, vec![], None, None)?;
        Ok(Some(Shadowed { coll: m.collection, id, seq: node.seq }))
    }

    /// Chain a raw op (no registered mapping) — tamper evidence only.
    pub fn chain_raw(&self, cmd: &str, key: &str, args: Value) -> anyhow::Result<u64> {
        let node = self.db.put(
            "__shadow_raw__",
            key,
            json!({ "cmd": cmd, "key": key, "args": args, "_source": "shadow_raw" }),
            vec![], None, None,
        )?;
        Ok(node.seq)
    }

    // ── full NEDB API (parity with the Python/JS `.nedb`) ───────────────────

    pub fn get(&self, coll: &str, id: &str) -> Option<Value> {
        self.db.get(coll, id).map(|n| n.data)
    }
    pub fn get_as_of(&self, coll: &str, id: &str, seq: u64) -> Option<Value> {
        self.db.get_as_of(coll, id, seq).map(|n| n.data)
    }
    pub fn query(&self, nql: &str) -> anyhow::Result<Vec<Value>> {
        let (rows, _) = nedb_engine::nql::query(&self.db, nql)?;
        Ok(rows)
    }
    pub fn delete(&self, coll: &str, id: &str) -> anyhow::Result<bool> {
        self.db.delete(coll, id)
    }
    pub fn link(&self, frm: &str, rel: &str, to: &str) -> anyhow::Result<()> {
        self.db.link(frm, rel, to)
    }
    pub fn unlink(&self, frm: &str, rel: &str, to: &str) -> anyhow::Result<bool> {
        self.db.unlink(frm, rel, to)
    }
    pub fn neighbors(&self, frm: &str, rel: &str) -> Vec<String> {
        self.db.neighbors(frm, rel).iter().map(|n| n.id.clone()).collect()
    }
    /// BLAKE2b chain + Merkle integrity across the whole DAG.
    pub fn verify(&self) -> bool {
        let (_, tampered) = self.db.verify();
        tampered.is_empty()
    }
    pub fn head(&self) -> String {
        self.db.head()
    }
    pub fn seq(&self) -> u64 {
        self.db.seq.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn flush(&self) {
        self.db.flush_all();
    }
    /// Direct engine access for advanced flows (batch, subscribe, checkpoints).
    pub fn engine(&self) -> &Db {
        &self.db
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn full_loop() {
        let s = Surface::in_memory();
        s.register("driver:*", "driver");
        s.shadow_writes.store(true, std::sync::atomic::Ordering::Relaxed);

        // SET — full replace
        let r1 = s.shadow("driver:d1", json!({"name":"Bob","status":"active"}), true)
            .unwrap().expect("mapped");
        assert_eq!(r1.id, "d1");

        // HSET-style — merge
        s.shadow("driver:d1", json!({"rating": 4.9}), false).unwrap();

        let doc = s.get("driver", "d1").expect("doc present");
        assert_eq!(doc["name"], json!("Bob"));
        assert_eq!(doc["rating"], json!(4.9));
        assert_eq!(doc["_source"], json!("shadow"));

        let rows = s.query("FROM driver WHERE status = \"active\"").unwrap();
        assert_eq!(rows.len(), 1);
        assert!(s.verify());
    }

    #[test]
    fn unmapped_key_is_none() {
        let s = Surface::in_memory();
        s.shadow_writes.store(true, std::sync::atomic::Ordering::Relaxed);
        let r = s.shadow("nomatch:x", json!({"a":1}), true).unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn shadow_off_is_noop() {
        let s = Surface::in_memory();
        s.register("driver:*", "driver");
        let r = s.shadow("driver:d1", json!({"a":1}), true).unwrap();
        assert!(r.is_none());
    }
}
