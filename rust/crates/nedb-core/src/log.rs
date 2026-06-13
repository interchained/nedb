//! The append-only, hash-chained, nonce-enforced, idempotent operation log.
//!
//! This is the single source of truth. Replay protection (strictly-monotonic
//! per-client nonces), idempotency (dedup by key), and tamper evidence (BLAKE3
//! hash chain) all live here. The head hash commits to the entire history and is
//! anchorable on-chain (e.g. ITC).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const GENESIS: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Op {
    pub seq: u64,
    pub client: String,
    pub nonce: u64,
    pub op: String,
    pub payload: Value,
    pub idem: Option<String>,
    pub prev_hash: String,
    pub hash: String,
}

#[derive(Debug, Clone)]
pub enum LogError {
    /// An op was replayed with a stale/duplicate nonce.
    Replay { client: String, nonce: u64, last: u64 },
}

impl std::fmt::Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogError::Replay { client, nonce, last } => write!(
                f,
                "replay/stale nonce for client '{client}': {nonce} <= {last}"
            ),
        }
    }
}
impl std::error::Error for LogError {}

/// Canonical JSON encoding for hashing. `serde_json::Value::Object` is backed by a
/// BTreeMap by default, so keys serialize in sorted order — deterministic across
/// platforms and language bindings.
fn canonical(v: &Value) -> String {
    serde_json::to_string(v).expect("serialize op body")
}

fn chain_hash(prev: &str, body: &Value) -> String {
    let mut h = blake3::Hasher::new();
    h.update(prev.as_bytes());
    h.update(canonical(body).as_bytes());
    h.finalize().to_hex().to_string()
}

#[derive(Default)]
pub struct OpLog {
    pub ops: Vec<Op>,
    last_nonce: HashMap<String, u64>,
    idem: HashMap<String, usize>,
    head: String,
}

impl OpLog {
    pub fn new() -> Self {
        Self {
            ops: Vec::new(),
            last_nonce: HashMap::new(),
            idem: HashMap::new(),
            head: GENESIS.to_string(),
        }
    }

    /// Append an op. Returns `(op, created)`; `created == false` means the op was
    /// deduplicated by its idempotency key (replay-safe no-op return).
    pub fn append(
        &mut self,
        client: &str,
        nonce: u64,
        op: &str,
        payload: Value,
        idem: Option<String>,
    ) -> Result<(Op, bool), LogError> {
        if let Some(k) = &idem {
            if let Some(&seq) = self.idem.get(k) {
                return Ok((self.ops[seq].clone(), false));
            }
        }
        let last = *self.last_nonce.get(client).unwrap_or(&0);
        if nonce <= last {
            return Err(LogError::Replay {
                client: client.to_string(),
                nonce,
                last,
            });
        }

        let seq = self.ops.len() as u64;
        let body = serde_json::json!({
            "seq": seq, "client": client, "nonce": nonce,
            "op": op, "payload": payload, "idem": idem,
        });
        let hash = chain_hash(&self.head, &body);
        let rec = Op {
            seq,
            client: client.to_string(),
            nonce,
            op: op.to_string(),
            payload,
            idem: idem.clone(),
            prev_hash: std::mem::replace(&mut self.head, hash.clone()),
            hash,
        };
        self.ops.push(rec.clone());
        self.last_nonce.insert(client.to_string(), nonce);
        if let Some(k) = idem {
            self.idem.insert(k, seq as usize);
        }
        Ok((rec, true))
    }

    /// Re-walk the chain and confirm no op has been tampered with.
    pub fn verify(&self) -> bool {
        let mut prev = GENESIS.to_string();
        for o in &self.ops {
            if o.prev_hash != prev {
                return false;
            }
            let body = serde_json::json!({
                "seq": o.seq, "client": o.client, "nonce": o.nonce,
                "op": o.op, "payload": o.payload, "idem": o.idem,
            });
            if o.hash != chain_hash(&prev, &body) {
                return false;
            }
            prev = o.hash.clone();
        }
        true
    }

    pub fn head(&self) -> &str {
        &self.head
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_is_rejected() {
        let mut log = OpLog::new();
        log.append("a", 1, "put", serde_json::json!({}), None).unwrap();
        let err = log.append("a", 1, "put", serde_json::json!({}), None);
        assert!(err.is_err());
    }

    #[test]
    fn idempotent_key_dedups() {
        let mut log = OpLog::new();
        let (_, c1) = log
            .append("a", 1, "put", serde_json::json!({}), Some("k".into()))
            .unwrap();
        let (_, c2) = log
            .append("a", 2, "put", serde_json::json!({}), Some("k".into()))
            .unwrap();
        assert!(c1 && !c2);
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn chain_verifies_and_detects_tamper() {
        let mut log = OpLog::new();
        log.append("a", 1, "put", serde_json::json!({"x": 1}), None)
            .unwrap();
        log.append("a", 2, "put", serde_json::json!({"x": 2}), None)
            .unwrap();
        assert!(log.verify());
        log.ops[0].payload = serde_json::json!({"x": 999});
        assert!(!log.verify());
    }
}
