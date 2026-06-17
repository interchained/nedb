//! Content-addressed object store — the foundation of NEDB v2.
//!
//! Every document version is stored as an immutable, encrypted, BLAKE2b-hashed
//! object at `objects/{hash[0:2]}/{hash[2:]}`. Once written, objects never change.
//!
//! Uncorruptable by design:
//! - Writes are atomic (write to .tmp → rename)
//! - Every read verifies the BLAKE2b hash of the content
//! - A partial write leaves a .tmp file that is ignored on startup
//! - There is no single mutable file that can be partially overwritten

use std::fs;
use std::path::{Path, PathBuf};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use blake2::{Blake2b512, Digest};

/// A single versioned document node in the DAG.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// User-supplied document ID (e.g. "618000", "abc-token-id")
    pub id:         String,
    /// Collection name (e.g. "blocks", "itsl_ops")
    pub coll:       String,
    /// Monotonic global sequence number assigned at write time
    pub seq:        u64,
    /// The document payload (arbitrary JSON)
    pub data:       serde_json::Value,
    /// BLAKE2b hash of the previous version of this document (version chain)
    pub prev:       Option<String>,
    /// BLAKE2b hashes of nodes that causally led to this write
    pub caused_by:  Vec<String>,
    /// Unix timestamp (seconds since epoch)
    pub ts:         f64,
    /// Bi-temporal valid-from (ISO 8601)
    pub valid_from: Option<String>,
    /// Bi-temporal valid-to   (ISO 8601); None = still valid
    pub valid_to:   Option<String>,
    /// BLAKE2b hash of this node's encrypted content (set after writing)
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub hash:       String,
}

/// Encryption key material (AES-256-GCM).
/// In v1 this was called DEK; the structure is the same.
#[derive(Clone)]
pub struct Dek(pub [u8; 32]);

impl Dek {
    pub fn from_tmk(tmk: &[u8; 32], salt: &[u8]) -> Self {
        use sha2::{Sha256, Digest as _};
        let mut h = Sha256::new();
        h.update(tmk);
        h.update(salt);
        let result = h.finalize();
        let mut key = [0u8; 32];
        key.copy_from_slice(&result[..32]);
        Dek(key)
    }
}

fn blake2b(data: &[u8]) -> String {
    let mut h = Blake2b512::new();
    h.update(data);
    hex::encode(&h.finalize()[..32])   // use first 32 bytes → 64 hex chars
}

fn encrypt(data: &[u8], dek: &Dek) -> Result<Vec<u8>> {
    use aes_gcm::{Aes256Gcm, KeyInit, aead::{Aead, OsRng, rand_core::RngCore}};
    let cipher = Aes256Gcm::new_from_slice(&dek.0)?;
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = aes_gcm::Nonce::from(nonce_bytes);
    let ciphertext = cipher.encrypt(&nonce, data)
        .map_err(|e| anyhow::anyhow!("encrypt failed: {:?}", e))?;
    // Format: 12-byte nonce || ciphertext
    let mut out = nonce_bytes.to_vec();
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

fn decrypt(data: &[u8], dek: &Dek) -> Result<Vec<u8>> {
    use aes_gcm::{Aes256Gcm, KeyInit, aead::Aead};
    if data.len() < 12 { bail!("ciphertext too short"); }
    let (nonce_bytes, ciphertext) = data.split_at(12);
    let cipher = Aes256Gcm::new_from_slice(&dek.0)?;
    let nonce = aes_gcm::Nonce::from_slice(nonce_bytes);
    cipher.decrypt(nonce, ciphertext)
        .map_err(|e| anyhow::anyhow!("decrypt failed: {:?}", e))
}

/// Content-addressed, encrypted, tamper-evident object store.
pub struct ObjectStore {
    root: PathBuf,
    dek:  Option<Dek>,
}

impl ObjectStore {
    pub fn new(db_root: &Path, dek: Option<Dek>) -> Result<Self> {
        let root = db_root.join("objects");
        fs::create_dir_all(&root)
            .context("create objects/ dir")?;
        Ok(Self { root, dek })
    }

    /// Write a node. Returns the content hash (the node's permanent ID in the DAG).
    pub fn write(&self, node: &mut Node) -> Result<String> {
        let raw = serde_json::to_vec(node)?;
        let content = match &self.dek {
            Some(dek) => encrypt(&raw, dek)?,
            None      => raw,
        };
        let hash = blake2b(&content);

        // Write atomically: content to .tmp, then rename
        let dir  = self.root.join(&hash[..2]);
        fs::create_dir_all(&dir)?;
        let path = dir.join(&hash[2..]);
        if !path.exists() {                          // idempotent
            let tmp = path.with_extension("tmp");
            fs::write(&tmp, &content)?;
            fs::rename(&tmp, &path)?;
        }
        node.hash = hash.clone();
        Ok(hash)
    }

    /// Read and verify a node by its hash. Returns error on hash mismatch (tamper).
    pub fn read(&self, hash: &str) -> Result<Node> {
        if hash.len() < 3 {
            anyhow::bail!("invalid object hash (too short): {:?}", hash);
        }
        let path = self.root.join(&hash[..2]).join(&hash[2..]);
        let content = fs::read(&path)
            .with_context(|| format!("read object {}", hash))?;

        // Hash verification — any bit rot or tampering is caught here
        let actual = blake2b(&content);
        if actual != hash {
            bail!("object {} tampered: expected {} got {}", hash, hash, actual);
        }

        let raw = match &self.dek {
            Some(dek) => decrypt(&content, dek)?,
            None      => content,
        };
        let mut node: Node = serde_json::from_slice(&raw)
            .context("deserialize node")?;
        // hash is skip_serializing_if = "is_empty" so it's not in the file.
        // Restore it from the path we used to look up this object.
        if node.hash.is_empty() {
            node.hash = hash.to_string();
        }
        Ok(node)
    }

    /// List all object hashes (for startup index rebuild).
    pub fn all_hashes(&self) -> impl Iterator<Item = String> + '_ {
        let root = self.root.clone();
        fs::read_dir(&root)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .flat_map(move |prefix_dir| {
                let prefix = prefix_dir.file_name().to_string_lossy().to_string();
                fs::read_dir(prefix_dir.path())
                    .into_iter()
                    .flatten()
                    .filter_map(|e| e.ok())
                    .filter_map(move |e| {
                        let name = e.file_name().to_string_lossy().to_string();
                        if name.ends_with(".tmp") { return None; }
                        Some(format!("{}{}", prefix, name))
                    })
            })
    }

    /// Verify all objects. Returns (ok_count, tampered_hashes).
    pub fn verify_all(&self) -> (usize, Vec<String>) {
        use rayon::prelude::*;
        let hashes: Vec<String> = self.all_hashes().collect();
        let results: Vec<(bool, String)> = hashes.par_iter().map(|h| {
            (self.read(h).is_ok(), h.clone())
        }).collect();
        let ok = results.iter().filter(|(ok, _)| *ok).count();
        let bad: Vec<String> = results.into_iter()
            .filter(|(ok, _)| !*ok)
            .map(|(_, h)| h)
            .collect();
        (ok, bad)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_node(id: &str, coll: &str, seq: u64) -> Node {
        Node {
            id: id.to_string(), coll: coll.to_string(), seq,
            data: serde_json::json!({"height": seq, "hash": "0000abc"}),
            prev: None, caused_by: vec![], ts: 1718400000.0,
            valid_from: None, valid_to: None, hash: String::new(),
        }
    }

    #[test]
    fn write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let store = ObjectStore::new(dir.path(), None).unwrap();
        let mut node = make_node("1", "blocks", 1);
        let hash = store.write(&mut node).unwrap();
        assert_eq!(hash.len(), 64);
        let read_back = store.read(&hash).unwrap();
        assert_eq!(read_back.id, "1");
        assert_eq!(read_back.coll, "blocks");
    }

    #[test]
    fn write_is_idempotent() {
        let dir = tempdir().unwrap();
        let store = ObjectStore::new(dir.path(), None).unwrap();
        let mut node = make_node("1", "blocks", 1);
        let h1 = store.write(&mut node).unwrap();
        let h2 = store.write(&mut node).unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn tamper_detected() {
        let dir = tempdir().unwrap();
        let store = ObjectStore::new(dir.path(), None).unwrap();
        let mut node = make_node("1", "blocks", 1);
        let hash = store.write(&mut node).unwrap();
        // Corrupt the object file
        let path = dir.path().join("objects").join(&hash[..2]).join(&hash[2..]);
        let mut content = fs::read(&path).unwrap();
        content[10] ^= 0xff;
        fs::write(&path, content).unwrap();
        assert!(store.read(&hash).is_err());
    }
}
