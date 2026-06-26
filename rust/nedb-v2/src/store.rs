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
use std::sync::Arc;
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

/// NEDB v3 opt-in: the `--dag-v3` flag sets `NEDB_DAG_V3`, which switches the
/// object substrate to the packed segment store. Default off → byte-for-byte v2.
fn dag_v3_enabled() -> bool {
    std::env::var("NEDB_DAG_V3")
        .map(|v| {
            let v = v.trim();
            v == "1" || v.eq_ignore_ascii_case("true")
                     || v.eq_ignore_ascii_case("on")
                     || v.eq_ignore_ascii_case("yes")
        })
        .unwrap_or(false)
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
    /// In-memory store: hash → raw bytes. None = disk-backed (normal mode).
    mem:  Option<Arc<dashmap::DashMap<String, Vec<u8>>>>,
    /// NEDB v3 packed substrate. Some = segment mode (NEDB_DAG_V3 / --dag-v3);
    /// new writes go to segments, reads fall back to loose v2 objects.
    seg:  Option<crate::segment::SegmentStore>,
}

impl ObjectStore {
    pub fn new(db_root: &Path, dek: Option<Dek>) -> Result<Self> {
        let root = db_root.join("objects");
        fs::create_dir_all(&root)
            .context("create objects/ dir")?;
        // v3 opt-in: bring up the packed segment substrate (and rebuild its
        // index by scanning existing segments). Off by default → loose objects.
        let seg = if dag_v3_enabled() {
            Some(crate::segment::SegmentStore::open(&root)?)
        } else {
            None
        };
        Ok(Self { root, dek, mem: None, seg })
    }

    /// Create a pure in-memory object store — no disk, no files.
    pub fn in_memory() -> Self {
        Self {
            root: PathBuf::from(":memory:"),
            dek:  None,
            mem:  Some(Arc::new(dashmap::DashMap::new())),
            seg:  None,
        }
    }

    /// Write a node. Returns the content hash (the node's permanent ID in the DAG).
    pub fn write(&self, node: &mut Node) -> Result<String> {
        // The content hash is taken over the node's CONTENT, never over its own
        // hash field (which is circular). `hash` is `skip_serializing_if =
        // "String::is_empty"`, so a *fresh* node already excludes it — but a
        // node being re-written (node.hash already set from a prior write) would
        // serialize the populated hash and produce a different content hash,
        // breaking idempotency. Clear it first so first-write and re-write
        // serialize byte-for-byte identical content.
        node.hash = String::new();
        let raw = serde_json::to_vec(node)?;
        let content = match &self.dek {
            Some(dek) => encrypt(&raw, dek)?,
            None      => raw,
        };
        let hash = blake2b(&content);

        if let Some(ref mem) = self.mem {
            // In-memory: store in DashMap — idempotent
            mem.entry(hash.clone()).or_insert_with(|| content);
        } else if let Some(ref seg) = self.seg {
            // v3: append into a packed segment (one fsync per batch via sync()).
            seg.put(&hash, &content)?;
        } else {
            // v2: loose object file, written atomically via tmp → rename
            let dir  = self.root.join(&hash[..2]);
            fs::create_dir_all(&dir)?;
            let path = dir.join(&hash[2..]);
            if !path.exists() {
                let tmp = path.with_extension("tmp");
                fs::write(&tmp, &content)?;
                fs::rename(&tmp, &path)?;
            }
        }
        node.hash = hash.clone();
        Ok(hash)
    }

    /// Read and verify a node by its hash. Returns error on hash mismatch (tamper).
    pub fn read(&self, hash: &str) -> Result<Node> {
        if hash.len() < 3 {
            anyhow::bail!("invalid object hash (too short): {:?}", hash);
        }

        // In-memory mode (tests).
        if let Some(ref mem) = self.mem {
            let content = mem.get(hash)
                .map(|v| v.clone())
                .ok_or_else(|| anyhow::anyhow!("object not found in memory: {}", hash))?;
            return self.decode(content, hash);
        }

        // v3 segment mode: try segments first (self-verifying), then fall back
        // to the loose-object path so existing v2 data stays readable.
        if let Some(ref seg) = self.seg {
            if let Some(content) = seg.get(hash)? {
                return self.decode(content, hash);
            }
            // miss → fall through to loose objects (dual-read migration)
        }

        // v2 loose object.
        let path = self.root.join(&hash[..2]).join(&hash[2..]);
        let c = fs::read(&path).with_context(|| format!("read object {}", hash))?;
        // Hash verification — any bit rot or tampering is caught here
        let actual = blake2b(&c);
        if actual != hash {
            bail!("object {} tampered: expected {} got {}", hash, hash, actual);
        }
        self.decode(c, hash)
    }

    /// Decrypt (if a DEK is set) and deserialize raw content bytes into a Node.
    /// Hash verification is the caller's responsibility (done before this for the
    /// loose path; inside SegmentStore::get for the segment path; trusted for mem).
    fn decode(&self, content: Vec<u8>, hash: &str) -> Result<Node> {
        let raw = match &self.dek {
            Some(dek) => decrypt(&content, dek)?,
            None      => content,
        };
        let mut node: Node = serde_json::from_slice(&raw)
            .context("deserialize node")?;
        if node.hash.is_empty() {
            node.hash = hash.to_string();
        }
        Ok(node)
    }

    /// List all object hashes (for startup index rebuild / verify).
    pub fn all_hashes(&self) -> Box<dyn Iterator<Item = String> + '_> {
        // In-memory: collect from DashMap
        if let Some(ref mem) = self.mem {
            let hashes: Vec<String> = mem.iter().map(|e| e.key().clone()).collect();
            return Box::new(hashes.into_iter());
        }

        // v3: union of packed-segment hashes and any loose v2 objects (deduped),
        // skipping the segments/ subdir during the loose walk.
        if let Some(ref seg) = self.seg {
            let mut seen: std::collections::HashSet<String> =
                seg.all_hashes().into_iter().collect();
            if let Ok(rd) = fs::read_dir(&self.root) {
                for prefix_dir in rd.flatten() {
                    if !prefix_dir.file_type().map(|t| t.is_dir()).unwrap_or(false) { continue; }
                    let prefix = prefix_dir.file_name().to_string_lossy().to_string();
                    if prefix.len() != 2 { continue; } // skip "segments" and non-prefix dirs
                    if let Ok(rd2) = fs::read_dir(prefix_dir.path()) {
                        for e in rd2.flatten() {
                            let name = e.file_name().to_string_lossy().to_string();
                            if name.ends_with(".tmp") { continue; }
                            seen.insert(format!("{}{}", prefix, name));
                        }
                    }
                }
            }
            return Box::new(seen.into_iter());
        }

        // v2 (default): lazy walk of the objects/ directory tree (unchanged).
        let root = self.root.clone();
        Box::new(fs::read_dir(&root)
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
            }))
    }

    /// Flush durable state for the active segment (v3): one fsync per batch,
    /// wired into Db::flush_all(). No-op for loose-object and in-memory modes.
    pub fn sync(&self) -> Result<()> {
        if let Some(ref seg) = self.seg {
            seg.sync()?;
        }
        Ok(())
    }

    /// Compact the packed segment store (v3), keeping only objects whose hash is
    /// in `live` and reclaiming the rest. No-op (zeroed stats) for loose-object
    /// and in-memory modes.
    pub fn compact(&self, live: &std::collections::HashSet<String>) -> Result<crate::segment::CompactStats> {
        match self.seg {
            Some(ref seg) => seg.compact(live),
            None => Ok(crate::segment::CompactStats::default()),
        }
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
