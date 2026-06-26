//! Index store for NEDB v2.
//!
//! Two index types:
//!
//! 1. **ID index** (`indexes/{coll}/id/{doc_id}` → object hash)
//!    Atomic file-per-document. Reading is a single `fs::read_to_string`.
//!    Writing is atomic (write .tmp → rename). Parallel reads are lock-free.
//!
//! 2. **Sorted index** (`indexes/{coll}/{field}.sorted` → in-memory BTreeMap)
//!    Rebuilt from object store on startup. Persisted as a compact binary
//!    file for fast cold start. Used for ORDER BY field ASC/DESC LIMIT n.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use anyhow::Result;
use dashmap::DashMap;
use serde_json::Value;

/// Ordered JSON value for BTree indexes (null < bool < number < string < array < object).
#[derive(Debug, Clone, PartialEq)]
pub enum OrderedValue {
    Null,
    Bool(bool),
    Number(f64),   // NaN-safe comparison via total_cmp
    Str(String),
    Array(Vec<OrderedValue>),
    Object,        // objects are all equal in ordering (sort by insertion order falls back to hash)
}

impl Eq for OrderedValue {}

impl PartialOrd for OrderedValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use OrderedValue::*;
        use std::cmp::Ordering::*;
        match (self, other) {
            (Null, Null)       => Equal,
            (Null, _)          => Less,
            (_, Null)          => Greater,
            (Bool(a), Bool(b)) => a.cmp(b),
            (Bool(_), _)       => Less,
            (_, Bool(_))       => Greater,
            (Number(a), Number(b)) => a.total_cmp(b),
            (Number(_), _)     => Less,
            (_, Number(_))     => Greater,
            (Str(a), Str(b))   => a.cmp(b),
            (Str(_), _)        => Less,
            (_, Str(_))        => Greater,
            (Array(a), Array(b)) => a.cmp(b),
            (Array(_), _)      => Less,
            (_, Array(_))      => Greater,
            (Object, Object)   => Equal,
        }
    }
}

impl From<&Value> for OrderedValue {
    fn from(v: &Value) -> Self {
        match v {
            Value::Null        => OrderedValue::Null,
            Value::Bool(b)     => OrderedValue::Bool(*b),
            Value::Number(n)   => OrderedValue::Number(n.as_f64().unwrap_or(f64::NAN)),
            Value::String(s)   => OrderedValue::Str(s.clone()),
            Value::Array(a)    => OrderedValue::Array(a.iter().map(|x| x.into()).collect()),
            Value::Object(_)   => OrderedValue::Object,
        }
    }
}

/// Compute a 2-char hex shard prefix from a document id.
/// Distributes files across 256 subdirectories to avoid flat-directory
/// slowdown on ext4/xfs when a collection has >50k documents.
fn id_shard(id: &str) -> String {
    // FNV-1a 32-bit — fast, no crypto needed, deterministic
    let mut hash: u32 = 2166136261;
    for b in id.bytes() {
        hash ^= b as u32;
        hash = hash.wrapping_mul(16777619);
    }
    format!("{:02x}", hash & 0xff)
}

/// Encode a document id into a filesystem-safe leaf filename.
///
/// The id-index stores one file per document, and the id is the filename. Raw
/// ids work on case-sensitive POSIX filesystems, but ids containing bytes that
/// are illegal in Windows filenames (`: | / \ < > " ? *`, control chars) — most
/// notably link ids like `driver:d1|handles|trip:t1` — cannot be written there,
/// so the write silently fails and the entry is lost on reopen.
///
/// We percent-escape every byte that isn't unreserved (`A-Z a-z 0-9 - _ .`).
/// `%` itself is escaped so decoding is unambiguous. Safe ids (block heights,
/// hex hashes, utxo keys) are all-unreserved and return UNCHANGED, so existing
/// chainstate paths are byte-for-byte identical and the hot path is unaffected.
fn encode_id(id: &str) -> String {
    fn is_unreserved(b: u8) -> bool {
        b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.')
    }
    if id.bytes().all(is_unreserved) {
        return id.to_string();
    }
    let mut out = String::with_capacity(id.len() + 8);
    for &b in id.as_bytes() {
        if is_unreserved(b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

/// Inverse of `encode_id`. A name with no `%` (a safe id, or a legacy raw id
/// written by an older version on a POSIX filesystem) is returned unchanged, so
/// `list_ids` recovers the right id for both new and pre-upgrade files.
fn decode_id(name: &str) -> String {
    if !name.contains('%') {
        return name.to_string();
    }
    fn hexval(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'A'..=b'F' => Some(b - b'A' + 10),
            b'a'..=b'f' => Some(b - b'a' + 10),
            _ => None,
        }
    }
    let bytes = name.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hexval(bytes[i + 1]), hexval(bytes[i + 2])) {
                out.push(hi * 16 + lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Per-document ID index — atomic file-per-doc, sharded across 256 subdirs.
///
/// Write path: updates go to `write_buf` (DashMap, zero I/O, lock-free).
/// Background ticker calls `flush_write_buf()` every 1s — Rayon-parallel disk writes.
/// Read path: `write_buf` checked first (latest value), then disk.
/// This eliminates per-PUT `fs::rename` from the hot path, fixing concurrent write contention.
pub struct IdIndex {
    root:      PathBuf,
    /// In-memory store: (coll, id) → hash. None = disk-backed (normal mode).
    mem:       Option<Arc<dashmap::DashMap<(String, String), String>>>,
    /// WAL write buffer — disk-backed mode buffers here, flushed to disk periodically.
    write_buf: Arc<dashmap::DashMap<(String, String), Option<String>>>,  // None = tombstone
}

impl IdIndex {
    pub fn new(db_root: &Path) -> Result<Self> {
        let root = db_root.join("indexes");
        fs::create_dir_all(&root)?;
        Ok(Self { root, mem: None, write_buf: Arc::new(dashmap::DashMap::new()) })
    }

    /// Create a pure in-memory id index — no disk I/O.
    pub fn in_memory() -> Self {
        Self {
            root:      PathBuf::from(":memory:"),
            mem:       Some(Arc::new(dashmap::DashMap::new())),
            write_buf: Arc::new(dashmap::DashMap::new()),
        }
    }

    /// Flush the WAL write buffer to disk in parallel. Called by the background ticker.
    /// No-op for in-memory databases. Safe to call concurrently with writes.
    pub fn flush_write_buf(&self) {
        if self.mem.is_some() || self.write_buf.is_empty() { return; }
        use rayon::prelude::*;
        // Drain all pending entries and write them in parallel
        let entries: Vec<((String, String), Option<String>)> = self.write_buf
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        entries.par_iter().for_each(|((coll, id), hash_opt)| {
            match hash_opt {
                Some(hash) => {
                    // Write/update: tmp → rename
                    let path = self.path(coll, id);
                    if let Some(parent) = path.parent() {
                        let _ = fs::create_dir_all(parent);
                    }
                    let tmp = path.with_extension("tmp");
                    if fs::write(&tmp, hash).is_ok() {
                        let _ = fs::rename(&tmp, &path);
                    }
                }
                None => {
                    // Tombstone: remove the file (encoded leaf + legacy raw if distinct)
                    let path = self.path(coll, id);
                    let _ = fs::remove_file(&path);
                    let raw = self.raw_path(coll, id);
                    if raw != path { let _ = fs::remove_file(&raw); }
                }
            }
        });
        // Clear flushed entries
        for ((coll, id), _) in &entries {
            self.write_buf.remove(&(coll.clone(), id.clone()));
        }
    }

    fn path(&self, coll: &str, id: &str) -> PathBuf {
        // Shard across 256 subdirectories using first 2 hex chars of a simple
        // hash of the id. Prevents flat-directory slowdown (ext4 htree degrades
        // past ~50k files per directory) for large collections like kv.
        // Format: indexes/{coll}/id/{shard}/{encode_id(id)}
        // Shard on the RAW id (stable across versions); only the leaf filename
        // is encoded so it is legal on every filesystem (incl. Windows).
        let shard = id_shard(id);
        self.root.join(coll).join("id").join(&shard).join(encode_id(id))
    }

    /// Legacy path: the raw id as the leaf filename (pre-`encode_id`). Used only
    /// as a read/cleanup fallback so id-index entries written by older versions
    /// on POSIX filesystems stay readable after upgrade. On Windows a raw path
    /// with illegal chars simply fails to open (→ treated as absent).
    fn raw_path(&self, coll: &str, id: &str) -> PathBuf {
        let shard = id_shard(id);
        self.root.join(coll).join("id").join(&shard).join(id)
    }

    /// Get the current object hash for a document.
    /// Checks WAL write buffer first (most recent), then disk.
    pub fn get(&self, coll: &str, id: &str) -> Option<String> {
        if let Some(ref mem) = self.mem {
            return mem.get(&(coll.to_string(), id.to_string())).map(|v| v.clone());
        }
        // Check WAL buffer first — may have an unflushed write or tombstone
        let key = (coll.to_string(), id.to_string());
        if let Some(entry) = self.write_buf.get(&key) {
            return entry.value().clone();  // None = tombstoned
        }
        // Fall through to disk: encoded filename first, then the legacy raw
        // filename (pre-upgrade data). For safe ids the two paths are identical,
        // so this is a single read on the hot path.
        let p = self.path(coll, id);
        let content = match fs::read_to_string(&p) {
            Ok(c) => c,
            Err(_) => {
                let raw = self.raw_path(coll, id);
                if raw == p { return None; }
                fs::read_to_string(&raw).ok()?
            }
        };
        let h = content.trim().to_string();
        if h.is_empty() { None } else { Some(h) }
    }

    /// Set the current object hash for a document.
    /// Disk mode: writes to WAL buffer only (zero I/O on hot path).
    /// Background ticker flushes WAL to disk every 1s via Rayon.
    pub fn set(&self, coll: &str, id: &str, hash: &str) -> Result<()> {
        if let Some(ref mem) = self.mem {
            mem.insert((coll.to_string(), id.to_string()), hash.to_string());
            return Ok(());
        }
        // WAL: buffer the update, no disk I/O here
        self.write_buf.insert(
            (coll.to_string(), id.to_string()),
            Some(hash.to_string()),
        );
        Ok(())
    }

    /// List all doc IDs in a collection (memory map or disk + WAL merge).
    pub fn list_ids(&self, coll: &str) -> Vec<String> {
        if let Some(ref mem) = self.mem {
            return mem.iter()
                .filter(|e| e.key().0 == coll)
                .map(|e| e.key().1.clone())
                .collect();
        }
        // Read from disk then overlay WAL (adds buffered writes, removes tombstones)
        let id_root = self.root.join(coll).join("id");
        // Each entry in id_root is a 2-char hex shard dir
        fs::read_dir(&id_root)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .flat_map(|shard_dir| {
                fs::read_dir(shard_dir.path())
                    .into_iter()
                    .flatten()
                    .filter_map(|e| e.ok())
                    .filter_map(|e| {
                        let name = e.file_name().to_string_lossy().to_string();
                        if name.ends_with(".tmp") { return None; }
                        // Decode the on-disk filename back to the document id
                        // (encoded for new files; identity for legacy/safe ids).
                        Some(decode_id(&name))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            // Overlay WAL: add buffered writes, remove tombstones
            .chain(
                self.write_buf.iter()
                    .filter(|e| e.key().0 == coll && e.value().is_some())
                    .map(|e| e.key().1.clone())
            )
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .filter(|id| {
                // Exclude WAL tombstones
                self.write_buf.get(&(coll.to_string(), id.clone()))
                    .map(|v| v.is_some())
                    .unwrap_or(true)
            })
            .collect()
    }

    /// Remove the id index entry for a document (tombstone / delete).
    /// Disk mode: writes a tombstone to the WAL buffer; flushed to disk on next ticker.
    pub fn remove(&self, coll: &str, id: &str) -> Result<()> {
        if let Some(ref mem) = self.mem {
            mem.remove(&(coll.to_string(), id.to_string()));
            return Ok(());
        }
        // WAL tombstone: None value means "delete this file on flush"
        self.write_buf.insert((coll.to_string(), id.to_string()), None);
        Ok(())
    }

    /// List all known collections.
    pub fn collections(&self) -> Vec<String> {
        if let Some(ref mem) = self.mem {
            let mut colls: Vec<String> = mem.iter()
                .map(|e| e.key().0.clone())
                .collect::<std::collections::HashSet<_>>()
                .into_iter().collect();
            colls.sort();
            return colls;
        }
        fs::read_dir(&self.root)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect()
    }
}

/// In-memory sorted index per (collection, field).
/// Rebuilt from object store on startup. O(log n) ORDER BY queries.
pub struct SortedIndexes {
    /// (coll, field) → BTreeMap<value, Vec<hash>>
    inner: DashMap<(String, String), BTreeMap<OrderedValue, Vec<String>>>,
}

impl SortedIndexes {
    pub fn new() -> Self {
        Self { inner: DashMap::new() }
    }

    /// Register a field as sorted-indexed for a collection.
    /// Must be called before any puts for that field to be indexed.
    pub fn ensure(&self, coll: &str, field: &str) {
        self.inner
            .entry((coll.to_string(), field.to_string()))
            .or_default();
    }

    /// Insert (or update) a value → hash mapping.
    pub fn insert(&self, coll: &str, field: &str, value: &Value, hash: &str) {
        let key = (coll.to_string(), field.to_string());
        if let Some(mut idx) = self.inner.get_mut(&key) {
            let ov = OrderedValue::from(value);
            idx.entry(ov)
               .or_default()
               .push(hash.to_string());
        }
    }

    /// Remove a hash from the index (on overwrite/delete of a doc version).
    pub fn remove(&self, coll: &str, field: &str, value: &Value, hash: &str) {
        let key = (coll.to_string(), field.to_string());
        if let Some(mut idx) = self.inner.get_mut(&key) {
            let ov = OrderedValue::from(value);
            if let Some(hashes) = idx.get_mut(&ov) {
                hashes.retain(|h| h != hash);
                if hashes.is_empty() { idx.remove(&ov); }
            }
        }
    }

    /// Return the top-k hashes ordered by field ASC.
    pub fn top_k_asc(&self, coll: &str, field: &str, k: usize) -> Vec<String> {
        let key = (coll.to_string(), field.to_string());
        self.inner.get(&key).map(|idx| {
            idx.values().flat_map(|v| v.iter().cloned()).take(k).collect()
        }).unwrap_or_default()
    }

    /// Return the top-k hashes ordered by field DESC.
    pub fn top_k_desc(&self, coll: &str, field: &str, k: usize) -> Vec<String> {
        let key = (coll.to_string(), field.to_string());
        self.inner.get(&key).map(|idx| {
            idx.values().rev().flat_map(|v| v.iter().cloned()).take(k).collect()
        }).unwrap_or_default()
    }

    /// Check if a sorted index exists for a (coll, field) pair.
    pub fn has(&self, coll: &str, field: &str) -> bool {
        self.inner.contains_key(&(coll.to_string(), field.to_string()))
    }

    /// True if no sorted indexes have been registered yet.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn id_index_roundtrip() {
        let dir = tempdir().unwrap();
        let idx = IdIndex::new(dir.path()).unwrap();
        idx.set("blocks", "618000", "abcdef1234").unwrap();
        assert_eq!(idx.get("blocks", "618000"), Some("abcdef1234".to_string()));
    }

    #[test]
    fn encode_decode_id_bijective() {
        // Safe ids pass through unchanged (chainstate paths stay identical).
        for safe in ["618000", "utxo-000000042", "abc_DEF.123", "deadBEEF"] {
            assert_eq!(encode_id(safe), safe, "safe id must be identity");
            assert_eq!(decode_id(&encode_id(safe)), safe);
        }
        // FS-unsafe ids (link ids, paths) round-trip and contain no illegal
        // Windows filename chars once encoded.
        for weird in ["driver:d1|handles|trip:t1", "a/b\\c", "x<y>z?\"*", "100%done"] {
            let enc = encode_id(weird);
            assert!(
                !enc.chars().any(|c| matches!(c,
                    ':' | '|' | '/' | '\\' | '<' | '>' | '?' | '"' | '*')),
                "encoded leaf must be filesystem-safe: {}", enc);
            assert_eq!(decode_id(&enc), weird, "encode/decode must round-trip");
        }
    }

    #[test]
    fn id_index_fs_unsafe_id_survives_disk_roundtrip() {
        // Regression: link ids contain ':' and '|', illegal in Windows filenames.
        // They must persist to the on-disk id-index and read back after reopen.
        let dir = tempdir().unwrap();
        let weird = "driver:d1|handles|trip:t1";
        {
            let idx = IdIndex::new(dir.path()).unwrap();
            idx.set("__links__", weird, "deadbeefcafe").unwrap();
            idx.flush_write_buf(); // persist WAL → disk (encoded leaf filename)
        }
        // Cold reopen: nothing in the WAL, must come from disk.
        let idx2 = IdIndex::new(dir.path()).unwrap();
        assert_eq!(idx2.get("__links__", weird), Some("deadbeefcafe".to_string()),
                   "FS-unsafe id must be readable from disk after reopen");
        assert_eq!(idx2.list_ids("__links__"), vec![weird.to_string()],
                   "list_ids must decode the on-disk filename back to the id");
    }

    #[test]
    fn ordered_value_ordering() {
        use OrderedValue::*;
        assert!(Null < Bool(false));
        assert!(Bool(false) < Bool(true));
        assert!(Bool(true) < Number(0.0));
        assert!(Number(1.0) < Number(2.0));
        assert!(Number(2.0) < Str("a".to_string()));
        assert!(Str("a".to_string()) < Str("b".to_string()));
    }

    #[test]
    fn sorted_index_top_k() {
        let idx = SortedIndexes::new();
        idx.ensure("blocks", "height");
        idx.insert("blocks", "height", &serde_json::json!(3), "hash3");
        idx.insert("blocks", "height", &serde_json::json!(1), "hash1");
        idx.insert("blocks", "height", &serde_json::json!(2), "hash2");
        let asc = idx.top_k_asc("blocks", "height", 2);
        assert_eq!(asc, vec!["hash1", "hash2"]);
        let desc = idx.top_k_desc("blocks", "height", 2);
        assert_eq!(desc, vec!["hash3", "hash2"]);
    }
}
