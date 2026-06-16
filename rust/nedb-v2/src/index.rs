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
use anyhow::{Context, Result};
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

/// Per-document ID index — atomic file-per-doc.
pub struct IdIndex {
    root: PathBuf,
}

impl IdIndex {
    pub fn new(db_root: &Path) -> Result<Self> {
        let root = db_root.join("indexes");
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn path(&self, coll: &str, id: &str) -> PathBuf {
        self.root.join(coll).join("id").join(id)
    }

    /// Get the current object hash for a document.
    pub fn get(&self, coll: &str, id: &str) -> Option<String> {
        let content = fs::read_to_string(self.path(coll, id)).ok()?;
        let h = content.trim().to_string();
        if h.is_empty() { None } else { Some(h) }
    }

    /// Set the current object hash for a document (atomic).
    pub fn set(&self, coll: &str, id: &str, hash: &str) -> Result<()> {
        let path = self.path(coll, id);
        fs::create_dir_all(path.parent().unwrap())?;
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, hash)?;
        fs::rename(&tmp, &path).context("atomic id index update")?;
        Ok(())
    }

    /// List all doc IDs in a collection.
    pub fn list_ids(&self, coll: &str) -> Vec<String> {
        let dir = self.root.join(coll).join("id");
        fs::read_dir(&dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                if name.ends_with(".tmp") { return None; }
                Some(name)
            })
            .collect()
    }

    /// Remove the id index entry for a document (tombstone / delete).
    pub fn remove(&self, coll: &str, id: &str) -> Result<()> {
        let path = self.path(coll, id);
        if path.exists() {
            fs::remove_file(&path).context("remove id index entry")?;
        }
        Ok(())
    }

    /// List all known collections.
    pub fn collections(&self) -> Vec<String> {
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
