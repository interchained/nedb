//! Main DAG database — coordinates ObjectStore, IdIndex, SortedIndexes, GraphStore.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use anyhow::Result;
use serde_json::Value;

use crate::store::{Dek, Node, ObjectStore};
use crate::index::{IdIndex, OrderedValue, SortedIndexes};
use crate::graph::GraphStore;
use crate::migrate;

pub struct Db {
    pub objects:        ObjectStore,
    pub id_index:       IdIndex,
    pub sorted_indexes: SortedIndexes,
    pub graph:          GraphStore,
    pub root:           PathBuf,
    pub seq:            AtomicU64,
}

impl Db {
    /// Open (or create) a database. Runs v1→v2 migration automatically if log.aof is present.
    pub fn open(db_root: &Path, dek: Option<Dek>) -> Result<Self> {
        std::fs::create_dir_all(db_root)?;

        let objects        = ObjectStore::new(db_root, dek.clone())?;
        let id_index       = IdIndex::new(db_root)?;
        let sorted_indexes = SortedIndexes::new();
        let graph          = GraphStore::new(db_root)?;

        let mut db = Self {
            objects,
            id_index,
            sorted_indexes,
            graph,
            root: db_root.to_path_buf(),
            seq:  AtomicU64::new(0),
        };

        // Auto-migrate v1 → v2 if needed (pass DEK so encrypted AOFs convert correctly)
        migrate::migrate_if_needed(
            db_root,
            &db.objects,
            &db.id_index,
            &db.sorted_indexes,
            &db.graph,
            dek.as_ref(),
        )?;

        // Rebuild sorted indexes + find max seq from existing objects
        db.rebuild_from_objects()?;

        Ok(db)
    }

    /// Rebuild in-memory sorted indexes and max seq from the object store.
    /// This is O(n_objects) but fully parallel via Rayon.
    fn rebuild_from_objects(&mut self) -> Result<()> {
        use rayon::prelude::*;

        let hashes: Vec<String> = self.objects.all_hashes().collect();
        let nodes: Vec<Node> = hashes.par_iter()
            .filter_map(|h| self.objects.read(h).ok())
            .collect();

        let max_seq = nodes.iter().map(|n| n.seq).max().unwrap_or(0);
        self.seq.store(max_seq + 1, Ordering::SeqCst);

        for node in &nodes {
            if let Value::Object(ref obj) = node.data {
                for (field, value) in obj {
                    if self.sorted_indexes.has(&node.coll, field) {
                        self.sorted_indexes.insert(&node.coll, field, value, &node.hash);
                    }
                }
            }
        }

        Ok(())
    }

    /// Write a document. Returns the new node with its content hash set.
    pub fn put(
        &self,
        coll: &str,
        id: &str,
        data: Value,
        caused_by: Vec<String>,
        valid_from: Option<String>,
        valid_to:   Option<String>,
    ) -> Result<Node> {
        let seq  = self.seq.fetch_add(1, Ordering::SeqCst);
        let prev = self.id_index.get(coll, id);

        // Remove old node from sorted indexes (it's being superseded)
        if let Some(old_hash) = &prev {
            if let Ok(old_node) = self.objects.read(old_hash) {
                if let Value::Object(ref obj) = old_node.data {
                    for (field, value) in obj {
                        self.sorted_indexes.remove(coll, field, value, old_hash);
                    }
                }
            }
        }

        let mut node = Node {
            id:         id.to_string(),
            coll:       coll.to_string(),
            seq,
            data:       data.clone(),
            prev,
            caused_by:  caused_by.clone(),
            ts:         now(),
            valid_from,
            valid_to,
            hash:       String::new(),
        };

        // Write to object store (atomic, content-addressed)
        let hash = self.objects.write(&mut node)?;

        // Update id index (atomic file)
        self.id_index.set(coll, id, &hash)?;

        // Update sorted indexes
        if let Value::Object(ref obj) = data {
            for (field, value) in obj {
                if self.sorted_indexes.has(coll, field) {
                    self.sorted_indexes.insert(coll, field, value, &hash);
                }
            }
        }

        // Write causal graph edges
        for cause in &caused_by {
            self.graph.add_edge(&hash, "caused_by", cause)?;
            self.graph.add_edge(cause, "caused_by_rev", &hash)?;
        }

        Ok(node)
    }

    /// Delete a document — writes a tombstone node and removes the id from the index.
    /// The object history is preserved in the DAG; only the live id pointer is cleared.
    pub fn delete(&self, coll: &str, id: &str) -> Result<bool> {
        let prev = match self.id_index.get(coll, id) {
            None => return Ok(false),   // already gone
            Some(h) => h,
        };
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);
        let mut tombstone = Node {
            id:         format!("_del_{}", id),
            coll:       coll.to_string(),
            seq,
            data:       serde_json::json!({"_deleted": id, "_prev": prev}),
            prev:       Some(prev),
            caused_by:  vec![],
            ts:         now(),
            valid_from: None,
            valid_to:   None,
            hash:       String::new(),
        };
        self.objects.write(&mut tombstone)?;
        // Remove the live id pointer — doc is now invisible to queries and list()
        self.id_index.remove(coll, id)?;
        Ok(true)
    }

    /// Get the current version of a document by id.
    pub fn get(&self, coll: &str, id: &str) -> Option<Node> {
        let hash = self.id_index.get(coll, id)?;
        self.objects.read(&hash).ok()
    }

    /// Get a specific version of a document by object hash.
    pub fn get_by_hash(&self, hash: &str) -> Option<Node> {
        self.objects.read(hash).ok()
    }

    /// Get a document AS OF a specific sequence number.
    /// Walks the version chain (prev links) backward until seq <= target.
    pub fn get_as_of(&self, coll: &str, id: &str, target_seq: u64) -> Option<Node> {
        let hash = self.id_index.get(coll, id)?;
        let mut current = self.objects.read(&hash).ok()?;
        loop {
            if current.seq <= target_seq {
                return Some(current);
            }
            let prev_hash = current.prev.as_deref()?;
            current = self.objects.read(prev_hash).ok()?;
        }
    }

    /// List all documents in a collection, returning current versions.
    pub fn list(&self, coll: &str) -> Vec<Node> {
        self.id_index
            .list_ids(coll)
            .into_iter()
            .filter_map(|id| self.get(coll, &id))
            .collect()
    }

    /// ORDER BY field ASC LIMIT n — uses sorted index if available, else falls back to full scan.
    pub fn order_by_asc(&self, coll: &str, field: &str, limit: usize) -> Vec<Node> {
        if self.sorted_indexes.has(coll, field) {
            self.sorted_indexes
                .top_k_asc(coll, field, limit)
                .into_iter()
                .filter_map(|h| self.objects.read(&h).ok())
                .collect()
        } else {
            let mut docs = self.list(coll);
            docs.sort_by(|a, b| {
                let av = a.data.get(field).map(OrderedValue::from).unwrap_or(OrderedValue::Null);
                let bv = b.data.get(field).map(OrderedValue::from).unwrap_or(OrderedValue::Null);
                av.cmp(&bv)
            });
            docs.truncate(limit);
            docs
        }
    }

    /// ORDER BY field DESC LIMIT n
    pub fn order_by_desc(&self, coll: &str, field: &str, limit: usize) -> Vec<Node> {
        if self.sorted_indexes.has(coll, field) {
            self.sorted_indexes
                .top_k_desc(coll, field, limit)
                .into_iter()
                .filter_map(|h| self.objects.read(&h).ok())
                .collect()
        } else {
            let mut docs = self.list(coll);
            docs.sort_by(|a, b| {
                let av = a.data.get(field).map(OrderedValue::from).unwrap_or(OrderedValue::Null);
                let bv = b.data.get(field).map(OrderedValue::from).unwrap_or(OrderedValue::Null);
                bv.cmp(&av)
            });
            docs.truncate(limit);
            docs
        }
    }

    /// TRACE caused_by — walk causal graph from a node.
    pub fn trace(&self, hash: &str, reverse: bool, limit: usize) -> Vec<Node> {
        self.graph
            .trace(hash, "caused_by", reverse, limit)
            .into_iter()
            .filter_map(|h| self.objects.read(&h).ok())
            .collect()
    }

    /// Verify tamper-evidence of all objects.
    pub fn verify(&self) -> (usize, Vec<String>) {
        self.objects.verify_all()
    }

    /// Create a sorted index for a (coll, field) pair.
    pub fn create_sorted_index(&self, coll: &str, field: &str) {
        self.sorted_indexes.ensure(coll, field);
        // Backfill from existing objects
        for id in self.id_index.list_ids(coll) {
            if let Some(node) = self.get(coll, &id) {
                if let Value::Object(ref obj) = node.data {
                    if let Some(value) = obj.get(field) {
                        self.sorted_indexes.insert(coll, field, value, &node.hash);
                    }
                }
            }
        }
    }
}

fn now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn put_and_get() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.put(
            "blocks", "618000",
            serde_json::json!({"height": 618000, "hash": "0000abc"}),
            vec![], None, None,
        ).unwrap();
        let node = db.get("blocks", "618000").unwrap();
        assert_eq!(node.id, "618000");
        assert_eq!(node.data["height"], 618000);
    }

    #[test]
    fn order_by_with_sorted_index() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.create_sorted_index("blocks", "height");
        for h in [3u64, 1, 5, 2, 4] {
            db.put("blocks", &h.to_string(),
                serde_json::json!({"height": h}),
                vec![], None, None).unwrap();
        }
        let asc = db.order_by_asc("blocks", "height", 3);
        let heights: Vec<u64> = asc.iter()
            .filter_map(|n| n.data["height"].as_u64())
            .collect();
        assert_eq!(heights, vec![1, 2, 3]);
    }

    #[test]
    fn causal_trace() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        let a = db.put("ops", "a", serde_json::json!({"op": "create"}), vec![], None, None).unwrap();
        let b = db.put("ops", "b", serde_json::json!({"op": "transfer"}), vec![a.hash.clone()], None, None).unwrap();
        let c = db.put("ops", "c", serde_json::json!({"op": "burn"}), vec![b.hash.clone()], None, None).unwrap();

        let trace = db.trace(&c.hash, false, 10);
        assert_eq!(trace.len(), 3);  // c → b → a
    }

    #[test]
    fn as_of() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        let v1 = db.put("docs", "x", serde_json::json!({"v": 1}), vec![], None, None).unwrap();
        let _v2 = db.put("docs", "x", serde_json::json!({"v": 2}), vec![], None, None).unwrap();

        let at_v1 = db.get_as_of("docs", "x", v1.seq).unwrap();
        assert_eq!(at_v1.data["v"], 1);
        let current = db.get("docs", "x").unwrap();
        assert_eq!(current.data["v"], 2);
    }
}
