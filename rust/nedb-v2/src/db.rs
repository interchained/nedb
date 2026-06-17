//! Main DAG database — coordinates ObjectStore, IdIndex, SortedIndexes, GraphStore.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use anyhow::Result;
use serde_json::Value;
use parking_lot::RwLock;

use crate::store::{Dek, Node, ObjectStore};
use crate::index::{IdIndex, OrderedValue, SortedIndexes};
use crate::graph::GraphStore;
use crate::migrate;

/// MANIFEST: cached {seq, head} written atomically after every write.
/// On startup, if MANIFEST exists and no sorted indexes need rebuilding,
/// startup is O(1) — just read this one file instead of scanning all objects.
#[derive(serde::Serialize, serde::Deserialize)]
struct Manifest {
    seq:  u64,
    head: String,
}

pub struct Db {
    pub objects:        ObjectStore,
    pub id_index:       IdIndex,
    pub sorted_indexes: SortedIndexes,
    pub graph:          GraphStore,
    pub root:           PathBuf,
    pub seq:            AtomicU64,
    /// Cached Merkle head — updated incrementally on every write (O(1)).
    head:               RwLock<String>,
    /// True once startup is fully ready (MANIFEST loaded or cold scan complete).
    /// Warm starts set this true before returning from open().
    /// Cold starts set this true in the background thread when scan completes.
    /// Writes are held with 503 until this is true; reads always proceed.
    pub startup_ready:  Arc<AtomicBool>,
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
            head: RwLock::new(String::new()),
            startup_ready: Arc::new(AtomicBool::new(false)),
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

        // Fast startup: load seq+head from MANIFEST if no sorted indexes need rebuilding.
        // Falls back to full object scan only when necessary (first open, or post-migration).
        db.startup_rebuild()?;

        Ok(db)
    }

    /// Smart startup:
    /// - Warm (MANIFEST exists): O(1) load → startup_ready = true immediately.
    /// - Cold (no MANIFEST): start server immediately, run scan in background thread.
    ///   Writes return 503 until scan completes; reads always proceed.
    fn startup_rebuild(&mut self) -> Result<()> {
        let manifest_path = self.root.join("MANIFEST");
        let needs_index_rebuild = !self.sorted_indexes.is_empty();

        // Warm path: MANIFEST + no sorted indexes to rebuild → instant start
        if manifest_path.exists() && !needs_index_rebuild {
            if let Some(m) = fs::read_to_string(&manifest_path)
                .ok()
                .and_then(|s| serde_json::from_str::<Manifest>(&s).ok())
            {
                self.seq.store(m.seq + 1, Ordering::SeqCst);
                *self.head.write() = m.head.clone();
                self.startup_ready.store(true, Ordering::SeqCst);
                println!("  [nedbd] warm start — seq={} head={}...", m.seq, &m.head[..8]);
                return Ok(());
            }
            eprintln!("  [nedbd] MANIFEST corrupt, falling back to cold scan");
        }

        // Cold path: spawn background thread, server starts immediately
        // Writes are gated on startup_ready until scan completes.
        let ready_flag  = Arc::clone(&self.startup_ready);
        let root        = self.root.clone();
        let objects_ref = unsafe {
            // SAFETY: Db outlives the background thread because Arc<Db> keeps it alive.
            // We pass a raw pointer to avoid cloning the ObjectStore.
            &self.objects as *const ObjectStore
        };
        let head_ref = unsafe { &self.head as *const RwLock<String> };
        let seq_ref  = unsafe { &self.seq  as *const AtomicU64 };
        let si_ref   = unsafe { &self.sorted_indexes as *const SortedIndexes };

        println!("  [nedbd] cold start — background scan starting, server accepting reads now");

        std::thread::spawn(move || {
            // SAFETY: Db is kept alive by Arc<Db> in Manager for the server's lifetime.
            let objects        = unsafe { &*objects_ref };
            let head           = unsafe { &*head_ref };
            let seq_atomic     = unsafe { &*seq_ref };
            let sorted_indexes = unsafe { &*si_ref };
            cold_scan_background(root, objects, head, seq_atomic, sorted_indexes, ready_flag);
        });

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

        // Update running Merkle head: O(1) chain, no full recompute.
        // new_head = BLAKE2b(prev_head || seq_bytes || new_object_hash)
        self.update_head(seq, &hash);

        Ok(node)
    }

    /// Update the running Merkle head with a new write. O(1).
    fn update_head(&self, seq: u64, new_hash: &str) {
        use blake2::{Blake2b512, Digest};
        let prev = self.head.read().clone();
        let mut h = Blake2b512::new();
        h.update(prev.as_bytes());
        h.update(seq.to_le_bytes());
        h.update(new_hash.as_bytes());
        *self.head.write() = hex::encode(&h.finalize()[..32]);
        // Persist MANIFEST so next startup is instant
        self.flush_manifest();
    }

    /// Atomically persist current seq+head to MANIFEST.
    fn flush_manifest(&self) {
        let seq  = self.seq.load(Ordering::SeqCst);
        let head = self.head.read().clone();
        let m = Manifest { seq, head };
        if let Ok(json) = serde_json::to_string(&m) {
            let path = self.root.join("MANIFEST");
            let tmp  = self.root.join("MANIFEST.tmp");
            let _ = fs::write(&tmp, &json);
            let _ = fs::rename(&tmp, &path);
        }
    }

    /// Return the current Merkle head string. O(1) — read from cache.
    pub fn head(&self) -> String {
        self.head.read().clone()
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
        let hash = self.objects.write(&mut tombstone)?;
        self.update_head(seq, &hash);
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

/// Background cold-scan worker. Called from a std::thread spawned by startup_rebuild().
/// Reads all objects, rebuilds seq/head/sorted-indexes, writes MANIFEST, sets ready flag.
fn cold_scan_background(
    root:           PathBuf,
    objects:        *const ObjectStore,
    head:           *const RwLock<String>,
    seq_atomic:     *const AtomicU64,
    sorted_indexes: *const SortedIndexes,
    ready_flag:     Arc<AtomicBool>,
) {
    use rayon::prelude::*;
    use blake2::{Blake2b512, Digest};

    // SAFETY: caller guarantees Db outlives this thread (Arc<Db> in Manager).
    let objects        = unsafe { &*objects };
    let head           = unsafe { &*head };
    let seq_atomic     = unsafe { &*seq_atomic };
    let sorted_indexes = unsafe { &*sorted_indexes };

    let hashes: Vec<String> = objects.all_hashes().collect();
    let total = hashes.len();

    if total == 0 {
        ready_flag.store(true, Ordering::SeqCst);
        return;
    }

    println!("  [nedbd] background scan — {} objects...", total);
    let t0 = std::time::Instant::now();
    let step = (total / 10).max(1000);

    let nodes: Vec<Node> = hashes.par_iter()
        .enumerate()
        .filter_map(|(i, h)| {
            if i > 0 && i % step == 0 {
                let pct     = i * 100 / total;
                let elapsed = t0.elapsed().as_secs_f32();
                let rate    = i as f32 / elapsed;
                let eta     = (total - i) as f32 / rate;
                eprint!("\r  [nedbd]   {:>3}%  {:>8,} / {:>8,}  ({:>8,.0}/s  eta {:.0}s)   ",
                    pct, i, total, rate, eta);
            }
            objects.read(h).ok()
        })
        .collect();

    eprintln!("\r  [nedbd]   100%  {:>8,} / {:>8,}  ({:.1}s)                        ",
        total, total, t0.elapsed().as_secs_f32());

    let max_seq = nodes.iter().map(|n| n.seq).max().unwrap_or(0);
    seq_atomic.store(max_seq + 1, Ordering::SeqCst);

    for node in &nodes {
        if let Value::Object(ref obj) = node.data {
            for (field, value) in obj {
                if sorted_indexes.has(&node.coll, field) {
                    sorted_indexes.insert(&node.coll, field, value, &node.hash);
                }
            }
        }
    }

    // Compute Merkle head from sorted hashes
    let mut sorted_hashes = hashes;
    sorted_hashes.sort();
    let mut h = Blake2b512::new();
    h.update(max_seq.to_le_bytes());
    for hash_str in &sorted_hashes {
        h.update(hash_str.as_bytes());
    }
    let new_head = hex::encode(&h.finalize()[..32]);
    *head.write() = new_head.clone();

    // Write MANIFEST atomically
    let m     = Manifest { seq: max_seq, head: new_head };
    let json  = serde_json::to_string(&m).unwrap_or_default();
    let path  = root.join("MANIFEST");
    let tmp   = root.join("MANIFEST.tmp");
    let _ = fs::write(&tmp, &json);
    let _ = fs::rename(&tmp, &path);

    // Signal server: writes can now proceed
    ready_flag.store(true, Ordering::SeqCst);
    println!("  [nedbd] background scan complete — seq={} objects={} MANIFEST written", max_seq, total);
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
