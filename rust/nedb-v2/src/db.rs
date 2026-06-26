//! Main DAG database — coordinates ObjectStore, IdIndex, SortedIndexes, GraphStore.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use anyhow::Result;
use dashmap::DashMap;
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
    /// Dirty flag — set true when head changes, cleared after manifest flush.
    /// Decouples flush_manifest from the hot write path so concurrent writes
    /// don't serialise on 2× file I/O per PUT.
    manifest_dirty:     Arc<AtomicBool>,
    pub seq:            AtomicU64,
    /// Cached Merkle head — updated incrementally on every write (O(1)).
    head:               RwLock<String>,
    /// True once startup is fully ready (MANIFEST loaded or cold scan complete).
    /// Warm starts set this true before returning from open().
    /// Cold starts set this true in the background thread when scan completes.
    /// Writes are held with 503 until this is true; reads always proceed.
    pub startup_ready:  Arc<AtomicBool>,
    /// Seq → hash lookup for v1 compatibility. Populated by put(), put_batch(),
    /// and the cold-scan background pass. Only covers nodes from the current
    /// process session + cold-scan; older seqs not in this map cannot be resolved.
    seq_index:          Arc<DashMap<u64, String>>,
}

impl Db {
    /// Create a pure in-memory database — no disk I/O, no migration, instant startup.
    /// Perfect for tests, hot-cache layers, and ephemeral sessions.
    /// All data is lost when the Db is dropped.
    pub fn in_memory() -> Self {
        Self {
            objects:        ObjectStore::in_memory(),
            id_index:       IdIndex::in_memory(),
            sorted_indexes: SortedIndexes::new(),
            graph:          GraphStore::in_memory(),
            root:           std::path::PathBuf::from(":memory:"),
            seq:            AtomicU64::new(0),
            head:           RwLock::new(String::new()),
            startup_ready:  Arc::new(AtomicBool::new(true)),  // always ready
            manifest_dirty: Arc::new(AtomicBool::new(false)),
            seq_index:      Arc::new(DashMap::new()),
        }
    }

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
            startup_ready:  Arc::new(AtomicBool::new(false)),
            manifest_dirty: Arc::new(AtomicBool::new(false)),
            seq_index:      Arc::new(DashMap::new()),
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
                // Self-heal: MANIFEST with an empty or short head is corrupt/stale.
                // Fall through to cold scan so the head is rebuilt correctly from objects.
                if m.head.len() < 8 {
                    eprintln!("  [nedbd] MANIFEST head invalid (len={}), self-healing via cold scan", m.head.len());
                } else {
                    self.seq.store(m.seq, Ordering::SeqCst); // m.seq is already the next-to-assign counter
                    *self.head.write() = m.head.clone();
                    self.startup_ready.store(true, Ordering::SeqCst);
                    println!("  [nedbd] warm start — seq={} head={}...", m.seq, &m.head[..8]);
                    return Ok(());
                }
            } else {
                eprintln!("  [nedbd] MANIFEST corrupt or missing, falling back to cold scan");
            }
        }

        // Cold path: mark as not ready, return immediately.
        // The actual background scan is started by Db::start_cold_scan(arc)
        // which is called from Manager::open_all() AFTER Arc::new(db) — when
        // the Db is heap-allocated and its field addresses are permanently stable.
        // Capturing field addresses here would cause UB: Db moves on return.
        println!("  [nedbd] cold start — background scan will start after heap allocation");
        Ok(())
    }

    /// Call this from Manager::open_all() after Arc::new(db).
    /// Spawns the cold scan background thread with stable heap addresses.
    /// No-op if startup is already complete (warm start).
    pub fn start_cold_scan(self_arc: Arc<Self>) {
        if self_arc.startup_ready.load(Ordering::SeqCst) {
            return; // warm start — already ready
        }
        // Fast path: if the database is empty (new or just created), skip the
        // background thread entirely. No objects to scan = instant startup.
        if self_arc.objects.all_hashes().next().is_none() {
            self_arc.startup_ready.store(true, Ordering::SeqCst);
            return;
        }
        println!("  [nedbd] cold start — background scan starting, server accepting reads now");
        std::thread::spawn(move || {
            let db = self_arc;
            cold_scan_background_arc(db);
        });
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
        self.seq_index.insert(seq, hash.clone());

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

    /// Batch put: write N documents in parallel, preserving monotonic seq ordering.
    /// Pre-allocates N seq numbers atomically, then parallelises object writes and
    /// id-index updates via Rayon. Each op is independent — safe to parallelise.
    /// Returns nodes in input order with assigned seq numbers.
    pub fn put_batch(
        &self,
        ops: Vec<(String, String, Value, Vec<String>, Option<String>, Option<String>)>,
        // (coll, id, data, caused_by, valid_from, valid_to)
    ) -> Result<Vec<Node>> {
        use rayon::prelude::*;

        if ops.is_empty() { return Ok(vec![]); }
        let n = ops.len() as u64;

        // Pre-allocate N consecutive seq numbers — preserves ordering under concurrency
        let base_seq = self.seq.fetch_add(n, Ordering::SeqCst);
        let ts = now();

        // Build nodes with assigned seq numbers
        let mut nodes: Vec<Node> = ops.into_iter().enumerate().map(|(i, (coll, id, data, caused_by, valid_from, valid_to))| {
            let prev = self.id_index.get(&coll, &id);
            Node {
                id, coll, seq: base_seq + i as u64,
                data, prev, caused_by,
                ts, valid_from, valid_to,
                hash: String::new(),
            }
        }).collect();

        // Parallel object writes (content-addressed, idempotent, safe to parallelise)
        let write_errors: Vec<anyhow::Error> = nodes.par_iter_mut()
            .filter_map(|node| self.objects.write(node).err())
            .collect();
        if let Some(e) = write_errors.into_iter().next() { return Err(e); }

        // Parallel id-index updates
        let index_errors: Vec<anyhow::Error> = nodes.par_iter()
            .filter_map(|node| self.id_index.set(&node.coll, &node.id, &node.hash).err())
            .collect();
        if let Some(e) = index_errors.into_iter().next() { return Err(e); }

        // Sorted indexes + causal graph (sequential — small overhead, usually no indexes)
        for node in &nodes {
            self.seq_index.insert(node.seq, node.hash.clone());
            if let Value::Object(ref obj) = node.data {
                for (field, value) in obj {
                    if self.sorted_indexes.has(&node.coll, field) {
                        self.sorted_indexes.insert(&node.coll, field, value, &node.hash);
                    }
                }
            }
            for cause in &node.caused_by {
                self.graph.add_edge(&node.hash, "caused_by", cause).ok();
                self.graph.add_edge(cause, "caused_by_rev", &node.hash).ok();
            }
        }

        // Single Merkle head update for the whole batch (chain all hashes)
        for node in &nodes {
            self.update_head(node.seq, &node.hash);
        }

        Ok(nodes)
    }

    /// Update the running Merkle head with a new write. O(1), lock-free on the flush path.
    /// Sets dirty flag — the background ticker calls flush_manifest periodically.
    /// This removes 2× file I/O ops from the hot write path, unblocking concurrent writes.
    fn update_head(&self, seq: u64, new_hash: &str) {
        use blake2::{Blake2b512, Digest};
        let prev = self.head.read().clone();
        let mut h = Blake2b512::new();
        h.update(prev.as_bytes());
        h.update(seq.to_le_bytes());
        h.update(new_hash.as_bytes());
        *self.head.write() = hex::encode(&h.finalize()[..32]);
        // Mark dirty — background ticker will flush to MANIFEST (no I/O on write path)
        self.manifest_dirty.store(true, Ordering::Release);
    }

    /// Flush both the id-index WAL and MANIFEST. Used on graceful shutdown.
    pub fn flush_all(&self) {
        self.id_index.flush_write_buf();
        // v3: fsync the active segment (no-op for loose/in-memory stores).
        // One durability point per batch instead of one fsync per object.
        if let Err(e) = self.objects.sync() {
            eprintln!("nedb: segment sync failed: {}", e);
        }
        self.flush_manifest();
    }

    /// Compact the v3 packed object store: keep the CURRENT version of every
    /// document (from the id-index) and reclaim everything else. No-op unless
    /// running with the v3 segment substrate (`--dag-v3` / NEDB_DAG_V3).
    ///
    /// This is a PRUNING operation: superseded/historical object versions are
    /// dropped, so AS OF / TRACE over pruned versions is discarded — that is
    /// what reclaims the space. Flushes first so all data is durable on disk
    /// before the old segments are deleted.
    pub fn compact(&self) -> Result<crate::segment::CompactStats> {
        self.flush_all();
        let mut live: std::collections::HashSet<String> = std::collections::HashSet::new();
        for coll in self.id_index.collections() {
            for id in self.id_index.list_ids(&coll) {
                if let Some(h) = self.id_index.get(&coll, &id) {
                    live.insert(h);
                }
            }
        }
        self.objects.compact(&live)
    }

    /// Flush MANIFEST to disk if dirty. No-op for in-memory databases.
    pub fn flush_manifest_if_dirty(&self) {
        if self.root == std::path::PathBuf::from(":memory:") { return; }
        if self.manifest_dirty.compare_exchange(
            true, false, Ordering::AcqRel, Ordering::Relaxed
        ).is_ok() {
            self.flush_manifest();
        }
    }

    /// Atomically persist current seq+head to MANIFEST. No-op for in-memory databases.
    pub fn flush_manifest(&self) {
        if self.root == std::path::PathBuf::from(":memory:") { return; }
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

    /// Start a background thread that flushes both the id-index WAL and MANIFEST
    /// every `interval_ms` milliseconds.
    /// Call this after Arc::new(db) — the Arc keeps Db alive for the thread's lifetime.
    pub fn start_manifest_ticker(self_arc: Arc<Self>, interval_ms: u64) {
        let db = self_arc;
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_millis(interval_ms));
                // Flush id-index WAL to disk (parallel Rayon writes)
                db.id_index.flush_write_buf();
                // Then flush MANIFEST
                db.flush_manifest_if_dirty();
            }
        });
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

    /// Resolve a sequence number to its content hash (v1 compatibility).
    /// Only covers nodes written in the current process session + cold-scan nodes.
    pub fn get_hash_by_seq(&self, seq: u64) -> Option<String> {
        self.seq_index.get(&seq).map(|r| r.clone())
    }

    /// Add an explicit named relation edge between two documents.
    /// Add an explicit named relation between two "coll:id" nodes.
    /// Relations stored as __links__ documents — NQL-queryable, time-travelable,
    /// consistent with the PyO3 binding which uses the same __links__ convention.
    pub fn link(&self, frm: &str, rel: &str, to: &str) -> Result<()> {
        let (frm_coll, frm_id) = frm.split_once(':')
            .ok_or_else(|| anyhow::anyhow!("link frm must be 'coll:id', got: {}", frm))?;
        let (to_coll, to_id) = to.split_once(':')
            .ok_or_else(|| anyhow::anyhow!("link to must be 'coll:id', got: {}", to))?;
        if self.id_index.get(frm_coll, frm_id).is_none() {
            anyhow::bail!("link: frm not found: {}", frm);
        }
        if self.id_index.get(to_coll, to_id).is_none() {
            anyhow::bail!("link: to not found: {}", to);
        }
        let link_id = format!("{}|{}|{}", frm, rel, to);
        let doc = serde_json::json!({"_from": frm, "_rel": rel, "_to": to});
        self.put("__links__", &link_id, doc, vec![], None, None)?;
        Ok(())
    }

    /// Remove a named relation (deletes the __links__ document).
    pub fn unlink(&self, frm: &str, rel: &str, to: &str) -> Result<bool> {
        let link_id = format!("{}|{}|{}", frm, rel, to);
        self.delete("__links__", &link_id)
    }

    /// Get neighbor nodes via a named relation.
    /// Queries __links__ — consistent with the PyO3 binding.
    pub fn neighbors(&self, frm: &str, rel: &str) -> Vec<Node> {
        self.id_index
            .list_ids("__links__")
            .into_iter()
            .filter_map(|id| self.get("__links__", &id))
            .filter(|node| {
                node.data.get("_from").and_then(|v| v.as_str()) == Some(frm)
                    && node.data.get("_rel").and_then(|v| v.as_str()) == Some(rel)
            })
            .filter_map(|node| {
                let to = node.data.get("_to")?.as_str()?;
                let (to_coll, to_id) = to.split_once(':')?;
                self.get(to_coll, to_id)
            })
            .collect()
    }
}

impl Drop for Db {
    /// Flush buffered state when the database is closed so a write-then-drop
    /// sequence is durable without an explicit `flush_all()`.
    ///
    /// `IdIndex::set` only stages updates in the in-memory WAL `write_buf`;
    /// disk persistence happens in `flush_write_buf()`, normally driven by the
    /// manifest ticker. A short-lived `Db` (a library user's `{ let db =
    /// Db::open(p)?; db.put(..)?; }` block, or a test) has no ticker, so without
    /// this its writes would be silently lost on reopen. Flushing on drop
    /// mirrors the flush-on-close contract of other embedded stores (sled,
    /// RocksDB).
    ///
    /// In production this is a harmless safety net, not the primary durability
    /// path: the manifest ticker thread holds an `Arc<Db>` for the process
    /// lifetime, so `Drop` only fires once every owning handle is gone. No-op
    /// for in-memory databases (`flush_all` short-circuits on `:memory:`).
    fn drop(&mut self) {
        self.flush_all();
    }
}

/// Background cold-scan worker. Takes Arc<Db> — safe, Db is on the heap.
fn cold_scan_background_arc(db: Arc<Db>) {
    use rayon::prelude::*;
    use blake2::{Blake2b512, Digest};

    let objects        = &db.objects;
    let head           = &db.head;
    let seq_atomic     = &db.seq;
    let sorted_indexes = &db.sorted_indexes;
    let root           = db.root.clone();
    let ready_flag     = Arc::clone(&db.startup_ready);

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
                eprint!("\r  [nedbd]   {:>3}%  {:>8} / {:>8}  ({:>8.0}/s  eta {:.0}s)   ",
                    pct, i, total, rate, eta);
            }
            objects.read(h).ok()
        })
        .collect();

    eprintln!("\r  [nedbd]   100%  {:>8} / {:>8}  ({:.1}s)                        ",
        total, total, t0.elapsed().as_secs_f32());

    let max_seq = nodes.iter().map(|n| n.seq).max().unwrap_or(0);
    seq_atomic.store(max_seq + 1, Ordering::SeqCst);

    for node in &nodes {
        db.seq_index.insert(node.seq, node.hash.clone());
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

#[cfg(test)]
mod tests_v2 {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn seq_index_populated_on_put() {
        let db = Db::in_memory();
        let a = db.put("item", "a", serde_json::json!({"x": 1}), vec![], None, None).unwrap();
        let b = db.put("item", "b", serde_json::json!({"x": 2}), vec![], None, None).unwrap();
        assert_eq!(db.get_hash_by_seq(a.seq), Some(a.hash.clone()));
        assert_eq!(db.get_hash_by_seq(b.seq), Some(b.hash.clone()));
        assert_eq!(db.get_hash_by_seq(9999), None);
    }

    #[test]
    fn seq_index_survives_batch() {
        let db = Db::in_memory();
        let nodes = db.put_batch(vec![
            ("item".into(), "x".into(), serde_json::json!({"v": 1}), vec![], None, None),
            ("item".into(), "y".into(), serde_json::json!({"v": 2}), vec![], None, None),
        ]).unwrap();
        for node in &nodes {
            assert_eq!(db.get_hash_by_seq(node.seq), Some(node.hash.clone()));
        }
    }

    #[test]
    fn link_and_neighbors() {
        let db = Db::in_memory();
        db.put("driver", "d1", serde_json::json!({"name": "Bob"}),   vec![], None, None).unwrap();
        db.put("driver", "d2", serde_json::json!({"name": "Carol"}), vec![], None, None).unwrap();
        db.put("trip",   "t1", serde_json::json!({"status": "req"}), vec![], None, None).unwrap();
        db.put("trip",   "t2", serde_json::json!({"status": "req"}), vec![], None, None).unwrap();

        db.link("driver:d1", "handles", "trip:t1").unwrap();
        db.link("driver:d1", "handles", "trip:t2").unwrap();
        db.link("driver:d2", "handles", "trip:t1").unwrap();

        let d1_trips = db.neighbors("driver:d1", "handles");
        assert_eq!(d1_trips.len(), 2);
        let ids: std::collections::HashSet<&str> = d1_trips.iter().map(|n| n.id.as_str()).collect();
        assert!(ids.contains("t1") && ids.contains("t2"));

        let d2_trips = db.neighbors("driver:d2", "handles");
        assert_eq!(d2_trips.len(), 1);
        assert_eq!(d2_trips[0].id, "t1");
    }

    #[test]
    fn link_stored_in_links_collection() {
        // Links are stored as __links__ documents, not as graph edges.
        // The __links__ collection is NQL-queryable and consistent with the PyO3 binding.
        let db = Db::in_memory();
        db.put("driver", "d1", serde_json::json!({"name": "Bob"}),   vec![], None, None).unwrap();
        db.put("trip",   "t1", serde_json::json!({"status": "req"}), vec![], None, None).unwrap();
        db.link("driver:d1", "handles", "trip:t1").unwrap();
        // Verify the __links__ document was created
        let link_doc = db.get("__links__", "driver:d1|handles|trip:t1");
        assert!(link_doc.is_some(), "__links__ doc should exist");
        let doc = link_doc.unwrap();
        assert_eq!(doc.data["_from"], "driver:d1");
        assert_eq!(doc.data["_rel"],  "handles");
        assert_eq!(doc.data["_to"],   "trip:t1");
        // neighbors() resolves to the target node
        let nb = db.neighbors("driver:d1", "handles");
        assert_eq!(nb.len(), 1);
        assert_eq!(nb[0].id, "t1");
    }

    #[test]
    fn link_missing_node_errors() {
        let db = Db::in_memory();
        db.put("driver", "d1", serde_json::json!({}), vec![], None, None).unwrap();
        assert!(db.link("driver:d1", "handles", "trip:ghost").is_err());
    }

    #[test]
    fn link_durable_survives_reopen() {
        let dir = tempdir().unwrap();
        {
            let db = Db::open(dir.path(), None).unwrap();
            db.put("driver", "d1", serde_json::json!({"name": "Bob"}),   vec![], None, None).unwrap();
            db.put("trip",   "t1", serde_json::json!({"status": "req"}), vec![], None, None).unwrap();
            db.link("driver:d1", "handles", "trip:t1").unwrap();
        }
        let db2 = Db::open(dir.path(), None).unwrap();
        db2.startup_ready.store(true, std::sync::atomic::Ordering::SeqCst);
        let trips = db2.neighbors("driver:d1", "handles");
        assert_eq!(trips.len(), 1);
        assert_eq!(trips[0].id, "t1");
    }
}
