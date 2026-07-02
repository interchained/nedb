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
    /// Object hash of the highest-seq node at flush time. Lets `tip()` resolve the
    /// last write O(1) on a warm boot — before any scan repopulates the in-memory
    /// seq index. `#[serde(default)]` so pre-2.5.43 MANIFESTs (no field) still parse.
    #[serde(default)]
    tip_hash: String,
    /// Per-collection tip: `coll -> object hash of the highest-seq node in that
    /// collection`. Lets `tip_collection()` resolve O(1) on a warm boot, same
    /// contract as `tip_hash` for the global head. `#[serde(default)]` so
    /// pre-this-field MANIFESTs still parse (empty map — self-heals on next write
    /// or cold scan).
    #[serde(default)]
    coll_tips: std::collections::HashMap<String, String>,
}

/// Default cap for `since()` when the caller passes `limit == 0`. Bounds the
/// engine primitive itself so a stale/offline consumer can never force an
/// unbounded materialization — the safety lives in the core, not the HTTP layer.
pub const DEFAULT_SINCE_LIMIT: usize = 10_000;

/// One page of the changefeed returned by `since()`. The replication contract:
/// apply `nodes` in ascending seq order, advance your cursor to `to_seq`, and keep
/// paging while `has_more` is true; then attach to the live `subscribe` edge.
/// `head_seq` tells the consumer how far the log currently extends (how far behind
/// it is).
#[derive(Debug, Clone, serde::Serialize)]
pub struct SinceBatch {
    /// Writes in (`from_seq`, `to_seq`], ascending by seq.
    pub nodes:    Vec<Node>,
    /// The exclusive cursor this page started from (echoes the request).
    pub from_seq: u64,
    /// Seq of the last node in this page — the consumer's next cursor.
    pub to_seq:   u64,
    /// Current head seq of the log (latest committed write).
    pub head_seq: u64,
    /// True when more writes remain past `to_seq` (the page hit `limit`).
    pub has_more: bool,
}

/// Replication readiness snapshot. `scan_complete` is the correctness gate: until
/// the cold-scan finishes rebuilding the seq index, an old cursor passed to
/// `since()` can return a PARTIAL page and look (wrongly) like "caught up". A
/// correctness-critical consumer MUST wait for `scan_complete == true` before
/// trusting historical catch-up. `indexed_seq_min/max` report the currently
/// resolvable seq range; `tip_seq` is the log head.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ScanStatus {
    /// Cold-scan finished — historical seqs fully resolvable; catch-up is safe.
    pub scan_complete:   bool,
    /// Head seq of the log (latest committed write).
    pub tip_seq:         u64,
    /// Lowest seq currently in the seq index (0 if empty).
    pub indexed_seq_min: u64,
    /// Highest seq currently in the seq index.
    pub indexed_seq_max: u64,
    /// Number of seqs currently resolvable via the index.
    pub indexed_count:   usize,
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
    /// `(seq, object hash)` of the most recent write (highest seq). Mirrors `head`
    /// but holds the tip's content hash, so `tip()` can resolve the last node O(1)
    /// on a warm boot when the in-memory `seq_index` is still cold. The seq rides
    /// along so concurrent writers can settle the tip by HIGHEST SEQ rather than
    /// arrival order (a slow older put must never clobber a newer tip). Only the
    /// hash is persisted in MANIFEST — format unchanged.
    tip_hash:           RwLock<(u64, String)>,
    /// Per-collection tip: `coll -> (seq, object hash)` of the highest-seq node in
    /// that collection. Kept current on every write (`update_head`, seq-guarded),
    /// restored from MANIFEST on warm boot, rebuilt by the cold scan — so
    /// `tip_collection()` is O(1) and durable across restarts in every startup
    /// regime, by construction.
    coll_tip_hash:      Arc<DashMap<String, (u64, String)>>,
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
            tip_hash:       RwLock::new((0, String::new())),
            coll_tip_hash:  Arc::new(DashMap::new()),
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
            tip_hash: RwLock::new((0, String::new())),
            coll_tip_hash: Arc::new(DashMap::new()),
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
                } else if m.tip_hash.is_empty() {
                    // Pre-2.5.43 MANIFEST (no persisted tip). Cold-scan once to rebuild
                    // the seq index and rewrite MANIFEST with tip_hash — warm + tip()-
                    // durable on every boot thereafter.
                    eprintln!("  [nedbd] MANIFEST predates durable tip() — cold scan once to upgrade");
                } else {
                    self.seq.store(m.seq, Ordering::SeqCst); // m.seq is already the next-to-assign counter
                    *self.head.write() = m.head.clone();
                    // The tip's seq is the last ASSIGNED seq (m.seq is next-to-assign).
                    *self.tip_hash.write() = (m.seq.saturating_sub(1), m.tip_hash.clone());
                    for (coll, hash) in &m.coll_tips {
                        // Per-coll seqs aren't persisted (MANIFEST format unchanged);
                        // seed 0 — every future write has seq >= m.seq > 0 and wins,
                        // and nothing older than the persisted tip can ever arrive
                        // because the seq counter resumes at m.seq.
                        self.coll_tip_hash.insert(coll.clone(), (0, hash.clone()));
                    }
                    self.startup_ready.store(true, Ordering::SeqCst);
                    println!("  [nedbd] warm start — seq={} head={}... tip={}...",
                        m.seq, &m.head[..8], &m.tip_hash[..8.min(m.tip_hash.len())]);
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

        // Remove old node from sorted indexes (it's being superseded).
        // Skip the old-object disk read entirely when no sorted index exists —
        // the read (open + BLAKE2b verify + optional AES-GCM decrypt + JSON
        // parse) was pure waste in the common unindexed case, ~2x read
        // amplification on every update (the itcd chainstate shape).
        if !self.sorted_indexes.is_empty() {
            if let Some(old_hash) = &prev {
                if let Ok(old_node) = self.objects.read(old_hash) {
                    if let Value::Object(ref obj) = old_node.data {
                        for (field, value) in obj {
                            self.sorted_indexes.remove(coll, field, value, old_hash);
                        }
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
        self.update_head(coll, seq, &hash);

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
        let index_live = !self.sorted_indexes.is_empty();
        let mut nodes: Vec<Node> = ops.into_iter().enumerate().map(|(i, (coll, id, data, caused_by, valid_from, valid_to))| {
            let prev = self.id_index.get(&coll, &id);
            // Parity with put(): drop the superseded version's values from any
            // sorted indexes, so top-k never returns stale hashes after a batch
            // update. Without this, batch updates left the old version's index
            // entries in place — ORDER BY surfaced superseded rows alongside
            // current ones. Only pay the old-object read when an index exists.
            if index_live {
                if let Some(old_hash) = &prev {
                    if let Ok(old_node) = self.objects.read(old_hash) {
                        if let Value::Object(ref obj) = old_node.data {
                            for (field, value) in obj {
                                self.sorted_indexes.remove(&coll, field, value, old_hash);
                            }
                        }
                    }
                }
            }
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
            self.update_head(&node.coll, node.seq, &node.hash);
        }

        Ok(nodes)
    }

    /// Update the running Merkle head with a new write. O(1); no file I/O — the
    /// background ticker flushes MANIFEST.
    ///
    /// Concurrency contract (this function is reached by parallel `put()`s —
    /// the server runs puts on blocking threads):
    /// - The head chain is extended under ONE write lock held across the whole
    ///   read-modify-write. The old read-then-write shape let two concurrent
    ///   writers both read the same prev head; one contribution was silently
    ///   dropped from the chain — a corrupted tamper-evidence primitive. The
    ///   chain is arrival-ordered under concurrency (a seq-ordered canonical
    ///   head is tracked as follow-up work); what this lock guarantees is that
    ///   EVERY write is committed into the chain exactly once.
    /// - Tip pointers settle by HIGHEST SEQ, not arrival order: concurrent
    ///   puts can reach here out of seq order, and "last call wins" could
    ///   persist a stale tip into MANIFEST for the next warm boot.
    fn update_head(&self, coll: &str, seq: u64, new_hash: &str) {
        use blake2::{Blake2b512, Digest};
        {
            let mut head = self.head.write();
            let mut h = Blake2b512::new();
            h.update(head.as_bytes());
            h.update(seq.to_le_bytes());
            h.update(new_hash.as_bytes());
            *head = hex::encode(&h.finalize()[..32]);
        }
        {
            let mut tip = self.tip_hash.write();
            if seq >= tip.0 {
                *tip = (seq, new_hash.to_string());
            }
        }
        self.coll_tip_hash
            .entry(coll.to_string())
            .and_modify(|t| {
                if seq >= t.0 {
                    *t = (seq, new_hash.to_string());
                }
            })
            .or_insert_with(|| (seq, new_hash.to_string()));
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
        let tip_hash = self.tip_hash.read().1.clone();
        let coll_tips: std::collections::HashMap<String, String> = self.coll_tip_hash
            .iter()
            .map(|kv| (kv.key().clone(), kv.value().1.clone()))
            .collect();
        let m = Manifest { seq, head, tip_hash, coll_tips };
        if let Ok(json) = serde_json::to_string(&m) {
            let path = self.root.join("MANIFEST");
            let tmp  = self.root.join("MANIFEST.tmp");
            // fsync the tmp file BEFORE the rename: rename-without-fsync can
            // leave a zero-length/partial MANIFEST at the final path after
            // power loss (ext4 delayed allocation). The startup self-heal
            // (invalid head -> cold scan) catches that, but a full rescan is
            // exactly the cost MANIFEST exists to avoid. One fsync per flush,
            // and flushes are already off the hot write path (ticker-driven).
            let wrote = (|| -> std::io::Result<()> {
                use std::io::Write;
                let mut f = fs::File::create(&tmp)?;
                f.write_all(json.as_bytes())?;
                f.sync_all()
            })();
            if wrote.is_ok() && fs::rename(&tmp, &path).is_ok() {
                // Make the rename itself durable (directory entry). Unix-only;
                // on Windows directory handles don't support this and the
                // rename is already journaled by NTFS.
                #[cfg(unix)]
                if let Ok(dir) = fs::File::open(&self.root) {
                    let _ = dir.sync_all();
                }
            }
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
                // Segment bytes must be durable BEFORE a MANIFEST that
                // references them: otherwise power loss can leave MANIFEST
                // pointing at a tip whose object bytes were still in the page
                // cache — the torn tail is truncated on reopen and the warm
                // boot resolves a tip that no longer exists, with the seq
                // counter ahead of durable data. Order: sync segments, then
                // MANIFEST. Gated on the dirty flag so an idle database pays
                // no per-tick fsync. (flush_all already used this order; the
                // ticker now matches it.)
                if db.manifest_dirty.load(Ordering::Acquire) {
                    if let Err(e) = db.objects.sync() {
                        eprintln!("nedb: segment sync failed: {}", e);
                    }
                    db.flush_manifest_if_dirty();
                }
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
        self.update_head(coll, seq, &hash);
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

    /// The tip — the most recently written node (highest seq), or `None` if the
    /// database is empty. O(1): `self.seq` is the next-to-assign counter, so the
    /// latest write sits at `seq - 1`; we resolve it through the same
    /// seq_index → object-store path a normal read uses, so the returned Node is
    /// byte-identical to one fetched by id or hash (it carries its own seq, hash,
    /// causal links, and valid-time). This is the cheap "give me the latest write"
    /// primitive — the head of the log, not an aggregate.
    pub fn tip(&self) -> Option<Node> {
        let next = self.seq.load(Ordering::SeqCst);
        if next == 0 {
            return None; // nothing written yet
        }
        // Fast path: resolve the head seq through the in-memory seq index
        // (populated by this session's writes or by the cold scan).
        if let Some(hash) = self.get_hash_by_seq(next - 1) {
            return self.get_by_hash(&hash);
        }
        // Warm-boot fallback: the seq index is still cold (warm start skips the
        // scan), but the tip's object hash was persisted in MANIFEST and restored
        // on open. O(1), no scan — this is what makes tip() survive a restart.
        let th = self.tip_hash.read().1.clone();
        if !th.is_empty() {
            return self.get_by_hash(&th);
        }
        None
    }

    /// The collection-local tip — the most recent write into `coll` (highest seq in
    /// that collection), or `None` if the collection has no writes. O(1): resolves
    /// through `coll_tip_hash`, a dedicated per-collection map kept current on every
    /// write (`update_head`), restored from MANIFEST on warm boot, and rebuilt by the
    /// cold scan — durable across restarts by construction, same contract as `tip()`
    /// for the global head. Conceptually a different index than the global `tip()`
    /// (global head vs collection head), kept as a separate method so each is
    /// explicit — parity with the Python reference's `tip(coll)`. Lets a consumer
    /// resume one chain (e.g. blocks / tx / utxo) without pulling global tip and
    /// filtering.
    pub fn tip_collection(&self, coll: &str) -> Option<Node> {
        let hash = self.coll_tip_hash.get(coll)?.1.clone();
        self.get_by_hash(&hash)
    }

    /// Changefeed page: up to `limit` nodes written AFTER `after_seq` (EXCLUSIVE),
    /// ascending by seq, wrapped in a `SinceBatch` cursor envelope. `after_seq` is
    /// the cursor you last applied (a prior `tip()` seq or `to_seq`). `limit` bounds
    /// the page — `0` means DEFAULT_SINCE_LIMIT, so the engine primitive can never
    /// materialize an unbounded batch even when embedders call it directly (the
    /// safety is here, not only in the HTTP layer). Drain by paging while
    /// `has_more`, advancing your cursor to `to_seq`, then hand off to the live
    /// `subscribe` edge. The append-only log IS the changefeed, so this is an
    /// O(page) walk; unresolved seqs (outside seq_index coverage — see
    /// `scan_status()`) are skipped rather than faked.
    pub fn since(&self, after_seq: u64, limit: usize) -> SinceBatch {
        let next = self.seq.load(Ordering::SeqCst);          // head + 1
        let head_seq = next.saturating_sub(1);
        let cap = if limit == 0 { DEFAULT_SINCE_LIMIT } else { limit };
        let mut nodes: Vec<Node> = Vec::new();
        let mut to_seq = after_seq;
        let mut hit_limit = false;
        let mut s = after_seq.saturating_add(1);
        while s < next {
            if nodes.len() >= cap { hit_limit = true; break; }
            if let Some(hash) = self.get_hash_by_seq(s) {
                if let Some(node) = self.get_by_hash(&hash) {
                    to_seq = node.seq;
                    nodes.push(node);
                }
            }
            s += 1;
        }
        SinceBatch { nodes, from_seq: after_seq, to_seq, head_seq, has_more: hit_limit }
    }

    /// Replication readiness — see `ScanStatus`. `scan_complete` gates safe
    /// historical catch-up: a consumer pulling an old cursor right after a cold
    /// start must wait for it, or `since()` may hand back a partial page that looks
    /// like "caught up". Computes the indexed range by scanning the in-memory seq
    /// index (O(index)) — intended for periodic status polls, not the per-write
    /// hot path.
    pub fn scan_status(&self) -> ScanStatus {
        let next = self.seq.load(Ordering::SeqCst);
        let mut min = u64::MAX;
        let mut max = 0u64;
        let mut count = 0usize;
        for kv in self.seq_index.iter() {
            let s = *kv.key();
            if s < min { min = s; }
            if s > max { max = s; }
            count += 1;
        }
        if count == 0 { min = 0; }
        ScanStatus {
            scan_complete:   self.startup_ready.load(Ordering::SeqCst),
            tip_seq:         next.saturating_sub(1),
            indexed_seq_min: min,
            indexed_seq_max: max,
            indexed_count:   count,
        }
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
    let seq_index      = &db.seq_index;
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

    // Populate the seq index AS objects are read here, not in a second pass
    // afterward: this loop is the slow, disk-I/O-bound phase (verifying and
    // parsing every object), and it can run for minutes on a multi-million
    // object store. `scan_status().indexed_count` reads `seq_index`'s size, so
    // inserting here — not after `.collect()` — is what makes that a real, live
    // progress signal through the phase that actually takes the time, instead
    // of reporting a flat 0 until this whole pass finishes. Safe: DashMap
    // supports concurrent inserts, and every parallel worker here inserts a
    // disjoint key (each object has its own seq).
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
            let node = objects.read(h).ok()?;
            seq_index.insert(node.seq, node.hash.clone());
            Some(node)
        })
        .collect();

    eprintln!("\r  [nedbd]   100%  {:>8} / {:>8}  ({:.1}s)                        ",
        total, total, t0.elapsed().as_secs_f32());

    let max_seq = nodes.iter().map(|n| n.seq).max().unwrap_or(0);
    seq_atomic.store(max_seq + 1, Ordering::SeqCst);

    // Per-collection tip: highest-seq node's hash, per coll. `nodes` is NOT
    // seq-ordered here (it comes from an unordered object-hash scan), so this
    // must track the max explicitly — unlike the live write path's "last call
    // wins" (which relies on ascending call order that a scan doesn't have).
    let mut coll_max: std::collections::HashMap<String, (u64, String)> = std::collections::HashMap::new();

    for node in &nodes {
        // seq_index was already populated above, during the read pass.
        coll_max.entry(node.coll.clone())
            .and_modify(|(s, h)| if node.seq > *s { *s = node.seq; *h = node.hash.clone(); })
            .or_insert_with(|| (node.seq, node.hash.clone()));
        if let Value::Object(ref obj) = node.data {
            for (field, value) in obj {
                if sorted_indexes.has(&node.coll, field) {
                    sorted_indexes.insert(&node.coll, field, value, &node.hash);
                }
            }
        }
    }

    for (coll, (seq, hash)) in coll_max {
        db.coll_tip_hash.insert(coll, (seq, hash));
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
    *head.write() = new_head;

    // Tip = the highest-seq object we indexed. Persist its hash so tip() resolves
    // O(1) on the next warm boot, before any scan repopulates the seq index.
    let tip_hash = db.seq_index.iter()
        .max_by_key(|kv| *kv.key())
        .map(|kv| kv.value().clone())
        .unwrap_or_default();
    *db.tip_hash.write() = (max_seq, tip_hash);

    // Write MANIFEST through the one canonical writer. The hand-rolled write
    // this replaces stored `seq: max_seq` (the last USED seq) — but the warm
    // boot loads `m.seq` as the NEXT-TO-ASSIGN counter, so a restart right
    // after a quiet cold scan handed the next write the tip's seq: a duplicate
    // seq in the log (seq_index overwrite, wrong since() page). flush_manifest
    // reads the live counter (already max_seq + 1) — correct by construction.
    db.flush_manifest();

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
    fn tip_and_since() {
        let db = Db::in_memory();
        // Empty db: no tip, empty changefeed.
        assert!(db.tip().is_none());
        assert!(db.since(0, 0).nodes.is_empty());

        let a = db.put("item", "a", serde_json::json!({"x": 1}), vec![], None, None).unwrap();
        let b = db.put("item", "b", serde_json::json!({"x": 2}), vec![], None, None).unwrap();

        // tip() = the most recent write (highest seq), returned as a full node.
        let t = db.tip().expect("tip after writes");
        assert_eq!(t.seq, b.seq);
        assert_eq!(t.id, "b");
        assert_eq!(t.hash, b.hash);

        // since(after_seq, limit) — EXCLUSIVE cursor, bounded page + envelope.
        let after_a = db.since(a.seq, 0);
        assert_eq!(after_a.nodes.len(), 1);
        assert_eq!(after_a.nodes[0].id, "b");
        assert_eq!(after_a.from_seq, a.seq);
        assert_eq!(after_a.to_seq, b.seq);
        assert_eq!(after_a.head_seq, b.seq);
        assert!(!after_a.has_more);

        // Nothing written after the tip.
        assert!(db.since(b.seq, 0).nodes.is_empty());

        // `limit` bounds the page and sets has_more; resume from to_seq.
        let c = db.put("item", "c", serde_json::json!({"x": 3}), vec![], None, None).unwrap();
        let page = db.since(a.seq, 1);             // (a..] capped at 1 -> [b], more pending
        assert_eq!(page.nodes.len(), 1);
        assert_eq!(page.nodes[0].id, "b");
        assert_eq!(page.to_seq, b.seq);
        assert!(page.has_more);
        let page2 = db.since(page.to_seq, 1);      // resume from b -> [c], done
        assert_eq!(page2.nodes.len(), 1);
        assert_eq!(page2.nodes[0].id, "c");
        assert_eq!(page2.to_seq, c.seq);
        assert!(!page2.has_more);
    }

    #[test]
    fn tip_collection_per_chain() {
        // The ITC sync-client case: separate chains in separate collections; a
        // consumer resumes ONE without pulling global tip and filtering.
        let db = Db::in_memory();
        assert!(db.tip_collection("blocks").is_none());

        db.put("blocks", "b0", serde_json::json!({"h": 0}), vec![], None, None).unwrap();
        db.put("tx",     "t0", serde_json::json!({"v": 1}), vec![], None, None).unwrap();
        let b1 = db.put("blocks", "b1", serde_json::json!({"h": 1}), vec![], None, None).unwrap();
        let t1 = db.put("tx",     "t1", serde_json::json!({"v": 2}), vec![], None, None).unwrap();

        // global tip = latest write overall (t1)
        assert_eq!(db.tip().unwrap().id, "t1");
        // collection-local tips = latest write in each collection
        let bt = db.tip_collection("blocks").expect("blocks tip");
        assert_eq!(bt.id, "b1");
        assert_eq!(bt.seq, b1.seq);
        assert_eq!(db.tip_collection("tx").unwrap().seq, t1.seq);
        assert!(db.tip_collection("absent").is_none());
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

    /// Regression: put_batch must remove the superseded version's sorted-index
    /// entries, exactly like put() does. Old behavior left the old hashes in
    /// the BTree — ORDER BY returned superseded rows alongside current ones
    /// (they resolve fine through the content-addressed store, which made the
    /// stale rows look legitimate).
    #[test]
    fn put_batch_removes_superseded_sorted_index_entries() {
        let db = Db::in_memory();
        db.create_sorted_index("blocks", "height");
        db.put("blocks", "x", serde_json::json!({"height": 1}), vec![], None, None).unwrap();
        db.put_batch(vec![
            ("blocks".into(), "x".into(), serde_json::json!({"height": 99}), vec![], None, None),
        ]).unwrap();

        let asc = db.order_by_asc("blocks", "height", 10);
        assert_eq!(asc.len(), 1, "stale index entry for the superseded version must be gone");
        assert_eq!(asc[0].data["height"], 99);
        assert_eq!(asc[0].id, "x");
    }

    /// Updates without any sorted index must keep full version-chain semantics
    /// (guards the new skip-old-object-read fast path in put()).
    #[test]
    fn update_without_indexes_preserves_chain() {
        let db = Db::in_memory();
        let v1 = db.put("docs", "x", serde_json::json!({"v": 1}), vec![], None, None).unwrap();
        let v2 = db.put("docs", "x", serde_json::json!({"v": 2}), vec![], None, None).unwrap();
        assert_eq!(v2.prev.as_deref(), Some(v1.hash.as_str()), "prev chain must survive the fast path");
        assert_eq!(db.get("docs", "x").unwrap().data["v"], 2);
        assert_eq!(db.get_as_of("docs", "x", v1.seq).unwrap().data["v"], 1);
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

    #[test]
    fn tip_survives_warm_restart() {
        // v2.5.43: tip() returns the last written object AND survives a warm restart.
        // On reopen the seq_index is cold (warm start skips the scan), so tip() must
        // resolve the last write via the MANIFEST tip_hash fallback — no scan.
        let dir = tempdir().unwrap();
        {
            let db = Db::open(dir.path(), None).unwrap();
            db.put("blocks", "b1", serde_json::json!({"h": 1}), vec![], None, None).unwrap();
            db.put("blocks", "b2", serde_json::json!({"h": 2}), vec![], None, None).unwrap();
            db.flush_all(); // persists MANIFEST incl. tip_hash
            assert_eq!(db.tip().expect("tip in-session").id, "b2");
        }
        // Warm reopen: MANIFEST present -> no cold scan -> seq_index cold.
        let db2 = Db::open(dir.path(), None).unwrap();
        assert!(db2.get_hash_by_seq(1).is_none(), "seq_index is cold on a warm boot");
        let tip = db2.tip().expect("tip() must survive a warm restart");
        assert_eq!(tip.id, "b2");
        assert_eq!(tip.data.get("h").and_then(|v| v.as_i64()), Some(2));
    }

    #[test]
    fn tip_collection_survives_warm_restart() {
        // Same contract as tip(), per collection: itc-node-rs resumes headers /
        // blocks / l2_receipts independently, so each must be its own durable
        // resume point — not just the global tip.
        let dir = tempdir().unwrap();
        {
            let db = Db::open(dir.path(), None).unwrap();
            db.put("blocks", "b1", serde_json::json!({"h": 1}), vec![], None, None).unwrap();
            db.put("tx",     "t1", serde_json::json!({"v": 1}), vec![], None, None).unwrap();
            let b2 = db.put("blocks", "b2", serde_json::json!({"h": 2}), vec![], None, None).unwrap();
            db.flush_all(); // persists MANIFEST incl. coll_tips
            assert_eq!(db.tip_collection("blocks").unwrap().id, "b2");
            assert_eq!(db.tip_collection("blocks").unwrap().seq, b2.seq);
        }
        // Warm reopen: MANIFEST present -> no cold scan -> seq_index cold.
        let db2 = Db::open(dir.path(), None).unwrap();
        assert!(db2.get_hash_by_seq(0).is_none(), "seq_index is cold on a warm boot");
        let blocks_tip = db2.tip_collection("blocks").expect("tip_collection must survive a warm restart");
        assert_eq!(blocks_tip.id, "b2");
        assert_eq!(blocks_tip.data.get("h").and_then(|v| v.as_i64()), Some(2));
        let tx_tip = db2.tip_collection("tx").expect("tx tip must also survive");
        assert_eq!(tx_tip.id, "t1");
        assert!(db2.tip_collection("absent").is_none());
    }

    #[test]
    fn cold_scan_indexes_every_object_and_reports_completion() {
        // Regression guard for the cold-scan refactor: seq_index is now populated
        // DURING the parallel read pass (for live scan_status().indexed_count
        // progress — see cold_scan_background_arc), not in a second pass
        // afterward. This asserts the end state is unchanged: every written
        // object is indexed, tip()/tip_collection() are correct, and
        // scan_complete eventually reports true.
        let dir = tempdir().unwrap();
        let n = 25u64;
        {
            let db = Db::open(dir.path(), None).unwrap();
            for i in 0..n {
                db.put("things", &i.to_string(), serde_json::json!({"i": i}), vec![], None, None).unwrap();
            }
            db.flush_all();
        }
        // Force a COLD start regardless of the MANIFEST nedb-v2 itself would
        // have written: delete it so startup_rebuild() takes the cold path and
        // start_cold_scan() actually spawns the background scan this test needs
        // to exercise.
        std::fs::remove_file(dir.path().join("MANIFEST")).unwrap();

        let db = Db::open(dir.path(), None).unwrap();
        assert!(!db.scan_status().scan_complete, "should be cold immediately after open");
        let db = std::sync::Arc::new(db);
        Db::start_cold_scan(std::sync::Arc::clone(&db));

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !db.scan_status().scan_complete {
            assert!(std::time::Instant::now() < deadline, "cold scan did not complete in time");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let status = db.scan_status();
        assert_eq!(status.indexed_count, n as usize, "every written object must be indexed");
        assert!(status.scan_complete);

        let tip = db.tip().expect("tip resolves after cold scan");
        assert_eq!(tip.data.get("i").and_then(|v| v.as_u64()), Some(n - 1));
        let coll_tip = db.tip_collection("things").expect("tip_collection resolves after cold scan");
        assert_eq!(coll_tip.id, tip.id);
    }

    /// Concurrent writers must settle the tip at the HIGHEST SEQ, and that tip
    /// must survive a warm restart. Before the seq-guarded tip fix, update_head
    /// was "last call wins": a slower thread carrying an OLDER seq could
    /// overwrite tip_hash after a newer write, and MANIFEST then persisted the
    /// stale tip for the next warm boot (flaky by nature — this pins the
    /// contract deterministically for the fixed code).
    #[test]
    fn concurrent_puts_tip_resolves_to_highest_seq_after_warm_restart() {
        let dir = tempdir().unwrap();
        let total: u64 = 100;
        {
            let db = std::sync::Arc::new(Db::open(dir.path(), None).unwrap());
            let mut handles = vec![];
            for t in 0..4u64 {
                let db2 = std::sync::Arc::clone(&db);
                handles.push(std::thread::spawn(move || {
                    for i in 0..25u64 {
                        db2.put("c", &format!("{}-{}", t, i),
                                serde_json::json!({"t": t, "i": i}),
                                vec![], None, None).unwrap();
                    }
                }));
            }
            for h in handles { h.join().unwrap(); }
            // In-session: tip must be the highest assigned seq.
            let expected = db.seq.load(std::sync::atomic::Ordering::SeqCst) - 1;
            assert_eq!(expected, total - 1, "exactly {} writes expected", total);
            assert_eq!(db.tip().expect("in-session tip").seq, expected);
            db.flush_all(); // persist MANIFEST incl. tip_hash
        }
        // Warm reopen: seq_index cold; tip() resolves via MANIFEST tip_hash.
        let db2 = Db::open(dir.path(), None).unwrap();
        let tip = db2.tip().expect("tip must survive warm restart after concurrent writes");
        assert_eq!(tip.seq, total - 1, "warm-boot tip must be the highest-seq write");
        // Per-collection tip: same contract.
        let ct = db2.tip_collection("c").expect("coll tip survives");
        assert_eq!(ct.seq, total - 1);
    }

    /// Regression for the cold-scan MANIFEST seq off-by-one. The scan's old
    /// hand-rolled MANIFEST stored `seq: max_seq` (the last USED seq), but the
    /// warm boot loads `m.seq` as the NEXT-TO-ASSIGN counter — so a restart
    /// right after a quiet cold scan handed the next write the tip's seq:
    /// a DUPLICATE seq in the log (seq_index overwrite, wrong since() page).
    /// The scan now writes MANIFEST via flush_manifest(), which reads the live
    /// counter (max_seq + 1).
    #[test]
    fn manifest_after_cold_scan_does_not_reuse_tip_seq() {
        let dir = tempdir().unwrap();
        let old_tip_seq;
        {
            let db = Db::open(dir.path(), None).unwrap();
            for i in 0..5u64 {
                db.put("things", &i.to_string(), serde_json::json!({"i": i}), vec![], None, None).unwrap();
            }
            db.flush_all();
            old_tip_seq = db.tip().unwrap().seq;
        }
        // Force a cold start: remove MANIFEST so the background scan runs and
        // writes a fresh MANIFEST itself.
        std::fs::remove_file(dir.path().join("MANIFEST")).unwrap();
        {
            let db = std::sync::Arc::new(Db::open(dir.path(), None).unwrap());
            Db::start_cold_scan(std::sync::Arc::clone(&db));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !db.scan_status().scan_complete {
                assert!(std::time::Instant::now() < deadline, "cold scan did not complete");
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            // No further writes — the scan's own MANIFEST is what the next boot sees.
        }
        // Warm reopen from the scan-written MANIFEST: the next write must get a
        // FRESH seq, never the tip's.
        let db3 = Db::open(dir.path(), None).unwrap();
        let tip_before = db3.tip().expect("tip survives scan-written MANIFEST");
        assert_eq!(tip_before.seq, old_tip_seq, "tip identity preserved across the scan");
        let new_node = db3.put("things", "next", serde_json::json!({"fresh": true}),
                               vec![], None, None).unwrap();
        assert!(new_node.seq > old_tip_seq,
                "new write reused seq {} (tip was {}) — duplicate seq in the log",
                new_node.seq, old_tip_seq);
    }
}
