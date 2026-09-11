// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

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
    /// True when the seq index actually covers the log — i.e. `since()` can
    /// resolve historical seqs. DISTINCT from `scan_complete`: a warm boot is
    /// "startup complete" in O(1) precisely because it SKIPS the scan, so
    /// `scan_complete` is true while this is false and `since()` resolves
    /// nothing. Replication consumers must gate on this field, not on
    /// `scan_complete`; call `rebuild_id_index()`/`repair()` to populate it.
    pub seq_index_ready: bool,
}

pub struct Db {
    pub objects:        ObjectStore,
    pub id_index:       IdIndex,
    /// Deleted id → its tombstone hash. The GRAVEYARD.
    ///
    /// `id_index` answers "what is the current version of this key?", so a
    /// delete has to remove the entry from it or the row would stay visible.
    /// But that made a deleted document's whole HISTORY unreachable: `AS OF`
    /// enumerates ids from `id_index`, so the id was never considered at any
    /// sequence — even one long before the delete. Nothing was lost on disk
    /// (the tombstone node keeps a `prev` link to the full version chain); it
    /// was simply unreferenced.
    ///
    /// That contradicted the central promise: a `DELETE` is a tombstone, not
    /// an erasure. So the pointer is not dropped, it is MOVED here — the id
    /// leaves the land of the living and stays addressable in history.
    ///
    /// It is a second `IdIndex` rather than a new namespace inside the first
    /// because every operation needed — set, get, list, remove, WAL buffering,
    /// sharded on-disk layout — already exists and is already tested. A
    /// deliberately boring choice.
    pub del_index:      IdIndex,
    pub sorted_indexes: SortedIndexes,
    pub graph:          GraphStore,
    pub root:           PathBuf,
    /// Advisory exclusive lock on the data directory (`LOCK` file), held for
    /// the Db's lifetime. One process owns a durable store at a time — a
    /// second opener gets a loud refusal instead of silent split-brain (two
    /// engines with independent in-memory state on one dir: cross-process
    /// writes invisible, CAS races — the 2026-07-20 aias multi-worker session
    /// bug, caught live). Released automatically on drop AND on any process
    /// death including SIGKILL, because the flock dies with the fd. `None`
    /// for in-memory databases and under NEDB_SHARED_OPEN=1 (operator
    /// override for tooling that accepts the risk).
    _dir_lock:          Option<std::fs::File>,
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
            del_index:      IdIndex::in_memory(),
            sorted_indexes: SortedIndexes::new(),
            graph:          GraphStore::in_memory(),
            root:           std::path::PathBuf::from(":memory:"),
            _dir_lock:      None,
            seq:            AtomicU64::new(0),
            head:           RwLock::new(String::new()),
            tip_hash:       RwLock::new((0, String::new())),
            coll_tip_hash:  Arc::new(DashMap::new()),
            startup_ready:  Arc::new(AtomicBool::new(true)),  // always ready
            manifest_dirty: Arc::new(AtomicBool::new(false)),
            seq_index:      Arc::new(DashMap::new()),
        }
    }

    /// Acquire the exclusive advisory lock on a durable data directory.
    /// Refuses (with the holder's pid when known) rather than allowing a
    /// second live engine on the same files. NEDB_SHARED_OPEN=1 skips the
    /// guard entirely — for tooling that knowingly accepts split-brain risk.
    fn acquire_dir_lock(db_root: &Path) -> Result<Option<std::fs::File>> {
        if std::env::var("NEDB_SHARED_OPEN").map(|v| v.trim() == "1").unwrap_or(false) {
            return Ok(None);
        }
        use fs2::FileExt as _;
        use std::io::Write as _;
        let lock_path = db_root.join("LOCK");
        let lock_file = std::fs::OpenOptions::new()
            .create(true).read(true).write(true).open(&lock_path)?;
        if lock_file.try_lock_exclusive().is_err() {
            let holder = std::fs::read_to_string(&lock_path).unwrap_or_default();
            let holder = holder.trim();
            anyhow::bail!(
                "data directory {:?} is locked by another process{} — refusing a \
                 split-brain open: a second engine on the same files cannot see this \
                 process's writes (invisible sessions, CAS races). Stop the other \
                 process, or set NEDB_SHARED_OPEN=1 only if you accept that risk.",
                db_root,
                if holder.is_empty() { String::new() } else { format!(" (pid {holder})") }
            );
        }
        // Best-effort: record our pid for the next contender's error message.
        let _ = lock_file.set_len(0);
        let _ = writeln!(&lock_file, "{}", std::process::id());
        let _ = lock_file.sync_all();
        Ok(Some(lock_file))
    }

    /// Open (or create) a database. Runs v1→v2 migration automatically if log.aof is present.
    pub fn open(db_root: &Path, dek: Option<Dek>) -> Result<Self> {
        std::fs::create_dir_all(db_root)?;

        // Split-brain guard FIRST — refuse before touching any store state.
        let dir_lock = Self::acquire_dir_lock(db_root)?;

        let objects        = ObjectStore::new(db_root, dek.clone())?;
        let id_index       = IdIndex::new(db_root)?;
        // The graveyard lives under its own root so it shares no path with the
        // live index and cannot be confused with it by any existing reader.
        let del_index      = IdIndex::new(&db_root.join("graveyard"))?;
        let sorted_indexes = SortedIndexes::new();
        let graph          = GraphStore::new(db_root)?;

        let mut db = Self {
            objects,
            id_index,
            del_index,
            sorted_indexes,
            graph,
            root: db_root.to_path_buf(),
            _dir_lock: dir_lock,
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
                } else {
                    // Pre-2.5.43 MANIFEST (no persisted tip): warm-boot ANYWAY.
                    //
                    // The old policy forced a full cold scan "once to upgrade" —
                    // on multi-million-object embedded stores (itcd -dagv3:
                    // 1.7M+ objects per database) that scan is hours of random
                    // reads on seek-bound media, it races the host's own boot
                    // I/O, and if the process exits before it completes the
                    // NEXT boot pays it again — a permanent boot tax for
                    // exactly the deployments that can least afford it. And it
                    // buys nothing that can't heal lazily: seq + head in the
                    // old MANIFEST are perfectly valid, and flush_manifest
                    // writes tip_hash + coll_tips from live state, so the very
                    // first write + flush after boot upgrades the MANIFEST
                    // organically. Until then tip()/tip_collection() simply
                    // return None on this boot — exactly their documented
                    // behavior for an unresolvable tip — and every other read
                    // and write path is unaffected.
                    if m.tip_hash.is_empty() {
                        eprintln!("  [nedbd] MANIFEST predates durable tip() — warm boot; tip()/tip_collection() heal on first flush (no forced scan)");
                    }
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
                        m.seq, &m.head[..8],
                        if m.tip_hash.is_empty() { "(pre-2.5.43, heals on flush)" }
                        else { &m.tip_hash[..8.min(m.tip_hash.len())] });
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

    /// Rebuild the id index from the object store, synchronously.
    ///
    /// Every object carries its own `coll`, `id` and `seq`, so the id index is
    /// fully derivable: for each (coll, id) the highest seq wins. Use this to
    /// recover a database whose id-index WAL never reached disk — the objects
    /// are intact and verify, but `list()`/`get()` return nothing.
    ///
    /// Idempotent, and safe on a healthy store (it rewrites the same winners).
    /// Returns the number of entries written. Flushes before returning.
    pub fn rebuild_id_index(&self) -> Result<usize> {
        let hashes: Vec<String> = self.objects.all_hashes().collect();
        let mut nodes: Vec<Node> = Vec::with_capacity(hashes.len());
        for h in &hashes {
            if let Ok(node) = self.objects.read(h) {
                self.seq_index.insert(node.seq, node.hash.clone());
                nodes.push(node);
            }
        }
        let written = rebuild_id_index_from_nodes(self, &nodes);

        // Per-collection tips, so tip_collection() resolves after a repair.
        let mut coll_max: std::collections::HashMap<String, (u64, String)> =
            std::collections::HashMap::new();
        for node in &nodes {
            coll_max
                .entry(node.coll.clone())
                .and_modify(|cur| {
                    if node.seq > cur.0 {
                        *cur = (node.seq, node.hash.clone());
                    }
                })
                .or_insert((node.seq, node.hash.clone()));
        }
        for (coll, (seq, hash)) in coll_max {
            self.coll_tip_hash.insert(coll, (seq, hash));
        }

        let max_seq = nodes.iter().map(|n| n.seq).max().unwrap_or(0);
        // Keep the seq counter ahead of everything we just found, so the next
        // write cannot reuse a seq that already exists in the log.
        let next = max_seq + 1;
        if !nodes.is_empty() && self.seq.load(Ordering::SeqCst) < next {
            self.seq.store(next, Ordering::SeqCst);
        }

        // Recompute head + tip through the shared implementation, so a repaired
        // database reopens WARM with a valid MANIFEST instead of coming back up
        // cold with an empty head (which reads as corruption to the next boot).
        if !nodes.is_empty() {
            recompute_head_and_tip(self, hashes, max_seq);
        }

        self.try_flush_all()?;
        Ok(written)
    }

    /// Full repair: rebuild the seq index and the id index from objects, even on
    /// a WARM store, then flush.
    ///
    /// [`start_cold_scan`] deliberately no-ops when startup is already complete,
    /// which meant the documented repair path ("idempotent — a no-op on a warm
    /// store, a full self-heal on a stale MANIFEST") could never repair a
    /// database that had a valid MANIFEST and a damaged id index. This is the
    /// forcing entry point; `start_cold_scan` keeps its O(1) warm-boot contract.
    pub fn repair(&self) -> Result<usize> {
        self.rebuild_id_index()
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

    /// Flush both the id-index WAL and MANIFEST, REPORTING failure.
    ///
    /// This is the durability boundary: until it returns `Ok(())`, writes that
    /// `put()` acknowledged may not be on disk. Callers that must not lose data
    /// — anything about to take a destructive or externally-visible action on
    /// the strength of a persisted record — should use this, not [`flush_all`].
    ///
    /// Every stage is attempted even if an earlier one fails (a MANIFEST flush
    /// is still worth doing when one index leaf failed), and the first error is
    /// returned. Failed id-index entries stay in the WAL for retry.
    pub fn try_flush_all(&self) -> Result<()> {
        let index_result = self.id_index.try_flush_write_buf()
            // The graveyard is as durable as the live index: a tombstone
            // pointer lost to a crash would take a document's history back out
            // of reach, which is the bug this index exists to prevent.
            .and(self.del_index.try_flush_write_buf());
        // v3: fsync the active segment (no-op for loose/in-memory stores).
        // One durability point per batch instead of one fsync per object.
        let sync_result = self.objects.sync();
        let manifest_result = self.try_flush_manifest();

        index_result.map_err(|e| anyhow::anyhow!("id-index WAL flush failed: {}", e))?;
        sync_result.map_err(|e| anyhow::anyhow!("object segment sync failed: {}", e))?;
        manifest_result.map_err(|e| anyhow::anyhow!("MANIFEST flush failed: {}", e))?;
        Ok(())
    }

    /// Flush both the id-index WAL and MANIFEST. Used on graceful shutdown.
    ///
    /// Errors are logged, not returned — kept for back-compat and for the
    /// ticker/`Drop` paths that have nowhere to propagate. Prefer
    /// [`try_flush_all`] whenever the outcome matters.
    pub fn flush_all(&self) {
        if let Err(e) = self.try_flush_all() {
            eprintln!("nedb: flush_all failed: {}", e);
        }
    }

    /// Compact the v3 packed object store: keep the CURRENT version of every
    /// document (from the id-index) and reclaim everything else. No-op unless
    /// running with the v3 segment substrate (`--dag-v3` / NEDB_DAG_V3).
    ///
    /// This is a PRUNING operation: superseded/historical object versions are
    /// dropped, so AS OF / TRACE over pruned versions is discarded — that is
    /// what reclaims the space. Flushes first so all data is durable on disk
    /// before the old segments are deleted.
    /// Reclaim space by rewriting the segments with only CURRENT versions.
    ///
    /// # This discards history. On purpose.
    ///
    /// The live set is each document's current-version hash and nothing else,
    /// so compaction drops every superseded version and every tombstone. After
    /// it runs, `AS OF` can no longer reach a prior value and `TRACE` can no
    /// longer walk to a pruned ancestor — the rows simply become unavailable
    /// rather than wrong, and `verify()` stays clean because what remains is
    /// still internally consistent.
    ///
    /// That is worth stating loudly, because NEDB's headline property is that
    /// history is permanent and never garbage-collected — and it is, right up
    /// until an operator calls THIS. Nothing calls it automatically: it is not
    /// on the HTTP surface, not in the CLI, and not on any timer. It exists for
    /// the operator who has decided, explicitly, to trade the audit trail for
    /// disk space.
    ///
    /// A graveyard entry whose tombstone was pruned is left pointing at an
    /// object that no longer exists. `get_as_of` degrades to `None` there
    /// rather than failing, so a compacted store answers "not available at that
    /// sequence" instead of erroring or inventing a value.
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

    /// Atomically persist current seq+head to MANIFEST, reporting failure.
    /// No-op (`Ok`) for in-memory databases.
    ///
    /// A silently failed MANIFEST write is not data loss — the startup
    /// self-heal rescans — but it IS a warm-boot regression and, on a full
    /// disk, the first symptom that persistence is failing. Callers deserve
    /// to know.
    pub fn try_flush_manifest(&self) -> std::io::Result<()> {
        if self.root == std::path::PathBuf::from(":memory:") { return Ok(()); }
        let seq  = self.seq.load(Ordering::SeqCst);
        let head = self.head.read().clone();
        let tip_hash = self.tip_hash.read().1.clone();
        let coll_tips: std::collections::HashMap<String, String> = self.coll_tip_hash
            .iter()
            .map(|kv| (kv.key().clone(), kv.value().1.clone()))
            .collect();
        let m = Manifest { seq, head, tip_hash, coll_tips };
        let json = serde_json::to_string(&m)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
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
        if let Err(e) = wrote {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
        fs::rename(&tmp, &path)?;
        // Make the rename itself durable (directory entry). Unix-only;
        // on Windows directory handles don't support this and the
        // rename is already journaled by NTFS.
        #[cfg(unix)]
        if let Ok(dir) = fs::File::open(&self.root) {
            let _ = dir.sync_all();
        }
        Ok(())
    }

    /// Atomically persist current seq+head to MANIFEST. No-op for in-memory databases.
    /// Errors are logged; prefer [`try_flush_manifest`] when the outcome matters.
    pub fn flush_manifest(&self) {
        if let Err(e) = self.try_flush_manifest() {
            eprintln!("nedb: MANIFEST flush failed: {}", e);
        }
    }


    /// Start a background thread that flushes both the id-index WAL and MANIFEST
    /// every `interval_ms` milliseconds.
    /// Call this after Arc::new(db) — the Arc keeps Db alive for the thread's lifetime.
    /// Flush cadence for EMBEDDED durable handles (the napi and pyo3 `open()` paths).
    ///
    /// `nedbd` has always run the manifest ticker at 1 s, so a server flushes the id-index WAL and
    /// MANIFEST every second and a hard kill loses at most a second of acknowledged writes. The
    /// embedded bindings did not start a ticker at all: their WAL was flushed only by the exit hooks
    /// (SIGINT/SIGTERM/atexit) — so an embedded app killed with SIGKILL, OOM-killed, or cut by power
    /// lost EVERY write since open, with no bound. Found by CHALK / Sports-Rater on 2026-09-04
    /// (acknowledged fan writes gone after `kill -9`). Since 2.8.5 the bindings start the ticker on
    /// durable open with this cadence — parity with nedbd.
    ///
    /// `NEDB_FLUSH_MS` overrides: an integer of milliseconds (min 50), or `0` / `off` to disable
    /// (only for hosts that own their own flush cadence). Unset → 1000.
    pub fn embedded_flush_interval_ms() -> Option<u64> {
        match std::env::var("NEDB_FLUSH_MS") {
            Err(_) => Some(1000),
            Ok(v) => {
                let v = v.trim().to_ascii_lowercase();
                if v.is_empty() { return Some(1000); }
                if v == "0" || v == "off" || v == "false" || v == "no" { return None; }
                match v.parse::<u64>() {
                    Ok(ms) => Some(ms.max(50)),
                    Err(_) => { eprintln!("nedb: NEDB_FLUSH_MS={:?} is not a number — using 1000", v); Some(1000) }
                }
            }
        }
    }

    /// Spawn the background flush ticker.
    ///
    /// The ticker holds a **`Weak<Db>`** and exits the first time the upgrade
    /// fails — i.e. as soon as the last real owner drops the database. The
    /// caller must therefore keep its own `Arc` alive for as long as it wants
    /// ticking; every current caller already does (nedbd stores it in its
    /// database map, the napi and pyo3 handles own theirs).
    ///
    /// It used to hold a strong `Arc` inside an unconditional `loop`, which
    /// meant the thread never exited and the `Db` was never dropped. Three
    /// consequences, all of them live since 2.8.5:
    ///
    /// * The exclusive data-dir `LOCK` taken in `Db::open` was never released,
    ///   so reopening the same path **in the same process** failed with
    ///   "locked by another process (pid N)" where N was the caller's own pid.
    /// * Every `open()` leaked a thread and the entire `Db` — indexes, caches,
    ///   segment handles — for the lifetime of the process.
    /// * `Drop for Db` (flush-on-close) could never fire for embedded users,
    ///   exactly as its own doc comment warned: it "only fires once every
    ///   owning handle is gone", and an immortal thread always held one.
    ///
    /// nedbd's `drop_db` was hit by the same thing: removing a database from
    /// the map did not free it, and an orphaned ticker went on fsyncing it.
    ///
    /// The `Arc` is upgraded inside the loop and dropped before the next
    /// sleep, so the ticker never extends the database's life across a tick.
    /// No final flush is needed here — the owner's `Drop` does it.
    pub fn start_manifest_ticker(self_arc: Arc<Self>, interval_ms: u64) {
        let weak = Arc::downgrade(&self_arc);
        // Do not let this function's own argument keep the database alive.
        drop(self_arc);
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_millis(interval_ms));
                // Last owner gone: stop ticking and let the thread die.
                let db = match weak.upgrade() {
                    Some(db) => db,
                    None => break,
                };
                // Flush id-index WAL to disk (parallel Rayon writes)
                db.id_index.flush_write_buf();
                db.del_index.flush_write_buf();
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
        // …and MOVE it to the graveyard, so history stays reachable.
        //
        // Removing the live pointer without this made the document's whole
        // version chain unaddressable: `AS OF` walks ids from `id_index`, so a
        // deleted id was skipped at every sequence — including sequences long
        // before the delete, where the row demonstrably existed. Nothing was
        // lost on disk, only unreferenced, which is the worst kind of data
        // loss because `verify()` still counts every object as healthy.
        //
        // The tombstone hash is the entry point: its `prev` links to the last
        // live version, and that chain back to the first write.
        self.del_index.set(coll, id, &hash)?;
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
    ///
    /// Reaches DELETED documents too. A delete moves the id's pointer into the
    /// graveyard rather than dropping it, so the version chain stays walkable
    /// and a row is still readable at a sequence before it was deleted — which
    /// is what "a DELETE is a tombstone, not an erasure" has to mean in
    /// practice. At or after the tombstone's own sequence the document is
    /// correctly absent.
    pub fn get_as_of(&self, coll: &str, id: &str, target_seq: u64) -> Option<Node> {
        // The live chain first: the common case, and the only one for an id
        // that was never deleted.
        if let Some(hash) = self.id_index.get(coll, id) {
            if let Some(node) = self.walk_back_to(&hash, target_seq) {
                return Some(node);
            }
            // Falling through matters for a RE-CREATED id. A `put` after a
            // delete starts a fresh chain with no `prev`, so the live chain
            // cannot reach a sequence from before the delete — but the
            // graveyard still can.
        }
        let tomb_hash = self.del_index.get(coll, id)?;
        let tomb = self.objects.read(&tomb_hash).ok()?;
        // As of the tombstone's own sequence the document is deleted. Returning
        // the tombstone node itself would surface `{_deleted, _prev}` as if it
        // were the document.
        if tomb.seq <= target_seq {
            return None;
        }
        self.walk_back_to(tomb.prev.as_deref()?, target_seq)
    }

    /// Walk `prev` links back from `hash` to the newest version at or before
    /// `target_seq`. `None` when the chain starts after it.
    fn walk_back_to(&self, hash: &str, target_seq: u64) -> Option<Node> {
        let mut current = self.objects.read(hash).ok()?;
        loop {
            if current.seq <= target_seq {
                return Some(current);
            }
            let prev_hash = current.prev.as_deref()?;
            current = self.objects.read(prev_hash).ok()?;
        }
    }

    /// Every id in a collection that AS OF must consider: the live ones, plus
    /// the deleted ones whose history is still addressable.
    ///
    /// Order is stable (sorted, deduplicated) so a historical query answers the
    /// same way run to run.
    pub fn list_ids_including_deleted(&self, coll: &str) -> Vec<String> {
        let mut ids = self.id_index.list_ids(coll);
        ids.extend(self.del_index.list_ids(coll));
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// List all documents in a collection, returning current versions.
    pub fn list(&self, coll: &str) -> Vec<Node> {
        self.id_index
            .list_ids(coll)
            .into_iter()
            .filter_map(|id| self.get(coll, &id))
            .collect()
    }

    /// Candidate nodes whose `field` falls in the given range, via the sorted
    /// index. `None` when no index covers (coll, field) — the caller must then
    /// fall back to a scan.
    ///
    /// Returns CURRENT versions only (the index drops a superseded hash on
    /// overwrite), so this must not be used to serve an `AS OF` query.
    pub fn range_scan(
        &self,
        coll: &str,
        field: &str,
        low: Option<&Value>,
        high: Option<&Value>,
        low_incl: bool,
        high_incl: bool,
    ) -> Option<Vec<Node>> {
        if !self.sorted_indexes.has(coll, field) {
            return None;
        }
        Some(
            self.sorted_indexes
                .range(coll, field, low, high, low_incl, high_incl)
                .into_iter()
                .filter_map(|h| self.objects.read(&h).ok())
                .collect(),
        )
    }

    /// Candidate nodes whose `field` equals any of `values` — the indexed path
    /// for `=` and for `IN (...)`. `None` when no index covers the field.
    pub fn index_lookup(&self, coll: &str, field: &str, values: &[Value]) -> Option<Vec<Node>> {
        if !self.sorted_indexes.has(coll, field) {
            return None;
        }
        // A value may legitimately appear in several arms of an IN list, and a
        // hash must not be returned twice.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut out = vec![];
        for v in values {
            for h in self.sorted_indexes.exact(coll, field, v) {
                if seen.insert(h.clone()) {
                    if let Ok(node) = self.objects.read(&h) {
                        out.push(node);
                    }
                }
            }
        }
        Some(out)
    }

    /// How many rows an indexed range covers, without reading any of them.
    /// `None` when no index covers the field.
    pub fn range_cardinality(
        &self,
        coll: &str,
        field: &str,
        low: Option<&Value>,
        high: Option<&Value>,
        low_incl: bool,
        high_incl: bool,
    ) -> Option<usize> {
        if !self.sorted_indexes.has(coll, field) {
            return None;
        }
        Some(self.sorted_indexes.range_len(coll, field, low, high, low_incl, high_incl))
    }

    /// True when a sorted index covers (coll, field).
    pub fn has_sorted_index(&self, coll: &str, field: &str) -> bool {
        self.sorted_indexes.has(coll, field)
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
        // `has_more` must never say "caught up" while the cursor is behind the
        // log head. Before 2.8.6 this was `hit_limit` alone, so any page whose
        // seqs could not be resolved (the whole range, on a warm boot: the warm
        // path skips the scan, leaving seq_index empty) returned zero nodes with
        // has_more=false — indistinguishable from genuinely up to date. A
        // consumer following the documented drain loop stopped forever, one call
        // in, on a database with every record unread.
        let has_more = hit_limit || to_seq < head_seq;
        SinceBatch { nodes, from_seq: after_seq, to_seq, head_seq, has_more }
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
            // The seq index covers the log when it resolves as many seqs as the
            // log has entries. On a warm boot it is empty while the log is not.
            seq_index_ready: count > 0 && (count as u64) >= next.saturating_sub(1),
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

    let objects        = &db.objects;
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

    // Rebuild the id index when it has no collections at all — the lost-WAL
    // case. Until 2.8.6 the cold scan restored seq_index, coll_tips, head and
    // MANIFEST but NEVER the id index, so a database whose id-index WAL never
    // reached disk came back with every object present and verifying while
    // `list()` and `get()` returned nothing — and `nedb-cli repair`, whose whole
    // job is this, reported success without fixing it.
    //
    // Gated on "no collections" so a normal cold boot of a healthy store (itcd:
    // millions of objects) does not pay N extra index writes. A partially lost
    // index is repaired by the explicit `rebuild_id_index()` path.
    if db.id_index.collections().is_empty() && !nodes.is_empty() {
        let restored = rebuild_id_index_from_nodes(&db, &nodes);
        println!("  [nedbd] id index was empty — rebuilt {} entries from objects", restored);
    }

    // Merkle head + tip, through the one shared implementation so the cold scan
    // and the explicit repair path can never drift apart.
    recompute_head_and_tip(&db, hashes, max_seq);

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

/// Recompute the Merkle head and the tip hash from the full object-hash set.
///
/// Shared by the cold scan and by `repair()` so the two can never disagree
/// about what the head of a rebuilt database is. `hashes` must be every object
/// hash in the store; `max_seq` the highest seq observed.
fn recompute_head_and_tip(db: &Db, hashes: Vec<String>, max_seq: u64) {
    use blake2::{Blake2b512, Digest};
    let mut sorted_hashes = hashes;
    sorted_hashes.sort();
    let mut h = Blake2b512::new();
    h.update(max_seq.to_le_bytes());
    for hash_str in &sorted_hashes {
        h.update(hash_str.as_bytes());
    }
    *db.head.write() = hex::encode(&h.finalize()[..32]);

    // Tip = the highest-seq object indexed. Persisting its hash lets tip()
    // resolve O(1) on the next warm boot, before any scan repopulates seq_index.
    let tip_hash = db.seq_index.iter()
        .max_by_key(|kv| *kv.key())
        .map(|kv| kv.value().clone())
        .unwrap_or_default();
    *db.tip_hash.write() = (max_seq, tip_hash);
}

/// Reconstruct id-index entries from already-read nodes: for every (coll, id),
/// the winner is the HIGHEST seq, which is exactly what `put()` would have left
/// behind. Returns the number of entries written.
///
/// The id index is fully derivable from the object store because every object
/// carries its own `coll`, `id` and `seq` — so a lost WAL is recoverable, and
/// nothing here invents data.
fn rebuild_id_index_from_nodes(db: &Db, nodes: &[Node]) -> usize {
    let mut winner: std::collections::HashMap<(String, String), (u64, String)> =
        std::collections::HashMap::new();
    for node in nodes {
        let key = (node.coll.clone(), node.id.clone());
        winner
            .entry(key)
            .and_modify(|cur| {
                if node.seq > cur.0 {
                    *cur = (node.seq, node.hash.clone());
                }
            })
            .or_insert((node.seq, node.hash.clone()));
    }
    let mut written = 0usize;
    for ((coll, id), (_seq, hash)) in &winner {
        if db.id_index.set(coll, id, hash).is_ok() {
            written += 1;
        }
    }
    // Persist immediately: a rebuild that only lands in the WAL would be lost
    // again by the very crash class this recovers from.
    if let Err(e) = db.id_index.try_flush_write_buf() {
        eprintln!("nedb: id-index rebuild flush failed: {}", e);
    }
    written
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

    // ── a DELETE is a tombstone, not an erasure ─────────────────────────────
    //
    // `delete()` used to just remove the live id pointer, which made the
    // document's whole history unreachable: `AS OF` enumerates ids from
    // `id_index`, so a deleted id was skipped at EVERY sequence — including
    // sequences long before the delete, where the row demonstrably existed.
    //
    // Nothing was lost on disk. The tombstone node keeps a `prev` link to the
    // full version chain and `verify()` counted every object as healthy — which
    // makes it the worst kind of data loss, the kind that passes its own audit.
    // The pointer is now MOVED to the graveyard instead of dropped.

    #[test]
    fn a_deleted_documents_history_is_still_readable_before_the_delete() {
        let db = Db::in_memory();
        let v1 = db.put("o", "a", serde_json::json!({"t": 55}), vec![], None, None).unwrap();
        let v2 = db.put("o", "a", serde_json::json!({"t": 66}), vec![], None, None).unwrap();
        assert!(db.delete("o", "a").unwrap());

        // Gone from the present — a delete must still delete.
        assert!(db.get("o", "a").is_none(), "a deleted doc must not be visible now");

        // …and readable at each sequence it existed at.
        let at_v1 = db.get_as_of("o", "a", v1.seq).expect("the ORIGINAL value survives");
        assert_eq!(at_v1.data["t"], serde_json::json!(55));
        let at_v2 = db.get_as_of("o", "a", v2.seq).expect("the UPDATED value survives");
        assert_eq!(at_v2.data["t"], serde_json::json!(66));
    }

    #[test]
    fn as_of_the_tombstone_or_later_reports_the_document_absent() {
        let db = Db::in_memory();
        db.put("o", "a", serde_json::json!({"t": 55}), vec![], None, None).unwrap();
        db.delete("o", "a").unwrap();
        let tomb_seq = db.tip().expect("the tombstone is the tip").seq;

        assert!(db.get_as_of("o", "a", tomb_seq).is_none(),
                "at the delete's own sequence the document is gone");
        assert!(db.get_as_of("o", "a", tomb_seq + 10).is_none(), "and after it");
        // Never the tombstone node itself: `{_deleted, _prev}` is bookkeeping,
        // and surfacing it would look like a document with strange fields.
        for s in 0..=tomb_seq + 1 {
            if let Some(n) = db.get_as_of("o", "a", s) {
                assert!(n.data.get("_deleted").is_none(),
                        "seq {} surfaced the tombstone as a document: {:?}", s, n.data);
            }
        }
    }

    #[test]
    fn an_as_of_query_lists_deleted_ids_alongside_live_ones() {
        let db = Db::in_memory();
        db.put("o", "keep", serde_json::json!({"n": 1}), vec![], None, None).unwrap();
        let gone = db.put("o", "gone", serde_json::json!({"n": 2}), vec![], None, None).unwrap();
        db.delete("o", "gone").unwrap();

        assert_eq!(db.id_index.list_ids("o"), vec!["keep".to_string()],
                   "the live index holds only the living");
        assert_eq!(db.list_ids_including_deleted("o"),
                   vec!["gone".to_string(), "keep".to_string()],
                   "AS OF must consider both, in a stable order");

        // The query path, end to end — this is what actually regressed.
        let (rows, _) = crate::nql::query(&db, &format!("FROM o AS OF {}", gone.seq)).unwrap();
        let ids: Vec<&str> = rows.iter().filter_map(|r| r["_id"].as_str()).collect();
        assert!(ids.contains(&"gone"), "AS OF must see the deleted row: {:?}", ids);
        assert!(ids.contains(&"keep"), "{:?}", ids);

        // And the present must not.
        let (now, _) = crate::nql::query(&db, "FROM o").unwrap();
        let ids: Vec<&str> = now.iter().filter_map(|r| r["_id"].as_str()).collect();
        assert_eq!(ids, vec!["keep"], "a delete still deletes");
    }

    #[test]
    fn a_recreated_id_keeps_the_history_from_before_its_delete() {
        // The edge case the graveyard fallback exists for: a `put` after a
        // delete starts a FRESH chain with no `prev`, so the live chain cannot
        // reach a sequence from before the delete. Only the graveyard can.
        let db = Db::in_memory();
        let old = db.put("o", "a", serde_json::json!({"era": "first"}), vec![], None, None).unwrap();
        db.delete("o", "a").unwrap();
        let new = db.put("o", "a", serde_json::json!({"era": "second"}), vec![], None, None).unwrap();

        assert_eq!(db.get("o", "a").unwrap().data["era"], serde_json::json!("second"));
        assert_eq!(db.get_as_of("o", "a", new.seq).unwrap().data["era"],
                   serde_json::json!("second"));
        assert_eq!(db.get_as_of("o", "a", old.seq).expect("the FIRST era survives").data["era"],
                   serde_json::json!("first"),
                   "re-creating an id must not orphan what came before it");
    }

    #[test]
    fn the_graveyard_survives_a_reopen() {
        // A tombstone pointer lost to a restart would put the history back out
        // of reach — the exact bug, just deferred. So it is flushed with the
        // live index and read back from disk.
        let dir = tempdir().unwrap();
        let seq = {
            let db = Db::open(dir.path(), None).unwrap();
            let v1 = db.put("o", "a", serde_json::json!({"t": 7}), vec![], None, None).unwrap();
            db.delete("o", "a").unwrap();
            db.try_flush_all().expect("flush must succeed");
            v1.seq
        };
        let db = Db::open(dir.path(), None).unwrap();
        assert!(db.get("o", "a").is_none(), "still deleted after a reopen");
        assert_eq!(db.get_as_of("o", "a", seq).expect("history survives a reopen").data["t"],
                   serde_json::json!(7));
        assert_eq!(db.list_ids_including_deleted("o"), vec!["a".to_string()]);
    }

    #[test]
    fn the_graveyard_is_invisible_to_everything_that_enumerates_the_store() {
        // It adds a directory to the data dir, so the risk is that it shows up
        // as a phantom COLLECTION or a phantom OBJECT. Both enumerations are
        // rooted at their own subdirectory rather than at the data dir, which
        // is why it cannot — but that is exactly the kind of reasoning worth
        // pinning, because a stray "graveyard" collection would be nasty and
        // would only surface in someone's UI.
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.put("orders", "a", serde_json::json!({"t": 1}), vec![], None, None).unwrap();
        // A surviving sibling, so the collection is still live after the
        // delete. (With `a` alone, `orders` would have no index entries left
        // and so no directory to enumerate — existing behaviour, unrelated to
        // the graveyard, but it would make this test assert the wrong thing.)
        db.put("orders", "b", serde_json::json!({"t": 2}), vec![], None, None).unwrap();
        db.delete("orders", "a").unwrap();
        db.try_flush_all().unwrap();

        let colls = db.id_index.collections();
        assert!(!colls.iter().any(|c| c == "graveyard"),
                "the graveyard must not look like a collection: {:?}", colls);
        assert_eq!(colls, vec!["orders".to_string()]);

        let (_checked, tampered) = db.verify();
        assert!(tampered.is_empty(), "{:?}", tampered);
    }

    #[test]
    fn a_delete_leaves_the_hash_chain_verifiable() {
        // The graveyard is an index, not a second source of truth: it must not
        // be able to make `verify()` disagree with the objects on disk.
        let db = Db::in_memory();
        db.put("o", "a", serde_json::json!({"t": 1}), vec![], None, None).unwrap();
        db.put("o", "b", serde_json::json!({"t": 2}), vec![], None, None).unwrap();
        db.delete("o", "a").unwrap();
        let (checked, tampered) = db.verify();
        assert!(tampered.is_empty(), "a delete must not break verify(): {:?}", tampered);
        assert!(checked >= 3, "the tombstone is an object too, got {}", checked);
    }

    #[test]
    fn deleting_a_missing_id_stays_a_no_op() {
        let db = Db::in_memory();
        assert!(!db.delete("o", "nope").unwrap(), "nothing to delete");
        assert!(db.list_ids_including_deleted("o").is_empty(),
                "a failed delete must not put anything in the graveyard");
    }

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

    /// A lost id-index WAL must be recoverable: the objects carry coll/id/seq,
    /// so `repair()` can reconstruct every row, and the repaired database must
    /// reopen WARM with a valid head.
    ///
    /// Regression for 2.8.5, where the cold scan rebuilt seq_index, coll_tips,
    /// head and MANIFEST but never the id index — so a database in this state
    /// returned 0 rows from `list()` while `verify()` reported every object
    /// healthy, and `nedb-cli repair` printed success without fixing anything.
    #[test]
    fn repair_rebuilds_id_index_after_lost_wal() {
        let dir = tempdir().unwrap();
        {
            let db = Db::open(dir.path(), None).unwrap();
            for i in 0..25 {
                db.put("rows", &format!("r{}", i), serde_json::json!({"i": i}), vec![], None, None)
                    .unwrap();
            }
            db.put("rows", "r0", serde_json::json!({"i": 0, "v": 2}), vec![], None, None).unwrap();
            db.try_flush_all().unwrap();
        }

        // Simulate the lost WAL: objects survive, the id index does not.
        std::fs::remove_dir_all(dir.path().join("indexes")).unwrap();

        {
            let db = Db::open(dir.path(), None).unwrap();
            assert_eq!(db.list("rows").len(), 0, "precondition: rows unreachable");
            let (ok, bad) = db.verify();
            assert!(ok > 0 && bad.is_empty(), "objects must still be intact and verifying");

            let written = db.repair().unwrap();
            assert_eq!(written, 25, "one entry per distinct (coll, id)");
            assert_eq!(db.list("rows").len(), 25, "every row must come back");

            // The winner for a re-put id is the HIGHEST seq, matching put().
            let r0 = db.get("rows", "r0").expect("r0 present");
            assert_eq!(r0.data.get("v").and_then(|v| v.as_i64()), Some(2),
                "repair must restore the latest version, not an older one");
        }

        // A repaired database must reopen warm with a real head.
        let db3 = Db::open(dir.path(), None).unwrap();
        assert_eq!(db3.list("rows").len(), 25);
        assert!(!db3.head().is_empty(), "repair must leave a valid MANIFEST head");
        assert!(db3.tip_collection("rows").is_some(), "tip_collection must resolve after repair");
    }

    /// `since()` must never report "caught up" while the cursor is behind head.
    ///
    /// Regression for 2.8.5: on a warm boot the seq index is empty by design
    /// (the warm path skips the scan), so every seq lookup missed and `since()`
    /// returned zero nodes with `has_more = false` — identical to genuinely up
    /// to date. A consumer following the documented drain loop stopped one call
    /// in, on a database with every record unread.
    #[test]
    fn since_never_reports_caught_up_while_behind_head() {
        let dir = tempdir().unwrap();
        {
            let db = Db::open(dir.path(), None).unwrap();
            for i in 0..10 {
                db.put("rows", &format!("r{}", i), serde_json::json!({"i": i}), vec![], None, None)
                    .unwrap();
            }
            db.try_flush_all().unwrap();
        }

        // Warm reopen: startup is "complete" in O(1) because the scan is skipped.
        let db2 = Db::open(dir.path(), None).unwrap();
        let st = db2.scan_status();
        assert!(st.tip_seq > 0, "log has entries");
        assert!(
            !st.seq_index_ready,
            "warm boot leaves the seq index cold — that is the honest signal"
        );

        let batch = db2.since(0, 100);
        assert!(
            batch.to_seq < batch.head_seq,
            "cursor is behind the log head in this state"
        );
        assert!(
            batch.has_more,
            "has_more must be true while the cursor is behind head — otherwise the \
             consumer reads 'caught up' and stops with every record unread"
        );

        // After a repair the index resolves and the drain actually completes.
        db2.repair().unwrap();
        assert!(db2.scan_status().seq_index_ready);
        let drained = db2.since(0, 100);
        assert!(!drained.has_more, "genuinely caught up reports has_more=false");

        // KNOWN SHARP EDGE, pinned here deliberately: the cursor is EXCLUSIVE
        // and seqs start at 0, so `since(0, _)` returns (0, head] and the very
        // first write in a database (seq 0) is not reachable through any cursor
        // value. 10 writes therefore drain as 9 records. Changing the cursor
        // convention would break existing replication consumers, so this is
        // documented rather than silently altered — but a replica seeded from
        // since() alone starts one record short.
        assert_eq!(
            drained.nodes.len(),
            9,
            "since(0) is exclusive of seq 0 — see the sharp edge noted above"
        );
        assert!(
            drained.nodes.iter().all(|n| n.seq >= 1),
            "seq 0 is unreachable via since()"
        );
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

    /// Pre-2.5.43 MANIFESTs (no tip_hash) must warm-boot, NOT force a cold
    /// scan. The old "cold scan once to upgrade" policy was hours of random
    /// reads on multi-million-object seek-bound stores (itcd -dagv3), re-paid
    /// on every boot if the process exited before the scan finished. seq+head
    /// in the old MANIFEST are valid; tip()/tip_collection() return None until
    /// the first write+flush organically rewrites MANIFEST with a tip.
    #[test]
    fn pre_durable_tip_manifest_warm_boots_and_heals_lazily() {
        let dir = tempdir().unwrap();
        {
            let db = Db::open(dir.path(), None).unwrap();
            for i in 0..5u64 {
                db.put("things", &i.to_string(), serde_json::json!({"i": i}), vec![], None, None).unwrap();
            }
            db.flush_all();
        }
        // Rewrite MANIFEST in the pre-2.5.43 shape: seq + head only.
        let manifest_path = dir.path().join("MANIFEST");
        let m: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
        let old_format = serde_json::json!({ "seq": m["seq"], "head": m["head"] });
        std::fs::write(&manifest_path, serde_json::to_string(&old_format).unwrap()).unwrap();

        // Reopen: must be WARM (startup_ready immediately — no cold scan gate).
        let db2 = Db::open(dir.path(), None).unwrap();
        assert!(db2.startup_ready.load(std::sync::atomic::Ordering::SeqCst),
                "pre-2.5.43 MANIFEST must warm-boot, not fall to a cold scan");
        // tip() unresolvable this boot — documented None, not a panic or scan.
        assert!(db2.tip().is_none(), "tip() is None until the manifest heals");
        // seq continuity: a new write gets a FRESH seq (no reuse).
        let n = db2.put("things", "next", serde_json::json!({"fresh": true}), vec![], None, None).unwrap();
        assert_eq!(n.seq, m["seq"].as_u64().unwrap(), "next write takes the persisted next-to-assign seq");
        db2.flush_all(); // organic upgrade: MANIFEST now carries tip_hash
        drop(db2);

        // Healed: next boot is warm AND tip() resolves.
        let db3 = Db::open(dir.path(), None).unwrap();
        assert!(db3.startup_ready.load(std::sync::atomic::Ordering::SeqCst));
        let tip = db3.tip().expect("tip() must resolve after the organic upgrade");
        assert_eq!(tip.id, "next");
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

    /// Regression: the flush ticker must NOT pin the database.
    ///
    /// Before this was fixed, `start_manifest_ticker` held a strong `Arc<Db>`
    /// in an unconditional `loop`, so the thread never exited, the `Db` was
    /// never dropped, and the exclusive data-dir `LOCK` from `Db::open` was
    /// never released. Reopening the same path in the SAME PROCESS then failed
    /// with "locked by another process (pid N)" — where N was the caller's own
    /// pid. Live in every release from 2.8.5 through 3.1.0, and invisible
    /// because no CI ran the suite (tests/test_native.py) that hit it.
    ///
    /// Put the strong `Arc` back in the ticker and this test fails.
    #[test]
    fn ticker_does_not_pin_the_db_across_a_reopen() {
        let dir = tempdir().unwrap();
        {
            let db = std::sync::Arc::new(Db::open(dir.path(), None).unwrap());
            Db::start_manifest_ticker(std::sync::Arc::clone(&db), 25);
            db.put("t", "a", serde_json::json!({"v": 1}), vec![], None, None).unwrap();
            // Let the ticker run at least a couple of times while the db lives.
            std::thread::sleep(std::time::Duration::from_millis(90));
        } // last owner dropped here -> Drop flushes -> LOCK released

        // The ticker upgrades its Weak for the duration of a tick, so at any
        // given instant it may legitimately hold a transient strong reference.
        // Release is therefore "eventual, within about one interval", not
        // instantaneous -- poll for it.
        //
        // The first version of this test sampled Arc::strong_count once and
        // asserted it was 1. That passed on an idle machine and failed the
        // first time it met a loaded CI runner, because the sample landed
        // mid-tick. A leak still fails this test deterministically: if the
        // ticker holds a strong Arc forever the LOCK is never released and
        // the deadline expires.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let db2 = loop {
            match Db::open(dir.path(), None) {
                Ok(db) => break db,
                Err(e) => {
                    assert!(std::time::Instant::now() < deadline,
                            "reopen never succeeded -- the ticker is pinning the Db: {e}");
                    std::thread::sleep(std::time::Duration::from_millis(25));
                }
            }
        };
        assert!(db2.get("t", "a").is_some(), "the write survived close/reopen");
    }

    /// The ticker thread must actually terminate, not merely stop pinning.
    #[test]
    fn ticker_thread_exits_when_the_last_owner_drops() {
        let dir = tempdir().unwrap();
        let weak = {
            let db = std::sync::Arc::new(Db::open(dir.path(), None).unwrap());
            Db::start_manifest_ticker(std::sync::Arc::clone(&db), 25);
            db.put("t", "a", serde_json::json!({"v": 1}), vec![], None, None).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(60));
            std::sync::Arc::downgrade(&db)
        };
        // Same reasoning as above: a tick in flight holds a real strong
        // reference for a few microseconds, so this is an eventual property.
        // A genuine leak never releases and blows the deadline.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while weak.upgrade().is_some() {
            assert!(std::time::Instant::now() < deadline,
                    "the Db outlived its last owner — the ticker is leaking it");
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
    }
}
