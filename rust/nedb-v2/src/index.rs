// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

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
    /// Flush the in-memory WAL to disk, reporting I/O failure to the caller.
    /// Every entry is attempted; the first error is returned after the pass.
    ///
    /// DURABILITY INVARIANT: an entry is dropped from `write_buf` ONLY when its
    /// disk write actually succeeded. A failed write (ENOSPC, EIO, EROFS) leaves
    /// the entry buffered so the next tick retries it.
    ///
    /// Before 2.8.6 this cleared the buffer unconditionally, which silently and
    /// permanently discarded acknowledged writes whenever a flush hit a full
    /// disk: `put()` had already returned `Ok` and the content-addressed object
    /// was durable (so `verify()` still counted it), but no id-index entry ever
    /// reached disk — so the row was simply absent on reopen, with no error
    /// anywhere. Reproduced on a full 22 MiB filesystem: 30 rows acknowledged,
    /// `verify()` reported 30 healthy objects, `list()` returned 0.
    pub fn try_flush_write_buf(&self) -> std::io::Result<()> {
        if self.mem.is_some() || self.write_buf.is_empty() { return Ok(()); }
        use rayon::prelude::*;
        // Drain all pending entries and write them in parallel
        let entries: Vec<((String, String), Option<String>)> = self.write_buf
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        let results: Vec<std::io::Result<()>> = entries.par_iter()
            .map(|((coll, id), hash_opt)| -> std::io::Result<()> {
                match hash_opt {
                    Some(hash) => {
                        // Write/update: tmp → rename
                        let path = self.path(coll, id);
                        if let Some(parent) = path.parent() {
                            fs::create_dir_all(parent)?;
                        }
                        let tmp = path.with_extension("tmp");
                        if let Err(e) = fs::write(&tmp, hash) {
                            // A partial/empty tmp must not be left behind on a
                            // full disk — it consumes the very space needed to
                            // retry, and it is not a valid index leaf.
                            let _ = fs::remove_file(&tmp);
                            return Err(e);
                        }
                        if let Err(e) = fs::rename(&tmp, &path) {
                            let _ = fs::remove_file(&tmp);
                            return Err(e);
                        }
                        Ok(())
                    }
                    None => {
                        // Tombstone: remove the file (encoded leaf + legacy raw if distinct).
                        // Already-absent is success — the desired end state holds.
                        let path = self.path(coll, id);
                        match fs::remove_file(&path) {
                            Ok(()) => {}
                            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(e) => return Err(e),
                        }
                        let raw = self.raw_path(coll, id);
                        if raw != path {
                            match fs::remove_file(&raw) {
                                Ok(()) => {}
                                Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {}
                                Err(e) => return Err(e),
                            }
                        }
                        Ok(())
                    }
                }
            })
            .collect();

        // Clear flushed entries — but ONLY when the write SUCCEEDED, and only
        // when the buffered value is still the exact value we flushed. An
        // unconditional remove() here would delete a NEWER value written between
        // the snapshot above and this point: that write would never reach disk
        // (the file holds the stale hash we just wrote) and get() would serve the
        // old version once the buffer check misses — a silent lost update.
        // remove_if closes the race; a newer value simply stays buffered and
        // flushes on the next tick.
        let mut first_err: Option<std::io::Error> = None;
        for ((key, flushed_val), result) in entries.iter().zip(results.iter()) {
            match result {
                Ok(()) => {
                    self.write_buf.remove_if(key, |_, current| current == flushed_val);
                }
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(std::io::Error::new(
                            e.kind(),
                            format!("id-index leaf {}/{}: {}", key.0, key.1, e),
                        ));
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Back-compat wrapper around [`try_flush_write_buf`]: flushes and logs.
    /// Prefer the `try_` form — a swallowed flush error is a lost write.
    pub fn flush_write_buf(&self) {
        if let Err(e) = self.try_flush_write_buf() {
            eprintln!(
                "nedb: id-index flush failed ({}) — affected writes are RETAINED in the WAL and will be retried on the next flush",
                e
            );
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
            // DashMap iteration order is also unspecified — sort here too, so
            // memory mode and disk mode agree.
            let mut ids: Vec<String> = mem.iter()
                .filter(|e| e.key().0 == coll)
                .map(|e| e.key().1.clone())
                .collect();
            ids.sort_unstable();
            return ids;
        }
        // Read from disk then overlay WAL (adds buffered writes, removes tombstones)
        let id_root = self.root.join(coll).join("id");
        // Each entry in id_root is a 2-char hex shard dir
        let mut ids: Vec<String> = fs::read_dir(&id_root)
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
            .collect::<Vec<_>>();

        // Deterministic order. The dedup above runs through HashSet, and Rust
        // seeds its hasher randomly PER PROCESS — so without this the same query
        // over unchanged data returns rows in a different order on every restart:
        //
        //   run 1:  o5 o8 o7 o4 o6 ...
        //   run 2:  o7 o5 o2 o6 o8 ...
        //
        // Cosmetic for a full scan, but not for `LIMIT 5` with no ORDER BY,
        // which then returns an arbitrary 5 of 8 and calls it an answer. It also
        // makes any snapshot/diff test flaky for reasons that look like data
        // corruption.
        //
        // Sorted by id, which for the common case of sequential ids is also
        // insertion order. Callers that want a different order say ORDER BY.
        ids.sort_unstable();
        ids
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
    ///
    /// Overlays the WAL, exactly as `ids()` does. A collection whose first write
    /// is still sitting in `write_buf` has no directory on disk yet, so a
    /// read_dir-only implementation reports it as absent for up to a full flush
    /// tick.
    ///
    /// That was a real bug, and a nasty one because it was invisible to a human
    /// at a terminal: type a PUT, type a query, and the 1s ticker has already
    /// fired in between. Only an automated caller — one that writes and reads in
    /// the same millisecond — ever sees the empty list. It surfaced through
    /// `/cast`, which checks the generated collection against this list and
    /// returned "collection does not exist" for a collection that had just been
    /// written successfully.
    ///
    /// Tombstoned entries are excluded, but only when the collection has no
    /// surviving documents anywhere — a delete of one document must not hide the
    /// whole collection.
    pub fn collections(&self) -> Vec<String> {
        if let Some(ref mem) = self.mem {
            let mut colls: Vec<String> = mem.iter()
                .map(|e| e.key().0.clone())
                .collect::<std::collections::HashSet<_>>()
                .into_iter().collect();
            colls.sort();
            return colls;
        }

        let mut set: std::collections::HashSet<String> = fs::read_dir(&self.root)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();

        // Overlay WAL: a buffered live write makes its collection visible now.
        for e in self.write_buf.iter() {
            if e.value().is_some() {
                set.insert(e.key().0.clone());
            }
        }

        let mut colls: Vec<String> = set.into_iter().collect();
        colls.sort();
        colls
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

    /// Hashes whose indexed value falls within the given bounds.
    ///
    /// The BTreeMap already orders by value, so a bounded predicate is a
    /// range walk rather than a full collection scan. `None` for either bound
    /// means unbounded on that side, which serves a one-sided inequality
    /// (`fee > 10`) as well as a two-sided `BETWEEN`.
    ///
    /// Documents where the field is ABSENT are not in this index at all, and
    /// are therefore not returned. That is correct for every predicate this
    /// serves: a missing field compares as null, which satisfies no ordering
    /// comparison.
    pub fn range(
        &self,
        coll: &str,
        field: &str,
        low: Option<&Value>,
        high: Option<&Value>,
        low_incl: bool,
        high_incl: bool,
    ) -> Vec<String> {
        use std::ops::Bound;
        let key = (coll.to_string(), field.to_string());
        self.inner.get(&key).map(|idx| {
            let lo = match low {
                None => Bound::Unbounded,
                Some(v) => {
                    let ov = OrderedValue::from(v);
                    if low_incl { Bound::Included(ov) } else { Bound::Excluded(ov) }
                }
            };
            let hi = match high {
                None => Bound::Unbounded,
                Some(v) => {
                    let ov = OrderedValue::from(v);
                    if high_incl { Bound::Included(ov) } else { Bound::Excluded(ov) }
                }
            };
            idx.range((lo, hi)).flat_map(|(_, v)| v.iter().cloned()).collect()
        }).unwrap_or_default()
    }

    /// Hashes whose indexed value equals `value` — an O(log n) point lookup,
    /// used for `=` and for each arm of an `IN (...)` list.
    pub fn exact(&self, coll: &str, field: &str, value: &Value) -> Vec<String> {
        let key = (coll.to_string(), field.to_string());
        self.inner.get(&key).map(|idx| {
            idx.get(&OrderedValue::from(value)).cloned().unwrap_or_default()
        }).unwrap_or_default()
    }

    /// How many hashes a range covers, without materialising them.
    ///
    /// Lets the planner compare two candidate indexes and pick the more
    /// selective one, rather than committing to whichever field it saw first.
    pub fn range_len(
        &self,
        coll: &str,
        field: &str,
        low: Option<&Value>,
        high: Option<&Value>,
        low_incl: bool,
        high_incl: bool,
    ) -> usize {
        use std::ops::Bound;
        let key = (coll.to_string(), field.to_string());
        self.inner.get(&key).map(|idx| {
            let lo = match low {
                None => Bound::Unbounded,
                Some(v) => {
                    let ov = OrderedValue::from(v);
                    if low_incl { Bound::Included(ov) } else { Bound::Excluded(ov) }
                }
            };
            let hi = match high {
                None => Bound::Unbounded,
                Some(v) => {
                    let ov = OrderedValue::from(v);
                    if high_incl { Bound::Included(ov) } else { Bound::Excluded(ov) }
                }
            };
            idx.range((lo, hi)).map(|(_, v)| v.len()).sum()
        }).unwrap_or(0)
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

    /// A failed flush must RETAIN the entry for retry, never discard it.
    ///
    /// Regression for the 2.8.5 silent-loss bug: `flush_write_buf` cleared every
    /// snapshotted key regardless of whether its disk write succeeded, so a
    /// flush that hit ENOSPC/EACCES permanently dropped acknowledged writes.
    /// The fault is injected by planting a regular FILE where the collection
    /// directory must go, so `create_dir_all` fails with NotADirectory. That is
    /// deliberately independent of file permissions: containers frequently run
    /// as root or hold `cap_dac_override`, where a chmod-0555 fixture is
    /// silently writable and the test would pass without exercising anything.
    #[test]
    fn failed_flush_retains_entries_for_retry() {
        let dir = tempdir().unwrap();
        let idx = IdIndex::new(dir.path()).unwrap();

        idx.set("rows", "a", "hash_a").unwrap();
        idx.set("rows", "b", "hash_b").unwrap();

        // Block the leaf path: `indexes/rows` is a file, so the index cannot
        // create `indexes/rows/id/<shard>/` beneath it.
        let blocker = dir.path().join("indexes").join("rows");
        fs::write(&blocker, b"not a directory").unwrap();

        let result = idx.try_flush_write_buf();
        assert!(
            result.is_err(),
            "flush must report the I/O failure, got Ok — callers cannot detect lost writes"
        );

        // THE INVARIANT: the entries are still buffered, so a later flush retries.
        assert_eq!(
            idx.write_buf.len(),
            2,
            "failed flush discarded buffered writes — acknowledged data would be lost"
        );

        // Clear the fault and retry: the writes must now land.
        fs::remove_file(&blocker).unwrap();
        idx.try_flush_write_buf()
            .expect("retry after the fault clears must succeed");
        assert!(idx.write_buf.is_empty(), "successful flush must drain the WAL");

        // Reopen from disk only — proves the retry actually persisted.
        let idx2 = IdIndex::new(dir.path()).unwrap();
        assert_eq!(idx2.get("rows", "a").as_deref(), Some("hash_a"));
        assert_eq!(idx2.get("rows", "b").as_deref(), Some("hash_b"));
    }

    /// A successful flush still drains the buffer and reports Ok.
    /// Guards the false-positive direction: the new error path must not make
    /// healthy flushes look like failures.
    #[test]
    fn successful_flush_reports_ok_and_drains() {
        let dir = tempdir().unwrap();
        let idx = IdIndex::new(dir.path()).unwrap();
        for i in 0..64 {
            idx.set("rows", &format!("id{}", i), &format!("h{}", i)).unwrap();
        }
        idx.try_flush_write_buf().expect("healthy flush must be Ok");
        assert!(idx.write_buf.is_empty());
        let idx2 = IdIndex::new(dir.path()).unwrap();
        assert_eq!(idx2.get("rows", "id63").as_deref(), Some("h63"));
    }

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

    /// Regression stress test for the flush_write_buf lost-update race.
    ///
    /// Old behavior: flush snapshotted the buffer, wrote files in parallel, then
    /// UNCONDITIONALLY removed each snapshotted key. A set() landing between the
    /// snapshot and the remove was deleted from the buffer without ever being
    /// flushed — disk kept the stale hash and (with no later write to re-insert
    /// the key) the newer value was lost forever.
    ///
    /// Shape: every key is written exactly twice (v1 then v2) while a flusher
    /// thread spins. Under the old code, keys whose v1 was snapshotted and whose
    /// v2 arrived during the parallel disk-write phase get their v2 dropped by
    /// the unconditional remove — the final assert catches them on disk at v1.
    /// With remove_if, a superseded snapshot entry leaves the newer value
    /// buffered for the next flush, so every key must read v2 at the end.
    /// (Probabilistic by nature, but the race window — thousands of parallel
    /// file writes — is wide; with 2000 keys the old code fails reliably.)
    #[test]
    fn flush_never_drops_a_concurrent_newer_write() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let dir = tempdir().unwrap();
        let idx = Arc::new(IdIndex::new(dir.path()).unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        const N: usize = 2000;

        let flusher = {
            let idx = Arc::clone(&idx);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    idx.flush_write_buf();
                }
            })
        };

        // v1 for every key, then v2 for every key — the flusher races both passes.
        for i in 0..N {
            idx.set("c", &format!("k{}", i), "v1").unwrap();
        }
        for i in 0..N {
            idx.set("c", &format!("k{}", i), "v2").unwrap();
        }

        stop.store(true, Ordering::Relaxed);
        flusher.join().unwrap();
        // Drain anything still buffered (remove_if leaves superseded entries in).
        idx.flush_write_buf();
        idx.flush_write_buf();

        // Every key must be v2 — from this handle AND from a cold reopen (disk).
        for i in 0..N {
            let k = format!("k{}", i);
            assert_eq!(idx.get("c", &k), Some("v2".to_string()),
                       "key {} lost its newer write (buffer path)", k);
        }
        let cold = IdIndex::new(dir.path()).unwrap();
        for i in 0..N {
            let k = format!("k{}", i);
            assert_eq!(cold.get("c", &k), Some("v2".to_string()),
                       "key {} lost its newer write (disk path)", k);
        }
    }

    /// Regression: a collection must be visible the instant it is written, not
    /// one flush tick later.
    ///
    /// Old behavior: `collections()` did a bare `read_dir` of the object root. A
    /// brand-new collection lives only in `write_buf` until the 1s ticker fires,
    /// so it was reported as ABSENT for up to a full second after a successful
    /// write. Every other read path (`get`, `list_ids`) already overlaid the WAL;
    /// this one silently did not.
    ///
    /// Why it hid for so long: a human at a terminal cannot reproduce it. Typing
    /// a PUT and then a query leaves hundreds of milliseconds in between, and the
    /// ticker has already run. Only a caller that writes and reads within the
    /// same millisecond sees the empty list — which is exactly what an automated
    /// test does. It surfaced through `/cast`, which validates the model's chosen
    /// collection against this list and rejected a collection that had just been
    /// written.
    ///
    /// NOTE the deliberate absence of any flush below. Calling flush_write_buf()
    /// here would make this test pass against the OLD code and assert nothing.
    #[test]
    fn collections_are_visible_before_flush() {
        let dir = tempdir().unwrap();
        let idx = IdIndex::new(dir.path()).unwrap();

        idx.set("orders", "o1", "hash1").unwrap();
        let colls = idx.collections();
        assert!(
            colls.contains(&"orders".to_string()),
            "collection invisible before flush: {colls:?}"
        );

        // Still correct once it does reach disk — no duplicates from the overlay.
        idx.flush_write_buf();
        let after = idx.collections();
        assert_eq!(after, vec!["orders".to_string()], "after flush: {after:?}");

        // Second collection, same story, and the first must not vanish.
        idx.set("stylists", "s1", "hash2").unwrap();
        let both = idx.collections();
        assert_eq!(both, vec!["orders".to_string(), "stylists".to_string()],
                   "expected both collections, got {both:?}");
    }

    /// Regression: `list_ids` must return a STABLE order.
    ///
    /// The dedup path runs through `HashSet`, and Rust seeds its hasher randomly
    /// per process. Observed on a real daemon — same query, same data, three
    /// consecutive runs:
    ///
    /// ```text
    ///   o5 o8 o7 o4 o6 ...
    ///   o7 o5 o2 o6 o8 ...
    ///   o6 o1 o5 o4 o7 ...
    /// ```
    ///
    /// Cosmetic on a full scan. NOT cosmetic for `LIMIT 5` with no `ORDER BY`,
    /// which then hands back an arbitrary 5 of 8 as though it were an answer.
    ///
    /// NOTE the id set below. Sequential ids (`o1`..`o8`) can land in a
    /// consistent order by chance, which would let this pass against the old
    /// code. These are deliberately hash-scattered strings, and 24 of them, so a
    /// single unsorted run being accidentally sorted is vanishingly unlikely.
    #[test]
    fn list_ids_order_is_stable() {
        let dir = tempdir().unwrap();
        let idx = IdIndex::new(dir.path()).unwrap();

        let ids: Vec<String> = (0..24).map(|i| format!("zq{:x}-{}", i * 7919, i)).collect();
        for id in &ids {
            idx.set("orders", id, "h").unwrap();
        }

        // Buffered (pre-flush) and on-disk (post-flush) must BOTH be sorted, and
        // must agree with each other — a flush is not a reordering event.
        let mut want = ids.clone();
        want.sort_unstable();

        let before = idx.list_ids("orders");
        assert_eq!(before, want, "unsorted before flush");

        idx.flush_write_buf();
        let after = idx.list_ids("orders");
        assert_eq!(after, want, "unsorted after flush");
        assert_eq!(before, after, "flush changed the order");

        // Repeat reads within a process must not drift either.
        for _ in 0..5 {
            assert_eq!(idx.list_ids("orders"), want, "order varied between reads");
        }
    }

    /// A tombstoned document must not resurrect its collection.
    #[test]
    fn collections_excludes_tombstone_only_writes() {
        let dir = tempdir().unwrap();
        let idx = IdIndex::new(dir.path()).unwrap();
        idx.remove("ghosts", "g1").unwrap();
        let colls = idx.collections();
        assert!(!colls.contains(&"ghosts".to_string()),
                "a tombstone conjured a collection: {colls:?}");
    }

}
