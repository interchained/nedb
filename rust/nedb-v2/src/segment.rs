//! segment.rs — NEDB v3 packed object substrate.
//!
//! v3 keeps the v2 logical model intact (content-addressed, immutable,
//! BLAKE2b-verified DAG nodes) and changes only *where the bytes live*: instead
//! of one filesystem object per node — which caps throughput at the OS
//! small-file metadata rate — many immutable objects are appended into
//! **segment files** addressed through an in-memory `hash -> (segment, offset,
//! len)` index. The hash is still `BLAKE2b(content)` and is re-verified on every
//! read, so content-addressing and tamper-evidence are unchanged.
//!
//! This module knows nothing about `Node`, JSON, or encryption: callers
//! (`ObjectStore`) pass already-serialized/encrypted `content` bytes and the
//! precomputed hash. That keeps the segment store a pure content<->location
//! layer and leaves all crypto/serialization in `store.rs`.
//!
//! Phases:
//!   1. Segment append + in-memory index + startup scan + tail-truncation.
//!   2. Compaction/pruning — rewrite the live object set into fresh segments,
//!      reclaiming dead (superseded/spent) records. `compact(live)`.
//!   3. On-disk `.idx` sidecars — a sealed segment gets a checksummed
//!      `hash -> (offset,len)` index file so cold start loads it instead of
//!      rescanning every record. Missing/corrupt `.idx` falls back to a scan.
//!
//! Opt-in only: `ObjectStore` instantiates this when `NEDB_DAG_V3` is set
//! (surfaced as the `--dag-v3` flag). Default storage is byte-for-byte v2.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{bail, Context, Result};
use blake2::{Blake2b512, Digest};
use dashmap::DashMap;

/// Default segment rollover size (256 MiB).
const DEFAULT_MAX_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;

/// Magic prefix for `.idx` sidecar files (NEDB v3 index, format 1).
const IDX_MAGIC: &[u8; 4] = b"NIX1";
/// `.idx` on-disk entry: 32-byte raw digest + u64 offset + u32 len.
const IDX_ENTRY_BYTES: usize = 32 + 8 + 4;
/// `.idx` header: magic(4) + count(8). Trailer: blake2b-256 checksum (32).
const IDX_HEADER_BYTES: usize = 4 + 8;
const IDX_CHECKSUM_BYTES: usize = 32;

/// Location of one content record inside the segment set.
#[derive(Clone, Copy, Debug)]
struct SegmentLocation {
    segment_id: u32,
    /// Byte offset of the CONTENT (immediately after the u32 length prefix).
    offset: u64,
    len: u32,
}

/// Result of a `compact()` pass.
#[derive(Clone, Copy, Debug, Default)]
pub struct CompactStats {
    /// Live objects copied forward into fresh segments.
    pub live_objects: usize,
    /// Dead objects dropped (superseded versions / pruned history).
    pub dropped_objects: usize,
    /// Bytes reclaimed by deleting the old segment files.
    pub bytes_reclaimed: u64,
    /// Number of segment files after compaction.
    pub segments_after: usize,
}

/// BLAKE2b-256 (first 32 bytes of Blake2b-512) raw digest.
fn blake2b_raw(data: &[u8]) -> [u8; 32] {
    let mut h = Blake2b512::new();
    h.update(data);
    let out = h.finalize();
    let mut a = [0u8; 32];
    a.copy_from_slice(&out[..32]);
    a
}

/// Hex-encoded BLAKE2b-256. MUST match `store::blake2b` so segment hashes equal
/// loose-object hashes.
fn blake2b(data: &[u8]) -> String {
    hex::encode(blake2b_raw(data))
}

/// The currently-appended-to segment.
struct Active {
    id: u32,
    file: File,
    /// End-of-file = next append position (kept in sync with the file cursor).
    offset: u64,
}

/// Append-only, content-addressed packed object store with an in-memory index.
pub struct SegmentStore {
    dir: PathBuf,
    index: DashMap<String, SegmentLocation>,
    active: Mutex<Active>,
    max_segment_bytes: u64,
    /// macOS fast-fsync opt-in (from `NEDB_FAST_FSYNC`). When true, durability
    /// points use a plain `fsync(2)` instead of std's `sync_all` (= F_FULLFSYNC
    /// on macOS). Off by default → identical, full-durability behavior.
    fast_fsync: bool,
}

/// Durability point for a segment file.
///
/// Default (`fast = false`) is `File::sync_all()` everywhere — the safe,
/// power-loss-durable choice. On macOS that maps to `F_FULLFSYNC` (a full
/// hardware-cache flush to platter), which is often 10-100x slower than the
/// plain `fsync(2)` Linux/Windows already use, especially on Fusion/SATA disks.
///
/// With `fast = true` (set via `NEDB_FAST_FSYNC`), macOS uses plain `fsync(2)`:
/// the write reaches the drive (crash-safe) but the drive may still hold it in a
/// volatile cache, so a sudden power cut could lose the last unsynced batch. NEDB
/// v3 tolerates that — content-addressed objects, torn-tail truncation on open,
/// and a reconstructible chainstate (re-sync from peers) — which is exactly why
/// Bitcoin Core itself uses plain fsync for LevelDB on macOS. On non-macOS the
/// flag is a no-op (`sync_all` is already a plain fsync / FlushFileBuffers).
fn durable_sync(file: &File, fast: bool) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    if fast {
        use std::os::unix::io::AsRawFd;
        // SAFETY: `file` is a valid, open descriptor for the duration of the call.
        let rc = unsafe { libc::fsync(file.as_raw_fd()) };
        return if rc == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) };
    }
    #[cfg(not(target_os = "macos"))]
    let _ = fast;
    file.sync_all()
}

impl SegmentStore {
    fn seg_path(dir: &Path, id: u32) -> PathBuf {
        dir.join(format!("seg-{:06}.dat", id))
    }
    fn idx_path(dir: &Path, id: u32) -> PathBuf {
        dir.join(format!("seg-{:06}.idx", id))
    }

    /// Open (or create) the segment store under `{objects_root}/segments`.
    pub fn open(objects_root: &Path) -> Result<Self> {
        Self::open_with_max(objects_root, DEFAULT_MAX_SEGMENT_BYTES)
    }

    /// Like `open`, with an explicit rollover size (used by tests).
    pub fn open_with_max(objects_root: &Path, max_segment_bytes: u64) -> Result<Self> {
        let dir = objects_root.join("segments");
        fs::create_dir_all(&dir).context("create objects/segments dir")?;

        // Discover existing segment ids.
        let mut ids: Vec<u32> = Vec::new();
        for entry in fs::read_dir(&dir).context("read segments dir")? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(rest) = name.strip_prefix("seg-") {
                if let Some(num) = rest.strip_suffix(".dat") {
                    if let Ok(id) = num.parse::<u32>() {
                        ids.push(id);
                    }
                }
            }
        }
        ids.sort_unstable();

        let index: DashMap<String, SegmentLocation> = DashMap::new();
        let mut active_id: u32 = 0;
        let mut active_end: u64 = 0;

        for (pos, &id) in ids.iter().enumerate() {
            let is_last = pos + 1 == ids.len();
            if is_last {
                // Active (last) segment: always scan (no .idx — it's still
                // mutable) and truncate any torn tail from a crash.
                let (valid_end, entries) = Self::scan_segment(&dir, id)?;
                for (h, o, l) in entries {
                    index.insert(h, SegmentLocation { segment_id: id, offset: o, len: l });
                }
                let path = Self::seg_path(&dir, id);
                let file_len = fs::metadata(&path)?.len();
                if valid_end < file_len {
                    let f = OpenOptions::new().write(true).open(&path)?;
                    f.set_len(valid_end)?;
                }
                active_id = id;
                active_end = valid_end;
            } else {
                // Sealed segment: load its checksummed .idx if present+valid;
                // otherwise scan it and heal by writing a fresh .idx.
                match Self::load_idx(&dir, id) {
                    Ok(Some(entries)) => {
                        for (h, o, l) in entries {
                            index.insert(h, SegmentLocation { segment_id: id, offset: o, len: l });
                        }
                    }
                    _ => {
                        let (_ve, entries) = Self::scan_segment(&dir, id)?;
                        for (h, o, l) in &entries {
                            index.insert(h.clone(), SegmentLocation { segment_id: id, offset: *o, len: *l });
                        }
                        let _ = Self::write_idx(&dir, id, &entries); // best-effort heal
                    }
                }
            }
        }

        // Open (creating if necessary) the active segment for appending.
        let active_path = Self::seg_path(&dir, active_id);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&active_path)
            .with_context(|| format!("open active segment {:?}", active_path))?;
        file.seek(SeekFrom::Start(active_end))?;

        let fast_fsync = std::env::var("NEDB_FAST_FSYNC")
            .map(|v| {
                let v = v.trim();
                v == "1" || v.eq_ignore_ascii_case("true")
                         || v.eq_ignore_ascii_case("on")
                         || v.eq_ignore_ascii_case("yes")
            })
            .unwrap_or(false);

        Ok(Self {
            dir,
            index,
            active: Mutex::new(Active { id: active_id, file, offset: active_end }),
            max_segment_bytes,
            fast_fsync,
        })
    }

    /// Scan one segment, returning (valid_end_offset, records). Records past a
    /// torn tail are not returned; `valid_end` marks where to truncate.
    fn scan_segment(dir: &Path, id: u32) -> Result<(u64, Vec<(String, u64, u32)>)> {
        let path = Self::seg_path(dir, id);
        let mut f = match File::open(&path) {
            Ok(f) => f,
            Err(_) => return Ok((0, Vec::new())),
        };
        let file_len = f.metadata()?.len();
        let mut pos: u64 = 0;
        let mut entries: Vec<(String, u64, u32)> = Vec::new();
        loop {
            if pos + 4 > file_len {
                break; // no room for a length prefix → torn tail
            }
            f.seek(SeekFrom::Start(pos))?;
            let mut len_buf = [0u8; 4];
            if f.read_exact(&mut len_buf).is_err() {
                break;
            }
            let len = u32::from_le_bytes(len_buf);
            let content_off = pos + 4;
            if content_off + (len as u64) > file_len {
                break; // declared length overruns EOF → torn content
            }
            let mut content = vec![0u8; len as usize];
            if f.read_exact(&mut content).is_err() {
                break;
            }
            entries.push((blake2b(&content), content_off, len));
            pos = content_off + len as u64;
        }
        Ok((pos, entries))
    }

    /// Read+verify the raw content at a location. Errors on tamper.
    fn read_content(dir: &Path, loc: &SegmentLocation, expect_hash: &str) -> Result<Vec<u8>> {
        let path = Self::seg_path(dir, loc.segment_id);
        let mut f = File::open(&path).with_context(|| format!("open segment {:?}", path))?;
        f.seek(SeekFrom::Start(loc.offset))?;
        let mut content = vec![0u8; loc.len as usize];
        f.read_exact(&mut content)
            .with_context(|| format!("read record from segment {}", loc.segment_id))?;
        let actual = blake2b(&content);
        if actual != expect_hash {
            bail!("segment object {} tampered: recomputed {}", expect_hash, actual);
        }
        Ok(content)
    }

    // ── Phase 3: .idx sidecars ────────────────────────────────────────────────

    /// Write a checksummed `.idx` for a SEALED segment (atomic via tmp→rename).
    /// Best-effort: a failure just means the next open scans the segment.
    fn write_idx(dir: &Path, id: u32, entries: &[(String, u64, u32)]) -> Result<()> {
        let mut body: Vec<u8> = Vec::with_capacity(IDX_HEADER_BYTES + entries.len() * IDX_ENTRY_BYTES);
        body.extend_from_slice(IDX_MAGIC);
        body.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for (hash, off, len) in entries {
            let raw = hex::decode(hash).map_err(|_| anyhow::anyhow!("bad hash hex in idx write"))?;
            if raw.len() != 32 {
                bail!("idx write: hash not 32 bytes");
            }
            body.extend_from_slice(&raw);
            body.extend_from_slice(&off.to_le_bytes());
            body.extend_from_slice(&len.to_le_bytes());
        }
        let checksum = blake2b_raw(&body);
        body.extend_from_slice(&checksum);

        let path = Self::idx_path(dir, id);
        let tmp = path.with_extension("idx.tmp");
        fs::write(&tmp, &body)?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Load a `.idx` if present and checksum-valid. Returns Ok(None) if absent
    /// or in any way unusable (caller then scans the segment).
    fn load_idx(dir: &Path, id: u32) -> Result<Option<Vec<(String, u64, u32)>>> {
        let path = Self::idx_path(dir, id);
        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(_) => return Ok(None),
        };
        if data.len() < IDX_HEADER_BYTES + IDX_CHECKSUM_BYTES {
            return Ok(None);
        }
        if &data[0..4] != IDX_MAGIC {
            return Ok(None);
        }
        let count = u64::from_le_bytes(data[4..12].try_into().unwrap()) as usize;
        let expected = IDX_HEADER_BYTES + count * IDX_ENTRY_BYTES + IDX_CHECKSUM_BYTES;
        if data.len() != expected {
            return Ok(None);
        }
        let body = &data[..data.len() - IDX_CHECKSUM_BYTES];
        let stored: [u8; 32] = match data[data.len() - IDX_CHECKSUM_BYTES..].try_into() {
            Ok(a) => a,
            Err(_) => return Ok(None),
        };
        if blake2b_raw(body) != stored {
            return Ok(None); // corrupt/stale → fall back to scan
        }
        let mut entries = Vec::with_capacity(count);
        let mut p = IDX_HEADER_BYTES;
        for _ in 0..count {
            let hash = hex::encode(&data[p..p + 32]);
            let off = u64::from_le_bytes(data[p + 32..p + 40].try_into().unwrap());
            let len = u32::from_le_bytes(data[p + 40..p + 44].try_into().unwrap());
            entries.push((hash, off, len));
            p += IDX_ENTRY_BYTES;
        }
        Ok(Some(entries))
    }

    /// Collect the index entries belonging to one segment (for sealing → .idx).
    fn entries_for_segment(&self, id: u32) -> Vec<(String, u64, u32)> {
        self.index
            .iter()
            .filter(|e| e.value().segment_id == id)
            .map(|e| (e.key().clone(), e.value().offset, e.value().len))
            .collect()
    }

    // ── core API ──────────────────────────────────────────────────────────────

    /// True if this hash is already stored in a segment.
    pub fn contains(&self, hash: &str) -> bool {
        self.index.contains_key(hash)
    }

    /// Append `content` under `hash` (idempotent). `hash` must equal
    /// `BLAKE2b(content)`; the caller computes it (parallel, outside the lock).
    pub fn put(&self, hash: &str, content: &[u8]) -> Result<()> {
        if self.index.contains_key(hash) {
            return Ok(());
        }
        let len = content.len() as u32;
        let record_size = 4u64 + content.len() as u64;

        let mut active = self.active.lock().unwrap();
        if self.index.contains_key(hash) {
            return Ok(());
        }

        // Roll over if this record would push the active segment past the cap.
        if active.offset > 0 && active.offset + record_size > self.max_segment_bytes {
            let _ = active.file.flush();
            let _ = durable_sync(&active.file, self.fast_fsync);
            // Seal: write the .idx for the segment we're leaving behind.
            let sealed_id = active.id;
            let entries = self.entries_for_segment(sealed_id);
            let _ = Self::write_idx(&self.dir, sealed_id, &entries);
            let next_id = sealed_id + 1;
            let path = Self::seg_path(&self.dir, next_id);
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(&path)
                .with_context(|| format!("open new segment {:?}", path))?;
            *active = Active { id: next_id, file, offset: 0 };
        }

        let content_off = active.offset + 4;
        let mut rec = Vec::with_capacity(4 + content.len());
        rec.extend_from_slice(&len.to_le_bytes());
        rec.extend_from_slice(content);
        active.file.write_all(&rec)?;

        let seg_id = active.id;
        active.offset += record_size;
        self.index.insert(
            hash.to_string(),
            SegmentLocation { segment_id: seg_id, offset: content_off, len },
        );
        Ok(())
    }

    /// Read the raw content bytes for `hash`, or `None` if not stored in any
    /// segment (caller then falls back to the loose-object path). Re-verifies.
    pub fn get(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        let loc = match self.index.get(hash) {
            Some(entry) => *entry.value(),
            None => return Ok(None),
        };
        Ok(Some(Self::read_content(&self.dir, &loc, hash)?))
    }

    /// All hashes currently stored in segments.
    pub fn all_hashes(&self) -> Vec<String> {
        self.index.iter().map(|e| e.key().clone()).collect()
    }

    /// Flush + fsync the active segment. One durability point per batch.
    pub fn sync(&self) -> Result<()> {
        let mut active = self.active.lock().unwrap();
        let _ = active.file.flush();
        durable_sync(&active.file, self.fast_fsync).context("fsync active segment")?;
        Ok(())
    }

    // ── Phase 2: compaction / pruning ─────────────────────────────────────────

    /// Rewrite the **live** object set into fresh segments and drop everything
    /// else, reclaiming dead (superseded/spent/pruned) records.
    ///
    /// `live` is the set of hashes to KEEP — typically the current version of
    /// every document (from the id-index). Hashes not in `live` are pruned, so
    /// historical versions / AS OF / TRACE for dropped objects are discarded by
    /// design (that is what reclaims space).
    ///
    /// Crash-safe: new segments are written + fsynced BEFORE any old segment is
    /// deleted, so live data is never lost. A crash mid-compaction leaves both
    /// the old and new segments (a re-open re-indexes the union — dead objects
    /// merely linger until the next compaction); it never loses a live object.
    ///
    /// Must be called when the store is quiescent (no concurrent reads): writes
    /// are blocked for the duration via the active lock, and the in-memory index
    /// is swapped in place.
    pub fn compact(&self, live: &HashSet<String>) -> Result<CompactStats> {
        let mut active = self.active.lock().unwrap();

        let total_before = self.index.len();
        let old_max = active.id;
        let new_base = old_max + 1;

        // Snapshot the live entries to copy forward.
        let to_copy: Vec<(String, SegmentLocation)> = self
            .index
            .iter()
            .filter(|e| live.contains(e.key()))
            .map(|e| (e.key().clone(), *e.value()))
            .collect();

        // Write live objects into fresh segments starting at new_base.
        let new_index: DashMap<String, SegmentLocation> = DashMap::new();
        let mut cur_id = new_base;
        let mut cur_path = Self::seg_path(&self.dir, cur_id);
        let mut cur_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&cur_path)
            .with_context(|| format!("open compaction segment {:?}", cur_path))?;
        let mut cur_off: u64 = 0;

        for (hash, loc) in &to_copy {
            let content = Self::read_content(&self.dir, loc, hash)?;
            let len = content.len() as u32;
            let record_size = 4u64 + content.len() as u64;

            if cur_off > 0 && cur_off + record_size > self.max_segment_bytes {
                let _ = cur_file.flush();
                durable_sync(&cur_file, self.fast_fsync).context("fsync sealed compaction segment")?;
                let entries: Vec<(String, u64, u32)> = new_index
                    .iter()
                    .filter(|e| e.value().segment_id == cur_id)
                    .map(|e| (e.key().clone(), e.value().offset, e.value().len))
                    .collect();
                let _ = Self::write_idx(&self.dir, cur_id, &entries);
                cur_id += 1;
                cur_path = Self::seg_path(&self.dir, cur_id);
                cur_file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .write(true)
                    .open(&cur_path)
                    .with_context(|| format!("open compaction segment {:?}", cur_path))?;
                cur_off = 0;
            }

            let content_off = cur_off + 4;
            let mut rec = Vec::with_capacity(4 + content.len());
            rec.extend_from_slice(&len.to_le_bytes());
            rec.extend_from_slice(&content);
            cur_file.write_all(&rec)?;
            new_index.insert(hash.clone(), SegmentLocation { segment_id: cur_id, offset: content_off, len });
            cur_off += record_size;
        }
        let _ = cur_file.flush();
        durable_sync(&cur_file, self.fast_fsync).context("fsync active compaction segment")?;

        // The last new segment becomes the active one (reuse its handle).
        let live_objects = to_copy.len();

        // Swap the in-memory index to the rebuilt one.
        self.index.clear();
        for e in new_index.iter() {
            self.index.insert(e.key().clone(), *e.value());
        }
        *active = Active { id: cur_id, file: cur_file, offset: cur_off };

        // Delete every old segment (id < new_base) + its .idx, after the new
        // ones are durable. Re-list so we only touch files that actually exist.
        let mut bytes_reclaimed: u64 = 0;
        if let Ok(rd) = fs::read_dir(&self.dir) {
            for entry in rd.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let id_of = name
                    .strip_prefix("seg-")
                    .and_then(|r| r.strip_suffix(".dat").or_else(|| r.strip_suffix(".idx")))
                    .and_then(|n| n.parse::<u32>().ok());
                if let Some(id) = id_of {
                    if id < new_base {
                        if name.ends_with(".dat") {
                            if let Ok(m) = entry.metadata() {
                                bytes_reclaimed += m.len();
                            }
                        }
                        let _ = fs::remove_file(entry.path());
                    }
                }
            }
        }

        let segments_after = (cur_id - new_base + 1) as usize;
        Ok(CompactStats {
            live_objects,
            dropped_objects: total_before.saturating_sub(live_objects),
            bytes_reclaimed,
            segments_after,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn put_get_hash(s: &SegmentStore, content: &[u8]) -> String {
        let h = blake2b(content);
        s.put(&h, content).unwrap();
        h
    }

    #[test]
    fn put_get_roundtrip() {
        let dir = tempdir().unwrap();
        let s = SegmentStore::open(dir.path()).unwrap();
        let h = put_get_hash(&s, b"hello nedb v3");
        assert_eq!(s.get(&h).unwrap().unwrap(), b"hello nedb v3");
        assert!(s.contains(&h));
        assert!(s.get(&"0".repeat(64)).unwrap().is_none());
    }

    #[test]
    fn idempotent_put() {
        let dir = tempdir().unwrap();
        let s = SegmentStore::open(dir.path()).unwrap();
        let h1 = put_get_hash(&s, b"dup");
        let h2 = put_get_hash(&s, b"dup");
        assert_eq!(h1, h2);
        assert_eq!(s.all_hashes().len(), 1);
    }

    #[test]
    fn index_rebuilt_on_reopen() {
        let dir = tempdir().unwrap();
        let h = {
            let s = SegmentStore::open(dir.path()).unwrap();
            let h = put_get_hash(&s, b"persisted");
            s.sync().unwrap();
            h
        };
        let s2 = SegmentStore::open(dir.path()).unwrap();
        assert_eq!(s2.get(&h).unwrap().unwrap(), b"persisted");
    }

    #[test]
    fn rollover_writes_idx_and_reopen_uses_it() {
        let dir = tempdir().unwrap();
        let s = SegmentStore::open_with_max(dir.path(), 32).unwrap();
        let mut hashes = Vec::new();
        for i in 0..8u32 {
            hashes.push(put_get_hash(&s, format!("record-{}", i).as_bytes()));
        }
        s.sync().unwrap();
        // Rollover should have produced sealed .idx sidecars.
        let idx_files = fs::read_dir(dir.path().join("segments"))
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().ends_with(".idx"))
            .count();
        assert!(idx_files >= 1, "expected at least one sealed .idx");
        // Reopen (loads sealed segments via .idx) — every object still reads.
        let s2 = SegmentStore::open(dir.path()).unwrap();
        for h in &hashes {
            assert!(s2.get(h).unwrap().is_some());
        }
    }

    #[test]
    fn corrupt_idx_falls_back_to_scan() {
        let dir = tempdir().unwrap();
        let mut hashes = Vec::new();
        {
            let s = SegmentStore::open_with_max(dir.path(), 32).unwrap();
            for i in 0..6u32 {
                hashes.push(put_get_hash(&s, format!("rec-{}", i).as_bytes()));
            }
            s.sync().unwrap();
        }
        // Corrupt every .idx (truncate to garbage). Reopen must still work via scan.
        for e in fs::read_dir(dir.path().join("segments")).unwrap().flatten() {
            if e.file_name().to_string_lossy().ends_with(".idx") {
                fs::write(e.path(), b"garbage").unwrap();
            }
        }
        let s2 = SegmentStore::open(dir.path()).unwrap();
        for h in &hashes {
            assert!(s2.get(h).unwrap().is_some(), "scan fallback must recover the object");
        }
    }

    #[test]
    fn torn_tail_is_truncated_on_open() {
        let dir = tempdir().unwrap();
        let good = {
            let s = SegmentStore::open(dir.path()).unwrap();
            let h = put_get_hash(&s, b"good record");
            s.sync().unwrap();
            h
        };
        let seg = dir.path().join("segments").join("seg-000000.dat");
        {
            let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
            f.write_all(&9999u32.to_le_bytes()).unwrap();
            f.write_all(b"short").unwrap();
        }
        let s2 = SegmentStore::open(dir.path()).unwrap();
        assert_eq!(s2.get(&good).unwrap().unwrap(), b"good record");
        let h2 = put_get_hash(&s2, b"after recovery");
        assert!(s2.get(&h2).unwrap().is_some());
    }

    #[test]
    fn tamper_detected_on_read() {
        let dir = tempdir().unwrap();
        let h = {
            let s = SegmentStore::open(dir.path()).unwrap();
            let h = put_get_hash(&s, b"authentic");
            s.sync().unwrap();
            h
        };
        let seg = dir.path().join("segments").join("seg-000000.dat");
        let mut bytes = fs::read(&seg).unwrap();
        let n = bytes.len();
        bytes[n - 1] ^= 0xff;
        fs::write(&seg, bytes).unwrap();
        let s2 = SegmentStore::open(dir.path()).unwrap();
        match s2.get(&h) {
            Ok(None) => {}
            Err(_) => {}
            Ok(Some(_)) => panic!("tampered content must not verify under original hash"),
        }
    }

    #[test]
    fn compaction_keeps_live_drops_dead() {
        let dir = tempdir().unwrap();
        let s = SegmentStore::open(dir.path()).unwrap();
        let keep = put_get_hash(&s, b"keep me");
        let _drop1 = put_get_hash(&s, b"drop me 1");
        let _drop2 = put_get_hash(&s, b"drop me 2");
        s.sync().unwrap();
        assert_eq!(s.all_hashes().len(), 3);

        let mut live = HashSet::new();
        live.insert(keep.clone());
        let stats = s.compact(&live).unwrap();
        assert_eq!(stats.live_objects, 1);
        assert_eq!(stats.dropped_objects, 2);

        // Live object survives; dead ones are gone.
        assert_eq!(s.get(&keep).unwrap().unwrap(), b"keep me");
        assert_eq!(s.all_hashes().len(), 1);

        // And it survives a reopen (new segments + index swap persisted).
        let s2 = SegmentStore::open(dir.path()).unwrap();
        assert_eq!(s2.get(&keep).unwrap().unwrap(), b"keep me");
        assert!(s2.get(&_drop1).unwrap().is_none());

        // Writes still work after compaction.
        let after = put_get_hash(&s, b"post-compaction");
        assert!(s.get(&after).unwrap().is_some());
    }

    #[test]
    fn compaction_reclaims_and_writes_still_read() {
        let dir = tempdir().unwrap();
        let s = SegmentStore::open_with_max(dir.path(), 64).unwrap();
        let mut all = Vec::new();
        for i in 0..20u32 {
            all.push(put_get_hash(&s, format!("obj-{:03}", i).as_bytes()));
        }
        s.sync().unwrap();
        // Keep only the even-indexed ones.
        let mut live = HashSet::new();
        for (i, h) in all.iter().enumerate() {
            if i % 2 == 0 {
                live.insert(h.clone());
            }
        }
        let stats = s.compact(&live).unwrap();
        assert_eq!(stats.live_objects, 10);
        assert_eq!(stats.dropped_objects, 10);
        for (i, h) in all.iter().enumerate() {
            let got = s.get(h).unwrap();
            if i % 2 == 0 {
                assert!(got.is_some(), "live object {} must survive", i);
            } else {
                assert!(got.is_none(), "dead object {} must be pruned", i);
            }
        }
    }
}
