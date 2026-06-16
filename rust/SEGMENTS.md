# Segment-based AOF — Design Doc

**Status:** Design accepted, implementation pending.
**Target version:** v1.4.0 (sorted indexes shipped in v1.3.0; segments are a
separate, larger change that's safer to land on its own).
**Authors:** Eth-Interchained / Claude.
**Last revised:** 2026-06-16.

## Motivation

Today every NEDB database is a single `log.aof` file. Every mutation appends
one encrypted, BLAKE2b-chained line, and on startup the engine replays the
whole file into the in-memory `OpLog + MvccStore + Relations + Indexes`.

This works beautifully at small scale and proves the chain on every boot, but
the cost is linear in total writes. With 600k+ block entries the cold-start
hang is multiple seconds and growing — the engine spends most of that time
re-doing work whose only artifact is the in-memory state we just threw away
when the process exited.

The fix is a classic **snapshot + segmented log**. Sealed segments stay
immutable for chain verification and time-travel; a periodic snapshot lets us
skip past most of them on boot.

## Goals

1. Cold-start time is bounded by `snapshot_size + tail_segment_size`, not total
   AOF size.
2. On-disk hash chain remains intact and end-to-end verifiable.
3. No on-disk format break for existing single-file databases — they must
   open and migrate seamlessly.
4. Time-travel (`AS OF seq`) and `db.verify()` keep working unchanged.
5. The whole change lives below the public API; PyO3 and napi bindings stay
   identical.

## Non-goals

- Sharding across multiple processes / files-per-collection. One database, one
  segment directory.
- Concurrent multi-writer safety (NEDB is still single-writer; the engine just
  has to be crash-safe).
- Streaming replication. The segment layout makes it easier later, but is not
  built here.

## On-disk layout

```
mydb/
  log.aof             # legacy single-file AOF — read on first open, then migrated
  dek.json            # data encryption key (unchanged)
  segments/
    000001.seg        # sealed, fsynced, immutable; head_hash recorded in MANIFEST
    000002.seg        # sealed
    000003.seg        # OPEN — appends go here until size >= rotate_threshold
  snapshots/
    snap-000002.ndb   # CBOR-serialized MvccStore + Relations + Indexes config
                      # + chain head hash + last_seq applied
    snap-000002.ndb.sha256
  MANIFEST            # JSON: latest snapshot id, sealed segment list with
                      # (id, first_seq, last_seq, sha256, head_hash_after_seg)
```

### File naming

- Segments: zero-padded 6-digit ID, suffix `.seg`. Monotonic, never reused.
- Snapshots: `snap-{segment_id}.ndb` where `segment_id` is the last sealed
  segment included in the snapshot. A snapshot is always taken on a segment
  boundary so replay always restarts at the start of the next segment.

### Segment record format

Same one-JSON-op-per-line format as today's `log.aof` — every record is
`{seq, client, nonce, op, payload, idem, prev_hash, hash}`. Encryption is
still per-record AES-256-GCM with the per-database DEK. The only difference
is that records are spread across multiple files instead of one.

### MANIFEST

Tiny JSON file rewritten atomically (write `MANIFEST.tmp`, fsync, `rename`):

```json
{
  "version": 1,
  "active_segment": "000003.seg",
  "latest_snapshot": "snap-000002.ndb",
  "snapshot_last_seq": 50000,
  "snapshot_head_hash": "ab12…",
  "segments": [
    {"id": "000001.seg", "first_seq": 0,     "last_seq": 24999, "sha256": "…", "head_after": "…"},
    {"id": "000002.seg", "first_seq": 25000, "last_seq": 49999, "sha256": "…", "head_after": "…"},
    {"id": "000003.seg", "first_seq": 50000, "last_seq": null,  "sha256": null, "head_after": null}
  ]
}
```

The `head_after` field per sealed segment lets us verify the chain
incrementally without re-reading prior segments.

## Rotation

```rust
struct AofConfig {
    rotate_bytes: u64,        // default 64 * 1024 * 1024 (64 MiB)
    snapshot_every_n_segments: u32, // default 16 — i.e. snapshot every ~1 GiB
}
```

After every append the writer checks `fstat(active_segment).len() >=
rotate_bytes`. If so:

1. fsync + close the active segment.
2. Compute its sha256 and capture the current chain head.
3. Update MANIFEST: mark sealed, append entry, advance `active_segment` to
   the next ID.
4. Open the new segment with `O_APPEND | O_CREAT`.

Rotation happens inline on the write path — it's cheap (one fsync, one
manifest swap) and only fires every ~64 MB.

## Snapshotting

The same writer thread maintains a counter of sealed segments since the last
snapshot. When it crosses `snapshot_every_n_segments`, after rotating, it:

1. Serializes `MvccStore + Relations + Indexes.config + log.head + log.len`
   to a temp file using CBOR (compact, schema-stable).
2. Writes its sha256 sidecar.
3. Updates MANIFEST: `latest_snapshot`, `snapshot_last_seq`, `snapshot_head_hash`.
4. Triggers compaction (below) asynchronously.

The snapshot is purely an optimization — losing it doesn't lose data, since
the segments still contain the full log.

## Startup algorithm

```
read MANIFEST (if absent: legacy single-file path — replay log.aof verbatim,
               then on the next clean shutdown migrate to segments)

if latest_snapshot exists:
    load snapshot into MvccStore/Relations/Indexes/log
    verify snapshot_head_hash matches log.head() after load
    starting_seq = snapshot_last_seq + 1
else:
    starting_seq = 0

for each sealed segment in MANIFEST whose last_seq >= starting_seq:
    replay records with seq >= starting_seq
    verify sha256
    verify head_after matches running chain head

replay the active (unsealed) segment from disk top-to-bottom
open it append-only for future writes
```

Cold-start cost is now bounded by **one snapshot load + one segment replay**
— regardless of total history.

### Migration from single-file AOF

If `log.aof` exists and no MANIFEST is present:

1. Open the database with today's exact code path (replay `log.aof`).
2. After replay succeeds and is verified, write a snapshot to
   `snapshots/snap-000000.ndb`, build a `segments/` directory whose only
   entry is the active segment `000001.seg` (empty), and write the MANIFEST.
3. Rename `log.aof` → `log.aof.legacy` and keep it for one release as a
   safety net. The engine never reads it after step 1.

The migration runs once. Subsequent opens see the MANIFEST and skip the
single-file path entirely.

## Compaction

For a versioned DB we never delete history — but we *can* delete redundant
older log records once they're covered by a snapshot. Compaction is
opt-in (`db.compact_below(snap_last_seq)`) and removes sealed segments whose
`last_seq <= snap_last_seq` after archiving them to `segments/archive/` (or
discarding them, with `purge=True`).

The chain hash stays verifiable because each segment carries its `head_after`
in the MANIFEST, and the snapshot itself anchors the chain at
`snapshot_head_hash`. Verification continues from the snapshot's head through
the remaining segments.

## Crash safety

- Records are appended with `write + fsync` in the same pattern as today.
- MANIFEST is written via `tmp + rename` so the directory never sees a
  half-updated manifest.
- Snapshots are written to `snap-XXX.ndb.tmp`, fsynced, renamed; the sha256
  sidecar is written *after* the rename so a missing sidecar means an
  in-progress snapshot — the loader ignores it and falls back to the prior
  one.
- Segment seal commits a sha256; if the MANIFEST swap doesn't complete the
  segment is treated as still-active and re-scanned on next boot. Idempotent.

## API surface

No new public methods are required for the basic case — the change is
transparent. We add three knobs on `Db::open`:

```rust
pub struct OpenOptions {
    pub rotate_bytes: Option<u64>,
    pub snapshot_every_n_segments: Option<u32>,
    pub compact_below_snapshot: bool,
}
impl Db {
    pub fn open_with(path: &str, opts: OpenOptions) -> std::io::Result<Self> { ... }
    /// Force a snapshot now. Returns the segment id it pinned to.
    pub fn snapshot(&mut self) -> std::io::Result<String> { ... }
}
```

Existing `Db::open(path)` keeps working with sensible defaults.

## Test plan

1. Unit: rotate at a 1 KiB threshold; assert MANIFEST and segment files
   match expectations.
2. Unit: snapshot at every 2 segments; reload and check `verify()` + a
   sampling of `get()` calls match a no-snapshot baseline.
3. Unit: corrupt a sealed segment's bytes → `verify()` returns false; engine
   refuses to open.
4. Integration: migrate a populated legacy `log.aof` and assert all docs +
   chain head survive.
5. Bench: 600k synthetic block entries — assert cold-start drops from
   multi-second to sub-200 ms.

## Open questions

- Should we compress sealed segments (zstd) on rotation? The chain stays
  intact and decompression is fast, but it adds a moving part. Defer until
  we measure disk pressure.
- Should `as_of` reads against the snapshot also need access to the pruned
  segments? Yes for now — `compact_below` archives instead of deletes, and
  AS OF below the snapshot transparently restores from archive. Time-travel
  past archived segments errors with a clear message.

## Out of scope for v1.3.0

Everything in this doc. v1.3.0 ships the sorted-index fast path only; segment
AOF lands in v1.4.0 once this design has marinated.
