# NEDB — Handoff · v3 pread handle cache (+ the correctness quick-wins session)

**Date:** 2026-07-02 · **Author:** Vex (Claude Fable 5) × Mark (Interchained) · **Session arc:** performance review → validation → PR #44 (merged, shipped in v2.5.55) → this branch.

This handoff covers **two things**: the pread handle cache on this branch (`hyperagent/nightly-2026-07-02-pread-handle-cache`), and the session context the next turn needs — the six correctness fixes already shipped in **v2.5.55** and the ranked backlog that remains.

---

## 1. This branch: cached read handles + positional reads (review finding #5, HIGH)

### The cost (verified against v2.5.45 code, not assumed)
`SegmentStore::read_content` (`segment.rs:271-283` pre-change) did `File::open` + `seek` + `read_exact` + implicit close **per point read** — 3-4 syscalls and fd churn per `get()`. This is the exact path itcd `-dagv3` chainstate reads live on (coin lookups during IBD, Ghost Protocol demand-loads, Rusty resume reads). On Windows — Nemo's platform — `CreateFile` is the expensive syscall, so the per-read open hurts most there.

### The fix
- `read_handles: DashMap<u32, Arc<File>>` on `SegmentStore` — one shared **read-only** handle per segment, opened lazily on first read.
- All reads go through `read_at()`: `read_exact_at` (pread) on Unix, a `seek_read` fill loop on Windows. Explicit offset per call → **no cursor state, no lock on the read path, no per-read open**.
- `read_content` became a method (`&self`); `get()` and `compact()` route through it.
- `compact()` calls `read_handles.clear()` after the index swap — old segment ids are never referenced again (ids strictly increase). Releasing fds before deleting files is hygiene, not correctness (Unix unlink semantics; Rust std opens with `FILE_SHARE_DELETE` on Windows).

### Why it's safe (the argument, so you don't have to re-derive it)
1. **Appender untouched.** The active segment's writer keeps its own separate handle (own cursor) behind the `active` mutex. Cached read handles are a *different* handle on the same file.
2. **Read-after-write coherence.** Index entries are inserted only AFTER `write_all` returns; reads through a second handle of the same file are page-cache-coherent on both Unix and Windows (no O_DIRECT anywhere).
3. **Windows `seek_read` moves that handle's file pointer** — harmless: cached handles are used exclusively through `read_at`, and every call passes its own absolute offset.
4. **Double-open race** (two threads miss the cache simultaneously): `entry().or_insert()` keeps the first handle; the loser's `File` drops (closes). Harmless.

### Tests
- **New:** `concurrent_reads_share_cached_handles` — 4 threads × 2 passes × 64 records across multiple segments (256-byte rollover forces sealed + active coverage), then compaction invalidation + re-read.
- **Existing coverage now exercising the cached path:** `tamper_detected_on_read`, `torn_tail_is_truncated_on_open`, `rollover_writes_idx_and_reopen_uses_it`, both compaction tests.

### Measure it — claim the number only after this
```bash
cargo run --release -p nedb-engine --example v3_bench -- 50000
```
Compare the v3 `reads / sec` row against a master run **on the same disk**. The PR claims the mechanism, not a number; the bench gives the number.

---

## 2. Session context: what already shipped today (v2.5.55, tag `fa8142e`)

PR #44 (merged `cfc22e2`) — six correctness fixes from the Fable5 performance review, all verified green by Mark (57/57 lib + 8/8 CLI + 1/1 v3 integration):

| Fix | File | One-liner |
|---|---|---|
| WAL flush lost-update race (**CRITICAL**) | `index.rs` | post-flush remove is now `remove_if` value-guarded — a write racing the flush stays buffered instead of being silently dropped |
| NQL `ORDER BY + WHERE + LIMIT` (**CRITICAL**) | `nql.rs` | limit no longer pushes into the index top-k when post-filters exist (diverged from Python reference: filter → sort → limit) |
| `put_batch` sorted-index parity | `db.rs` | batch updates now remove superseded index entries like `put()` does |
| `put()` wasted old-object read | `db.rs` | old-object disk read skipped entirely when no sorted index exists (~2× read amp gone on unindexed updates) |
| Head race + seq-guarded tips | `db.rs` | head chain RMW under one write lock (no dropped contributions); `tip_hash`/`coll_tip_hash` carry `(seq, hash)` and only advance on `seq >=` — stale arrivals can't clobber a newer tip in MANIFEST. **Found during implementation:** cold-scan MANIFEST wrote `seq: max_seq` but warm boot reads `m.seq` as next-to-assign → restart after quiet cold scan **reused the tip's seq** (duplicate seq in log). Scan now persists via `flush_manifest()` — one canonical writer. |
| Durability ordering | `db.rs` | MANIFEST tmp fsync'd before rename + dir fsync (unix); ticker syncs segments BEFORE MANIFEST flush, dirty-gated |

Registries verified live at **2.5.55**: PyPI ×3 (nedb-engine, cryptodb, aof-db — curl-verified), crates.io (curl-verified), npm ×3 (Mark-verified; sandbox egress blocks npm). `release` workflow: success. `release-distros`: was still assembling distro npm at last check — verify its conclusion before the next release.

Full review lives in the thread doc: **"NEDB Performance Review — v2.5.45 (Fable5, 2026-07-02)"** — 10 ranked opportunities, 5 strengths, quick-wins/medium/major buckets, missing-benchmarks list, CTO answer. Every claim cites file:line.

---

## 3. Versioning — flag before you run release.py

This branch does **not** bump versions. When it merges and the next release cuts, remember the tool takes the leading `v` on both args:
```bash
python3 scripts/release.py "v2.5.55" "vNEXT"
```

---

## 4. What this unblocks / ranked backlog for the next turn

In review-priority order (see the review doc for gain/complexity/risk per item):
1. **Streaming cold scan + id-index rebuild** (Medium #2) — folds coll-tips/sorted-index/id-index rebuild into the parallel read pass; kills the O(dataset) RAM spike AND closes the last recovery gap (id-index loss currently has NO rebuild path — cold scan never calls `id_index.set`).
2. **WHERE acceleration through the sorted index** (Medium #4) — eq + range via `BTreeMap::range`; Python reference already does eq (`engine.py:572-577`); Rust NQL currently full-scans everything except `_id =`.
3. **Coalesced subscription evaluator** (Medium #3) — `notify_subscribers` still re-runs every live query synchronously per write (`server.rs:106-135`).
4. **Canonical seq-ordered head** (review #7b) — the incremental chain is arrival-ordered under concurrency and the cold-scan formula differs; needs an anchoring-consumer decision (Mark + Oracle) before code. Do NOT solo this one; it changes head values.
5. **mmap sealed segments** — only if the pread numbers from §1 aren't already saturating.

**Working agreement proven this session:** Vex writes blind (no linker in the sandbox), Mark compiles, merge on green, tag only after registries verified. It held — keep it.
