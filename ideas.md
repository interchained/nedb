# NEDB — Next-Turn Ideas

Specific, actionable ideas grounded in the **current v3 state** (segment store shipped in 2.3.3, documented in 2.3.333, proven on itcd chainstate). Each: one line _what_ + one line _why_.

---

### 1. Make `--dag-v3` the default after a cross-platform soak
**What:** flip v3 to default-on with an explicit `--no-dag-v3` (loose) escape hatch, once it's run a full sync clean on Linux, macOS, and Windows.
**Why:** the loose one-file-per-object store is a proven UX/perf liability (minutes-long flushes, ~185 writes/s ceiling); v3 is non-destructive via dual-read, so the upgrade path is safe and the itcd numbers (minutes → <2 s) make "off by default" the wrong default long-term.

### 2. Online / scheduled compaction during long syncs
**What:** trigger `compact()` automatically on a cadence (every N segments or once dead bytes exceed M MiB) instead of only on demand.
**Why:** chainstate overwrites coins constantly (spend → new version), so a long IBD accumulates unbounded superseded versions in the segment dir; the pruning primitive already exists — it just needs a scheduler so v3's on-disk-size win doesn't erode over a multi-day sync.

### 3. Segment observability + scoped integrity check
**What:** expose per-store segment count, live-vs-dead bytes, and last-compaction stats via a nedbd HTTP endpoint (and matching `nedb_*` FFI), plus a segment-scoped `verify` fast path.
**Why:** operators running v3 at chainstate scale currently have zero visibility into pack health or compaction pressure, and `-verifynedb` re-hashes every object (O(n), minutes) — a segment-targeted check would make routine integrity verification cheap enough to run often.

---

_Future (not this cycle): a NEDB Studio segment viewer to visualize packs + compaction. Deferred — no Studio work in the current scope._
