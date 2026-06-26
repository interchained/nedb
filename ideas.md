# NEDB — Next-Turn Ideas

Grounded in the current state (**v2.4.0** — v3 segment/pack store + macOS fast-fsync shipped, documented, and spec'd; proven on itcd chainstate). Each: one line _what_ + one line _why_.

---

### 1. Compaction end-to-end (engine `compact()` → FFI → itcd trigger) — the open v3 gap
**What:** expose `Db::compact()` through a new `nedb_compact()` FFI call and trigger it on a cadence (flush-checkpoint or shutdown) in itcd.
**Why:** v3 segments accumulate every dead/superseded UTXO version over a full sync — without pruning the chainstate store bloats toward *all* history, not the live set, eroding v3's on-disk win and risking unbounded growth. The primitive exists in the engine; it's just unreachable from the node.

### 2. Make `--dag-v3` the default — after compaction lands
**What:** flip v3 on by default with a `--no-dag-v3` (loose) escape hatch.
**Why:** a full overnight itcd sync on v3 ran clean and the flush win is an order of magnitude, so "off by default" is the wrong long-term default — but gate it behind #1 so the default store can't bloat over time.

### 3. Segment observability — in-engine seal log + flush metrics
**What:** emit a one-line log when a segment seals / writes its `.idx`, and expose segment count / live-vs-dead bytes / last-compaction via a nedbd endpoint (+ a segment-scoped `verify` fast path).
**Why:** operators have zero visibility into pack health or compaction pressure today, and the watcher episode proved observability must live *inside* the engine — external polling of the live store perturbs the very fsync it's trying to measure.

---

_Longer horizon: reconcile `SPEC.md` §2 (still the v1 op-log model) with the shipped v2 content-addressed engine; update the PyO3 + napi bindings from the v1 AOF API to the v2 DAG API; Merkle inclusion proofs._
