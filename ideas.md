# NEDB — Next-Turn Ideas

Grounded in the current state (**v2.4.2** — v3 segment/pack store + macOS fast-fsync shipped, documented, spec'd, and proven on itcd chainstate; `nedbd-v2` now has real CLI parsing and `npm test` ships a cinematic native smoke demo). Each: one line _what_ + one line _why_.

---

### 1. Compaction end-to-end (engine `compact()` → FFI → itcd trigger) — the open v3 gap
**What:** expose `Db::compact()` through a new `nedb_compact()` FFI call and trigger it on a cadence (RPC `compactchainstate`, shutdown, and a `-dagcompact=<MiB>` dead-bytes gate, the auto-trigger off by default) in itcd.
**Why:** v3 segments accumulate every dead/superseded UTXO version over a full sync — without pruning the chainstate store bloats toward *all* history, not the live set, eroding v3's on-disk win and risking unbounded growth. The primitive exists in the engine; it's just unreachable from the node.

### 2. Run the new smoke test in CI as a pre-publish gate
**What:** wire `npm test` (the native `test/smoke.mjs`) into `release.yml` so a tag build runs it against the freshly-built `.node` addon *before* `npm publish` / the wheel upload.
**Why:** npm versions are immutable and this very release found a `--dag-v3` regression by hand — a green smoke on the built binding would have caught it automatically and would stop a broken addon from ever shipping under a burned version number.

### 3. Make `--dag-v3` the default — after compaction lands
**What:** flip v3 on by default with a `--no-dag-v3` (loose) escape hatch, now that the flag is actually parsed.
**Why:** a full overnight itcd sync on v3 ran clean and the flush win is an order of magnitude, so "off by default" is the wrong long-term default — but gate it behind #1 so the default store can't bloat over time.

---

_Longer horizon: segment observability (in-engine seal log + flush metrics surface — external polling perturbs the very fsync it measures); reconcile `SPEC.md` §2 (still the v1 op-log model) with the shipped v2 content-addressed engine; update the PyO3 + napi bindings from the v1 AOF API to the v2 DAG API; Merkle inclusion proofs._
