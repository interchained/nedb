<!-- NEDB working spec · signed handoff artifact -->

> **Document:** Handoff — `tip_collection()` durability across warm restart
> **Generated:** 2026-07-01T15:55:05Z (UTC)
> **Author:** Mark Allen Evans Jr. · INTERCHAINED LLC
> **Built by:** Vex (Claude Sonnet 5) · pair-programming partner
> **Engine target:** nedb-engine v2.5.44 (from v2.5.43) — version TBD, see §4
> **Source:** live catch during the itc-node-rs durability session, same day as v2.5.43
> **Status:** implementation complete on branch, awaiting compile + merge

-----

# NEDB — Handoff · `tip_collection()` durability

**Unit:** Make `tip_collection(coll)` survive a warm restart, with the exact same
guarantee v2.5.43 gave `tip()`.

**Why this one, right now:** v2.5.43 shipped durable `tip()` — the global head now
resolves O(1) across a warm restart via a `tip_hash` persisted in MANIFEST. Mark
caught, correctly, that `tip_collection(coll)` was NOT touched by that fix and still
had the identical bug: it walked the in-memory `seq_index` backward from the head,
which is populated by live-session writes or the cold scan and is **empty on a warm
boot**. For itc-node-rs specifically this matters directly — headers, L1 blocks, and
L2 receipts each live in their own NEDB collection, and the planned resume path is
per-collection (`tip_collection("headers")`, `tip_collection("blocks")`), not just
the global tip.

**What shipped, not just designed:** this handoff describes code that is already
written on the branch below — not a proposal. Mark's instruction was: write this
handoff, push a PR, and he compiles before merging or releasing.

-----

## 1. The bug (verified against the actual v2.5.43 code, not assumed)

```rust
// pre-fix
pub fn tip_collection(&self, coll: &str) -> Option<Node> {
    let mut s = self.seq.load(Ordering::SeqCst); // exclusive upper bound (head + 1)
    while s > 0 {
        s -= 1;
        if let Some(hash) = self.get_hash_by_seq(s) {          // <- seq_index, cold on warm boot
            if let Some(node) = self.get_by_hash(&hash) {
                if node.coll.as_str() == coll { return Some(node); }
            }
        }
    }
    None
}
```

`get_hash_by_seq` resolves through `seq_index: Arc<DashMap<u64, String>>` — populated
by `put`/`put_batch` in a live session, or by the cold-scan background pass. A warm
boot loads `{seq, head, tip_hash}` from MANIFEST and **skips the cold scan**
(`start_cold_scan` returns early when `startup_ready` is already true). So on a warm
restart `seq_index` is empty, the backward walk finds nothing at any `s`, and
`tip_collection(coll)` returns `None` — same failure class as pre-2.5.43 `tip()`, and
would have reproduced the exact "no persisted tip → syncing from genesis" symptom
seen in the itc-node-rs logs, but per-collection instead of globally.

Secondary issue, independent of correctness: even when the seq_index IS warm, the
backward walk is O(distance since that collection's last write) — for a collection
written rarely relative to the log's total volume, that is a real per-call cost, not
just a boot-time one.

## 2. The fix — a dedicated per-collection tip map (not a fallback bolted onto the old walk)

Rather than add a MANIFEST-fallback branch on top of the existing backward scan (the
two-path shape `tip()` needed, because it already had two live sources of truth —
in-session `seq_index` vs restored `tip_hash`), `tip_collection` gets ONE map that is
correct in every regime by construction:

```rust
/// Per-collection tip: coll -> object hash of the highest-seq node in that
/// collection. Kept current on every write (update_head), restored from MANIFEST
/// on warm boot, rebuilt by the cold scan.
coll_tip_hash: Arc<DashMap<String, String>>,
```

- **Write path** — `update_head` gained a `coll: &str` parameter (threaded through its
  3 call sites: `put`, the `put_batch` loop, `delete`) and does
  `self.coll_tip_hash.insert(coll.to_string(), new_hash.to_string());` on every call.
  Call order within a single collection is ascending seq (same guarantee `tip_hash`
  already relies on), so last-write-wins is correct for the live-session case.
- **Persistence** — `Manifest` gained `coll_tips: HashMap<String, String>`
  (`#[serde(default)]`, so a pre-this-patch MANIFEST — including a plain v2.5.43 one —
  still parses; missing entries self-heal on the collection's next write or a cold
  scan). `flush_manifest()` snapshots `coll_tip_hash` into it on every flush.
- **Warm restore** — `startup_rebuild()`'s warm-start branch restores
  `coll_tip_hash` from `m.coll_tips` alongside the existing `tip_hash` restore.
- **Cold scan** — `cold_scan_background_arc` does NOT get to assume ascending order
  (the object-hash scan is unordered), so it explicitly tracks `(max_seq, hash)` per
  collection across the whole node set, then populates `coll_tip_hash` and writes
  `coll_tips` into the MANIFEST it produces at the end. This is also how a
  pre-this-patch database self-heals: the existing "MANIFEST predates durable tip() →
  cold scan once to upgrade" branch already fires for any MANIFEST missing
  `tip_hash`, and that same scan now populates `coll_tips` too — one upgrade pass
  fixes both.
- **Read path** — `tip_collection` collapses to:
  ```rust
  pub fn tip_collection(&self, coll: &str) -> Option<Node> {
      let hash = self.coll_tip_hash.get(coll)?.clone();
      self.get_by_hash(&hash)
  }
  ```
  O(1), no scan, no fallback branch needed — simpler code than what it replaced, not
  just a bugfix on top of it.

## 3. Tests

- `tip_collection_survives_warm_restart` (new): writes into two collections
  (`blocks`, `tx`), `flush_all()`, drops the handle, reopens **warm**, asserts
  `seq_index` is cold (`get_hash_by_seq(0).is_none()`), then asserts
  `tip_collection("blocks")` and `tip_collection("tx")` both still resolve to the
  correct last-written node, and `tip_collection("absent")` is still `None`.
- Existing `tip_collection_per_chain` (unchanged, still passes) covers the
  live-session correctness (global tip vs. per-collection tips, multiple
  collections).
- `docs/REPLICATION.md` updated: the "`tip()` survives restarts" section is now
  "`tip()` and `tip_collection()` survive restarts," documenting the per-collection
  durable-resume pattern for a node with independent chains per collection.

## 4. Versioning — flag before you run release.py

Mark's plan is `python3 scripts/release.py v2.5.43 v2.5.5` after merge. **v2.5.5 is
numerically LOWER than v2.5.43** in semver's per-segment comparison (`5 < 34/43` on
the patch number) — npm/PyPI/crates would accept the publish, but dependency
resolvers preferring "latest satisfying" would keep picking v2.5.43 over v2.5.5 in
most installs, and the `latest` dist-tag semantics get confusing (a "downgrade" tag
move). This is very likely a typo for **v2.5.44**. Flagging it here rather than
silently substituting — the target version is your call; this doc does not run
release.py.

## 5. What this unblocks

Once merged + released, itc-node-rs's resume path (still pending, separate work) can
use `tip_collection("headers")`, `tip_collection("blocks")`, and
`tip_collection("l2_receipts")` as independent, durable-across-restart resume
points — exactly the shape Mark specified: `tip()` (or `tip_collection()`) +
`since(tip.seq − EPOCH_SAFETY_WINDOW)`, with no scan and no hand-rolled `__tip__`
document hack.

— © INTERCHAINED LLC · handoff prepared by Vex (Claude Sonnet 5)
