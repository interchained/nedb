# NEDB — Architecture Specification

Status: v2.4.2 · Rust v2 content-addressed DAG engine shipped (PyPI · npm · crates.io) · v3 segment/pack substrate opt-in (`--dag-v3`, parsed as a real `nedbd-v2` flag as of v2.4.2)

---

## 0. Thesis

A database that is **as fast as Redis where the comparison is honest**, but adds the
primitives real systems hand-roll badly: relations, history, idempotency, replay
protection, search, and integrity. The trick is that one structure — a **nonce-enforced,
hash-chained, append-only operation log** — is the substrate for almost all of it.

Non-goals: inventing a new general-purpose entropy coder; inventing a general-purpose
programming language. NQL is a *small focused query DSL*, nothing more.

---

## 1. Single-source, dual-registry distribution

```
                 ┌───────────────┐
                 │   nedb-core   │   one Rust crate (the engine)
                 └───────┬───────┘
        ┌────────────────┼───────────────────┬───────────────┐
        ▼                ▼                   ▼               ▼
   nedb-py (PyO3)   nedb-node (napi-rs)   nedbd server     WASM build
   maturin → PyPI   prebuilt → npm        RESP + native    browser/edge
```

Rust is the only language that compiles natively on every OS **and** has mature binding
toolchains for both targets (PyO3+maturin → PyPI; napi-rs → npm). Same source, no rewrite.
The pure-Python package is the reference/fallback and the executable specification.

---

## 2. The operation log (source of truth)

Every mutation is an `Op`:

```
Op { seq, client, nonce, op, payload, idem, prev_hash, hash }
```

- **seq** — monotonic, assigned by the log. Defines global order and the time-travel axis.
- **nonce** — per-client, strictly increasing. `nonce <= last_seen[client]` ⇒ **rejected**
  (`ReplayError`). This is replay protection in the blockchain sense.
- **idem** — optional idempotency key. A key seen before returns the original op and does
  **not** append again. Writes become safe under at-least-once delivery and retries.
- **hash chain** — `hash_n = BLAKE2b(hash_{n-1} ‖ canonical(body))`. Any tampering breaks
  the chain (`verify()`); the head hash commits to the entire history and is **anchorable
  on-chain (ITC)**.

State is a pure function of the log: `fold(apply, ops)`. This yields crash recovery,
deterministic `rebuild()`, and `AS OF` time-travel with no extra machinery.

---

NEDB v2 makes the log **content-addressed**: every document version is an immutable object
addressed by `BLAKE2b(content)` and written under `objects/{hash[:2]}/{hash[2:]}`. Nothing is
ever overwritten — reads re-verify their own bytes, and a partial write is just an
unreferenced object ignored on startup.

- **MVCC / time-travel** — an id-index maps `(collection, id) → current object hash`; each
  object carries a `prev` link to its prior version, so `AS OF seq` walks the version chain
  backward to the newest object with `seq ≤ N`. Readers never block writers.
- **Concurrency (production)** — a group-commit **Sequencer** batches writes (one committer
  thread, parallel readers); the id-index buffers updates in a lock-free WAL and flushes them
  to disk in parallel. This is how NEDB scales writes across cores where Redis is single-threaded.
- **Merkle head** — every write advances a running head
  `H_n = BLAKE2b(H_{n-1} ‖ seq ‖ object_hash)`, a tamper-evident commitment to all state at the
  current seq, returned on every response and anchorable on-chain (ITC).

### 3.1 v3 — segment / pack object store (opt-in)

The default one-file-per-object layout is corruption-proof but caps throughput at the
filesystem's small-file metadata rate (low hundreds of writes/s on real disks) — a hard
ceiling for high-write workloads (blockchain chainstate, event sourcing). The **v3 substrate**
(`--dag-v3` / `NEDB_DAG_V3`, **default off**) keeps the v2 logical model byte-for-byte and
changes only *where the bytes live*:

- **Segment packs** — immutable objects are appended into `objects/segments/seg-NNNNNN.dat` as
  `[content_len: u32][content]`, addressed by an in-memory `hash → (segment, offset, len)`
  index. A batch commits with **one `fsync`** instead of one per object, so flush cost scales
  with **bytes written** (sequential append), not object-count × syscalls. In production
  (itcd chainstate) this drops a multi-thousand-coin flush from *minutes* to **~1.3 s**.
- **`.idx` sidecars** — a sealed segment carries a checksummed `hash → (offset, len)` index so
  cold start loads it instead of rescanning; a missing/corrupt sidecar falls back to scan-and-heal.
- **Compaction / pruning** — `compact(live)` rewrites the live object set into fresh segments
  and reclaims superseded/dead versions, bounding on-disk size over long histories.
- **Dual-read migration** — opening an existing v2 loose store in v3 mode is non-destructive:
  old objects stay readable; only new writes go to segments. Full format: [`rust/SEGMENTS.md`](../rust/SEGMENTS.md).

### 3.2 Durability

Tunable: pure in-memory → WAL-buffered id-index → per-batch `fsync` (one durability point per
group-commit). The object store *is* the WAL; a `MANIFEST` of `(seq, head)` checkpoints for
O(1) **warm restart**, and a `Db` flushes on close (`Drop`) so write-then-drop is durable
without an explicit flush. On macOS, `NEDB_FAST_FSYNC` (`-dagfastsync`) substitutes a plain
`fsync(2)` for std's `F_FULLFSYNC` (a full hardware-cache barrier, 10–100× slower on
Fusion/SATA) — crash-safe, much faster flush, default off. A torn segment tail is truncated on open.

---

## 4. Relations (graph layer)

Edges stored as adjacency lists with reverse index, each carrying `(added_seq, removed_seq)`.
Traversal is O(1) per hop; queries may be asked `AS OF` any seq, so the **graph time-travels**
exactly like records. The planner walks relations without N+1 blowups.

---

## 5. Indexes

| Kind | Structure | Powers |
|---|---|---|
| equality | hash: value → {ids} | `WHERE f = v` |
| ordered | sorted array / ART (prod) | `WHERE f </<=/>/>=`, `ORDER BY` |
| search | inverted: token → {ids} | `SEARCH "..."` |

Maintained incrementally on write at HEAD. Time-travel queries fall back to a version scan;
temporally-indexed reads are a documented later optimization.

---

## 6. NQL (query language)

Grammar in [README](../README.md#nql). Text form and fluent builder compile to one plan dict;
the Rust parser/planner is the single source of truth. Execution: pick the most selective
access path (search → equality index → scan), apply the full predicate set on loaded rows
(correctness regardless of index path), then order → traverse → limit.

---

## 7. Cascade — compression & the git-style file layer

A git-style versioned file manager is the **same substrate** seen through a file lens:

| git | NEDB |
|---|---|
| blob | content-addressed value |
| tree | relation graph (directory) |
| commit | named log snapshot |
| checkout | time-travel read |
| history | the operation log |

**The Cascade pipeline** (proven primitives; novel *composition* — no new entropy coder):

1. **Content-defined chunking** (Gear rolling hash) — boundaries follow content, so a small
   edit only changes nearby chunks → cross-file, cross-version dedup.
2. **Content-addressed dedup** (BLAKE) — identical chunks stored once everywhere.
3. **Similarity-picked binary deltas** *(prod)* — delta against the most similar blob (simhash),
   not just the previous version.
4. **Schema-aware columnar transforms** *(prod)* — the DB knows field types, so columnar
   grouping, delta-of-delta timestamps, dictionary + bit-packing **before** entropy coding.
   The structural edge git/borg/Redis cannot have.
5. **Entropy + tiers** — warm: fast codec (zstd-dict in prod; zlib in reference);
   cold/archival: LZMA-class.

**Resolving "fastest" vs "maximum compression" — tier by temperature:**

| Tier | Data | Treatment | Goal |
|---|---|---|---|
| Hot | working set | raw / fast, in memory | Redis-class latency |
| Warm | cooling | zstd-dict + columnar | balance |
| Cold | old versions / history | delta + LZMA | maximum ratio |

Version history is naturally cold and rarely read — exactly what we can afford to crush.
Reference results: **39.9× warm, 88.9× cold**, 20/22 chunks deduped on a mid-file edit.

---

## 8. Provable history (the connective idea)

CDC chunks + BLAKE form a **Merkle DAG**. Any version's bytes are committed by a Merkle root;
membership is provable in O(log n) (`file_proof`/`verify_proof`). The root (and the log head)
can be **anchored on ITC** for tamper-evident, notarized version history — a DB whose entire
history is cryptographically verifiable against your own chain.

---

## 9. Benchmarking — claiming "fastest" honestly

- **Embedded:** in-process; no socket. The near-certain latency win. Measured directly.
- **Networked:** `nedbd` speaks RESP, so `redis-benchmark`/`memtier` run unchanged against
  NEDB and Redis/Dragonfly/KeyDB. We publish apples-to-apples numbers and claim "fastest"
  **only where the data holds**. `bench/bench_redis.py` is the starter harness.

---

## 10. Milestones

`M0` spec + scaffold ✓ · `M1` core (content-addressed DAG / MVCC / recovery) ✓ · `M2` relations
+ indexes + file layer ✓ · `M3` NQL + time-travel + bi-temporal + causal TRACE ✓ · `M4` PyO3/napi
+ CI publish (PyPI · npm · crates.io) ✓ · `M5` nedbd server + RESP2 + benchmarks ✓ · **`M6` v3
segment/pack store + compaction + macOS fast-fsync ✓ (v2.4.0)** · `M7` WASM + Merkle inclusion proofs.

---

## 11. Open questions

- ART vs B-tree for the ordered index under MVCC epoch reclamation.
- Columnar transform boundary: per-record vs per-column-segment flush from hot→warm.
- Branch/merge conflict policy for the file layer (3-way on chunk DAG).
- Exact on-chain anchoring cadence (per-commit vs batched root) on ITC.
