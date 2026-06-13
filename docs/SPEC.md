# NEDB — Architecture Specification

Status: draft v0.1 · Reference engine implemented & tested · Rust core in progress

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
- **hash chain** — `hash_n = BLAKE3(hash_{n-1} ‖ canonical(body))`. Any tampering breaks
  the chain (`verify()`); the head hash commits to the entire history and is **anchorable
  on-chain (ITC)**.

State is a pure function of the log: `fold(apply, ops)`. This yields crash recovery,
deterministic `rebuild()`, and `AS OF` time-travel with no extra machinery.

---

## 3. Storage engine

- **MVCC store** — each key → ascending `(seq, value|tombstone)` versions. HEAD read = last;
  `as_of=N` read = newest with `seq ≤ N` (binary search). Readers never block writers.
- **Concurrency (production)** — keyspace sharded by key hash; readers pin an epoch/snapshot;
  writers append. This is how NEDB scales writes across cores where Redis is single-threaded.
- **Durability (tunable)** — pure in-memory → WAL-buffered → fsync-per-commit. The op log *is*
  the WAL; periodic snapshots checkpoint for fast recovery.

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

`M0` spec + scaffold ✓ · `M1` core (log/MVCC/recovery) · `M2` relations + indexes + file layer
· `M3` NQL + builders + time-travel + commits/branches/diff/merge · `M4` PyO3/napi + CI publish
+ Cascade pipeline + tiering · `M5` nedbd server + benchmarks + Merkle/ITC anchoring · `M6` docs
+ WASM.

---

## 11. Open questions

- ART vs B-tree for the ordered index under MVCC epoch reclamation.
- Columnar transform boundary: per-record vs per-column-segment flush from hot→warm.
- Branch/merge conflict policy for the file layer (3-way on chunk DAG).
- Exact on-chain anchoring cadence (per-commit vs batched root) on ITC.
