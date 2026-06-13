<div align="center">

# NEDB

**A versioned, self-compressing, time-traveling embedded database.**

Replay-protected · idempotent · relational · filterable · sortable · searchable · provable.
One Rust core → ships to **PyPI** and **npm** from a single source.

</div>

---

## Why NEDB

Redis is fast because it's in-memory and simple — but relations are hand-rolled, history is gone the moment you overwrite, and every call pays a network hop. NEDB keeps the speed and adds the things real systems actually need:

- **Faster-than-Redis latency where it's honest to claim it** — NEDB runs **embedded, in-process**, so point reads pay *no socket hop*. The networked server (`nedbd`, RESP-compatible) competes on the Rust core's merits.
- **Replay protection + idempotency in the core, not the app.** Every write carries a strictly-monotonic per-client nonce and an optional idempotency key. Retries are no-ops; stale/out-of-order ops are rejected. This is built into one **hash-chained, append-only log**.
- **Time-travel.** Read the database *exactly as it existed* at any past sequence — `AS OF seq`. Debugging, audit, MVCC snapshots, and deterministic replay all fall out of the same log.
- **First-class relations.** Adjacency-list graph edges with O(1) traversal — *and the graph time-travels too*.
- **Filter / sort / search.** Equality, ordered, and full-text inverted indexes, maintained incrementally.
- **git-style files with maximum compression.** Content-defined chunking + content-addressed dedup + temperature tiers (fast warm codec, max-ratio cold archival). Every file version has a Merkle root you can **anchor on-chain**.

> **The keystone:** one nonce-enforced append-only log is the substrate for idempotency, replay protection, crash recovery, MVCC, *and* time-travel — simultaneously.

---

## Quickstart (Python reference engine — runs today, zero build)

```bash
git clone https://github.com/interchained/nedb && cd nedb
pip install -e .                 # pure-Python reference; no toolchain needed
python3 examples/demo.py         # see every feature
python3 tests/test_nedb.py       # 10/10 invariants
```

```python
from nedb import NEDB

db = NEDB()
db.create_index("users", "status", "eq")
db.create_index("users", "age", "ordered")
db.create_index("users", "bio", "search")

db.put("users", "alice", {"name": "Alice", "age": 31, "status": "active",
                          "city": "Austin", "bio": "rust systems hacker"})

# Idempotent, replay-protected write (safe to retry forever):
db.put("orders", "o1", {"total": 42}, client="checkout", nonce=7, idem="charge-o1")

# NQL — filter + sort
db.query('FROM users WHERE age >= 25 AND status = "active" ORDER BY age DESC')

# Full-text search
db.query('FROM users SEARCH "rust"')

# Relations + graph traversal
db.link("users:alice", "follows", "users:bob")
db.q("users").where("_id", "=", "alice").traverse("follows").run()

# Time-travel
s = db.seq
db.put("users", "alice", {"name": "Alice", "city": "Lisbon", "age": 31, "status": "active"})
db.get("users", "alice", as_of=s)["city"]      # -> "Austin"

# git-style files with Cascade compression + provable history
v1 = db.put_file("notes.txt", open("notes.txt","rb").read())
db.file_root("notes.txt", v1)                  # Merkle root — anchorable on ITC
```

---

## NQL — the NEDB Query Language

One small grammar; the Rust parser is the single source of truth so Python and Node share identical semantics. A fluent builder compiles to the same plan.

```
FROM <collection>
  [ AS OF <seq> ]
  [ WHERE <field> <op> <value> (AND ...)* ]      op ∈ = != < <= > >=
  [ SEARCH "<text>" ]
  [ ORDER BY <field> [ASC|DESC] ]
  [ TRAVERSE <relation> ]
  [ LIMIT <n> ]
```

---

## What's measured (reference engine, pure Python, 2 vCPU)

| Operation | Result |
|---|---|
| GET (embedded, in-process) | **~1.2M ops/s** (~800 ns/op) |
| SET (logged + indexed) | ~77K ops/s |
| Indexed query latency | ~75 µs |
| File compression — warm (zlib stand-in) | **39.9×** |
| File compression — cold (LZMA archival) | **88.9×** |
| Cross-version dedup | 20 of 22 chunks reused on edit |

The reference engine proves the **architecture**. The Rust core (`rust/`) is the speed target — see `bench/bench_redis.py` for the embedded-vs-Redis harness.

---

## Architecture

```
            ┌──────────────────────────────────────────────┐
  put/del → │  OpLog  (append-only · BLAKE3 hash chain ·    │ ← single source of truth
  link      │          per-client nonce · idempotency keys) │
            └───────────────┬──────────────────────────────┘
            deterministic fold │ (state = pure function of the log)
        ┌──────────────┬───────┴────────┬───────────────────┐
        ▼              ▼                ▼                   ▼
   MVCC store     Relations         Indexes            BlobStore (Cascade)
   (time-travel)  (graph, AS OF)    eq/ordered/search  CDC+dedup+tiers, Merkle roots
```

One Rust core (`nedb-core`) → **PyO3** wheels (PyPI) and **napi-rs** binaries (npm), plus a future `nedbd` server (RESP-compatible) and a WASM build for browser/edge.

Full design: [`docs/SPEC.md`](docs/SPEC.md).

---

## Repo layout

```
nedb/            pure-Python reference engine (this is what `pip install` ships today)
rust/            production core — nedb-core + nedb-py (PyO3) + nedb-node (napi-rs)
examples/demo.py end-to-end walkthrough
tests/           invariant tests
bench/           embedded micro-bench + Redis head-to-head harness
docs/SPEC.md     architecture specification
.github/         release CI → PyPI + npm on tag
```

## Roadmap

- [x] Reference engine: log, MVCC, relations, indexes, NQL, Cascade, Merkle
- [ ] Rust core parity + criterion benches + `cargo test`
- [ ] PyO3 wheels + napi-rs binaries published on tag
- [ ] `nedbd` server: RESP-compatible + native protocol
- [ ] Similarity-picked deltas + schema-aware columnar transforms
- [ ] On-chain (ITC) root anchoring; WASM build

## License

Apache-2.0. Part of the [Interchained](https://github.com/interchained) ecosystem.
