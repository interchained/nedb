<div align="center">

# NEDB

**Content-addressed Merkle DAG · Hash-chained · Time-traveling · Bi-temporal · Causally-provable embedded database.**

Replay-protected · idempotent · relational · filterable · sortable · searchable · concurrent.
One Rust core → ships to **PyPI** and **npm** from a single source.

[![PyPI](https://img.shields.io/pypi/v/nedb-engine?label=PyPI&color=6366f1)](https://pypi.org/project/nedb-engine/)
[![npm](https://img.shields.io/npm/v/nedb-engine?label=npm&color=00d4ff)](https://www.npmjs.com/package/nedb-engine)
[![Tests](https://img.shields.io/badge/tests-266%20passing-34d399)](https://github.com/Eth-Interchained/nedb/actions)
[![nedb-engine-client PyPI](https://img.shields.io/pypi/v/nedb-engine-client?label=nedb-engine-client&color=34d399)](https://pypi.org/project/nedb-engine-client/)
[![nedb-engine-client npm](https://img.shields.io/npm/v/nedb-engine-client?label=nedb-engine-client&color=34d399)](https://www.npmjs.com/package/nedb-engine-client)

**[Studio → studio.interchained.org](https://studio.interchained.org)**  ·  **[nedb.aiassist.net](https://nedb.aiassist.net)**

</div>

---

## v2 — The DAG Engine (new in 2.0.5)

NEDB v2 replaces the append-only log (AOF) with a **content-addressed Merkle DAG**. Every document version is an immutable, BLAKE2b-verified object. Nothing is ever overwritten.

```bash
# Run the v2 DAG engine — ships inside pip install nedb-engine
nedbd --dag --data ./data
# or
NEDBD_DAG=1 NEDB_TMK=<32-byte-hex> nedbd --data ./data

curl http://127.0.0.1:7070/health
# {"ok":true,"version":"2.0.5","service":"nedbd","encrypted":true}
```

| Property | v2 DAG | v1 AOF |
|---|:---:|:---:|
| Uncorruptable (atomic writes, hash-verified reads) | ✅ | ⚠️ |
| Instant cold start (no AOF replay) | ✅ | ❌ |
| Parallel writes (no global lock) | ✅ | ❌ |
| BLAKE2b Merkle head on every response | ✅ | ❌ |
| Tombstone deletes (history preserved) | ✅ | ✅ |
| Auto-migrates v1 AOF → v2 DAG on startup | ✅ | — |
| Same HTTP API — Vision, Studio, all clients unchanged | ✅ | ✅ |

**v1 AOF engine is still shipped and unchanged** — `nedbd` (no flag) runs v1.

---

## What makes NEDB different

Every database stores *what*. NEDB stores *what*, *when*, *when it was true*, and *why* — all sealed in a cryptographic hash chain that proves none of it was tampered with.

| Capability | NEDB | SQLite | Redis | MongoDB |
|---|:---:|:---:|:---:|:---:|
| Hash-chained tamper evidence | ✅ | ❌ | ❌ | ❌ |
| Time-travel reads (`AS OF seq`) | ✅ | ❌ | ❌ | ❌ |
| Bi-temporal (`VALID AS OF date`) | ✅ | ❌ | ❌ | ❌ |
| Causal Write Provenance | ✅ | ❌ | ❌ | ❌ |
| Replay-protected idempotent writes | ✅ | ❌ | ❌ | ❌ |
| SQL + Redis + MongoDB adapters | ✅ | — | — | — |
| Concurrent group-commit daemon | ✅ | ❌ | ✅ | ✅ |
| At-rest AES-256-GCM encryption | ✅ | ❌ | ❌ | — |

---

## Install

```bash
pip install nedb-engine      # Python ≥ 3.8 — pure-Python + optional Rust native wheel
npm install nedb-engine       # Node ≥ 16   — napi-rs prebuilt binaries
```

---

## Python — 5-minute tour

```python
from nedb import NEDB

db = NEDB("./mydata")          # durable: every op is AOF-logged, fsync'd, and hash-chained
# db = NEDB()                  # or in-memory

db.create_index("users", "status", "eq")
db.create_index("users", "bio",    "search")

db.put("users", "alice", {"name": "Alice", "age": 31, "status": "active", "bio": "rust hacker"})
db.put("users", "bob",   {"name": "Bob",   "age": 24, "status": "active", "bio": "python dev"})

# NQL: WHERE + ORDER BY + LIMIT + SEARCH + TRAVERSE + GROUP BY
db.query('FROM users WHERE status = "active" ORDER BY age ASC')
db.query('FROM users SEARCH "rust"')
db.query('FROM users GROUP BY status COUNT')

# Time-travel — AS OF any past sequence
snap = db.seq
db.put("users", "alice", {"name": "Alice", "age": 32, "status": "retired"})
db.get("users", "alice", as_of=snap)          # → age 31, status active

# Bi-temporal — VALID AS OF any past date
db.put("policy", "rate_2024", {"pct": 5.0}, valid_from="2024-01-01", valid_to="2024-12-31")
db.put("policy", "rate_2025", {"pct": 6.0}, valid_from="2025-01-01")
db.query('FROM policy VALID AS OF "2024-06-15"')   # → rate 5.0

# Causal Write Provenance — why did this write happen?
db.put("inputs", "msg_1", {"text": "user prefers dark mode"})
seq_msg = db.seq
db.put("beliefs", "dark_mode", {"value": True},
       caused_by=[seq_msg], evidence="user_message", confidence=0.95)
db.query('FROM beliefs WHERE _id = "dark_mode" TRACE caused_by')   # → msg_1
db.query('FROM inputs WHERE _id = "msg_1" TRACE caused_by REVERSE') # → dark_mode

# Relations + graph traversal
db.link("users:alice", "follows", "users:bob")
db.query('FROM users WHERE _id = "alice" TRAVERSE follows')

# Hash-chain integrity
assert db.verify()             # cryptographic proof — no tampering

# SQL, Redis, MongoDB compatibility adapters
from nedb import sql_exec, RedisCompat, MongoClient
sql_exec(db, "SELECT * FROM users WHERE status = 'active' ORDER BY age DESC")
r = RedisCompat(db); r.execute("HSET", "user:1", "name", "Alice")
MongoClient(db)["users"].find({"status": "active"}).sort("age", -1).to_list()
```

---

## Redis layer-2 — wrap_redis()

Already running on Redis? Wrap your connection in one line and gain NEDB features *alongside* your existing Redis app — no migration required.

```python
import redis, json
from nedb import wrap_redis

r = wrap_redis(redis.Redis("localhost", 6379), db_name="rideshare")

# Step 1 — register: map Redis key globs to NEDB collections (chainable)
(r.nedb
 .register("driver:*", collection="driver", value_parser=json.loads)
 .register("trip:*",   collection="trip",   value_type="hash")
)

# Step 2 — backfill: import all existing Redis data into NEDB in one pass
imported = r.nedb.backfill()           # → int (keys imported)

# Step 3 — shadow: all future r.set/hset/... auto-chain into NEDB
r.nedb.shadow_writes = True

# ─── Alice's app keeps running — zero changes ───────────────────────────
r.set("driver:d1", json.dumps({"name": "Bob", "status": "active"}))   # ← shadowed
r.hset("trip:t1", mapping={"status": "en_route", "driver_id": "d1"})  # ← shadowed

# ─── New features available on the same connection ──────────────────────
r.nedb.query('FROM driver WHERE status = "active" ORDER BY lat ASC')
r.nedb.verify()       # → True  (every write chain-verified)
r.nedb.head()         # → 64-char BLAKE2b commitment hash
```

**Isolation guarantee:** NEDB never writes to Alice's namespace. It owns only:

| Key | Type | Purpose |
|-----|------|---------|
| `nedb:{db_name}:oplog` | Redis Stream | append-only op log |
| `nedb:{db_name}:snapshot` | Redis Hash | checkpoint |
| `nedb:{db_name}:meta` | Redis Hash | index config |

See [`examples/fakeredis_demo.py`](examples/fakeredis_demo.py) for a full local demo (no Redis server needed).

---

## Node.js

```javascript
import { NedbCore } from "nedb-engine";

const db = new NedbCore();               // in-memory
// const db = NedbCore.open("./data");   // durable

db.createIndex("users", "status", "eq");
db.put("users", "alice", JSON.stringify({ name: "Alice", age: 31, status: "active" }));

// Time-travel
const snap = db.seq();                   // BigInt
db.put("users", "alice", JSON.stringify({ name: "Alice", age: 32, status: "retired" }));
JSON.parse(db.getAsOf("users", "alice", snap)).age;  // → 31

// Full NQL
const rows = db.query('FROM users WHERE status = "active" ORDER BY age ASC');
rows.map(r => JSON.parse(r));

// Tamper evidence
db.verify();   // → true
db.head();     // → 64-char BLAKE2b commitment hash
db.seq();      // → BigInt
```

---

## nedbd — the concurrent server daemon

nedbd runs NEDB as a long-lived process with an HTTP/JSON API and an optional RESP2 wire protocol. Built on a **single-writer group-commit sequencer** — parallel reads, batched durable writes, one hash-chain per database, zero write-write races.

```bash
nedbd                                     # :7070, data ./nedb-data
NEDBD_RESP2_PORT=6380 nedbd               # also speak RESP2 (redis-cli compatible)
nedbd --log-level 2                       # 0=errors 1=requests 2=deploy 3=verbose
```

```bash
# Create a database with seed data and relations
curl -X POST :7070/v1/databases -d '{
  "name": "shop",
  "init": {
    "indexes": [["users","status","eq"]],
    "seed": {"users": [{"_id":"u1","name":"Alice","status":"active"}]},
    "links": [["users:u1","buys","orders:o1"]]
  }}'

# Query (full NQL including time-travel and bi-temporal)
curl -X POST :7070/v1/databases/shop/query \
  -d '{"nql":"FROM users WHERE status = \"active\" ORDER BY name ASC"}'

# Verify the hash chain
curl :7070/v1/databases/shop/verify

# MongoDB-compatible endpoint
curl -X POST :7070/v1/databases/shop/mongo \
  -d '{"collection":"users","op":"find","filter":{"status":"active"},"limit":10}'
```

**From redis-cli — no Redis installation needed:**
```bash
redis-cli -p 6380 SELECT shop
redis-cli -p 6380 SELECT shop EVAL 'FROM users SEARCH "alice"' 0
redis-cli -p 6380 SELECT shop EVAL 'FROM users AS OF 10 WHERE status = "active"' 0
redis-cli -p 6380 SELECT shop EVAL 'FROM beliefs TRACE caused_by' 0
```

---

## NQL — the NEDB Query Language

```
FROM <collection>
  [ AS OF <seq> ]                            transaction time (when was it written?)
  [ VALID AS OF "<date>" ]                   valid time (when was it true in the world?)
  [ WHERE <field> <op> <value> (AND ...) ]   op: = != < <= > >=
  [ SEARCH "<text>" ]                        full-text search
  [ ORDER BY <field> [ASC|DESC] ]
  [ TRAVERSE <relation> ]                    graph traversal
  [ TRACE caused_by [REVERSE] ]              causal provenance (why? / what did this cause?)
  [ LIMIT <n> ]
  [ GROUP BY <field> [COUNT|SUM f|AVG f|MIN f|MAX f] ]
```

Combine both time axes:
```python
# What did the system know at seq 200 about what was true on 2024-02-15?
db.query('FROM policy AS OF 200 VALID AS OF "2024-02-15"')
```

---

## Performance

**v1 Python server (baseline — single-threaded AOF):**

| Operation | Throughput | p99 latency |
|---|---|---|
| Sequential PUT | ~23/s | 44 ms |
| Concurrent PUT (16 workers) | ~92/s | 48 ms |
| Batch PUT (500 ops/request) | ~520 ops/s | 1.9 ms/op |
| Point-lookup read (NQL) | ~23/s | 44 ms |
| Rust napi PUT (FFI) | ~70K/s | — |
| Rust napi GET (FFI) | ~330K/s | — |

**v2 DAG Rust server — tokio/axum, no GIL, lock-free per-doc writes:**
> Benchmarks in progress — target 5,000–50,000 ops/s. Run `python3 tests/test_dag_perf.py` against your own instance.

Reproduce with the included benchmark:

```bash
NEDBD_DAG=1 nedbd --data /tmp/perf &
python3 tests/test_dag_perf.py --n 10000 --reads 100000
```

---

## Architecture

```
            ┌──────────────────────────────────────────────────────────┐
  put/del → │  OpLog  (BLAKE2b hash chain · per-client nonce ·          │ ← single source of truth
  link      │          idempotency keys · causal provenance fields)     │
            └───────────────┬──────────────────────────────────────────┘
            deterministic fold │ (state = pure function of the log)
     ┌──────────────┬──────────┴──────┬───────────────┬────────────────┐
     ▼              ▼                 ▼               ▼                ▼
MVCC store     Relations          Indexes         CauseMap          BlobStore
(time-travel)  (graph+AS OF)      eq/ord/search   (reverse index)   (Cascade CDC)

                     ┌─────────────────────────────────┐
  Thread-safe →      │  Sequencer (group-commit)         │ ← single writer, parallel readers
                     │  — one committer thread/db        │
                     │  — batch fsync                    │
                     └─────────────────────────────────┘

Compatibility adapters:  SQL  ·  Redis  ·  MongoDB
Wire protocols:          HTTP/JSON  ·  RESP2
Encryption:              AES-256-GCM at-rest (TMK/DEK double-envelope)
```

---

## nedb-client — lightweight HTTP client

Connect to any running nedbd instance from Python or TypeScript without embedding the engine:

```bash
pip install nedb-engine-client          # async Python
npm install nedb-engine-client   # TypeScript / Node.js 18+
```

```python
from nedb_client import NedbClient

async with NedbClient("http://127.0.0.1:7070", db="mydb") as db:
    await db.put("blocks", "618000", {"height": 618000})
    rows = await db.query("FROM blocks ORDER BY height DESC LIMIT 10")
    head = await db.head()    # BLAKE2b Merkle root — changes on every write
    ok   = await db.verify()  # tamper-evidence check across all objects
```

```typescript
import { NedbClient } from "nedb-engine-client";
const db = new NedbClient({ url: "http://127.0.0.1:7070", db: "mydb" });
await db.put("blocks", "618000", { height: 618000 });
const rows = await db.query("FROM blocks LIMIT 10");
```

---

## Repo layout

```
python/nedb/        reference engine (pure Python — always-works baseline)
rust/
  nedb-core/        v1 production Rust engine (shared by both runtimes)
  nedb-py/          maturin PyO3 binding → PyPI native wheels
  nedb-node/        napi-rs binding → npm native addons
  nedb-v2/          v2 DAG engine (tokio + axum + BLAKE2b DAG)
client/
  python/           nedb-client — async Python HTTP client (pip install nedb-engine-client)
  node/             nedb-client — TypeScript HTTP client  (npm install nedb-client)
tests/              engine + concurrent + causal + bitemporal + deploy + perf benchmarks
examples/           resp2_python.py  resp2_demo.sh
docs/               index.html  reference.html  SPEC.md
```

---

## Roadmap

- [x] Hash-chained append-only log — tamper evidence, replay protection, idempotency
- [x] MVCC time-travel — `AS OF seq`
- [x] Bi-temporal — `VALID AS OF "date"` (transaction time + valid time)
- [x] Causal Write Provenance — `caused_by`, `evidence`, `confidence`, `TRACE`
- [x] Durable AOF persistence + snapshot checkpoints
- [x] Concurrent group-commit sequencer (nedbd, 15K writes/s under load)
- [x] AES-256-GCM at-rest encryption (TMK/DEK double-envelope)
- [x] SQL / Redis / MongoDB compatibility adapters
- [x] RESP2 wire protocol (redis-cli / redis-benchmark compatible)
- [x] Rust native core — napi-rs (npm) + maturin PyO3 (PyPI)
- [x] Self-healing AOF — auto-truncates corrupt tail on startup, never hangs
- [x] **v2 DAG engine** — content-addressed Merkle DAG, atomic writes, instant cold start
- [x] **`nedbd --dag`** — one flag switches to v2 Rust engine; v1 untouched
- [x] **BLAKE2b Merkle head** — tamper-evident root on every response
- [x] **Tombstone deletes** — history preserved in DAG, live id removed from index
- [x] **Auto-migration** — v1 AOF → v2 DAG on first `--dag` startup
- [x] **nedb-client** — async Python + TypeScript HTTP client (`pip/npm install nedb-client`)
- [x] **Intel Mac support** — native wheels for `aarch64` + `x86_64` Apple Darwin
- [ ] In-memory DAG mode — `Db::in_memory()` for zero-disk ephemeral sessions
- [ ] PyO3 + napi-rs bindings updated to v2 DAG API
- [ ] NEDB Studio DAG mode toggle
- [ ] Merkle inclusion proofs — prove a document existed at a specific time to a third party
- [ ] Git-style branching — fork database state, experiment, merge or discard
- [ ] Agent Memory SDK — `Memory.remember()` / `Memory.recall()` / `Memory.trace()`
- [ ] Live query subscriptions (SSE) — push diffs when query results change

---

## NEDB Studio

Prompt-to-database scaffolding GUI with schema graph, NQL console, time-travel slider, causal provenance panel, and MongoDB/SQL/Redis tabs. Deploy from a description, query live data, edit inline.

**[studio.interchained.org](https://studio.interchained.org)** · **[github.com/Eth-Interchained/nedb-studio](https://github.com/Eth-Interchained/nedb-studio)** (GPLv3)

---

## Repos

| Repo | Description |
|---|---|
| [Eth-Interchained/nedb](https://github.com/Eth-Interchained/nedb) | Canonical source — engine, Rust core, CI |
| [Eth-Interchained/nedb-studio](https://github.com/Eth-Interchained/nedb-studio) | Studio UI (GPLv3) |
| [aiassistsecure/nedb](https://github.com/aiassistsecure/nedb) | Production mirror |
| [aiassistsecure/nedb-studio](https://github.com/aiassistsecure/nedb-studio) | Production mirror — studio |

**Packages:** [PyPI nedb-engine](https://pypi.org/project/nedb-engine/) · [npm nedb-engine](https://www.npmjs.com/package/nedb-engine)

---

## License

See `LICENSE` file. · © INTERCHAINED, LLC — [interchained.org](https://interchained.org)

---

## Authors

Built by **[Mark Allen Evans Jr.](https://interchained.org)** (INTERCHAINED, LLC)
with **Claude Sonnet 4.6** on [Hyperagent](https://hyperagent.com/refer/J2G6TCD7).

> *"Take one idea, turn it into an LP, then an app, then a system, then a platform, then infrastructure that is irreplaceable."*

[![Built with Hyperagent](https://img.shields.io/badge/Built%20with-Hyperagent-6366f1?style=flat-square)](https://hyperagent.com/refer/J2G6TCD7)
[![AiAssist](https://img.shields.io/badge/Powered%20by-AiAssist-00d4ff?style=flat-square)](https://aiassist.net)
