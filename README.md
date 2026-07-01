<div align="center">

# NEDB

**Content-addressed Merkle DAG · Hash-chained · Time-traveling · Bi-temporal · Causally-provable embedded database.**

Replay-protected · idempotent · relational · filterable · sortable · searchable · concurrent.
One Rust core → ships to **PyPI** and **npm** from a single source.

[![PyPI](https://img.shields.io/pypi/v/nedb-engine?label=PyPI&color=6366f1)](https://pypi.org/project/nedb-engine/)
[![crates.io](https://img.shields.io/crates/v/nedb-engine?label=crates.io&color=f97316)](https://crates.io/crates/nedb-engine)
[![npm](https://img.shields.io/npm/v/nedb-engine?label=npm&color=00d4ff)](https://www.npmjs.com/package/nedb-engine)
[![CI](https://img.shields.io/github/actions/workflow/status/Eth-Interchained/nedb/release.yml?label=CI&color=34d399)](https://github.com/Eth-Interchained/nedb/actions)
[![nedb-engine-client PyPI](https://img.shields.io/pypi/v/nedb-engine-client?label=nedb-engine-client&color=34d399)](https://pypi.org/project/nedb-engine-client/)
[![nedb-engine-client npm](https://img.shields.io/npm/v/nedb-engine-client?label=nedb-engine-client&color=34d399)](https://www.npmjs.com/package/nedb-engine-client)

**[Studio → studio.interchained.org](https://studio.interchained.org)**  ·  **[nedb.aiassist.net](https://nedb.aiassist.net)**

</div>

---

## NEDB v2.5.34 — Production Stable

**Current stable: 2.5.34** — NEDB ships as **three version-aligned distributions** on one tag — `nedb-engine` (flagship), `crypto-database` (verifiable v2/v3 DAG), and `aof-db` (fast append-only) — across npm / PyPI / crates.io with full mac + linux + windows native addons (see [**Releasing**](#releasing) below). On the engine side it remains a polish release on the complete cross-platform line. The `nedbd-v2` daemon now does **real CLI parsing** — `--dag-v3`, `--data`, `--fast-fsync`, `--help`, `--version` are recognized flags instead of being silently swallowed as the positional data dir — and `npm test` ships a **cinematic native smoke test** that tours v1→v2 migration, the v2 DAG, the v3 segment store, and a causal-provenance audit. All native wheels (Linux + Windows on GitHub Actions; macOS arm64 + x86_64 on Codemagic M2 Mac Minis) **plus** the universal pure-Python wheel ship from a single `v*` tag, with the `nedbd-v2` binary bundled inside `pip install nedb-engine`.

**New in 2.5.x:**

- **Durable-mode auto-flush-on-exit** — a durable store flushes buffered writes on `Ctrl+C` / `SIGTERM`, not just on a clean `Drop`. Automatic in the Node and Python bindings; `Db::install_exit_flush(Arc<Db>)` for standalone Rust binaries. See [**docs/DURABILITY.md**](docs/DURABILITY.md).
- **`nedb-cli`** — operate on a store directory offline (`head` · `status` · `verify` · `get` · `scan` · `flush` · `repair` · `export`), and **`nedb-inspector`** — a deterministic (no-regex, no-LLM) checker that warns when a durable open lacks flush-on-exit wiring. See [**docs/CLI.md**](docs/CLI.md).
- **Replication contract** — `tip()` (the latest write), a bounded `since()` changefeed, and a `scan_status()` readiness gate. See [**docs/REPLICATION.md**](docs/REPLICATION.md).

**The v3 storage line — consolidated, spec'd, and (as of 2.4.3) cleanly published across every platform.** It makes the NEDB **v3 segment/pack object store** a first-class, fully-documented feature:

- **`--dag-v3`** (opt-in) — append-only segment store: one `fsync` per group-commit, `.idx` sidecars, compaction, non-destructive dual-read. Took a real itcd chainstate flush from *minutes* to **~1.3 s**. Parsed as a real flag by `nedbd-v2` as of v2.4.3 (or set `NEDB_DAG_V3=1`). (See the v3 section below.)
- **`NEDB_FAST_FSYNC`** — macOS fast-fsync: a plain `fsync(2)` instead of `F_FULLFSYNC` (default off; no-op on Linux/Windows).
- Durable **flush-on-close** — and, as of 2.5.x, **flush-on-exit** on `Ctrl+C` / `SIGTERM` (see [**docs/DURABILITY.md**](docs/DURABILITY.md)) — a **Windows-safe id-index** (percent-encodes filesystem-unsafe ids), and idempotent object re-writes.
- **`docs/SPEC.md` §3** now formally specifies the v2 object store, the v3 substrate, and the durability model.

NEDB v2 replaces the append-only log (AOF) with a **content-addressed Merkle DAG**. Every document version is an immutable, BLAKE2b-verified object. Nothing is ever overwritten. As of **v2.2.31**, restarts after the first open are **O(1) warm starts** (driven by a `MANIFEST` of `seq` + Merkle head), the **cold scan is deferred** so the daemon accepts connections immediately, and a new **`GET /events` SSE endpoint** streams scan progress + per-write events live.

```bash
# Run the v2 DAG engine — ships inside pip install nedb-engine
nedbd --dag --data ./data
# or
NEDBD_DAG=1 NEDB_TMK=<32-byte-hex> nedbd --data ./data

curl http://127.0.0.1:7070/health
# {"ok":true,"version":"2.2.31","service":"nedbd","engine":"dag","startup_ready":true,"encrypted":true}

# Tail the live event stream (new in v2.2.31)
curl http://127.0.0.1:7070/events
# event: scan   data: {"objects":730000,"of":1310703,"rate":21043,"eta_s":28}
# event: ready  data: {"seq":1310703,"head":"b2:9c14e07a…"}
# event: write  data: {"seq":1310704,"coll":"beliefs","head":"b2:7af3c11e…"}
```

| Property | v2 DAG | v1 AOF |
|---|:---:|:---:|
| Uncorruptable (atomic writes, hash-verified reads) | ✅ | ⚠️ |
| O(1) warm start via MANIFEST (no scan, no replay) | ✅ | ❌ |
| Deferred cold scan (socket open immediately) | ✅ | ❌ |
| O(1) incremental Merkle head (never recomputed) | ✅ | ❌ |
| Parallel writes (no global lock) | ✅ | ❌ |
| BLAKE2b Merkle head on every response | ✅ | ❌ |
| IdIndex sharded across 256 subdirectories | ✅ | ❌ |
| TCP_NODELAY (no 40–200 ms loopback Nagle delay) | ✅ | ❌ |
| `GET /events` SSE log stream | ✅ | ❌ |
| Tombstone deletes (history preserved) | ✅ | ✅ |
| Auto-migrates v1 AOF → v2 DAG on startup | ✅ | — |
| Same HTTP API — Vision, Studio, all clients unchanged | ✅ | ✅ |

**v1 AOF engine is still shipped and unchanged** — `nedbd` (no flag) runs v1.

**Production status:** [vision.interchained.org](https://vision.interchained.org) is live on v2.2.31 — **1,310,703 sequences** indexed in the Vision database, AES-256-GCM encrypted at rest, at block height **620,989**.

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
nedbd                                     # :7070, data ./nedb-data (v1 AOF engine)
nedbd --dag --data ./data                 # v2 DAG engine (or NEDBD_DAG=1)
NEDBD_RESP2_PORT=6380 nedbd               # also speak RESP2 (redis-cli compatible)
nedbd --log-level 2                       # 0=errors 1=requests 2=deploy 3=verbose

# Live event stream (new in v2.2.31) — SSE: scan progress, ready, per-write head
curl http://127.0.0.1:7070/events
```

### Companion CLIs

Alongside the daemon, `cargo install nedb-engine` ships **`nedb-cli`** — operate on a store directory offline (`head`/`status`/`verify`/`get`/`scan`/`flush`/`repair`/`export`) — and **`nedb-inspector`**, a deterministic checker that warns when a durable open lacks flush-on-exit wiring. Full reference: [**docs/CLI.md**](docs/CLI.md).

### Startup modes (v2.2.31)

- **Warm start** — every restart after the first open reads the `MANIFEST` file and restores `seq` + Merkle `head` in **O(1)**. No scan, no replay, independent of dataset size. Boots in milliseconds.
- **Cold start** — first open of an existing dataset spawns the integrity scan in a background thread *and accepts connections immediately*. Reads serve instantly from the content-addressed DAG; writes return `HTTP 503 startup in progress` until the `startup_ready` gate flips. Progress (objects, rate, ETA) streams over `GET /events`.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `NEDBD_DAG` | `0` | Set `1` to launch the v2 DAG engine (`nedbd-v2`). Same as `--dag`. |
| `NEDBD_HOST` | `127.0.0.1` | Bind address. **v2.2.31** defaults to loopback (was `0.0.0.0`) — security hardening fix. Set explicitly to `0.0.0.0` to expose. |
| `NEDBD_PORT` | `7070` | HTTP bind port. |
| `NEDBD_TOKEN` | unset | Optional bearer token; required on every `/v1/*` request when set. |
| `NEDB_TMK` | unset | 32-byte hex AES-256-GCM at-rest encryption key. |
| `NEDBD_DATA` | `./nedb-data` | Root directory. v2 creates `dag/`, IdIndex sharded across **256 subdirectories**, and a small `MANIFEST` file. |

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

**v2 DAG Rust server (v2.2.31, Intel iMac — 10k writes / 100k reads / 30k objects, AES-256-GCM on):**

| Operation | Throughput | p50 | p99 |
|---|---|---|---|
| Sequential writes | **418 ops/s** | 2.3 ms | 3.3 ms |
| Point-lookup reads | **478 ops/s** | 2.0 ms | 3.0 ms |
| ORDER BY queries | **489 ops/s** | 1.8 ms | 4.3 ms |
| Batch writes (500 ops/req) | **1,104 ops/s** | 0.9 ms | 1.2 ms |
| Tamper-verify (30k objects) | ~21,000 BLAKE2b/sec | — | 1.38 s total |

p99 latencies hold because of `TCP_NODELAY` on the axum listener — without it macOS loopback adds the Nagle algorithm's 40–200 ms delay on small writes.

**v1 Python server (baseline — single-threaded AOF):**

| Operation | Throughput | p99 latency |
|---|---|---|
| Sequential PUT | ~23/s | 44 ms |
| Concurrent PUT (16 workers) | ~92/s | 48 ms |
| Batch PUT (500 ops/request) | ~520 ops/s | 1.9 ms/op |
| Point-lookup read (NQL) | ~23/s | 44 ms |
| Rust napi PUT (FFI) | ~70K/s | — |
| Rust napi GET (FFI) | ~330K/s | — |

Reproduce with the included benchmark:

```bash
NEDBD_DAG=1 nedbd --data /tmp/perf &
python3 tests/test_dag_perf.py --n 10000 --reads 100000
```

---

## NEDB v3 — Segment / Pack Object Store

**v3 is an opt-in storage substrate that replaces the loose one-file-per-object layout with append-only *segment packs* — the difference between a chainstate flush that takes *minutes* and one that takes *under two seconds*.** It is **off by default** (byte-for-byte v2), enabled with one flag, and **transparent** to everything above the storage layer: NQL, `AS OF`, `VALID AS OF`, `TRACE`, the BLAKE2b Merkle head, and causal provenance all behave identically.

### Why it exists

v2 stores every document version as its own content-addressed file at `objects/{hash[:2]}/{hash[2:]}`. That makes writes trivially atomic (write `.tmp` → `rename`) and corruption-proof — but each write costs a file create + `fsync` + rename **plus** a directory B-tree update. At scale that filesystem-metadata churn dominates: on a busy disk it caps sustained writes around **~185/s**, and a batch flush of a few thousand objects degrades into minutes. The bottleneck is the *number of files touched*, not the bytes written.

### What it does

v3 batches objects into append-only **segment packs** — `objects/segments/seg-NNNNNN.dat` — where each record is `[content_len: u32-LE][content]`. A write appends to the active segment and updates an in-memory `hash → (segment_id, offset, len)` map; a batch commits with a **single `fsync`**. Thousands of per-file syscalls collapse into one sequential append plus one durability point, so **flush cost scales with bytes (sequential I/O), not object-count × syscall overhead.**

- **Compaction / pruning** — `compact()` keeps the *live set* (the current version of every document, resolved from the id-index), rewrites those records into fresh segments, and reclaims the superseded/dead versions.
- **`.idx` sidecars** — each segment carries a sidecar (`NIX1` magic + entry count + fixed 44-byte entries + a BLAKE2b-256 checksum) so reopen rebuilds the in-memory index by reading the sidecar instead of scanning the whole pack. A missing or corrupt sidecar falls back to a full scan-and-heal — slower, never fatal.
- **Dual-read migration** — opening an existing v2 store in v3 mode is **non-destructive**: old loose objects stay fully readable, and only *new* writes go to segments. No migration step, no downtime, no rewrite.
- **Durable flush-on-close** — `flush_all()` (and `Db`'s `Drop`) fsync the active segment, matching the flush-on-close contract of sled / RocksDB.

### How to enable

```bash
# Engine / nedbd-v2 (the native daemon from npm / the native wheel)
nedbd-v2 --dag-v3 --data /var/lib/nedb     # real flag as of v2.4.3 — or set NEDB_DAG_V3=1

# itcd — Bitcoin-fork node embedding NEDB via nedb-ffi
interchainedd -dagv3                        # puts chainstate AND block index on segments
```

The switch is read once, when each database's object store is constructed at open time. Default off → v2 loose objects.

### Real-world result

itcd (a Bitcoin Core 0.21 fork that replaces LevelDB chainstate with NEDB) syncing on `-dagv3`, measured `FlushStateToDisk` on real chainstate:

| Flush (coins → disk) | v3 segment store | v2 loose store |
|---|---|---|
| 2,002 coins / 275 kB | **1.93 s** | *minutes* |
| 2,549 coins / 366 kB | **1.71 s** | *minutes* |

Note the *larger* batch finishing *faster* — v3's cost is dominated by the single per-batch `fsync`, not per-coin work, so effective throughput (~1,000–1,500 coins/s here) climbs as batches grow, against the loose store's ~185 writes/s metadata ceiling. The gap only widens as the UTXO set grows: sequential-append cost tracks data volume, while per-file cost compounds with object count.

### When to use it

Reach for v3 on high-write, large-object-count workloads — blockchain chainstate / block index, event sourcing, high-frequency agent memory. For small or read-mostly stores the loose layout is perfectly fine, which is exactly why v3 stays opt-in.

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
- [x] **v3 segment/pack object store** — opt-in `--dag-v3`: append-only packs, one fsync per batch, compaction + `.idx` sidecars, non-destructive dual-read (minutes → <2s chainstate flush on itcd)
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

**[studio.interchained.org](https://studio.interchained.org)** · **[github.com/aiassistsecure/nedb-studio](https://github.com/aiassistsecure/nedb-studio)** (GPLv3)

---

## Repos

| Repo | Description |
|---|---|
| [aiassistsecure/nedb](https://github.com/aiassistsecure/nedb) | Source — engine, Rust core, CI |
| [aiassistsecure/nedb-studio](https://github.com/aiassistsecure/nedb-studio) | Studio UI (GPLv3) |

**Packages:** [PyPI nedb-engine](https://pypi.org/project/nedb-engine/) · [npm nedb-engine](https://www.npmjs.com/package/nedb-engine)

---

## Releasing

NEDB and its two distributions (`crypto-database`, `aof-db`) ship from a **single version tag** via one committed tool:

```bash
python3 scripts/release.py "vFROM" "vTO"
# e.g. the first run of the 2.4.468 line:
python3 scripts/release.py "v2.4.68" "v2.4.468"
```

Both arguments require the leading `v` (e.g. `v2.4.468`). The script:

1. **Bumps every version-bearing manifest** (npm / PyPI / crates + the engine crate, clients, and the maturin project) in the flagship **and** both distribution forks from `FROM` to `TO`, opening and merging a release PR per repo.
2. **Repoints the `distributions/*` submodules** to the freshly-bumped fork masters.
3. **Tags `vTO`** on `master`, firing CI/CD — `release.yml` (flagship) + `release-distros.yml` (distros) + Codemagic (macOS wheels/addons) — to publish `nedb-engine` + `crypto-database` + `aof-db` aligned on one version across npm, PyPI, and crates.io.

It is **idempotent**: a manifest line already at `TO` is left untouched, a repo already fully at `TO` produces no empty PR, and an existing `vTO` tag is left in place — so a half-finished release can be re-run safely, and the remaining steps (submodule repoint, tag) always run even when the version was already correct.

Requires `GITHUB_TOKEN` (`repo` + `workflow` scope) in the environment. It never force-pushes `master` and never commits to it directly — every change lands through a branch + PR + merge.

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
