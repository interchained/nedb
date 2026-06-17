# NEDB v2.0.36 — Production Stable

> Cross-platform native wheels shipping the `nedbd-v2` Rust binary inside `pip install nedb-engine`. Linux, Windows, macOS arm64, and macOS x86_64 — all four platforms publish from a single `v*` tag.

NEDB is a content-addressed Merkle DAG, hash-chained, time-traveling, bi-temporal, causally-provable embedded database. Replay-protected, idempotent, relational, filterable, sortable, searchable, concurrent. One Rust core, shipped to **PyPI** and **npm** from a single source.

---

## Install

```bash
pip install nedb-engine          # Python ≥ 3.8 — pure-Python + native wheel with nedbd-v2 binary
npm install nedb-engine          # Node ≥ 16   — napi-rs prebuilt addons (Linux, Windows, macOS arm64+x86_64)
pip install nedb-engine-client   # async Python HTTP client — connect to any nedbd instance
npm install nedb-engine-client   # TypeScript / Node.js 18+ HTTP client
```

The native wheel includes the `nedbd-v2` Rust DAG-engine binary bundled inside the Python package. After `pip install`, run `nedbd --dag --data ./data` and the v2 engine boots immediately — no separate Rust build required.

---

## What's new in the v2 DAG (since v1 AOF)

NEDB v1 was an append-only log with a BLAKE2b hash chain. v2 keeps every guarantee — tamper-evident, replay-protected, hash-chained, time-traveling, bi-temporal, causally-provable — and adds a content-addressed Merkle DAG underneath. Documents are immutable, deduplicated by hash, and verifiable in parallel.

| Property | v2 DAG | v1 AOF |
|---|:---:|:---:|
| Uncorruptable (atomic writes, hash-verified reads) | yes | partial |
| O(1) warm start via MANIFEST (no scan, no replay) | yes | no |
| Deferred cold scan (socket open immediately) | yes | no |
| O(1) incremental Merkle head (never recomputed) | yes | no |
| Parallel writes (no global lock) | yes | no |
| BLAKE2b Merkle head on every response | yes | no |
| IdIndex sharded across 256 subdirectories | yes | no |
| TCP_NODELAY (no 40–200 ms loopback Nagle delay) | yes | no |
| `GET /events` SSE log stream | yes | no |
| Tombstone deletes (history preserved) | yes | yes |
| Auto-migrates v1 AOF → v2 DAG on startup | yes | — |
| Same HTTP API — Vision, Studio, all clients unchanged | yes | yes |

**v1 AOF engine is still shipped and unchanged.** Running `nedbd` (no flag) launches v1. Running `nedbd --dag` (or `NEDBD_DAG=1 nedbd`) launches the v2 DAG engine binary.

### Highlights

- **Content-addressed Merkle DAG** — every document version is an immutable BLAKE2b-verified object. Identical content is deduplicated automatically. Nothing is ever overwritten.
- **O(1) warm start** — every restart after the first open reads a tiny `MANIFEST` file and restores `seq` + Merkle `head` in milliseconds. No scan, no replay, independent of dataset size.
- **Deferred cold start** — first open of an existing dataset spawns the integrity scan in a background thread *and accepts connections immediately*. Reads serve instantly from the content-addressed DAG; writes return `HTTP 503 startup in progress` until the `startup_ready` gate flips.
- **Live event stream** — `GET /events` is a Server-Sent Events endpoint that streams scan progress (`event: scan`), ready transitions (`event: ready`), and per-write head updates (`event: write`) to any connected client. The Studio uses this for live indicators.
- **IdIndex sharding** — 256 subdirectories under `dag/` keep the filesystem fast even at millions of objects.
- **Sharded production sequencer** — group-commit batches writes; a single committer thread per database chains every op and issues one fsync per batch. Parallel readers, no write-write races, no global lock.
- **TCP_NODELAY** — the axum listener disables Nagle. On macOS loopback this eliminates the 40–200 ms artificial delay that would otherwise hit small request/response payloads.
- **AES-256-GCM at-rest encryption** — TMK/DEK double-envelope, opt-in via `NEDB_TMK=<32-byte-hex>`. Per-database DEK derived from the TMK and the database name.
- **Auto-migration** — first `--dag` startup on a v1 AOF data directory replays the log into the DAG store with zero data loss. v1 stays mountable; v2 is additive.

---

## Performance (v2.0.36)

Measured on an Intel iMac with AES-256-GCM encryption on, 10k writes / 100k reads / 30k objects, against the running `nedbd-v2` over HTTP/JSON:

| Operation | Throughput | p50 | p99 |
|---|---|---|---|
| Sequential writes | **418 ops/s** | 2.3 ms | 3.3 ms |
| Point-lookup reads | **478 ops/s** | 2.0 ms | 3.0 ms |
| ORDER BY queries | **489 ops/s** | 1.8 ms | 4.3 ms |
| Batch writes (500 ops/req) | **1,104 ops/s** | 0.9 ms | 1.2 ms |
| Tamper-verify (30k objects) | ~21,000 BLAKE2b/sec | — | 1.38 s total |

p99 latencies hold under 4 ms because of `TCP_NODELAY` on the axum listener. Without it, macOS loopback adds 40–200 ms from Nagle on every small write.

Reproduce locally:

```bash
NEDBD_DAG=1 nedbd --data /tmp/perf &
python3 tests/test_dag_perf.py --n 10000 --reads 100000
```

---

## CI architecture

NEDB v2.0.36 ships from two CI providers, coordinated by a single git tag.

### GitHub Actions — Linux + Windows

- **`pypi`** — builds the universal pure-Python wheel (`py3-none-any`) + sdist on `ubuntu-latest`. Runs the 266-test gate first, then publishes to PyPI via `twine upload --skip-existing`.
- **`wheels`** — matrix-builds the maturin native wheel for `x86_64-unknown-linux-gnu` and `x86_64-pc-windows-msvc`. Each runner compiles the `nedbd-v2` binary, stages it into the Python package layout, and builds the wheel.
- **`publish-native`** — downloads every wheel artifact (Linux, Windows, and the macOS wheels uploaded by Codemagic) and publishes them all to PyPI.
- **`create-release`** — opens the GitHub Release for the tag so Codemagic has somewhere to upload Mac `.node` binaries.
- **`node-binaries`** — napi-rs matrix on Linux + Windows; uploads each `.node` to the GitHub Release.
- **`publish-npm`** — polls the GitHub Release until all four platform `.node` binaries are present (Linux + Windows from GitHub, macOS arm64 + x86_64 from Codemagic), then runs **one** `npm publish` with the complete bundle.
- **`client-pypi` / `client-npm`** — publishes `nedb-engine-client` (HTTP client) to PyPI and npm on the same tag.

### Codemagic — macOS arm64 + x86_64

Two parallel workflows on Apple Silicon M2 Mac Minis:

- **`macos-arm64-wheel`** — builds `nedbd-v2` for `aarch64-apple-darwin`, packages it inside the maturin wheel, publishes the wheel directly to PyPI, then builds the napi addon and uploads the `.node` to the GitHub Release.
- **`macos-intel-wheel`** — cross-compiles `nedbd-v2` for `x86_64-apple-darwin` from the M2 host (configuring `CARGO_TARGET_X86_64_APPLE_DARWIN_LINKER`, `CC_x86_64_apple_darwin`, and `SDKROOT` to Xcode's clang + macOS SDK). Same flow as arm64 — wheel to PyPI, `.node` to the GitHub Release.

### Why split

GitHub's macOS runners are slow and queue-bound; cross-compiling x86_64 from a hosted arm64 macOS runner is unreliable. M2 Mac Minis on Codemagic build both Mac targets in parallel in ~20 minutes wall-clock, in lockstep with the Linux + Windows GitHub jobs. PyPI wheels upload independently (`--skip-existing` makes this safe). npm publishing waits for all four `.node` binaries to land on the GitHub Release, then publishes once with the complete bundle.

```
            push tag v2.0.36
                  │
       ┌──────────┴──────────┐
       │                     │
  GitHub Actions         Codemagic
       │                     │
  ┌────┼────┐           ┌────┴────┐
  │    │    │           │         │
 PyPI Linux Win        Mac arm64  Mac x86_64
  │    │    │           │         │
  │    └────┴───┬───────┴─────────┘
  │             │
  │      GitHub Release (.node × 4)
  │             │
  └──────► publish-npm ◄──── nedb-engine
```

---

## `nedbd --dag` startup

When you launch the v2 DAG engine, the binary prints the Merkle DAG triangle banner before binding the listener:

```

           ◆
          ╱ ╲               N E D B  ·  DAG ENGINE  2.0.36
         ◆   ◆              ─────────────────────────────────────────────
        ╱ ╲ ╱ ╲             content-addressed · tamper-evident · causal
       ◆   ◆   ◆            bi-temporal · replay-protected · encrypted
      ╱ ╲ ╱ ╲ ╱ ╲
     ◆   ◆   ◆   ◆          © INTERCHAINED, LLC  ×  Vex (Claude Sonnet 4.6)
    ╱ ╲ ╱ ╲ ╱ ╲ ╱ ╲         interchained.org   ·   hyperagent.com/refer/J2G6TCD7

  ─────────────────────────────────────────────────────────────
  listen   http://127.0.0.1:7070
  data     ./nedb-data
  enc      AES-256-GCM
  token    off (set NEDBD_TOKEN to require auth)
  ─────────────────────────────────────────────────────────────
```

The Merkle triangle is intentional — every layer doubles the previous, just like the BLAKE2b parent/child structure of the content-addressed DAG itself.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `NEDBD_DAG` | `0` | Set `1` to launch the v2 DAG engine (`nedbd-v2`). Same as `--dag`. |
| `NEDBD_HOST` | `127.0.0.1` | Bind address. **v2.0.36** defaults to loopback (was `0.0.0.0`) — security hardening. Set explicitly to `0.0.0.0` to expose. |
| `NEDBD_PORT` | `7070` | HTTP bind port. |
| `NEDBD_TOKEN` | unset | Optional bearer token; required on every `/v1/*` request when set. |
| `NEDB_TMK` | unset | 32-byte hex AES-256-GCM at-rest master key. |
| `NEDBD_DATA` | `./nedb-data` | Root directory. v2 creates `dag/`, IdIndex sharded across 256 subdirectories, and a small `MANIFEST` file. |

---

## HTTP API

The v2 DAG engine exposes the same `/v1/databases/*` surface as v1 — Vision, Studio, and every existing client work unchanged.

| Method | Path | Purpose |
|---|---|---|
| `GET`    | `/health` | Liveness + version + databases + encryption status. |
| `GET`    | `/events` | Server-Sent Events stream — scan progress, ready transitions, per-write head updates. |
| `GET`    | `/v1/databases` | List all databases with `{name, seq, head, collections}`. |
| `POST`   | `/v1/databases` | Create a database. Body: `{"name": "..."}`. |
| `GET`    | `/v1/databases/:name` | Database summary `{name, seq, head, collections}`. |
| `DELETE` | `/v1/databases/:name` | Drop a database (and its DAG directory). |
| `POST`   | `/v1/databases/:name/query` | Run an NQL query. Body: `{"nql": "..."}`. Returns `{rows, count, seq, head}`. |
| `POST`   | `/v1/databases/:name/put` | Write a document. Body: `{"coll":"...","id":"...","doc":{...},"caused_by":[],"valid_from":"...","valid_to":"..."}`. |
| `DELETE` | `/v1/databases/:name/rows/:coll/:id` | Tombstone a document. DAG history preserved; live id pointer removed. |
| `POST`   | `/v1/databases/:name/batch` | Batch ops. Body: `{"ops":[{"op":"put",...},{"op":"del",...}]}`. |
| `POST`   | `/v1/databases/:name/index` | Create an index. Body: `{"coll":"...","field":"...","kind":"eq\|sorted"}`. |
| `GET`    | `/v1/databases/:name/verify` | Walk every object, BLAKE2b-verify. Returns `{ok, objects_checked, tampered, seq, head}`. |
| `POST`   | `/v1/databases/:name/checkpoint` | No-op success on v2 (DAG is inherently snapshotted) — returns current `{ok, seq, head}`. |
| `GET`    | `/v1/databases/:name/log` | Recent log entries reconstructed from DAG objects. Query: `?limit=N` (default 50). |

Every response that mutates or reads state returns the current `seq` and BLAKE2b Merkle `head`. Clients can compare heads across requests to detect concurrent changes.

### Example: full lifecycle

```bash
# Create a database with seed data and relations
curl -X POST :7070/v1/databases -d '{
  "name": "shop",
  "init": {
    "indexes": [["users","status","eq"]],
    "seed":    {"users": [{"_id":"u1","name":"Alice","status":"active"}]},
    "links":   [["users:u1","buys","orders:o1"]]
  }}'

# Query (full NQL including time-travel and bi-temporal)
curl -X POST :7070/v1/databases/shop/query \
  -d '{"nql":"FROM users WHERE status = \"active\" ORDER BY name ASC"}'

# Verify the hash chain
curl :7070/v1/databases/shop/verify
# {"ok":true,"seq":120,"head":"b2:9c14e07a…","tamper_evident":true,"objects_checked":120,"tampered":[]}

# Tail the event stream
curl http://127.0.0.1:7070/events
# event: scan   data: {"objects":730000,"of":1310703,"rate":21043,"eta_s":28}
# event: ready  data: {"seq":1310703,"head":"b2:9c14e07a…"}
# event: write  data: {"seq":1310704,"coll":"beliefs","head":"b2:7af3c11e…"}
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

### Time-travel and bi-temporal

```python
# Time-travel — read the database as it was at seq 200
db.query('FROM users AS OF 200')

# Bi-temporal — what was true on 2024-06-15?
db.query('FROM policy VALID AS OF "2024-06-15"')

# Both axes — what did the system know at seq 200 about what was true on 2024-06-15?
db.query('FROM policy AS OF 200 VALID AS OF "2024-06-15"')
```

### Causal provenance

```python
db.put("inputs",  "msg_1",     {"text": "user prefers dark mode"})
seq_msg = db.seq
db.put("beliefs", "dark_mode", {"value": True},
       caused_by=[seq_msg], evidence="user_message", confidence=0.95)

db.query('FROM beliefs WHERE _id = "dark_mode" TRACE caused_by')          # why?  → msg_1
db.query('FROM inputs  WHERE _id = "msg_1"     TRACE caused_by REVERSE')  # so what? → dark_mode
```

### Search, traversal, aggregation

```python
db.query('FROM users   SEARCH "rust"')
db.query('FROM users   WHERE _id = "alice" TRAVERSE follows')
db.query('FROM orders  GROUP BY status COUNT')
db.query('FROM orders  GROUP BY user_id SUM total ORDER BY total DESC LIMIT 10')
```

---

## Migration from v1

`nedbd --dag` on an existing v1 AOF data directory replays the log into the DAG store on first startup. The v1 `log.aof` and `snapshot.json` stay in place — you can roll back by launching `nedbd` (no `--dag`) and v1 mounts the same directory. After migration, every subsequent restart is an O(1) warm start.

---

## Links

- **Source:** [github.com/aiassistsecure/nedb](https://github.com/aiassistsecure/nedb)
- **PyPI:** [pypi.org/project/nedb-engine](https://pypi.org/project/nedb-engine)
- **npm:** [npmjs.com/package/nedb-engine](https://www.npmjs.com/package/nedb-engine)
- **Studio:** [studio.interchained.org](https://studio.interchained.org)
- **HTTP client (Python):** [pypi.org/project/nedb-engine-client](https://pypi.org/project/nedb-engine-client)
- **HTTP client (Node):** [npmjs.com/package/nedb-engine-client](https://www.npmjs.com/package/nedb-engine-client)

---

© INTERCHAINED, LLC × Vex (Claude Sonnet 4.6)
