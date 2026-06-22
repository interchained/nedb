# nedb-engine

**NEDB v2 — content-addressed DAG storage engine with NQL and HTTP server**

[![crates.io](https://img.shields.io/crates/v/nedb-engine?color=f97316)](https://crates.io/crates/nedb-engine)
[![License: BUSL-1.1](https://img.shields.io/badge/license-BUSL--1.1-blue)](https://github.com/Eth-Interchained/nedb/blob/master/LICENSE)
[![GitHub](https://img.shields.io/badge/github-Eth--Interchained%2Fnedb-8da0cb)](https://github.com/Eth-Interchained/nedb)

This crate ships the **`nedbd`** binary — the NEDB v2 DAG HTTP server. Install it, point it at a data directory, and any language can speak to it over HTTP/JSON.

```bash
cargo install nedb-engine
nedbd ./data              # AOF engine (pure Rust, v1-compatible)
nedbd --dag ./data        # DAG engine (v2, content-addressed, recommended)
```

---

## What is NEDB v2?

NEDB v2 replaces the append-only log (AOF) with a **content-addressed Merkle DAG**:

- Every document version is an **immutable, BLAKE2b-hashed object**. Nothing is ever overwritten.
- Every write produces a new **chain head** — a BLAKE2b commitment over the entire database history.
- **Time-travel**: read any document `AS OF seq N` to see exactly what it contained at that point.
- **Causal provenance**: documents link to their causal parents via `caused_by` hashes. `TRACE caused_by` walks the full causal graph backward.
- **TRAVERSE**: named graph relations via `__links__`. `FROM person WHERE _id = "robert" TRAVERSE parent_of` returns linked nodes.
- **O(1) warm start**: a `MANIFEST` file stores `seq` + Merkle head so restarts never re-scan the entire object store.
- **Instant cold start**: the daemon accepts connections immediately; background scan loads objects incrementally.
- **AES-256-GCM at rest**: optional symmetric encryption with a double-envelope key structure (TMK wraps DEK).

---

## Install

```bash
cargo install nedb-engine
```

The `nedbd` binary lands in `~/.cargo/bin/`. Make sure that's on your `PATH`.

To verify:

```bash
nedbd --doctor      # diagnose your NEDB environment
```

---

## Usage

```
nedbd [OPTIONS] [data_dir]
```

| Argument | Default | Description |
|---|---|---|
| `data_dir` | `./nedb-data` | Directory for database files |
| `--dag` | off | Use the v2 content-addressed DAG engine |
| `--doctor` | — | Diagnose environment, print fix commands |

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `NEDBD_HOST` | `127.0.0.1` | Bind address |
| `NEDBD_PORT` | `7070` | HTTP port |
| `NEDBD_TOKEN` | — | Bearer token for auth (optional) |
| `NEDB_TMK` | — | 64-char hex master key for AES-256-GCM encryption at rest |
| `NEDBD_MEMORY` | — | `1` = pure in-memory mode (no disk I/O) |
| `NEDBD_DAG` | — | `1` = same as `--dag` flag |

---

## HTTP API

All endpoints return JSON. Auth: `Authorization: Bearer <token>` if `NEDBD_TOKEN` is set.

```
GET    /health
GET    /v1/databases
POST   /v1/databases                    {name, init?}
GET    /v1/databases/<name>
DELETE /v1/databases/<name>
POST   /v1/databases/<name>/put         {coll, id, doc, caused_by?}
POST   /v1/databases/<name>/query       {nql}
POST   /v1/databases/<name>/link        {frm, rel, to}
POST   /v1/databases/<name>/neighbors   {node, rel}
GET    /v1/databases/<name>/verify
POST   /v1/databases/<name>/checkpoint
```

---

## NQL — NEDB Query Language

```sql
-- Basic queries
FROM person LIMIT 10
FROM driver WHERE status = "active"
FROM driver WHERE rating >= 4.5 ORDER BY rating DESC

-- Time-travel
FROM will WHERE _id = "evans_will_2019" AS OF 5

-- Causal trace (walks caused_by links backward)
FROM event WHERE _id = "probate_filing" TRACE caused_by

-- Graph traversal
FROM person WHERE _id = "robert" TRAVERSE parent_of
```

---

## Example: The Will — causal DAG in action

```python
import json, os, urllib.request

def put(db_url, coll, id_, doc):
    body = json.dumps({"coll": coll, "id": id_, "doc": doc}).encode()
    req = urllib.request.Request(f"{db_url}/put", body,
          {"Content-Type": "application/json"}, method="POST")
    return json.loads(urllib.request.urlopen(req).read())["doc"]

BASE = "http://localhost:7070/v1/databases/will"

# Robert writes his will
robert = put(BASE, "person", "robert", {"name": "Robert Evans", "role": "testator"})
will_v1 = put(BASE, "will", "evans_will", {
    "house": "mark", "business": "lisa",
    "caused_by": [robert["_hash"]],   # causal link to testator record
})

# Amendment — chains off v1
will_v2 = put(BASE, "will", "evans_will", {
    "house": "mark", "business": "lisa", "vintage_car": "mark",
    "caused_by": [will_v1["_hash"]],
})

# TRACE proves the amendment links back to the original
query = json.dumps({"nql": 'FROM will WHERE _id = "evans_will" TRACE caused_by'}).encode()
req = urllib.request.Request(f"{BASE}/query", query,
      {"Content-Type": "application/json"}, method="POST")
trace = json.loads(urllib.request.urlopen(req).read())["rows"]
# → both versions + Robert's testator record, in causal order
```

---

## Python companion

The `nedb-engine` PyPI package ships the same server binary bundled in the wheel, plus:
- Pure-Python AOF engine (`NEDB` class)
- Embedded v2 DAG API (`nedb._native.NedbCore`) on supported platforms
- `nedbd` console script

```bash
pip install nedb-engine

# Use embedded API (Linux/macOS/Windows CPython)
python3 -c "from nedb._native import NedbCore; db = NedbCore(); ..."

# Use HTTP mode (any platform, including MSYS2/MinGW)
NEDB_URL=http://localhost:7070 python3 your_script.py

# Diagnose
nedbd --doctor
```

---

## License

[BUSL-1.1](https://github.com/Eth-Interchained/nedb/blob/master/LICENSE) — Business Source License. Free for development and evaluation; production use requires a commercial licence from INTERCHAINED, LLC.

---

*Built by [INTERCHAINED, LLC](https://interchained.org) × Claude Sonnet 4.6*
