# nedb-engine-client

> Async Python client for [nedbd](https://github.com/Eth-Interchained/nedb) — the NEDB server daemon.

[![PyPI](https://img.shields.io/pypi/v/nedb-engine-client?color=6366f1)](https://pypi.org/project/nedb-engine-client/)
[![Python](https://img.shields.io/pypi/pyversions/nedb-engine-client?color=34d399)](https://pypi.org/project/nedb-engine-client/)
[![License](https://img.shields.io/badge/license-GPL--3.0--or--later-818cf8)](https://github.com/Eth-Interchained/nedb/blob/master/LICENSE)

Connect to any running `nedbd` instance — local or remote — with a clean async API. No engine code embedded, no Rust toolchain required. Just HTTP.

---

## Install

```bash
pip install nedb-engine-client
```

Requires Python ≥ 3.8 and `httpx`.

---

## Quick start

```python
import asyncio
from nedb_client import NedbClient

async def main():
    async with NedbClient("http://127.0.0.1:7070", db="mydb") as db:

        # Write a document
        await db.put("blocks", "618000", {
            "height": 618000,
            "hash": "000000000000000000024bead8df69990852c202db0e0097c1a12ea637d7e96d",
            "tx_count": 2734,
        })

        # Query with NQL
        rows = await db.query("FROM blocks ORDER BY height DESC LIMIT 10")

        # Time-travel: what did the DB look like at seq 100?
        old = await db.query("FROM blocks AS OF 100 WHERE height > 600000")

        # Bi-temporal: what was true on 2024-06-15?
        valid = await db.query('FROM policy VALID AS OF "2024-06-15"')

        # Causal trace: why was this written?
        trace = await db.query("FROM blocks TRACE caused_by")

        # BLAKE2b Merkle head — changes on every write, anchorable
        head = await db.head()
        print(f"head: {head}")

        # Tamper-evidence check across all objects
        report = await db.verify()
        assert report["ok"], "tamper detected!"

asyncio.run(main())
```

---

## nedbd — start the server

```bash
pip install nedb-engine

# v1 AOF engine (default)
nedbd --data ./data

# v2 DAG engine (recommended — instant cold start, tamper-evident)
NEDBD_DAG=1 nedbd --data ./data

# With AES-256-GCM encryption
NEDBD_DAG=1 NEDB_TMK=<32-byte-hex> nedbd --data ./data

# Check health
curl http://127.0.0.1:7070/health
# {"ok":true,"version":"2.0.8","service":"nedbd","encrypted":true}
```

---

## API reference

### Client lifecycle

```python
# Async context manager (recommended)
async with NedbClient(url, db=name, token=token) as db:
    ...

# Manual
db = NedbClient(url="http://127.0.0.1:7070", db="mydb", token="secret")
await db.open()
await db.close()
```

### Writes

| Method | Description |
|--------|-------------|
| `await db.put(coll, id, doc, **opts)` | Write a document |
| `await db.delete(coll, id)` | Tombstone delete (history preserved in DAG) |
| `await db.batch(ops)` | Batch put/del in one HTTP round-trip |
| `await db.create_index(coll, field)` | Create sorted index for ORDER BY |

**Put options:**

```python
await db.put("claims", "c1", {"fact": "..."}, 
    caused_by=["abc123hash"],   # DAG causal provenance
    valid_from="2024-01-01",    # bi-temporal valid window
    valid_to="2024-12-31",
    evidence="sensor-42",       # human-readable provenance note
    confidence=0.95,            # confidence score 0–1
    idem="dedup-key",           # idempotency key
)
```

### Reads

| Method | Description |
|--------|-------------|
| `await db.get(coll, id)` | Fetch current version of a document |
| `await db.query(nql)` | NQL query → list of dicts |
| `await db.query_full(nql)` | NQL query → full response (rows + seq + head) |

### NQL — NEDB Query Language

```
FROM <collection>
  [AS OF <seq>]                  transaction time (when was it written?)
  [VALID AS OF "<date>"]         valid time (when was it true in the world?)
  [WHERE field = value [AND ...]] op: = != < <= > >=
  [ORDER BY field [DESC]]
  [LIMIT n]
  [GROUP BY field COUNT|SUM|AVG|MIN|MAX]
  [TRACE caused_by [REVERSE]]    causal graph traversal
  [SEARCH "text"]                full-text search
```

### Integrity

| Method | Description |
|--------|-------------|
| `await db.verify()` | BLAKE2b tamper-evidence check across all objects |
| `await db.head()` | Current Merkle root — changes on every write |
| `await db.seq()` | Current global sequence number |
| `await db.log(limit)` | Recent write log |
| `await db.checkpoint()` | Explicit checkpoint (no-op on v2 DAG) |

### Server management

| Method | Description |
|--------|-------------|
| `await db.health()` | Server health — version, databases, encryption |
| `await db.ping()` | Boolean reachability check |
| `await db.list_databases()` | All databases on this server |
| `await db.create_database()` | Create this database explicitly |
| `await db.drop_database()` | Drop this database (irreversible) |

---

## Error handling

```python
from nedb_client import NedbClient, NedbError

async with NedbClient("http://127.0.0.1:7070", db="mydb") as db:
    try:
        await db.put("coll", "id", {"data": "value"})
    except NedbError as e:
        print(f"HTTP {e.status}: {e.message}")
```

Queries on missing collections return `[]` rather than raising — resilient by default.

---

## Links

- **Engine:** [pip install nedb-engine](https://pypi.org/project/nedb-engine/) · [npm install nedb-engine](https://www.npmjs.com/package/nedb-engine)
- **JS/TS client:** [npm install nedb-engine-client](https://www.npmjs.com/package/nedb-engine-client)
- **Source:** [github.com/Eth-Interchained/nedb](https://github.com/Eth-Interchained/nedb)
- **Studio:** [studio.interchained.org](https://studio.interchained.org)

---

© [INTERCHAINED, LLC](https://interchained.org) · GPL-3.0-or-later · Built with [Hyperagent](https://hyperagent.com/refer/J2G6TCD7)
