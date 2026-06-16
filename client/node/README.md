# nedb-engine-client

> TypeScript/JavaScript client for [nedbd](https://github.com/Eth-Interchained/nedb) — the NEDB server daemon.

[![npm](https://img.shields.io/npm/v/nedb-engine-client?color=00d4ff)](https://www.npmjs.com/package/nedb-engine-client)
[![License](https://img.shields.io/badge/license-GPL--3.0--or--later-818cf8)](https://github.com/Eth-Interchained/nedb/blob/master/LICENSE)

Connect to any running `nedbd` instance from Node.js or the browser. No engine embedded, no native dependencies. Just fetch.

---

## Install

```bash
npm install nedb-engine-client
```

Requires Node.js ≥ 18 (uses native `fetch`). Works in modern browsers too.

---

## Quick start

```typescript
import { NedbClient } from "nedb-engine-client";

const db = new NedbClient({ url: "http://127.0.0.1:7070", db: "mydb" });

// Write a document
await db.put("blocks", "618000", {
    height: 618000,
    hash: "000000000000000000024bead8df69990852c202db0e0097c1a12ea637d7e96d",
    txCount: 2734,
});

// Query with NQL
const rows = await db.query("FROM blocks ORDER BY height DESC LIMIT 10");

// Time-travel: what did the DB look like at seq 100?
const old = await db.query("FROM blocks AS OF 100 WHERE height > 600000");

// Bi-temporal: what was true on 2024-06-15?
const valid = await db.query('FROM policy VALID AS OF "2024-06-15"');

// BLAKE2b Merkle head — changes on every write, anchorable
const head = await db.head();

// Tamper-evidence check across all objects
const report = await db.verify();
console.assert(report.ok, "tamper detected!");
```

---

## nedbd — start the server

```bash
npm install nedb-engine   # or: pip install nedb-engine

# v2 DAG engine (recommended — instant cold start, tamper-evident)
NEDBD_DAG=1 nedbd --data ./data

# Check health
curl http://127.0.0.1:7070/health
# {"ok":true,"version":"2.0.8","service":"nedbd","encrypted":true}
```

---

## API reference

### Constructor

```typescript
const db = new NedbClient({
    url:            "http://127.0.0.1:7070",  // nedbd base URL
    db:             "mydb",                    // database name
    token:          "secret",                  // bearer auth (optional)
    autoCreate:     true,                      // create DB on first write
    readTimeoutMs:  3_000,                     // query timeout
    writeTimeoutMs: 30_000,                    // write timeout
});
```

### Writes

| Method | Description |
|--------|-------------|
| `db.put(coll, id, doc, opts?)` | Write a document |
| `db.delete(coll, id)` | Tombstone delete (history preserved in DAG) |
| `db.batch(ops)` | Batch put/del in one HTTP round-trip |
| `db.createIndex(coll, field)` | Create sorted index for ORDER BY |

**Put options:**

```typescript
await db.put("claims", "c1", { fact: "..." }, {
    causedBy:   ["abc123hash"],  // DAG causal provenance
    validFrom:  "2024-01-01",    // bi-temporal valid window
    validTo:    "2024-12-31",
    evidence:   "sensor-42",     // human-readable provenance note
    confidence: 0.95,            // confidence score 0–1
    idem:       "dedup-key",     // idempotency key
});
```

### Reads

| Method | Description |
|--------|-------------|
| `db.get(coll, id)` | Fetch current version of a document |
| `db.query(nql)` | NQL query → array of objects |
| `db.queryFull(nql)` | NQL query → full response (rows + seq + head) |

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
| `db.verify()` | BLAKE2b tamper-evidence check across all objects |
| `db.head()` | Current Merkle root — changes on every write |
| `db.seq()` | Current global sequence number |
| `db.log(limit?)` | Recent write log |
| `db.checkpoint()` | Explicit checkpoint (no-op on v2 DAG) |

### Server management

| Method | Description |
|--------|-------------|
| `db.health()` | Server health — version, databases, encryption |
| `db.ping()` | Boolean reachability check |
| `db.listDatabases()` | All databases on this server |
| `db.createDatabase()` | Create this database explicitly |
| `db.dropDatabase()` | Drop this database (irreversible) |

---

## Error handling

```typescript
import { NedbClient, NedbError } from "nedb-engine-client";

try {
    await db.put("coll", "id", { data: "value" });
} catch (e) {
    if (e instanceof NedbError) {
        console.error(`HTTP ${e.status}: ${e.message}`);
    }
}
```

Queries on missing collections return `[]` rather than throwing — resilient by default.

---

## Batch writes

```typescript
await db.batch([
    { op: "put", coll: "blocks", id: "618001", doc: { height: 618001 } },
    { op: "put", coll: "blocks", id: "618002", doc: { height: 618002 } },
    { op: "del", coll: "blocks", id: "617999" },
]);
```

One HTTP request, multiple ops — best throughput for bulk ingestion.

---

## Links

- **Engine:** [pip install nedb-engine](https://pypi.org/project/nedb-engine/) · [npm install nedb-engine](https://www.npmjs.com/package/nedb-engine)
- **Python client:** [pip install nedb-engine-client](https://pypi.org/project/nedb-engine-client/)
- **Source:** [github.com/Eth-Interchained/nedb](https://github.com/Eth-Interchained/nedb)
- **Studio:** [studio.interchained.org](https://studio.interchained.org)

---

© [INTERCHAINED, LLC](https://interchained.org) · GPL-3.0-or-later · Built with [Hyperagent](https://hyperagent.com/refer/J2G6TCD7)
