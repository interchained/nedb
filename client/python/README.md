# nedb-client (Python)

Async Python client for the [nedbd](https://github.com/Eth-Interchained/nedb) HTTP API.

```bash
pip install nedb-client
```

## Quick start

```python
from nedb_client import NedbClient

async with NedbClient("http://127.0.0.1:7070", db="mydb") as db:
    # Write
    await db.put("blocks", "618000", {"height": 618000, "hash": "000abc"})

    # Query (full NQL)
    rows = await db.query("FROM blocks ORDER BY height DESC LIMIT 10")

    # Causal provenance + bi-temporal
    result = await db.put("claims", "c1", {"fact": "..."}, 
                          caused_by=["abc123..."],
                          valid_from="2024-01-01",
                          evidence="sensor-42")

    # Merkle head (tamper-evident root)
    head = await db.head()

    # Full tamper-evidence check
    report = await db.verify()
    assert report["ok"]
```

## API

| Method | Description |
|--------|-------------|
| `put(coll, id, doc, **meta)` | Write a document |
| `get(coll, id)` | Fetch current version |
| `delete(coll, id)` | Tombstone delete |
| `query(nql)` | NQL query → list of dicts |
| `query_full(nql)` | NQL query → full response with seq + head |
| `batch(ops)` | Batch put/del in one round-trip |
| `create_index(coll, field)` | Create sorted index |
| `verify()` | BLAKE2b tamper-evidence check |
| `head()` | Current Merkle head |
| `seq()` | Current sequence number |
| `checkpoint()` | Explicit checkpoint |
| `log(limit)` | Recent write log |
| `health()` | Server health |
| `ping()` | Boolean reachability check |
| `list_databases()` | All databases on server |
| `create_database()` | Create this database |
| `drop_database()` | Drop this database |
