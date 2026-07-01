# NEDB Replication Contract — `tip()` · `since()` · `subscribe`

NEDB is not just a storage engine — it's a **replication substrate**. The
append-only, hash-chained log gives every downstream consumer (the ITC sync
client, the L2 sequencer, bridge oracles, payout workers, forumBridge, indexers,
dashboards, agents) a single, deterministic sync shape:

> **Where was I?** → `tip()` / a persisted cursor
> **What happened after that?** → `since(cursor, limit)`
> **Now keep me live.** → `subscribe`

Consumers do **not** replay genesis→tip. They get deterministic catch-up with
full provenance, then a clean handoff to live updates.

---

## The three primitives

| Primitive | Question it answers | Shape |
|---|---|---|
| `tip()` | What's the head right now? | `Node?` (global latest write) |
| `since(after_seq, limit)` | What committed after my cursor? | `SinceBatch` (bounded page) |
| `subscribe` (POST `…/subscribe`) | Push me new writes as they land | live stream |

### `SinceBatch` — the cursor envelope

```
SinceBatch {
    nodes:    [Node],   // writes in (from_seq, to_seq], ascending by seq
    from_seq: u64,      // the exclusive cursor this page started from
    to_seq:   u64,      // seq of the last node — your NEXT cursor
    head_seq: u64,      // current log head (how far the log extends)
    has_more: bool,     // true when more remains past to_seq (page hit `limit`)
}
```

`since()` is **bounded in the engine itself** — `limit == 0` falls back to
`DEFAULT_SINCE_LIMIT` (10 000). A stale/offline consumer can never force the
engine to materialize an unbounded batch; the safety lives in the core API, not
only the HTTP layer.

Each `Node` carries its full record — `seq`, `hash`, `prev`, `data`,
`caused_by` (causal provenance), `valid_from`/`valid_to` (bi-temporal). Catch-up
is therefore **verifiable**, not a value-only delta.

---

## The correctness gate — `scan_status().scan_complete`

The seq index that `since()` resolves against covers the **current session +
the cold-scan pass**. On a durable database, the historical seq index is rebuilt
in the background after open.

**This is a correctness boundary, not an implementation detail.** Before the
cold-scan finishes, a request like `since(1000)` can return an **empty or
partial** page — which a naïve consumer reads as *"I'm caught up"* when the
truth is *"the historical index isn't ready yet."* That is a silent,
data-losing sync bug.

**Rule: any correctness-critical consumer MUST wait for `scan_complete == true`
before trusting historical catch-up.**

`scan_status()` (HTTP: `GET /v1/databases/:name/status`):

```json
{
  "ok": true,
  "scan_complete": true,
  "tip_seq": 123456,
  "indexed_seq_min": 1,
  "indexed_seq_max": 123456,
  "indexed_count": 123456
}
```

---

## The blessed loop — catch-up, then live

Every serious consumer should use exactly this pattern. Drain history with
`since()`, persist your cursor as you go, then attach to `subscribe`:

```text
cursor = load_persisted_cursor()
if cursor is empty:
    cursor = tip().seq          # or 0 to replay the whole log

wait_until(scan_status().scan_complete)   # HARD gate — do not skip

loop:
    batch = since(cursor, LIMIT)
    if batch.nodes is empty:
        break                   # caught up
    apply(batch.nodes)          # in ascending seq order
    cursor = batch.to_seq
    persist(cursor)             # durable: survive a crash mid-catch-up
    if not batch.has_more:
        break

subscribe(from = cursor)        # live continuation
```

Properties this gives you: **durable cursor, deterministic replay, full
provenance, bounded batches, safe catch-up, live continuation.**

---

## API surface

| | Rust core (`nedb-v2`) | napi / PyO3 | HTTP |
|---|---|---|---|
| head | `Db::tip() -> Node?` | `tip()` | `GET …/tip` |
| per-collection head | `Db::tip_collection(coll) -> Node?` | `tip_collection(coll)` | `GET …/collections/:coll/tip` |
| changefeed | `Db::since(after_seq, limit) -> SinceBatch` | `since(after_seq, limit)` | `GET …/since?after_seq=&limit=` |
| readiness | `Db::scan_status() -> ScanStatus` | `scan_status()` | `GET …/status` |
| live | — | — | `POST …/subscribe` |

Bindings return JSON strings; the HTTP routes return JSON. `Node` JSON carries
`_id`/`_hash`/`_seq`/`_coll` plus the document fields.

— © INTERCHAINED LLC × Claude
