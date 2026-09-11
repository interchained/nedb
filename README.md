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
[![License: BUSL-1.1](https://img.shields.io/badge/license-BUSL--1.1-f59e0b)](https://github.com/Eth-Interchained/nedb/blob/master/LICENSE) [![Free under $1M revenue](https://img.shields.io/badge/free%20under%20%241M%20revenue-22c55e)](https://github.com/Eth-Interchained/nedb/blob/master/LICENSE)

**[Studio → studio.interchained.org](https://studio.interchained.org)**  ·  **[nedb.aiassist.net](https://nedb.aiassist.net)**

> ## 🟢 Free in production under $1M revenue
> NEDB 4.0.0 is licensed under the **Business Source License 1.1**. If your organisation's annual
> revenue is **under USD $1,000,000**, you may use it in production — commercially, embedded, in
> closed-source software — with **no permission needed and no royalty**. At **$1M or more**, you
> need an additional use grant from Interchained LLC: **licensing@interchained.org**.
>
> Non-production use is unrestricted for everyone, at any revenue: development, testing, CI,
> evaluation, research, teaching. On **2030-09-11** this version converts to **Apache 2.0**
> automatically and permanently. See [`LICENSE`](LICENSE).

</div>

---

## New in 3.3.0 — the query language grew up

`WHERE` was six operators wide (`= != > < >= <=`) joined by an implicit `AND`.
It now takes a full boolean expression, in **both** engines, and the clauses
around it run in SQL's order.

```sql
FROM jobs
WHERE (status IN ("open", "pending") OR fee > 100)
  AND miner IS NOT NULL
  AND NOT (region LIKE "eu-%")
GROUP BY region SUM fee
HAVING sum_fee > 10000
ORDER BY sum_fee DESC
LIMIT 20 OFFSET 40
```

| added | |
| --- | --- |
| `IN (…)` / `NOT IN (…)` | set membership |
| `BETWEEN a AND b` / `NOT BETWEEN` | inclusive both ends, as in SQL |
| `LIKE` / `NOT LIKE` / `ILIKE` | `%` any run, `_` any one char |
| `IS NULL` / `IS NOT NULL` | matches absent **and** explicitly-null |
| `AND` / `OR` / `NOT` / `(…)` | `AND` binds tighter; parens nest to any depth |
| `OFFSET n` | pagination; pairs with `LIMIT` |
| `ORDER BY a, b DESC` | multi-key, per-key direction |
| `HAVING <predicate>` | filters the aggregated rows |
| `COUNT` / `SUM f` / `AVG f` / `MIN f` / `MAX f` | whole-result aggregate, one row |

**Indexed range scans.** `=`, `IN`, `BETWEEN` and the inequalities are served
from a sorted index when one covers the field — a point lookup on 20,000 rows
goes from 137 ms to 0.01 ms, a 1%-selective `BETWEEN` from 186 ms to 1.1 ms.
See [**Indexes**](#indexes) for the measured table and the three cases that
deliberately decline the index.

**Nine silent defects fixed.** None of them crashed; they all returned a
confident wrong answer with HTTP 200. The worst:

- `GROUP BY status MAX fee` aggregated the *group* field, not the target —
  answering `1.0` where the real maxima were 40 and 30.
- `LIMIT 2 GROUP BY status COUNT` truncated the aggregate's **input**, so
  twelve rows across three statuses reported counts summing to 2.
- `ORDER BY count DESC` on grouped rows sorted the raw documents on a field
  that only exists after grouping — silently inert.
- `FROM jobs OFFSET 2` and a misspelled `ORDRE BY fee` were silently **dropped**
  and a different query answered. Unknown clauses are now a parse error.
- `SUM` ran through `f64`, losing integer precision above 2^53 — a real problem
  for satoshi amounts and block heights. Integer inputs now stay in `i64`.
- An ordering comparison against a missing field was true in the Rust engine
  (`WHERE fee < 5` returned rows with no `fee` at all) and false in the Python
  reference. Now false in both, matching SQL.
- A field named like a keyword (`count`, `min`, `value`, `status`) was
  unaddressable, because both lexers canonicalised case at field positions.

**Compatibility.** Verified by building a `nedbd` from the released v3.2.2 tag
and diffing every answer: **40 of 45 legacy queries byte-identical, zero
regressions.** The differences are the three bug fixes above, each documented
in `tests/test_backcompat.py`. `scripts/compare_engine_answers.py` reproduces
the comparison against any released binary.

**The DAG is untouched.** `tests/test_dag_preserved.py` runs the entire new
query surface against a live chain and asserts head, seq and `verify()` are
unchanged afterwards — and that `verify()` still returns *false* when the log
is tampered with. Reads are reads.

**Cross-engine parity is now gated.** Nothing previously checked that the
Python reference and the Rust core agreed, which is how they had drifted apart
in five places. Two suites now run the same battery through both and assert
identical answers.

---

## Postgres wire protocol — run your SQL, get history for free

`nedbd --pg-port 5433` opens a **PostgreSQL wire-protocol endpoint** — reads
*and* writes. `psql`, DBeaver, Metabase, Grafana, psycopg — anything that
speaks pgwire can use a tamper-evident NEDB store with ordinary SQL, with no
bespoke client.

**The reason the write path matters** is that SQL's write semantics and NEDB's
append-only model already line up:

| SQL | NEDB | and therefore |
| --- | --- | --- |
| `INSERT` | a put | — |
| `UPDATE … WHERE` | a **new version** of each match | the prior value stays readable |
| `DELETE … WHERE` | a **tombstone** | the deleted row stays in history |

So this is not a compromise of the append-only design — it *is* the design,
reached through a protocol every tool already speaks:

```sql
UPDATE orders SET total = 999 WHERE _id = 'o1';
SELECT total FROM orders WHERE _id = 'o1';                       -- 999
SELECT total FROM orders AS OF SYSTEM TIME 0 WHERE _id = 'o1';   -- 120
```

Run the SQL you would run against Postgres, and the tamper-evident audit trail
is free. No triggers, no shadow table, no application code. `verify()` still
passes afterwards, because a SQL write is an ordinary engine write and not a
side door around the hash chain.

Writes are **on by default**. `NEDBD_PG_READ_ONLY=1` gives the deployment where
this door must never mutate anything.

```console
$ nedbd --data ./data --pg-port 5433
  pgwire   postgres endpoint on 127.0.0.1:5433 — psql / DBeaver / psycopg (SELECT + INSERT/UPDATE/DELETE)

$ psql -h 127.0.0.1 -p 5433 -d shop
shop=> SELECT status, total FROM orders WHERE status IN ('paid','open') ORDER BY total DESC;
 status | total
--------+-------
 paid   |   300
 paid   |   120
 open   |    40

shop=> SELECT SUM(total) FROM orders WHERE region = 'eu';
 sum
-----
 420

shop=> SELECT * FROM orders AS OF SYSTEM TIME 1;     -- time travel, in SQL
```

**`AS OF SYSTEM TIME` is the bridge worth knowing about.** It is the spelling
Postgres and CockroachDB use, and here it reaches NEDB's permanent,
never-garbage-collected history rather than a few hours of MVCC.[^compact] A wall-clock
timestamp is refused with the reason: NEDB's history is sequence-addressed, so
a seq is exact where a time would be approximate.

[^compact]: One caveat, stated rather than buried. `Db::compact()` reclaims disk
    space by rewriting the object segments with only each document's **current**
    version — so it prunes superseded versions and tombstones, and `AS OF` can
    no longer reach them. Nothing invokes it automatically: it is not on the
    HTTP surface, not in the CLI, and not on any timer. It exists for the
    operator who has explicitly chosen to trade the audit trail for space. A
    compacted store answers "not available at that sequence" rather than
    returning a stale value, and `verify()` stays clean.

Provenance is selectable like any other column:

```sql
SELECT _id, _hash, _seq FROM audit ORDER BY _seq;
```

**This is not "NEDB speaks SQL", and the endpoint is careful to say so.** It is
a documented subset of `SELECT` translated to NQL:

| Supported | Refused, with the reason |
| --- | --- |
| `*`, a column list, `COUNT(*)`, `SUM`/`AVG`/`MIN`/`MAX(col)` | `JOIN` — NQL is single-collection |
| `WHERE` — the whole NQL predicate surface | subqueries, `UNION`, window functions |
| `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET` | expressions in the select list |
| `AS OF SYSTEM TIME <seq>` | DDL, `TRUNCATE`, `GRANT`/`REVOKE` |
| `INSERT` / `UPDATE` / `DELETE`, all with `RETURNING` | an `INSERT` with no column list |
| `_caused_by` / `_valid_from` / `_valid_to` as INSERT columns | values that are expressions, not literals |

Every refusal names the boundary instead of saying "syntax error", and a
grouped query that projects a column SQL would reject gets Postgres's own
message rather than a silent `NULL`.

**Provenance is settable from SQL**, so the causal chain does not require the
HTTP API:

```sql
INSERT INTO audit (_id, _caused_by, kind) VALUES ('leaf', '<parent-hash>', 'reprice');
SELECT _id FROM audit TRACE caused_by;
```

An `INSERT` requires an explicit column list, because NEDB is schemaless and
there is no declared column order to infer. Values must be literals — a number,
a quoted string, `TRUE`/`FALSE`/`NULL` — since storing an unevaluated
expression as text would be worse than refusing it.

### Your driver, not just `psql`

**Both wire protocols are implemented**, which is the difference between "psql
works" and "your application framework works":

| Protocol | Used by | Status |
| --- | --- | --- |
| simple (`Q`) | `psql`, libpq/`PQexec`, **psycopg2** | ✅ |
| extended (`Parse`/`Bind`/`Describe`/`Execute`) | **psycopg3**, **asyncpg**, **JDBC** | ✅ |

Those last three send `Parse`/`Bind` for every parameterised statement, so
until the extended protocol landed they could not run a *single* query — not
slower, not degraded: psycopg3 hung and asyncpg refused outright.

```python
# psycopg3 — parameters are bound server-side
cur.execute("SELECT _id, total FROM orders WHERE status = %s AND total > %s",
            ("paid", 100))

# asyncpg — same statement, same endpoint
await conn.fetch("SELECT _id, total FROM orders WHERE status = $1 AND total > $2",
                 "paid", 100)
```

Parameters arrive in text **and binary** format (psycopg3 sends a small `int`
as binary int2, a float as binary float8), and a row-capped `Execute` suspends
its portal, so a JDBC `setFetchSize` pages a large result instead of stalling.

**Typing parameters in a store with no schema** is the interesting part. A
relational server reads `$1`'s type out of its catalogue; NEDB has no
catalogue, so the type is sampled from the documents already stored — the
stored data *is* the schema. Where a placeholder sits in a clause rather than
beside a column (`AS OF SYSTEM TIME $1`, `LIMIT $1`) the grammar supplies the
type, and an aggregate is typed from what it means: a `COUNT` is an integer, an
`AVG` fractional, a `MAX` whatever the field it ranges over is.

A driver that declares its own parameter types is believed, and only its
unspecified slots are inferred.

### Limits, stated plainly

SQL-level cursors (`DECLARE`/`FETCH`) and `pg_catalog` introspection are not
implemented, so `\dt` and DBeaver's schema browser come back empty — both are
refused by name rather than hanging. A column whose stored values disagree
about their type across documents is advertised as `text`, and cannot be sent
in binary format.

And the connection is **cleartext**, which is why the endpoint is off unless
you pass `--pg-port` and binds to loopback by default. Put it behind a tunnel
to go further.

Verified in CI by `tests/test_pgwire.py` — **64 checks driven through psycopg2,
which is libpq**. Unit tests can prove the translation; only a real client
proves the protocol. One of those checks is the one that matters: after a plain
SQL `UPDATE`, the prior value is still readable at its original sequence.

---

## New in 3.2.0 — wrap the databases you already run

NEDB adds **tamper-evident causal provenance to a database you already have**, in one line, without
rip-and-replace. Five adapters, one surface:

**One flag. No table list, no registration, no per-write calls.**

```python
from nedb import wrap_postgresql
import psycopg2

conn = wrap_postgresql(psycopg2.connect("dbname=app"), db_name="app")
conn.nedb.shadow_writes = True          # ← that is the whole setup

# Your app runs UNCHANGED.
cur = conn.cursor()
cur.execute("INSERT INTO drivers (name, status) VALUES (%s, %s)", ("Bob", "active"))
cur.execute("UPDATE drivers SET status = %s WHERE name = %s", ("off", "Bob"))
conn.commit()

conn.nedb.query('FROM drivers WHERE status = "off"')   # NQL over your Postgres data
conn.nedb.query('FROM drivers AS OF 0')                # → status "active", the prior value
conn.nedb.verify()                                     # True — BLAKE2b chain intact
```

Setting the flag reads the host's own catalogue and mirrors **every table**, with its
real primary key. Coverage is **opt-out**, because opt-in auditing has a worse failure
mode than none: three tables registered out of twelve looks exactly like a complete
audit trail until the day you need it.

```python
conn.nedb.exclude = {"sessions", "audit_log_*"}        # tables to leave alone
conn.nedb.exclude_columns = {"password_hash", "ssn"}   # columns that must NEVER be mirrored
assert not conn.nedb.unmirrored_tables                 # a real check, not a hope
```

`exclude_columns` is the one setting that is about correctness rather than convenience.
**NEDB cannot forget** — that is the product, and it is exactly wrong for a secret or for
a row somebody has a right to erase. Nothing is excluded by default; guessing which of
your columns are sensitive would be its own silent wrong answer.

| wrapper | host | shadowing |
|---|---|---|
| `wrap_redis` | `redis.Redis` / compatible | **automatic** — every write command intercepted |
| `wrap_sqlite` | `sqlite3.Connection` | **automatic** — `conn.execute`, `cursor.execute`, `executemany` |
| `wrap_postgresql` | DB-API 2.0 (psycopg2, psycopg 3) | **automatic** — every cursor write, via `RETURNING` |
| `wrap_mysql` | DB-API 2.0 (mysql-connector, PyMySQL) | explicit `shadow_row()` |
| `wrap_mongo` | `pymongo.MongoClient` | explicit `shadow_row()` |

**NEDB never writes into the host database's namespace.** Shadow data lives only in the NEDB engine.

### The limit, stated plainly

Interception sees writes made **through this connection**. Your production Postgres is
also written by `psql`, cron jobs, migration tools, and other services — and none of
those pass through here. For whole-database coverage independent of the client, the right
mechanism is Postgres **logical replication** (a replication slot decoded with the
built-in `pgoutput`), which observes every committed change whatever made it. That needs
`wal_level = logical` and a replication role, and it is not implemented yet.

So this covers your application's writes completely, and it does not pretend to cover
writes it cannot see. A write to a table that could not be mirrored lands in
`nedb.unmirrored_tables` rather than vanishing.

Three backends behind the same surface, selected by `backend="auto"`: **nedbd over HTTP** (`nedbd_url=`),
**embedded v2/v3 DAG** (the Rust core, in-process, no server — `dag_path=` for a durable store,
`dag_tmk=` for AES-256-GCM at rest), or the **v1 in-process AOF** engine as a universal fallback. On the
DAG backend you also get `tip()`, `tip_collection()`, `since()` (changefeed) and `scan_status()`.

### Licensing — BUSL-1.1 as of 4.0.0

| Your annual revenue | Production use |
|---|---|
| **under USD $1,000,000** | **free.** No permission, no royalty, no registration. Commercial, embedded and closed-source all included. |
| **USD $1,000,000 or more** | needs an additional use grant — **licensing@interchained.org** |

Measured on your whole organisation, not on revenue attributable to NEDB. Non-production use —
development, testing, CI, evaluation, research, teaching — is unrestricted for **everyone**, at any
revenue. Offering NEDB itself as a hosted database service needs a separate commercial license
regardless of revenue.

**Change Date 2030-09-11**: this version becomes **Apache 2.0** on that date, automatically. The
grant is in the license text, not a promise — nobody has to be asked, and it cannot be withdrawn.
The full Apache text ships as [`COPYING-APACHE-2.0.txt`](COPYING-APACHE-2.0.txt) so that is
verifiable from the source tree alone.

Every source file carries `SPDX-License-Identifier: BUSL-1.1`, so license scanners read the terms
out of the code rather than guessing. Dependencies keep their own licenses — the Change License
never relicenses them — and they are inventoried in [`THIRD_PARTY.md`](THIRD_PARTY.md). There is
**no GPL, AGPL, SSPL or BUSL third-party dependency** in the tree; all 206 third-party crates are
permissive, and the two Python runtime dependencies are BSD and Apache.

**Versions 3.0.0 – 3.3.1 stay MIT, irrevocably.** If you already have NEDB at 3.3.1 or earlier, your
rights in that copy are untouched. This applies to 4.0.0 and later only.

### Also in 3.2.0

- **A durability defect that pinned every embedded database.** The background flush ticker held a
  strong `Arc<Db>` in an unconditional loop, so the handle was never dropped: the exclusive data-dir
  `LOCK` was never released (reopening the same path *in the same process* failed with "locked by
  another process" naming your own pid), every `open()` leaked a thread and the whole `Db`, and
  flush-on-close could never fire. The ticker now holds a `Weak<Db>` and exits when its owner does.
  **Live in 2.8.5 through 3.1.0 — upgrade if you embed the engine.**
- **`wrap_redis` crashed on any install without the native wheel** — the pure-Python fallback path
  raised `AttributeError` from inside `wrap_redis()`. Fixed.
- **Prebuilt binaries for `linux-arm64` and musl/Alpine**, on npm and PyPI. Graviton, Ampere, Linux
  containers on Apple Silicon, and Alpine images previously installed cleanly and then failed at
  import.
- **CI actually runs the test suites.** Until now the only workflows fired on a version tag, so the
  first automated opinion about a change arrived *after* it was published to three registries. All
  26 suites now run on every push and pull request. It found four real defects in its first hour,
  including two of the ones listed above.

---

## Earlier — 2.8.6 durability & recovery

Three defects found by killing a real engine at every persistence boundary and by filling a real
filesystem to zero free blocks. **If you are on 2.8.5 or earlier, upgrade.**

**1. A failed flush silently discarded acknowledged writes.** `IdIndex::flush_write_buf` cleared every
buffered entry regardless of whether its disk write succeeded, so a flush that hit `ENOSPC` threw the
entry away and no later flush retried it. Reproduced on a full 22 MiB filesystem: 30 rows acknowledged
by `put() -> Ok`, then `list()` returned 0 after reopen — while `verify()` reported all 30 objects
healthy. The content-addressed objects were durable; the id-index entries that make them findable were
gone. An entry now leaves the WAL only when its write actually landed.

**2. Flush errors were unobservable.** `flush_all()` returns `()`, so a caller could not tell a durable
flush from a failed one.

```rust
db.try_flush_all()?;      // Result<()> — use this when the outcome matters
db.flush_all();           // still logs; for ticker / Drop, nowhere to propagate
```

Also added: `Db::try_flush_manifest()` and `IdIndex::try_flush_write_buf()`.

**3. `repair` could not repair, and `since()` claimed "caught up" while behind.** The cold scan rebuilt
`seq_index`, per-collection tips, the Merkle head and `MANIFEST` — but never the id index, and
`start_cold_scan()` is a deliberate no-op on a warm store, so `nedb-cli repair` printed success on
exactly the database it exists to fix.

```bash
nedb-cli repair ./data
# repaired: 203 id-index entr(ies) rebuilt, 203 node(s) verified, flushed
```

Every object carries its own `coll`, `id` and `seq`, so the index is fully derivable — nothing is
invented. Separately, `since()` set `has_more = hit_limit` alone; on a warm boot the seq index is empty
*by design*, so `since()` returned zero nodes with `has_more = false` — indistinguishable from
genuinely up to date, and a consumer following the documented drain loop stopped one call in with every
record unread. `has_more` is now true whenever the cursor is behind head, and `ScanStatus` gains
**`seq_index_ready`** — gate replication on that, not on `scan_complete`.

**Known sharp edge (documented, not changed):** `since()`'s cursor is **exclusive** and seqs start at 0,
so `since(0, _)` returns `(0, head]` and the very first write (seq 0) is unreachable through any cursor
value. Ten writes drain as nine records. Changing the convention would break existing consumers.

---

## NEDB v3.2.0 — Production Stable

**Current stable: 3.2.0** — NEDB ships as **three version-aligned distributions** on one tag — `nedb-engine` (flagship), `crypto-database` (verifiable v2/v3 DAG), and `aof-db` (fast append-only) — across npm / PyPI / crates.io with native addons for **macOS (arm64 + x86_64), Linux (x86_64 + aarch64, glibc + musl) and Windows x86_64** (see [**Releasing**](#releasing) below). All native wheels (Linux + Windows on GitHub Actions; macOS on Codemagic M2 Mac Minis) **plus** the universal pure-Python wheel ship from a single `v*` tag, with the `nedbd-v2` binary bundled inside `pip install nedb-engine`.

### New in 2.8.0 — Cast: the database understands English

`POST /v1/databases/<name>/cast` turns a short English prompt into NQL, using a **3.33M-parameter model that runs locally on CPU**. No API key, no network call, no per-token bill.

```bash
nedbd --dag --cast ./data     # requires: cargo install nedb-engine --features cast

curl -X POST localhost:7070/v1/databases/shop/cast \
  -d '{"prompt":"orders over 100"}'
# → {"nql":"FROM orders WHERE total > 100","valid":true,"collection_known":true,"executed":false}
```

The model ([**nedb-cast-slm**](https://github.com/aiassistsecure/nedb-cast-slm)) was trained on NQL using NEDB's own parser as generator, grader, and gate — then shipped to PyPI, crates.io, and npm, all three loading the identical weights.

**Why it lives in the engine and not in a client:** the hard part of natural-language querying is knowing the schema, and the engine already holds the live collection list. A plan naming a collection that doesn't exist returns **422 with the reason**, never a silently empty result set. See [**Cast**](#cast--natural-language-into-nql) below.

Off by default — feature-gated at compile time, flag-gated at runtime, and `execute` defaults to `false` so you review the plan before it runs.

**Also in 2.8.0 — an engine bug the feature exposed.** `IdIndex::collections()` did a bare `read_dir` while every other read path overlaid the WAL write buffer, so a brand-new collection was invisible until the 1s flush ticker fired. Unreachable by hand (the ticker fires between keystrokes) but reliable from a script, and latent in `Db::compact()` too, where a missed collection's live objects would be reclaimed as garbage. Fixed, with regression tests that seed *without* flushing.

**New in 2.5.x:**

- **Durable-mode auto-flush-on-exit** — a durable store flushes buffered writes on `Ctrl+C` / `SIGTERM`, not just on a clean `Drop`. Automatic in the Node and Python bindings; `Db::install_exit_flush(Arc<Db>)` for standalone Rust binaries. See [**docs/DURABILITY.md**](docs/DURABILITY.md).
- **2.8.5 — embedded bindings flush on a cadence.** `NedbCore.open()` (Node + Python) now runs the 1 s manifest ticker exactly as `nedbd` does, so a `SIGKILL` / OOM / power cut loses at most one tick of acknowledged writes instead of everything since open. `NEDB_FLUSH_MS` tunes or disables it. See [docs/DURABILITY.md](docs/DURABILITY.md).
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
# {"ok":true,"version":"3.2.0","service":"nedbd","engine":"dag","startup_ready":true,"encrypted":true}

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

### Prebuilt platforms

Both registries ship prebuilt binaries for:

| Platform | libc | Python wheel | Node addon |
|---|---|---|---|
| Linux x86_64 | glibc | ✅ manylinux | ✅ |
| Linux x86_64 | musl (Alpine) | ✅ musllinux | ✅ |
| Linux aarch64 (Graviton, Ampere, Apple-Silicon containers) | glibc | ✅ manylinux | ✅ |
| Linux aarch64 | musl (Alpine) | ✅ musllinux | ✅ |
| macOS arm64 + x86_64 | — | ✅ | ✅ |
| Windows x86_64 | MSVC | ✅ | ✅ |

On Python, any platform without a prebuilt wheel still installs: pip falls back
to the universal `py3-none-any` wheel and you get the pure-Python v1 AOF engine
(correct, slower, no embedded DAG). On Node there is no such fallback — an
unlisted platform has no addon.

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

# Full boolean predicates — IN, BETWEEN, LIKE, IS NULL, OR, NOT, parentheses
db.query('FROM users WHERE status IN ("active", "trialing")')
db.query('FROM users WHERE age BETWEEN 25 AND 40')
db.query('FROM users WHERE bio LIKE "%rust%" AND NOT (status = "retired")')
db.query('FROM users WHERE (age < 25 OR age > 60) AND bio IS NOT NULL')

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

## Official Python client — talk to nedbd over HTTP

Running the daemon? `nedb.client.NedbClient` is the official client for its
HTTP API — extracted from the battle-tested clients that ran a production
Redis→NEDB mainnet migration, speaking the full route surface: queries,
atomic CAS transactions, TTL, indexes, relations, Merkle proofs, and the
Mongo-compat endpoint. Env-var defaults (`NEDBD_URL`, `NEDBD_TOKEN`,
`NEDB_DB`) mirror the daemon's own.

```python
from nedb import NedbClient, PreconditionFailed, op_put

c = NedbClient("http://127.0.0.1:7070", db="app", token="s3cret")
c.ensure_database()

c.put("users", "u1", {"id": "u1", "email": "a@b.c"}, idem="signup-u1")
c.query('FROM users WHERE email = "a@b.c"')      # full NQL rides through
c.query("FROM users AS OF 41")                    # time-travel included

# Atomic all-or-nothing transaction with engine-checked preconditions —
# the primitive that replaces Redis Lua scripts (if_seq: N = CAS, -1 = create-once)
doc = c.get_doc("users", "u1")                    # docs carry _seq
c.tx([op_put("users", "u1", {**doc, "plan": "pro"}, if_seq=doc["_seq"])])

# Contested writes: retry ONLY on PreconditionFailed, capped backoff
def bump():
    d = c.get_doc("counters", "hits") or {"n": 0}
    return c.tx([op_put("counters", "hits", {"n": d.get("n", 0) + 1},
                        if_seq=d.get("_seq", -1))])
c.cas_retry(bump)

# Integrity, verifiable WITHOUT trusting the server
proof = c.proof(c.log(limit=1)[0]["hash"])
from nedb import verify_proof; verify_proof(proof)  # -> True, locally
```

A CAS miss raises the **same `PreconditionFailed`** (with the same
`.failures` shape) the embedded engine raises — code written against
`NEDB.tx` ports to the HTTP client without changing its except-clauses.
Typed errors throughout: `NedbAuthError`, `NedbNotFound`, `NedbBadRequest`,
`NedbConflict`, `CasExhausted`.

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
| `NEDBD_CAST` | `0` | Set `1` to enable the `/cast` natural-language planner. Same as `--cast`. Requires a build with `--features cast`. See [**Cast**](#cast--natural-language-into-nql). |
| `NEDBD_CAST_MODEL` | unset | Explicit path to a `model.cast` container. Otherwise searched in the data dir, `$CAST_HOME`, and `~/.cache/nedb-cast-slm/`. |

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
  [ WHERE <predicate> ]                      full boolean predicate, see below
  [ SEARCH "<text>" ]                        full-text search
  [ TRAVERSE <relation> ]                    graph traversal
  [ TRACE caused_by [REVERSE] ]              causal provenance (why? / what did this cause?)
  [ GROUP BY <field> [COUNT|SUM f|AVG f|MIN f|MAX f] ]
  [ COUNT | SUM f | AVG f | MIN f | MAX f ]  whole-result aggregate, one row
  [ HAVING <predicate> ]                     filters the AGGREGATED rows
  [ ORDER BY <field> [ASC|DESC] (, ...) ]
  [ LIMIT <n> ] [ OFFSET <n> ]
```

Clauses are evaluated in SQL's order, whatever order you write them in:

```
FROM → WHERE → GROUP BY → HAVING → ORDER BY → OFFSET → LIMIT
```

That matters, and before 3.3.0 it was wrong. `LIMIT` truncated the *input* to
an aggregate rather than the result, so `LIMIT 5 GROUP BY status COUNT` over
twelve rows reported counts summing to 5 — it said only five rows existed when
twelve did. `ORDER BY` ran before grouping, so sorting on `count` or `sum_fee`
silently did nothing. `VALID AS OF` was applied after `LIMIT` in the Python
engine, so a limited bi-temporal query returned fewer valid rows than exist.

### Predicates

`WHERE` takes a full boolean expression. `AND` binds tighter than `OR`;
parentheses override, and nest to any depth.

```
<predicate> := <or>
<or>        := <and> [OR <and>]*
<and>       := <not> [AND <not>]*
<not>       := [NOT] <primary>
<primary>   := "(" <predicate> ")" | <comparison>
```

| Comparison | Example |
| --- | --- |
| `= != < <= > >=` | `WHERE height > 600000` |
| `IN (…)` / `NOT IN (…)` | `WHERE status IN ("open", "pending")` |
| `BETWEEN a AND b` | `WHERE height BETWEEN 100 AND 200` — inclusive, as in SQL |
| `NOT BETWEEN a AND b` | `WHERE fee NOT BETWEEN 10 AND 20` |
| `LIKE` / `NOT LIKE` | `WHERE miner LIKE "Acme%"` — `%` any run, `_` any one char |
| `ILIKE` | `WHERE miner ILIKE "acme%"` — case-insensitive |
| `IS NULL` / `IS NOT NULL` | `WHERE miner IS NULL` — matches absent **and** explicitly-null |

```python
db.query('''FROM jobs
            WHERE (status IN ("open", "pending") OR fee > 100)
              AND miner IS NOT NULL
              AND NOT (region LIKE "eu-%")
            ORDER BY fee DESC LIMIT 20''')
```

Filterable metadata fields: `_id`, `_coll`, `_hash`, `_seq`.

`_id = "x"` is an O(1) index lookup rather than a scan — but only when it is a
genuine conjunct. Under an `OR` it cannot constrain the result set, so the
planner correctly declines the fast path there.

### Indexes

`=`, `IN (...)`, `BETWEEN` and the one-sided inequalities are served from a
sorted index when one covers the field, turning a full collection scan into a
bounded range walk:

```python
db.create_index("blocks", "height", "sorted")
db.query("FROM blocks WHERE height BETWEEN 600000 AND 600100")
```

Measured on 20,000 rows with `scripts/bench_index_range.py` — two identical
databases, one indexed, one not:

| Query | Scan | Indexed | Speedup |
| --- | --- | --- | --- |
| `WHERE fee = 10000` | 137 ms | 0.01 ms | 17,000× |
| `WHERE fee IN (a, b, c)` | 185 ms | 0.02 ms | 9,700× |
| `WHERE fee BETWEEN …` (1% of rows) | 186 ms | 1.1 ms | 170× |
| `WHERE fee BETWEEN …` (10% of rows) | 188 ms | 13 ms | 14× |
| unindexed field (control) | 188 ms | 187 ms | 1.0× |

The planner asks the index how many rows each candidate range covers and takes
the narrowest, so `WHERE region = "eu" AND height BETWEEN 600000 AND 600001`
uses the height index rather than whichever field it saw first. Same-field
bounds are merged, so `height > 100 AND height < 200` is one walk.

An index is only ever used to NARROW candidates — the full predicate is
re-evaluated on whatever comes back, so the answer never depends on whether an
index exists. Three cases deliberately decline it:

- **`IS NULL`.** A document whose field is absent is not in that field's
  index, so an index scan would return the exact complement of the answer.
- **Anything under `OR` or `NOT`.** A disjunct does not constrain the result
  set; narrowing on one arm would silently drop the rows the other arm matched.
- **`AS OF`.** The sorted index holds current versions only (a superseded hash
  is dropped on overwrite), so it cannot answer a historical query.

> **Predicates over NULL follow SQL's three-valued logic.** An ordering
> comparison (`<` `<=` `>` `>=`, and therefore `BETWEEN`) against a missing or
> null field is never true — `WHERE fee < 5` will not return a row that has no
> `fee` at all. `LIKE` is false in *both* polarities, so a null row appears in
> neither `LIKE` nor `NOT LIKE`. `=` and `!=` do operate on null, so
> `WHERE fee = NULL` selects rows where the field is absent or null, and
> `WHERE fee != 5` includes them. Use `IS NULL` / `IS NOT NULL` to test
> presence explicitly.

### Unknown clauses are errors

A query containing a clause the engine does not implement is **rejected**, not
silently reinterpreted. Before 3.3.0 the parser skipped tokens it did not
recognise, so `FROM jobs OFFSET 2` returned un-offset rows with HTTP 200 and a
misspelled `ORDRE BY fee` returned unsorted rows — the engine answered a
*different query* than the one asked, and said nothing. Both now return
HTTP 400 with the offending token.

### Aggregates

`SUM`/`AVG`/`MIN`/`MAX` take the field to aggregate; `COUNT` (the default when
no aggregate is given) takes none.

```python
db.query('FROM items GROUP BY cat MAX price')
# → [{"cat": "x", "count": 3, "max_price": 10.0, "value": 10.0}, …]
```

`count` is the group size. The aggregate only considers rows whose target field
is numeric, so a group of 5 where 2 carry a numeric `price` reports `count: 5`
and averages over 2. An aggregate with no numeric input is `null`, never `0`.
The `value` key is a back-compatible alias for the aggregate result.

Integer inputs give integer results — `SUM`/`MIN`/`MAX` stay in 64-bit
integers rather than passing through a float, so a sum over satoshi amounts or
block heights above 2^53 is exact. `AVG` is always fractional.

Groups come back sorted by key unless you say otherwise, so results are stable
run to run and identical across engines.

Drop the `GROUP BY` for a whole-result aggregate, which returns exactly one row:

```python
db.query('FROM orders COUNT')                    # → [{"count": 1049, "value": 1049}]
db.query('FROM orders WHERE status = "paid" COUNT')
db.query('FROM orders SUM total')                # → [{"count": 1049, "sum_total": 88123, …}]
```

`COUNT` of an empty result is one row holding `0` — a caller asking "how many?"
always gets a number. `SUM` of an empty result is `null`.

### HAVING

`WHERE` filters rows before they are grouped; `HAVING` filters the groups.

```python
db.query('FROM orders GROUP BY region SUM total HAVING sum_total > 10000')
db.query('FROM orders GROUP BY region COUNT HAVING count BETWEEN 5 AND 50')
```

`HAVING` runs through the same evaluator as `WHERE`, so it gets the whole
predicate surface — `IN`, `BETWEEN`, `LIKE`, `OR`, `NOT`, parentheses — rather
than a poorer second copy.

### Sorting and paging

```python
db.query('FROM orders ORDER BY region, total DESC')   # ties broken by the next key
db.query('FROM orders ORDER BY total DESC LIMIT 20 OFFSET 40')
```

`OFFSET` skips rows of the result and pairs with `LIMIT` for pagination. An
offset past the end is an empty page, not an error.

A field whose name collides with a reserved word is still addressable — a
document may legitimately have a `count`, `min`, `value` or `status` field, and
`WHERE count > 3` reads that field rather than the aggregate:

```python
db.query('FROM metrics WHERE count > 3 ORDER BY count DESC')
```

Combine both time axes:
```python
# What did the system know at seq 200 about what was true on 2024-02-15?
db.query('FROM policy AS OF 200 VALID AS OF "2024-02-15"')
```

---

## Cast — natural language into NQL

*New in 2.8.0. Optional, feature-gated, off by default.*

Ten clauses and six operators. That's the whole grammar above — small enough that a **3.33M-parameter** model can learn it completely, and small enough that shipping every query to a frontier model is an absurd amount of machinery.

So we trained one. It runs on CPU, in-process, in milliseconds.

```bash
curl -X POST localhost:7070/v1/databases/shop/cast \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"orders over 100"}'
```

```json
{
  "prompt": "orders over 100",
  "nql": "FROM orders WHERE total > 100",
  "valid": true,
  "collection": "orders",
  "collection_known": true,
  "collections": ["orders"],
  "executed": false,
  "seq": 3,
  "head": "262fd9…"
}
```

### NEDB is on both ends of this

The model is [**nedb-cast-slm**](https://github.com/aiassistsecure/nedb-cast-slm), and NEDB built it as much as it consumes it.

**NEDB's parser was the training pipeline.** It generated the corpus (sample a random plan → render NQL → render a human paraphrase; 200,000 pairs in 16.5 seconds, perfect labels, zero annotation cost). It was the grader — scoring *parsed plan* equality, not string equality, so `FROM orders WHERE total > 99` and `from orders where total>99` both earn full credit. And it was the gate: no example entered the corpus unless it round-tripped through the real parser to a canonically identical plan.

> Most text-to-DSL projects hand-write a verifier and hope it's right. We didn't write one — it already shipped, and it's the same code the database runs in production.

**Training lineage lives in NEDB too**, chained by `caused_by`:

```
datasets ──▶ training_runs ──▶ checkpoints ──▶ evals
```

```python
db.query("FROM evals TRACE caused_by")   # the exact data behind any score
```

### Why the planner lives in the engine

The hard part of natural-language querying is not the model. It's the **schema** — and a client has to fetch the collection list and pass it in, where it's stale on arrival. The engine already holds the live list.

So the plan is checked against collections that actually exist, at the moment of the call:

```json
{ "prompt": "show me all stylists",
  "nql": "FROM stylists",
  "valid": true,
  "collection_known": false,
  "error": "collection \"stylists\" does not exist in \"shop\"" }
```

HTTP **422**. Not zero rows — zero rows reads as *"no matching data"*, which would be a lie. The query was perfectly well-formed; the collection was imagined. That's the model's known failure mode on an unfamiliar schema, and the engine is the one component positioned to catch it.

Every `nedbd` client — Python, Node, Studio, `curl` — inherits this without writing a line.

### Three safety properties

| | |
|---|---|
| **The model never executes** | It emits text. The text goes to the same `nql::query` path a hand-typed query uses. No second executor exists to audit. |
| **Validation is parsing** | `nql::parse` and `nql::execute` share one code path, so they cannot disagree about what is well-formed. Invalid output returns 422 *with the offending text*. |
| **`execute` defaults to false** | You get a plan for review. Running a guess silently is worse than admitting uncertainty. |

That last default earns its keep. A real miss, from a real run:

```
prompt   "paid orders over 100"
nql      FROM orders WHERE status = "paid" LIMIT 100      ← wrong
correct  FROM orders WHERE status = "paid" AND total > 100
```

It read *"over 100"* as `LIMIT 100` and dropped the predicate. The count still came back **2** — because both paid orders happened to exceed 100. A count-only assertion would have scored it a pass. A human reading `LIMIT 100` catches it in a heartbeat; an auto-executing client does not.

Multi-predicate `WHERE` is the model's weakest clause: **85.1%** exact-plan match on eval, 61.2% on adversarial holdout. The [model card](https://github.com/aiassistsecure/nedb-cast-slm#what-it-gets-wrong) documents every failure mode with examples.

To run it anyway, ask:

```bash
curl -X POST localhost:7070/v1/databases/shop/cast \
  -d '{"prompt":"orders over 100","execute":true}'
# → { …, "executed": true, "count": 2, "rows": [ … ] }
```

### The failure `valid` cannot catch

A literal the model **invented** rather than copied:

```
"memories about pricing"  ->  FROM memories SEARCH "handoff"
```

That query parses. It names a real collection. It returns real rows. Both
`valid` and `collection_known` are `true` — and it answers a question nobody
asked. Measured on the released checkpoint:

| terms | in vocabulary | copied correctly |
|---|---|---|
| `release flow` · `guardrail` · `handoff` | yes | **3/3** |
| `pricing` · `deadlines` · `kubernetes` | no | **0/3** — all became `"handoff"` |

So the response carries a `drift` field when a quoted literal is absent from the
prompt:

```json
{ "nql": "FROM memories SEARCH \"handoff\"",
  "valid": true,
  "collection_known": true,
  "drift": "generated the literal \"handoff\", which does not appear in the prompt — likely outside the model's vocabulary and substituted. Verify before trusting these results." }
```

It is **advisory, never fatal** — the plan may still be what you wanted, and
discarding a valid query would be its own kind of lie. But an unattended caller
should treat it as a third gate:

```python
if plan["valid"] and plan["collection_known"] and not plan.get("drift"):
    rows = await db.query(plan["nql"])
```

Same root cause as truncated digits (`height 400000` → `4000`): no copy
mechanism over prompt tokens. Verified at 24/24 on real model output — 3 true
positives, 21 true negatives, zero false alarms, including the case that matters
most (correctly inferred enum values like *"refunded orders"* → `status =
"refunded"` stay silent).

### Enabling it

Two gates, because most deployments want neither the model dependency nor the weights:

```bash
# compile-time
cargo install nedb-engine --features cast

# or from a source checkout — builds the engine only, not the language bindings
cd rust && cargo build --release --features cast

# weights (~13 MB) — GitHub release asset, checksum-verified on load
curl -L -o ./data/model.cast \
  https://github.com/aiassistsecure/nedb-cast-slm/releases/download/v10.30.90/model.cast

# runtime
nedbd --dag --cast ./data
#   cast     enabled — 3.33M params, vocab 581, ./data/model.cast
```

Search order: `$NEDBD_CAST_MODEL` → `<data_dir>/model.cast` → `$CAST_HOME/model.cast` → `~/.cache/nedb-cast-slm/v10.30.90/model.cast` (the Python/npm cache location, so a machine that has run either package is already ready).

Built without the feature, the route returns **501** rather than 404 — clients can detect the capability instead of guessing. Built with it but missing weights, the daemon logs loudly and serves everything else normally.

**Verify the whole path:**

```bash
./scripts/test-cast.sh --boot     # boots a daemon, seeds, casts, executes, checks failure modes
```

### Casting from a shell

```bash
./scripts/seed-shop.sh       # a shop database the model already understands
. ./scripts/nedb.sh          # bash / zsh / Git Bash

nedb-dbs                     # which databases exist
nedb-use shop                # pick one
cast "orders over 100"       # plan only — nothing runs
cast -x "orders over 100"    # plan AND execute
```

**Seed the names it was trained on.** The model learned six synthetic domains, and `shop` is one of them — `orders(total, status, quantity, customer, placed_at, discounted)`, `products(price, stock, category, rating, title)`, `customers(age, city, tier, lifetime_value, name)`, plus the relations `purchased` / `reviewed` / `belongs_to`. Those names live in its 581-token vocabulary.

Call your collection `purchases` with a `cost` field and it will still emit `FROM orders WHERE total > …`, because that is what it knows. It is a 3.3M-parameter model, not a schema reader. On an unfamiliar schema you get `collection_known: false` — caught, not silently wrong, but caught.

```
  nql         FROM orders WHERE total > 100
  valid       yes    collection orders known: yes
  executed    no  (add -x to run it)
```

The summary leads with the NQL because **reading it is the job**. `valid: yes` means it parses, not that it's what you meant — `LIMIT 100` parses perfectly.

`NEDB=http://host:7070` points at a remote daemon. Prompts are JSON-escaped, so apostrophes and quotes are safe.

### Prompts it handles well

Accuracy varies by clause, so phrasing matters more than length:

| you want | say | eval |
|---|---|---|
| `TRACE caused_by` | *what caused these checkpoints* | 96.5% |
| `TRAVERSE` | *orders traverse placed_by* | 93.3% |
| one `WHERE` | *orders over 100* · *active drivers* | 91.2% |
| `LIMIT` | *top 5 orders* | 91.1% |
| `SEARCH` | *search orders for refund* | 90.5% |
| `ORDER BY` | *orders sorted by total descending* | 87.7% |
| two+ `WHERE` | *paid orders with total over 100* | 85.1% |
| `GROUP BY` + agg | *orders grouped by status with sum of total* | 77.0% |

Two habits that avoid most misses:

- **Name the field** when a number could be a limit. *"orders with total over 100"* beats *"orders over 100"* — bare *"over N"* is what produced the `LIMIT 100` miss above.
- **Check numbers over four digits.** Digits are tokenized one at a time, so `height 400000` can come back `4000`.

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

**MIT License** — free for any use, including commercial and production. See [`LICENSE`](LICENSE).
© 2026 INTERCHAINED LLC — [interchained.org](https://interchained.org)

---

## Authors

Built by **[Mark Allen Evans Jr.](https://interchained.org)** (INTERCHAINED, LLC)
with the **Interchained AI fleet** on [Hyperagent](https://hyperagent.com/refer/J2G6TCD7) —
Vex (GLM · Claude Sonnet · Opus · Fable · GPT-6 Astra/Sol), across hundreds of sessions.

> *"Take one idea, turn it into an LP, then an app, then a system, then a platform, then infrastructure that is irreplaceable."*

[![Built with Hyperagent](https://img.shields.io/badge/Built%20with-Hyperagent-6366f1?style=flat-square)](https://hyperagent.com/refer/J2G6TCD7)
[![AiAssist](https://img.shields.io/badge/Powered%20by-AiAssist-00d4ff?style=flat-square)](https://aiassist.net)
