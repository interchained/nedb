# HANDOFF — NEDB

**For the next agent picking this up, including future me.** Written 2026-09-11,
at v4.0.0. Read this before planning anything.

This is not a marketing document. It is what is actually true about the code
today, what is deliberately not built, and what I would do next and why.

---

## 1. Where we are now

### The product, in one sentence

NEDB is an append-only, content-addressed, tamper-evident database with
permanent history — bi-temporal (`AS OF <seq>` for system time, `VALID AS OF
<date>` for valid time), causally provable (`caused_by` sealed inside each
node's hash, walkable with `TRACE`), embeddable, and reachable over HTTP, RESP2
(Redis wire) and the **PostgreSQL wire protocol**.

The differentiator is not speed. It is that **you cannot quietly change the
past**, and that the proof of that is cryptographic and locally verifiable.

### What shipped today (2026-09-11), in order

| PR | What | Why it mattered |
|---|---|---|
| #105 | pgwire read endpoint | `psql`/DBeaver/libpq can read a NEDB store |
| #106 | **SQL writes** — INSERT/UPDATE/DELETE + RETURNING | an `UPDATE` is a new version, so history comes free |
| #107 | **Extended query protocol** | psycopg3, asyncpg and JDBC could not run a *single* query before this |
| #108 | **A DELETE is a tombstone, not an erasure** | `AS OF` returned nothing for a deleted id, at *every* sequence |
| #109 | **`shadow_writes = True` is the whole setup** | it used to be a silent no-op |

### The two engines, and which one to trust

There are **two implementations of the same semantics**, and this is the single
most important operational fact in the codebase:

* **`python/nedb/` — the Python reference engine.** Slower. Simpler. Has been
  *right* more often.
* **`rust/nedb-v2/` — the Rust core.** Ships to crates.io, npm (napi-rs) and
  PyPI (maturin). This is what production runs.

**Two of today's three silent wrong answers were the Rust core disagreeing with
a correct Python reference.** `MAX(_seq)` returned NULL in Rust and the right
answer in Python. `AS OF` after a `DELETE` returned nothing in Rust and the
right answer in Python.

> **Treat "the Rust core has drifted from the reference" as a standing
> hypothesis, not a closed issue.** When something looks wrong, run the same
> query through both engines before doing anything else. That is a five-second
> check that has twice gone straight to the root cause.

The gate for this is `tests/test_nql_shaping.py` and
`tests/test_nql_predicates.py`, which run the *same corpus* through both and
assert identical answers. Extend that corpus whenever you touch query
semantics. It is the cheapest bug-catcher in the repo.

### The failure mode this codebase actually has

Not crashes. Not corruption. **Confident wrong answers that pass their own
audit.**

Every significant bug found today returned HTTP 200, or `verify() == True`, or
a plausible-looking value:

* `MAX(_seq)` → `NULL`, while `SELECT _seq` listed the values fine.
* `AS OF` after a delete → empty, while the version chain sat intact on disk
  and `verify()` counted every object as healthy.
* `shadow_writes = True` → mirrored nothing, raised nothing, counted nothing.
* A column type taken from the *first* non-null value, so a field holding `3`
  and `"n/a"` was advertised as `int8`.
* Table matching by **substring**, so a write to `drivers_archive` was recorded
  against `drivers` — false provenance, a record that looks authoritative and
  describes something that never happened.

**The lesson, and please internalise it:** unit tests were green through all of
these. Every one was found by driving the *real* thing — a real libpq client, a
real psycopg3 connection, a real asyncpg connection, a real PostgreSQL 16.2
server, a real prior binary. Unit tests prove the shape; only real clients prove
the behaviour.

Corollary: **a self-skipping test is indistinguishable from a passing one.**
`tests/test_wrap_autoshadow.py` therefore fails rather than skips when
`NEDB_REQUIRE_PG=1`. Do the same for any suite whose dependency might silently
go missing in CI.

### Test surface (all green at v4.0.0)

| Tier | Count | Notes |
|---|---|---|
| `cargo test --lib` | 234 | the Rust core |
| `cargo test --tests` | 15 | integration, incl. v3 segments + compaction |
| Python suites | 20 files | dependency-free tier |
| cross-engine parity | 157 + 182 | Python **and** Rust, same corpus |
| backwards compatibility | 109 | frozen v3.2.2 answers, 0 regressions |
| pgwire simple protocol | 64 | real psycopg2 / libpq |
| pgwire extended protocol | 44 | real psycopg3 + asyncpg + raw sockets |
| automatic shadowing | 55 | **real PostgreSQL 16.2** via `pgserver` |
| Node | 5 suites | addon gate, smoke, durability, wrap_family, inspector |

CI is eight jobs in `.github/workflows/test.yml`. All eight must be green
before merging — `skills/nedb-release/merge_pr.py` enforces that in code
because honouring it by hand works right up until the one time it doesn't.

---

## 2. Where we are going

### The strategic frame (Mark's, and it is the right one)

> *"Our database runs alongside your existing Postgres. Down the road, as we
> mature, they will choose NEDB over Postgres — if not for regulations then for
> speed and simplicity."*

So the near-term product is **not** "replace your database". It is **"keep your
Postgres, gain an audit trail you cannot forge"**. Two doors implement that:

1. **Inbound** — the pgwire endpoint. Point any Postgres tool at NEDB and run
   ordinary SQL. Writes go into the hash chain automatically.
2. **Outbound** — `wrap_postgresql`. Keep writing to your Postgres; every write
   is mirrored into NEDB. One flag, whole database.

Both now work. The gap is **coverage and comfort**, not capability.

### The honest competitive position vs CockroachDB

Scored against "parity or better", last assessed at 3.3.0 — **re-score this, do
not trust the numbers below blindly.**

**We lead on:** tamper evidence, causal provenance, permanent history,
embeddability, footprint, simplicity of operation, bi-temporality.

**We are at parity on:** SQL wire protocol (both protocols now), basic SQL
surface for single-table work.

**We are behind on:** CDC/changefeeds at scale, write throughput at scale,
multi-region, JOINs, transactions across statements, and maturity. Those are
real and should not be dressed up.

CockroachDB facts worth keeping straight: CSL (not OSI), free tier gated at
$10M revenue *plus telemetry*, throttles to 5 concurrent SQL transactions,
`gc.ttlseconds` defaults to **14400 — four hours** of history, changefeeds are
at-least-once with **no cryptographic integrity**, no tamper evidence, no
causal provenance, 3-node minimum, clock-sync sensitive, not embeddable.

**Four hours of history versus permanent, provable history is the whole pitch.**
Lead with it.

---

## 3. How to get there — the queue, in order

Work top-down. Each item says *why* so you can re-order with judgement rather
than just obeying a list.

### 3.1 `pg_catalog` + `information_schema` — **next**

`\dt` and DBeaver's schema browser come back **empty**. That is an evaluator's
first ten minutes and it currently looks broken.

I captured the real queries with `psql -E` against a real server — do not guess
them, capture them again if the version moves:

* `\dn` needs only `pg_namespace` + `pg_get_userbyid()` + the `!~` operator +
  `ORDER BY 1`. **No JOIN.** This is the first milestone and it proves the
  approach.
* `\dt` additionally needs `LEFT JOIN`, a `CASE` expression, and
  `pg_table_is_visible()`.
* `\d <table>` sends **nine** queries across fifteen catalog relations with
  correlated subqueries, `::` casts and `generate_series`. **Chasing full `\d`
  fidelity is a trap — do not.**

Implement the catalog as **real queryable virtual tables**, not as pattern
matches against known query strings. Pattern matching breaks silently when psql
changes its query, and a silently empty table list looks exactly like "you have
no tables" — the same disease as everything in §1.

### 3.2 JOIN (nested loop first)

Needed by `\dt`, by every BI tool, and it is the biggest remaining SQL gap. A
nested-loop join over materialised rows is honest for small results and unlocks
real tooling; `O(n*m)` is acceptable as a v1 **if documented**. The pieces
already exist: `eval_pred_with` is generic over a field resolver, and
`columns_for`/`sort_by_keys`/`paginate` already operate on JSON rows.

### 3.3 `DECLARE` / `FETCH` cursors

psycopg3's *named* cursor uses SQL-level cursors, **not** the row-capped
`Execute` (which is implemented and does suspend the portal correctly — JDBC's
`setFetchSize` works). Currently refused by name, which is the right behaviour
until it is built.

### 3.4 Postgres logical replication — the real "entire database" answer

`wrap_postgresql` interception only sees writes through **our connection**. A
production Postgres is also written by `psql`, cron jobs, migration tools and
other services. This is a **structural** limit, not a bug, and it is documented
as such in the README and the module docstring. Do not quietly imply otherwise.

The fix is a replication slot decoded with **`pgoutput`** (core since PG 10, no
extension needed), which observes every committed change whatever made it.
Requires `wal_level = logical` and a replication role — a genuine deployment
requirement that must be stated, not buried. `psycopg2.extras.LogicalReplicationConnection`
and psycopg3 both support it.

This is the single highest-value item for the "audit your existing Postgres"
pitch, and it is bigger than it looks. Scope it deliberately.

### 3.5 Redraw the evaluator comparison page

Last published at `crdb_vs_nedb_2.html` (artifact `cmtx23ldf0h0m07ad30ab7afg`).
**Stale** — it predates everything above. Mark wants it for developers
evaluating databases. Ground every claim in code that exists today, and include
the caveats in §4; an evaluator who finds an unstated limitation stops trusting
the whole page.

### 3.6 Remaining wrappers

`wrap_mysql` and `wrap_mongo` still require explicit `shadow_row()` calls — the
exact anti-pattern removed from Postgres and SQLite in #109. MySQL can take the
same `ShadowCursor` treatment (its cursor path is identical). Mongo needs
**change streams**, which are its equivalent of a replication slot.

---

## 4. Sharp edges — state these, never hide them

An evaluator who finds an unstated limitation stops trusting everything else.

* **`Db::compact()` discards history.** It rewrites the object segments keeping
  only each document's *current* version, so it prunes superseded versions and
  tombstones, and `AS OF` can no longer reach them. It is **intentional** and
  already asserted by `v3_integration`; it is opt-in and nothing invokes it
  automatically — not the HTTP surface, not the CLI, not any timer. But it means
  "history is never garbage-collected" is true *unless an operator runs this*.
  Documented on the function and footnoted in the README. **Consider a
  `keep_history` mode** so dead segment space can be reclaimed without torching
  the audit trail — right now it is all or nothing.

* **A table created *after* `shadow_writes = True`** is reported in
  `nedb.unmirrored_tables` but not picked up. A periodic or on-miss re-scan
  would close it.

* **Heterogeneous columns cannot be sent in binary format.** A field holding a
  number in one document and a string in another is advertised as `text`.
  `cell_binary` refuses a mismatch loudly rather than coercing, because sending
  a zero under a binary header corrupts the value undetectably.

* **`exclude_columns` exists for a reason that is not convenience.** NEDB
  cannot forget. Mirroring a `password_hash`, an API key or a national ID into
  an append-only tamper-evident store creates a liability, not an audit trail —
  and it collides with a GDPR deletion request. Nothing is excluded by default,
  because guessing which of a caller's columns are sensitive would be its own
  silent wrong answer.

* **pgwire is cleartext.** SSL is declined (`N`). Loopback by default. Put it
  behind a tunnel.

* **Old databases predating #108** have deletes with no graveyard entry, so
  those specific deleted rows stay unreachable in history. New deletes work.

---

## 5. Working rules that are load-bearing

Learned the hard way. Breaking these has cost real time.

**Process**

* Branch + PR for everything. **Never** commit to `master`. Never force-push,
  never rewrite public history.
* **All eight CI jobs green before merge**, verified from the API, not assumed.
  `merge_pr.py` refuses otherwise — including refusing to merge when there are
  *no* checks at all.
* **Never re-tag a release.** Bump the version instead.
* One PR at a time off `master`; do not stack a new PR on an unmerged one.
* Exception to self-merge autonomy: on `aiassistsecure/aias` (branch
  `march_2026`) open PRs but **never** merge — that is Mark's alone.

**Debugging**

* `pkill -x <exact-name>`, never `pkill -f <pattern>` — the pattern matches
  your own command line and kills your shell. Cost three shells.
* Never pipe a build through `| tail` — it masks the exit code and you end up
  testing a stale binary. Cost a long mis-diagnosis.
* After any scripted string-replace, **grep to verify it landed.** A silent
  non-match cost an hour.
* Separate the **trigger** from the **root cause**, always. The delete *looked*
  like the cause of the `AS OF` bug; the root cause was id enumeration from a
  present-time index.
* Build the release binary before running suites that prefer
  `target/release/` — the tests pick that up over `debug/` and will happily
  test yesterday's code.

**Tooling in this repo**

`skills/nedb-release/` — run everything through `RunWithCredentials`, never
print a token:

```bash
push_branch.py --branch <b> --pr-title T --pr-body-file F   # push + open PR
ci_status.py   --pr N                                       # status AND conclusion
ci_log.py      --pr N                                       # failing job logs
merge_pr.py    --pr N --delete-branch                       # merges only if green
release.py     --target github|pypi|npm
```

`FetchSkillScripts` restores only the scripts *recorded on the skill* and
removes the rest — an unregistered helper is deleted the next time credentials
refresh. That cost three rewrites of `push_branch.py` before the helpers were
registered. **Register a helper as soon as it works.**

**Local environment**

* `cargo` is not on `PATH`: `export PATH="$HOME/.cargo/bin:$PATH"`.
* A real PostgreSQL for testing: `pip install pgserver` →
  `pgserver.get_server("/tmp/pgs").get_uri()`. Gives PostgreSQL 16.2. The
  `postgresql-wheel` package is a 14 KB stub with no binaries — do not bother.
* The native Python wheel: stage `python/nedb` into
  `rust/crates/nedb-py/python/nedb` first, then
  `maturin build --release --manifest-path rust/crates/nedb-py/Cargo.toml`.
  Without the wheel, `test_native` reports 36/40 — those four failures are the
  *missing wheel*, not regressions. With it, 58/58.

---

## 6. Licensing as of 4.0.0

**BUSL-1.1**, Change License **Apache-2.0**, Change Date **2030-09-11**.

Free in production under **USD $1M** annual revenue, measured on the whole
organisation. At $1M or more, an additional use grant is required from
Interchained LLC (`licensing@interchained.org`). Offering NEDB itself as a
hosted database service needs a separate commercial license at any revenue.
Non-production use is unrestricted for everyone.

**Versions 3.0.0 – 3.3.1 remain MIT, irrevocably.** MIT is not retractable for
copies already distributed, and the LICENSE says so explicitly. 4.0.0 and later
only.

Every source file carries a three-line header — **120 of 120** tracked
first-party sources:

```
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)
```

**120, not 141.** A raw `find` reports 141 because it double-counts the
gitignored maturin staging copy of `python/nedb` (29 files, recreated on every
build) and omits the 8 tracked `.js`/`.ts` files. 141 − 29 + 8 = 120. Use
`git ls-files`, excluding `rust/crates/nedb-py/python/` and `.d.ts`.

The SPDX line matters more than it looks: scanners (`reuse lint`, `cargo-deny`,
`pip-licenses`, FOSSA, Snyk) read the identifier out of the FILE. A repo whose
LICENSE says BUSL-1.1 while its sources say nothing reads as **UNKNOWN**, and
in a procurement review "unknown" is worse than "restrictive" — a restrictive
license can be approved by exception, an unknown one cannot be approved at all.

The Change License text ships as `COPYING-APACHE-2.0.txt` so the 2030 grant is
verifiable from the source tree. Dependencies are inventoried in
`THIRD_PARTY.md`, generated from real `cargo metadata` and installed-package
metadata rather than by hand — **the Change License never relicenses
dependencies**, so that inventory is a permanent obligation, not a snapshot.
Regenerate it whenever the dependency graph moves; the command is in the file.
Current state: 206 third-party crates, **zero copyleft obligations**, no GPL /
AGPL / SSPL / BUSL inbound.

For context when this comes up: CockroachDB's free tier is gated at **$10M**
revenue *and* requires telemetry. Our $1M threshold with no telemetry is a
deliberate choice of Mark's, taken knowing that comparison. **This is a
business decision, not an engineering one — do not change the threshold, and
get counsel review before invoicing against it.**

---

*© INTERCHAINED LLC × Claude Opus 5. Written to be useful to whoever reads it
next, which is probably me with no memory of any of this.*
