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
| #110–#114 | BUSL relicense, fork relicense, SPDX headers, `~` regex, `pg_catalog` as real tables | the licence actually holds; `\dn` became reachable |
| #115 | **a real SQL evaluator** (`sqlselect.rs`) | `psql \dt` works — 11/14 backslash commands |
| #116 | **hash join + the frozen semantic corpus** | the evaluator became a subsystem with a contract |
| #117 | **execution plan + `EXPLAIN` + the row budget** | the engine can say why it ran a query that way; `LIMIT` stopped materialising whole joins |
| #118 | **ambiguous bindings refused; duplicate output names fixed** | two wrong-answer surfaces closed before any further optimiser work |
| #119 | **conservative predicate pushdown, with recorded refusals** | a selective filter no longer waits for the whole join |
| #120 | **physical `Filter`-in-`Join`** | `WHERE ... LIMIT n` stops early; `ON` and `WHERE` stay logically distinct |
| #121 | **streaming `Resolver`** | `LIMIT 20` pulls exactly 20 source rows, not 8000 |

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

### The two failure modes that keep recurring

Both were caught again this session, and both are cheap to check for:

1. **A test that cannot fail.** `duplicate_output_names_are_not_silently_renamed`
   asserted the column NAMES and never the VALUES, so it passed while
   `SELECT e.name, e2.name` returned one value twice. The multi-join budget
   guard passed under the mutation that removed the guard. **Mutate the
   implementation and confirm the suite screams** — if it does not, the test is
   decorative.
2. **Asserting content instead of structure.** No test caught the `EXPLAIN`
   tree printing a join's two scans at different depths, because every
   assertion checked which relation and how many rows — and those were right.
   `Plan::tree()` now exists so tests assert TOPOLOGY (`children().len() == 2`,
   `depth()`), and five of them fail if the shape regresses.

### Test surface (all green at v4.0.0)

| Tier | Count | Notes |
|---|---|---|
| `cargo test --lib` | 355 | the Rust core |
| `cargo test --tests` | 15 | integration, incl. v3 segments + compaction |
| **semantic corpus** | 44 | frozen SQL meaning, run under **every** join strategy |
| **join differential** | 11 | hash vs nested loop, incl. 640 generated cases |
| `EXPLAIN` over libpq | 15 | inside the `pg_catalog` suite, real psycopg2 |
| bindings + duplicate names | 6 | same suite; duplicate names verified positionally |
| plan topology | 6 | structural, not rendered text |
| **pushdown differential** | 6 | on vs off, x both join strategies |
| **fusion differential** | 9 | fused vs unfused, x both join strategies |
| **streaming** | 12 | counts rows PULLED, with exact assertions |
| psql introspection | 44 | drives the **real `psql` binary** |
| `pg_catalog` | 35 | catalogue as queryable tables |
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

### 3.1 `pg_catalog` + `information_schema` — **DONE** (#114, #115)

Implemented as **real queryable virtual tables** (`pgcatalog.rs`), not as
pattern matches against known query strings — pattern matching breaks silently
when psql changes its query, and a silently empty table list looks exactly
like "you have no tables".

`psql` now runs **11 of 14** backslash commands: `\dn \dt \dv \di \dm \dS
\l \du \dg \df \dx`. The three that do not are refused **by name**:
`\dp` needs an ARRAY constructor, `\dT` a subquery, `\d <table>` regex
capture groups.

Do **not** chase 14/14 for its own sake. If a remaining command needs
disproportionate catalogue emulation with no benefit to ordinary SQL users,
leave it explicit and move on. Subqueries are worth building; a
PostgreSQL-specific catalogue curiosity is not.

### 3.2 JOIN — **DONE** (#115 nested loop, #116 hash)

Both strategies are permanent and their roles are distinct:

* **nested loop** — the semantic reference, the implementation for
  non-equality predicates, and the oracle the differential suite compares
  against. Never delete it.
* **hash join** — chosen when the planner can *prove* an equality key. The
  hash table only NARROWS candidates; every surviving pair is re-checked
  against the complete, unmodified `ON` expression. See `sqljoin.rs`.

The reason that split exists is worth internalising: **equality in this engine
is not transitive.** `1 = '1'` and `1 = '1.0'` are both TRUE while
`'1' = '1.0'` is FALSE, because numbers and numeric strings compare
numerically but two strings compare exactly. Bucketing assumes an equivalence
relation, so bucketing alone cannot be correct here. `hkey()` needs exactly one
property — *if `a = b` is TRUE then `hkey(a) == hkey(b)`* — and everything else
is performance.

**Done in #117:**

* **An execution plan** (`src/sqlplan.rs`) and `EXPLAIN` / `EXPLAIN ANALYZE`
  over the wire. The plan is **emitted by the executor** as it works, never
  assembled alongside it — a plan built independently can drift, and an
  `EXPLAIN` that confidently describes a pipeline the engine did not run is
  worse than none, because it sends the reader to optimise a shape that never
  existed. For the same reason `EXPLAIN` of a statement the **NQL path** runs
  says so plainly instead of inventing a plan for it.
* `EXPLAIN` always reports **actual** rows. There are no statistics to estimate
  from, and a guess printed as a number is worse than the truth. The output
  says so, so nobody mistakes it for PostgreSQL's estimate.
* **The row budget** — when the final answer is a prefix of a join's output,
  the join stops once it has enough rows. Every disqualifying condition is
  load-bearing and independently proven by mutation: `ORDER BY`, `DISTINCT`, a
  `WHERE` clause, or more than one join. `OFFSET` is *added* to the budget
  rather than disqualifying it.

**Done in #118** — wrong-answer surfaces, closed before continuing:

* Ambiguous relation bindings refused by name (see §4).
* **Duplicate output names carried the same value.** PostgreSQL permits
  `SELECT e.name, e2.name`, and generated SQL relies on it — but rows here are
  JSON objects, so two columns sharing a name shared a KEY and the second write
  silently overwrote the first. Output columns are now `OutCol { key, name }`:
  the key is disambiguated, the display name is untouched. Renaming the column
  instead would be worse, because generated SQL asks for the name it wrote.
* A latent index drift in projection: a single counter walked both the
  name-building and projection stages, and a `*` that skipped an already-named
  column left it pointing at the wrong name. Each select item now owns an
  explicit span of output columns.

**Done in #119** — conservative predicate pushdown.

A `WHERE` conjunct reading exactly ONE relation is COPIED to pre-filter that
relation before the join. The `WHERE` is retained and still runs afterwards.

**Retention alone is not sufficient, and believing it was cost a wrong
answer.** The first version argued a copy-not-move was safe for every join
type, because newly-unmatched rows get NULL-extended and the retained filter
then drops them. But a predicate can be SATISFIED by a synthesised NULL:

```sql
SELECT e.name FROM emp e LEFT JOIN dept d ON e.dept_id = d.id
 WHERE d.dname IS NULL          -- 1 row, and it became 5
```

No `dept` row has a NULL `dname`, so pre-filtering empties `dept`, every `emp`
row becomes unmatched, and `IS NULL` is TRUE for all of them. **The semantic
corpus caught it on the first run.** That is the corpus doing precisely the job
it was built for.

So the real rule is about NULL SYNTHESIS:

> A predicate may be pre-applied to relation `R` only if `R` is never
> NULL-synthesised in this query.

`sqlpush::nullable_bindings` computes that set: a join's right binding is
nullable under `LEFT`/`FULL`, and a LATER `RIGHT`/`FULL` join retroactively
makes every binding accumulated before it nullable — the `FROM` relation
included. An all-inner query can push everything, which is the common case.

Refusals are **recorded on the plan**, not silent, and visible in `EXPLAIN`:
`Filter retained above join: predicate references nullable side of an outer
join (n)`. An optimiser that silently declines cannot be audited — you cannot
tell "correctly refused" from "forgot to look".

Measured: `join + selective pred` at 8000x1500 went 8774 -> 118ms on the
nested loop and 27.1 -> 8.0ms on the hash join. `join + broad pred` barely
moved, which is correct — it keeps 85% of the rows.

**Done in #120** — physical `Filter`-in-`Join`.

The `WHERE` clause is evaluated inside the final join's loop rather than as a
separate pass. It is a PHYSICAL change only, and one rule keeps it that way:

> Whether a row counts as MATCHED is decided by `ON` alone.

That is semantic law, not a preference. An `ON` predicate and a post-join
`WHERE` predicate mean different things:

```sql
LEFT JOIN ... ON a.k = b.k AND b.tag = 'q'     -- keeps every left row
LEFT JOIN ... ON a.k = b.k WHERE b.tag = 'q'   -- discards the outer rows
```

If the filter were allowed to influence `matched`, a left row whose only
partner fails the filter would be NULL-extended — and `WHERE b.tag IS NULL`
would then ACCEPT that synthesised row, inventing output the unfused pipeline
never produces. Same trap as the pushdown mistake, in a different place.
Evaluation order is fixed: candidate pair -> `ON` -> NULL synthesis if the
outer join requires it -> post-join filter -> count toward the row budget.

The plan reports the fused filter as its own number
(`post-join filter removed N`) rather than folding it into the join's row
count, so the two remain distinguishable in `EXPLAIN`.

The payoff: a `WHERE` no longer disqualifies the row budget. Measured by
`examples/fusebench` at 8000x1500 returning 20 rows — nested 4657 -> 34.2ms
(136x), hash 19.9 -> 11.2ms (1.8x).

**Read that hash number as a signal, not a disappointment.** The hash gain
SHRINKS with size (2.7x -> 3.2x -> 1.8x) because at that shape the hash path
is dominated by materialising relations, not probing them. Stopping the probe
early cannot recover time already spent cloning 9500 rows.

**Done in #121** — the streaming `Resolver`.

`Resolver` now returns `Box<dyn Relation>`, a two-method trait: `next_row` and
an optional `size_hint`. Not an async stream, not a borrowing iterator with a
lifetime threaded through the evaluator — the smallest thing that permits
*pull a row* and *stop*.

The DRIVING relation is streamed and pre-filtered inline; the INNER side of
every join is still materialised, on purpose, because a hash join must build
its table before probing and a nested loop re-scans the inner side per left
row. That limit is pinned by a test so it is recorded rather than rediscovered
and mistaken for a bug.

**Verified by counting, not by timing.** A fast run proves nothing about how
many rows were requested. The test source counts every row it hands out:

| query | left rows pulled (of 8000) |
|---|---|
| `... JOIN ... LIMIT 20` | **20** |
| `... JOIN ... WHERE amount > 500 LIMIT 20` | **92** |
| `... JOIN ... LIMIT 5 OFFSET 40` | **45** |
| `... JOIN ...` (no limit) | 8000 |
| `... ORDER BY ... LIMIT 20` | 8000 (correctly refused) |

Those are EXACT assertions, and that matters. The first version asserted
`left < 100` — which would have passed at 99 and hidden a real inefficiency.
Mark asked "why doesn't it pull 20?", and the honest answer was that it does;
the test simply was not saying so. Moving the budget check to the bottom of
the loop makes it pull 21, and the exact assertion catches that where the
loose bound did not.

The 92 is worth keeping written down because it looks arbitrary and is not:
`amount` is `(7 * i) % 1000`, so rows 0..=71 all fail `> 500` (7 * 71 = 497)
and rows 72..=91 supply the twenty survivors. 72 rejected + 20 kept, minimal
for that data.

**Storage is still eager.** `nql::query` materialises a whole collection, so
the daemon's own resolver hands back a `from_vec`. The EVALUATOR no longer
requires that, which is the half of the work that had to come first — but
nobody should read the streaming interface as a claim that the storage scan is
lazy. Making `nql::query` yield rows is the next step and is noted in
`pgwire.rs` where the eager call lives.

**The next item.**

* **A lazy storage scan**, so the `LIMIT 20` result above holds end to end
  rather than only above the storage boundary.
* **Skew benchmark, then derive the hash crossover from it.**
  `AUTO_HASH_MIN_PAIRS = 64` is a guess. Measure uniform-unique, moderate
  duplicates, a single hot key, all-same-key, and coercion-heavy
  numeric/string keys — the threshold must come from the ugly shapes.

Then **subqueries**, one semantic class at a time, each with its own
regression corpus: scalar uncorrelated, `IN (SELECT ...)`, `EXISTS`,
correlated, derived tables. Prioritise by the captured `psql` queries, not by
abstract SQL completeness.

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

* **`SELECT *` column order.** Now the document's own order, because
  `serde_json`'s `preserve_order` feature is enabled. That feature is
  **workspace-wide** (Cargo unifies features), so the napi and pyo3 bindings
  get it too. It is safe for the content-addressed store because a node's hash
  is taken over the **bytes as written** and verification re-hashes those same
  bytes — `decode()` never re-serialises a parsed node, so key order cannot
  invalidate an existing hash. Verified, not assumed: backcompat 109/109 and
  DAG-preservation 62/62 green with it on, including the case that asserts
  `verify()` still FAILS when it should.
* **`1` and `1.0` are the same value.** PostgreSQL distinguishes integer from
  numeric-with-scale; JSON has no numeric-with-scale type, so it cannot be
  represented. Integral values render as integers (`SELECT 1` → `1`, fixed in
  #116 — it used to answer `1.0`, which clients read as the text "1.0").
  `SELECT 1.0` therefore also renders as `1`. Unrepresentable either way;
  stated rather than hidden.
* **A relation used twice without aliases is now REFUSED** (#118), with
  `ambiguous relation binding: "a" appears more than once; use aliases`. It
  used to return silently NOTHING, because a qualified reference takes the
  first matching binding and so `emp.mgr = emp.id` compared every row to
  itself. The check is case-insensitive, because binding resolution is.
  `FROM a JOIN a AS a2` is the supported spelling.

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
* **A masked failure is indistinguishable from a pass.** `cargo check … | grep`
  printed nothing and looked clean when the truth was `cargo: command not
  found` (it lives in `~/.cargo/bin`, not on the default PATH here). Check exit
  codes, not just filtered output. Same disease as `| tail`.
* **Prove the test can fail.** A differential suite that passes on broken code
  is worthless. Mutate the implementation deliberately and confirm the suite
  screams: bucketing numeric strings as text (losing `1 = '1'`) must fail ~7
  tests; skipping the confirm step must fail loudly. Two of four mutations I
  tried were *duds* that changed no answer — without running them I would have
  credited the suite with catching things it never could.
* **`npm run build` for the node addon, never bare `napi build`.** The package
  script passes `--js native.js --dts native.d.ts`; without those flags napi
  OVERWRITES the hand-written `index.js` durable-mode wrapper with a generated
  loader, and `durability.test.mjs` then fails in a way that looks like a
  durability regression. It is not. `git checkout -- index.js index.d.ts`.
* A `git checkout -- a b c` with one **untracked** path in the list fails
  wholesale and restores *nothing*, silently. Restore tracked paths only.
* **Never patch Rust string literals from inside a Python heredoc.** A trailing
  `\` inside a Python triple-quoted string is a PYTHON line continuation, so
  Rust's own `\`-newline continuations get flattened *with their source
  indentation baked in* — three user-visible strings shipped reading
  "executed by                      the storage engine". Use the editor for
  string literals, or grep for runs of 6+ spaces inside quotes afterwards.
* **A join has two inputs, so a plan renderer is not a list.** The first
  `EXPLAIN` indented a join's two scans differently, which reads as "the left
  relation was scanned inside the scan of the right one" — a false claim about
  execution, caught only by looking at real `psql` output.

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
