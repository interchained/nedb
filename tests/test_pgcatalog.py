#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
`pg_catalog` and `information_schema` — driven by a real libpq client.

# Why this matters

`\\dt` in psql and DBeaver's schema browser came back **empty**. An empty table
list does not read as "unsupported" to an evaluator — it reads as "this
database is broken" or "my data is gone". That is an evaluator's first ten
minutes.

# Why it is built the way it is

The cheap implementation is to recognise psql's exact query text and answer it
from a fixed table. Several pgwire-compatible engines do that. It is the wrong
choice here for one specific reason: **it breaks silently.** psql changes its
catalogue queries between versions, and when the pattern stops matching the
result is an empty table list — indistinguishable from a database that
genuinely has no tables. Same class of confidently-wrong answer as every other
bug this engine has had to fix.

So the catalogue is a set of REAL TABLES, synthesised from the live database
and queried through the ordinary predicate path. Every check below goes over
the wire through psycopg2/libpq, because unit tests prove the rows and only a
real client proves they are reachable.

# What a schemaless engine can honestly report

A collection is a table. A field observed in a sampled document is a column,
typed the way the wire layer types it. Everything Postgres tracks that NEDB
does not have — owners, tablespaces, ACLs, statistics — reports a fixed value
rather than a fabricated plausible one, and the tables NEDB genuinely has
nothing for come back EMPTY rather than absent, because generated SQL that
only wants to find none must not error.

Run: python3 tests/test_pgcatalog.py
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

try:
    import psycopg2
except ImportError:                                                # pragma: no cover
    print("SKIP: psycopg2 is not installed (pip install psycopg2-binary)")
    sys.exit(0)

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("  ok  " if cond else "  FAIL  ") + name + (f"  — {detail}" if detail else ""))


def section(title):
    print(f"\n  ── {title} " + "─" * max(2, 50 - len(title)))


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def find_nedbd():
    for cand in (
        os.path.join(ROOT, "rust", "target", "release", "nedbd"),
        os.path.join(ROOT, "rust", "target", "debug", "nedbd"),
        shutil.which("nedbd-v2") or "",
    ):
        if cand and os.path.exists(cand):
            return cand
    return None


BIN = find_nedbd()
if not BIN:
    print("SKIP: no nedbd binary found — build it with")
    print("      cargo build --release --bin nedbd -p nedb-engine")
    sys.exit(0)


def http(port, method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data,
                                 method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read() or b"null")


def run_suite(cur):
    def q(sql):
        cur.execute(sql)
        return cur.fetchall()

    # ── the table list, in both places a client looks ───────────────────────
    section("a collection is a table")

    rels = q("SELECT relname, relkind FROM pg_class ORDER BY relname")
    # Three collections are seeded, including one deliberately named `tables`
    # to prove the catalogue cannot shadow a user's own data.
    check("pg_class lists every collection as an ordinary table ('r')",
          rels == [("drivers", "r"), ("orders", "r"), ("tables", "r")], f"{rels}")

    # JDBC's DatabaseMetaData and most BI tools read information_schema, psql
    # reads pg_class. A collection visible in one and not the other is a
    # database that looks half empty depending on which tool you opened.
    tabs = q("SELECT table_name, table_type FROM information_schema.tables "
             "ORDER BY table_name")
    check("information_schema.tables agrees with pg_class",
          tabs == [("drivers", "BASE TABLE"), ("orders", "BASE TABLE"),
                   ("tables", "BASE TABLE")], f"{tabs}")

    check("the schema-qualified spelling resolves too",
          q("SELECT relname FROM pg_catalog.pg_class ORDER BY relname")
          == [("drivers",), ("orders",), ("tables",)])

    check("internal bookkeeping is not presented as a user table",
          not any(r[0].startswith("__") for r in rels), f"{rels}")

    # ── columns, with the types the wire actually sends ──────────────────────
    section("a document field is a column")

    cols = q("SELECT column_name, data_type FROM information_schema.columns "
             "WHERE table_name = 'orders' ORDER BY ordinal_position")
    got = dict(cols)
    check("every observed field is a column", {"status", "total"} <= set(got), f"{cols}")
    # The catalogue and the protocol share one typing function, so they cannot
    # disagree. A column reported `bigint` that arrives as text on the wire
    # would be a self-contradiction a client is entitled to trust.
    check("an integer field reports bigint", got.get("total") == "bigint", f"{got}")
    check("a text field reports text", got.get("status") == "text", f"{got}")
    check("a bool field reports boolean",
          dict(q("SELECT column_name, data_type FROM information_schema.columns "
                 "WHERE table_name = 'drivers'")).get("active") == "boolean")
    check("engine metadata is addressable as a column too",
          got.get("_seq") == "bigint", f"_seq -> {got.get('_seq')}")

    # Checked without an aggregate on purpose: an aggregate over a catalogue
    # table is refused (see the last section), so asking for MIN() here would
    # be testing the refusal rather than the numbering.
    firsts = q("SELECT column_name, ordinal_position FROM information_schema.columns "
               "WHERE table_name = 'orders' ORDER BY ordinal_position LIMIT 1")
    check("ordinal_position is 1-based, as Postgres numbers columns",
          firsts and firsts[0][1] == 1, f"{firsts}")

    # Nothing is NOT NULL in a schemaless store: any document may omit any
    # field, so claiming otherwise would be a promise the engine cannot keep.
    check("every column is nullable",
          all(r[0] == "YES" for r in q("SELECT is_nullable FROM information_schema.columns")))

    # ── pg_attribute correlates with pg_class ───────────────────────────────
    section("the catalogue is internally consistent")

    oid = q("SELECT oid FROM pg_class WHERE relname = 'orders'")[0][0]
    attrs = q(f"SELECT attname FROM pg_attribute WHERE attrelid = {oid} ORDER BY attnum")
    check("pg_attribute rows point at the matching pg_class oid",
          len(attrs) > 0 and ("total",) in attrs, f"{attrs}")
    check("the same oid comes back on a second query (a JOIN would hold)",
          q("SELECT oid FROM pg_class WHERE relname = 'orders'")[0][0] == oid)
    check("a synthetic oid stays inside Postgres's 32-bit OID range",
          0 < oid < 2**31, f"{oid}")

    # ── the ordinary predicate surface, on catalogue tables ─────────────────
    section("catalogue tables take ordinary SQL")

    check("WHERE works",
          q("SELECT relname FROM pg_class WHERE relname = 'orders'") == [("orders",)])
    check("ORDER BY DESC works",
          q("SELECT relname FROM pg_class ORDER BY relname DESC")
          == [("tables",), ("orders",), ("drivers",)])
    check("LIMIT works", len(q("SELECT relname FROM pg_class LIMIT 1")) == 1)
    check("OFFSET works",
          q("SELECT relname FROM pg_class ORDER BY relname OFFSET 1")
          == [("orders",), ("tables",)])

    # THE operator psql's catalogue filters need — and the reason the regex
    # operator was built before this.
    check("the ~ regex operator works on a catalogue table",
          q("SELECT relname FROM pg_class WHERE relname ~ '^ord'") == [("orders",)])
    check("…and !~ excludes",
          q("SELECT nspname FROM pg_namespace WHERE nspname !~ '^pg_' ORDER BY nspname")
          == [("information_schema",), ("public",)])

    # ── the schemas ─────────────────────────────────────────────────────────
    section("schemas, types and databases")

    check("pg_namespace reports public plus the two system schemas",
          q("SELECT nspname FROM pg_namespace ORDER BY nspname")
          == [("information_schema",), ("pg_catalog",), ("public",)])
    check("information_schema.schemata agrees",
          q("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
          == [("information_schema",), ("pg_catalog",), ("public",)])
    check("pg_database reports the server's database",
          q("SELECT datname, datallowconn FROM pg_database") == [("nedb", True)])
    check("pg_type lists a type the wire can actually send",
          q("SELECT typname FROM pg_type WHERE typname = 'int8'") == [("int8",)])
    # Listing Postgres's whole type table would advertise encoders the wire
    # layer does not have.
    check("…and NOT one it cannot",
          q("SELECT typname FROM pg_type WHERE typname = 'tsvector'") == [])

    # ── empty, not absent ───────────────────────────────────────────────────
    section("what NEDB has nothing for is EMPTY, not an error")

    for t in ["pg_index", "pg_constraint", "pg_description", "pg_tablespace",
              "information_schema.key_column_usage",
              "information_schema.table_constraints"]:
        try:
            rows = q(f"SELECT * FROM {t}")
            ok = rows == []
        except Exception as e:                                      # noqa: BLE001
            ok, rows = False, f"raised {type(e).__name__}: {str(e)[:60]}"
        # Returning rows would fabricate structure; erroring would break
        # generated SQL that only wants to find none.
        check(f"{t} is empty rather than an error", ok, f"{rows}")

    # ── a user collection is not shadowed by the catalogue ──────────────────
    section("a user collection named like a catalogue table")

    # `tables` and `columns` are words somebody will genuinely name a
    # collection. The information_schema qualifier is kept precisely so the
    # catalogue cannot shadow their data.
    check("a bare `tables` resolves to the USER's collection, not the catalogue",
          q("SELECT mine FROM tables") == [("real data",)],
          str(q("SELECT mine FROM tables")))
    check("…while the qualified spelling still reaches the catalogue",
          len(q("SELECT table_name FROM information_schema.tables")) == 3,
          str(q("SELECT table_name FROM information_schema.tables")))

    # ── clauses a catalogue cannot honour must be REFUSED ───────────────────
    section("a catalogue has no history — say so, do not fake it")

    for sql, why in [
        ("SELECT relname FROM pg_class AS OF SYSTEM TIME 0", "AS OF"),
        ("SELECT COUNT(*) FROM pg_class", "an aggregate"),
    ]:
        try:
            q(sql)
            check(f"{why} on a catalogue table is refused", False, "it answered")
        except Exception as e:                                      # noqa: BLE001
            # Silently ignoring AS OF would answer a time-travel question with
            # present-day rows, which is worse than refusing it.
            check(f"{why} on a catalogue table is refused",
                  "catalogue" in str(e), str(e)[:90])


def run_explain_suite(cur):
    def q(sql):
        cur.execute(sql)
        return [r[0] for r in cur.fetchall()]

    section("EXPLAIN reports what actually ran")

    plan = q("EXPLAIN SELECT n.nspname FROM pg_namespace n")
    check("EXPLAIN returns a QUERY PLAN column with plan lines",
          len(plan) >= 2 and any("Seq Scan on pg_namespace" in l for l in plan),
          f"{plan}")
    # No statistics exist, so an estimate would be a guess dressed as a number.
    check("the plan states it reports ACTUAL rows, not estimates",
          any("ACTUAL rows" in l for l in plan), f"{plan}")
    check("the column is named QUERY PLAN exactly, as PostgreSQL names it",
          [d.name for d in cur.description] == ["QUERY PLAN"],
          f"{[d.name for d in cur.description]}")

    # The single most useful line when a join is unexpectedly slow.
    joined = q(
        "EXPLAIN SELECT c.relname, n.nspname FROM pg_class c "
        "LEFT JOIN pg_namespace n ON n.oid = c.relnamespace")
    check("a join names its strategy",
          any("Nested Loop" in l or "Hash Join" in l for l in joined), f"{joined}")
    check("a join reports its proven key count",
          any("hash key" in l or "no equality key" in l for l in joined), f"{joined}")
    check("the join names the kind of join it was",
          any("Left" in l for l in joined), f"{joined}")

    # Indentation: the outermost operation first, inputs nested under it.
    check("inputs are indented under the operation that consumes them",
          any(l.startswith("  -> ") for l in joined), f"{joined}")

    filtered = q("EXPLAIN SELECT relname FROM pg_class WHERE relkind = 'r'")
    check("a WHERE clause appears as a Filter reporting what it removed",
          any("Filter" in l and "removed" in l for l in filtered), f"{filtered}")

    limited = q("EXPLAIN SELECT relname FROM pg_class LIMIT 1")
    check("LIMIT appears in the plan", any("Limit" in l for l in limited), f"{limited}")

    check("EXPLAIN ANALYZE is accepted (it already reports actual rows)",
          any("Seq Scan" in l
              for l in q("EXPLAIN ANALYZE SELECT nspname FROM pg_namespace")))

    # A statement the SQL evaluator does NOT run must not be given a plan that
    # describes a pipeline it never took.
    user = q("EXPLAIN SELECT * FROM orders")
    check("a statement the NQL path runs says so instead of inventing a plan",
          any("NQL path" in l for l in user) and not any("Seq Scan" in l for l in user),
          f"{user}")
    check("and it says WHY no plan is reported",
          any("never executed" in l for l in user), f"{user}")

    # The plan must describe the query asked about, not a cached one.
    a = q("EXPLAIN SELECT nspname FROM pg_namespace")
    bq = q("EXPLAIN SELECT relname FROM pg_class")
    check("two different queries get two different plans", a != bq)


def run_binding_suite(cur):
    def q(sql):
        cur.execute(sql)
        return cur.fetchall()

    section("duplicate output names, and ambiguous bindings")

    # PostgreSQL permits two output columns with the same name, and generated
    # SQL relies on it. Rows here are JSON objects, so two columns sharing a
    # name shared a KEY — the second write silently overwrote the first, and
    # this query returned the same value twice while reporting two columns.
    cur.execute("SELECT c.relname, n.nspname AS relname FROM pg_class c "
                "LEFT JOIN pg_namespace n ON n.oid = c.relnamespace "
                "ORDER BY 1 LIMIT 1")
    row = cur.fetchone()
    names = [d.name for d in cur.description]
    check("two columns may share a name over the wire",
          names == ["relname", "relname"], f"{names}")
    check("and they carry DIFFERENT values, positionally",
          row == ("drivers", "public"), f"{row}")

    # A silently empty answer is indistinguishable from "there is no such
    # data", which is why this is refused rather than answered.
    for sql, why in [
        ("SELECT c.relname FROM pg_class c JOIN pg_class c ON c.oid = c.oid",
         "the same alias twice"),
        ("SELECT relname FROM pg_class JOIN pg_class ON 1 = 1",
         "the same bare relation twice"),
    ]:
        try:
            q(sql)
            check(f"{why} is refused", False, "it answered")
        except Exception as e:                                      # noqa: BLE001
            check(f"{why} is refused", "ambiguous relation binding" in str(e),
                  str(e)[:110])

    # The supported spelling still works.
    got = q("SELECT a.nspname, b.nspname FROM pg_namespace a "
            "JOIN pg_namespace b ON a.oid = b.oid ORDER BY 1 LIMIT 1")
    check("aliasing the second use is the supported spelling",
          len(got) == 1 and got[0][0] == got[0][1], f"{got}")

    # An ordinary single-use query must be untouched by the guard.
    check("a relation used once is unaffected",
          len(q("SELECT relname FROM pg_class ORDER BY 1")) == 3)


def main():
    tmp = tempfile.mkdtemp(prefix="nedb-pgcat-")
    http_port, pg_port = free_port(), free_port()
    proc = subprocess.Popen(
        [BIN, "--data", os.path.join(tmp, "data"),
         "--port", str(http_port), "--pg-port", str(pg_port)],
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        env={**os.environ, "NEDBD_SWEEP_S": "0"},
    )
    try:
        for _ in range(60):
            time.sleep(0.25)
            try:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{http_port}/health", timeout=2).read()
                break
            except Exception:                                       # noqa: BLE001
                continue
        else:
            sys.exit("nedbd never came up")

        http(http_port, "POST", "/v1/databases", {"name": "shop"})
        for i, d in [("1", {"status": "paid", "total": 120}),
                     ("2", {"status": "open", "total": 40})]:
            http(http_port, "POST", "/v1/databases/shop/put",
                 {"coll": "orders", "id": i, "doc": d})
        http(http_port, "POST", "/v1/databases/shop/put",
             {"coll": "drivers", "id": "d1", "doc": {"name": "Bob", "active": True}})
        # A collection named exactly like a catalogue relation, to prove the
        # catalogue cannot shadow a user's own data.
        http(http_port, "POST", "/v1/databases/shop/put",
             {"coll": "tables", "id": "t1", "doc": {"mine": "real data"}})

        conn = psycopg2.connect(host="127.0.0.1", port=pg_port, dbname="shop",
                                user="nedb", connect_timeout=10)
        conn.autocommit = True
        cur = conn.cursor()
        run_suite(cur)
        run_explain_suite(cur)
        run_binding_suite(cur)
        cur.close()
        conn.close()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:                                           # noqa: BLE001
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        for f in FAIL:
            print("  FAILED:", f)
        sys.exit(1)
    print("pg_catalog and information_schema are real queryable tables —")
    print("derived from the live database, filtered with ordinary SQL.")


if __name__ == "__main__":
    main()
