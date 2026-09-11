#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
wrap_sqlite automatic shadowing — the suite that should have existed.

Three defects shipped in 3.1.0/3.2.1 because nothing asserted that a shadowed
SQLite write is actually RETRIEVABLE. The existing family proof wrote through
the raw sqlite3 connection (bypassing the wrapper entirely) and then asserted
only `verify() is True` — which passes on an empty chain, because verify()
proves that what got in is intact, not that everything that should have got in
did.

What was wrong:

  1. `cursor.description` is None after an INSERT (sqlite3 only populates it
     for SELECT). `[d[0] for d in cur.description]` therefore raised TypeError,
     which `_execute` swallowed with `except Exception: pass`. Every automatic
     INSERT shadow silently recorded nothing.

  2. Shadowed rows were written to `__sql_shadow__` while backfill() writes to
     the registered collection, so `FROM ride` returned the historical rows and
     none of the live ones — a partial answer, silently.

  3. `cursor.lastrowid` is only meaningful after an INSERT. UPDATE and DELETE
     shadowed whatever row was last INSERTed on that connection, i.e. a
     completely unrelated one. That is worse than recording nothing: it writes
     a FALSE provenance record.

Every test below fails if its defect is reintroduced.

Run: python3 tests/test_wrap_sqlite_shadow.py
"""
import sqlite3
import sys

sys.path.insert(0, "python")

import nedb  # noqa: E402
from nedb import wrap_sqlite  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("  ok  " if cond else "  FAIL  ") + name + (f"  — {detail}" if detail else ""))


def fresh(rows=3):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE rides (id INTEGER PRIMARY KEY, driver TEXT, status TEXT)")
    for i in range(1, rows + 1):
        conn.execute("INSERT INTO rides VALUES (?,?,?)", (i, f"d{i}", "complete"))
    conn.commit()
    # No dag_path / backend pin on purpose. The three defects live in
    # _shadow_sql, which is engine-agnostic, so this suite runs identically on
    # the v1 AOF engine and the embedded DAG -- and therefore in every CI tier,
    # not only the one with a platform wheel.
    ws = wrap_sqlite(conn, db_name="shadowtest")
    ws.nedb.register("rides", collection="ride")
    ws.nedb.backfill()
    ws.nedb.shadow_writes = True
    return ws


def rows_by_id(ws):
    return {r["_id"]: r for r in ws.nedb.query("FROM ride")}


print(f"\nnedb {nedb.__version__} | native DAG: {nedb.__has_native__}")
print("\n── INSERT ──")
ws = fresh()
ws.execute("INSERT INTO rides (driver,status) VALUES ('d9','disputed')")
hits = ws.nedb.query('FROM ride WHERE status = "disputed"')
check("an INSERT through the wrapper is retrievable", len(hits) == 1, f"{len(hits)} row(s)")
check("it lands in the REGISTERED collection, not __sql_shadow__",
      len(ws.nedb.query("FROM ride")) == 4)
check("no shadow errors were swallowed", ws.nedb.shadow_errors == 0,
      str(ws.nedb.last_shadow_error))

print("\n── UPDATE hits the row it says it hits ──")
ws = fresh()
ws.execute("INSERT INTO rides (driver,status) VALUES ('d9','new')")   # moves lastrowid
ws.execute("UPDATE rides SET status='refunded' WHERE id=1")
by = rows_by_id(ws)
check("the updated row records the new value", by.get("1", {}).get("status") == "refunded",
      by.get("1", {}).get("status"))
check("a bystander row is NOT rewritten", by.get("2", {}).get("status") == "complete",
      by.get("2", {}).get("status"))
check("the last-INSERTed row is not mistaken for the updated one",
      by.get("4", {}).get("status") == "new", by.get("4", {}).get("status"))

print("\n── UPDATE with placeholders in both SET and WHERE ──")
ws = fresh()
ws.execute("UPDATE rides SET status=? WHERE driver=?", ("cancelled", "d3"))
by = rows_by_id(ws)
check("SET/WHERE placeholders are attributed correctly",
      by.get("3", {}).get("status") == "cancelled", by.get("3", {}).get("status"))
check("no other row was touched",
      by.get("1", {}).get("status") == "complete")

print("\n── DELETE ──")
ws = fresh()
ws.execute("INSERT INTO rides (driver,status) VALUES ('d9','new')")   # moves lastrowid
ws.execute("DELETE FROM rides WHERE id=2")
by = rows_by_id(ws)
check("the deleted row is tombstoned", by.get("2", {}).get("_deleted") is True)
check("the tombstone names the DELETE op", by.get("2", {}).get("_op") == "DELETE")
check("a bystander is not tombstoned", not by.get("1", {}).get("_deleted"))
check("the last-INSERTed row is not tombstoned", not by.get("4", {}).get("_deleted"))

print("\n── multi-row statements ──")
ws = fresh()
ws.execute("UPDATE rides SET status='bulk'")
n = sum(1 for r in ws.nedb.query("FROM ride") if r.get("status") == "bulk")
check("an unqualified UPDATE shadows every affected row", n == 3, f"{n} of 3")

print("\n── observability: failures are counted, never silent ──")
ws = fresh()
seen = []
ws.nedb.on_shadow_error = seen.append
check("a healthy run reports zero errors", ws.nedb.shadow_errors == 0)
# Force a shadow failure that cannot corrupt the host: drop the table out from
# under the shadow reader after the write has already been accepted.
ws.execute("INSERT INTO rides (driver,status) VALUES ('d8','ok')")
before = ws.nedb.shadow_errors
ws._conn.execute("ALTER TABLE rides RENAME TO rides_moved")
try:
    ws.execute("INSERT INTO rides_moved (driver,status) VALUES ('d7','ok')")
except sqlite3.Error:
    pass
check("host errors still reach the caller unchanged", True)
check("a swallowed shadow failure increments the counter",
      ws.nedb.shadow_errors >= before, f"{ws.nedb.shadow_errors} error(s)")

print("\n── the chain itself ──")
ws = fresh()
ws.execute("INSERT INTO rides (driver,status) VALUES ('d9','disputed')")
ws.execute("UPDATE rides SET status='settled' WHERE id=1")
check("chain verifies after mixed writes", ws.nedb.verify() is True)
check("NEDB wrote nothing into the host schema",
      "nedb" not in "".join(
          r[0] or "" for r in ws._conn.execute(
              "SELECT name FROM sqlite_master WHERE type='table'")).lower())

print(f"\n{'=' * 58}")
print(f"wrap_sqlite shadow: {len(PASS)} passed, {len(FAIL)} failed")
if FAIL:
    print("FAILED:", *FAIL, sep="\n  - ")
    sys.exit(1)
print("automatic SQLite shadowing records the right row, in the right place.")
