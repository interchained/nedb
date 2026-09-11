#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
`shadow_writes = True` and nothing else — verified against REAL PostgreSQL.

# What this suite is for

Shadowing used to demand three steps (`register()` every table, `backfill()`,
then the flag) and, on Postgres, a fourth: a manual `shadow_row()` call after
every single write. That is not a feature with setup cost. It is a feature that
hands the work back to the caller and can never be finished.

And it failed SILENTLY, in four distinct ways — each found by running the code
rather than reading it:

  1. `shadow_writes = True` with no `register()` calls was a NO-OP. The flag
     read "on", mirrored nothing, raised nothing, counted nothing.

  2. Registration was opt-IN, so the normal outcome was not "nothing happens"
     but SILENT PARTIAL COVERAGE. Three tables of twelve looks exactly like a
     complete audit trail until the day you need it.

  3. Table matching was a SUBSTRING test — "does any registered table name
     appear anywhere in this SQL?". Fine with one table registered; with every
     table registered (which is what discovery does) a write to
     `drivers_archive` matched the mapping for `drivers` and was mirrored into
     the wrong collection under the wrong row's id. FALSE PROVENANCE: a record
     that looks authoritative and describes something that never happened.

  4. `conn.cursor().execute(...)` — the canonical DB-API idiom, and the one the
     modules' own docstrings showed — was never intercepted. Only
     `conn.execute` was, which psycopg does not even have. The host got two
     rows, NEDB got one, `verify()` returned True, nothing was reported.

Every check below pins one of those shut, or pins the behaviour that replaced
it. The Postgres half runs against a real PostgreSQL server (via `pgserver`),
because "an audit layer for Postgres" verified only against a mock is not
verified at all.

Run: python3 tests/test_wrap_autoshadow.py
"""
import os
import shutil
import sqlite3
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "python"))

from nedb import wrap_sqlite                                    # noqa: E402
from nedb.wrap_core import target_table, write_op               # noqa: E402

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("  ok  " if cond else "  FAIL  ") + name + (f"  — {detail}" if detail else ""))


def section(title):
    print(f"\n  ── {title} " + "─" * max(2, 52 - len(title)))


def plain(rows):
    """Drop engine metadata, keep the shadow's own annotations."""
    return [{k: v for k, v in r.items()
             if not k.startswith("_") or k in ("_op", "_deleted", "_table")}
            for r in rows]


# ── the statement parser the whole thing rests on ───────────────────────────

def suite_target_table():
    section("which table does a write target?")

    cases = [
        ("INSERT INTO drivers (a) VALUES (1)", "drivers", "INSERT"),
        ("insert into drivers (a) values (1)", "drivers", "INSERT"),
        ("INSERT INTO public.drivers (a) VALUES (1)", "drivers", "INSERT"),
        ('INSERT INTO "drivers" (a) VALUES (1)', "drivers", "INSERT"),
        ('INSERT INTO "public"."drivers" (a) VALUES (1)', "drivers", "INSERT"),
        ("INSERT INTO `drivers` (a) VALUES (1)", "drivers", "INSERT"),
        ("INSERT OR REPLACE INTO drivers (a) VALUES (1)", "drivers", "INSERT"),
        ("REPLACE INTO drivers (a) VALUES (1)", "drivers", "INSERT"),
        ("UPDATE drivers SET a = 1", "drivers", "UPDATE"),
        ("UPDATE drivers_archive SET a = 1", "drivers_archive", "UPDATE"),
        ("DELETE FROM drivers WHERE a = 1", "drivers", "DELETE"),
        ("  \n  DELETE  FROM   drivers", "drivers", "DELETE"),
    ]
    for sql, table, op in cases:
        got_t, got_o = target_table(sql), write_op(sql)
        check(f"{sql[:46]!r} → {table}",
              got_t == table and got_o == op, f"got {got_t!r}/{got_o!r}")

    # THE case that made substring matching dangerous.
    check("a write to `drivers_archive` is NOT read as `drivers`",
          target_table("INSERT INTO drivers_archive (a) VALUES (1)") == "drivers_archive",
          "substring matching mirrored this into the wrong collection")

    # A quoted identifier may legally contain a dot, so the schema cannot be
    # stripped by splitting the captured text on ".".
    dotted = target_table('INSERT INTO "my.table" (a) VALUES (1)')
    check("a dot INSIDE a quoted identifier is part of the name",
          dotted == "my.table", f"got {dotted!r}")
    qualified = target_table('UPDATE "public"."my.table" SET a = 1')
    check("…and a schema-qualified quoted name still reduces to the table",
          qualified == "my.table", f"got {qualified!r}")

    for sql in ["SELECT * FROM drivers", "CREATE TABLE drivers (a int)",
                "WITH x AS (SELECT 1) INSERT INTO drivers SELECT * FROM x", ""]:
        check(f"not a plain write: {sql[:40]!r}", target_table(sql) is None,
              f"got {target_table(sql)!r} — a CTE write must be declined, "
              f"not guessed at")


# ── SQLite: one flag, whole database ───────────────────────────────────────

def suite_sqlite():
    section("SQLite — shadow_writes = True and nothing else")

    conn = wrap_sqlite(sqlite3.connect(":memory:"), db_name="app")
    conn.execute("CREATE TABLE drivers (id INTEGER PRIMARY KEY, name TEXT, status TEXT)")
    conn.execute("CREATE TABLE drivers_archive (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("CREATE TABLE trips (id INTEGER PRIMARY KEY, driver TEXT, fare INTEGER)")
    conn.execute("CREATE VIEW active AS SELECT * FROM drivers WHERE status = 'active'")

    # The whole setup.
    conn.nedb.shadow_writes = True

    found = sorted(m.pattern for m in conn.nedb._mappings)
    check("every table is discovered from the catalogue, with no registration",
          found == ["drivers", "drivers_archive", "trips"], f"{found}")
    check("a VIEW is not mirrored (it is not writable)",
          "active" not in found, f"{found}")

    # Path 1: conn.execute — the sqlite3 convenience.
    conn.execute("INSERT INTO drivers (name, status) VALUES ('Ann', 'active')")
    # Path 2: cursor.execute — the canonical DB-API idiom, once invisible.
    cur = conn.cursor()
    cur.execute("INSERT INTO drivers (name, status) VALUES (?, ?)", ("Bob", "active"))
    # Path 3: executemany.
    cur.executemany("INSERT INTO trips (driver, fare) VALUES (?, ?)",
                    [("Ann", 10), ("Bob", 20)])
    conn.commit()

    names = sorted(r.get("name") for r in conn.nedb.query("FROM drivers"))
    check("conn.execute AND cursor.execute are BOTH mirrored",
          names == ["Ann", "Bob"], f"{names} — cursor.execute was the silent hole")
    fares = sorted(r.get("fare") for r in conn.nedb.query("FROM trips"))
    check("executemany mirrors every parameter set", fares == [10, 20], f"{fares}")

    # The substring bug, end to end.
    cur.execute("INSERT INTO drivers_archive (name) VALUES (?)", ("Old",))
    conn.commit()
    arch = plain(conn.nedb.query("FROM drivers_archive"))
    check("a write to drivers_archive lands in drivers_archive",
          len(arch) == 1 and arch[0].get("name") == "Old", f"{arch}")
    check("…and NOT in drivers",
          sorted(r.get("name") for r in conn.nedb.query("FROM drivers")) == ["Ann", "Bob"],
          "false provenance: the wrong collection got the row")

    # An UPDATE supersedes rather than accumulating, and history survives.
    conn.execute("UPDATE drivers SET status = 'off' WHERE name = 'Bob'")
    conn.commit()
    now = {r["name"]: r.get("status") for r in conn.nedb.query("FROM drivers")}
    check("an UPDATE supersedes the same document", now.get("Bob") == "off", f"{now}")
    check("…and the prior value is still readable at its sequence",
          "active" in [r.get("status") for r in conn.nedb.query("FROM drivers AS OF 1")],
          str([r.get("status") for r in conn.nedb.query("FROM drivers AS OF 1")]))

    # A DELETE is a tombstone in the shadow too.
    conn.execute("DELETE FROM trips WHERE fare = 10")
    conn.commit()
    tomb = [r for r in conn.nedb.query("FROM trips") if r.get("_deleted")]
    check("a host DELETE becomes a tombstone, not an erasure",
          len(tomb) == 1, f"{plain(conn.nedb.query('FROM trips'))}")

    check("no silent gaps and no swallowed errors",
          not conn.nedb.unmirrored_tables and conn.nedb.shadow_errors == 0,
          f"unmirrored={conn.nedb.unmirrored_tables} errors={conn.nedb.shadow_errors}")
    check("the chain verifies", conn.nedb.verify() is True)


def suite_sqlite_optout():
    section("SQLite — coverage is opt-OUT")

    conn = wrap_sqlite(sqlite3.connect(":memory:"), db_name="opt")
    conn.execute("CREATE TABLE keep (id INTEGER PRIMARY KEY, v TEXT)")
    conn.execute("CREATE TABLE sessions (id INTEGER PRIMARY KEY, tok TEXT)")
    conn.execute("CREATE TABLE audit_log_2026 (id INTEGER PRIMARY KEY, v TEXT)")

    conn.nedb.exclude = {"sessions", "audit_log_*"}
    conn.nedb.shadow_writes = True
    found = sorted(m.pattern for m in conn.nedb._mappings)
    check("excluded tables are never mapped (exact name and glob)",
          found == ["keep"], f"{found}")

    conn.execute("INSERT INTO sessions (tok) VALUES ('s')")
    conn.commit()
    check("a write to an excluded table mirrors nothing",
          conn.nedb.query("FROM sessions") == [])
    check("…and a DELIBERATE omission is not reported as an accidental gap",
          not conn.nedb.unmirrored_tables,
          f"{conn.nedb.unmirrored_tables} — `assert not unmirrored_tables` has "
          f"to be able to pass on a correctly configured connection")

    # An accidental gap, by contrast, MUST be visible.
    conn.execute("CREATE TABLE added_later (id INTEGER PRIMARY KEY, v TEXT)")
    conn.execute("INSERT INTO added_later (v) VALUES ('x')")
    conn.commit()
    check("a table created AFTER discovery is reported as an unmirrored gap",
          "added_later" in conn.nedb.unmirrored_tables,
          f"{conn.nedb.unmirrored_tables} — silence here is the original sin")

    # An explicit register() still wins over discovery.
    c2 = wrap_sqlite(sqlite3.connect(":memory:"), db_name="ovr")
    c2.execute("CREATE TABLE drivers (id INTEGER PRIMARY KEY, name TEXT)")
    c2.nedb.register("drivers", collection="driver")
    c2.nedb.shadow_writes = True
    c2.execute("INSERT INTO drivers (name) VALUES ('Zed')")
    c2.commit()
    check("an explicit register() overrides the discovered default",
          len(c2.nedb.query("FROM driver")) == 1 and c2.nedb.query("FROM drivers") == [],
          f"driver={c2.nedb.query('FROM driver')} drivers={c2.nedb.query('FROM drivers')}")


# ── Postgres: the one that was entirely manual ─────────────────────────────

def suite_postgres():
    section("PostgreSQL — REAL server, one flag, zero shadow_row() calls")

    try:
        import pgserver
        import psycopg2
    except ImportError as e:
        # A self-skipping test is indistinguishable from a passing one, which
        # is the precise failure mode this whole suite exists to catch. So CI
        # sets NEDB_REQUIRE_PG=1 and a missing server becomes a FAILURE there,
        # while a local run without Postgres installed still skips politely.
        if os.environ.get("NEDB_REQUIRE_PG") == "1":
            check("a real PostgreSQL server is available", False,
                  f"NEDB_REQUIRE_PG=1 but {e} — refusing to silently skip the "
                  f"half of this suite that tests the actual product")
            return
        print("  …  skipped: pip install pgserver psycopg2-binary")
        print("     (an audit layer for Postgres verified only against a mock")
        print("      is not verified at all — so this half needs a real server)")
        print("     set NEDB_REQUIRE_PG=1 to make a missing server a failure")
        return

    from nedb import wrap_postgresql

    tmp = tempfile.mkdtemp(prefix="nedb-pgauto-")
    srv = None
    try:
        srv = pgserver.get_server(os.path.join(tmp, "pg"))
        uri = srv.get_uri()

        setup = psycopg2.connect(uri)
        setup.autocommit = True
        c = setup.cursor()
        for stmt in [
            "CREATE TABLE drivers (id serial PRIMARY KEY, name text, "
            "status text, password_hash text)",
            "CREATE TABLE drivers_archive (id serial PRIMARY KEY, name text)",
            "CREATE TABLE trips (id serial PRIMARY KEY, driver text, fare int)",
            "CREATE TABLE sessions (id serial PRIMARY KEY, tok text)",
            "CREATE VIEW active AS SELECT * FROM drivers WHERE status = 'active'",
        ]:
            c.execute(stmt)
        c.close()
        setup.close()

        conn = wrap_postgresql(psycopg2.connect(uri), db_name="pgauto")
        conn.autocommit = True

        # The whole setup: two opt-outs and the flag.
        conn.nedb.exclude = {"sessions"}
        conn.nedb.exclude_columns = {"password_hash"}
        conn.nedb.shadow_writes = True

        found = sorted(m.pattern for m in conn.nedb._mappings)
        check("every table discovered from information_schema, no registration",
              found == ["drivers", "drivers_archive", "trips"], f"{found}")
        check("a VIEW is not mirrored", "active" not in found, f"{found}")
        check("the REAL primary key is read from pg_index, not guessed",
              conn.nedb._pks.get("drivers") == "id", f"{conn.nedb._pks}")

        cur = conn.cursor()

        # A write with no RETURNING — the overwhelmingly common shape, and the
        # one that used to require a manual shadow_row() call.
        cur.execute("INSERT INTO drivers (name, status, password_hash) "
                    "VALUES (%s, %s, %s)", ("Bob", "active", "TOPSECRET"))
        check("a plain INSERT is mirrored with no shadow_row() call",
              len(conn.nedb.query("FROM drivers")) == 1,
              str(plain(conn.nedb.query("FROM drivers"))))
        check("…and the cursor still looks resultless to the caller",
              cur.fetchone() is None,
              "RETURNING was added by this layer; the caller must not see it")

        # The security property. NEDB cannot forget, so a secret must never
        # enter it in the first place.
        shadowed = conn.nedb.query("FROM drivers")
        check("an excluded COLUMN never enters the append-only chain",
              all("password_hash" not in r for r in shadowed),
              f"{shadowed} — NEDB cannot forget, so a secret must not get in")
        check("…while the host row is untouched", True, "NEDB never writes host tables")

        cur.executemany("INSERT INTO trips (driver, fare) VALUES (%s, %s)",
                        [("Ann", 10), ("Bob", 20)])
        fares = sorted(r.get("fare") for r in conn.nedb.query("FROM trips"))
        check("executemany mirrors every parameter set", fares == [10, 20], f"{fares}")

        cur.execute("INSERT INTO drivers_archive (name) VALUES (%s)", ("Old",))
        check("a write to drivers_archive lands in drivers_archive",
              len(conn.nedb.query("FROM drivers_archive")) == 1)
        check("…and NOT in drivers (no substring misrouting)",
              len(conn.nedb.query("FROM drivers")) == 1,
              str(plain(conn.nedb.query("FROM drivers"))))

        cur.execute("INSERT INTO sessions (tok) VALUES (%s)", ("s1",))
        check("an excluded TABLE mirrors nothing",
              conn.nedb.query("FROM sessions") == [])

        # UPDATE: a new version, so the prior value survives.
        cur.execute("UPDATE drivers SET status = %s WHERE name = %s", ("off", "Bob"))
        check("an UPDATE supersedes the same document",
              [r.get("status") for r in conn.nedb.query("FROM drivers")] == ["off"],
              str(plain(conn.nedb.query("FROM drivers"))))
        check("…and the prior value is readable at an earlier sequence",
              "active" in [r.get("status")
                           for r in conn.nedb.query("FROM drivers AS OF 0")],
              "this is the whole point of shadowing into NEDB")

        # DELETE → tombstone.
        cur.execute("DELETE FROM trips WHERE fare = %s", (10,))
        tomb = [r for r in conn.nedb.query("FROM trips") if r.get("_deleted")]
        check("a host DELETE becomes a tombstone in the shadow", len(tomb) == 1,
              str(plain(conn.nedb.query("FROM trips"))))

        # The caller's own RETURNING must reach the caller UNCHANGED…
        cur.execute("INSERT INTO trips (driver, fare) VALUES (%s, %s) "
                    "RETURNING id, fare", ("Cid", 99))
        got = cur.fetchone()
        check("the caller's own RETURNING still reaches the caller",
              got is not None and tuple(got)[1] == 99, f"{got}")
        # …and the mirrored row must still be COMPLETE, not just the two
        # columns the caller happened to ask for.
        cid = [r for r in conn.nedb.query("FROM trips") if r.get("fare") == 99]
        check("…while the MIRRORED row is widened to the whole row",
              cid and cid[0].get("driver") == "Cid",
              f"{cid} — a narrow RETURNING must not yield partial provenance")

        cur.execute("INSERT INTO trips (driver, fare) VALUES (%s, %s) RETURNING *",
                    ("Dee", 7))
        allrows = cur.fetchall()
        check("RETURNING * is served back in full", len(allrows) == 1, f"{allrows}")

        check("no accidental gaps, no partial rows, no swallowed errors",
              not conn.nedb.unmirrored_tables
              and not conn.nedb.partial_shadows
              and conn.nedb.shadow_errors == 0,
              f"unmirrored={conn.nedb.unmirrored_tables} "
              f"partial={conn.nedb.partial_shadows} "
              f"errors={conn.nedb.shadow_errors} ({conn.nedb.last_shadow_error})")
        check("the chain verifies after every kind of host write",
              conn.nedb.verify() is True)

        # A SELECT must pass through completely untouched.
        cur.execute("SELECT name, status FROM drivers ORDER BY id")
        rows = cur.fetchall()
        check("reads pass through unchanged", rows == [("Bob", "off")], f"{rows}")

        conn.close()
    finally:
        if srv is not None:
            try:
                srv.cleanup()
            except Exception:
                pass
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    print("\n" + "=" * 62)
    print("automatic shadowing — one flag, whole database")
    print("=" * 62)
    suite_target_table()
    suite_sqlite()
    suite_sqlite_optout()
    suite_postgres()

    print("\n" + "=" * 62)
    print(f"{len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("FAILED:", *FAIL, sep="\n  - ")
        sys.exit(1)
    print("`shadow_writes = True` now means what it says: every table, every")
    print("write path, no registration — and every gap is visible.")


if __name__ == "__main__":
    main()
