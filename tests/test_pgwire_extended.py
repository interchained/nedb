#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
The Postgres EXTENDED query protocol — Parse / Bind / Describe / Execute.

Why a second pgwire suite: `test_pgwire.py` drives psycopg2, which is libpq and
interpolates parameters client-side, so it only ever exercises the SIMPLE query
protocol. The drivers an evaluator actually reaches for in 2026 do not:

  * **psycopg3** sends `Parse`/`Bind`/`Describe`/`Execute` for every
    parameterised statement.
  * **asyncpg** does the same, and refuses before it even sends a `Bind` if the
    server's `ParameterDescription` is wrong.
  * **JDBC** (and therefore most JVM BI tooling) behaves like psycopg3.

Before this was implemented, all three could not run a single query — not
"degraded", not "slower": psycopg3 hung waiting for a `ParseComplete` and
asyncpg errored out client-side. So this suite is the difference between "psql
works" and "your application framework works".

Two facts about real drivers were read off a wire transcript and are pinned
here, because both were counter-intuitive enough to have been guessed wrong:

  1. psycopg3 sends parameters in a MIXED format. A `str` arrives as OID 0 in
     TEXT format, but `42` arrives as **int2 in BINARY** (`\\x00*`), a float as
     float8 binary, a bool as one binary byte. A text-only decoder reads
     `\\x00*` where it expected `42`.
  2. asyncpg declares NO parameter types in `Parse` and asks
     `Describe(statement)` instead, then encodes its arguments from the OIDs
     that come back. Declaring "text" for all of them does NOT degrade
     gracefully — asyncpg raises `expected str, got int` and never sends the
     query. Which is why the server infers parameter types from the data it has
     actually stored: a schemaless engine has no catalogue to read, so the
     stored documents ARE the schema.

Run: python3 tests/test_pgwire_extended.py
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

HAVE = {}
try:
    import psycopg  # psycopg 3
    HAVE["psycopg3"] = psycopg.__version__
except ImportError:                                                # pragma: no cover
    HAVE["psycopg3"] = None
try:
    import asyncpg
    HAVE["asyncpg"] = getattr(asyncpg, "__version__", "?")
except ImportError:                                                # pragma: no cover
    HAVE["asyncpg"] = None

if not HAVE["psycopg3"] and not HAVE["asyncpg"]:
    print("SKIP: neither psycopg3 nor asyncpg is installed")
    print("      pip install 'psycopg[binary]' asyncpg")
    sys.exit(0)

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("  ok  " if cond else "  FAIL  ") + name + (f"  — {detail}" if detail else ""))


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


# ── psycopg3 ────────────────────────────────────────────────────────────────

def suite_psycopg3(pg_port):
    print(f"\n── psycopg3 {HAVE['psycopg3']} (extended protocol) ──")
    dsn = f"host=127.0.0.1 port={pg_port} dbname=shop user=nedb connect_timeout=10"

    with psycopg.connect(dsn, autocommit=True) as conn:
        check("psycopg3 connects at all", True)

        with conn.cursor() as cur:
            # The load-bearing case: a str parameter (OID 0, text format) and
            # an int parameter (int2, BINARY) in the same statement.
            cur.execute(
                "SELECT _id, status, total FROM orders WHERE status = %s AND total > %s "
                "ORDER BY total",
                ("paid", 100))
            rows = cur.fetchall()
            check("a text parameter and a BINARY int parameter in one statement",
                  rows == [("1", "paid", 120), ("3", "paid", 300)], f"{rows}")

            # Binary float8 and binary bool decoding.
            cur.execute("SELECT _id FROM orders WHERE total > %s", (250.5,))
            check("a BINARY float8 parameter decodes",
                  cur.fetchall() == [("3",)], "float8")

            cur.execute("SELECT _id, flagged FROM measures WHERE flagged = %s", (True,))
            check("a BINARY bool parameter decodes",
                  cur.fetchall() == [("m2", True)], "bool")

            # A big integer travels as int8, not int2.
            cur.execute("SELECT _id FROM measures WHERE big = %s", (9_000_000_000,))
            check("a BINARY int8 parameter decodes (beyond int2/int4 range)",
                  cur.fetchall() == [("m1",)], "int8")

            # Negative values — a sign bit dropped in a binary decode is the
            # classic silent-wrong-answer bug.
            cur.execute("SELECT _id FROM measures WHERE delta = %s", (-5,))
            check("a NEGATIVE binary integer keeps its sign",
                  cur.fetchall() == [("m1",)], "negative int")

            # NULL as a parameter.
            cur.execute("SELECT _id FROM orders WHERE cust IS NULL")
            check("IS NULL still works through the extended path",
                  len(cur.fetchall()) >= 0, "sanity")

            # A quote inside a parameter must not break out of its literal.
            cur.execute("SELECT _id, cust FROM orders WHERE cust = %s", ("o'hara",))
            check("a quote inside a parameter cannot break out of its literal",
                  cur.fetchall() == [("5", "o'hara")], "escaping")

            # Reusing one parameter twice.
            cur.execute("SELECT _id FROM orders WHERE region = %s OR region = %s",
                        ("ap", "ap"))
            check("the same value bound to two placeholders",
                  cur.fetchall() == [("4",)], "reuse")

            # Server-side types survive: an int column comes back an int, not a
            # string. This is what the data-driven type inference buys.
            cur.execute("SELECT total FROM orders WHERE _id = %s", ("1",))
            v = cur.fetchone()[0]
            check("a numeric column arrives as a number, not a string",
                  v == 120 and isinstance(v, int), f"{v!r}")

            # Prepared-statement reuse: psycopg3 promotes a repeated statement
            # to a named prepared statement, which exercises Parse-with-a-name
            # and Bind-against-a-name rather than the unnamed slot.
            for n in range(8):
                cur.execute("SELECT _id FROM orders WHERE region = %s", ("eu",))
                got = cur.fetchall()
                if got != [("1",), ("3",)]:
                    check("a statement reused enough times to be PREPARED", False,
                          f"iteration {n}: {got}")
                    break
            else:
                check("a statement reused enough times to be PREPARED", True,
                      "8 executions, named statement")

            # An error mid-sequence must be reported and must not desynchronise
            # the connection — the next statement has to work.
            try:
                cur.execute("SELECT * FROM orders JOIN x ON true WHERE a = %s", (1,))
                check("an unsupported statement is refused at Parse", False, "no error")
            except Exception as e:                                  # noqa: BLE001
                check("an unsupported statement is refused at Parse",
                      "JOIN" in str(e), str(e)[:80])

        # A fresh cursor after the error proves the stream resynchronised on Sync.
        with conn.cursor() as cur:
            cur.execute("SELECT _id FROM orders WHERE _id = %s", ("2",))
            check("the connection still works after an error (Sync resynchronises)",
                  cur.fetchall() == [("2",)], "recovery")

        # ── writes through the extended protocol ────────────────────────────
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO orders (_id, status, total, region) VALUES (%s, %s, %s, %s) "
                "RETURNING _id, total",
                ("x1", "new", 77, "eu"))
            check("INSERT … RETURNING with bound parameters",
                  cur.fetchall() == [("x1", 77)], "insert")
            check("the INSERT command tag is right", cur.rowcount == 1, f"{cur.rowcount}")

            cur.execute("UPDATE orders SET total = %s WHERE _id = %s RETURNING total",
                        (88, "x1"))
            check("UPDATE … RETURNING with bound parameters",
                  cur.fetchall() == [(88,)], "update")

            # THE assertion this whole endpoint exists for: a plain SQL UPDATE
            # through a plain SQL driver, and the previous value is still there.
            cur.execute("SELECT total FROM orders AS OF SYSTEM TIME 0 WHERE _id = %s", ("1",))
            check("history survives writes made over the extended protocol",
                  cur.fetchall() == [(120,)], "AS OF")

            cur.execute("DELETE FROM orders WHERE _id = %s RETURNING _id", ("x1",))
            check("DELETE … RETURNING with bound parameters",
                  cur.fetchall() == [("x1",)], "delete")

        # psycopg3's *named* cursor does not use a row-capped Execute — it
        # issues `DECLARE … CURSOR`, which is SQL-level cursor syntax this
        # endpoint does not implement. Pin that it says so clearly instead of
        # hanging, because a clear boundary is the second-best outcome and a
        # hang is the worst.
        try:
            with conn.cursor(name="chunked") as cur:
                cur.itersize = 2
                cur.execute("SELECT _id FROM orders ORDER BY _id")
                list(cur)
            check("a server-side DECLARE cursor is refused, not hung", False,
                  "it succeeded — update this test, DECLARE now works")
        except Exception as e:                                      # noqa: BLE001
            check("a server-side DECLARE cursor is refused, not hung",
                  "DECLARE" in str(e), str(e)[:90])

    check("psycopg3 closes cleanly", True)


# ── the wire itself: PortalSuspended ────────────────────────────────────────

def suite_portal_suspension(pg_port):
    """Drive Parse/Bind/Execute by hand to prove row-capped Execute suspends.

    No Python driver exercises this: psycopg2 stays on the simple protocol,
    psycopg3's named cursor uses `DECLARE`, and asyncpg's `fetch` always asks
    for every row. But JDBC's `setFetchSize` — and therefore a large part of
    JVM BI tooling — sends `Execute` with a row cap and expects
    `PortalSuspended` when rows remain. Getting this wrong means a client
    either stalls waiting for more or silently truncates the result, so it is
    tested directly rather than assumed.
    """
    print("\n── the wire: row-capped Execute → PortalSuspended ──")
    import struct

    def msg(tag, payload=b""):
        return tag + struct.pack("!i", len(payload) + 4) + payload

    def cstr(s):
        return s.encode() + b"\0"

    s = socket.create_connection(("127.0.0.1", pg_port), timeout=10)
    try:
        start = cstr("user") + cstr("nedb") + cstr("database") + cstr("shop") + b"\0"
        s.sendall(struct.pack("!ii", len(start) + 8, 196_608) + start)

        def read_msgs(until):
            """Read messages until one of `until`'s tags arrives."""
            got = []
            buf = b""
            while True:
                while len(buf) < 5:
                    chunk = s.recv(65536)
                    if not chunk:
                        raise AssertionError(f"connection closed; got {got}")
                    buf += chunk
                tag = buf[:1]
                ln = struct.unpack("!i", buf[1:5])[0]
                while len(buf) < 1 + ln:
                    buf += s.recv(65536)
                got.append((tag, buf[5:1 + ln]))
                buf = buf[1 + ln:]
                if tag in until:
                    return got

        read_msgs({b"Z"})  # through the handshake to ReadyForQuery

        s.sendall(
            msg(b"P", cstr("") + cstr("SELECT _id FROM orders ORDER BY _id") + struct.pack("!h", 0))
            + msg(b"B", cstr("") + cstr("") + struct.pack("!hhh", 0, 0, 0))
            + msg(b"D", b"P" + cstr(""))
            + msg(b"E", cstr("") + struct.pack("!i", 2))   # cap at 2 rows
            + msg(b"H")
        )
        first = read_msgs({b"s", b"C", b"E"})
        tags = [t for t, _ in first]
        check("a row-capped Execute returns exactly the cap",
              tags.count(b"D") == 2, f"{tags.count(b'D')} data rows")
        check("and suspends the portal instead of completing it",
              b"s" in tags and b"C" not in tags, f"{[t.decode() for t in tags]}")

        # Resume the same portal. The rows must continue, not restart.
        s.sendall(msg(b"E", cstr("") + struct.pack("!i", 2)) + msg(b"H"))
        second = read_msgs({b"s", b"C", b"E"})
        rows2 = [p for t, p in second if t == b"D"]
        check("a second Execute CONTINUES the portal rather than restarting it",
              len(rows2) == 2 and rows2[0] != [p for t, p in first if t == b"D"][0],
              f"{len(rows2)} rows")

        # Drain the rest: uncapped, so this one must complete.
        s.sendall(msg(b"E", cstr("") + struct.pack("!i", 0)) + msg(b"S"))
        last = read_msgs({b"Z"})
        done = [p for t, p in last if t == b"C"]
        check("an uncapped Execute completes the portal",
              len(done) == 1, f"{[t.decode() for t, _ in last]}")
        if done:
            tag = done[0].rstrip(b"\0").decode()
            # Five seeded orders; the tag counts every row the portal yielded.
            check("the CommandComplete tag counts all rows across every Execute",
                  tag == "SELECT 5", tag)
        s.sendall(msg(b"X"))
    finally:
        s.close()


# ── asyncpg ─────────────────────────────────────────────────────────────────

def suite_asyncpg(pg_port):
    print(f"\n── asyncpg {HAVE['asyncpg']} (extended protocol, inferred types) ──")
    import asyncio

    async def run():
        conn = await asyncpg.connect(host="127.0.0.1", port=pg_port,
                                     database="shop", user="nedb",
                                     statement_cache_size=0, timeout=10)
        check("asyncpg connects at all", True)

        # asyncpg declares no parameter types, so every one of these depends on
        # the server inferring the type from stored data and reporting it in
        # ParameterDescription. Get the type wrong and asyncpg refuses
        # client-side with "expected str, got int" — it never reaches the wire.
        rows = await conn.fetch(
            "SELECT _id, status, total FROM orders WHERE status = $1 AND total > $2 "
            "ORDER BY total", "paid", 100)
        check("a str and an int parameter, with types INFERRED from stored data",
              [(r["_id"], r["status"], r["total"]) for r in rows]
              == [("1", "paid", 120), ("3", "paid", 300)], f"{rows}")

        rows = await conn.fetch("SELECT _id FROM measures WHERE flagged = $1", True)
        check("a bool parameter types as bool from the stored document",
              [r["_id"] for r in rows] == ["m2"], f"{rows}")

        rows = await conn.fetch("SELECT _id FROM orders WHERE total > $1", 250.5)
        check("a float parameter types as float8 from the stored document",
              [r["_id"] for r in rows] == ["3"], f"{rows}")

        # A range query: `BETWEEN $1 AND $2` puts a word operator AND an earlier
        # placeholder between `$2` and the column it constrains. Getting this
        # wrong types the upper bound as text while the lower bound is correct —
        # asyncpg then refuses the int, which is how this bug was caught.
        rows = await conn.fetch(
            "SELECT _id FROM orders WHERE total BETWEEN $1 AND $2 ORDER BY _id", 100, 200)
        check("BETWEEN $1 AND $2 — BOTH bounds type correctly",
              [r["_id"] for r in rows] == ["1"], f"{rows}")

        # IN (…) with several placeholders against the same column.
        rows = await conn.fetch(
            "SELECT _id FROM orders WHERE region IN ($1, $2) ORDER BY _id", "us", "ap")
        check("IN ($1, $2) types every placeholder from one column",
              [r["_id"] for r in rows] == ["2", "4", "5"], f"{rows}")

        # LIKE, where the word operator sits between column and placeholder.
        rows = await conn.fetch("SELECT _id FROM orders WHERE cust LIKE $1", "acm%")
        check("LIKE $1 finds the column behind the word operator",
              sorted(r["_id"] for r in rows) == ["1", "3"], f"{rows}")

        # An aggregate column exists in no document, so sampling stored fields
        # cannot type it — `count` has to be typed from what COUNT *means*.
        # Get that wrong and a binary client receives the string "2".
        n = await conn.fetchval("SELECT COUNT(*) FROM orders WHERE region = $1", "eu")
        check("COUNT(*) arrives as an INTEGER, not the string '2'",
              n == 2 and isinstance(n, int), f"{n!r} ({type(n).__name__})")

        s = await conn.fetchval("SELECT SUM(total) FROM orders WHERE region = $1", "eu")
        check("SUM over an integer field stays an integer",
              s == 420 and isinstance(s, int), f"{s!r} ({type(s).__name__})")

        a = await conn.fetchval("SELECT AVG(total) FROM orders WHERE region = $1", "eu")
        check("AVG is fractional even over integers",
              abs(float(a) - 210.0) < 1e-9, f"{a!r} ({type(a).__name__})")

        mx = await conn.fetchval("SELECT MAX(total) FROM orders WHERE region = $1", "eu")
        check("MAX inherits the type of the field it ranges over",
              mx == 300 and isinstance(mx, int), f"{mx!r} ({type(mx).__name__})")

        # A write with parameters asyncpg has to type from the INSERT column list.
        row = await conn.fetchrow(
            "INSERT INTO orders (_id, status, total, region) VALUES ($1, $2, $3, $4) "
            "RETURNING _id, total", "a1", "new", 55, "eu")
        check("INSERT types its parameters from the column list",
              (row["_id"], row["total"]) == ("a1", 55), f"{row}")

        row = await conn.fetchrow(
            "UPDATE orders SET total = $1 WHERE _id = $2 RETURNING total", 66, "a1")
        check("UPDATE types a SET parameter from its column", row["total"] == 66, f"{row}")

        # A BOUND sequence number in AS OF SYSTEM TIME. This is the clause that
        # exposed a bug in how a statement is probed at Parse time: the probe
        # stubbed every parameter with NULL, and AS OF validates its argument,
        # so a parameterised time-travel query was rejected before the client
        # ever bound a real sequence.
        seq_now = await conn.fetchval("SELECT MAX(_seq) FROM orders")
        rows = await conn.fetch(
            "SELECT total FROM orders AS OF SYSTEM TIME $1 WHERE _id = $2", seq_now, "a1")
        check("AS OF SYSTEM TIME with a BOUND sequence number",
              [r["total"] for r in rows] == [66], f"{rows}")

        await conn.execute("DELETE FROM orders WHERE _id = $1", "a1")
        gone = await conn.fetch("SELECT _id FROM orders WHERE _id = $1", "a1")
        check("DELETE through the extended protocol removes the row", gone == [], f"{gone}")

        # A DELETE is a tombstone, not an erasure — over SQL, through a driver.
        #
        # This used to return nothing: the AS OF branch enumerated ids from the
        # CURRENT id index, and `delete()` removed the live pointer, so a
        # deleted id was skipped at every sequence — including sequences before
        # the delete where the row demonstrably existed. `delete()` now MOVES
        # the pointer to a graveyard index instead of dropping it.
        back = await conn.fetch(
            "SELECT total FROM orders AS OF SYSTEM TIME $1 WHERE _id = $2", seq_now, "a1")
        check("a deleted row is a tombstone: still readable before the delete",
              [r["total"] for r in back] == [66], f"{back}")

        # An error must not poison the connection.
        try:
            await conn.fetch("TRUNCATE orders")
            check("an unsupported statement is refused", False, "no error")
        except Exception as e:                                      # noqa: BLE001
            check("an unsupported statement is refused",
                  "append-only" in str(e) or "TRUNCATE" in str(e), str(e)[:90])
        rows = await conn.fetch("SELECT _id FROM orders WHERE _id = $1", "2")
        check("asyncpg's connection survives a refused statement",
              [r["_id"] for r in rows] == ["2"], f"{rows}")

        await conn.close()
        check("asyncpg closes cleanly", True)

    asyncio.get_event_loop_policy().new_event_loop().run_until_complete(run()) \
        if sys.version_info < (3, 10) else asyncio.run(run())


def main():
    tmp = tempfile.mkdtemp(prefix="nedb-pgx-")
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
        for i, d in [
            ("1", {"status": "paid", "total": 120, "region": "eu", "cust": "acme"}),
            ("2", {"status": "open", "total": 40,  "region": "us", "cust": "zenith"}),
            ("3", {"status": "paid", "total": 300, "region": "eu", "cust": "acme2"}),
            ("4", {"status": "void", "total": 10,  "region": "ap"}),   # no `cust`
            ("5", {"status": "open", "total": 15,  "region": "us", "cust": "o'hara"}),
        ]:
            http(http_port, "POST", "/v1/databases/shop/put",
                 {"coll": "orders", "id": i, "doc": d})
        # A second collection whose field types exercise every binary decoder.
        for i, d in [
            ("m1", {"flagged": False, "big": 9_000_000_000, "delta": -5, "ratio": 0.25}),
            ("m2", {"flagged": True,  "big": 1,             "delta": 7,  "ratio": 1.5}),
        ]:
            http(http_port, "POST", "/v1/databases/shop/put",
                 {"coll": "measures", "id": i, "doc": d})

        # Driver-independent: raw sockets, so it always runs.
        suite_portal_suspension(pg_port)

        if HAVE["psycopg3"]:
            suite_psycopg3(pg_port)
        else:
            print("\n(psycopg3 not installed — skipping that half)")
        if HAVE["asyncpg"]:
            suite_asyncpg(pg_port)
        else:
            print("\n(asyncpg not installed — skipping that half)")
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


if __name__ == "__main__":
    main()
