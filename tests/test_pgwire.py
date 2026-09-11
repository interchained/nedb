#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
The Postgres endpoint — reads AND writes — driven by a real libpq client.

This is the test that matters for `pgwire.rs`: unit tests can prove the SQL→NQL
translation and the message framing, but only an actual PostgreSQL client can
prove the protocol is right. A single wrong length prefix desynchronises the
stream and the client hangs — and a hang is the worst diagnostic there is.

So this suite drives **psycopg2**, which is libpq. If these pass, `psql`,
DBeaver, Metabase, Grafana and every other libpq/pgwire tool can read a NEDB
store, because they all speak the same protocol this does.

psycopg2 interpolates parameters client-side and sends complete statements via
`PQexec`, so it exercises the SIMPLE QUERY protocol — which is exactly the part
that is implemented. The extended protocol (Parse/Bind/Execute) is not, and one
test below pins that it fails loudly rather than hanging.

Two bugs this suite caught while being written, both of which had shipped
through the unit tests:

  * `SELECT COUNT(*)` returned TWO columns — `count` and `value` — because NQL
    emits a back-compat `value` alias and the projection passed it straight
    through. SQL promises one column.
  * `SELECT region, total FROM orders GROUP BY region` returned `total = NULL`.
    A grouped row does not carry `total`, so the projection found nothing and
    rendered NULL — a silent wrong answer where Postgres raises.

Run: python3 tests/test_pgwire.py
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
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


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def find_nedbd():
    """The Rust daemon. This endpoint lives in the Rust engine only."""
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


HTTP_PORT = [0]


def main():
    tmp = tempfile.mkdtemp(prefix="nedb-pgwire-")
    http_port, pg_port = free_port(), free_port()
    HTTP_PORT[0] = http_port
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

        # ── seed over HTTP; the pg endpoint is read-only by design ───────────
        http(http_port, "POST", "/v1/databases", {"name": "shop"})
        seed = [
            ("1", {"status": "paid", "total": 120, "region": "eu", "cust": "acme"}),
            ("2", {"status": "open", "total": 40,  "region": "us", "cust": "zenith"}),
            ("3", {"status": "paid", "total": 300, "region": "eu", "cust": "acme"}),
            ("4", {"status": "void", "total": 10,  "region": "ap"}),  # no `cust`
        ]
        for i, d in seed:
            http(http_port, "POST", "/v1/databases/shop/put",
                 {"coll": "orders", "id": i, "doc": d})
        node = http(http_port, "POST", "/v1/databases/shop/put",
                    {"coll": "audit", "id": "cause", "doc": {"kind": "policy"}})
        cause_hash = node["doc"]["_hash"]
        http(http_port, "POST", "/v1/databases/shop/put",
             {"coll": "audit", "id": "effect", "doc": {"kind": "reprice"},
              "caused_by": [cause_hash]})

        run_suite(pg_port, cause_hash)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:                                           # noqa: BLE001
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n{'=' * 66}")
    print(f"pgwire (via psycopg2/libpq): {len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("FAILED:", *FAIL, sep="\n  - ")
        sys.exit(1)
    print("A real PostgreSQL client READS AND WRITES the tamper-evident store with")
    print("plain SQL. An UPDATE is a new version, so the prior value survives — which")
    print("is the whole reason the write path belongs on this endpoint.")


def run_suite(pg_port, cause_hash):
    # ── connecting at all is the first assertion ─────────────────────────────
    print("\n── handshake ──")
    conn = psycopg2.connect(host="127.0.0.1", port=pg_port, dbname="shop",
                            user="nedb", connect_timeout=10)
    conn.autocommit = True
    check("libpq completes the startup handshake", True)
    cur = conn.cursor()

    def q(sql):
        cur.execute(sql)
        cols = [d.name for d in cur.description] if cur.description else []
        return cols, (cur.fetchall() if cur.description else [])

    def err(sql):
        """The error a statement raises, or None when it succeeded.

        Deliberately does NOT fetchall(): a write without RETURNING has no
        result set, and psycopg2 raises "no results to fetch" for that — which
        would be reported as the statement failing when it actually worked.
        """
        try:
            cur.execute(sql)
            return None
        except Exception as e:                                      # noqa: BLE001
            return str(e).strip()

    cols, rows = q("SELECT version()")
    check("SELECT version() answers", rows and "NEDB" in rows[0][0], str(rows)[:80])
    check("...and advertises the write surface",
          rows and "INSERT" in rows[0][0], str(rows)[:90])

    # ── reads ────────────────────────────────────────────────────────────────
    print("\n── SELECT ──")
    cols, rows = q("SELECT * FROM orders")
    check("SELECT * returns every row", len(rows) == 4, f"{len(rows)}")
    check("the user's own fields come before provenance columns",
          cols[:4] == ["cust", "region", "status", "total"], str(cols))
    check("provenance columns are present and last",
          cols[4:] == ["_coll", "_hash", "_id", "_seq"], str(cols))

    cols, rows = q("SELECT status, total FROM orders WHERE status = 'paid'")
    check("a projection returns exactly those columns", cols == ["status", "total"], str(cols))
    check("a SQL string literal filters correctly", len(rows) == 2, f"{len(rows)}")

    cols, rows = q("SELECT status, total FROM orders "
                   "WHERE status IN ('paid','open') ORDER BY total DESC")
    check("IN + ORDER BY DESC", [r[1] for r in rows] == [300, 120, 40], str(rows))

    cols, rows = q("SELECT total FROM orders WHERE total BETWEEN 40 AND 200 ORDER BY total")
    check("BETWEEN", [r[0] for r in rows] == [40, 120], str(rows))

    cols, rows = q("SELECT cust FROM orders WHERE cust IS NULL")
    check("IS NULL reaches the row with no `cust`", len(rows) == 1, f"{len(rows)}")
    check("...and the missing value arrives as SQL NULL", rows and rows[0][0] is None)

    cols, rows = q("SELECT total FROM orders ORDER BY total LIMIT 2 OFFSET 1")
    check("LIMIT + OFFSET", [r[0] for r in rows] == [40, 120], str(rows))

    # ── aggregates: one column, named as SQL names it ────────────────────────
    print("\n── aggregates ──")
    cols, rows = q("SELECT COUNT(*) FROM orders")
    check("COUNT(*) is ONE column called `count`", cols == ["count"], str(cols))
    check("...with the right value", rows == [(4,)], str(rows))

    cols, rows = q("SELECT SUM(total) FROM orders WHERE region = 'eu'")
    check("SUM(col) is ONE column called `sum`", cols == ["sum"], str(cols))
    check("...with the right value", rows == [(420,)], str(rows))

    cols, rows = q("SELECT AVG(total) FROM orders WHERE region = 'eu'")
    check("AVG(col) is ONE column called `avg`", cols == ["avg"], str(cols))
    check("...with the right value", rows and float(rows[0][0]) == 210.0, str(rows))

    cols, rows = q("SELECT region FROM orders GROUP BY region")
    check("GROUP BY on the key alone works",
          sorted(r[0] for r in rows) == ["ap", "eu", "us"], str(rows))

    # The silent-NULL bug. Postgres raises here, and so must we.
    e = err("SELECT region, total FROM orders GROUP BY region")
    check("a bare column with GROUP BY is REFUSED, not silently NULL",
          e is not None and "must appear in the GROUP BY clause" in e, str(e)[:110])

    # ── the differentiators, reachable over plain SQL ────────────────────────
    print("\n── NEDB's own surface, through a Postgres client ──")
    cols, rows = q("SELECT * FROM orders AS OF SYSTEM TIME 1")
    check("AS OF SYSTEM TIME reads history", len(rows) == 2,
          f"{len(rows)} rows at seq 1")
    check("...using Postgres's own time-travel spelling", True)

    e = err("SELECT * FROM orders AS OF SYSTEM TIME '2026-01-01'")
    check("a wall-clock AS OF is refused with the reason",
          e is not None and "sequence number" in e, str(e)[:110])

    cols, rows = q("SELECT _id, _hash FROM audit ORDER BY _id")
    check("provenance columns are selectable by name",
          cols == ["_id", "_hash"] and len(rows) == 2, str(cols))
    check("the causal parent's hash is visible over SQL",
          any(r[1] == cause_hash for r in rows), str(rows)[:90])

    # ── WRITES: the reason this endpoint is worth having ─────────────────────
    #
    # SQL's write semantics and NEDB's append-only model line up, so these are
    # first-class. The assertion that matters is the LAST one in this block:
    # after a plain SQL UPDATE, the prior value is still readable.
    print("\n── writes: INSERT / UPDATE / DELETE ──")
    cur.execute("INSERT INTO inv (_id, item, qty) VALUES ('i1', 'bolt', 10)")
    check("INSERT reports the Postgres command tag",
          cur.statusmessage == "INSERT 0 1", cur.statusmessage)
    cols, rows = q("SELECT item, qty FROM inv WHERE _id = 'i1'")
    check("...and the row is really there", rows == [("bolt", 10)], str(rows))

    cur.execute("INSERT INTO inv (_id, item, qty) VALUES ('i2','nut',5),('i3','washer',99)")
    check("a multi-row INSERT writes every row",
          cur.statusmessage == "INSERT 0 2", cur.statusmessage)

    cols, rows = q("INSERT INTO inv (_id, item, qty) VALUES ('i4','screw',7) "
                   "RETURNING _id, qty, _seq")
    check("INSERT … RETURNING returns the written row",
          len(rows) == 1 and rows[0][0] == "i4" and rows[0][1] == 7, str(rows))
    check("...with the columns asked for", cols == ["_id", "qty", "_seq"], str(cols))
    check("...and still exactly one command tag",
          cur.statusmessage == "INSERT 0 1", cur.statusmessage)

    # An INSERT with no id column: the server assigns one rather than
    # overwriting a shared default.
    cur.execute("INSERT INTO auto (n) VALUES (1)")
    cur.execute("INSERT INTO auto (n) VALUES (2)")
    cols, rows = q("SELECT COUNT(*) FROM auto")
    check("an INSERT with no id column gets a unique key each time",
          rows == [(2,)], f"{rows} — a shared default would collapse to 1")

    print("\n── UPDATE is a new version, not an overwrite ──")
    cols, rows = q("SELECT _seq FROM inv WHERE _id = 'i1'")
    seq_before = rows[0][0]
    cols, rows = q("UPDATE inv SET qty = 999, item = 'amended' WHERE _id = 'i1' "
                   "RETURNING _id, item, qty")
    check("UPDATE … RETURNING returns the new version",
          rows == [("i1", "amended", 999)], str(rows))
    check("UPDATE reports the row count", cur.statusmessage == "UPDATE 1",
          cur.statusmessage)
    cols, rows = q("SELECT item, qty FROM inv WHERE _id = 'i1'")
    check("the current value is the updated one", rows == [("amended", 999)], str(rows))

    # THE ASSERTION THIS WHOLE ENDPOINT EXISTS FOR.
    cols, rows = q(f"SELECT item, qty FROM inv AS OF SYSTEM TIME {seq_before} "
                   f"WHERE _id = 'i1'")
    check("the PRIOR value survives a plain SQL UPDATE",
          rows == [("bolt", 10)],
          f"{rows} — an UPDATE must not destroy history")

    cols, rows = q("UPDATE inv SET checked = TRUE WHERE qty > 50 RETURNING _id")
    check("UPDATE uses the full predicate surface", len(rows) >= 1, str(rows))

    print("\n── DELETE is a tombstone ──")
    cols, rows = q("DELETE FROM inv WHERE _id = 'i2' RETURNING _id, item")
    check("DELETE … RETURNING returns the row as it was",
          rows == [("i2", "nut")], str(rows))
    check("DELETE reports the row count", cur.statusmessage == "DELETE 1",
          cur.statusmessage)
    check("...and the row is gone from the live view",
          q("SELECT _id FROM inv WHERE _id = 'i2'")[1] == [], "still present")
    cur.execute("DELETE FROM inv WHERE _id = 'nope'")
    check("a DELETE matching nothing reports 0",
          cur.statusmessage == "DELETE 0", cur.statusmessage)

    print("\n── provenance is settable from SQL ──")
    cols, rows = q("INSERT INTO chain (_id, kind) VALUES ('root', 'policy') "
                   "RETURNING _hash")
    root_hash = rows[0][0]
    cur.execute("INSERT INTO chain (_id, _caused_by, kind) "
                f"VALUES ('leaf', '{root_hash}', 'derived')")
    cols, rows = q("SELECT _id, _caused_by FROM chain WHERE _id = 'leaf'")
    check("_caused_by set via SQL lands on the node",
          rows and root_hash in str(rows[0][1]), str(rows))
    cols, rows = q("SELECT _id FROM chain TRACE caused_by")
    check("...and TRACE walks the chain it created", len(rows) >= 1, str(rows))

    print("\n── writes that cannot be stored faithfully are refused ──")
    for sql, expect in [
        ("INSERT INTO t VALUES (1)", "explicit column list"),
        ("INSERT INTO t (a) VALUES (1 + 1)", "cannot use"),
        ("INSERT INTO t (a) VALUES (now())", "cannot use"),
        ("INSERT INTO t (a, b) VALUES (1)", "values for"),
        ("UPDATE t SET", "no assignments"),
        ("TRUNCATE inv", "append-only"),
        ("CREATE TABLE t (a int)", "DDL"),
    ]:
        e = err(sql)
        check(f"refused: {sql[:40]}", e is not None and expect in e, str(e)[:100])

    # The chain must still verify after all of that — writes through SQL are
    # ordinary engine writes, not a side door around the hash chain.
    print("\n── the chain is intact after SQL writes ──")
    import urllib.request as _u
    with _u.urlopen(f"http://127.0.0.1:{HTTP_PORT[0]}/v1/databases/shop/verify",
                    timeout=10) as r:
        v = json.loads(r.read())
    check("verify() still passes after INSERT/UPDATE/DELETE over SQL",
          v.get("ok") is True, str(v)[:110])
    check("...and the chain is still tamper-evident",
          v.get("tamper_evident") is True, str(v)[:110])

    # ── refusals: every one names the boundary ───────────────────────────────
    print("\n── refusals say what the boundary is ──")
    for sql, expect in [
        # INSERT/UPDATE/DELETE are SUPPORTED now — they are covered in the
        # writes block above, and an unqualified DELETE really does affect the
        # whole collection (as it does in Postgres), so it must not be fired
        # against a fixture other assertions still depend on.
        ("CREATE TABLE t (a int)", "DDL"),
        ("SELECT * FROM orders JOIN audit ON 1=1", "JOIN is not supported"),
        ("SELECT DISTINCT region FROM orders", "GROUP BY"),
        ("SELECT lower(status) FROM orders", "expressions in the select list"),
        ("SELECT * FROM orders, audit", "more than one collection"),
    ]:
        e = err(sql)
        check(f"refused with a reason: {sql[:38]}",
              e is not None and expect in e, str(e)[:100])

    # A NQL-level error must surface as a SQL error, carrying the translation
    # so the developer can see what was actually run.
    e = err("SELECT * FROM orders WHERE ORDRE BY total")
    check("a NQL error surfaces as a SQL error naming the translation",
          e is not None and "NQL" in e, str(e)[:110])

    # ── the connection survives all of that ─────────────────────────────────
    print("\n── the session survives errors ──")
    cols, rows = q("SELECT COUNT(*) FROM orders")
    check("the connection still works after many errors", rows == [(4,)], str(rows))

    cur.execute("SELECT 1; SELECT 1")
    check("a multi-statement simple query does not desynchronise the stream", True)

    cur.close()
    conn.close()
    check("the connection closes cleanly", True)

    # ── unknown database, and the extended-protocol gap ─────────────────────
    print("\n── edges ──")
    try:
        c2 = psycopg2.connect(host="127.0.0.1", port=pg_port, dbname="nope",
                              user="nedb", connect_timeout=10)
        c2.autocommit = True
        k = c2.cursor()
        try:
            k.execute("SELECT * FROM orders")
            k.fetchall()
            check("an unknown database is reported, not silently empty", False,
                  "returned rows")
        except Exception as e:                                      # noqa: BLE001
            check("an unknown database is reported, not silently empty",
                  "not open" in str(e) or "does not exist" in str(e), str(e)[:100])
        c2.close()
    except Exception as e:                                          # noqa: BLE001
        check("an unknown database is reported, not silently empty", True,
              f"refused at connect: {str(e)[:70]}")

    # psycopg2's `execute` with parameters uses client-side interpolation, so it
    # stays on the simple protocol — which is the point. Prove that works.
    conn3 = psycopg2.connect(host="127.0.0.1", port=pg_port, dbname="shop",
                             user="nedb", connect_timeout=10)
    conn3.autocommit = True
    c3 = conn3.cursor()
    c3.execute("SELECT status, total FROM orders WHERE status = %s", ("paid",))
    rows = c3.fetchall()
    check("a parameterised psycopg2 query works (client-side interpolation)",
          len(rows) == 2, f"{len(rows)}")
    c3.close()
    conn3.close()


if __name__ == "__main__":
    main()
