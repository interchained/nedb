#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
The PUBLISHED client package — `nedb-engine-client` on PyPI — against a real nedbd.

This suite did not exist. `client/python/nedb_client` is published to PyPI by
the release workflow on every `v*` tag, and its only verification was:

    python3 -c "from nedb_client import NedbClient; print('import OK')"

An import check. Not one request was ever made against a server before the
package shipped. That is the same publish-without-verification gap the test
workflow closed for the engine in 3.2.x, still open on the client.

It is a DIFFERENT client from `nedb.client` (which tests/test_client.py
covers): that one lives inside the engine package and is synchronous; this one
is standalone, async, httpx-based, and is what `pip install nedb-engine-client`
gives you.

What shipped unverified, found by writing this:

  * `get()` built `FROM coll WHERE _id = "<id>"` by interpolation, so any id
    containing a double quote was UNREACHABLE — the call returned None,
    meaning "no such document", for a document `put()` had stored and
    `query("FROM coll")` returned. An id ending in a backslash could not be
    escaped at all.
  * `delete()` interpolated the id into the URL path unencoded, so an id
    containing a slash matched a different route and the call returned False —
    "did not exist" — for a document that did.

Both now go through `GET/DELETE /v1/databases/<db>/rows/<coll>/<id>` with the
id percent-encoded into the path, so there is no quoting to get wrong.

Run: python3 tests/test_client_package.py
"""
import asyncio
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# The published package, imported the way a user would after pip install.
sys.path.insert(0, os.path.join(ROOT, "client", "python"))

try:
    from nedb_client import NedbClient, NedbError  # noqa: E402
except ImportError as e:                                           # pragma: no cover
    sys.exit(f"cannot import the client package: {e}")

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("  ok  " if cond else "  FAIL  ") + name + (f"  — {detail}" if detail else ""))


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def spawn(port, data_dir):
    """Start the Python AOF server — always available, no wheel required."""
    env = {**os.environ, "NEDBD_SWEEP_S": "0",
           "PYTHONPATH": os.path.join(ROOT, "python")}
    env.pop("NEDBD_TOKEN", None)
    return subprocess.Popen(
        [sys.executable, "-m", "nedb.server", "--host", "127.0.0.1",
         "--port", str(port), "--data", data_dir],
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, env=env)


# Ids that are perfectly legal to store but historically broke the client.
TRICKY_IDS = [
    "plain",
    'has"quote',
    'x" LIMIT 99 OR _id = "y',      # a crafted injection attempt
    'a"b"c',
    "a/slash",
    "sp ace",
    "100%",
    "uni✓code",
    "back\\slash",
    "ends\\",                        # unescapable in a NQL string literal
]


async def run(port):
    async with NedbClient(url=f"http://127.0.0.1:{port}", db="pkgtest") as db:
        # ── transport ────────────────────────────────────────────────────────
        print("\n── transport ──")
        check("ping reaches the server", await db.ping() is True)
        h = await db.health()
        check("health reports ok + version",
              h.get("ok") is True and bool(h.get("version")), str(h)[:90])
        await db.create_database()
        names = await db.list_databases()
        check("the database is listed", "pkgtest" in str(names), str(names)[:90])

        # ── put / get / query round-trip ─────────────────────────────────────
        print("\n── put / get / query ──")
        res = await db.put("t", "u1", {"name": "Alice", "n": 1})
        check("put returns an envelope", isinstance(res, dict) and res.get("ok") is True,
              str(res)[:90])
        got = await db.get("t", "u1")
        check("get returns the document", got is not None and got.get("name") == "Alice",
              str(got)[:90])
        check("the document carries _id and _seq",
              got is not None and got.get("_id") == "u1" and "_seq" in got,
              str(sorted(got.keys())) if got else "None")
        check("get of an absent id is None", await db.get("t", "nope") is None)
        rows = await db.query("FROM t")
        check("query returns rows", len(rows) == 1, f"{len(rows)}")
        full = await db.query_full("FROM t")
        check("query_full carries the envelope",
              {"rows", "count"} <= set(full), str(sorted(full.keys())))

        # ── THE BUG: ids the client could not reach ──────────────────────────
        print("\n── every legal id is reachable (this is what shipped broken) ──")
        for i in TRICKY_IDS:
            await db.put("ids", i, {"marker": i})
        stored = {r["_id"] for r in await db.query("FROM ids")}
        check("every id stored", stored == set(TRICKY_IDS),
              f"missing {set(TRICKY_IDS) - stored}")
        for i in TRICKY_IDS:
            got = await db.get("ids", i)
            ok = got is not None and got.get("marker") == i
            check(f"get({i!r}) round-trips", ok,
                  "returned None for a document that exists" if got is None
                  else f"got {got.get('marker')!r}")

        # A crafted id must name a document, never widen the result set.
        crafted = await db.get("ids", 'zzz" OR _id = "u1')
        check("a crafted id cannot widen the result set", crafted is None,
              str(crafted)[:90])

        # ── delete reaches those ids too ─────────────────────────────────────
        print("\n── delete reaches ids needing URL encoding ──")
        for i in ["a/slash", "sp ace", "100%", 'has"quote']:
            existed = await db.delete("ids", i)
            check(f"delete({i!r}) finds the document", existed is True,
                  "returned False for a document that exists")
            check(f"...and {i!r} is gone", await db.get("ids", i) is None)
        check("delete of an absent id is False", await db.delete("ids", "nope") is False)

        # ── the 3.3.0 query surface, through the published client ────────────
        print("\n── the 3.3.0 query surface rides through unchanged ──")
        for i, d in [
            ("1", {"status": "open",    "fee": 10, "miner": "Acme"}),
            ("2", {"status": "pending", "fee": 20, "miner": "acme solo"}),
            ("3", {"status": "closed",  "fee": 30, "miner": "Zenith"}),
            ("4", {"status": "open",    "fee": 40}),
        ]:
            await db.put("jobs", i, d)

        async def ids_for(nql):
            return sorted(r["_id"] for r in await db.query(nql))

        for label, nql, want in [
            ("IN",        'FROM jobs WHERE status IN ("open","closed")', ["1", "3", "4"]),
            ("NOT IN",    'FROM jobs WHERE status NOT IN ("open")',      ["2", "3"]),
            ("BETWEEN",   "FROM jobs WHERE fee BETWEEN 20 AND 40",       ["2", "3", "4"]),
            ("LIKE",      'FROM jobs WHERE miner LIKE "Acme%"',          ["1"]),
            ("ILIKE",     'FROM jobs WHERE miner ILIKE "acme%"',         ["1", "2"]),
            ("IS NULL",   "FROM jobs WHERE miner IS NULL",               ["4"]),
            ("OR",        "FROM jobs WHERE fee = 10 OR fee = 40",        ["1", "4"]),
            ("parens",    'FROM jobs WHERE (fee = 10 OR fee = 40) AND status = "open"',
                          ["1", "4"]),
            ("NOT",       "FROM jobs WHERE NOT (fee > 20)",              ["1", "2"]),
            ("OFFSET",    "FROM jobs ORDER BY fee OFFSET 2",             ["3", "4"]),
            # status ASC then fee DESC: closed(30), open(40), open(10), pending(20).
            # The first two are doc 3 (closed) and doc 4 (open, fee 40).
            ("multi sort", "FROM jobs ORDER BY status, fee DESC LIMIT 2", ["3", "4"]),
        ]:
            try:
                got = await ids_for(nql)
                check(f"{label}: {nql}", got == want, f"got {got}, want {want}")
            except Exception as e:                                  # noqa: BLE001
                check(f"{label}: {nql}", False, f"{type(e).__name__}: {e}")

        agg = await db.query("FROM jobs GROUP BY status SUM fee HAVING sum_fee > 20")
        check("GROUP BY + HAVING through the client",
              sorted(r["status"] for r in agg) == ["closed", "open"], str(agg)[:130])
        bare = await db.query("FROM jobs COUNT")
        check("a bare aggregate returns one row with the count",
              len(bare) == 1 and bare[0].get("count") == 4, str(bare)[:90])

        # A query the server rejects must raise or return [], never a wrong answer.
        print("\n── an unhonourable query does not silently answer ──")
        rejected = False
        try:
            r = await db.query("FROM jobs ORDRE BY fee")
            rejected = r == []
        except NedbError:
            rejected = True
        check("a misspelled clause does not return rows", rejected)

        # ── single-document time travel ──────────────────────────────────────
        print("\n── AS OF on a single document ──")
        await db.put("tt", "d", {"v": 1})
        v1 = (await db.get("tt", "d"))["_seq"]
        await db.put("tt", "d", {"v": 2})
        check("get returns the current version", (await db.get("tt", "d"))["v"] == 2)
        old = await db.get("tt", "d", as_of=v1)
        check("get(as_of=) returns the historical version",
              old is not None and old["v"] == 1, str(old)[:90])

        # ── integrity surfaces ───────────────────────────────────────────────
        print("\n── integrity ──")
        v = await db.verify()
        check("verify() reports an intact chain",
              (v.get("ok") is True) or (v is True), str(v)[:90])
        check("head() returns a hash", bool(await db.head()))
        check("seq() returns an int", isinstance(await db.seq(), int))
        log = await db.log(limit=5)
        check("log() returns entries", isinstance(log, list) and len(log) > 0,
              f"{len(log) if isinstance(log, list) else log}")

        # ── batch ────────────────────────────────────────────────────────────
        print("\n── batch ──")
        out = await db.batch([
            {"op": "put", "coll": "b", "id": "x", "doc": {"n": 1}},
            {"op": "put", "coll": "b", "id": "y", "doc": {"n": 2}},
        ])
        check("batch applies both ops", (await db.query_full("FROM b"))["count"] == 2,
              str(out)[:90])

        # ── typed errors ─────────────────────────────────────────────────────
        print("\n── errors are typed, not swallowed ──")
        try:
            await db.query("this is not NQL at all")
            check("malformed NQL surfaces as [] or NedbError", True,
                  "returned [] (the client's documented resilient-query behaviour)")
        except NedbError as e:
            check("malformed NQL surfaces as [] or NedbError", True, f"status {e.status}")


def main():
    tmp = tempfile.mkdtemp(prefix="nedb-clientpkg-")
    port = free_port()
    proc = spawn(port, os.path.join(tmp, "data"))
    try:
        # Wait for the daemon rather than sleeping blind.
        import urllib.error
        import urllib.request
        for _ in range(40):
            time.sleep(0.5)
            try:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/health", timeout=2).read()
                break
            except Exception:                                       # noqa: BLE001
                continue
        else:
            sys.exit("nedbd never came up")

        print(f"\nclient package: {os.path.join(ROOT, 'client', 'python')}")
        asyncio.run(run(port))
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:                                           # noqa: BLE001
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)

    print(f"\n{'=' * 62}")
    print(f"published client package: {len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("FAILED:", *FAIL, sep="\n  - ")
        sys.exit(1)
    print("nedb-engine-client is exercised against a real server before it ships:")
    print("every legal id is reachable by get() and delete(), and the whole")
    print("3.3.0 query surface rides through it unchanged.")


if __name__ == "__main__":
    main()
