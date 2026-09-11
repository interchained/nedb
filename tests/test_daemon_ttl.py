#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""Daemon TTL: ttl_s over HTTP /put and /batch(tx), the /sweep route, the
background sweeper, and time-travel preservation of expired docs.

Engine TTL was lazy-only (fires on as_of=None gets), but daemon reads are
snapshot-pinned — so without an active sweep, expired docs lingered forever
over HTTP. This suite proves the daemon-side enforcement end-to-end.
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

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"))

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ok  {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name} {detail}")


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _req(port, method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


port = _free_port()
data_dir = tempfile.mkdtemp()
env = {**os.environ,
       "PYTHONPATH": os.path.join(os.path.dirname(
           os.path.dirname(os.path.abspath(__file__))), "python"),
       "NEDBD_SWEEP_S": "1"}  # fast sweeper for the background test
daemon = subprocess.Popen(
    [sys.executable, "-m", "nedb.server", "--host", "127.0.0.1",
     "--port", str(port), "--data", data_dir],
    stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, env=env)
deadline = time.time() + 10
while time.time() < deadline:
    try:
        if _req(port, "GET", "/health")[0] == 200:
            break
    except Exception:
        time.sleep(0.2)

try:
    _req(port, "POST", "/v1/databases", {"name": "ttldb"})

    def rows(nql):
        return _req(port, "POST", "/v1/databases/ttldb/query",
                    {"nql": nql})[1]["rows"]

    # ── 1. ttl_s forwarded on /put ───────────────────────────────────────────
    st, out = _req(port, "POST", "/v1/databases/ttldb/put",
                   {"coll": "sessions", "id": "s1",
                    "doc": {"token": "tok_abc"}, "ttl_s": 2.0})
    got = rows('FROM sessions WHERE _id = "s1"')
    check("put forwards ttl_s (doc has _expires_at)",
          st == 200 and got and "_expires_at" in got[0],
          f"doc={got[0] if got else None}")

    # ── 2. ttl_s inside an atomic tx (/batch) ────────────────────────────────
    st, out = _req(port, "POST", "/v1/databases/ttldb/batch", {
        "ops": [{"op": "put", "coll": "invite_tokens", "id": "tok1",
                 "doc": {"seat_id": "seat1"}, "ttl_s": 2.0}]})
    got = rows('FROM invite_tokens WHERE _id = "tok1"')
    check("tx forwards ttl_s", st == 200 and got and "_expires_at" in got[0])

    # ── 3. permanent doc control (must survive every sweep) ─────────────────
    _req(port, "POST", "/v1/databases/ttldb/put",
         {"coll": "sessions", "id": "forever", "doc": {"token": "keep"}})

    # snapshot the seq while both TTL docs are alive (for time-travel check)
    seq_alive = _req(port, "POST", "/v1/databases/ttldb/query",
                     {"nql": "FROM sessions"})[1]["seq"]

    # ── 4. manual /sweep before expiry: nothing swept ────────────────────────
    st, out = _req(port, "POST", "/v1/databases/ttldb/sweep")
    check("sweep before expiry sweeps 0",
          st == 200 and out["swept"] == 0, f"swept={out.get('swept')}")

    # ── 5. manual /sweep after expiry deletes both TTL docs ─────────────────
    time.sleep(2.2)
    st, out = _req(port, "POST", "/v1/databases/ttldb/sweep")
    check("sweep after expiry sweeps the 2 TTL docs",
          st == 200 and out["swept"] == 2, f"swept={out.get('swept')}")
    check("expired session gone from queries",
          rows('FROM sessions WHERE _id = "s1"') == [])
    check("permanent doc survives",
          len(rows('FROM sessions WHERE _id = "forever"')) == 1)

    # ── 6. TIME-TRAVEL: expiry is an op, not an erasure ─────────────────────
    hist = rows(f'FROM sessions AS OF {seq_alive}')
    ids = {r["_id"] for r in hist}
    check("AS OF before expiry still sees the expired doc",
          "s1" in ids and "forever" in ids, f"ids={ids}")

    # ── 7. background sweeper (NEDBD_SWEEP_S=1) does it unattended ──────────
    _req(port, "POST", "/v1/databases/ttldb/put",
         {"coll": "sessions", "id": "s2",
          "doc": {"token": "tok_bg"}, "ttl_s": 1.0})
    time.sleep(3.0)  # expiry (1s) + sweeper cadence (1s) + margin
    check("background sweeper removed it without any manual call",
          rows('FROM sessions WHERE _id = "s2"') == [])

    # ── 8. ordering sanity for slices 2-3 (already-shipped feature, pinned) ─
    for i, amt in enumerate([5.0, 9.0, 2.0]):
        _req(port, "POST", "/v1/databases/ttldb/put",
             {"coll": "txs", "id": f"t{i}", "doc": {"amount": amt, "n": i}})
    ordered = rows("FROM txs ORDER BY amount DESC LIMIT 2")
    check("ORDER BY + LIMIT over HTTP",
          [r["amount"] for r in ordered] == [9.0, 5.0])
finally:
    daemon.terminate()
    shutil.rmtree(data_dir, ignore_errors=True)

print(f"\n===== daemon-ttl: {PASS} passed, {FAIL} failed =====")
sys.exit(1 if FAIL else 0)
