#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""Atomic transaction (tx) + if_seq CAS preconditions — engine, Sequencer, HTTP.

The headline test is the RACE PROOF: the exact concurrency bug that Redis Lua
scripts protect against in production (two workers both passing a balance
check before either decrements — a double-spend) is first DEMONSTRATED with
unguarded read-modify-write, then PROVEN CLOSED with if_seq CAS + retry.
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"))
from nedb import NEDB, PreconditionFailed          # noqa: E402
from nedb.concurrent import Sequencer               # noqa: E402

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ok  {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name} {detail}")


# ── 1. Engine-level tx semantics ─────────────────────────────────────────────
print("\n── engine tx: CAS semantics ──")
tmp = tempfile.mkdtemp()
try:
    db = NEDB(tmp)

    # unconditional tx applies all ops
    out = db.tx([
        {"op": "put", "coll": "pin", "id": "billing:op1",
         "doc": {"earnings_balance": 100.0}},
        {"op": "put", "coll": "pin", "id": "withdrawals:w0",
         "doc": {"status": "seed"}},
    ])
    # db.seq is the LAST op's seq (0-based): two ops -> seq == 1
    check("unconditional tx applies all", out["count"] == 2 and db.seq == 1)

    # CAS pass: correct if_seq
    v = db.last_seq("pin", "billing:op1")
    out = db.tx([
        {"op": "put", "coll": "pin", "id": "billing:op1",
         "doc": {"earnings_balance": 90.0}, "if_seq": v},
        {"op": "put", "coll": "pin", "id": "withdrawals:w1",
         "doc": {"amount": 10.0, "status": "pending"}},
    ])
    check("CAS pass applies", db.get("pin", "billing:op1")["earnings_balance"] == 90.0)

    # CAS fail: stale if_seq -> PreconditionFailed, NOTHING applied
    seq_before = db.seq
    try:
        db.tx([
            {"op": "put", "coll": "pin", "id": "billing:op1",
             "doc": {"earnings_balance": 80.0}, "if_seq": v},  # stale!
            {"op": "put", "coll": "pin", "id": "withdrawals:w2",
             "doc": {"amount": 10.0}},
        ])
        check("stale CAS raises", False)
    except PreconditionFailed as e:
        check("stale CAS raises", True)
        check("failure detail carries expected/actual",
              e.failures[0]["expected"] == v
              and e.failures[0]["actual"] == db.last_seq("pin", "billing:op1"))
    check("NOTHING applied on failure",
          db.seq == seq_before
          and db.get("pin", "billing:op1")["earnings_balance"] == 90.0
          and db.get("pin", "withdrawals:w2") is None)

    # must-not-exist (-1): create-once
    out = db.tx([{"op": "put", "coll": "licenses", "id": "L1",
                  "doc": {"status": "available"}, "if_seq": -1}])
    check("create-once passes when absent", out["count"] == 1)
    try:
        db.tx([{"op": "put", "coll": "licenses", "id": "L1",
                "doc": {"status": "available"}, "if_seq": -1}])
        check("create-once fails when present", False)
    except PreconditionFailed:
        check("create-once fails when present", True)

    # CAS correctly fails after a DELETE (tombstone advances last_seq)
    v = db.last_seq("licenses", "L1")
    db.delete("licenses", "L1")
    try:
        db.tx([{"op": "put", "coll": "licenses", "id": "L1",
                "doc": {"status": "active"}, "if_seq": v}])
        check("CAS fails across delete", False)
    except PreconditionFailed:
        check("CAS fails across delete", True)

    # idem inside tx dedupes engine-side
    s = db.seq
    db.tx([{"op": "put", "coll": "kv", "id": "i1",
            "doc": {"v": 1}, "idem": "tx-idem-1"}])
    s_after_first = db.seq
    db.tx([{"op": "put", "coll": "kv", "id": "i1",
            "doc": {"v": 1}, "idem": "tx-idem-1"}])
    check("idem dedupes inside tx",
          s_after_first == s + 1 and db.seq == s_after_first)
finally:
    shutil.rmtree(tmp, ignore_errors=True)

# ── 2. Sequencer tx: one intent on the committer ────────────────────────────
print("\n── sequencer tx ──")
tmp = tempfile.mkdtemp()
try:
    seqr = Sequencer(NEDB(tmp))
    seqr.put("pin", "b1", {"bal": 5})
    v = seqr.db.last_seq("pin", "b1")
    out = seqr.tx([{"op": "put", "coll": "pin", "id": "b1",
                    "doc": {"bal": 4}, "if_seq": v}])
    check("sequencer tx applies", seqr.get("pin", "b1")["bal"] == 4)
    try:
        seqr.tx([{"op": "put", "coll": "pin", "id": "b1",
                  "doc": {"bal": 3}, "if_seq": v}])
        check("sequencer tx propagates PreconditionFailed", False)
    except PreconditionFailed:
        check("sequencer tx propagates PreconditionFailed", True)
    seqr.close()
finally:
    shutil.rmtree(tmp, ignore_errors=True)


# ── 3. THE RACE PROOF over HTTP against a real daemon ────────────────────────
print("\n── race proof: unguarded loses updates; if_seq closes it ──")


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
daemon = subprocess.Popen(
    [sys.executable, "-m", "nedb.server", "--host", "127.0.0.1",
     "--port", str(port), "--data", data_dir],
    stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
    env={**os.environ, "PYTHONPATH": os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python")})
deadline = time.time() + 10
while time.time() < deadline:
    try:
        if _req(port, "GET", "/health")[0] == 200:
            break
    except Exception:
        time.sleep(0.2)

try:
    _req(port, "POST", "/v1/databases", {"name": "race"})

    def get_doc(coll, rid):
        st, out = _req(port, "POST", "/v1/databases/race/query",
                       {"nql": f'FROM {coll} WHERE _id = "{rid}"'})
        rows = out.get("rows", [])
        return (rows[0] if rows else None), out.get("seq")

    THREADS, DECS_EACH = 4, 25   # 4 workers × 25 decrements of a 100 balance

    # -- control: UNGUARDED read-modify-write (how it breaks without Lua) ----
    _req(port, "POST", "/v1/databases/race/put",
         {"coll": "pin", "id": "unguarded", "doc": {"bal": 100}})
    barrier = threading.Barrier(THREADS)

    def unguarded(tid):
        barrier.wait()
        for _ in range(DECS_EACH):
            doc, _s = get_doc("pin", "unguarded")
            time.sleep(0.002)  # widen the read->write window (2 workers do this implicitly)
            _req(port, "POST", "/v1/databases/race/put",
                 {"coll": "pin", "id": "unguarded",
                  "doc": {"bal": doc["bal"] - 1}})

    ts = [threading.Thread(target=unguarded, args=(i,)) for i in range(THREADS)]
    [t.start() for t in ts]; [t.join() for t in ts]
    final_unguarded = get_doc("pin", "unguarded")[0]["bal"]
    lost = final_unguarded - 0
    print(f"    unguarded final balance: {final_unguarded} "
          f"(should be 0 — {lost} update(s) LOST to the race)")
    check("control demonstrates lost updates (race exists)", final_unguarded > 0,
          f"final={final_unguarded} — if 0, environment serialized by luck; rerun")

    # -- guarded: if_seq CAS + retry (the Lua replacement) --------------------
    _req(port, "POST", "/v1/databases/race/put",
         {"coll": "pin", "id": "guarded", "doc": {"bal": 100}})
    conflicts = [0] * THREADS
    barrier2 = threading.Barrier(THREADS)

    def last_seq_of(rid):
        # derive the doc's version anchor: read, then CAS against it.
        # the /query response's seq is the DB head; the per-doc anchor comes
        # from attempting the tx — so read-validate-tx-retry uses log query:
        st, out = _req(port, "POST", "/v1/databases/race/query",
                       {"nql": f'FROM pin WHERE _id = "{rid}"'})
        return out["rows"][0], out["seq"]

    def guarded(tid):
        barrier2.wait()
        done = 0
        while done < DECS_EACH:
            doc, head = last_seq_of("guarded")
            if doc["bal"] <= 0:
                break  # guard: insufficient balance (never happens here)
            time.sleep(0.002)  # same widened window as the control
            st, out = _req(port, "POST", "/v1/databases/race/batch", {
                "client": f"worker-{tid}",
                "ops": [
                    {"op": "put", "coll": "pin", "id": "guarded",
                     "doc": {"bal": doc["bal"] - 1}, "if_seq": doc["_seq"]},
                    {"op": "put", "coll": "pin",
                     "id": f"wd:{tid}:{done}",
                     "doc": {"amount": 1, "status": "pending"}},
                ]})
            if st == 200:
                done += 1
            elif st == 409:
                conflicts[tid] += 1  # lost the CAS — re-read and retry
            else:
                raise AssertionError(f"unexpected status {st}: {out}")

    ts = [threading.Thread(target=guarded, args=(i,)) for i in range(THREADS)]
    [t.start() for t in ts]; [t.join() for t in ts]

    final_guarded = get_doc("pin", "guarded")[0]["bal"]
    total_conflicts = sum(conflicts)
    print(f"    guarded final balance: {final_guarded} "
          f"(exactly 0 = no lost updates), conflicts caught+retried: "
          f"{total_conflicts}")
    check("guarded: ZERO lost updates", final_guarded == 0,
          f"final={final_guarded}")
    check("guarded: every decrement recorded",
          len(_req(port, "POST", "/v1/databases/race/query",
                   {"nql": "FROM pin"})[1]["rows"]) >= 100)
    check("guarded: conflicts were caught (the race fired and was closed)",
          total_conflicts > 0, f"conflicts={total_conflicts}")

    # -- HTTP 409 shape --------------------------------------------------------
    st, out = _req(port, "POST", "/v1/databases/race/batch", {
        "ops": [{"op": "put", "coll": "pin", "id": "guarded",
                 "doc": {"bal": 999}, "if_seq": 1}]})
    check("HTTP 409 precondition shape",
          st == 409 and out.get("error") == "precondition_failed"
          and out["failures"][0]["expected"] == 1)

    # -- legacy batch back-compat ---------------------------------------------
    st, out = _req(port, "POST", "/v1/databases/race/batch", {
        "ops": [{"op": "put", "coll": "kv", "id": "legacy",
                 "doc": {"v": 1}},
                {"op": "link", "frm": "kv:legacy", "rel": "touched_by",
                 "to": "kv:legacy"}]})
    check("legacy batch (no if_seq) still works",
          st == 200 and out["count"] == 2 and out.get("atomic") is True)
finally:
    daemon.terminate()
    shutil.rmtree(data_dir, ignore_errors=True)

print(f"\n===== tx: {PASS} passed, {FAIL} failed =====")
sys.exit(1 if FAIL else 0)
