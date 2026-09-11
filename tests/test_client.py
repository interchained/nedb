#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""Official NedbClient — daemon-spawning proof suite.

The client is an EXTRACTION of the two battle-tested hand-rolled clients
that ran the AiAS mainnet migration (storage adapter + backfill), so this
suite proves the promoted surface against a real nedbd:

  C1  transport: health / wait_ready / typed unreachable error
  C2  database lifecycle: ensure (idempotent) / list / detail / 404
  C3  put + get_doc roundtrip, _seq exposure, idem replay-dedupe
  C4  query / query_full envelope / count / NQL error typing /
      quote-injection guard
  C5  tx: atomic multi-put, CAS if_seq (miss -> PreconditionFailed with
      .failures + .seq, NOTHING applied), create-once if_seq=-1, op_del
  C6  ttl_s + sweep
  C7  indexes + WHERE, link/neighbors
  C8  integrity: verify / checkpoint / log / proof (verified server-side
      AND re-verified locally via nedb.proof.verify_proof)
  C9  mongo passthrough
  C10 cas_retry: 2-thread contention converges exactly; exhaustion raises
      CasExhausted
  C11 bearer-token auth: /health open, /v1 typed 401, token accepted
"""
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"))

from nedb.client import (  # noqa: E402
    CasExhausted, NedbAuthError, NedbBadRequest, NedbClient, NedbError,
    NedbNotFound, PreconditionFailed, op_del, op_put)
from nedb.proof import verify_proof  # noqa: E402

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


def _spawn(port, data_dir, token=None):
    env = {**os.environ, "NEDBD_SWEEP_S": "0",
           "PYTHONPATH": os.path.join(os.path.dirname(
               os.path.dirname(os.path.abspath(__file__))), "python")}
    env.pop("NEDBD_TOKEN", None)
    if token:
        env["NEDBD_TOKEN"] = token
    return subprocess.Popen(
        [sys.executable, "-m", "nedb.server", "--host", "127.0.0.1",
         "--port", str(port), "--data", data_dir],
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, env=env)


tmp = tempfile.mkdtemp(prefix="nedb-client-")
port = _free_port()
daemon = _spawn(port, os.path.join(tmp, "data"))

c = NedbClient(f"http://127.0.0.1:{port}", db="clienttest", token=None)

# ── C1 transport ─────────────────────────────────────────────────────────────
print("\n[C1] transport")
h = c.wait_ready(timeout=10)
check("C1 wait_ready -> health ok + version",
      h.get("ok") is True and bool(h.get("version")))
dead = NedbClient(f"http://127.0.0.1:{_free_port()}", db="x")
try:
    dead.health()
    check("C1 unreachable raises NedbError", False)
except NedbError as e:
    check("C1 unreachable raises NedbError (typed, no bare URLError)",
          "unreachable" in str(e))

# ── C2 database lifecycle ────────────────────────────────────────────────────
print("\n[C2] database lifecycle")
check("C2 ensure_database creates on first call", c.ensure_database() is True)
check("C2 ensure_database idempotent on second", c.ensure_database() is False)
check("C2 databases() lists it",
      any(d.get("name") == "clienttest" for d in c.databases()))
check("C2 database_detail answers", isinstance(c.database_detail(), dict))
try:
    c.database_detail("no-such-db")
    check("C2 unknown db raises typed error", False)
except (NedbNotFound, NedbBadRequest, NedbError) as e:
    check("C2 unknown db raises typed error",
          isinstance(e, NedbError) and e.status in (400, 404),
          f"got status={e.status}")

# ── C3 put/get + idem ────────────────────────────────────────────────────────
print("\n[C3] put / get_doc / idem")
r = c.put("users", "u1", {"id": "u1", "email": "a@b.c", "plan": "free"})
check("C3 put envelope {ok, doc, seq, head}",
      r.get("ok") is True and "seq" in r and "head" in r
      and r["doc"]["email"] == "a@b.c")
d = c.get_doc("users", "u1")
check("C3 get_doc returns doc with _id and _seq",
      d is not None and d["_id"] == "u1" and isinstance(d.get("_seq"), int))
check("C3 get_doc absent -> None", c.get_doc("users", "nope") is None)
c.put("users", "u2", {"id": "u2", "v": "FIRST"}, idem="idem-key-1")
c.put("users", "u2", {"id": "u2", "v": "SECOND"}, idem="idem-key-1")
check("C3 idem replay is a silent dedupe (doc keeps FIRST write)",
      c.get_doc("users", "u2")["v"] == "FIRST")

# ── C4 query surface ─────────────────────────────────────────────────────────
print("\n[C4] query surface")
rows = c.query('FROM users WHERE email = "a@b.c"')
check("C4 query returns rows", len(rows) == 1 and rows[0]["_id"] == "u1")
qf = c.query_full("FROM users")
check("C4 query_full envelope {rows, count, seq, head}",
      set(qf) >= {"rows", "count", "seq", "head"} and qf["count"] == 2)
check("C4 count()", c.count("users") == 2)
try:
    c.query("FROM WHERE nonsense !!")
    check("C4 bad NQL -> NedbBadRequest", False)
except NedbBadRequest as e:
    check("C4 bad NQL -> NedbBadRequest", e.status == 400)
# get_doc takes the id from the URL path (GET …/rows/<coll>/<id>), so nothing
# is concatenated into a query and injection is impossible BY CONSTRUCTION.
#
# This used to assert that a quoted id RAISED, because get_doc interpolated the
# id into `FROM coll WHERE _id = "..."` and refused anything unquotable. That
# made legitimate ids unusable: put() accepts an id containing a quote, the
# engine stores it, `FROM coll` returns it — and get_doc could not fetch it.
# The stronger property is asserted instead: a crafted id can only ever name a
# document, and a real one round-trips.
check("C4 a crafted id cannot widen the result set",
      c.get_doc("users", 'u1" OR 1') is None)
c.put("users", 'quo"ted', {"v": "Q"})
got_q = c.get_doc("users", 'quo"ted')
check("C4 an id containing a quote round-trips",
      got_q is not None and got_q.get("v") == "Q")
c.put("users", "sl/ash", {"v": "S"})
got_s = c.get_doc("users", "sl/ash")
check("C4 an id containing a slash round-trips",
      got_s is not None and got_s.get("v") == "S")
check("C4 delete reaches a slashed id", c.delete("users", "sl/ash").get("ok") is True)
check("C4 ...and it is gone", c.get_doc("users", "sl/ash") is None)

# ── C5 tx / CAS ──────────────────────────────────────────────────────────────
print("\n[C5] tx / CAS")
out = c.tx([op_put("orgs", "o1", {"id": "o1", "n": 1}),
            op_put("orgs", "o2", {"id": "o2", "n": 2})])
check("C5 atomic multi-put commits together",
      out.get("atomic") is True and c.count("orgs") == 2)
o1 = c.get_doc("orgs", "o1")
c.tx([op_put("orgs", "o1", {"id": "o1", "n": 10}, if_seq=o1["_seq"])])
check("C5 CAS with correct _seq applies",
      c.get_doc("orgs", "o1")["n"] == 10)
try:
    c.tx([op_put("orgs", "o1", {"id": "o1", "n": 99}, if_seq=o1["_seq"]),
          op_put("orgs", "o3", {"id": "o3", "n": 3})])
    check("C5 stale if_seq raises PreconditionFailed", False)
except PreconditionFailed as e:
    f = e.failures[0]
    check("C5 stale if_seq raises PreconditionFailed with .failures",
          f["coll"] == "orgs" and f["id"] == "o1"
          and f["expected"] == o1["_seq"])
    check("C5 .seq rides on the exception", isinstance(e.seq, int))
check("C5 ATOMICITY: nothing applied on mixed-tx rejection",
      c.get_doc("orgs", "o3") is None and c.get_doc("orgs", "o1")["n"] == 10)
c.tx([op_put("orgs", "once", {"id": "once"}, if_seq=-1)])
try:
    c.tx([op_put("orgs", "once", {"id": "once"}, if_seq=-1)])
    check("C5 create-once (if_seq=-1) rejects the second creator", False)
except PreconditionFailed:
    check("C5 create-once (if_seq=-1) rejects the second creator", True)
cur = c.get_doc("orgs", "o2")
c.tx([op_del("orgs", "o2", if_seq=cur["_seq"])])
check("C5 op_del with if_seq deletes", c.get_doc("orgs", "o2") is None)
c.delete("orgs", "once")
check("C5 delete() route removes the row", c.get_doc("orgs", "once") is None)

# ── C6 TTL ───────────────────────────────────────────────────────────────────
print("\n[C6] ttl + sweep")
c.put("sessions", "tok1", {"user": "u1"}, ttl_s=0.05)
time.sleep(0.15)
swept = c.sweep()
check("C6 ttl_s + sweep() expires the doc",
      swept >= 1 and c.get_doc("sessions", "tok1") is None,
      f"swept={swept}")

# ── C7 indexes + relations ───────────────────────────────────────────────────
print("\n[C7] indexes + relations")
n = c.ensure_indexes([("users", "email"), ("users", "plan")])
check("C7 ensure_indexes applies (idempotent server-side)",
      n == 2 and c.ensure_indexes([("users", "email")]) == 1)
check("C7 indexed WHERE answers",
      c.query('FROM users WHERE plan = "free"')[0]["_id"] == "u1")
c.link("users:u1", "member_of", "orgs:o1")
check("C7 link + neighbors traverse",
      c.neighbors("users:u1", "member_of") == ["orgs:o1"])

# ── C8 integrity ─────────────────────────────────────────────────────────────
print("\n[C8] integrity")
check("C8 verify() ok", c.verify().get("ok") is True)
cp = c.checkpoint()
check("C8 checkpoint returns head", bool(cp.get("head")))
lg = c.log(limit=5)
check("C8 log newest-first with chain hashes",
      len(lg) == 5 and all("hash" in o for o in lg)
      and lg[0]["seq"] > lg[-1]["seq"])
pf = c.proof(lg[0]["hash"])
check("C8 proof verified server-side", pf.get("verified") is True)
check("C8 proof re-verified LOCALLY (no server trust)", verify_proof(pf))

# ── C9 mongo passthrough ─────────────────────────────────────────────────────
print("\n[C9] mongo passthrough")
c.mongo("crm", "insertOne", document={"_id": "c1", "name": "Acme"})
got = c.mongo("crm", "findOne", filter={"name": "Acme"})
check("C9 insertOne/findOne roundtrip",
      (got.get("doc") or {}).get("_id") == "c1")

# ── C10 cas_retry ────────────────────────────────────────────────────────────
print("\n[C10] cas_retry under contention")
c.put("counters", "hits", {"n": 0})
errors: list = []


def bump_n(times):
    def one():
        d = c.get_doc("counters", "hits")
        return c.tx([op_put("counters", "hits", {"n": int(d["n"]) + 1},
                            if_seq=d["_seq"])])
    for _ in range(times):
        try:
            c.cas_retry(one)
        except Exception as e:  # noqa: BLE001
            errors.append(e)


t1 = threading.Thread(target=bump_n, args=(20,))
t2 = threading.Thread(target=bump_n, args=(20,))
t1.start(); t2.start(); t1.join(); t2.join()
check("C10 two threads x20 CAS increments converge EXACTLY (no lost updates)",
      not errors and c.get_doc("counters", "hits")["n"] == 40,
      f"n={c.get_doc('counters', 'hits')['n']} errors={errors[:2]}")


def always_lose():
    raise PreconditionFailed([{"index": 0, "coll": "x", "id": "y",
                               "expected": 1, "actual": 2}])


try:
    c.cas_retry(always_lose, max_retries=3, backoff_base=0.001)
    check("C10 exhaustion raises CasExhausted", False)
except CasExhausted:
    check("C10 exhaustion raises CasExhausted", True)

# ── C11 bearer auth ──────────────────────────────────────────────────────────
print("\n[C11] bearer-token auth")
port2 = _free_port()
d2 = _spawn(port2, os.path.join(tmp, "data2"), token="s3cret")
try:
    open_c = NedbClient(f"http://127.0.0.1:{port2}", db="authdb", token=None)
    open_c.wait_ready(timeout=10)
    check("C11 /health stays open without a token", True)
    try:
        open_c.databases()
        check("C11 /v1 without token -> NedbAuthError", False)
    except NedbAuthError as e:
        check("C11 /v1 without token -> NedbAuthError", e.status == 401)
    auth_c = NedbClient(f"http://127.0.0.1:{port2}", db="authdb",
                        token="s3cret")
    auth_c.ensure_database()
    auth_c.put("k", "1", {"id": "1"})
    check("C11 token accepted end-to-end (create/put/get)",
          auth_c.get_doc("k", "1") is not None)
finally:
    d2.terminate()

daemon.terminate()
shutil.rmtree(tmp, ignore_errors=True)
print(f"\n===== client: {PASS} passed, {FAIL} failed =====")
sys.exit(1 if FAIL else 0)
