#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
Integration test for the full nedbd deploy path.

Exercises POST /v1/databases with a realistic scaffold init payload
(indexes + seed data + relation links) — the exact operation the studio
performs when you click Deploy. This is the gap that let the deploy 502
go undetected: the engine and concurrent tests never exercised this call.
"""
import json, os, subprocess, sys, tempfile, time, shutil, http.client

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"))

PORT = 7172  # isolated port so it never collides with a running nedbd
DATA = tempfile.mkdtemp()

PASS = FAIL = 0

def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ok  {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}{(' — ' + str(detail)) if detail else ''}")


def req(method, path, body=None, expected=None):
    c = http.client.HTTPConnection("127.0.0.1", PORT, timeout=20)
    payload = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if payload else {}
    c.request(method, path, payload, headers)
    r = c.getresponse()
    data = r.read().decode()
    c.close()
    try:
        parsed = json.loads(data) if data else {}
    except Exception:
        parsed = {"_raw": data}
    if expected is not None and r.status != expected:
        print(f"    -> {method} {path}: HTTP {r.status} (expected {expected}), body: {data[:300]}")
    return r.status, parsed


env = dict(os.environ,
           NEDBD_PORT=str(PORT), NEDBD_HOST="127.0.0.1",
           NEDBD_DATA=DATA)
env.pop("NEDB_TMK", None)

proc = subprocess.Popen(
    [sys.executable, "-m", "nedb.server"],
    env=env, cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
)

try:
    # wait for health
    up = False
    for _ in range(40):
        try:
            s, _ = req("GET", "/health")
            if s == 200:
                up = True
                break
        except Exception:
            pass
        time.sleep(0.15)
    check("daemon started", up)
    if not up:
        print("  daemon did not start — aborting")
        sys.exit(1)

    # ── 1. Simple deploy (no seed, no links) ──────────────────────────────────
    print("\n── simple deploy (empty init) ──")
    s, r = req("POST", "/v1/databases", {"name": "simple_db"}, expected=201)
    check("create empty db: 201", s == 201, r)
    s, r = req("GET", "/v1/databases/simple_db")
    check("get empty db", s == 200, r)

    # ── 2. Deploy with indexes only ───────────────────────────────────────────
    print("\n── deploy with indexes ──")
    init = {
        "indexes": [
            ["clients", "status", "eq"],
            ["clients", "name",   "search"],
            ["clients", "age",    "ordered"],
        ],
    }
    s, r = req("POST", "/v1/databases", {"name": "idx_db", "init": init}, expected=201)
    check("create db with indexes: 201", s == 201, r)

    # ── 3. Deploy with seed data ──────────────────────────────────────────────
    print("\n── deploy with seed data ──")
    seed_init = {
        "indexes": [["users", "status", "eq"]],
        "seed": {
            "users": [
                {"_id": "u1", "name": "Alice", "age": 31, "status": "active"},
                {"_id": "u2", "name": "Bob",   "age": 24, "status": "active"},
                {"_id": "u3", "name": "Carol", "age": 41, "status": "inactive"},
            ],
            "orders": [
                {"_id": "o1", "user_id": "u1", "amount": 99.0, "status": "paid"},
                {"_id": "o2", "user_id": "u2", "amount": 49.0, "status": "pending"},
            ],
        },
    }
    s, r = req("POST", "/v1/databases", {"name": "seed_db", "init": seed_init}, expected=201)
    check("create db with seed: 201", s == 201, r)
    check("seed_db has rows", (r.get("database") or r).get("rows", 0) == 5, r)

    # Verify data is actually there
    s, r = req("POST", "/v1/databases/seed_db/query", {"nql": 'FROM users WHERE status = "active"'})
    check("seed data queryable", s == 200 and r.get("count") == 2, r)

    # ── 4. Deploy with seed AND links (the full studio payload) ───────────────
    print("\n── full studio deploy (indexes + seed + links) ──")
    full_init = {
        "indexes": [
            ["clients",      "status",   "eq"],
            ["clients",      "full_name","search"],
            ["stylists",     "specialty","eq"],
            ["appointments", "date",     "ordered"],
            ["appointments", "status",   "eq"],
        ],
        "seed": {
            "clients": [
                {"_id": "client_001", "full_name": "Mia Thornton",  "status": "active", "phone": "555-0101"},
                {"_id": "client_002", "full_name": "James Okafor",  "status": "active", "phone": "555-0102"},
                {"_id": "client_003", "full_name": "Sofia Reyes",   "status": "active", "phone": "555-0103"},
            ],
            "stylists": [
                {"_id": "stylist_001", "name": "Jordan Kim",  "specialty": "color"},
                {"_id": "stylist_002", "name": "Taylor Moss", "specialty": "cuts"},
            ],
            "services": [
                {"_id": "svc_001", "name": "Color & Style",  "price": 120.0, "duration_min": 90},
                {"_id": "svc_002", "name": "Precision Cut",  "price": 65.0,  "duration_min": 45},
                {"_id": "svc_003", "name": "Deep Condition",  "price": 45.0,  "duration_min": 30},
            ],
            "appointments": [
                {"_id": "appt_001", "client_id": "client_001", "stylist_id": "stylist_001",
                 "service_id": "svc_001", "date": "2024-02-15", "status": "confirmed"},
                {"_id": "appt_002", "client_id": "client_002", "stylist_id": "stylist_002",
                 "service_id": "svc_002", "date": "2024-02-16", "status": "pending"},
                {"_id": "appt_003", "client_id": "client_003", "stylist_id": "stylist_001",
                 "service_id": "svc_003", "date": "2024-02-17", "status": "confirmed"},
            ],
        },
        "links": [
            ["clients:client_001", "books", "appointments:appt_001"],
            ["clients:client_002", "books", "appointments:appt_002"],
            ["clients:client_003", "books", "appointments:appt_003"],
            ["stylists:stylist_001", "handles", "appointments:appt_001"],
            ["stylists:stylist_001", "handles", "appointments:appt_003"],
            ["stylists:stylist_002", "handles", "appointments:appt_002"],
        ],
    }
    s, r = req("POST", "/v1/databases", {"name": "salonbooking", "init": full_init}, expected=201)
    check("full studio deploy: 201", s == 201, r)

    total_rows = sum(len(v) for v in full_init["seed"].values())
    actual_rows = (r.get("database") or r).get("rows", -1)
    check(f"all {total_rows} seed rows present (got {actual_rows})", actual_rows == total_rows, r)

    # Query + traverse
    s, q = req("POST", "/v1/databases/salonbooking/query",
               {"nql": 'FROM clients WHERE _id = "client_001" TRAVERSE books'})
    check("TRAVERSE works after deploy", s == 200 and q.get("count", 0) >= 1, q)

    # Verify chain
    s, v = req("GET", "/v1/databases/salonbooking/verify")
    check("integrity.ok after full deploy", s == 200 and v.get("ok") is True, v)

    # ── 5. Name collision → auto-retry ───────────────────────────────────────
    print("\n── name collision handling ──")
    s, r2 = req("POST", "/v1/databases", {"name": "salonbooking"}, expected=409)
    check("duplicate name → 409", s == 409, r2)

    # ── 6. Persist + reload ───────────────────────────────────────────────────
    print("\n── persistence across restart ──")
    # quick check: list still shows all dbs
    s, lst = req("GET", "/v1/databases")
    names = [d["name"] for d in (lst.get("databases") or [])]
    check("salonbooking in list", "salonbooking" in names, names)

finally:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except Exception:
        proc.kill()
    shutil.rmtree(DATA, ignore_errors=True)

print(f"\nDeploy: {PASS} passed, {FAIL} failed {'✅' if not FAIL else '❌'}")
sys.exit(1 if FAIL else 0)


# ── Extra: encrypted deploy (the actual production bug) ───────────────────────
# This is the test that would have caught the deploy 502 immediately:
# NEDB_TMK set + brand-new database = FileNotFoundError on key.enc.tmp
# because _dek creation ran before _open() created the directory.
import os as _os
print("\n── encrypted deploy (NEDB_TMK set) ──")
_os.environ["NEDB_TMK"] = "220cd848749585949890625368ba44115247b8983b3dc8d53823dcfcaef02ef2"
import importlib, nedb as _nedb_mod; importlib.reload(_nedb_mod)
from nedb import NEDB as _NEDB
import tempfile as _tmp, shutil as _sh
_etmp = _tmp.mkdtemp()
try:
    _edb = _NEDB(_etmp)
    _edb.put("t", "r1", {"v": 1})
    _ev = _edb.verify()
    _edb.close()
    _edb2 = _NEDB(_etmp)
    check("encrypted new DB: create + reopen + verify", _ev and _edb2.verify())
    _edb2.close()
finally:
    _sh.rmtree(_etmp, ignore_errors=True)
    _os.environ.pop("NEDB_TMK", None)

total = PASS + FAIL
print(f"\nDeploy: {PASS}/{total} passed {'✅' if not FAIL else '❌'}")
sys.exit(1 if FAIL else 0)
