#!/usr/bin/env python3
"""
test_wrap_redis_nedbd.py — Integration tests for wrap_redis() in nedbd mode.

Spins up a real nedbd server on a temp port, wraps a fakeredis connection
with nedbd_url= pointing at it, and verifies the full surface-2 API works
identically to in-process mode.

    pip install fakeredis
    python3 tests/test_wrap_redis_nedbd.py

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
import os, sys, json, time, socket, tempfile, subprocess, threading
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"))

try:
    import fakeredis
except ImportError:
    print("SKIP: fakeredis not installed — pip install fakeredis")
    sys.exit(0)

from nedb import wrap_redis

PASS = FAIL = 0
def check(name, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  ok  {name}")
    else:    FAIL += 1; print(f"  FAIL {name}{(' — '+str(detail)) if detail else ''}")

def section(t): print(f"\n  ── {t} {'─'*(46-len(t))}")

# ─────────────────────────────────────────────────────────────────────────────
# Start a real nedbd on a free port
# ─────────────────────────────────────────────────────────────────────────────

def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]

PORT    = free_port()
TMPDIR  = tempfile.mkdtemp(prefix="nedb_test_")
NEDBD_URL = f"http://127.0.0.1:{PORT}"

print(f"\n  Starting nedbd on port {PORT} (data dir: {TMPDIR})…")
proc = subprocess.Popen(
    [sys.executable, "-m", "nedb.server", "--port", str(PORT), "--data", TMPDIR],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE
)

# Wait for nedbd to be ready
import urllib.request
for _ in range(30):
    try:
        urllib.request.urlopen(f"{NEDBD_URL}/health", timeout=1)
        break
    except Exception:
        time.sleep(0.1)
else:
    proc.terminate()
    sys.exit(f"FATAL: nedbd didn't start on port {PORT}")

print(f"  nedbd ready at {NEDBD_URL}")

# ─────────────────────────────────────────────────────────────────────────────
section("nedbd mode: wrap_redis with nedbd_url")
# ─────────────────────────────────────────────────────────────────────────────
raw = fakeredis.FakeRedis()
r = wrap_redis(raw, db_name="rideshare", nedbd_url=NEDBD_URL)

check("nedbd_mode flag set",          r.nedb._nedbd_mode)
check("surface-1 still works",        r.set("config:v", "1.0") is not None)
check("surface-1 get still works",    r.get("config:v") == b"1.0")

# ─────────────────────────────────────────────────────────────────────────────
section("nedbd mode: NEDB put / get / query")
# ─────────────────────────────────────────────────────────────────────────────
r.nedb.create_index("driver", "status", "eq")
r.nedb.put("driver", "d1", {"name": "Bob",   "status": "active",   "lat": 37.7749})
r.nedb.put("driver", "d2", {"name": "Carol", "status": "active",   "lat": 37.8044})
r.nedb.put("driver", "d3", {"name": "Dave",  "status": "inactive", "lat": 37.6879})

d1 = r.nedb.get("driver", "d1")
check("get by id",                    d1 is not None)
check("get value correct",            d1["name"] == "Bob")
check("get missing → None",           r.nedb.get("driver", "zzz") is None)

active = r.nedb.query('FROM driver WHERE status = "active"')
check("NQL WHERE active count",       len(active) == 2)
check("NQL names correct",            {d["name"] for d in active} == {"Bob", "Carol"})

grouped = r.nedb.query("FROM driver GROUP BY status COUNT")
counts = {g["status"]: g["count"] for g in grouped}
check("GROUP BY count",               counts.get("active") == 2)
check("GROUP BY inactive",            counts.get("inactive") == 1)

# ─────────────────────────────────────────────────────────────────────────────
section("nedbd mode: time-travel AS OF")
# ─────────────────────────────────────────────────────────────────────────────
snap = r.nedb.seq
r.nedb.put("driver", "d1", {"name": "Bob", "status": "offline", "lat": 37.9})

current = r.nedb.get("driver", "d1")
past    = r.nedb.get_as_of("driver", "d1", snap)

check("current status offline",       current["status"] == "offline")
check("AS OF snap status active",     past["status"] == "active")

old_rows = r.nedb.query(f'FROM driver AS OF {snap} WHERE status = "active"')
check("NQL AS OF active",             len(old_rows) >= 1)

# ─────────────────────────────────────────────────────────────────────────────
section("nedbd mode: causal provenance")
# ─────────────────────────────────────────────────────────────────────────────
r.nedb.put("event", "loc1", {"type": "location", "driver": "d1", "lat": 37.77})
seq_loc = r.nedb.seq

r.nedb.put("trip", "t1",
    {"rider": "u1", "driver": "d1", "status": "assigned"},
    caused_by=[seq_loc],
    evidence="inference",
    confidence=0.95)

trip = r.nedb.get("trip", "t1")
check("caused_by stored via nedbd",   trip.get("_caused_by") == [seq_loc])
check("evidence stored via nedbd",    trip.get("_evidence") == "inference")
check("confidence stored via nedbd",  trip.get("_confidence") == 0.95)

trace = r.nedb.query('FROM trip WHERE _id = "t1" TRACE caused_by')
check("TRACE via nedbd",              len(trace) >= 1)

# ─────────────────────────────────────────────────────────────────────────────
section("nedbd mode: hash chain verify + seq/head")
# ─────────────────────────────────────────────────────────────────────────────
check("verify() via nedbd",           r.nedb.verify())
check("head() is 64-char hex",        len(r.nedb.head()) == 64)
check("seq > 0",                      r.nedb.seq > 0)

# ─────────────────────────────────────────────────────────────────────────────
section("nedbd mode: backfill existing Redis data")
# ─────────────────────────────────────────────────────────────────────────────
raw2 = fakeredis.FakeRedis()
raw2.set("zone:z1", json.dumps({"name": "SoMa",  "active": True}))
raw2.set("zone:z2", json.dumps({"name": "BART",  "active": False}))
raw2.set("zone:z3", json.dumps({"name": "Castro","active": True}))

r2 = wrap_redis(raw2, db_name="zones", nedbd_url=NEDBD_URL)
r2.nedb.register("zone:*", "zone", value_parser=json.loads)
imported = r2.nedb.backfill()
check("backfill via nedbd returns count",  imported == 3)
check("backfilled data queryable",         len(r2.nedb.query("FROM zone")) == 3)
active_zones = r2.nedb.query('FROM zone WHERE active = true')
check("NQL filter on backfilled data",     len(active_zones) == 2)

# ─────────────────────────────────────────────────────────────────────────────
section("nedbd mode: write shadowing to nedbd")
# ─────────────────────────────────────────────────────────────────────────────
raw3 = fakeredis.FakeRedis()
r3 = wrap_redis(raw3, db_name="shadow_nd", nedbd_url=NEDBD_URL)
r3.nedb.register("sensor:*", "sensor", value_parser=json.loads)
r3.nedb.shadow_writes = True

r3.set("sensor:s1", json.dumps({"temp": 22.5, "unit": "C"}))
r3.set("sensor:s2", json.dumps({"temp": 18.1, "unit": "C"}))

check("shadow SET goes to nedbd",     r3.nedb.get("sensor", "s1") is not None)
check("shadow value correct",         r3.nedb.get("sensor", "s1")["temp"] == 22.5)
sensors = r3.nedb.query("FROM sensor")
check("shadow: both sensors in nedbd",sensors is not None and len(sensors) == 2)
check("verify after shadow writes",   r3.nedb.verify())

# ─────────────────────────────────────────────────────────────────────────────
section("nedbd mode: durable — data survives across wrap_redis calls")
# ─────────────────────────────────────────────────────────────────────────────
# New WrappedRedis on same nedbd → same database already has the data
r4 = wrap_redis(fakeredis.FakeRedis(), db_name="rideshare", nedbd_url=NEDBD_URL)
rows_on_reconnect = r4.nedb.query("FROM driver")
check("data persists in nedbd",       len(rows_on_reconnect) >= 3)
check("verify on reconnect",          r4.nedb.verify())

# ─────────────────────────────────────────────────────────────────────────────
# Shutdown nedbd
# ─────────────────────────────────────────────────────────────────────────────
proc.terminate()
proc.wait(timeout=5)

import shutil
shutil.rmtree(TMPDIR, ignore_errors=True)

total = PASS + FAIL
print(f"\n  {'═'*52}")
print(f"  wrap_redis nedbd  |  {PASS}/{total} passed{'  ✅' if not FAIL else f'  ❌  {FAIL} FAILED'}")
print(f"  {'═'*52}\n")
sys.exit(1 if FAIL else 0)
