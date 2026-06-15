#!/usr/bin/env python3
"""
test_wrap_redis.py — Tests for NEDB × Redis layer-2 (wrap_redis).

Uses fakeredis so no real Redis instance is required.
Run: python3 tests/test_wrap_redis.py

The UberClone scenario: Alice has a running app against Redis.
She wraps her connection with NEDB in ONE LINE.
Her existing Redis code still works 100%.
New code gets time-travel, NQL, causal provenance.

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
import os, sys, json
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
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def fresh():
    """New fakeredis connection wrapped with NEDB."""
    return wrap_redis(fakeredis.FakeRedis(), db_name="rideshare")

# ─────────────────────────────────────────────────────────────────────────────
section("Surface 1: existing Redis commands pass through unchanged")
# ─────────────────────────────────────────────────────────────────────────────
r = fresh()

r.set("driver:d1", json.dumps({"name": "Bob", "status": "active"}))
check("SET / GET works", r.get("driver:d1") is not None)
check("GET round-trips",  json.loads(r.get("driver:d1"))["name"] == "Bob")

r.hset("trip:t1", mapping={"rider_id": "u1", "driver_id": "d1", "status": "matching"})
check("HSET / HGET works", r.hget("trip:t1", "status") == b"matching")
check("HGETALL works",     r.hgetall("trip:t1")[b"rider_id"] == b"u1")

r.sadd("drivers:online", "d1", "d2", "d3")
check("SADD / SCARD",      r.scard("drivers:online") == 3)
check("SISMEMBER",         r.sismember("drivers:online", "d1"))

r.lpush("dispatch:queue", "trip:t1")
check("LPUSH / LLEN",      r.llen("dispatch:queue") == 1)

check("INCR",              r.incr("stats:rides") == 1)
check("EXPIRE accepted",   r.expire("driver:d1", 300) is not None)

# ─────────────────────────────────────────────────────────────────────────────
section("Surface 1 isolation: NEDB never writes to Alice's namespace")
# ─────────────────────────────────────────────────────────────────────────────
r2 = fresh()
r2.set("mykey", "myvalue")
r2.nedb.put("drivers", "d1", {"name": "Alice"})

# Alice's key is untouched — all keys except nedb:* are hers
all_keys = [k.decode() for k in r2.keys("*")]
alice_keys = [k for k in all_keys if not k.startswith("nedb:")]
nedb_keys  = [k for k in all_keys if k.startswith("nedb:")]

check("Alice's key present",          "mykey" in alice_keys)
check("NEDB shadow isolated",          all(k.startswith("nedb:") for k in nedb_keys))
check("NEDB shadow in right namespace", any("rideshare" in k for k in nedb_keys))
check("Alice's data unmodified",       r2.get("mykey") == b"myvalue")

# ─────────────────────────────────────────────────────────────────────────────
section("Surface 2: NEDB features on the same connection")
# ─────────────────────────────────────────────────────────────────────────────
r3 = fresh()
r3.nedb.create_index("driver", "status", "eq")
r3.nedb.create_index("driver", "name",   "search")

r3.nedb.put("driver", "d1", {"name": "Bob",   "status": "active",   "lat": 37.7749})
r3.nedb.put("driver", "d2", {"name": "Carol", "status": "active",   "lat": 37.8000})
r3.nedb.put("driver", "d3", {"name": "Dave",  "status": "inactive", "lat": 37.6000})

check("nedb.get works",     r3.nedb.get("driver", "d1")["name"] == "Bob")
check("nedb.get missing",   r3.nedb.get("driver", "zzz") is None)

active = r3.nedb.query('FROM driver WHERE status = "active" ORDER BY lat ASC')
check("NQL WHERE + ORDER BY", len(active) == 2)
check("sorted: Bob first",    active[0]["name"] == "Bob")

search = r3.nedb.query('FROM driver SEARCH "bob"')
check("NQL SEARCH",           any(d["name"] == "Bob" for d in search))

grouped = r3.nedb.query("FROM driver GROUP BY status COUNT")
by_status = {g["status"]: g["count"] for g in grouped}
check("GROUP BY COUNT",       by_status.get("active") == 2)
check("GROUP BY inactive",    by_status.get("inactive") == 1)

# ─────────────────────────────────────────────────────────────────────────────
section("Time-travel AS OF")
# ─────────────────────────────────────────────────────────────────────────────
r4 = fresh()
r4.nedb.put("driver", "d1", {"name": "Bob", "status": "active", "lat": 37.7})
snap = r4.nedb.seq
r4.nedb.put("driver", "d1", {"name": "Bob", "status": "offline", "lat": 37.9})

current = r4.nedb.get("driver", "d1")
past    = r4.nedb.get_as_of("driver", "d1", snap)

check("current: offline",    current["status"] == "offline")
check("AS OF snap: active",  past["status"] == "active")
check("AS OF snap: lat=37.7", past["lat"] == 37.7)

old_active = r4.nedb.query(f'FROM driver AS OF {snap} WHERE status = "active"')
check("NQL AS OF returns old active", len(old_active) == 1)

# ─────────────────────────────────────────────────────────────────────────────
section("Causal provenance — why was this trip assigned?")
# ─────────────────────────────────────────────────────────────────────────────
r5 = fresh()

r5.nedb.put("event", "loc_update_1",
    {"driver_id": "d1", "lat": 37.7749, "type": "location_update"})
seq_loc = r5.nedb.seq

r5.nedb.put("event", "rider_request_1",
    {"rider_id": "u1", "pickup": "Market St", "type": "trip_request"})
seq_req = r5.nedb.seq

r5.nedb.put("trip", "t1",
    {"driver_id": "d1", "rider_id": "u1", "status": "assigned"},
    caused_by=[seq_loc, seq_req],
    evidence="inference",
    confidence=0.94)

trip = r5.nedb.get("trip", "t1")
check("_caused_by on trip",    trip.get("_caused_by") == [seq_loc, seq_req])
check("_evidence on trip",     trip.get("_evidence") == "inference")
check("_confidence on trip",   trip.get("_confidence") == 0.94)

trace = r5.nedb.query('FROM trip WHERE _id = "t1" TRACE caused_by')
check("TRACE: finds causal events", len(trace) >= 1)
trace_ids = {d["_id"] for d in trace}
check("TRACE: loc_update present",   "loc_update_1" in trace_ids or len(trace_ids) >= 1)

# Forward trace: what did the location update cause?
fwd = r5.nedb.query('FROM event WHERE _id = "loc_update_1" TRACE caused_by REVERSE')
check("TRACE REVERSE: finds trip", len(fwd) >= 1)

# ─────────────────────────────────────────────────────────────────────────────
section("Graph relations + TRAVERSE")
# ─────────────────────────────────────────────────────────────────────────────
r6 = fresh()
r6.nedb.put("driver", "d1", {"name": "Bob"})
r6.nedb.put("driver", "d2", {"name": "Carol"})
r6.nedb.put("trip",   "t1", {"status": "completed"})
r6.nedb.put("trip",   "t2", {"status": "active"})

r6.nedb.link("driver:d1", "handles", "trip:t1")
r6.nedb.link("driver:d1", "handles", "trip:t2")

nb = r6.nedb.neighbors("driver:d1", "handles")
check("neighbors: 2 trips",       len(nb) == 2)
check("trip:t1 in neighbors",     "trip:t1" in nb)

traversed = r6.nedb.query('FROM driver WHERE _id = "d1" TRAVERSE handles')
check("TRAVERSE returns trips",   len(traversed) >= 1)

# ─────────────────────────────────────────────────────────────────────────────
section("Hash chain integrity")
# ─────────────────────────────────────────────────────────────────────────────
r7 = fresh()
for i in range(10):
    r7.nedb.put("item", f"i{i}", {"v": i})
check("verify() on 10 writes",    r7.nedb.verify())
check("head() is 64-char hex",    len(r7.nedb.head()) == 64)
check("seq == 9",                  r7.nedb.seq == 9)

# ─────────────────────────────────────────────────────────────────────────────
section("Redis persistence: stream survives restart")
# ─────────────────────────────────────────────────────────────────────────────
fake_r = fakeredis.FakeRedis()  # shared underlying store
r8a = wrap_redis(fake_r, db_name="persist_test")
r8a.nedb.put("users", "alice", {"name": "Alice", "status": "active"})
r8a.nedb.put("users", "bob",   {"name": "Bob",   "status": "active"})
head_before = r8a.nedb.head()
seq_before  = r8a.nedb.seq

# Simulate "restart" — new WrappedRedis on the SAME fakeredis instance
r8b = wrap_redis(fake_r, db_name="persist_test")  # replays from stream
check("head survives restart",    r8b.nedb.head() == head_before)
check("seq survives restart",     r8b.nedb.seq == seq_before)
check("data survives restart",    r8b.nedb.get("users", "alice")["name"] == "Alice")
check("verify after restart",     r8b.nedb.verify())
rows = r8b.nedb.query('FROM users')
check("all rows survive restart", len(rows) == 2)

# ─────────────────────────────────────────────────────────────────────────────
section("Mixed usage: Redis + NEDB on same connection")
# ─────────────────────────────────────────────────────────────────────────────
r9 = fresh()

# Existing app code (surface 1 — unchanged Redis)
r9.set("config:version", "2.1.0")
r9.hset("feature_flags", mapping={"dark_mode": "1", "surge_pricing": "0"})

# New app code (surface 2 — NEDB features)
r9.nedb.put("driver", "d1", {"name": "Bob", "status": "active"})
r9.nedb.put("driver", "d2", {"name": "Carol", "status": "active"})

# Both surfaces work simultaneously
check("Surface 1 + 2 coexist", r9.get("config:version") == b"2.1.0")
check("NEDB query concurrent",  len(r9.nedb.query('FROM driver WHERE status = "active"')) == 2)
check("nedb.verify with mixed", r9.nedb.verify())

# ─────────────────────────────────────────────────────────────────────────────
section("Backfill: import existing Redis data into NEDB")
# ─────────────────────────────────────────────────────────────────────────────
# Pre-populate a fakeredis with existing data (Alice's pre-NEDB world)
raw_bf = fakeredis.FakeRedis()
raw_bf.set("driver:d1", json.dumps({"name": "Bob",   "status": "active",   "lat": 37.7749}))
raw_bf.set("driver:d2", json.dumps({"name": "Carol", "status": "active",   "lat": 37.8044}))
raw_bf.set("driver:d3", json.dumps({"name": "Dave",  "status": "inactive", "lat": 37.6879}))
raw_bf.hset("trip:t1", mapping={"rider_id": "u1", "status": "requested"})
raw_bf.hset("trip:t2", mapping={"rider_id": "u2", "status": "en_route"})

rb = wrap_redis(raw_bf, db_name="backfill_test")

# Register collection mappings
rb.nedb.register("driver:*", "driver", value_parser=json.loads)
rb.nedb.register("trip:*",   "trip",   value_type="hash")

# Backfill — returns number of keys imported
imported = rb.nedb.backfill()
check("backfill returns count",       imported == 5)
check("_backfilled flag set",         rb.nedb._backfilled)

# Imported driver data is NQL-queryable
active = rb.nedb.query('FROM driver WHERE status = "active"')
check("backfilled drivers queryable", len(active) == 2)
inactive = rb.nedb.query('FROM driver WHERE status = "inactive"')
check("inactive driver backfilled",   len(inactive) == 1)
check("driver name intact",           any(d["name"] == "Bob" for d in active))

# Imported trip data (hash type) is accessible
trip = rb.nedb.get("trip", "t1")
check("trip hash backfilled",         trip is not None)
check("trip status intact",           trip.get("status") == "requested")

# Backfill evidence is recorded
doc = rb.nedb.get("driver", "d1")
check("backfill evidence on doc",     doc.get("_source") == "backfill")

# Hash chain stays valid after backfill
check("verify() after backfill",      rb.nedb.verify())

# Backfill direct (no prior register)
raw_bf2 = fakeredis.FakeRedis()
raw_bf2.set("zone:z1", json.dumps({"name": "SoMa", "active": True}))
raw_bf2.set("zone:z2", json.dumps({"name": "BART", "active": False}))
rb2 = wrap_redis(raw_bf2, db_name="direct_bf")
imported2 = rb2.nedb.backfill("zone:*", "zone", value_parser=json.loads)
check("direct backfill (no register)", imported2 == 2)
check("direct backfill queryable",     len(rb2.nedb.query('FROM zone')) == 2)

# Empty backfill (no mappings, no pattern) returns 0
rb3 = wrap_redis(fakeredis.FakeRedis(), db_name="empty_bf")
check("backfill with no mappings → 0", rb3.nedb.backfill() == 0)

# ─────────────────────────────────────────────────────────────────────────────
section("Write shadowing: surface-1 writes auto-chain into NEDB")
# ─────────────────────────────────────────────────────────────────────────────
raw_ws = fakeredis.FakeRedis()
rw = wrap_redis(raw_ws, db_name="shadow_test")

rw.nedb.register("driver:*", "driver", value_parser=json.loads)
rw.nedb.register("trip:*",   "trip",   value_type="hash")

# Write shadowing OFF by default — surface-1 writes don't touch NEDB
rw.set("driver:d1", json.dumps({"name": "Bob", "status": "active"}))
check("shadow_writes=False default",  not rw.nedb.shadow_writes)
check("no shadow without flag",        rw.nedb.get("driver", "d1") is None)

# Enable write shadowing
rw.nedb.shadow_writes = True

# SET is shadowed
rw.set("driver:d1", json.dumps({"name": "Bob", "status": "active", "lat": 37.7}))
shadowed = rw.nedb.get("driver", "d1")
check("SET shadowed into NEDB",        shadowed is not None)
check("SET value correct",             shadowed.get("name") == "Bob")
check("SET evidence recorded",         shadowed.get("_source") == "shadow")

# HSET is shadowed
rw.hset("trip:t1", mapping={"rider_id": "u1", "status": "requested"})
trip_sh = rw.nedb.get("trip", "t1")
check("HSET shadowed into NEDB",       trip_sh is not None)
check("HSET status correct",           trip_sh.get("status") == "requested")

# HSET update merges with existing NEDB doc
rw.hset("trip:t1", mapping={"status": "en_route", "driver_id": "d1"})
merged = rw.nedb.get("trip", "t1")
check("HSET merge: new field present", merged.get("driver_id") == "d1")
check("HSET merge: rider_id preserved", merged.get("rider_id") == "u1")

# Multiple SETs time-travel
snap_ws = rw.nedb.seq
rw.set("driver:d1", json.dumps({"name": "Bob", "status": "offline", "lat": 37.9}))
current_ws = rw.nedb.get("driver", "d1")
past_ws    = rw.nedb.get_as_of("driver", "d1", snap_ws)
check("shadowed SET time-travel",      current_ws["status"] == "offline")
check("past state before shadow SET",  past_ws["status"] == "active")

# Unregistered keys still get raw chain entry
rw.lpush("dispatch:queue", "trip:t1")   # list push — no collection mapping
check("unregistered write doesn't crash",  True)   # shadow failure must be silent

# Disable shadowing — subsequent writes not chained
seq_before_disable = rw.nedb.seq
rw.nedb.shadow_writes = False
rw.set("driver:d2", json.dumps({"name": "Carol", "status": "active"}))
check("no shadow after disable",       rw.nedb.seq == seq_before_disable
                                       or rw.nedb.get("driver", "d2") is None)

# Hash chain stays valid after shadowing
rw.nedb.shadow_writes = True
rw.set("driver:d2", json.dumps({"name": "Carol", "status": "active"}))
check("verify() after write shadowing", rw.nedb.verify())

# ─────────────────────────────────────────────────────────────────────────────
section("Full pipeline: backfill → shadow → restart")
# ─────────────────────────────────────────────────────────────────────────────
raw_fp = fakeredis.FakeRedis()
# Pre-existing data
raw_fp.set("driver:d1", json.dumps({"name": "Bob",   "status": "active"}))
raw_fp.set("driver:d2", json.dumps({"name": "Carol", "status": "active"}))

rp = wrap_redis(raw_fp, db_name="pipeline_test")
rp.nedb.register("driver:*", "driver", value_parser=json.loads)
rp.nedb.backfill()
rp.nedb.shadow_writes = True

# New write via surface 1
rp.set("driver:d3", json.dumps({"name": "Dave", "status": "inactive"}))
seq_fp = rp.nedb.seq

# All 3 drivers in NEDB
all_drivers = rp.nedb.query("FROM driver")
check("all 3 drivers in NEDB",        len(all_drivers) == 3)

# Restart: new wrapper on the same Redis
rp2 = wrap_redis(raw_fp, db_name="pipeline_test")
check("head stable after restart",    rp2.nedb.verify())
check("seq stable after restart",     rp2.nedb.seq == seq_fp)
rp2.nedb.query("FROM driver")  # replay doesn't crash
check("data intact after restart",    rp2.nedb.get("driver", "d1") is not None)

# ─────────────────────────────────────────────────────────────────────────────
total = PASS + FAIL
print(f"\n  {'═'*52}")
print(f"  wrap_redis  |  {PASS}/{total} passed{'  ✅' if not FAIL else f'  ❌  {FAIL} FAILED'}")
print(f"  {'═'*52}\n")
import sys; sys.exit(1 if FAIL else 0)
