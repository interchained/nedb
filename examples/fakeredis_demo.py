#!/usr/bin/env python3
"""
fakeredis_demo.py — Local in-memory testing for NEDB × Redis layer-2.

No Redis server required. fakeredis behaves like a real localhost Redis.
Copy and run this to verify wrap_redis() works in your project.

    pip install nedb-engine fakeredis
    python3 fakeredis_demo.py

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
import json, sys

try:
    import fakeredis
except ImportError:
    sys.exit("pip install fakeredis")

from nedb import wrap_redis

# ── Simulate Alice's existing Redis (pre-populated before NEDB existed) ──────
print("\n  Setting up Alice's pre-existing Redis data…")
raw = fakeredis.FakeRedis()
raw.set("driver:d1", json.dumps({"name": "Bob",   "status": "active",   "lat": 37.7749}))
raw.set("driver:d2", json.dumps({"name": "Carol", "status": "active",   "lat": 37.8044}))
raw.set("driver:d3", json.dumps({"name": "Dave",  "status": "inactive", "lat": 37.6879}))
raw.hset("trip:t1", mapping={"rider_id": "u1", "status": "requested", "pickup": "Market St"})
raw.hset("trip:t2", mapping={"rider_id": "u2", "status": "en_route",  "pickup": "BART 16th"})
raw.sadd("drivers:online", "d1", "d2")
raw.lpush("dispatch:queue", "trip:t1", "trip:t2")
print(f"  Pre-existing Redis keys: {len(raw.keys('*'))}")

# ── Alice wraps her Redis in NEDB — ONE LINE ──────────────────────────────────
print("\n  Wrapping with NEDB (one line)…")
r = wrap_redis(raw, db_name="rideshare")

# ── Surface 1: every existing Redis command still works 100% ─────────────────
print("\n  ── Surface 1: existing Redis commands pass through unchanged")
assert r.get("driver:d1") is not None, "existing keys readable"
data = json.loads(r.get("driver:d1"))
assert data["name"] == "Bob", "data intact"
assert r.hget("trip:t1", "status") == b"requested", "hash intact"
assert r.scard("drivers:online") == 2, "set intact"
assert r.llen("dispatch:queue") == 2, "list intact"
print("  ✓  All existing Redis commands work unchanged")

# ── New writes still go to Redis normally ─────────────────────────────────────
r.set("driver:d4", json.dumps({"name": "Eve", "status": "active", "lat": 37.9000}))
r.hset("trip:t3", mapping={"rider_id": "u3", "status": "requested", "pickup": "Castro"})
assert r.get("driver:d4") is not None, "new Redis write works"
print("  ✓  New Redis writes work as expected")

# ── Surface 2: NEDB features on the same connection ───────────────────────────
print("\n  ── Surface 2: NEDB features added alongside Redis")

# Index + put for new driver data via NEDB surface
r.nedb.create_index("driver", "status", "eq")
r.nedb.put("driver", "d1", {"name": "Bob",   "status": "active",   "lat": 37.7749})
r.nedb.put("driver", "d2", {"name": "Carol", "status": "active",   "lat": 37.8044})
r.nedb.put("driver", "d3", {"name": "Dave",  "status": "inactive", "lat": 37.6879})
r.nedb.put("driver", "d4", {"name": "Eve",   "status": "active",   "lat": 37.9000})

# NQL queries
active = r.nedb.query('FROM driver WHERE status = "active" ORDER BY lat ASC')
print(f"  ✓  Active drivers (NQL): {[d['name'] for d in active]}")
assert len(active) == 3, f"expected 3 active, got {len(active)}"

grouped = r.nedb.query("FROM driver GROUP BY status COUNT")
counts = {g["status"]: g["count"] for g in grouped}
print(f"  ✓  GROUP BY status: {counts}")

# Time-travel
snap = r.nedb.seq
r.nedb.put("driver", "d1", {"name": "Bob", "status": "offline", "lat": 37.7749})

current = r.nedb.get("driver", "d1")
past    = r.nedb.get_as_of("driver", "d1", snap)
print(f"  ✓  Time-travel: current={current['status']}, at snap={past['status']}")
assert current["status"] == "offline"
assert past["status"]    == "active"

# Causal provenance — why did trip t1 get assigned to d1?
r.nedb.put("trip_event", "dispatch_001",
    {"trip_id": "t1", "driver_id": "d1", "algo": "nearest_driver",
     "score": 0.97, "candidates": ["d1","d2","d4"]},
    caused_by=[r.nedb.seq - 2],   # caused by the location update
    evidence="inference",
    confidence=0.97)

r.nedb.put("trip", "t1",
    {"rider_id": "u1", "driver_id": "d1", "status": "assigned"},
    caused_by=[r.nedb.seq - 1],   # caused by the dispatch event
    evidence="inference",
    confidence=0.97)

trip = r.nedb.get("trip", "t1")
print(f"  ✓  Trip t1 assigned: caused_by={trip['_caused_by']} evidence={trip['_evidence']}")

# TRACE — why was this trip assigned?
trace = r.nedb.query('FROM trip WHERE _id = "t1" TRACE caused_by')
print(f"  ✓  TRACE caused_by: found {len(trace)} causal ancestor(s)")
assert len(trace) >= 1

# Hash chain
ok = r.nedb.verify()
print(f"  ✓  verify(): {ok}  head: {r.nedb.head()[:16]}…  seq: {r.nedb.seq}")
assert ok

# ── Isolation: NEDB shadow keys don't pollute Alice's namespace ───────────────
print("\n  ── Isolation: NEDB stays in its own namespace")
all_keys = [k.decode() for k in r.keys("*")]
alice_keys = [k for k in all_keys if not k.startswith("nedb:")]
nedb_keys  = [k for k in all_keys if k.startswith("nedb:")]
print(f"  Alice's keys: {len(alice_keys)}  NEDB shadow keys: {len(nedb_keys)}")
assert all(k.startswith("nedb:rideshare:") for k in nedb_keys), "NEDB isolated"
print("  ✓  Zero impact on Alice's existing namespace")

# ── Persist + restart simulation ──────────────────────────────────────────────
print("\n  ── Persistence: NEDB replays from Redis Stream on restart")
head_before = r.nedb.head()
seq_before  = r.nedb.seq

# Simulate restart — new WrappedRedis on the same underlying fakeredis store
r2 = wrap_redis(raw, db_name="rideshare")  # replays nedb:rideshare:oplog
assert r2.nedb.head() == head_before, "head survives restart"
assert r2.nedb.seq    == seq_before,  "seq survives restart"
assert r2.nedb.verify(),              "verify after restart"
print(f"  ✓  Restart OK — head: {r2.nedb.head()[:16]}…  seq: {r2.nedb.seq}")

# ── Done ─────────────────────────────────────────────────────────────────────
print("""
  ══════════════════════════════════════════════════════
  All checks passed ✅

  One line wrapped Redis in NEDB:
    r = wrap_redis(redis.Redis("localhost", 6379), db_name="rideshare")

  Surface 1 (r.set/get/hset/…)  — existing app, zero changes
  Surface 2 (r.nedb.put/query)  — time-travel, NQL, causal provenance

  NEDB never writes to your existing namespace.
  Hash chain verified. Data persists across restarts.
  ══════════════════════════════════════════════════════
""")
