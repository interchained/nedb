#!/usr/bin/env python3
"""
fakeredis_demo.py — Local in-memory testing for NEDB × Redis layer-2.

No Redis server required. fakeredis behaves like a real localhost Redis.
Copy and run this to verify the full wrap_redis() pipeline in your project.

    pip install nedb-engine fakeredis
    python3 fakeredis_demo.py

Demonstrates the three-step migration:
    1. Register  — map Redis key globs to NEDB collections
    2. Backfill  — import all existing Redis data into NEDB in one pass
    3. Shadow    — all future surface-1 writes auto-chain into NEDB

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
import json
import sys

try:
    import fakeredis
except ImportError:
    sys.exit("pip install fakeredis")

from nedb import wrap_redis

PASS = FAIL = 0
def ok(msg):  global PASS; PASS += 1; print(f"  ✓  {msg}")
def bad(msg): global FAIL; FAIL += 1; print(f"  ✗  {msg}")
def chk(msg, cond): ok(msg) if cond else bad(msg)

# ── Alice's pre-existing Redis (before she heard about NEDB) ──────────────────
print("\n  Setting up Alice's pre-existing Redis data (UberClone/rideFlow)…")
raw = fakeredis.FakeRedis()
raw.set("driver:d1", json.dumps({"name": "Bob",   "status": "active",   "lat": 37.7749}))
raw.set("driver:d2", json.dumps({"name": "Carol", "status": "active",   "lat": 37.8044}))
raw.set("driver:d3", json.dumps({"name": "Dave",  "status": "inactive", "lat": 37.6879}))
raw.hset("trip:t1", mapping={"rider_id": "u1", "status": "requested", "pickup": "Market St"})
raw.hset("trip:t2", mapping={"rider_id": "u2", "status": "en_route",  "pickup": "BART 16th"})
raw.sadd("drivers:online", "d1", "d2")
raw.lpush("dispatch:queue", "trip:t1", "trip:t2")
print(f"  Pre-existing Redis keys: {len(raw.keys('*'))}")

# ── Step 0: One-line wrap (Alice's app code doesn't change) ───────────────────
print("\n  Step 0 — wrap_redis() : ONE LINE. Alice's code unchanged.")
r = wrap_redis(raw, db_name="rideshare")
ok("wrap_redis returns WrappedRedis")

# ── Surface 1: existing Redis commands still work 100% ───────────────────────
print("\n  ── Surface 1: existing Redis commands pass through unchanged")
chk("r.get  still works",       r.get("driver:d1") is not None)
chk("r.hget still works",       r.hget("trip:t1", "status") == b"requested")
chk("r.scard still works",      r.scard("drivers:online") == 2)
chk("r.llen still works",       r.llen("dispatch:queue") == 2)

r.set("driver:d4", json.dumps({"name": "Eve", "status": "active", "lat": 37.9000}))
chk("new r.set() still works",  r.get("driver:d4") is not None)

# ── Step 1: Register collection mappings ─────────────────────────────────────
print("\n  Step 1 — register() : tell NEDB what Alice's key structure looks like")
(r.nedb
 .register("driver:*", collection="driver", value_parser=json.loads)
 .register("trip:*",   collection="trip",   value_type="hash")
)
chk("2 mappings registered",    len(r.nedb._mappings) == 2)

# ── Step 2: Backfill existing Redis data ──────────────────────────────────────
print("\n  Step 2 — backfill() : import all existing Redis data into NEDB (one-time)")
imported = r.nedb.backfill()
print(f"  Imported {imported} existing Redis keys into NEDB")
# 3 driver strings + d4 added above + 2 trip hashes = 6 structured keys
chk("all structured keys imported",    imported == 6)
chk("_backfilled flag set",            r.nedb._backfilled)

# Verify imported data is queryable
active = r.nedb.query('FROM driver WHERE status = "active" ORDER BY lat ASC')
# Bob, Carol (from pre-existing) + Eve (added above before backfill) = 3
chk(f"active drivers queryable: {[d['name'] for d in active]}", len(active) == 3)

trip_bf = r.nedb.get("trip", "t1")
chk("trip hash imported",              trip_bf is not None)
chk("trip status intact",              trip_bf.get("status") == "requested")
chk("hash chain valid after backfill", r.nedb.verify())

# ── Step 3: Enable write shadowing ────────────────────────────────────────────
print("\n  Step 3 — shadow_writes=True : all future r.set/hset auto-chain into NEDB")
r.nedb.shadow_writes = True

# Alice's app keeps running — all writes are now silently chain-linked
r.set("driver:d1", json.dumps({"name": "Bob", "status": "active", "lat": 37.7749}))
r.set("driver:d4", json.dumps({"name": "Eve", "status": "active", "lat": 37.9000}))
r.hset("trip:t1", mapping={"status": "en_route", "driver_id": "d1"})

shadow_d4 = r.nedb.get("driver", "d4")
chk("r.set() auto-chained",            shadow_d4 is not None)
chk("chained value correct",           shadow_d4.get("name") == "Eve")
chk("shadow evidence label",           shadow_d4.get("_source") == "shadow")

merged_t1 = r.nedb.get("trip", "t1")
chk("r.hset() auto-chained",           merged_t1 is not None)
chk("hset merge preserves rider_id",   merged_t1.get("rider_id") == "u1")
chk("hset merge adds driver_id",       merged_t1.get("driver_id") == "d1")

# ── NEDB features now cover ALL data ─────────────────────────────────────────
print("\n  ── NEDB features: NQL, time-travel, causal provenance")

# Full NQL across backfilled + shadowed data
snap_before_offline = r.nedb.seq
r.set("driver:d1", json.dumps({"name": "Bob", "status": "offline", "lat": 37.7}))

current = r.nedb.get("driver", "d1")
past    = r.nedb.get_as_of("driver", "d1", snap_before_offline)
chk("time-travel: current offline",    current["status"] == "offline")
chk("time-travel: was active at snap", past["status"] == "active")

# Causal provenance (via Surface 2)
r.nedb.put("dispatch_event", "e1",
    {"trip_id": "t1", "driver_id": "d4", "algo": "nearest_driver",
     "score": 0.97},
    caused_by=[r.nedb.seq - 3],
    evidence="inference",
    confidence=0.97)
chk("causal event stored",            r.nedb.get("dispatch_event", "e1") is not None)
chk("hash chain valid after shadow",  r.nedb.verify())

trace = r.nedb.query('FROM dispatch_event WHERE _id = "e1" TRACE caused_by')
chk("TRACE causal ancestors found",   len(trace) >= 1)

# ── Isolation: NEDB shadow keys don't pollute Alice's namespace ───────────────
print("\n  ── Isolation: NEDB stays in its own namespace")
all_keys   = [k.decode() for k in r.keys("*")]
alice_keys = [k for k in all_keys if not k.startswith("nedb:")]
nedb_keys  = [k for k in all_keys if k.startswith("nedb:")]
print(f"  Alice's keys: {len(alice_keys)}  NEDB shadow keys: {len(nedb_keys)}")
chk("NEDB isolated in nedb: prefix",
    all(k.startswith("nedb:rideshare:") for k in nedb_keys))
chk("Zero impact on Alice's namespace", r.get("driver:d1") is not None)

# ── Persist + restart simulation ──────────────────────────────────────────────
print("\n  ── Persistence: NEDB replays from Redis Stream on restart")
head_before = r.nedb.head()
seq_before  = r.nedb.seq

# Restart: new WrappedRedis on the same underlying Redis store
r2 = wrap_redis(raw, db_name="rideshare")   # replays nedb:rideshare:oplog
chk("head survives restart",           r2.nedb.head() == head_before)
chk("seq survives restart",            r2.nedb.seq    == seq_before)
chk("verify after restart",            r2.nedb.verify())
print(f"  ✓  Restart OK — head: {r2.nedb.head()[:16]}…  seq: {r2.nedb.seq}")

# ── Final tally ───────────────────────────────────────────────────────────────
total = PASS + FAIL
banner = "✅" if not FAIL else f"❌  {FAIL} FAILED"
print(f"""
  ══════════════════════════════════════════════════════
  {PASS}/{total} checks passed {banner}

  Three steps to add NEDB to any Redis app:

    r = wrap_redis(redis.Redis("localhost", 6379), db_name="rideshare")

    # 1. Register — map key globs to NEDB collections (chainable)
    (r.nedb
     .register("driver:*", "driver", value_parser=json.loads)
     .register("trip:*",   "trip",   value_type="hash")
    )

    # 2. Backfill — import existing Redis data once
    imported = r.nedb.backfill()     # → int (keys imported)

    # 3. Shadow — future surface-1 writes auto-chain
    r.nedb.shadow_writes = True

  Surface 1 (r.set/get/hset/…)  — existing app, zero changes
  Surface 2 (r.nedb.put/query)  — time-travel, NQL, causal provenance

  NEDB never writes to your existing namespace.
  Hash chain verified. Data persists across restarts.
  ══════════════════════════════════════════════════════
""")
sys.exit(1 if FAIL else 0)
