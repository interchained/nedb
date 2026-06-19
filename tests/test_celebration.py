#!/usr/bin/env python3
"""
test_celebration.py — The Incident Report

A rider files a complaint: "Why was I assigned Dave? He had a 2.1 rating."
NEDB proves exactly what happened — which algorithm ran, what data it saw,
why it chose Dave — and that nothing was tampered with after the fact.

Full stack:
  ✦ wrap_redis()     — one-line wrap of the existing rideshare Redis
  ✦ backfill()       — import Alice's historical driver data
  ✦ shadow_writes    — capture live dispatch events
  ✦ caused_by DAG    — causal chain from sensor → dispatch → assignment
  ✦ TRACE            — full audit trail for the complaint
  ✦ get_as_of        — what did the algorithm actually see at T=0?
  ✦ verify() + head  — tamper-evident proof, courtroom-ready

Run: pip install nedb-engine fakeredis && python3 tests/test_celebration.py

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
import json, sys, time

try:
    import fakeredis
except ImportError:
    sys.exit("pip install fakeredis")

from nedb import wrap_redis

PASS = FAIL = 0
def ok(msg):   global PASS; PASS += 1; print(f"  ✓  {msg}")
def bad(msg):  global FAIL; FAIL += 1; print(f"  ✗  FAIL: {msg}")
def chk(msg, cond): ok(msg) if cond else bad(msg)

def banner(title):
    pad = (60 - len(title)) // 2
    print(f"\n  {'─'*pad} {title} {'─'*pad}")

# ─────────────────────────────────────────────────────────────────────────────
print("""
  ╔══════════════════════════════════════════════════════════╗
  ║                   INCIDENT REPORT #4471                  ║
  ║        "Why was I assigned a 2.1-rated driver?"          ║
  ║                                                          ║
  ║  NEDB will reconstruct the full causal chain.            ║
  ║  Every fact is hash-chained. Nothing can be altered.     ║
  ╚══════════════════════════════════════════════════════════╝
""")

# ─────────────────────────────────────────────────────────────────────────────
banner("SCENE 1 — UberClone/rideFlow is live on Redis")
# ─────────────────────────────────────────────────────────────────────────────
raw = fakeredis.FakeRedis()

# Alice's existing Redis — populated before NEDB existed
raw.set("driver:d1", json.dumps({"name": "Bob",   "rating": 4.9, "status": "active",   "lat": 37.7749, "lng": -122.4194}))
raw.set("driver:d2", json.dumps({"name": "Carol",  "rating": 4.7, "status": "active",   "lat": 37.8044, "lng": -122.2708}))
raw.set("driver:d3", json.dumps({"name": "Dave",   "rating": 2.1, "status": "active",   "lat": 37.7751, "lng": -122.4180}))
raw.hset("rider:r1",  mapping={"name": "Mark", "rating": 4.8, "preferred_min_rating": "4.0"})
raw.set("zone:downtown", json.dumps({"surge": 1.4, "demand": "high"}))

print(f"  UberClone Redis keys: {len(raw.keys('*'))}")

# ─────────────────────────────────────────────────────────────────────────────
banner("SCENE 2 — NEDB wraps Redis in one line")
# ─────────────────────────────────────────────────────────────────────────────
r = wrap_redis(raw, db_name="rideflow")

r.nedb.register("driver:*", "driver", value_parser=json.loads)
r.nedb.register("zone:*",   "zone",   value_parser=json.loads)

imported = r.nedb.backfill()
chk(f"backfilled {imported} existing keys into NEDB", imported >= 4)

r.nedb.shadow_writes = True
chk("shadow_writes enabled — all future Redis writes are now chain-linked", r.nedb.shadow_writes)

# ─────────────────────────────────────────────────────────────────────────────
banner("SCENE 3 — The incident: Mark requests a ride at 9:47 PM")
# ─────────────────────────────────────────────────────────────────────────────

# Step 1: GPS pings arrive — shadowed into NEDB automatically
r.set("driver:d1", json.dumps({"name": "Bob",  "rating": 4.9, "status": "active", "lat": 37.7750, "lng": -122.4195}))
seq_bob_ping  = r.nedb.seq

r.set("driver:d3", json.dumps({"name": "Dave", "rating": 2.1, "status": "active", "lat": 37.7752, "lng": -122.4181}))
seq_dave_ping = r.nedb.seq

r.set("driver:d2", json.dumps({"name": "Carol","rating": 4.7, "status": "active", "lat": 37.8050, "lng": -122.2710}))
seq_carol_ping = r.nedb.seq

chk("GPS pings shadowed into NEDB chain", r.nedb.get("driver", "d3") is not None)

# Step 2: Rider request — via surface 2 (new feature code)
r.nedb.put("trip_request", "req_001", {
    "rider_id":    "r1",
    "pickup":      "Market St & 5th",
    "lat":         37.7750,
    "lng":         -122.4194,
    "requested_at": "21:47:03",
}, evidence="observation")
seq_request = r.nedb.seq

chk("rider request recorded with provenance", r.nedb.get("trip_request", "req_001") is not None)

# Step 3: Dispatch algorithm runs — CAUSAL RECORD of its decision
# The algorithm saw: Dave (0.04mi away), Bob (0.1mi away), Carol (2.1mi away)
# It chose Dave by distance alone — ignoring the 2.1 rating. BUG.
r.nedb.put("dispatch_decision", "disp_001", {
    "algo":           "nearest_driver_v1",      # ← the smoking gun
    "rider_id":       "r1",
    "candidates":     ["d3", "d1", "d2"],
    "distances_mi":   {"d3": 0.04, "d1": 0.10, "d2": 2.14},
    "winner":         "d3",
    "reason":         "minimum_distance",
    "rating_checked": False,                    # ← did not filter by rating
},
    caused_by=[seq_dave_ping, seq_bob_ping, seq_carol_ping, seq_request],
    evidence="inference",
    confidence=0.99)
seq_dispatch = r.nedb.seq

chk("dispatch decision stored with full causal ancestry", r.nedb.get("dispatch_decision", "disp_001") is not None)

# Step 4: Trip assignment — caused by the dispatch decision
r.nedb.put("trip", "trip_001", {
    "rider_id":    "r1",
    "driver_id":   "d3",
    "status":      "assigned",
    "pickup":      "Market St & 5th",
    "assigned_at": "21:47:04",
},
    caused_by=[seq_dispatch],
    evidence="inference",
    confidence=1.0)
seq_assignment = r.nedb.seq

chk("trip assigned — causal link to dispatch decision", r.nedb.get("trip", "trip_001") is not None)

# Trip completes — 2-star rating from Mark
# Trip lifecycle events are separate documents — cleaner causality chain
r.nedb.put("trip_event", "ev_002", {
    "trip_id":   "trip_001",
    "event":     "completed",
    "rating":    2,
    "complaint": "Driver had 2.1 stars — why was I matched?",
},
    caused_by=[seq_assignment],
    evidence="observation",
    confidence=1.0)

chk("trip completion event stored as separate causal node",
    r.nedb.get("trip_event", "ev_002") is not None)

# ─────────────────────────────────────────────────────────────────────────────
banner("SCENE 4 — The investigation: TRACE the full causal chain")
# ─────────────────────────────────────────────────────────────────────────────
print()
print("  Complaint received. Running TRACE from trip_001 backward...")
print()

# TRACE from the complaint event — follows the full causal chain back to GPS pings
trace = r.nedb.query('FROM trip_event WHERE _id = "ev_002" TRACE caused_by')

print(f"  Found {len(trace)} causal ancestors:\n")
for node in sorted(trace, key=lambda x: x.get("_seq", 0)):
    ev = node.get("_evidence", "?")
    nid = node.get("_id", "?")
    src = node.get("_source", "")
    # Summarize what each ancestor represents
    if "dispatch" in nid:
        algo  = node.get("algo", "?")
        rated = node.get("rating_checked", "?")
        print(f"  [{ev:11s}]  DISPATCH DECISION  algo={algo}  rating_checked={rated}")
    elif "req_" in nid:
        print(f"  [{ev:11s}]  RIDER REQUEST      pickup={node.get('pickup','?')}")
    elif src == "shadow":
        name = node.get("name", "?")
        dist = {"d1": 0.10, "d2": 2.14, "d3": 0.04}.get(nid, "?")
        rating = node.get("rating", "?")
        print(f"  [{ev:11s}]  GPS PING           driver={name:<6} rating={rating}  dist={dist}mi")
    else:
        print(f"  [{ev:11s}]  {nid}")

chk("TRACE found dispatch decision in causal chain",
    any(d.get("_id") == "disp_001" or d.get("algo") == "nearest_driver_v1" for d in trace))
chk("TRACE found rider request",
    any("req_" in d.get("_id","") for d in trace))
chk("TRACE found GPS pings (shadowed Redis writes)",
    any(d.get("_source") == "shadow" for d in trace))

# ─────────────────────────────────────────────────────────────────────────────
banner("SCENE 5 — Time-travel: what did the algorithm actually see?")
# ─────────────────────────────────────────────────────────────────────────────
print()
print("  Rewinding to seq=" + str(seq_dave_ping) + " (moment of Dave's last GPS ping)...")

dave_at_dispatch = r.nedb.get_as_of("driver", "d3", seq_dave_ping)
bob_at_dispatch  = r.nedb.get_as_of("driver", "d1", seq_bob_ping)

print(f"\n  At the moment the algorithm ran:")
print(f"    Dave  — rating: {dave_at_dispatch.get('rating')}  status: {dave_at_dispatch.get('status')}")
print(f"    Bob   — rating: {bob_at_dispatch.get('rating')}   status: {bob_at_dispatch.get('status')}")

chk("time-travel confirms Dave's rating was 2.1 at dispatch time",
    dave_at_dispatch.get("rating") == 2.1)
chk("Bob was available with rating 4.9 — should have been chosen",
    bob_at_dispatch.get("rating") == 4.9)

dispatch = r.nedb.get("dispatch_decision", "disp_001")
chk("algo='nearest_driver_v1' confirmed — rating filter was disabled",
    dispatch.get("rating_checked") == False and dispatch.get("algo") == "nearest_driver_v1")

# ─────────────────────────────────────────────────────────────────────────────
banner("SCENE 6 — Tamper-evident proof: the chain is clean")
# ─────────────────────────────────────────────────────────────────────────────
verified = r.nedb.verify()
head     = r.nedb.head()
seq_now  = r.nedb.seq

chk("verify() — every op in the chain is intact", verified)
chk("head is a 64-char BLAKE2b commitment hash",  len(head) == 64)
print(f"\n  Chain commitment: {head[:32]}...")
print(f"  Total ops sealed: {seq_now + 1}")

# ─────────────────────────────────────────────────────────────────────────────
banner("SCENE 7 — Persist + restart (Redis stream survives)")
# ─────────────────────────────────────────────────────────────────────────────
head_before = r.nedb.head()
seq_before  = r.nedb.seq

r2 = wrap_redis(raw, db_name="rideflow")   # replay from Redis Stream
chk("head survives restart",   r2.nedb.head() == head_before)
chk("seq survives restart",    r2.nedb.seq    == seq_before)
chk("verify() after restart",  r2.nedb.verify())
chk("dispatch decision intact after restart",
    r2.nedb.get("dispatch_decision", "disp_001") is not None)

# ─────────────────────────────────────────────────────────────────────────────
total = PASS + FAIL
print(f"""
  ══════════════════════════════════════════════════════════
  {PASS}/{total} checks passed {'✅' if not FAIL else f'❌  {FAIL} FAILED'}

  VERDICT: nearest_driver_v1 did NOT check driver ratings.
  At T=21:47:04, Dave (rating 2.1, 0.04mi) was chosen over
  Bob (rating 4.9, 0.10mi) by distance alone.

  The causal chain is sealed in the NEDB hash chain.
  Head: {head[:32]}...
  This record cannot be altered without detection.

  Built by INTERCHAINED LLC × Claude Sonnet 4.6
  ══════════════════════════════════════════════════════════
""")
sys.exit(1 if FAIL else 0)
