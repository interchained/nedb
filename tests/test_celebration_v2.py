#!/usr/bin/env python3
"""
test_celebration_v2.py — Incident Report #4471 on the v2 DAG Engine

Same rideshare story as test_celebration.py, but running directly on the
v2 content-addressed DAG via nedb._native (NedbCore) — no HTTP, no Redis.

What's different from v1:
  ✦ caused_by links use content HASHES not seq numbers
  ✦ TRAVERSE follows named relations stored in __links__
  ✦ Every node has a _hash — cryptographic identity
  ✦ Instant cold start — no AOF replay, reads MANIFEST

Run: pip install nedb-engine && python3 tests/test_celebration_v2.py

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
import json, sys

try:
    from nedb._native import NedbCore
    import nedb as _nedb
    print(f"\n  nedb-engine {_nedb.__version__}  |  native: {_nedb.__has_native__}")
except ImportError:
    sys.exit("nedb._native not available — install a platform wheel: pip install nedb-engine")

PASS = FAIL = 0
def ok(msg):  global PASS; PASS += 1; print(f"  ✓  {msg}")
def bad(msg): global FAIL; FAIL += 1; print(f"  ✗  FAIL: {msg}")
def chk(msg, cond): ok(msg) if cond else bad(msg)
def banner(t): pad = (58-len(t))//2; print(f"\n  {'─'*pad} {t} {'─'*pad}")
def doc(r): return json.loads(r) if r else {}

print("""
  ╔══════════════════════════════════════════════════════════╗
  ║            INCIDENT REPORT #4471  ·  v2 DAG              ║
  ║      "Why was I assigned a 2.1-rated driver?"            ║
  ║                                                          ║
  ║  Content-addressed. Tamper-evident. Causal by design.    ║
  ╚══════════════════════════════════════════════════════════╝
""")

# ─────────────────────────────────────────────────────────────────────────────
banner("SCENE 1 — Fleet data enters the v2 DAG")
# ─────────────────────────────────────────────────────────────────────────────
db = NedbCore()   # in-memory v2 DAG — zero disk I/O
db.create_index("driver",   "status", "eq")
db.create_index("driver",   "rating", "ordered")

# Each put returns a JSON string containing _hash — the content address
r_bob   = json.loads(db.put("driver", "d1", json.dumps({"name": "Bob",   "rating": 4.9, "status": "active", "lat": 37.7750, "lng": -122.4195})))
r_dave  = json.loads(db.put("driver", "d3", json.dumps({"name": "Dave",  "rating": 2.1, "status": "active", "lat": 37.7752, "lng": -122.4181})))
r_carol = json.loads(db.put("driver", "d2", json.dumps({"name": "Carol", "rating": 4.7, "status": "active", "lat": 37.8050, "lng": -122.2710})))

# In v2, causal links use content hashes — immutable, cryptographic
hash_bob   = r_bob["_hash"]
hash_dave  = r_dave["_hash"]
hash_carol = r_carol["_hash"]

chk("Bob   entered DAG — hash is 64 hex chars", len(hash_bob) == 64)
chk("Dave  entered DAG — hash is 64 hex chars", len(hash_dave) == 64)
chk("Carol entered DAG — hash is 64 hex chars", len(hash_carol) == 64)
chk("All three drivers queryable", len(db.query('FROM driver WHERE status = "active"')) == 3)

# ─────────────────────────────────────────────────────────────────────────────
banner("SCENE 2 — Rider request + dispatch decision")
# ─────────────────────────────────────────────────────────────────────────────
r_req = json.loads(db.put("trip_request", "req_001", json.dumps({
    "rider_id": "r1", "pickup": "Market St & 5th",
    "lat": 37.7750, "lng": -122.4194, "requested_at": "21:47:03",
})))
hash_req = r_req["_hash"]

# Dispatch algorithm — causal parents are the HASHES of the GPS pings + request
r_disp = json.loads(db.put("dispatch_decision", "disp_001", json.dumps({
    "algo":           "nearest_driver_v1",
    "candidates":     ["d3", "d1", "d2"],
    "distances_mi":   {"d3": 0.04, "d1": 0.10, "d2": 2.14},
    "winner":         "d3",
    "reason":         "minimum_distance",
    "rating_checked": False,                   # ← the smoking gun
    "caused_by": [hash_dave, hash_bob, hash_carol, hash_req],  # v2: hashes
})))
hash_disp = r_disp["_hash"]

chk("dispatch decision in DAG — hash assigned", len(hash_disp) == 64)

# ─────────────────────────────────────────────────────────────────────────────
banner("SCENE 3 — Trip assignment + completion event")
# ─────────────────────────────────────────────────────────────────────────────
r_trip = json.loads(db.put("trip", "trip_001", json.dumps({
    "rider_id": "r1", "driver_id": "d3", "status": "assigned",
    "assigned_at": "21:47:04",
    "caused_by": [hash_disp],
})))
hash_trip = r_trip["_hash"]

r_ev = json.loads(db.put("trip_event", "ev_002", json.dumps({
    "trip_id": "trip_001", "event": "completed",
    "rating": 2, "complaint": "Driver had 2.1 stars — why was I matched?",
    "caused_by": [hash_trip],
})))

chk("trip assigned — hash recorded", len(hash_trip) == 64)
chk("complaint event in DAG",        r_ev.get("_hash") is not None)

# ─────────────────────────────────────────────────────────────────────────────
banner("SCENE 4 — TRACE the causal chain (by hash)")
# ─────────────────────────────────────────────────────────────────────────────
print()
print("  TRACE caused_by from complaint → back to GPS pings...\n")

# In v2, TRACE walks the graph by hash — no seq needed
trace_rows = db.query('FROM trip_event WHERE _id = "ev_002" TRACE caused_by')
trace = [doc(r) for r in trace_rows]

print(f"  Found {len(trace)} causal ancestors:\n")
for node in sorted(trace, key=lambda x: x.get("_seq", 0)):
    nid    = node.get("_id", "?")
    causes = node.get("caused_by", [])
    if nid == "disp_001":
        print(f"  [inference]    DISPATCH  algo={node.get('algo')}  rating_checked={node.get('rating_checked')}")
    elif nid == "req_001":
        print(f"  [observation]  RIDER REQUEST  pickup={node.get('pickup','?')}")
    elif nid in ("d1","d2","d3"):
        name   = node.get("name","?")
        rating = node.get("rating","?")
        dist   = {"d1":0.10,"d2":2.14,"d3":0.04}.get(nid,"?")
        print(f"  [observation]  GPS PING  driver={name:<6} rating={rating}  dist={dist}mi")
    else:
        print(f"  [?]            {nid}")

chk("TRACE found dispatch decision",      any(doc(r).get("_id") == "disp_001" for r in trace_rows))
chk("TRACE found GPS pings (3 drivers)",  sum(1 for r in trace_rows if doc(r).get("_id") in ("d1","d2","d3")) == 3)
chk("TRACE found rider request",          any(doc(r).get("_id") == "req_001"  for r in trace_rows))

# ─────────────────────────────────────────────────────────────────────────────
banner("SCENE 5 — Time-travel: what the algorithm actually saw")
# ─────────────────────────────────────────────────────────────────────────────
dave_at_t0 = doc(db.get("driver", "d3", as_of=r_dave["_seq"]))
bob_at_t0  = doc(db.get("driver", "d1", as_of=r_bob["_seq"]))

print(f"\n  At dispatch time:")
print(f"    Dave  — rating: {dave_at_t0.get('rating')}   dist: 0.04mi  ← chosen")
print(f"    Bob   — rating: {bob_at_t0.get('rating')}   dist: 0.10mi  ← skipped\n")

chk("time-travel: Dave's rating was 2.1 at assignment", dave_at_t0.get("rating") == 2.1)
chk("time-travel: Bob was 4.9 — should have been chosen", bob_at_t0.get("rating") == 4.9)

# ─────────────────────────────────────────────────────────────────────────────
banner("SCENE 6 — TRAVERSE: driver–trip relations")
# ─────────────────────────────────────────────────────────────────────────────
# v2 TRAVERSE uses __links__ collection — queryable, time-travelable
db.link("driver:d3", "assigned_to", "trip:trip_001")
db.link("driver:d1", "candidate_for", "trip:trip_001")
db.link("driver:d2", "candidate_for", "trip:trip_001")

dave_trips = db.neighbors("driver:d3", "assigned_to")
candidates = db.neighbors("trip:trip_001", "candidate_for_rev")   # inbound check

chk("TRAVERSE: Dave assigned to trip_001",      "trip:trip_001" in dave_trips)
chk("2 candidates linked to trip via relation",
    len(db.neighbors("driver:d1", "candidate_for")) + len(db.neighbors("driver:d2", "candidate_for")) == 2)

# NQL TRAVERSE — v2.2.8: link() uses __links__ collection, fully consistent
traverse_rows = [doc(r) for r in db.query('FROM driver WHERE _id = "d3" TRAVERSE assigned_to')]
chk("NQL TRAVERSE finds Dave→trip_001", any(r.get("_id") == "trip_001" for r in traverse_rows))

# ─────────────────────────────────────────────────────────────────────────────
banner("SCENE 7 — Tamper-evident proof")
# ─────────────────────────────────────────────────────────────────────────────
verified = db.verify()
head     = db.head()

chk("verify() — every object hash checks out", verified)
chk("head is 64-char BLAKE2b chain commitment", len(head) == 64)

print(f"\n  Chain head: {head[:32]}...")
print(f"  Seq:        {db.seq()}")

# ─────────────────────────────────────────────────────────────────────────────
total = PASS + FAIL
print(f"""
  ══════════════════════════════════════════════════════════
  {PASS}/{total} checks passed {'✅' if not FAIL else f'❌  {FAIL} FAILED'}

  VERDICT (v2 DAG):
  nearest_driver_v1 did NOT check driver ratings.
  Dave (2.1★, 0.04mi) beat Bob (4.9★, 0.10mi) on distance alone.

  Every fact is content-addressed. Every cause is hash-linked.
  No seq numbers. No mutable log. Nothing to forge.

  Head: {head[:32]}...

  Built by INTERCHAINED LLC × Claude Sonnet 4.6
  ══════════════════════════════════════════════════════════
""")
sys.exit(1 if FAIL else 0)
