"""v0.5.0 feature tests: snapshots, TTL, GROUP BY"""
import os, sys, shutil, tempfile, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"))

from nedb import NEDB

PASS = FAIL = 0
def check(name, cond):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  ok  {name}")
    else:    FAIL += 1; print(f"  FAIL {name}")

# ─── Snapshots ────────────────────────────────────────────────────────────────
print("\n── Snapshot checkpoints ──")

def test_checkpoint_basic():
    d = tempfile.mkdtemp()
    try:
        db = NEDB(d)
        db.create_index("users", "status", "eq")
        for i in range(200):
            db.put("users", str(i), {"name": f"User{i}", "age": i, "status": "active" if i%2==0 else "inactive"})
        snap_seq = db.seq
        head_before = db.head
        db.checkpoint()
        check("checkpoint: snapshot.json created", os.path.exists(os.path.join(d, "snapshot.json")))
        # 10 more writes after checkpoint (the "delta")
        for i in range(200, 210):
            db.put("users", str(i), {"name": f"User{i}", "age": i, "status": "active"})
        head_final, seq_final = db.head, db.seq
        db.close()

        # ── Reload from snapshot (should be fast — only 10 delta ops replayed) ──
        db2 = NEDB(d)
        check("reload: verify() holds", db2.verify())
        check("reload: head preserved", db2.head == head_final)
        check("reload: seq preserved",  db2.seq  == seq_final)
        check("reload: pre-snap data",  db2.get("users", "0")["name"] == "User0")
        check("reload: post-snap data", db2.get("users", "205")["name"] == "User205")
        check("reload: AS OF still works", db2.get("users", "0", as_of=snap_seq) is not None)
        rows = db2.query('FROM users WHERE status = "active"')
        check("reload: index works after snapshot", len(rows) > 0)
        check("reload: verify_determinism", db2.verify_determinism())
        db2.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)

def test_checkpoint_chain_continuous():
    """The checkpoint op is a real log entry — the chain runs through it."""
    d = tempfile.mkdtemp()
    try:
        db = NEDB(d)
        for i in range(50):
            db.put("k", str(i), {"v": i})
        db.checkpoint()
        for i in range(50, 60):
            db.put("k", str(i), {"v": i})
        check("chain continuous through checkpoint", db.verify())
        db.close()
        db2 = NEDB(d)
        check("chain verified after reload", db2.verify())
        db2.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)

def test_checkpoint_inmemory_raises():
    db = NEDB()
    try:
        db.checkpoint()
        check("in-memory checkpoint raises", False)
    except ValueError:
        check("in-memory checkpoint raises", True)

test_checkpoint_basic()
test_checkpoint_chain_continuous()
test_checkpoint_inmemory_raises()

# ─── TTL ─────────────────────────────────────────────────────────────────────
print("\n── TTL / expiry ──")

def test_ttl_expires():
    db = NEDB()
    db.put("cache", "k1", {"v": "hello"}, ttl_s=0.05)  # 50ms
    check("TTL: live immediately after put", db.get("cache", "k1") is not None)
    time.sleep(0.08)
    check("TTL: expired after sleep", db.get("cache", "k1") is None)
    check("TTL: delete appended to log", any(o.op == "delete" for o in db.log.ops))

def test_ttl_no_expiry():
    db = NEDB()
    db.put("k", "1", {"v": 42})
    check("no TTL: always returned", db.get("k", "1") is not None)

def test_ttl_expire_method():
    db = NEDB()
    db.put("k", "1", {"v": 1})
    db.expire("k", "1", 0.05)
    time.sleep(0.08)
    check("expire(): doc expires after set", db.get("k", "1") is None)
    check("expire(): returns False for missing", db.expire("k", "nope", 1.0) == False)

def test_ttl_sweep():
    db = NEDB()
    db.put("k", "a", {"v": 1}, ttl_s=0.05)
    db.put("k", "b", {"v": 2}, ttl_s=0.05)
    db.put("k", "c", {"v": 3})   # no TTL
    time.sleep(0.08)
    n = db.sweep()
    check("sweep(): deletes 2 expired", n == 2)
    check("sweep(): keeps non-expired", db.get("k", "c") is not None)

def test_ttl_time_travel_ignores_expiry():
    """AS OF reads never trigger lazy expiry."""
    db = NEDB()
    db.put("k", "1", {"v": 99}, ttl_s=0.05)
    snap = db.seq
    time.sleep(0.08)
    check("TTL: AS OF before expiry still visible", db.get("k", "1", as_of=snap) is not None)

test_ttl_expires()
test_ttl_no_expiry()
test_ttl_expire_method()
test_ttl_sweep()
test_ttl_time_travel_ignores_expiry()

# ─── GROUP BY ─────────────────────────────────────────────────────────────────
print("\n── GROUP BY / aggregations ──")

def test_group_by_count():
    db = NEDB()
    for i in range(10):
        db.put("orders", str(i), {"status": "paid" if i<6 else "pending", "amount": i*10})
    rows = db.query("FROM orders GROUP BY status COUNT")
    check("GROUP BY COUNT: returns 2 groups", len(rows) == 2)
    totals = {r["status"]: r["count"] for r in rows}
    check("GROUP BY COUNT: paid=6", totals.get("paid") == 6)
    check("GROUP BY COUNT: pending=4", totals.get("pending") == 4)

def test_group_by_sum():
    db = NEDB()
    for i in range(6):
        db.put("sales", str(i), {"region": "north" if i<3 else "south", "revenue": 100})
    rows = db.query("FROM sales GROUP BY region SUM revenue")
    totals = {r["region"]: r.get("sum_revenue") for r in rows}
    check("GROUP BY SUM: north=300", totals.get("north") == 300)
    check("GROUP BY SUM: south=300", totals.get("south") == 300)

def test_group_by_avg():
    db = NEDB()
    for i in range(4):
        db.put("scores", str(i), {"grade": "A" if i<2 else "B", "score": (i+1)*10})
    rows = db.query("FROM scores GROUP BY grade AVG score")
    avgs = {r["grade"]: r.get("avg_score") for r in rows}
    check("GROUP BY AVG: A=(10+20)/2=15", avgs.get("A") == 15.0)
    check("GROUP BY AVG: B=(30+40)/2=35", avgs.get("B") == 35.0)

def test_group_by_min_max():
    db = NEDB()
    for i in range(6):
        db.put("items", str(i), {"cat": "x" if i<3 else "y", "price": i*5})
    rows = db.query("FROM items GROUP BY cat MIN price")
    mins = {r["cat"]: r.get("min_price") for r in rows}
    check("GROUP BY MIN: x=0", mins.get("x") == 0)
    rows2 = db.query("FROM items GROUP BY cat MAX price")
    maxs = {r["cat"]: r.get("max_price") for r in rows2}
    check("GROUP BY MAX: y=25", maxs.get("y") == 25)

test_group_by_count()
test_group_by_sum()
test_group_by_avg()
test_group_by_min_max()

print(f"\nv0.5.0: {PASS} passed, {FAIL} failed {'✅' if not FAIL else '❌'}")
sys.exit(1 if FAIL else 0)
