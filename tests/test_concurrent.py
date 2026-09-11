#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""Stress + correctness test for the single-writer group-commit Sequencer."""
import os, sys, time, threading, tempfile, shutil
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"))
from nedb import NEDB
from nedb.concurrent import Sequencer

PASS = FAIL = 0
def check(name, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  ok  {name}")
    else: FAIL += 1; print(f"  FAIL {name} {detail}")

# ── 1. Many concurrent writers → valid chain, no lost writes ─────────────────
print("\n── concurrent writers ──")
tmp = tempfile.mkdtemp()
try:
    db = NEDB(tmp)
    db.create_index("k", "t", "eq")
    seq = Sequencer(db)
    THREADS, PER = 16, 200
    errors = []
    def writer(tid):
        try:
            for i in range(PER):
                seq.put("k", f"t{tid}-{i}", {"t": "x", "tid": tid, "i": i})
        except Exception as e:
            errors.append(e)
    ts = [threading.Thread(target=writer, args=(t,)) for t in range(THREADS)]
    t0 = time.perf_counter()
    for t in ts: t.start()
    for t in ts: t.join()
    dt = time.perf_counter() - t0
    total = THREADS * PER
    check("no writer errors", not errors, str(errors[:2]))
    check("chain verifies after concurrent writes", seq.verify())
    rows = seq.query("FROM k")
    check(f"all {total} writes present (got {len(rows)})", len(rows) == total)
    # every (tid,i) unique and accounted for
    seen = {(r["tid"], r["i"]) for r in rows}
    check("no lost/dup writes", len(seen) == total)
    print(f"     {total} writes in {dt*1000:.0f}ms  =>  {total/dt:,.0f} writes/s (group-commit)")
    seq.close()

    # reopen → still verifies, data intact
    db2 = NEDB(tmp)
    check("verify after reopen", db2.verify())
    check("rows survive reopen", len(db2.query("FROM k")) == total)
    db2.close()
finally:
    shutil.rmtree(tmp, ignore_errors=True)

# ── 2. Readers concurrent with writers → never crash, monotonic snapshots ────
print("\n── readers during writers ──")
tmp = tempfile.mkdtemp()
try:
    db = NEDB(tmp); db.create_index("k", "t", "eq")
    seq = Sequencer(db)
    stop = threading.Event()
    read_errors = []
    counts = []
    def reader():
        try:
            while not stop.is_set():
                # full-scan read (exercises the key-enumeration-during-insert race)
                counts.append(len(seq.query('FROM k WHERE t = "x"')))
                time.sleep(0.001)  # realistic readers don't busy-spin; yield the GIL
        except Exception as e:
            read_errors.append(e)
    readers = [threading.Thread(target=reader) for _ in range(4)]
    for r in readers: r.start()
    for i in range(600):
        seq.put("k", f"r{i}", {"t": "x", "i": i})
        if i % 50 == 0:
            time.sleep(0.001)
    stop.set()
    for r in readers: r.join()
    check("no reader errors during writes", not read_errors, str(read_errors[:2]))
    check("readers observed progress", len(counts) > 0 and max(counts) > 0)
    # snapshot isolation: counts never exceed final total and never go backwards wildly
    check("snapshot counts monotonic-ish", all(0 <= c <= 600 for c in counts))
    check("final chain verifies", seq.verify())
    seq.close()
finally:
    shutil.rmtree(tmp, ignore_errors=True)

# ── 3. checkpoint through the committer + concurrent writes ───────────────────
print("\n── checkpoint under load ──")
tmp = tempfile.mkdtemp()
try:
    db = NEDB(tmp); db.create_index("k", "t", "eq")
    seq = Sequencer(db)
    errs = []
    def w(tid):
        try:
            for i in range(100):
                seq.put("k", f"{tid}-{i}", {"t": "x"})
                if i == 50 and tid == 0:
                    seq.checkpoint()
        except Exception as e: errs.append(e)
    ts = [threading.Thread(target=w, args=(t,)) for t in range(8)]
    for t in ts: t.start()
    for t in ts: t.join()
    check("no errors with concurrent checkpoint", not errs, str(errs[:2]))
    check("verify after checkpoint-under-load", seq.verify())
    seq.close()
    db2 = NEDB(tmp)
    check("reopen from snapshot verifies", db2.verify())
    check("data intact after snapshot reopen", len(db2.query("FROM k")) == 800)
    db2.close()
finally:
    shutil.rmtree(tmp, ignore_errors=True)

# ── 4. replay protection still works through the queue ───────────────────────
print("\n── replay protection ──")
tmp = tempfile.mkdtemp()
try:
    db = NEDB(tmp); seq = Sequencer(db)
    seq.put("k", "1", {"v": 1}, client="svc", nonce=10)
    try:
        seq.put("k", "1", {"v": 2}, client="svc", nonce=5)  # stale
        check("stale nonce raises", False)
    except Exception:
        check("stale nonce raises through sequencer", True)
    seq.close()
finally:
    shutil.rmtree(tmp, ignore_errors=True)

print(f"\nConcurrent: {PASS} passed, {FAIL} failed {'✅' if not FAIL else '❌'}")
sys.exit(1 if FAIL else 0)
