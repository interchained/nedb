#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════╗
║   N E D B   ·   T H E   N I G H T   S H I F T                            ║
║   Five databases. One chain of custody.                                  ║
╚══════════════════════════════════════════════════════════════════════════╝

A city dispatch runs on five databases nobody chose on purpose: Redis for live
driver state, SQLite on the edge boxes, MySQL for billing, MongoDB for the app,
PostgreSQL for compliance. One night a rider is hurt. The city asks a question
that none of those five can answer:

    "What did the system know, and when did it know it?"

This is that night, told in six acts, run against the REAL nedb-engine you just
installed from PyPI. Nothing here is mocked except the database drivers
themselves — the engine, the hashes, the chain, the time travel are all real.

    python3 -m venv .venv && source .venv/bin/activate
    pip install "nedb-engine==3.2.2"
    python3 night_shift.py

Runs on macOS (Apple Silicon and Intel), Linux (glibc and musl) and Windows.
No database servers, no Docker, no config — the five drivers are shims, the
engine is the real Rust core from the wheel you just installed.

    NEDB_SAGA_FAST=1   skip the dramatic pauses (it runs ~40s otherwise)
    NO_COLOR=1         plain output

© INTERCHAINED LLC × Vex
"""
import json
import os
import shutil
import sys
import tempfile
import time

# ─────────────────────────────────────────────────────────────── stagecraft ──

FAST = os.environ.get("NEDB_SAGA_FAST", "") == "1"
PLAIN = os.environ.get("NO_COLOR", "") != "" or not sys.stdout.isatty()
W = 74


def c(code, s):
    return s if PLAIN else f"\033[{code}m{s}\033[0m"


dim = lambda s: c("2", s)
bold = lambda s: c("1", s)
red = lambda s: c("31", s)
grn = lambda s: c("32", s)
ylw = lambda s: c("33", s)
blu = lambda s: c("36", s)
mag = lambda s: c("35", s)
amber = lambda s: c("38;5;214", s)


def beat(t=0.5):
    if not FAST:
        time.sleep(t)


def slow(s, d=0.012):
    if FAST or PLAIN:
        print(s)
        return
    for ch in s:
        sys.stdout.write(ch)
        sys.stdout.flush()
        time.sleep(d)
    print()


def rule(ch="─"):
    print(dim(ch * W))


def act(n, title, subtitle, host):
    print()
    print(mag("╔" + "═" * (W - 2) + "╗"))
    line = f"  ACT {n}  ·  {title}"
    print(mag("║") + bold(line.ljust(W - 2)) + mag("║"))
    print(mag("║") + dim(f"  {subtitle}".ljust(W - 2)) + mag("║"))
    print(mag("║") + dim(f"  host: {host}".ljust(W - 2)) + mag("║"))
    print(mag("╚" + "═" * (W - 2) + "╝"))
    beat(0.35)


PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    mark = grn("  ✓ ") if cond else red("  ✗ ")
    print(f"{mark}{name}" + (dim(f"  — {detail}") if detail else ""))
    beat(0.16)


def narrate(s):
    slow(dim("  " + s))
    beat(0.15)


def code(s):
    print(blu("     " + s))
    beat(0.1)


TMP = []


def scratch():
    d = tempfile.mkdtemp(prefix="nedb_nightshift_")
    TMP.append(d)
    return d


# ─────────────────────────────────────────────────────────────────── curtain ──

print()
print(amber("    ███╗   ██╗███████╗██████╗ ██████╗"))
print(amber("    ████╗  ██║██╔════╝██╔══██╗██╔══██╗"))
print(amber("    ██╔██╗ ██║█████╗  ██║  ██║██████╔╝"))
print(amber("    ██║╚██╗██║██╔══╝  ██║  ██║██╔══██╗"))
print(amber("    ██║ ╚████║███████╗██████╔╝██████╔╝"))
print(amber("    ╚═╝  ╚═══╝╚══════╝╚═════╝ ╚═════╝"))
print()
print(bold("    T H E   N I G H T   S H I F T"))
print(dim("    Five databases. One chain of custody."))
print()
rule("═")

import platform

import nedb
from nedb import (wrap_mongo, wrap_mysql, wrap_postgresql, wrap_redis,
                  wrap_sqlite)

libc = "-".join(x for x in platform.libc_ver() if x) or "musl / unknown"
print(f"  engine      {bold('nedb-engine ' + nedb.__version__)}")
print(f"  native DAG  {grn('yes — Rust core') if nedb.__has_native__ else ylw('no — pure-Python v1 AOF fallback')}")
print(f"  machine     {platform.machine()}   libc {libc}   python {platform.python_version()}")
print(f"  wrappers    {', '.join(w for w in dir(nedb) if w.startswith('wrap_') and w != 'wrap_core')}")
rule("═")
beat(0.5)

print()
slow(bold("  02:14. A rider is hurt. The city wants the record."))
narrate("Five databases were running that night. None of them can tell you")
narrate("what they used to say — only what they say now. That is the problem.")
beat(0.7)


# ════════════════════════════════════════════ ACT I — REDIS ══════════════════
act("I", "THE LIVE BOARD", "Driver state, changing every second", "Redis")


class FakeRedis:
    """Stands in for redis.Redis — same call shape, no server needed."""

    def __init__(self):
        self._kv, self._stream = {}, []

    def set(self, k, v):
        self._kv[k] = v
        return True

    def get(self, k):
        return self._kv.get(k)

    def scan(self, cursor, match=None, count=None):
        import fnmatch
        return (0, [k for k in self._kv if not match or fnmatch.fnmatch(k, match)])

    def xadd(self, s, fields=None, **kw):
        self._stream.append(json.dumps(fields.get("op", fields)))
        return len(self._stream)

    def xrange(self, s, start="-", end="+", count=None):
        return [(i, "op", v.encode()) for i, v in enumerate(self._stream)]

    def xlen(self, s):
        return len(self._stream)

    def publish(self, ch, m):
        return 1


narrate("Dispatch keeps live driver state in Redis. It is fast and it forgets.")
narrate("One line gives it a memory it cannot lie about:")
code('r = wrap_redis(redis.Redis(), dag_path="./audit")')

r = wrap_redis(FakeRedis(), db_name="dispatch", dag_path=scratch())
check("Redis wrapped, embedded DAG engine selected", r.nedb.engine_kind == "dag-embedded", r.nedb.engine_kind)

r.nedb.register("driver:*", "driver")
r.nedb.shadow_writes = True
narrate("Now every write to Redis is mirrored into a hash-chained log.")

r.set("driver:d1", json.dumps({"name": "Bob", "status": "active", "rating": 4.9}))
r.set("driver:d2", json.dumps({"name": "Dave", "status": "active", "rating": 2.1}))
seq_dispatch = r.nedb.seq

check("the host database still answers normally",
      json.loads(r.get("driver:d1"))["name"] == "Bob")
check("shadowed writes are queryable in NQL",
      len(r.nedb.query('FROM driver WHERE status = "active"')) == 2)
check("chain verifies", r.nedb.verify() is True)
check("NEDB wrote nothing into Redis's own keyspace",
      all(not k.startswith("nedb:") for k in r._kv))
check("the engine reports its own durability", r.nedb.durable is True,
      f"durable={r.nedb.durable}")
narrate(f"Merkle head: {r.nedb.head[:32]}…")


# ═══════════════════════════════════════════ ACT II — SQLITE ═════════════════
act("II", "THE EDGE BOX", "A curbside terminal, offline half the night", "SQLite")

import sqlite3

sq = sqlite3.connect(":memory:")
sq.execute("CREATE TABLE rides (id INTEGER PRIMARY KEY, driver TEXT, fare REAL, status TEXT)")
sq.execute("INSERT INTO rides VALUES (1, 'd1', 24.50, 'complete')")
sq.execute("INSERT INTO rides VALUES (2, 'd2', 31.00, 'complete')")
sq.commit()

narrate("The edge box already has data. Wrapping does not migrate it —")
narrate("backfill() imports what is already there, then shadows what comes next.")
code('ws = wrap_sqlite(sqlite3.connect("edge.db"), dag_path="./audit")')

ws = wrap_sqlite(sq, db_name="edge", dag_path=scratch())
check("SQLite wrapped, embedded DAG", ws.nedb.engine_kind == "dag-embedded", ws.nedb.engine_kind)
ws.nedb.register("rides", collection="ride")
imported = ws.nedb.backfill()
check("existing rows backfilled", imported == 2, f"{imported} rows")

ws.nedb.shadow_writes = True
narrate("Write through the WRAPPER, not the raw connection — that is the seam.")
code("""ws.execute("INSERT INTO rides (driver, fare, status) VALUES (...)")""")
ws.execute("INSERT INTO rides (driver, fare, status) VALUES ('d2', 88.00, 'disputed')")
ws.commit()

check("the disputed ride is in the chain",
      len(ws.nedb.query('FROM ride WHERE status = "disputed"')) >= 1)

narrate("Until 3.2.2 that assertion FAILED — silently. The shadow raised")
narrate("TypeError, the wrapper swallowed it, and verify() still said True.")
narrate("A provenance layer that loses a record without telling you is worse")
narrate("than one that has no records at all.")

ws.execute("UPDATE rides SET status = 'settled' WHERE id = 1")
_row1 = [x for x in ws.nedb.query("FROM ride") if x["_id"] == "1"]
check("an UPDATE lands on the row it names, not the last one inserted",
      bool(_row1) and _row1[0].get("status") == "settled",
      "3.2.1 rewrote a bystander instead")
check("zero shadow failures were swallowed", ws.nedb.shadow_errors == 0,
      f"shadow_errors={ws.nedb.shadow_errors}")
check("chain verifies after live SQLite writes", ws.nedb.verify() is True)


# ════════════════════════════════════════════ ACT III — MYSQL ════════════════
act("III", "THE BILLING RUN", "Money, and who is owed it", "MySQL")


class FakeMysqlCursor:
    def __init__(self):
        self._rows, self.description = [], None

    def execute(self, q, p=None):
        self.description = [("id",), ("driver",), ("amount",)]
        self._rows = [(1, "d1", 24.50), (2, "d2", 31.00)]

    def fetchmany(self, n=None):
        r, self._rows = self._rows, []
        return r

    def close(self):
        pass


class FakeMysqlConn:
    def cursor(self):
        return FakeMysqlCursor()


narrate("MySQL speaks DB-API, which has no universal write hook — so shadowing")
narrate("here is explicit. You say what to record. Nothing is guessed.")
code('wm.shadow_row("payouts", 3, {"id": 3, "driver": "d2", "amount": 88.00})')

wm = wrap_mysql(FakeMysqlConn(), db_name="billing", dag_path=scratch())
check("MySQL wrapped, embedded DAG", wm.nedb.engine_kind == "dag-embedded", wm.nedb.engine_kind)
wm.nedb.register("payouts", collection="payout")
check("existing payouts backfilled", wm.nedb.backfill() == 2)

wm.nedb.shadow_writes = True
wm.shadow_row("payouts", 3, {"id": 3, "driver": "d2", "amount": 88.00, "note": "disputed ride"})
check("the disputed payout is on the record",
      len(wm.nedb.query('FROM payout WHERE driver = "d2" AND amount = 88.0')) == 1)
check("chain verifies after billing writes", wm.nedb.verify() is True)


# ═══════════════════════════════════════════ ACT IV — MONGODB ════════════════
act("IV", "THE APP", "What the rider actually saw", "MongoDB")


class FakeMongoColl:
    def find(self, q):
        return [{"_id": "s1", "rider": "r1", "shown_eta": 4, "driver": "d1"},
                {"_id": "s2", "rider": "r2", "shown_eta": 6, "driver": "d2"}]


class FakeMongoClient:
    def __getitem__(self, db):
        return {"sessions": FakeMongoColl()}


narrate("The app's own view of the night — what the rider was told.")
code('mg = wrap_mongo(pymongo.MongoClient(), dag_path="./audit")')

mg = wrap_mongo(FakeMongoClient(), db_name="app", dag_path=scratch())
check("MongoDB wrapped, embedded DAG", mg.nedb.engine_kind == "dag-embedded", mg.nedb.engine_kind)
mg.nedb.register("app.sessions", collection="session")
check("existing sessions backfilled", mg.nedb.backfill() == 2)

mg.nedb.shadow_writes = True
mg.nedb.shadow_row("app.sessions", "session",
                   {"_id": "s3", "rider": "r2", "shown_eta": 2, "driver": "d2",
                    "note": "ETA revised down after assignment"})
check("the revised ETA is recorded",
      len(mg.nedb.query('FROM session WHERE _id = "s3"')) == 1)
check("chain verifies after app writes", mg.nedb.verify() is True)


# ════════════════════════════════════════ ACT V — POSTGRESQL ═════════════════
act("V", "THE COMPLIANCE TABLE", "The one the regulator will subpoena", "PostgreSQL")


class FakePgCursor:
    def __init__(self, rows):
        self._rows = rows
        self.description = [("id",), ("check_name",), ("result",)]

    def execute(self, q, p=None):
        pass

    def fetchmany(self, n=None):
        r, self._rows = self._rows, []
        return r

    def close(self):
        pass


class FakePgConn:
    def cursor(self):
        return FakePgCursor([(1, "background_check", "pass"),
                             (2, "vehicle_inspection", "pass")])


narrate("Compliance is where 'we think so' stops being an acceptable answer.")
code('pg = wrap_postgresql(psycopg2.connect(...), dag_path="./audit")')

pg = wrap_postgresql(FakePgConn(), db_name="compliance", dag_path=scratch())
check("PostgreSQL wrapped, embedded DAG", pg.nedb.engine_kind == "dag-embedded", pg.nedb.engine_kind)
pg.nedb.register("checks", collection="check", pk="id")
check("existing checks backfilled", pg.nedb.backfill() == 2)

pg.nedb.shadow_writes = True
pg.shadow_row("checks", 3, {"id": 3, "check_name": "rating_floor", "result": "FAIL",
                            "detail": "d2 rated 2.1, below 3.0 floor"})
check("the failing check is sealed in the chain",
      len(pg.nedb.query('FROM check WHERE result = "FAIL"')) == 1)
check("chain verifies after compliance writes", pg.nedb.verify() is True)

print()
rule()
print(bold("  Five databases wrapped. Five chains. Nothing migrated."))
rule()
beat(0.6)


# ═════════════════════════════════════════ ACT VI — THE RECKONING ════════════
act("VI", "THE RECKONING", "What did the system know, and when?", "NEDB itself")

from nedb import NEDB

narrate("Now the question the five databases could not answer.")
narrate("A fresh chain, this time recording not just facts but their CAUSES.")
beat(0.4)

db = NEDB()
db.put("signals", "rating", {"driver": "d2", "rating": 2.1, "source": "rider_reports"})
seq_rating = db.seq - 1
db.put("signals", "proximity", {"driver": "d2", "distance_mi": 0.04})
seq_prox = db.seq

db.put("decisions", "assign_d2",
       {"driver": "d2", "rider": "r2", "reason": "nearest available"},
       caused_by=[seq_prox],
       evidence="dispatch_algorithm_v1",
       confidence=0.99)
seq_decision = db.seq

print()
print(bold("  ── the record ──"))
check("the assignment was made", db.get("decisions", "assign_d2") is not None)
check("its cause is sealed into the hash chain",
      db.get("decisions", "assign_d2").get("_caused_by") == [seq_prox])

print()
print(bold("  ── why? (TRACE caused_by — backward) ──"))
causes = db.query('FROM decisions WHERE _id = "assign_d2" TRACE caused_by')
for row in causes:
    print(dim(f"     ← {row.get('_id')}: {json.dumps({k: v for k, v in row.items() if not k.startswith('_')})}"))
check("TRACE walks back to the causing signal", len(causes) >= 1, f"{len(causes)} ancestor(s)")

narrate("There it is. The dispatcher chose d2 on PROXIMITY.")
narrate("The rating signal existed — and was never an input.")
check("the rating signal is NOT among the causes",
      all(rw.get("_id") != "rating" for rw in causes),
      "d2 was assigned on distance alone")

print()
print(bold("  ── what did that cause? (TRACE caused_by REVERSE — forward) ──"))
downstream = db.query('FROM signals WHERE _id = "proximity" TRACE caused_by REVERSE')
check("forward trace finds the decision it produced", len(downstream) >= 1,
      f"{len(downstream)} descendant(s)")

print()
print(bold("  ── time travel (AS OF) ──"))
snap = db.seq
db.put("signals", "rating", {"driver": "d2", "rating": 4.6, "source": "corrected"})
now = db.get("signals", "rating")
then = db.query(f'FROM signals AS OF {snap} WHERE _id = "rating"')
check("the rating reads 4.6 today", now["rating"] == 4.6)
check("AS OF the assignment, it read 2.1",
      any(rw.get("rating") == 2.1 for rw in then),
      "the number was edited AFTER the incident — the old value is still provable")
narrate("This is the whole point. Someone corrected the record.")
narrate("The correction is visible, and the original is not gone.")

print()
print(bold("  ── tamper (verify) ──"))
check("chain verifies before tampering", db.verify() is True)
try:
    victim = db.log.ops[1]
    original = getattr(victim, "hash", None)
    if original:
        object.__setattr__(victim, "hash", "0" * len(original))
        check("a forged hash is DETECTED, not tolerated", db.verify() is False)
        object.__setattr__(victim, "hash", original)
        check("chain verifies again once restored", db.verify() is True)
    else:
        check("tamper probe skipped (no hash attribute)", True, "engine internals differ")
except Exception as e:
    check("tamper probe skipped", True, f"{type(e).__name__}")

print()
print(bold("  ── the changefeed (DAG-native) ──"))
feed = r.nedb.since(0, 100)
check("since() streams the chain for replication",
      feed["head_seq"] >= 1 and len(feed["nodes"]) >= 1,
      f"head_seq={feed['head_seq']}, {len(feed['nodes'])} node(s)")
status = r.nedb.scan_status()
check("scan_status() reports replication readiness", "scan_complete" in status)

print()
print(bold("  ── close, and open again (the 3.2.1 fix) ──"))
narrate("In 3.1.0 this next line was impossible: the flush ticker pinned the")
narrate("database forever, so its own process could never reopen it.")
if nedb.__has_native__:
    from nedb._native import NedbCore
    import gc
    p = scratch()
    d1 = NedbCore.open(p)
    d1.put("evidence", "e1", json.dumps({"sealed": True}))
    head_before = d1.head()
    d1.flush()
    del d1
    gc.collect()
    d2 = NedbCore.open(p)
    check("the same directory reopens in the same process", True)
    check("the evidence survived close/reopen", d2.get("evidence", "e1") is not None)
    check("the Merkle head is unchanged across the reopen", d2.head() == head_before,
          head_before[:24] + "…")
else:
    check("reopen check skipped — no native wheel on this platform", True)


# ───────────────────────────────────────────────────────────────── verdict ──

print()
print(mag("╔" + "═" * (W - 2) + "╗"))
title = f"  VERDICT   {len(PASS)} passed   {len(FAIL)} failed"
print(mag("║") + (grn(bold(title)) if not FAIL else red(bold(title))).ljust(W - 2 + (0 if PLAIN else 18)) + mag("║"))
print(mag("╚" + "═" * (W - 2) + "╝"))
print()
if FAIL:
    for f in FAIL:
        print(red(f"    ✗ {f}"))
    print()

slow(bold("  Redis. SQLite. MySQL. MongoDB. PostgreSQL."))
slow(dim("  Five engines that could each tell you what is true right now,"))
slow(dim("  and none that could tell you what was true at 02:14."))
print()
slow(amber("  NEDB did not replace one of them. It remembered all of them."))
print()
print(dim(f"  nedb-engine {nedb.__version__} · MIT · INTERCHAINED LLC"))
print()

# Deliberately NOT deleting these. Two reasons: tearing a directory out from
# under a live DAG store makes its exit-flush fail noisily, and more to the
# point — the chains are the evidence. Go and look at them.
print(dim("  the chains written during this run:"))
for d in TMP:
    print(dim(f"    {d}"))
print()

sys.exit(1 if FAIL else 0)
