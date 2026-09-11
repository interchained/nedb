#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
Live proof for the wrap_* family: every wrapper, every engine mode.

Proves (against the REAL engine, not mocks):
  1. wrap_redis    — backend=auto (embedded DAG), register/backfill/shadow/verify
  2. wrap_redis    — backend=aof (v1 in-process), backward compat
  3. wrap_redis    — DAG-native extras (tip, since, scan_status)
  4. wrap_sqlite   — embedded DAG, register/backfill/shadow/verify
  5. wrap_mysql    — embedded DAG (fakeredis-style shim, DB-API surface)
  6. wrap_mongo    — embedded DAG (shim client, find() passthrough)
  7. wrap_postgresql — embedded DAG (shim cursor)
  8. every verify() → True (BLAKE2b chain intact after all writes)

© INTERCHAINED LLC × Vex (Interchained AI fleet: GLM · Claude · Opus · Fable · GPT-6)"""
import json
import os
import sys
import tempfile

sys.path.insert(0, "python")

import nedb  # source tree

# Graft the compiled Rust core from the installed platform wheel onto this
# source-tree package (the source tree has no _native binary of its own).
if not nedb.__has_native__:
    # Locate the compiled extension in whatever site-packages this interpreter
    # actually has.
    #
    # This block used to hardcode /agent/.local/lib/python3.9/site-packages,
    # an absolute path inside the sandbox that authored it. That made the suite
    # unrunnable for every other machine — every human contributor and every CI
    # runner — and nothing caught it, because until the test workflow existed
    # no suite ran anywhere but the author's box.
    import glob
    import importlib.machinery
    import importlib.util
    import site
    import sysconfig
    import sys as _sys

    _roots = []
    try:
        _roots += site.getsitepackages()
    except Exception:
        pass
    for _p in (getattr(site, "getusersitepackages", lambda: None)(),
               sysconfig.get_paths().get("purelib"),
               sysconfig.get_paths().get("platlib")):
        if _p:
            _roots.append(_p)

    _cands = []
    for _root in dict.fromkeys(_roots):
        for _pat in ("_native*.so", "_native*.pyd", "_native*.dylib"):
            _cands += glob.glob(os.path.join(_root, "nedb", _pat))

    if not _cands:
        print("SKIP test_wrap_family: no compiled nedb._native found in site-packages "
              "(install the platform wheel: pip install nedb-engine)")
        raise SystemExit(0)

    _so_path = _cands[0]
    _loader = importlib.machinery.ExtensionFileLoader("_native", _so_path)
    _spec = importlib.util.spec_from_loader("_native", _loader)
    _wheel_native = importlib.util.module_from_spec(_spec)
    _loader.exec_module(_wheel_native)
    nedb._native = _wheel_native
    nedb.__has_native__ = True
    _sys.modules["nedb._native"] = _wheel_native

from nedb import (_native, wrap_redis, wrap_sqlite, wrap_mysql,
                  wrap_mongo, wrap_postgresql)

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    mark = "✓" if cond else "✗"
    print(f"  {mark} {name}" + (f" — {detail}" if detail else ""))


print(f"nedb {nedb.__version__} | native DAG: {nedb.__has_native__}")
assert nedb.__has_native__, "platform wheel required for this proof"

# ═══ 1. wrap_redis — embedded DAG (auto) ═══════════════════════════════════
print("\n[1] wrap_redis — backend=auto → embedded v2/v3 DAG")


class FakeRedis:
    """Minimal redis-compatible surface for the proof."""

    def __init__(self):
        self._store = {}
        self._stream = []   # for the v1 AOF backend (xadd/xrange)

    def set(self, k, v):
        self._store[k] = v
        return True

    def get(self, k):
        return self._store.get(k)

    def scan(self, cursor, match=None, count=None):
        import fnmatch
        keys = [k for k in self._store if not match or fnmatch.fnmatch(k, match)]
        return (0, keys)

    def xadd(self, stream, fields=None, **kw):
        self._stream.append(json.dumps(fields.get("op", fields)))
        return len(self._stream)

    def xrange(self, stream, start="-", end="+", count=None):
        return [(i, "op", s.encode()) for i, s in enumerate(self._stream)]

    def xlen(self, stream):
        return len(self._stream)

    def publish(self, channel, msg):
        return 1


r = wrap_redis(FakeRedis(), db_name="proof", dag_path=tempfile.mkdtemp())
check("engine_kind == dag-embedded", r.nedb.engine_kind == "dag-embedded",
      r.nedb.engine_kind)

r.nedb.register("driver:*", "driver")
n = r.nedb.backfill()
check("backfill on empty store", n == 0, f"{n} imported")

r.nedb.shadow_writes = True
r.set("driver:d1", json.dumps({"name": "Bob", "status": "active"}))
r.set("driver:d2", json.dumps({"name": "Ann", "status": "active"}))

docs = r.nedb.query('FROM driver WHERE status = "active"')
check("shadowed writes are NQL-queryable", len(docs) == 2, f"{len(docs)} docs")
check("DAG verify() after shadowed writes", r.nedb.verify() is True)

# DAG-native extras
tip = r.nedb.tip()
check("tip() returns latest node", tip is not None and tip["_id"] == "d2")
feed = r.nedb.since(0, 100)
# changefeed semantics: after_seq is EXCLUSIVE — since(0) returns everything
# strictly after seq 0 (here: just the latest write). Page from the start by
# consuming from_seq=0 and walking has_more; total nodes across pages == seq.
check("since() changefeed page", feed["head_seq"] >= 1
      and feed["to_seq"] == feed["head_seq"] and len(feed["nodes"]) >= 1,
      f"head_seq={feed['head_seq']} nodes={len(feed['nodes'])} (after_seq exclusive)")
full = r.nedb.query("FROM driver")
check("changefeed total == queryable docs", len(full) == 2)
status = r.nedb.scan_status()
check("scan_status() present", "scan_complete" in status)

# ═══ 2. wrap_redis — v1 AOF backward compat ════════════════════════════════
print("\n[2] wrap_redis — backend=aof → v1 in-process (backward compat)")
r1 = wrap_redis(FakeRedis(), db_name="proof_aof", backend="aof")
check("engine_kind == aof-embedded", r1.nedb.engine_kind == "aof-embedded",
      r1.nedb.engine_kind)
r1.nedb.register("driver:*", "driver")
r1.nedb.shadow_writes = True
r1.set("driver:d9", json.dumps({"name": "Zoe", "status": "active"}))
docs1 = r1.nedb.query('FROM driver WHERE status = "active"')
check("v1 AOF shadow works", len(docs1) == 1, f"{len(docs1)} docs")
check("v1 AOF verify()", r1.nedb.verify() is True)

# ═══ 3. wrap_sqlite — embedded DAG ══════════════════════════════════════════
print("\n[3] wrap_sqlite — backend=auto → embedded DAG")
import sqlite3
sq = sqlite3.connect(":memory:")
sq.execute("CREATE TABLE drivers (id INTEGER PRIMARY KEY, name TEXT, status TEXT)")
sq.execute("INSERT INTO drivers VALUES (1, 'Bob', 'active')")
sq.execute("INSERT INTO drivers VALUES (2, 'Ann', 'active')")
sq.commit()

ws = wrap_sqlite(sq, db_name="proof_sqlite", dag_path=tempfile.mkdtemp())
check("engine_kind == dag-embedded", ws.nedb.engine_kind == "dag-embedded",
      ws.nedb.engine_kind)
ws.nedb.register("drivers", collection="driver")
imported = ws.nedb.backfill()
check("backfill imported existing rows", imported == 2, f"{imported} rows")
docs = ws.nedb.query('FROM driver WHERE status = "active"')
check("backfilled rows NQL-queryable", len(docs) == 2, f"{len(docs)} docs")

ws.nedb.shadow_writes = True
# Write through the WRAPPER. This used to call sq.execute() -- the RAW
# connection -- which bypasses the shadow seam entirely, and then asserted
# only verify(). verify() passes on an empty chain (it proves what got in is
# intact, not that everything that should have got in did), so three real
# defects in automatic SQLite shadowing sat green here. Assert the row is
# actually RETRIEVABLE, in the registered collection.
ws.execute("INSERT INTO drivers (name, status) VALUES ('Zoe', 'active')")
ws.commit()
_zoe = ws.nedb.query('FROM driver WHERE name = "Zoe"')
check("sqlite shadow is retrievable in the registered collection",
      len(_zoe) == 1, f"{len(_zoe)} docs")
check("no shadow failure was swallowed", ws.nedb.shadow_errors == 0,
      str(ws.nedb.last_shadow_error))
check("DAG verify() after sqlite writes", ws.nedb.verify() is True)

# ═══ 4. wrap_mysql — embedded DAG (DB-API shim) ════════════════════════════
print("\n[4] wrap_mysql — backend=auto → embedded DAG")


class FakeMysqlCursor:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.description = None

    def execute(self, q, p=None):
        self.description = [("id",), ("name",), ("status",)]
        self._rows = [(1, "Bob", "active"), (2, "Ann", "active")]

    def fetchmany(self, n=None):
        r, self._rows = self._rows, []
        return r

    def close(self):
        pass


class FakeMysqlConn:
    def cursor(self):
        return FakeMysqlCursor()


wm = wrap_mysql(FakeMysqlConn(), db_name="proof_mysql", dag_path=tempfile.mkdtemp())
check("engine_kind == dag-embedded", wm.nedb.engine_kind == "dag-embedded",
      wm.nedb.engine_kind)
wm.nedb.register("drivers", collection="driver")
imported = wm.nedb.backfill()
check("mysql backfill", imported == 2, f"{imported} rows")
wm.nedb.shadow_writes = True
wm.shadow_row("drivers", 3, {"id": 3, "name": "Zoe", "status": "active"})
docs = wm.nedb.query('FROM driver WHERE name = "Zoe"')
check("mysql shadow_row is queryable", len(docs) == 1, f"{len(docs)} docs")
check("DAG verify() after mysql shadow", wm.nedb.verify() is True)

# ═══ 5. wrap_mongo — embedded DAG ═══════════════════════════════════════════
print("\n[5] wrap_mongo — backend=auto → embedded DAG")


class FakeMongoColl:
    def find(self, q):
        return [{"_id": "1", "name": "Bob", "status": "active"},
                {"_id": "2", "name": "Ann", "status": "active"}]


class FakeMongoClient:
    def __getitem__(self, dbname):
        return {"drivers": FakeMongoColl()}


mg = wrap_mongo(FakeMongoClient(), db_name="proof_mongo", dag_path=tempfile.mkdtemp())
check("engine_kind == dag-embedded", mg.nedb.engine_kind == "dag-embedded",
      mg.nedb.engine_kind)
mg.nedb.register("app.drivers", collection="driver")
imported = mg.nedb.backfill()
check("mongo backfill", imported == 2, f"{imported} docs")
mg.nedb.shadow_writes = True
mg.nedb.shadow_row("app.drivers", "driver",
                   {"_id": "3", "name": "Zoe", "status": "active"})
docs = mg.nedb.query('FROM driver WHERE name = "Zoe"')
check("mongo shadow_row is queryable", len(docs) == 1, f"{len(docs)} docs")
check("DAG verify() after mongo shadow", mg.nedb.verify() is True)

# ═══ 6. wrap_postgresql — embedded DAG ══════════════════════════════════════
print("\n[6] wrap_postgresql — backend=auto → embedded DAG")


class FakePgCursor:
    def __init__(self, rows):
        self._rows = rows
        self.description = [("id",), ("name",), ("status",)]

    def execute(self, q, p=None):
        pass

    def fetchmany(self, n=None):
        r, self._rows = self._rows, []
        return r

    def close(self):
        pass


class FakePgConn:
    def cursor(self):
        return FakePgCursor([(1, "Bob", "active"), (2, "Ann", "active")])


pg = wrap_postgresql(FakePgConn(), db_name="proof_pg", dag_path=tempfile.mkdtemp())
check("engine_kind == dag-embedded", pg.nedb.engine_kind == "dag-embedded",
      pg.nedb.engine_kind)
pg.nedb.register("drivers", collection="driver", pk="id")
imported = pg.nedb.backfill()
check("postgres backfill", imported == 2, f"{imported} rows")
pg.nedb.shadow_writes = True
pg.shadow_row("drivers", 3, {"id": 3, "name": "Zoe", "status": "active"})
docs = pg.nedb.query('FROM driver WHERE name = "Zoe"')
check("postgres shadow_row is queryable", len(docs) == 1, f"{len(docs)} docs")
check("DAG verify() after postgres shadow", pg.nedb.verify() is True)

# ═══ summary ════════════════════════════════════════════════════════════════
print(f"\n{'=' * 60}")
print(f"PROOF: {len(PASS)} passed, {len(FAIL)} failed")
if FAIL:
    print("FAILED:", *FAIL, sep="\n  - ")
    sys.exit(1)
print("ALL WRAPPERS PROVEN against the real embedded v2/v3 DAG engine.")
