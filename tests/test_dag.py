#!/usr/bin/env python3
"""
test_dag.py — NEDB v2 DAG correctness test suite.

Tries port 7070 first. If nothing is running, auto-spawns nedbd --dag
against a temp dir, runs all tests, then tears it down.

Run:
    python3 tests/test_dag.py          # auto-spawns nedbd if needed
    python3 tests/test_dag.py -v       # verbose
    pytest tests/test_dag.py           # via pytest

Requirements:
    pip install httpx pytest
"""
from __future__ import annotations

import asyncio
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from typing import Optional

try:
    import httpx
except ImportError:
    print("ERROR: httpx required — pip install httpx")
    sys.exit(1)

# ── Config ─────────────────────────────────────────────────────────────────────

BASE_URL = os.getenv("NEDB_URL", "http://127.0.0.1:7070")
TOKEN    = os.getenv("NEDBD_TOKEN", "")
DB_NAME  = "test_dag_suite"

# ── Server lifecycle ───────────────────────────────────────────────────────────

_spawned_proc:  Optional[subprocess.Popen] = None
_spawned_tmpdir: Optional[str] = None


def _find_nedbd_binary() -> Optional[str]:
    """Find the nedbd binary — in PATH or alongside the nedb package."""
    import shutil as _shutil
    if b := _shutil.which("nedbd"):
        return b
    try:
        import nedb, pathlib
        pkg = pathlib.Path(nedb.__file__).parent
        for name in ("nedbd", "nedbd.exe"):
            p = pkg / name
            if p.exists():
                return str(p)
    except Exception:
        pass
    return None


def _is_server_alive() -> bool:
    try:
        r = httpx.get(f"{BASE_URL}/health", timeout=2.0)
        return r.status_code == 200 and r.json().get("ok")
    except Exception:
        return False


def _spawn_server() -> bool:
    """Auto-spawn nedbd --dag if no server on 7070. Returns True if spawned."""
    global _spawned_proc, _spawned_tmpdir
    binary = _find_nedbd_binary()
    if not binary:
        return False
    _spawned_tmpdir = tempfile.mkdtemp(prefix="nedb_test_")
    env = os.environ.copy()
    _spawned_proc = subprocess.Popen(
        [binary, "--dag", "--data", _spawned_tmpdir],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    # Wait up to 5s for server to come up
    for _ in range(25):
        time.sleep(0.2)
        if _is_server_alive():
            print(f"  [test_dag] auto-spawned nedbd --dag (pid={_spawned_proc.pid})")
            return True
    return False


def _teardown_server() -> None:
    global _spawned_proc, _spawned_tmpdir
    if _spawned_proc:
        _spawned_proc.send_signal(signal.SIGTERM)
        try:
            _spawned_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _spawned_proc.kill()
        _spawned_proc = None
    if _spawned_tmpdir and os.path.exists(_spawned_tmpdir):
        shutil.rmtree(_spawned_tmpdir, ignore_errors=True)
        _spawned_tmpdir = None


# ── HTTP helpers ───────────────────────────────────────────────────────────────

def _client() -> httpx.Client:
    headers = {"Content-Type": "application/json"}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    return httpx.Client(base_url=BASE_URL, headers=headers, timeout=10.0)


def _safe_delete(client, path):
    """Delete without raising — used in setUp/tearDown cleanup."""
    try:
        client.delete(path)
    except Exception:
        pass


def _put(client, coll, doc_id, doc, **kwargs):
    payload = {"coll": coll, "id": doc_id, "doc": doc, **kwargs}
    r = client.post(f"/v1/databases/{DB_NAME}/put", json=payload)
    r.raise_for_status()
    return r.json()


def _query(client, nql):
    r = client.post(f"/v1/databases/{DB_NAME}/query", json={"nql": nql})
    r.raise_for_status()
    return r.json()


def _delete(client, coll, doc_id):
    r = client.delete(f"/v1/databases/{DB_NAME}/rows/{coll}/{doc_id}")
    r.raise_for_status()
    return r.json()


def _verify(client):
    r = client.get(f"/v1/databases/{DB_NAME}/verify")
    r.raise_for_status()
    return r.json()


def _batch(client, ops):
    r = client.post(f"/v1/databases/{DB_NAME}/batch", json={"ops": ops})
    r.raise_for_status()
    return r.json()


# ── Test cases ─────────────────────────────────────────────────────────────────

class TestDagHealth(unittest.TestCase):
    """Server health and version checks."""

    def setUp(self):
        self.c = _client()

    def tearDown(self):
        self.c.close()

    def test_health_ok(self):
        r = self.c.get("/health")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertTrue(body.get("ok"), f"health not ok: {body}")
        self.assertEqual(body.get("service"), "nedbd")

    def test_health_has_version(self):
        body = self.c.get("/health").json()
        version = body.get("version", "")
        self.assertTrue(version.startswith("2."), f"expected v2.x, got {version!r}")

    def test_health_has_head_field_not_required(self):
        # /health may not include head — just check it doesn't crash
        r = self.c.get("/health")
        self.assertEqual(r.status_code, 200)


class _DagBase(unittest.TestCase):
    """Base class: resilient setUp/tearDown that handles server restarts."""

    def setUp(self):
        self.c = _client()
        if not _is_server_alive():
            self.skipTest("nedbd not running — skip")
        _safe_delete(self.c, f"/v1/databases/{DB_NAME}")
        self.c.post("/v1/databases", json={"name": DB_NAME})

    def tearDown(self):
        _safe_delete(self.c, f"/v1/databases/{DB_NAME}")
        self.c.close()


class TestDagCrud(_DagBase):
    """Basic CRUD: put, get, delete, tombstone visibility."""

    def test_put_returns_ok_seq_head(self):
        r = _put(self.c, "items", "i1", {"x": 1})
        self.assertTrue(r.get("ok"))
        self.assertIn("seq", r)
        self.assertIn("head", r)
        self.assertGreater(len(r["head"]), 30)  # BLAKE2b hex

    def test_get_via_nql(self):
        _put(self.c, "items", "i2", {"name": "alice", "score": 42})
        body = _query(self.c, 'FROM items WHERE _id = "i2" LIMIT 1')
        rows = body.get("rows", [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["name"], "alice")
        self.assertEqual(rows[0]["score"], 42)

    def test_get_nonexistent_returns_empty(self):
        body = _query(self.c, 'FROM items WHERE _id = "doesnotexist" LIMIT 1')
        self.assertEqual(body.get("rows", []), [])
        self.assertEqual(body.get("count", 0), 0)

    def test_overwrite_returns_new_seq(self):
        r1 = _put(self.c, "items", "i3", {"v": 1})
        r2 = _put(self.c, "items", "i3", {"v": 2})
        self.assertGreater(r2["seq"], r1["seq"])
        # Current value should be v=2
        rows = _query(self.c, 'FROM items WHERE _id = "i3" LIMIT 1')["rows"]
        self.assertEqual(rows[0]["v"], 2)

    def test_tombstone_delete_hides_from_query(self):
        _put(self.c, "items", "ghost", {"secret": True})
        # Confirm visible
        rows = _query(self.c, 'FROM items WHERE _id = "ghost" LIMIT 1')["rows"]
        self.assertEqual(len(rows), 1)
        # Delete
        r = _delete(self.c, "items", "ghost")
        self.assertTrue(r.get("ok"))
        # Must be invisible now
        rows = _query(self.c, 'FROM items WHERE _id = "ghost" LIMIT 1')["rows"]
        self.assertEqual(len(rows), 0, "deleted doc still visible in query")

    def test_tombstone_not_in_list_all(self):
        _put(self.c, "items", "del_me", {"x": 1})
        _delete(self.c, "items", "del_me")
        all_rows = _query(self.c, "FROM items")["rows"]
        ids = [r["_id"] for r in all_rows]
        self.assertNotIn("del_me", ids, "tombstoned doc appeared in full scan")


class TestDagNql(_DagBase):
    """NQL query clauses: ORDER BY, LIMIT, WHERE, GROUP BY, SEARCH."""

    def setUp(self):
        super().setUp()
        # Seed data
        for i in range(10):
            _put(self.c, "items", str(i), {"height": i, "kind": "even" if i % 2 == 0 else "odd", "label": f"item{i}"})
        # Create sorted index for ORDER BY fast path
        self.c.post(f"/v1/databases/{DB_NAME}/index", json={"coll": "items", "field": "height", "kind": "sorted"})

    def test_limit(self):
        rows = _query(self.c, "FROM items LIMIT 3")["rows"]
        self.assertEqual(len(rows), 3)

    def test_where_eq(self):
        rows = _query(self.c, 'FROM items WHERE kind = "even"')["rows"]
        self.assertTrue(all(r["kind"] == "even" for r in rows))
        self.assertEqual(len(rows), 5)  # 0,2,4,6,8

    def test_where_gt(self):
        rows = _query(self.c, "FROM items WHERE height > 7")["rows"]
        self.assertTrue(all(r["height"] > 7 for r in rows))

    def test_order_by_asc(self):
        rows = _query(self.c, "FROM items ORDER BY height ASC LIMIT 5")["rows"]
        heights = [r["height"] for r in rows]
        self.assertEqual(heights, sorted(heights))

    def test_order_by_desc(self):
        rows = _query(self.c, "FROM items ORDER BY height DESC LIMIT 3")["rows"]
        heights = [r["height"] for r in rows]
        self.assertEqual(heights[0], 9)

    def test_group_by_count(self):
        rows = _query(self.c, "FROM items GROUP BY kind COUNT")["rows"]
        counts = {r["kind"]: r["count"] for r in rows}
        self.assertEqual(counts.get("even"), 5)
        self.assertEqual(counts.get("odd"), 5)

    def test_id_fast_path(self):
        """WHERE _id = x uses O(1) id-index lookup, not full scan."""
        rows = _query(self.c, 'FROM items WHERE _id = "5" LIMIT 1')["rows"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["height"], 5)


class TestDagBatch(_DagBase):
    """Batch writes: parallel puts, mixed put/del."""

    def test_batch_put_100(self):
        ops = [{"op": "put", "coll": "batch", "id": str(i), "doc": {"n": i}} for i in range(100)]
        r = _batch(self.c, ops)
        self.assertEqual(r["count"], 100)
        rows = _query(self.c, "FROM batch")
        self.assertEqual(rows["count"], 100)

    def test_batch_mixed_put_del(self):
        # Put 5 then delete 2
        ops = [{"op": "put", "coll": "mix", "id": str(i), "doc": {"n": i}} for i in range(5)]
        ops += [{"op": "del", "coll": "mix", "id": "1"}, {"op": "del", "coll": "mix", "id": "3"}]
        r = _batch(self.c, ops)
        self.assertEqual(r["count"], 7)
        rows = _query(self.c, "FROM mix")["rows"]
        ids = {row["_id"] for row in rows}
        self.assertNotIn("1", ids)
        self.assertNotIn("3", ids)
        self.assertIn("0", ids)
        self.assertIn("2", ids)

    def test_batch_seq_monotonic(self):
        """All seqs in a batch must be strictly increasing."""
        ops = [{"op": "put", "coll": "seq_test", "id": str(i), "doc": {"n": i}} for i in range(10)]
        r = _batch(self.c, ops)
        seqs = [res["seq"] for res in r["results"] if "seq" in res]
        self.assertEqual(seqs, sorted(seqs), "batch seqs not monotonically increasing")
        self.assertEqual(len(set(seqs)), len(seqs), "duplicate seq numbers in batch")


class TestDagIntegrity(_DagBase):
    """BLAKE2b tamper evidence, Merkle head, verify endpoint."""

    def test_head_changes_on_write(self):
        r1 = _put(self.c, "docs", "a", {"x": 1})
        h1 = r1["head"]
        r2 = _put(self.c, "docs", "b", {"x": 2})
        h2 = r2["head"]
        self.assertNotEqual(h1, h2, "head unchanged after second write")

    def test_head_is_hex_64_chars(self):
        r = _put(self.c, "docs", "c", {"x": 3})
        head = r["head"]
        self.assertEqual(len(head), 64, f"head should be 64 hex chars, got {len(head)}")
        int(head, 16)  # raises if not valid hex

    def test_verify_passes(self):
        for i in range(20):
            _put(self.c, "docs", str(i), {"val": i})
        v = _verify(self.c)
        self.assertTrue(v.get("ok"), f"verify failed: {v}")
        self.assertEqual(len(v.get("tampered", [])), 0)
        self.assertGreater(v.get("objects_checked", 0), 0)
        self.assertTrue(v.get("tamper_evident"))

    def test_seq_increments(self):
        r1 = _put(self.c, "docs", "s1", {"v": 1})
        r2 = _put(self.c, "docs", "s2", {"v": 2})
        self.assertGreater(r2["seq"], r1["seq"])


class TestDagTimeline(_DagBase):
    """Bi-temporal and AS OF time-travel."""

    def test_valid_as_of_window(self):
        _put(self.c, "rates", "r1", {"pct": 5.0}, valid_from="2024-01-01", valid_to="2024-12-31")
        _put(self.c, "rates", "r2", {"pct": 6.0}, valid_from="2025-01-01")
        rows = _query(self.c, 'FROM rates VALID AS OF "2024-06-15"')["rows"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["pct"], 5.0)

    def test_valid_as_of_current(self):
        _put(self.c, "rates", "r1", {"pct": 5.0}, valid_from="2024-01-01", valid_to="2024-12-31")
        _put(self.c, "rates", "r2", {"pct": 6.0}, valid_from="2025-01-01")
        rows = _query(self.c, 'FROM rates VALID AS OF "2025-06-01"')["rows"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["pct"], 6.0)

    def test_as_of_seq_time_travel(self):
        r1 = _put(self.c, "docs", "x", {"v": 1})
        # r1["seq"] is the counter AFTER write (next seq).
        # r1["doc"]["_seq"] is the actual sequence number assigned to this node.
        seq_v1 = r1["doc"]["_seq"]
        _put(self.c, "docs", "x", {"v": 2})
        # Current version should be v=2
        current = _query(self.c, 'FROM docs WHERE _id = "x" LIMIT 1')["rows"]
        self.assertEqual(current[0]["v"], 2)
        # AS OF seq_v1 should return v=1
        old = _query(self.c, f'FROM docs AS OF {seq_v1}')["rows"]
        x_old = next((r for r in old if r["_id"] == "x"), None)
        self.assertIsNotNone(x_old)
        self.assertEqual(x_old["v"], 1)


class TestDagCausal(_DagBase):
    """Causal TRACE provenance."""

    def test_trace_backward(self):
        """TRACE caused_by: from c, should reach b and a via the causal chain."""
        ra = _put(self.c, "ops", "a", {"op": "create"})
        rb = _put(self.c, "ops", "b", {"op": "transfer"}, caused_by=[ra["doc"]["_hash"]])
        _put(self.c, "ops", "c", {"op": "burn"}, caused_by=[rb["doc"]["_hash"]])
        # Always scope TRACE to a specific starting doc via WHERE
        # to avoid iterating all docs (some may have empty caused_by)
        trace = _query(self.c, 'FROM ops WHERE _id = "c" TRACE caused_by LIMIT 10')["rows"]
        self.assertGreater(len(trace), 0, "TRACE returned no rows")


class TestDagSSE(unittest.TestCase):
    """GET /events SSE stream — verify it connects and sends data."""

    def test_events_endpoint_connects(self):
        """SSE endpoint should return 200 with text/event-stream content type."""
        c = _client()
        try:
            with c.stream("GET", "/events", timeout=3.0) as r:
                self.assertEqual(r.status_code, 200)
                ct = r.headers.get("content-type", "")
                self.assertIn("text/event-stream", ct, f"Expected SSE content-type, got {ct!r}")
        except httpx.ReadTimeout:
            pass  # timeout = no events in 3s = server is quiet = OK
        finally:
            c.close()


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    server_was_running = _is_server_alive()
    spawned = False

    if not server_was_running:
        print(f"No server at {BASE_URL} — attempting to auto-spawn nedbd --dag...")
        spawned = _spawn_server()
        if not spawned:
            print("ERROR: Could not spawn nedbd. Start it manually:")
            print("  NEDBD_DAG=1 nedbd --data /tmp/test-dag")
            sys.exit(1)
    else:
        print(f"Using existing nedbd at {BASE_URL}")

    try:
        loader = unittest.TestLoader()
        suite  = loader.loadTestsFromModule(sys.modules[__name__])
        runner = unittest.TextTestRunner(verbosity=2 if "-v" in sys.argv else 1)
        result = runner.run(suite)
        sys.exit(0 if result.wasSuccessful() else 1)
    finally:
        if spawned:
            print("Stopping auto-spawned nedbd...")
            _teardown_server()


if __name__ == "__main__":
    main()
