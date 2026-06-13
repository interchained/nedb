"""NEDB invariant tests. Run: python3 -m pytest -q   (or python3 tests/test_nedb.py)"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nedb import NEDB, ReplayError  # noqa: E402


def fresh():
    db = NEDB()
    db.create_index("users", "status", "eq")
    db.create_index("users", "age", "ordered")
    db.create_index("users", "bio", "search")
    db.put("users", "alice", {"name": "Alice", "age": 31, "status": "active", "bio": "rust db"})
    db.put("users", "bob", {"name": "Bob", "age": 24, "status": "active", "bio": "python data"})
    db.put("users", "carol", {"name": "Carol", "age": 41, "status": "inactive", "bio": "rust systems"})
    return db


def test_idempotency():
    db = fresh()
    n = len(db.log)
    db.put("users", "z", {"name": "Z", "age": 1, "status": "active"}, idem="k1")
    db.put("users", "z", {"name": "Z", "age": 1, "status": "active"}, idem="k1")
    assert len(db.log) == n + 1, "idempotent retry must not append twice"


def test_replay_protection():
    db = fresh()
    db.put("o", "1", {"t": 1}, client="svc", nonce=10)
    try:
        db.put("o", "1", {"t": 2}, client="svc", nonce=5)
        assert False, "stale nonce must be rejected"
    except ReplayError:
        pass


def test_filter_sort():
    db = fresh()
    rows = db.query('FROM users WHERE age >= 25 AND status = "active" ORDER BY age DESC')
    assert [r["name"] for r in rows] == ["Alice"], rows


def test_search():
    db = fresh()
    names = sorted(r["name"] for r in db.query('FROM users SEARCH "rust"'))
    assert names == ["Alice", "Carol"], names


def test_relations_and_traverse():
    db = fresh()
    db.link("users:alice", "follows", "users:bob")
    assert db.neighbors("users:alice", "follows") == ["users:bob"]
    rows = db.q("users").where("_id", "=", "alice").traverse("follows").run()
    assert [r["name"] for r in rows] == ["Bob"]


def test_time_travel():
    db = fresh()
    s = db.seq
    db.put("users", "alice", {"name": "Alice", "age": 31, "status": "active", "bio": "rust db", "city": "X"})
    assert db.get("users", "alice")["city"] == "X"
    assert "city" not in db.get("users", "alice", as_of=s)


def test_relation_time_travel():
    db = fresh()
    db.link("users:alice", "follows", "users:bob")
    s = db.seq
    db.unlink("users:alice", "follows", "users:bob")
    assert db.neighbors("users:alice", "follows") == []
    assert db.neighbors("users:alice", "follows", as_of=s) == ["users:bob"]


def test_integrity_and_determinism():
    db = fresh()
    assert db.verify()
    assert db.verify_determinism()
    saved = db.log.ops[0].payload["doc"]["name"]
    db.log.ops[0].payload["doc"]["name"] = "EVIL"
    assert not db.verify(), "tamper must be detected"
    db.log.ops[0].payload["doc"]["name"] = saved
    assert db.verify()


def test_files_dedup_and_roundtrip():
    db = fresh()
    base = ("".join(f"row {i:06d} payload\n" for i in range(30000))).encode()
    v1 = db.put_file("f", base)
    edited = bytearray(base)
    edited[300000:300010] = b"CHANGED!!!!"
    v2 = db.put_file("f", bytes(edited))
    st = db.compression_stats("warm")
    assert db.get_file("f", v1) == base
    assert db.get_file("f", v2) == bytes(edited)
    assert st["dedup_hits"] > 0, "v2 must reuse v1 chunks via content-defined chunking"
    assert st["ratio"] > 1.0


def test_merkle_proof():
    db = fresh()
    data = ("".join(f"x{i}\n" for i in range(40000))).encode()
    v = db.put_file("m", data)
    leaf, proof, root = db.file_proof("m", 1, v)
    assert NEDB.verify_proof(leaf, proof, root)
    assert not NEDB.verify_proof("00" * 32, proof, root)


def run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = 0
    for fn in fns:
        fn()
        print(f"  PASS  {fn.__name__}")
        passed += 1
    print(f"\n{passed}/{len(fns)} tests passed ✅")


if __name__ == "__main__":
    run_all()
