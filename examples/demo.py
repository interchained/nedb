"""End-to-end NEDB demo: every headline feature, runnable in one shot."""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nedb import NEDB, ReplayError  # noqa: E402


def line(t=""):
    print(t)


def rule(t):
    print("\n" + "=" * 64 + f"\n  {t}\n" + "=" * 64)


db = NEDB()

rule("1. Indexes + records")
db.create_index("users", "status", "eq")
db.create_index("users", "age", "ordered")
db.create_index("users", "bio", "search")

db.put("users", "alice", {"name": "Alice", "age": 31, "status": "active",
                          "city": "Austin", "bio": "rust systems hacker and db nerd"})
db.put("users", "bob", {"name": "Bob", "age": 24, "status": "active",
                        "city": "Denver", "bio": "python data plumber"})
db.put("users", "carol", {"name": "Carol", "age": 41, "status": "inactive",
                          "city": "Reno", "bio": "rust and distributed systems"})
line(f"inserted 3 users; log seq head = {db.seq}, head hash = {db.head[:16]}...")

rule("2. Idempotency (same idem key applied twice -> one write)")
before = len(db.log)
db.put("users", "dave", {"name": "Dave", "age": 28, "status": "active", "bio": "ops"},
       idem="signup-dave-001")
db.put("users", "dave", {"name": "Dave", "age": 28, "status": "active", "bio": "ops"},
       idem="signup-dave-001")  # retry — deduplicated
after = len(db.log)
line(f"two identical idempotent puts -> log grew by {after - before} op (replay-safe retry)")

rule("3. Replay protection (stale nonce rejected)")
db.put("orders", "o1", {"total": 10}, client="svcA", nonce=5)
try:
    db.put("orders", "o1", {"total": 999}, client="svcA", nonce=3)  # replay an old op
    line("!! replay accepted — BUG")
except ReplayError as e:
    line(f"rejected stale nonce as expected -> {e}")

rule("4. NQL: filter + sort")
for u in db.query('FROM users WHERE age >= 25 AND status = "active" ORDER BY age DESC'):
    line(f"  {u['name']:6} age={u['age']} status={u['status']}")

rule("5. Full-text search (inverted index)")
for u in db.query('FROM users SEARCH "rust"'):
    line(f"  {u['name']:6} bio={u['bio']!r}")

rule("6. Relations + graph traversal")
db.link("users:alice", "follows", "users:bob")
db.link("users:bob", "follows", "users:carol")
line(f"alice follows: {db.neighbors('users:alice', 'follows')}")
line("NQL TRAVERSE (who does alice follow):")
for u in db.q("users").where("_id", "=", "alice").traverse("follows").run():
    line(f"  -> {u['name']}")

rule("7. Time-travel (AS OF a past seq)")
seq_before_move = db.seq
db.put("users", "alice", {"name": "Alice", "age": 31, "status": "active",
                          "city": "Lisbon", "bio": "rust systems hacker and db nerd"})
line(f"alice city now      : {db.get('users', 'alice')['city']}")
line(f"alice city AS OF {seq_before_move:>3}: "
     f"{db.get('users', 'alice', as_of=seq_before_move)['city']}")
line("same via NQL:")
for u in db.query(f"FROM users AS OF {seq_before_move} WHERE _id = \"alice\""):
    line(f"  alice@{seq_before_move} -> {u['city']}")

rule("8. Integrity + determinism")
line(f"log hash-chain verifies : {db.verify()}")
line(f"state == replay(log)    : {db.verify_determinism()}")
saved = db.log.ops[0].payload["doc"].get("city")
db.log.ops[0].payload["doc"]["city"] = "TAMPERED"
line(f"after tampering op[0]   : verify() = {db.verify()}  (tamper detected)")
db.log.ops[0].payload["doc"]["city"] = saved  # restore

rule("9. git-style files + Cascade compression + dedup across versions")
base = ("".join(f"line {i:05d}: the quick brown fox jumps over the lazy dog\n"
                for i in range(20000))).encode()
v1 = db.put_file("notes.txt", base, tier="warm")
edited = bytearray(base)
edited[500000:500040] = b"<<< a small edit in the middle >>>>>>>>>"
v2 = db.put_file("notes.txt", bytes(edited), tier="warm")
st = db.compression_stats("warm")
line(f"stored 2 versions of a {len(base)//1024} KiB file (~{2*len(base)//1024} KiB logical)")
line(f"  unique chunks stored : {st['unique_chunks']}")
line(f"  dedup hits (reused)  : {st['dedup_hits']}  <- v2 reused almost all of v1")
line(f"  warm ratio (zlib)    : {st['ratio']}x  ({st['logical_bytes']//1024} KiB -> "
     f"{st['stored_bytes']//1024} KiB)")
db.put_file("notes_cold.bin", base, tier="cold")
line(f"  cold ratio (LZMA)    : {db.compression_stats('cold')['ratio']}x  (archival tier)")
assert db.get_file("notes.txt", v1) == base, "v1 roundtrip"
assert db.get_file("notes.txt", v2) == bytes(edited), "v2 roundtrip"
line(f"  v1 root (anchorable) : {db.file_root('notes.txt', v1)[:32]}...")
leaf, proof, root = db.file_proof("notes.txt", 0, v1)
line(f"  Merkle proof chunk#0 : verifies = {NEDB.verify_proof(leaf, proof, root)} "
     f"({len(proof)} hashes, O(log n))")

rule("10. Embedded micro-benchmark (in-process, no socket hop)")
N = 50000
t0 = time.perf_counter()
for i in range(N):
    db.put("kv", str(i), {"v": i}, client="bench", nonce=i + 1)
t1 = time.perf_counter()
for i in range(N):
    db.get("kv", str(i))
t2 = time.perf_counter()
wq = time.perf_counter()
_ = db.query('FROM users WHERE status = "active"')
wq2 = time.perf_counter()
line(f"  SET  {N}: {N/(t1-t0):,.0f} ops/s  ({(t1-t0)/N*1e9:,.0f} ns/op)")
line(f"  GET  {N}: {N/(t2-t1):,.0f} ops/s  ({(t2-t1)/N*1e9:,.0f} ns/op)")
line(f"  indexed query latency: {(wq2-wq)*1e6:,.1f} us")
line("\n(reference engine is pure Python; the Rust core targets ~50-150x this. "
     "The point proven here is the ARCHITECTURE — bench/bench_redis.py runs the\n head-to-head vs Redis when a server is available.)")
line("\nNEDB demo complete. ✅")
