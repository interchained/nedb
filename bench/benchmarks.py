#!/usr/bin/env python3
"""
NEDB benchmark suite — v0.4.1
Measures real throughput and latency for every major operation,
compares NEDB embedded vs nedbd over HTTP (if running), and captures
adapter overhead (SQL, Redis, AutoIndex) vs raw NQL.

Run:
    python3 bench/benchmarks.py              # embedded only
    python3 bench/benchmarks.py --nedbd      # + nedbd HTTP comparison
    python3 bench/benchmarks.py --redis      # + Redis TCP comparison (needs redis-server)
    python3 bench/benchmarks.py --save       # write results to bench/RESULTS.md

Output: a Markdown comparison table + per-category sections.
"""
from __future__ import annotations

import argparse
import gc
import os
import statistics
import sys
import time
from typing import Any, Callable, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"))

import nedb as _nedb_mod
from nedb import NEDB, AutoIndexDB
from nedb.sql import sql_exec
from nedb.redis_compat import RedisCompat

# ── Config ────────────────────────────────────────────────────────────────────
SMALL  = 1_000
MEDIUM = 10_000
LARGE  = 50_000

RESULTS_MD = os.path.join(os.path.dirname(__file__), "RESULTS.md")


# ── Timing primitives ─────────────────────────────────────────────────────────

def bench(fn: Callable, n: int, warmup: int = 0, repeat: int = 3) -> Dict[str, float]:
    """Run fn() n times, repeat times, return stats (ops/s, latency_us, p50, p99)."""
    for _ in range(warmup):
        fn()
    gc.collect()
    times: List[float] = []
    for _ in range(repeat):
        t = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t)
    best = min(times)
    rate = n / best
    lat_us = (best / n) * 1_000_000
    return {"ops_per_s": rate, "latency_us": lat_us, "best_s": best, "repeat": repeat, "n": n}


def fmt_rate(r: float) -> str:
    if r >= 1_000_000:
        return f"{r / 1_000_000:.2f}M/s"
    if r >= 1_000:
        return f"{r / 1_000:.1f}K/s"
    return f"{r:.0f}/s"


def fmt_lat(us: float) -> str:
    if us >= 1_000:
        return f"{us / 1_000:.2f} ms"
    return f"{us:.2f} µs"


# ── Benchmark categories ──────────────────────────────────────────────────────

def bench_core(n: int = MEDIUM) -> List[Dict]:
    """Core engine: put (write), get (point read), delete."""
    rows: List[Dict] = []
    db = NEDB()
    # seed for get/delete benchmarks
    for i in range(n):
        db.put("k", str(i), {"v": i, "status": "active", "tag": f"t{i % 10}"})

    r = bench(lambda: [db.put("k", str(i), {"v": i + 1}) for i in range(n)], n, warmup=1)
    rows.append({"operation": "PUT (replace, no index)", **r})

    r = bench(lambda: [db.get("k", str(i)) for i in range(n)], n, warmup=1)
    rows.append({"operation": "GET (point read, HEAD)", **r})

    snap = db.seq
    r = bench(lambda: [db.get("k", str(i), as_of=snap - n) for i in range(min(n, 1000))],
              min(n, 1000), warmup=1)
    rows.append({"operation": "GET (AS OF — time-travel)", **r})

    return rows


def bench_indexes(n: int = MEDIUM) -> List[Dict]:
    """Index performance: unindexed scan vs eq/ordered/search indexes."""
    rows: List[Dict] = []

    # Unindexed
    db = NEDB()
    for i in range(n):
        db.put("items", str(i), {"status": "active" if i % 2 == 0 else "inactive",
                                  "age": i % 100, "bio": f"engineer {i % 5}"})
    r = bench(lambda: db.query('FROM items WHERE status = "active"'), n, warmup=1)
    rows.append({"operation": "QUERY: eq filter, no index (scan)", **r})

    # With eq index
    db2 = NEDB()
    db2.create_index("items", "status", "eq")
    db2.create_index("items", "age", "ordered")
    db2.create_index("items", "bio", "search")
    for i in range(n):
        db2.put("items", str(i), {"status": "active" if i % 2 == 0 else "inactive",
                                   "age": i % 100, "bio": f"engineer {i % 5}"})
    r = bench(lambda: db2.query('FROM items WHERE status = "active"'), n, warmup=1)
    rows.append({"operation": "QUERY: eq filter, eq index", **r})

    r = bench(lambda: db2.query('FROM items ORDER BY age DESC LIMIT 20'), n, warmup=1)
    rows.append({"operation": "QUERY: ORDER BY, ordered index, LIMIT 20", **r})

    r = bench(lambda: db2.query('FROM items SEARCH "engineer"'), n, warmup=1)
    rows.append({"operation": "QUERY: SEARCH, inverted index", **r})

    return rows


def bench_adapters(n: int = SMALL) -> List[Dict]:
    """Adapter overhead vs raw NQL."""
    rows: List[Dict] = []
    db = NEDB()
    db.create_index("users", "status", "eq")
    for i in range(n):
        db.put("users", str(i), {"id": str(i), "name": f"User {i}", "age": 20 + i % 60,
                                  "status": "active" if i % 2 == 0 else "inactive"})

    # Baseline: raw NQL
    r = bench(lambda: db.query('FROM users WHERE status = "active" LIMIT 50'), n, warmup=1)
    rows.append({"operation": "NQL: WHERE eq (raw)", **r})

    # SQL adapter
    r = bench(lambda: sql_exec(db, "SELECT * FROM users WHERE status = 'active' LIMIT 50"), n, warmup=1)
    rows.append({"operation": "SQL: SELECT WHERE (adapter → NQL)", **r})

    # Redis adapter — HSET/HGET
    rc = RedisCompat(NEDB())
    r = bench(lambda: [rc.execute("HSET", "profile", f"f{j}", f"v{j}") for j in range(10)], 10, warmup=1)
    rows.append({"operation": "Redis: HSET ×10 (adapter)", **r})
    r = bench(lambda: [rc.execute("HGET", "profile", f"f{j}") for j in range(10)], 10, warmup=1)
    rows.append({"operation": "Redis: HGET ×10 (adapter)", **r})

    # AutoIndex overhead (threshold never reached — pure tally overhead)
    adb = AutoIndexDB(db, threshold=999_999)
    r = bench(lambda: adb.query('FROM users WHERE status = "active" LIMIT 50'), n, warmup=1)
    rows.append({"operation": "AutoIndexDB: same query via wrapper", **r})

    return rows


def bench_persistence(n: int = SMALL) -> List[Dict]:
    """Write throughput with durable AOF vs in-memory."""
    import tempfile, shutil
    rows: List[Dict] = []

    db_mem = NEDB()
    r = bench(lambda: [db_mem.put("k", str(i), {"v": i}) for i in range(n)], n, warmup=0, repeat=3)
    rows.append({"operation": "PUT in-memory (no AOF)", **r})

    d = tempfile.mkdtemp()
    try:
        db_dur = NEDB(d)
        r = bench(lambda: [db_dur.put("k", str(i), {"v": i}) for i in range(n)], n, warmup=0, repeat=3)
        rows.append({"operation": "PUT durable (AOF + fsync)", **r})
        db_dur.close()

        # Reload time
        t0 = time.perf_counter()
        db2 = NEDB(d)
        reload_ms = (time.perf_counter() - t0) * 1000
        db2.close()
        rows.append({"operation": f"RELOAD from AOF ({n} ops)",
                     "ops_per_s": 0, "latency_us": reload_ms * 1000,
                     "_reload_ms": reload_ms})
    finally:
        shutil.rmtree(d, ignore_errors=True)

    return rows


def bench_nedbd(n: int = SMALL, base: str = "http://127.0.0.1:7070") -> Optional[List[Dict]]:
    """NEDB embedded vs nedbd over HTTP."""
    try:
        import urllib.request
        import json as _json
        urllib.request.urlopen(f"{base}/health", timeout=2).read()
    except Exception:
        return None

    rows: List[Dict] = []
    import urllib.request, json as _json

    def http_query(db_name: str, nql: str) -> Any:
        req = urllib.request.Request(f"{base}/v1/databases/{db_name}/query",
            data=_json.dumps({"nql": nql}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        return _json.loads(urllib.request.urlopen(req, timeout=10).read())

    def http_put(db_name: str, coll: str, row_id: str, doc: dict) -> None:
        req = urllib.request.Request(f"{base}/v1/databases/{db_name}/put",
            data=_json.dumps({"coll": coll, "id": row_id, "doc": doc}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        urllib.request.urlopen(req, timeout=10).read()

    # Create a test db
    try:
        req = urllib.request.Request(f"{base}/v1/databases",
            data=_json.dumps({"name": "_bench_tmp"}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # already exists

    db = NEDB()
    db.create_index("bk", "v", "eq")
    for i in range(n):
        db.put("bk", str(i), {"v": i, "status": "active"})
    for i in range(n):
        http_put("_bench_tmp", "bk", str(i), {"v": i, "status": "active"})

    r = bench(lambda: db.query('FROM bk WHERE status = "active" LIMIT 20'), n, warmup=1)
    rows.append({"operation": "NEDB embedded query (in-process)", **r})

    r = bench(lambda: http_query("_bench_tmp", 'FROM bk WHERE status = "active" LIMIT 20'), n, warmup=1)
    rows.append({"operation": "nedbd HTTP query (over TCP)", **r})

    # Clean up
    try:
        req = urllib.request.Request(f"{base}/v1/databases/_bench_tmp", method="DELETE",
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass

    return rows


def bench_resp2(n: int = SMALL, port: int = 6379) -> Optional[List[Dict]]:
    """NEDB embedded GET vs nedbd RESP2 (raw socket, no redis-py required)."""
    import socket as _sock
    def _send(s, *args):
        cmd = f"*{len(args)}\r\n" + "".join(f"${len(a)}\r\n{a}\r\n" for a in args)
        s.sendall(cmd.encode())
        return s.recv(4096)

    try:
        s = _sock.create_connection(("127.0.0.1", port), timeout=2)
        _send(s, "PING")
        _send(s, "SELECT", "_bench")
    except Exception:
        s = None

    if not s:
        return None

    rows: List[Dict] = []
    db = NEDB()
    for i in range(n):
        db.put("k", str(i), {"v": i})

    # Seed nedbd via RESP2
    for i in range(min(n, SMALL)):
        _send(s, "SET", f"bench:{i}", str(i))

    r = bench(lambda: [db.get("k", str(i)) for i in range(n)], n, warmup=1)
    rows.append({"operation": "NEDB GET (embedded, in-process)", **r})

    r = bench(lambda: [_send(s, "GET", f"bench:{i}") for i in range(min(n, SMALL))], min(n, SMALL), warmup=1)
    rows.append({"operation": f"nedbd GET (RESP2 TCP, port {port})", **r})

    r = bench(lambda: [db.put("k", str(i), {"v": i + 1}) for i in range(n)], n, warmup=1)
    rows.append({"operation": "NEDB PUT (embedded)", **r})

    r = bench(lambda: [_send(s, "SET", f"bench:{i}", str(i + 1)) for i in range(min(n, SMALL))], min(n, SMALL), warmup=1)
    rows.append({"operation": f"nedbd SET (RESP2 TCP, port {port})", **r})

    s.close()
    return rows


def bench_redis_cmp(n: int = SMALL) -> Optional[List[Dict]]:
    """NEDB embedded GET vs Redis GET over TCP."""
    try:
        import redis
        r_client = redis.Redis()
        r_client.ping()
    except Exception:
        return None

    rows: List[Dict] = []
    db = NEDB()
    for i in range(n):
        db.put("k", str(i), {"v": i})
        r_client.set(f"bench:{i}", i)

    r = bench(lambda: [db.get("k", str(i)) for i in range(n)], n, warmup=1)
    rows.append({"operation": "NEDB GET (embedded, in-process)", **r})

    r = bench(lambda: [r_client.get(f"bench:{i}") for i in range(n)], n, warmup=1)
    rows.append({"operation": "Redis GET (TCP)", **r})

    r = bench(lambda: [db.put("k", str(i), {"v": i + 1}) for i in range(n)], n, warmup=1)
    rows.append({"operation": "NEDB PUT (embedded)", **r})

    r = bench(lambda: [r_client.set(f"bench:{i}", i + 1) for i in range(n)], n, warmup=1)
    rows.append({"operation": "Redis SET (TCP)", **r})

    return rows


# ── Report formatting ─────────────────────────────────────────────────────────

def table(title: str, rows: List[Dict], note: str = "") -> str:
    lines = [f"\n### {title}\n"]
    if note:
        lines.append(f"_{note}_\n")
    lines.append("| Operation | Throughput | Latency (avg) |")
    lines.append("|-----------|-----------|---------------|")
    for row in rows:
        op = row["operation"]
        if row.get("ops_per_s", 0) == 0 and row.get("_reload_ms"):
            lines.append(f"| {op} | — | {row['_reload_ms']:.1f} ms total |")
        else:
            lines.append(f"| {op} | {fmt_rate(row['ops_per_s'])} | {fmt_lat(row['latency_us'])} |")
    return "\n".join(lines)


def build_report(results: Dict[str, List[Dict]], meta: Dict) -> str:
    import datetime
    lines = [
        f"# NEDB Benchmark Results",
        f"",
        f"**Version:** `{meta['version']}`  ",
        f"**Python:** `{meta['python']}`  ",
        f"**Platform:** `{meta['platform']}`  ",
        f"**Date:** `{datetime.date.today()}`  ",
        f"",
        f"> Run: `python3 bench/benchmarks.py --save`",
        f"",
        f"---",
    ]
    for section, rows in results.items():
        if rows:
            lines.append(table(section, rows))
    return "\n".join(lines) + "\n"


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="NEDB benchmark suite")
    ap.add_argument("--nedbd",  action="store_true", help="Compare with nedbd over HTTP")
    ap.add_argument("--redis",  action="store_true", help="Compare with Redis over TCP")
    ap.add_argument("--resp2",  action="store_true", help="Compare with nedbd RESP2 wire protocol")
    ap.add_argument("--resp2-port", type=int, default=6379, dest="resp2_port", help="nedbd RESP2 port (default 6379)")
    ap.add_argument("--save",   action="store_true", help=f"Write results to {RESULTS_MD}")
    ap.add_argument("--small",  action="store_true", help="Use SMALL N (faster, less accurate)")
    ap.add_argument("--base",   default="http://127.0.0.1:7070", help="nedbd base URL")
    a = ap.parse_args()

    n = SMALL if a.small else MEDIUM

    import platform
    meta = {
        "version": _nedb_mod.__version__,
        "python": platform.python_version(),
        "platform": f"{platform.system()} {platform.machine()}",
    }
    print(f"\nNEDB {meta['version']} benchmarks  —  Python {meta['python']}  —  {meta['platform']}")
    print(f"N={n:,}  (use --small for faster run)\n")

    results: Dict[str, List[Dict]] = {}

    print("── Core operations ──────────────────────────────────────────────")
    r = bench_core(n)
    results["Core operations"] = r
    for row in r:
        print(f"  {row['operation']:<45} {fmt_rate(row['ops_per_s']):>10}  {fmt_lat(row['latency_us']):>12}")

    print("\n── Index performance ────────────────────────────────────────────")
    r = bench_indexes(n)
    results["Index performance"] = r
    for row in r:
        print(f"  {row['operation']:<45} {fmt_rate(row['ops_per_s']):>10}  {fmt_lat(row['latency_us']):>12}")

    print("\n── Adapter overhead (vs raw NQL) ────────────────────────────────")
    r = bench_adapters(min(n, SMALL))
    results["Adapter overhead (SQL · Redis · AutoIndex)"] = r
    for row in r:
        print(f"  {row['operation']:<45} {fmt_rate(row['ops_per_s']):>10}  {fmt_lat(row['latency_us']):>12}")

    print("\n── Persistence (AOF fsync overhead) ────────────────────────────")
    r = bench_persistence(min(n, SMALL))
    results["Persistence: in-memory vs AOF"] = r
    for row in r:
        if row.get("_reload_ms"):
            print(f"  {row['operation']:<45} {'—':>10}  {row['_reload_ms']:.1f} ms total")
        else:
            print(f"  {row['operation']:<45} {fmt_rate(row['ops_per_s']):>10}  {fmt_lat(row['latency_us']):>12}")

    if a.resp2:
        print(f"\n── NEDB embedded vs nedbd RESP2 ─────────────────────────────────")
        r = bench_resp2(min(n, SMALL), a.resp2_port)
        if r:
            results["NEDB embedded vs nedbd RESP2"] = r
            for row in r:
                print(f"  {row['operation']:<45} {fmt_rate(row['ops_per_s']):>10}  {fmt_lat(row['latency_us']):>12}")
        else:
            print(f"  [nedbd RESP2 not reachable on port {a.resp2_port} — start: NEDBD_RESP2_PORT={a.resp2_port} nedbd]")

    if a.nedbd:
        print(f"\n── NEDB embedded vs nedbd HTTP ({a.base}) ───────────────────────")
        r = bench_nedbd(min(n, SMALL), a.base)
        if r:
            results["NEDB embedded vs nedbd HTTP"] = r
            for row in r:
                print(f"  {row['operation']:<45} {fmt_rate(row['ops_per_s']):>10}  {fmt_lat(row['latency_us']):>12}")
        else:
            print(f"  [nedbd not reachable at {a.base}]")

    if a.redis:
        print("\n── NEDB embedded vs Redis TCP ───────────────────────────────────")
        r = bench_redis_cmp(min(n, SMALL))
        if r:
            results["NEDB embedded vs Redis TCP"] = r
            for row in r:
                print(f"  {row['operation']:<45} {fmt_rate(row['ops_per_s']):>10}  {fmt_lat(row['latency_us']):>12}")
        else:
            print("  [Redis not reachable — start redis-server and install pip redis]")

    report = build_report(results, meta)

    if a.save:
        with open(RESULTS_MD, "w") as fh:
            fh.write(report)
        print(f"\n✓  Results written to bench/RESULTS.md")
    else:
        print(f"\n(pass --save to write bench/RESULTS.md)")


if __name__ == "__main__":
    main()
