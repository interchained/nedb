#!/usr/bin/env python3
"""
test_dag_perf.py — NEDB v2 DAG write/read throughput benchmark.

Requires a running nedbd server (--dag mode recommended):
    NEDBD_DAG=1 nedbd --data /tmp/nedb-perf-test

Run:
    python3 tests/test_dag_perf.py
    python3 tests/test_dag_perf.py --url http://127.0.0.1:7070 --n 10000

Reports p50/p95/p99 latency + throughput for:
    - 10k sequential writes
    - 10k concurrent writes  (asyncio.gather, configurable concurrency)
    - 100k reads             (sequential point lookups)
    - 100k NQL queries       (FROM bench LIMIT 1)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from typing import List

try:
    import httpx
except ImportError:
    print("ERROR: httpx required — pip install httpx")
    sys.exit(1)

BASE_URL = os.getenv("NEDB_URL", "http://127.0.0.1:7070")
DB_NAME  = os.getenv("NEDB_PERF_DB", "bench")
TOKEN    = os.getenv("NEDBD_TOKEN", "")
WRITE_TIMEOUT = httpx.Timeout(connect=2.0, read=60.0, write=60.0, pool=5.0)
READ_TIMEOUT  = httpx.Timeout(connect=2.0, read=10.0, write=10.0, pool=5.0)

# ── helpers ───────────────────────────────────────────────────────────────────

def _headers() -> dict:
    h = {"Content-Type": "application/json"}
    if TOKEN:
        h["Authorization"] = f"Bearer {TOKEN}"
    return h


def percentiles(latencies_ms: List[float]) -> dict:
    s = sorted(latencies_ms)
    n = len(s)
    def pct(p: float) -> float:
        idx = int(n * p / 100)
        return round(s[min(idx, n - 1)], 3)
    return {
        "p50":  pct(50),
        "p95":  pct(95),
        "p99":  pct(99),
        "p999": pct(99.9),
        "min":  round(s[0], 3),
        "max":  round(s[-1], 3),
        "mean": round(statistics.mean(s), 3),
    }


def print_result(label: str, n: int, elapsed: float, latencies: List[float]) -> None:
    p = percentiles(latencies)
    rps = n / max(elapsed, 0.001)
    print(f"\n  {label}")
    print(f"    ops:        {n:>10,}")
    print(f"    elapsed:    {elapsed:>10.3f}s")
    print(f"    throughput: {rps:>10,.0f} ops/s")
    print(f"    p50:        {p['p50']:>10.3f} ms")
    print(f"    p95:        {p['p95']:>10.3f} ms")
    print(f"    p99:        {p['p99']:>10.3f} ms")
    print(f"    p99.9:      {p['p999']:>10.3f} ms")
    print(f"    min/max:    {p['min']:>7.3f} / {p['max']:.3f} ms")


# ── setup / teardown ──────────────────────────────────────────────────────────

async def setup(client: httpx.AsyncClient) -> None:
    """Ensure bench DB exists and is clean."""
    # Drop if exists
    await client.delete(f"{BASE_URL}/v1/databases/{DB_NAME}")
    # Create fresh
    r = await client.post(f"{BASE_URL}/v1/databases", json={"name": DB_NAME})
    if r.status_code not in (201, 409):
        print(f"  ERROR: could not create bench DB: {r.status_code} {r.text}")
        sys.exit(1)
    # Create sorted index on height for ORDER BY queries
    await client.post(f"{BASE_URL}/v1/databases/{DB_NAME}/index",
                      json={"coll": "bench", "field": "height", "kind": "sorted"})


async def teardown(client: httpx.AsyncClient) -> None:
    await client.delete(f"{BASE_URL}/v1/databases/{DB_NAME}")


# ── benchmarks ────────────────────────────────────────────────────────────────

def _progress(i: int, n: int, label: str = "") -> None:
    """Print a live progress line every 5% or 1000 ops."""
    step = max(n // 20, 100)
    if i > 0 and i % step == 0:
        pct = i / n * 100
        print(f"\r    {label} {i:>8,}/{n:,}  ({pct:.0f}%)   ", end="", flush=True)


async def bench_sequential_writes(client: httpx.AsyncClient, n: int) -> tuple[float, List[float]]:
    """n sequential PUT requests — isolates single-writer latency."""
    latencies: List[float] = []
    t_total = time.perf_counter()

    for i in range(n):
        payload = {
            "coll": "bench", "id": str(i),
            "doc": {
                "height": i,
                "hash": f"000000{'abc' * 8}{i:010d}",
                "tx_count": i % 300,
                "size_bytes": 1024 + (i % 8192),
                "miner": f"miner_{i % 50}",
            }
        }
        t0 = time.perf_counter()
        try:
            r = await client.post(f"{BASE_URL}/v1/databases/{DB_NAME}/put", json=payload)
            lat = (time.perf_counter() - t0) * 1000
            latencies.append(lat)
            if r.status_code != 200:
                print(f"\n  WARN: PUT {i} returned {r.status_code}")
        except httpx.ReadTimeout:
            latencies.append(60_000.0)
            print(f"\n  TIMEOUT at i={i}")
        _progress(i, n, "seq-write")

    print(f"\r    done {n:,} sequential writes" + " " * 20)
    return time.perf_counter() - t_total, latencies


async def bench_concurrent_writes(
    client: httpx.AsyncClient, n: int, concurrency: int,
) -> tuple[float, List[float]]:
    """n concurrent PUTs using asyncio semaphore — measures parallel write throughput."""
    sem = asyncio.Semaphore(concurrency)
    latencies: List[float] = []
    lock = asyncio.Lock()

    async def _put(i: int) -> None:
        payload = {
            "coll": "bench_conc", "id": str(i),
            "doc": {
                "height": n + i,
                "hash": f"conc{'def' * 8}{i:010d}",
                "concurrent": True,
                "worker": i % concurrency,
            }
        }
        async with sem:
            t0 = time.perf_counter()
            r = await client.post(f"{BASE_URL}/v1/databases/{DB_NAME}/put", json=payload)
            lat = (time.perf_counter() - t0) * 1000
        async with lock:
            latencies.append(lat)
            _progress(len(latencies), n, "conc-write")
        if r.status_code != 200:
            print(f"\n  WARN: concurrent PUT {i} returned {r.status_code}")

    t_total = time.perf_counter()
    await asyncio.gather(*(_put(i) for i in range(n)))
    print(f"\r    done {n:,} concurrent writes" + " " * 20)
    return time.perf_counter() - t_total, latencies


async def bench_sequential_reads(client: httpx.AsyncClient, n: int, max_id: int) -> tuple[float, List[float]]:
    """n sequential point-lookup reads via NQL WHERE _id = x."""
    latencies: List[float] = []
    t_total = time.perf_counter()

    for i in range(n):
        doc_id = str(i % max_id)
        payload = {"nql": f'FROM bench WHERE _id = "{doc_id}" LIMIT 1'}
        t0 = time.perf_counter()
        r = await client.post(f"{BASE_URL}/v1/databases/{DB_NAME}/query", json=payload)
        latencies.append((time.perf_counter() - t0) * 1000)
        if r.status_code != 200:
            print(f"\n  WARN: query {i} returned {r.status_code}")
        _progress(i, n, "seq-read ")

    print(f"\r    done {n:,} reads" + " " * 30)
    return time.perf_counter() - t_total, latencies


async def bench_ordered_queries(client: httpx.AsyncClient, n: int) -> tuple[float, List[float]]:
    """n ORDER BY height DESC LIMIT 10 queries — exercises sorted index."""
    latencies: List[float] = []
    t_total = time.perf_counter()

    for i in range(n):
        payload = {"nql": "FROM bench ORDER BY height DESC LIMIT 10"}
        t0 = time.perf_counter()
        r = await client.post(f"{BASE_URL}/v1/databases/{DB_NAME}/query", json=payload)
        latencies.append((time.perf_counter() - t0) * 1000)
        if r.status_code != 200:
            print(f"  WARN: ordered query {i} returned {r.status_code}")

    return time.perf_counter() - t_total, latencies


async def bench_batch_writes(client: httpx.AsyncClient, n: int, batch_size: int = 500) -> tuple[float, List[float]]:
    """Writes via batch endpoint — batch_size ops per HTTP request."""
    latencies: List[float] = []
    t_total = time.perf_counter()

    for start in range(0, n, batch_size):
        end = min(start + batch_size, n)
        ops = [
            {
                "op": "put", "coll": "bench_batch", "id": str(start + j),
                "doc": {"height": start + j, "batch": True}
            }
            for j in range(end - start)
        ]
        t0 = time.perf_counter()
        r = await client.post(f"{BASE_URL}/v1/databases/{DB_NAME}/batch", json={"ops": ops})
        lat = (time.perf_counter() - t0) * 1000
        latencies.append(lat)
        if r.status_code != 200:
            print(f"  WARN: batch {start} returned {r.status_code}")

    # Normalize: report per-op latency
    ops_per_req = batch_size
    per_op = [l / ops_per_req for l in latencies]
    return time.perf_counter() - t_total, per_op


# ── verify integrity ──────────────────────────────────────────────────────────

async def bench_verify(client: httpx.AsyncClient) -> None:
    t0 = time.perf_counter()
    r = await client.get(f"{BASE_URL}/v1/databases/{DB_NAME}/verify")
    elapsed = (time.perf_counter() - t0) * 1000
    if r.status_code == 200:
        body = r.json()
        print(f"\n  Tamper-evidence verify")
        checked = body.get('objects_checked', '?')
        print(f"    objects checked: {checked!s:>6}")
        print(f"    tampered:        {len(body.get('tampered', [])):>6}")
        print(f"    ok:              {body.get('ok', '?')}")
        print(f"    elapsed:         {elapsed:>10.1f} ms")
        print(f"    head:            {body.get('head', '?')[:24]}...")
    else:
        print(f"  WARN: verify returned {r.status_code}")


# ── main ──────────────────────────────────────────────────────────────────────

async def run(args: argparse.Namespace) -> int:
    global BASE_URL, DB_NAME
    BASE_URL = args.url.rstrip("/")
    DB_NAME  = args.db

    headers = _headers()

    async with httpx.AsyncClient(headers=headers, timeout=WRITE_TIMEOUT) as client:
        # Health check
        try:
            h = await client.get(f"{BASE_URL}/health")
            h.raise_for_status()
            info = h.json()
        except Exception as e:
            print(f"\nERROR: Cannot reach nedbd at {BASE_URL}: {e}")
            print("  Start nedbd first:")
            print("    NEDBD_DAG=1 nedbd --data /tmp/nedb-perf")
            return 1

        print(f"\nNEDB DAG Write/Read Benchmark")
        print(f"{'=' * 56}")
        print(f"  server:      {BASE_URL}")
        print(f"  version:     {info.get('version', '?')}")
        print(f"  encrypted:   {info.get('encrypted', False)}")
        print(f"  databases:   {info.get('databases', [])}")
        print(f"  db:          {DB_NAME}")
        print(f"  N writes:    {args.n:,}")
        print(f"  N reads:     {args.reads:,}")
        print(f"  concurrency: {args.concurrency}")
        print(f"  batch size:  {args.batch_size}")
        print(f"{'=' * 56}")

        # Warmup — one PUT to wake up any lazy connection / first-write init
        print("\n  Warming up (1 write)...", end="", flush=True)
        try:
            wr = await client.post(f"{BASE_URL}/v1/databases/{DB_NAME}/put",
                                   json={"coll": "bench", "id": "_warmup", "doc": {"warmup": True}})
            print(f" {wr.status_code} OK" if wr.status_code == 200 else f" {wr.status_code} WARN")
        except httpx.ReadTimeout:
            print(" TIMEOUT — nedbd is very slow on first write")
            print("  This usually means:")
            print("    1. Python v1 server is loading a large AOF (wait and retry)")
            print("    2. Use --dag mode: NEDBD_DAG=1 nedbd --data /tmp/fresh-perf-dir")
            print("    3. Or point to a fresh empty data dir with no existing AOF")
            return 1

        await setup(client)

        results = {}

        def phase(num: str, label: str) -> None:
            print(f"\n{'─'*56}")
            print(f"  {num}  {label}")
            print(f"{'─'*56}")

        # 1 — Sequential writes
        phase("[1/5]", f"Sequential writes — {args.n:,} ops, one at a time")
        elapsed, lats = await bench_sequential_writes(client, args.n)
        results["seq_writes"] = (args.n, elapsed, lats)
        print_result("Sequential writes", args.n, elapsed, lats)

        # 2 — Concurrent writes
        phase("[2/5]", f"Concurrent writes — {args.n:,} ops, {args.concurrency} workers")
        elapsed, lats = await bench_concurrent_writes(client, args.n, args.concurrency)
        results["conc_writes"] = (args.n, elapsed, lats)
        print_result("Concurrent writes", args.n, elapsed, lats)

        # 3 — Batch writes
        phase("[3/5]", f"Batch writes — {args.n:,} ops, {args.batch_size} per request")
        elapsed, lats = await bench_batch_writes(client, args.n, args.batch_size)
        results["batch_writes"] = (args.n, elapsed, lats)
        print_result(f"Batch writes (per-op, batch={args.batch_size})", args.n, elapsed, lats)

        # 4 — Sequential reads
        phase("[4/5]", f"Sequential reads — {args.reads:,} point lookups")
        elapsed, lats = await bench_sequential_reads(client, args.reads, args.n)
        results["seq_reads"] = (args.reads, elapsed, lats)
        print_result("Sequential reads (NQL WHERE _id)", args.reads, elapsed, lats)

        # 5 — Ordered queries
        n_ordered = min(args.reads // 10, 1000)
        phase("[5/5]", f"ORDER BY queries — {n_ordered:,} ops")
        elapsed, lats = await bench_ordered_queries(client, n_ordered)
        results["ordered_queries"] = (n_ordered, elapsed, lats)
        print_result("ORDER BY height DESC LIMIT 10", n_ordered, elapsed, lats)

        # Verify tamper evidence
        await bench_verify(client)

        # Summary table
        print(f"\n{'=' * 56}")
        print(f"  {'Operation':<28} {'ops/s':>10}  {'p99 ms':>8}")
        print(f"  {'-' * 54}")
        labels = [
            ("seq_writes",    "Sequential writes"),
            ("conc_writes",   "Concurrent writes"),
            ("batch_writes",  "Batch writes (per-op)"),
            ("seq_reads",     "Point-lookup reads"),
            ("ordered_queries","ORDER BY queries"),
        ]
        for key, label in labels:
            if key not in results:
                continue
            n, elapsed, lats = results[key]
            rps = n / max(elapsed, 0.001)
            p99 = percentiles(lats)["p99"]
            print(f"  {label:<28} {rps:>10,.0f}  {p99:>8.3f}")
        print(f"{'=' * 56}\n")

        # Write JSON report if requested
        if args.output:
            report = {}
            for key, label in labels:
                if key not in results:
                    continue
                n, elapsed, lats = results[key]
                p = percentiles(lats)
                report[key] = {
                    "label": label,
                    "n": n,
                    "elapsed_s": round(elapsed, 4),
                    "ops_per_s": round(n / max(elapsed, 0.001)),
                    **p,
                }
            with open(args.output, "w") as f:
                json.dump(report, f, indent=2)
            print(f"  Report written to {args.output}")

        if not args.keep:
            await teardown(client)

    return 0


def main() -> None:
    p = argparse.ArgumentParser(
        description="NEDB DAG write/read throughput benchmark",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--url",         default=os.getenv("NEDB_URL", "http://127.0.0.1:7070"))
    p.add_argument("--db",          default="bench")
    p.add_argument("--n",           type=int, default=10_000, help="number of write ops")
    p.add_argument("--reads",       type=int, default=100_000, help="number of read ops")
    p.add_argument("--concurrency", type=int, default=16,     help="concurrent write workers")
    p.add_argument("--batch-size",  type=int, default=500,    help="ops per batch request")
    p.add_argument("--keep",        action="store_true",      help="keep bench DB after run")
    p.add_argument("--output",      default=None,             help="write JSON report to file")
    args = p.parse_args()
    sys.exit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
