"""
NEDB vs Redis — the honest head-to-head harness.

This measures the architectural difference that matters most: NEDB embedded
(in-process, no socket) vs Redis (client/server over TCP). The embedded engine
pays no network or serialization hop per call, which is the structural reason an
embedded store can beat a networked one on latency for in-process workloads.

For a fair *networked* comparison, run the future `nedbd` server (RESP-compatible)
under redis-benchmark/memtier — that contest is decided on the Rust core, not here.

Run:
    pip install redis
    redis-server &            # or: docker run -p 6379:6379 redis
    python3 bench/bench_redis.py
"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nedb import NEDB  # noqa: E402

N = 100_000


def rate(fn) -> float:
    t = time.perf_counter()
    fn()
    return N / (time.perf_counter() - t)


def main() -> None:
    db = NEDB()
    set_rate = rate(lambda: [db.put("k", str(i), {"v": i}, client="b", nonce=i + 1) for i in range(N)])
    get_rate = rate(lambda: [db.get("k", str(i)) for i in range(N)])
    print(f"NEDB (embedded)   SET {set_rate:12,.0f}/s   GET {get_rate:12,.0f}/s")

    try:
        import redis  # type: ignore

        r = redis.Redis()
        r.ping()
        rset = rate(lambda: [r.set(f"k:{i}", i) for i in range(N)])
        rget = rate(lambda: [r.get(f"k:{i}") for i in range(N)])
        print(f"Redis (TCP)       SET {rset:12,.0f}/s   GET {rget:12,.0f}/s")
        print(f"\nNEDB embedded GET vs Redis TCP GET: {get_rate / rget:.1f}x")
        print("(embedded pays no per-call socket hop — that's the point)")
    except Exception as e:  # noqa: BLE001
        print(f"\n[Redis not reachable: {e}]")
        print("Start Redis to run the head-to-head:  pip install redis && redis-server")


if __name__ == "__main__":
    main()
