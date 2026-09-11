#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
Measure the indexed range/point scan against the collection scan it replaces.

3.3.0 taught the planner to serve `=`, `IN (...)`, `BETWEEN` and the
one-sided inequalities from the sorted index (a BTreeMap per (coll, field))
instead of walking the whole collection and filtering. This script quantifies
that on a real embedded database rather than asserting it — the equivalence of
the two paths is covered by unit tests, this is about how much work is saved.

The comparison is two databases holding IDENTICAL data, one with a sorted
index on the queried field and one without, so the only variable is the
candidate-generation path.

    python3 scripts/bench_index_range.py [--rows N] [--repeat R]

Needs the native wheel (`nedb._native`), because the sorted index lives in the
Rust core. Reports per-query wall time and the speedup; no assertions, so it
never gates CI.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import sys
import tempfile
import time

sys.path.append("python")

import nedb  # noqa: E402

if not nedb.__has_native__:
    sys.exit("this benchmark needs the native core: "
             "maturin build --release -m rust/crates/nedb-py/Cargo.toml && pip install …")

from nedb._native import NedbCore  # noqa: E402


def build(root: str, rows: int, indexed: bool):
    path = os.path.join(root, "idx" if indexed else "plain")
    core = NedbCore.open(path)
    if indexed:
        core.create_index("t", "fee", "sorted")
        core.create_index("t", "height", "sorted")
    for i in range(rows):
        core.put("t", str(i), json.dumps({
            # `fee` is high-cardinality, so a point lookup is very selective.
            "fee": i,
            # `height` is bucketed, so a range covers a predictable slice.
            "height": i % 1000,
            "status": ["open", "closed", "void"][i % 3],
            "note": f"row-{i}",
        }))
    return core


def timed(fn, repeat: int):
    # Warm once — the first call pays for lazy index/segment materialisation,
    # which would otherwise be attributed to the query itself.
    fn()
    samples = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        n = fn()
        samples.append(time.perf_counter() - t0)
    return min(samples), statistics.median(samples), n


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=20000)
    ap.add_argument("--repeat", type=int, default=7)
    a = ap.parse_args()

    root = tempfile.mkdtemp(suffix="-benchidx")
    try:
        print(f"building two {a.rows}-row databases (one indexed, one not)…")
        t0 = time.perf_counter()
        idx = build(root, a.rows, True)
        plain = build(root, a.rows, False)
        print(f"  built in {time.perf_counter() - t0:.1f}s\n")

        mid = a.rows // 2
        QUERIES = [
            ("point =",        f"FROM t WHERE fee = {mid}"),
            ("IN, 3 arms",     f"FROM t WHERE fee IN ({mid}, {mid+1}, {mid+2})"),
            ("BETWEEN, 0.05%", f"FROM t WHERE fee BETWEEN {mid} AND {mid+10}"),
            ("BETWEEN, 1%",    f"FROM t WHERE fee BETWEEN {mid} AND {mid + a.rows//100}"),
            ("BETWEEN, 10%",   f"FROM t WHERE fee BETWEEN {mid} AND {mid + a.rows//10}"),
            (">= top 1%",      f"FROM t WHERE fee >= {a.rows - a.rows//100}"),
            ("< bottom 1%",    f"FROM t WHERE fee < {a.rows//100}"),
            ("bucketed =",     "FROM t WHERE height = 500"),
            ("range + filter", f"FROM t WHERE fee BETWEEN {mid} AND {mid+50} "
                               f'AND status = "open"'),
            ("range + order",  f"FROM t WHERE fee BETWEEN {mid} AND {mid+50} "
                               f"ORDER BY height DESC LIMIT 5"),
            ("range + count",  f"FROM t WHERE fee BETWEEN {mid} AND {mid+50} COUNT"),
            # Unindexed field — the control. Both sides scan, so the numbers
            # should match; a difference here would mean the harness is biased.
            ("control: unindexed field", 'FROM t WHERE status = "open" COUNT'),
        ]

        print(f"{'query':<26} {'scan':>10} {'indexed':>10} {'speedup':>9}   rows")
        print("-" * 72)
        for label, nql in QUERIES:
            def run_idx(q=nql):
                return len(idx.query(q))

            def run_plain(q=nql):
                return len(plain.query(q))

            i_min, _, i_n = timed(run_idx, a.repeat)
            p_min, _, p_n = timed(run_plain, a.repeat)
            if i_n != p_n:
                print(f"{label:<26}  MISMATCH indexed={i_n} scan={p_n}")
                continue
            speed = p_min / i_min if i_min > 0 else float("inf")
            print(f"{label:<26} {p_min*1000:>9.2f}ms {i_min*1000:>9.2f}ms "
                  f"{speed:>8.1f}x   {i_n}")

        print("\nBest-of-%d wall time. Row counts are asserted equal between the two"
              % a.repeat)
        print("paths on every query — the index must never change the answer.")
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
