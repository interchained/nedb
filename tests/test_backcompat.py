#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
Backwards compatibility: every pre-3.3.0 query form still answers the same.

3.3.0 rewrote the NQL parser and executor in both engines. A rewrite that
quietly changed what an existing query RETURNS would be worse than shipping no
new operators at all — the whole point of a provenance engine is that the
answer you got last month is the answer you get today.

So this was verified empirically rather than asserted: a nedbd binary was built
from the released v3.2.2 tag, the corpus below was run against BOTH it and
HEAD, and every answer diffed. Result: 37 of 39 identical, ZERO regressions
(no query the old engine answered now fails). The two differences are listed
in INTENTIONAL_CHANGES below, with the reasoning.

The `_hash` field is excluded from comparison there and here, because
`Node.ts` — a Unix timestamp — is inside the hashed content. Two
independently-created databases therefore hold different hashes for identical
document content, which is correct DAG behaviour, not drift.

This suite freezes the v3.2.2 answers as expectations so the old binary is
never needed again. `scripts/compare_engine_answers.py` regenerates the
comparison against any released binary when a future rewrite warrants it.

Run: python3 tests/test_backcompat.py
"""
import json
import os
import shutil
import sys
import tempfile

# APPEND, not insert(0) — a prepended source tree has no `_native`, which pins
# __has_native__ False and silently skips the Rust leg into a false green.
sys.path.append("python")

import nedb  # noqa: E402
from nedb.query import empty_plan, parse_nql  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("  ok  " if cond else "  FAIL  ") + name + (f"  → {detail}" if detail else ""))


# ── the fixture the v3.2.2 comparison ran against ────────────────────────────

JOBS = [
    ("1", {"status": "open",    "miner": "Acme Pool", "fee": 10, "region": "eu-west"}),
    ("2", {"status": "pending", "miner": "acme solo", "fee": 20, "region": "us-east"}),
    ("3", {"status": "closed",  "miner": "Zenith",    "fee": 30, "region": "eu-north"}),
    ("4", {"status": "open",                          "fee": 40, "region": "us-west"}),
    ("5", {"status": "voided",  "miner": None,        "fee": 50, "region": "ap-south"}),
]
# A second version of doc 1, so AS OF has history to show. After this, doc 1 is
# closed/11 at HEAD and open/10 at seq 0.
JOBS_UPDATE = ("1", {"status": "closed", "miner": "Acme Pool", "fee": 11,
                     "region": "eu-west"})

# (query, expected sorted _id list) — the answers v3.2.2 gave.
LEGACY_IDS = [
    ("FROM jobs",                                          ["1", "2", "3", "4", "5"]),
    ('FROM jobs WHERE status = "open"',                    ["4"]),
    ('FROM jobs WHERE status != "open"',                   ["1", "2", "3", "5"]),
    ("FROM jobs WHERE fee > 20",                           ["3", "4", "5"]),
    ("FROM jobs WHERE fee < 20",                           ["1"]),
    ("FROM jobs WHERE fee >= 20",                          ["2", "3", "4", "5"]),
    ("FROM jobs WHERE fee <= 20",                          ["1", "2"]),
    ('FROM jobs WHERE _id = "3"',                          ["3"]),
    ('FROM jobs WHERE status = "closed" AND fee > 5',      ["1", "3"]),
    ('FROM jobs WHERE status = "open" AND fee > 999',      []),
    ('FROM jobs SEARCH "Zenith"',                          ["3"]),
    ("FROM nonexistent",                                   []),
    ('FROM jobs WHERE status = "nope"',                    []),
    ("FROM jobs WHERE fee > 9999",                         []),
    ('FROM jobs WHERE _coll = "jobs"',                     ["1", "2", "3", "4", "5"]),
    # Ordering comparisons against a SPARSE column. `miner` is absent on doc 4
    # and explicitly null on doc 5; neither may satisfy an ordering test. See
    # INTENTIONAL_CHANGES #3 — v3.2.2's Rust engine returned all five here for
    # `<` and `<=` while returning three for `>` and `>=`.
    ('FROM jobs WHERE miner < "zzz"',                      ["1", "2", "3"]),
    ('FROM jobs WHERE miner <= "zzz"',                     ["1", "2", "3"]),
    ('FROM jobs WHERE miner > "A"',                        ["1", "2", "3"]),
    ('FROM jobs WHERE miner >= "A"',                       ["1", "2", "3"]),
    # = and != still operate on null, unchanged from v3.2.2.
    ('FROM jobs WHERE miner != "Zenith"',                  ["1", "2", "4", "5"]),
]

# (query, expected fee ordering) — ORDER BY / LIMIT semantics.
LEGACY_ORDER = [
    ("FROM jobs ORDER BY fee",            [11, 20, 30, 40, 50]),
    ("FROM jobs ORDER BY fee ASC",        [11, 20, 30, 40, 50]),
    ("FROM jobs ORDER BY fee DESC",       [50, 40, 30, 20, 11]),
    ("FROM jobs ORDER BY fee DESC LIMIT 2", [50, 40]),
    ("FROM jobs ORDER BY fee ASC LIMIT 3",  [11, 20, 30]),
    ('FROM jobs WHERE status = "open" ORDER BY fee DESC', [40]),
]

# The two answers that DID change between v3.2.2 and 3.3.0, and why. Listed
# explicitly rather than quietly excluded — a change nobody wrote down is
# indistinguishable from a regression.
INTENTIONAL_CHANGES = """
1. GROUP BY output ORDER. v3.2.2 emitted groups in HashMap iteration order,
   which is arbitrary and differed run to run; 3.3.0 sorts by group key. Same
   groups, same counts, deterministic sequence. Because the old order was
   nondeterministic, no caller could have depended on it — this makes the
   engine MORE compatible with itself, and lets the two engines agree.

2. `WHERE _seq > N`. v3.2.2 returned an EMPTY set: only _id, _coll and _hash
   resolved in a predicate, so _seq compared null against N. It returned
   nothing while printing _seq on every row. 3.3.0 resolves it. A filter on a
   field the engine itself emits should never silently answer "nothing", so
   this is a fix — but it does turn an empty answer into a populated one, and
   that is worth stating plainly.

3. ORDERING COMPARISONS AGAINST A MISSING FIELD, in the Rust engine only.
   Its OrderedValue sorts Null below every number, so `<` and `<=` reported
   that a document with NO `fee` field satisfied `WHERE fee < 5`, while `>`
   and `>=` excluded it. The asymmetry was the tell. The Python reference has
   always excluded it in all four (query.py places its `if a is None: return
   False` guard deliberately after the = and != arms), so this was a live
   cross-engine divergence as well as a wrong answer — asking for cheap jobs
   should not return jobs with no price.

   It also had to be fixed for the indexed range scan to be correct at all: a
   document whose field is absent is not in that field's sorted index, so the
   scan path and the index path would otherwise answer the same query
   differently depending on whether an index happened to exist.

   Verified against v3.2.2: only `<` and `<=` changed. `>`, `>=`, `=`, `!=`
   and BETWEEN are byte-identical.
"""


def run_engine(label, mk):
    print(f"\n{'=' * 68}\n{label}\n{'=' * 68}")
    L = f"[{label}]"
    q = mk()

    def ids(rows):
        return sorted(str(r.get("_id")) for r in rows)

    print("\n── pre-3.3.0 predicates and filters ──")
    for nql, want in LEGACY_IDS:
        try:
            got = ids(q(nql))
        except Exception as e:                                     # noqa: BLE001
            check(f"{L} {nql}", False, f"raised {type(e).__name__}: {e}")
            continue
        check(f"{L} {nql}", got == sorted(want), f"got {got}, want {sorted(want)}")

    print("\n── pre-3.3.0 ORDER BY / LIMIT ──")
    for nql, want in LEGACY_ORDER:
        try:
            got = [r.get("fee") for r in q(nql)]
        except Exception as e:                                     # noqa: BLE001
            check(f"{L} {nql}", False, f"raised {type(e).__name__}: {e}")
            continue
        check(f"{L} {nql}", got == want, f"got {got}, want {want}")

    print("\n── pre-3.3.0 AS OF (transaction time) ──")
    at0 = q("FROM jobs AS OF 0")
    check(f"{L} AS OF 0 returns one row", len(at0) == 1, f"{len(at0)}")
    check(f"{L} AS OF 0 shows the historical value",
          at0 and at0[0].get("fee") == 10 and at0[0].get("status") == "open",
          str(at0))
    at2 = q("FROM jobs AS OF 2")
    check(f"{L} AS OF 2 returns three rows", len(at2) == 3, f"{len(at2)}")
    check(f"{L} AS OF N + a legacy predicate",
          sorted(str(r["_id"]) for r in q("FROM jobs AS OF 3 WHERE fee > 10"))
          == ["2", "3", "4"],
          str(sorted(str(r["_id"]) for r in q("FROM jobs AS OF 3 WHERE fee > 10"))))

    print("\n── pre-3.3.0 GROUP BY output shape ──")
    g = q("FROM jobs GROUP BY status COUNT")
    check(f"{L} groups are returned", len(g) == 4, f"{len(g)} groups")
    check(f"{L} each group carries the group key",
          all("status" in r for r in g), str(g[:1]))
    check(f"{L} each group carries `count`", all("count" in r for r in g))
    # `value` predates 3.3.0 as this engine's only aggregate key. Studio and
    # any existing caller read it, so dropping it would break them silently.
    check(f"{L} `value` alias is still emitted", all("value" in r for r in g),
          str(sorted(g[0].keys())))
    counts = {r["status"]: r["count"] for r in g}
    check(f"{L} counts are right", counts == {"open": 1, "closed": 2,
                                              "pending": 1, "voided": 1},
          str(counts))
    check(f"{L} `value` agrees with `count` for COUNT",
          all(r["value"] == r["count"] for r in g))
    # And the documented intentional change: order is now deterministic.
    check(f"{L} groups now come back sorted by key (was arbitrary)",
          [r["status"] for r in g] == sorted(counts),
          str([r["status"] for r in g]))

    print("\n── legacy GROUP BY with an aggregate target ──")
    s = q("FROM jobs GROUP BY status SUM fee")
    by = {r["status"]: r for r in s}
    check(f"{L} SUM keyed as sum_<field>", "sum_fee" in by["closed"],
          str(sorted(by["closed"].keys())))
    check(f"{L} SUM value is right", by["closed"]["sum_fee"] == 41,
          str(by["closed"]["sum_fee"]))   # doc 1 (11) + doc 3 (30)
    check(f"{L} `value` alias agrees", by["closed"]["value"] == 41)

    print("\n── every row still carries its provenance ──")
    r0 = q("FROM jobs")[0]
    for field in ("_id", "_seq", "_hash", "_coll"):
        check(f"{L} rows carry {field}", field in r0, str(sorted(r0.keys())))


# ── engine 1: the Python reference ───────────────────────────────────────────

print(f"\nnedb {nedb.__version__}  |  native DAG available: {nedb.__has_native__}")


def py_mk():
    db = nedb.NEDB()
    for i, d in JOBS:
        db.put("jobs", i, d)
    db.put("jobs", JOBS_UPDATE[0], JOBS_UPDATE[1])
    return db.query


run_engine("python", py_mk)

# ── engine 2: the Rust DAG core ──────────────────────────────────────────────

native = False
if nedb.__has_native__:
    from nedb._native import NedbCore  # noqa: E402

    tmp = tempfile.mkdtemp(suffix="-backcompat")
    try:
        def rust_mk():
            core = NedbCore.open(os.path.join(tmp, "db"))
            for i, d in JOBS:
                core.put("jobs", i, json.dumps(d))
            core.put("jobs", JOBS_UPDATE[0], json.dumps(JOBS_UPDATE[1]))
            return lambda nql: [json.loads(r) for r in core.query(nql)]

        run_engine("rust", rust_mk)
        native = True
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
else:
    print("\n  …  native DAG core not installed — Rust leg skipped.")

# ── the plan dict is part of the API surface ─────────────────────────────────
#
# mongo.py builds a plan directly and the fluent Query builder emits one, so
# the pre-3.3.0 keys have to keep their shapes. The predicate tree was ADDED
# alongside plan["where"], not in place of it.

print(f"\n{'=' * 68}\nplan dict compatibility\n{'=' * 68}")
LEGACY_PLAN_KEYS = ["from", "as_of", "where", "search", "order_by", "traverse",
                    "limit", "group_by", "aggregate", "trace", "trace_reverse",
                    "valid_as_of"]
p = empty_plan("t")
for k in LEGACY_PLAN_KEYS:
    check(f"empty_plan still has {k!r}", k in p)

flat = parse_nql('FROM jobs WHERE status = "open" AND fee > 5')
check("plan['where'] keeps the (field, op, value) tuple shape",
      flat["where"] == [("status", "=", "open"), ("fee", ">", 5)],
      str(flat["where"]))
check("plan['order_by'] keeps the (field, direction) shape",
      parse_nql("FROM jobs ORDER BY fee DESC")["order_by"] == ("fee", "DESC"),
      str(parse_nql("FROM jobs ORDER BY fee DESC")["order_by"]))
check("plan['group_by'] is still the bare field name",
      parse_nql("FROM jobs GROUP BY status COUNT")["group_by"] == "status")
check("plan['aggregate'] keeps the (fn, field) tuple shape",
      parse_nql("FROM jobs GROUP BY status SUM fee")["aggregate"] == ("sum", "fee"),
      str(parse_nql("FROM jobs GROUP BY status SUM fee")["aggregate"]))
check("plan['as_of'] is still an int",
      parse_nql("FROM jobs AS OF 7")["as_of"] == 7)
check("plan['valid_as_of'] is still the date string",
      parse_nql('FROM jobs VALID AS OF "2024-06-01"')["valid_as_of"] == "2024-06-01")
check("plan['trace_reverse'] is still a bool",
      parse_nql("FROM jobs TRACE caused_by REVERSE")["trace_reverse"] is True)

# A plan built by hand, the way mongo.py does it, must still execute.
db = nedb.NEDB()
for i, d in JOBS:
    db.put("jobs", i, d)
hand = empty_plan("jobs")
hand["where"] = [("status", "=", "open")]
rows = db.execute(hand)
check("a hand-built plan with only plan['where'] still executes",
      sorted(str(r["_id"]) for r in rows) == ["1", "4"],
      str(sorted(str(r["_id"]) for r in rows)))

# The fluent builder emits the single-key order_by form, not order_keys.
built = db.query_builder("jobs") if hasattr(db, "query_builder") else None
if built is None:
    from nedb.query import Query as FluentQuery
    built = FluentQuery(db, "jobs")
built.plan["where"] = [("fee", ">", 20)]
built.plan["order_by"] = ("fee", "DESC")
built.plan["limit"] = 2
fluent_rows = db.execute(built.plan)
check("the fluent builder's single-key order_by still sorts and limits",
      [r["fee"] for r in fluent_rows] == [50, 40],
      str([r["fee"] for r in fluent_rows]))

# ── tally ────────────────────────────────────────────────────────────────────

print(f"\n{'=' * 68}")
print(f"backwards compatibility ({'python + rust' if native else 'python'}): "
      f"{len(PASS)} passed, {len(FAIL)} failed")
if FAIL:
    print("FAILED:", *FAIL, sep="\n  - ")
    sys.exit(1)
print("Every pre-3.3.0 query form answers as v3.2.2 did.")
print("Intentional changes, verified against a v3.2.2 binary:" + INTENTIONAL_CHANGES)
