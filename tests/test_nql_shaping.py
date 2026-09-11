#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
NQL result shaping — OFFSET, multi-key ORDER BY, HAVING, bare aggregates, and
the SQL pipeline order. Cross-engine, like test_nql_predicates.py.

THE PIPELINE ORDER WAS WRONG IN BOTH ENGINES, in three separate ways. SQL runs

    FROM -> WHERE -> GROUP BY -> HAVING -> ORDER BY -> OFFSET -> LIMIT

and both engines ran ORDER BY -> LIMIT -> (VALID AS OF) -> GROUP BY. That is
not a stylistic difference; it produced confident wrong numbers:

  1. LIMIT truncated the INPUT to an aggregate instead of the RESULT.
     Twelve rows across three statuses, `LIMIT 5 GROUP BY status COUNT`:
     counts summed to 5, not 12. The query reported that only five rows
     existed when twelve did.

  2. ORDER BY ran before grouping, so it sorted the raw documents on a field
     that only exists AFTER grouping (`count`, `sum_fee`). The grouped output
     came back in arbitrary order and the clause was silently inert.

  3. Python applied VALID AS OF after LIMIT, so
     `VALID AS OF "<date>" LIMIT 3` truncated to three RAW rows and only then
     dropped the ones not valid at that date — returning fewer rows than
     exist. The Rust engine filters valid-time BEFORE limiting, so this was
     also a live divergence between the two engines, not just a bug in one.

Everything below either pins a new clause or locks one of those three bugs
shut. Run: python3 tests/test_nql_shaping.py
"""
import os
import shutil
import sys
import tempfile

# APPEND, not insert(0) — see the note in test_nql_predicates.py. A prepended
# source tree has no `_native`, which pins __has_native__ False and silently
# skips the cross-engine leg.
sys.path.append("python")

import nedb  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("  ok  " if cond else "  FAIL  ") + name + (f"  — {detail}" if detail else ""))


# ── fixtures ─────────────────────────────────────────────────────────────────

# 12 rows, 3 statuses x 4 rows each, fee = i. Uneven enough that a truncated
# input is obvious in the counts.
SPREAD = [
    (str(i), {"status": ["open", "closed", "void"][i % 3], "fee": i})
    for i in range(12)
]

# Two sort columns where the second has to break the first's ties.
PAIRS = [
    ("0", {"g": "open",   "fee": 30}),
    ("1", {"g": "open",   "fee": 10}),
    ("2", {"g": "closed", "fee": 20}),
    ("3", {"g": "open",   "fee": 20}),
    ("4", {"g": "closed", "fee": 5}),
]

# Uneven group sizes: 1 x a, 3 x b, 2 x c.
UNEVEN = [(str(i), {"g": g, "n": i}) for i, g in enumerate(["a", "b", "b", "b", "c", "c"])]


def run_engine(label, mk):
    """`mk(rows)` returns a `query` callable over a fresh db holding `rows`."""
    print(f"\n{'=' * 62}\n{label}\n{'=' * 62}")

    def ids(rows):
        return [str(r.get("_id")) for r in rows]

    def fees(rows):
        return [r.get("fee") for r in rows]

    L = f"[{label}]"

    # ── OFFSET ───────────────────────────────────────────────────────────────
    print("\n── OFFSET ──")
    q = mk(SPREAD)
    check(f"{L} OFFSET skips result rows",
          fees(q("FROM t ORDER BY fee OFFSET 9")) == [9, 10, 11],
          str(fees(q("FROM t ORDER BY fee OFFSET 9"))))
    check(f"{L} OFFSET 0 skips nothing",
          len(q("FROM t OFFSET 0")) == 12)
    check(f"{L} OFFSET past the end is an empty page",
          q("FROM t OFFSET 99") == [])

    # Paging must produce disjoint, ordered, complete coverage. The bug this
    # catches is an index pushdown that fetches only `limit` rows and therefore
    # returns page 1 for every page.
    pages, seen = 0, []
    while True:
        page = fees(q(f"FROM t ORDER BY fee LIMIT 5 OFFSET {pages * 5}"))
        if not page:
            break
        seen.extend(page)
        pages += 1
        if pages > 10:
            break
    check(f"{L} LIMIT+OFFSET pages the whole result exactly once",
          seen == list(range(12)), f"{pages} pages -> {seen}")

    check(f"{L} OFFSET applies after the filter",
          fees(q("FROM t WHERE fee >= 9 ORDER BY fee OFFSET 1")) == [10, 11],
          str(fees(q("FROM t WHERE fee >= 9 ORDER BY fee OFFSET 1"))))

    # ── multi-key ORDER BY ───────────────────────────────────────────────────
    print("\n── multi-key ORDER BY ──")
    q2 = mk(PAIRS)
    got = [(r["g"], r["fee"]) for r in q2("FROM t ORDER BY g, fee DESC")]
    check(f"{L} ORDER BY a, b DESC — second key breaks ties",
          got == [("closed", 20), ("closed", 5),
                  ("open", 30), ("open", 20), ("open", 10)], str(got))
    got2 = [(r["g"], r["fee"]) for r in q2("FROM t ORDER BY g DESC, fee ASC")]
    check(f"{L} ORDER BY a DESC, b ASC — mixed directions",
          got2 == [("open", 10), ("open", 20), ("open", 30),
                   ("closed", 5), ("closed", 20)], str(got2))
    check(f"{L} a single key still behaves as before",
          [r["fee"] for r in q2("FROM t ORDER BY fee DESC")] == [30, 20, 20, 10, 5])

    # ── LIMIT applies to the RESULT, not the aggregate's input ───────────────
    print("\n── pipeline: LIMIT after GROUP BY, not before ──")
    q3 = mk(SPREAD)
    allg = q3("FROM t GROUP BY status COUNT")
    check(f"{L} every input row is counted",
          sum(r["count"] for r in allg) == 12,
          f"counts sum to {sum(r['count'] for r in allg)} of 12")
    limited = q3("FROM t GROUP BY status COUNT LIMIT 2")
    check(f"{L} LIMIT caps the number of GROUPS", len(limited) == 2, str(len(limited)))
    check(f"{L} each group keeps its true count",
          all(r["count"] == 4 for r in limited), str([r["count"] for r in limited]))

    # ── ORDER BY sorts the GROUPED rows ──────────────────────────────────────
    print("\n── pipeline: ORDER BY sorts grouped rows ──")
    q4 = mk(UNEVEN)
    by_count = [(r["g"], r["count"])
                for r in q4("FROM t GROUP BY g COUNT ORDER BY count DESC")]
    check(f"{L} ORDER BY count DESC on grouped rows",
          by_count == [("b", 3), ("c", 2), ("a", 1)], str(by_count))
    by_key = [r["g"] for r in q4("FROM t GROUP BY g COUNT ORDER BY g DESC")]
    check(f"{L} ORDER BY the group key", by_key == ["c", "b", "a"], str(by_key))
    check(f"{L} default group order is deterministic (sorted by key)",
          [r["g"] for r in q4("FROM t GROUP BY g COUNT")] == ["a", "b", "c"],
          str([r["g"] for r in q4("FROM t GROUP BY g COUNT")]))
    paged = q4("FROM t GROUP BY g COUNT ORDER BY g LIMIT 1 OFFSET 1")
    check(f"{L} grouped rows can be paged",
          len(paged) == 1 and paged[0]["g"] == "b", str(paged))

    # ── HAVING ───────────────────────────────────────────────────────────────
    print("\n── HAVING ──")
    q5 = mk(UNEVEN)
    keys = sorted(r["g"] for r in q5("FROM t GROUP BY g COUNT HAVING count > 1"))
    check(f"{L} HAVING count > 1 drops the single-row group",
          keys == ["b", "c"], str(keys))
    check(f"{L} HAVING can reject every group",
          q5("FROM t GROUP BY g COUNT HAVING count > 99") == [])
    check(f"{L} HAVING can accept every group",
          len(q5("FROM t GROUP BY g COUNT HAVING count >= 1")) == 3)

    q6 = mk(SPREAD)
    # fee sums: open = 0+3+6+9 = 18, closed = 1+4+7+10 = 22, void = 2+5+8+11 = 26
    hv = sorted(r["status"] for r in q6("FROM t GROUP BY status SUM fee HAVING sum_fee > 20"))
    check(f"{L} HAVING filters on the aggregate value",
          hv == ["closed", "void"], str(hv))
    # HAVING runs through the same evaluator as WHERE, so it gets the whole
    # predicate surface rather than a second, poorer copy.
    check(f"{L} HAVING supports IN",
          len(q6('FROM t GROUP BY status COUNT HAVING status IN ("open")')) == 1)
    check(f"{L} HAVING supports BETWEEN",
          sorted(r["status"] for r in
                 q6("FROM t GROUP BY status SUM fee HAVING sum_fee BETWEEN 20 AND 25"))
          == ["closed"])
    check(f"{L} HAVING supports OR",
          len(q6("FROM t GROUP BY status SUM fee HAVING sum_fee < 20 OR count = 4")) == 3)
    check(f"{L} HAVING supports LIKE",
          len(q6('FROM t GROUP BY status COUNT HAVING status LIKE "%pen"')) == 1)
    check(f"{L} HAVING supports NOT",
          len(q6('FROM t GROUP BY status COUNT HAVING NOT (status = "open")')) == 2)

    # WHERE filters inputs, HAVING filters groups. Confusing them gives
    # different answers, so the distinction has to hold.
    w = q6("FROM t WHERE fee > 8 GROUP BY status SUM fee")
    h = q6("FROM t GROUP BY status SUM fee HAVING sum_fee > 8")
    check(f"{L} WHERE shrinks the aggregate's input",
          sum(r["count"] for r in w) == 3, str([r["count"] for r in w]))
    check(f"{L} HAVING leaves the aggregate intact",
          sum(r["count"] for r in h) == 12, str([r["count"] for r in h]))

    # ── bare aggregates, no GROUP BY ─────────────────────────────────────────
    print("\n── bare aggregates ──")
    q7 = mk(SPREAD)
    c = q7("FROM t COUNT")
    check(f"{L} FROM t COUNT returns exactly one row", len(c) == 1, str(c))
    check(f"{L} ... with the total", c and c[0]["count"] == 12, str(c))
    check(f"{L} COUNT respects WHERE",
          q7("FROM t WHERE fee > 8 COUNT")[0]["count"] == 3)
    # fee 0..11 -> sum 66, avg 5.5, min 0, max 11
    check(f"{L} SUM", q7("FROM t SUM fee")[0]["sum_fee"] == 66)
    check(f"{L} AVG", q7("FROM t AVG fee")[0]["avg_fee"] == 5.5)
    check(f"{L} MIN", q7("FROM t MIN fee")[0]["min_fee"] == 0)
    check(f"{L} MAX", q7("FROM t MAX fee")[0]["max_fee"] == 11)
    check(f"{L} the `value` alias agrees",
          q7("FROM t SUM fee")[0]["value"] == 66)

    # COUNT of nothing is 0 — a caller asking "how many?" must get a number.
    empty = q7("FROM t WHERE fee > 999 COUNT")
    check(f"{L} COUNT of an empty result is one row",
          len(empty) == 1, str(empty))
    check(f"{L} ... holding 0", empty and empty[0]["count"] == 0, str(empty))
    check(f"{L} SUM of an empty result is null, not 0",
          q7("FROM t WHERE fee > 999 SUM fee")[0]["sum_fee"] is None)
    # A GROUPED aggregate over zero rows correctly has no groups.
    check(f"{L} a grouped aggregate over zero rows has no groups",
          q7("FROM t WHERE fee > 999 GROUP BY status COUNT") == [])
    check(f"{L} a bare aggregate carries no group key",
          set(q7("FROM t COUNT")[0].keys()) == {"count", "value"},
          str(sorted(q7("FROM t COUNT")[0].keys())))
    check(f"{L} bare aggregate + HAVING",
          len(q7("FROM t COUNT HAVING count > 3")) == 1
          and q7("FROM t COUNT HAVING count > 99") == [])

    # ── an aggregate over a `_`-prefixed METADATA field ──────────────────────
    #
    # `_seq` does not live in a document's payload — it lives on the node. The
    # Rust aggregator read the payload directly and so answered NULL for
    # `MAX _seq`, while `SELECT _seq` listed the values and `WHERE _seq > 5`
    # filtered on them perfectly. Python builds its groups from projected dicts
    # that already carry `_seq` and answered correctly, so this was a live
    # cross-engine divergence as well as a wrong answer.
    #
    # It matters more than its size: "what is the newest sequence?" is the
    # question replication, `since()` and time travel are all built on, and a
    # confident null is the worst possible shape for that answer.
    print("\n── aggregates over node metadata ──")
    seqs = [r.get("_seq") for r in q7("FROM t")]
    check(f"{L} every row carries a _seq to aggregate over",
          all(isinstance(s, int) for s in seqs), str(seqs[:4]))
    check(f"{L} MAX _seq is the highest sequence, not null",
          q7("FROM t MAX _seq")[0]["max__seq"] == max(seqs),
          str(q7("FROM t MAX _seq")))
    check(f"{L} MIN _seq is the lowest sequence",
          q7("FROM t MIN _seq")[0]["min__seq"] == min(seqs))
    check(f"{L} SUM _seq adds the sequences",
          q7("FROM t SUM _seq")[0]["sum__seq"] == sum(seqs))
    check(f"{L} MAX _seq respects WHERE",
          q7("FROM t WHERE fee > 8 MAX _seq")[0]["max__seq"]
          == max(r["_seq"] for r in q7("FROM t WHERE fee > 8")))
    check(f"{L} GROUP BY _seq keys on the sequence, not null",
          len(q7("FROM t GROUP BY _seq COUNT")) == 12
          and all(g.get("_seq") is not None for g in q7("FROM t GROUP BY _seq COUNT")),
          str(q7("FROM t GROUP BY _seq COUNT")[:2]))

    # ── a field named like a reserved word ───────────────────────────────────
    #
    # Field positions accept a keyword as a field name, but both engines
    # canonicalised the case there (Rust uppercased, Python lowercased) and
    # then looked up a key the document does not have. Silent, and it hit real
    # names: count, min, max, sum, avg, value, limit, offset, group, search.
    print("\n── a field named like a keyword is still addressable ──")
    KWFIELDS = [
        ("1", {"count": 5, "min": 1, "max": 9, "sum": 3, "avg": 2,
               "value": "keep", "limit": 7, "offset": 8, "group": "g1"}),
        ("2", {"count": 1, "min": 0, "max": 2, "sum": 0, "avg": 0,
               "value": "drop", "limit": 0, "offset": 0, "group": "g2"}),
    ]
    q8 = mk(KWFIELDS)
    for nql in [
        "FROM t WHERE count > 3", "FROM t WHERE min = 1", "FROM t WHERE max >= 9",
        "FROM t WHERE sum = 3", "FROM t WHERE avg = 2", 'FROM t WHERE value = "keep"',
        "FROM t WHERE limit = 7", "FROM t WHERE offset = 8", 'FROM t WHERE group = "g1"',
    ]:
        got = q8(nql)
        check(f"{L} {nql}", len(got) == 1 and str(got[0].get("_id")) == "1",
              f"matched {len(got)} row(s)")
    check(f"{L} ORDER BY a keyword-named field",
          str(q8("FROM t ORDER BY count DESC")[0].get("_id")) == "1")
    check(f"{L} GROUP BY a keyword-named field",
          len(q8("FROM t GROUP BY group COUNT")) == 2)

    # ── VALID AS OF must filter BEFORE the limit ─────────────────────────────
    print("\n── pipeline: VALID AS OF before LIMIT ──")
    # 6 docs, alternating: 3 valid at the probe date, 3 expired before it.
    valid_rows = []
    for i in range(6):
        ok = i % 2 == 0
        valid_rows.append((str(i), {
            "n": i,
            "_valid_from": "2024-01-01",
            "_valid_to": "2030-01-01" if ok else "2024-02-01",
        }))
    q9 = mk(valid_rows, valid=True)
    probe = '2024-06-01'
    total = q9(f'FROM t VALID AS OF "{probe}"')
    check(f"{L} 3 of 6 docs are valid at the probe date",
          len(total) == 3, f"{len(total)} valid")
    lim = q9(f'FROM t VALID AS OF "{probe}" LIMIT 3')
    check(f"{L} VALID AS OF + LIMIT 3 returns 3 valid rows",
          len(lim) == 3, f"got {len(lim)} — the limit truncated before the filter")
    check(f"{L} every returned row is actually valid",
          all(r.get("_valid_to", "9999") > probe for r in lim), str(lim))


# ── engine 1: the Python reference ───────────────────────────────────────────

print(f"\nnedb {nedb.__version__}  |  native DAG available: {nedb.__has_native__}")


def py_mk(rows, valid=False):
    db = nedb.NEDB()
    for i, d in rows:
        if valid:
            body = {k: v for k, v in d.items() if not k.startswith("_valid")}
            db.put("t", i, body,
                   valid_from=d.get("_valid_from"), valid_to=d.get("_valid_to"))
        else:
            db.put("t", i, d)
    return db.query


run_engine("python", py_mk)

# ── engine 2: the Rust DAG core, when a platform wheel is installed ─────────

native = False
if nedb.__has_native__:
    import json as _json

    from nedb._native import NedbCore  # noqa: E402

    tmproot = tempfile.mkdtemp(suffix="-nqlshape")
    _seq = [0]

    def rust_mk(rows, valid=False):
        _seq[0] += 1
        core = NedbCore.open(os.path.join(tmproot, f"db{_seq[0]}"))
        for i, d in rows:
            if valid:
                # The PyO3 binding reads valid-time from `valid_from` /
                # `valid_to` keys INSIDE the doc, not as kwargs.
                body = {k: v for k, v in d.items() if not k.startswith("_valid")}
                body["valid_from"] = d.get("_valid_from")
                body["valid_to"] = d.get("_valid_to")
                core.put("t", i, _json.dumps(body))
            else:
                core.put("t", i, _json.dumps(d))
        return lambda nql: [_json.loads(r) for r in core.query(nql)]

    try:
        run_engine("rust", rust_mk)

        # ── the parity assertion itself ──────────────────────────────────────
        print(f"\n{'=' * 62}\ncross-engine parity\n{'=' * 62}")
        CASES = [
            (SPREAD, False, "FROM t ORDER BY fee OFFSET 9"),
            (SPREAD, False, "FROM t ORDER BY fee LIMIT 5 OFFSET 5"),
            (SPREAD, False, "FROM t OFFSET 99"),
            (PAIRS,  False, "FROM t ORDER BY g, fee DESC"),
            (PAIRS,  False, "FROM t ORDER BY g DESC, fee ASC"),
            (SPREAD, False, "FROM t GROUP BY status COUNT"),
            (SPREAD, False, "FROM t GROUP BY status COUNT LIMIT 2"),
            (SPREAD, False, "FROM t GROUP BY status SUM fee ORDER BY sum_fee DESC"),
            (SPREAD, False, "FROM t GROUP BY status SUM fee HAVING sum_fee > 20"),
            (UNEVEN, False, "FROM t GROUP BY g COUNT ORDER BY count DESC"),
            (UNEVEN, False, "FROM t GROUP BY g COUNT HAVING count > 1"),
            (UNEVEN, False, "FROM t GROUP BY g COUNT ORDER BY g LIMIT 1 OFFSET 1"),
            (SPREAD, False, "FROM t COUNT"),
            (SPREAD, False, "FROM t SUM fee"),
            (SPREAD, False, "FROM t AVG fee"),
            (SPREAD, False, "FROM t MIN fee"),
            (SPREAD, False, "FROM t MAX fee"),
            (SPREAD, False, "FROM t WHERE fee > 8 COUNT"),
            (SPREAD, False, "FROM t WHERE fee > 999 COUNT"),
            (SPREAD, False, "FROM t WHERE fee > 999 SUM fee"),
            (SPREAD, False, "FROM t COUNT HAVING count > 3"),
            (SPREAD, False, "FROM t WHERE fee >= 9 ORDER BY fee OFFSET 1"),
        ]

        def norm(rows, nql=""):
            """Compare on sorted key/value pairs.

            The engines legitimately differ on incidental metadata (the Rust
            core injects _hash/_seq/_coll), so compare the fields the QUERY is
            about: everything not prefixed with `_`, plus `_id`.

            ROW ORDER is only compared when the query ASKED for an order. A
            result set with no ORDER BY is unordered by definition — NQL makes
            no promise there, and the two engines legitimately arrive at
            different orders (`FROM t AS OF 2` returned the same two rows as
            `[keep, a]` from Python and `[a, keep]` from Rust).

            Comparing those as ordered lists made this harness assert a
            guarantee the engines do not give, which is a FLAKY TEST: it passed
            locally and failed in CI on identical data. A flaky parity gate is
            worse than none, because the next real divergence gets dismissed as
            "just the flaky one".
            """
            out = []
            for r in rows:
                keep = {k: v for k, v in r.items()
                        if not k.startswith("_") or k == "_id"}
                out.append(tuple(sorted((k, str(v)) for k, v in keep.items())))
            # Ordering is part of the assertion only when the query requested
            # it. GROUP BY is included: both engines document a deterministic
            # group order, so a difference there IS a divergence.
            up = nql.upper()
            if "ORDER BY" in up or "GROUP BY" in up:
                return out
            return sorted(out)

        for rows, valid, nql in CASES:
            a = norm(py_mk(rows, valid=valid)(nql), nql)
            b = norm(rust_mk(rows, valid=valid)(nql), nql)
            check(f"parity: {nql}", a == b,
                  "" if a == b else f"\n      python {a}\n      rust   {b}")

        # ── a DELETE is a tombstone, not an erasure ──────────────────────────
        #
        # `AS OF` used to return NOTHING for a deleted id in the Rust engine —
        # at every sequence, including sequences long before the delete where
        # the row demonstrably existed. So the headline claim ("a DELETE is a
        # tombstone, the row stays in history") was false on the engine that
        # ships to crates.io and npm.
        #
        # The Python reference was already RIGHT, which made this a live
        # cross-engine divergence, not just a bug in one. Nothing was ever lost
        # on disk — the tombstone keeps a `prev` link to the chain and
        # `verify()` counted every object as healthy. That is the worst kind of
        # data loss: the kind that passes its own audit.
        #
        # Needs its own factories because `run_engine`'s only write puts rows.
        print(f"\n{'=' * 62}\ncross-engine: history survives a DELETE\n{'=' * 62}")

        def py_delete_case():
            db = nedb.NEDB()
            db.put("t", "a", {"t": 55})
            db.put("t", "a", {"t": 66})
            db.put("t", "keep", {"t": 1})
            db.delete("t", "a")
            return db.query

        def rust_delete_case():
            _seq[0] += 1
            core = NedbCore.open(os.path.join(tmproot, f"del{_seq[0]}"))
            core.put("t", "a", _json.dumps({"t": 55}))
            core.put("t", "a", _json.dumps({"t": 66}))
            core.put("t", "keep", _json.dumps({"t": 1}))
            core.delete("t", "a")
            return lambda nql: [_json.loads(r) for r in core.query(nql)]

        for label, q in (("python", py_delete_case()), ("rust", rust_delete_case())):
            now = q("FROM t")
            check(f"[{label}] a delete still deletes",
                  [r["_id"] for r in now] == ["keep"], str([r["_id"] for r in now]))
            at0 = q("FROM t AS OF 0")
            check(f"[{label}] AS OF 0 — the ORIGINAL value survives the delete",
                  [r.get("t") for r in at0] == [55], str(at0))
            at1 = [r for r in q("FROM t AS OF 1") if r["_id"] == "a"]
            check(f"[{label}] AS OF 1 — the UPDATED value survives the delete",
                  [r.get("t") for r in at1] == [66], str(at1))
            at3 = [r for r in q("FROM t AS OF 3") if r["_id"] == "a"]
            check(f"[{label}] AS OF at/after the tombstone reports it absent",
                  at3 == [], str(at3))
            check(f"[{label}] the tombstone is never surfaced as a document",
                  all("_deleted" not in r for s in range(5)
                      for r in q(f"FROM t AS OF {s}")),
                  "a _deleted field would look like a real document")

        # And the two engines must agree on every one of those.
        pq, rq = py_delete_case(), rust_delete_case()
        for nql in ["FROM t", "FROM t AS OF 0", "FROM t AS OF 1",
                    "FROM t AS OF 2", "FROM t AS OF 3"]:
            a, b = norm(pq(nql), nql), norm(rq(nql), nql)
            check(f"parity after a delete: {nql}", a == b,
                  "" if a == b else f"\n      python {a}\n      rust   {b}")

        native = True
    finally:
        shutil.rmtree(tmproot, ignore_errors=True)
else:
    print("\n  …  native DAG core not installed — cross-engine parity skipped.")
    print("      (the python-native CI tier installs the maturin wheel and runs it)")

# ── tally ────────────────────────────────────────────────────────────────────

print(f"\n{'=' * 62}")
print(f"NQL shaping ({'python + rust' if native else 'python'}): "
      f"{len(PASS)} passed, {len(FAIL)} failed")
if FAIL:
    print("FAILED:", *FAIL, sep="\n  - ")
    sys.exit(1)
print("OFFSET / multi-key ORDER BY / HAVING / bare aggregates — and LIMIT,")
print("ORDER BY and VALID AS OF now apply where SQL says they do.")
