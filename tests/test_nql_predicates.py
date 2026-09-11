#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
NQL predicate surface — and the cross-engine parity gate.

NEDB ships TWO independent NQL implementations: the Python reference
(python/nedb/query.py + engine.py) and the Rust engine (rust/nedb-v2/src/nql.rs)
that backs nedbd, the napi addon and the PyO3 wheel. Nothing asserted that they
AGREE, and they had silently drifted apart in three separate places:

  1. Rust REQUIRED the aggregate keyword after GROUP BY; Python made it
     optional. `FROM t GROUP BY status` was valid in one engine and a parse
     error in the other.

  2. Rust accepted no target field for SUM/AVG/MIN/MAX and aggregated the
     GROUP BY field itself, so `GROUP BY cat MAX price` reported the max *cat*
     — coerced to 1.0 for a non-numeric value, making every group answer 1.
     Python aggregated `price` correctly. The target field was lexed and then
     silently discarded.

  3. Rust emitted the aggregate under the key `value`; Python emitted
     `max_price`. A caller reading one key got nothing from the other engine.

Underneath all three sat the same root cause: Rust's clause loop ended with
`_ => { self.advance(); }` — "skip unrecognised". A token the parser did not
understand was dropped, so the engine answered a DIFFERENT query than the one
asked and returned HTTP 200 while doing it. Python had always raised on
trailing tokens. Rust is now strict too.

This suite runs the SAME queries against every engine available in the
environment and asserts identical answers. It is deliberately engine-agnostic
so it runs in the dependency-free CI tier (Python engine alone) and gains the
native DAG engine automatically wherever a platform wheel is installed.

Run: python3 tests/test_nql_predicates.py
"""
import os
import shutil
import sys
import tempfile

# APPEND, not insert(0): an installed native wheel must win over the source
# tree, or the cross-engine leg silently skips — the source `python/nedb` has
# no `_native` extension module, so prepending it pins __has_native__ to False
# even on a machine where the Rust core is installed. The dependency-free CI
# tier sets PYTHONPATH=python and has no wheel, so it still resolves here.
sys.path.append("python")

import nedb  # noqa: E402
from nedb.query import like_to_regex, parse_nql  # noqa: E402

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("  ok  " if cond else "  FAIL  ") + name + (f"  — {detail}" if detail else ""))


# ── the fixture, identical on every engine ───────────────────────────────────
#
# `miner` is absent on row 4 and explicitly null on row 5, so IS NULL and the
# LIKE-over-null corner are both exercised.

# `miner` is sparse on purpose (absent on row 4, explicitly null on row 5), and
# `bonus` is sparse too (present ONLY on rows 1 and 2) so an ordering
# comparison against a missing numeric field is directly assertable.
ROWS = [
    ("1", {"status": "open",    "miner": "Acme Pool", "fee": 10, "region": "eu-west",
           "bonus": 5}),
    ("2", {"status": "pending", "miner": "acme solo", "fee": 20, "region": "us-east",
           "bonus": 50}),
    ("3", {"status": "closed",  "miner": "Zenith",    "fee": 30, "region": "eu-north"}),
    ("4", {"status": "open",                          "fee": 40, "region": "us-west"}),
    ("5", {"status": "voided",  "miner": None,        "fee": 50, "region": "ap-south"}),
]

# (name, NQL, expected sorted _id list)
CASES = [
    ("IN",                'FROM jobs WHERE status IN ("open","closed")',        ["1", "3", "4"]),
    ("IN numeric",        "FROM jobs WHERE fee IN (10, 30)",                     ["1", "3"]),
    ("IN single = eq",    "FROM jobs WHERE fee IN (30)",                         ["3"]),
    ("NOT IN",            'FROM jobs WHERE status NOT IN ("open")',              ["2", "3", "5"]),
    ("BETWEEN inclusive", "FROM jobs WHERE fee BETWEEN 20 AND 40",               ["2", "3", "4"]),
    ("NOT BETWEEN",       "FROM jobs WHERE fee NOT BETWEEN 20 AND 40",           ["1", "5"]),
    ("BETWEEN then AND",  'FROM jobs WHERE fee BETWEEN 10 AND 40 AND status = "open"', ["1", "4"]),
    ("LIKE prefix",       'FROM jobs WHERE miner LIKE "Acme%"',                  ["1"]),
    ("LIKE suffix",       'FROM jobs WHERE miner LIKE "%Pool"',                  ["1"]),
    ("LIKE infix",        'FROM jobs WHERE status LIKE "%pen%"',                 ["1", "2", "4"]),
    ("LIKE underscore",   'FROM jobs WHERE status LIKE "ope_"',                  ["1", "4"]),
    ("LIKE no match",     'FROM jobs WHERE status LIKE "open_"',                 []),
    ("ILIKE",             'FROM jobs WHERE miner ILIKE "acme%"',                 ["1", "2"]),
    ("LIKE case-sens",    'FROM jobs WHERE miner LIKE "acme%"',                  ["2"]),
    ("NOT LIKE",          'FROM jobs WHERE status NOT LIKE "open"',              ["2", "3", "5"]),
    ("IS NULL",           "FROM jobs WHERE miner IS NULL",                       ["4", "5"]),
    ("IS NOT NULL",       "FROM jobs WHERE miner IS NOT NULL",                   ["1", "2", "3"]),
    ("OR",                "FROM jobs WHERE fee = 10 OR fee = 50",                ["1", "5"]),
    ("AND over OR",       'FROM jobs WHERE fee = 10 OR fee = 50 AND status = "nope"', ["1"]),
    ("parens override",   'FROM jobs WHERE (fee = 10 OR fee = 50) AND status = "open"', ["1"]),
    ("NOT group",         "FROM jobs WHERE NOT (fee > 20)",                      ["1", "2"]),
    ("prefix NOT",        "FROM jobs WHERE NOT fee = 10",                        ["2", "3", "4", "5"]),
    ("nested groups",
     'FROM jobs WHERE ((fee >= 20 AND fee <= 40) OR fee = 10) AND status != "closed"',
     ["1", "2", "4"]),
    ("three-way OR",      "FROM jobs WHERE fee = 10 OR fee = 30 OR fee = 50",    ["1", "3", "5"]),
    ("LIKE + NOT + OR",
     'FROM jobs WHERE region LIKE "eu-%" OR NOT (fee < 50)',
     ["1", "3", "5"]),
    ("_id metadata",      'FROM jobs WHERE _id IN ("1","2")',                    ["1", "2"]),

    # ── ordering comparisons against a MISSING or NULL field ────────────────
    #
    # The gap that let a real divergence through. The fixture has a sparse
    # `miner` column (absent on row 4, explicitly null on row 5) but it was
    # only ever exercised with LIKE and IS NULL — never with < <= > >=. The
    # Rust engine's OrderedValue sorts Null below every number, so `<` and
    # `<=` reported that a row with NO miner satisfied the comparison, while
    # `>` and `>=` excluded it. The Python reference excluded it in all four.
    #
    # A sparse numeric column makes it directly assertable: `bonus` is present
    # only on rows 1 and 2.
    ("< skips a missing field",   "FROM jobs WHERE bonus < 100",        ["1", "2"]),
    ("<= skips a missing field",  "FROM jobs WHERE bonus <= 100",       ["1", "2"]),
    ("> skips a missing field",   "FROM jobs WHERE bonus > 0",          ["1", "2"]),
    (">= skips a missing field",  "FROM jobs WHERE bonus >= 0",         ["1", "2"]),
    ("BETWEEN skips it too",      "FROM jobs WHERE bonus BETWEEN 0 AND 100", ["1", "2"]),
    # = and != still operate on null, matching the reference exactly: its
    # None guard sits deliberately AFTER those two arms.
    ("!= still matches a missing field",
     "FROM jobs WHERE bonus != 5", ["2", "3", "4", "5"]),
    ("= NULL matches absent and explicit null",
     "FROM jobs WHERE bonus = NULL", ["3", "4", "5"]),
    ("ordering on a sparse string column",
     'FROM jobs WHERE miner > "A"', ["1", "2", "3"]),
    ("ordering on a sparse string column, other direction",
     'FROM jobs WHERE miner < "zzz"', ["1", "2", "3"]),
]

# Queries that MUST be rejected. Each one previously either parsed into a
# different query (Rust) or is genuinely malformed.
REJECT = [
    "FROM jobs ORDRE BY fee",                # typo, silently skipped before
    "FROM jobs LIMIT",                       # missing count
    "FROM jobs OFFSET",                      # missing count
    "FROM jobs ORDER BY",                    # missing sort key
    "FROM jobs HAVING count > 1",            # HAVING with no aggregate
    "FROM jobs SELECT fee",                  # wrong dialect
    "FROM jobs WHERE fee > 3 JUNK",          # trailing garbage
    "FROM jobs WHERE fee >",                 # missing value
    "FROM jobs WHERE fee IN (",              # unterminated list
    "FROM jobs WHERE fee BETWEEN 1",         # missing AND high
    "FROM jobs WHERE fee BETWEEN 1 3",       # missing AND
    "FROM jobs WHERE (fee = 1",              # unbalanced paren
    "FROM jobs WHERE fee IS 3",              # IS without NULL
    "FROM jobs WHERE fee NOT = 1",           # infix NOT before a comparison op
    "FROM jobs WHERE fee LIKE",              # missing pattern
    "FROM jobs GROUP BY status SUM",         # aggregate without a target field
]

# GROUP BY: (NQL, group key field, {group value: {expected key: expected number}})
GROUPS = [
    ("FROM jobs GROUP BY status MAX fee", "status", {"open": {"max_fee": 40, "count": 2}}),
    ("FROM jobs GROUP BY status MIN fee", "status", {"open": {"min_fee": 10, "count": 2}}),
    ("FROM jobs GROUP BY status SUM fee", "status", {"open": {"sum_fee": 50, "count": 2}}),
    ("FROM jobs GROUP BY status AVG fee", "status", {"open": {"avg_fee": 25, "count": 2}}),
    ("FROM jobs GROUP BY status",         "status", {"open": {"count": 2}}),
]


def ids(rows):
    return sorted(str(r.get("_id")) for r in rows)


def run_engine(label, put, query):
    """Run the whole battery against one engine."""
    print(f"\n{'=' * 62}\n{label}\n{'=' * 62}")

    for i, d in ROWS:
        put(i, d)

    print("\n── predicates ──")
    for name, nql, want in CASES:
        try:
            got = ids(query(nql))
        except Exception as e:                                  # noqa: BLE001
            check(f"[{label}] {name}", False, f"raised {type(e).__name__}: {e}")
            continue
        check(f"[{label}] {name}", got == sorted(want),
              f"got {got}, want {sorted(want)}")

    print("\n── a query the engine cannot honour must FAIL, not answer ──")
    for nql in REJECT:
        rejected = False
        try:
            query(nql)
        except Exception:                                       # noqa: BLE001
            rejected = True
        check(f"[{label}] rejects: {nql}", rejected,
              "returned rows instead of raising")

    print("\n── GROUP BY aggregates the TARGET field ──")
    for nql, keyfield, expected in GROUPS:
        try:
            rows = query(nql)
        except Exception as e:                                  # noqa: BLE001
            check(f"[{label}] {nql}", False, f"raised {type(e).__name__}: {e}")
            continue
        by = {str(r.get(keyfield)): r for r in rows}
        for gval, kv in expected.items():
            row = by.get(gval)
            if row is None:
                check(f"[{label}] {nql} [{gval}]", False, f"no such group in {rows}")
                continue
            for k, v in kv.items():
                check(f"[{label}] {nql} [{gval}].{k}",
                      row.get(k) == v, f"got {row.get(k)!r}, want {v!r}")


# ── engine 1: the Python reference ───────────────────────────────────────────

print(f"\nnedb {nedb.__version__}  |  native DAG available: {nedb.__has_native__}")

py_db = nedb.NEDB()
run_engine(
    "python",
    lambda i, d: py_db.put("jobs", i, d),
    py_db.query,
)

# ── engine 2: the Rust DAG core, when a platform wheel is installed ──────────

native_results = None
if nedb.__has_native__:
    from nedb._native import NedbCore  # noqa: E402

    tmpdir = tempfile.mkdtemp(suffix="-nqlparity")
    try:
        core = NedbCore.open(os.path.join(tmpdir, "db"))
        import json as _json

        run_engine(
            "rust",
            lambda i, d: core.put("jobs", i, _json.dumps(d)),
            lambda nql: [_json.loads(r) for r in core.query(nql)],
        )

        # The parity assertion itself: identical answers, engine to engine.
        print("\n── cross-engine parity ──")
        for name, nql, _want in CASES:
            try:
                a = ids(py_db.query(nql))
                b = ids([_json.loads(r) for r in core.query(nql)])
            except Exception as e:                              # noqa: BLE001
                check(f"parity: {name}", False, f"raised {type(e).__name__}: {e}")
                continue
            check(f"parity: {name}", a == b,
                  "" if a == b else f"python {a} vs rust {b}")

        native_results = True
        del core
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
else:
    print("\n  …  native DAG core not installed — cross-engine parity skipped.")
    print("      (the python-native CI tier installs the maturin wheel and runs it)")

# ── unit coverage for the LIKE translator ────────────────────────────────────

print("\n── LIKE pattern translation ──")
LIKE_CASES = [
    ("abcabcabd", "%abc%abd", True),
    ("aaa",       "%a",       True),
    ("",          "%",        True),
    ("x",         "%%%",      True),
    ("abc",       "%abd",     False),
    ("ab",        "ab_",      False),
    ("héllo wörld", "h_llo w%d", True),
    # Regex metacharacters in a pattern must be literal, not a character class.
    ("a.c",       "a.c",      True),
    ("abc",       "a.c",      False),
    ("a[b]c",     "a[b]c",    True),
    ("a+c",       "a+c",      True),
    ("aaac",      "a+c",      False),
]
for value, pattern, want in LIKE_CASES:
    got = bool(like_to_regex(pattern).match(value))
    check(f"LIKE {value!r} ~ {pattern!r} -> {want}", got == want, f"got {got}")

# ── the plan shape contract ──────────────────────────────────────────────────
#
# plan["where"] is the flat [(field, op, value)] list the equality-index
# accelerator and mongo.py both consume. It must stay populated for a pure
# conjunction of comparisons, and must be EMPTY for anything richer — handing
# the accelerator a partial view of an OR would let it narrow to the wrong
# candidate set and drop matching rows.

print("\n── plan shape: the index accelerator must never see a partial predicate ──")
flat = parse_nql('FROM jobs WHERE status = "open" AND fee > 5')
check("pure AND of comparisons still fills plan['where']",
      flat["where"] == [("status", "=", "open"), ("fee", ">", 5)], str(flat["where"]))
check("and also carries the tree", flat["predicate"] is not None)

for nql in [
    'FROM jobs WHERE status = "open" OR fee > 5',
    'FROM jobs WHERE status IN ("open")',
    "FROM jobs WHERE fee BETWEEN 1 AND 2",
    'FROM jobs WHERE miner LIKE "a%"',
    "FROM jobs WHERE miner IS NULL",
    'FROM jobs WHERE NOT (status = "open")',
]:
    p = parse_nql(nql)
    check(f"plan['where'] empty for: {nql}", p["where"] == [], str(p["where"]))
    check(f"plan['predicate'] present for: {nql}", p["predicate"] is not None)

# An OR that the accelerator must not narrow: row 1 matches only the first arm,
# row 5 only the second. If the accelerator narrowed on either arm alone, one
# of them would vanish.
or_rows = ids(py_db.query('FROM jobs WHERE status = "open" OR fee = 50'))
check("an OR returns the union, not one arm",
      or_rows == ["1", "4", "5"], str(or_rows))

# ── tally ────────────────────────────────────────────────────────────────────

print(f"\n{'=' * 62}")
engines = "python + rust" if native_results else "python"
print(f"NQL predicates ({engines}): {len(PASS)} passed, {len(FAIL)} failed")
if FAIL:
    print("FAILED:", *FAIL, sep="\n  - ")
    sys.exit(1)
print("IN / BETWEEN / LIKE / ILIKE / IS NULL / OR / NOT / parens — and both")
print("engines agree, aggregate the target field, and refuse what they cannot do.")
