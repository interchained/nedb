#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
The DAG and provenance survive the new query surface — and reads never mutate.

3.3.0 rewrote the NQL parser and executor in both engines: a predicate tree
replacing a flat clause list, a reordered pipeline, new aggregate paths. All of
that is READ-side work, and none of it may touch the write-side guarantees
that are the actual product:

  * the BLAKE2b hash chain
  * verify(), and its ability to still detect tampering
  * AS OF (transaction time) and VALID AS OF (valid time)
  * TRACE caused_by, forward and reverse
  * the provenance fields carried on every returned row

A query engine that quietly advanced the head, dropped `_caused_by` from its
output, or made `verify()` unreachable would be a far worse regression than any
missing SQL operator. Nothing asserted that it didn't, so this suite does.

The central invariant: running the ENTIRE new query surface leaves head, seq
and verify() exactly where they were. Reads are reads.

Run: python3 tests/test_dag_preserved.py
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

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    print(("  ok  " if cond else "  FAIL  ") + name + (f"  — {detail}" if detail else ""))


# Every shape the 3.3.0 work introduced or reordered. Run as a battery against
# a live chain; none may disturb it.
READ_BATTERY = [
    "FROM jobs",
    'FROM jobs WHERE status IN ("open", "closed")',
    'FROM jobs WHERE status NOT IN ("open")',
    "FROM jobs WHERE fee BETWEEN 10 AND 40",
    "FROM jobs WHERE fee NOT BETWEEN 10 AND 40",
    'FROM jobs WHERE miner LIKE "Acme%"',
    'FROM jobs WHERE miner ILIKE "acme%"',
    'FROM jobs WHERE miner NOT LIKE "Acme%"',
    "FROM jobs WHERE miner IS NULL",
    "FROM jobs WHERE miner IS NOT NULL",
    "FROM jobs WHERE fee = 10 OR fee = 50",
    'FROM jobs WHERE (fee = 10 OR fee = 50) AND status = "open"',
    "FROM jobs WHERE NOT (fee > 20)",
    "FROM jobs ORDER BY status, fee DESC",
    "FROM jobs ORDER BY fee LIMIT 2 OFFSET 1",
    "FROM jobs OFFSET 3",
    "FROM jobs COUNT",
    "FROM jobs SUM fee",
    "FROM jobs AVG fee",
    "FROM jobs MIN fee",
    "FROM jobs MAX fee",
    "FROM jobs GROUP BY status COUNT",
    "FROM jobs GROUP BY status SUM fee",
    "FROM jobs GROUP BY status COUNT HAVING count > 1",
    "FROM jobs GROUP BY status SUM fee ORDER BY sum_fee DESC LIMIT 1",
    'FROM jobs SEARCH "acme"',
    "FROM jobs AS OF 1",
    'FROM rates VALID AS OF "2024-06-01"',
    "FROM effects TRACE caused_by",
]


def run_engine(label, eng):
    """`eng` exposes: query, verify, head, seq, put_jobs, put_rates, put_causal."""
    print(f"\n{'=' * 66}\n{label}\n{'=' * 66}")
    L = f"[{label}]"

    eng.seed()

    head_before = eng.head()
    seq_before = eng.seq()
    check(f"{L} chain verifies before the battery", eng.verify() is True)
    check(f"{L} head is non-empty", bool(head_before), repr(head_before))

    print("\n── the entire new query surface, against a live chain ──")
    ran = 0
    for nql in READ_BATTERY:
        try:
            eng.query(nql)
            ran += 1
        except Exception as e:                                    # noqa: BLE001
            check(f"{L} query ran: {nql}", False, f"{type(e).__name__}: {e}")
    check(f"{L} all {len(READ_BATTERY)} query forms executed",
          ran == len(READ_BATTERY), f"{ran}/{len(READ_BATTERY)}")

    # THE CENTRAL INVARIANT.
    print("\n── reads are reads: nothing moved ──")
    check(f"{L} head is unchanged after the battery",
          eng.head() == head_before, f"{head_before} -> {eng.head()}")
    check(f"{L} seq is unchanged after the battery",
          eng.seq() == seq_before, f"{seq_before} -> {eng.seq()}")
    check(f"{L} chain still verifies after the battery", eng.verify() is True)

    # Repeat the battery — an engine that mutated once per read would drift.
    for nql in READ_BATTERY:
        try:
            eng.query(nql)
        except Exception:                                          # noqa: BLE001
            pass
    check(f"{L} head is still unchanged after a second pass",
          eng.head() == head_before)
    check(f"{L} seq is still unchanged after a second pass",
          eng.seq() == seq_before)

    # ── provenance fields survive the new output path ────────────────────────
    print("\n── provenance fields are still emitted ──")
    rows = eng.query('FROM jobs WHERE status IN ("open", "closed")')
    check(f"{L} rows carry _id", all("_id" in r for r in rows))
    check(f"{L} rows carry _hash", all("_hash" in r for r in rows),
          str(sorted(rows[0].keys())) if rows else "no rows")
    check(f"{L} rows carry _seq", all("_seq" in r for r in rows))
    check(f"{L} _hash values are non-empty",
          all(r.get("_hash") for r in rows))

    eff = eng.query("FROM effects")
    check(f"{L} a caused_by write still reports its causes",
          bool(eff) and bool(eff[0].get("_caused_by")),
          str(eff[0]) if eff else "no rows")

    rates = eng.query("FROM rates")
    check(f"{L} bi-temporal rows still carry _valid_from/_valid_to",
          any(r.get("_valid_from") and r.get("_valid_to") for r in rates),
          str(rates[0]) if rates else "no rows")

    # ── the metadata fields are FILTERABLE, not just decorative ──────────────
    #
    # Before 3.3.0 only _id, _coll and _hash resolved in a predicate, so
    # `WHERE _seq > 1` compared null against 1 and returned an empty set —
    # even though _seq appears on every row the engine emits. Filtering on a
    # field the engine itself prints should never silently answer "nothing".
    print("\n── metadata fields resolve in predicates ──")
    all_rows = eng.query("FROM jobs")
    max_seq = max(r["_seq"] for r in all_rows)
    by_seq = eng.query(f"FROM jobs WHERE _seq >= {max_seq}")
    check(f"{L} _seq is filterable", len(by_seq) >= 1, f"{len(by_seq)} row(s)")
    check(f"{L} _seq filter agrees with the printed value",
          all(r["_seq"] >= max_seq for r in by_seq))
    check(f"{L} _coll is filterable",
          len(eng.query('FROM jobs WHERE _coll = "jobs"')) == len(all_rows))
    one = all_rows[0]
    check(f"{L} _id is filterable",
          len(eng.query(f'FROM jobs WHERE _id = "{one["_id"]}"')) == 1)
    check(f"{L} _hash is filterable",
          len(eng.query(f'FROM jobs WHERE _hash = "{one["_hash"]}"')) == 1)
    # And through the NEW operators, not just equality.
    check(f"{L} _id works with IN",
          len(eng.query(f'FROM jobs WHERE _id IN ("{one["_id"]}")')) == 1)

    # ── time travel still works, and composes with the new predicates ────────
    print("\n── AS OF composes with the new predicate surface ──")
    hist = eng.query("FROM jobs AS OF 0")
    check(f"{L} AS OF 0 returns the first write only",
          len(hist) == 1, f"{len(hist)} row(s)")
    check(f"{L} AS OF 0 returns the HISTORICAL value",
          hist and hist[0].get("status") == "open", str(hist))
    # doc 1 was later updated to status=closed, fee=11. At seq 0 it is still
    # open/10, so an IN predicate evaluated AS OF must see the old value.
    old_in = eng.query('FROM jobs AS OF 0 WHERE status IN ("open")')
    check(f"{L} a predicate AS OF sees the historical value", len(old_in) == 1,
          f"{len(old_in)} row(s)")
    new_in = eng.query('FROM jobs WHERE _id = "1" AND status IN ("open")')
    check(f"{L} ... and the current value differs", len(new_in) == 0,
          f"{len(new_in)} row(s) — doc 1 is now closed")

    print("\n── VALID AS OF composes with the new predicate surface ──")
    v = eng.query('FROM rates VALID AS OF "2024-06-01"')
    check(f"{L} VALID AS OF filters by valid time", len(v) == 2, f"{len(v)} row(s)")
    vin = eng.query('FROM rates VALID AS OF "2024-06-01" WHERE n IN (0, 2)')
    check(f"{L} VALID AS OF + IN", len(vin) == 2, f"{len(vin)} row(s)")
    # The pipeline fix: valid-time filters BEFORE the limit, so a limited
    # bi-temporal query returns as many valid rows as exist.
    vlim = eng.query('FROM rates VALID AS OF "2024-06-01" LIMIT 2')
    check(f"{L} VALID AS OF + LIMIT 2 returns 2 VALID rows",
          len(vlim) == 2, f"{len(vlim)} — the limit truncated before the filter")

    print("\n── TRACE composes with the new predicate surface ──")
    back = eng.query("FROM effects TRACE caused_by")
    check(f"{L} TRACE caused_by walks backward", len(back) >= 1, f"{len(back)} row(s)")
    fwd = eng.query("FROM causes TRACE caused_by REVERSE")
    check(f"{L} TRACE REVERSE walks forward", len(fwd) >= 1, f"{len(fwd)} row(s)")
    traced = eng.query('FROM effects WHERE kind IN ("derived") TRACE caused_by')
    check(f"{L} TRACE after an IN predicate", len(traced) >= 1, f"{len(traced)} row(s)")

    # ── verify() still detects tampering ─────────────────────────────────────
    #
    # The whole product promise. An engine where verify() cannot fail is worse
    # than one with no verify() at all, because it lies with authority.
    print("\n── tamper detection still fires ──")
    if eng.can_tamper():
        check(f"{L} verify() is True on an intact chain", eng.verify() is True)
        eng.tamper()
        check(f"{L} verify() is FALSE after tampering", eng.verify() is False,
              "a chain that cannot fail verification proves nothing")
    else:
        print("    …  this engine exposes no in-process tamper hook — skipped")


# ── engine 1: the Python reference ───────────────────────────────────────────

class PyEngine:
    def __init__(self):
        self.tmp = tempfile.mkdtemp(suffix="-dagpy")
        self.db = nedb.NEDB(os.path.join(self.tmp, "db"))

    def seed(self):
        jobs = [
            ("1", {"status": "open",    "miner": "Acme Pool", "fee": 10}),
            ("2", {"status": "pending", "miner": "acme solo", "fee": 20}),
            ("3", {"status": "closed",  "miner": "Zenith",    "fee": 30}),
            ("4", {"status": "open",                          "fee": 40}),
            ("5", {"status": "voided",  "miner": None,        "fee": 50}),
        ]
        for i, d in jobs:
            self.db.put("jobs", i, d)
        # A second version of doc 1, so AS OF has history to show.
        self.db.put("jobs", "1", {"status": "closed", "miner": "Acme Pool", "fee": 11})
        for i in range(4):
            ok = i % 2 == 0
            self.db.put("rates", str(i), {"n": i},
                        valid_from="2024-01-01",
                        valid_to="2030-01-01" if ok else "2024-02-01")
        # The Python engine's caused_by takes integer SEQs (the Rust core
        # takes node hashes) — db.seq is the seq of the write just made.
        self.db.put("causes", "c1", {"kind": "root"})
        self.db.put("effects", "e1", {"kind": "derived"}, caused_by=[self.db.seq])

    def query(self, nql):
        return self.db.query(nql)

    def verify(self):
        return self.db.verify()

    def head(self):
        # `head` and `seq` are PROPERTIES on the Python engine and METHODS on
        # the Rust core; this shim hides that so one battery drives both.
        return self.db.head

    def seq(self):
        return self.db.seq

    def can_tamper(self):
        return True

    def tamper(self):
        # Rewrite a payload in the materialized log without touching the
        # recorded hash, which is exactly what verify() exists to catch.
        for op in self.db.log.ops:
            if op.op == "put":
                op.payload["doc"]["fee"] = 999999
                return

    def close(self):
        shutil.rmtree(self.tmp, ignore_errors=True)


py = PyEngine()
print(f"\nnedb {nedb.__version__}  |  native DAG available: {nedb.__has_native__}")
try:
    run_engine("python", py)
finally:
    py.close()


# ── engine 2: the Rust DAG core ──────────────────────────────────────────────

native = False
if nedb.__has_native__:
    from nedb._native import NedbCore  # noqa: E402

    class RustEngine:
        def __init__(self):
            self.tmp = tempfile.mkdtemp(suffix="-dagrs")
            self.core = NedbCore.open(os.path.join(self.tmp, "db"))

        def seed(self):
            jobs = [
                ("1", {"status": "open",    "miner": "Acme Pool", "fee": 10}),
                ("2", {"status": "pending", "miner": "acme solo", "fee": 20}),
                ("3", {"status": "closed",  "miner": "Zenith",    "fee": 30}),
                ("4", {"status": "open",                          "fee": 40}),
                ("5", {"status": "voided",  "miner": None,        "fee": 50}),
            ]
            for i, d in jobs:
                self.core.put("jobs", i, json.dumps(d))
            self.core.put("jobs", "1", json.dumps(
                {"status": "closed", "miner": "Acme Pool", "fee": 11}))
            for i in range(4):
                ok = i % 2 == 0
                # The PyO3 binding reads valid-time from keys inside the doc.
                self.core.put("rates", str(i), json.dumps({
                    "n": i, "valid_from": "2024-01-01",
                    "valid_to": "2030-01-01" if ok else "2024-02-01"}))
            # The PyO3 put returns the node as JSON in the same shape a query
            # row has, so the hash is under `_hash`; the Rust core's caused_by
            # takes node HASHES where the Python engine takes integer seqs.
            node = json.loads(self.core.put("causes", "c1", json.dumps({"kind": "root"})))
            self.core.put("effects", "e1", json.dumps(
                {"kind": "derived", "caused_by": [node["_hash"]]}))

        def query(self, nql):
            return [json.loads(r) for r in self.core.query(nql)]

        def verify(self):
            v = self.core.verify()
            # The PyO3 binding returns a plain bool; nedbd's HTTP /verify
            # returns an object with `ok`. Accept either.
            if isinstance(v, str):
                v = json.loads(v)
            if isinstance(v, dict):
                return v.get("ok")
            return bool(v)

        def head(self):
            return self.core.head()

        def seq(self):
            return self.core.seq()

        def can_tamper(self):
            # Tampering the segment store means writing bytes behind the
            # engine's back; store.rs and segment.rs already carry dedicated
            # tamper_detected tests for exactly that, so it is not repeated
            # through the Python binding here.
            return False

        def tamper(self):
            raise NotImplementedError

        def close(self):
            del self.core
            shutil.rmtree(self.tmp, ignore_errors=True)

    rs = RustEngine()
    try:
        run_engine("rust", rs)
        native = True
    finally:
        rs.close()
else:
    print("\n  …  native DAG core not installed — Rust leg skipped.")
    print("      (the python-native CI tier installs the maturin wheel and runs it)")


# ── tally ────────────────────────────────────────────────────────────────────

print(f"\n{'=' * 66}")
print(f"DAG preservation ({'python + rust' if native else 'python'}): "
      f"{len(PASS)} passed, {len(FAIL)} failed")
if FAIL:
    print("FAILED:", *FAIL, sep="\n  - ")
    sys.exit(1)
print("The new query surface reads the chain without moving it: head, seq and")
print("verify() are untouched, provenance fields survive, AS OF / VALID AS OF /")
print("TRACE still compose, and verify() can still fail when it should.")
