#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
Bi-temporal queries — test suite for v1.0.0.

NEDB now carries two independent time axes:
  transaction time  — WHEN the fact was written to the database (seq / AS OF)
  valid time        — WHEN the fact was TRUE IN THE WORLD (VALID AS OF)

Every fact can have both, either, or neither time dimension. The two combine
to answer the four fundamental bi-temporal questions:

  1. What is true NOW?                      → plain FROM query
  2. What was true IN THE WORLD on date D?  → VALID AS OF "D"
  3. What did we KNOW at seq N?             → AS OF N
  4. What did we KNOW at seq N about what   → AS OF N VALID AS OF "D"
     was true on date D?

The last question — "what did the system believe, at a specific past moment,
about a specific past state of reality?" — is the gold standard for audit,
compliance, and AI agent reasoning. No other embedded database has it.

© INTERCHAINED LLC × Vex (Interchained AI fleet: GLM · Claude · Opus · Fable · GPT-6)"""
from __future__ import annotations
import os, sys, json, tempfile, shutil
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"))

from nedb import NEDB

PASS = FAIL = 0
def check(name, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  ok  {name}")
    else:    FAIL += 1; print(f"  FAIL {name}{(' — '+str(detail)) if detail else ''}")

def section(title): print(f"\n  ── {title} {'─'*(46-len(title))}")

# ─────────────────────────────────────────────────────────────────────────────
section("Basic valid-time writes and reads")
db = NEDB()

# Policy rate: 5% through 2023, then 6% from 2024 onwards
db.put("policy", "rate_2023", {"name": "Rate", "value": 0.05},
       valid_from="2023-01-01", valid_to="2023-12-31")
db.put("policy", "rate_2024", {"name": "Rate", "value": 0.06},
       valid_from="2024-01-01")

check("_valid_from mirrored into doc",
    db.get("policy", "rate_2023").get("_valid_from") == "2023-01-01")
check("_valid_to mirrored into doc",
    db.get("policy", "rate_2023").get("_valid_to")   == "2023-12-31")
check("open-ended _valid_to is absent",
    db.get("policy", "rate_2024").get("_valid_to") is None)
check("chain verifies after valid-time writes", db.verify())

# ─────────────────────────────────────────────────────────────────────────────
section("VALID AS OF — point-in-time queries")

rows = db.query('FROM policy VALID AS OF "2023-06-15"')
check("rate on 2023-06-15 is 5%",
    len(rows) == 1 and rows[0]["value"] == 0.05, rows)

rows = db.query('FROM policy VALID AS OF "2024-03-01"')
check("rate on 2024-03-01 is 6%",
    len(rows) == 1 and rows[0]["value"] == 0.06, rows)

rows = db.query('FROM policy VALID AS OF "2022-12-31"')
check("nothing valid before 2023",
    len(rows) == 0, rows)

# ─────────────────────────────────────────────────────────────────────────────
section("Backward compat — undated docs always pass VALID AS OF")
db2 = NEDB()
db2.put("config", "k1", {"v": 1})                  # no valid time
db2.put("config", "k2", {"v": 2},
        valid_from="2024-01-01", valid_to="2024-06-30")

rows = db2.query('FROM config VALID AS OF "2025-01-01"')
ids = {r["_id"] for r in rows}
check("undated doc always passes VALID AS OF",   "k1" in ids, ids)
check("expired dated doc excluded",              "k2" not in ids, ids)

rows_all = db2.query('FROM config')
check("plain query still returns all docs",      len(rows_all) == 2)

# ─────────────────────────────────────────────────────────────────────────────
section("Combined AS OF + VALID AS OF — the four-dimensional question")
db3 = NEDB()

# Write employee salary history with valid time
db3.put("employees", "alice", {"name": "Alice", "salary": 80_000},
        valid_from="2023-01-01", valid_to="2023-12-31")
snap1 = db3.seq   # transaction-time snapshot

# Later (higher seq): give Alice a raise, starting 2024
db3.put("employees", "alice", {"name": "Alice", "salary": 95_000},
        valid_from="2024-01-01")
snap2 = db3.seq

check("current salary is 95K",
    db3.query('FROM employees WHERE _id = "alice"')[0]["salary"] == 95_000)

# "What did the SYSTEM KNOW at snap1 about Alice's salary on 2023-06-01?"
rows_q3 = db3.query(f'FROM employees AS OF {snap1} VALID AS OF "2023-06-01"')
check("AS OF snap1 + VALID AS OF 2023-06-01: sees 80K",
    len(rows_q3) == 1 and rows_q3[0]["salary"] == 80_000, rows_q3)

# "What do we know NOW about what was true on 2023-06-01?"
# At HEAD, alice's record is the 2024 version (valid_from=2024-01-01). The 2023
# version was overwritten (MVCC keeps history, but HEAD only has one live record
# per id). The 2024 record doesn't cover 2023 → no result. This is correct:
# to read the 2023 version, use AS OF snap1 (transaction time).
rows_q4 = db3.query('FROM employees VALID AS OF "2023-06-01"')
check("NOW + VALID AS OF 2023-06-01: empty (2024 record not valid then)",
    len(rows_q4) == 0, rows_q4)

# Multiple valid-time versions at HEAD simultaneously → use different IDs (SCD Type 2)
db3.put("employees", "bob_2023", {"name": "Bob", "salary": 70_000},
        valid_from="2023-01-01", valid_to="2023-12-31")
db3.put("employees", "bob_2024", {"name": "Bob", "salary": 82_000},
        valid_from="2024-01-01")
rows_bob_2023 = db3.query('FROM employees VALID AS OF "2023-06-01"')
rows_bob_2024 = db3.query('FROM employees VALID AS OF "2024-06-01"')
check("SCD2: bob_2023 visible at HEAD on 2023-06-01",
    any(r["salary"] == 70_000 for r in rows_bob_2023), rows_bob_2023)
check("SCD2: bob_2024 visible at HEAD on 2024-06-01",
    any(r["salary"] == 82_000 for r in rows_bob_2024), rows_bob_2024)

# "What do we know NOW about what is true on 2024-06-01?"
rows_q5 = db3.query('FROM employees VALID AS OF "2024-06-01"')
check("NOW + VALID AS OF 2024-06-01: sees 95K",
    any(r["salary"] == 95_000 for r in rows_q5), rows_q5)

# ─────────────────────────────────────────────────────────────────────────────
section("VALID AS OF + WHERE")
db4 = NEDB()
db4.create_index("contracts", "status", "eq")

db4.put("contracts", "c1", {"client": "ACME",   "value": 100_000, "status": "active"},
        valid_from="2024-01-01", valid_to="2024-06-30")
db4.put("contracts", "c2", {"client": "Globex", "value": 200_000, "status": "active"},
        valid_from="2024-03-01")
db4.put("contracts", "c3", {"client": "Initech","value": 50_000,  "status": "inactive"},
        valid_from="2024-01-01")

rows = db4.query('FROM contracts WHERE status = "active" VALID AS OF "2024-04-15"')
clients = {r["client"] for r in rows}
check("WHERE active + VALID 2024-04-15: ACME and Globex",
    clients == {"ACME", "Globex"}, clients)
check("Initech (inactive) excluded", "Initech" not in clients)

rows_2025 = db4.query('FROM contracts WHERE status = "active" VALID AS OF "2025-01-01"')
clients_2025 = {r["client"] for r in rows_2025}
check("ACME expired by 2025, only Globex remains",
    clients_2025 == {"Globex"}, clients_2025)

# ─────────────────────────────────────────────────────────────────────────────
section("Bi-temporal sealed in hash chain")
db5 = NEDB()
db5.put("facts", "f1", {"v": 1}, valid_from="2024-01-01", valid_to="2024-12-31")

op = next(o for o in db5.log.ops if o.op == "put")
check("Op.valid_from sealed",  op.valid_from == "2024-01-01")
check("Op.valid_to sealed",    op.valid_to   == "2024-12-31")
check("chain verifies (valid_from/to in hash)", db5.verify())

# ─────────────────────────────────────────────────────────────────────────────
section("Persistence — valid time survives restart")
tmp = tempfile.mkdtemp()
try:
    db6 = NEDB(tmp)
    db6.put("rates", "r1", {"pct": 3.5}, valid_from="2024-01-01", valid_to="2024-12-31")
    db6.put("rates", "r2", {"pct": 4.0}, valid_from="2025-01-01")
    db6.close()

    db7 = NEDB(tmp)
    check("reopen: verify",          db7.verify())
    rows_2024 = db7.query('FROM rates VALID AS OF "2024-06-01"')
    check("reopen: 2024 rate correct",
        len(rows_2024) == 1 and rows_2024[0]["pct"] == 3.5)
    rows_2025 = db7.query('FROM rates VALID AS OF "2025-06-01"')
    check("reopen: 2025 rate correct",
        len(rows_2025) == 1 and rows_2025[0]["pct"] == 4.0)
    db7.close()
finally:
    shutil.rmtree(tmp, ignore_errors=True)

# ─────────────────────────────────────────────────────────────────────────────
section("Mixed: causal provenance + bi-temporal + time-travel")
db8 = NEDB()

db8.put("inputs", "msg_1", {"text": "rate hike announced"})
s1 = db8.seq

# Belief about a future rate, valid only for Q1 2025
db8.put("beliefs", "rate_q1_2025",
    {"prediction": "rate will reach 5.5%"},
    caused_by=[s1], evidence="inference", confidence=0.7,
    valid_from="2025-01-01", valid_to="2025-03-31")
snap = db8.seq

# Later: the prediction was wrong; update
db8.put("beliefs", "rate_q1_2025",
    {"prediction": "rate stayed at 4.8%"},
    valid_from="2025-01-01", valid_to="2025-03-31")

check("chain verifies (all three features combined)", db8.verify())

# What did we believe AS OF snap, VALID on 2025-02-01?
combo = db8.query(f'FROM beliefs AS OF {snap} VALID AS OF "2025-02-01"')
check("AS OF snap + VALID 2025-02-01: sees old wrong prediction",
    len(combo) == 1 and "5.5%" in combo[0].get("prediction", ""), combo)

# What do we believe NOW, VALID on 2025-02-01?
current = db8.query('FROM beliefs VALID AS OF "2025-02-01"')
check("NOW + VALID 2025-02-01: sees corrected prediction",
    any("4.8%" in r.get("prediction","") for r in current), current)

# ─────────────────────────────────────────────────────────────────────────────
total = PASS + FAIL
print(f"\n  {'═'*52}")
print(f"  Bi-temporal  |  {PASS}/{total} passed{'  ✅' if not FAIL else f'  ❌  {FAIL} FAILED'}")
print(f"  {'═'*52}\n")
sys.exit(1 if FAIL else 0)
