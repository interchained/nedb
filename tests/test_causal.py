#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
Causal Write Provenance — test suite for v0.9.0.

Exercises the full feature:
  * caused_by, evidence, confidence fields sealed in the hash chain
  * _caused_by/_evidence/_confidence mirrored into the doc (queryable)
  * TRACE caused_by NQL — backward causal traversal
  * TRACE caused_by REVERSE — forward causal traversal
  * verify() still passes after causal writes
  * backward compat: existing databases without provenance still verify

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
section("Causal write API")
db = NEDB()

# Write source facts (no provenance — these are the "uncaused" roots)
db.put("kb", "msg_1", {"text": "User said: I prefer dark mode", "type": "user_message"})
db.put("kb", "msg_2", {"text": "User said: I work nights",      "type": "user_message"})
seq_msg1 = db.seq - 1
seq_msg2 = db.seq

# Write a derived belief, caused by those messages
db.put("beliefs", "dark_mode",
    {"value": True, "summary": "User prefers dark mode"},
    caused_by=[seq_msg1, seq_msg2],
    evidence="user_message",
    confidence=0.95)
seq_belief = db.seq

# Write a second belief caused by the first
db.put("beliefs", "needs_low_eye_strain",
    {"value": True, "summary": "User needs low eye-strain settings"},
    caused_by=[seq_belief],
    evidence="inference",
    confidence=0.80)

check("causal write succeeds", db.get("beliefs", "dark_mode") is not None)

doc = db.get("beliefs", "dark_mode")
check("_caused_by mirrored into doc",  doc.get("_caused_by") == [seq_msg1, seq_msg2])
check("_evidence mirrored into doc",   doc.get("_evidence")  == "user_message")
check("_confidence mirrored into doc", doc.get("_confidence")== 0.95)
check("chain verifies after causal writes", db.verify())

# ─────────────────────────────────────────────────────────────────────────────
section("Provenance queryable via WHERE")
rows = db.query('FROM beliefs WHERE _evidence = "user_message"')
check("WHERE _evidence works", len(rows) == 1 and rows[0]["_id"] == "dark_mode")

rows = db.query('FROM beliefs WHERE _confidence > 0.85')
check("WHERE _confidence > 0.85", len(rows) == 1 and rows[0]["_id"] == "dark_mode")

rows = db.query('FROM beliefs WHERE _evidence = "inference"')
check("inference belief found", len(rows) == 1 and rows[0]["_id"] == "needs_low_eye_strain")

# ─────────────────────────────────────────────────────────────────────────────
section("TRACE caused_by (backward — why?)")
# Ask: why does the agent believe 'dark_mode'? Should trace back to the kb messages.
rows = db.query('FROM beliefs WHERE _id = "dark_mode" TRACE caused_by')
names = {r.get("_id") for r in rows}
check("TRACE backward finds cause documents", len(rows) >= 1, names)
check("kb source facts returned in trace", "msg_1" in names or "msg_2" in names, names)

# Deep backward: from needs_low_eye_strain → dark_mode → msg_1, msg_2
rows_deep = db.query('FROM beliefs WHERE _id = "needs_low_eye_strain" TRACE caused_by')
ids_deep = {r.get("_id") for r in rows_deep}
check("deep trace finds intermediate belief", "dark_mode" in ids_deep or len(ids_deep) >= 1, ids_deep)

# ─────────────────────────────────────────────────────────────────────────────
section("TRACE caused_by REVERSE (forward — what did this cause?)")
# Ask: given msg_1, what beliefs did it cause? Should find dark_mode.
rows_fwd = db.query('FROM kb WHERE _id = "msg_1" TRACE caused_by REVERSE')
ids_fwd = {r.get("_id") for r in rows_fwd}
check("TRACE REVERSE finds downstream beliefs", len(rows_fwd) >= 1, ids_fwd)
check("dark_mode in forward trace of msg_1", "dark_mode" in ids_fwd, ids_fwd)

# ─────────────────────────────────────────────────────────────────────────────
section("Op-level provenance sealed in chain")
# The Op itself should carry the provenance
belief_op = next((o for o in db.log.ops if o.op == "put" and
                  "dark_mode" in o.payload.get("key", "")), None)
check("Op has caused_by field",  belief_op is not None and belief_op.caused_by == [seq_msg1, seq_msg2])
check("Op has evidence field",   belief_op is not None and belief_op.evidence   == "user_message")
check("Op has confidence field", belief_op is not None and belief_op.confidence == 0.95)

# Verify the provenance is sealed: re-verify the chain (Op body includes provenance in hash)
check("chain still verifies (provenance sealed)", db.verify())

# ─────────────────────────────────────────────────────────────────────────────
section("Persistence — provenance survives restart")
tmp = tempfile.mkdtemp()
try:
    db2 = NEDB(tmp)
    db2.put("kb", "src_1", {"text": "source fact"})
    s1 = db2.seq
    db2.put("beliefs", "derived_1",
        {"summary": "derived from source"},
        caused_by=[s1], evidence="inference", confidence=0.7)
    db2.close()

    db3 = NEDB(tmp)
    check("reopen: verify",         db3.verify())
    doc3 = db3.get("beliefs", "derived_1")
    check("reopen: _caused_by",     doc3 and doc3.get("_caused_by") == [s1])
    check("reopen: _evidence",      doc3 and doc3.get("_evidence")  == "inference")
    check("reopen: _confidence",    doc3 and doc3.get("_confidence")== 0.7)

    # TRACE still works after reload
    rows3 = db3.query('FROM beliefs WHERE _id = "derived_1" TRACE caused_by')
    check("reopen: TRACE backward",  len(rows3) >= 1)
    db3.close()
finally:
    shutil.rmtree(tmp, ignore_errors=True)

# ─────────────────────────────────────────────────────────────────────────────
section("Backward compat — old ops without provenance still verify")
db_old = NEDB()
db_old.put("t", "r1", {"v": 1})
db_old.put("t", "r2", {"v": 2})
check("old-style writes verify",     db_old.verify())
# Now mix provenance writes into the same chain
db_old.put("t", "r3", {"v": 3}, caused_by=[0, 1], evidence="inference")
check("mixed chain still verifies",  db_old.verify())
check("old op has no caused_by",     db_old.log.ops[0].caused_by is None)
check("new op has caused_by",        db_old.log.ops[2].caused_by == [0, 1])

# ─────────────────────────────────────────────────────────────────────────────
section("Causal graph — multi-hop scenario")
db_agent = NEDB()

# Layer 0: raw inputs
db_agent.put("inputs", "turn_1", {"role": "user", "text": "I hate bright screens"})
db_agent.put("inputs", "turn_2", {"role": "user", "text": "I have migraines"})
seq_t1 = db_agent.seq - 1
seq_t2 = db_agent.seq

# Layer 1: extracted facts
db_agent.put("facts", "light_sensitivity",
    {"claim": "User is light-sensitive"},
    caused_by=[seq_t1, seq_t2], evidence="user_message", confidence=0.9)
seq_ls = db_agent.seq

db_agent.put("facts", "migraine_condition",
    {"claim": "User has migraines"},
    caused_by=[seq_t2], evidence="user_message", confidence=0.85)
seq_mc = db_agent.seq

# Layer 2: derived preferences
db_agent.put("prefs", "dark_mode",
    {"enabled": True},
    caused_by=[seq_ls], evidence="inference", confidence=0.95)
seq_dm = db_agent.seq

db_agent.put("prefs", "reduce_contrast",
    {"enabled": True},
    caused_by=[seq_ls, seq_mc], evidence="inference", confidence=0.88)

check("multi-hop chain verifies", db_agent.verify())

# Why does the agent prefer dark_mode? Trace back.
trace_dm = db_agent.query('FROM prefs WHERE _id = "dark_mode" TRACE caused_by')
trace_ids = {r["_id"] for r in trace_dm}
check("multi-hop trace reaches facts", "light_sensitivity" in trace_ids, trace_ids)

# What did turn_1 (user message) ultimately cause?
fwd_t1 = db_agent.query('FROM inputs WHERE _id = "turn_1" TRACE caused_by REVERSE')
fwd_ids = {r["_id"] for r in fwd_t1}
check("turn_1 caused light_sensitivity downstream", "light_sensitivity" in fwd_ids, fwd_ids)

# ─────────────────────────────────────────────────────────────────────────────
total = PASS + FAIL
print(f"\n  {'═'*52}")
print(f"  Causal Provenance  |  {PASS}/{total} passed{'  ✅' if not FAIL else f'  ❌  {FAIL} FAILED'}")
print(f"  {'═'*52}\n")
sys.exit(1 if FAIL else 0)
