#!/usr/bin/env python3
"""
test_the_will.py — The Promise That Cannot Be Broken

A father records his true wishes in NEDB before he passes.
Years later a dispute arises — someone claims the will was altered.
The BLAKE2b chain proves it wasn't. TRACE shows every amendment,
every witness, in exact order. The hash is the proof.

NEDB is the system that kept the promise intact
when no one else could.

Features demonstrated:
  ✦ Durable DAG (writes to disk, survives process restart)
  ✦ Content-addressed objects (each fact has a cryptographic identity)
  ✦ Causal provenance (amendments caused_by the original)
  ✦ Time-travel (see the will as it stood on any date)
  ✦ TRAVERSE (family relations)
  ✦ verify() (tamper-evident proof — nothing was changed)
  ✦ The AHA: close the DB, reopen, chain is intact

Run: pip install nedb-engine && python3 tests/test_the_will.py

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
import json, sys, shutil, tempfile, time
from pathlib import Path

try:
    from nedb._native import NedbCore
    import nedb as _nedb
except ImportError:
    print("  SKIP  nedb._native not available (universal wheel — no platform extension)")
    print("        pip install --force-reinstall --no-cache-dir nedb-engine")
    print("        on a platform with a native wheel (Linux x86_64, macOS, Windows x64)")
    sys.exit(0)

PASS = FAIL = 0
def ok(msg):  global PASS; PASS += 1; print(f"  ✓  {msg}")
def bad(msg): global FAIL; FAIL += 1; print(f"  ✗  FAIL: {msg}")
def chk(msg, cond): ok(msg) if cond else bad(msg)
def J(s): return json.loads(s) if s else {}

DATA_DIR = Path(tempfile.mkdtemp(prefix="nedb_will_"))

print(f"""
  ╔══════════════════════════════════════════════════════════╗
  ║               THE PROMISE THAT CANNOT BE BROKEN          ║
  ║                                                          ║
  ║  A father's true wishes.                                 ║
  ║  Sealed in a hash chain.                                 ║
  ║  Untouched across time.                                  ║
  ╚══════════════════════════════════════════════════════════╝

  nedb-engine {_nedb.__version__}  ·  durable DAG  ·  {DATA_DIR}
""")

# ─────────────────────────────────────────────────────────────────────────────
print("  ── CHAPTER 1 — A father records his wishes  [2019-03-14] ──\n")
print("  Robert Evans opens NEDB and writes his will.\n"
      "  Every fact gets a cryptographic identity.\n"
      "  Nobody can alter it without breaking the chain.\n")
# ─────────────────────────────────────────────────────────────────────────────

db = NedbCore.open(str(DATA_DIR))

# The family — each person is an immutable content-addressed object
r_robert = J(db.put("person", "robert", json.dumps({
    "name": "Robert Allen Evans", "role": "testator",
    "dob":  "1948-06-12",         "signed": "2019-03-14",
})))
hash_robert = r_robert["_hash"]
seq_robert  = r_robert["_seq"]

r_mark = J(db.put("person", "mark", json.dumps({
    "name": "Mark Allen Evans Jr.", "role": "heir", "relation": "son",
})))
hash_mark = r_mark["_hash"]

r_lisa = J(db.put("person", "lisa", json.dumps({
    "name": "Lisa Evans", "role": "heir", "relation": "daughter",
})))
hash_lisa = r_lisa["_hash"]

chk(f"Robert's identity sealed — hash: {hash_robert[:16]}…", len(hash_robert) == 64)
chk(f"Mark's identity sealed  — hash: {hash_mark[:16]}…",   len(hash_mark)   == 64)

# Original will — caused_by lives INSIDE the doc (v2 native format)
r_will_v1 = J(db.put("will", "evans_will_2019", json.dumps({
    "testator":     "robert",
    "date":         "2019-03-14",
    "version":      1,
    "house":        "mark",
    "business":     "lisa",
    "estate_split": "50/50",
    "note":         "These are my true wishes, recorded this day.",
    "caused_by":    [hash_robert],   # caused by Robert's testator record
})))
hash_will_v1 = r_will_v1["_hash"]
seq_will_v1  = r_will_v1["_seq"]

chk(f"Original will sealed  — hash: {hash_will_v1[:16]}…", len(hash_will_v1) == 64)
print(f"\n    Hash: {hash_will_v1}")
print(f"    Change one character — hash changes — chain breaks — verify() fails.\n")

# Witnesses — caused_by the will they witnessed
r_atty = J(db.put("witness", "attorney_chen", json.dumps({
    "name": "Chen & Associates", "type": "attorney",
    "witnessed": "2019-03-14",
    "caused_by": [hash_will_v1],
})))
hash_atty = r_atty["_hash"]

r_notary = J(db.put("witness", "notary_williams", json.dumps({
    "name": "Williams Notary", "type": "notary",
    "witnessed": "2019-03-14", "seal": "CA-2019-0031",
    "caused_by": [hash_will_v1],
})))
hash_notary = r_notary["_hash"]

chk("Attorney witness sealed in chain",  len(hash_atty)   == 64)
chk("Notary seal sealed in chain",       len(hash_notary) == 64)

# Family graph via TRAVERSE
db.link("person:robert", "parent_of", "person:mark")
db.link("person:robert", "parent_of", "person:lisa")
family = db.neighbors("person:robert", "parent_of")
chk("Family graph: Robert → [Mark, Lisa]", len(family) == 2)

# ─────────────────────────────────────────────────────────────────────────────
print("\n  ── CHAPTER 2 — An amendment  [2021-08-30] ──\n")
print("  Two years later Robert adds a codicil:\n"
      "  Mark inherits the house AND the vintage car.\n"
      "  The amendment chains off the original — history extends.\n")
# ─────────────────────────────────────────────────────────────────────────────

r_will_v2 = J(db.put("will", "evans_will_2019", json.dumps({  # same id → new version
    "testator":     "robert",
    "date":         "2021-08-30",
    "version":      2,
    "house":        "mark",
    "business":     "lisa",
    "vintage_car":  "mark",        # ← the 1967 Mustang
    "estate_split": "50/50",
    "note":         "Amendment: the 1967 Mustang goes to Mark. My choice.",
    "caused_by":    [hash_will_v1],  # caused by the original
})))
hash_will_v2 = r_will_v2["_hash"]
seq_will_v2  = r_will_v2["_seq"]

chk(f"Amendment sealed — hash: {hash_will_v2[:16]}…",  len(hash_will_v2) == 64)
chk("Amendment has different hash from original",       hash_will_v2 != hash_will_v1)
print(f"\n    Original: {hash_will_v1[:32]}…")
print(f"    Amendment:{hash_will_v2[:32]}…")
print(f"    Both exist forever. Neither can be erased.\n")

# ─────────────────────────────────────────────────────────────────────────────
print("  ── CHAPTER 3 — Robert passes  [2024-11-02] ──\n")
# ─────────────────────────────────────────────────────────────────────────────

r_passing = J(db.put("event", "robert_passing", json.dumps({
    "type": "death", "person": "robert", "date": "2024-11-02",
    "note": "Robert Allen Evans, 76, passed peacefully at home.",
    "caused_by": [hash_robert],
})))
hash_passing = r_passing["_hash"]

r_probate = J(db.put("event", "probate_filing", json.dumps({
    "type":      "probate",
    "filed_by":  "attorney_chen",
    "will_hash": hash_will_v2,   # attorney cites the exact object hash
    "date":      "2024-11-15",
    "caused_by": [hash_passing, hash_will_v2, hash_atty],
})))

head_before_close = db.head()
seq_before_close  = db.seq()

chk("Passing recorded with causal link", len(hash_passing) == 64)
print(f"\n    Chain head at probate: {head_before_close[:32]}…")
print(f"    Total facts sealed:    {seq_before_close}\n")
print("  ── The database closes. A dispute arises. ──\n")
time.sleep(0.5)

del db  # process ends — data is on disk

# ─────────────────────────────────────────────────────────────────────────────
print("  ── CHAPTER 4 — The dispute  [reopening the database] ──\n")
print("  A distant relative claims the Mustang bequest never existed.")
print("  The attorney reopens NEDB.\n")
# ─────────────────────────────────────────────────────────────────────────────

db2 = NedbCore.open(str(DATA_DIR))
time.sleep(0.2)  # let background scan complete for small DB

verified  = db2.verify()
head_now  = db2.head()
seq_now   = db2.seq()

print(f"  verify() → {verified}")
print(f"  Chain head: {head_now[:32]}…\n")

chk("verify() — every object in the DAG is intact",  verified)
chk("Database reopened with correct seq",             seq_now == seq_before_close)

# ─────────────────────────────────────────────────────────────────────────────
print("\n  ── CHAPTER 5 — The proof ──\n")
print("  TRACE from the probate filing backward.\n")
# ─────────────────────────────────────────────────────────────────────────────

trace_rows = db2.query('FROM event WHERE _id = "probate_filing" TRACE caused_by')
trace = [J(r) for r in trace_rows]

print(f"  Causal chain ({len(trace)} ancestors):\n")
labels = {
    "robert_passing":   "DEATH RECORD     — Robert Evans, 2024-11-02",
    "evans_will_2019":  "WILL             — v{v} dated {d}",
    "attorney_chen":    "WITNESS          — Chen & Associates (attorney)",
    "notary_williams":  "WITNESS          — Williams Notary, seal CA-2019-0031",
    "robert":           "TESTATOR RECORD  — Robert Allen Evans, b.1948",
}
for node in sorted(trace, key=lambda x: x.get("_seq", 0)):
    nid   = node.get("_id", "?")
    label = labels.get(nid, f"record: {nid}")
    if "{v}" in label:
        label = label.format(v=node.get("version","?"), d=node.get("date","?"))
    print(f"  [{node.get('_seq',0):>3}]  {label}")

chk("TRACE reaches original will",          any(J(r).get("_id") == "evans_will_2019"  for r in trace_rows))
chk("TRACE reaches Robert's testator record", any(J(r).get("_id") == "robert"          for r in trace_rows))
chk("Attorney witness in causal chain",     any(J(r).get("_id") == "attorney_chen"   for r in trace_rows))

# Time-travel: what did the will say BEFORE the amendment?
original = J(db2.get("will", "evans_will_2019", as_of=seq_will_v1))
current  = J(db2.get("will", "evans_will_2019"))

print(f"\n  Time-travel to 2019-03-14 (before amendment):")
print(f"    house={original.get('house')}  business={original.get('business')}"
      f"  vintage_car={original.get('vintage_car', '(none)')}\n")
print(f"  Current will (v{current.get('version')} — {current.get('date')}):")
print(f"    house={current.get('house')}  business={current.get('business')}"
      f"  vintage_car={current.get('vintage_car')}\n")

chk("Time-travel: 2019 will had NO vintage_car bequest",
    original.get("vintage_car") is None)
chk("Current will confirms Mustang → Mark",
    current.get("vintage_car") == "mark")
chk("Amendment is version 2",  current.get("version") == 2)
chk("Original was version 1",  original.get("version") == 1)

# ─────────────────────────────────────────────────────────────────────────────
total = PASS + FAIL
status = "✅" if not FAIL else f"❌  {FAIL} FAILED"
print(f"""
  ── CHAPTER 6 — The verdict ──

  The claim is dismissed.

  The 1967 Mustang bequest was recorded on 2021-08-30,
  caused by the original 2019 will, witnessed by Chen &
  Associates and Williams Notary. The BLAKE2b chain is
  unbroken. verify() → {verified}. The hash is the proof.

  Robert's promise kept. Mark gets the Mustang.

  ══════════════════════════════════════════════════════════
  {PASS}/{total} checks passed {status}

  "Every fact in this system has a cryptographic identity.
   Every cause is linked to its effect.
   Nothing can be changed without breaking the chain.
   That is what NEDB is."

      — INTERCHAINED LLC × Claude Sonnet 4.6
  ══════════════════════════════════════════════════════════
""")
shutil.rmtree(DATA_DIR, ignore_errors=True)
sys.exit(1 if FAIL else 0)
