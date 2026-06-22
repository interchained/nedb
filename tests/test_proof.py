#!/usr/bin/env python3
"""
test_proof.py — Merkle inclusion proofs over the NEDB hash chain.

Every NEDB write produces an Op whose ``hash`` is the chain head after that op.
``nedb.verify_proof`` lets a client confirm — offline, without the server — that
a given op-hash was at a specific seq in the log and that all subsequent writes
fold into the claimed head. This test exercises the round trip end-to-end.

Run: python3 tests/test_proof.py
"""
from __future__ import annotations

import hashlib
import os
import shutil
import sys
import tempfile

# Make the in-tree package importable when running this file directly.
sys.path.insert(
    0,
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"),
)

from nedb import NEDB, verify_proof, fold_head  # noqa: E402
from nedb.proof import GENESIS  # noqa: E402


PASS = FAIL = 0


def ok(msg: str) -> None:
    global PASS
    PASS += 1
    print(f"  ✓  {msg}")


def bad(msg: str) -> None:
    global FAIL
    FAIL += 1
    print(f"  ✗  FAIL: {msg}")


def chk(msg: str, cond: bool) -> None:
    ok(msg) if cond else bad(msg)


def banner(title: str) -> None:
    pad = (60 - len(title)) // 2
    print(f"\n  {'-' * pad} {title} {'-' * pad}")


def build_proof(db: NEDB, target_hash: str) -> dict:
    """Construct a proof for the op with the given hash — mirrors the server.

    ``prev_head`` is the value of the MERKLE FOLD at the point just before this
    op (i.e. fold of op-hashes [0..seq-1] starting from GENESIS), NOT the
    engine's per-op ``op.prev_hash``. The two are different commitment schemes
    over the same log — the fold is what the client re-derives.
    """
    ops = db.log.ops
    target = next((o for o in ops if o.hash == target_hash), None)
    assert target is not None, f"no op with hash {target_hash}"
    op_hashes = [o.hash for o in ops]
    prev_head = fold_head(op_hashes[: target.seq], start=GENESIS)
    subsequent = op_hashes[target.seq + 1:]
    derived_head = fold_head(op_hashes, start=GENESIS)
    return {
        "hash":       target.hash,
        "seq":        target.seq,
        "prev_head":  prev_head,
        "subsequent": subsequent,
        "head":       derived_head,
    }


# ─────────────────────────────────────────────────────────────────────────────
print("""
  +==========================================================+
  |          MERKLE INCLUSION PROOF — END-TO-END              |
  |                                                          |
  |  Write 5 docs, prove the middle one is at its seq, then  |
  |  tamper and confirm verification fails closed.           |
  +==========================================================+
""")

# Use a temp-dir database so this exercises the durable path too (the proof
# math is identical for in-memory and on-disk, but durability adds confidence).
tmp = tempfile.mkdtemp(prefix="nedb-proof-")
try:
    banner("SCENE 1 - write five documents")
    db = NEDB(tmp)
    docs = []
    for i, name in enumerate(["Alice", "Bob", "Carol", "Dave", "Eve"]):
        stored = db.put("users", f"u{i}", {"name": name, "age": 20 + i})
        # The Op for this write is the one we just appended — its hash is at
        # the tail of the log right now.
        op = db.log.ops[-1]
        docs.append({"seq": op.seq, "name": name, "hash": op.hash})
        ok(f"put users/u{i} ({name}) at seq={op.seq}, op.hash={op.hash[:12]}...")

    chk("five writes produced five Ops", len(db.log.ops) == 5)
    chk("engine chain verifies",          db.verify())

    # ───────────────────────────────────────────────────────────────────────
    banner("SCENE 2 - request a proof for the MIDDLE write (seq=2 / Carol)")
    target = docs[2]
    chk("middle doc is Carol",  target["name"] == "Carol")
    chk("middle doc is seq=2",  target["seq"] == 2)

    proof = build_proof(db, target["hash"])
    ok(f"proof.hash       = {proof['hash'][:12]}...")
    ok(f"proof.seq        = {proof['seq']}")
    ok(f"proof.prev_head  = {proof['prev_head'][:12]}...")
    ok(f"proof.subsequent = {len(proof['subsequent'])} subsequent op-hashes")
    ok(f"proof.head       = {proof['head'][:12]}... (derived)")

    chk("proof.seq matches target",                       proof["seq"] == 2)
    chk("proof has exactly N - seq - 1 subsequent ops",
        len(proof["subsequent"]) == len(db.log.ops) - target["seq"] - 1)
    # prev_head is the Merkle fold over op-hashes [0..seq-1], NOT the engine's
    # per-op op.prev_hash. The two are different (but parallel) commitment chains.
    expected_prev = fold_head([o.hash for o in db.log.ops[:2]], start=GENESIS)
    chk("proof.prev_head equals fold-of-prior-op-hashes",
        proof["prev_head"] == expected_prev)

    # ───────────────────────────────────────────────────────────────────────
    banner("SCENE 3 - verify the proof client-side (no server)")
    chk("verify_proof(proof) returns True",  verify_proof(proof) is True)

    # Re-derive the head manually using the exact spec the docstring promises,
    # to prove verify_proof isn't just doing a no-op equality check.
    manual_head = hashlib.blake2b(
        bytes.fromhex(proof["prev_head"]) + bytes.fromhex(proof["hash"]),
        digest_size=32,
    ).hexdigest()
    for sub in proof["subsequent"]:
        manual_head = hashlib.blake2b(
            bytes.fromhex(manual_head) + bytes.fromhex(sub),
            digest_size=32,
        ).hexdigest()
    chk("manual blake2b fold reproduces proof.head", manual_head == proof["head"])

    # ───────────────────────────────────────────────────────────────────────
    banner("SCENE 4 - the proof holds against the database's full chain")
    full_head = fold_head([o.hash for o in db.log.ops], start=GENESIS)
    chk("server-side fold over the entire log == proof.head",
        full_head == proof["head"])
    # Sanity: folding the FIRST op alone from genesis should match an
    # equivalent partial-proof for seq=0 (prev_head = GENESIS, no subsequent).
    first_proof = build_proof(db, db.log.ops[0].hash)
    chk("seq=0 proof verifies (prev_head == GENESIS)",
        first_proof["prev_head"] == GENESIS and verify_proof(first_proof))

    # ───────────────────────────────────────────────────────────────────────
    banner("SCENE 5 - tamper detection")
    # Flip one nibble of `hash` — the rest of the proof is untouched, so this is
    # the cleanest possible test of cryptographic binding.
    tampered = dict(proof)
    h = list(tampered["hash"])
    h[0] = "0" if h[0] != "0" else "1"
    tampered["hash"] = "".join(h)
    chk("verify_proof rejects flipped hash",       verify_proof(tampered) is False)

    # Flip one byte of subsequent
    if proof["subsequent"]:
        tampered2 = dict(proof)
        sub = list(tampered2["subsequent"])
        first = list(sub[0])
        first[0] = "0" if first[0] != "0" else "1"
        sub[0] = "".join(first)
        tampered2["subsequent"] = sub
        chk("verify_proof rejects flipped subsequent[0]",
            verify_proof(tampered2) is False)

    # Reorder subsequent — same hashes, different order → must fail
    if len(proof["subsequent"]) >= 2:
        tampered3 = dict(proof)
        tampered3["subsequent"] = list(reversed(proof["subsequent"]))
        chk("verify_proof rejects reordered subsequent",
            verify_proof(tampered3) is False)

    # Wrong seq number alone doesn't break the math (seq isn't hashed in the
    # fold), but a malformed prev_head does.
    tampered4 = dict(proof)
    tampered4["prev_head"] = "f" * 64
    chk("verify_proof rejects wrong prev_head",    verify_proof(tampered4) is False)

    # Malformed input rejected gracefully (no exception, just False).
    chk("verify_proof rejects non-hex hash",       verify_proof({**proof, "hash": "ZZ" * 32}) is False)
    chk("verify_proof rejects too-short hash",     verify_proof({**proof, "hash": "ab"}) is False)
    chk("verify_proof rejects missing key",        verify_proof({k: v for k, v in proof.items() if k != "head"}) is False)
    chk("verify_proof rejects non-dict input",     verify_proof("not a proof") is False)

    # ───────────────────────────────────────────────────────────────────────
    banner("SCENE 6 - integration with engine restart")
    db.close()
    db2 = NEDB(tmp)
    chk("chain verifies after reload",                       db2.verify())
    # Recompute the proof against the reloaded engine — must produce the same fold.
    target_hash_after = db2.log.ops[2].hash
    chk("op.hash for seq=2 survives the reload",
        target_hash_after == target["hash"])
    proof_after = build_proof(db2, target_hash_after)
    chk("reloaded proof.head matches original",
        proof_after["head"] == proof["head"])
    chk("reloaded proof verifies",                           verify_proof(proof_after))
    db2.close()
finally:
    shutil.rmtree(tmp, ignore_errors=True)


# ─────────────────────────────────────────────────────────────────────────────
print()
print(f"  {'-' * 60}")
print(f"  {PASS} passed, {FAIL} failed")
print(f"  {'-' * 60}")
sys.exit(0 if FAIL == 0 else 1)
