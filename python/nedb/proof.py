"""
nedb.proof — Merkle inclusion proofs for the hash-chained op log.

Every NEDB write produces an Op whose ``hash`` field is the chain head after that op
(``op.hash = blake2b(prev_head.utf8 + canon(body))`` — see ``nedb.log``). The chain
head therefore commits cryptographically to the entire write history.

This module derives a *parallel*, compact Merkle commitment chain over the same
op-hash sequence so a client can verify — offline, with no server round-trip and
without re-running the engine — that a specific op-hash was at a specific seq in
the log, and that the log's head followed from chaining all subsequent op-hashes
on top of it.

The chain step used by both ``verify_proof`` (client) and the server endpoint is::

    head_next = blake2b(bytes.fromhex(head_prev) || bytes.fromhex(op_hash)).hexdigest()

starting from the genesis head (the all-zero 32-byte string). Folding over the
full ordered list of op-hashes yields a deterministic head; tamper with any op
hash (or reorder ops) and the fold no longer matches. The Op's own ``hash`` field
already commits to its body (payload, seq, nonce, ts, …) under the engine's chain
formula, so this fold is a complete commitment to the write history through the
op-hashes alone — exactly the property a Merkle inclusion proof needs.

The fold's terminal value (``proof["head"]``) is a *derived* head, distinct from
``db.head`` (the engine's UTF-8-hex chain) but a deterministic function of the
same log. The server computes the derived head over the live log when issuing
each proof, so any divergence between client fold and server fold means the
proof was tampered with in transit.
"""
from __future__ import annotations

import hashlib
from typing import Dict, List

# Genesis head matches the engine: all-zero 32-byte string in hex.
GENESIS = "0" * 64


def _blake(data: bytes) -> str:
    """BLAKE2b-256 over raw bytes, returned as hex — same primitive the engine uses."""
    return hashlib.blake2b(data, digest_size=32).hexdigest()


def _chain_step(head_hex: str, op_hash_hex: str) -> str:
    """One Merkle-chain step on raw bytes (decoded from hex).

    Mirrors the user-facing spec exactly:
        new_head = blake2b(prev_head_bytes || op_hash_bytes)
    """
    return _blake(bytes.fromhex(head_hex) + bytes.fromhex(op_hash_hex))


def fold_head(op_hashes: List[str], start: str = GENESIS) -> str:
    """Fold an ordered list of op-hashes into a Merkle-chain head.

    Used by the server to produce ``proof["head"]`` for any given log state and
    by tests to sanity-check that the fold is order-sensitive and deterministic.
    """
    head = start
    for h in op_hashes:
        head = _chain_step(head, h)
    return head


def verify_proof(proof: Dict) -> bool:
    """Verify a Merkle inclusion proof client-side — no server, no engine needed.

    ``proof`` shape::

        {
            "hash":       "<64-hex>",   # the document op's content hash (op.hash)
            "seq":        N,            # sequence number of this op in the log
            "prev_head":  "<64-hex>",   # chain head BEFORE this op (= op.prev_hash)
            "subsequent": ["<64-hex>"], # op-hashes of every op AFTER this one
            "head":       "<64-hex>",   # claimed final (derived) chain head
        }

    Verification (matches the spec):
      1. ``head_at_seq = blake2b(prev_head_bytes || hash_bytes)``
      2. For each ``sub_hash`` in subsequent:
            ``head = blake2b(head_bytes || sub_hash_bytes)``
      3. ``final head == proof["head"]``

    Returns True iff every step is internally consistent. Tampering with any
    field — flipping a byte of ``hash``, reordering ``subsequent``, swapping
    ``prev_head``, etc. — will flip the fold and make this return False.
    """
    if not isinstance(proof, dict):
        return False

    # Required keys with the right shapes.
    required = ("hash", "seq", "prev_head", "subsequent", "head")
    if not all(k in proof for k in required):
        return False
    if not isinstance(proof["seq"], int) or proof["seq"] < 0:
        return False
    if not isinstance(proof["subsequent"], list):
        return False

    # Hex shape check on the 64-char fields. Anything malformed → not verified.
    def _is_hex32(s: object) -> bool:
        if not isinstance(s, str) or len(s) != 64:
            return False
        try:
            bytes.fromhex(s)
        except ValueError:
            return False
        return True

    if not _is_hex32(proof["hash"]):       return False
    if not _is_hex32(proof["prev_head"]):  return False
    if not _is_hex32(proof["head"]):       return False
    for s in proof["subsequent"]:
        if not _is_hex32(s):
            return False

    # 1. Re-derive the chain head AT this op's seq from prev_head + this op's hash.
    head = _chain_step(proof["prev_head"], proof["hash"])

    # 2. Fold each subsequent op-hash on top to advance the chain to the tail.
    for sub in proof["subsequent"]:
        head = _chain_step(head, sub)

    # 3. The fold must equal the claimed final head bit-for-bit.
    return head == proof["head"]
