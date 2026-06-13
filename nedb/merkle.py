"""
nedb.merkle — Merkle tree over content-addressed chunk hashes.

Because every file version is a list of BLAKE-addressed chunks, a file version has
a Merkle root that commits to its exact bytes. Any chunk's membership is provable in
O(log n), and the root can be anchored on-chain (e.g. ITC) for tamper-evident,
notarized version history.
"""
from __future__ import annotations

import hashlib
from typing import List, Tuple


def _h(b: bytes) -> bytes:
    return hashlib.blake2b(b, digest_size=32).digest()


def _to_bytes(x) -> bytes:
    return bytes.fromhex(x) if isinstance(x, str) else bytes(x)


def merkle_root(leaves: List[str]) -> str:
    if not leaves:
        return "0" * 64
    level = [_to_bytes(x) for x in leaves]
    while len(level) > 1:
        nxt = []
        for i in range(0, len(level), 2):
            a = level[i]
            b = level[i + 1] if i + 1 < len(level) else level[i]
            nxt.append(_h(a + b))
        level = nxt
    return level[0].hex()


def merkle_proof(leaves: List[str], idx: int) -> List[Tuple[str, str]]:
    """Return inclusion proof for leaf at idx: list of (sibling_hex, side)."""
    level = [_to_bytes(x) for x in leaves]
    path: List[Tuple[str, str]] = []
    while len(level) > 1:
        if idx % 2 == 0:
            sib = level[idx + 1] if idx + 1 < len(level) else level[idx]
            path.append((sib.hex(), "R"))
        else:
            path.append((level[idx - 1].hex(), "L"))
        nxt = []
        for i in range(0, len(level), 2):
            a = level[i]
            b = level[i + 1] if i + 1 < len(level) else level[i]
            nxt.append(_h(a + b))
        level = nxt
        idx //= 2
    return path


def merkle_verify(leaf: str, path: List[Tuple[str, str]], root: str) -> bool:
    h = _to_bytes(leaf)
    for sib_hex, side in path:
        sib = _to_bytes(sib_hex)
        h = _h(h + sib) if side == "R" else _h(sib + h)
    return h.hex() == root
