"""
nedb.cascade — the Cascade compression pipeline + content-addressed blob store.

This is what makes NEDB double as a git-style file manager with maximum compression
WITHOUT inventing a new entropy coder. The novelty is the pipeline composition:

  1. Content-defined chunking (Gear rolling hash) — boundaries follow content, so a
     one-byte insert only changes the chunk(s) around it, not everything after it.
  2. Content-addressed dedup (BLAKE) — identical chunks across all files and all
     versions are stored exactly once.
  3. Temperature tiers — warm data uses a fast codec (zstd in prod; zlib in this
     reference), cold/archival history uses a maximum-ratio codec (LZMA).

The production pipeline adds similarity-picked binary deltas (zstd --patch-from) and
schema-aware columnar transforms before the entropy stage; both are documented in
docs/SPEC.md and stubbed for the reference engine.
"""
from __future__ import annotations

import hashlib
import lzma
import random
import zlib
from typing import Dict, List

from .merkle import merkle_root

# --- Gear-hash content-defined chunking -------------------------------------
_MASK = (1 << 13) - 1            # ~8 KiB average chunk
_MIN = 2 * 1024
_MAX = 64 * 1024
_M64 = 0xFFFFFFFFFFFFFFFF
_GEAR = [random.Random(0x12345678 + i).getrandbits(64) for i in range(256)]


def chunk(data: bytes) -> List[bytes]:
    chunks: List[bytes] = []
    n = len(data)
    i = 0
    while i < n:
        limit = min(i + _MAX, n)
        h = 0
        pos = i
        cut = limit
        while pos < limit:
            h = ((h << 1) + _GEAR[data[pos]]) & _M64
            pos += 1
            if (pos - i) >= _MIN and (h & _MASK) == 0:
                cut = pos
                break
        chunks.append(data[i:cut])
        i = cut
    return chunks


def _blake(b: bytes) -> str:
    return hashlib.blake2b(b, digest_size=32).hexdigest()


# --- temperature tiers ------------------------------------------------------
def warm_compress(b: bytes) -> bytes:    # zstd stand-in in the reference
    return zlib.compress(b, 6)


def warm_decompress(b: bytes) -> bytes:
    return zlib.decompress(b)


def cold_compress(b: bytes) -> bytes:    # real LZMA — the maximum-ratio archival tier
    return lzma.compress(b, preset=9 | lzma.PRESET_EXTREME)


def cold_decompress(b: bytes) -> bytes:
    return lzma.decompress(b)


class BlobStore:
    """Content-addressed, deduplicated, tiered blob store with versioned files."""

    def __init__(self, tier: str = "warm") -> None:
        self.tier = tier
        self.chunks: Dict[str, bytes] = {}                      # hash -> compressed bytes
        self.files: Dict[str, Dict[str, list]] = {}            # name -> {versions, roots}
        self.logical_bytes = 0
        self.dedup_hits = 0

    def _compress(self, b: bytes) -> bytes:
        return cold_compress(b) if self.tier == "cold" else warm_compress(b)

    def _decompress(self, b: bytes) -> bytes:
        return cold_decompress(b) if self.tier == "cold" else warm_decompress(b)

    def put_file(self, name: str, data: bytes) -> int:
        recipe: List[str] = []
        for c in chunk(data):
            hh = _blake(c)
            recipe.append(hh)
            if hh in self.chunks:
                self.dedup_hits += 1
            else:
                self.chunks[hh] = self._compress(c)
        self.logical_bytes += len(data)
        f = self.files.setdefault(name, {"versions": [], "roots": []})
        f["versions"].append(recipe)
        f["roots"].append(merkle_root(recipe))
        return len(f["versions"]) - 1

    def get_file(self, name: str, version: int = -1) -> bytes:
        recipe = self.files[name]["versions"][version]
        out = bytearray()
        for hh in recipe:
            out += self._decompress(self.chunks[hh])
        return bytes(out)

    def root(self, name: str, version: int = -1) -> str:
        return self.files[name]["roots"][version]

    def stored_bytes(self) -> int:
        return sum(len(v) for v in self.chunks.values())

    def stats(self) -> dict:
        stored = self.stored_bytes()
        return {
            "tier": self.tier,
            "unique_chunks": len(self.chunks),
            "dedup_hits": self.dedup_hits,
            "logical_bytes": self.logical_bytes,
            "stored_bytes": stored,
            "ratio": round(self.logical_bytes / stored, 2) if stored else 0.0,
        }
