# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.log — the append-only, hash-chained, nonce-enforced, idempotent operation log.

This is the single source of truth for NEDB. Every mutation in the database is an
Op appended here. Three guarantees live in this one structure:

  * Replay protection  — each client has a strictly-monotonic nonce; an op whose
                         nonce is <= the client's last seen nonce is rejected.
  * Idempotency        — an op carrying an idempotency key that was already applied
                         returns the original result and is NOT appended again.
  * Tamper evidence    — ops are chained by hash (h_n = H(h_{n-1} || op_n)), so the
                         whole history is a verifiable chain and the head hash is a
                         commitment to the entire log (anchorable on a blockchain).

The same log is the substrate for MVCC snapshot isolation, crash recovery, and
time-travel reads: every Op has a monotonic `seq`, and state "AS OF seq N" is just
the log truncated at N.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

GENESIS = "0" * 64


def canon(obj: Any) -> bytes:
    """Deterministic canonical encoding for hashing."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode()


def blake(data: bytes) -> str:
    # Reference uses BLAKE2b (stdlib). The production Rust core uses BLAKE3
    # (faster, natively tree-structured for the Merkle history).
    return hashlib.blake2b(data, digest_size=32).hexdigest()


class ReplayError(Exception):
    """Raised when an op is replayed with a stale/duplicate nonce."""


@dataclass
class Op:
    seq: int
    client: str
    nonce: int
    op: str  # put | delete | link | unlink | put_file
    payload: dict
    ts: float
    idem: Optional[str]
    prev_hash: str
    hash: str
    # ── Causal provenance (v0.9.0+) ─────────────────────────────────────────
    # Optional fields that, when present, are sealed inside the hash chain so
    # they are tamper-evident and time-stamped at write time.
    #   caused_by  — seqs of the ops that led to this write (backward trace).
    #   evidence   — source type: "user_message" | "inference" | "tool_result"
    #                             | "correction" | "external"
    #   confidence — agent's certainty in this write (0.0 – 1.0).
    caused_by:  Optional[List[int]] = None
    evidence:   Optional[str]       = None
    confidence: Optional[float]     = None

    # ── Bi-temporal valid time (v1.0.0+) ─────────────────────────────────────
    # When was this fact TRUE IN THE WORLD (independent of when it was written)?
    #   valid_from — ISO 8601 date/datetime string; None = "from the beginning"
    #   valid_to   — ISO 8601 date/datetime string; None = "still valid / open-ended"
    #
    # ISO 8601 strings sort lexicographically correctly, so comparisons are
    # safe as plain string ops:  "2024-01-01" < "2024-06-15" ✓
    #
    # Backward-compatible: ops without valid-time fields are treated as always
    # valid (they pass every VALID AS OF filter). Existing chains verify unchanged.
    valid_from: Optional[str] = None
    valid_to:   Optional[str] = None

    def to_dict(self) -> dict:
        """Serialize for the append-only log file (AOF)."""
        d: dict = {
            "seq": self.seq, "client": self.client, "nonce": self.nonce,
            "op": self.op, "payload": self.payload, "ts": self.ts,
            "idem": self.idem, "prev_hash": self.prev_hash, "hash": self.hash,
        }
        if self.caused_by  is not None: d["caused_by"]  = self.caused_by
        if self.evidence   is not None: d["evidence"]   = self.evidence
        if self.confidence is not None: d["confidence"] = self.confidence
        if self.valid_from is not None: d["valid_from"] = self.valid_from
        if self.valid_to   is not None: d["valid_to"]   = self.valid_to
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Op":
        return cls(
            d["seq"], d["client"], d["nonce"], d["op"], d["payload"],
            d["ts"], d.get("idem"), d["prev_hash"], d["hash"],
            caused_by  = d.get("caused_by"),
            evidence   = d.get("evidence"),
            confidence = d.get("confidence"),
            valid_from = d.get("valid_from"),
            valid_to   = d.get("valid_to"),
        )


class OpLog:
    def __init__(self) -> None:
        self.ops: List[Op] = []
        self._last_nonce: Dict[str, int] = {}
        self._idem: Dict[str, int] = {}  # idem key -> seq of original op
        self._head = GENESIS

    def append(
        self,
        client: str,
        nonce: int,
        op: str,
        payload: dict,
        idem: Optional[str] = None,
        ts: Optional[float] = None,
        caused_by:  Optional[List[int]] = None,
        evidence:   Optional[str]       = None,
        confidence: Optional[float]     = None,
        valid_from: Optional[str]       = None,
        valid_to:   Optional[str]       = None,
    ) -> Tuple[Op, bool]:
        """Append an op. Returns (op, created). `created` is False when the op was
        deduplicated by its idempotency key (a no-op replay-safe return)."""
        # Idempotency: a known key returns the original op without re-appending.
        if idem is not None and idem in self._idem:
            return self.ops[self._idem[idem]], False

        # Replay protection: nonce must strictly exceed the client's last nonce.
        last = self._last_nonce.get(client, 0)
        if nonce <= last:
            raise ReplayError(
                f"replay/stale nonce for client '{client}': {nonce} <= {last}"
            )

        seq = len(self.ops)
        ts = time.time() if ts is None else ts
        body: dict = {
            "seq": seq, "client": client, "nonce": nonce,
            "op": op, "payload": payload, "ts": ts, "idem": idem,
        }
        # Provenance fields are sealed INTO the hash when present so they are
        # tamper-evident — omitting them when absent keeps old ops verifiable.
        if caused_by  is not None: body["caused_by"]  = caused_by
        if evidence   is not None: body["evidence"]   = evidence
        if confidence is not None: body["confidence"] = confidence
        if valid_from is not None: body["valid_from"] = valid_from
        if valid_to   is not None: body["valid_to"]   = valid_to
        h = blake(self._head.encode() + canon(body))
        rec = Op(seq, client, nonce, op, payload, ts, idem, self._head, h,
                 caused_by=caused_by, evidence=evidence, confidence=confidence,
                 valid_from=valid_from, valid_to=valid_to)

        self.ops.append(rec)
        self._last_nonce[client] = nonce
        if idem is not None:
            self._idem[idem] = seq
        self._head = h
        return rec, True

    def load(self, ops: List[Op]) -> None:
        """Rehydrate the log from persisted ops WITHOUT recomputing hashes, so the
        original chain (and thus verify() and the head commitment) is preserved
        exactly across a restart. Nonce, idempotency, and head state are restored
        from the ops themselves — replay protection survives a reload."""
        self.ops = list(ops)
        self._last_nonce = {}
        self._idem = {}
        for o in self.ops:
            if o.nonce > self._last_nonce.get(o.client, 0):
                self._last_nonce[o.client] = o.nonce
            if o.idem is not None and o.idem not in self._idem:
                self._idem[o.idem] = o.seq
        self._head = self.ops[-1].hash if self.ops else GENESIS

    @staticmethod
    def _op_body(o: "Op") -> dict:
        """The canonical hash body for an op — must match exactly what append() hashes."""
        body: dict = {
            "seq": o.seq, "client": o.client, "nonce": o.nonce,
            "op": o.op, "payload": o.payload, "ts": o.ts, "idem": o.idem,
        }
        # Optional fields included only when present (backward-compat with old ops).
        if o.caused_by  is not None: body["caused_by"]  = o.caused_by
        if o.evidence   is not None: body["evidence"]   = o.evidence
        if o.confidence is not None: body["confidence"] = o.confidence
        if o.valid_from is not None: body["valid_from"] = o.valid_from
        if o.valid_to   is not None: body["valid_to"]   = o.valid_to
        return body

    def verify(self) -> bool:
        """Re-walk the chain and confirm no op has been tampered with."""
        prev = GENESIS
        for o in self.ops:
            body = self._op_body(o)
            if o.prev_hash != prev:
                return False
            if o.hash != blake(prev.encode() + canon(body)):
                return False
            prev = o.hash
        return True

    @property
    def head(self) -> str:
        return self._head

    def slice_until(self, as_of: int) -> List[Op]:
        return [o for o in self.ops if o.seq <= as_of]

    def __len__(self) -> int:
        return len(self.ops)
