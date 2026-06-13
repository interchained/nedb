"""
NEDB — a versioned, self-compressing, time-traveling embedded database.

  * Replay-protected & idempotent: every write carries a monotonic nonce and an
    optional idempotency key, enforced by a hash-chained append-only log.
  * Time-travel: read the database AS OF any past sequence number.
  * Relational: first-class, time-travel-aware relations with O(1) traversal.
  * Filterable / sortable / searchable: equality, ordered, and full-text indexes.
  * Queryable: NQL text queries and a fluent builder that share one plan.
  * git-style files with Cascade compression: content-defined chunking + dedup +
    temperature tiers, with a Merkle root per version anchorable on-chain.

This pure-Python package is the reference implementation. The production speed core
is Rust (see ../rust), exposed to PyPI via PyO3 and to npm via napi-rs.
"""
from __future__ import annotations

from .engine import NEDB
from .log import Op, OpLog, ReplayError
from .query import Query, parse_nql

__all__ = ["NEDB", "OpLog", "Op", "ReplayError", "Query", "parse_nql"]
__version__ = "0.1.0"
