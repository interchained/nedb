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

The pure-Python package is the reference implementation and the always-works
fallback. When installed from a platform wheel, the compiled Rust core is available
as ``nedb._native`` (``nedb.__has_native__`` reports whether it loaded).
"""
from __future__ import annotations

from .engine import NEDB
from .log import Op, OpLog, ReplayError
from .query import Query, parse_nql

try:  # compiled Rust core, present in platform wheels (PyO3 via maturin)
    from . import _native  # type: ignore
    __has_native__ = True
except ImportError:  # pure-Python install (sdist / unsupported platform)
    _native = None  # type: ignore
    __has_native__ = False

__all__ = ["NEDB", "OpLog", "Op", "ReplayError", "Query", "parse_nql",
           "_native", "__has_native__"]
__version__ = "0.1.2"
