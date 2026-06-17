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
from .snapshot import save_snapshot, load_snapshot
from .crypto import resolve_tmk, rewrap_dek
from .sql import sql_exec, sql_to_nql, SQLError, SQLUnsupportedError
from .redis_compat import RedisCompat, RedisError, RedisUnsupportedError
from .mongo import (
    MongoCompat, MongoClient, MongoError, MongoUnsupportedError, ObjectId,
)
from .autoindex import AutoIndexDB
from .concurrent import Sequencer
from .wrap_redis import wrap_redis, WrappedRedis

try:  # compiled Rust core, present in platform wheels (PyO3 via maturin)
    from . import _native  # type: ignore
    __has_native__ = True
except ImportError:  # pure-Python install (sdist / unsupported platform)
    _native = None  # type: ignore
    __has_native__ = False

__all__ = [
    "NEDB", "OpLog", "Op", "ReplayError", "Query", "parse_nql",
    "save_snapshot", "load_snapshot",
    "sql_exec", "sql_to_nql", "SQLError", "SQLUnsupportedError",
    "RedisCompat", "RedisError", "RedisUnsupportedError",
    "MongoCompat", "MongoClient", "MongoError", "MongoUnsupportedError", "ObjectId",
    "AutoIndexDB", "Sequencer",
    "wrap_redis", "WrappedRedis",
    "_native", "__has_native__",
]
__version__ = "2.0.27"
