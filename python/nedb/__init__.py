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
    # Provide a stub module so `from nedb._native import NedbCore` raises an
    # informative error instead of a bare ImportError with no guidance.
    import types as _types, sys as _sys

    class _NativeStub(_types.ModuleType):
        _MSG = (
            "\n\n"
            "  nedb._native is not available on this platform.\n\n"
            "  The compiled Rust core ships with platform wheels (Linux x86_64,\n"
            "  macOS arm64/x86_64, Windows x64 CPython).  It is NOT included in\n"
            "  the universal wheel installed on MSYS2/MinGW Python.\n\n"
            "  Options:\n"
            "    1. Use the HTTP server instead (works on any platform):\n"
            "         nedbd --dag ./data                              # start DAG server\n"
            "         NEDB_URL=http://localhost:7070 python3 script.py\n\n"
            "    2. Re-install on a platform that has a native wheel:\n"
            "         pip install --force-reinstall --no-cache-dir nedb-engine\n\n"
            "    3. Run 'nedbd --doctor' for a full diagnosis.\n"
        )
        def __getattr__(self, name: str):
            raise ImportError(f"nedb._native.{name} is not available.{self._MSG}")

    _native_stub = _NativeStub("nedb._native")
    _native_stub.__package__ = "nedb"
    _sys.modules["nedb._native"] = _native_stub  # type: ignore
    _native = _native_stub  # type: ignore
    __has_native__ = False
    del _types, _sys, _NativeStub, _native_stub

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
__version__ = "2.2.27"
