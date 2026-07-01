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
from .proof import verify_proof, fold_head

try:  # compiled Rust core, present in platform wheels (PyO3 via maturin)
    from . import _native  # type: ignore
    __has_native__ = True
except ImportError:  # pure-Python install (sdist / unsupported platform)
    # Provide a stub module so `from nedb._native import NedbCore` raises an
    # informative error instead of a bare ImportError with no guidance.
    import types as _types, sys as _sys

    import sys as _sys_tmp, os as _os_tmp
    _is_msys2 = bool(_os_tmp.environ.get("MSYSTEM")) or "mingw" in _sys_tmp.executable.lower()
    del _sys_tmp, _os_tmp

    class _NativeStub(_types.ModuleType):
        # Primary fix: install the Rust crate → get the nedbd server → use HTTP mode.
        # Secondary fix (CPython only): pip reinstall to get the platform wheel with _native embedded.
        _MSG_MSYS2 = (
            "\n\n"
            "  nedb._native (embedded v2 DAG core) is not available on MSYS2/MinGW Python.\n\n"
            "  To use NEDB v2 features, install the server binary and use HTTP mode:\n\n"
            "    cargo install nedb-engine          # install nedbd v2 server\n"
            "    nedbd --dag ./data                 # start DAG server\n"
            "    NEDB_URL=http://localhost:7070 python3 your_script.py\n\n"
            "  Run 'nedbd --doctor' for a full diagnosis.\n"
        )
        _MSG_OTHER = (
            "\n\n"
            "  nedb._native (embedded v2 DAG core) is not available.\n"
            "  You have the universal wheel — reinstall to get the platform wheel:\n\n"
            "    pip install --force-reinstall --no-cache-dir nedb-engine\n\n"
            "  Or install the server binary and use HTTP mode (works everywhere):\n\n"
            "    cargo install nedb-engine          # install nedbd v2 server\n"
            "    nedbd --dag ./data                 # start DAG server\n"
            "    NEDB_URL=http://localhost:7070 python3 your_script.py\n\n"
            "  Run 'nedbd --doctor' for a full diagnosis.\n"
        )
        _MSG = _MSG_MSYS2 if _is_msys2 else _MSG_OTHER

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
    "verify_proof", "fold_head",
    "_native", "__has_native__",
]
__version__ = "2.5.44"
