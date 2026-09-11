# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.redis_compat — Redis command compatibility adapter.

Maps the Redis command surface deterministically to NEDB primitives. No Redis
or hiredis code is used or required — Redis commands are a familiar entry point;
NEDB executes everything natively using its append-only log, MVCC, and relations.

Usage::

    from nedb import NEDB
    from nedb.redis_compat import RedisCompat

    db   = NEDB("./data")
    r    = RedisCompat(db)

    r.execute("SET", "mykey", "hello")     # → "OK"
    r.execute("GET", "mykey")              # → "hello"
    r.execute("HSET", "user:1", "name", "Ada", "age", "31")
    r.execute("HGETALL", "user:1")         # → {"name": "Ada", "age": "31"}
    r.execute("SADD", "tags", "python", "rust")
    r.execute("SMEMBERS", "tags")          # → {"python", "rust"}

Key → NEDB mapping
──────────────────
Strings (SET/GET)   →  collection "_kv",   id = key, doc = {"_v": value}
Hashes  (HSET/…)   →  collection = key,   id = field, doc = {"_v": value}
Sets    (SADD/…)   →  relation edges from "_set:<key>" to "_smember:<key>:<member>"
Lists   (LPUSH/…)  →  collection "_list:<key>", auto-seq id, doc = {"_v", "_seq"}

Unsupported commands (todo):
  EXPIRE, TTL, PEXPIRE, PTTL, PERSIST   — no built-in TTL mechanism
  SUBSCRIBE, PUBLISH, UNSUBSCRIBE        — no pub-sub layer
  MULTI, EXEC, DISCARD, WATCH           — no transaction isolation
"""
from __future__ import annotations

import re
import time
import uuid
from typing import Any, Dict, List, Optional, Set


def _safe(name: str) -> str:
    """Encode a Redis key into a NQL-safe NEDB collection name.
    Characters outside [A-Za-z0-9_] are replaced with __XX__ (hex).
    This is deterministic and collision-free — 'user:1' → 'user__3a__1'.
    """
    return re.sub(r"[^A-Za-z0-9_]", lambda m: f"__{ord(m.group()):02x}__", name)


_KV_COLL = "_kv"
_LIST_PREFIX = "_list_"
_SET_NODE_PREFIX = "_set_"
_SET_MEMBER_PREFIX = "_smember_"

UNSUPPORTED = {
    "EXPIRE", "EXPIREAT", "EXPIRETIME", "PEXPIRE", "PEXPIREAT",
    "TTL", "PTTL", "PERSIST",
    "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE",
    "PUBLISH", "PUBSUB",
    "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
    "WAIT", "OBJECT", "DEBUG", "MONITOR", "SLOWLOG",
    "CLUSTER", "REPLICAOF", "SLAVEOF", "BGSAVE", "BGREWRITEAOF",
    "LASTSAVE", "SAVE", "RESTORE", "DUMP", "MIGRATE", "MOVE",
    "SORT", "EVAL", "EVALSHA", "SCRIPT",
    "GEORADIUSBYMEMBER", "GEOADD", "GEODIST", "GEOPOS",
    "XADD", "XREAD", "XLEN", "XRANGE",
    "BF.ADD", "CF.ADD",
}

_TODO_REASON = {
    "EXPIRE": "TTL/expiry is on the NEDB roadmap. Track: github.com/Eth-Interchained/nedb/issues.",
    "TTL": "TTL is on the NEDB roadmap.",
    "PTTL": "TTL is on the NEDB roadmap.",
    "SUBSCRIBE": "Pub-sub is on the NEDB roadmap.",
    "PUBLISH": "Pub-sub is on the NEDB roadmap.",
    "MULTI": "Transactions (MULTI/EXEC) are on the NEDB roadmap.",
    "EXEC": "Transactions (MULTI/EXEC) are on the NEDB roadmap.",
    "DISCARD": "Transactions are on the NEDB roadmap.",
}


class RedisUnsupportedError(Exception):
    """Raised when a Redis command is not yet implemented in NEDB."""


class RedisError(Exception):
    """Raised on a Redis-compatible argument or usage error."""


class RedisCompat:
    """
    Redis-compatible command interface over a NEDB database.

    Each command is executed transactionally (append-only log) with NEDB's
    replay-protection. Pass ``client`` to scope nonce counters per service.
    """

    def __init__(self, db: Any, client: str = "redis-compat"):
        self._db = db
        self._client = client

    # ── dispatch ──────────────────────────────────────────────────────────────

    def execute(self, command: str, *args: Any) -> Any:
        """
        Execute a Redis command and return the result.

        Arguments mirror the Redis protocol — all string, no type coercion
        except what Redis itself performs (INCR coerces to int, HSET takes
        alternating field/value pairs, etc.).
        """
        cmd = command.upper()

        if cmd in UNSUPPORTED:
            reason = _TODO_REASON.get(cmd, "Not yet implemented in NEDB.")
            raise RedisUnsupportedError(
                f"{cmd} is not supported. {reason}"
            )

        # ── String commands ────────────────────────────────────────────────
        if cmd == "PING":
            return "PONG" if not args else args[0]
        if cmd == "SET":
            return self._set(*args)
        if cmd == "GET":
            return self._get(*args)
        if cmd == "GETDEL":
            return self._getdel(*args)
        if cmd == "SETNX":
            return self._setnx(*args)
        if cmd == "MSET":
            return self._mset(*args)
        if cmd == "MGET":
            return self._mget(*args)
        if cmd == "DEL":
            return self._del(*args)
        if cmd == "UNLINK":
            return self._del(*args)  # same semantics for our purposes
        if cmd == "EXISTS":
            return self._exists(*args)
        if cmd == "INCR":
            return self._incrby(args[0], 1)
        if cmd == "INCRBY":
            return self._incrby(args[0], int(args[1]))
        if cmd == "DECR":
            return self._incrby(args[0], -1)
        if cmd == "DECRBY":
            return self._incrby(args[0], -int(args[1]))
        if cmd == "APPEND":
            return self._append(*args)
        if cmd == "STRLEN":
            v = self._get(args[0])
            return len(str(v)) if v is not None else 0
        if cmd == "TYPE":
            return self._type(*args)
        if cmd == "RENAME":
            return self._rename(*args)
        if cmd == "KEYS":
            return self._keys(args[0] if args else "*")
        if cmd == "DBSIZE":
            return len(self._keys("*"))
        if cmd == "FLUSHDB":
            return self._flushdb()

        # ── Hash commands ──────────────────────────────────────────────────
        if cmd == "HSET":
            return self._hset(*args)
        if cmd == "HMSET":
            self._hset(*args); return "OK"
        if cmd == "HSETNX":
            return self._hsetnx(*args)
        if cmd == "HGET":
            return self._hget(*args)
        if cmd == "HMGET":
            return self._hmget(*args)
        if cmd == "HGETALL":
            return self._hgetall(*args)
        if cmd == "HDEL":
            return self._hdel(*args)
        if cmd == "HEXISTS":
            return self._hexists(*args)
        if cmd == "HKEYS":
            return self._hkeys(*args)
        if cmd == "HVALS":
            return self._hvals(*args)
        if cmd == "HLEN":
            return self._hlen(*args)
        if cmd == "HINCRBY":
            return self._hincrby(*args)

        # ── Set commands ───────────────────────────────────────────────────
        if cmd == "SADD":
            return self._sadd(*args)
        if cmd == "SMEMBERS":
            return self._smembers(*args)
        if cmd == "SISMEMBER":
            return self._sismember(*args)
        if cmd == "SREM":
            return self._srem(*args)
        if cmd == "SCARD":
            return self._scard(*args)
        if cmd == "SUNION":
            result: Set[str] = set()
            for k in args:
                result |= self._smembers(k)
            return result
        if cmd == "SINTER":
            sets = [self._smembers(k) for k in args]
            return sets[0].intersection(*sets[1:]) if sets else set()
        if cmd == "SDIFF":
            sets = [self._smembers(k) for k in args]
            return sets[0].difference(*sets[1:]) if sets else set()

        # ── List commands ──────────────────────────────────────────────────
        if cmd == "LPUSH":
            return self._lpush(*args)
        if cmd == "RPUSH":
            return self._rpush(*args)
        if cmd == "LRANGE":
            return self._lrange(*args)
        if cmd == "LLEN":
            return self._llen(*args)
        if cmd == "LINDEX":
            return self._lindex(*args)
        if cmd == "LSET":
            return self._lset(*args)
        if cmd == "LPOP":
            return self._lpop(*args)
        if cmd == "RPOP":
            return self._rpop(*args)

        raise RedisError(f"Unknown command: {command!r}")

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _put(self, coll: str, row_id: str, doc: Dict[str, Any], idem: Optional[str] = None) -> Any:
        return self._db.put(coll, row_id, doc, client=self._client, idem=idem)

    def _get_raw(self, coll: str, row_id: str) -> Optional[Dict[str, Any]]:
        return self._db.get(coll, row_id)

    def _del_raw(self, coll: str, row_id: str) -> None:
        self._db.delete(coll, row_id, client=self._client)

    # ── String ops ────────────────────────────────────────────────────────────

    def _set(self, key: str, value: Any, *opts: Any) -> str:
        flags = {str(o).upper() for o in opts}
        if "NX" in flags:
            return self._setnx(key, value)
        if "XX" in flags:
            if self._get_raw(_KV_COLL, key) is None:
                return None  # type: ignore[return-value]
        self._put(_KV_COLL, str(key), {"_v": str(value)})
        return "OK"

    def _get(self, key: str) -> Optional[str]:
        doc = self._get_raw(_KV_COLL, str(key))
        return str(doc["_v"]) if doc and "_v" in doc else None

    def _getdel(self, key: str) -> Optional[str]:
        val = self._get(key)
        if val is not None:
            self._del_raw(_KV_COLL, str(key))
        return val

    def _setnx(self, key: str, value: Any) -> int:
        if self._get_raw(_KV_COLL, str(key)) is not None:
            return 0
        self._put(_KV_COLL, str(key), {"_v": str(value)})
        return 1

    def _mset(self, *pairs: Any) -> str:
        if len(pairs) % 2:
            raise RedisError("MSET requires an even number of arguments (key value ...)")
        for i in range(0, len(pairs), 2):
            self._set(pairs[i], pairs[i + 1])
        return "OK"

    def _mget(self, *keys: Any) -> List[Optional[str]]:
        return [self._get(k) for k in keys]

    def _del(self, *keys: Any) -> int:
        count = 0
        for k in keys:
            doc = self._get_raw(_KV_COLL, str(k))
            if doc is not None:
                self._del_raw(_KV_COLL, str(k))
                count += 1
        return count

    def _exists(self, *keys: Any) -> int:
        return sum(1 for k in keys if self._get_raw(_KV_COLL, str(k)) is not None)

    def _incrby(self, key: str, delta: int) -> int:
        doc = self._get_raw(_KV_COLL, str(key))
        current = int(doc["_v"]) if doc and "_v" in doc else 0
        new_val = current + delta
        self._put(_KV_COLL, str(key), {"_v": str(new_val)})
        return new_val

    def _append(self, key: str, value: str) -> int:
        existing = self._get(key) or ""
        combined = existing + str(value)
        self._put(_KV_COLL, str(key), {"_v": combined})
        return len(combined)

    def _type(self, key: str) -> str:
        if self._get_raw(_KV_COLL, str(key)) is not None:
            return "string"
        if self._hlen(str(key)) > 0:
            return "hash"
        if self._scard(str(key)) > 0:
            return "set"
        if self._llen(str(key)) > 0:
            return "list"
        return "none"

    def _rename(self, src: str, dst: str) -> str:
        val = self._get(src)
        if val is None:
            raise RedisError(f"ERR no such key: {src!r}")
        self._set(dst, val)
        self._del_raw(_KV_COLL, str(src))
        return "OK"

    def _keys(self, pattern: str = "*") -> List[str]:
        import fnmatch
        rows = self._db.query(f"FROM {_KV_COLL}")
        all_keys = [r.get("_id", "") for r in rows]
        if pattern == "*":
            return all_keys
        return [k for k in all_keys if fnmatch.fnmatch(k, pattern)]

    def _flushdb(self) -> str:
        for key in self._keys("*"):
            self._del_raw(_KV_COLL, key)
        return "OK"

    # ── Hash ops (collection = hash name; id = field) ─────────────────────────

    def _hset(self, name: str, *pairs: Any) -> int:
        if len(pairs) % 2:
            raise RedisError("HSET requires alternating field value pairs")
        coll = _safe(str(name))
        created = 0
        for i in range(0, len(pairs), 2):
            field, value = str(pairs[i]), pairs[i + 1]
            existed = self._get_raw(coll, field) is not None
            self._put(coll, field, {"_v": str(value)})
            if not existed:
                created += 1
        return created

    def _hsetnx(self, name: str, field: str, value: Any) -> int:
        coll = _safe(str(name))
        if self._get_raw(coll, str(field)) is not None:
            return 0
        self._put(coll, str(field), {"_v": str(value)})
        return 1

    def _hget(self, name: str, field: str) -> Optional[str]:
        doc = self._get_raw(_safe(str(name)), str(field))
        return str(doc["_v"]) if doc and "_v" in doc else None

    def _hmget(self, name: str, *fields: Any) -> List[Optional[str]]:
        return [self._hget(str(name), str(f)) for f in fields]

    def _hgetall(self, name: str) -> Dict[str, str]:
        rows = self._db.query(f"FROM {_safe(str(name))}")
        return {r["_id"]: str(r["_v"]) for r in rows if "_v" in r}

    def _hdel(self, name: str, *fields: Any) -> int:
        coll = _safe(str(name))
        count = 0
        for f in fields:
            if self._get_raw(coll, str(f)) is not None:
                self._del_raw(coll, str(f))
                count += 1
        return count

    def _hexists(self, name: str, field: str) -> int:
        return 1 if self._get_raw(_safe(str(name)), str(field)) is not None else 0

    def _hkeys(self, name: str) -> List[str]:
        return [r["_id"] for r in self._db.query(f"FROM {_safe(str(name))}") if "_v" in r]

    def _hvals(self, name: str) -> List[str]:
        return [str(r["_v"]) for r in self._db.query(f"FROM {_safe(str(name))}") if "_v" in r]

    def _hlen(self, name: str) -> int:
        return len(self._hkeys(name))

    def _hincrby(self, name: str, field: str, delta: Any) -> int:
        coll = _safe(str(name))
        doc = self._get_raw(coll, str(field))
        current = int(doc["_v"]) if doc and "_v" in doc else 0
        new_val = current + int(delta)
        self._put(coll, str(field), {"_v": str(new_val)})
        return new_val

    # ── Set ops (using NEDB relations) ────────────────────────────────────────

    def _set_node(self, key: str) -> str:
        return f"{_SET_NODE_PREFIX}{_safe(key)}"

    def _set_member_node(self, key: str, member: str) -> str:
        return f"{_SET_MEMBER_PREFIX}{_safe(key)}__{_safe(member)}"

    def _sadd(self, key: str, *members: Any) -> int:
        count = 0
        node = self._set_node(str(key))
        existing = set(self._smembers(str(key)))
        for m in members:
            ms = str(m)
            if ms not in existing:
                target = self._set_member_node(str(key), ms)
                self._db.link(node, "member", target, client=self._client)
                existing.add(ms)
                count += 1
        return count

    def _smembers(self, key: str) -> Set[str]:
        node = self._set_node(str(key))
        prefix = f"{_SET_MEMBER_PREFIX}{_safe(str(key))}__"
        neighbors = self._db.neighbors(node, "member")
        return {nb[len(prefix):] for nb in neighbors if nb.startswith(prefix)}

    def _sismember(self, key: str, member: str) -> int:
        return 1 if str(member) in self._smembers(str(key)) else 0

    def _srem(self, key: str, *members: Any) -> int:
        count = 0
        node = self._set_node(str(key))
        existing = self._smembers(str(key))
        for m in members:
            ms = str(m)
            if ms in existing:
                target = self._set_member_node(str(key), ms)
                self._db.unlink(node, "member", target, client=self._client)
                count += 1
        return count

    def _scard(self, key: str) -> int:
        return len(self._smembers(str(key)))

    # ── List ops (append-only; id = zero-padded timestamp for order) ──────────

    def _list_coll(self, key: str) -> str:
        return f"{_LIST_PREFIX}{_safe(key)}"

    def _lpush(self, key: str, *values: Any) -> int:
        coll = self._list_coll(str(key))
        for v in reversed(values):
            seq_id = f"L-{time.monotonic_ns():020d}-{uuid.uuid4().hex[:6]}"
            self._put(coll, seq_id, {"_v": str(v), "_side": "L", "_ts": time.monotonic_ns()})
        return self._llen(str(key))

    def _rpush(self, key: str, *values: Any) -> int:
        coll = self._list_coll(str(key))
        for v in values:
            seq_id = f"R-{time.monotonic_ns():020d}-{uuid.uuid4().hex[:6]}"
            self._put(coll, seq_id, {"_v": str(v), "_side": "R", "_ts": time.monotonic_ns()})
        return self._llen(str(key))

    def _list_rows(self, key: str) -> List[Dict[str, Any]]:
        coll = self._list_coll(str(key))
        rows = self._db.query(f"FROM {coll}")
        return sorted(rows, key=lambda r: (r.get("_side", "R"), r.get("_ts", 0)))

    def _lrange(self, key: str, start: Any, stop: Any) -> List[str]:
        rows = self._list_rows(str(key))
        vals = [str(r["_v"]) for r in rows if "_v" in r]
        start, stop = int(start), int(stop)
        if stop == -1:
            stop = len(vals) - 1
        return vals[start:stop + 1]

    def _llen(self, key: str) -> int:
        return len(self._list_rows(str(key)))

    def _lindex(self, key: str, index: Any) -> Optional[str]:
        vals = self._lrange(str(key), 0, -1)
        i = int(index)
        return vals[i] if 0 <= i < len(vals) else (vals[i] if -len(vals) <= i < 0 else None)

    def _lset(self, key: str, index: Any, value: Any) -> str:
        rows = self._list_rows(str(key))
        i = int(index)
        if i < 0:
            i = len(rows) + i
        if i < 0 or i >= len(rows):
            raise RedisError("ERR index out of range")
        coll = self._list_coll(str(key))
        row = rows[i]
        self._put(coll, row["_id"], {**row, "_v": str(value)})
        return "OK"

    def _lpop(self, key: str) -> Optional[str]:
        rows = self._list_rows(str(key))
        if not rows:
            return None
        r = rows[0]
        coll = self._list_coll(str(key))
        self._del_raw(coll, r["_id"])
        return str(r.get("_v", ""))

    def _rpop(self, key: str) -> Optional[str]:
        rows = self._list_rows(str(key))
        if not rows:
            return None
        r = rows[-1]
        coll = self._list_coll(str(key))
        self._del_raw(coll, r["_id"])
        return str(r.get("_v", ""))
