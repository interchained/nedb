# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.resp2 — RESP2 wire protocol server for nedbd.

Clients that speak Redis (redis-cli, redis-benchmark, every Redis client
library in every language) connect to this server and NEDB handles the
commands natively. No Redis installation required — nedbd IS the server.

Protocol: RESP2 (Redis Serialization Protocol v2)
  +OK\r\n           — Simple string
  -ERR msg\r\n      — Error
  :42\r\n           — Integer
  $6\r\nhello\r\n   — Bulk string  ($-1 = null)
  *3\r\n...         — Array of N elements

Commands mapped to NEDB (database = selected via SELECT or default "db0"):
  PING [msg]
  SELECT <db_name>         — switch active database (creates if needed)
  SET key value [EX secs]
  GET key
  DEL key [key …]
  EXISTS key [key …]
  INCR key / INCRBY key n / DECR key / DECRBY key n
  MSET k v [k v …] / MGET k [k …]
  SETNX key value
  HSET hash field value [field value …]
  HGET hash field
  HMGET hash field [field …]
  HGETALL hash
  HDEL hash field [field …]
  HEXISTS hash field / HKEYS hash / HVALS hash / HLEN hash
  SADD key member [member …] / SMEMBERS key / SISMEMBER key m / SREM key m / SCARD key
  LPUSH key val [val …] / RPUSH key val [val …]
  LRANGE key start stop / LLEN key / LPOP key / RPOP key
  KEYS pattern / TYPE key / DBSIZE / FLUSHDB
  COMMAND (stub — returns OK so redis-cli connects cleanly)
  QUIT

NQL pass-through:
  EVAL "FROM users WHERE status = \\"active\\"" 0  → runs NQL directly

Unsupported (on roadmap): EXPIRE TTL SUBSCRIBE PUBLISH MULTI EXEC
"""
from __future__ import annotations

import os
import socketserver
import threading
from typing import Any, Dict, List, Optional

from .redis_compat import RedisCompat, RedisUnsupportedError

DEFAULT_DB = "db0"


# ── RESP2 encoding ────────────────────────────────────────────────────────────

def _bulk(s: Optional[str]) -> bytes:
    if s is None:
        return b"$-1\r\n"
    enc = str(s).encode()
    return b"$" + str(len(enc)).encode() + b"\r\n" + enc + b"\r\n"


def _int(n: int) -> bytes:
    return b":" + str(int(n)).encode() + b"\r\n"


def _ok() -> bytes:
    return b"+OK\r\n"


def _err(msg: str) -> bytes:
    return b"-ERR " + msg.replace("\r\n", " ").encode() + b"\r\n"


def _arr(items: List[Any]) -> bytes:
    out = b"*" + str(len(items)).encode() + b"\r\n"
    for item in items:
        # If the item is already RESP2-encoded bytes (e.g. from _bulk()), use it
        # directly — calling _encode() on bytes would stringify them ("b'...'").
        out += item if isinstance(item, bytes) else _encode(item)
    return out


def _encode(value: Any) -> bytes:
    if value is None:
        return b"$-1\r\n"
    if isinstance(value, bool):
        return _int(1 if value else 0)
    if isinstance(value, int):
        return _int(value)
    if isinstance(value, (set, frozenset)):
        return _arr([_bulk(str(x)) for x in sorted(value)])
    if isinstance(value, list):
        return _arr([_encode(x) for x in value])
    if isinstance(value, dict):
        parts = []
        for k, v in value.items():
            parts.append(_bulk(str(k)))
            parts.append(_bulk(str(v) if v is not None else None))
        return _arr(parts)
    return _bulk(str(value))


# ── RESP2 parser ──────────────────────────────────────────────────────────────

class RespReader:
    def __init__(self, rfile):
        self._f = rfile

    def read_command(self) -> Optional[List[str]]:
        line = self._f.readline()
        if not line:
            return None
        line = line.rstrip(b"\r\n")
        if not line:
            return None
        if line.startswith(b"*"):
            n = int(line[1:])
            args = []
            for _ in range(n):
                bulk_hdr = self._f.readline().rstrip(b"\r\n")
                if not bulk_hdr.startswith(b"$"):
                    raise ValueError(f"Expected bulk string, got {bulk_hdr!r}")
                length = int(bulk_hdr[1:])
                data = self._f.read(length + 2)  # +2 for \r\n
                args.append(data[:-2].decode(errors="replace"))
            return args
        # Inline command (e.g. redis-cli in inline mode, PING from telnet)
        return line.decode(errors="replace").split()


# ── Connection handler ────────────────────────────────────────────────────────

class _Handler(socketserver.StreamRequestHandler):
    def setup(self):
        super().setup()
        self._db_name: str = DEFAULT_DB
        self._rc: Optional[RedisCompat] = None

    def _get_rc(self) -> RedisCompat:
        if self._rc is None or getattr(self._rc, "_db_name", None) != self._db_name:
            db = self.server.manager.open(self._db_name)
            self._rc = RedisCompat(db)
            self._rc._db_name = self._db_name  # type: ignore[attr-defined]
        return self._rc

    def handle(self):
        reader = RespReader(self.rfile)
        while True:
            try:
                args = reader.read_command()
            except Exception:
                break
            if args is None:
                break
            if not args:
                continue
            cmd = args[0].upper()
            rest = args[1:]
            try:
                reply = self._dispatch(cmd, rest)
            except RedisUnsupportedError as e:
                reply = _err(str(e))
            except Exception as e:
                reply = _err(str(e))
            try:
                self.wfile.write(reply)
                self.wfile.flush()
            except Exception:
                break

    def _dispatch(self, cmd: str, args: List[str]) -> bytes:
        rc = self._get_rc

        # ── server / connection ──────────────────────────────────────────────
        if cmd == "PING":
            return b"+PONG\r\n" if not args else _bulk(args[0])
        if cmd == "QUIT":
            return _ok()
        if cmd == "COMMAND":
            return _ok()  # stub — enough for redis-cli to connect
        if cmd == "SELECT":
            if not args:
                return _err("SELECT requires a database name")
            self._db_name = args[0]
            self._rc = None
            return _ok()
        if cmd == "DBSIZE":
            return _int(rc().execute("DBSIZE"))

        # ── NQL pass-through via EVAL ────────────────────────────────────────
        if cmd == "EVAL":
            if not args:
                return _err("EVAL requires a NQL string")
            nql = args[0]
            import json as _json
            db = self.server.manager.open(self._db_name)
            rows = db.query(nql)
            # Each row is returned as a compact JSON string — clients can parse it
            return _arr([_bulk(_json.dumps(r, separators=(",", ":"))) for r in rows])

        # ── strings ──────────────────────────────────────────────────────────
        if cmd in ("SET", "GET", "GETDEL", "SETNX", "MSET", "MGET",
                   "DEL", "UNLINK", "EXISTS", "INCR", "INCRBY",
                   "DECR", "DECRBY", "APPEND", "STRLEN", "TYPE",
                   "RENAME", "KEYS", "FLUSHDB"):
            result = rc().execute(cmd, *args)
            return _encode(result)

        # ── hashes ───────────────────────────────────────────────────────────
        if cmd in ("HSET", "HMSET", "HSETNX", "HGET", "HMGET",
                   "HGETALL", "HDEL", "HEXISTS", "HKEYS", "HVALS",
                   "HLEN", "HINCRBY"):
            result = rc().execute(cmd, *args)
            return _encode(result)

        # ── sets ─────────────────────────────────────────────────────────────
        if cmd in ("SADD", "SMEMBERS", "SISMEMBER", "SREM",
                   "SCARD", "SUNION", "SINTER", "SDIFF"):
            result = rc().execute(cmd, *args)
            return _encode(result)

        # ── lists ────────────────────────────────────────────────────────────
        if cmd in ("LPUSH", "RPUSH", "LRANGE", "LLEN",
                   "LINDEX", "LSET", "LPOP", "RPOP"):
            result = rc().execute(cmd, *args)
            return _encode(result)

        # ── unsupported with clear message ───────────────────────────────────
        if cmd in ("EXPIRE", "EXPIREAT", "TTL", "PTTL", "PEXPIRE", "PERSIST"):
            return _err(f"{cmd} is on the NEDB roadmap — use db.expire() via the Python API.")
        if cmd in ("SUBSCRIBE", "PUBLISH", "UNSUBSCRIBE", "PSUBSCRIBE"):
            return _err(f"{cmd} (pub-sub) is on the NEDB roadmap.")
        if cmd in ("MULTI", "EXEC", "DISCARD", "WATCH"):
            return _err(f"{cmd} (transactions) is on the NEDB roadmap.")

        return _err(f"unknown command '{cmd}'")


class _ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def make_resp2_server(manager, host: str, port: int):
    """Create and return a threaded TCP server that speaks RESP2."""
    srv = _ThreadedTCPServer((host, port), _Handler)
    srv.manager = manager
    return srv
