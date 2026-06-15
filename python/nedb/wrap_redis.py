"""
nedb.wrap_redis — wrap an existing Redis connection with NEDB's layer-2.

ONE LINE. Alice's existing app doesn't change. New parts of her app get
time-travel, bi-temporal, causal provenance, and NQL.

    from nedb import wrap_redis
    import redis

    r = wrap_redis(redis.Redis("localhost", 6379), db_name="rideshare")

    # ── Step 1: register collection mappings ──────────────────────────────
    r.nedb.register("driver:*",   collection="driver",
                    value_parser=json.loads)
    r.nedb.register("trip:*",     collection="trip",
                    value_type="hash")

    # ── Step 2: backfill existing Redis data into NEDB ────────────────────
    r.nedb.backfill()     # scans all registered patterns, imports them once

    # ── Step 3: enable write shadowing ────────────────────────────────────
    r.nedb.shadow_writes = True   # all future surface-1 writes auto-chained

    # Now: Alice's app runs unchanged AND new writes are in NEDB's hash chain
    r.set("driver:d1", '{"name":"Bob","status":"active"}')   # ← shadowed
    r.nedb.query('FROM driver WHERE status = "active"')       # ← NEDB query

Isolation guarantee: NEDB NEVER writes to Alice's namespace. It owns only:
    nedb:{db_name}:oplog      Redis Stream  (op log)
    nedb:{db_name}:snapshot   Redis Hash    (checkpoint)
    nedb:{db_name}:events     Pub/Sub       (live subs, future)
    nedb:{db_name}:meta       Redis Hash    (index config)

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
from __future__ import annotations

import fnmatch
import json
import re
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional

from .engine import NEDB as _NEDB
from .backends.redis_backend import RedisBackend


# ── NedBdProxy ────────────────────────────────────────────────────────────────

class NedBdProxy:
    """
    Drop-in replacement for the in-process NEDB engine when a nedbd server
    is available.  All r.nedb.* calls are forwarded to nedbd's HTTP/JSON API
    instead of running in-process.

    Usage::

        r = wrap_redis(redis.Redis(...), db_name="rideshare",
                       nedbd_url="http://localhost:8421",
                       nedbd_token="secret")   # token is optional

    The proxy auto-creates the database on first write.
    ``head`` and ``seq`` are cached from the last response and refreshed
    on demand via ``_refresh()``.
    """

    def __init__(self, base_url: str, db_name: str, token: Optional[str] = None):
        self._base   = base_url.rstrip("/")
        self._name   = db_name
        self._token  = token
        self._seq: int  = -1
        self._head: str = "0" * 64
        self._ensure_db()

    # ── HTTP helpers ─────────────────────────────────────────────────────────

    def _headers(self) -> dict:
        h = {"Content-Type": "application/json", "Accept": "application/json"}
        if self._token:
            h["Authorization"] = f"Bearer {self._token}"
        return h

    def _req(self, method: str, path: str, body: Optional[dict] = None) -> dict:
        url  = f"{self._base}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req  = urllib.request.Request(url, data=data, headers=self._headers(),
                                      method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read().decode())
                if "seq"  in result: self._seq  = result["seq"]
                if "head" in result: self._head = result["head"]
                return result
        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="replace")
            try:
                detail = json.loads(body_text).get("error", body_text)
            except Exception:
                detail = body_text
            raise RuntimeError(f"nedbd {method} {url} → HTTP {e.code}: {detail}") from e

    def _db(self, suffix: str = "") -> str:
        return f"/v1/databases/{self._name}{suffix}"

    # ── Bootstrap ────────────────────────────────────────────────────────────

    def _ensure_db(self) -> None:
        """Create the database if it doesn't exist yet; load seq/head if it does."""
        try:
            info = self._req("GET", self._db())
            self._seq  = info.get("seq",  self._seq)
            self._head = info.get("head", self._head)
        except RuntimeError as e:
            if "404" in str(e):
                self._req("POST", "/v1/databases", {"name": self._name})
            else:
                raise

    def _refresh(self) -> None:
        info = self._req("GET", self._db())
        self._seq  = info.get("seq",  self._seq)
        self._head = info.get("head", self._head)

    # ── NEDB engine interface ─────────────────────────────────────────────────

    def put(self, coll: str, id: str, doc: dict, **kw) -> dict:
        payload: dict = {"coll": coll, "id": id, "doc": doc}
        for k in ("client", "nonce", "idem", "evidence", "confidence",
                  "valid_from", "valid_to"):
            if kw.get(k) is not None:
                payload[k] = kw[k]
        if kw.get("caused_by") is not None:
            payload["caused_by"] = list(kw["caused_by"])
        result = self._req("POST", self._db("/put"), payload)
        return result.get("doc", doc)

    def get(self, coll: str, id: str, as_of: Optional[int] = None):
        as_of_clause = f" AS OF {as_of}" if as_of is not None else ""
        nql = f'FROM {coll}{as_of_clause} WHERE _id = "{id}"'
        rows = self._req("POST", self._db("/query"), {"nql": nql}).get("rows", [])
        return rows[0] if rows else None

    def query(self, nql: str) -> List[dict]:
        return self._req("POST", self._db("/query"), {"nql": nql}).get("rows", [])

    def create_index(self, coll: str, field: str, kind: str = "eq") -> None:
        self._req("POST", self._db("/index"), {"coll": coll, "field": field, "kind": kind})

    def delete(self, coll: str, id: str, **_kw) -> None:
        self._req("DELETE", f"/v1/databases/{self._name}/rows/{coll}/{id}")

    def link(self, frm: str, rel: str, to: str, **_kw) -> None:
        self._req("POST", self._db("/link"), {"frm": frm, "rel": rel, "to": to})

    def unlink(self, frm: str, rel: str, to: str, **_kw) -> None:
        # nedbd doesn't have a dedicated unlink endpoint yet — use NQL delete
        # fallback: store as a tombstone via link with _unlinked flag
        pass  # no-op; future: DELETE /v1/databases/{name}/links/{frm}/{rel}/{to}

    def neighbors(self, frm: str, rel: str, as_of: Optional[int] = None) -> List[str]:
        frm_coll, frm_id = (frm.split(":", 1) + [""])[:2]
        as_of_clause = f" AS OF {as_of}" if as_of is not None else ""
        nql = f'FROM {frm_coll}{as_of_clause} WHERE _id = "{frm_id}" TRAVERSE {rel}'
        rows = self._req("POST", self._db("/query"), {"nql": nql}).get("rows", [])
        return [f"{r.get('_coll', frm_coll)}:{r['_id']}" for r in rows if "_id" in r]

    def inbound(self, to: str, rel: str, as_of: Optional[int] = None) -> List[str]:
        # Approximate via NQL — nedbd doesn't expose inbound traversal directly
        to_coll, to_id = (to.split(":", 1) + [""])[:2]
        nql = f'FROM {to_coll} WHERE _id = "{to_id}" TRAVERSE {rel} REVERSE'
        try:
            rows = self._req("POST", self._db("/query"), {"nql": nql}).get("rows", [])
            return [f"{r.get('_coll', to_coll)}:{r['_id']}" for r in rows if "_id" in r]
        except Exception:
            return []

    def verify(self) -> bool:
        return self._req("GET", self._db("/verify")).get("ok", False)

    def checkpoint(self) -> str:
        return self._req("POST", self._db("/checkpoint")).get("head", self._head)

    @property
    def head(self) -> str:
        return self._head

    @property
    def seq(self) -> int:
        return self._seq

# ── Write command detection ──────────────────────────────────────────────────

# Redis commands that mutate state — these are shadowed when shadow_writes=True
_WRITE_CMDS = frozenset({
    "set", "setnx", "setex", "psetex", "getset", "getdel", "getex",
    "mset", "msetnx",
    "hset", "hmset", "hsetnx", "hincrby", "hincrbyfloat", "hdel",
    "lpush", "rpush", "lset", "linsert", "ltrim", "lpop", "rpop",
    "sadd", "srem", "smove",
    "zadd", "zincrby", "zrem", "zremrangebyscore", "zremrangebyrank",
    "del", "delete", "unlink",
    "rename", "renamenx",
    "append", "incr", "incrby", "decr", "decrby",
    "setrange",
})


# ── Collection mapping ───────────────────────────────────────────────────────

class CollectionMapping:
    """Maps a Redis key glob pattern to a NEDB collection."""

    def __init__(
        self,
        pattern: str,
        collection: str,
        id_extractor: Optional[Callable[[str], str]] = None,
        value_parser: Optional[Callable[[Any], dict]] = None,
        value_type: str = "string",   # "string" | "hash" | "json"
    ):
        self.pattern      = pattern
        self.collection   = collection
        self.value_type   = value_type
        # Default id extractor: take the part after the LAST colon separator
        # "driver:d1" → "d1",   "trip:zone:t1" → "t1"
        self.id_extractor = id_extractor or (lambda k: k.rsplit(":", 1)[-1])
        # Default value parser: try JSON, fall back to {"_v": raw}
        self.value_parser = value_parser or self._default_parse

    @staticmethod
    def _default_parse(v: Any) -> dict:
        if isinstance(v, (bytes, str)):
            s = v.decode() if isinstance(v, bytes) else v
            try:
                parsed = json.loads(s)
                if isinstance(parsed, dict):
                    return parsed
                return {"_v": parsed}
            except (json.JSONDecodeError, ValueError):
                return {"_v": s}
        if isinstance(v, dict):
            return {(k.decode() if isinstance(k, bytes) else k):
                    (vv.decode() if isinstance(vv, bytes) else vv)
                    for k, vv in v.items()}
        return {"_v": str(v)}

    def matches(self, key: str) -> bool:
        return fnmatch.fnmatch(key, self.pattern)

    def extract_id(self, key: str) -> str:
        return self.id_extractor(key)

    def parse_value(self, value: Any) -> dict:
        return self.value_parser(value)


# ── NEDBSurface ───────────────────────────────────────────────────────────────

class NEDBSurface:
    """
    The `r.nedb` attribute — full NEDB feature access.

    Key features added vs v1.1.0:
    - register(pattern, collection, ...)  → teach NEDB about Alice's key structure
    - backfill()                          → one-time import of existing Redis data
    - shadow_writes = True               → auto-chain all surface-1 Redis writes

    nedbd mode (nedbd_url=):
    - All r.nedb.* calls are forwarded to a running nedbd HTTP server.
    - nedbd handles its own persistence (durable AOF on disk).
    - Redis Stream backend is bypassed; _persist_last_op() is a no-op.
    """

    def __init__(self, r: Any, db_name: str,
                 nedbd_url: Optional[str] = None,
                 nedbd_token: Optional[str] = None):
        self._r        = r
        self._db_name  = db_name
        self._mappings: List[CollectionMapping] = []
        self.shadow_writes: bool = False
        self._backfilled: bool = False
        self._nedbd_mode: bool = nedbd_url is not None

        if self._nedbd_mode:
            # Route all NEDB operations to a running nedbd server
            self._db      = NedBdProxy(nedbd_url, db_name, token=nedbd_token)  # type: ignore[assignment]
            self._backend = None
        else:
            # In-process engine with Redis Stream persistence
            self._backend = RedisBackend(r, db_name)
            self._db = _NEDB()
            self._db._backend = self._backend
            self._reload()

    def _reload(self) -> None:
        if self._nedbd_mode:
            return  # nedbd manages its own state
        ops_json = self._backend.read_all()
        if not ops_json:
            return
        from .log import Op
        ops = []
        for s in ops_json:
            try:
                ops.append(Op.from_dict(json.loads(s)))
            except Exception:
                continue
        if ops:
            self._db.log.load(ops)
            from .engine import apply_op
            for op in self._db.log.ops:
                if op.op != "checkpoint":
                    apply_op(self._db.store, self._db.relations,
                             self._db.indexes, op, self._db.cause_map)
            self._db._nonce = dict(self._db.log._last_nonce)

    def _persist_last_op(self) -> None:
        if self._nedbd_mode:
            return  # nedbd persists atomically on each HTTP call
        if self._db.log.ops:
            last = self._db.log.ops[-1]
            self._backend.append(json.dumps(last.to_dict()))
            self._backend.publish_ops([json.dumps(last.to_dict())])

    # ── Collection registration ───────────────────────────────────────────────

    def register(
        self,
        pattern: str,
        collection: str,
        id_extractor: Optional[Callable[[str], str]] = None,
        value_parser: Optional[Callable[[Any], dict]] = None,
        value_type: str = "string",
    ) -> "NEDBSurface":
        """
        Register a Redis key glob pattern as a NEDB collection.

        After registering, backfill() can import existing keys and
        shadow_writes=True will auto-chain future writes.

        Args:
            pattern:       Redis key glob, e.g. "driver:*"
            collection:    NEDB collection name, e.g. "driver"
            id_extractor:  fn(key) → id. Default: key.rsplit(":", 1)[-1]
            value_parser:  fn(raw_value) → dict. Default: JSON / fallback
            value_type:    "string" | "hash" | "json" (hint for backfill)

        Returns self for chaining::

            (r.nedb
             .register("driver:*", "driver", value_parser=json.loads)
             .register("trip:*",   "trip",   value_type="hash")
             .backfill()
             )
        """
        self._mappings.append(CollectionMapping(
            pattern, collection, id_extractor, value_parser, value_type
        ))
        return self

    def _mapping_for(self, key: str) -> Optional[CollectionMapping]:
        for m in self._mappings:
            if m.matches(key):
                return m
        return None

    # ── Backfill ─────────────────────────────────────────────────────────────

    def backfill(
        self,
        pattern: Optional[str] = None,
        collection: Optional[str] = None,
        id_extractor: Optional[Callable[[str], str]] = None,
        value_parser: Optional[Callable[[Any], dict]] = None,
        value_type: str = "string",
        batch_size: int = 200,
    ) -> int:
        """
        Scan Alice's existing Redis keys and import them into NEDB once.

        If called without arguments, imports all registered patterns.
        Can also be called with explicit args to backfill one pattern.

        Args:
            pattern:     Redis key glob to scan. If None, uses all registered.
            collection:  NEDB collection name.
            id_extractor / value_parser: same as register().
            batch_size:  keys processed per SCAN cursor iteration.

        Returns:
            Number of keys imported.

        Example::

            # Register then backfill
            r.nedb.register("driver:*", "driver", value_parser=json.loads)
            count = r.nedb.backfill()
            print(f"Imported {count} existing driver records")

            # Or backfill directly (no register needed)
            r.nedb.backfill("driver:*", "driver",
                            value_parser=json.loads)
        """
        if pattern is not None:
            # Explicit one-shot backfill — register temporarily
            mappings_to_use = [CollectionMapping(
                pattern, collection or pattern.split(":")[0],
                id_extractor, value_parser, value_type
            )]
        else:
            mappings_to_use = list(self._mappings)

        if not mappings_to_use:
            return 0

        total = 0
        for mapping in mappings_to_use:
            count = self._backfill_one(mapping, batch_size)
            total += count

        self._backfilled = True
        return total

    def _backfill_one(self, mapping: CollectionMapping, batch_size: int) -> int:
        """Import all keys matching one mapping from Redis into NEDB."""
        count = 0
        cursor = 0
        while True:
            cursor, keys = self._r.scan(cursor, match=mapping.pattern,
                                        count=batch_size)
            for raw_key in keys:
                key = raw_key.decode() if isinstance(raw_key, bytes) else raw_key
                doc_id = mapping.extract_id(key)
                try:
                    if mapping.value_type == "hash":
                        raw_val = self._r.hgetall(key)
                    else:
                        raw_val = self._r.get(key)
                    if raw_val is None:
                        continue
                    doc = mapping.parse_value(raw_val)
                    doc.setdefault("_source", "backfill")
                    self._db.put(mapping.collection, doc_id, doc,
                                 client="__backfill__",
                                 evidence="backfill",
                                 confidence=1.0)
                    self._persist_last_op()
                    count += 1
                except Exception:
                    pass  # skip unreadable keys
            if cursor == 0:
                break
        return count

    # ── Write shadowing ───────────────────────────────────────────────────────

    def _shadow(self, cmd: str, args: tuple, kwargs: dict) -> None:
        """
        Shadow a Redis surface write into the NEDB chain.

        Two paths:
        1. If the key matches a registered collection: full NEDB put()
           (NQL-queryable, time-travel, causal).
        2. Otherwise: raw command op (tamper evidence only, not NQL-queryable).
        """
        if not args:
            return

        # Determine the key from the command args
        key = args[0]
        if isinstance(key, bytes):
            key = key.decode()

        mapping = self._mapping_for(key)

        if mapping is not None:
            # Full NEDB put — queryable via NQL
            doc_id = mapping.extract_id(key)
            try:
                if cmd == "hset":
                    # hset key field value OR hset key mapping={...}
                    if len(args) >= 3:
                        pairs = list(args[1:])
                        field_vals = {str(pairs[i]): str(pairs[i+1])
                                      for i in range(0, len(pairs)-1, 2)}
                    elif "mapping" in kwargs:
                        field_vals = {
                            (k.decode() if isinstance(k, bytes) else str(k)):
                            (v.decode() if isinstance(v, bytes) else str(v))
                            for k, v in kwargs["mapping"].items()
                        }
                    else:
                        return
                    # Merge with existing NEDB doc
                    existing = self._db.get(mapping.collection, doc_id) or {}
                    merged = {**existing, **field_vals}
                    raw_doc = mapping.parse_value(merged)
                elif cmd in ("set", "setex", "psetex", "setnx"):
                    raw_doc = mapping.parse_value(args[1] if len(args) > 1 else b"")
                elif cmd in ("incr", "incrby", "decr", "decrby"):
                    existing = self._db.get(mapping.collection, doc_id) or {}
                    raw_doc = {**existing, "_v": str(args[1] if len(args) > 1 else "")}
                else:
                    # For other write types: store the raw command as metadata
                    existing = self._db.get(mapping.collection, doc_id) or {}
                    raw_doc = {**existing,
                               f"_redis_{cmd}": str(args[1]) if len(args) > 1 else ""}
                raw_doc.setdefault("_source", "shadow")
                self._db.put(mapping.collection, doc_id, raw_doc,
                             client="__shadow__",
                             evidence="redis_write",
                             confidence=1.0)
                self._persist_last_op()
            except Exception:
                pass  # shadow failures must never break the Redis surface call
        else:
            # No mapping — raw tamper-evidence chain entry only
            # (not NQL-queryable, but proves the write happened)
            try:
                raw_op = {
                    "cmd":  cmd,
                    "key":  key,
                    "args": [str(a) for a in args[1:3]],  # limit size
                }
                self._db.put("__redis_shadow__", key,
                             {"cmd": cmd, "key": key, "_source": "shadow_raw"},
                             client="__shadow__",
                             evidence="redis_write")
                self._persist_last_op()
            except Exception:
                pass

    # ── Full NEDB API ─────────────────────────────────────────────────────────

    def create_index(self, coll: str, field: str, kind: str = "eq") -> None:
        self._db.create_index(coll, field, kind)

    def put(self, coll: str, id: str, doc: dict, **kw) -> dict:
        result = self._db.put(coll, id, doc, **kw)
        self._persist_last_op()
        return result

    def delete(self, coll: str, id: str, **kw) -> None:
        self._db.delete(coll, id, **kw)
        self._persist_last_op()

    def get(self, coll: str, id: str, as_of: Optional[int] = None):
        return self._db.get(coll, id, as_of)

    def get_as_of(self, coll: str, id: str, as_of: int):
        return self._db.get(coll, id, as_of)

    def query(self, nql: str) -> List[dict]:
        return self._db.query(nql)

    def link(self, frm: str, rel: str, to: str, **kw) -> None:
        self._db.link(frm, rel, to, **kw)
        self._persist_last_op()

    def unlink(self, frm: str, rel: str, to: str, **kw) -> None:
        self._db.unlink(frm, rel, to, **kw)
        self._persist_last_op()

    def neighbors(self, frm: str, rel: str, as_of: Optional[int] = None):
        return self._db.neighbors(frm, rel, as_of)

    def inbound(self, to: str, rel: str, as_of: Optional[int] = None):
        return self._db.inbound(to, rel, as_of)

    def verify(self) -> bool:
        return self._db.verify()

    def head(self) -> str:
        return self._db.head

    @property
    def seq(self) -> int:
        return self._db.seq

    def checkpoint(self) -> str:
        return self._db.checkpoint()


# ── WrappedRedis ──────────────────────────────────────────────────────────────

class WrappedRedis:
    """
    Transparent Redis proxy with NEDB shadow layer.

    Surface 1 (r.set/get/hset/…): every Redis command passes through unchanged.
    Surface 2 (r.nedb.*): full NEDB API + backfill + write shadowing.

    nedbd mode::

        r = wrap_redis(redis.Redis(...), db_name="rideshare",
                       nedbd_url="http://localhost:8421",
                       nedbd_token="secret")   # token optional
    """

    def __init__(self, r: Any, db_name: str,
                 nedbd_url: Optional[str] = None,
                 nedbd_token: Optional[str] = None):
        object.__setattr__(self, "_r",       r)
        object.__setattr__(self, "_db_name", db_name)
        object.__setattr__(self, "nedb",
                           NEDBSurface(r, db_name,
                                       nedbd_url=nedbd_url,
                                       nedbd_token=nedbd_token))

    def __getattr__(self, name: str) -> Any:
        r    = object.__getattribute__(self, "_r")
        nedb = object.__getattribute__(self, "nedb")
        attr = getattr(r, name)
        if not callable(attr) or name not in _WRITE_CMDS:
            return attr
        # Wrap write commands so we can shadow them when shadow_writes=True
        def _intercepted(*args, **kwargs):
            result = attr(*args, **kwargs)
            if nedb.shadow_writes:
                nedb._shadow(name, args, kwargs)
            return result
        return _intercepted

    def __repr__(self) -> str:
        r  = object.__getattribute__(self, "_r")
        db = object.__getattribute__(self, "_db_name")
        return f"<WrappedRedis db_name={db!r} redis={r!r}>"


def wrap_redis(r: Any, db_name: str = "default",
               nedbd_url: Optional[str] = None,
               nedbd_token: Optional[str] = None) -> WrappedRedis:
    """
    Wrap an existing Redis connection with NEDB's layer-2 features.

    Args:
        r:        An existing ``redis.Redis`` (or compatible) connection.
        db_name:  Logical database name. NEDB uses ``nedb:{db_name}:*``.

    Args:
        r:           An existing ``redis.Redis`` (or compatible) connection.
        db_name:     Logical database name. NEDB uses ``nedb:{db_name}:*``.
        nedbd_url:   Optional URL of a running nedbd server, e.g.
                     ``"http://localhost:8421"``.  When set, all ``r.nedb.*``
                     calls are forwarded to nedbd instead of running in-process.
                     nedbd handles its own durable AOF persistence on disk.
        nedbd_token: Optional bearer token for nedbd authentication
                     (set via ``NEDBD_TOKEN`` env on the server).

    Returns:
        A ``WrappedRedis`` with ``.nedb`` for the full NEDB API.

    Quick-start::

        from nedb import wrap_redis
        import redis, json

        r = wrap_redis(redis.Redis("localhost", 6379), db_name="rideshare")

        # Register key patterns → NEDB collections
        r.nedb.register("driver:*", "driver", value_parser=json.loads)
        r.nedb.register("trip:*",   "trip",   value_type="hash")

        # Import all existing Redis data into NEDB (one-time)
        imported = r.nedb.backfill()

        # Enable write shadowing — all future r.set/hset/... auto-chain
        r.nedb.shadow_writes = True

        # Existing app — unchanged
        r.set("driver:d5", json.dumps({"name": "Fiona", "status": "active"}))

        # New app — NEDB features
        r.nedb.query('FROM driver WHERE status = "active" ORDER BY name ASC')
        r.nedb.verify()   # → True
    """
    return WrappedRedis(r, db_name, nedbd_url=nedbd_url, nedbd_token=nedbd_token)
