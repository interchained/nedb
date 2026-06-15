"""
nedb.wrap_redis — wrap an existing Redis connection with NEDB's layer-2.

ONE LINE. Alice's existing app doesn't change. New parts of her app get
time-travel, bi-temporal, causal provenance, and NQL.

Usage::

    from nedb import wrap_redis
    import redis

    r = wrap_redis(redis.Redis("localhost", 6379), db_name="rideshare")

    # Surface 1: every Redis command passes through unchanged
    r.set("driver:d1", '{"name":"Bob","status":"active"}')
    r.hset("trip:t1", mapping={"rider_id": "u1", "status": "matching"})
    r.sadd("drivers:online", "d1", "d2")

    # Surface 2: NEDB features on the same connection + data
    r.nedb.create_index("driver", "status", "eq")
    r.nedb.put("driver", "d1", {"name": "Bob", "status": "active"},
               caused_by=[r.nedb.seq], evidence="location_update")

    r.nedb.query('FROM driver WHERE status = "active" ORDER BY name ASC')
    r.nedb.query('FROM driver WHERE _id = "d1" TRACE caused_by')
    r.nedb.get_as_of("driver", "d1", as_of=r.nedb.seq - 3)  # time-travel
    r.nedb.verify()   # → True (hash chain intact)

Isolation guarantee: NEDB NEVER writes to Alice's namespace. It owns only:
    nedb:{db_name}:oplog      Redis Stream  (op log)
    nedb:{db_name}:snapshot   Redis Hash    (checkpoint)
    nedb:{db_name}:events     Pub/Sub       (live subs, future)
    nedb:{db_name}:meta       Redis Hash    (index config)

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
from __future__ import annotations

import json
import time
from typing import Any, List, Optional

from .engine import NEDB as _NEDB
from .backends.redis_backend import RedisBackend


# Redis write commands whose args we shadow into the NEDB op log.
# This lets the tamper-evident proxy (Idea 3) chain ALL Redis writes,
# not just ones made through r.nedb.
_WRITE_CMDS = frozenset({
    "set", "setnx", "setex", "psetex", "getset", "mset", "msetnx",
    "hset", "hmset", "hsetnx", "hincrby", "hincrbyfloat",
    "lpush", "rpush", "lset", "linsert", "ltrim",
    "sadd", "smove", "srem",
    "zadd", "zincrby", "zrem", "zremrangebyscore", "zremrangebyrank",
    "del", "delete", "unlink", "rename", "renamenx",
    "append", "incr", "incrby", "decr", "decrby",
    "xadd",   # except our own shadow — handled separately
    "setrange",
})


class NEDBSurface:
    """
    The `r.nedb` attribute on a WrappedRedis — full NEDB feature access.

    This is a thin façade over an in-memory NEDB instance whose persistence
    layer is backed by the same Redis connection (RedisBackend).
    """

    def __init__(self, r: Any, db_name: str):
        backend = RedisBackend(r, db_name)
        self._backend = backend
        # Build the NEDB engine and replay the op log from Redis
        self._db = _NEDB.__new__(_NEDB)
        self._db.__init__()          # in-memory baseline
        self._db._backend = backend  # attach backend for future flushes
        self._db_name = db_name
        self._r = r
        self._reload()

    def _reload(self) -> None:
        """Replay the Redis Stream to rebuild MVCC state."""
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
                if op.op not in ("checkpoint",):
                    apply_op(self._db.store, self._db.relations,
                             self._db.indexes, op, self._db.cause_map)
            self._db._nonce = dict(self._db.log._last_nonce)

    # ── Delegate the full NEDB API ───────────────────────────────────────────────

    def create_index(self, coll: str, field: str, kind: str = "eq") -> None:
        self._db.create_index(coll, field, kind)

    def put(self, coll: str, id: str, doc: dict, **kw) -> dict:
        result = self._db.put(coll, id, doc, **kw)
        # Persist the new op to Redis
        last_op = self._db.log.ops[-1]
        self._backend.append(json.dumps(last_op.to_dict()))
        self._backend.publish_ops([json.dumps(last_op.to_dict())])
        return result

    def delete(self, coll: str, id: str, **kw) -> None:
        self._db.delete(coll, id, **kw)
        last_op = self._db.log.ops[-1]
        self._backend.append(json.dumps(last_op.to_dict()))

    def get(self, coll: str, id: str, as_of: Optional[int] = None):
        return self._db.get(coll, id, as_of)

    def get_as_of(self, coll: str, id: str, as_of: int):
        return self._db.get(coll, id, as_of)

    def query(self, nql: str) -> List[dict]:
        return self._db.query(nql)

    def link(self, frm: str, rel: str, to: str, **kw) -> None:
        self._db.link(frm, rel, to, **kw)
        last_op = self._db.log.ops[-1]
        self._backend.append(json.dumps(last_op.to_dict()))

    def unlink(self, frm: str, rel: str, to: str, **kw) -> None:
        self._db.unlink(frm, rel, to, **kw)
        last_op = self._db.log.ops[-1]
        self._backend.append(json.dumps(last_op.to_dict()))

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


class WrappedRedis:
    """
    Transparent Redis proxy with NEDB shadow layer.

    Every standard Redis command passes through unchanged. `r.nedb` exposes
    the full NEDB API on the same connection. NEDB's shadow keys never
    appear in Alice's namespace.
    """

    def __init__(self, r: Any, db_name: str):
        # Store on object dict directly to avoid __getattr__ recursion
        object.__setattr__(self, "_r", r)
        object.__setattr__(self, "_db_name", db_name)
        object.__setattr__(self, "_stream", f"nedb:{db_name}:oplog")
        object.__setattr__(self, "nedb", NEDBSurface(r, db_name))

    def __getattr__(self, name: str) -> Any:
        """Pass every attribute/method through to the underlying Redis client."""
        return getattr(object.__getattribute__(self, "_r"), name)

    def __repr__(self) -> str:
        r = object.__getattribute__(self, "_r")
        db = object.__getattribute__(self, "_db_name")
        return f"<WrappedRedis db_name={db!r} redis={r!r}>"


def wrap_redis(r: Any, db_name: str = "default") -> WrappedRedis:
    """
    Wrap an existing Redis connection with NEDB's layer-2 features.

    Args:
        r:        An existing ``redis.Redis`` (or compatible) connection.
        db_name:  Logical database name. NEDB uses ``nedb:{db_name}:*`` as
                  its shadow namespace — Alice's keys are never touched.

    Returns:
        A ``WrappedRedis`` that:
        - Passes all standard Redis commands through unchanged (surface 1)
        - Exposes ``.nedb`` for time-travel, NQL, causal provenance (surface 2)

    Example::

        from nedb import wrap_redis
        import redis

        r = wrap_redis(redis.Redis("localhost", 6379), db_name="rideshare")
        r.set("driver:d1", '{"name":"Bob"}')    # unchanged Redis
        r.nedb.query("FROM driver LIMIT 5")      # NEDB surface
    """
    return WrappedRedis(r, db_name)
