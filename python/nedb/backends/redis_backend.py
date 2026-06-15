"""
nedb.backends.redis_backend — Redis Streams as the NEDB append-only log.

Alice's existing Redis keys are NEVER touched. NEDB operates in a strictly
isolated namespace:

    nedb:{db_name}:oplog       Redis Stream  — hash-chained op log
    nedb:{db_name}:snapshot    Redis Hash    — checkpoint for fast restart
    nedb:{db_name}:events      Pub/Sub chan  — live subscriptions (future)
    nedb:{db_name}:meta        Redis Hash    — version, index config

On startup NEDB replays the stream to rebuild its in-memory MVCC store.
On every write a new entry is XADD'd. One Redis connection, zero impact on
the user's existing keys.

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


class RedisBackend:
    """
    Redis-Streams-backed persistence for NEDB.

    Pass an instance to NEDB as the `backend` parameter::

        import redis
        from nedb.backends.redis_backend import RedisBackend
        from nedb import NEDB

        r = redis.Redis("localhost", 6379)
        db = NEDB(backend=RedisBackend(r, "rideshare"))
    """

    def __init__(self, r: Any, db_name: str):
        self._r        = r
        self.db_name   = db_name
        self.stream    = f"nedb:{db_name}:oplog"
        self.snap_key  = f"nedb:{db_name}:snapshot"
        self.meta_key  = f"nedb:{db_name}:meta"
        self.events_ch = f"nedb:{db_name}:events"

    # ── Op log ──────────────────────────────────────────────────────────────────

    def append(self, op_json: str) -> None:
        """Append one JSON-serialised op to the stream."""
        self._r.xadd(self.stream, {"op": op_json})

    def append_batch(self, ops: List[str]) -> None:
        """Append multiple ops in a single pipeline (one round-trip)."""
        pipe = self._r.pipeline(transaction=False)
        for op_json in ops:
            pipe.xadd(self.stream, {"op": op_json})
        pipe.execute()

    def read_all(self) -> List[str]:
        """Return all ops from the stream in insertion order."""
        entries = self._r.xrange(self.stream, "-", "+")
        return [e[1][b"op"].decode() for e in entries]

    def read_after(self, last_id: str = "0") -> List[str]:
        """Return ops appended after `last_id` (for incremental replay)."""
        entries = self._r.xrange(self.stream, f"({last_id}", "+")
        return [e[1][b"op"].decode() for e in entries]

    # ── Snapshot / checkpoint ────────────────────────────────────────────────────

    def save_snapshot(self, data: Dict[str, Any]) -> None:
        """Persist a checkpoint so restart replay only needs the delta."""
        self._r.hset(self.snap_key, mapping={
            k: json.dumps(v, separators=(",", ":"), default=str)
            for k, v in data.items()
        })

    def load_snapshot(self) -> Optional[Dict[str, Any]]:
        """Load the last checkpoint, or None if none exists."""
        raw = self._r.hgetall(self.snap_key)
        if not raw:
            return None
        return {k.decode(): json.loads(v) for k, v in raw.items()}

    # ── Pub/sub live events ──────────────────────────────────────────────────────

    def publish_ops(self, ops: List[str]) -> None:
        """Publish committed ops to the events channel for live subscribers."""
        if ops:
            payload = json.dumps(ops, separators=(",", ":"))
            self._r.publish(self.events_ch, payload)

    # ── Meta ─────────────────────────────────────────────────────────────────────

    def save_meta(self, meta: Dict[str, Any]) -> None:
        self._r.hset(self.meta_key, mapping={
            k: json.dumps(v, separators=(",", ":"), default=str)
            for k, v in meta.items()
        })

    def load_meta(self) -> Dict[str, Any]:
        raw = self._r.hgetall(self.meta_key)
        if not raw:
            return {}
        return {k.decode(): json.loads(v) for k, v in raw.items()}

    # ── Utility ──────────────────────────────────────────────────────────────────

    def stream_len(self) -> int:
        return self._r.xlen(self.stream)

    def flush(self) -> None:
        """Delete all NEDB shadow keys for this database (non-destructive to user keys)."""
        for key in [self.stream, self.snap_key, self.meta_key]:
            self._r.delete(key)
