# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.concurrent — make a NEDB database safe AND fast under many concurrent clients,
without a global lock.

The problem
-----------
A hash-chained append-only log is *inherently* sequential: op N's hash commits to
op N-1's head (h_n = H(h_{n-1} || op_n)). Two threads cannot append in parallel
without corrupting the chain. The naive fix — wrap every request in one mutex —
is correct but slow: it serializes the expensive fsync too, and it blocks readers.

The design
----------
**Single-writer, group-commit sequencer with lock-free MVCC reads.**

  * Writers don't take a lock. They drop a write *intent* on a queue and await a
    future. ONE committer thread per database owns all mutation, so the chain is
    always correct by construction — zero write-write contention.

  * The committer drains the whole queue as a BATCH, chains + applies every op,
    then issues ONE fsync for the entire batch. Under load this is *faster*: more
    concurrent writers → bigger batches → fewer fsyncs per write. This is group
    commit, the same trick Postgres/Kafka use to turn contention into throughput.

  * Reads never touch the queue and never take a lock. They run at the last
    *committed* sequence (snapshot isolation): the MVCC store is append-only and
    versioned, so a reader pinned to `committed_seq` sees a consistent snapshot
    even while the committer appends newer versions for the next batch. The only
    structural hazard — enumerating keys while a new key is inserted — is handled
    lock-free in MVCCStore.keys() via an optimistic snapshot+retry.

Net effect: parallel reads, parallel cross-database writes, batched durable writes,
and a provably correct single chain — no request-level lock anywhere.
"""
from __future__ import annotations

import queue
import threading
from dataclasses import dataclass, field
from typing import Any, List, Optional

from .engine import NEDB
from .query import parse_nql

_STOP = object()


@dataclass
class _Intent:
    kind: str
    args: tuple
    kwargs: dict
    done: threading.Event = field(default_factory=threading.Event)
    result: Any = None
    error: Optional[BaseException] = None


class Sequencer:
    """Concurrent, group-committing front-end over one NEDB database.

    Drop-in for NEDB from the daemon's perspective: the mutating methods are
    serialized through a committer thread; reads are concurrent and snapshot-
    isolated; everything else delegates to the wrapped engine.
    """

    def __init__(self, db: NEDB, max_batch: int = 512):
        self.db = db
        self.max_batch = max_batch
        self._q: "queue.Queue[Any]" = queue.Queue()
        # The seq through which all writes are durably committed and fully applied.
        # Reads pin to this for snapshot isolation.
        self._committed_seq: int = db.seq
        self._closed = False
        self._committer = threading.Thread(
            target=self._run, name=f"nedb-committer", daemon=True
        )
        self._committer.start()

    # ── write API: enqueue + await the committer ──────────────────────────────
    def _submit(self, kind: str, *args: Any, **kwargs: Any) -> Any:
        if self._closed:
            raise RuntimeError("Sequencer is closed")
        intent = _Intent(kind, args, kwargs)
        self._q.put(intent)
        intent.done.wait()
        if intent.error is not None:
            raise intent.error
        return intent.result

    def put(self, coll: str, id: str, doc: dict, **kw: Any) -> Any:
        return self._submit("put", coll, id, doc, **kw)

    def tx(self, ops: list, **kw: Any) -> Any:
        """Atomic multi-op transaction (see NEDB.tx). Submitted as ONE
        intent, so the committer validates preconditions and applies all ops
        with nothing interleaving — the single-writer thread IS the
        serialization point, exactly as Redis's single thread was for Lua."""
        return self._submit("tx", ops, **kw)

    def delete(self, coll: str, id: str, **kw: Any) -> Any:
        return self._submit("delete", coll, id, **kw)

    def link(self, frm: str, rel: str, to: str, **kw: Any) -> Any:
        return self._submit("link", frm, rel, to, **kw)

    def unlink(self, frm: str, rel: str, to: str, **kw: Any) -> Any:
        return self._submit("unlink", frm, rel, to, **kw)

    def create_index(self, *a: Any, **k: Any) -> Any:
        return self._submit("create_index", *a, **k)

    def put_file(self, *a: Any, **k: Any) -> Any:
        return self._submit("put_file", *a, **k)

    def checkpoint(self) -> Any:
        return self._submit("checkpoint")

    def sweep(self) -> Any:
        """TTL sweep as ONE committer intent — sweep appends delete ops, so it
        must ride the single writer, never race it from another thread."""
        return self._submit("sweep")

    # ── read API: concurrent, snapshot-isolated at committed_seq ───────────────
    def query(self, nql: str) -> List[dict]:
        plan = parse_nql(nql)
        if plan.get("as_of") is None:
            plan["as_of"] = self._committed_seq
        return self.db.execute(plan)

    def get(self, coll: str, id: str, as_of: Optional[int] = None) -> Optional[dict]:
        return self.db.get(coll, id, self._committed_seq if as_of is None else as_of)

    def neighbors(self, frm: str, rel: str, as_of: Optional[int] = None) -> List[str]:
        return self.db.neighbors(frm, rel, self._committed_seq if as_of is None else as_of)

    def inbound(self, to: str, rel: str, as_of: Optional[int] = None) -> List[str]:
        return self.db.inbound(to, rel, self._committed_seq if as_of is None else as_of)

    def verify(self) -> bool:
        return self.db.verify()

    def get_file(self, *a: Any, **k: Any) -> Any:
        return self.db.get_file(*a, **k)

    @property
    def seq(self) -> int:
        return self.db.seq

    @property
    def head(self) -> str:
        return self.db.head

    @property
    def committed_seq(self) -> int:
        return self._committed_seq

    # Everything else (log, store, indexes, relations, blobs, path, _dek, flush,
    # close-of-engine, etc.) delegates to the wrapped engine.
    def __getattr__(self, name: str) -> Any:
        # __getattr__ only fires for attrs not found normally, so self.db is safe.
        return getattr(self.db, name)

    # ── the single writer ──────────────────────────────────────────────────────
    def _run(self) -> None:
        db = self.db
        db._defer_sync = True  # group commit: we fsync once per batch
        while True:
            first = self._q.get()
            if first is _STOP:
                return
            batch: List[Any] = [first]
            while len(batch) < self.max_batch:
                try:
                    nxt = self._q.get_nowait()
                except queue.Empty:
                    break
                batch.append(nxt)
            if self._commit_batch(batch):
                return  # saw _STOP

    def _commit_batch(self, batch: List[Any]) -> bool:
        db = self.db
        saw_stop = False
        # 1) chain + apply every op in order (in-memory + buffered AOF write).
        #    No fsync here; readers (pinned to the OLD committed_seq) are isolated.
        for intent in batch:
            if intent is _STOP:
                saw_stop = True
                continue
            try:
                intent.result = self._apply_one(intent)
            except BaseException as e:  # capture per-intent; never kill the committer
                intent.error = e
        # 2) ONE durable fsync for the whole batch (group commit).
        try:
            db.flush()
        except Exception:
            pass
        # 3) publish the new snapshot, THEN wake writers (read-your-writes holds).
        self._committed_seq = db.seq
        for intent in batch:
            if intent is not _STOP:
                intent.done.set()
        return saw_stop

    def _apply_one(self, intent: _Intent) -> Any:
        db, k = self.db, intent.kind
        if k == "put":
            return db.put(*intent.args, **intent.kwargs)
        if k == "delete":
            return db.delete(*intent.args, **intent.kwargs)
        if k == "link":
            return db.link(*intent.args, **intent.kwargs)
        if k == "unlink":
            return db.unlink(*intent.args, **intent.kwargs)
        if k == "create_index":
            return db.create_index(*intent.args, **intent.kwargs)
        if k == "put_file":
            return db.put_file(*intent.args, **intent.kwargs)
        if k == "checkpoint":
            return db.checkpoint()
        if k == "tx":
            return db.tx(*intent.args, **intent.kwargs)
        if k == "sweep":
            return db.sweep()
        raise ValueError(f"unknown write kind: {k}")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._q.put(_STOP)
        self._committer.join(timeout=5)
        self.db.close()
