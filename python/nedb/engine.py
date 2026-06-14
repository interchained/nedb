"""
nedb.engine — the NEDB database: log + MVCC store + relations + indexes + Cascade.

The OpLog is the source of truth. Every mutation appends an Op; `_apply` deterministically
folds an Op into the materialized state (store / relations / indexes). Because state is a
pure function of the log, we get crash recovery and determinism (rebuild) for free, and
"AS OF seq" time-travel because the log carries monotonic seqs.
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from .cascade import BlobStore
from . import snapshot as _snap
from .index import Indexes, tokenize
from .log import Op, OpLog, ReplayError  # noqa: F401  (re-exported)
from .merkle import merkle_proof, merkle_verify
from .query import Query, cmp, parse_nql
from .relations import Relations
from .store import MVCCStore


def apply_op(store: MVCCStore, relations: Relations, indexes: Indexes, op: Op) -> None:
    """Deterministically fold one op into materialized state."""
    p = op.payload
    if op.op == "put":
        key, coll, doc = p["key"], p["coll"], p["doc"]
        old = store.get(key)
        if old is not None:
            indexes.remove(coll, key, old)
        store.put(key, doc, op.seq)
        indexes.add(coll, key, doc)
    elif op.op == "delete":
        key, coll = p["key"], p["coll"]
        old = store.get(key)
        if old is not None:
            indexes.remove(coll, key, old)
        store.delete(key, op.seq)
    elif op.op == "link":
        relations.link(p["frm"], p["rel"], p["to"], op.seq)
    elif op.op == "unlink":
        relations.unlink(p["frm"], p["rel"], p["to"], op.seq)
    elif op.op == "put_file":
        pass  # bytes live in the content-addressed BlobStore; log records the root only


class NEDB:
    def __init__(self, path: Optional[str] = None) -> None:
        """Create a database.

        With no `path`, NEDB is in-memory (the original behavior). With a `path`
        (a directory), NEDB is DURABLE: every op is appended to a hash-chained
        append-only log file (AOF) and fsync'd, and the database reloads by
        replaying that log on open — Redis-style persistence, except the log is
        the same tamper-evident chain the engine already treats as the source of
        truth, so verify() and AS OF hold across restarts. The append-only log is
        never rewritten: the chain (and its anchorable head) stays provable.
        """
        self.log = OpLog()
        self.store = MVCCStore()
        self.relations = Relations()
        self.indexes = Indexes()
        self.blobs: Dict[str, BlobStore] = {"warm": BlobStore("warm"), "cold": BlobStore("cold")}
        self._nonce: Dict[str, int] = {}

        self.path = path
        self._aof = None
        if path is not None:
            self._open(path)

    # --- persistence (AOF) --------------------------------------------------
    def _open(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)
        self._aof_path = os.path.join(path, "log.aof")
        self._meta_path = os.path.join(path, "meta.json")
        if os.path.exists(self._aof_path) or os.path.exists(self._meta_path):
            self._load()
        # Append mode: never truncates the existing log.
        self._aof = open(self._aof_path, "a", encoding="utf-8")

    def _load(self) -> None:
        # ── Try snapshot-assisted load first (O(delta) instead of O(total)) ─
        snap_seq = _snap.load_snapshot(self)
        if snap_seq >= 0:
            # Snapshot loaded: only replay AOF ops AFTER the checkpoint op.
            ops: List[Op] = []
            if os.path.exists(self._aof_path):
                with open(self._aof_path, encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if line:
                            ops.append(Op.from_dict(json.loads(line)))
            # Build the full log (needed for verify() and AS OF) but only
            # apply ops that arrive after the checkpoint to avoid double-fold.
            self.log.load(ops)
            for op in self.log.ops:
                if op.seq > snap_seq:
                    apply_op(self.store, self.relations, self.indexes, op)
            self._nonce = dict(self.log._last_nonce)
            return

        # ── No snapshot: full replay (original behaviour) ─────────────────
        # 1) index configuration
        if os.path.exists(self._meta_path):
            with open(self._meta_path, encoding="utf-8") as fh:
                for coll, field, kind in json.load(fh).get("indexes", []):
                    self.indexes.ensure(coll, field, kind)
        # 2) the hash-chained op log
        ops = []
        if os.path.exists(self._aof_path):
            with open(self._aof_path, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        ops.append(Op.from_dict(json.loads(line)))
        self.log.load(ops)
        # 3) fold
        for op in self.log.ops:
            apply_op(self.store, self.relations, self.indexes, op)
        # 4) nonce restoration
        self._nonce = dict(self.log._last_nonce)

    def _persist_meta(self) -> None:
        if self.path is None:
            return
        with open(self._meta_path, "w", encoding="utf-8") as fh:
            json.dump({"indexes": [list(t) for t in self.indexes.config]}, fh)

    def _log_append(self, client: str, nonce: int, op: str, payload: dict,
                    idem: Optional[str] = None):
        """Append to the in-memory log AND, if durable, to the AOF (fsync'd)."""
        rec, created = self.log.append(client, nonce, op, payload, idem)
        if created and self._aof is not None:
            self._aof.write(json.dumps(rec.to_dict()) + "\n")
            self._aof.flush()
            os.fsync(self._aof.fileno())
        return rec, created

    def flush(self) -> None:
        """Force buffered writes to disk."""
        if self._aof is not None:
            self._aof.flush()
            os.fsync(self._aof.fileno())

    def close(self) -> None:
        """Flush and close the append-only log."""
        if self._aof is not None:
            self._aof.flush()
            os.fsync(self._aof.fileno())
            self._aof.close()
            self._aof = None

    def __enter__(self) -> "NEDB":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def checkpoint(self) -> str:
        """
        Capture a snapshot checkpoint and anchor it in the hash chain.

        Writes ``snapshot.json`` alongside the AOF so future opens load in
        O(delta) time instead of replaying the full log. The chain is never
        broken — the checkpoint is a real op in the AOF whose hash chains
        from the previous op, so ``verify()`` and ``AS OF`` remain valid.

        Returns the head hash after the checkpoint op.

        Example::

            db = NEDB("./data")
            # … write 100 K rows …
            db.checkpoint()   # O(total) once; future opens are O(delta)
            db.close()
            db2 = NEDB("./data")  # fast: loads snapshot then replays only new ops
            assert db2.verify()

        Call periodically for long-running databases or before a planned restart.
        """
        return _snap.save_snapshot(self)

    # --- nonce helper -------------------------------------------------------
    def _next(self, client: str) -> int:
        n = self._nonce.get(client, 0) + 1
        self._nonce[client] = n
        return n

    # --- TTL helpers --------------------------------------------------------
    @staticmethod
    def _embed_ttl(doc: dict, ttl_s: Optional[float]) -> dict:
        if ttl_s is None:
            return doc
        import time
        d = dict(doc)
        d["_expires_at"] = time.time() + ttl_s
        return d

    def _check_ttl(self, coll: str, id: str, doc: Optional[dict]) -> Optional[dict]:
        """Lazy expiry: if the doc has _expires_at and it has passed, delete it."""
        if doc is None:
            return None
        exp = doc.get("_expires_at")
        if exp is None:
            return doc
        import time
        if time.time() > exp:
            key = f"{coll}:{id}"
            self._log_append("__ttl__", self._next("__ttl__"), "delete",
                             {"key": key, "coll": coll, "id": id})
            apply_op(self.store, self.relations, self.indexes,
                     self.log.ops[-1])
            return None
        return doc

    # --- mutations ----------------------------------------------------------
    def put(self, coll: str, id: str, doc: dict, client: str = "local",
            nonce: Optional[int] = None, idem: Optional[str] = None,
            ttl_s: Optional[float] = None) -> dict:
        key = f"{coll}:{id}"
        doc = dict(doc)
        doc.setdefault("_id", id)
        doc = self._embed_ttl(doc, ttl_s)
        nonce = self._next(client) if nonce is None else nonce
        op, created = self._log_append(client, nonce, "put",
                                       {"key": key, "coll": coll, "id": id, "doc": doc}, idem)
        if created:
            apply_op(self.store, self.relations, self.indexes, op)
        return self.store.get(key)

    def delete(self, coll: str, id: str, client: str = "local",
               nonce: Optional[int] = None, idem: Optional[str] = None) -> None:
        key = f"{coll}:{id}"
        nonce = self._next(client) if nonce is None else nonce
        op, created = self._log_append(client, nonce, "delete",
                                       {"key": key, "coll": coll, "id": id}, idem)
        if created:
            apply_op(self.store, self.relations, self.indexes, op)

    def get(self, coll: str, id: str, as_of: Optional[int] = None) -> Optional[dict]:
        doc = self.store.get(f"{coll}:{id}", as_of)
        if as_of is None:
            return self._check_ttl(coll, id, doc)
        return doc  # time-travel reads never trigger lazy expiry

    def expire(self, coll: str, id: str, ttl_s: float) -> bool:
        """Set or update the TTL on an existing document. Returns False if not found."""
        doc = self.store.get(f"{coll}:{id}")
        if doc is None:
            return False
        self.put(coll, id, doc, ttl_s=ttl_s)
        return True

    def sweep(self) -> int:
        """Delete all documents whose TTL has expired. Returns the count deleted."""
        import time
        now = time.time()
        deleted = 0
        for key in list(self.store.keys()):
            doc = self.store.get(key)
            if doc and isinstance(doc, dict) and doc.get("_expires_at") and now > doc["_expires_at"]:
                coll, id_ = key.split(":", 1)
                self._log_append("__ttl__", self._next("__ttl__"), "delete",
                                 {"key": key, "coll": coll, "id": id_})
                apply_op(self.store, self.relations, self.indexes, self.log.ops[-1])
                deleted += 1
        if deleted and self.path:
            self._persist_meta()
        return deleted

    # --- relations ----------------------------------------------------------
    def link(self, frm: str, rel: str, to: str, client: str = "local",
             nonce: Optional[int] = None) -> None:
        nonce = self._next(client) if nonce is None else nonce
        op, created = self._log_append(client, nonce, "link", {"frm": frm, "rel": rel, "to": to})
        if created:
            apply_op(self.store, self.relations, self.indexes, op)

    def unlink(self, frm: str, rel: str, to: str, client: str = "local",
               nonce: Optional[int] = None) -> None:
        nonce = self._next(client) if nonce is None else nonce
        op, created = self._log_append(client, nonce, "unlink", {"frm": frm, "rel": rel, "to": to})
        if created:
            apply_op(self.store, self.relations, self.indexes, op)

    def neighbors(self, frm: str, rel: str, as_of: Optional[int] = None) -> List[str]:
        return self.relations.neighbors(frm, rel, as_of)

    def inbound(self, to: str, rel: str, as_of: Optional[int] = None) -> List[str]:
        return self.relations.inbound(to, rel, as_of)

    # --- indexes ------------------------------------------------------------
    def create_index(self, coll: str, field: str, kind: str = "eq") -> None:
        self.indexes.ensure(coll, field, kind)
        # backfill existing rows at HEAD
        for key in self.store.keys(coll + ":"):
            doc = self.store.get(key)
            if doc is not None:
                self.indexes.add(coll, key, doc)
        # index config isn't an op-log entry, so snapshot it for durable reload
        self._persist_meta()

    # --- queries ------------------------------------------------------------
    def q(self, coll: str) -> Query:
        return Query(self, coll)

    def query(self, nql: str) -> List[dict]:
        return self.execute(parse_nql(nql))

    def execute(self, plan: dict) -> List[dict]:
        coll = plan["from"]
        as_of = plan.get("as_of")
        prefix = coll + ":"
        where = plan.get("where", [])
        search = plan.get("search")

        candidates: Optional[set] = None

        # 1) full-text search is usually most selective
        if search:
            sfields = self.indexes.search_fields(coll)
            if sfields:
                per_term = []
                for term in tokenize(search):
                    s: set = set()
                    for f in sfields:
                        s |= self.indexes.search_lookup(coll, f, term)
                    per_term.append(s)
                candidates = set.intersection(*per_term) if per_term else set()

        # 2) equality-index acceleration (HEAD reads only)
        if candidates is None and as_of is None:
            for (f, op, v) in where:
                if op == "=" and self.indexes.has_eq(coll, f):
                    candidates = self.indexes.eq_lookup(coll, f, v)
                    break

        # 3) fallback: scan the collection
        if candidates is None:
            candidates = set(self.store.keys(prefix, as_of))

        # load + final predicate filter (guarantees correctness regardless of index path)
        rows = []
        for key in candidates:
            doc = self.store.get(key, as_of)
            if doc is None:
                continue
            if all(cmp(doc.get(f), op, v) for (f, op, v) in where):
                if search and not self.indexes.search_fields(coll):
                    blob = " ".join(str(x) for x in doc.values()).lower()
                    if not all(t in blob for t in tokenize(search)):
                        continue
                rows.append((key, doc))

        # order
        ob = plan.get("order_by")
        if ob:
            field, direction = ob
            try:
                rows.sort(key=lambda kv: (kv[1].get(field) is None, kv[1].get(field)),
                          reverse=(direction == "DESC"))
            except TypeError:
                rows.sort(key=lambda kv: str(kv[1].get(field)), reverse=(direction == "DESC"))

        # traverse relations
        if plan.get("traverse"):
            rel = plan["traverse"]
            seen, trav = set(), []
            for key, _ in rows:
                for nb in self.relations.neighbors(key, rel, as_of):
                    if nb in seen:
                        continue
                    seen.add(nb)
                    d = self.store.get(nb, as_of)
                    if d is not None:
                        trav.append((nb, d))
            rows = trav

        if plan.get("limit") is not None:
            rows = rows[: plan["limit"]]

        result = [d for _, d in rows]

        # GROUP BY [COUNT | SUM f | AVG f | MIN f | MAX f]
        if plan.get("group_by"):
            gb_field = plan["group_by"]
            agg      = plan.get("aggregate")
            groups: dict = {}
            for d in result:
                gkey = d.get(gb_field)
                groups.setdefault(gkey, []).append(d)
            grouped = []
            for gval, gdocs in groups.items():
                entry: dict = {gb_field: gval, "count": len(gdocs)}
                if agg:
                    fn, af = agg
                    if fn == "count":
                        pass  # already in entry["count"]
                    else:
                        nums = [d[af] for d in gdocs if af in d and isinstance(d[af], (int, float))]
                        if fn == "sum":
                            entry[f"sum_{af}"] = sum(nums)
                        elif fn == "avg":
                            entry[f"avg_{af}"] = sum(nums) / len(nums) if nums else None
                        elif fn == "min":
                            entry[f"min_{af}"] = min(nums) if nums else None
                        elif fn == "max":
                            entry[f"max_{af}"] = max(nums) if nums else None
                grouped.append(entry)
            return grouped

        return result

    # --- files (git-style, Cascade-compressed) ------------------------------
    def put_file(self, name: str, data: bytes, tier: str = "warm", client: str = "local",
                 nonce: Optional[int] = None, idem: Optional[str] = None) -> int:
        """Store a file version (Cascade-compressed, deduplicated). Returns the
        integer version index; fetch its anchorable hash via file_root(name, version)."""
        bs = self.blobs[tier]
        version = bs.put_file(name, data)
        root = bs.root(name, version)
        nonce = self._next(client) if nonce is None else nonce
        self._log_append(client, nonce, "put_file",
                         {"name": name, "tier": tier, "version": version, "root": root}, idem)
        return version

    def get_file(self, name: str, version: int = -1, tier: str = "warm") -> bytes:
        return self.blobs[tier].get_file(name, version)

    def file_root(self, name: str, version: int = -1, tier: str = "warm") -> str:
        return self.blobs[tier].root(name, version)

    def file_proof(self, name: str, chunk_index: int, version: int = -1, tier: str = "warm"):
        """Return (leaf, proof, root) proving chunk_index is part of the version."""
        recipe = self.blobs[tier].files[name]["versions"][version]
        root = self.blobs[tier].files[name]["roots"][version]
        leaf = recipe[chunk_index]
        return leaf, merkle_proof(recipe, chunk_index), root

    @staticmethod
    def verify_proof(leaf, proof, root) -> bool:
        return merkle_verify(leaf, proof, root)

    def compression_stats(self, tier: str = "warm") -> dict:
        return self.blobs[tier].stats()

    # --- integrity / determinism -------------------------------------------
    def verify(self) -> bool:
        """Verify the hash-chained op log has not been tampered with."""
        return self.log.verify()

    def rebuild(self):
        """Replay the log into fresh state — proves state is a pure function of the log."""
        store, relations, indexes = MVCCStore(), Relations(), Indexes()
        for (c, f, k) in self.indexes.config:
            indexes.ensure(c, f, k)
        for op in self.log.ops:
            apply_op(store, relations, indexes, op)
        return store, relations, indexes

    def verify_determinism(self) -> bool:
        store, _, _ = self.rebuild()
        return store.snapshot() == self.store.snapshot()

    @property
    def head(self) -> str:
        return self.log.head

    @property
    def seq(self) -> int:
        return len(self.log) - 1
