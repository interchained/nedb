"""
nedb.engine — the NEDB database: log + MVCC store + relations + indexes + Cascade.

The OpLog is the source of truth. Every mutation appends an Op; `_apply` deterministically
folds an Op into the materialized state (store / relations / indexes). Because state is a
pure function of the log, we get crash recovery and determinism (rebuild) for free, and
"AS OF seq" time-travel because the log carries monotonic seqs.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .cascade import BlobStore
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
    def __init__(self) -> None:
        self.log = OpLog()
        self.store = MVCCStore()
        self.relations = Relations()
        self.indexes = Indexes()
        self.blobs: Dict[str, BlobStore] = {"warm": BlobStore("warm"), "cold": BlobStore("cold")}
        self._nonce: Dict[str, int] = {}

    # --- nonce helper -------------------------------------------------------
    def _next(self, client: str) -> int:
        n = self._nonce.get(client, 0) + 1
        self._nonce[client] = n
        return n

    # --- mutations ----------------------------------------------------------
    def put(self, coll: str, id: str, doc: dict, client: str = "local",
            nonce: Optional[int] = None, idem: Optional[str] = None) -> dict:
        key = f"{coll}:{id}"
        doc = dict(doc)
        doc.setdefault("_id", id)
        nonce = self._next(client) if nonce is None else nonce
        op, created = self.log.append(client, nonce, "put",
                                      {"key": key, "coll": coll, "id": id, "doc": doc}, idem)
        if created:
            apply_op(self.store, self.relations, self.indexes, op)
        return self.store.get(key)

    def delete(self, coll: str, id: str, client: str = "local",
               nonce: Optional[int] = None, idem: Optional[str] = None) -> None:
        key = f"{coll}:{id}"
        nonce = self._next(client) if nonce is None else nonce
        op, created = self.log.append(client, nonce, "delete",
                                      {"key": key, "coll": coll, "id": id}, idem)
        if created:
            apply_op(self.store, self.relations, self.indexes, op)

    def get(self, coll: str, id: str, as_of: Optional[int] = None) -> Optional[dict]:
        return self.store.get(f"{coll}:{id}", as_of)

    # --- relations ----------------------------------------------------------
    def link(self, frm: str, rel: str, to: str, client: str = "local",
             nonce: Optional[int] = None) -> None:
        nonce = self._next(client) if nonce is None else nonce
        op, created = self.log.append(client, nonce, "link", {"frm": frm, "rel": rel, "to": to})
        if created:
            apply_op(self.store, self.relations, self.indexes, op)

    def unlink(self, frm: str, rel: str, to: str, client: str = "local",
               nonce: Optional[int] = None) -> None:
        nonce = self._next(client) if nonce is None else nonce
        op, created = self.log.append(client, nonce, "unlink", {"frm": frm, "rel": rel, "to": to})
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
        return [d for _, d in rows]

    # --- files (git-style, Cascade-compressed) ------------------------------
    def put_file(self, name: str, data: bytes, tier: str = "warm", client: str = "local",
                 nonce: Optional[int] = None, idem: Optional[str] = None) -> int:
        """Store a file version (Cascade-compressed, deduplicated). Returns the
        integer version index; fetch its anchorable hash via file_root(name, version)."""
        bs = self.blobs[tier]
        version = bs.put_file(name, data)
        root = bs.root(name, version)
        nonce = self._next(client) if nonce is None else nonce
        self.log.append(client, nonce, "put_file",
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
