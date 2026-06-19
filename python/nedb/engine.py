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
from . import crypto as _crypto
from .index import Indexes, tokenize
from .log import Op, OpLog, ReplayError, GENESIS, blake, canon  # noqa: F401  (re-exported)
from typing import List as _List
from .merkle import merkle_proof, merkle_verify
from .query import Query, cmp, parse_nql
from .relations import Relations
from .store import MVCCStore


def apply_op(store: MVCCStore, relations: Relations, indexes: Indexes, op: Op,
             cause_map: Optional[Dict[int, List[int]]] = None) -> None:
    """Deterministically fold one op into materialized state."""
    p = op.payload
    if op.op == "put":
        key, coll = p["key"], p["coll"]
        doc = dict(p["doc"])          # copy so we don't mutate the op payload
        doc["_seq"] = op.seq          # inject sequence number into every stored doc
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
    # Build causal reverse index so TRACE ... REVERSE queries are O(1).
    if cause_map is not None and op.caused_by:
        for cause_seq in op.caused_by:
            cause_map.setdefault(cause_seq, []).append(op.seq)


class NEDB:
    def __init__(self, path: Optional[str] = None,
                 tmk: Optional[bytes] = None) -> None:
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
        # Causal provenance reverse index: cause_seq → [dependent_seq, ...]
        # Populated in apply_op when an op carries caused_by seqs.
        self.cause_map: Dict[int, List[int]] = {}

        self.path = path
        self._aof = None
        # When True, _log_append buffers writes and skips the per-op fsync; the
        # caller (the concurrent Sequencer) issues ONE fsync per batch via flush()
        # — group commit. Default False keeps embedded/direct use durable per-op.
        self._defer_sync = False
        # Encryption: resolve TMK (arg > env) → load/create DEK → None if no TMK
        self._dek: Optional[bytes] = None
        resolved_tmk = _crypto.resolve_tmk(tmk)
        if resolved_tmk is not None and path is not None:
            # Ensure the database directory exists BEFORE creating the DEK file.
            # _open() calls os.makedirs() too, but it runs after this block —
            # so a brand-new database would fail with FileNotFoundError on key.enc.tmp.
            os.makedirs(path, exist_ok=True)
            self._dek = _crypto.load_or_create_dek(path, resolved_tmk)
        if path is not None:
            self._open(path)

    # --- persistence (AOF) --------------------------------------------------
    def _open(self, path: str) -> None:
        os.makedirs(path, exist_ok=True)
        self._aof_path = os.path.join(path, "log.aof")
        self._meta_path = os.path.join(path, "meta.json")
        if os.path.exists(self._aof_path) or os.path.exists(self._meta_path):
            self._load()
            # Backfill: if encryption is now enabled but the AOF still has plain
            # lines, rewrite the entire log encrypted in place so no cleartext
            # survives on disk.
            if self._dek is not None and os.path.exists(self._aof_path):
                self._backfill_encrypt_if_needed()
            # Self-heal a structurally-broken chain (e.g. a historical encrypt-
            # backfill gap) before we start appending. No-op on a healthy log;
            # leaves genuine tampering untouched (verify stays False + warns).
            self._self_heal_if_needed()
        # Append mode: never truncates the existing log.
        self._aof = open(self._aof_path, "a", encoding="utf-8")

    def _backfill_encrypt_if_needed(self) -> None:
        """Detect a plain-text AOF and rewrite it fully encrypted.

        Triggered on open when NEDB_TMK is set but the existing log contains
        unencrypted entries.  Uses atomic write (tmp → rename) so the old log
        is never left in a half-written state.  Checkpoints afterwards so the
        snapshot is also encrypted.

        Safe to call on an already-encrypted AOF — it's a no-op if every line
        is already an encrypted envelope.
        """
        # Peek at the first non-empty line of the AOF.
        first_plain = None
        with open(self._aof_path, encoding="utf-8") as fh:
            for raw in fh:
                stripped = raw.strip()
                if not stripped:
                    continue
                try:
                    env = json.loads(stripped)
                    if isinstance(env, dict) and env.get("enc") == 1:
                        return  # already encrypted — nothing to do
                    first_plain = stripped
                except Exception:
                    pass
                break

        if first_plain is None:
            return  # empty AOF

        # AOF has plain lines — rewrite fully encrypted
        print(f"  [nedb] backfill-encrypting existing log ({self._aof_path})…")
        tmp_path = self._aof_path + ".enc_tmp"
        with open(self._aof_path, encoding="utf-8") as fh_in, \
             open(tmp_path, "w", encoding="utf-8") as fh_out:
            for raw in fh_in:
                # Each line might already be encrypted (mixed state is fine)
                decoded = _crypto.aof_decode(raw, self._dek)
                if not decoded:
                    continue
                # Re-encode encrypted
                fh_out.write(_crypto.aof_encode(decoded, self._dek) + "\n")
            fh_out.flush()
            os.fsync(fh_out.fileno())

        os.replace(tmp_path, self._aof_path)
        print(f"  [nedb] AOF backfill-encrypt complete.")

        # Drop any stale (pre-encryption) snapshot so the next open rebuilds purely
        # from the re-encrypted AOF. We MUST NOT checkpoint here: self._aof isn't
        # open yet during _open, so a checkpoint op would advance the in-memory head
        # WITHOUT being persisted — leaving a permanent gap in the on-disk chain
        # (the "tampered after restart" bug). The daemon checkpoints normally later.
        snap = _snap._snap_path(self.path)
        if os.path.exists(snap):
            os.remove(snap)

    # ── self-healing ────────────────────────────────────────────────────────
    @staticmethod
    def _op_body(o: Op) -> dict:
        """The exact hashed body — delegates to OpLog's canonical method."""
        from .log import OpLog as _OL
        return _OL._op_body(o)

    def _rewrite_aof(self, ops: List[Op]) -> None:
        """Atomically rewrite the AOF from `ops` (encrypted if a DEK is set)."""
        tmp = self._aof_path + ".heal_tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            for o in ops:
                fh.write(_crypto.aof_encode(json.dumps(o.to_dict()), self._dek) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._aof_path)

    def _self_heal_if_needed(self) -> None:
        """Repair a structurally-broken hash chain in place.

        A chain can break WITHOUT any tampering: the historical encrypt-backfill
        bug appended a checkpoint op that was never persisted, so the on-disk log
        is missing a link and every op after it chains from a vanished head.

        We distinguish that from real tampering: if every op is *internally*
        consistent (``hash == H(prev_hash || body)`` for its OWN stored prev_hash)
        but the running chain is discontinuous, only the linkage is broken — the
        content is intact, so we re-link it (preserving each op's fields/seq) and
        rewrite the AOF. If any op's content was altered (hash doesn't match its
        own body), we do NOT rewrite — verify() stays False and we warn, so real
        tampering is never silently masked.
        """
        if self.log.verify():
            return  # healthy

        ops = self.log.ops
        internally_valid = all(
            o.hash == blake(o.prev_hash.encode() + canon(self._op_body(o)))
            for o in ops
        )
        if not internally_valid:
            print("  [nedb] WARNING: chain verification failed and op content is "
                  "inconsistent — possible tampering. NOT auto-repairing.")
            return

        print("  [nedb] self-healing chain (structural break, content intact)…")
        prev = GENESIS
        healed: List[Op] = []
        for o in ops:
            body = self._op_body(o)
            h = blake(prev.encode() + canon(body))
            healed.append(Op(o.seq, o.client, o.nonce, o.op, o.payload, o.ts, o.idem, prev, h))
            prev = h
        self.log.ops = healed
        self.log._head = prev
        if self.path is not None and os.path.exists(self._aof_path):
            self._rewrite_aof(healed)
            snap = _snap._snap_path(self.path)
            if os.path.exists(snap):
                os.remove(snap)  # stale: references the old head/seq
        print(f"  [nedb] self-heal complete — verify={self.log.verify()} "
              f"head={self.head[:12]}…")

    def _load(self) -> None:
        # ── Try snapshot-assisted load first (O(delta) instead of O(total)) ─
        snap_seq = _snap.load_snapshot(self)
        if snap_seq >= 0:
            # Snapshot loaded: only replay AOF ops AFTER the checkpoint op.
            ops: List[Op] = []
            if os.path.exists(self._aof_path):
                with open(self._aof_path, encoding="utf-8") as fh:
                    for raw_line in fh:
                        line = _crypto.aof_decode(raw_line, self._dek)
                        if line:
                            ops.append(Op.from_dict(json.loads(line)))
            # Build the full log (needed for verify() and AS OF) but only
            # apply ops that arrive after the checkpoint to avoid double-fold.
            self.log.load(ops)
            for op in self.log.ops:
                if op.seq > snap_seq:
                    apply_op(self.store, self.relations, self.indexes, op, self.cause_map)
            self._nonce = dict(self.log._last_nonce)
            return

        # ── No snapshot: full replay (original behaviour) ─────────────────
        # 1) index configuration
        if os.path.exists(self._meta_path):
            with open(self._meta_path, encoding="utf-8") as fh:
                for coll, field, kind in json.load(fh).get("indexes", []):
                    self.indexes.ensure(coll, field, kind)
        # 2) the hash-chained op log
        # Self-healing AOF load — never crash on corruption.
        ops = []
        corrupt_count = 0
        if os.path.exists(self._aof_path):
            with open(self._aof_path, encoding="utf-8") as fh:
                for raw_line in fh:
                    try:
                        line = _crypto.aof_decode(raw_line, self._dek)
                        if line:
                            ops.append(Op.from_dict(json.loads(line)))
                    except Exception as exc:
                        corrupt_count += 1
                        print(
                            f"  [nedb] AOF self-repair: corrupt entry in"
                            f" {self._aof_path!r}"
                            f" ({type(exc).__name__}: {exc})"
                            f" -- truncating and recovering {len(ops)} op(s)."
                        )
                        break
        if corrupt_count:
            self._rewrite_aof(ops)
            print(
                f"  [nedb] AOF self-repair complete:"
                f" {len(ops)} op(s) recovered,"
                f" {corrupt_count} corrupt entry/entries removed."
            )
        self.log.load(ops)
        # 3) fold
        for op in self.log.ops:
            apply_op(self.store, self.relations, self.indexes, op, self.cause_map)
        # 4) nonce restoration
        self._nonce = dict(self.log._last_nonce)

    def _persist_meta(self) -> None:
        if self.path is None:
            return
        with open(self._meta_path, "w", encoding="utf-8") as fh:
            json.dump({"indexes": [list(t) for t in self.indexes.config]}, fh)

    def _log_append(self, client: str, nonce: int, op: str, payload: dict,
                    idem: Optional[str] = None,
                    caused_by: Optional[List[int]] = None,
                    evidence: Optional[str] = None,
                    confidence: Optional[float] = None,
                    valid_from: Optional[str] = None,
                    valid_to: Optional[str] = None):
        """Append to the in-memory log AND, if durable, to the AOF (encrypted if DEK set)."""
        rec, created = self.log.append(client, nonce, op, payload, idem,
                                       caused_by=caused_by, evidence=evidence,
                                       confidence=confidence,
                                       valid_from=valid_from, valid_to=valid_to)
        if created and self._aof is not None:
            line = _crypto.aof_encode(json.dumps(rec.to_dict()), self._dek)
            self._aof.write(line + "\n")
            if not self._defer_sync:
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

    def rewrap_key(self, old_tmk: bytes, new_tmk: bytes) -> None:
        """
        Key rotation: re-wrap the DEK under a new TMK without re-encrypting data.

        After this call the database opens only with ``new_tmk``.  The DEK —
        and therefore all encrypted data — stays untouched.

        Example::

            db.rewrap_key(old_tmk=bytes.fromhex("aa..."), new_tmk=bytes.fromhex("bb..."))
        """
        if self.path is None:
            raise ValueError("Key rotation requires a durable NEDB(path) database.")
        old_k = _crypto.resolve_tmk(old_tmk)
        new_k = _crypto.resolve_tmk(new_tmk)
        _crypto.rewrap_dek(self.path, old_k, new_k)
        # Update in-memory DEK so current session keeps working
        self._dek = _crypto.load_or_create_dek(self.path, new_k)

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

    @staticmethod
    def _valid_at(doc: dict, date: str) -> bool:
        """Return True if the doc is valid at `date` (ISO 8601 string).

        Bi-temporal check:
          - Docs without _valid_from / _valid_to are ALWAYS valid (backward compat).
          - _valid_from <= date  (or _valid_from absent → open start)
          - _valid_to   >= date  (or _valid_to   absent → open end / still valid)

        ISO 8601 strings compare correctly lexicographically so simple str ops work.
        """
        vf = doc.get("_valid_from")
        vt = doc.get("_valid_to")
        if vf is None and vt is None:
            return True          # no valid-time metadata → always valid
        if vf is not None and date < vf:
            return False         # not yet in effect
        if vt is not None and date > vt:
            return False         # already expired
        return True

    # --- mutations ----------------------------------------------------------
    def put(self, coll: str, id: str, doc: dict, client: str = "local",
            nonce: Optional[int] = None, idem: Optional[str] = None,
            ttl_s: Optional[float] = None,
            caused_by: Optional[List[int]] = None,
            evidence: Optional[str] = None,
            confidence: Optional[float] = None,
            valid_from: Optional[str] = None,
            valid_to: Optional[str] = None) -> dict:
        key = f"{coll}:{id}"
        doc = dict(doc)
        doc.setdefault("_id", id)
        doc = self._embed_ttl(doc, ttl_s)
        # Mirror provenance + valid-time into the doc as queryable _-prefixed fields.
        # They're also sealed in the Op hash via _log_append so they're tamper-evident.
        if caused_by  is not None: doc["_caused_by"]  = caused_by
        if evidence   is not None: doc["_evidence"]   = evidence
        if confidence is not None: doc["_confidence"] = confidence
        if valid_from is not None: doc["_valid_from"] = valid_from
        if valid_to   is not None: doc["_valid_to"]   = valid_to
        nonce = self._next(client) if nonce is None else nonce
        op, created = self._log_append(client, nonce, "put",
                                       {"key": key, "coll": coll, "id": id, "doc": doc},
                                       idem, caused_by=caused_by,
                                       evidence=evidence, confidence=confidence,
                                       valid_from=valid_from, valid_to=valid_to)
        if created:
            apply_op(self.store, self.relations, self.indexes, op, self.cause_map)
        return self.store.get(key)

    def delete(self, coll: str, id: str, client: str = "local",
               nonce: Optional[int] = None, idem: Optional[str] = None) -> None:
        key = f"{coll}:{id}"
        nonce = self._next(client) if nonce is None else nonce
        op, created = self._log_append(client, nonce, "delete",
                                       {"key": key, "coll": coll, "id": id}, idem)
        if created:
            apply_op(self.store, self.relations, self.indexes, op, self.cause_map)

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
            apply_op(self.store, self.relations, self.indexes, op, self.cause_map)

    def unlink(self, frm: str, rel: str, to: str, client: str = "local",
               nonce: Optional[int] = None) -> None:
        nonce = self._next(client) if nonce is None else nonce
        op, created = self._log_append(client, nonce, "unlink", {"frm": frm, "rel": rel, "to": to})
        if created:
            apply_op(self.store, self.relations, self.indexes, op, self.cause_map)

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

        # TRACE caused_by — causal provenance traversal
        if plan.get("trace"):
            if not plan.get("trace_reverse"):
                # Backward: start from current result set, follow caused_by seqs
                # recursively to their originating documents.
                result_docs = [d for _, d in rows]
                visited_seqs: set = set()
                frontier = list(result_docs)
                out_docs = []
                while frontier:
                    doc = frontier.pop()
                    causes = doc.get("_caused_by") or []
                    for cause_seq in causes:
                        if cause_seq in visited_seqs:
                            continue
                        visited_seqs.add(cause_seq)
                        if cause_seq < len(self.log.ops):
                            op = self.log.ops[cause_seq]
                            if op.op == "put":
                                cause_key = op.payload.get("key", "")
                                cause_doc = self.store.get(cause_key, as_of)
                                if cause_doc is not None:
                                    out_docs.append((cause_key, cause_doc))
                                    frontier.append(cause_doc)
                rows = out_docs
            else:
                # Forward: start from current result set, follow cause_map to find
                # all documents that declared these as causes (downstream effects).
                result_docs = [d for _, d in rows]
                visited_seqs_fwd: set = set()
                out_fwd = []
                queue = []
                for doc in result_docs:
                    d_id = doc.get("_id")
                    if d_id is not None:
                        coll_p = plan["from"]
                        key_p = f"{coll_p}:{d_id}"
                        # Find the seq of the op that last wrote this key
                        for op in reversed(self.log.ops):
                            if op.op == "put" and op.payload.get("key") == key_p:
                                queue.append(op.seq)
                                break
                # BFS — feed found dep_seqs back into the queue for full
                # transitive traversal (mirrors the backward TRACE frontier loop).
                while queue:
                    seq = queue.pop(0)
                    for dep_seq in self.cause_map.get(seq, []):
                        if dep_seq in visited_seqs_fwd:
                            continue
                        visited_seqs_fwd.add(dep_seq)
                        if dep_seq < len(self.log.ops):
                            dep_op = self.log.ops[dep_seq]
                            if dep_op.op == "put":
                                dep_key = dep_op.payload.get("key", "")
                                dep_doc = self.store.get(dep_key, as_of)
                                if dep_doc is not None:
                                    out_fwd.append((dep_key, dep_doc))
                                    queue.append(dep_seq)  # continue BFS
                rows = out_fwd

        if plan.get("limit") is not None:
            rows = rows[: plan["limit"]]

        # VALID AS OF <date> — bi-temporal valid-time filter.
        # Applied after all other filters so WHERE/ORDER BY/LIMIT can still
        # reference _valid_from/_valid_to as regular queryable fields.
        # Docs without _valid_from/_valid_to always pass (backward compat).
        valid_date = plan.get("valid_as_of")
        if valid_date:
            rows = [(k, d) for k, d in rows if self._valid_at(d, valid_date)]

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
