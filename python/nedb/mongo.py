# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.mongo — MongoDB compatibility adapter.

Maps the MongoDB document/collection API deterministically onto NEDB primitives.
No pymongo, bson, or MongoDB server code is used or required — the MongoDB API is
simply a familiar entry point; the NEDB engine executes everything natively using
its append-only log, MVCC store, relations, and indexes (so every write is
replay-protected and hash-chained, and time-travel still holds).

Usage::

    from nedb import NEDB
    from nedb.mongo import MongoCompat

    db    = NEDB("./data")
    mongo = MongoCompat(db)
    users = mongo["users"]                       # or mongo.collection("users")

    users.insert_one({"name": "Ada", "age": 31, "status": "active"})
    users.insert_many([{"name": "Bob", "age": 24}, {"name": "Carol", "age": 41}])

    users.find_one({"name": "Ada"})              # → {"_id": ..., "name": "Ada", ...}
    list(users.find({"age": {"$gt": 25}}).sort("age", -1).limit(10))
    users.update_one({"name": "Ada"}, {"$set": {"age": 32}, "$inc": {"logins": 1}})
    users.delete_many({"status": "inactive"})
    users.count_documents({"status": "active"})
    users.distinct("status")
    users.aggregate([{"$group": {"_id": "$status", "n": {"$sum": 1}}}])

MongoDB → NEDB mapping
──────────────────────
collection name   →  NEDB collection
document          →  NEDB doc (id = str(_id); ObjectId auto-generated if absent)
filter operators  →  $eq $ne $gt $gte $lt $lte $in $nin $exists $regex
                     $and $or $nor $not $size $all $mod (dotted paths supported)
update operators  →  $set $unset $inc $mul $min $max $rename $push $addToSet
                     $pull $pop $setOnInsert (+ full-document replacement)
find cursor       →  .sort() .skip() .limit() .to_list() (lazy, chainable)
aggregate stages  →  $match $group $sort $skip $limit $count $project
accumulators      →  $sum $avg $min $max $first $last $push $addToSet

Equality-indexed fields (db.create_index) accelerate filters automatically; any
filter the planner can't express as a simple AND of comparisons falls back to a
correctness-guaranteed in-engine scan + Python match.

Unsupported (raise MongoUnsupportedError): $where/JS, $text $search index search
via runCommand, $lookup/$unwind/$facet aggregation stages, GridFS, change streams,
multi-document transactions (sessions), map-reduce, geospatial operators.
"""
from __future__ import annotations

import binascii
import json
import os
import random
import re
import struct
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from .query import empty_plan


# ── Errors ──────────────────────────────────────────────────────────────────────

class MongoError(Exception):
    """Raised on a MongoDB-compatible usage or argument error."""


class MongoUnsupportedError(MongoError):
    """Raised when a MongoDB feature is not yet implemented in NEDB."""


# ── ObjectId ────────────────────────────────────────────────────────────────────

_OID_COUNTER = random.randint(0, 0xFFFFFF)
_OID_MACHINE = os.urandom(5)


def ObjectId() -> str:
    """Generate a MongoDB-style 24-hex-char ObjectId.

    Layout matches MongoDB: 4-byte timestamp + 5-byte random + 3-byte counter.
    Returned as a plain ``str`` so it is JSON-serializable and round-trips through
    NEDB's log unchanged (no bson dependency).
    """
    global _OID_COUNTER
    _OID_COUNTER = (_OID_COUNTER + 1) & 0xFFFFFF
    raw = struct.pack(">I", int(time.time())) + _OID_MACHINE + struct.pack(">I", _OID_COUNTER)[1:]
    return binascii.hexlify(raw).decode()


# ── Result objects (mirror pymongo) ──────────────────────────────────────────────

class InsertOneResult:
    __slots__ = ("inserted_id", "acknowledged")

    def __init__(self, inserted_id: Any):
        self.inserted_id = inserted_id
        self.acknowledged = True

    def __repr__(self) -> str:
        return f"InsertOneResult({self.inserted_id!r})"


class InsertManyResult:
    __slots__ = ("inserted_ids", "acknowledged")

    def __init__(self, inserted_ids: List[Any]):
        self.inserted_ids = inserted_ids
        self.acknowledged = True

    def __repr__(self) -> str:
        return f"InsertManyResult({self.inserted_ids!r})"


class UpdateResult:
    __slots__ = ("matched_count", "modified_count", "upserted_id", "acknowledged")

    def __init__(self, matched: int, modified: int, upserted_id: Any = None):
        self.matched_count = matched
        self.modified_count = modified
        self.upserted_id = upserted_id
        self.acknowledged = True

    def __repr__(self) -> str:
        return (f"UpdateResult(matched={self.matched_count}, "
                f"modified={self.modified_count}, upserted_id={self.upserted_id!r})")


class DeleteResult:
    __slots__ = ("deleted_count", "acknowledged")

    def __init__(self, deleted: int):
        self.deleted_count = deleted
        self.acknowledged = True

    def __repr__(self) -> str:
        return f"DeleteResult(deleted={self.deleted_count})"


# ── Path + comparison helpers ─────────────────────────────────────────────────────

_MISSING = object()  # sentinel distinguishing "field absent" from "field is null"


def _get_path(doc: Any, path: str) -> Any:
    """Resolve a dotted field path ('a.b.c'); return _MISSING if any hop is absent."""
    cur = doc
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return _MISSING
    return cur


def _set_path(doc: dict, path: str, value: Any) -> None:
    """Set a dotted field path, creating intermediate dicts as needed."""
    parts = path.split(".")
    cur = doc
    for part in parts[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = nxt
    cur[parts[-1]] = value


def _unset_path(doc: dict, path: str) -> None:
    parts = path.split(".")
    cur = doc
    for part in parts[:-1]:
        cur = cur.get(part)
        if not isinstance(cur, dict):
            return
    cur.pop(parts[-1], None)


def _eq(actual: Any, operand: Any) -> bool:
    """MongoDB equality: null matches missing-or-null; scalar matches array membership."""
    if actual is _MISSING:
        return operand is None
    if isinstance(actual, list) and not isinstance(operand, list):
        return operand in actual or actual == operand
    return actual == operand


def _cmp(actual: Any, operand: Any, fn) -> bool:
    """Type-safe comparison; array operands match if ANY element satisfies (Mongo semantics)."""
    if actual is _MISSING or actual is None:
        return False
    if isinstance(actual, list):
        return any(_cmp(a, operand, fn) for a in actual)
    try:
        return bool(fn(actual, operand))
    except TypeError:
        return False


def _sort_key(v: Any):
    """Total order across mixed BSON-ish types so heterogeneous sorts never raise."""
    if v is _MISSING or v is None:
        return (0, 0)
    if isinstance(v, bool):
        return (1, int(v))
    if isinstance(v, (int, float)):
        return (2, v)
    if isinstance(v, str):
        return (3, v)
    return (4, str(v))


# ── Filter matching ───────────────────────────────────────────────────────────────

def _match(doc: dict, filt: dict) -> bool:
    """Return True if ``doc`` satisfies a MongoDB filter document."""
    for key, cond in filt.items():
        if key == "$and":
            if not all(_match(doc, sub) for sub in cond):
                return False
        elif key == "$or":
            if not any(_match(doc, sub) for sub in cond):
                return False
        elif key == "$nor":
            if any(_match(doc, sub) for sub in cond):
                return False
        elif key == "$not":
            if _match(doc, cond):
                return False
        elif key.startswith("$"):
            raise MongoUnsupportedError(f"Unsupported top-level operator {key!r}")
        else:
            if not _match_field(_get_path(doc, key), cond):
                return False
    return True


def _is_operator_doc(cond: Any) -> bool:
    return isinstance(cond, dict) and len(cond) > 0 and all(k.startswith("$") for k in cond)


def _match_field(actual: Any, cond: Any) -> bool:
    if _is_operator_doc(cond):
        options = cond.get("$options", "")
        for op, operand in cond.items():
            if op == "$options":
                continue
            if not _match_op(actual, op, operand, options):
                return False
        return True
    return _eq(actual, cond)


def _match_op(actual: Any, op: str, operand: Any, options: str = "") -> bool:
    if op == "$eq":
        return _eq(actual, operand)
    if op == "$ne":
        return not _eq(actual, operand)
    if op == "$gt":
        return _cmp(actual, operand, lambda a, b: a > b)
    if op == "$gte":
        return _cmp(actual, operand, lambda a, b: a >= b)
    if op == "$lt":
        return _cmp(actual, operand, lambda a, b: a < b)
    if op == "$lte":
        return _cmp(actual, operand, lambda a, b: a <= b)
    if op == "$in":
        if actual is _MISSING:
            return any(v is None for v in operand)
        if isinstance(actual, list):
            return any(a in operand for a in actual)
        return actual in operand
    if op == "$nin":
        return not _match_op(actual, "$in", operand, options)
    if op == "$exists":
        return (actual is not _MISSING) == bool(operand)
    if op == "$regex":
        if actual is _MISSING or not isinstance(actual, str):
            return False
        flags = re.IGNORECASE if "i" in options else 0
        if "m" in options:
            flags |= re.MULTILINE
        if "s" in options:
            flags |= re.DOTALL
        try:
            return re.search(operand, actual, flags) is not None
        except re.error as e:
            raise MongoError(f"invalid $regex: {e}")
    if op == "$not":
        return not _match_field(actual, operand)
    if op == "$size":
        return isinstance(actual, list) and len(actual) == operand
    if op == "$all":
        return isinstance(actual, list) and all(x in actual for x in operand)
    if op == "$mod":
        if actual is _MISSING or not isinstance(actual, (int, float)):
            return False
        divisor, remainder = operand
        try:
            return int(actual) % int(divisor) == int(remainder)
        except (ZeroDivisionError, TypeError, ValueError):
            return False
    if op == "$elemMatch":
        if not isinstance(actual, list):
            return False
        return any(
            _match(item, operand) if not _is_operator_doc(operand) else _match_field(item, operand)
            for item in actual
        )
    raise MongoUnsupportedError(f"Unsupported query operator {op!r}")


# ── Projection ────────────────────────────────────────────────────────────────────

def _project(doc: dict, projection: Optional[dict]) -> dict:
    if not projection:
        return doc
    include = {k: v for k, v in projection.items() if k != "_id"}
    if not include:
        # exclusion-only (possibly with _id:0)
        out = {k: v for k, v in doc.items()}
        for k, v in projection.items():
            if not v:
                out.pop(k, None)
        return out
    modes = set(bool(v) for v in include.values())
    if modes == {True}:
        out = {k: doc[k] for k in include if k in doc}
        if projection.get("_id", 1):
            if "_id" in doc:
                out["_id"] = doc["_id"]
        return out
    if modes == {False}:
        out = {k: v for k, v in doc.items()}
        for k, v in include.items():
            if not v:
                out.pop(k, None)
        if not projection.get("_id", 1):
            out.pop("_id", None)
        return out
    raise MongoError("Projection cannot mix inclusion and exclusion (except _id).")


# ── Update operators ──────────────────────────────────────────────────────────────

def _apply_update(existing: Optional[dict], update: dict, *, on_insert: bool = False) -> dict:
    """Apply a MongoDB update document. A doc with no $-operators is a full replacement."""
    has_ops = any(k.startswith("$") for k in update)
    if not has_ops:
        # full-document replacement — _id is preserved by the caller
        return dict(update)

    doc = dict(existing or {})
    for op, spec in update.items():
        if op == "$set":
            for field, value in spec.items():
                _set_path(doc, field, value)
        elif op == "$setOnInsert":
            if on_insert:
                for field, value in spec.items():
                    _set_path(doc, field, value)
        elif op == "$unset":
            for field in spec:
                _unset_path(doc, field)
        elif op == "$inc":
            for field, delta in spec.items():
                cur = _get_path(doc, field)
                base = cur if isinstance(cur, (int, float)) and not isinstance(cur, bool) else 0
                _set_path(doc, field, base + delta)
        elif op == "$mul":
            for field, factor in spec.items():
                cur = _get_path(doc, field)
                base = cur if isinstance(cur, (int, float)) and not isinstance(cur, bool) else 0
                _set_path(doc, field, base * factor)
        elif op == "$min":
            for field, value in spec.items():
                cur = _get_path(doc, field)
                if cur is _MISSING or _sort_key(value) < _sort_key(cur):
                    _set_path(doc, field, value)
        elif op == "$max":
            for field, value in spec.items():
                cur = _get_path(doc, field)
                if cur is _MISSING or _sort_key(value) > _sort_key(cur):
                    _set_path(doc, field, value)
        elif op == "$rename":
            for field, new_field in spec.items():
                cur = _get_path(doc, field)
                if cur is not _MISSING:
                    _unset_path(doc, field)
                    _set_path(doc, new_field, cur)
        elif op == "$push":
            for field, value in spec.items():
                arr = _get_path(doc, field)
                arr = list(arr) if isinstance(arr, list) else []
                if isinstance(value, dict) and "$each" in value:
                    arr.extend(value["$each"])
                else:
                    arr.append(value)
                _set_path(doc, field, arr)
        elif op == "$addToSet":
            for field, value in spec.items():
                arr = _get_path(doc, field)
                arr = list(arr) if isinstance(arr, list) else []
                items = value["$each"] if isinstance(value, dict) and "$each" in value else [value]
                for it in items:
                    if it not in arr:
                        arr.append(it)
                _set_path(doc, field, arr)
        elif op == "$pull":
            for field, cond in spec.items():
                arr = _get_path(doc, field)
                if not isinstance(arr, list):
                    continue
                if _is_operator_doc(cond):
                    arr = [x for x in arr if not _match_field(x, cond)]
                else:
                    arr = [x for x in arr if x != cond]
                _set_path(doc, field, arr)
        elif op == "$pop":
            for field, direction in spec.items():
                arr = _get_path(doc, field)
                if isinstance(arr, list) and arr:
                    arr = list(arr)
                    arr.pop(0 if direction < 0 else -1)
                    _set_path(doc, field, arr)
        else:
            raise MongoUnsupportedError(f"Unsupported update operator {op!r}")
    return doc


# ── Cursor ────────────────────────────────────────────────────────────────────────

class Cursor:
    """Lazy, chainable result of ``find()`` — mirrors pymongo's Cursor surface."""

    def __init__(self, collection: "Collection", filt: dict, projection: Optional[dict]):
        self._coll = collection
        self._filt = filt or {}
        self._proj = projection
        self._sort: Optional[List[Tuple[str, int]]] = None
        self._skip = 0
        self._limit: Optional[int] = None

    def sort(self, key_or_list: Union[str, List[Tuple[str, int]]],
             direction: Optional[int] = None) -> "Cursor":
        if isinstance(key_or_list, str):
            self._sort = [(key_or_list, direction if direction is not None else 1)]
        else:
            self._sort = list(key_or_list)
        return self

    def skip(self, n: int) -> "Cursor":
        self._skip = int(n)
        return self

    def limit(self, n: int) -> "Cursor":
        self._limit = int(n)
        return self

    def _materialize(self) -> List[dict]:
        docs = self._coll._find_docs(self._filt)
        if self._sort:
            for field, dirn in reversed(self._sort):
                docs.sort(key=lambda d: _sort_key(_get_path(d, field)), reverse=(dirn < 0))
        if self._skip:
            docs = docs[self._skip:]
        if self._limit is not None:
            docs = docs[:self._limit]
        if self._proj is not None:
            docs = [_project(d, self._proj) for d in docs]
        return docs

    def to_list(self, length: Optional[int] = None) -> List[dict]:
        docs = self._materialize()
        return docs if length is None else docs[:length]

    def __iter__(self):
        return iter(self._materialize())

    def __len__(self) -> int:
        return len(self._materialize())

    def count(self) -> int:
        return len(self._materialize())


# ── Collection ─────────────────────────────────────────────────────────────────────

class Collection:
    """A MongoDB-style collection backed by one NEDB collection."""

    def __init__(self, db: Any, name: str, client: str = "mongo-compat"):
        self._db = db
        self._coll = name
        self._client = client

    @property
    def name(self) -> str:
        return self._coll

    # ── candidate selection (index fast-paths, then guaranteed match) ──────────
    def _candidates(self, filt: dict) -> List[dict]:
        """Return a SUPERSET of matching docs; _match() does the authoritative filter.

        Narrowing is only delegated to the engine for fields that have an eq index.
        Such fields are guaranteed scalar (NEDB can't eq-index an unhashable array),
        so the engine's strict ``=`` is lossless there. Every other field — which may
        hold arrays or be absent — is left to _match so MongoDB's array-membership and
        null/missing semantics are preserved.
        """
        idq = filt.get("_id") if isinstance(filt, dict) else None
        if isinstance(idq, (str, int)) and not isinstance(idq, bool):
            d = self._db.get(self._coll, str(idq))
            return [d] if d is not None else []
        if isinstance(idq, dict) and set(idq.keys()) == {"$in"}:
            out = []
            for v in idq["$in"]:
                d = self._db.get(self._coll, str(v))
                if d is not None:
                    out.append(d)
            return out

        where: List[Tuple[str, str, Any]] = []
        if filt:
            for k, v in filt.items():
                if k.startswith("$") or "." in k:
                    where = []  # logical/dotted query → can't narrow safely; full scan
                    break
                # only scalar equality on an eq-indexed (hence scalar) field is lossless
                if (not isinstance(v, (dict, list))
                        and self._db.indexes.has_eq(self._coll, k)):
                    where.append((k, "=", v))
        if where:
            plan = empty_plan(self._coll)
            plan["where"] = where
            return self._db.execute(plan)
        return self._db.query(f"FROM {self._coll}")

    def _find_docs(self, filt: dict) -> List[dict]:
        return [d for d in self._candidates(filt) if _match(d, filt or {})]

    # ── inserts ────────────────────────────────────────────────────────────────
    def insert_one(self, document: dict) -> InsertOneResult:
        doc = dict(document)
        _id = doc.get("_id")
        if _id is None:
            _id = ObjectId()
            doc["_id"] = _id
        if self._db.get(self._coll, str(_id)) is not None:
            raise MongoError(f"E11000 duplicate key error: _id {_id!r} already exists")
        self._db.put(self._coll, str(_id), doc, client=self._client)
        return InsertOneResult(_id)

    def insert_many(self, documents: Iterable[dict], ordered: bool = True) -> InsertManyResult:
        ids: List[Any] = []
        for document in documents:
            ids.append(self.insert_one(document).inserted_id)
        return InsertManyResult(ids)

    # ── reads ────────────────────────────────────────────────────────────────────
    def find(self, filter: Optional[dict] = None, projection: Optional[dict] = None) -> Cursor:
        return Cursor(self, filter or {}, projection)

    def find_one(self, filter: Optional[dict] = None,
                 projection: Optional[dict] = None) -> Optional[dict]:
        docs = self.find(filter or {}, projection).limit(1).to_list()
        return docs[0] if docs else None

    def count_documents(self, filter: Optional[dict] = None) -> int:
        return len(self._find_docs(filter or {}))

    def estimated_document_count(self) -> int:
        return len(self._db.query(f"FROM {self._coll}"))

    def distinct(self, key: str, filter: Optional[dict] = None) -> List[Any]:
        seen: List[Any] = []
        for d in self._find_docs(filter or {}):
            val = _get_path(d, key)
            if val is _MISSING:
                continue
            vals = val if isinstance(val, list) else [val]
            for v in vals:
                if v not in seen:
                    seen.append(v)
        return seen

    # ── updates ────────────────────────────────────────────────────────────────
    def _write(self, doc: dict, _id: Any) -> None:
        doc = dict(doc)
        doc["_id"] = _id
        self._db.put(self._coll, str(_id), doc, client=self._client)

    def update_one(self, filter: dict, update: dict, upsert: bool = False) -> UpdateResult:
        return self._update(filter, update, upsert, many=False)

    def update_many(self, filter: dict, update: dict, upsert: bool = False) -> UpdateResult:
        return self._update(filter, update, upsert, many=True)

    def _update(self, filter: dict, update: dict, upsert: bool, many: bool) -> UpdateResult:
        matches = self._find_docs(filter or {})
        if not matches and upsert:
            seed: dict = {}
            # seed equality fields from the filter
            for k, v in (filter or {}).items():
                if not k.startswith("$") and not _is_operator_doc(v) and not isinstance(v, (dict, list)):
                    seed[k] = v
            new_doc = _apply_update(seed, update, on_insert=True)
            _id = new_doc.get("_id") or filter.get("_id") or ObjectId()
            new_doc["_id"] = _id
            self._db.put(self._coll, str(_id), new_doc, client=self._client)
            return UpdateResult(0, 0, upserted_id=_id)

        targets = matches if many else matches[:1]
        modified = 0
        for doc in targets:
            _id = doc.get("_id")
            updated = _apply_update(doc, update)
            updated["_id"] = _id  # _id is immutable
            if updated != doc:
                self._write(updated, _id)
                modified += 1
        return UpdateResult(len(targets), modified)

    def replace_one(self, filter: dict, replacement: dict, upsert: bool = False) -> UpdateResult:
        if any(k.startswith("$") for k in replacement):
            raise MongoError("replace_one requires a replacement document (no update operators).")
        matches = self._find_docs(filter or {})
        if not matches:
            if upsert:
                doc = dict(replacement)
                _id = doc.get("_id") or filter.get("_id") or ObjectId()
                doc["_id"] = _id
                self._db.put(self._coll, str(_id), doc, client=self._client)
                return UpdateResult(0, 0, upserted_id=_id)
            return UpdateResult(0, 0)
        target = matches[0]
        _id = target.get("_id")
        doc = dict(replacement)
        doc["_id"] = _id
        changed = doc != target
        if changed:
            self._write(doc, _id)
        return UpdateResult(1, 1 if changed else 0)

    # ── deletes ────────────────────────────────────────────────────────────────
    def delete_one(self, filter: dict) -> DeleteResult:
        matches = self._find_docs(filter or {})
        if not matches:
            return DeleteResult(0)
        self._db.delete(self._coll, str(matches[0]["_id"]), client=self._client)
        return DeleteResult(1)

    def delete_many(self, filter: dict) -> DeleteResult:
        matches = self._find_docs(filter or {})
        for d in matches:
            self._db.delete(self._coll, str(d["_id"]), client=self._client)
        return DeleteResult(len(matches))

    def drop(self) -> None:
        for d in self._db.query(f"FROM {self._coll}"):
            self._db.delete(self._coll, str(d["_id"]), client=self._client)

    # ── indexes ──────────────────────────────────────────────────────────────────
    def create_index(self, keys: Union[str, List[Tuple[str, int]]], **kwargs: Any) -> str:
        """Create a NEDB index. A Mongo 'text' index → NEDB 'search'; otherwise 'eq'.

        Pass ``nedb_kind="ordered"`` for a range index on a single field.
        """
        kind = kwargs.get("nedb_kind", "eq")
        if isinstance(keys, str):
            fields = [(keys, kwargs.get("nedb_kind", kind))]
        else:
            fields = []
            for field, direction in keys:
                k = "search" if direction == "text" else kwargs.get("nedb_kind", kind)
                fields.append((field, k))
        for field, k in fields:
            self._db.create_index(self._coll, field, k)
        return "_".join(f for f, _ in fields) + "_index"

    # ── aggregation ──────────────────────────────────────────────────────────────
    def aggregate(self, pipeline: List[dict]) -> List[dict]:
        docs: List[dict] = self._db.query(f"FROM {self._coll}")
        for stage in pipeline:
            if len(stage) != 1:
                raise MongoError(f"Each aggregation stage needs exactly one operator: {stage!r}")
            (op, spec), = stage.items()
            if op == "$match":
                docs = [d for d in docs if _match(d, spec)]
            elif op == "$sort":
                for field, dirn in reversed(list(spec.items())):
                    docs.sort(key=lambda d: _sort_key(_get_path(d, field)), reverse=(dirn < 0))
            elif op == "$skip":
                docs = docs[spec:]
            elif op == "$limit":
                docs = docs[:spec]
            elif op == "$count":
                docs = [{spec: len(docs)}]
            elif op == "$project":
                docs = [_project(d, spec) for d in docs]
            elif op == "$group":
                docs = _group(docs, spec)
            else:
                raise MongoUnsupportedError(
                    f"Aggregation stage {op!r} is not yet supported. "
                    f"Supported: $match $group $sort $skip $limit $count $project."
                )
        return docs


def _agg_val(doc: dict, expr: Any) -> Any:
    if isinstance(expr, str) and expr.startswith("$"):
        v = _get_path(doc, expr[1:])
        return None if v is _MISSING else v
    return expr


def _group(docs: List[dict], spec: dict) -> List[dict]:
    id_expr = spec.get("_id")

    def key_of(d: dict) -> Any:
        if id_expr is None:
            return None
        if isinstance(id_expr, str) and id_expr.startswith("$"):
            v = _get_path(d, id_expr[1:])
            return None if v is _MISSING else v
        if isinstance(id_expr, dict):
            return {k: _agg_val(d, e) for k, e in id_expr.items()}
        return id_expr

    groups: Dict[str, List[dict]] = {}
    order: List[Tuple[str, Any]] = []
    for d in docs:
        k = key_of(d)
        hk = json.dumps(k, sort_keys=True, default=str)
        if hk not in groups:
            groups[hk] = []
            order.append((hk, k))
        groups[hk].append(d)

    out: List[dict] = []
    for hk, k in order:
        g = groups[hk]
        entry: dict = {"_id": k}
        for field, acc in spec.items():
            if field == "_id":
                continue
            if not isinstance(acc, dict) or len(acc) != 1:
                raise MongoError(f"Invalid accumulator for {field!r}: {acc!r}")
            (afn, aexpr), = acc.items()
            vals = [_agg_val(d, aexpr) for d in g]
            nums = [v for v in vals if isinstance(v, (int, float)) and not isinstance(v, bool)]
            if afn == "$sum":
                entry[field] = len(g) if aexpr in (1, "1") else sum(nums)
            elif afn == "$avg":
                entry[field] = (sum(nums) / len(nums)) if nums else None
            elif afn == "$min":
                entry[field] = min(nums) if nums else None
            elif afn == "$max":
                entry[field] = max(nums) if nums else None
            elif afn == "$first":
                entry[field] = vals[0] if vals else None
            elif afn == "$last":
                entry[field] = vals[-1] if vals else None
            elif afn == "$push":
                entry[field] = vals
            elif afn == "$addToSet":
                uniq: List[Any] = []
                for v in vals:
                    if v not in uniq:
                        uniq.append(v)
                entry[field] = uniq
            else:
                raise MongoUnsupportedError(f"Accumulator {afn!r} is not supported.")
        out.append(entry)
    return out


# ── Client ──────────────────────────────────────────────────────────────────────────

class MongoCompat:
    """
    MongoDB-compatible client over a NEDB database.

    NEDB is a single logical database, so collections are reached directly::

        mongo = MongoCompat(db)
        mongo["users"]            # item access
        mongo.collection("users") # explicit
        mongo.db["users"]         # pymongo-style db handle (mongo.db is self)

    Every write goes through NEDB's replay-protected, hash-chained log; pass
    ``client`` to scope nonce counters per service.
    """

    def __init__(self, db: Any, client: str = "mongo-compat"):
        self._db = db
        self._client = client

    def collection(self, name: str) -> Collection:
        return Collection(self._db, name, self._client)

    def __getitem__(self, name: str) -> Collection:
        return self.collection(name)

    @property
    def db(self) -> "MongoCompat":
        return self

    def list_collection_names(self) -> List[str]:
        names = set()
        for key in self._db.store.keys():
            if ":" in key:
                names.add(key.split(":", 1)[0])
        return sorted(names)

    def drop_collection(self, name: str) -> None:
        self.collection(name).drop()


# pymongo users reach for MongoClient — provide it as an alias.
MongoClient = MongoCompat
