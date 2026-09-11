# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.wrap_mongo — wrap an existing PyMongo connection/collection with NEDB's layer-2.

ONE LINE. Your existing Mongo code doesn't change. New parts of your app get
time-travel, bi-temporal causal provenance, and NQL on top of your collections.

    from nedb import wrap_mongo
    from pymongo import MongoClient

    client = wrap_mongo(MongoClient("mongodb://localhost:27017"), db_name="app")
    db = client.app                                  # passthrough — unchanged

    # ── Surface 2: NEDB layer ─────────────────────────────────────────────
    client.nedb.register("app.drivers", collection="driver")   # db.collection
    imported = client.nedb.backfill()                          # one-time import
    client.nedb.shadow_writes = True

    # Your app runs unchanged — and each shadow_row() call chains the write:
    db.drivers.insert_one({"name": "Bob", "status": "active"})
    client.nedb.shadow_row("app.drivers", "driver")

    client.nedb.query('FROM driver WHERE status = "active"')
    client.nedb.verify()   # → True

Engine selection mirrors wrap_redis: backend="auto" → nedbd (if nedbd_url=)
→ embedded v2/v3 DAG (if the Rust wheel is installed) → v1 AOF engine.

Isolation guarantee: NEDB NEVER writes to your Mongo collections.

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from .wrap_core import WrapSurface, open_engine


class MongoSurface(WrapSurface):
    """The `.nedb` attribute of a wrapped MongoClient."""

    def __init__(self, client: Any, db_name: str, engine=None, persist=None):
        super().__init__(db_name, engine=engine, persist=persist)
        self._client = client

    def _host_scan(self, mapping, batch_size: int):
        """Yield (_id, doc) for every doc in the mapped db.collection."""
        try:
            dbname, collname = mapping.pattern.split(".", 1)
        except ValueError:
            return
        try:
            coll = self._client[dbname][collname]
            for doc in coll.find({}):
                yield str(doc.get("_id")), {k: v for k, v in doc.items()
                                            if k != "_id"}
        except Exception:
            return

    def _shadow_doc(self, mapping, key, args, kwargs) -> Optional[Dict[str, Any]]:
        return None  # Mongo shadowing is explicit via shadow_row()

    def shadow_row(self, db_coll: str, collection: Optional[str] = None,
                   doc: Optional[Dict[str, Any]] = None,
                   op: str = "UPSERT") -> None:
        """Chain one Mongo write into NEDB. Call after your insert/update.

        Registers the mapping on first use. doc=None chains a DELETE tombstone.
        No-ops unless shadow_writes=True.
        """
        if not self.shadow_writes:
            return
        try:
            coll = collection or db_coll.rsplit(".", 1)[-1]
            if not any(m.pattern == db_coll for m in self._mappings):
                self.register(db_coll, coll)
            if doc is None:
                self.put("__mongo_shadow__", f"{db_coll}:del:{op}",
                         {"ns": db_coll, "_op": "DELETE"}, client="__shadow__")
            else:
                did = str(doc.get("_id") or op)
                body = {k: v for k, v in doc.items() if k != "_id"}
                self.put(coll if coll != db_coll else coll, did,
                         {**body, "_ns": db_coll, "_op": op},
                         client="__shadow__")
        except Exception:
            pass


class WrappedMongoClient:
    """
    Transparent MongoClient proxy with NEDB shadow layer.

    Surface 1 (get_database / __getitem__ / …): passes through unchanged.
    Surface 2 (.nedb.*): full NEDB API + backfill + explicit shadow_row().
    """

    def __init__(self, client: Any, db_name: str,
                 nedbd_url: Optional[str] = None,
                 nedbd_token: Optional[str] = None,
                 backend: str = "auto",
                 dag_path: Optional[str] = None,
                 dag_tmk: Optional[str] = None):
        object.__setattr__(self, "_client", client)
        object.__setattr__(self, "_db_name", db_name)
        engine, _ = open_engine(backend=backend, db_name=db_name,
                                nedbd_url=nedbd_url, nedbd_token=nedbd_token,
                                dag_path=dag_path, dag_tmk=dag_tmk)
        object.__setattr__(self, "nedb", MongoSurface(client, db_name, engine=engine))

    def __getattr__(self, name: str) -> Any:
        return getattr(object.__getattribute__(self, "_client"), name)

    def __getitem__(self, name: str):
        return object.__getattribute__(self, "_client")[name]

    def __repr__(self):
        client = object.__getattribute__(self, "_client")
        db = object.__getattribute__(self, "_db_name")
        return f"<WrappedMongoClient db_name={db!r} mongo={client!r}>"


def wrap_mongo(client: Any, db_name: str = "default",
               nedbd_url: Optional[str] = None,
               nedbd_token: Optional[str] = None,
               backend: str = "auto",
               dag_path: Optional[str] = None,
               dag_tmk: Optional[str] = None) -> WrappedMongoClient:
    """
    Wrap an existing pymongo.MongoClient with NEDB's layer-2.

    Args mirror wrap_redis: backend="auto" picks nedbd (if nedbd_url=) →
    embedded DAG (if the native wheel is installed) → v1 AOF engine.
    """
    return WrappedMongoClient(client, db_name, nedbd_url=nedbd_url,
                              nedbd_token=nedbd_token, backend=backend,
                              dag_path=dag_path, dag_tmk=dag_tmk)
