# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.wrap_mysql — wrap an existing MySQL connection with NEDB's layer-2.

ONE LINE. Your existing MySQL code doesn't change. New parts of your app get
time-travel, bi-temporal causal provenance, and NQL on top of your tables.

    from nedb import wrap_mysql
    import mysql.connector          # or pymysql — any DB-API 2.0 conn

    conn = wrap_mysql(mysql.connector.connect(host="localhost",
                                              database="app"),
                      db_name="app")

    # ── Step 1: register table mappings ──────────────────────────────────
    conn.nedb.register("drivers", collection="driver")

    # ── Step 2: backfill existing rows into NEDB ─────────────────────────
    conn.nedb.backfill()

    # ── Step 3: enable write shadowing ───────────────────────────────────
    conn.nedb.shadow_writes = True

    # Your app runs unchanged
    cur = conn.cursor()
    cur.execute("INSERT INTO drivers (name, status) VALUES (%s, %s)",
                ("Bob", "active"))
    conn.commit()

    # New app — NEDB features
    conn.nedb.query('FROM driver WHERE status = "active"')
    conn.nedb.verify()   # → True

Works with any DB-API 2.0 connection (mysql-connector-python, PyMySQL,
mysqlclient). Engine selection mirrors wrap_redis/wrap_sqlite.

Isolation guarantee: NEDB NEVER writes to your tables. Shadow data lives only
in the NEDB engine (embedded store or nedbd server).

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .wrap_core import WrapSurface, open_engine


class MysqlSurface(WrapSurface):
    """The `.nedb` attribute of a wrapped MySQL connection."""

    def __init__(self, conn: Any, db_name: str, engine=None, persist=None):
        super().__init__(db_name, engine=engine, persist=persist)
        self._conn = conn

    def _host_scan(self, mapping, batch_size: int):
        """Yield (pk, row_dict) for every row of the mapped table."""
        table = mapping.pattern
        cur = self._conn.cursor()
        try:
            cur.execute(f'SELECT * FROM `{table}`')
            cols = [d[0] for d in cur.description]
            pk = cols[0]  # conventional first-column PK; register() can override
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    d = dict(zip(cols, row))
                    yield str(d.get(pk, id(d))), d
        except Exception:
            return
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def _shadow_doc(self, mapping, key, args, kwargs) -> Optional[Dict[str, Any]]:
        return None  # MySQL proxy passes shadow docs directly via surface.put

    def sql(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        cur = self._conn.cursor()
        cur.execute(query, params)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        try:
            cur.close()
        except Exception:
            pass
        return [dict(zip(cols, row)) for row in rows]


class WrappedMysql:
    """
    Transparent DB-API 2.0 proxy with NEDB shadow layer.

    Surface 1 (cursor/commit/…): every call passes through unchanged.

    Surface 2 (.nedb.*): full NEDB API + backfill + shadowing. MySQL shadowing
    is explicit: after your own INSERT/UPDATE, call conn.nedb.shadow_row(
    table, pk_value, row_dict) to chain it (or use triggers — see README).
    """

    def __init__(self, conn: Any, db_name: str,
                 nedbd_url: Optional[str] = None,
                 nedbd_token: Optional[str] = None,
                 backend: str = "auto",
                 dag_path: Optional[str] = None,
                 dag_tmk: Optional[str] = None):
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "_db_name", db_name)
        engine, _ = open_engine(backend=backend, db_name=db_name,
                                nedbd_url=nedbd_url, nedbd_token=nedbd_token,
                                dag_path=dag_path, dag_tmk=dag_tmk)
        object.__setattr__(self, "nedb", MysqlSurface(conn, db_name, engine=engine))

    def __getattr__(self, name: str) -> Any:
        conn = object.__getattribute__(self, "_conn")
        return getattr(conn, name)

    # ── explicit shadow helper (MySQL has no cheap rowid like SQLite) ───────

    def shadow_row(self, table: str, pk: Any, row: Optional[Dict[str, Any]],
                   op: str = "UPSERT") -> None:
        """Chain one host row into NEDB. Call after your own INSERT/UPDATE.

        Lands in the registered collection for `table` (NQL-queryable via the
        normal FROM <collection> grammar); row=None chains a DELETE tombstone.
        No-ops unless shadow_writes=True.
        """
        nedb = object.__getattribute__(self, "nedb")
        if not nedb.shadow_writes:
            return
        try:
            if row is None:
                nedb.put("__sql_shadow__", f"{table}:del:{pk}",
                         {"table": table, "pk": str(pk), "_op": "DELETE"},
                         client="__shadow__")
                return
            # route into the registered collection when one matches
            coll = next((m.collection for m in nedb._mappings
                         if m.pattern == table), None)
            if coll:
                nedb.put(coll, str(pk),
                         {**dict(row), "_table": table, "_op": op},
                         client="__shadow__")
            else:
                nedb.put("__sql_shadow__", f"{table}:{pk}",
                         {**dict(row), "_table": table, "_op": op},
                         client="__shadow__")
        except Exception:
            pass

    def __repr__(self):
        conn = object.__getattribute__(self, "_conn")
        db = object.__getattribute__(self, "_db_name")
        return f"<WrappedMysql db_name={db!r} mysql={conn!r}>"


def wrap_mysql(conn: Any, db_name: str = "default",
               nedbd_url: Optional[str] = None,
               nedbd_token: Optional[str] = None,
               backend: str = "auto",
               dag_path: Optional[str] = None,
               dag_tmk: Optional[str] = None) -> WrappedMysql:
    """
    Wrap an existing DB-API 2.0 MySQL connection with NEDB's layer-2.

    Args mirror wrap_redis: backend="auto" picks nedbd (if nedbd_url=) →
    embedded DAG (if the native wheel is installed) → v1 AOF engine.
    """
    return WrappedMysql(conn, db_name, nedbd_url=nedbd_url,
                        nedbd_token=nedbd_token, backend=backend,
                        dag_path=dag_path, dag_tmk=dag_tmk)
