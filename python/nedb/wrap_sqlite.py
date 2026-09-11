# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.wrap_sqlite — wrap an existing SQLite database with NEDB's layer-2.

ONE LINE. Your existing sqlite3 code doesn't change. New parts of your app get
time-travel, bi-temporal, causal provenance, and NQL on top of your tables.

    from nedb import wrap_sqlite
    import sqlite3

    conn = wrap_sqlite(sqlite3.connect("app.db"), db_name="app")

    # ── Step 1: register table mappings ──────────────────────────────────
    conn.nedb.register("drivers", collection="driver")
    conn.nedb.register("trips",   collection="trip")

    # ── Step 2: backfill existing rows into NEDB ─────────────────────────
    conn.nedb.backfill()

    # ── Step 3: enable write shadowing ───────────────────────────────────
    conn.nedb.shadow_writes = True

    # Your app runs unchanged — INSERT/UPDATE/DELETE are shadowed
    cur = conn.cursor()
    cur.execute("INSERT INTO drivers (name, status) VALUES (?, ?)",
                ("Bob", "active"))
    conn.commit()

    # New app — NEDB features on the shadowed data
    conn.nedb.query('FROM driver WHERE status = "active"')
    conn.nedb.verify()   # → True

Engine selection mirrors wrap_redis:
    backend="auto" → embedded v2/v3 DAG (Rust wheel) → v1 AOF fallback
    backend="dag"  → force embedded DAG (dag_path= for a durable store)
    nedbd_url=     → HTTP nedbd (v1 AOF, nedbd --dag, or nedbd --dag-v3)

Isolation guarantee: NEDB NEVER writes to your tables. Shadow data lives only
in the NEDB engine (embedded store or nedbd server), keyed nedb:{db_name}:*.

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Callable, Dict, List, Optional, Tuple

from .wrap_core import (ShadowCursor, WrapSurface, open_engine, target_table,
                        write_op)


# ── The .nedb surface for SQLite ─────────────────────────────────────────────

class SqliteSurface(WrapSurface):
    """
    The `.nedb` attribute of a wrapped SQLite connection.

    register(table, collection, row_parser=…) maps a host TABLE (not a key
    glob — SQLite has structure, so patterns are just table names) to a NEDB
    collection. Row dicts come straight from sqlite3.Row.
    """

    def __init__(self, conn: sqlite3.Connection, db_name: str, engine=None,
                 persist=None):
        super().__init__(db_name, engine=engine, persist=persist)
        self._conn = conn

    # ── host hooks ───────────────────────────────────────────────────────────

    def register(self, pattern: str, collection: str,              # type: ignore[override]
                 id_extractor=None, value_parser=None, value_type: str = "string",
                 pk: Optional[str] = None):
        """register(table, collection) — `pk` accepted for symmetry with Postgres.

        SQLite addresses rows by `rowid`, which every ordinary table has, so
        the primary key is not needed to shadow a write. It is accepted so
        automatic discovery can offer it uniformly across the SQL adapters.
        """
        return super().register(pattern, collection,
                                id_extractor, value_parser, value_type)

    def _discover_tables(self):
        """Every ordinary table in the database, with its declared PK.

        Read from `sqlite_master`, so turning shadowing on covers the whole
        database without the caller naming a single table. Internal
        `sqlite_*` tables and views are skipped: a view is not writable, so
        mirroring one would only ever produce an empty mapping.
        """
        try:
            names = [r[0] for r in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        except sqlite3.Error:
            return
        for table in names:
            pk = None
            try:
                for r in self._conn.execute(f'PRAGMA table_info("{table}")'):
                    # (cid, name, type, notnull, dflt_value, pk)
                    if r[5]:
                        pk = r[1]
                        break
            except sqlite3.Error:
                pass
            yield table, pk

    def _host_scan(self, mapping, batch_size: int):
        """Yield (rowid, row_dict) for every row of the mapped table."""
        table = mapping.pattern  # for sqlite, the "pattern" is the table name
        try:
            # Column metadata via PRAGMA (stable, no SELECT * rowid aliasing
            # surprises when the table declares INTEGER PRIMARY KEY).
            cols = [r[1] for r in self._conn.execute(f'PRAGMA table_info("{table}")')]
            cur = self._conn.execute(f'SELECT rowid AS __nedb_rowid, * FROM "{table}"')
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    rowid = row[0]
                    d = dict(zip(cols, row[1:]))
                    # A table with INTEGER PRIMARY KEY aliases rowid in *,
                    # duplicating it under its real name — harmless: keep the
                    # dict but drop a duplicate __nedb_rowid key if any.
                    d.pop("__nedb_rowid", None)
                    yield str(rowid), d
        except sqlite3.Error:
            return

    def _shadow_doc(self, mapping, key, args, kwargs) -> Optional[Dict[str, Any]]:
        """Intercepted write → NEDB doc.

        The SQLite proxy passes shadow metadata through kwargs:
            _nedb_shadow = (table, op, rowid, after_row_dict_or_None)
        """
        meta = kwargs.get("_nedb_shadow")
        if not meta:
            return None
        table, op, rowid, after = meta
        if after is None:
            return None  # DELETE — tombstone handled by the proxy, not here
        doc = dict(after)
        doc["_op"] = op
        return doc

    # ── SQLite-native extras ─────────────────────────────────────────────────

    def sql(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Run raw SQL on the host connection (read helper)."""
        cur = self._conn.execute(query, params)
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── The transparent proxy connection ─────────────────────────────────────────

class WrappedSqlite:
    """
    Transparent sqlite3.Connection proxy with NEDB shadow layer.

    Surface 1 (execute/commit/…): every call passes through unchanged; write
    statements on registered tables are shadowed after they succeed.

    Surface 2 (.nedb.*): full NEDB API + backfill + write shadowing.
    """

    _WRITE_PREFIXES = ("INSERT", "UPDATE", "DELETE", "REPLACE")

    def __init__(self, conn: sqlite3.Connection, db_name: str,
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
        object.__setattr__(self, "nedb", SqliteSurface(conn, db_name, engine=engine))

    # ── proxying ─────────────────────────────────────────────────────────────

    def __getattr__(self, name: str) -> Any:
        conn = object.__getattribute__(self, "_conn")
        nedb = object.__getattribute__(self, "nedb")
        attr = getattr(conn, name)

        if name == "execute":
            return self._execute
        if name == "executemany":
            return self._executemany
        if name == "cursor":
            return self._cursor
        if not callable(attr):
            return attr

        def _passthrough(*a, **kw):
            return attr(*a, **kw)
        return _passthrough

    # ── the cursor path ─────────────────────────────────────────────────────
    #
    # `conn.cursor().execute(...)` is the canonical DB-API idiom — and it used
    # to bypass shadowing completely, because only `conn.execute` was
    # intercepted. The host got the row, NEDB did not, and nothing said so.
    # Every path a write can take has to go through the same door.

    def _cursor(self, *a, **kw):
        conn = object.__getattribute__(self, "_conn")
        return ShadowCursor(conn.cursor(*a, **kw), self._cursor_execute)

    def _cursor_execute(self, cur, proxy, sql, params, a, kw):
        """`ShadowCursor.execute` → run on the real cursor, then shadow."""
        nedb = object.__getattribute__(self, "nedb")
        params = () if params is None else params
        pre_rowids, shadowing = self._before_write(nedb, sql, params)
        result = cur.execute(sql, params, *a, **kw)
        self._after_write(nedb, sql, cur, pre_rowids, shadowing)
        # sqlite3 returns the cursor itself; hand back the PROXY so a chained
        # `.execute(...).fetchone()` still goes through the shadow layer.
        return proxy if result is cur else result

    def _before_write(self, nedb, sql: str, params):
        """Resolve affected rowids before the statement runs, if needed."""
        head = sql.lstrip().upper()
        shadowing = head.startswith(self._WRITE_PREFIXES) and nedb.shadow_writes
        if not shadowing:
            # A write to a table nobody is mirroring is a visible gap, not a
            # silent one — but only while shadowing is meant to be on.
            if nedb.shadow_writes and write_op(sql) and \
                    nedb.mapping_for_table(target_table(sql)) is None:
                nedb.note_unmirrored(target_table(sql))
            return None, False
        if nedb.mapping_for_table(target_table(sql)) is None:
            nedb.note_unmirrored(target_table(sql))
            return None, False
        if head.startswith("INSERT"):
            return None, True
        try:
            return self._affected_rowids(nedb, sql, params), True
        except Exception as e:
            nedb.note_shadow_error(e)
            return None, True

    def _after_write(self, nedb, sql: str, cur, pre_rowids, shadowing) -> None:
        if not shadowing:
            return
        try:
            self._shadow_sql(nedb, sql, cur, pre_rowids)
        except Exception as e:
            # Must never break the host call — but must never be invisible
            # either. Counted on nedb.shadow_errors, reported through
            # nedb.on_shadow_error, and re-raised when strict_shadow is set.
            nedb.note_shadow_error(e)

    def _executemany(self, sql: str, seq_of_params):
        last = None
        for params in seq_of_params:
            last = self._execute(sql, params)
        return last

    def _execute(self, sql: str, params: tuple = ()):
        """`conn.execute()` — the sqlite3 convenience path.

        Shares `_before_write`/`_after_write` with the cursor path, so the two
        cannot drift: the reason the cursor path was broken for so long is that
        it was a SEPARATE path with no shadowing at all.

        UPDATE and DELETE need the affected rowids captured BEFORE the
        statement runs -- see _affected_rowids for why cursor.lastrowid
        cannot be used for them.
        """
        conn = object.__getattribute__(self, "_conn")
        nedb = object.__getattribute__(self, "nedb")
        pre_rowids, shadowing = self._before_write(nedb, sql, params)
        cur = conn.execute(sql, params)
        self._after_write(nedb, sql, cur, pre_rowids, shadowing)
        return cur

    def _affected_rowids(self, nedb, sql: str, params: tuple):
        """Rowids an UPDATE/DELETE is about to touch, resolved before it runs.

        `cursor.lastrowid` is ONLY meaningful after an INSERT. After an UPDATE
        or DELETE it still holds the id of the last row *inserted* on that
        connection, so shadowing off it recorded a completely unrelated row --
        a FALSE provenance record, which is worse than none at all.

        The affected set is therefore resolved up front with
        `SELECT rowid FROM <table> WHERE <same predicate>`.

        Placeholders: for UPDATE, some `?` may live in the SET clause, so the
        leading params belonging to SET are skipped and only the remainder is
        bound to the WHERE. Anything that cannot be parsed with confidence
        raises, and the caller records a COUNTED shadow error rather than
        inventing a row.
        """
        import re
        mapping = self._mapping_for_sql(nedb, sql)
        if mapping is None:
            return None
        conn = object.__getattribute__(self, "_conn")
        table = mapping.pattern

        m = re.search(r"\bWHERE\b(.*)$", sql, re.IGNORECASE | re.DOTALL)
        if not m:
            # No predicate: an unqualified UPDATE/DELETE touches every row.
            return [r[0] for r in conn.execute(f'SELECT rowid FROM "{table}"')]

        where = m.group(1)
        where_params = params or ()
        if sql.lstrip().upper().startswith("UPDATE") and where_params:
            set_part = sql[:m.start()]
            n_set = set_part.count("?")
            if n_set > len(where_params):
                raise ValueError(
                    "cannot attribute placeholders between SET and WHERE; "
                    "refusing to shadow a row that may be the wrong one")
            where_params = tuple(where_params)[n_set:]

        rows = conn.execute(
            f'SELECT rowid FROM "{table}" WHERE {where}', where_params)
        return [r[0] for r in rows]

    @staticmethod
    def _mapping_for_sql(nedb, sql: str):
        """The mapping for the table this statement WRITES TO.

        Was: "does any registered table name appear anywhere in this SQL?" —
        a substring test. With one table registered that mostly worked. With
        every table registered, which is what automatic discovery does, a
        write to `drivers_archive` matched the mapping for `drivers` and was
        shadowed into the wrong collection under the wrong row's id.
        FALSE PROVENANCE: a record that looks authoritative and describes
        something that never happened. The target is now parsed out of the
        statement.
        """
        return nedb.mapping_for_table(target_table(sql))

    def _shadow_sql(self, nedb: SqliteSurface, sql: str, cur: sqlite3.Cursor,
                    pre_rowids=None) -> None:
        """Mirror a write into the SAME collection backfill() writes to.

        Two defects lived here, and the second hid the first.

        1. ``cols = [d[0] for d in cur.description] or [...]`` -- sqlite3 sets
           ``cursor.description`` to **None** after an INSERT (it is only
           populated for SELECT). The list comprehension therefore raised
           ``TypeError`` before ``or`` could ever evaluate its fallback, and
           ``_execute`` swallowed it. Every automatic INSERT shadow silently
           recorded nothing while ``verify()`` kept returning True.

        2. Rows were written to ``__sql_shadow__`` while ``backfill()`` writes
           to the registered collection. Even with (1) fixed, a user who ran
           ``register("rides", collection="ride")`` and queried ``FROM ride``
           got the historical rows and none of the live ones -- a partial
           answer, silently.

        Both are fixed: the description is guarded, and every op lands in
        ``mapping.collection`` under the same id ``_host_scan`` yields
        (``str(rowid)``), so an UPDATE supersedes the backfilled document
        instead of accumulating beside it.
        """
        conn = object.__getattribute__(self, "_conn")
        mapping = self._mapping_for_sql(nedb, sql)
        if mapping is None:
            return
        table = mapping.pattern

        up = sql.lstrip().upper()
        op = ("DELETE" if up.startswith("DELETE")
              else "UPDATE" if up.startswith("UPDATE")
              else "INSERT")

        if op == "INSERT":
            rowids = [cur.lastrowid]
        else:
            if pre_rowids is None:
                raise ValueError(
                    f"{op} on {table!r}: affected rows could not be resolved, "
                    "nothing shadowed (see nedb.last_shadow_error)")
            rowids = pre_rowids

        if op == "DELETE":
            # Tombstone rather than a NEDB delete: deleting would drop history,
            # and surviving history is the entire point. Written into the mapped
            # collection so `FROM ride` shows the row as deleted rather than
            # appearing to still exist.
            for rid in rowids:
                nedb.put(mapping.collection, str(rid),
                         {"_table": table, "_op": "DELETE", "_deleted": True,
                          "_rowid": str(rid)},
                         client="__shadow__")
            return

        # Read each affected row back post-write (inside the open transaction).
        desc = conn.execute(f'SELECT * FROM "{table}" LIMIT 0').description
        cols = [d[0] for d in desc]
        for rid in rowids:
            row = conn.execute(
                f'SELECT * FROM "{table}" WHERE rowid = ?', (rid,)).fetchone()
            if row is None:
                continue
            doc = dict(zip(cols, row))
            nedb.put(mapping.collection, str(rid),
                     {**doc, "_table": table, "_op": op, "_source": "shadow"},
                     client="__shadow__")

    def __repr__(self):
        conn = object.__getattribute__(self, "_conn")
        db = object.__getattribute__(self, "_db_name")
        return f"<WrappedSqlite db_name={db!r} sqlite={conn!r}>"


# ── Entry point ───────────────────────────────────────────────────────────────

def wrap_sqlite(conn: sqlite3.Connection, db_name: str = "default",
                nedbd_url: Optional[str] = None,
                nedbd_token: Optional[str] = None,
                backend: str = "auto",
                dag_path: Optional[str] = None,
                dag_tmk: Optional[str] = None) -> WrappedSqlite:
    """
    Wrap an existing sqlite3.Connection with NEDB's layer-2 features.

    Args mirror wrap_redis: backend="auto" picks nedbd (if nedbd_url=) →
    embedded DAG (if the native wheel is installed) → v1 AOF engine.
    dag_path=/dag_tmk= configure a durable/encrypted embedded DAG store.
    """
    return WrappedSqlite(conn, db_name, nedbd_url=nedbd_url,
                         nedbd_token=nedbd_token, backend=backend,
                         dag_path=dag_path, dag_tmk=dag_tmk)
