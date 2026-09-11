# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.wrap_postgresql — wrap an existing psycopg/DB-API connection with NEDB's layer-2.

ONE LINE, then ONE FLAG. Your existing Postgres code does not change. Every
write it makes gets mirrored into a tamper-evident, append-only chain with
time travel, bi-temporal validity, and causal provenance.

    from nedb import wrap_postgresql
    import psycopg2                 # or psycopg (v3) — any DB-API 2.0 conn

    conn = wrap_postgresql(psycopg2.connect("dbname=app"), db_name="app")
    conn.nedb.shadow_writes = True  # ← that is the whole setup

    # Your app runs UNCHANGED. Nothing else to configure.
    cur = conn.cursor()
    cur.execute("INSERT INTO drivers (name, status) VALUES (%s, %s)",
                ("Bob", "active"))
    conn.commit()

    conn.nedb.query('FROM drivers WHERE status = "active"')
    conn.nedb.query('FROM drivers AS OF 0')     # the value before the last write
    conn.nedb.verify()                          # → True

# What changed, and why the old shape was wrong

This module used to require three steps — `register()` every table, then
`backfill()`, then `shadow_writes = True` — and even then it mirrored nothing
until you ALSO called `shadow_row(table, pk, row)` by hand after every write.

That is not a feature with setup cost; it is a feature that hands the work
back to the caller and cannot ever be finished. Worse, it failed silently in
two directions:

  * `shadow_writes = True` with no `register()` calls was a NO-OP. The flag
    read "on", mirrored nothing, raised nothing, and counted nothing.
  * registration was opt-IN, so the normal outcome was SILENT PARTIAL
    COVERAGE — three tables of twelve, which looks exactly like a complete
    audit trail until the day you need it.

So coverage is now automatic and opt-OUT:

  * `shadow_writes = True` reads `information_schema` and mirrors EVERY table,
    with its real primary key. No registration, no table list.
  * writes are intercepted on the CURSOR, which is the only path psycopg has —
    there is no `conn.execute` in psycopg — so `cur.execute`, `cur.executemany`
    and `with conn.cursor() as cur` are all covered.
  * a write that could not be mirrored lands in `nedb.unmirrored_tables`
    instead of vanishing. `assert not conn.nedb.unmirrored_tables` is a real
    check.

`register()` survives as an OVERRIDE (rename a collection, supply a parser),
never as a prerequisite. `exclude` keeps tables out; `exclude_columns` keeps
COLUMNS out, which matters more than it sounds — see below.

# How a write is captured

Postgres has no `lastrowid`, so the affected rows are obtained from Postgres
itself: the statement is rewritten to add `RETURNING *` when it does not
already have one, the returned rows are read, and each is chained into NEDB.

That is exact. It needs no predicate re-parsing, no placeholder attribution
between SET and WHERE, and it treats INSERT, UPDATE and DELETE identically.
When the `RETURNING` was added by this layer the rows are consumed here and
the cursor is presented as having no results, so calling code sees exactly
what it saw before.

# Keeping secrets OUT of an append-only store

NEDB cannot forget. That is the product — and it is precisely wrong for a
password hash, an API key, a card number, or a row somebody has a right to
erase. Mirroring those into a store designed to be immutable creates a
liability, not an audit trail:

    conn.nedb.exclude_columns = {"password_hash", "*_secret", "ssn"}
    conn.nedb.exclude = {"sessions", "audit_log_*"}

Nothing is excluded by default. Guessing which of your columns are sensitive
would be its own silent wrong answer, so the decision is yours and explicit.

# The limit worth knowing

Interception sees writes made through THIS connection. A production Postgres
is also written by psql, cron jobs, migration tools, and other services — and
none of those pass through here. For whole-database coverage independent of the
client, the right mechanism is Postgres logical replication (a replication slot
decoded with the built-in `pgoutput`), which observes every committed change
whatever made it. That needs `wal_level = logical` and a replication role, and
it is not implemented yet.

So: this covers your application's writes completely, and it does not pretend
to cover writes it cannot see.

Works with any DB-API 2.0 connection (psycopg2, psycopg 3). Engine selection
mirrors wrap_redis: backend="auto" → nedbd (if nedbd_url=) → embedded v2/v3 DAG
(if the Rust wheel is installed) → v1 AOF engine.

Isolation guarantee: NEDB NEVER writes to your tables. Shadow data lives only
in the NEDB engine.

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .wrap_core import (ShadowCursor, WrapSurface, open_engine, target_table,
                        write_op)


class PostgresSurface(WrapSurface):
    """The `.nedb` attribute of a wrapped Postgres connection."""

    def __init__(self, conn: Any, db_name: str, engine=None, persist=None):
        super().__init__(db_name, engine=engine, persist=persist)
        self._conn = conn
        self._pks: Dict[str, str] = {}   # table → primary-key column
        #: Tables whose mirrored rows may be incomplete, because the caller's
        #: own `RETURNING` list omitted the primary key and so the full row
        #: could not be fetched. Visible rather than silent.
        self.partial_shadows: set = set()

    def register(self, pattern: str, collection: str,             # type: ignore[override]
                 id_extractor=None, value_parser=None, value_type: str = "string",
                 pk: Optional[str] = None):
        """register(table, collection, pk="id") — an OVERRIDE, not a prerequisite.

        Use it to rename a collection or attach a parser. Coverage itself comes
        from `shadow_writes = True`, which discovers every table.
        """
        surface = super().register(pattern, collection,
                                   id_extractor, value_parser, value_type)
        if pk:
            self._pks[pattern] = pk
        return surface

    # ── automatic discovery ─────────────────────────────────────────────────

    def _discover_tables(self) -> Iterable[Tuple[str, Optional[str]]]:
        """Every ordinary table in the connected schemas, with its real PK.

        Read from `information_schema` and `pg_index`, so turning shadowing on
        covers the whole database without the caller naming a single table.

        Views and foreign tables are skipped (`BASE TABLE` only): a view is not
        directly writable, so a mapping for one could never fire. System
        schemas are skipped for the obvious reason — `pg_catalog` is Postgres's
        bookkeeping, not the caller's data.
        """
        sql = """
            SELECT c.relname,
                   (SELECT a.attname
                      FROM pg_index i
                      JOIN pg_attribute a
                        ON a.attrelid = i.indrelid
                       AND a.attnum   = ANY (i.indkey)
                     WHERE i.indrelid = c.oid AND i.indisprimary
                     ORDER BY a.attnum
                     LIMIT 1) AS pk
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relkind = 'r'
               AND n.nspname NOT IN ('pg_catalog', 'information_schema')
               AND n.nspname NOT LIKE 'pg_toast%'
             ORDER BY c.relname
        """
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            for table, pk in cur.fetchall():
                yield table, pk
        finally:
            try:
                cur.close()
            except Exception:
                pass

    # ── backfill ────────────────────────────────────────────────────────────

    def _host_scan(self, mapping, batch_size: int):
        """Yield (pk_value, row_dict) for every row of the mapped table."""
        table = mapping.pattern
        pk = self._pks.get(table)
        cur = self._conn.cursor()
        try:
            if pk:
                cur.execute(f'SELECT * FROM "{table}" ORDER BY "{pk}"')
            else:
                cur.execute(f'SELECT * FROM "{table}"')
            if cur.description is None:
                return
            cols = [d[0] for d in cur.description]
            pk = pk or cols[0]
            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                for row in rows:
                    d = dict(zip(cols, row))
                    yield str(d.get(pk, id(d))), d
        except Exception as e:
            self.note_shadow_error(e)
            return
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def _shadow_doc(self, mapping, key, args, kwargs) -> Optional[Dict[str, Any]]:
        # Postgres shadowing goes through the cursor (see PostgresShadow),
        # which writes rows directly rather than routing through shadow().
        return None

    # ── row id + chaining ───────────────────────────────────────────────────

    def row_id(self, table: str, row: Dict[str, Any]) -> str:
        """The NEDB document id for a host row.

        The table's real primary key when Postgres reported one, so an UPDATE
        supersedes the same document that `backfill()` wrote rather than
        accumulating a second copy beside it. Falling back to the first column
        is a guess, and a composite key is joined rather than truncated — a
        truncated composite key would collide two distinct rows onto one
        document, which is false provenance.
        """
        pk = self._pks.get(table)
        if pk and pk in row:
            return str(row[pk])
        if row:
            return "|".join(str(v) for v in list(row.values())[:1])
        return ""

    def chain_row(self, table: str, row: Dict[str, Any], op: str) -> None:
        """Mirror one host row into its collection. Never raises into the host."""
        if not self.shadow_writes:
            return
        try:
            mapping = self.mapping_for_table(table)
            if mapping is None:
                self.note_unmirrored(table)
                return
            doc = self.redact(dict(row))
            doc["_table"] = table
            doc["_op"] = op
            if op == "DELETE":
                # A tombstone, not a NEDB delete: dropping the document would
                # discard the history this layer exists to keep. The row is
                # marked deleted so `FROM drivers` shows it as gone rather
                # than appearing to still exist.
                doc["_deleted"] = True
            self._db.put(mapping.collection, self.row_id(table, row), doc,
                         client="__shadow__")
            self._persist_after()
        except Exception as e:
            self.note_shadow_error(e)


# ── statement rewriting ──────────────────────────────────────────────────────

_HAS_RETURNING = re.compile(r"\bRETURNING\b", re.IGNORECASE)


def _has_bare_semicolon(sql: str) -> bool:
    """True if a `;` appears outside a string literal before the very end.

    Guards the `RETURNING` rewrite: appending to a multi-statement payload
    would produce `INSERT …; UPDATE … RETURNING *`, attaching the clause to
    the wrong statement. Better to decline the rewrite and record a visible
    gap than to mirror the wrong thing.
    """
    in_s = False
    body = sql.rstrip().rstrip(";")
    for ch in body:
        if ch == "'":
            in_s = not in_s
        elif ch == ";" and not in_s:
            return True
    return False


def add_returning(sql: str) -> Tuple[str, bool]:
    """`(sql, added)` — append `RETURNING *` unless one is already there.

    Postgres has no `lastrowid`, so this is how the affected rows are
    obtained: from Postgres itself, exactly, for INSERT, UPDATE and DELETE
    alike. The alternative — re-running the WHERE predicate as a SELECT and
    attributing placeholders between SET and WHERE — is guesswork that gets
    the wrong rows when it is wrong, and the wrong rows are worse than none.
    """
    if _HAS_RETURNING.search(sql) or _has_bare_semicolon(sql):
        return sql, False
    return sql.rstrip().rstrip(";") + " RETURNING *", True


class PostgresShadow(ShadowCursor):
    """A psycopg cursor that mirrors every write it runs.

    Rows the caller did not ask for are consumed here and never surfaced, so
    the cursor behaves exactly as an unwrapped one: `description`, `rowcount`,
    fetches and iteration are all the driver's own, except when this layer
    added a `RETURNING` the caller did not write — then there is nothing to
    fetch, which is what the caller already expected.
    """

    def __init__(self, cur: Any, surface: PostgresSurface):
        super().__init__(cur, self._run)
        object.__setattr__(self, "_surface", surface)
        # Rows read from the driver to capture provenance. `None` means "this
        # layer did not touch the results"; a list means the rows were consumed
        # here and must be served back from the buffer, because a DB-API fetch
        # is destructive and the driver's cursor is now empty.
        object.__setattr__(self, "_buffer", None)

    def _passthrough(self, cur, sql, params, a, kw):
        object.__setattr__(self, "_buffer", None)
        return cur.execute(sql, params, *a, **kw) if params is not None \
            else cur.execute(sql, *a, **kw)

    def _run(self, cur, proxy, sql, params, a, kw):
        nedb: PostgresSurface = object.__getattribute__(self, "_surface")

        op = write_op(sql) if nedb.shadow_writes else None
        table = target_table(sql) if op else None
        mapping = nedb.mapping_for_table(table) if table else None

        if op is None or mapping is None:
            if op is not None and mapping is None:
                nedb.note_unmirrored(table)
            return self._passthrough(cur, sql, params, a, kw)

        stmt, added = add_returning(sql)
        if not added and not _HAS_RETURNING.search(stmt):
            # The rewrite was declined — a multi-statement payload, where
            # appending would attach the clause to the wrong statement. Run the
            # original and record the gap rather than mirror the wrong thing.
            nedb.note_unmirrored(table)
            return self._passthrough(cur, sql, params, a, kw)

        result = cur.execute(stmt, params, *a, **kw) if params is not None \
            else cur.execute(stmt, *a, **kw)

        rows = []
        try:
            rows = self._drain(cur)
            for row in rows:
                nedb.chain_row(table, self._complete(nedb, table, row, op), op)
        except Exception as e:
            nedb.note_shadow_error(e)

        # `added` → the caller never asked for these rows, so the cursor is
        # presented as resultless and it sees exactly what it saw before.
        # Otherwise the rows are the caller's: serve them from the buffer,
        # because fetching them here already drained the driver's cursor.
        object.__setattr__(self, "_buffer", [] if added else rows)
        return result

    def _complete(self, nedb: PostgresSurface, table: str,
                  row: Dict[str, Any], op: str) -> Dict[str, Any]:
        """Widen a partial `RETURNING` row into the whole row.

        A caller who wrote `RETURNING id, fare` shows this layer two columns,
        so the mirrored document would hold two columns — a PARTIAL provenance
        record that looks complete. Same disease as every other bug in this
        module: not wrong enough to notice, wrong enough to matter.

        Their statement is left exactly as written; the full row is fetched
        separately by primary key. A DELETE needs no widening — the row is gone
        and the tombstone only identifies it. When the primary key is not among
        the returned columns there is nothing to fetch by, so the partial row
        is mirrored and the table is recorded in `partial_shadows` rather than
        passing silently.
        """
        if op == "DELETE":
            return row
        pk = nedb._pks.get(table)
        if not pk:
            return row
        if pk not in row:
            nedb.partial_shadows.add(table)
            return row
        cur = object.__getattribute__(self, "_cur").connection.cursor()
        try:
            cur.execute(f'SELECT * FROM "{table}" WHERE "{pk}" = %s', (row[pk],))
            got = cur.fetchone()
            if got is None or cur.description is None:
                return row
            return dict(zip([d[0] for d in cur.description], got))
        except Exception as e:
            nedb.note_shadow_error(e)
            return row
        finally:
            try:
                cur.close()
            except Exception:
                pass

    @staticmethod
    def _drain(cur) -> List[Dict[str, Any]]:
        """Read the RETURNING rows as dicts, tolerating a resultless cursor."""
        if cur.description is None:
            return []
        cols = [d[0] for d in cur.description]
        try:
            raw = cur.fetchall()
        except Exception:
            return []
        out = []
        for r in raw:
            # psycopg3 row factories may already yield a mapping.
            out.append(dict(r) if isinstance(r, dict) else dict(zip(cols, r)))
        return out

    # ── fetches served from the buffer when this layer consumed the rows ────

    def _rows(self) -> Optional[List[Dict[str, Any]]]:
        return object.__getattribute__(self, "_buffer")

    def _as_tuples(self, rows: List[Dict[str, Any]]) -> List[tuple]:
        """Back to the driver's shape: a tuple per row, in `description` order."""
        cur = object.__getattribute__(self, "_cur")
        if cur.description is None:
            return [tuple(r.values()) for r in rows]
        cols = [d[0] for d in cur.description]
        return [tuple(r.get(c) for c in cols) for r in rows]

    def fetchone(self, *a, **kw):
        buf = self._rows()
        if buf is None:
            return object.__getattribute__(self, "_cur").fetchone(*a, **kw)
        if not buf:
            return None
        head, rest = buf[0], buf[1:]
        object.__setattr__(self, "_buffer", rest)
        return self._as_tuples([head])[0]

    def fetchmany(self, size: Optional[int] = None, *a, **kw):
        buf = self._rows()
        if buf is None:
            cur = object.__getattribute__(self, "_cur")
            return cur.fetchmany(size, *a, **kw) if size is not None \
                else cur.fetchmany(*a, **kw)
        n = size if size is not None else getattr(
            object.__getattribute__(self, "_cur"), "arraysize", 1)
        take, rest = buf[:n], buf[n:]
        object.__setattr__(self, "_buffer", rest)
        return self._as_tuples(take)

    def fetchall(self, *a, **kw):
        buf = self._rows()
        if buf is None:
            return object.__getattribute__(self, "_cur").fetchall(*a, **kw)
        object.__setattr__(self, "_buffer", [])
        return self._as_tuples(buf)

    def __iter__(self):
        buf = self._rows()
        if buf is None:
            return iter(object.__getattribute__(self, "_cur"))
        object.__setattr__(self, "_buffer", [])
        return iter(self._as_tuples(buf))


class WrappedPostgres:
    """
    Transparent DB-API 2.0 proxy with NEDB shadow layer.

    Surface 1 (cursor/commit/…): every call passes through unchanged; writes
    made on a cursor are mirrored after they succeed.

    Surface 2 (.nedb.*): the full NEDB API, plus `backfill()` and
    `shadow_writes`.
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
        object.__setattr__(self, "nedb", PostgresSurface(conn, db_name, engine=engine))

    def cursor(self, *a, **kw):
        """A cursor that mirrors its writes.

        The cursor is the ONLY write path psycopg offers — there is no
        `conn.execute` — so this is where shadowing has to live. A named
        (server-side) cursor is handed back unwrapped: it is a read construct,
        and `RETURNING` has no meaning there.
        """
        conn = object.__getattribute__(self, "_conn")
        nedb = object.__getattribute__(self, "nedb")
        cur = conn.cursor(*a, **kw)
        named = bool(a) or bool(kw.get("name"))
        return cur if named else PostgresShadow(cur, nedb)

    def __getattr__(self, name: str) -> Any:
        return getattr(object.__getattribute__(self, "_conn"), name)

    def shadow_row(self, table: str, pk: Any, row: Optional[Dict[str, Any]],
                   op: str = "UPSERT") -> None:
        """Chain one host row into NEDB by hand. **No longer necessary.**

        Kept so existing code keeps working. Writes made through
        `conn.cursor()` are mirrored automatically once
        `nedb.shadow_writes = True`; calling this as well would record the same
        row twice, which is harmless (NEDB is content-addressed and the second
        write supersedes the first) but pointless.

        Use it only for a write this layer cannot see — one made on a raw
        connection obtained elsewhere. `row=None` chains a DELETE tombstone.
        """
        nedb: PostgresSurface = object.__getattribute__(self, "nedb")
        if not nedb.shadow_writes:
            return
        if row is None:
            nedb.chain_row(table, {"_pk": str(pk)}, "DELETE")
            return
        nedb.chain_row(table, dict(row), op)

    def __repr__(self):
        conn = object.__getattribute__(self, "_conn")
        db = object.__getattribute__(self, "_db_name")
        return f"<WrappedPostgres db_name={db!r} pg={conn!r}>"


def wrap_postgresql(conn: Any, db_name: str = "default",
                    nedbd_url: Optional[str] = None,
                    nedbd_token: Optional[str] = None,
                    backend: str = "auto",
                    dag_path: Optional[str] = None,
                    dag_tmk: Optional[str] = None) -> WrappedPostgres:
    """
    Wrap an existing DB-API 2.0 Postgres connection with NEDB's layer-2.

    Then set `conn.nedb.shadow_writes = True` and you are done — every table is
    discovered and every write through a cursor is mirrored.

    Args mirror wrap_redis: backend="auto" picks nedbd (if nedbd_url=) →
    embedded DAG (if the native wheel is installed) → v1 AOF engine.
    """
    return WrappedPostgres(conn, db_name, nedbd_url=nedbd_url,
                           nedbd_token=nedbd_token, backend=backend,
                           dag_path=dag_path, dag_tmk=dag_tmk)
