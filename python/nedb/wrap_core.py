# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.wrap_core — the shared wrap_* surface.

One engine-agnostic implementation of the NEDB layer-2 contract:

    register(pattern, collection, ...)   → teach NEDB about the host DB's shape
    backfill()                           → one-time import of existing data
    shadow_writes = True                 → auto-chain future host-DB writes
    .nedb.<full NEDB API>                → put/get/query/TRACE/AS OF/verify/…

Three backends, one surface:
    v1 in-process AOF engine   (NEDB() + a host-specific Backend)
    embedded v2/v3 DAG         (backends.dag.DagBackend — Rust native core)
    HTTP nedbd                 (NedBdProxy — v1 AOF, v2 DAG, or v3 --dag-v3)

Host adapters (wrap_redis / wrap_sqlite / wrap_mysql) supply:
    _host_scan(mapping)              → iterate existing host records
    _host_doc(mapping, key, value)   → host value → NEDB doc
    _shadow_put(mapping, doc_id, d)  → write a shadowed doc

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
from __future__ import annotations

import fnmatch
import json
import re
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from .engine import NEDB as _NEDB
from .wrap_redis import CollectionMapping, NedBdProxy  # reuse, don't duplicate
from .backends.dag import DagBackend, has_dag_native


# ── Which table does a write statement target? ───────────────────────────────

# One identifier part: quoted in any dialect's style, or bare.
_IDENT = r"""(?: " [^"]+ " | ` [^`]+ ` | \[ [^\]]+ \] | [A-Za-z_][\w$]* )"""

_WRITE_TARGET = re.compile(
    r"""^\s*
        (?:INSERT\s+(?:OR\s+\w+\s+)?INTO     # INSERT INTO, INSERT OR REPLACE INTO
          |REPLACE\s+INTO
          |UPDATE(?:\s+OR\s+\w+)?            # UPDATE, UPDATE OR ROLLBACK
          |DELETE\s+FROM)
        \s+
        # The qualification suffix has to wrap the WHOLE part alternation, not
        # just its last branch. Attached to the bare branch alone,
        # `"public"."drivers"` matched only `"public"` — and the extracted
        # "table" was the SCHEMA, so nothing ever mapped.
        (?P<table> """ + _IDENT + r"""
                   (?: \s* \. \s* """ + _IDENT + r""" )* )
    """,
    re.IGNORECASE | re.VERBOSE,
)

_IDENT_PART = re.compile(_IDENT, re.VERBOSE)


def target_table(sql: str) -> Optional[str]:
    """The table a write statement writes to, or `None` if it isn't a write.

    This has to be exact, and the reason is worth stating: the first version
    asked "does any registered table name appear anywhere in this SQL?", which
    is a SUBSTRING test. With one table registered it usually worked. With
    every table registered — which is what automatic discovery does — a write
    to `drivers_archive` matches the mapping for `drivers` and gets shadowed
    into the wrong collection under the wrong row's id.

    That is FALSE PROVENANCE: a record that looks authoritative and describes
    something that never happened. Strictly worse than no record at all, and
    the exact failure this layer exists to prevent. So the target is parsed
    from the statement instead of guessed at, and anything unparseable returns
    `None` so the caller can record a visible gap rather than invent a row.

    Schema qualification is reduced to the bare table name, matching the
    engine's single namespace.
    """
    m = _WRITE_TARGET.match(sql or "")
    if not m:
        return None
    raw = m.group("table")
    # Reduce `"public"."drivers"` → `drivers`. Taken as the LAST identifier
    # part rather than by splitting on ".", because a quoted identifier may
    # legally contain a dot — `"my.table"` is one name, not two.
    parts = _IDENT_PART.findall(raw)
    part = (parts[-1] if parts else raw).strip()
    if len(part) >= 2 and part[0] == part[-1] == '"':
        return part[1:-1]
    if len(part) >= 2 and part[0] == part[-1] == "`":
        return part[1:-1]
    if len(part) >= 2 and part[0] == "[" and part[-1] == "]":
        return part[1:-1]
    return part


def write_op(sql: str) -> Optional[str]:
    """`INSERT` | `UPDATE` | `DELETE` for a write statement, else `None`."""
    head = (sql or "").lstrip().upper()
    if head.startswith("INSERT") or head.startswith("REPLACE"):
        return "INSERT"
    if head.startswith("UPDATE"):
        return "UPDATE"
    if head.startswith("DELETE"):
        return "DELETE"
    return None


# ── Engine selection ─────────────────────────────────────────────────────────

class _MemoryPersist:
    """No-op persistence for engines that persist themselves (DAG, nedbd)."""

    def append(self, _line: str) -> None:
        pass

    def publish_ops(self, _lines) -> None:
        pass

    def read_all(self):
        return []


def open_engine(
    backend: str = "auto",            # "auto" | "aof" | "dag" | "nedbd"
    db_name: str = "default",
    nedbd_url: Optional[str] = None,
    nedbd_token: Optional[str] = None,
    dag_path: Optional[str] = None,
    dag_tmk: Optional[str] = None,
):
    """
    Resolve the NEDB engine handle for a wrap_* surface.

    backend="auto": nedbd_url wins if given; else embedded DAG if the native
    core is importable; else v1 in-process AOF engine.
    """
    if backend == "nedbd" or (backend == "auto" and nedbd_url):
        return NedBdProxy(nedbd_url, db_name, token=nedbd_token), None

    if backend == "dag" or (backend == "auto" and has_dag_native()):
        try:
            return _open_dag(dag_path, dag_tmk), None
        except ImportError:
            if backend == "dag":
                raise   # an explicit request must never silently downgrade
            # auto: no compiled core after all — fall through to v1 AOF, the
            # universal fallback. wrap_redis has always done this; open_engine
            # did not, so wrap_sqlite / wrap_mysql / wrap_mongo /
            # wrap_postgresql raised ImportError out of the constructor on
            # every install without a platform wheel.

    if backend in ("aof", "auto"):
        return _NEDB(), None

    raise ValueError(f"unknown backend {backend!r} "
                     "(expected 'auto' | 'aof' | 'dag' | 'nedbd')")


def _open_dag(dag_path, dag_tmk):
    """Construct the embedded DAG backend, warning when it is not durable."""
    if dag_path is None:
        # An embedded DAG with no path is IN-MEMORY: the chain dies with the
        # process. These adapters have no durable in-host fallback the way
        # wrap_redis has Redis Streams, so the choice stands -- but silently
        # handing someone a provenance log that evaporates is exactly the
        # failure this engine exists to prevent. Say so, once.
        import warnings
        warnings.warn(
            "NEDB: embedded DAG opened with no dag_path= — this chain is "
            "IN-MEMORY and will not survive the process. Pass "
            'dag_path="./audit" for a durable store, or nedbd_url= to use '
            "a server.",
            RuntimeWarning, stacklevel=4)
    return DagBackend(path=dag_path, tmk=dag_tmk)


# ── The intercepting cursor ──────────────────────────────────────────────────

class ShadowCursor:
    """A DB-API 2.0 cursor proxy that mirrors writes into NEDB.

    This exists because intercepting `connection.execute` is not enough, and
    that gap was invisible: `conn.cursor().execute(...)` — the canonical DB-API
    idiom, and the one this package's own examples showed — bypassed shadowing
    entirely. The host ended up with two rows, NEDB with one, `verify()`
    returning True and no error counted anywhere. Half an audit trail is
    indistinguishable from a whole one until you need it.

    `conn.execute` is a sqlite3 convenience that psycopg does not even have, so
    on Postgres the cursor is the ONLY path. Wrapping it is not an extra: it is
    where the writes are.

    Everything not named here is delegated, so the object stays a drop-in
    cursor — `description`, `rowcount`, `lastrowid`, `fetchone`, iteration,
    context-manager use, `with` blocks, all unchanged.
    """

    def __init__(self, cur: Any, on_execute: Callable[..., Any]):
        object.__setattr__(self, "_cur", cur)
        object.__setattr__(self, "_on_execute", on_execute)

    # ── the two intercepted calls ───────────────────────────────────────────

    def execute(self, sql: str, params: Any = None, *a, **kw):
        return object.__getattribute__(self, "_on_execute")(
            object.__getattribute__(self, "_cur"), self, sql, params, a, kw)

    def executemany(self, sql: str, seq_of_params: Iterable[Any], *a, **kw):
        """Run each parameter set through `execute`, so each one is mirrored.

        A driver's `executemany` is usually this loop anyway, and the
        alternative — passing it straight through — would mirror nothing while
        reporting success. Losing the driver's batching is a real cost; losing
        the writes silently is not a trade worth making.
        """
        last = None
        for params in seq_of_params:
            last = self.execute(sql, params, *a, **kw)
        return last

    # ── pure delegation ─────────────────────────────────────────────────────

    def __getattr__(self, name: str) -> Any:
        return getattr(object.__getattribute__(self, "_cur"), name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(object.__getattribute__(self, "_cur"), name, value)

    def __iter__(self):
        return iter(object.__getattribute__(self, "_cur"))

    def __enter__(self):
        cur = object.__getattribute__(self, "_cur")
        enter = getattr(cur, "__enter__", None)
        if enter is not None:
            enter()
        return self

    def __exit__(self, *exc):
        cur = object.__getattribute__(self, "_cur")
        ex = getattr(cur, "__exit__", None)
        if ex is not None:
            return ex(*exc)
        try:
            cur.close()
        except Exception:
            pass
        return False

    def __repr__(self):
        return f"<ShadowCursor {object.__getattribute__(self, '_cur')!r}>"


# ── The shared surface ───────────────────────────────────────────────────────

class WrapSurface:
    """
    The `.nedb` attribute of any wrapped host connection.

    Same contract as NEDBSurface in wrap_redis, generalized:
    register → backfill → shadow_writes=True → full NEDB API.
    """

    def __init__(self, db_name: str, engine=None, persist=None):
        self._db_name = db_name
        self._mappings: List[CollectionMapping] = []
        self._shadow_writes: bool = False
        self._backfilled: bool = False
        # ── automatic discovery ─────────────────────────────────────────────
        # `shadow_writes = True` used to be a SILENT NO-OP until the caller had
        # also hand-registered every table: the flag read "on" and mirrored
        # nothing, with no error raised and no error counted. For an audit
        # layer that is the worst available outcome — you believe you have a
        # provenance chain, you have zero, and nothing tells you.
        #
        # Worse than not working: registration was opt-IN, so the normal
        # failure was not "nothing happens" but SILENT PARTIAL COVERAGE. Three
        # tables registered out of twelve is a quarter of an audit trail that
        # looks exactly like a whole one.
        #
        # So discovery is automatic and coverage is opt-OUT. Turning shadowing
        # on reads the host's own catalogue and mirrors every table it finds.
        # `register()` survives as an OVERRIDE (rename a collection, supply a
        # parser), not as a prerequisite.
        self.auto_discover: bool = True
        #: Tables never to mirror, by exact name or fnmatch pattern.
        self.exclude: Set[str] = set()
        #: Columns stripped from every shadowed row, by exact name or pattern.
        #:
        #: The one setting that is about correctness rather than convenience.
        #: NEDB is append-only and tamper-evident BY DESIGN — which is exactly
        #: wrong for a secret or for a row someone has a right to erase.
        #: Mirroring a `password_hash`, an API key or a national id into a
        #: store that cannot forget is a liability, not a feature, so it must
        #: be possible to keep columns out. Nothing is excluded by default:
        #: guessing which of a caller's columns are sensitive would be its own
        #: silent wrong answer.
        self.exclude_columns: Set[str] = set()
        self._discovered: bool = False
        #: Tables written to while shadowing was on that could not be mirrored.
        #: A visible record of the gap, rather than a silent one.
        self.unmirrored_tables: Set[str] = set()
        self._db = engine
        self._persist = persist or _MemoryPersist()
        # ── shadow observability ────────────────────────────────────────────
        # A shadow failure must never break the host database call, but
        # "don't raise into the host's stack" and "don't tell anyone" are
        # different requirements. For a PROVENANCE layer, silently dropping a
        # record is the worst available outcome: the chain gets a hole and
        # verify() still returns True, because it verifies what got in, not
        # that everything that should have got in did.
        #
        # Every swallowed failure is now counted and, optionally, reported.
        self.shadow_errors: int = 0
        self.last_shadow_error: Optional[str] = None
        self.on_shadow_error: Optional[Callable[[BaseException], None]] = None
        # True when the chain outlives the process — set by the adapter.
        self.durable: bool = True
        # strict=True re-raises instead of swallowing — for tests and for
        # deployments that would rather fail loudly than lose provenance.
        self.strict_shadow: bool = False

    # ── shadow_writes: a switch that means what it says ─────────────────────

    @property
    def shadow_writes(self) -> bool:
        """Mirror every host write into NEDB.

        Setting this to `True` discovers the host's tables immediately, so no
        further configuration is required — which is the only reading of the
        flag that is not a lie. Set it to `False` and mirroring stops; set it
        back and the discovery is reused.
        """
        return self._shadow_writes

    @shadow_writes.setter
    def shadow_writes(self, on: bool) -> None:
        self._shadow_writes = bool(on)
        if on:
            self.ensure_discovered()

    def ensure_discovered(self) -> int:
        """Register a mapping per host table, once. Returns tables added.

        Idempotent, and never overrides an explicit `register()`: a caller who
        mapped `drivers` to the collection `driver` keeps that, and discovery
        fills in only the tables they did not name.
        """
        if self._discovered or not self.auto_discover:
            return 0
        self._discovered = True   # set first: a failing scan must not re-run
        added = 0
        try:
            for table, pk in self._discover_tables():
                if self._is_excluded(table):
                    continue
                if any(m.pattern == table for m in self._mappings):
                    continue  # an explicit register() wins
                # Identity mapping by default. Predictable beats clever: the
                # collection is named after the table, so `FROM drivers`
                # queries what `INSERT INTO drivers` wrote.
                #
                # Only the SQL adapters take `pk`; the others do not, so the
                # primary key is offered and not insisted on.
                try:
                    self.register(table, table, pk=pk) if pk else self.register(table, table)
                except TypeError:
                    self.register(table, table)
                added += 1
        except Exception as e:
            self.note_shadow_error(e)
        return added

    def _is_excluded(self, table: str) -> bool:
        return any(table == pat or fnmatch.fnmatch(table, pat)
                   for pat in self.exclude)

    def redact(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Drop `exclude_columns` from a row on its way into NEDB."""
        if not self.exclude_columns:
            return row
        return {k: v for k, v in row.items()
                if not any(k == pat or fnmatch.fnmatch(k, pat)
                           for pat in self.exclude_columns)}

    def mapping_for_table(self, table: Optional[str]):
        """The mapping for an EXACT table name — never a substring match."""
        if not table:
            return None
        for m in self._mappings:
            if m.pattern == table:
                return m
        # A pattern-style registration (`sess:*`) may still legitimately match.
        for m in self._mappings:
            if any(ch in m.pattern for ch in "*?[") and fnmatch.fnmatch(table, m.pattern):
                return m
        return None

    def note_unmirrored(self, table: Optional[str]) -> None:
        """Record that a write could not be mirrored, so the gap is visible.

        Silence here is what made the old behaviour dangerous. A caller can
        assert `not conn.nedb.unmirrored_tables` and actually learn something.

        A table the caller put in `exclude` is NOT recorded: that gap is
        deliberate, and mixing chosen omissions in with accidental ones would
        make the signal useless — the assertion above has to be able to pass on
        a correctly configured connection.
        """
        if table and self._is_excluded(table):
            return
        self.unmirrored_tables.add(table or "<unparsed statement>")

    def _discover_tables(self) -> Iterable[Tuple[str, Optional[str]]]:
        """Yield `(table, primary_key_or_None)` from the host. Override me."""
        return iter(())

    def note_shadow_error(self, exc: BaseException) -> None:
        """Record a swallowed shadow failure. Never raises unless strict."""
        self.shadow_errors += 1
        self.last_shadow_error = f"{type(exc).__name__}: {exc}"
        cb = self.on_shadow_error
        if cb is not None:
            try:
                cb(exc)
            except Exception:
                pass  # a broken callback must not compound the original failure
        if self.strict_shadow:
            raise exc

    # ── host hooks (override in the host adapter) ────────────────────────────

    def _host_scan(self, mapping: CollectionMapping, batch_size: int):
        """Yield (key, raw_value) pairs from the host DB. Override me."""
        return iter(())

    def _shadow_doc(self, mapping: CollectionMapping, key: str,
                    args: tuple, kwargs: dict) -> Optional[Dict[str, Any]]:
        """Host write command → NEDB doc, or None to skip. Override me."""
        return None

    # ── registration / backfill (shared) ─────────────────────────────────────

    def register(self, pattern: str, collection: str,
                 id_extractor: Optional[Callable[[str], str]] = None,
                 value_parser: Optional[Callable[[Any], dict]] = None,
                 value_type: str = "string") -> "WrapSurface":
        self._mappings.append(CollectionMapping(
            pattern, collection, id_extractor, value_parser, value_type))
        return self

    def _mapping_for(self, key: str) -> Optional[CollectionMapping]:
        for m in self._mappings:
            if m.matches(key):
                return m
        return None

    def backfill(self, pattern: Optional[str] = None,
                 collection: Optional[str] = None,
                 id_extractor: Optional[Callable[[str], str]] = None,
                 value_parser: Optional[Callable[[Any], dict]] = None,
                 value_type: str = "string",
                 batch_size: int = 200) -> int:
        if pattern is not None:
            mappings = [CollectionMapping(pattern, collection or pattern.split(":")[0],
                                          id_extractor, value_parser, value_type)]
        else:
            mappings = list(self._mappings)
        total = 0
        for m in mappings:
            for key, raw in self._host_scan(m, batch_size):
                doc_id = m.extract_id(key)
                doc = m.parse_value(raw)
                doc.setdefault("_source", "backfill")
                try:
                    self._db.put(m.collection, doc_id, doc, client="__backfill__")
                    total += 1
                except Exception as e:
                    # Counted, not silent: backfill() returns the number of rows
                    # imported, and a caller comparing that to the host's row
                    # count deserves to know the difference was errors.
                    self.note_shadow_error(e)
                    continue
        self._backfilled = True
        return total

    # ── shadowing (shared) ───────────────────────────────────────────────────

    def shadow(self, cmd: str, key: str, *args, **kwargs) -> None:
        """Route a host write command into NEDB. Failures never propagate."""
        if not self.shadow_writes:
            return
        try:
            m = self._mapping_for(key)
            if m is None:
                self._db.put("__shadow_raw__", key,
                             {"cmd": cmd, "key": key, "_source": "shadow_raw"},
                             client="__shadow__")
                self._persist_after()
                return
            doc = self._shadow_doc(m, key, args, kwargs)
            if doc is None:
                return
            doc.setdefault("_source", "shadow")
            self._db.put(m.collection, m.extract_id(key), doc,
                         client="__shadow__")
            self._persist_after()
        except Exception as e:
            self.note_shadow_error(e)

    def _persist_after(self) -> None:
        # DAG + nedbd persist themselves; v1 AOF needs the last op appended
        eng = self._db
        if isinstance(eng, _NEDB) and getattr(eng, "log", None) and eng.log.ops:
            last = eng.log.ops[-1]
            self._persist.append(json.dumps(last.to_dict()))

    # ── full NEDB API (shared across every backend) ──────────────────────────

    def put(self, coll, id, doc, **kw):
        r = self._db.put(coll, id, doc, **kw)
        self._persist_after()
        return r

    def get(self, coll, id, as_of=None):
        return self._db.get(coll, id, as_of=as_of)

    def query(self, nql):
        return self._db.query(nql)

    def create_index(self, coll, field, kind="eq"):
        self._db.create_index(coll, field, kind)

    def delete(self, coll, id, **kw):
        self._db.delete(coll, id)
        self._persist_after()

    def link(self, frm, rel, to, **kw):
        self._db.link(frm, rel, to)
        self._persist_after()

    def unlink(self, frm, rel, to, **kw):
        self._db.unlink(frm, rel, to)
        self._persist_after()

    def neighbors(self, frm, rel, as_of=None):
        return self._db.neighbors(frm, rel, as_of=as_of)

    def inbound(self, to, rel, as_of=None):
        return self._db.inbound(to, rel, as_of=as_of)

    def verify(self) -> bool:
        return self._db.verify()

    @property
    def head(self) -> str:
        return self._db.head

    @property
    def seq(self) -> int:
        return self._db.seq

    def checkpoint(self) -> str:
        return self._db.checkpoint()

    # ── DAG-native passthroughs (no-op/None on other backends) ──────────────

    def tip(self):
        if hasattr(self._db, "tip"):
            return self._db.tip()
        return None

    def tip_collection(self, coll):
        if hasattr(self._db, "tip_collection"):
            return self._db.tip_collection(coll)
        return None

    def since(self, after_seq: int, limit: int = 0):
        if hasattr(self._db, "since"):
            return self._db.since(after_seq, limit)
        raise RuntimeError("changefeed requires the DAG backend "
                           "(embedded native or nedbd --dag)")

    def scan_status(self):
        if hasattr(self._db, "scan_status"):
            return self._db.scan_status()
        raise RuntimeError("scan_status requires the DAG backend")

    # ── introspection ────────────────────────────────────────────────────────

    @property
    def engine_kind(self) -> str:
        if isinstance(self._db, NedBdProxy):
            return "nedbd-http"
        if isinstance(self._db, DagBackend):
            return "dag-embedded"
        return "aof-embedded"

    def __repr__(self):
        return (f"<WrapSurface db={self._db_name!r} "
                f"engine={self.engine_kind} "
                f"mappings={len(self._mappings)}>")
