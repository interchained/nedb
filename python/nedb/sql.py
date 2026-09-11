# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.sql — SQL compatibility adapter.

Translates standard SQL statements deterministically to NQL queries and NEDB
API calls. No external dependencies; no MariaDB or MySQL code is used or
required. SQL is simply a familiar entry point — the NEDB engine executes
everything natively.

Supported:
  SELECT  * | col,…  FROM <table>  [AS OF <n>]
          [WHERE <col> <op> <val> (AND <col> <op> <val>)*]
          [LIKE → SEARCH]  [ORDER BY <col> [ASC|DESC]]  [LIMIT <n>]
  INSERT  INTO <table> (col,…) VALUES (val,…)
  UPDATE  <table> SET col=val [, col=val]* WHERE id = <id>
  DELETE  FROM <table> WHERE id = <id>

Unsupported SQL features raise ``SQLUnsupportedError`` with a clear message.
OR conditions, JOINs, subqueries, and aggregates are not yet implemented.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple, Union


class SQLError(Exception):
    """Raised on a parse or translation error."""


class SQLUnsupportedError(SQLError):
    """Raised when a SQL construct is not yet supported in NEDB."""


# ── Token definitions ─────────────────────────────────────────────────────────

_WS = re.compile(r"\s+")
_TOK = re.compile(
    r"""(?x)
    (?P<STRING>  '(?:[^'\\]|\\.)*' | "(?:[^"\\]|\\.)*")
  | (?P<FLOAT>   -?\d+\.\d+)
  | (?P<INT>     -?\d+)
  | (?P<STAR>    \*)
  | (?P<OP>      !=|<>|<=|>=|[=<>!])
  | (?P<COMMA>   ,)
  | (?P<LPAREN>  \()
  | (?P<RPAREN>  \))
  | (?P<SEMI>    ;)
  | (?P<WORD>    [A-Za-z_][A-Za-z0-9_.@#]*)
    """,
)
_KEYWORDS = {
    "SELECT", "FROM", "WHERE", "AND", "OR", "ORDER", "BY", "ASC", "DESC",
    "LIMIT", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "LIKE",
    "AS", "OF", "NULL", "TRUE", "FALSE", "NOT", "IN", "IS",
}


def _lex(sql: str) -> List[Tuple[str, str]]:
    tokens: List[Tuple[str, str]] = []
    pos = 0
    s = sql.strip().rstrip(";")
    while pos < len(s):
        if s[pos] in (" ", "\t", "\n", "\r"):
            pos += 1
            continue
        m = _TOK.match(s, pos)
        if not m:
            raise SQLError(f"Unexpected character {s[pos]!r} at position {pos}")
        kind = m.lastgroup
        val = m.group()
        if kind == "WORD" and val.upper() in _KEYWORDS:
            kind = "KW"
        tokens.append((kind, val))
        pos = m.end()
    return tokens


class _Parser:
    def __init__(self, tokens: List[Tuple[str, str]]):
        self._t = tokens
        self._i = 0

    def peek(self, offset: int = 0) -> Optional[Tuple[str, str]]:
        i = self._i + offset
        return self._t[i] if i < len(self._t) else None

    def peek_val(self, offset: int = 0) -> str:
        p = self.peek(offset)
        return p[1] if p else ""

    def peek_upper(self, offset: int = 0) -> str:
        return self.peek_val(offset).upper()

    def eat(self, kind: Optional[str] = None, val: Optional[str] = None) -> Tuple[str, str]:
        tok = self._t[self._i]
        if kind and tok[0] != kind:
            raise SQLError(f"Expected token type {kind}, got {tok}")
        if val and tok[1].upper() != val.upper():
            raise SQLError(f"Expected {val!r}, got {tok[1]!r}")
        self._i += 1
        return tok

    def done(self) -> bool:
        return self._i >= len(self._t)

    def kw(self, word: str) -> bool:
        return not self.done() and self.peek_upper() == word.upper() and self.peek()[0] == "KW"  # type: ignore[index]

    def eat_kw(self, word: str) -> None:
        self.eat("KW", word)

    def eat_ident(self) -> str:
        tok = self.peek()
        if tok and tok[0] in ("WORD", "KW"):
            self._i += 1
            return tok[1]
        raise SQLError(f"Expected identifier, got {tok}")

    def eat_value(self) -> Any:
        tok = self.peek()
        if not tok:
            raise SQLError("Expected value, got EOF")
        k, v = tok
        if k == "STRING":
            self._i += 1
            inner = v[1:-1].replace("\\'", "'").replace('\\"', '"')
            return inner
        if k == "FLOAT":
            self._i += 1
            return float(v)
        if k == "INT":
            self._i += 1
            return int(v)
        if k == "KW" and v.upper() == "NULL":
            self._i += 1
            return None
        if k == "KW" and v.upper() == "TRUE":
            self._i += 1
            return True
        if k == "KW" and v.upper() == "FALSE":
            self._i += 1
            return False
        if k == "WORD":
            self._i += 1
            return v
        raise SQLError(f"Expected value, got {tok}")


def _parse_where(p: _Parser) -> List[Tuple[str, str, Any]]:
    """Parse a WHERE clause into [(field, op, value), …]. Only AND supported."""
    conditions: List[Tuple[str, str, Any]] = []
    while True:
        if p.kw("OR"):
            raise SQLUnsupportedError(
                "OR in WHERE is not yet supported — use separate queries and combine results."
            )
        field = p.eat_ident()

        if p.kw("LIKE"):
            p.eat_kw("LIKE")
            pattern = p.eat_value()
            conditions.append((field, "LIKE", str(pattern)))
        elif p.kw("IS"):
            p.eat_kw("IS")
            if p.kw("NOT"):
                p.eat_kw("NOT")
                p.eat_kw("NULL")
                conditions.append((field, "!=", None))
            else:
                p.eat_kw("NULL")
                conditions.append((field, "=", None))
        elif p.kw("NOT"):
            p.eat_kw("NOT")
            if p.kw("LIKE"):
                p.eat_kw("LIKE")
                pattern = p.eat_value()
                raise SQLUnsupportedError("NOT LIKE is not yet supported.")
            raise SQLUnsupportedError("NOT in WHERE is not yet supported.")
        elif p.kw("IN"):
            raise SQLUnsupportedError("IN is not yet supported — use multiple WHERE = conditions.")
        else:
            op_tok = p.eat("OP")
            op = "<>" if op_tok[1] == "<>" else op_tok[1]
            if op == "<>":
                op = "!="
            value = p.eat_value()
            conditions.append((field, op, value))

        if p.kw("OR"):
            raise SQLUnsupportedError(
                "OR in WHERE is not yet supported — use separate queries and combine results."
            )
        if p.kw("AND"):
            p.eat_kw("AND")
            continue
        break
    return conditions


def _quote(v: Any) -> str:
    """Format a value for NQL (double-quoted strings)."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    return f'"{str(v)}"'


def _conditions_to_nql(conditions: List[Tuple[str, str, Any]]) -> Tuple[str, Optional[str]]:
    """Convert WHERE conditions to a (nql_where_fragment, search_term|None)."""
    search: Optional[str] = None
    parts: List[str] = []
    for field, op, value in conditions:
        if op == "LIKE":
            # Extract search term from %…% pattern
            term = re.sub(r"^%|%$", "", str(value))
            search = term
        else:
            parts.append(f"{field} {op} {_quote(value)}")
    return " AND ".join(parts), search


# ── Public API ────────────────────────────────────────────────────────────────

def sql_to_nql(sql: str) -> str:
    """
    Translate a SQL SELECT statement to an NQL query string.
    Raises ``SQLError`` / ``SQLUnsupportedError`` for unsupported constructs.

    Example::

        sql_to_nql("SELECT * FROM users WHERE status='active' ORDER BY age DESC LIMIT 10")
        # → 'FROM users WHERE status = "active" ORDER BY age DESC LIMIT 10'
    """
    tokens = _lex(sql)
    p = _Parser(tokens)
    p.eat_kw("SELECT")

    # columns (we don't project yet — * and explicit columns both return full docs)
    if p.peek() and p.peek()[0] == "STAR":
        p.eat()
    else:
        # consume column list until FROM
        while not p.kw("FROM"):
            if p.peek() and p.peek()[0] == "COMMA":  # type: ignore[index]
                p.eat()
            else:
                p.eat_ident()

    p.eat_kw("FROM")
    table = p.eat_ident()

    parts = [f"FROM {table}"]

    # AS OF (NEDB extension — also accepted in SQL via the AS OF syntax)
    if p.kw("AS") and p.peek_upper(1) == "OF":
        p.eat_kw("AS")
        p.eat_kw("OF")
        seq_tok = p.eat("INT")
        parts.append(f"AS OF {seq_tok[1]}")

    # WHERE
    search: Optional[str] = None
    if p.kw("WHERE"):
        p.eat_kw("WHERE")
        conditions = _parse_where(p)
        where_frag, search = _conditions_to_nql(conditions)
        if where_frag:
            parts.append(f"WHERE {where_frag}")

    # SEARCH (from LIKE conversion)
    if search:
        parts.append(f'SEARCH "{search}"')

    # ORDER BY
    if p.kw("ORDER"):
        p.eat_kw("ORDER")
        p.eat_kw("BY")
        col = p.eat_ident()
        direction = "ASC"
        if not p.done() and p.peek()[0] == "KW" and p.peek_upper() in ("ASC", "DESC"):
            direction = p.peek_upper()
            p._i += 1
        parts.append(f"ORDER BY {col} {direction}")

    # LIMIT
    if p.kw("LIMIT"):
        p.eat_kw("LIMIT")
        n = p.eat("INT")
        parts.append(f"LIMIT {n[1]}")

    return " ".join(parts)


def sql_exec(db: Any, sql: str) -> Any:
    """
    Execute a SQL statement against a NEDB database instance.

    Returns:
    - SELECT  → ``list[dict]``
    - INSERT  → the stored ``dict``
    - UPDATE  → the updated ``dict`` or ``None``
    - DELETE  → ``None``

    Example::

        from nedb import NEDB
        from nedb.sql import sql_exec

        db = NEDB()
        db.create_index("users", "status", "eq")
        sql_exec(db, "INSERT INTO users (id, name, age, status) VALUES ('u1', 'Ada', 31, 'active')")
        sql_exec(db, "SELECT * FROM users WHERE status = 'active' ORDER BY age DESC")
    """
    tokens = _lex(sql)
    if not tokens:
        raise SQLError("Empty SQL statement")
    kw = tokens[0][1].upper()

    if kw == "SELECT":
        nql = sql_to_nql(sql)
        return db.query(nql)

    if kw == "INSERT":
        return _exec_insert(db, _Parser(tokens))

    if kw == "UPDATE":
        return _exec_update(db, _Parser(tokens))

    if kw == "DELETE":
        return _exec_delete(db, _Parser(tokens))

    raise SQLUnsupportedError(f"SQL statement type {kw!r} is not supported. Supported: SELECT, INSERT, UPDATE, DELETE.")


def _exec_insert(db: Any, p: _Parser) -> dict:
    p.eat_kw("INSERT")
    p.eat_kw("INTO")
    table = p.eat_ident()

    columns: List[str] = []
    p.eat("LPAREN")
    while True:
        columns.append(p.eat_ident())
        if p.peek() and p.peek()[0] == "COMMA":  # type: ignore[index]
            p.eat()
        else:
            break
    p.eat("RPAREN")

    p.eat_kw("VALUES")
    values: List[Any] = []
    p.eat("LPAREN")
    while True:
        values.append(p.eat_value())
        if p.peek() and p.peek()[0] == "COMMA":  # type: ignore[index]
            p.eat()
        else:
            break
    p.eat("RPAREN")

    if len(columns) != len(values):
        raise SQLError(f"Column count ({len(columns)}) does not match value count ({len(values)})")

    doc = dict(zip(columns, values))
    row_id = doc.get("id") or doc.get("_id") or doc.get("ID")
    if row_id is None:
        raise SQLError("INSERT requires an 'id' or '_id' column to identify the row.")
    return db.put(table, str(row_id), doc)


def _exec_update(db: Any, p: _Parser) -> Optional[dict]:
    p.eat_kw("UPDATE")
    table = p.eat_ident()
    p.eat_kw("SET")

    updates: Dict[str, Any] = {}
    while True:
        col = p.eat_ident()
        p.eat("OP", "=")
        val = p.eat_value()
        updates[col] = val
        if p.peek() and p.peek()[0] == "COMMA":  # type: ignore[index]
            p.eat()
        else:
            break

    row_id: Optional[str] = None
    if p.kw("WHERE"):
        p.eat_kw("WHERE")
        conditions = _parse_where(p)
        id_conds = [(f, op, v) for f, op, v in conditions if f.lower() in ("id", "_id") and op == "="]
        if not id_conds:
            raise SQLUnsupportedError(
                "UPDATE WHERE must target a specific id column (id = '...'). "
                "Range updates are not yet supported."
            )
        row_id = str(id_conds[0][2])

    if row_id is None:
        raise SQLError("UPDATE requires a WHERE id = '...' clause.")

    existing = db.get(table, row_id) or {}
    merged = {**existing, **updates, "_id": row_id}
    return db.put(table, row_id, merged)


def _exec_delete(db: Any, p: _Parser) -> None:
    p.eat_kw("DELETE")
    p.eat_kw("FROM")
    table = p.eat_ident()

    row_id: Optional[str] = None
    if p.kw("WHERE"):
        p.eat_kw("WHERE")
        conditions = _parse_where(p)
        id_conds = [(f, op, v) for f, op, v in conditions if f.lower() in ("id", "_id") and op == "="]
        if not id_conds:
            raise SQLUnsupportedError(
                "DELETE WHERE must target a specific id column (id = '...'). "
                "Range deletes are not yet supported."
            )
        row_id = str(id_conds[0][2])

    if row_id is None:
        raise SQLError("DELETE requires a WHERE id = '...' clause.")

    db.delete(table, row_id)
