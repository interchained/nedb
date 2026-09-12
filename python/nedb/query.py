# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.query — NQL (the NEDB Query Language) parser + the fluent query builder.

Both the text form and the fluent builder compile to the SAME plan dict, so the two
front-ends share identical semantics. In the production engine the parser lives once
in Rust; Python and Node get the exact same grammar for free.

NQL grammar (keywords case-insensitive):

    FROM <collection>
      [ AS OF <seq> ]
      [ WHERE <predicate> ]
      [ SEARCH "<text>" ]
      [ ORDER BY <field> [ASC|DESC] ]
      [ TRAVERSE <relation> ]
      [ LIMIT <n> ]

    <predicate>  := <or>
    <or>         := <and> (OR <and>)*
    <and>        := <not> (AND <not>)*
    <not>        := [NOT] <primary>
    <primary>    := "(" <predicate> ")" | <comparison>
    <comparison> := <field> <op> <value>
                  | <field> [NOT] IN "(" <value> ("," <value>)* ")"
                  | <field> [NOT] BETWEEN <value> AND <value>
                  | <field> [NOT] LIKE|ILIKE <pattern>
                  | <field> IS [NOT] NULL

    op    := = | != | < | <= | > | >=
    value := number | "string" | 'string' | true | false | null

Predicates compile to a TREE at plan["predicate"]. For the special case of a
pure conjunction of comparisons — the only shape the equality-index
accelerator can exploit — plan["where"] is ALSO populated with the flat
[(field, op, value)] list it has always held, so that fast path and the
mongo.py caller keep working unchanged.
"""
from __future__ import annotations

import re
from typing import Any, List, Optional, Tuple

_TOKEN_RE = re.compile(
    r"""\s+
      | "(?P<dq>[^"]*)"
      | '(?P<sq>[^']*)'
      | (?P<num>-?\d+(?:\.\d+)?)
      | (?P<op>!~\*|!~|~\*|<=|>=|!=|=|<|>|~)
      | (?P<punct>[(),])
      | (?P<word>[A-Za-z_][A-Za-z0-9_]*)
    """,
    re.VERBOSE,
)

_KEYWORDS = {"from", "as", "of", "where", "and", "or", "search", "order", "by",
             "asc", "desc", "traverse", "trace", "reverse", "limit", "offset",
             "having",
             "valid", "true", "false", "null", "group", "count", "sum", "avg", "min", "max",
             "not", "in", "between", "like", "ilike", "is"}


def _lex(text: str) -> List[Tuple[str, Any, str]]:
    """Tokenize. Every token is (kind, canonical value, RAW source text)."""
    toks: List[Tuple[str, Any, str]] = []
    pos = 0
    while pos < len(text):
        m = _TOKEN_RE.match(text, pos)
        if not m:
            raise SyntaxError(f"NQL: cannot tokenize near: {text[pos:pos+20]!r}")
        pos = m.end()
        if m.group("dq") is not None:
            toks.append(("str", m.group("dq"), m.group("dq")))
        elif m.group("sq") is not None:
            toks.append(("str", m.group("sq"), m.group("sq")))
        elif m.group("num") is not None:
            n = m.group("num")
            toks.append(("num", float(n) if "." in n else int(n), n))
        elif m.group("op") is not None:
            toks.append(("op", m.group("op"), m.group("op")))
        elif m.group("punct") is not None:
            toks.append(("punct", m.group("punct"), m.group("punct")))
        elif m.group("word") is not None:
            w = m.group("word")
            lw = w.lower()
            # A keyword token carries BOTH the canonical lowercase form (for
            # clause matching) and the raw spelling (for field positions).
            #
            # The raw spelling has to survive: field positions accept a keyword
            # as a field name — a document may legitimately have a field called
            # `count`, `min`, `value` or `status` — and using the canonicalised
            # form there looks up a key that does not exist. A field spelled
            # `Count` became `count` and matched nothing, silently.
            toks.append(("kw", lw, w) if lw in _KEYWORDS else ("word", w, w))
        # whitespace -> skip
    return toks


def empty_plan(coll: str) -> dict:
    return {"from": coll, "as_of": None, "where": [], "search": None,
            "order_by": None, "order_keys": None,
            "traverse": None, "limit": None, "offset": None,
            "group_by": None, "aggregate": None, "having": None,
            "trace": None, "trace_reverse": False,
            "valid_as_of": None, "predicate": None}


# ── LIKE ─────────────────────────────────────────────────────────────────────

def like_to_regex(pattern: str, ci: bool = False) -> "re.Pattern":
    """Compile a SQL LIKE pattern: `%` = any run, `_` = any single char.

    Everything else is escaped, so a pattern containing regex metacharacters
    (`.`, `*`, `[`, `(`) matches them literally — the reason this is not
    `fnmatch`, which would additionally treat `[...]` as a character class and
    silently reinterpret a literal bracket.
    """
    out = []
    for ch in pattern:
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        else:
            out.append(re.escape(ch))
    flags = re.DOTALL | (re.IGNORECASE if ci else 0)
    return re.compile("^" + "".join(out) + "$", flags)


# ── `~` / `!~` — POSIX regex over a DOCUMENTED SUBSET ────────────────────────

# Supported: ^ $ . and literal text. Everything else is REFUSED.
#
# This exists because Postgres catalogue introspection needs it: psql's `\dn`
# filters with `nspname !~ '^pg_'`. Without the operator those queries cannot
# run at all.
#
# Python has `re` and could match the full language — but the RUST engine
# cannot without taking a regex dependency it should not take for two anchored
# prefix patterns. Two engines that accept different regex languages is a
# divergence, and a divergence in a FILTER silently includes or excludes rows.
# So Python deliberately implements the same subset and refuses the same
# patterns, rather than being quietly more capable.
_REGEX_UNSUPPORTED = set("*+?[](){}|\\")


def unsupported_regex_char(pattern: str):
    """The first metacharacter outside the supported subset, or None."""
    for ch in pattern:
        if ch in _REGEX_UNSUPPORTED:
            return ch
    return None


def regex_match(value: str, pattern: str, ci: bool = False) -> bool:
    """Match the supported subset. `^`/`$` anchor; `.` is exactly one char.

    A lone `$` is an END ANCHOR and matches every string — POSIX says so, and
    treating it as a literal dollar sign was a real bug caught by test.
    """
    if ci:
        value, pattern = value.lower(), pattern.lower()
    start = pattern.startswith("^")
    end = pattern.endswith("$")
    body = pattern[1 if start else 0: len(pattern) - (1 if end else 0)]

    def at(i: int) -> bool:
        if i + len(body) > len(value):
            return False
        return all(pc == "." or pc == value[i + j] for j, pc in enumerate(body))

    if start and end:
        return len(value) == len(body) and at(0)
    if start:
        return at(0)
    if end:
        return len(value) >= len(body) and at(len(value) - len(body))
    return any(at(i) for i in range(len(value) - len(body) + 1))


def parse_nql(text: str) -> dict:
    toks = _lex(text)
    i = 0

    def peek():
        """(kind, canonical value) — deliberately 2-wide.

        Every clause test in this parser compares `peek() == ("kw", "limit")`,
        so widening the return value would break all of them. Field positions
        that need the user's original spelling call `peek_raw()` instead.
        """
        return toks[i][:2] if i < len(toks) else (None, None)

    def peek_raw():
        """The RAW source text of the current token, case preserved."""
        return toks[i][2] if i < len(toks) else None

    def expect_kw(kw):
        nonlocal i
        t, v = peek()
        if t != "kw" or v != kw:
            raise SyntaxError(f"NQL: expected '{kw.upper()}', got {v!r}")
        i += 1

    def value():
        nonlocal i
        t, v = peek()
        if t in ("num", "str"):
            i += 1
            return v
        if t == "kw" and v in ("true", "false", "null"):
            i += 1
            return {"true": True, "false": False, "null": None}[v]
        if t == "word":
            i += 1
            return v
        raise SyntaxError(f"NQL: expected value, got {v!r}")

    expect_kw("from")
    t, v = peek()
    if t not in ("word", "kw"):
        raise SyntaxError("NQL: expected collection after FROM")
    coll_raw = peek_raw()
    i += 1
    plan = empty_plan(coll_raw)

    # AS OF <seq>
    if peek() == ("kw", "as"):
        i += 1
        expect_kw("of")
        t, v = peek()
        if t != "num":
            raise SyntaxError("NQL: AS OF expects an integer seq")
        i += 1
        plan["as_of"] = int(v)

    # VALID AS OF <date>  — bi-temporal valid-time filter
    # Syntax: VALID AS OF "2024-02-15"  (ISO 8601 date or datetime string)
    # Can appear before or after WHERE; "valid" is the disambiguating keyword.
    if peek() == ("kw", "valid"):
        i += 1
        # expect AS OF
        t, v = peek()
        if t == "kw" and v == "as":
            i += 1
        t, v = peek()
        if t == "kw" and v == "of":
            i += 1
        t, v = peek()
        if t != "str":
            raise SyntaxError("NQL: VALID AS OF expects a quoted date string")
        i += 1
        plan["valid_as_of"] = v

    # ── WHERE <predicate> ────────────────────────────────────────────────────
    # Recursive descent: OR binds loosest, then AND, then NOT, with
    # parentheses overriding. Mirrors the Rust engine's grammar exactly so the
    # two engines cannot disagree about what a query means.

    def eat_kw(kw) -> bool:
        nonlocal i
        if peek() == ("kw", kw):
            i += 1
            return True
        return False

    def expect_punct(ch):
        nonlocal i
        t, v = peek()
        if t != "punct" or v != ch:
            raise SyntaxError(f"NQL: expected {ch!r}, got {v!r}")
        i += 1

    def field_name(ctx):
        """A field name, which MAY collide with a reserved word.

        Returns the RAW spelling: a document can legitimately have a field
        called `count`, `min`, `value` or `status`, and canonicalising it here
        looks up a key the document does not have. `WHERE Count = 5` matched
        nothing before this, silently.
        """
        nonlocal i
        t, v = peek()
        if t not in ("word", "kw"):
            raise SyntaxError(f"NQL: {ctx}: expected field name, got {v!r}")
        raw = peek_raw()
        i += 1
        return raw

    def p_or():
        terms = [p_and()]
        while eat_kw("or"):
            terms.append(p_and())
        return terms[0] if len(terms) == 1 else {"op": "or", "terms": terms}

    def p_and():
        terms = [p_not()]
        # BETWEEN consumes its own AND inside p_cmp, so any AND arriving here
        # is a genuine conjunction.
        while eat_kw("and"):
            terms.append(p_not())
        return terms[0] if len(terms) == 1 else {"op": "and", "terms": terms}

    def p_not():
        if eat_kw("not"):
            return {"op": "not", "term": p_not()}
        return p_primary()

    def p_primary():
        t, v = peek()
        if t == "punct" and v == "(":
            expect_punct("(")
            inner = p_or()
            expect_punct(")")
            return inner
        return p_cmp()

    def p_cmp():
        nonlocal i
        field = field_name("WHERE")

        if eat_kw("is"):
            negated = eat_kw("not")
            if not eat_kw("null"):
                raise SyntaxError("NQL: expected NULL after IS")
            return {"op": "isnull", "field": field, "negated": negated}

        negated = eat_kw("not")

        if eat_kw("in"):
            expect_punct("(")
            values = [value()]
            while peek() == ("punct", ","):
                i += 1
                values.append(value())
            expect_punct(")")
            return {"op": "in", "field": field, "values": values, "negated": negated}

        if eat_kw("between"):
            low = value()
            if not eat_kw("and"):
                raise SyntaxError("NQL: BETWEEN expects AND between its bounds")
            high = value()
            return {"op": "between", "field": field, "low": low, "high": high,
                    "negated": negated}

        ci = peek() == ("kw", "ilike")
        if ci or peek() == ("kw", "like"):
            i += 1
            t, pat = peek()
            if t not in ("str", "word"):
                raise SyntaxError(f"NQL: LIKE expects a pattern, got {pat!r}")
            i += 1
            return {"op": "like", "field": field, "pattern": pat,
                    "negated": negated, "ci": ci}

        if negated:
            raise SyntaxError(
                "NQL: NOT must be followed by IN, BETWEEN, LIKE or ILIKE "
                "(use `NOT (field = value)` or `field != value` instead)")

        t, op = peek()
        if t != "op":
            raise SyntaxError("NQL: expected operator in WHERE")
        i += 1

        # `~` / `~*` / `!~` / `!~*` — POSIX regex over the supported subset.
        # Its argument is a PATTERN, not a value, so it is taken as text and
        # validated here rather than through value(), which would read `^1` as
        # something numeric.
        if op in ("~", "~*", "!~", "!~*"):
            t2, pat = peek()
            if t2 not in ("str", "word"):
                raise SyntaxError(f"NQL: {op} expects a pattern string, got {pat!r}")
            i += 1
            bad = unsupported_regex_char(pat)
            if bad is not None:
                # Refused at PARSE time so the caller learns at the point of
                # the mistake, and refused identically in both engines so the
                # two cannot accept different regex languages.
                raise SyntaxError(
                    f"NQL: regex {pat!r} uses {bad!r}, which this engine does not "
                    f"implement. The supported subset is ^ $ . and literal text — "
                    f"enough for catalogue filters like '^pg_'. Matching the rest "
                    f"approximately would silently include or exclude rows, so it "
                    f"is refused instead")
            return {"op": "regex", "field": field, "pattern": pat,
                    "negated": op.startswith("!"), "ci": op.endswith("*")}

        return {"op": "cmp", "field": field, "cmp": op, "value": value()}

    def flatten_conjuncts(pred):
        """Return [(field, op, value)] if `pred` is a pure AND of comparisons.

        Used to keep plan["where"] populated for the equality-index
        accelerator. Returns None for anything containing OR/NOT/IN/BETWEEN/
        LIKE/IS NULL, because a flat conjunct list cannot represent those and
        handing the accelerator a partial view of the predicate would let it
        narrow to the wrong candidate set.
        """
        if pred is None:
            return []
        if pred.get("op") == "cmp":
            return [(pred["field"], pred["cmp"], pred["value"])]
        if pred.get("op") == "and":
            out = []
            for t in pred["terms"]:
                sub = flatten_conjuncts(t)
                if sub is None:
                    return None
                out.extend(sub)
            return out
        return None

    if peek() == ("kw", "where"):
        i += 1
        pred = p_or()
        plan["predicate"] = pred
        flat = flatten_conjuncts(pred)
        plan["where"] = flat if flat is not None else []

    # VALID AS OF <date> — also accepted after WHERE (second position)
    if peek() == ("kw", "valid") and plan["valid_as_of"] is None:
        i += 1
        for kw in ("as", "of"):
            if peek()[1] == kw: i += 1
        t, v = peek()
        if t != "str":
            raise SyntaxError("NQL: VALID AS OF expects a quoted date string")
        i += 1
        plan["valid_as_of"] = v

    # SEARCH "text"
    if peek() == ("kw", "search"):
        i += 1
        t, v = peek()
        if t != "str":
            raise SyntaxError("NQL: SEARCH expects a quoted string")
        i += 1
        plan["search"] = v

    # ORDER BY field [ASC|DESC] (, field [ASC|DESC])*
    #
    # plan["order_by"] keeps its historical (field, direction) shape for a
    # single key so existing callers and the fluent builder are unaffected;
    # plan["order_keys"] carries the full list. The executor reads order_keys.
    #
    # Absorbed in two positions for the same reason as LIMIT/OFFSET: SQL puts
    # ORDER BY after GROUP BY, and the Rust engine's clause loop is position-
    # independent, so accepting only the pre-GROUP-BY position here made
    # `GROUP BY g COUNT ORDER BY count DESC` a syntax error in one engine and
    # valid in the other.
    def absorb_order():
        nonlocal i
        if peek() != ("kw", "order") or plan["order_keys"] is not None:
            return
        i += 1
        expect_kw("by")
        keys = []
        while True:
            t, _f = peek()
            if t not in ("word", "kw"):
                raise SyntaxError("NQL: expected field after ORDER BY")
            field = peek_raw()
            i += 1
            direction = "ASC"
            if peek() == ("kw", "asc"):
                i += 1
            elif peek() == ("kw", "desc"):
                i += 1
                direction = "DESC"
            keys.append((field, direction))
            if peek() == ("punct", ","):
                i += 1
                continue
            break
        plan["order_keys"] = keys
        plan["order_by"] = keys[0]

    absorb_order()

    # TRAVERSE relation
    if peek() == ("kw", "traverse"):
        i += 1
        t, rel = peek()
        if t not in ("word", "kw"):
            raise SyntaxError("NQL: expected relation after TRAVERSE")
        i += 1
        plan["traverse"] = rel

    # TRACE <field> [REVERSE]
    # Walks the causal provenance graph.
    # TRACE caused_by          → backward: which ops caused these documents?
    # TRACE caused_by REVERSE  → forward:  which documents did these ops cause?
    if peek() == ("kw", "trace"):
        i += 1
        t, tf = peek()
        if t not in ("word", "kw"):
            raise SyntaxError("NQL: expected field name after TRACE")
        i += 1
        plan["trace"] = tf
        if peek() == ("kw", "reverse"):
            i += 1
            plan["trace_reverse"] = True

    # LIMIT n [OFFSET m]  /  OFFSET m [LIMIT n] — either order.
    #
    # Absorbed in TWO positions: before GROUP BY (where this parser has always
    # accepted LIMIT) and again after GROUP BY/HAVING, which is where SQL
    # actually puts it. The Rust engine parses clauses in a position-
    # independent loop, so accepting only the pre-GROUP-BY position here made
    # `GROUP BY status COUNT LIMIT 2` a syntax error in one engine and valid
    # in the other.
    def absorb_paging():
        nonlocal i
        for _ in range(2):
            if peek() == ("kw", "limit") and plan["limit"] is None:
                i += 1
                t, v = peek()
                if t != "num" or int(v) < 0:
                    raise SyntaxError("NQL: LIMIT expects a non-negative integer")
                i += 1
                plan["limit"] = int(v)
            elif peek() == ("kw", "offset") and plan["offset"] is None:
                i += 1
                t, v = peek()
                if t != "num" or int(v) < 0:
                    raise SyntaxError("NQL: OFFSET expects a non-negative integer")
                i += 1
                plan["offset"] = int(v)

    absorb_paging()

    def parse_agg():
        """<AGG> [target_field] — COUNT takes none, the rest require one."""
        nonlocal i
        _t, agg = peek()
        i += 1
        if agg == "count":
            return ("count", None)
        t2, _f = peek()
        if t2 not in ("word", "kw"):
            raise SyntaxError(f"NQL: {agg.upper()} expects a field name")
        agg_field = peek_raw()
        i += 1
        return (agg, agg_field)

    _AGGS = ("count", "sum", "avg", "min", "max")

    # GROUP BY field [COUNT | SUM field | AVG field | MIN field | MAX field]
    if peek() == ("kw", "group"):
        i += 1
        expect_kw("by")
        t, _f = peek()
        if t not in ("word", "kw"):
            raise SyntaxError("NQL: expected field after GROUP BY")
        plan["group_by"] = peek_raw()
        i += 1
        t, agg = peek()
        if t == "kw" and agg in _AGGS:
            plan["aggregate"] = parse_agg()

    # A bare aggregate with no GROUP BY: `FROM t COUNT`, `FROM t SUM fee`.
    # Returns exactly one row. Previously inexpressible — the aggregate
    # keywords only existed after GROUP BY, so "how many rows match?" meant
    # fetching every row and counting client-side.
    elif peek()[0] == "kw" and peek()[1] in _AGGS:
        plan["aggregate"] = parse_agg()

    # HAVING <predicate> — filters the AGGREGATED rows, so it can test
    # `count`, `sum_fee`, or the group key itself. Distinct from WHERE, which
    # filters input rows before they are grouped.
    if peek() == ("kw", "having"):
        i += 1
        if plan["aggregate"] is None and plan["group_by"] is None:
            raise SyntaxError(
                "NQL: HAVING requires an aggregate — add GROUP BY <field>, "
                "or use WHERE to filter individual rows")
        plan["having"] = p_or()

    # ORDER BY and LIMIT/OFFSET in their SQL positions, after grouping.
    absorb_order()
    absorb_paging()

    if i != len(toks):
        raise SyntaxError(f"NQL: unexpected trailing tokens: {toks[i:]}")
    return plan


def cmp(a, op, b) -> bool:
    try:
        if op == "=":
            return a == b
        if op == "!=":
            return a != b
        if a is None:
            return False
        if op == "<":
            return a < b
        if op == "<=":
            return a <= b
        if op == ">":
            return a > b
        if op == ">=":
            return a >= b
    except TypeError:
        return False
    return False


def eval_predicate(doc: dict, pred: Optional[dict]) -> bool:
    """Evaluate a predicate tree against a document.

    Semantics are pinned to the Rust engine's `eval_pred` — including the
    three-valued-logic corner where LIKE over a missing/null field is false in
    BOTH polarities, so a null row appears in neither `LIKE` nor `NOT LIKE`.
    """
    if pred is None:
        return True

    op = pred["op"]

    if op == "and":
        return all(eval_predicate(doc, t) for t in pred["terms"])
    if op == "or":
        return any(eval_predicate(doc, t) for t in pred["terms"])
    if op == "not":
        return not eval_predicate(doc, pred["term"])

    field = pred["field"]
    val = doc.get(field)

    if op == "cmp":
        return cmp(val, pred["cmp"], pred["value"])

    if op == "in":
        hit = any(cmp(val, "=", v) for v in pred["values"])
        return hit != pred["negated"]

    if op == "between":
        # Inclusive on both ends, as in SQL.
        hit = cmp(val, ">=", pred["low"]) and cmp(val, "<=", pred["high"])
        return hit != pred["negated"]

    if op == "like":
        if val is None:
            return False
        hit = bool(like_to_regex(pred["pattern"], pred["ci"]).match(str(val)))
        return hit != pred["negated"]

    if op == "regex":
        # Same three-valued logic as LIKE, and the same in the Rust engine: a
        # predicate over a missing/null field is false in BOTH polarities, so
        # `x !~ 'p'` does not resurrect an absent row. psql's catalogue
        # filters depend on that.
        if val is None:
            return False
        hit = regex_match(str(val), pred["pattern"], pred["ci"])
        return hit != pred["negated"]

    if op == "isnull":
        # Absent and explicitly-null are the same observable state in a
        # schemaless store, and `doc.get` collapses them identically.
        return (val is None) != pred["negated"]

    raise ValueError(f"NQL: unknown predicate op {op!r}")


class Query:
    """Fluent builder that compiles to the same plan dict as NQL text."""

    def __init__(self, engine, coll: str) -> None:
        self._engine = engine
        self.plan = empty_plan(coll)

    def as_of(self, seq: int) -> "Query":
        self.plan["as_of"] = seq
        return self

    def where(self, field: str, op: str, value: Any) -> "Query":
        self.plan["where"].append((field, op, value))
        return self

    def search(self, text: str) -> "Query":
        self.plan["search"] = text
        return self

    def order_by(self, field: str, desc: bool = False) -> "Query":
        self.plan["order_by"] = (field, "DESC" if desc else "ASC")
        return self

    def traverse(self, rel: str) -> "Query":
        self.plan["traverse"] = rel
        return self

    def limit(self, n: int) -> "Query":
        self.plan["limit"] = n
        return self

    def run(self) -> List[dict]:
        return self._engine.execute(self.plan)
