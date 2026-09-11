# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.autoindex — automatic index management.

Wraps a NEDB instance and intercepts query() calls. It tracks which fields are
used in WHERE and ORDER BY clauses per collection. Once a field reaches the
usage threshold it auto-creates the appropriate index:

  - Equality conditions (= / !=)   → "eq"    index
  - Ordered comparisons (< > ≤ ≥) → "ordered" index
  - ORDER BY field                  → "ordered" index
  - SEARCH clause on a field        → deferred (no per-field signal in NQL)

Usage::

    from nedb import NEDB
    from nedb.autoindex import AutoIndexDB

    db = AutoIndexDB(NEDB("./data"), threshold=3)
    db.query('FROM users WHERE status = "active"')   # tallied
    db.query('FROM users WHERE status = "active"')
    db.query('FROM users WHERE status = "active"')   # threshold reached → index created
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple


_WHERE_RE = re.compile(r"\bWHERE\b([\s\S]*?)(?:\bSEARCH\b|\bORDER\b|\bTRAVERSE\b|\bLIMIT\b|$)", re.IGNORECASE)
_ORDER_RE = re.compile(r"\bORDER\s+BY\s+(\w+)", re.IGNORECASE)
_FROM_RE  = re.compile(r"\bFROM\s+(\w+)", re.IGNORECASE)
_COND_RE  = re.compile(r"(\w+)\s*(=|!=|<>|<=|>=|<|>)", re.IGNORECASE)


def _parse_signals(nql: str) -> List[Tuple[str, str, str]]:
    """Return [(collection, field, 'eq'|'ordered')] from a NQL query string."""
    signals = []
    fm = _FROM_RE.search(nql)
    if not fm:
        return signals
    coll = fm.group(1)

    wm = _WHERE_RE.search(nql)
    if wm:
        for m in _COND_RE.finditer(wm.group(1)):
            field, op = m.group(1), m.group(2)
            kind = "eq" if op in ("=", "!=", "<>") else "ordered"
            signals.append((coll, field, kind))

    om = _ORDER_RE.search(nql)
    if om:
        signals.append((coll, om.group(1), "ordered"))

    return signals


class AutoIndexDB:
    """
    NEDB wrapper that creates indexes automatically based on query usage.

    Parameters
    ----------
    db : NEDB
        A NEDB database instance (embedded or opened with a path).
    threshold : int
        Number of times a (collection, field, kind) combination must be
        observed before the index is created. Default: 5.
    verbose : bool
        Print a message when an index is auto-created. Default: False.
    """

    def __init__(self, db: Any, threshold: int = 5, verbose: bool = False):
        self._db = db
        self.threshold = threshold
        self.verbose = verbose
        # counts[(coll, field, kind)] = n
        self._counts: Dict[Tuple[str, str, str], int] = defaultdict(int)
        # indexes already created so we don't re-create
        self._created: set = set()
        # Seed from existing index config if available
        if hasattr(db, "indexes") and hasattr(db.indexes, "config"):
            for coll, field, kind in db.indexes.config:
                self._created.add((coll, field, kind))

    # ── Proxy every NEDB attribute ────────────────────────────────────────────

    def __getattr__(self, name: str) -> Any:
        return getattr(self._db, name)

    # ── Instrumented query ────────────────────────────────────────────────────

    def query(self, nql: str) -> List[dict]:
        """Execute a NQL query, tally field usage, and auto-create indexes."""
        signals = _parse_signals(nql)
        for coll, field, kind in signals:
            key = (coll, field, kind)
            if key in self._created:
                continue
            # "ordered" supersedes "eq" — if we already have eq, upgrade to ordered
            eq_key = (coll, field, "eq")
            if kind == "ordered" and eq_key not in self._created:
                self._counts[key] += 1
            elif kind == "eq" and (coll, field, "ordered") not in self._created:
                self._counts[key] += 1
            else:
                self._counts[key] += 1

            if self._counts[key] >= self.threshold:
                self._auto_create(coll, field, kind)

        return self._db.query(nql)

    def _auto_create(self, coll: str, field: str, kind: str) -> None:
        key = (coll, field, kind)
        if key in self._created:
            return
        # Don't index internal NEDB fields
        if field.startswith("_") and field not in ("_id",):
            return
        self._db.create_index(coll, field, kind)
        self._created.add(key)
        if self.verbose:
            print(f"[autoindex] created {kind} index on {coll}.{field} (threshold={self.threshold})")

    # ── Manual analysis ───────────────────────────────────────────────────────

    def analyze(self) -> Dict[str, Any]:
        """Return current tallies and the indexes already created."""
        return {
            "tallies": {f"{c}.{f} ({k})": n for (c, f, k), n in self._counts.items()},
            "indexes_created": [f"{c}.{f} ({k})" for (c, f, k) in sorted(self._created)],
            "threshold": self.threshold,
        }

    def suggest(self) -> List[str]:
        """Return suggestions for indexes that are close to the threshold."""
        out = []
        for (coll, field, kind), count in sorted(self._counts.items(), key=lambda x: -x[1]):
            if (coll, field, kind) not in self._created:
                out.append(f"{coll}.{field} ({kind}) — {count}/{self.threshold} queries")
        return out
