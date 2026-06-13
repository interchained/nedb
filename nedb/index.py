"""
nedb.index — secondary indexes: equality (hash), ordered (bisect), full-text (inverted).

Indexes are maintained incrementally on write and reflect HEAD. They turn filter,
sort and search from O(n) scans into index lookups. Indexes are keyed by
"collection.field" so each collection has its own index namespace.

(Time-travel queries fall back to a version scan in the engine; temporally-indexed
reads are a documented later optimization.)
"""
from __future__ import annotations

import bisect
import re
from typing import Any, Dict, List, Set

_TOKEN = re.compile(r"[a-z0-9]+")


def tokenize(text: str) -> Set[str]:
    return set(_TOKEN.findall(text.lower()))


class Indexes:
    def __init__(self) -> None:
        self.eq: Dict[str, Dict[Any, Set[str]]] = {}        # key -> value -> {ids}
        self.ordered: Dict[str, List[tuple]] = {}           # key -> sorted [(value,id)]
        self.inv: Dict[str, Dict[str, Set[str]]] = {}       # key -> token -> {ids}
        self.config: List[tuple] = []                       # [(coll, field, kind)]

    def ensure(self, coll: str, field: str, kind: str = "eq") -> None:
        k = f"{coll}.{field}"
        if (coll, field, kind) not in self.config:
            self.config.append((coll, field, kind))
        if kind == "eq":
            self.eq.setdefault(k, {})
        elif kind == "ordered":
            self.ordered.setdefault(k, [])
        elif kind == "search":
            self.inv.setdefault(k, {})
        else:
            raise ValueError(f"unknown index kind: {kind}")

    def add(self, coll: str, key: str, doc: dict) -> None:
        for field, vmap in self.eq.items():
            f = field.split(".", 1)[1]
            if field.startswith(coll + ".") and f in doc:
                vmap.setdefault(doc[f], set()).add(key)
        for field, lst in self.ordered.items():
            f = field.split(".", 1)[1]
            if field.startswith(coll + ".") and f in doc and isinstance(doc[f], (int, float, str)):
                bisect.insort(lst, (doc[f], key))
        for field, inv in self.inv.items():
            f = field.split(".", 1)[1]
            if field.startswith(coll + ".") and isinstance(doc.get(f), str):
                for tok in tokenize(doc[f]):
                    inv.setdefault(tok, set()).add(key)

    def remove(self, coll: str, key: str, doc: dict) -> None:
        for field, vmap in self.eq.items():
            f = field.split(".", 1)[1]
            if field.startswith(coll + ".") and f in doc and doc[f] in vmap:
                vmap[doc[f]].discard(key)
        for field, lst in self.ordered.items():
            f = field.split(".", 1)[1]
            if field.startswith(coll + ".") and f in doc:
                try:
                    lst.remove((doc[f], key))
                except ValueError:
                    pass
        for field, inv in self.inv.items():
            f = field.split(".", 1)[1]
            if field.startswith(coll + ".") and isinstance(doc.get(f), str):
                for tok in tokenize(doc[f]):
                    if tok in inv:
                        inv[tok].discard(key)

    def eq_lookup(self, coll: str, field: str, value: Any):
        return set(self.eq.get(f"{coll}.{field}", {}).get(value, set()))

    def search_lookup(self, coll: str, field: str, term: str):
        return set(self.inv.get(f"{coll}.{field}", {}).get(term, set()))

    def has_eq(self, coll: str, field: str) -> bool:
        return f"{coll}.{field}" in self.eq

    def search_fields(self, coll: str) -> List[str]:
        return [k.split(".", 1)[1] for k in self.inv if k.startswith(coll + ".")]
