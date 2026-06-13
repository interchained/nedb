"""
nedb.relations — first-class, time-travel-aware relations (the graph layer).

Relations are stored as adjacency lists for O(1) traversal. Each edge records the
seq at which it was added and (optionally) removed, so relation queries can also be
asked "AS OF" any past sequence — the graph time-travels just like the records do.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple


class Relations:
    def __init__(self) -> None:
        # (frm, rel) -> list of [to, added_seq, removed_seq|None]
        self._adj: Dict[Tuple[str, str], List[list]] = {}
        # (to, rel) -> list of [frm, added_seq, removed_seq|None]  (reverse index)
        self._radj: Dict[Tuple[str, str], List[list]] = {}

    def link(self, frm: str, rel: str, to: str, seq: int) -> None:
        for e in self._adj.get((frm, rel), []):
            if e[0] == to and e[2] is None:
                return  # already linked
        self._adj.setdefault((frm, rel), []).append([to, seq, None])
        self._radj.setdefault((to, rel), []).append([frm, seq, None])

    def unlink(self, frm: str, rel: str, to: str, seq: int) -> None:
        for e in self._adj.get((frm, rel), []):
            if e[0] == to and e[2] is None:
                e[2] = seq
        for e in self._radj.get((to, rel), []):
            if e[0] == frm and e[2] is None:
                e[2] = seq

    @staticmethod
    def _live(edges, as_of):
        out = []
        for node, added, removed in edges:
            if as_of is None:
                if removed is None:
                    out.append(node)
            else:
                if added <= as_of and (removed is None or removed > as_of):
                    out.append(node)
        return out

    def neighbors(self, frm: str, rel: str, as_of: Optional[int] = None) -> List[str]:
        return self._live(self._adj.get((frm, rel), []), as_of)

    def inbound(self, to: str, rel: str, as_of: Optional[int] = None) -> List[str]:
        return self._live(self._radj.get((to, rel), []), as_of)
