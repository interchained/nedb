"""
nedb.store — MVCC versioned key/value store with time-travel.

Each key maps to an ascending list of (seq, value) versions. Reads at HEAD return
the newest version; reads `as_of=N` return the newest version with seq <= N. This
gives snapshot isolation (readers never block writers) and time-travel for free,
because the version history IS the operation log's effect, replayed.
"""
from __future__ import annotations

import bisect
from typing import Any, Dict, List, Optional, Tuple

TOMB = object()  # tombstone marker for deletes


class MVCCStore:
    def __init__(self) -> None:
        self._v: Dict[str, List[Tuple[int, Any]]] = {}
        self._seqs: Dict[str, List[int]] = {}  # parallel seq list for bisect

    def put(self, key: str, value: Any, seq: int) -> None:
        self._v.setdefault(key, []).append((seq, value))
        self._seqs.setdefault(key, []).append(seq)

    def delete(self, key: str, seq: int) -> None:
        self._v.setdefault(key, []).append((seq, TOMB))
        self._seqs.setdefault(key, []).append(seq)

    def get(self, key: str, as_of: Optional[int] = None) -> Optional[Any]:
        chain = self._v.get(key)
        if not chain:
            return None
        if as_of is None:
            _, val = chain[-1]
        else:
            i = bisect.bisect_right(self._seqs[key], as_of) - 1
            if i < 0:
                return None
            _, val = chain[i]
        return None if val is TOMB else val

    def keys(self, prefix: str = "", as_of: Optional[int] = None) -> List[str]:
        out = []
        for k in self._v:
            if prefix and not k.startswith(prefix):
                continue
            if self.get(k, as_of) is not None:
                out.append(k)
        return out

    def snapshot(self, prefix: str = "", as_of: Optional[int] = None) -> Dict[str, Any]:
        return {k: self.get(k, as_of) for k in self.keys(prefix, as_of)}
