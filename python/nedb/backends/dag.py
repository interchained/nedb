# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.backends.dag — embedded v2/v3 DAG backend for the wrap_* surface.

Wraps the Rust native core (`nedb._native.NedbCore`) in the same duck-typed
engine contract that NEDBSurface expects from the v1 in-process engine and
the NedBdProxy (HTTP nedbd) backend:

    put(coll, id, doc, **kw) -> dict
    get(coll, id, as_of=None) -> dict | None
    query(nql) -> list[dict]
    create_index(coll, field, kind) -> None
    delete(coll, id) -> None
    link(frm, rel, to) / unlink(frm, rel, to)
    neighbors(frm, rel, as_of=None) / inbound(to, rel, as_of=None)
    verify() -> bool
    head (str) / seq (int)
    checkpoint() -> str

Plus DAG-native extras the other backends don't have:
    tip()          — latest node JSON
    tip_collection(coll)
    since(after_seq, limit) — changefeed page
    scan_status()  — replication readiness
    flush()        — durable flush

© INTERCHAINED LLC × Claude Sonnet 4.6
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


def has_dag_native() -> bool:
    """True when the embedded Rust v2/v3 DAG core is actually LOADED.

    This used to be `from .. import __has_native__; return True` — which tests
    whether the NAME imports, never what it holds. `__has_native__` is always
    defined (it is False on the universal pure-Python wheel), so this returned
    True unconditionally and `backend="auto"` chose the DAG on machines that
    had no compiled core at all.
    """
    try:
        from .. import __has_native__
        return bool(__has_native__)
    except Exception:
        return False


def _require_native():
    from .. import _native  # raises ImportError with a helpful message if absent
    return _native


class DagBackend:
    """
    Embedded v2/v3 DAG engine handle.

    Modes:
        DagBackend()                       → in-memory DAG (zero disk I/O)
        DagBackend(path="/data/nedb")      → durable DAG store (v2 loose layout)
        NEDB_DAG_V3=1 (env) or --dag-v3    → v3 segment/pack substrate (opt-in
                                             at the engine layer; this backend
                                             inherits it transparently)

    tmk: optional 64-hex TMK → AES-256-GCM at-rest encryption, key derived
    exactly as nedbd-v2 derives it (SHA-256(TMK ‖ basename(path))) so the same
    directory opened embedded or served over HTTP decrypts identically.
    """

    def __init__(self, path: Optional[str] = None, tmk: Optional[str] = None):
        native = _require_native()
        if path is None:
            self._core = native.NedbCore()
            self._path = None
        else:
            self._core = native.NedbCore.open(str(path), tmk=tmk)
            self._path = str(path)

    # ── internal: node JSON → doc dict (same shape as nedbd HTTP returns) ────

    @staticmethod
    def _node_to_doc(node_json: str) -> Dict[str, Any]:
        doc = json.loads(node_json)
        return doc

    # ── engine contract ──────────────────────────────────────────────────────

    def put(self, coll: str, id: str, doc: Dict[str, Any], **kw) -> Dict[str, Any]:
        body = dict(doc)
        for k in ("caused_by", "valid_from", "valid_to"):
            if kw.get(k) is not None:
                body[k] = kw[k]
        node = self._core.put(coll, id, json.dumps(body))
        return json.loads(node)

    def get(self, coll: str, id: str, as_of: Optional[int] = None) -> Optional[Dict[str, Any]]:
        node = self._core.get(coll, id, as_of=as_of)
        return json.loads(node) if node else None

    def query(self, nql: str) -> List[Dict[str, Any]]:
        return [json.loads(r) for r in self._core.query(nql)]

    def create_index(self, coll: str, field: str, kind: str = "eq") -> None:
        self._core.create_index(coll, field, kind)

    def delete(self, coll: str, id: str, **kw) -> None:
        self._core.delete(coll, id)

    def link(self, frm: str, rel: str, to: str, **kw) -> None:
        self._core.link(frm, rel, to)

    def unlink(self, frm: str, rel: str, to: str, **kw) -> None:
        self._core.unlink(frm, rel, to)

    def neighbors(self, frm: str, rel: str, as_of: Optional[int] = None) -> List[str]:
        return list(self._core.neighbors(frm, rel, as_of=as_of))

    def inbound(self, to: str, rel: str, as_of: Optional[int] = None) -> List[str]:
        return list(self._core.inbound(to, rel, as_of=as_of))

    def verify(self) -> bool:
        return bool(self._core.verify())

    @property
    def head(self) -> str:
        return self._core.head()

    @property
    def seq(self) -> int:
        return int(self._core.seq())

    def checkpoint(self) -> str:
        self.flush()
        return self.head

    # ── DAG-native extras ────────────────────────────────────────────────────

    def tip(self) -> Optional[Dict[str, Any]]:
        node = self._core.tip()
        return json.loads(node) if node else None

    def tip_collection(self, coll: str) -> Optional[Dict[str, Any]]:
        node = self._core.tip_collection(coll)
        return json.loads(node) if node else None

    def since(self, after_seq: int, limit: int = 0) -> Dict[str, Any]:
        return json.loads(self._core.since(after_seq, limit))

    def scan_status(self) -> Dict[str, Any]:
        return json.loads(self._core.scan_status())

    def flush(self) -> None:
        self._core.flush()

    @property
    def engine_kind(self) -> str:
        return "dag-embedded"
