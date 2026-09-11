# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
NedbClient — the official Python client for nedbd's HTTP API.

Extracted from production: this client is the promotion of the two
battle-tested hand-rolled clients that ran the AiAS mainnet migration
(the storage adapter's ``NedbdClient`` and the backfill's ``Nedbd``),
unified and extended to speak the daemon's full route surface. Design
choices carry their scars:

  * **Typed errors, one vocabulary.** A CAS miss raises the SAME
    :class:`~nedb.engine.PreconditionFailed` the embedded engine raises,
    with the same ``.failures`` shape — code written against ``NEDB.tx``
    ports to the HTTP client without changing its except-clauses.
  * **No transparent write retries.** Writes are only safe to repeat when
    the caller supplies an ``idem`` key (the daemon dedupes replays into
    silent no-ops), so the client never retries on its own. The one
    sanctioned loop is :meth:`NedbClient.cas_retry`, which retries ONLY
    on ``PreconditionFailed`` with capped exponential backoff — the same
    discipline the AiAS Lua-parity guards shipped with.
  * **Env-var defaults** (``NEDBD_URL``, ``NEDBD_TOKEN``, ``NEDB_DB``)
    mirror the daemon's own configuration surface, so
    ``NedbClient()`` with no arguments does the right thing next to a
    conventionally-configured nedbd.

Quick start::

    from nedb.client import NedbClient, op_put

    c = NedbClient("http://127.0.0.1:7070", db="app")
    c.ensure_database()
    c.put("users", "u1", {"id": "u1", "email": "a@b.c"})
    c.query('FROM users WHERE email = "a@b.c"')

    # atomic CAS transaction — all-or-nothing, engine-checked
    doc = c.get_doc("users", "u1")
    c.tx([op_put("users", "u1", {**doc, "plan": "pro"},
                 if_seq=doc["_seq"])])

    # contested writes: retry only on precondition failures
    def bump():
        d = c.get_doc("counters", "hits") or {"n": 0}
        return c.tx([op_put("counters", "hits", {"n": int(d.get("n", 0)) + 1},
                            if_seq=d.get("_seq", -1))])
    c.cas_retry(bump)

Not yet wrapped (use :meth:`NedbClient.request` directly if needed):
the files API (``POST/GET …/files``) — its tier/version semantics get a
dedicated helper in a follow-up release.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, TypeVar

from .engine import PreconditionFailed  # one exception type, both transports

__all__ = [
    "NedbClient", "op_put", "op_del",
    "NedbError", "NedbAuthError", "NedbNotFound", "NedbBadRequest",
    "NedbConflict", "CasExhausted", "PreconditionFailed",
]

T = TypeVar("T")

DEFAULT_URL = "http://localhost:7070"
DEFAULT_TIMEOUT_S = 30.0
CAS_MAX_RETRIES = 16       # matches the AiAS guard budget
CAS_BACKOFF_BASE_S = 0.01  # grows 1.5x per attempt …
CAS_BACKOFF_CAP_S = 0.2    # … capped here


# ── errors ───────────────────────────────────────────────────────────────────

def _seg(value: str) -> str:
    """Percent-encode one URL path segment.

    `/` MUST be encoded (hence ``safe=""``): an id containing a slash used to
    split the path so the route matched a different id, and ``delete_doc``
    returned "did not exist" for a document that did.
    """
    return urllib.parse.quote(str(value), safe="")


class NedbError(RuntimeError):
    """Base error for daemon interactions. ``status`` is the HTTP status
    (``None`` for transport failures); ``payload`` is the parsed error body."""

    def __init__(self, message: str, status: Optional[int] = None,
                 payload: Optional[dict] = None):
        super().__init__(message)
        self.status = status
        self.payload = payload or {}


class NedbAuthError(NedbError):
    """401/403 — missing or invalid bearer token."""


class NedbNotFound(NedbError):
    """404 — unknown database, route, or op hash."""


class NedbBadRequest(NedbError):
    """400 — malformed request or NQL error."""


class NedbConflict(NedbError):
    """409 that is NOT a CAS precondition failure — e.g. a nonce replay
    (``ReplayError`` server-side). Preconditions raise
    :class:`~nedb.engine.PreconditionFailed` instead."""


class CasExhausted(NedbError):
    """:meth:`NedbClient.cas_retry` lost the race ``max_retries`` times in a
    row. At sane write volumes this indicates a stuck loop or a hot-doc
    pathology, not load."""


# ── tx op builders ───────────────────────────────────────────────────────────

def op_put(coll: str, doc_id: str, doc: dict, *,
           if_seq: Optional[int] = None,
           ttl_s: Optional[float] = None,
           caused_by: Optional[List[int]] = None,
           evidence: Optional[str] = None,
           confidence: Optional[float] = None,
           valid_from: Optional[str] = None,
           valid_to: Optional[str] = None) -> Dict[str, Any]:
    """A ``put`` op for :meth:`NedbClient.tx`.

    ``if_seq`` semantics (checked atomically across the whole tx):
      * ``N``  — the doc's current ``_seq`` must equal N (CAS update)
      * ``-1`` — the doc must NOT exist (create-once)
      * ``None`` — unconditional
    """
    op: Dict[str, Any] = {"op": "put", "coll": coll, "id": doc_id, "doc": doc}
    if if_seq is not None:
        op["if_seq"] = if_seq
    if ttl_s is not None:
        op["ttl_s"] = ttl_s
    if caused_by is not None:
        op["caused_by"] = caused_by
    if evidence is not None:
        op["evidence"] = evidence
    if confidence is not None:
        op["confidence"] = confidence
    if valid_from is not None:
        op["valid_from"] = valid_from
    if valid_to is not None:
        op["valid_to"] = valid_to
    return op


def op_del(coll: str, doc_id: str, *,
           if_seq: Optional[int] = None) -> Dict[str, Any]:
    """A ``del`` op for :meth:`NedbClient.tx` (``if_seq`` as in
    :func:`op_put`)."""
    op: Dict[str, Any] = {"op": "del", "coll": coll, "id": doc_id}
    if if_seq is not None:
        op["if_seq"] = if_seq
    return op


# ── the client ───────────────────────────────────────────────────────────────

class NedbClient:
    """Synchronous client for one nedbd daemon, scoped to one database.

    Thread-safe: holds no mutable state beyond configuration, and every
    request is an independent HTTP call (nedbd's Sequencer provides the
    single-committer serialization server-side).
    """

    def __init__(self, url: Optional[str] = None, db: Optional[str] = None,
                 token: Optional[str] = None,
                 timeout: float = DEFAULT_TIMEOUT_S,
                 client_tag: str = "nedb-client"):
        self.url = (url or os.getenv("NEDBD_URL", DEFAULT_URL)).rstrip("/")
        self.db = db or os.getenv("NEDB_DB", "default")
        self.token = token if token is not None else os.getenv("NEDBD_TOKEN")
        self.timeout = timeout
        self.client_tag = client_tag

    # ── transport ────────────────────────────────────────────────────────────

    def request(self, method: str, path: str, body: Optional[dict] = None,
                timeout: Optional[float] = None) -> dict:
        """Raw authenticated request. Public on purpose: routes this client
        doesn't wrap yet stay reachable without giving up auth/error typing."""
        req = urllib.request.Request(
            f"{self.url}{path}",
            data=json.dumps(body).encode() if body is not None else None,
            method=method)
        req.add_header("Content-Type", "application/json")
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        try:
            with urllib.request.urlopen(
                    req, timeout=self.timeout if timeout is None else timeout
            ) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            raw = e.read().decode(errors="replace")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {"error": raw[:300]}
            self._raise_typed(e.code, payload, method, path)
        except urllib.error.URLError as e:
            raise NedbError(
                f"nedbd unreachable at {self.url}: {e.reason}") from e

    @staticmethod
    def _raise_typed(status: int, payload: dict, method: str,
                     path: str) -> None:
        msg = str(payload.get("error") or payload)[:300]
        if status == 409 and payload.get("error") == "precondition_failed":
            exc = PreconditionFailed(payload.get("failures") or [])
            exc.seq = payload.get("seq")  # server seq at rejection time
            raise exc
        if status in (401, 403):
            raise NedbAuthError(msg, status, payload)
        if status == 404:
            raise NedbNotFound(msg, status, payload)
        if status == 400:
            raise NedbBadRequest(msg, status, payload)
        if status == 409:
            raise NedbConflict(msg, status, payload)
        raise NedbError(f"nedbd {method} {path} -> {status}: {msg}",
                        status, payload)

    def _dbp(self, action: str = "") -> str:
        tail = f"/{action}" if action else ""
        return f"/v1/databases/{self.db}{tail}"

    # ── server / database management ─────────────────────────────────────────

    def health(self) -> dict:
        """``GET /health`` — unauthenticated liveness + daemon version."""
        return self.request("GET", "/health")

    def wait_ready(self, timeout: float = 10.0, interval: float = 0.2) -> dict:
        """Poll ``/health`` until the daemon answers (startup helper)."""
        deadline = time.time() + timeout
        last: Optional[Exception] = None
        while time.time() < deadline:
            try:
                return self.health()
            except NedbError as e:  # noqa: PERF203 — startup poll
                last = e
                time.sleep(interval)
        raise NedbError(f"nedbd not ready after {timeout}s: {last}")

    def databases(self) -> List[dict]:
        return self.request("GET", "/v1/databases").get("databases", [])

    def database_detail(self, name: Optional[str] = None) -> dict:
        return self.request("GET", f"/v1/databases/{name or self.db}")

    def create_database(self, name: Optional[str] = None,
                        init: Optional[dict] = None) -> dict:
        body: Dict[str, Any] = {"name": name or self.db}
        if init is not None:
            body["init"] = init
        return self.request("POST", "/v1/databases", body)

    def ensure_database(self, name: Optional[str] = None) -> bool:
        """Create the database iff missing (idempotent). True if created."""
        want = name or self.db
        if want in (self.health().get("databases") or []):
            return False
        self.create_database(want)
        return True

    def drop_database(self, name: str) -> dict:
        """``DELETE /v1/databases/{name}`` — destructive; the name is
        REQUIRED (never defaults to ``self.db``) so a drop is always spelled
        out at the call site."""
        return self.request("DELETE", f"/v1/databases/{name}")

    # ── reads ────────────────────────────────────────────────────────────────

    def query(self, nql: str) -> List[dict]:
        """Run NQL, return rows. (AS OF / VALID AS OF / TRACE / ORDER BY /
        LIMIT / GROUP BY all ride through — the daemon owns the grammar.)"""
        return self.query_full(nql).get("rows", [])

    def query_full(self, nql: str) -> dict:
        """Run NQL, return the full envelope ``{rows, count, seq, head}``."""
        return self.request("POST", self._dbp("query"), {"nql": nql})

    def get_doc(self, coll: str, doc_id: str,
                as_of: Optional[int] = None) -> Optional[dict]:
        """Fetch one doc by id (returns ``None`` when absent). Docs carry
        ``_id`` and ``_seq`` — feed ``_seq`` to ``op_put(if_seq=…)``.

        With ``as_of``, returns the version at or before that sequence number.

        Uses ``GET …/rows/{coll}/{id}``, which takes the id from the URL path.
        This used to build ``FROM coll WHERE _id = "…"`` and interpolate the id
        into it — so an id containing a double quote was REJECTED outright
        (``unquotable identifier``) even though ``put`` accepts it, ``FROM
        coll`` returns it and the engine stores it fine. The guard was there
        because the interpolation was unsafe; taking the id from the path
        removes the interpolation, so the id no longer needs guarding and
        legitimate ids with quotes now work.

        Injection is impossible by construction here: nothing is concatenated
        into a query, so a crafted id can only ever name a document.

        Falls back to the query path against a server predating the route
        (nedb-engine < 3.3.0 answers 405 there, having registered only DELETE),
        where the historical ``unquotable identifier`` guard still applies
        because the interpolation is back.
        """
        path = self._dbp(f"rows/{_seg(coll)}/{_seg(doc_id)}")
        if as_of is not None:
            path = f"{path}?as_of={int(as_of)}"
        try:
            # The route answers 200 with `row: null` for a missing document,
            # precisely so this cannot be confused with "no such route".
            return self.request("GET", path).get("row")
        except NedbError as e:
            # 404/405 => the server predates this route (the Rust daemon
            # registered only DELETE on this path; the Python AOF server had
            # no such path at all). Anything else is a real error.
            if getattr(e, "status", None) not in (404, 405):
                raise
        if '"' in coll or '"' in str(doc_id):
            raise NedbBadRequest(f"unquotable identifier: {coll}:{doc_id}")
        as_of_clause = f" AS OF {int(as_of)}" if as_of is not None else ""
        rows = self.query(f'FROM {coll}{as_of_clause} WHERE _id = "{doc_id}"')
        return rows[0] if rows else None

    def count(self, coll: str) -> int:
        return int(self.query_full(f"FROM {coll}").get("count", 0))

    # ── writes ───────────────────────────────────────────────────────────────

    def put(self, coll: str, doc_id: str, doc: dict, *,
            idem: Optional[str] = None,
            ttl_s: Optional[float] = None,
            nonce: Optional[int] = None,
            caused_by: Optional[List[int]] = None,
            evidence: Optional[str] = None,
            confidence: Optional[float] = None,
            valid_from: Optional[str] = None,
            valid_to: Optional[str] = None,
            client: Optional[str] = None) -> dict:
        """Single unconditional put → ``{ok, doc, seq, head}``.

        ``idem`` makes the write replay-safe: the daemon dedupes a repeated
        key into a silent no-op returning the ORIGINAL result — the property
        the AiAS backfill's restart-safety is built on. For compare-and-set
        preconditions use :meth:`tx` (``if_seq`` is transactional-only).
        """
        body: Dict[str, Any] = {"coll": coll, "id": doc_id, "doc": doc,
                                "client": client or self.client_tag}
        for k, v in (("idem", idem), ("ttl_s", ttl_s), ("nonce", nonce),
                     ("caused_by", caused_by), ("evidence", evidence),
                     ("confidence", confidence), ("valid_from", valid_from),
                     ("valid_to", valid_to)):
            if v is not None:
                body[k] = v
        return self.request("POST", self._dbp("put"), body)

    def delete(self, coll: str, doc_id: str) -> dict:
        """``DELETE …/rows/{coll}/{id}`` → ``{ok, seq, head}``."""
        return self.request(
            "DELETE", self._dbp(f"rows/{_seg(coll)}/{_seg(doc_id)}"))

    def tx(self, ops: List[Dict[str, Any]], *,
           client: Optional[str] = None) -> dict:
        """Atomic all-or-nothing transaction (``POST …/batch``).

        Validates every ``if_seq`` precondition first, then applies every op
        inside one group-commit — the engine primitive that replaced Redis
        Lua scripts. Raises :class:`~nedb.engine.PreconditionFailed` (with
        ``.failures`` and ``.seq``) when any check misses; NOTHING is applied.
        Build ops with :func:`op_put` / :func:`op_del`.
        """
        return self.request("POST", self._dbp("batch"),
                            {"ops": ops, "client": client or self.client_tag})

    def cas_retry(self, fn: Callable[[], T], *,
                  max_retries: int = CAS_MAX_RETRIES,
                  backoff_base: float = CAS_BACKOFF_BASE_S,
                  backoff_cap: float = CAS_BACKOFF_CAP_S) -> T:
        """Run ``fn`` (read → build ops → :meth:`tx`), retrying ONLY on
        :class:`PreconditionFailed` with capped exponential backoff. Every
        other error propagates untouched. This is the guard discipline from
        the AiAS migration, packaged."""
        for attempt in range(max_retries):
            try:
                return fn()
            except PreconditionFailed:
                if attempt == max_retries - 1:
                    break
                time.sleep(min(backoff_base * (1.5 ** attempt), backoff_cap))
        raise CasExhausted(
            f"CAS retry budget exhausted after {max_retries} attempts")

    # ── indexes / relations ──────────────────────────────────────────────────

    def create_index(self, coll: str, field: str, kind: str = "eq") -> dict:
        """Idempotent server-side."""
        return self.request("POST", self._dbp("index"),
                            {"coll": coll, "field": field, "kind": kind})

    def ensure_indexes(self, pairs: Iterable[Tuple[str, str]]) -> int:
        n = 0
        for coll, field in pairs:
            self.create_index(coll, field)
            n += 1
        return n

    def link(self, frm: str, rel: str, to: str) -> dict:
        return self.request("POST", self._dbp("link"),
                            {"frm": frm, "rel": rel, "to": to})

    def neighbors(self, node: str, rel: str) -> List[str]:
        return self.request("POST", self._dbp("neighbors"),
                            {"node": node, "rel": rel}).get("nodes", [])

    # ── integrity / ops ──────────────────────────────────────────────────────

    def verify(self) -> dict:
        """Recompute the hash chain server-side → ``{ok, seq, head}``."""
        return self.request("GET", self._dbp("verify"))

    def checkpoint(self) -> dict:
        return self.request("POST", self._dbp("checkpoint"))

    def sweep(self) -> int:
        """Expire TTL'd docs now; returns the number swept."""
        return int(self.request("POST", self._dbp("sweep")).get("swept", 0))

    def log(self, limit: int = 50) -> List[dict]:
        """Most-recent-first op log (each op carries its chain ``hash``)."""
        return self.request(
            "GET", self._dbp(f"log?limit={int(limit)}")).get("log", [])

    def proof(self, op_hash: str) -> dict:
        """Merkle inclusion proof for an op hash. Verify locally with
        :func:`nedb.proof.verify_proof` — no server trust required."""
        return self.request("POST", self._dbp("proof"), {"hash": op_hash})

    def mongo(self, collection: str, op: str, **kwargs: Any) -> dict:
        """Thin passthrough to the Mongo-compat endpoint
        (``find/findOne/count/insertOne/…``)."""
        return self.request("POST", self._dbp("mongo"),
                            {"collection": collection, "op": op, **kwargs})
