"""
NedbClient — async HTTP client for the nedbd server.

Compatible with both v1 AOF (nedb-engine <= 2.0.x) and
v2 DAG (nedb-engine >= 2.0.4 with --dag / NEDBD_DAG=1).

All /v1/databases/* routes are covered. The client handles:
  - Bearer token auth
  - Separate read (3s) and write (30s) timeout clients
  - Auto-create database on first write (404 → create → retry)
  - Resilient queries: 400/404 returns [] instead of raising
  - Async context manager for clean lifecycle management
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Union

try:
    import httpx
except ImportError as exc:  # pragma: no cover
    raise ImportError("nedb-client requires httpx: pip install httpx") from exc


class NedbError(Exception):
    """Raised when nedbd returns a non-2xx response (except auto-handled cases)."""
    def __init__(self, status: int, message: str) -> None:
        self.status = status
        self.message = message
        super().__init__(f"NedbError {status}: {message}")


# ── Timeouts ──────────────────────────────────────────────────────────────────

_READ_TIMEOUT  = httpx.Timeout(connect=2.0, read=3.0,  write=10.0, pool=2.0)
_WRITE_TIMEOUT = httpx.Timeout(connect=2.0, read=30.0, write=30.0, pool=2.0)


class NedbClient:
    """
    Async HTTP client for nedbd.

    Parameters
    ----------
    url : str
        Base URL of the nedbd server, e.g. "http://127.0.0.1:7070".
    db : str
        Database name. All operations target this database.
    token : str, optional
        Bearer token (set NEDBD_TOKEN on the server to require it).
    auto_create : bool
        Automatically create the database on first write if it doesn't exist.
        Default True.

    Examples
    --------
    Async context manager (recommended):

        async with NedbClient("http://127.0.0.1:7070", db="vision") as client:
            await client.put("blocks", "618000", {"height": 618000})
            rows = await client.query("FROM blocks LIMIT 5")

    Manual lifecycle:

        client = NedbClient("http://127.0.0.1:7070", db="vision")
        await client.open()
        try:
            await client.put("blocks", "1", {"height": 1})
        finally:
            await client.close()
    """

    def __init__(
        self,
        url: str = "http://127.0.0.1:7070",
        db: str = "default",
        token: str = "",
        auto_create: bool = True,
    ) -> None:
        self._base = url.rstrip("/")
        self._db = db
        self._token = token
        self._auto_create = auto_create
        self._read_client:  Optional[httpx.AsyncClient] = None
        self._write_client: Optional[httpx.AsyncClient] = None
        self._lock = asyncio.Lock()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def open(self) -> "NedbClient":
        """Open the underlying HTTP clients. Called automatically by __aenter__."""
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        self._read_client  = httpx.AsyncClient(base_url=self._base, headers=headers, timeout=_READ_TIMEOUT)
        self._write_client = httpx.AsyncClient(base_url=self._base, headers=headers, timeout=_WRITE_TIMEOUT)
        return self

    async def close(self) -> None:
        """Close the underlying HTTP clients. Called automatically by __aexit__."""
        if self._read_client:
            await self._read_client.aclose()
        if self._write_client:
            await self._write_client.aclose()

    async def __aenter__(self) -> "NedbClient":
        await self.open()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    def _rc(self) -> httpx.AsyncClient:
        if self._read_client is None:
            raise RuntimeError("NedbClient not open — use 'async with NedbClient(...) as c'")
        return self._read_client

    def _wc(self) -> httpx.AsyncClient:
        if self._write_client is None:
            raise RuntimeError("NedbClient not open — use 'async with NedbClient(...) as c'")
        return self._write_client

    # ── Internal HTTP helpers ─────────────────────────────────────────────────

    async def _raise(self, resp: httpx.Response) -> None:
        try:
            body = resp.json()
            msg = body.get("error", resp.text)
        except Exception:
            msg = resp.text
        raise NedbError(resp.status_code, msg)

    async def _query_raw(self, nql: str) -> Dict[str, Any]:
        resp = await self._rc().post(f"/v1/databases/{self._db}/query", json={"nql": nql})
        if resp.status_code in (400, 404):
            return {"rows": [], "count": 0, "seq": 0, "head": ""}
        if not resp.is_success:
            await self._raise(resp)
        return resp.json()

    async def _put_raw(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        wc = self._wc()
        resp = await wc.post(f"/v1/databases/{self._db}/put", json=payload)
        if resp.status_code == 404 and self._auto_create:
            # DB doesn't exist yet — create it and retry once
            cr = await wc.post("/v1/databases", json={"name": self._db})
            if not cr.is_success and cr.status_code != 409:
                await self._raise(cr)
            resp = await wc.post(f"/v1/databases/{self._db}/put", json=payload)
        if not resp.is_success:
            await self._raise(resp)
        return resp.json()

    # ── Core CRUD ─────────────────────────────────────────────────────────────

    async def put(
        self,
        coll: str,
        id: str,
        doc: Dict[str, Any],
        *,
        caused_by:  Optional[List[str]] = None,
        valid_from: Optional[str] = None,
        valid_to:   Optional[str] = None,
        evidence:   Optional[str] = None,
        confidence: Optional[float] = None,
        idem:       Optional[str] = None,
        nonce:      Optional[int] = None,
        client_id:  Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Write a document. Returns ``{"ok": True, "doc": {...}, "seq": N, "head": "..."}``.

        Parameters
        ----------
        coll       : Collection name (e.g. "blocks", "itsl_ops").
        id         : Document ID (must be unique within the collection).
        doc        : Arbitrary JSON-serialisable dict.
        caused_by  : List of object hashes that causally led to this write (DAG provenance).
        valid_from : Bi-temporal valid-from date (ISO 8601).
        valid_to   : Bi-temporal valid-to date (ISO 8601).
        evidence   : Human-readable provenance note.
        confidence : Confidence score 0–1.
        idem       : Idempotency key — duplicate puts with the same key are no-ops.
        nonce      : Replay-protection nonce (monotonically increasing per client_id).
        client_id  : Client identifier for replay protection.
        """
        payload: Dict[str, Any] = {"coll": coll, "id": id, "doc": doc}
        if caused_by  is not None: payload["caused_by"]  = caused_by
        if valid_from is not None: payload["valid_from"] = valid_from
        if valid_to   is not None: payload["valid_to"]   = valid_to
        if evidence   is not None: payload["evidence"]   = evidence
        if confidence is not None: payload["confidence"] = confidence
        if idem       is not None: payload["idem"]       = idem
        if nonce      is not None: payload["nonce"]      = nonce
        if client_id  is not None: payload["client"]     = client_id
        return await self._put_raw(payload)

    async def get(self, coll: str, id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch the current version of a document. Returns the doc dict or None.
        """
        result = await self._query_raw(f'FROM {coll} WHERE _id = "{id}" LIMIT 1')
        rows = result.get("rows", [])
        return rows[0] if rows else None

    async def delete(self, coll: str, id: str) -> bool:
        """
        Tombstone-delete a document.
        The object history is preserved in the DAG; the live id pointer is removed.
        Returns True if the document existed.
        """
        resp = await self._wc().delete(f"/v1/databases/{self._db}/rows/{coll}/{id}")
        if resp.status_code == 404:
            return False
        if not resp.is_success:
            await self._raise(resp)
        return resp.json().get("ok", False)

    async def query(self, nql: str) -> List[Dict[str, Any]]:
        """
        Run a NQL query. Returns a list of document dicts.

        NQL syntax::

            FROM <coll>
              [AS OF <seq>]
              [VALID AS OF "<date>"]
              [WHERE field = value [AND ...]]
              [ORDER BY field [DESC]]
              [LIMIT n]
              [GROUP BY field COUNT|SUM|AVG|MIN|MAX]
              [TRACE caused_by [REVERSE]]
              [SEARCH "text"]
        """
        result = await self._query_raw(nql)
        return result.get("rows", [])

    async def query_full(self, nql: str) -> Dict[str, Any]:
        """Like :meth:`query` but returns the full response including ``seq`` and ``head``."""
        return await self._query_raw(nql)

    # ── Batch ─────────────────────────────────────────────────────────────────

    async def batch(self, ops: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Run a batch of put/del operations atomically in a single HTTP round-trip.

        Each op is a dict with ``op`` ("put" or "del") and relevant fields::

            await client.batch([
                {"op": "put", "coll": "blocks", "id": "1", "doc": {"height": 1}},
                {"op": "del", "coll": "blocks", "id": "0"},
            ])
        """
        resp = await self._wc().post(f"/v1/databases/{self._db}/batch", json={"ops": ops})
        if resp.status_code == 404 and self._auto_create:
            cr = await self._wc().post("/v1/databases", json={"name": self._db})
            if not cr.is_success and cr.status_code != 409:
                await self._raise(cr)
            resp = await self._wc().post(f"/v1/databases/{self._db}/batch", json={"ops": ops})
        if not resp.is_success:
            await self._raise(resp)
        return resp.json().get("results", [])

    # ── Indexes ───────────────────────────────────────────────────────────────

    async def create_index(self, coll: str, field: str, kind: str = "sorted") -> Dict[str, Any]:
        """
        Create a sorted index on ``(coll, field)`` for fast ORDER BY queries.

        Parameters
        ----------
        kind : "sorted" (default) or "eq"
        """
        resp = await self._wc().post(
            f"/v1/databases/{self._db}/index",
            json={"coll": coll, "field": field, "kind": kind},
        )
        if not resp.is_success:
            await self._raise(resp)
        return resp.json()

    # ── Integrity ─────────────────────────────────────────────────────────────

    async def verify(self) -> Dict[str, Any]:
        """
        Run a full BLAKE2b tamper-evidence check over all objects.
        Returns ``{"ok": True, "objects_checked": N, "tampered": [], "head": "..."}``.
        """
        resp = await self._rc().get(f"/v1/databases/{self._db}/verify")
        if not resp.is_success:
            await self._raise(resp)
        return resp.json()

    async def head(self) -> str:
        """Return the current BLAKE2b Merkle head of the database."""
        resp = await self._rc().get(f"/v1/databases/{self._db}")
        if not resp.is_success:
            await self._raise(resp)
        return resp.json().get("head", "")

    async def seq(self) -> int:
        """Return the current global sequence number."""
        resp = await self._rc().get(f"/v1/databases/{self._db}")
        if not resp.is_success:
            await self._raise(resp)
        return resp.json().get("seq", 0)

    async def checkpoint(self) -> Dict[str, Any]:
        """Trigger an explicit checkpoint (no-op on v2 DAG — always snapshotted)."""
        resp = await self._wc().post(f"/v1/databases/{self._db}/checkpoint")
        if not resp.is_success:
            await self._raise(resp)
        return resp.json()

    async def log(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Return the last ``limit`` write operations."""
        resp = await self._rc().get(f"/v1/databases/{self._db}/log", params={"limit": limit})
        if not resp.is_success:
            await self._raise(resp)
        return resp.json().get("log", [])

    # ── Server ────────────────────────────────────────────────────────────────

    async def health(self) -> Dict[str, Any]:
        """Ping the server. Returns ``{"ok": True, "version": "...", ...}``."""
        resp = await self._rc().get("/health")
        if not resp.is_success:
            await self._raise(resp)
        return resp.json()

    async def ping(self) -> bool:
        """Returns True if the server is reachable and healthy."""
        try:
            result = await self.health()
            return bool(result.get("ok"))
        except Exception:
            return False

    async def list_databases(self) -> List[str]:
        """Return a list of all database names on this server."""
        resp = await self._rc().get("/v1/databases")
        if not resp.is_success:
            await self._raise(resp)
        return [d["name"] for d in resp.json().get("databases", [])]

    async def create_database(self) -> Dict[str, Any]:
        """Explicitly create the database. Idempotent."""
        resp = await self._wc().post("/v1/databases", json={"name": self._db})
        if resp.status_code == 409:
            return {"database": {"name": self._db}}  # already exists
        if not resp.is_success:
            await self._raise(resp)
        return resp.json()

    async def drop_database(self) -> bool:
        """Drop the database and all its data. Irreversible."""
        resp = await self._wc().delete(f"/v1/databases/{self._db}")
        if not resp.is_success:
            await self._raise(resp)
        return resp.json().get("dropped", False)

    # ── Repr ──────────────────────────────────────────────────────────────────

    def __repr__(self) -> str:
        return f"NedbClient(url={self._base!r}, db={self._db!r})"
