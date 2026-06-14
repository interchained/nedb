"""
nedbd — the NEDB server daemon.

Runs the NEDB engine as a long-lived process behind an HTTP/JSON API, so clients
(NEDB Studio, apps, scripts) connect over a URL instead of embedding the engine —
the way you run Redis or Postgres. Each named database is a durable ``NEDB(path)``
(append-only log on disk, fsync'd) held open in memory for fast queries; the engine
owns the log/MVCC/time-travel/integrity.

Config (env):
  NEDBD_HOST    bind host            (default 127.0.0.1)
  NEDBD_PORT    bind port            (default 7070)
  NEDBD_DATA    data root directory  (default ./nedb-data)
  NEDBD_TOKEN   bearer token         (optional; if set, every /v1 route requires it)

Run:
  nedbd                 # console script (pip install nedb-engine)
  python -m nedb.server

HTTP API (all JSON):
  GET    /health
  GET    /v1/databases
  POST   /v1/databases                         {name, init?: {indexes, seed, links}}
  GET    /v1/databases/<name>
  DELETE /v1/databases/<name>
  POST   /v1/databases/<name>/query            {nql}
  POST   /v1/databases/<name>/put              {coll, id, doc, client?, nonce?, idem?}
  POST   /v1/databases/<name>/index            {coll, field, kind}
  POST   /v1/databases/<name>/link             {frm, rel, to}
  DELETE /v1/databases/<name>/rows/<coll>/<id>
  GET    /v1/databases/<name>/verify
  GET    /v1/databases/<name>/log?limit=N
"""
from __future__ import annotations

import json
import os
import re
import shutil
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs

from . import __version__
from .engine import NEDB
from .log import ReplayError

NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")


class HttpError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class Manager:
    """Owns the set of durable databases under a data root."""

    def __init__(self, root: str):
        self.root = root
        os.makedirs(root, exist_ok=True)
        self._open: Dict[str, NEDB] = {}
        # Resolve TMK once at startup — applies to all databases opened by this daemon.
        from .crypto import resolve_tmk
        self._tmk = resolve_tmk()  # reads NEDB_TMK / NEDB_TMK_FILE env; None if unset
        if self._tmk is not None:
            print("  encryption: AES-256-GCM enabled (NEDB_TMK configured)")

    def _path(self, name: str) -> str:
        return os.path.join(self.root, name)

    def _valid(self, name: str) -> None:
        if not NAME_RE.fullmatch(name):
            raise HttpError(400, f"invalid database name: {name!r}")

    def exists(self, name: str) -> bool:
        if name in self._open:
            return True
        p = self._path(name)
        return os.path.exists(os.path.join(p, "log.aof")) or os.path.exists(os.path.join(p, "meta.json"))

    def open(self, name: str) -> NEDB:
        self._valid(name)
        if name not in self._open:
            snap_path = os.path.join(self._path(name), "snapshot.json")
            had_snap = os.path.exists(snap_path)
            # Pass the manager-level TMK so every database is encrypted consistently.
            db = NEDB(self._path(name), tmk=self._tmk)
            if had_snap:
                print(f"  [{name}] loaded from snapshot (seq={db.seq})")
            self._open[name] = db
        return self._open[name]

    def require(self, name: str) -> NEDB:
        self._valid(name)
        if not self.exists(name):
            raise HttpError(404, f"database not found: {name}")
        return self.open(name)

    def create(self, name: str, init: Optional[dict]) -> dict:
        self._valid(name)
        if self.exists(name):
            raise HttpError(409, f"database already exists: {name}")
        db = self.open(name)
        init = init or {}
        for spec in init.get("indexes", []):
            coll, field, kind = spec[0], spec[1], (spec[2] if len(spec) > 2 else "eq")
            db.create_index(coll, field, kind)
        for coll, docs in (init.get("seed") or {}).items():
            for i, doc in enumerate(docs):
                rid = str(doc.get("_id") or doc.get("id") or f"{coll}-{i + 1}")
                db.put(coll, rid, dict(doc))
        for link in init.get("links", []):
            db.link(link[0], link[1], link[2])
        return self.summary(name)

    def drop(self, name: str) -> bool:
        self._valid(name)
        if not self.exists(name):
            return False
        if name in self._open:
            self._open[name].close()
            del self._open[name]
        shutil.rmtree(self._path(name), ignore_errors=True)
        return True

    def names(self) -> List[str]:
        found = set(self._open)
        if os.path.isdir(self.root):
            for entry in os.listdir(self.root):
                p = os.path.join(self.root, entry)
                if os.path.isdir(p) and (
                    os.path.exists(os.path.join(p, "log.aof")) or os.path.exists(os.path.join(p, "meta.json"))
                ):
                    found.add(entry)
        return sorted(found)

    @staticmethod
    def collection_counts(db: NEDB) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for key in db.store.keys(""):
            coll = key.split(":", 1)[0]
            counts[coll] = counts.get(coll, 0) + 1
        return counts

    def summary(self, name: str) -> dict:
        db = self.require(name)
        counts = self.collection_counts(db)
        return {
            "name": name,
            "seq": db.seq,
            "head": db.head,
            "rows": sum(counts.values()),
            "collections": counts,
        }

    def checkpoint_all(self) -> Dict[str, str]:
        """Checkpoint every open database — call before shutdown."""
        heads: Dict[str, str] = {}
        for name, db in self._open.items():
            try:
                head = db.checkpoint()
                heads[name] = head
                print(f"  [{name}] checkpoint saved  head={head[:12]}…  seq={db.seq}")
            except Exception as e:  # noqa: BLE001
                print(f"  [{name}] checkpoint failed: {e}")
        return heads

    def close_all(self) -> None:
        # Checkpoint each database before closing so the next startup is O(delta).
        self.checkpoint_all()
        for db in self._open.values():
            db.close()
        self._open.clear()


def make_handler(manager: Manager, token: Optional[str]):
    class Handler(BaseHTTPRequestHandler):
        server_version = f"nedbd/{__version__}"
        protocol_version = "HTTP/1.1"

        # ── helpers ──────────────────────────────────────────────────────────
        def _cors(self) -> None:
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")

        def _send(self, status: int, obj: Any) -> None:
            body = json.dumps(obj).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self._cors()
            self.end_headers()
            self.wfile.write(body)

        def _body(self) -> dict:
            n = int(self.headers.get("Content-Length", 0) or 0)
            if not n:
                return {}
            try:
                return json.loads(self.rfile.read(n) or b"{}")
            except Exception:
                raise HttpError(400, "invalid JSON body")

        def _auth(self) -> None:
            if not token:
                return
            got = self.headers.get("Authorization", "")
            if got != f"Bearer {token}":
                raise HttpError(401, "missing or invalid bearer token")

        def log_message(self, fmt: str, *args: Any) -> None:  # quieter logs
            return

        # ── dispatch ─────────────────────────────────────────────────────────
        def _parts(self):
            u = urlparse(self.path)
            return [p for p in u.path.split("/") if p], parse_qs(u.query)

        def do_OPTIONS(self) -> None:
            self.send_response(204)
            self._cors()
            self.end_headers()

        def do_GET(self) -> None:
            self._handle("GET")

        def do_POST(self) -> None:
            self._handle("POST")

        def do_DELETE(self) -> None:
            self._handle("DELETE")

        def _handle(self, method: str) -> None:
            try:
                parts, query = self._parts()

                if method == "GET" and (not parts or parts == ["health"]):
                    self._send(200, {"ok": True, "service": "nedbd", "version": __version__,
                                     "databases": manager.names(),
                                     "encrypted": manager._tmk is not None})
                    return

                # everything under /v1 requires auth (if a token is configured)
                if parts[:1] == ["v1"]:
                    self._auth()

                if parts == ["v1", "databases"]:
                    if method == "GET":
                        self._send(200, {"databases": [manager.summary(n) for n in manager.names()]})
                        return
                    if method == "POST":
                        b = self._body()
                        name = str(b.get("name", "")).strip()
                        if not name:
                            raise HttpError(400, "name is required")
                        self._send(201, {"database": manager.create(name, b.get("init"))})
                        return

                if len(parts) == 3 and parts[:2] == ["v1", "databases"]:
                    name = parts[2]
                    if method == "GET":
                        self._send(200, self._detail(name))
                        return
                    if method == "DELETE":
                        self._send(200, {"dropped": manager.drop(name)})
                        return

                if len(parts) == 4 and parts[:2] == ["v1", "databases"]:
                    name, action = parts[2], parts[3]
                    db = manager.require(name)
                    if method == "POST" and action == "query":
                        nql = str(self._body().get("nql", "")).strip()
                        if not nql:
                            raise HttpError(400, "nql is required")
                        try:
                            rows = db.query(nql)
                        except Exception as e:  # noqa: BLE001
                            raise HttpError(400, f"NQL error: {e}")
                        self._send(200, {"rows": rows, "count": len(rows), "seq": db.seq, "head": db.head})
                        return
                    if method == "POST" and action == "put":
                        b = self._body()
                        coll, rid, doc = b.get("coll"), b.get("id"), b.get("doc")
                        if not coll or rid is None or not isinstance(doc, dict):
                            raise HttpError(400, "coll, id, and doc are required")
                        kw = {k: b[k] for k in ("client", "nonce", "idem") if b.get(k) is not None}
                        try:
                            stored = db.put(str(coll), str(rid), dict(doc), **kw)
                        except ReplayError as e:
                            raise HttpError(409, str(e))
                        self._send(200, {"ok": True, "doc": stored, "seq": db.seq, "head": db.head})
                        return
                    if method == "POST" and action == "index":
                        b = self._body()
                        if not b.get("coll") or not b.get("field"):
                            raise HttpError(400, "coll and field are required")
                        db.create_index(str(b["coll"]), str(b["field"]), str(b.get("kind", "eq")))
                        self._send(200, {"ok": True})
                        return
                    if method == "POST" and action == "link":
                        b = self._body()
                        if not (b.get("frm") and b.get("rel") and b.get("to")):
                            raise HttpError(400, "frm, rel, and to are required")
                        db.link(str(b["frm"]), str(b["rel"]), str(b["to"]))
                        self._send(200, {"ok": True, "seq": db.seq, "head": db.head})
                        return
                    if method == "GET" and action == "verify":
                        self._send(200, {"ok": db.verify(), "seq": db.seq, "head": db.head})
                        return
                    if method == "POST" and action == "checkpoint":
                        head = db.checkpoint()
                        self._send(200, {"ok": True, "head": head, "seq": db.seq})
                        return
                    if method == "GET" and action == "log":
                        limit = int(query.get("limit", ["50"])[0])
                        ops = [o.to_dict() for o in db.log.ops[-limit:]][::-1]
                        self._send(200, {"log": ops, "seq": db.seq, "head": db.head})
                        return

                # DELETE /v1/databases/<name>/rows/<coll>/<id>
                if method == "DELETE" and len(parts) == 6 and parts[:2] == ["v1", "databases"] and parts[3] == "rows":
                    db = manager.require(parts[2])
                    db.delete(parts[4], parts[5])
                    self._send(200, {"ok": True, "seq": db.seq, "head": db.head})
                    return

                raise HttpError(404, "no such route")
            except HttpError as e:
                self._send(e.status, {"error": e.message})
            except Exception as e:  # noqa: BLE001
                self._send(500, {"error": str(e)})

        def _detail(self, name: str) -> dict:
            db = manager.require(name)
            counts = Manager.collection_counts(db)
            snap_path = os.path.join(manager._path(name), "snapshot.json")
            return {
                "name": name,
                "seq": db.seq,
                "head": db.head,
                "rows": sum(counts.values()),
                "collections": counts,
                "indexes": [list(t) for t in db.indexes.config],
                "integrity": {"ok": db.verify()},
                "encrypted": db._dek is not None,
                "has_snapshot": os.path.exists(snap_path),
            }

    return Handler


def main() -> None:
    host = os.environ.get("NEDBD_HOST", "127.0.0.1")
    port = int(os.environ.get("NEDBD_PORT", "7070"))
    data = os.environ.get("NEDBD_DATA", "./nedb-data")
    token = os.environ.get("NEDBD_TOKEN") or None

    import signal
    import threading

    manager = Manager(data)
    httpd = ThreadingHTTPServer((host, port), make_handler(manager, token))
    auth = "on" if token else "off"
    print(f"nedbd {__version__} — http://{host}:{port}  data={os.path.abspath(data)}  auth={auth}")
    # Eagerly open all known databases so backfill-encrypt (if needed) runs
    # immediately and is visible in the boot log, not hidden on first request.
    names = manager.names()
    for name in names:
        try:
            manager.open(name)
        except Exception as e:
            print(f"  [{name}] failed to open: {e}")
    print(f"  {len(names)} database(s): {', '.join(names) or '(none)'}")

    def _shutdown(signum, _frame):
        """SIGTERM / SIGINT — checkpoint all databases then exit cleanly.
        httpd.shutdown() MUST be called from a different thread than serve_forever()
        or it deadlocks; we spawn a daemon thread to do it."""
        sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
        n = len(manager._open)
        print(f"\nnedbd {sig_name} — checkpointing {n} database(s)…")
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT,  _shutdown)

    try:
        httpd.serve_forever()   # blocks; unblocked by _shutdown → httpd.shutdown()
    finally:
        httpd.server_close()
        manager.close_all()   # checkpoint → fsync → close every open database
        print("nedbd stopped cleanly.")


if __name__ == "__main__":
    main()
