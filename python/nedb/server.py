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
  POST   /v1/databases/<name>/files             {name, data_b64, tier?}  — store a file (Cascade-compressed)
  GET    /v1/databases/<name>/files/<filename>?version=N&tier=warm  — retrieve a file
  GET    /v1/databases/<name>/files/<filename>/root?version=N&tier=warm  — Merkle root (anchorable)
  POST   /v1/databases/<name>/checkpoint        — on-demand checkpoint
  GET    /v1/databases/<name>/batch             — batch writes (array of {op,coll,id,doc})
"""
from __future__ import annotations

import json
import os
import re
import shutil
import traceback as _tb
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs

from . import __version__
from .engine import NEDB
from .concurrent import Sequencer
from .log import ReplayError

NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")

# Log level — resolved at startup in main() from --log-level flag or NEDBD_DEBUG env.
# 0 = errors only (default), 1 = requests, 2 = deploy phases, 3 = everything.
# main() patches _log_level on this module after argument parsing.
_log_level: int = 3 if os.environ.get("NEDBD_DEBUG", "").strip() in ("1", "true", "yes") else 0

def _log(msg: str, level: int = 3) -> None:
    """Print if current log level >= required level."""
    if _log_level >= level:
        print(msg, flush=True)


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

    def open(self, name: str) -> Sequencer:
        self._valid(name)
        if name not in self._open:
            snap_path = os.path.join(self._path(name), "snapshot.json")
            had_snap = os.path.exists(snap_path)
            # Pass the manager-level TMK so every database is encrypted consistently.
            db = NEDB(self._path(name), tmk=self._tmk)
            if had_snap:
                print(f"  [{name}] loaded from snapshot (seq={db.seq})")
            # Wrap every database in a single-writer, group-commit Sequencer so the
            # threaded daemon can serve concurrent clients safely AND fast: parallel
            # reads, batched durable writes, one correct chain. No global lock.
            self._open[name] = Sequencer(db)
        return self._open[name]

    def require(self, name: str) -> Sequencer:
        self._valid(name)
        if not self.exists(name):
            raise HttpError(404, f"database not found: {name}")
        return self.open(name)

    def create(self, name: str, init: Optional[dict]) -> dict:
        self._valid(name)
        if self.exists(name):
            raise HttpError(409, f"database already exists: {name}")
        _log(f"  [nedbd] creating db '{name}'…", level=2)
        db = self.open(name)
        init = init or {}
        for spec in init.get("indexes", []):
            coll, field, kind = spec[0], spec[1], (spec[2] if len(spec) > 2 else "eq")
            _log(f"  [nedbd]   index  {coll}.{field} ({kind})", level=3)
            db.create_index(coll, field, kind)
        for coll, docs in (init.get("seed") or {}).items():
            _log(f"  [nedbd]   seed   {coll} × {len(docs)} rows", level=2)
            for i, doc in enumerate(docs):
                rid = str(doc.get("_id") or doc.get("id") or f"{coll}-{i + 1}")
                db.put(coll, rid, dict(doc))
        links = init.get("links", [])
        if links:
            _log(f"  [nedbd]   links  {len(links)}", level=2)
        for link in links:
            db.link(link[0], link[1], link[2])
        _log(f"  [nedbd] db '{name}' created  seq={db.seq}", level=2)
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
            _log(f"  [nedbd] {method} {self.path}", level=1)
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
                        init = b.get("init") or {}
                        if not name:
                            raise HttpError(400, "name is required")
                        n_idx  = len(init.get("indexes", []))
                        n_seed = sum(len(v) for v in (init.get("seed") or {}).values())
                        n_lnk  = len(init.get("links", []))
                        _log(f"  [nedbd] deploy '{name}' — {n_idx} indexes, {n_seed} seed rows, {n_lnk} links", level=2)
                        self._send(201, {"database": manager.create(name, init)})
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
                        _scalar = ("client", "nonce", "idem", "evidence", "confidence",
                                   "valid_from", "valid_to")
                        kw = {k: b[k] for k in _scalar if b.get(k) is not None}
                        if b.get("caused_by") is not None:
                            kw["caused_by"] = list(b["caused_by"])
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
                    # MongoDB-compatible query endpoint
                    # POST /v1/databases/<name>/mongo
                    # Body: {collection, op, ...op-specific args}
                    # op can be: find findOne count aggregate insertOne updateOne updateMany
                    #            deleteOne deleteMany distinct replaceOne
                    if method == "POST" and action == "mongo":
                        from .mongo import MongoCompat, MongoError, MongoUnsupportedError
                        b = self._body()
                        coll_name = b.get("collection")
                        op = b.get("op")
                        if not coll_name or not op:
                            raise HttpError(400, "collection and op are required")
                        mc = MongoCompat(db.db if isinstance(db, __import__("nedb.concurrent", fromlist=["Sequencer"]).Sequencer) else db)
                        coll = mc[str(coll_name)]
                        try:
                            result: dict = {}
                            if op == "find":
                                filt = b.get("filter") or {}
                                sort = b.get("sort")
                                limit = b.get("limit")
                                skip = b.get("skip", 0)
                                cur = coll.find(filt)
                                if sort:
                                    cur = cur.sort(list(sort.items()) if isinstance(sort, dict) else sort)
                                if skip: cur = cur.skip(int(skip))
                                if limit: cur = cur.limit(int(limit))
                                rows = cur.to_list()
                                result = {"rows": rows, "count": len(rows)}
                            elif op == "findOne":
                                doc = coll.find_one(b.get("filter") or {})
                                result = {"doc": doc}
                            elif op == "count":
                                result = {"count": coll.count_documents(b.get("filter") or {})}
                            elif op == "distinct":
                                result = {"values": coll.distinct(str(b.get("key", "_id")), b.get("filter"))}
                            elif op == "aggregate":
                                pipeline = b.get("pipeline") or []
                                result = {"rows": coll.aggregate(pipeline), "count": len(coll.aggregate(pipeline))}
                            elif op == "insertOne":
                                r = coll.insert_one(dict(b.get("document") or {}))
                                result = {"insertedId": r.inserted_id, "acknowledged": r.acknowledged}
                            elif op == "updateOne":
                                r = coll.update_one(b.get("filter") or {}, b.get("update") or {}, upsert=bool(b.get("upsert")))
                                result = {"matchedCount": r.matched_count, "modifiedCount": r.modified_count, "upsertedId": r.upserted_id}
                            elif op == "updateMany":
                                r = coll.update_many(b.get("filter") or {}, b.get("update") or {}, upsert=bool(b.get("upsert")))
                                result = {"matchedCount": r.matched_count, "modifiedCount": r.modified_count, "upsertedId": r.upserted_id}
                            elif op == "deleteOne":
                                r = coll.delete_one(b.get("filter") or {})
                                result = {"deletedCount": r.deleted_count}
                            elif op == "deleteMany":
                                r = coll.delete_many(b.get("filter") or {})
                                result = {"deletedCount": r.deleted_count}
                            elif op == "replaceOne":
                                r = coll.replace_one(b.get("filter") or {}, b.get("replacement") or {}, upsert=bool(b.get("upsert")))
                                result = {"matchedCount": r.matched_count, "modifiedCount": r.modified_count, "upsertedId": r.upserted_id}
                            else:
                                raise HttpError(400, f"unknown mongo op: {op!r}")
                            result["seq"] = db.seq
                            self._send(200, result)
                        except (MongoError, MongoUnsupportedError) as e:
                            raise HttpError(400, str(e))
                        return

                    if method == "POST" and action == "files":
                        b = self._body()
                        fname = b.get("name")
                        data_b64 = b.get("data_b64")
                        if not fname or not data_b64:
                            raise HttpError(400, "name and data_b64 are required")
                        import base64 as _b64
                        data = _b64.b64decode(data_b64)
                        tier = str(b.get("tier", "warm"))
                        version = db.put_file(str(fname), data, tier=tier)
                        root = db.file_root(str(fname), version, tier=tier)
                        self._send(201, {"name": fname, "version": version, "root": root,
                                         "size": len(data), "tier": tier})
                        return

                # DELETE /v1/databases/<name>/rows/<coll>/<id>
                if method == "DELETE" and len(parts) == 6 and parts[:2] == ["v1", "databases"] and parts[3] == "rows":
                    db = manager.require(parts[2])
                    db.delete(parts[4], parts[5])
                    self._send(200, {"ok": True, "seq": db.seq, "head": db.head})
                    return

                # GET  /v1/databases/<name>/files/<filename>          → file bytes as base64
                # GET  /v1/databases/<name>/files/<filename>/root      → Merkle root hex
                if method == "GET" and len(parts) >= 5 and parts[:2] == ["v1", "databases"] and parts[3] == "files":
                    import base64 as _b64
                    db = manager.require(parts[2])
                    fname = parts[4]
                    version = int(query.get("version", ["-1"])[0])
                    tier = query.get("tier", ["warm"])[0]
                    # /root sub-resource
                    if len(parts) == 6 and parts[5] == "root":
                        root = db.file_root(fname, version, tier=tier)
                        self._send(200, {"name": fname, "version": version, "root": root, "tier": tier})
                        return
                    data = db.get_file(fname, version, tier=tier)
                    self._send(200, {"name": fname, "version": version,
                                     "data_b64": _b64.b64encode(data).decode(),
                                     "size": len(data), "tier": tier})
                    return

                # POST /v1/databases/<name>/batch  — multiple ops in one request
                # Body: {ops: [{op:"put"|"del"|"link", coll, id, doc?, frm?, rel?, to?}, ...]}
                if method == "POST" and len(parts) == 4 and parts[:2] == ["v1", "databases"] and parts[3] == "batch":
                    db = manager.require(parts[2])
                    b = self._body()
                    ops_list = b.get("ops") or []
                    if not isinstance(ops_list, list) or not ops_list:
                        raise HttpError(400, "ops array is required")
                    results = []
                    for op in ops_list:
                        kind = str(op.get("op", "put")).lower()
                        if kind == "put":
                            doc = db.put(str(op["coll"]), str(op["id"]), dict(op.get("doc") or {}))
                            results.append({"op": "put", "id": op["id"], "seq": db.seq})
                        elif kind == "del":
                            db.delete(str(op["coll"]), str(op["id"]))
                            results.append({"op": "del", "id": op["id"], "seq": db.seq})
                        elif kind == "link":
                            db.link(str(op["frm"]), str(op["rel"]), str(op["to"]))
                            results.append({"op": "link", "seq": db.seq})
                        else:
                            results.append({"op": kind, "error": "unknown op"})
                    self._send(200, {"results": results, "count": len(results),
                                     "seq": db.seq, "head": db.head})
                    return

                raise HttpError(404, "no such route")
            except HttpError as e:
                _log(f"  [nedbd] HTTP {e.status}: {e.message}", level=1)
                self._send(e.status, {"error": e.message})
            except Exception as e:  # noqa: BLE001
                # Always print errors (regardless of debug flag) so failures are visible.
                print(f"  [nedbd] ERROR {method} {self.path}: {e}", flush=True)
                _tb.print_exc()
                self._send(500, {"error": str(e), "trace": _tb.format_exc()})

        def _detail(self, name: str) -> dict:
            db = manager.require(name)
            counts = Manager.collection_counts(db)
            snap_path = os.path.join(manager._path(name), "snapshot.json")
            # Extract all unique relations from the adjacency list so the
            # studio graph can show edges without a prior schema document.
            relations = []
            seen = set()
            for (frm, rel), edges in db.relations._adj.items():
                frm_coll = frm.split(":", 1)[0] if ":" in frm else frm
                for to, added, removed in edges:
                    if removed is not None:
                        continue  # skip unlinked edges
                    to_coll = to.split(":", 1)[0] if ":" in to else to
                    key = (frm_coll, rel, to_coll)
                    if key not in seen:
                        seen.add(key)
                        relations.append({"from": frm_coll, "relation": rel,
                                          "to": to_coll, "cardinality": "one_to_many"})
            return {
                "name": name,
                "seq": db.seq,
                "head": db.head,
                "rows": sum(counts.values()),
                "collections": counts,
                "indexes": [list(t) for t in db.indexes.config],
                "relations": relations,
                "integrity": {"ok": db.verify()},
                "encrypted": db._dek is not None,
                "has_snapshot": os.path.exists(snap_path),
            }

    return Handler


def main() -> None:
    import argparse as _ap, signal, threading
    from .resp2 import make_resp2_server

    parser = _ap.ArgumentParser(
        prog="nedbd",
        description="NEDB server daemon — serves durable, time-traveling databases over HTTP/JSON.",
    )
    parser.add_argument("--host",       default=os.environ.get("NEDBD_HOST",  "127.0.0.1"))
    parser.add_argument("--port",       type=int, default=int(os.environ.get("NEDBD_PORT", "7070")))
    parser.add_argument("--data",       default=os.environ.get("NEDBD_DATA",  "./nedb-data"))
    parser.add_argument("--token",      default=os.environ.get("NEDBD_TOKEN") or None)
    parser.add_argument("--resp2-port", type=int, default=int(os.environ.get("NEDBD_RESP2_PORT", "0")))
    parser.add_argument(
        "--log-level", type=int, default=None, metavar="N",
        help="Verbosity: 0=errors only (default), 1=requests, 2=deploy phases, 3=everything. "
             "Env var NEDBD_DEBUG=1 is equivalent to --log-level 3.",
    )
    parser.add_argument(
        "--dag", action="store_true",
        default=os.environ.get("NEDBD_DAG", "").lower() in ("1", "true", "yes"),
        help="Run the v2 content-addressed DAG engine (Rust binary) instead of the v1 AOF engine. "
             "No AOF, no global lock, instant cold start. Env var NEDBD_DAG=1 also enables this.",
    )
    args = parser.parse_args()

    # ── DAG mode: exec into the Rust v2 binary, replacing this process entirely ──
    if args.dag:
        import shutil as _shutil, sys as _sys
        _pkg_dir = os.path.dirname(os.path.abspath(__file__))
        _candidates = [
            os.path.join(_pkg_dir, "nedbd-v2"),
            os.path.join(_pkg_dir, "nedbd_v2"),
            _shutil.which("nedbd-v2") or "",
            _shutil.which("nedbd_v2") or "",
        ]
        _bin = next((c for c in _candidates if c and os.path.isfile(c)), None)
        if _bin is None:
            print("nedbd --dag: v2 Rust binary not found.", file=_sys.stderr)
            print("  Expected 'nedbd-v2' alongside the package or in PATH.", file=_sys.stderr)
            print("  Build: cd rust/nedb-v2 && cargo build --release", file=_sys.stderr)
            print("  Copy:  cp rust/nedb-v2/target/release/nedbd nedbd-v2 && nedbd --dag", file=_sys.stderr)
            _sys.exit(1)
        os.environ["NEDBD_PORT"] = str(args.port)
        if args.token:
            os.environ["NEDBD_TOKEN"] = args.token
        # exec replaces this process — clean signal handling, no Python overhead
        os.execv(_bin, [_bin, args.data])

    # Resolve log level: CLI flag beats env var.
    # Level 0: only errors (always printed)
    # Level 1: every request method+path
    # Level 2: deploy phases (index/seed/link counts)
    # Level 3: all of the above — equivalent to old NEDBD_DEBUG=1
    _env_level = 3 if os.environ.get("NEDBD_DEBUG", "").strip() in ("1", "true", "yes") else 0
    _log_level: int = args.log_level if args.log_level is not None else _env_level

    # Patch the module-level _log and _DEBUG so the handler uses the resolved level.
    import nedb.server as _srv_mod  # noqa: PLC0415
    _srv_mod._log_level = _log_level  # type: ignore[attr-defined]

    host  = args.host
    port  = args.port
    data  = args.data
    token = args.token
    resp2_port = args.resp2_port
    manager = Manager(data)
    httpd = ThreadingHTTPServer((host, port), make_handler(manager, token))
    auth = "on" if token else "off"
    BANNER = f"""\
  ███╗   ██╗███████╗██████╗ ██████╗
  ████╗  ██║██╔════╝██╔══██╗██╔══██╗
  ██╔██╗ ██║█████╗  ██║  ██║██████╔╝
  ██║╚██╗██║██╔══╝  ██║  ██║██╔══██╗
  ██║ ╚████║███████╗██████╔╝██████╔╝
  ╚═╝  ╚═══╝╚══════╝╚═════╝ ╚═════╝

  a versioned, time-traveling, encrypted database
  ─────────────────────────────────────────────────
  INTERCHAINED, LLC    ×    Claude Sonnet 4.6
  interchained.org       hyperagent.com/refer/J2G6TCD7
"""
    print(BANNER)
    level_label = {0: "errors-only", 1: "requests", 2: "deploy", 3: "verbose"}.get(_log_level, str(_log_level))
    print(f"  nedbd {__version__} — http://{host}:{port}  data={os.path.abspath(data)}  auth={auth}  log={level_label}")
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

    # Optional hour-aligned periodic checkpoint.
    # NEDBD_CHECKPOINT_INTERVAL=60 means checkpoint every 60 minutes, on the hour mark.
    # The system clock handles the cadence — we sleep to the next :00 boundary, checkpoint,
    # then repeat. Default: disabled (0). Value is in minutes.
    ckpt_interval = int(os.environ.get("NEDBD_CHECKPOINT_INTERVAL", "0"))
    _stop_ckpt = threading.Event()

    if ckpt_interval > 0:
        def _checkpoint_loop():
            import time
            interval_s = ckpt_interval * 60
            while not _stop_ckpt.is_set():
                # Sleep until the next clock-aligned boundary
                now = time.time()
                secs_into_interval = now % interval_s
                sleep_s = interval_s - secs_into_interval
                # Wake slightly after the boundary so we always round up
                _stop_ckpt.wait(timeout=sleep_s + 0.5)
                if _stop_ckpt.is_set():
                    break
                ts = time.strftime("%Y-%m-%d %H:%M")
                n = len(manager._open)
                print(f"  [checkpoint:{ts}] checkpointing {n} database(s)…")
                manager.checkpoint_all()

        ckpt_thread = threading.Thread(target=_checkpoint_loop, daemon=True)
        ckpt_thread.start()
        print(f"  checkpoint — every {ckpt_interval}min on the clock mark (NEDBD_CHECKPOINT_INTERVAL)")

    # Optional RESP2 server (redis-cli / redis-benchmark compatible)
    resp2_srv = None
    if resp2_port > 0:
        resp2_srv = make_resp2_server(manager, host, resp2_port)
        t = threading.Thread(target=resp2_srv.serve_forever, daemon=True)
        t.start()
        print(f"  resp2  — redis://  {host}:{resp2_port}  (RESP2 wire protocol)")

    try:
        httpd.serve_forever()   # blocks; unblocked by _shutdown → httpd.shutdown()
    finally:
        _stop_ckpt.set()  # stop the checkpoint loop
        httpd.server_close()
        if resp2_srv:
            resp2_srv.shutdown()
        manager.close_all()   # checkpoint → fsync → close every open database
        print("nedbd stopped cleanly.")


if __name__ == "__main__":
    main()
