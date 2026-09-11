# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
nedb.snapshot — checkpoint persistence for NEDB.

A checkpoint is a point-in-time snapshot of the materialized state (store,
relations, index config, nonce/idem tables) that is *anchored* in the hash chain
by appending a ``checkpoint`` op to the AOF at the moment of capture.  The chain
runs continuously through the checkpoint op, so:

  genesis → ops → [checkpoint op] → delta ops…

``verify()`` still walks the full chain (the checkpoint op is a real op).  Loading
with a snapshot skips replaying ops before the checkpoint — O(delta) instead of
O(total).  Time-travel AS OF any seq >= 0 still works because delta ops carry all
version information for seqs after the checkpoint; pre-checkpoint time-travel is
not supported without the full AOF (users who need it should keep the full log).

Snapshot file:  ``<data_dir>/snapshot.json``
Format v1:
  {
    "version":         1,
    "seq":             <int — seq of the checkpoint op>,
    "head":            "<hex — head hash AFTER the checkpoint op>",
    "checkpoint_seq":  <int — same as seq>,
    "store": {
        "<key>": {"v": <value|null>, "seq": <int last-write-seq>}
    },
    "relations": {
        "<frm>|<rel>": [{"to": "<to>", "added": <int>, "removed": <int|null>}]
    },
    "index_config":    [[coll, field, kind], …],
    "nonces":          {"<client>": <int>},
    "idem":            {"<key>": <int>}
  }
"""
from __future__ import annotations

import json
import os
import time
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from .engine import NEDB

SNAP_FILE = "snapshot.json"
SNAP_VERSION = 1


def _snap_path(data_dir: str) -> str:
    return os.path.join(data_dir, SNAP_FILE)


def save_snapshot(db: "NEDB") -> str:
    """
    Capture a checkpoint.

    1. Append a ``checkpoint`` op to the AOF (anchors the snapshot in the chain).
    2. Serialize the current materialized state to ``snapshot.json``.

    Returns the head hash after the checkpoint op.
    """
    if db.path is None:
        raise ValueError("Snapshots require a durable NEDB(path) database.")

    # ── 1. Append the checkpoint op to the AOF so the chain runs through it ──
    op, _ = db._log_append(
        client="__snapshot__",
        nonce=int(time.time() * 1_000),   # millisecond-resolution unique nonce
        op="checkpoint",
        payload={"note": "snapshot captured"},
    )
    snap_seq  = op.seq
    snap_head = db.head

    # ── 2. Serialise store (HEAD values only — pre-checkpoint time-travel
    #       not supported; users who need it keep the full AOF)  ─────────────
    store_data: Dict[str, Any] = {}
    for key, versions in db.store._v.items():  # type: ignore[attr-defined]
        last_seq, last_val = versions[-1]
        from .store import TOMB
        store_data[key] = {
            "v":   None if last_val is TOMB else last_val,
            "seq": last_seq,
            "deleted": last_val is TOMB,
        }

    # ── 3. Serialise relations ────────────────────────────────────────────────
    rel_data: Dict[str, Any] = {}
    for (frm, rel), edges in db.relations._adj.items():  # type: ignore[attr-defined]
        rel_data[f"{frm}|{rel}"] = [
            {"to": to, "added": added, "removed": removed}
            for to, added, removed in edges
        ]

    # ── 4. Prepare BlobStore (actual encoding happens in step 5) ─────────────
    import base64
    blobs_data: Dict[str, Any] = {}

    # ── 5. Encrypt blob chunks if encryption is enabled ───────────────────────
    from . import crypto as _crypto
    dek = getattr(db, "_dek", None)
    for tier_name, bs in db.blobs.items():
        blobs_data[tier_name] = {
            "chunks": {
                h: base64.b64encode(
                    _crypto.chunk_encode(data, dek) if dek else data
                ).decode()
                for h, data in bs.chunks.items()
            },
            "files":  bs.files,
            "logical_bytes": bs.logical_bytes,
            "dedup_hits":    bs.dedup_hits,
        }

    # ── 6. Write snapshot.json (encrypted if DEK set) ─────────────────────────
    snap: Dict[str, Any] = {
        "version":        SNAP_VERSION,
        "seq":            snap_seq,
        "head":           snap_head,
        "checkpoint_seq": snap_seq,
        "store":          store_data,
        "relations":      rel_data,
        "index_config":   [list(t) for t in db.indexes.config],
        "nonces":         dict(db.log._last_nonce),
        "idem":           {k: v for k, v in db.log._idem.items()},
        "blobs":          blobs_data,
    }

    path = _snap_path(db.path)
    tmp  = path + ".tmp"
    raw = json.dumps(snap, separators=(",", ":")).encode()
    encoded = _crypto.snapshot_encode(raw, dek)
    with open(tmp, "wb") as fh:
        fh.write(encoded)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)   # atomic rename
    db.flush()               # ensure checkpoint op is on disk before we return
    return snap_head


def load_snapshot(db: "NEDB") -> int:
    """
    Rehydrate a ``NEDB(path)`` database from a snapshot.

    Called from ``NEDB._open`` before log replay when ``snapshot.json`` exists.
    Returns the checkpoint seq so the caller knows which AOF ops to replay.

    The chain integrity guarantee is preserved:
    - The snapshot's ``head`` is exactly the BLAKE2b state AFTER the checkpoint op.
    - When we later replay the delta AOF ops their ``prev_hash`` chains from that head.
    - ``verify()`` confirms the full chain (including the checkpoint op itself).
    """
    path = _snap_path(db.path)  # type: ignore[arg-type]
    if not os.path.exists(path):
        return -1   # no snapshot — full replay

    from . import crypto as _crypto
    dek = getattr(db, "_dek", None)
    with open(path, "rb") as fh:
        raw = fh.read()
    raw = _crypto.snapshot_decode(raw, dek)
    snap = json.loads(raw)

    if snap.get("version", 0) != SNAP_VERSION:
        return -1   # unknown format — fall back to full replay

    # ── Restore index config ──────────────────────────────────────────────────
    for coll, field, kind in snap.get("index_config", []):
        db.indexes.ensure(coll, field, kind)

    # ── Restore store (HEAD values only) ─────────────────────────────────────
    from .store import TOMB
    for key, entry in snap.get("store", {}).items():
        val  = None if entry.get("deleted") else entry["v"]
        sseq = entry["seq"]
        if val is None:
            db.store._v[key]    = [(sseq, TOMB)]
            db.store._seqs[key] = [sseq]
        else:
            db.store._v[key]    = [(sseq, val)]
            db.store._seqs[key] = [sseq]
            db.indexes.add(key.split(":", 1)[0] if ":" in key else key, key, val)

    # ── Restore relations ─────────────────────────────────────────────────────
    for key_str, edges in snap.get("relations", {}).items():
        frm, rel = key_str.split("|", 1)
        for e in edges:
            db.relations._adj.setdefault((frm, rel), []).append(
                [e["to"], e["added"], e.get("removed")]
            )
            db.relations._radj.setdefault((e["to"], rel), []).append(
                [frm, e["added"], e.get("removed")]
            )

    # ── Restore nonce / idem state ────────────────────────────────────────────
    db.log._last_nonce.update(snap.get("nonces", {}))
    db.log._idem.update({k: int(v) for k, v in snap.get("idem", {}).items()})
    db._nonce = dict(db.log._last_nonce)

    # ── Restore BlobStore — decrypt chunks if encryption enabled ─────────────
    import base64
    for tier_name, bs_data in snap.get("blobs", {}).items():
        if tier_name not in db.blobs:
            from .cascade import BlobStore
            db.blobs[tier_name] = BlobStore(tier_name)
        bs = db.blobs[tier_name]
        bs.chunks = {
            h: _crypto.chunk_decode(base64.b64decode(enc), dek)
            for h, enc in bs_data.get("chunks", {}).items()
        }
        bs.files         = bs_data.get("files", {})
        bs.logical_bytes = bs_data.get("logical_bytes", 0)
        bs.dedup_hits    = bs_data.get("dedup_hits", 0)

    return int(snap["checkpoint_seq"])
