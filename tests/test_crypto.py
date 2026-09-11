# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""Tests for AES-256-GCM at-rest encryption."""
import os, sys, shutil, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"))

from nedb import NEDB
from nedb.crypto import (derive_key, wrap_dek, unwrap_dek, encrypt_bytes,
                          decrypt_bytes, aof_encode, aof_decode,
                          snapshot_encode, snapshot_decode)

PASS = FAIL = 0
def check(name, cond):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  ok  {name}")
    else:    FAIL += 1; print(f"  FAIL {name}")

TMK  = os.urandom(32)
TMK2 = os.urandom(32)   # for key rotation

print("\n── Crypto primitives ──")

def test_encrypt_decrypt():
    dek = os.urandom(32)
    pt = b"hello NEDB encryption"
    ct = encrypt_bytes(pt, dek)
    check("encrypt changes plaintext",  ct != pt)
    check("decrypt recovers plaintext", decrypt_bytes(ct, dek) == pt)

def test_tamper_detected():
    dek = os.urandom(32)
    ct = bytearray(encrypt_bytes(b"secret", dek))
    ct[-1] ^= 0xFF   # flip a bit in the GCM tag
    try:
        decrypt_bytes(bytes(ct), dek)
        check("tamper raises", False)
    except Exception:
        check("tamper raises", True)

def test_hkdf_any_length():
    k1 = derive_key(b"short")
    k2 = derive_key(b"short")
    k3 = derive_key(b"different")
    check("HKDF deterministic",   k1 == k2)
    check("HKDF different inputs", k1 != k3)
    check("HKDF 32 bytes",        len(k1) == 32)

def test_dek_wrap_unwrap():
    dek = os.urandom(32)
    wrapped = wrap_dek(dek, TMK)
    check("wrap produces dict",    isinstance(wrapped, dict))
    check("unwrap recovers DEK",   unwrap_dek(wrapped, TMK) == dek)
    try:
        unwrap_dek(wrapped, TMK2)
        check("wrong TMK raises", False)
    except Exception:
        check("wrong TMK raises", True)

def test_aof_roundtrip():
    dek = os.urandom(32)
    op = '{"seq":1,"op":"put","payload":{"k":"v"}}'
    enc = aof_encode(op, dek)
    check("aof encoded != plain", enc != op)
    dec = aof_decode(enc, dek)
    check("aof decoded = original", dec == op)
    check("aof no-dek passthrough", aof_encode(op, None) == op)

def test_snapshot_roundtrip():
    dek = os.urandom(32)
    data = b'{"test":true,"v":42}'
    enc = snapshot_encode(data, dek)
    check("snapshot encoded != plain", enc != data)
    dec = snapshot_decode(enc, dek)
    check("snapshot decoded = original", dec == data)
    check("snapshot no-dek passthrough", snapshot_encode(data, None) == data)

test_encrypt_decrypt(); test_tamper_detected(); test_hkdf_any_length()
test_dek_wrap_unwrap(); test_aof_roundtrip(); test_snapshot_roundtrip()

print("\n── Encrypted NEDB (durable) ──")

def test_encrypted_database():
    d = tempfile.mkdtemp()
    try:
        db = NEDB(d, tmk=TMK)
        check("key.enc created", os.path.exists(os.path.join(d, "key.enc")))
        db.create_index("users", "status", "eq")
        db.put("users", "alice", {"name": "Alice", "age": 31, "status": "active"})
        db.put("users", "bob",   {"name": "Bob",   "age": 25, "status": "active"})
        snap = db.seq
        db.put("users", "alice", {"name": "Alice", "age": 32, "status": "active"})
        db.link("users:alice", "follows", "users:bob")
        check("data readable live", db.get("users", "alice")["age"] == 32)
        check("time-travel live",   db.get("users", "alice", as_of=snap)["age"] == 31)
        check("query live",         len(db.query('FROM users WHERE status = "active"')) == 2)
        check("verify live",        db.verify())
        head = db.head
        db.close()

        # AOF on disk must be encrypted (not plain JSON)
        with open(os.path.join(d, "log.aof")) as f:
            first_line = f.readline().strip()
        check("AOF is encrypted on disk", '"enc":1' in first_line or first_line.startswith('{"enc"'))

        # Reopen WITH correct TMK
        db2 = NEDB(d, tmk=TMK)
        check("reopen: verify",       db2.verify())
        check("reopen: head matches", db2.head == head)
        check("reopen: data",         db2.get("users", "alice")["age"] == 32)
        check("reopen: time-travel",  db2.get("users", "alice", as_of=snap)["age"] == 31)
        check("reopen: query",        len(db2.query('FROM users WHERE status = "active"')) == 2)
        check("reopen: relations",    db2.neighbors("users:alice", "follows") == ["users:bob"])
        db2.close()

        # Reopen WITHOUT TMK must fail or return encrypted garbage
        try:
            db3 = NEDB(d)   # no TMK
            db3.get("users", "alice")
            # If it silently opens but can't decrypt, data should be None or garbled
            val = db3.get("users", "alice")
            check("no-TMK: cannot read plaintext", val is None or not isinstance(val, dict) or "name" not in val)
            db3.close()
        except Exception:
            check("no-TMK: cannot read plaintext", True)

    finally:
        shutil.rmtree(d, ignore_errors=True)

def test_encrypted_checkpoint_restart():
    d = tempfile.mkdtemp()
    try:
        db = NEDB(d, tmk=TMK)
        v1 = db.put_file("doc.txt", b"hello world " * 500)
        db.put("k", "1", {"v": 42})
        root_before = db.file_root("doc.txt", v1)
        db.checkpoint()
        head = db.head
        db.close()

        # snapshot.json should be encrypted
        with open(os.path.join(d, "snapshot.json"), "rb") as f:
            snap_raw = f.read()
        check("snapshot.json encrypted", b'"enc":1' in snap_raw)

        # Reopen — loads from encrypted snapshot
        db2 = NEDB(d, tmk=TMK)
        check("restart: verify",        db2.verify())
        check("restart: head",          db2.head == head)
        check("restart: data",          db2.get("k", "1")["v"] == 42)
        check("restart: file intact",   db2.get_file("doc.txt", v1) == b"hello world " * 500)
        check("restart: root matches",  db2.file_root("doc.txt", v1) == root_before)
        db2.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)

def test_key_rotation():
    d = tempfile.mkdtemp()
    try:
        db = NEDB(d, tmk=TMK)
        db.put("k", "1", {"secret": "data"})
        db.close()

        # Rotate to TMK2
        db2 = NEDB(d, tmk=TMK)
        db2.rewrap_key(TMK, TMK2)
        db2.close()

        # Old TMK must now fail
        try:
            db3 = NEDB(d, tmk=TMK)
            db3.get("k", "1")
            check("old TMK rejected after rotation", False)
        except Exception:
            check("old TMK rejected after rotation", True)

        # New TMK works
        db4 = NEDB(d, tmk=TMK2)
        check("new TMK accepted",   db4.get("k", "1")["secret"] == "data")
        check("new TMK: verify",    db4.verify())
        db4.close()
    finally:
        shutil.rmtree(d, ignore_errors=True)

def test_env_tmk():
    import os
    d = tempfile.mkdtemp()
    try:
        tmk_hex = os.urandom(32).hex()
        os.environ["NEDB_TMK"] = tmk_hex
        db = NEDB(d)   # no explicit tmk — reads from env
        db.put("k", "1", {"v": 99})
        db.close()
        db2 = NEDB(d)
        check("NEDB_TMK env: data readable", db2.get("k", "1")["v"] == 99)
        db2.close()
    finally:
        del os.environ["NEDB_TMK"]
        shutil.rmtree(d, ignore_errors=True)

test_encrypted_database()
test_encrypted_checkpoint_restart()
test_key_rotation()
test_env_tmk()

print(f"\nCrypto: {PASS} passed, {FAIL} failed {'✅' if not FAIL else '❌'}")
sys.exit(1 if FAIL else 0)
