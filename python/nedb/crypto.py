"""
nedb.crypto — AES-256-GCM encryption at rest with a double-envelope key structure.

Architecture
────────────
External TMK (Table Master Key)  ← provided by operator (env / arg / key file)
         ↓  AES-256-GCM wrap
       DEK  (Data Encryption Key)  ← random, per database, stored in key.enc
         ↓  AES-256-GCM encrypt
       Data  (AOF lines, snapshot.json, blob chunks)

Key rotation: supply a new TMK and call rewrap_dek(). The DEK (and therefore
all data) stays untouched — only key.enc is rewritten.

Toggle: if no TMK is configured (no arg, no env, no key file), every function
is a zero-overhead pass-through. Existing unencrypted databases work unchanged.

TMK sources (priority order):
  1. NEDB(path, tmk=<bytes>)              — programmatic
  2. NEDB_TMK=<64-char hex>               — environment variable
  3. NEDB_TMK_FILE=/path/to/keyfile       — raw bytes from a file
  4. (none)                               — encryption disabled

HKDF normalization: the TMK may be any length ≥ 16 bytes; it is always
stretched / compressed to exactly 32 bytes via HKDF-SHA256 before use, so
passphrases and key files of any size are accepted safely.

Backend: pycryptodome (primary, cross-platform, pre-built wheels for all OSes
including Windows MinGW — no cffi / C compiler required).  Falls back to
cryptography if pycryptodome is not available (backwards compatibility for
existing installations that already have cryptography).

Install:
    pip install nedb-engine[encryption]      # installs pycryptodome
"""
from __future__ import annotations

import base64
import json
import os
from typing import Optional

# ── Backend detection ────────────────────────────────────────────────────────
# pycryptodome is the primary backend: pre-built binary wheels for all
# platforms (Linux / macOS / Windows x86 / Windows arm64 / Windows MinGW)
# with no cffi dependency — installs everywhere without a C compiler.
_BACKEND: Optional[str] = None
_HAVE_CRYPTO = False

try:
    from Crypto.Cipher import AES as _PCD_AES             # type: ignore[import]
    from Crypto.Protocol.KDF import HKDF as _PCD_HKDF     # type: ignore[import]
    from Crypto.Hash import SHA256 as _PCD_SHA256          # type: ignore[import]
    _BACKEND      = "pycryptodome"
    _HAVE_CRYPTO  = True
except ImportError:
    pass

if not _HAVE_CRYPTO:
    # Fallback: cryptography (older installations / explicit [encryption] extra)
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM as _CG_AESGCM  # type: ignore[import]
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF as _CG_HKDF          # type: ignore[import]
        from cryptography.hazmat.primitives import hashes as _CG_hashes                # type: ignore[import]
        _BACKEND     = "cryptography"
        _HAVE_CRYPTO = True
    except ImportError:
        pass

KEY_LEN   = 32    # 256-bit
NONCE_LEN = 12    # 96-bit GCM nonce (standard recommendation)
TAG_LEN   = 16    # 128-bit GCM authentication tag

# Additional Authenticated Data tags — bind ciphertext to its purpose.
_AAD_DEK  = b"NEDB-DEK-v1"
_AAD_DATA = b"NEDB-data-v1"


def _require_crypto() -> None:
    if not _HAVE_CRYPTO:
        raise ImportError(
            "NEDB encryption at rest requires pycryptodome or cryptography.\n"
            "Install with:  pip install 'nedb-engine[encryption]'\n"
            "  (or:         pip install pycryptodome)"
        )


# ── Key derivation ────────────────────────────────────────────────────────────

def derive_key(material: bytes) -> bytes:
    """Normalise any-length key material to exactly 32 bytes via HKDF-SHA256."""
    _require_crypto()
    if _BACKEND == "pycryptodome":
        return _PCD_HKDF(
            master=material, key_len=KEY_LEN,
            salt=b"NEDB-hkdf-v1",
            hashmod=_PCD_SHA256,
            context=b"nedb-key",
        )
    else:
        h = _CG_HKDF(
            algorithm=_CG_hashes.SHA256(), length=KEY_LEN,
            salt=b"NEDB-hkdf-v1", info=b"nedb-key",
        )
        return h.derive(material)


def resolve_tmk(tmk_arg: Optional[bytes] = None) -> Optional[bytes]:
    """
    Return the 32-byte TMK to use, or None if encryption is not configured.
    Priority: explicit arg > NEDB_TMK env (hex) > NEDB_TMK_FILE env.
    """
    material: Optional[bytes] = None
    if tmk_arg is not None:
        material = tmk_arg
    elif os.environ.get("NEDB_TMK"):
        try:
            material = bytes.fromhex(os.environ["NEDB_TMK"])
        except ValueError as e:
            raise ValueError(f"NEDB_TMK is not valid hex: {e}") from e
    elif os.environ.get("NEDB_TMK_FILE"):
        with open(os.environ["NEDB_TMK_FILE"], "rb") as fh:
            material = fh.read().strip()
    if material is None:
        return None
    return derive_key(material)


# ── Low-level primitives ──────────────────────────────────────────────────────
# On-disk format: nonce‖ciphertext‖tag  (12 + len + 16 bytes)
# Both backends produce and consume the same byte layout for full compatibility
# with databases created by either backend.

def encrypt_bytes(plaintext: bytes, dek: bytes, aad: bytes = _AAD_DATA) -> bytes:
    """AES-256-GCM encrypt. Returns nonce‖ciphertext‖tag (12 + len + 16 bytes)."""
    _require_crypto()
    nonce = os.urandom(NONCE_LEN)
    if _BACKEND == "pycryptodome":
        cipher = _PCD_AES.new(dek, _PCD_AES.MODE_GCM, nonce=nonce)
        cipher.update(aad)
        ciphertext, tag = cipher.encrypt_and_digest(plaintext)
        return nonce + ciphertext + tag
    else:
        ct_with_tag = _CG_AESGCM(dek).encrypt(nonce, plaintext, aad)
        return nonce + ct_with_tag


def decrypt_bytes(data: bytes, dek: bytes, aad: bytes = _AAD_DATA) -> bytes:
    """AES-256-GCM decrypt. Raises ValueError / InvalidTag on tampering."""
    _require_crypto()
    nonce      = data[:NONCE_LEN]
    ciphertext = data[NONCE_LEN:-TAG_LEN]
    tag        = data[-TAG_LEN:]
    if _BACKEND == "pycryptodome":
        cipher = _PCD_AES.new(dek, _PCD_AES.MODE_GCM, nonce=nonce)
        cipher.update(aad)
        return cipher.decrypt_and_verify(ciphertext, tag)
    else:
        return _CG_AESGCM(dek).decrypt(nonce, ciphertext + tag, aad)


# ── DEK management ────────────────────────────────────────────────────────────

KEY_ENC_FILE = "key.enc"


def _key_enc_path(data_dir: str) -> str:
    return os.path.join(data_dir, KEY_ENC_FILE)


def generate_dek() -> bytes:
    """Generate a fresh random 256-bit Data Encryption Key."""
    return os.urandom(KEY_LEN)


def wrap_dek(dek: bytes, tmk: bytes) -> dict:
    """Encrypt the DEK with the TMK → a JSON-serialisable dict."""
    _require_crypto()
    nonce = os.urandom(NONCE_LEN)
    if _BACKEND == "pycryptodome":
        cipher = _PCD_AES.new(tmk, _PCD_AES.MODE_GCM, nonce=nonce)
        cipher.update(_AAD_DEK)
        ct, tag = cipher.encrypt_and_digest(dek)
        ct_with_tag = ct + tag
    else:
        ct_with_tag = _CG_AESGCM(tmk).encrypt(nonce, dek, _AAD_DEK)
    return {"v": 1, "alg": "AES-256-GCM", "n": nonce.hex(), "ct": ct_with_tag.hex()}


def unwrap_dek(wrapped: dict, tmk: bytes) -> bytes:
    """Decrypt the DEK using the TMK. Raises if the TMK is wrong or data tampered."""
    _require_crypto()
    nonce       = bytes.fromhex(wrapped["n"])
    ct_with_tag = bytes.fromhex(wrapped["ct"])
    ct          = ct_with_tag[:-TAG_LEN]
    tag         = ct_with_tag[-TAG_LEN:]
    if _BACKEND == "pycryptodome":
        cipher = _PCD_AES.new(tmk, _PCD_AES.MODE_GCM, nonce=nonce)
        cipher.update(_AAD_DEK)
        return cipher.decrypt_and_verify(ct, tag)
    else:
        return _CG_AESGCM(tmk).decrypt(nonce, ct_with_tag, _AAD_DEK)


def load_or_create_dek(data_dir: str, tmk: bytes) -> bytes:
    """
    Load and unwrap the DEK from key.enc, or generate a new one if the file
    does not yet exist (new encrypted database).
    """
    path = _key_enc_path(data_dir)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            wrapped = json.load(fh)
        return unwrap_dek(wrapped, tmk)
    dek = generate_dek()
    _save_wrapped_dek(data_dir, dek, tmk)
    return dek


def _save_wrapped_dek(data_dir: str, dek: bytes, tmk: bytes) -> None:
    path = _key_enc_path(data_dir)
    tmp  = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(wrap_dek(dek, tmk), fh)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def rewrap_dek(data_dir: str, old_tmk: bytes, new_tmk: bytes) -> None:
    """
    Key rotation: re-wrap the DEK under a new TMK without touching any data.
    After this call, the database will only open with new_tmk.
    """
    dek = load_or_create_dek(data_dir, old_tmk)
    _save_wrapped_dek(data_dir, dek, new_tmk)


# ── AOF line helpers ──────────────────────────────────────────────────────────

def aof_encode(op_json: str, dek: Optional[bytes]) -> str:
    if dek is None:
        return op_json
    ct = encrypt_bytes(op_json.encode(), dek)
    return json.dumps({"enc": 1, "ct": base64.b64encode(ct).decode()},
                      separators=(",", ":"))


def aof_decode(line: str, dek: Optional[bytes]) -> str:
    stripped = line.strip()
    if not stripped:
        return stripped
    if dek is not None:
        try:
            env = json.loads(stripped)
            if isinstance(env, dict) and env.get("enc") == 1:
                ct = base64.b64decode(env["ct"])
                return decrypt_bytes(ct, dek).decode()
        except Exception:
            pass
    return stripped


# ── Snapshot helpers ──────────────────────────────────────────────────────────

def snapshot_encode(content: bytes, dek: Optional[bytes]) -> bytes:
    if dek is None:
        return content
    ct = encrypt_bytes(content, dek)
    return json.dumps({"enc": 1, "ct": base64.b64encode(ct).decode()},
                      separators=(",", ":")).encode()


def snapshot_decode(raw: bytes, dek: Optional[bytes]) -> bytes:
    if dek is None:
        return raw
    try:
        env = json.loads(raw)
        if isinstance(env, dict) and env.get("enc") == 1:
            ct = base64.b64decode(env["ct"])
            return decrypt_bytes(ct, dek)
    except Exception:
        pass
    return raw


# ── BlobStore chunk helpers ───────────────────────────────────────────────────

def chunk_encode(compressed_bytes: bytes, dek: Optional[bytes]) -> bytes:
    return encrypt_bytes(compressed_bytes, dek) if dek is not None else compressed_bytes


def chunk_decode(stored_bytes: bytes, dek: Optional[bytes]) -> bytes:
    return decrypt_bytes(stored_bytes, dek) if dek is not None else stored_bytes
