#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
NEDB × RESP2 — pure-Python demo. Zero extra dependencies.

Starts nedbd with RESP2 enabled, connects via raw TCP, and shows:
  1. Standard Redis commands (SET/GET/HSET/SADD/LPUSH)
  2. NQL via EVAL — query, filter, sort, search, GROUP BY
  3. Time-travel — AS OF any past sequence number
  4. Causal provenance — TRACE caused_by backward and forward
  5. Tamper evidence — verify() the hash chain

This script is the proof. Run it:
    python3 examples/resp2_python.py

No redis-cli. No redis-py. Just sockets and NEDB.

© INTERCHAINED LLC × Vex (Interchained AI fleet: GLM · Claude · Opus · Fable · GPT-6)"""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "python"))

# ─────────────────────────────────────────────────────────────────────────────
# Minimal RESP2 client — speaks the Redis wire protocol over a raw socket.
# Any Redis client library in any language would work here too.
# ─────────────────────────────────────────────────────────────────────────────

class RESP2Client:
    """A lightweight RESP2 client that needs nothing but the stdlib."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6380):
        self._sock = socket.create_connection((host, port), timeout=10)
        self._f = self._sock.makefile("rb")

    def cmd(self, *args: str) -> object:
        """Send a command and return the parsed response."""
        payload = f"*{len(args)}\r\n" + "".join(
            f"${len(a.encode())}\r\n{a}\r\n" for a in args
        )
        self._sock.sendall(payload.encode())
        return self._read()

    def _read(self) -> object:
        line = self._f.readline().rstrip(b"\r\n")
        t = chr(line[0])
        body = line[1:]
        if t == "+":
            return body.decode()
        if t == "-":
            raise RuntimeError(body.decode())
        if t == ":":
            return int(body)
        if t == "$":
            n = int(body)
            if n < 0:
                return None
            data = self._f.read(n + 2)[:-2]
            return data.decode()
        if t == "*":
            n = int(body)
            return [self._read() for _ in range(n)]
        raise ValueError(f"unknown RESP type {t!r}")

    def close(self):
        self._sock.close()


# ─────────────────────────────────────────────────────────────────────────────
# Pretty output helpers
# ─────────────────────────────────────────────────────────────────────────────

W  = "\x1b[0m"
G  = "\x1b[32m"
C  = "\x1b[36m"
Y  = "\x1b[33m"
M  = "\x1b[35m"
B  = "\x1b[1m"
DIM= "\x1b[2m"

def banner(text: str) -> None:
    print(f"\n{B}  ── {text} {'─' * max(0, 50 - len(text))}{W}")

def cmd_show(c: RESP2Client, label: str, *args: str) -> object:
    result = c.cmd(*args)
    cmd_str = " ".join(args)
    if len(cmd_str) > 72:
        cmd_str = cmd_str[:69] + "…"
    print(f"  {C}{cmd_str:<74}{W}  →  {G}{_fmt(result)}{W}")
    return result

def _fmt(v: object) -> str:
    if v is None:
        return "(nil)"
    if isinstance(v, list):
        if not v:
            return "(empty)"
        # try to parse NQL rows
        parsed = []
        for item in v:
            if isinstance(item, str):
                try:
                    parsed.append(json.loads(item))
                except Exception:
                    parsed.append(item)
        if all(isinstance(p, dict) for p in parsed):
            return "\n" + "\n".join(f"    {M}{json.dumps(p, separators=(',', ':'))}{W}" for p in parsed)
        return str(v[:3]) + ("…" if len(v) > 3 else "")
    return repr(v)


# ─────────────────────────────────────────────────────────────────────────────
# Main demo
# ─────────────────────────────────────────────────────────────────────────────

HOST, PORT = "127.0.0.1", 6380
DATA = tempfile.mkdtemp(prefix="nedb-resp2-demo-")
NEDB_PORT = 7373

def main() -> None:
    print()
    print(f"  {B}◆ NEDB × RESP2{W}  {DIM}redis-cli talking to a time-traveling database{W}")
    print(f"  {DIM}{'─' * 58}{W}")
    print()

    # ── Start nedbd with RESP2 enabled ────────────────────────────────────────
    env = dict(os.environ,
               NEDBD_HOST="127.0.0.1",
               NEDBD_PORT=str(NEDB_PORT),
               NEDBD_DATA=DATA,
               NEDBD_RESP2_PORT=str(PORT))
    env.pop("NEDB_TMK", None)

    proc = subprocess.Popen(
        [sys.executable, "-m", "nedb.server"],
        env=env,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    # Wait for RESP2 to be ready
    for _ in range(40):
        try:
            s = socket.create_connection((HOST, PORT), timeout=1)
            s.close()
            break
        except OSError:
            time.sleep(0.1)
    else:
        print("nedbd did not start")
        proc.terminate()
        return

    print(f"  {G}nedbd started{W}  {DIM}HTTP :{NEDB_PORT}  RESP2 :{PORT}  data={DATA}{W}")

    c = RESP2Client(HOST, PORT)

    try:
        # ── 1. It's just Redis ────────────────────────────────────────────────
        banner("Standard Redis commands — it just works")

        cmd_show(c, "ping",         "PING")
        cmd_show(c, "select db",    "SELECT", "salon")
        cmd_show(c, "SET",          "SET", "owner", "INTERCHAINED LLC")
        cmd_show(c, "GET",          "GET", "owner")
        print()

        cmd_show(c, "HSET client",  "HSET", "client:001",
                 "name", "Mia Thornton", "status", "active", "phone", "555-0101")
        cmd_show(c, "HSET client",  "HSET", "client:002",
                 "name", "James Okafor",  "status", "active", "phone", "555-0102")
        cmd_show(c, "HSET client",  "HSET", "client:003",
                 "name", "Sofia Reyes",   "status", "inactive","phone", "555-0103")
        cmd_show(c, "HGETALL",      "HGETALL", "client:001")
        print()

        cmd_show(c, "SADD tags",    "SADD", "tags:vip", "client:001", "client:002")
        cmd_show(c, "SMEMBERS",     "SMEMBERS", "tags:vip")
        cmd_show(c, "RPUSH queue",  "RPUSH", "appt:queue", "Mon 9am", "Tue 2pm", "Wed 11am")
        cmd_show(c, "LRANGE",       "LRANGE", "appt:queue", "0", "-1")
        cmd_show(c, "DBSIZE",       "DBSIZE")

        # ── 2. NQL via EVAL ───────────────────────────────────────────────────
        banner("NQL via EVAL — this is the superpower")
        print(f"  {DIM}The EVAL command passes NQL directly to the query engine.{W}")
        print(f"  {DIM}Every feature of the NEDB query language is available here.{W}\n")

        # Seed structured data first
        c.cmd("SELECT", "users")
        from nedb import NEDB  # type: ignore
        db = NEDB(os.path.join(DATA, "users"))
        db.create_index("users", "status", "eq")
        db.create_index("users", "age",    "ordered")
        db.create_index("users", "bio",    "search")
        rows_in = [
            ("alice", "Alice",  31, "active",   "rust systems hacker"),
            ("bob",   "Bob",    24, "active",   "python data scientist"),
            ("carol", "Carol",  41, "inactive", "rust embedded engineer"),
            ("dave",  "Dave",   28, "active",   "go distributed systems"),
        ]
        for uid, name, age, status, bio in rows_in:
            db.put("users", uid, {"name": name, "age": age, "status": status, "bio": bio})
        snap_seq = db.seq   # remember this for time-travel
        db.close()

        print(f"  {DIM}(seeded 4 users directly via Python API){W}\n")

        cmd_show(c, "WHERE + ORDER BY",
                 "EVAL", 'FROM users WHERE status = "active" ORDER BY age ASC', "0")
        print()
        cmd_show(c, "SEARCH full-text",
                 "EVAL", 'FROM users SEARCH "rust"', "0")
        print()
        cmd_show(c, "GROUP BY aggregation",
                 "EVAL", "FROM users GROUP BY status COUNT", "0")
        print()
        cmd_show(c, "LIMIT",
                 "EVAL", "FROM users ORDER BY age DESC LIMIT 2", "0")

        # ── 3. Time travel ────────────────────────────────────────────────────
        banner("Time-travel — AS OF any past sequence number")
        print(f"  {DIM}snapshot was taken at seq={snap_seq}{W}\n")

        # Update alice after the snapshot
        db = NEDB(os.path.join(DATA, "users"))
        db.put("users", "alice", {"name": "Alice", "age": 99, "status": "retired",
                                   "bio": "now on sabbatical"})
        db.close()

        print(f"  {DIM}→ updated alice after snapshot: age=99, status=retired{W}\n")

        cmd_show(c, "read NOW",
                 "EVAL", 'FROM users WHERE _id = "alice"', "0")
        print()
        cmd_show(c, f"read AS OF {snap_seq}",
                 "EVAL", f'FROM users AS OF {snap_seq} WHERE _id = "alice"', "0")

        # ── 4. Causal provenance via TRACE ────────────────────────────────────
        banner("Causal Write Provenance — TRACE via RESP2")
        print(f"  {DIM}Every write can declare why it happened — sealed in the hash chain.{W}\n")

        c.cmd("SELECT", "agent")
        db = NEDB(os.path.join(DATA, "agent"))
        db.put("inputs", "msg_1", {"role": "user", "text": "I hate bright screens"})
        s1 = db.seq
        db.put("inputs", "msg_2", {"role": "user", "text": "I have migraines"})
        s2 = db.seq
        db.put("beliefs", "dark_mode",
               {"value": True, "summary": "User prefers dark mode"},
               caused_by=[s1, s2], evidence="user_message", confidence=0.95)
        sb = db.seq
        db.put("beliefs", "low_blue_light",
               {"value": True, "summary": "Enable blue-light filter"},
               caused_by=[sb], evidence="inference", confidence=0.82)
        db.close()

        print(f"  {DIM}wrote msg_1 (seq {s1}), msg_2 (seq {s2}){W}")
        print(f"  {DIM}derived belief 'dark_mode' from both inputs{W}")
        print(f"  {DIM}inferred 'low_blue_light' from 'dark_mode'{W}\n")

        print(f"  {Y}Why does the agent believe dark_mode?{W}")
        cmd_show(c, "TRACE backward",
                 "EVAL", 'FROM beliefs WHERE _id = "dark_mode" TRACE caused_by', "0")
        print()

        print(f"  {Y}What did msg_1 cause downstream?{W}")
        cmd_show(c, "TRACE forward",
                 "EVAL", 'FROM inputs WHERE _id = "msg_1" TRACE caused_by REVERSE', "0")
        print()

        print(f"  {Y}Show high-confidence beliefs only:{W}")
        cmd_show(c, "WHERE _confidence",
                 "EVAL", "FROM beliefs WHERE _confidence > 0.9", "0")

        # ── 5. Tamper evidence ────────────────────────────────────────────────
        banner("Tamper evidence — verify the hash chain")
        print(f"  {DIM}Every op is BLAKE2b-hashed into a chain. Changing any value breaks it.{W}\n")

        for db_name in ("salon", "users", "agent"):
            db = NEDB(os.path.join(DATA, db_name))
            ok = db.verify()
            seq = db.seq
            head = db.head[:16]
            icon = f"{G}●{W}" if ok else f"\x1b[31m✗{W}"
            print(f"  {icon}  {db_name:<8}  seq={seq:<4}  head={head}…  verify={ok}")
            db.close()

        print()
        print(f"  {DIM}{'─' * 58}{W}")
        print(f"  {B}That was a hash-chained, MVCC, time-traveling,{W}")
        print(f"  {B}replay-protected, causally-provable database{W}")
        print(f"  {B}spoken to over the Redis wire protocol.{W}")
        print(f"  {DIM}No Redis. No redis-py. Just NEDB.{W}")
        print(f"  {DIM}{'─' * 58}{W}")
        print()

    finally:
        c.close()
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        shutil.rmtree(DATA, ignore_errors=True)


if __name__ == "__main__":
    main()
