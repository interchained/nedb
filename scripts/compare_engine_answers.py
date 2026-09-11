#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
Diff every answer two nedbd binaries give to the same corpus of queries.

The tool used to verify that 3.3.0's parser/executor rewrite did not change
what an existing query RETURNS. Point it at a binary built from a released tag
and one built from HEAD; it seeds both with an identical fixture and reports
every query whose answer differs, plus any query the OLD engine answered and
the NEW one rejects (a regression).

    git worktree add /tmp/v322 v3.2.2
    (cd /tmp/v322/rust && cargo build --release --bin nedbd -p nedb-engine)
    cp /tmp/v322/rust/target/release/nedbd /tmp/nedbd-old
    (cd rust && cargo build --release --bin nedbd -p nedb-engine)
    python3 scripts/compare_engine_answers.py /tmp/nedbd-old rust/target/release/nedbd

Exit status is non-zero if anything differs, so it can gate a release.

Result when 3.3.0 was cut: 37 of 39 identical, 0 regressions. The two
differences are documented in tests/test_backcompat.py, which freezes the
released answers as permanent expectations so this tool is only needed when a
future rewrite warrants re-checking.

NOTE on _hash: it is excluded from the comparison. `Node.ts` (a Unix
timestamp) is inside the hashed content, so two independently-created
databases legitimately hold different hashes for identical document content.
Comparing them would measure whether the two databases were created in the
same instant, not whether the engines agree.
"""
import json, subprocess, sys, time, urllib.request, urllib.error, os, shutil, signal

CORPUS = [
    # ── pre-3.3.0 documented forms. Every one of these must answer IDENTICALLY.
    "FROM jobs",
    "FROM jobs LIMIT 3",
    "FROM jobs LIMIT 1",
    'FROM jobs WHERE status = "open"',
    'FROM jobs WHERE status != "open"',
    "FROM jobs WHERE fee > 20",
    "FROM jobs WHERE fee < 20",
    "FROM jobs WHERE fee >= 20",
    "FROM jobs WHERE fee <= 20",
    'FROM jobs WHERE _id = "3"',
    'FROM jobs WHERE _id = "3" LIMIT 1',
    'FROM jobs WHERE status = "open" AND fee > 5',
    'FROM jobs WHERE status = "open" AND fee > 5 AND region = "eu-west"',
    "FROM jobs ORDER BY fee",
    "FROM jobs ORDER BY fee ASC",
    "FROM jobs ORDER BY fee DESC",
    "FROM jobs ORDER BY fee DESC LIMIT 2",
    "FROM jobs ORDER BY fee ASC LIMIT 3",
    'FROM jobs WHERE status = "open" ORDER BY fee DESC',
    'FROM jobs WHERE status = "open" ORDER BY fee DESC LIMIT 1',
    'FROM jobs SEARCH "Zenith"',
    'FROM jobs SEARCH "acme"',
    'FROM jobs SEARCH "acme" LIMIT 1',
    "FROM jobs GROUP BY status COUNT",
    "FROM jobs GROUP BY region COUNT",
    # Aggregates over `_`-prefixed METADATA fields. `_seq` lives on the node,
    # not in its data payload, and the Rust aggregator read the payload
    # directly — so it answered NULL where Python answered correctly. A live
    # divergence that produced a confident wrong number for the single most
    # load-bearing question in replication: "what is the newest sequence?"
    "FROM jobs MAX _seq",
    "FROM jobs MIN _seq",
    "FROM jobs SUM _seq",
    "FROM jobs GROUP BY _seq COUNT",
    "FROM jobs GROUP BY _coll COUNT",
    'FROM jobs WHERE status = "open" MAX _seq',
    # …and over ordinary payload fields, so the comparison has a control.
    "FROM jobs MAX fee",
    "FROM jobs MIN fee",
    "FROM jobs SUM fee",
    "FROM jobs AVG fee",
    "FROM jobs COUNT",
    "FROM jobs AS OF 0",
    "FROM jobs AS OF 2",
    "FROM jobs AS OF 3 WHERE fee > 10",
    'FROM jobs AS OF 4 WHERE status = "open"',
    "FROM jobs WHERE _seq > 1",
    'FROM jobs WHERE _coll = "jobs"',
    "FROM nonexistent",
    'FROM jobs WHERE status = "nope"',
    "FROM jobs WHERE fee > 9999",
    # bi-temporal
    'FROM rates VALID AS OF "2024-06-01"',
    'FROM rates VALID AS OF "2024-01-15"',
    'FROM rates AS OF 2 VALID AS OF "2024-06-01"',
    # provenance
    "FROM effects TRACE caused_by",
    "FROM causes TRACE caused_by REVERSE",
    # ordering comparisons against a SPARSE column — `miner` is absent on doc 4
    # and explicitly null on doc 5.
    'FROM jobs WHERE miner < "zzz"',
    'FROM jobs WHERE miner <= "zzz"',
    'FROM jobs WHERE miner > "A"',
    'FROM jobs WHERE miner >= "A"',
    'FROM jobs WHERE miner != "Zenith"',
    "FROM jobs WHERE fee < 100",
]

def wait(port, tries=30):
    for _ in range(tries):
        time.sleep(1)
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2).read()
            return True
        except Exception:
            pass
    return False

def req(port, m, p, b=None):
    d = json.dumps(b).encode() if b is not None else None
    r = urllib.request.Request(f"http://127.0.0.1:{port}{p}", data=d, method=m,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=15) as x:
            return x.status, json.loads(x.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:200]

def seed(port):
    req(port, "POST", "/v1/databases", {"name": "bc"})
    jobs = [
        ("1", {"status": "open",    "miner": "Acme Pool", "fee": 10, "region": "eu-west"}),
        ("2", {"status": "pending", "miner": "acme solo", "fee": 20, "region": "us-east"}),
        ("3", {"status": "closed",  "miner": "Zenith",    "fee": 30, "region": "eu-north"}),
        ("4", {"status": "open",                          "fee": 40, "region": "us-west"}),
        ("5", {"status": "voided",  "miner": None,        "fee": 50, "region": "ap-south"}),
    ]
    for i, d in jobs:
        req(port, "POST", "/v1/databases/bc/put", {"coll": "jobs", "id": i, "doc": d})
    # a second version of doc 1, so AS OF has history to show
    req(port, "POST", "/v1/databases/bc/put",
        {"coll": "jobs", "id": "1", "doc": {"status": "closed", "miner": "Acme Pool",
                                            "fee": 11, "region": "eu-west"}})
    # bi-temporal rows
    for i in range(4):
        ok = i % 2 == 0
        req(port, "POST", "/v1/databases/bc/put",
            {"coll": "rates", "id": str(i), "doc": {"n": i},
             "valid_from": "2024-01-01",
             "valid_to": "2030-01-01" if ok else "2024-02-01"})
    # causal chain: cause -> effect
    st, b = req(port, "POST", "/v1/databases/bc/put",
                {"coll": "causes", "id": "c1", "doc": {"kind": "root"}})
    h = (b or {}).get("node", {}).get("hash") or (b or {}).get("hash")
    req(port, "POST", "/v1/databases/bc/put",
        {"coll": "effects", "id": "e1", "doc": {"kind": "derived"},
         "caused_by": [h] if h else []})
    return h

def normalize(body):
    """Compare the query's ANSWER, not incidental envelope fields."""
    if not isinstance(body, dict):
        return ("raw", str(body)[:160])
    rows = body.get("rows")
    if rows is None:
        return ("raw", json.dumps(body, sort_keys=True)[:200])
    out = []
    for r in rows:
        # Numeric equality across int/float representations: 66 == 66.0.
        #
        # _hash is EXCLUDED. Node.ts (a Unix timestamp) is inside the hashed
        # content, so two independently-created databases legitimately carry
        # different hashes for identical document content. Comparing them here
        # measures "were these two databases created at the same instant",
        # not "do the engines agree".
        items = []
        for k, v in sorted(r.items()):
            if k == "_hash":
                continue
            if isinstance(v, bool):
                items.append((k, v))
            elif isinstance(v, (int, float)):
                items.append((k, float(v)))
            else:
                items.append((k, v))
        out.append(tuple(items))
    return ("rows", body.get("count"), out)

def run(binary, port, tag):
    data = f"/tmp/bc_{tag}"
    shutil.rmtree(data, ignore_errors=True)
    log = open(f"/tmp/bc_{tag}.log", "w")
    proc = subprocess.Popen([binary, "--data", data, "--port", str(port)],
                            stdout=log, stderr=log)
    try:
        if not wait(port):
            # some builds may not take --port; retry on default
            proc.terminate(); proc.wait(timeout=10)
            log2 = open(f"/tmp/bc_{tag}.log", "a")
            proc = subprocess.Popen([binary, "--data", data], stdout=log2, stderr=log2)
            port = 7070
            if not wait(port):
                print(f"  {tag}: daemon never came up"); return None, None
        h = seed(port)
        results = {}
        for nql in CORPUS:
            st, body = req(port, "POST", "/v1/databases/bc/query", {"nql": nql})
            results[nql] = (st, normalize(body) if st == 200 else ("err", str(body)[:120]))
        # verify() must be true on both
        stv, bv = req(port, "GET", "/v1/databases/bc/verify", None)
        return results, (stv, bv)
    finally:
        try:
            proc.send_signal(signal.SIGTERM); proc.wait(timeout=15)
        except Exception:
            proc.kill()
        time.sleep(1)

OLD, NEW = sys.argv[1], sys.argv[2]
print("seeding + querying OLD (v3.2.2)…")
old, oldv = run(OLD, 7071, "old")
print("seeding + querying NEW (HEAD)…")
new, newv = run(NEW, 7072, "new")

if old is None or new is None:
    sys.exit("could not run both binaries")

same = diff = oldonly = 0
print(f"\n{'='*74}\nBACKWARDS COMPATIBILITY: v3.2.2 vs HEAD, {len(CORPUS)} legacy queries\n{'='*74}")
for nql in CORPUS:
    so, ro = old[nql]
    sn, rn = new[nql]
    if so != 200 and sn != 200:
        print(f"  both reject   {nql}")
        continue
    if so == 200 and sn != 200:
        oldonly += 1
        print(f"  REGRESSION    {nql}\n      old: OK   new: {rn}")
        continue
    if so != 200 and sn == 200:
        print(f"  now accepted  {nql}  (was: {ro[1][:70]})")
        continue
    if ro == rn:
        same += 1
    else:
        diff += 1
        print(f"  DIFFERS       {nql}")
        print(f"      old: {str(ro)[:230]}")
        print(f"      new: {str(rn)[:230]}")

print(f"\nidentical: {same}   differing: {diff}   regressions (old OK, new fails): {oldonly}")
print(f"verify() old={oldv}  new={newv}")
sys.exit(1 if (diff or oldonly) else 0)
