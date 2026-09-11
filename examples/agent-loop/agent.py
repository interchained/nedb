#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
# SPDX-License-Identifier: BUSL-1.1
# NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

"""
An agent that keeps its knowledge in NEDB and retrieves it by casting English.

    python3 agent.py "what do I know about the release flow?"
    python3 agent.py --seed          # build the knowledge base first
    python3 agent.py --demo          # scripted multi-turn run

WHAT THIS IS, AND WHAT IT ISN'T
-------------------------------
nedb-cast-slm does exactly one thing: English -> NQL. It is 3.3M parameters and
it has no other output type. Asked "what should I do next" it emits:

    FROM FROM FROM FROM AND AND AND AND = = = ...

So it is NOT the agent. It is a TOOL the agent calls -- specifically the
retrieval tool, the one that turns a question into a query over stored
knowledge. Tool *selection* is the caller's job (keyword routing here, an LLM in
a real deployment). Tool *execution* is NEDB's job. Cast sits between them and
does the one part it is good at.

That division is the whole point. A 3.3M-param model that reliably writes
`FROM memories WHERE importance >= 4` is more useful than a 70B model that does
it slower, more expensively, and over the network -- as long as you never ask it
to do anything else.

WHY THE KNOWLEDGE LIVES IN NEDB
-------------------------------
Three properties a vector store does not give you:

  caused_by   Every memory records the run that produced it. `TRACE caused_by`
              answers "why do I believe this" with the actual chain, not a
              similarity score.
  AS OF       Replay what the agent knew at any past sequence. Debug a decision
              against the knowledge it ACTUALLY had, not what it has now.
  verify()    The ledger is hash-chained. Nobody edits a memory after the fact
              and pretends it was always there.

ITERATION
---------
Each turn appends a `runs` document, and memories written during that turn point
back at it. So turn N can query the results of turns 1..N-1 -- the agent reads
its own history through the same interface it reads everything else. There is no
separate "conversation memory" system; it is all just NEDB.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

NEDB = os.environ.get("NEDB", "http://127.0.0.1:7070")
DB = os.environ.get("NEDB_DB", "brain")

C = "\033[36m"; G = "\033[32m"; Y = "\033[33m"; R = "\033[31m"; D = "\033[2m"; Z = "\033[0m"
if not sys.stdout.isatty():
    C = G = Y = R = D = Z = ""


# ── transport ────────────────────────────────────────────────────────────────

def _post(path: str, payload: dict) -> tuple[int, Any]:
    # rstrip the join: path="" must give /v1/databases, not /v1/databases/ --
    # the trailing slash is a different route and the daemon drops the
    # connection on it.
    url = f"{NEDB}/v1/databases/{path}".rstrip("/")
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.load(r)
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.load(e)
        except Exception:
            return e.code, {"error": e.read().decode()[:200]}
    except urllib.error.URLError as e:
        sys.exit(f"{R}no nedbd at {NEDB}: {e.reason}{Z}\n"
                 f"start it:  nedbd --dag --cast ./data")


# ── the three tools ──────────────────────────────────────────────────────────
#
# Deliberately small and explicit. An agent framework would wrap these in a
# schema and let an LLM pick; the point here is that `recall` is the interesting
# one and it is powered by a model small enough to run in the database.

def recall(question: str, execute: bool = True) -> dict:
    """TOOL: turn an English question into NQL and run it over stored knowledge.

    Returns the plan alongside the rows, always. The caller needs to see the
    query that produced an answer -- an agent that reports facts without being
    able to show the query behind them is not auditable.
    """
    code, body = _post(f"{DB}/cast", {"prompt": question, "execute": execute})
    if code == 501:
        sys.exit(f"{R}this nedbd was built without --features cast{Z}")
    # `drift` arrives from the ENGINE (nedbd >= 2.8.2). It is set when the plan
    # contains a quoted literal that is absent from the prompt -- the model
    # substituted a memorised value for a word outside its vocabulary.
    #
    # This check used to live here, in the example. It belongs in the daemon:
    # every client inherits it instead of each one reimplementing it, exactly
    # like the collection check. An example is the wrong place for a safety
    # property that production callers need.
    return {"ok": code == 200, "status": code, **body}


def trustworthy(plan: dict) -> bool:
    """The three-way guard an unattended caller should apply.

    `valid` and `collection_known` are necessary and NOT sufficient:

        "memories about pricing"  ->  FROM memories SEARCH "handoff"

    That is valid, names a real collection, and returns real rows -- for a
    question nobody asked. Only `drift` catches it.
    """
    return (bool(plan.get("valid"))
            and bool(plan.get("collection_known"))
            and not plan.get("drift"))


def remember(text: str, category: str = "project", importance: int = 3,
             agent: str = "vex", caused_by: list[str] | None = None) -> dict:
    """TOOL: write a memory, optionally linked to the run that produced it."""
    mid = f"m{int(time.time() * 1000) % 10_000_000}"
    doc = {"text": text, "category": category, "importance": importance, "agent": agent}
    payload = {"coll": "memories", "id": mid, "doc": doc}
    if caused_by:
        payload["caused_by"] = caused_by
    code, body = _post(f"{DB}/put", payload)
    return {"ok": code == 200, "id": mid, **(body if isinstance(body, dict) else {})}


def query(nql: str) -> dict:
    """TOOL: run NQL directly, for when the agent already knows the query."""
    code, body = _post(f"{DB}/query", {"nql": nql})
    return {"ok": code == 200, **(body if isinstance(body, dict) else {})}


# ── the loop ─────────────────────────────────────────────────────────────────

def decide(question: str) -> str:
    """Pick a tool. Keyword routing, because that is honest about what it is.

    In a real deployment an LLM does this. It is NOT cast's job: cast has one
    output type (NQL) and asking it to choose a tool produces garbage. Keeping
    the router dumb and separate makes that boundary visible instead of blurred.
    """
    q = question.lower()
    if q.startswith(("remember", "note that", "save")):
        return "remember"
    if q.startswith(("from ", "select ")):
        return "query"
    return "recall"


def turn(question: str, n: int, run_id: str) -> dict:
    """One iteration: decide, act, report, record."""
    tool = decide(question)
    print(f"\n{C}[{n}]{Z} {question}")
    print(f"    {D}tool: {tool}{Z}")

    if tool == "remember":
        text = question.split(" ", 1)[1] if " " in question else question
        res = remember(text, caused_by=[run_id])
        print(f"    {G}stored{Z} {res['id']}  {D}caused_by {run_id}{Z}")
        return {"tool": tool, "result": res}

    if tool == "query":
        res = query(question)
        print(f"    {G}{res.get('count', 0)} rows{Z}")
        return {"tool": tool, "result": res}

    res = recall(question)
    nql = res.get("nql", "")
    print(f"    {Y}nql{Z}  {nql}")

    if not res["ok"]:
        # The failure modes are distinct and worth distinguishing. A plan that
        # names a missing collection is a MODEL miss; a plan that will not parse
        # is a different miss. Neither is "no results", and reporting either as
        # an empty answer would be a lie.
        why = res.get("error", "?")
        print(f"    {R}miss{Z} {why}")
        return {"tool": tool, "result": res, "miss": why}

    if drift := res.get("drift"):
        # Loud, and NOT counted as an answer. The rows below are real rows to
        # a question the user did not ask, which is worse than zero rows.
        print(f"    {R}drift{Z} {drift}")
        print(f"    {D}→ rephrase using words the model knows, or use `query` "
              f"with explicit NQL{Z}")
        return {"tool": tool, "result": res, "miss": drift}

    rows = res.get("rows", [])
    print(f"    {G}{res.get('count', 0)} rows{Z}")
    for r in rows[:4]:
        txt = r.get("text") or json.dumps({k: v for k, v in r.items() if not k.startswith("_")})
        print(f"      {D}·{Z} {txt[:96]}")
    if len(rows) > 4:
        print(f"      {D}… {len(rows) - 4} more{Z}")
    return {"tool": tool, "result": res}


def run(questions: list[str]) -> None:
    """A session. Every turn is recorded, so later turns can query earlier ones."""
    run_id = f"r{int(time.time()) % 1_000_000}"
    _post(f"{DB}/put", {"coll": "runs", "id": run_id,
                        "doc": {"status": "running", "owner": "vex",
                                "turns": len(questions),
                                "started_at": time.strftime("%Y-%m-%d")}})
    print(f"{D}run {run_id} · {DB} @ {NEDB}{Z}")

    out = []
    for i, q in enumerate(questions, 1):
        out.append(turn(q, i, run_id))

    misses = sum(1 for r in out if r.get("miss"))
    _post(f"{DB}/put", {"coll": "runs", "id": run_id,
                        "doc": {"status": "failed" if misses else "done",
                                "owner": "vex", "turns": len(questions),
                                "misses": misses,
                                "started_at": time.strftime("%Y-%m-%d")}})
    print(f"\n{D}run {run_id} → {'failed' if misses else 'done'} "
          f"({misses} miss{'es' if misses != 1 else ''}){Z}")


# ── seed ─────────────────────────────────────────────────────────────────────
#
# Field names come from the model's `agent` training domain: memories carries
# importance/category/agent, runs carries status/owner/duration. Those words are
# in its 581-token vocabulary. Invent your own field names and the model still
# writes queries against THESE -- it is a 3.3M-param model, not a schema reader.

SEED = [
    ("release flow is: tag locally, then release.py --target github", "project", 5),
    ("never retag a release, always bump the version", "preference", 5),
    ("guardrail: never push to interchained org without asking", "preference", 5),
    ("the handoff to studio happens after a nedb-engine bump", "project", 4),
    ("cast defaults execute to false so a human reviews the plan", "domain", 4),
    ("multi-predicate WHERE is the model's weakest clause at 85.1%", "domain", 4),
    ("mark prefers shipping without asking for confirmation each time", "preference", 3),
    ("smoke gate blocks npm publish when the addon test fails", "project", 3),
    ("digits tokenize one at a time, so long numbers get truncated", "domain", 2),
    ("the parser was the corpus generator, grader, and entry gate", "domain", 2),
]


def seed() -> None:
    code, _ = _post("", {"name": DB})
    print(f"{D}database {DB} ({'created' if code in (200, 201) else 'exists'}){Z}")
    rid = "r000seed"
    _post(f"{DB}/put", {"coll": "runs", "id": rid,
                        "doc": {"status": "done", "owner": "vex", "duration": 1,
                                "started_at": time.strftime("%Y-%m-%d")}})
    n = 0
    for i, (text, cat, imp) in enumerate(SEED):
        code, _ = _post(f"{DB}/put", {
            "coll": "memories", "id": f"m{i:03d}",
            "doc": {"text": text, "category": cat, "importance": imp, "agent": "vex"},
            "caused_by": [rid],
        })
        n += code == 200
    got = query("FROM memories").get("count", 0)
    print(f"{G}{n} memories written{Z} {D}· verified {got} readable{Z}")
    if got != len(SEED):
        sys.exit(f"{R}seed verification failed: expected {len(SEED)}, read {got}{Z}")


DEMO = [
    "memories with importance 5",
    "memories about the release flow",
    "memories grouped by category count",
    "remember the agent loop reads its own runs from nedb",
    "runs by vex",
    "memories about pricing",          # deliberate: an untrained SEARCH term
]


def main() -> None:
    ap = argparse.ArgumentParser(description="agent with knowledge in NEDB")
    ap.add_argument("question", nargs="*", help="what to ask")
    ap.add_argument("--seed", action="store_true", help="build the knowledge base")
    ap.add_argument("--demo", action="store_true", help="scripted multi-turn run")
    a = ap.parse_args()

    if a.seed:
        seed()
        if not (a.question or a.demo):
            return
    if a.demo:
        run(DEMO)
    elif a.question:
        run([" ".join(a.question)])
    elif not a.seed:
        ap.print_help()


if __name__ == "__main__":
    main()
