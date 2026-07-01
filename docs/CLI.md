# Command-line tools

`cargo install nedb-engine` ships three binaries: the **`nedbd`** server daemon (documented in the README), plus two companions:

- **`nedb-cli`** — operate on a store *directory* directly, offline.
- **`nedb-inspector`** — statically check how your code embeds NEDB.

---

## `nedb-cli` — the store CLI/sidecar

`nedbd` *serves* a store over HTTP; `nedb-cli` works on a store directory **directly, with no server running**. It is the plumbing — `fsck` + `cat` + `tail` + a time-machine — for a NEDB v2 store.

```
nedb-cli <command> <path> [args]
```

| Command | What it does |
|---|---|
| `head <path>` | Print the tamper-evident Merkle head. |
| `status <path>` | Readiness snapshot as JSON: `scan_complete`, `tip_seq`, indexed seq range/count, plus `seq` + `head`. |
| `verify <path>` | Re-hash **every** node and report any tamper (exit 1 if tampered). |
| `get <path> <coll> <id> [seq]` | Print one document as JSON. With `seq`, time-travel **AS OF** that sequence. |
| `scan <path> [after] [limit]` | Print the changefeed page after `after` as NDJSON (one node per line); the `{from_seq,to_seq,head_seq,has_more}` envelope goes to stderr. |
| `flush <path>` | Make buffered writes durable now (WAL + MANIFEST). |
| `repair <path>` | Rebuild the index from the content-addressed objects (self-heal a stale/missing MANIFEST), then verify + flush. |
| `export <path> [coll]` | Dump live documents — all collections, or one — as NDJSON. |

**Exit codes:** `0` ok · `1` error / tamper / not-found · `2` usage.

```bash
nedb-cli status ./data
# { "head": "…", "seq": 1310704, "scan_complete": true, "tip_seq": 1310703, ... }

nedb-cli get ./data users u1
nedb-cli get ./data users u1 200          # AS OF seq 200

nedb-cli scan ./data 0 500 > page.ndjson  # changefeed catch-up
nedb-cli export ./data users > users.ndjson

nedb-cli verify ./data                    # exit 1 if any node fails its hash
nedb-cli repair ./data                    # rebuild index from objects, verify, flush
```

> **Safety.** Reads (`head`/`status`/`verify`/`get`/`scan`/`export`) are always safe. The write commands (`flush`/`repair`) want a **stopped** store — or use the `nedbd` HTTP API on a live one. Two writers to one directory is not supported.

`status`/`scan` are built on the same replication primitives (`scan_status`, `since`) documented in [REPLICATION.md](./REPLICATION.md).

---

## `nedb-inspector` — durability guardrail

A deterministic checker that reads how a program **embeds** NEDB and warns loudly, with the exact correct pattern, when a durable database is opened without flush-on-exit wiring (see [DURABILITY.md](./DURABILITY.md)).

```
nedb-inspector <path.(rs|js|mjs|ts|py)> [more paths...]
```

**Deterministic — no regex, no LLM.** A per-language lexer masks comment and string bodies first, so a `Db::open` inside a comment or a string literal is never matched, and an `install_exit_flush` sitting only in a comment never counts as wired. Detection then runs as structural token matching over the masked code.

What it flags:

- **Rust** — a durable `Db::open()` with no `install_exit_flush` → **WARN**.
- **Node** — a durable `NedbCore.open()` via the raw native binding with no `process.on(...)` flush wiring → **WARN**. Importing from `'nedb-engine'` (the wrapper auto-arms) is OK.
- **Python** — native `NedbCore.open()` is `atexit`-armed (OK), but `os._exit()` bypasses `atexit` → **WARN**. Pure-Python `NEDB(path)` is per-op durable → OK.

**Exit codes:** `0` clean · `1` warnings · `2` usage. Set `NO_COLOR=1` for plain output.

```text
$ nedb-inspector src/store.rs

 WARN  src/store.rs [rust]
   • durable Db::open() at line 39
   ┌─ NEDB DURABILITY WARNING ─────────────────────────────────
   │ RUST_NO_EXIT_FLUSH  (line 39)
   │ durable Db::open() with NO install_exit_flush — writes staged since the
   │ last flush are LOST on SIGINT/SIGTERM (Drop does not run on a signalled exit).
   │ Use this pattern:
   │   let db = Arc::new(Db::open(path, None)?);
   │   Db::install_exit_flush(Arc::clone(&db));
   └────────────────────────────────────────────────────────────
```

Wire it into CI to keep every embedder honest:

```bash
nedb-inspector $(git ls-files '*.rs' '*.mjs' '*.py') || exit 1
```
