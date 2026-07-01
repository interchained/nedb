# Durability & flush-on-exit

How a NEDB v2 store makes writes durable — and the **durable-mode auto-flush-on-exit** contract that keeps a hard shutdown from losing buffered writes.

---

## The model

A durable store (`Db::open(path)`, not `Db::in_memory()`) has three layers:

1. **Objects** — every document version is an immutable, content-addressed, BLAKE2b-verified object written atomically (`tmp` → `rename`). Once written, an object is durable and never mutated.
2. **Id-index (WAL)** — the map *collection/id → current object hash* is staged in an in-memory write buffer and persisted by `flush_write_buf()`. This is the part that is **buffered**.
3. **MANIFEST** — a tiny `{seq, head}` file rewritten atomically on flush, enabling O(1) warm starts.

Between flushes, the object bytes are on disk but the **index pointing at the newest versions may not be**. Flushing persists the WAL + MANIFEST (and `fsync`s the active segment under `--dag-v3`).

Flushing happens in three ways:

- **The manifest ticker** — `start_manifest_ticker(db, interval_ms)` flushes the WAL + MANIFEST on an interval (nedbd runs it at 1 s).
- **`Drop`** — closing the last `Arc<Db>` handle calls `flush_all()` (the flush-on-close contract, like sled/RocksDB).
- **An explicit `flush_all()` / `flush()`** — on demand.

## The gap this closes

`Drop` only fires once **every** owning handle is gone. But a manifest-ticker thread (or a server that blocks forever in `serve()`) holds an `Arc<Db>` for the whole process lifetime — so on a **hard exit** (`Ctrl+C`, `SIGTERM` from an orchestrator, `kill`) `Drop` never runs, and writes staged since the last tick are lost.

`nedbd` and the Python server already handle SIGINT/SIGTERM → flush, but that wiring lived **inside the server binaries**. An application that embeds the library directly used to get nothing. **Durable-mode auto-flush-on-exit** fixes that at every surface.

> Not on every put. Durability on exit is a **one-time** flush on the way out; the hot write path stays hot.

---

## Rust — `Db::install_exit_flush`

For a **standalone Rust binary** (a relay node, a job runner — anything that owns the process and may be signalled):

```rust
use std::sync::Arc;
use nedb_engine::Db;

let db = Arc::new(Db::open(std::path::Path::new("/data/mydb"), None)?);
Db::start_manifest_ticker(Arc::clone(&db), 1000);  // periodic flush (optional but recommended)
Db::install_exit_flush(Arc::clone(&db));           // flush on SIGINT / SIGTERM
```

- **How** — installs a `SIGINT`/`SIGTERM` handler using a self-pipe. The handler is async-signal-safe (it only `write()`s the signal number to a pipe); a dedicated reader thread runs `flush_all()` from a normal context, restores the signal's default disposition, and re-raises it so the process still terminates with the conventional `128 + signum` status.
- **Opt-in** — a *library* that unilaterally seized signal handlers would trample a host app's own shutdown logic, so the core never installs this implicitly. Call it once, after wrapping the `Db` in an `Arc`.
- **Idempotent** — safe to call from multiple databases; the handler and reader thread are installed once per process. Holds only a `Weak<Db>`, so it never keeps the database alive.
- **`:memory:` no-op** — an in-memory database has nothing to flush.
- **Unix only** — on non-Unix targets it is a documented no-op; flush via `Drop` or an explicit `flush_all()` on shutdown.

Do **not** use this from inside a managed runtime (Node/Python) — those have their own cooperative hooks (below), and a C-level signal handler would fight the runtime's signal machinery.

## Node — automatic on durable open

The `nedb-engine` package wrapper arms the flush for you when you open a durable database:

```js
import { NedbCore } from 'nedb-engine';
const db = NedbCore.open('/data/mydb');   // registers process.on('SIGINT'|'SIGTERM'|'exit', () => db.flush())
```

- Uses `process.on(...)` — the **libuv-cooperative** hook — never a C-level `sigaction` inside the addon (which would clobber libuv's own signal handling).
- `new NedbCore()` (in-memory) is **never** armed.
- Opt out with **`NEDB_NO_EXIT_FLUSH=1`** if your app owns shutdown and calls `db.flush()` itself.
- On `SIGINT`/`SIGTERM` the wrapper flushes and then exits with `128 + signum` (registering a signal listener suppresses Node's default termination, so the wrapper owns the exit).

If you bypass the package entry and load the raw native binding directly, you get **no** auto-flush — wire `process.on(...)` yourself. (`nedb-inspector` will warn you about this — see [CLI.md](./CLI.md).)

## Python — automatic on durable open

The compiled core (`nedb._native.NedbCore.open`) registers a Python **`atexit`** hook:

```python
from nedb._native import NedbCore
db = NedbCore.open('/data/mydb')   # atexit-registered flush
```

- `atexit` runs on normal interpreter exit **and** on `Ctrl+C` (SIGINT → `KeyboardInterrupt` → exit), so no C-level `sigaction` is taken — which is deliberate: seizing SIGINT would break `KeyboardInterrupt`.
- Holds only a `Weak<Db>`; best-effort, so a hook-registration hiccup never fails `open()`.
- **`os._exit()` bypasses `atexit`.** If you must hard-exit, call `db.flush()` first.
- The **pure-Python** `NEDB(path=...)` engine is a different implementation — it is **per-op fsync durable** by default, so it needs no exit flush.

---

## When to flush manually

- Before a checkpoint you must not lose (e.g. after committing a batch).
- Before `os._exit()` / `process.exit()` / any hard exit your code controls.
- Offline, against a stopped store: `nedb-cli flush <path>` (see [CLI.md](./CLI.md)).

## Verifying the wiring

`nedb-inspector <file>` statically checks that a durable open has flush-on-exit wired, and warns with the exact pattern if not. See [CLI.md](./CLI.md).
