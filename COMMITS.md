# NEDB — Commit & Release Log

Living progress log for the NEDB engine, focused on the **v3 segment/pack object store** and the 2.3.x releases. The engine is the source of truth; downstream consumers (itcd) are tracked where they exercise engine capabilities.

_Last updated: 2026-06-26 — release **v2.3.333** (comprehensive v3 documentation)._

---

## Releases

| Version | What shipped | Registries |
|---|---|---|
| **v2.3.3333** | Opt-in macOS fast-fsync for the v3 segment store (`NEDB_FAST_FSYNC`, default off) — plain `fsync(2)` instead of `F_FULLFSYNC`, no-op off-mac. Closes the 3's cycle; next is 2.4.0. | PyPI · npm · crates.io |
| **v2.3.333** | Comprehensive v3 documentation (README section + this log + ideas.md). Engine code unchanged from 2.3.33. | PyPI · npm · crates.io |
| **v2.3.33** | Durable flush-on-close (`Db::drop` → `flush_all`), cross-platform Windows-safe id-index (percent-encoded filesystem-unsafe ids), idempotent re-writes; `cargo test -p nedb-engine` green (43/43). | PyPI · npm · crates.io |
| **v2.3.3** | NEDB **v3** segment/pack object store landed behind `--dag-v3` (Phases 1–3: segments, compaction/pruning, `.idx` sidecars). Default off. | PyPI · npm · crates.io |
| v2.2.33 | Graph AS-OF time-travel + Node test suite + mini-chain example. | PyPI · npm · crates.io |

---

## NEDB engine — recent commits (newest first)

| Commit | Summary |
|---|---|
| `d49dcbe` | fix(engine): cargo-test green — Windows-safe id-index, durable `Drop`, idempotent write (#14) |
| `4f91bee` | chore(release): bump engine + clients to 2.3.33; refresh README banner |
| `2eaa0ab` | fix(index): filesystem-safe id-index filenames so link ids persist on Windows |
| `5fa3794` | fix(engine): durable flush-on-close + idempotent re-write; fix nql test-harness temp-dir lifetime |
| `2b09e97` | fix(test): v3 integration test + bench treated `verify()`'s `Vec<bad_hashes>` as a count |
| `d1e55ff` | test(v3): segment benchmark example + Db-level integration tests |
| `cfdd6c9` | feat(store): NEDB v3 Phase 2 (compaction/pruning) + Phase 3 (`.idx`); bump to 2.3.3 |
| `3888267` | feat(store): NEDB v3 segment/pack ObjectStore behind `--dag-v3` (default off) |

---

## v3 in the wild — itcd integration (downstream)

itcd (Bitcoin Core 0.21 fork; NEDB replaces LevelDB for chainstate + block index via `nedb-ffi`) now runs on the v3 segment store via a new `-dagv3` flag.

| Commit / PR | Summary |
|---|---|
| `52684625` (itcd #55) | feat(nedb): itcd `-dagv3` — v3 segment store via FFI |
| `ea2c178` | nedb-ffi: pin `nedb-engine` @ `v2.3.33`; add `nedb_set_dag_v3()`; `dbwrapper_nedb.cpp` flips it before `nedb_open`; register `-dagv3` in `init.cpp` |

**Measured win** (real chainstate `FlushStateToDisk`, Windows node, `-dagv3`):

| Flush | v3 segments | v2 loose |
|---|---|---|
| 2,002 coins / 275 kB | **1.93 s** | _minutes_ |
| 2,549 coins / 366 kB | **1.71 s** | _minutes_ |

Larger batch, less time — v3 cost is one `fsync` per batch, not per object. The old loose store's ~185 writes/s metadata ceiling is gone.

---

## Agent PRs

| Repo | PR | Title |
|---|---|---|
| nedb | #10–#13 | NEDB v3 Phases 1–3 (segment store, compaction/pruning, `.idx` sidecars) + benchmark/integration tests |
| nedb | #14 | cargo-test green: Windows-safe id-index, durable `Drop`, idempotent write → tag `v2.3.33` |
| nedb | _this PR_ | docs(v3): comprehensive README v3 section + COMMITS.md + ideas.md → tag `v2.3.333` |
| itcd | #55 | feat(nedb): `-dagv3` — chainstate/block-index on the NEDB v3 segment store via FFI |
