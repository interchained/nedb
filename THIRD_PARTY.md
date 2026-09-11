# Third-party licenses

**The Change License does not relicense dependencies.** When NEDB 4.0.0 becomes
Apache-2.0 on 2030-09-11, that applies to NEDB's own source. Every dependency
keeps its own terms, permanently. This file is the inventory of those terms.

Generated from real dependency metadata, not by hand — regenerate with the
command at the bottom whenever the dependency graph moves.

---

## Rust (`rust/` — the engine, the CLI, the napi and PyO3 bindings)

Third-party crates in the dependency graph: **206**

| Crates | SPDX expression |
|---:|---|
| 105 | `MIT OR Apache-2.0` |
| 38 | `MIT` |
| 18 | `Unicode-3.0` |
| 13 | `Apache-2.0 OR MIT` |
| 8 | `MIT/Apache-2.0` |
| 4 | `Apache-2.0` |
| 3 | `Unlicense OR MIT` |
| 3 | `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT` |
| 2 | `BSD-3-Clause` |
| 2 | `Unlicense/MIT` |
| 2 | `BSD-2-Clause OR Apache-2.0 OR MIT` |
| 1 | `CC0-1.0 OR Apache-2.0 OR Apache-2.0 WITH LLVM-exception` |
| 1 | `CC0-1.0 OR MIT-0 OR Apache-2.0` |
| 1 | `ISC` |
| 1 | `MIT AND BSD-3-Clause` |
| 1 | `MIT OR Apache-2.0 OR LGPL-2.1-or-later` |
| 1 | `Apache-2.0 OR BSL-1.0` |
| 1 | `Apache-2.0 WITH LLVM-exception` |
| 1 | `(MIT OR Apache-2.0) AND Unicode-3.0` |

**No copyleft obligation anywhere in this graph.** Every `OR` expression above
offers a permissive option, and NEDB takes it:

- `r-efi` offers `LGPL-2.1-or-later` as one of three options alongside `MIT`
  and `Apache-2.0`. We take MIT/Apache-2.0. It is also a `std`-internal
  transitive dependency, not something NEDB links directly.
- `Unicode-3.0` (18 crates, the `icu_*` family via `idna`) is a permissive
  Unicode data license. It requires the copyright notice be retained, which
  distributing this file satisfies.
- `Unlicense`, `CC0-1.0`, `MIT-0`, `ISC`, `BSD-2-Clause`, `BSD-3-Clause` and
  `BSL-1.0` are all permissive.
- `Apache-2.0 WITH LLVM-exception` is Apache-2.0 with the static-linking
  exception, which is strictly more permissive.
- `MIT/Apache-2.0` and `Unlicense/MIT` are the pre-SPDX spellings of the same
  dual grant. They mean `OR`.

**No `GPL`, `AGPL`, `SSPL` or `BUSL` third-party dependency exists in this
tree.** The BUSL-1.1 crates cargo reports (`nedb-engine`, `nedb-core`,
`nedb-node`, `nedb-py`, `nedb-wrap`, `nedb-cast-slm`) are all
Interchained-owned, which is this project's own license, not an inbound
obligation.

## Python (`nedb-engine` on PyPI)

| Package | License | Required? |
|---|---|---|
| `pycryptodome >= 3.19` | BSD-2-Clause + Public Domain | **required** — AES-256-GCM encryption at rest |
| `cryptography >= 41` | `Apache-2.0 OR BSD-3-Clause` | optional (`[encryption]` extra) — alternative backend, used only if pycryptodome is absent |

The pure-Python engine has **no other runtime dependency**. Everything else it
uses is in the standard library, deliberately: a database that is a pain to
install does not get installed.

## Node (`nedb-engine` on npm)

**No runtime dependencies.** The published package is the prebuilt native addon
plus a hand-written JS wrapper.

Build-time only, and not redistributed:

| Package | License | Used for |
|---|---|---|
| `@napi-rs/cli` | MIT | building the per-platform native addon |
| `typescript` | Apache-2.0 | type declarations for the HTTP client |

## Test-time only, never redistributed

These appear in CI and in local test runs. They are not dependencies of any
published artifact. Each license below was read out of the installed package's
own metadata (`License-Expression`, or the bundled `LICENSE` file where the
package exposes none) rather than recalled.

| Package | License | Used for |
|---|---|---|
| `psycopg2-binary` | LGPL with exceptions | driving the pgwire endpoint as a real libpq client |
| `psycopg` (v3) | LGPL-3.0-only | the extended query protocol suite |
| `asyncpg` | Apache-2.0 | the extended query protocol suite |
| `pgserver` | Apache-2.0 | a real PostgreSQL 16.2 server for the shadowing suite |
| `fakeredis` | BSD-3-Clause | `wrap_redis` without a Redis server |
| `maturin` | `MIT OR Apache-2.0` | building the native Python wheel |

`psycopg2`'s LGPL is worth naming rather than leaving to be discovered: it is a
**test** dependency. NEDB does not link it, ship it, or require it at runtime —
the pgwire endpoint is a server, and psycopg is one of the clients used to
prove it works.

---

## Regenerating this file

```sh
cargo metadata --manifest-path rust/Cargo.toml --format-version 1 --all-features \
  | python3 -c '
import json, sys, collections
m = json.load(sys.stdin)
OWN = {"nedb-engine","nedb-core","nedb-node","nedb-py","nedb-wrap","nedb-cli","nedb-cast-slm"}
lic = collections.Counter()
for p in m["packages"]:
    if p["name"] in OWN: continue
    lic[p.get("license") or "UNKNOWN"] += 1
for k, v in lic.most_common(): print(f"{v:5}  {k}")'
```

If that ever prints `UNKNOWN`, or any expression whose every option is
copyleft, stop and resolve it before shipping — an unknown license is worse
than a restrictive one in a procurement review, because "unknown" cannot be
approved by policy.

---

*SPDX-FileCopyrightText: 2026 INTERCHAINED LLC*
*SPDX-License-Identifier: BUSL-1.1*
