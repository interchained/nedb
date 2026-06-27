# NEDB — Next-Turn Ideas

Grounded in the current state (**v2.4.468** — the 3-distribution split ships green: `nedb-engine` + `crypto-database` + `aof-db` on one tag across npm/PyPI/crates, distro npm now bundles macOS addons, and `scripts/release.py "vFROM" "vTO"` is the one-command release path). The distributions are real and aligned — but still **byte-identical engines** under three names. Each: one line _what_ + one line _why_.

---

### 1. Per-distro defaults — make crypto-database and aof-db diverge out of the box (no flags)
**What:** flip the engine defaults per distribution at the wrapper seam (`rust/crates/<distro>/src/lib.rs`): `crypto-database` defaults to the verifiable v2/v3 content-addressed DAG (verify / AS OF / TRACE on), `aof-db` defaults to the fast append-only path — so each product behaves as its name promises with zero flags.
**Why:** the 3-distribution infrastructure now ships green and identical; the entire point of the split was differentiated defaults, and shipping three identical engines under three names is only justified once they actually behave differently.

### 2. Give `scripts/release.py` a pre-flight registry name-availability check
**What:** before bumping/tagging, probe npm / PyPI / crates.io for each product's TO-version name — including npm's hyphen-insensitive "too similar" normalization — and abort with a clear message if a name is taken or too similar.
**Why:** the npm 403 (`nitrodb`/`cryptodb` "too similar to existing `nitro-db`/`crypto-db`") burned several immutable version numbers this cycle; a two-second pre-flight would have caught it before a tag was ever spent.

### 3. De-collide the per-distro `nedbd-v2` server binaries on the shared release
**What:** name the Codemagic-uploaded daemon per distro (e.g. `nedbd-v2-<distro>-darwin-arm64`) and have `distro-publish-npm` fetch its own; today every distro + the flagship upload `nedbd-v2-darwin-arm64` to the same release with `--clobber`, so the last writer wins.
**Why:** the napi `.node` addons are already distro-distinct and now assembled correctly into each npm tarball, but the bundled `nedbd-v2` daemon is not — a distro package can silently ship another product's server binary.

---

_Longer horizon: compaction end-to-end (engine `compact()` → `nedb_compact()` FFI → itcd `-dagcompact` gate) so the v3 chainstate prunes dead UTXO versions instead of bloating toward all history; reconcile `SPEC.md` §2 (still the v1 op-log model) with the shipped v2 content-addressed engine; make `--dag-v3` the default after compaction lands._
