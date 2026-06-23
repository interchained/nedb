# NEDB Mini-Chain

> The database where your data **is** a blockchain.

A ~80-line runnable micro case-study for `nedb-engine`. It tells NEDB's whole
story in one gamified console run — and uses the exact primitives
(`caused_by` / `AS OF` / `verify`) that back the real ITC node (**itcd**),
where NEDB replaces LevelDB as the block-index + chainstate.

## Run

```bash
npm install nedb-engine
node mini-chain.mjs            # default 5,000 blocks
node mini-chain.mjs 50000      # crank it
```

## What it demonstrates

| Step | NEDB feature | Why it matters |
|------|--------------|----------------|
| ⛏ Mine N blocks | content-addressed writes | one doc per block; hash-chained automatically |
| ⚡ Read them back | indexed `get` | real writes/sec + reads/sec from *your* machine |
| ⏳ Time-travel | `getAsOf(seq)` (MVCC) | read any block as it was at a past sequence |
| 🧭 Provenance | `link` / `neighbors` (causal DAG) | walk a block's `prev` edge |
| 🛡 Tamper-evidence | `verify()` (BLAKE2b) | corrupt a block on disk → `verify()` flips to `false` |

The tamper step is **real**: it flips a byte in a block's on-disk object and
re-opens the store — `verify()` catches the BLAKE2b mismatch. You can't fake
history in NEDB.

## Storage strategy

One document per block in a `blocks` collection, keyed by height, each carrying
`prev`. Time-travel, causal provenance, and tamper-proofing come **free** — no
extra tables, no external indexer. That is the same model itcd uses for the ITC
chain.

*© Interchained LLC — GPL-3.0-or-later*
