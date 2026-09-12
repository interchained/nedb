<!--
SPDX-License-Identifier: BUSL-1.1
SPDX-FileCopyrightText: © 2026 INTERCHAINED LLC × Claude Sonnet 4.6
-->

# SQL evaluator baselines — nested loop vs hash join

Recorded at engine **4.0.0**, PRs #116, #117, #119 and #120, on the CI-class
sandbox this repo is developed in (Linux x86-64). Reproduce with:

```bash
cargo run --release --example sqlbench    # the workload table
cargo run --release --example fusebench   # filter fusion, measured directly
```

## Why this file exists

So that the next person to change join execution can tell whether they made it
faster or only believe they did. The inputs are generated from a **fixed seed**
inside `rust/nedb-v2/examples/sqlbench.rs`, so a number measured later is
comparable with a number measured here. That is the whole value — an absolute
millisecond count on unspecified data is not evidence of anything.

These are **not** marketing figures. The machine is a shared sandbox, the
timings move ±20% run to run, and the relation sizes are small because the
nested loop is quadratic and the largest shape has to finish. Read the
*ratios* and the *trend*, not the absolute milliseconds.

## How to read it honestly

* Rows marked `no join` have no join at all. Both columns time the same scan;
  the ratio is noise.
* Rows marked `both ran Nested Loop — no hash path available` have no provable
  equality key, so both columns time **the same code twice**. A ~1.00x here is
  the harness working correctly, not a disappointing result.
* The strategy in each row is read back from the execution report, not from
  the flag that was requested. A benchmark that times the nested loop twice
  while believing one run was a hash join would manufacture a speedup out of
  nothing, so the harness checks.
* Row counts are compared between the two strategies. A mismatch is printed as
  `!! ROW COUNT DIFFERS` rather than being reported as a speed difference,
  because two strategies answering different questions cannot be compared at
  all.

## Results

### orders=1000, customers=500, 250 distinct keys (median of 5)

| workload | nested (ms) | hash (ms) | speedup | rows |
|---|---:|---:|---:|---:|
| scan | 0.90 | 0.86 | — *(no join)* | 1000 |
| filtered scan | 0.68 | 0.67 | — *(no join)* | 117 |
| equality join | 366.62 | 4.05 | **90.6x** | 1922 |
| left join | 373.27 | 4.03 | **92.5x** | 1961 |
| join + selective pred | 3.66 | 1.02 | **3.6x** | 14 |
| join + broad pred | 336.34 | 4.68 | **71.9x** | 1750 |
| join + sort | 403.51 | 6.35 | **63.6x** | 1922 |
| join + limit | 5.13 | 0.97 | **5.3x** | 20 |
| non-equality join | 344.69 | 346.33 | — *(same path)* | 2500 |

### orders=3000, customers=1000, 500 distinct keys (median of 3)

| workload | nested (ms) | hash (ms) | speedup | rows |
|---|---:|---:|---:|---:|
| scan | 2.88 | 2.88 | — *(no join)* | 3000 |
| filtered scan | 2.21 | 2.26 | — *(no join)* | 320 |
| equality join | 2204.29 | 16.00 | **137.7x** | 5708 |
| left join | 2208.46 | 17.48 | **126.3x** | 5854 |
| join + selective pred | 23.09 | 2.72 | **8.5x** | 56 |
| join + broad pred | 1966.10 | 14.65 | **134.2x** | 5144 |
| join + sort | 2202.42 | 20.90 | **105.4x** | 5708 |
| join + limit | 10.37 | 2.57 | **4.0x** | 20 |
| non-equality join | 2079.45 | 2125.99 | — *(same path)* | 14000 |

### orders=8000, customers=1500, 1500 distinct keys (single run)

| workload | nested (ms) | hash (ms) | speedup | rows |
|---|---:|---:|---:|---:|
| scan | 8.09 | 7.64 | — *(no join)* | 8000 |
| filtered scan | 6.45 | 6.27 | — *(no join)* | 781 |
| equality join | 8776.76 | 28.77 | **305.0x** | 7578 |
| left join | 8837.90 | 30.17 | **293.0x** | 8000 |
| join + selective pred | 118.55 | 8.01 | **14.8x** | 64 |
| join + broad pred | 8145.32 | 36.81 | **221.3x** | 6816 |
| join + sort | 8804.33 | 36.91 | **238.5x** | 7578 |
| join + limit | 29.59 | 6.72 | **4.4x** | 20 |
| non-equality join | 8367.82 | 8569.10 | — *(same path)* | 40500 |

## What the numbers say

**The speedup grows with the input, which is the only part that really
matters.** 90x → 138x → 305x across the three shapes is the signature of
O(n·m) being replaced by O(n+m): the ratio is a function of relation size, so
it will keep widening. A fixed multiplier would have suggested a constant-factor
win instead, and that distinction is the difference between an optimisation and
a micro-optimisation.

**A nested-loop join over user collections was genuinely not shippable.** 8.8
seconds to join 8000 rows against 1500 is not a tuning problem, it is a wrong
algorithm — which is exactly why the nested loop proved the semantics and then
stopped being the only option.

**Scans are untouched**, as they should be: identical numbers in both columns
confirm this change is confined to join execution and did not perturb the
surrounding pipeline.

**`join + limit` was the most interesting remaining gap, and #117 closed it.**
`LIMIT` used to be applied *after* the whole join had been materialised, so
neither strategy stopped early. With the row budget, the join stops as soon as
it has enough rows:

| shape | nested before | nested after | hash before | hash after |
|---|---:|---:|---:|---:|
| 1000 x 500 | 368.21 | **5.13** | 4.21 | **0.97** |
| 3000 x 1000 | 2191.93 | **10.37** | 14.82 | **2.57** |
| 8000 x 1500 | 8848.69 | **29.59** | 32.12 | **6.72** |

At the largest shape that is **299x** off the nested loop and **4.8x** off the
hash join. Note what the speedup *column* now shows for that row: a mere 4.4x,
because both strategies got faster. The column compares strategies, not
releases — which is exactly why the before/after has to be stated separately
rather than read off the table.

The residual 6.7ms is almost entirely relation materialisation (the resolver
hands back an owned `Vec`), not join work. Removing that needs a streaming
resolver, not a cleverer join.

**`join + selective pred` was the next gap, and #119 closed it** with
predicate pushdown — a conjunct reading exactly one relation is pre-applied to
that relation before the join:

| shape | nested before | nested after | hash before | hash after |
|---|---:|---:|---:|---:|
| 1000 x 500 | 370.32 | **3.66** | 3.85 | **1.02** |
| 3000 x 1000 | 2229.06 | **23.09** | 12.77 | **2.72** |
| 8000 x 1500 | 8774.78 | **118.55** | 27.07 | **8.01** |

**76x** off the nested loop and **3.4x** off the hash join at the largest
shape. `join + broad pred` gains almost nothing (8929 -> 8145 nested), which is
exactly right: the predicate keeps 85% of the rows, so there is little to
remove. A selective predicate is where pushdown pays, and the two rows
together show the optimiser is doing something real rather than something
uniform.

Pushdown is **refused** when the relation can be NULL-synthesised, so an
outer-join query with a predicate on the nullable side sees no improvement —
and the plan says why. That is not a gap to close; it is the correctness
boundary.

**A `WHERE` clause used to disqualify the row budget entirely**, because
filtering happened after the join — so `... WHERE ... LIMIT 20` had to
materialise the whole join first. #120 fuses the filter INTO the final join,
which makes the join's own output already-filtered and therefore a true
prefix.

Measured directly by `examples/fusebench`, which runs the same query with
`fuse_filter` off and on and asserts both return the same row count:

```
SELECT o.id, c.name FROM orders o JOIN customers c
  ON o.customer_id = c.id WHERE o.amount > 500 LIMIT 20
```

| shape | unfused | fused | gain |
|---|---:|---:|---:|
| 1000 x 500, nested | 190.92 | **5.28** | 36.1x |
| 1000 x 500, hash | 3.23 | **1.21** | 2.7x |
| 3000 x 1000, nested | 1115.02 | **11.43** | 97.5x |
| 3000 x 1000, hash | 10.34 | **3.20** | 3.2x |
| 8000 x 1500, nested | 4657.18 | **34.20** | 136.2x |
| 8000 x 1500, hash | 19.90 | **11.22** | 1.8x |

Two things in that table are worth reading carefully.

The nested-loop gain **grows** with size (36x -> 98x -> 136x), which is the
expected signature of replacing "materialise everything, then take 20" with
"stop at 20".

The hash-join gain **shrinks** at the largest shape (2.7x -> 3.2x -> 1.8x),
and that is not noise — it is the next bottleneck becoming visible. At
8000 x 1500 the hash path spends most of its 11ms materialising relations
rather than probing, and stopping the probe early cannot recover time already
spent cloning 9500 rows. That is the argument for a streaming `Resolver`,
stated by measurement rather than by intuition.

Note also that the unfused nested figure (4657ms) is well below the
`join + broad pred` figure (8055ms) for a comparable predicate: pushdown has
already halved `orders` before the join runs. The two optimisations compose.

**`join + broad pred` and `join + sort` gain least**, which makes sense —
their cost is dominated by materialising and then sorting ~5–7k output rows,
not by finding the matches. Predicate pushdown is the lever there, and it has
to be done conservatively: a predicate on the nullable side of a `LEFT JOIN`
means something different in `WHERE` than in `ON`, and both spellings are
pinned in `tests/sql_semantics_corpus.rs`.

## Caveats worth stating

* The `Resolver` contract hands back an owned `Vec<Value>`, so every execution
  clones the whole relation before doing any work. That cost is charged to
  both strategies equally and is real in production too, but it means these
  numbers include materialisation and are **not** a measure of join throughput
  alone.
* Key cardinality is a controlled variable (`distinct keys`), because bucket
  depth drives hash-join cost. A single hot key degenerates a hash join toward
  the nested loop's pair count; that case is covered for *correctness* in the
  differential tests but is not benchmarked here yet.
* Timings are medians, not minimums, and the largest shape is a single run.
  Treat sub-2x differences as noise.
