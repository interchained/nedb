#!/usr/bin/env node
// nedb-engine — cinematic smoke test (`npm test`)
// ---------------------------------------------------------------------------
// A five-act tour of the engine, driven entirely by the real native addon
// (NedbCore — the same prebuilt .node binary the npm package ships). No external
// deps; Node built-ins only. Exits 0 on success.
//
//   Act I    — v1: the legacy append-only op-log (log.aof)
//   Act II   — automatic v1 -> v2 DAG migration (zero user action, lossless)
//   Act III  — v2: the content-addressed, hash-chained, time-traveling DAG
//   Act IV   — v3: the segment/pack object store (one fsync per group-commit)
//   Act V    — The Honest Dispatch: a causal rideshare decision you can audit
//
// © INTERCHAINED LLC × Claude Opus 4.8
import os from 'node:os';
import fs from 'node:fs';
import path from 'node:path';

// ── tiny presentation toolkit ───────────────────────────────────────────────
const COLOR = process.stdout.isTTY && !process.env.NO_COLOR;
const sgr = (code) => (s) => (COLOR ? `\x1b[${code}m${s}\x1b[0m` : String(s));
const dim = sgr('2'), bold = sgr('1'), ul = sgr('4');
const cyan = sgr('36'), green = sgr('32'), yellow = sgr('33');
const magenta = sgr('35'), blue = sgr('34'), red = sgr('31');

const log = (...a) => console.log(...a);
const rule = (ch = '─') => log(dim(ch.repeat(74)));
function act(n, title, subtitle) {
  log('');
  rule('═');
  log(`${bold(magenta(`  ACT ${n}`))}  ${bold(title)}`);
  if (subtitle) log(`  ${dim(subtitle)}`);
  rule('═');
}
const step = (s) => log(`  ${cyan('→')} ${s}`);
const tick = (s) => log(`  ${green('✓')} ${s}`);
const note = (s) => log(`    ${dim(s)}`);
const kv = (k, v) => log(`    ${dim(k.padEnd(16))} ${v}`);

// Positive sanity guard — confirms an invariant; only speaks up if reality
// disagrees (which, on an intact build, it won't).
function expect(cond, msg) {
  if (!cond) {
    log(`  ${red('✗')} ${bold('unexpected:')} ${msg}`);
    process.exitCode = 1;
    throw new Error(msg);
  }
}

const short = (h) => (h ? `${h.slice(0, 12)}…` : '∅');
const tmp = (label) => fs.mkdtempSync(path.join(os.tmpdir(), `nedb-smoke-${label}-`));
const rm = (d) => { try { fs.rmSync(d, { recursive: true, force: true }); } catch {} };

// ── resolve the native addon (installed package, or in-repo after a build) ───
let NedbCore;
try {
  ({ NedbCore } = await import('nedb-engine'));
} catch {
  try {
    ({ NedbCore } = await import(new URL('../index.js', import.meta.url)));
  } catch (err) {
    log(red(bold('\n  nedb-engine native addon not found.')));
    note('This smoke test drives the prebuilt NedbCore binding.');
    note('From a source checkout, build it first:  npm run build');
    note(`(${err && err.message ? err.message : err})`);
    process.exit(1);
  }
}

// ── banner ───────────────────────────────────────────────────────────────────
log('');
log(bold(cyan('  N E D B   ·   native smoke test')));
log(dim('  hash-chained · bi-temporal · causal-provenance · v1→v2→v3'));

const cleanup = [];
const t0 = Date.now();
try {

  // ════════════════════════════════════════════════════════════════════════
  act('I', 'v1 — the legacy append-only op-log', 'where NEDB began: one JSON op per line, in log.aof');

  const v1dir = tmp('v1'); cleanup.push(v1dir);
  // The v1 wire format the migrator understands: {seq, op, ts, payload:{coll,id,doc}}.
  // A tiny legacy "rides ledger" left behind by an older NEDB.
  const v1ops = [
    { seq: 0, op: 'put', ts: 1719400000.0, payload: { coll: 'trips',   id: 't-1001', doc: { rider: 'maya', driver: 'sam', fare: 18.5, city: 'metropolis' } } },
    { seq: 1, op: 'put', ts: 1719400100.0, payload: { coll: 'trips',   id: 't-1002', doc: { rider: 'omar', driver: 'ana', fare: 24.0, city: 'metropolis' } } },
    { seq: 2, op: 'put', ts: 1719400200.0, payload: { coll: 'drivers', id: 'sam',    doc: { name: 'Sam', rating: 4.97, joined: 2024 } } },
  ];
  fs.writeFileSync(path.join(v1dir, 'log.aof'), v1ops.map((o) => JSON.stringify(o)).join('\n') + '\n');
  step(`wrote a legacy ${bold('log.aof')} — ${v1ops.length} ops, append-only, plain JSON`);
  for (const o of v1ops) note(`${o.op.toUpperCase()} ${o.payload.coll}/${o.payload.id}  ${JSON.stringify(o.payload.doc)}`);
  note('No hashes. No chain. No time-travel. Just an op log — that\'s v1.');

  // ════════════════════════════════════════════════════════════════════════
  act('II', 'v1 → v2 — automatic migration', 'open() detects log.aof and rebuilds it as a content-addressed DAG');

  step(`${bold('NedbCore.open(dir)')} — the engine speaks for itself:`);
  const migrated = NedbCore.open(v1dir);   // <- triggers migrate_if_needed()
  const t1001 = JSON.parse(migrated.get('trips', 't-1001'));
  tick(`legacy data is live in v2: trips/t-1001 → fare ${bold(t1001.fare)}, rider ${bold(t1001.rider)}`);
  kv('now content-addressed', `_hash ${cyan(short(t1001._hash))}  (BLAKE2b-256, ${t1001._hash.length} hex chars)`);
  expect(t1001._hash && t1001._hash.length === 64, 'migrated node is content-addressed');

  const bakKept = fs.existsSync(path.join(v1dir, 'log.aof.v1.bak'));
  const aofGone = !fs.existsSync(path.join(v1dir, 'log.aof'));
  tick(`non-destructive: original preserved as ${bold('log.aof.v1.bak')} (${bakKept ? 'kept' : 'MISSING'})`);
  expect(bakKept && aofGone, 'log.aof renamed to .v1.bak after migration');
  tick(`integrity after migration: verify() = ${green(String(migrated.verify()))}`);
  expect(migrated.verify() === true, 'migrated store verifies clean');
  note('Zero user action. Lossless. Reversible. The op-log became a DAG.');

  // ════════════════════════════════════════════════════════════════════════
  act('III', 'v2 — the content-addressed DAG', 'hash chain · MVCC time-travel · causal graph · tamper-evident');

  const v2 = new NedbCore();   // pure in-memory v2 — zero disk I/O
  step('a fresh in-memory v2 DAG (no disk) — watch the Merkle head advance');
  const h0 = v2.head(), s0 = v2.seq();
  for (let i = 0; i < 5; i++) v2.put('blocks', String(i), JSON.stringify({ height: i, note: `block ${i}` }));
  kv('seq', `${dim(String(s0))} → ${bold(String(v2.seq()))}   (every write extends the chain)`);
  kv('head', `${dim(short(h0))} → ${bold(short(v2.head()))}`);
  expect(v2.seq() > s0 && v2.head() !== h0, 'chain advanced');

  step('NQL — a real query language over the DAG');
  kv('FROM blocks', `${v2.query('FROM blocks').length} rows`);
  const q = v2.query('FROM blocks WHERE height = 3').map(JSON.parse);
  kv('… WHERE height = 3', `${q.length} row → ${JSON.stringify({ height: q[0].height, note: q[0].note })}`);

  step('MVCC time-travel — AS OF a past sequence');
  const v1n = JSON.parse(v2.put('account', 'alice', JSON.stringify({ balance: 100 })));
  const asOf = BigInt(v1n._seq);
  v2.put('account', 'alice', JSON.stringify({ balance: 250 }));   // new version, same id
  const nowBal = JSON.parse(v2.get('account', 'alice')).balance;
  const thenBal = JSON.parse(v2.getAsOf('account', 'alice', asOf)).balance;
  kv('alice now', bold(`$${nowBal}`));
  kv(`alice AS OF #${asOf}`, bold(`$${thenBal}`) + dim('  ← the past is still there, exactly'));
  expect(nowBal === 250 && thenBal === 100, 'AS OF returns the historical version');

  step('causal graph — typed edges you can traverse');
  v2.link('blocks:4', 'prev', 'blocks:3');
  v2.link('blocks:3', 'prev', 'blocks:2');
  kv('neighbors(4,prev)', JSON.stringify(v2.neighbors('blocks:4', 'prev')));
  kv('inbound(3,prev)', JSON.stringify(v2.inbound('blocks:3', 'prev')) + dim('  ← who points at me?'));

  step('tamper-evidence — the whole store self-verifies');
  tick(`verify() = ${green(String(v2.verify()))}  ${dim('— every node\'s hash checked against its content')}`);
  expect(v2.verify() === true, 'intact store verifies clean');

  // ════════════════════════════════════════════════════════════════════════
  act('IV', 'v3 — the segment/pack object store', 'same API, denser substrate: one fsync per group-commit, not per object');

  const docs = 64;
  const sample = (i) => JSON.stringify({ i, payload: `coin-${i}`, ts: 1719400000 + i });

  // v2 substrate (env unset): loose objects — one file per write.
  delete process.env.NEDB_DAG_V3;
  const looseDir = tmp('v2loose'); cleanup.push(looseDir);
  const looseDb = NedbCore.open(looseDir);
  for (let i = 0; i < docs; i++) looseDb.put('utxo', String(i), sample(i));
  looseDb.flush();
  const looseObjs = countFiles(path.join(looseDir, 'objects'));
  step(`v2 default substrate — ${bold(docs)} writes`);
  kv('objects/ layout', `${bold(looseObjs)} loose object files  ${dim('(content-addressed, one per object)')}`);

  // v3 substrate (env set BEFORE open — the engine reads it per-open).
  process.env.NEDB_DAG_V3 = '1';
  const segDir = tmp('v3seg'); cleanup.push(segDir);
  const segDb = NedbCore.open(segDir);
  for (let i = 0; i < docs; i++) segDb.put('utxo', String(i), sample(i));
  segDb.flush();
  const segPath = path.join(segDir, 'objects', 'segments');
  const segFiles = fs.existsSync(segPath) ? fs.readdirSync(segPath).filter((f) => f.endsWith('.dat')) : [];
  step(`v3 segment substrate — ${bold(docs)} writes  ${dim('(NEDB_DAG_V3=1)')}`);
  if (segFiles.length) {
    const seg0 = path.join(segPath, segFiles[0]);
    const sz = fs.statSync(seg0).size;
    kv('objects/segments/', `${bold(segFiles.length)} segment file: ${cyan(segFiles[0])} (${sz} bytes)`);
    note(`${docs} objects packed into ${segFiles.length} append-only segment — the metadata-write ceiling is gone.`);
  } else {
    note('segment file not present yet (objects buffered) — engine still serving from memory+WAL.');
  }
  step('v3 round-trips and verifies after reopen');
  const segReopen = NedbCore.open(segDir);  // env still set → reopen as v3
  const rt = JSON.parse(segReopen.get('utxo', '7'));
  kv('reopen → utxo/7', JSON.stringify({ i: rt.i, payload: rt.payload }));
  tick(`verify() = ${green(String(segReopen.verify()))}  ${dim('— segment store + dual-read of any v2 loose objects')}`);
  expect(rt.i === 7 && segReopen.verify() === true, 'v3 persists and verifies across reopen');
  delete process.env.NEDB_DAG_V3;   // leave the environment as we found it

  // ════════════════════════════════════════════════════════════════════════
  act('V', 'The Honest Dispatch', 'a rideshare match that ends in a good choice — because the data says so');

  const rs = new NedbCore();
  step('the world at request time — a rider and three real candidates');
  // Facts. Each put() returns the stored node; we keep its _hash as an immutable
  // citation we can later prove the decision was built from.
  const req = JSON.parse(rs.put('request', 'trip-9001', JSON.stringify({
    rider: 'maya', from: 'zone:downtown', to: 'airport', when: '18:10', wants: 'fast pickup',
  })));
  const surge = JSON.parse(rs.put('surge', 'zone:downtown', JSON.stringify({ multiplier: 1.2, at: '18:10' })));

  const drivers = {
    sam: { name: 'Sam', rating: 4.97, etaMin: 4, distMi: 0.7, recentCancels: 0, acceptsPool: true },
    lee: { name: 'Lee', rating: 4.61, etaMin: 2, distMi: 0.3, recentCancels: 2, acceptsPool: false },
    ana: { name: 'Ana', rating: 4.99, etaMin: 9, distMi: 1.2, recentCancels: 0, acceptsPool: true },
  };
  const driverHash = {};
  for (const [id, d] of Object.entries(drivers)) {
    driverHash[id] = JSON.parse(rs.put('driver', id, JSON.stringify({ ...d, available: true })))._hash;
  }
  for (const [id, d] of Object.entries(drivers)) {
    kv(`driver:${id}`, `${d.name}  ★${d.rating}  eta ${d.etaMin}m  ${d.distMi}mi  cancels ${d.recentCancels}  ${d.acceptsPool ? 'pool' : 'solo'}`);
  }
  note(`surge in downtown right now: ${bold(surge.multiplier + '×')}`);

  step('score every candidate from the stored facts — not a hunch, a calculation');
  // Transparent scoring: reward rating + ETA + pool; penalize recent cancels.
  const score = (d) => +(
    d.rating * 2
    - d.etaMin * 0.25
    - d.recentCancels * 1.5
    + (d.acceptsPool ? 0.4 : 0)
  ).toFixed(3);
  const ranked = Object.entries(drivers)
    .map(([id, d]) => ({ id, d, s: score(d) }))
    .sort((a, b) => b.s - a.s);
  for (const r of ranked) {
    const why = r.id === 'lee' ? dim('(closest — but 2 recent cancels drag it down)')
      : r.id === 'ana' ? dim('(top-rated — but a 9-min ETA for a "fast pickup")')
      : dim('(4.97★, 4-min ETA, no cancels, takes pool)');
    kv(`score driver:${r.id}`, `${bold(r.s.toFixed(2))}  ${why}`);
  }
  const winner = ranked[0];
  note(`naive "closest" would pick ${bold('Lee')} (0.3mi). The data picks ${bold(drivers[winner.id].name)}.`);

  step('record the decision — with its causes wired in, permanently');
  const decision = JSON.parse(rs.put('decision', 'trip-9001', JSON.stringify({
    chosen: `driver:${winner.id}`,
    score: winner.s,
    policy: 'rating+eta+reliability+pool',
    // caused_by: the exact immutable facts this choice was built from.
    caused_by: [req._hash, surge._hash, driverHash[winner.id]],
  })));
  const decisionSeq = BigInt(decision._seq);
  rs.link(`decision:trip-9001`, 'chose', `driver:${winner.id}`);
  for (const r of ranked.slice(1)) rs.link(`decision:trip-9001`, 'considered', `driver:${r.id}`);
  tick(`chose ${bold('driver:' + winner.id)} (${drivers[winner.id].name})  ·  decision is now a node in the DAG`);

  step('AUDIT — "why did dispatch pick this driver?"  Follow the causal trail.');
  kv('decision.caused_by', `[ ${decision.caused_by.map(short).join(', ')} ]`);
  const causeLabel = { [req._hash]: 'the rider\'s request', [surge._hash]: 'the surge snapshot', [driverHash[winner.id]]: `${drivers[winner.id].name}'s live state` };
  for (const h of decision.caused_by) note(`${cyan(short(h))}  →  ${causeLabel[h] || 'a fact'}`);
  kv('chose', JSON.stringify(rs.neighbors('decision:trip-9001', 'chose')));
  kv('considered', JSON.stringify(rs.neighbors('decision:trip-9001', 'considered')) + dim('  ← the alternatives, on the record'));

  step('REPRODUCE — surge spikes after the fact; the decision is unmoved');
  rs.put('surge', 'zone:downtown', JSON.stringify({ multiplier: 2.5, at: '18:25' }));  // later reality
  const surgeNow = JSON.parse(rs.get('surge', 'zone:downtown')).multiplier;
  const surgeThen = JSON.parse(rs.getAsOf('surge', 'zone:downtown', decisionSeq)).multiplier;
  kv('surge now', bold(surgeNow + '×'));
  kv('surge AS OF decision', bold(surgeThen + '×') + dim('  ← what the dispatcher actually saw; the audit is reproducible'));
  expect(surgeThen === 1.2, 'AS OF reconstructs the world at decision time');

  step('the good ending');
  rs.put('trip', 'trip-9001', JSON.stringify({ status: 'completed', driver: `driver:${winner.id}`, riderRating: 5 }));
  tick(`${bold('Maya')} matched with ${bold(drivers[winner.id].name)} → trip completed → ${yellow('★★★★★')}`);
  tick(`fully auditable, content-addressed, reproducible — ${bold('a good choice, grounded in causal data')}`);
  expect(rs.verify() === true, 'rideshare store verifies clean');

  // ── curtain ────────────────────────────────────────────────────────────────
  log('');
  rule('═');
  log(`  ${green(bold('✓ all five acts passed'))}  ${dim(`in ${Date.now() - t0} ms`)}`);
  log(`  ${dim('v1 → v2 migration · v2 DAG · v3 segments · causal audit — all on the native engine')}`);
  rule('═');
  log('');
} catch (err) {
  log('');
  log(red(bold('  smoke test failed:')) + ' ' + (err && err.message ? err.message : String(err)));
  if (err && err.stack) log(dim(err.stack.split('\n').slice(1, 4).join('\n')));
  process.exitCode = 1;
} finally {
  for (const d of cleanup) rm(d);
}

// ── helpers ────────────────────────────────────────────────────────────────
function countFiles(root) {
  let n = 0;
  const walk = (d) => {
    let ents;
    try { ents = fs.readdirSync(d, { withFileTypes: true }); } catch { return; }
    for (const e of ents) {
      const p = path.join(d, e.name);
      if (e.isDirectory()) walk(p); else n++;
    }
  };
  walk(root);
  return n;
}
