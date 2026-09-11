#!/usr/bin/env node
// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

// nedb-engine — embedded durability under SIGKILL (2.8.5)
// ---------------------------------------------------------------------------
// The contract: an acknowledged write is on disk within one manifest tick (default 1 s) or at
// exit, whichever comes first. This test proves the tick half the only way that means anything:
// a child process opens a durable db, puts a document, gets the hash back, waits past one tick,
// and is killed with SIGKILL — no exit hook can run. The parent then opens the same directory
// and must find the document.
//
// Negative control: the same sequence with NEDB_FLUSH_MS=0 (ticker disabled) must LOSE the
// document — otherwise this test would pass for reasons unrelated to the ticker.
// Wrapper case: ticker off + graceful SIGTERM must KEEP the document — the exit-flush wrapper's job.
//
// © INTERCHAINED LLC
import os from 'node:os';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';

let NedbCore, addonSpecifier;
try { ({ NedbCore } = await import('nedb-engine')); addonSpecifier = 'nedb-engine'; }
catch { addonSpecifier = new URL('../index.js', import.meta.url).href; ({ NedbCore } = await import(addonSpecifier)); }

const CHILD = `
  const { NedbCore } = await import(process.argv[1]); // node -e: argv[1] is the first user arg
  const db = NedbCore.open(process.argv[2]);
  const out = db.put('durability', 'doc-1', JSON.stringify({ written_at: Date.now(), note: 'must survive SIGKILL' }));
  process.stdout.write('PUT ' + JSON.parse(out)._hash + '\\n');
  setInterval(() => {}, 1 << 30); // stay alive until killed
`;

function runChild(dir, env) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--input-type=module', '-e', CHILD, addonSpecifier, dir], { env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'inherit'] });
    let buf = '';
    child.stdout.on('data', (d) => { buf += d; const m = buf.match(/PUT ([0-9a-f]+)/); if (m) resolve({ child, hash: m[1] }); });
    child.on('exit', (code, sig) => reject(new Error(`child exited early (${code ?? sig}) — ${buf}`)));
    setTimeout(() => reject(new Error('child never acknowledged the put')), 15000).unref();
  });
}

const tmp = (l) => fs.mkdtempSync(path.join(os.tmpdir(), `nedb-dur-${l}-`));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const readBack = (dir) => { const db = NedbCore.open(dir); const raw = db.get('durability', 'doc-1'); return raw ? JSON.parse(raw) : null; };
let failed = 0;
const check = (ok, msg) => { console.log(`  ${ok ? '✓' : '✗'} ${msg}`); if (!ok) failed++; };

console.log('\n  N E D B  ·  embedded durability under SIGKILL');

// ── positive: default ticker (1000 ms) ─────────────────────────────────────
{
  const dir = tmp('tick');
  const { child, hash } = await runChild(dir, { NEDB_FLUSH_MS: '1000' });
  console.log(`  → child acknowledged put ${hash.slice(0, 12)}…; waiting 2.5 s (two ticks) then SIGKILL`);
  await sleep(2500);
  child.kill('SIGKILL'); await new Promise((r) => child.on('exit', r));
  const doc = readBack(dir);
  check(doc !== null && doc._hash === hash, `after SIGKILL + reopen the document is present with the acknowledged hash (${doc ? doc._hash.slice(0, 12) + '…' : 'GONE'})`);
  fs.rmSync(dir, { recursive: true, force: true });
}

// ── negative control: ticker disabled → the write must be lost ─────────────
{
  const dir = tmp('notick');
  const { child, hash } = await runChild(dir, { NEDB_FLUSH_MS: '0' });
  console.log(`  → control: NEDB_FLUSH_MS=0, put ${hash.slice(0, 12)}…, 2.5 s, SIGKILL`);
  await sleep(2500);
  child.kill('SIGKILL'); await new Promise((r) => child.on('exit', r));
  const doc = readBack(dir);
  check(doc === null, `with the ticker disabled the same sequence loses the write (${doc ? 'unexpectedly present' : 'gone, as the control requires'})`);
  fs.rmSync(dir, { recursive: true, force: true });
}

// ── wrapper: graceful SIGTERM with the ticker OFF → the exit flush must save it ──
{
  check(NedbCore.__exitFlushWrapped === true, 'index.js is the durable-mode wrapper (not the generated loader)');
  const dir = tmp('term');
  const { child, hash } = await runChild(dir, { NEDB_FLUSH_MS: '0' });
  console.log(`  → wrapper: NEDB_FLUSH_MS=0, put ${hash.slice(0, 12)}…, SIGTERM (graceful)`);
  child.kill('SIGTERM'); await new Promise((r) => child.on('exit', r));
  const doc = readBack(dir);
  check(doc !== null && doc._hash === hash, `after SIGTERM the exit-flush wrapper made the write durable (${doc ? 'present' : 'GONE'})`);
  fs.rmSync(dir, { recursive: true, force: true });
}

if (failed) { console.log(`\n  ${failed} check(s) failed`); process.exit(1); }
console.log('\n  durability contract holds: on disk within one tick, or at exit.\n');
