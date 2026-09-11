#!/usr/bin/env node
// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

// test/wrap_family.test.mjs — live proof for the JS wrap adapter family.
//
// Drives the REAL native NedbCore (Rust DAG) through every wrapper:
// wrapRedis, wrapSqlite, wrapMysql, wrapPg, wrapMongo — engine selection,
// backfill, shadowing, NQL queryability, verify().
//
//   node --experimental-vm-modules test/wrap_family.test.mjs
// (plain `node` works — no VM modules needed; kept for parity with other tests)
//
// © INTERCHAINED LLC × Claude Sonnet 4.6
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);

// resolve the native addon (installed package, or repo build)
let NedbCore;
try { ({ NedbCore } = await import('nedb-engine')); }
catch { ({ NedbCore } = await import(new URL('../index.js', import.meta.url).href)); }
assert(NedbCore, 'native addon required for this proof');

const wrap = require('../wrap/index.js');

const PASS = [], FAIL = [];
const check = (name, cond, detail = '') => {
  (cond ? PASS : FAIL).push(name);
  console.log(`  ${cond ? '✓' : '✗'} ${name}${detail ? ` — ${detail}` : ''}`);
};
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'nedb-wrap-'));

console.log(`nedb wrap family — native DAG: ${!!NedbCore}`);

// ═══ 1. wrapRedis — embedded DAG, full loop ═════════════════════════════════
console.log('\n[1] wrapRedis — embedded DAG');

class FakeRedis {
  constructor() { this.store = new Map(); }
  set(k, v) { this.store.set(k, v); return 'OK'; }
  get(k) { return this.store.has(k) ? this.store.get(k) : null; }
  keys(pattern) {
    const rx = new RegExp('^' + pattern.replace(/\*/g, '.*') + '$');
    return [...this.store.keys()].filter((k) => rx.test(k));
  }
  hgetall(k) { try { return JSON.parse(this.store.get(k)); } catch { return {}; } }
  // async client shape (node-redis v4) — used via Promise path
  // not needed for the sync fake
}

const fr = new FakeRedis();
const r = wrap.wrapRedis(fr, { dbName: 'proof', dagPath: tmp(), native: NedbCore });
check('engineKind == dag-embedded', r.nedb.engineKind === 'dag-embedded', r.nedb.engineKind);

r.nedb.register('driver:*', 'driver');
const imported = r.nedb.backfill();
check('backfill on empty store', imported === 0, `${imported}`);

r.nedb.shadowWrites = true;
fr.set('driver:d1', JSON.stringify({ name: 'Bob', status: 'active' }));
fr.set('driver:d2', JSON.stringify({ name: 'Ann', status: 'active' }));
// explicit shadow via the surface (args after the key — same as the interceptor)
r.nedb.shadow('set', 'driver:d1', fr.get('driver:d1'));
r.nedb.shadow('set', 'driver:d2', fr.get('driver:d2'));

const docs = r.nedb.query('FROM driver WHERE status = "active"');
check('shadowed writes are NQL-queryable', docs.length === 2, `${docs.length} docs`);
check('DAG verify() after shadowed writes', r.nedb.verify() === true);
check('tip() is latest node', r.nedb.tip() && r.nedb.tip()._id === 'd2');
const feed = r.nedb.since(0, 100);
check('since() changefeed', feed.head_seq >= 1 && feed.nodes.length >= 1,
  `head_seq=${feed.head_seq} nodes=${feed.nodes.length}`);

// interception actually wraps client methods
check('client method intercepted', fr.set.__nedbWrapped === true);

// ═══ 2. wrapSqlite — better-sqlite3-shape + DAG ═════════════════════════════
console.log('\n[2] wrapSqlite — embedded DAG (better-sqlite3 shape)');

class FakeSqlite {
  constructor() {
    this.rows = new Map();
    this.nextId = 1;
  }
  prepare(sql) {
    const self = this;
    if (/SELECT rowid/.test(sql)) {
      return {
        * iterate() {
          for (const [id, row] of self.rows) {
            yield { __nedb_rowid: id, ...row };
          }
        },
      };
    }
    return {
      run(...args) {
        const id = self.nextId++;
        self.rows.set(id, { name: args[0], status: args[1] });
        return { lastInsertRowid: id };
      },
      * iterate() { for (const [id, row] of self.rows) yield { __nedb_rowid: id, ...row }; },
    };
  }
}

const sq = new FakeSqlite();
sq.rows.set(1, { name: 'Bob', status: 'active' });
sq.rows.set(2, { name: 'Ann', status: 'active' });
const ws = wrap.wrapSqlite(sq, { dbName: 'proof_sqlite', dagPath: tmp(), native: NedbCore });
check('engineKind == dag-embedded', ws.nedb.engineKind === 'dag-embedded');
ws.nedb.register('drivers', 'driver');
const sqImported = ws.nedb.backfill();
check('sqlite backfill', sqImported === 2, `${sqImported} rows`);
const sqDocs = ws.nedb.query('FROM driver WHERE status = "active"');
check('backfilled rows NQL-queryable', sqDocs.length === 2, `${sqDocs.length} docs`);
ws.nedb.shadowWrites = true;
ws.nedb.shadowRow('drivers', 3, { name: 'Zoe', status: 'active' });
const zoe = ws.nedb.query('FROM driver WHERE name = "Zoe"');
check('shadowRow is queryable', zoe.length === 1, `${zoe.length} docs`);
check('DAG verify() after sqlite', ws.nedb.verify() === true);

// ═══ 3. wrapMysql / wrapPg — explicit shadowRow ═════════════════════════════
console.log('\n[3] wrapMysql / wrapPg — explicit shadowRow');
const fakeConn = { query: () => {}, execute: () => {} };
const wm = wrap.wrapMysql(fakeConn, { dbName: 'proof_mysql', dagPath: tmp(), native: NedbCore });
check('mysql engineKind', wm.nedb.engineKind === 'dag-embedded');
wm.nedb.register('drivers', 'driver');
wm.nedb.shadowWrites = true;
wm.nedb.shadowRow('drivers', 1, { id: 1, name: 'Bob', status: 'active' });
check('mysql shadowRow queryable', wm.nedb.query('FROM driver WHERE name = "Bob"').length === 1);
check('mysql verify()', wm.nedb.verify() === true);

const wp = wrap.wrapPg({ query: () => {} }, { dbName: 'proof_pg', dagPath: tmp(), native: NedbCore });
check('pg engineKind', wp.nedb.engineKind === 'dag-embedded');
wp.nedb.register('drivers', 'driver');
wp.nedb.shadowWrites = true;
wp.nedb.shadowRow('drivers', 1, { id: 1, name: 'Ann', status: 'active' });
check('pg shadowRow queryable', wp.nedb.query('FROM driver WHERE name = "Ann"').length === 1);
check('pg verify()', wp.nedb.verify() === true);

// ═══ 4. wrapMongo — explicit shadowRow ══════════════════════════════════════
console.log('\n[4] wrapMongo — explicit shadowRow');
const mg = wrap.wrapMongo({ db: () => ({ collection: () => ({}) }) },
  { dbName: 'proof_mongo', dagPath: tmp(), native: NedbCore });
check('mongo engineKind', mg.nedb.engineKind === 'dag-embedded');
mg.nedb.register('app.drivers', 'driver');
mg.nedb.shadowWrites = true;
mg.nedb.shadowRow('app.drivers', { _id: '1', name: 'Bob', status: 'active' });
check('mongo shadowRow queryable', mg.nedb.query('FROM driver WHERE name = "Bob"').length === 1);
check('mongo verify()', mg.nedb.verify() === true);

// ═══ summary ═════════════════════════════════════════════════════════════════
console.log(`\n${'='.repeat(60)}`);
console.log(`PROOF: ${PASS.length} passed, ${FAIL.length} failed`);
if (FAIL.length) { console.log('FAILED:', FAIL.join(' | ')); process.exit(1); }
console.log('ALL JS WRAPPERS PROVEN against the real embedded v2/v3 DAG engine.');
