// nedb-engine — Node/napi binding test suite
// Run: node --test nedb-engine.test.mjs
//
// The engine's main test suite is Python; this closes the gap by exercising the
// real napi surface (NedbCore) the npm package ships. No external deps —
// Node's built-in node:test + node:assert.
//
// API discovered empirically from nedb-engine@2.2.32:
//   NedbCore.open(path) -> db
//   db.put(coll,id,jsonStr) -> stored node JSON (injects _id,_coll,_seq,_hash)
//   db.get(coll,id) -> node JSON | null
//   db.getAsOf(coll,id, BigInt seq) -> node JSON | null   (MVCC time-travel)
//   db.query(nql) -> [node JSON, ...]
//   db.delete(coll,id)
//   db.link(frm,rel,to) / db.neighbors(frm,rel) / db.inbound(to,rel)  (causal DAG)
//   db.verify() -> bool   db.head() -> string   db.seq() -> BigInt   db.flush()

import { test } from 'node:test';
import assert from 'node:assert/strict';
import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs';

// Resolve the binding whether run standalone (`npm i nedb-engine`) or in-repo
// from test/node/ (where `npm run build` emits ../../index.js).
let NedbCore;
try { ({ NedbCore } = await import('nedb-engine')); }
catch { ({ NedbCore } = await import('../../index.js')); }

let n = 0;
function freshDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `nedbtest-${process.pid}-${n++}-`));
  return { db: NedbCore.open(dir), dir };
}

test('open returns a usable handle', () => {
  const { db } = freshDb();
  assert.ok(db, 'NedbCore.open returned a handle');
  assert.equal(typeof db.put, 'function');
  assert.equal(typeof db.verify, 'function');
});

test('put/get round-trip preserves user fields and injects metadata', () => {
  const { db } = freshDb();
  db.put('blocks', '0', JSON.stringify({ height: 0, msg: 'genesis' }));
  const row = JSON.parse(db.get('blocks', '0'));
  assert.equal(row.height, 0);
  assert.equal(row.msg, 'genesis');
  assert.equal(row._id, '0');
  assert.equal(row._coll, 'blocks');
  assert.equal(typeof row._seq, 'number');
  assert.equal(typeof row._hash, 'string');
  assert.equal(row._hash.length, 64, 'BLAKE2b-256 hex = 64 chars');
});

test('get on a missing key returns null', () => {
  const { db } = freshDb();
  assert.equal(db.get('blocks', 'nope'), null);
});

test('seq() advances and head() changes on every write', () => {
  const { db } = freshDb();
  const s0 = db.seq();
  const h0 = db.head();
  db.put('c', 'a', JSON.stringify({ v: 1 }));
  const s1 = db.seq();
  const h1 = db.head();
  assert.ok(s1 > s0, `seq advanced ${s0} -> ${s1}`);
  assert.notEqual(h0, h1, 'Merkle head advanced after a write');
});

test('verify() reports an intact store', () => {
  const { db } = freshDb();
  for (let i = 0; i < 50; i++) db.put('blocks', String(i), JSON.stringify({ height: i }));
  assert.equal(db.verify(), true, 'untampered store verifies clean');
});

test('MVCC time-travel: getAsOf returns the historical version', () => {
  const { db } = freshDb();
  const r1 = JSON.parse(db.put('acct', 'alice', JSON.stringify({ bal: 100 })));
  const seqV1 = BigInt(r1._seq);
  db.put('acct', 'alice', JSON.stringify({ bal: 250 })); // new version, same id
  assert.equal(JSON.parse(db.get('acct', 'alice')).bal, 250, 'latest is v2');
  assert.equal(JSON.parse(db.getAsOf('acct', 'alice', seqV1)).bal, 100,
    'AS OF the v1 seq returns the old balance');
});

test('query runs NQL and filters with WHERE', () => {
  const { db } = freshDb();
  for (let i = 0; i < 5; i++) db.put('blocks', String(i), JSON.stringify({ height: i }));
  assert.equal(db.query('FROM blocks').length, 5);
  const filtered = db.query('FROM blocks WHERE height = 3').map(JSON.parse);
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].height, 3);
});

test('delete removes the live id', () => {
  const { db } = freshDb();
  db.put('c', 'x', JSON.stringify({ v: 1 }));
  assert.ok(db.get('c', 'x'));
  db.delete('c', 'x');
  assert.equal(db.get('c', 'x'), null);
});

test('causal DAG: link + neighbors traverse a typed edge', () => {
  const { db } = freshDb();
  db.put('blocks', '0', JSON.stringify({ height: 0 }));
  db.put('blocks', '1', JSON.stringify({ height: 1 }));
  db.link('blocks:1', 'prev', 'blocks:0');   // block 1 -> prev -> block 0
  // NOTE (napi binding contract): neighbors/inbound return "coll:id" REFERENCE
  // strings, not full node JSON (the Python/Rust neighbors returns full nodes —
  // a binding inconsistency worth tracking). Hydrate a ref with get(coll,id).
  assert.deepEqual(db.neighbors('blocks:1', 'prev'), ['blocks:0']);
  assert.deepEqual(db.inbound('blocks:0', 'prev'), ['blocks:1'],
    'inbound edge points back to block 1');
  const [coll, id] = db.neighbors('blocks:1', 'prev')[0].split(':');
  assert.equal(JSON.parse(db.get(coll, id)).height, 0, 'ref hydrates to the real node');
});

test('persistence: data survives close + reopen of the same path', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `nedbpersist-${process.pid}-`));
  const db1 = NedbCore.open(dir);
  db1.put('blocks', 'g', JSON.stringify({ msg: 'durable' }));
  db1.flush();
  const headBefore = db1.head();
  const db2 = NedbCore.open(dir);          // reopen same dir
  assert.equal(JSON.parse(db2.get('blocks', 'g')).msg, 'durable', 'value survived reopen');
  assert.equal(db2.verify(), true, 'reopened store verifies clean');
  assert.equal(db2.head(), headBefore, 'Merkle head stable across reopen');
});

test('putEx stores like put (extended client/nonce/idem args accepted)', () => {
  const { db } = freshDb();
  const r = db.putEx('c', 'x', JSON.stringify({ v: 7 }), null, null, null);
  assert.equal(typeof r, 'string', 'putEx returns the stored node JSON');
  assert.equal(JSON.parse(db.get('c', 'x')).v, 7);
});

test('createIndex is accepted and does not disturb reads', () => {
  const { db } = freshDb();
  for (let i = 0; i < 5; i++) db.put('c', String(i), JSON.stringify({ v: i }));
  db.createIndex('c', 'v', 'sorted');            // perf hint — must not throw
  assert.equal(db.query('FROM c WHERE v = 3').length, 1, 'query still correct after createIndex');
});

test('error paths: invalid NQL throws, unknown collection yields []', () => {
  const { db } = freshDb();
  assert.throws(() => db.query('THIS IS NOT VALID NQL @@@'), /FROM/, 'malformed NQL is rejected');
  assert.deepEqual(db.query('FROM ghosts'), [], 'unknown collection returns empty, not an error');
});

test('graph time-travel: neighborsAsOf / inboundAsOf honor as_of', () => {
  // Regression guard for the napi fix: neighbors_as_of/inbound_as_of must query
  // __links__ AS OF {seq}, not ignore the arg. Passes once the binding is built
  // with the fix; fails against pre-fix published binaries (the documented gap).
  const { db } = freshDb();
  db.put('blocks', '0', JSON.stringify({ h: 0 }));   // seq 0
  db.put('blocks', '1', JSON.stringify({ h: 1 }));   // seq 1
  const linkSeq = db.seq();                          // BigInt — the seq the link gets
  db.link('blocks:1', 'prev', 'blocks:0');
  assert.deepEqual(db.neighborsAsOf('blocks:1', 'prev', linkSeq - 1n), [],
    'edge is invisible strictly before it was linked');
  assert.deepEqual(db.neighborsAsOf('blocks:1', 'prev', linkSeq), ['blocks:0'],
    'edge visible from its link seq onward');
  assert.deepEqual(db.inboundAsOf('blocks:0', 'prev', linkSeq), ['blocks:1'],
    'inbound edge time-travels too');
});
