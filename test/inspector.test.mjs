// nedb-inspector — test suite. Run: node --test test/inspector.test.mjs
//
// Exercises the deterministic analyzer: durable-open detection, flush-on-exit
// wiring recognition, and — critically — that the lexer masks comments/strings
// so tokens inside them never cause false positives OR false negatives.
//
// © INTERCHAINED LLC × Claude Opus 4.8

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { inspect } from '../nedb-inspector.mjs';

const codes = (r) => r.warnings.map((w) => w.code);

// ── Rust ─────────────────────────────────────────────────────────────────────
test('rust: durable Db::open without install_exit_flush → WARN', () => {
  const src = `use nedb_engine::Db;
fn main() {
    let db = Db::open(path, None).unwrap();
    serve(db);
}`;
  const r = inspect(src, 'store.rs');
  assert.equal(r.ok, false);
  assert.deepEqual(codes(r), ['RUST_NO_EXIT_FLUSH']);
});

test('rust: durable Db::open WITH install_exit_flush → OK', () => {
  const src = `use std::sync::Arc;
use nedb_engine::Db;
fn main() {
    let db = Arc::new(Db::open(path, None).unwrap());
    Db::install_exit_flush(Arc::clone(&db));
}`;
  const r = inspect(src, 'store.rs');
  assert.equal(r.ok, true);
  assert.equal(r.warnings.length, 0);
});

test('rust: in_memory only → OK', () => {
  const r = inspect('let db = Db::in_memory();', 'x.rs');
  assert.equal(r.ok, true);
});

test('rust: Db::open in a COMMENT and a STRING is ignored (no false positive)', () => {
  const src = `fn note() {
    // example: let db = Db::open(p, None);  install_exit_flush later
    let s = "Db::open( lives in this string";
    let db = Db::in_memory();
}`;
  const r = inspect(src, 'x.rs');
  assert.equal(r.ok, true, 'commented/stringified Db::open must not trigger');
});

test('rust: install_exit_flush only in a COMMENT still WARNs (no false negative)', () => {
  const src = `fn run() {
    // TODO: wire install_exit_flush(Arc::clone(&db)) eventually
    let db = Db::open(path, None).unwrap();
}`;
  const r = inspect(src, 'x.rs');
  assert.equal(r.ok, false);
  assert.deepEqual(codes(r), ['RUST_NO_EXIT_FLUSH']);
});

test('rust: XDb::open must not match Db::open (identifier boundary)', () => {
  const src = `let db = MyDb::open(p);`;
  const r = inspect(src, 'x.rs');
  assert.equal(r.ok, true, 'MyDb::open is not nedb_engine::Db::open');
});

// ── Node / JS ────────────────────────────────────────────────────────────────
test('js: durable open importing nedb-engine → OK (auto-armed)', () => {
  const src = `import { NedbCore } from 'nedb-engine';
const db = NedbCore.open('/data/x');`;
  const r = inspect(src, 'app.mjs');
  assert.equal(r.ok, true);
});

test('js: durable open via raw native binding, no wiring → WARN', () => {
  const src = `const { NedbCore } = require('./native.js');
const db = NedbCore.open('/data/x');`;
  const r = inspect(src, 'app.cjs');
  assert.equal(r.ok, false);
  assert.deepEqual(codes(r), ['JS_NO_EXIT_FLUSH']);
});

test('js: raw native binding WITH process.on flush wiring → OK', () => {
  const src = `const { NedbCore } = require('./native.js');
const db = NedbCore.open('/data/x');
process.on('SIGTERM', () => { db.flush(); process.exit(143); });`;
  const r = inspect(src, 'app.cjs');
  assert.equal(r.ok, true);
});

test('js: prose containing the word "native" does not count as a native import', () => {
  const src = `import { NedbCore } from 'nedb-engine';
console.log('NEDB native smoke test');
const db = NedbCore.open('/data/x');`;
  const r = inspect(src, 'app.mjs');
  assert.equal(r.ok, true, "imports 'nedb-engine' → auto-armed, prose must not flip it to native");
});

test('js: new NedbCore() in-memory only → OK', () => {
  const src = `import { NedbCore } from 'nedb-engine';
const db = new NedbCore();`;
  const r = inspect(src, 'app.mjs');
  assert.equal(r.ok, true);
});

// ── Python ───────────────────────────────────────────────────────────────────
test('py: native open + os._exit without atexit → WARN', () => {
  const src = `from nedb._native import NedbCore
import os
db = NedbCore.open('/data/x')
os._exit(0)`;
  const r = inspect(src, 'svc.py');
  assert.equal(r.ok, false);
  assert.deepEqual(codes(r), ['PY_OS_EXIT_BYPASS']);
});

test('py: native open, normal exit → OK (auto-armed info)', () => {
  const src = `from nedb._native import NedbCore
db = NedbCore.open('/data/x')`;
  const r = inspect(src, 'svc.py');
  assert.equal(r.ok, true);
});

test('py: pure NEDB(path) is per-op durable → OK', () => {
  const src = `from nedb import NEDB
db = NEDB("/data/x")`;
  const r = inspect(src, 'svc.py');
  assert.equal(r.ok, true);
});

test('py: NEDB() with no arg is in-memory → OK, no durable finding', () => {
  const src = `from nedb import NEDB
db = NEDB()`;
  const r = inspect(src, 'svc.py');
  assert.equal(r.ok, true);
  assert.equal(r.findings.length, 0);
});

// ── misc ─────────────────────────────────────────────────────────────────────
test('unsupported extension is skipped, ok=true', () => {
  const r = inspect('whatever', 'notes.txt');
  assert.equal(r.skipped, true);
  assert.equal(r.ok, true);
});
