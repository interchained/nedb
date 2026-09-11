// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

'use strict';
// nedb/wrap/surface.js — the engine-agnostic NEDB layer-2 surface (JS).
//
// One contract, three engines:
//   dag      — embedded Rust core (NedbCore from the napi-rs addon)
//   nedbd    — HTTP nedbd server (v1 AOF, v2 DAG, v3 --dag-v3)
//   memory   — a caller-supplied engine object (tests, custom backends)
//
// Host adapters (wrapRedis/wrapSqlite/wrapMysql/wrapMongo/wrapPg) supply:
//   hostScan(mapping)          → iterate existing host records
//   shadowDoc(mapping, args)   → host write → NEDB doc (or null)
//
// © INTERCHAINED LLC × Claude Sonnet 4.6
'use strict';

const fs = require('fs');
const path = require('path');

// ── engine resolution ────────────────────────────────────────────────────────

/** Acquire the NedbCore class: caller-supplied, native addon, or throw. */
function resolveNative(provided) {
  if (provided) return provided;
  try {
    return require('../index.js').NedbCore;
  } catch (_) {
    throw new Error(
      'nedb-engine native addon not found — rebuild with `npm run build` ' +
      'or pass an engine explicitly (wrap(x, { engine }) / { nedbdUrl })');
  }
}

/**
 * Open an engine handle for a wrap_* surface.
 *   openEngine({ nedbdUrl, nedbdToken, dagPath, engine }) → handle
 * The handle exposes: put/get/query/createIndex/delete/link/unlink/
 * neighbors/inbound/verify()/head/seq + engineKind.
 */
function openEngine(opts = {}) {
  if (opts.engine) return normalizeEngine(opts.engine, 'supplied');

  if (opts.nedbdUrl) return new NedbdProxy(opts.nedbdUrl, opts.dbName, opts.nedbdToken);

  const NedbCore = resolveNative(opts.native);
  const core = opts.dagPath
    ? NedbCore.open(opts.dagPath, opts.dagTmk || null)
    : new NedbCore();
  return normalizeEngine(core, 'dag-embedded');
}

/** Normalize any engine-ish object into the canonical duck-typed contract. */
function normalizeEngine(core, kind) {
  // HTTP proxy shape → pass through as-is
  if (core instanceof NedbdProxy) return core;
  return new NativeEngine(core, kind);
}

// ── Native engine handle (embedded Rust DAG) ────────────────────────────────

class NativeEngine {
  constructor(core, kind) {
    this.core = core;
    this.kind = kind || 'dag-embedded';
  }
  put(coll, id, doc) {
    const body = { ...doc };
    for (const k of ['caused_by', 'valid_from', 'valid_to']) {
      if (body[k] === undefined) delete body[k];
    }
    const node = this.core.put(coll, id, JSON.stringify(body));
    return typeof node === 'string' ? JSON.parse(node) : node;
  }
  get(coll, id, asOf) {
    const node = asOf === undefined || asOf === null
      ? this.core.get(coll, id)
      : this.core.get(coll, id, asOf);
    return node ? (typeof node === 'string' ? JSON.parse(node) : node) : null;
  }
  query(nql) {
    const rows = this.core.query(nql);
    return rows.map((r) => (typeof r === 'string' ? JSON.parse(r) : r));
  }
  createIndex(coll, field, kind) { this.core.createIndex(coll, field, kind || 'eq'); }
  delete(coll, id) { this.core.delete(coll, id); }
  link(frm, rel, to) { this.core.link(frm, rel, to); }
  unlink(frm, rel, to) { this.core.unlink(frm, rel, to); }
  neighbors(frm, rel, asOf) {
    return asOf === undefined ? this.core.neighbors(frm, rel)
                              : this.core.neighbors(frm, rel, asOf);
  }
  inbound(to, rel, asOf) {
    return asOf === undefined ? this.core.inbound(to, rel)
                              : this.core.inbound(to, rel, asOf);
  }
  verify() { return this.core.verify() === true; }
  get head() { return this.core.head(); }
  get seq() { return this.core.seq(); }
  checkpoint() { this.core.flush(); return this.head; }
  tip() { const t = this.core.tip ? this.core.tip() : null; return t ? JSON.parse(t) : null; }
  since(afterSeq, limit) {
    if (!this.core.since) throw new Error('changefeed requires the DAG backend');
    // napi binding: after_seq is u64 (BigInt), limit is usize (Number)
    const after = typeof afterSeq === 'bigint' ? afterSeq : BigInt(afterSeq);
    const lim = limit === undefined ? 0 : Number(limit);
    return JSON.parse(this.core.since(after, lim));
  }
  scanStatus() {
    if (!this.core.scanStatus) throw new Error('scanStatus requires the DAG backend');
    return JSON.parse(this.core.scanStatus());
  }
  flush() { if (this.core.flush) this.core.flush(); }
  get engineKind() { return this.kind; }
}

// ── HTTP nedbd handle ────────────────────────────────────────────────────────

class NedbdProxy {
  constructor(baseUrl, dbName, token) {
    this.base = baseUrl.replace(/\/$/, '');
    this.name = dbName;
    this.token = token || null;
    this._ensureDb();
  }
  _headers() {
    const h = { 'Content-Type': 'application/json', Accept: 'application/json' };
    if (this.token) h.Authorization = `Bearer ${this.token}`;
    return h;
  }
  _req(method, p, body) {
    const http = require('http');
    const url = new URL(this.base + p);
    const payload = body === undefined ? null : JSON.stringify(body);
    const res = http.request({
      hostname: url.hostname, port: url.port || 80, path: url.pathname + url.search,
      method, headers: { ...this._headers(), ...(payload ? { 'Content-Length': Buffer.byteLength(payload) } : {}) },
    });
    // synchronous-feeling via Atomics.wait on a SharedArrayBuffer flag
    const flag = new Int32Array(new SharedArrayBuffer(4));
    let out = { status: 0, text: '' };
    const r2 = res;
    r2.on('response', (r) => {
      let t = '';
      r.on('data', (c) => { t += c; });
      r.on('end', () => { out = { status: r.statusCode, text: t }; Atomics.store(flag, 0, 1); Atomics.notify(flag, 0); });
    });
    r2.on('error', (e) => { out = { status: 0, text: String(e) }; Atomics.store(flag, 0, 1); Atomics.notify(flag, 0); });
    if (payload) r2.write(payload);
    r2.end();
    Atomics.wait(flag, 0, 0);
    if (out.status === 0) throw new Error(`nedbd ${method} ${p} failed: ${out.text}`);
    let parsed;
    try { parsed = JSON.parse(out.text); } catch { parsed = { raw: out.text }; }
    if (out.status >= 400) throw new Error(`nedbd ${method} ${p} → HTTP ${out.status}: ${out.text.slice(0, 200)}`);
    return parsed;
  }
  _db(suffix) { return `/v1/databases/${this.name}${suffix || ''}`; }
  _ensureDb() {
    try { this._req('GET', this._db()); } catch (e) {
      if (String(e).includes('404')) this._req('POST', '/v1/databases', { name: this.name });
      else throw e;
    }
  }
  put(coll, id, doc, kw = {}) {
    const payload = { coll, id, doc };
    for (const k of ['client', 'nonce', 'idem', 'evidence', 'confidence', 'valid_from', 'valid_to', 'caused_by']) {
      if (kw[k] !== undefined && kw[k] !== null) payload[k] = kw[k];
    }
    const r = this._req('POST', this._db('/put'), payload);
    return r.doc !== undefined ? r.doc : doc;
  }
  get(coll, id, asOf) {
    const clause = asOf !== undefined && asOf !== null ? ` AS OF ${asOf}` : '';
    const rows = this.query(`FROM ${coll}${clause} WHERE _id = "${id}"`);
    return rows.length ? rows[0] : null;
  }
  query(nql) { return this._req('POST', this._db('/query'), { nql }).rows || []; }
  createIndex(coll, field, kind) { this._req('POST', this._db('/index'), { coll, field, kind: kind || 'eq' }); }
  delete(coll, id) { this._req('DELETE', `/v1/databases/${this.name}/rows/${coll}/${id}`); }
  link(frm, rel, to) {
    try { this._req('POST', this._db('/link'), { frm, rel, to }); }
    catch (e) {
      if (String(e).includes('404') || String(e).toLowerCase().includes('not found')) {
        this.put('__links__', `${frm}|${rel}|${to}`, { _from: frm, _rel: rel, _to: to });
      } else throw e;
    }
  }
  unlink(frm, rel, to) {
    try { this._req('DELETE', `/v1/databases/${this.name}/links/${frm}/${rel}/${to}`); }
    catch (_) { try { this._req('DELETE', `/v1/databases/${this.name}/rows/__links__/${frm}|${rel}|${to}`); } catch (_) {} }
  }
  neighbors(frm, rel, asOf) {
    const clause = asOf !== undefined && asOf !== null ? ` AS OF ${asOf}` : '';
    const [c] = frm.split(':');
    const rows = this.query(`FROM ${c}${clause} WHERE _id = "${frm.split(':')[1] || ''}" TRAVERSE ${rel}`);
    return rows.filter((r) => r._id).map((r) => `${r._coll || c}:${r._id}`);
  }
  inbound(to, rel, asOf) {
    const clause = asOf !== undefined && asOf !== null ? ` AS OF ${asOf}` : '';
    const [c] = to.split(':');
    try {
      const rows = this.query(`FROM ${c} WHERE _id = "${to.split(':')[1] || ''}" TRAVERSE ${rel} REVERSE`);
      return rows.filter((r) => r._id).map((r) => `${r._coll || c}:${r._id}`);
    } catch (_) { return []; }
  }
  verify() { return this._req('GET', this._db('/verify')).ok === true; }
  get head() { return this._req('GET', this._db()).head || '0'.repeat(64); }
  get seq() { return this._req('GET', this._db()).seq || 0; }
  checkpoint() { return this._req('POST', this._db('/checkpoint')).head || this.head; }
  get engineKind() { return 'nedbd-http'; }
}

// ── collection mapping (key/table glob → NEDB collection) ───────────────────

class CollectionMapping {
  constructor(pattern, collection, opts = {}) {
    this.pattern = pattern;
    this.collection = collection;
    this.idExtractor = opts.idExtractor || ((k) => k.split(':').pop());
    this.valueParser = opts.valueParser || CollectionMapping.defaultParse;
    this.valueType = opts.valueType || 'string';
  }
  static defaultParse(v) {
    if (v === null || v === undefined) return { _v: null };
    if (typeof v === 'object') return v;
    const s = String(v);
    try {
      const p = JSON.parse(s);
      return (p && typeof p === 'object') ? p : { _v: p };
    } catch { return { _v: s }; }
  }
  matches(key) {
    // translate a glob to a RegExp (tiny, no deps)
    const rx = new RegExp('^' + this.pattern
      .replace(/[.+^${}()|[\]\\]/g, '\\$&')
      .replace(/\*/g, '.*').replace(/\?/g, '.') + '$');
    return rx.test(key);
  }
  extractId(key) { return this.idExtractor(key); }
  parseValue(v) { return this.valueParser(v); }
}

// ── the shared surface (the `.nedb` attribute) ──────────────────────────────

class WrapSurface {
  constructor(dbName, engine) {
    this.dbName = dbName;
    this.engine = engine;
    this.mappings = [];
    this.shadowWrites = false;
    this.backfilled = false;
  }

  // host hooks — override in host adapters
  hostScan(/* mapping, batchSize */) { return []; }
  shadowDoc(/* mapping, cmd, args */) { return null; }

  register(pattern, collection, opts) {
    this.mappings.push(new CollectionMapping(pattern, collection, opts));
    return this;
  }
  mappingFor(key) { return this.mappings.find((m) => m.matches(key)) || null; }

  backfill(opts = {}) {
    const mappings = opts.pattern
      ? [new CollectionMapping(opts.pattern, opts.collection || opts.pattern.split(':')[0], opts)]
      : this.mappings;
    let total = 0;
    for (const m of mappings) {
      for (const [key, raw] of this.hostScan(m, opts.batchSize || 200)) {
        try {
          const doc = m.parseValue(raw);
          doc._source = 'backfill';
          this.engine.put(m.collection, m.extractId(key), doc);
          total += 1;
        } catch (_) { /* skip unreadable */ }
      }
    }
    this.backfilled = true;
    return total;
  }

  shadow(cmd, key, ...rest) {
    if (!this.shadowWrites) return;
    try {
      const m = this.mappingFor(key);
      if (!m) {
        this.engine.put('__shadow_raw__', key, { cmd, key, _source: 'shadow_raw' });
        return;
      }
      // rest = the host call's arguments AFTER the key (value(s) / fields)
      const doc = this.shadowDoc(m, cmd, rest);
      if (!doc) return;
      doc._source = 'shadow';
      const id = m.extractId(key);
      // merge over the existing doc (hset/incr are incremental by nature;
      // set replaces — the adapter marks replacement with doc.__replace)
      const prev = this.engine.get(m.collection, id);
      const merged = doc.__replace || !prev ? doc : { ...prev, ...doc };
      delete merged.__replace;
      this.engine.put(m.collection, id, merged);
    } catch (e) { if (process.env.NEDB_WRAP_DEBUG) console.error('[nedb shadow]', e); }
  }

  // ── full NEDB API ──────────────────────────────────────────────────────────
  put(coll, id, doc, kw) { return this.engine.put(coll, id, doc, kw); }
  get(coll, id, asOf) { return this.engine.get(coll, id, asOf); }
  query(nql) { return this.engine.query(nql); }
  createIndex(coll, field, kind) { this.engine.createIndex(coll, field, kind); }
  delete(coll, id) { return this.engine.delete(coll, id); }
  link(frm, rel, to) { return this.engine.link(frm, rel, to); }
  unlink(frm, rel, to) { return this.engine.unlink(frm, rel, to); }
  neighbors(frm, rel, asOf) { return this.engine.neighbors(frm, rel, asOf); }
  inbound(to, rel, asOf) { return this.engine.inbound(to, rel, asOf); }
  verify() { return this.engine.verify(); }
  get head() { return this.engine.head; }
  get seq() { return this.engine.seq; }
  checkpoint() { return this.engine.checkpoint(); }
  tip() { return this.engine.tip ? this.engine.tip() : null; }
  since(afterSeq, limit) { return this.engine.since(afterSeq, limit); }
  scanStatus() { return this.engine.scanStatus(); }
  get engineKind() { return this.engine.engineKind; }

  [Symbol.for('nodejs.util.inspect.custom')]() {
    return `<WrapSurface db=${JSON.stringify(this.dbName)} engine=${this.engineKind} mappings=${this.mappings.length}>`;
  }
}

module.exports = {
  openEngine, NativeEngine, NedbdProxy, CollectionMapping, WrapSurface, resolveNative,
};
