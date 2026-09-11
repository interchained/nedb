// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

'use strict';
// nedb/wrap/redis.js — wrapRedis: one-line causal provenance for Redis (JS).
//
//   const { wrapRedis } = require('nedb-engine/wrap');
//   const redis = require('redis');
//
//   const r = wrapRedis(redis.createClient(), { dbName: 'rideshare' });
//
//   r.nedb.register('driver:*', 'driver');
//   r.nedb.backfill();
//   r.nedb.shadowWrites = true;
//
//   await r.set('driver:d1', JSON.stringify({ name: 'Bob' }));  // shadowed
//   r.nedb.query('FROM driver');                                 // NQL on top
//   r.nedb.verify();                                             // → true
//
// Works with any client whose write methods are functions on the connection
// (node-redis v4, ioredis, redis-mock). Write commands are intercepted by
// wrapping the method — the surface-1 behavior is unchanged.
//
// © INTERCHAINED LLC × Claude Sonnet 4.6
'use strict';

const { WrapSurface, openEngine } = require('./surface');

// Redis write commands we shadow (mirrors the Python _WRITE_CMDS set).
const WRITE_CMDS = new Set([
  'set', 'setnx', 'setex', 'psetex', 'getset', 'getdel', 'getex',
  'mset', 'msetnx', 'hset', 'hmset', 'hsetnx', 'hincrby', 'hincrbyfloat', 'hdel',
  'lpush', 'rpush', 'lset', 'linsert', 'ltrim', 'lpop', 'rpop',
  'sadd', 'srem', 'smove', 'zadd', 'zincrby', 'zrem',
  'del', 'unlink', 'rename', 'renamenx', 'append', 'incr', 'incrby', 'decr', 'decrby',
]);

class RedisSurface extends WrapSurface {
  constructor(client, dbName, engine) {
    super(dbName, engine);
    this.client = client;
  }

  // ── host scan: iterate keys matching the pattern, read their values ──────
  hostScan(mapping, batchSize) {
    // node-redis v4 / ioredis both expose scan + get/hgetall; a sync-scan
    // fallback covers in-memory fakes.
    const out = [];
    const scanSync = typeof this.client.scanKeys === 'function'
      ? this.client.scanKeys(mapping.pattern)
      : (this.client.keys ? this.client.keys(mapping.pattern) : []);
    for (const key of scanSync || []) {
      try {
        const raw = mapping.valueType === 'hash'
          ? this.client.hgetall(key)
          : this.client.get(key);
        if (raw !== null && raw !== undefined) out.push([key, raw]);
      } catch (_) { /* skip unreadable */ }
      if (out.length >= (batchSize || 200)) break;
    }
    return out;
  }

  // ── host write → NEDB doc ──────────────────────────────────────────────────
  // args = the host call's arguments AFTER the key.
  // __replace: true → surface puts doc as-is (full replace); default merges.
  shadowDoc(mapping, cmd, args) {
    if (cmd === 'hset') {
      // hset key field value | hset key {obj}
      if (typeof args[0] === 'object' && args[0] !== null) return mapping.parseValue(args[0]);
      return { [args[0]]: args[1] };   // merged over existing by the surface
    }
    if (cmd === 'set' || cmd === 'setex' || cmd === 'psetex' || cmd === 'setnx' || cmd === 'getset') {
      return { ...mapping.parseValue(args[0]), __replace: true };
    }
    if (cmd === 'incr' || cmd === 'incrby' || cmd === 'decr' || cmd === 'decrby') {
      return { _v: String(args[0] === undefined ? '' : args[0]) };
    }
    if (cmd === 'del' || cmd === 'unlink') {
      return { _deleted: true, __replace: true };
    }
    // other write types: store the command as metadata (merged)
    return { [`_redis_${cmd}`]: String(args[0] === undefined ? '' : args[0]) };
  }
}

class WrappedRedis {
  constructor(client, opts = {}) {
    const dbName = opts.dbName || 'default';
    const engine = openEngine({
      dbName, nedbdUrl: opts.nedbdUrl, nedbdToken: opts.nedbdToken,
      dagPath: opts.dagPath, dagTmk: opts.dagTmk, native: opts.native,
    });
    this._client = client;
    this.nedb = new RedisSurface(client, dbName, engine);
    this._installInterception();
  }

  _installInterception() {
    const surface = this.nedb;
    const client = this._client;
    for (const cmd of WRITE_CMDS) {
      const orig = client[cmd];
      if (typeof orig !== 'function' || orig.__nedbWrapped) continue;
      const wrapped = function (...args) {
        const result = orig.apply(client, args);
        try {
          const key = args[0];
          if (typeof key === 'string' && surface.shadowWrites) surface.shadow(cmd, key, ...args.slice(1));
        } catch (_) { /* never break the host call */ }
        return result;
      };
      wrapped.__nedbWrapped = true;
      try { client[cmd] = wrapped; } catch (_) { /* frozen client — skip */ }
    }
  }

  // passthrough for everything else
  __getattrPrivate(name) { return this._client[name]; }
  get _raw() { return this._client; }
}

function wrapRedis(client, opts) {
  const w = new WrappedRedis(client, opts);
  // Proxy property access to the underlying client so `await r.get(...)`
  // works naturally, while `.nedb` stays on the wrapper.
  return new Proxy(w, {
    get(target, prop, receiver) {
      if (prop in target || prop === 'nedb' || prop === '_client') return Reflect.get(target, prop, receiver);
      const v = target._client[prop];
      return typeof v === 'function' ? v.bind(target._client) : v;
    },
    set(target, prop, value) { target._client[prop] = value; return true; },
  });
}

module.exports = { wrapRedis, WrappedRedis, RedisSurface, WRITE_CMDS };
