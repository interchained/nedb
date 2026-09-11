// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

'use strict';
// nedb/wrap/mongo.js — wrapMongo: causal provenance for MongoDB (JS).
//
//   const { wrapMongo } = require('nedb-engine/wrap');
//   const client = wrapMongo(mongoClient, { dbName: 'app' });
//
//   client.nedb.register('app.drivers', 'driver');   // "db.collection" ns
//   client.nedb.backfill();
//   client.nedb.shadowWrites = true;
//
//   // your code unchanged; after each write, chain it:
//   await client.db().collection('drivers').insertOne({ name: 'Bob' });
//   client.nedb.shadowRow('app.drivers', { _id: r.insertedId, name: 'Bob' });
//
// © INTERCHAINED LLC × Claude Sonnet 4.6
'use strict';

const { WrapSurface, openEngine } = require('./surface');

class MongoSurface extends WrapSurface {
  constructor(client, dbName, engine) {
    super(dbName, engine);
    this.client = client;
  }

  /** hostScan: iterate docs of a "db.collection" namespace (sync only —
   *  pymongo-style cursor.toArray is async, so JS backfill for the native
   *  driver is async; provide scanAsync). */
  hostScan(mapping) { return []; }

  async hostScanAsync(mapping, batchSize) {
    const [dbName, collName] = mapping.pattern.split('.');
    const out = [];
    try {
      const coll = this.client.db(dbName).collection(collName);
      const cursor = coll.find({});
      while (await cursor.hasNext()) {
        const doc = await cursor.next();
        const { _id, ...rest } = doc;
        out.push([String(_id), rest]);
        if (out.length >= (batchSize || 200)) break;
      }
    } catch (_) { /* skip */ }
    return out;
  }

  shadowDoc() { return null; }

  /** Explicit row shadow: routes into the registered collection. */
  shadowRow(ns, doc, op = 'UPSERT') {
    if (!this.shadowWrites) return;
    try {
      const m = this.mappings.find((x) => x.pattern === ns);
      const coll = m ? m.collection : ns.split('.').pop();
      if (doc === null || doc === undefined) {
        this.engine.put('__mongo_shadow__', `${ns}:del:${op}`,
          { ns, _op: 'DELETE' });
        return;
      }
      const { _id, ...rest } = doc;
      this.engine.put(coll, String(_id ?? op), { ...rest, _ns: ns, _op: op });
    } catch (_) { /* never break the host */ }
  }
}

class WrappedMongoClient {
  constructor(client, opts = {}) {
    const dbName = opts.dbName || 'default';
    const engine = openEngine({
      dbName, nedbdUrl: opts.nedbdUrl, nedbdToken: opts.nedbdToken,
      dagPath: opts.dagPath, dagTmk: opts.dagTmk, native: opts.native,
    });
    this._client = client;
    this.nedb = new MongoSurface(client, dbName, engine);
  }
  _raw() { return this._client; }
}

function wrapMongo(client, opts) {
  const w = new WrappedMongoClient(client, opts);
  return new Proxy(w, {
    get(target, prop, receiver) {
      if (prop in target || prop === 'nedb') return Reflect.get(target, prop, receiver);
      const v = Reflect.get(target._client, prop);
      return typeof v === 'function' ? v.bind(target._client) : v;
    },
    set(target, prop, value) { target._client[prop] = value; return true; },
  });
}

module.exports = { wrapMongo, WrappedMongoClient, MongoSurface };
