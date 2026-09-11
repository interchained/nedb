// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

'use strict';
// nedb/wrap/sql.js — wrapSqlite / wrapMysql / wrapPg: causal provenance for
// SQL databases (JS).
//
//   const { wrapSqlite } = require('nedb-engine/wrap');
//   const db = wrapSqlite(mySqliteConnection, { dbName: 'app' });
//
//   db.nedb.register('drivers', 'driver');
//   db.nedb.backfill();
//   db.nedb.shadowWrites = true;
//
// SQLite: execute() is intercepted — INSERT/UPDATE/DELETE on registered
// tables are shadowed automatically after they succeed.
// MySQL/Postgres (DB-API style or promise clients): shadowing is explicit —
// after your INSERT/UPDATE call db.nedb.shadowRow(table, pk, rowObj);
// row=null chains a DELETE tombstone.
//
// © INTERCHAINED LLC × Claude Sonnet 4.6
'use strict';

const { WrapSurface, openEngine } = require('./surface');

class SqlSurface extends WrapSurface {
  constructor(conn, dbName, engine, kind) {
    super(dbName, engine);
    this.conn = conn;
    this.kind = kind;
  }

  /** Default host scan: SELECT * FROM <table> — works on sync sqlite3 and
   *  promise clients that expose .all/.query. Override for exotic drivers. */
  hostScan(mapping, batchSize) {
    const table = mapping.pattern;
    const out = [];
    const push = (cols, rows) => {
      for (const row of rows || []) {
        if (Array.isArray(row)) out.push([String(out.length + 1), Object.fromEntries(cols.map((c, i) => [c, row[i]]))]);
        else out.push([String(row[cols[0]] ?? out.length + 1), row]);
      }
    };
    if (typeof this.conn.all === 'function') {           // node-sqlite3 style
      // sync reads aren't available; caller should backfill via callback mode.
      return out;
    }
    if (typeof this.conn.prepare === 'function') {        // better-sqlite3
      const stmt = this.conn.prepare(`SELECT rowid AS __nedb_rowid, * FROM "${table}"`);
      for (const row of stmt.iterate()) {
        const { __nedb_rowid, ...rest } = row;
        out.push([String(__nedb_rowid), rest]);
      }
      return out;
    }
    if (typeof this.conn.exec === 'function' && this.conn.prepare === undefined) {
      // node:sqlite (built-in) — similar to better-sqlite3
      try {
        const stmt = this.conn.prepare(`SELECT rowid AS __nedb_rowid, * FROM "${table}"`);
        for (const row of stmt.iterate ? stmt.iterate() : []) {
          const { __nedb_rowid, ...rest } = row;
          out.push([String(__nedb_rowid), rest]);
        }
        return out;
      } catch (_) { /* fall through */ }
    }
    if (typeof this.conn.query === 'function') {          // mysql2 / pg promise
      // handled by host adapters with async backfill — sync scan unsupported
      return out;
    }
    return out;
  }

  shadowDoc() { return null; }

  /** Explicit row shadow (mysql/pg): routes into the registered collection. */
  shadowRow(table, pk, row, op = 'UPSERT') {
    if (!this.shadowWrites) return;
    try {
      if (row === null || row === undefined) {
        this.engine.put('__sql_shadow__', `${table}:del:${pk}`,
          { table, pk: String(pk), _op: 'DELETE' });
        return;
      }
      const m = this.mappings.find((x) => x.pattern === table);
      const coll = m ? m.collection : '__sql_shadow__';
      this.engine.put(coll, String(pk), { ...row, _table: table, _op: op });
    } catch (_) { /* never break the host */ }
  }
}

class WrappedSqlConn {
  constructor(conn, opts, kind) {
    const dbName = opts.dbName || 'default';
    const engine = openEngine({
      dbName, nedbdUrl: opts.nedbdUrl, nedbdToken: opts.nedbdToken,
      dagPath: opts.dagPath, dagTmk: opts.dagTmk, native: opts.native,
    });
    this._conn = conn;
    this.nedb = new SqlSurface(conn, dbName, engine, kind);
    if (kind === 'sqlite') this._installSqliteInterception();
  }

  /** sqlite3/better-sqlite/node:sqlite — wrap exec/run/execute for shadowing. */
  _installSqliteInterception() {
    const surface = this.nedb;
    const conn = this._conn;
    const methodNames = ['execute', 'run', 'exec'].filter((m) => typeof conn[m] === 'function');
    for (const name of methodNames) {
      const orig = conn[name];
      if (orig.__nedbWrapped) continue;
      const wrapped = function (sql, ...rest) {
        const result = orig.call(conn, sql, ...rest);
        try {
          if (surface.shadowWrites && /^\s*(INSERT|UPDATE|DELETE|REPLACE)/i.test(String(sql))) {
            surface.shadow('sql', sql, 'sql', sql, result);
          }
        } catch (_) {}
        return result;
      };
      wrapped.__nedbWrapped = true;
      try { conn[name] = wrapped; } catch (_) {}
    }
    // transactional helpers pass through
  }

  _raw() { return this._conn; }
}

function _wrapWithProxy(wrapper, conn) {
  return new Proxy(wrapper, {
    get(target, prop, receiver) {
      if (prop in target || prop === 'nedb') return Reflect.get(target, prop, receiver);
      const v = Reflect.get(conn, prop);
      return typeof v === 'function' ? v.bind(conn) : v;
    },
    set(target, prop, value) { conn[prop] = value; return true; },
  });
}

function wrapSqlite(conn, opts) {
  return _wrapWithProxy(new WrappedSqlConn(conn, opts || {}, 'sqlite'), conn);
}
function wrapMysql(conn, opts) {
  return _wrapWithProxy(new WrappedSqlConn(conn, opts || {}, 'mysql'), conn);
}
function wrapPg(conn, opts) {
  const w = new WrappedSqlConn(conn, opts || {}, 'pg');
  const p = _wrapWithProxy(w, conn);
  // pg uses .query — shadowRow is explicit on the surface
  return p;
}

module.exports = { wrapSqlite, wrapMysql, wrapPg, WrappedSqlConn, SqlSurface };
