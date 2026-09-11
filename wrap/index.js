// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

'use strict';
// nedb/wrap/index.js — the JS wrap adapter family.
//
//   const { wrapRedis, wrapSqlite, wrapMysql, wrapPg, wrapMongo } =
//     require('nedb-engine/wrap');
//
// Every wrapper: register → backfill → shadowWrites=true → full NEDB API on
// `.nedb`, with the embedded v2/v3 DAG (Rust napi core) as the default engine.
//
// © INTERCHAINED LLC × Claude Sonnet 4.6
'use strict';

const { WrapSurface, openEngine, CollectionMapping, NedbdProxy, NativeEngine } = require('./surface');
const { wrapRedis, WRITE_CMDS } = require('./redis');
const { wrapSqlite, wrapMysql, wrapPg, SqlSurface } = require('./sql');
const { wrapMongo, MongoSurface } = require('./mongo');

module.exports = {
  // adapters
  wrapRedis, wrapSqlite, wrapMysql, wrapPg, wrapMongo,
  // surface primitives (for custom adapters)
  WrapSurface, openEngine, CollectionMapping, NedbdProxy, NativeEngine,
  SqlSurface, MongoSurface, WRITE_CMDS,
};
