// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

'use strict';
// nedb-engine — durable-mode auto-flush-on-exit wrapper.
//
// The native addon (generated napi binding in ./native.js) exposes `NedbCore`.
// A durable `NedbCore.open(path)` buffers writes in the engine's id-index WAL and
// only makes them durable on `flush()`; a hard exit (Ctrl+C, `SIGTERM` from an
// orchestrator, `pm2 stop`) that never runs an explicit flush would lose writes
// staged since the last flush.
//
// We close that gap the libuv-cooperative way — `process.on('SIGINT'|'SIGTERM'
// |'exit', () => db.flush())` — NOT a C-level signal handler inside the addon,
// which would clobber libuv's own signal machinery. In-memory databases
// (`new NedbCore()`) are never armed; there is nothing to flush.
//
// Escape hatch: set NEDB_NO_EXIT_FLUSH=1 to leave signal handling entirely to
// the host app (it can still call `db.flush()` itself).
//
// © INTERCHAINED LLC × Vex (Interchained AI fleet: GLM · Claude · Opus · Fable · GPT-6)
const native = require('./native.js');

const Native = native.NedbCore;

// The napi class defines `open` as a NON-writable, NON-configurable static, so the
// 2.5.x wrapper's `NedbCore.open = …` threw ("Cannot assign to read only property")
// — and CI's `napi build` was overwriting this file with the generated loader
// anyway, so the published package never carried the wrapper at all. Both fixed
// in 2.8.5: wrap by SUBCLASS (an own static on the subclass shadows the parent's),
// build with `--js native.js` so this file survives, and gate the publish on
// `NedbCore.__exitFlushWrapped` (see test/durability.test.mjs).
let NedbCore = Native;
if (Native && typeof Native.open === 'function' && !Native.__exitFlushWrapped) {
  // Durable handles opened in this process. Strong refs: a durable DB is meant to
  // live for the process, and we must be able to flush it on the way out.
  const live = new Set();
  let armed = false;

  const flushAll = () => {
    for (const db of live) {
      try {
        db.flush();
      } catch (_) {
        // Best-effort on shutdown — never throw out of an exit handler.
      }
    }
  };

  const arm = () => {
    if (armed || process.env.NEDB_NO_EXIT_FLUSH) return;
    armed = true;
    // 'exit' fires on normal termination; handlers must be synchronous, and
    // db.flush() is a synchronous native call — so this is safe and sufficient
    // for clean exits and uncaught-exception exits.
    process.on('exit', flushAll);
    // Registering a SIGINT/SIGTERM listener SUPPRESSES Node's default
    // termination, so once we listen we own the exit: flush, then terminate with
    // the conventional 128+signum status.
    const onSignal = (signum) => () => {
      flushAll();
      process.exit(128 + signum);
    };
    process.on('SIGINT', onSignal(2));
    process.on('SIGTERM', onSignal(15));
  };

  NedbCore = class NedbCore extends Native {
    static open(path) {
      const db = Native.open(path);
      live.add(db);
      arm();
      return db;
    }
  };
  // Mark so a re-require (or a wrapped re-export) never double-wraps.
  Object.defineProperty(NedbCore, '__exitFlushWrapped', { value: true, enumerable: false });
}

module.exports = { ...native, NedbCore };
// Explicit named re-export so ESM `import { NedbCore } from 'nedb-engine'` (used
// by the test suite) resolves the class through cjs-module-lexer.
module.exports.NedbCore = NedbCore;
