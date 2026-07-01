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
// © INTERCHAINED LLC × Claude Opus 4.8

const native = require('./native.js');

const { NedbCore } = native;

if (NedbCore && typeof NedbCore.open === 'function' && !NedbCore.__exitFlushWrapped) {
  const nativeOpen = NedbCore.open.bind(NedbCore);

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

  NedbCore.open = function open(path) {
    const db = nativeOpen(path);
    live.add(db);
    arm();
    return db;
  };

  // Mark so a re-require (or a wrapped re-export) never double-wraps.
  Object.defineProperty(NedbCore, '__exitFlushWrapped', {
    value: true,
    enumerable: false,
  });
}

module.exports = native;
// Explicit named re-export so ESM `import { NedbCore } from 'nedb-engine'` (used
// by the test suite) resolves the class through cjs-module-lexer.
module.exports.NedbCore = NedbCore;
