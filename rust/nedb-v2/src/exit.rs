//! Durable-mode auto-flush-on-exit.
//!
//! A durable [`Db`] stages id-index updates in an in-memory WAL (`write_buf`) and
//! only makes them durable in `flush_all()` — normally driven by the manifest
//! ticker or by `Drop`. But `Drop` "only fires once every owning handle is gone",
//! and a ticker thread (or a server that blocks forever in `serve()`) holds an
//! `Arc<Db>` for the whole process lifetime — so on a hard exit (`Ctrl+C`,
//! `SIGTERM` from an orchestrator, `kill`) `Drop` NEVER runs and the writes staged
//! since the last tick are lost. That is the gap this module closes.
//!
//! [`Db::install_exit_flush`] registers a durable database to be flushed when the
//! process receives `SIGINT` or `SIGTERM`. It flushes exactly once, on the way
//! out — NOT on every put — so the hot write path stays hot.
//!
//! # Design
//!
//! - **Opt-in.** A *library* that unilaterally seizes signal handlers would
//!   trample a host application's own shutdown logic, so the core never installs
//!   this implicitly. The napi (`nedb-node`) and pyo3 (`nedb-py`) `open()` paths
//!   call it for durable databases — so Node and Python embedders get
//!   flush-on-exit for free — and Rust applications call it explicitly. `nedbd`
//!   keeps its own tokio graceful-shutdown handler and does not use this.
//!
//! - **Async-signal-safe.** The installed handler does exactly one thing: a
//!   non-blocking `write(2)` of the signal number to a self-pipe (`write` is on
//!   the POSIX async-signal-safe list). A dedicated reader thread blocks on the
//!   read end; when woken it runs `flush_all()` on every registered database from
//!   a *normal* thread context (locks, allocation and file I/O are all safe
//!   there), restores the signal's default disposition, and re-raises it so the
//!   process terminates with the correct `128 + signum` status.
//!
//! - **Idempotent.** The handler and reader thread are installed once per
//!   process; each subsequent call just registers another database. In-memory
//!   (`:memory:`) databases are ignored — there is nothing to flush.
//!
//! - **Weak references.** The registry holds `Weak<Db>`, so a registered database
//!   that is otherwise dropped is not kept alive (no leak) and is pruned on the
//!   next flush pass.

use std::sync::{Arc, Mutex, OnceLock, Weak};
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use crate::db::Db;

/// Registered durable databases to flush on exit. Weak so we never keep a `Db`
/// alive past its owner.
static REGISTRY: OnceLock<Mutex<Vec<Weak<Db>>>> = OnceLock::new();

fn registry() -> &'static Mutex<Vec<Weak<Db>>> {
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

/// Flush every still-live registered database. Runs on the reader thread (normal
/// context) — safe to take locks and do I/O. Prunes dead weak refs as it goes.
fn flush_all_registered() {
    if let Some(reg) = REGISTRY.get() {
        let mut guard = match reg.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(), // a panicked writer must not stop the flush
        };
        guard.retain(|w| w.strong_count() > 0);
        for w in guard.iter() {
            if let Some(db) = w.upgrade() {
                db.flush_all();
            }
        }
    }
}

impl Db {
    /// Flush this durable database's buffered state on `SIGINT`/`SIGTERM`
    /// (`Ctrl+C`, `kill`, orchestrator shutdown) — the flush-on-close contract
    /// extended to hard exits that never run `Drop`.
    ///
    /// Call once, after the database is wrapped in an `Arc` (the registry holds a
    /// `Weak`, so this never keeps the `Db` alive). Idempotent; safe to call from
    /// multiple databases. A no-op for in-memory (`:memory:`) databases.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use nedb_engine::Db;
    /// let db = Arc::new(Db::open(std::path::Path::new("/data/mydb"), None)?);
    /// Db::install_exit_flush(Arc::clone(&db));   // durable across Ctrl+C / SIGTERM
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn install_exit_flush(self_arc: Arc<Db>) {
        // Nothing to flush for an in-memory database.
        if self_arc.root == std::path::PathBuf::from(":memory:") {
            return;
        }
        // Register (dedup by pointer identity so repeated calls don't stack).
        {
            let mut reg = match registry().lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            let already = reg
                .iter()
                .any(|w| w.upgrade().is_some_and(|a| Arc::ptr_eq(&a, &self_arc)));
            if !already {
                reg.push(Arc::downgrade(&self_arc));
            }
        }
        install_signal_handler_once();
    }
}

// ── Unix: self-pipe + sigaction, dependency-free (libc). ─────────────────────

#[cfg(unix)]
static INSTALLED: AtomicBool = AtomicBool::new(false);
/// Write end of the self-pipe, read by the signal handler. `-1` until installed.
#[cfg(unix)]
static PIPE_WRITE_FD: AtomicI32 = AtomicI32::new(-1);

/// The signal handler. MUST be async-signal-safe: it does nothing but write the
/// signal number to the self-pipe (non-blocking, so it can never stall the
/// interrupted thread). All real work happens on the reader thread.
#[cfg(unix)]
extern "C" fn handler(sig: libc::c_int) {
    let fd = PIPE_WRITE_FD.load(Ordering::SeqCst);
    if fd >= 0 {
        let byte = [sig as u8];
        // write(2) is async-signal-safe; ignore the result (EAGAIN if the pipe is
        // already full means a signal is already pending — which is all we need).
        unsafe {
            let _ = libc::write(fd, byte.as_ptr() as *const libc::c_void, 1);
        }
    }
}

#[cfg(unix)]
fn install_signal_handler_once() {
    // Exactly one handler + reader thread per process.
    if INSTALLED.swap(true, Ordering::SeqCst) {
        return;
    }
    unsafe {
        // Self-pipe. write end non-blocking so the handler never blocks.
        let mut fds = [0i32; 2];
        if libc::pipe(fds.as_mut_ptr()) != 0 {
            INSTALLED.store(false, Ordering::SeqCst); // let a later call retry
            return;
        }
        let (read_fd, write_fd) = (fds[0], fds[1]);
        let flags = libc::fcntl(write_fd, libc::F_GETFL);
        if flags != -1 {
            libc::fcntl(write_fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
        PIPE_WRITE_FD.store(write_fd, Ordering::SeqCst);

        // Install the handler for SIGINT (Ctrl+C) and SIGTERM (kill / orchestrator).
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = handler as extern "C" fn(libc::c_int) as libc::sighandler_t;
        libc::sigemptyset(&mut sa.sa_mask);
        sa.sa_flags = libc::SA_RESTART;
        libc::sigaction(libc::SIGINT, &sa, std::ptr::null_mut());
        libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut());

        // Reader thread: block on the pipe, flush, restore default, re-raise.
        std::thread::Builder::new()
            .name("nedb-exit-flush".into())
            .spawn(move || {
                reader_loop(read_fd);
            })
            .ok();
    }
}

/// Block on the self-pipe. On the first signal: flush every registered database,
/// restore that signal's default disposition, and re-raise so the process
/// terminates with the correct status. Never returns.
#[cfg(unix)]
fn reader_loop(read_fd: i32) -> ! {
    let mut buf = [0u8; 1];
    loop {
        let n = unsafe { libc::read(read_fd, buf.as_mut_ptr() as *mut libc::c_void, 1) };
        if n <= 0 {
            continue; // EINTR / spurious wakeup — keep waiting
        }
        let sig = buf[0] as libc::c_int;

        flush_all_registered();

        // Restore default disposition and re-raise: preserves 128+signum exit
        // status and lets the OS terminate us the way the sender intended.
        unsafe {
            let mut sa: libc::sigaction = std::mem::zeroed();
            sa.sa_sigaction = libc::SIG_DFL;
            libc::sigemptyset(&mut sa.sa_mask);
            libc::sigaction(sig, &sa, std::ptr::null_mut());
            libc::raise(sig);
        }
        // If raise somehow returns, fall back to a clean exit after flushing.
        std::process::exit(128 + sig);
    }
}

// ── Non-Unix: no POSIX signals. Durability on exit relies on `Drop` / an
//    explicit `flush()`. Documented, honest no-op. ────────────────────────────

#[cfg(not(unix))]
fn install_signal_handler_once() {
    // Windows has no SIGTERM; SIGINT semantics differ. Embedders on non-Unix
    // should flush explicitly on shutdown (or rely on `Drop` for short-lived
    // handles). Left as a no-op rather than pretending to install a handler.
}

#[cfg(test)]
mod tests {
    use super::*;

    /// In-memory databases have nothing to flush → must never be registered.
    /// Checks this specific handle (not the registry length), so it stays correct
    /// under parallel test runs that share the process-global registry.
    #[test]
    fn in_memory_is_not_registered() {
        let db = Arc::new(Db::in_memory());
        Db::install_exit_flush(Arc::clone(&db));
        let found = registry()
            .lock()
            .unwrap()
            .iter()
            .any(|w| w.upgrade().is_some_and(|a| Arc::ptr_eq(&a, &db)));
        assert!(!found, ":memory: db must not be registered");
    }

    /// Registering the same durable db twice adds exactly one registry entry.
    #[test]
    fn durable_registration_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Db::open(dir.path(), None).unwrap());
        let count = || {
            registry()
                .lock()
                .unwrap()
                .iter()
                .filter(|w| w.upgrade().is_some_and(|a| Arc::ptr_eq(&a, &db)))
                .count()
        };
        Db::install_exit_flush(Arc::clone(&db));
        assert_eq!(count(), 1, "first install registers exactly once");
        Db::install_exit_flush(Arc::clone(&db));
        assert_eq!(count(), 1, "second install does not duplicate");
    }

    /// The flush path the reader thread runs makes staged writes durable: write,
    /// flush via the registry, reopen from disk, and the doc is present. Exercises
    /// everything the signal path does except the raise-and-die tail (which cannot
    /// be asserted in-process — see tests/exit_flush_signal.rs for the
    /// child-process end-to-end proof).
    #[test]
    fn registered_flush_makes_writes_durable_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        {
            let db = Arc::new(Db::open(&path, None).unwrap());
            Db::install_exit_flush(Arc::clone(&db));
            db.put("k", "v1", serde_json::json!({ "n": 1 }), vec![], None, None)
                .unwrap();
            flush_all_registered(); // what the reader thread does on SIGTERM (no raise)
        }
        let reopened = Db::open(&path, None).unwrap();
        let got = reopened.get("k", "v1");
        assert!(got.is_some(), "write must survive flush + reopen");
        assert_eq!(got.unwrap().data.get("n").and_then(|v| v.as_i64()), Some(1));
    }
}
