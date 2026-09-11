// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

//! nedb-wrap — embed NEDB causal provenance into the databases you already run (Rust).
//!
//! The Rust leg of the wrap adapter family. Where the Python and JS wrappers
//! shadow writes at the *connection* layer, the Rust surface embeds the DAG
//! engine directly and exposes the same layer-2 contract as a trait:
//!
//! ```ignore
//! use nedb_wrap::Surface;
//!
//! let mut s = Surface::in_memory();
//! s.register("driver:*", "driver");
//! s.shadow_writes = true;
//! let id = s.shadow("set", "driver:d1", br#"{"name":"Bob"}"#)?;
//! assert!(s.verify());
//! ```
//!
//! Adapters behind feature flags:
//! * `redis` — a [`WrapSink`](crate::sink::WrapSink) for the `redis` crate that
//!   records every pipelined write command into the DAG after it succeeds.
//!
//! Isolation guarantee: NEDB never writes to the host database's namespace.
//! Shadow data lives only in the embedded engine.

pub mod mapping;
pub mod sink;
pub mod surface;

pub use mapping::CollectionMapping;
pub use sink::Tracked;
pub use surface::{Shadowed, Surface};

/// Engine handle re-export — the DAG core the surface embeds.
pub use nedb_engine as engine;
