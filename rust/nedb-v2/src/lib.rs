//! NEDB v2 — Content-addressed DAG storage engine.
//!
//! Architecture:
//!
//!   objects/     — content-addressed, encrypted, BLAKE2b-verified nodes (immutable)
//!   indexes/     — id index (file-per-doc) + sorted indexes (BTreeMap, in-memory)
//!   graph/       — typed DAG edges (filesystem entries)
//!   MANIFEST     — BLAKE2b Merkle root of all collection HEADs
//!
//! Properties:
//!   - Uncorruptable: atomic writes (tmp→rename), hash verification on read
//!   - Parallel: no global lock on writes; each doc has its own index file
//!   - Instant cold start: no AOF replay; rebuild sorted indexes in parallel
//!   - Self-healing: migrate.rs handles v1→v2 on first startup
//!   - Tamper-evident: BLAKE2b chain from MANIFEST → collection heads → nodes

pub mod store;
pub mod segment;
pub mod index;
pub mod graph;
pub mod migrate;
pub mod db;
pub mod exit;
pub mod nql;
pub mod server;

pub use store::{Dek, Node, ObjectStore};
pub use index::{IdIndex, OrderedValue, SortedIndexes};
pub use graph::GraphStore;
pub use db::Db;
