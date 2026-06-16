//! nedbd v2 — NEDB DAG storage daemon.
//!
//! Usage:
//!   nedbd [data_dir]
//!
//! Environment:
//!   NEDBD_HOST=127.0.0.1    Bind address (default 127.0.0.1 — loopback only)
//!   NEDBD_PORT=7070         HTTP port (default 7070)
//!   NEDBD_TOKEN=<token>     Bearer token for auth (optional)
//!   NEDB_TMK=<32-byte-hex>  Master key for AES-256-GCM encryption (optional)

use nedb_core_v2::server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let data_dir = std::env::args().nth(1)
        .unwrap_or_else(|| "./nedb-data".to_string());

    let host = std::env::var("NEDBD_HOST")
        .unwrap_or_else(|_| "127.0.0.1".to_string());

    let port: u16 = std::env::var("NEDBD_PORT")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(7070);

    let token = std::env::var("NEDBD_TOKEN").ok()
        .filter(|s| !s.is_empty());

    let tmk: Option<[u8; 32]> = std::env::var("NEDB_TMK").ok()
        .and_then(|s| hex::decode(s).ok())
        .and_then(|b| b.try_into().ok());

    server::run(&host, port, &data_dir, tmk, token).await
}
