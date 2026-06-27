//! nedbd v2 — NEDB DAG storage daemon.
//!
//! Usage:
//!   nedbd-v2 [OPTIONS] [DATA_DIR]
//!
//! Real argument parsing (added in v2.4.2): flags are recognized instead of being
//! swallowed as the positional data dir. `--dag-v3` / `--fast-fsync` set the
//! corresponding engine env vars *before* the database is opened, so they take
//! effect (the engine reads them per-open). Flags take precedence over env vars.
//!
//! Environment (still honored as defaults):
//!   NEDBD_HOST=127.0.0.1    Bind address (default 127.0.0.1 — loopback only)
//!   NEDBD_PORT=7070         HTTP port (default 7070)
//!   NEDBD_TOKEN=<token>     Bearer token for auth (optional)
//!   NEDBD_MEMORY=1          Pure in-memory mode — no disk I/O, data lost on exit
//!   NEDB_DAG_V3=1           Use the v3 segment/pack object store (see --dag-v3)
//!   NEDB_FAST_FSYNC=1       macOS fast fsync (see --fast-fsync)
//!   NEDB_TMK=<32-byte-hex>  Master key for AES-256-GCM encryption (env-only —
//!                           never a CLI flag, so it never lands in shell history)

use nedb_engine::server;

const HELP: &str = "\
nedbd-v2 — NEDB v2/v3 DAG storage daemon

USAGE:
    nedbd-v2 [OPTIONS] [DATA_DIR]

OPTIONS:
    -d, --data <DIR>      Data directory (default: ./nedb-data)
        --dag-v3          Use the v3 segment/pack object store (sets NEDB_DAG_V3=1)
        --fast-fsync      macOS fast fsync — plain fsync(2) instead of F_FULLFSYNC
                          (sets NEDB_FAST_FSYNC=1; no-op off macOS).
                          Alias: --dag-fast-sync
    -H, --host <ADDR>     Bind address (default: 127.0.0.1 — loopback only)
    -p, --port <PORT>     HTTP port (default: 7070)
        --token <TOKEN>   Bearer token required on every request
    -m, --memory          Pure in-memory mode (no disk I/O; data lost on exit)
    -h, --help            Print this help and exit
    -V, --version         Print version and exit

ENVIRONMENT (flags take precedence when both are set):
    NEDBD_HOST  NEDBD_PORT  NEDBD_TOKEN  NEDBD_MEMORY  NEDB_DAG_V3  NEDB_FAST_FSYNC
    NEDB_TMK    32-byte hex master key for AES-256-GCM (env-only — never a flag)

EXAMPLES:
    nedbd-v2 ./data                  # v2 DAG (loose objects) at ./data
    nedbd-v2 --dag-v3 ./data         # v3 segment store at ./data
    nedbd-v2 --dag-v3 --data ./data --port 7171
    NEDB_DAG_V3=1 nedbd-v2 ./data    # env form (equivalent to --dag-v3)
";

/// Print an error to stderr and exit with code 2 (usage error).
fn die(msg: String) -> ! {
    eprintln!("nedbd-v2: {}", msg);
    eprintln!("Try 'nedbd-v2 --help' for usage.");
    std::process::exit(2);
}

/// Pull the value for a flag that requires one: either inline (`--flag=value`)
/// or the next argv token (`--flag value`). Advances `i` past a consumed token.
fn need_val(args: &[String], i: &mut usize, inline: Option<&str>, name: &str) -> String {
    if let Some(v) = inline {
        return v.to_string();
    }
    *i += 1;
    if *i >= args.len() {
        die(format!("option '{}' requires a value", name));
    }
    args[*i].clone()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Defaults come from the environment (preserved for back-compat); any CLI
    // flag below overrides its env counterpart.
    let mut data_dir: Option<String> = None;
    let mut host = std::env::var("NEDBD_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let mut port: u16 = std::env::var("NEDBD_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7070);
    let mut token = std::env::var("NEDBD_TOKEN").ok().filter(|s| !s.is_empty());
    let mut memory_mode = std::env::var("NEDBD_MEMORY")
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let raw = args[i].clone();
        let (key, inline): (String, Option<String>) = match raw.split_once('=') {
            Some((k, v)) => (k.to_string(), Some(v.to_string())),
            None => (raw.clone(), None),
        };

        match key.as_str() {
            "-h" | "--help" => {
                print!("{}", HELP);
                return Ok(());
            }
            "-V" | "--version" => {
                println!("nedbd-v2 {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            "--dag-v3" => {
                // Engaged before the Db is opened below; the ObjectStore reads
                // NEDB_DAG_V3 at open time, so setting it here takes effect.
                std::env::set_var("NEDB_DAG_V3", "1");
            }
            "--fast-fsync" | "--dag-fast-sync" => {
                std::env::set_var("NEDB_FAST_FSYNC", "1");
            }
            "-m" | "--memory" => {
                memory_mode = true;
            }
            "-d" | "--data" => {
                data_dir = Some(need_val(&args, &mut i, inline.as_deref(), "--data"));
            }
            "-H" | "--host" => {
                host = need_val(&args, &mut i, inline.as_deref(), "--host");
            }
            "-p" | "--port" => {
                let v = need_val(&args, &mut i, inline.as_deref(), "--port");
                port = v
                    .parse()
                    .unwrap_or_else(|_| die(format!("invalid --port '{}': expected 0-65535", v)));
            }
            "--token" => {
                token = Some(need_val(&args, &mut i, inline.as_deref(), "--token"))
                    .filter(|s| !s.is_empty());
            }
            // Any other dash-prefixed token is an unknown flag — never silently
            // treated as the data dir (that was the v2.4.1 bug).
            _ if key.starts_with('-') && key != "-" => {
                die(format!("unknown option '{}'", key));
            }
            // First bare token is the positional data dir (legacy form).
            _ => {
                if data_dir.is_some() {
                    die(format!("unexpected extra argument '{}'", raw));
                }
                data_dir = Some(raw);
            }
        }
        i += 1;
    }

    let tmk: Option<[u8; 32]> = std::env::var("NEDB_TMK")
        .ok()
        .and_then(|s| hex::decode(s).ok())
        .and_then(|b| b.try_into().ok());

    let data_dir = data_dir.unwrap_or_else(|| "./nedb-data".to_string());

    server::run(&host, port, &data_dir, tmk, token, memory_mode).await
}
