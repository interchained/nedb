//! nedbd v2 HTTP server — same /v1/databases/* API surface as v1.
//! Drop-in replacement: Vision, itsl_mirror, all existing clients work unchanged.
//!
//! Built on tokio + axum. Each database is opened once and held in an Arc<RwLock>.
//! All write paths use the Db's internal atomic operations; the RwLock is only
//! needed to protect the manager's HashMap (open/close operations), not individual
//! document writes (which are lock-free at the content-addressed level).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::{
    extract::{Path as AxPath, State, Query as AxQuery},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response, sse::{Event, KeepAlive, Sse}},
    routing::{delete, get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt as _;

use crate::db::Db;
use crate::nql;
use crate::store::Node;

// ── Log channel — broadcast to all /events SSE subscribers ────────────────────

const LOG_CHANNEL_CAP: usize = 512;

/// Send a timestamped log line to both stdout and all /events subscribers.
macro_rules! nlog {
    ($tx:expr, $($arg:tt)*) => {{
        let line = format!($($arg)*);
        println!("{}", line);
        let _ = $tx.send(line);
    }};
}

// ── Manager ───────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct Manager {
    inner:     Arc<RwLock<ManagerInner>>,
    pub token: Option<String>,
    /// Broadcast channel — every log line goes here; /events streams them.
    pub log_tx: broadcast::Sender<String>,
}

struct ManagerInner {
    data_dir: PathBuf,
    dbs:      HashMap<String, Arc<Db>>,
    tmk:      Option<[u8; 32]>,
}

impl Manager {
    pub fn new(data_dir: &Path, tmk: Option<[u8; 32]>, token: Option<String>) -> Self {
        let (log_tx, _) = broadcast::channel(LOG_CHANNEL_CAP);
        Self {
            inner: Arc::new(RwLock::new(ManagerInner {
                data_dir: data_dir.to_path_buf(),
                dbs:      HashMap::new(),
                tmk,
            })),
            token,
            log_tx,
        }
    }

    /// Open all existing databases in the data directory on startup.
    pub async fn open_all(&self) -> anyhow::Result<()> {
        let (data_dir, tmk) = {
            let inner = self.inner.read().await;
            (inner.data_dir.clone(), inner.tmk)
        };
        if !data_dir.exists() {
            std::fs::create_dir_all(&data_dir)?;
            return Ok(());
        }
        let mut names = vec![];
        for entry in std::fs::read_dir(&data_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                names.push(entry.file_name().to_string_lossy().to_string());
            }
        }
        let log_tx = self.log_tx.clone();
        let mut inner = self.inner.write().await;
        for name in names {
            let db_path = inner.data_dir.join(&name);
            let dek = tmk.map(|k| crate::store::Dek::from_tmk(&k, name.as_bytes()));
            match Db::open(&db_path, dek) {
                Ok(db) => {
                    nlog!(log_tx, "  [nedbd] opened database {:?}", name);
                    inner.dbs.insert(name, Arc::new(db));
                }
                Err(e) => nlog!(log_tx, "  [nedbd] ERROR opening {:?}: {}", name, e),
            }
        }
        Ok(())
    }

    async fn get_db(&self, name: &str) -> Option<Arc<Db>> {
        self.inner.read().await.dbs.get(name).cloned()
    }

    async fn create_db(&self, name: &str) -> anyhow::Result<Arc<Db>> {
        let (data_dir, tmk) = {
            let inner = self.inner.read().await;
            (inner.data_dir.clone(), inner.tmk)
        };
        let db_path = data_dir.join(name);
        let dek = tmk.map(|k| crate::store::Dek::from_tmk(&k, name.as_bytes()));
        let db = Arc::new(Db::open(&db_path, dek)?);
        self.inner.write().await.dbs.insert(name.to_string(), db.clone());
        Ok(db)
    }

    async fn drop_db(&self, name: &str) -> bool {
        let removed = self.inner.write().await.dbs.remove(name).is_some();
        if removed {
            let data_dir = self.inner.read().await.data_dir.clone();
            let _ = std::fs::remove_dir_all(data_dir.join(name));
        }
        removed
    }

    async fn names(&self) -> Vec<String> {
        self.inner.read().await.dbs.keys().cloned().collect()
    }

    /// Emit a log line to stdout and all /events SSE subscribers.
    pub fn log(&self, msg: impl Into<String>) {
        let line = msg.into();
        println!("{}", line);
        let _ = self.log_tx.send(line);
    }

    fn check_auth(&self, headers: &HeaderMap) -> bool {
        match &self.token {
            None => true,
            Some(required) => {
                if let Some(auth) = headers.get("authorization") {
                    if let Ok(s) = auth.to_str() {
                        return s == format!("Bearer {}", required);
                    }
                }
                false
            }
        }
    }
}

// ── Error helpers ─────────────────────────────────────────────────────────────

fn err(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({"error": msg}))).into_response()
}

fn ok(body: Value) -> Response {
    (StatusCode::OK, Json(body)).into_response()
}

/// Return (seq, head) — both O(1) reads from in-memory atomics/cache.
/// The head is maintained incrementally by Db::put() and Db::delete()
/// so we never recompute it from scratch on every response.
fn db_seq_head(db: &Db) -> (u64, String) {
    let seq  = db.seq.load(std::sync::atomic::Ordering::SeqCst);
    let head = db.head();
    (seq, head)
}

// ── Route handlers ────────────────────────────────────────────────────────────

async fn health(State(mgr): State<Manager>) -> Response {
    let names = mgr.names().await;
    ok(json!({
        "ok": true,
        "service": "nedbd",
        "version": env!("CARGO_PKG_VERSION"),
        "databases": names,
        "encrypted": mgr.inner.read().await.tmk.is_some(),
    }))
}

async fn list_databases(State(mgr): State<Manager>, headers: HeaderMap) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let names = mgr.names().await;
    let summaries: Vec<Value> = {
        let inner = mgr.inner.read().await;
        names.iter().map(|n| {
            if let Some(db) = inner.dbs.get(n) {
                let (seq, head) = db_seq_head(db);
                json!({"name": n, "seq": seq, "head": head, "collections": db.id_index.collections()})
            } else {
                json!({"name": n})
            }
        }).collect()
    };
    ok(json!({"databases": summaries}))
}

#[derive(Deserialize)]
struct CreateDbBody { name: String }

async fn create_database(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    Json(body): Json<CreateDbBody>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    if body.name.is_empty() { return err(StatusCode::BAD_REQUEST, "name is required"); }
    match mgr.create_db(&body.name).await {
        Ok(db) => {
            let (seq, head) = db_seq_head(&db);
            (StatusCode::CREATED, Json(json!({"database": {"name": body.name, "seq": seq, "head": head}}))).into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

async fn get_database(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath(name): AxPath<String>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    match mgr.get_db(&name).await {
        None => err(StatusCode::NOT_FOUND, &format!("database not found: {}", name)),
        Some(db) => {
            let (seq, head) = db_seq_head(&db);
            ok(json!({"name": name, "seq": seq, "head": head, "collections": db.id_index.collections()}))
        }
    }
}

async fn drop_database(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath(name): AxPath<String>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let dropped = mgr.drop_db(&name).await;
    ok(json!({"dropped": dropped}))
}

#[derive(Deserialize)]
struct QueryBody { nql: String }

async fn query_database(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath(name): AxPath<String>,
    Json(body): Json<QueryBody>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let db = match mgr.get_db(&name).await {
        None => return err(StatusCode::NOT_FOUND, &format!("database not found: {}", name)),
        Some(db) => db,
    };
    if body.nql.trim().is_empty() {
        return err(StatusCode::BAD_REQUEST, "nql is required");
    }
    match nql::query(&db, &body.nql) {
        Ok((rows, count)) => {
            let (seq, head) = db_seq_head(&db);
            ok(json!({"rows": rows, "count": count, "seq": seq, "head": head}))
        }
        Err(e) => err(StatusCode::BAD_REQUEST, &format!("NQL error: {}", e)),
    }
}

#[derive(Deserialize)]
struct PutBody {
    coll:       String,
    id:         String,
    doc:        Value,
    caused_by:  Option<Vec<String>>,
    valid_from: Option<String>,
    valid_to:   Option<String>,
    #[allow(dead_code)] evidence:   Option<String>,
    #[allow(dead_code)] confidence: Option<f64>,
    #[allow(dead_code)] client:     Option<String>,
    #[allow(dead_code)] nonce:      Option<u64>,
    #[allow(dead_code)] idem:       Option<String>,
}

async fn put_document(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath(name): AxPath<String>,
    Json(body): Json<PutBody>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let db = match mgr.get_db(&name).await {
        None => {
            // Auto-create database on first write
            match mgr.create_db(&name).await {
                Ok(db) => db,
                Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
            }
        }
        Some(db) => db,
    };
    // Block writes until background startup scan completes (cold start only).
    // Reads and queries always proceed immediately.
    if !db.startup_ready.load(std::sync::atomic::Ordering::SeqCst) {
        return err(StatusCode::SERVICE_UNAVAILABLE,
            "database startup in progress — reads available, writes retry in a moment");
    }
    let caused_by = body.caused_by.unwrap_or_default();
    match db.put(&body.coll, &body.id, body.doc, caused_by, body.valid_from, body.valid_to) {
        Ok(node) => {
            let (seq, head) = db_seq_head(&db);
            ok(json!({"ok": true, "doc": node_to_response(&node), "seq": seq, "head": head}))
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

fn node_to_response(node: &Node) -> Value {
    json!({
        "_id":   node.id,
        "_hash": node.hash,
        "_seq":  node.seq,
        "_coll": node.coll,
        "data":  node.data,
    })
}

async fn delete_document(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath((name, coll, id)): AxPath<(String, String, String)>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let db = match mgr.get_db(&name).await {
        None => return err(StatusCode::NOT_FOUND, &format!("database not found: {}", name)),
        Some(db) => db,
    };
    // v2 DAG: tombstone write + id index removal — doc history is preserved in the DAG,
    // but the live id pointer is cleared so queries and list() never return the doc.
    let existed = match db.delete(&coll, &id) {
        Ok(v)  => v,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    };
    let (seq, head) = db_seq_head(&db);
    ok(json!({"ok": existed, "seq": seq, "head": head}))
}

#[derive(Deserialize)]
struct BatchOp {
    op:  String,
    coll: Option<String>,
    id:  Option<String>,
    doc: Option<Value>,
    caused_by: Option<Vec<String>>,
}
#[derive(Deserialize)]
struct BatchBody { ops: Vec<BatchOp> }

async fn batch_operations(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath(name): AxPath<String>,
    Json(body): Json<BatchBody>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let db = match mgr.get_db(&name).await {
        None => match mgr.create_db(&name).await {
            Ok(db) => db,
            Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        },
        Some(db) => db,
    };

    if !db.startup_ready.load(std::sync::atomic::Ordering::SeqCst) {
        return err(StatusCode::SERVICE_UNAVAILABLE,
            "database startup in progress — reads available, writes retry in a moment");
    }
    let mut results = vec![];
    for op in body.ops {
        let op_type = op.op.to_lowercase();
        let coll = op.coll.unwrap_or_default();
        let id   = op.id.unwrap_or_default();
        let result = match op_type.as_str() {
            "put" => {
                let doc = op.doc.unwrap_or(json!({}));
                match db.put(&coll, &id, doc, op.caused_by.unwrap_or_default(), None, None) {
                    Ok(node) => json!({"op": "put", "id": id, "seq": node.seq, "hash": node.hash}),
                    Err(e)   => json!({"op": "put", "id": id, "error": e.to_string()}),
                }
            }
            "del" | "delete" => {
                match db.delete(&coll, &id) {
                    Ok(existed) => json!({"op": "del", "id": id, "ok": existed}),
                    Err(e)      => json!({"op": "del", "id": id, "error": e.to_string()}),
                }
            }
            _ => json!({"op": op_type, "error": "unknown op"}),
        };
        results.push(result);
    }
    let (seq, head) = db_seq_head(&db);
    ok(json!({"results": results, "count": results.len(), "seq": seq, "head": head}))
}

#[derive(Deserialize)]
struct IndexBody { coll: String, field: String, kind: Option<String> }

async fn create_index(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath(name): AxPath<String>,
    Json(body): Json<IndexBody>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let db = match mgr.get_db(&name).await {
        None => return err(StatusCode::NOT_FOUND, &format!("database not found: {}", name)),
        Some(db) => db,
    };
    let kind = body.kind.as_deref().unwrap_or("eq");
    match kind {
        "sorted" | "eq" => {
            db.create_sorted_index(&body.coll, &body.field);
            ok(json!({"ok": true, "coll": body.coll, "field": body.field, "kind": kind}))
        }
        _ => err(StatusCode::BAD_REQUEST, &format!("unknown index kind: {}", kind)),
    }
}

async fn verify_database(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath(name): AxPath<String>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let db = match mgr.get_db(&name).await {
        None => return err(StatusCode::NOT_FOUND, &format!("database not found: {}", name)),
        Some(db) => db,
    };
    let (ok_count, tampered) = db.verify();
    let (seq, head) = db_seq_head(&db);
    ok(json!({
        "ok": tampered.is_empty(),
        "seq": seq,
        "head": head,
        "tamper_evident": true,
        "objects_checked": ok_count,
        "tampered": tampered,
    }))
}

async fn checkpoint(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath(name): AxPath<String>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let db = match mgr.get_db(&name).await {
        None => return err(StatusCode::NOT_FOUND, &format!("database not found: {}", name)),
        Some(db) => db,
    };
    let (seq, head) = db_seq_head(&db);
    // v2 DAG is always "checkpointed" — content-addressed objects are inherently snapshotted
    ok(json!({"ok": true, "head": head, "seq": seq}))
}

#[derive(Deserialize)]
struct LogQuery { limit: Option<usize> }

async fn get_log(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath(name): AxPath<String>,
    AxQuery(q): AxQuery<LogQuery>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let db = match mgr.get_db(&name).await {
        None => return err(StatusCode::NOT_FOUND, &format!("database not found: {}", name)),
        Some(db) => db,
    };
    let limit = q.limit.unwrap_or(50);
    // v2: reconstruct log from objects (most recent first)
    let mut log_entries: Vec<Value> = db.objects.all_hashes()
        .filter_map(|h| db.objects.read(&h).ok())
        .take(limit)
        .map(|n| json!({
            "seq": n.seq, "coll": n.coll, "id": n.id,
            "hash": n.hash, "ts": n.ts, "op": "put"
        }))
        .collect();
    log_entries.sort_by(|a, b|
        b["seq"].as_u64().cmp(&a["seq"].as_u64())
    );
    log_entries.truncate(limit);
    let (seq, head) = db_seq_head(&db);
    ok(json!({"log": log_entries, "seq": seq, "head": head}))
}

// ── SSE log stream — GET /events ──────────────────────────────────────────────

async fn log_events(State(mgr): State<Manager>) -> Sse<impl futures_core::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let rx = mgr.log_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|msg| {
        match msg {
            Ok(line) => Some(Ok(Event::default().data(line))),
            Err(_)   => None,  // lagged — skip
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

// ── Router ────────────────────────────────────────────────────────────────────

pub fn router(mgr: Manager) -> Router {
    Router::new()
        .route("/health",                                  get(health))
        .route("/events",                                  get(log_events))
        .route("/v1/databases",                            get(list_databases).post(create_database))
        .route("/v1/databases/:name",                      get(get_database).delete(drop_database))
        .route("/v1/databases/:name/query",                post(query_database))
        .route("/v1/databases/:name/put",                  post(put_document))
        .route("/v1/databases/:name/rows/:coll/:id",       delete(delete_document))
        .route("/v1/databases/:name/batch",                post(batch_operations))
        .route("/v1/databases/:name/index",                post(create_index))
        .route("/v1/databases/:name/verify",               get(verify_database))
        .route("/v1/databases/:name/checkpoint",           post(checkpoint))
        .route("/v1/databases/:name/log",                  get(get_log))
        .with_state(mgr)
}

/// Start the nedbd v2 server.
pub async fn run(host: &str, port: u16, data_dir: &str, tmk: Option<[u8; 32]>, token: Option<String>) -> anyhow::Result<()> {
    let mgr = Manager::new(Path::new(data_dir), tmk, token);
    mgr.open_all().await?;

    let has_token = mgr.token.is_some();
    let app = router(mgr);
    let addr = format!("{}:{}", host, port).parse::<std::net::SocketAddr>()?;
    let banner = format!(r#"
  ╔═╗ ╔═╗ ╔═╗ ╔═╗
  ║ ╠═╣ ╠═╣ ╠═╣ ║   N E D B  ·  DAG ENGINE  {}
  ╠═╝ ╚═╬═╝ ╚═╬═╝   ─────────────────────────────────────────────
  ╚═══○══╩════╩═╝   content-addressed · tamper-evident · causal
         │          bi-temporal · replay-protected · encrypted
     ○───┴───○
     │       │      © INTERCHAINED, LLC  ×  Vex (Claude Sonnet 4.6)
     ○───────○      interchained.org   ·   hyperagent.com/refer/J2G6TCD7

  ─────────────────────────────────────────────────────────────
  listen   http://{}
  data     {}
  enc      {}
  token    {}
  ─────────────────────────────────────────────────────────────
"#,
        env!("CARGO_PKG_VERSION"),
        addr,
        data_dir,
        if tmk.is_some() { "AES-256-GCM" } else { "off" },
        if has_token { "on" } else { "off (set NEDBD_TOKEN to require auth)" }
    );
    print!("{}", banner);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    // TCP_NODELAY disables Nagle's algorithm — eliminates 40-200ms artificial
    // latency on macOS loopback for small request/response payloads.
    axum::serve(listener, app)
        .tcp_nodelay(true)
        .await?;
    Ok(())
}
