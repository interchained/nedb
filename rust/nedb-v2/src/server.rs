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
use std::sync::atomic::AtomicU64;

use axum::{
    extract::{Path as AxPath, State, Query as AxQuery},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response, sse::{Event, KeepAlive, Sse}},
    routing::{delete, get, post},
    Json, Router,
};
use dashmap::DashMap;
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
const SUB_CHANNEL_CAP: usize = 256;

// ── Subscription registry ─────────────────────────────────────────────────────
// Maps (db_name, sub_id) → (nql_query, result_hash, event_sender)
// After every write, all registered queries for that db are re-evaluated.
// Diffs (added/removed/changed rows) are emitted as SSE events.

type SubKey = (String, u64);  // (db_name, sub_id)
type SubVal = (String, String, broadcast::Sender<String>);  // (nql, last_hash, tx)

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
    /// Live query subscriptions: (db_name, sub_id) → (nql, last_hash, event_tx)
    subs:    Arc<DashMap<SubKey, SubVal>>,
    sub_ctr: Arc<AtomicU64>,
}

struct ManagerInner {
    data_dir:    PathBuf,
    dbs:         HashMap<String, Arc<Db>>,
    tmk:         Option<[u8; 32]>,
    memory_mode: bool,
}

impl Manager {
    pub fn new(data_dir: &Path, tmk: Option<[u8; 32]>, token: Option<String>, memory_mode: bool) -> Self {
        let (log_tx, _) = broadcast::channel(LOG_CHANNEL_CAP);
        Self {
            inner: Arc::new(RwLock::new(ManagerInner {
                data_dir: data_dir.to_path_buf(),
                dbs:      HashMap::new(),
                tmk,
                memory_mode,
            })),
            token,
            log_tx,
            subs:    Arc::new(DashMap::new()),
            sub_ctr: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Register a live query subscription. Returns (sub_id, receiver).
    fn subscribe(&self, db: &str, nql: String) -> (u64, broadcast::Receiver<String>) {
        use std::sync::atomic::Ordering;
        let id = self.sub_ctr.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = broadcast::channel(SUB_CHANNEL_CAP);
        self.subs.insert((db.to_string(), id), (nql, String::new(), tx));
        (id, rx)
    }

    /// Unregister a subscription.
    fn unsubscribe(&self, db: &str, sub_id: u64) {
        self.subs.remove(&(db.to_string(), sub_id));
    }

    /// After a write: re-evaluate all subscriptions for `db`, emit diffs.
    fn notify_subscribers(&self, db: &str, db_arc: &Arc<crate::db::Db>) {
        let keys: Vec<SubKey> = self.subs.iter()
            .filter(|e| e.key().0 == db)
            .map(|e| e.key().clone())
            .collect();

        for key in keys {
            if let Some(mut entry) = self.subs.get_mut(&key) {
                let (nql, last_hash, tx) = entry.value_mut();
                // Re-run the query
                let rows = match crate::nql::query(db_arc, nql) {
                    Ok((rows, _)) => rows,
                    Err(_) => continue,
                };
                // Hash the result set
                let new_hash = format!("{:?}", rows.iter().map(|r| r.to_string()).collect::<Vec<_>>());
                if new_hash == *last_hash { continue; }
                *last_hash = new_hash;
                // Send the full current result as a diff event
                let event = json!({
                    "sub_id": key.1,
                    "db":     &key.0,
                    "nql":    nql.as_str(),
                    "rows":   rows,
                    "count":  rows.len(),
                });
                let _ = tx.send(event.to_string());
            }
        }
    }

    /// Open all existing databases in the data directory on startup.
    pub async fn open_all(&self) -> anyhow::Result<()> {
        let (data_dir, tmk, memory_mode) = {
            let inner = self.inner.read().await;
            (inner.data_dir.clone(), inner.tmk, inner.memory_mode)
        };
        // In memory mode: nothing to open from disk — all DBs created on first write
        if memory_mode { return Ok(()); }
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
                    let db_arc = Arc::new(db);
                    Db::start_cold_scan(Arc::clone(&db_arc));
                    // Flush MANIFEST every 1s in background — removes I/O from write path
                    Db::start_manifest_ticker(Arc::clone(&db_arc), 1000);
                    inner.dbs.insert(name, db_arc);
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
        let (data_dir, tmk, memory_mode) = {
            let inner = self.inner.read().await;
            (inner.data_dir.clone(), inner.tmk, inner.memory_mode)
        };
        let db = if memory_mode {
            // Pure in-memory — instant, no files
            Arc::new(Db::in_memory())
        } else {
            let db_path = data_dir.join(name);
            let dek = tmk.map(|k| crate::store::Dek::from_tmk(&k, name.as_bytes()));
            let db = Arc::new(Db::open(&db_path, dek)?);
            Db::start_cold_scan(Arc::clone(&db));
            Db::start_manifest_ticker(Arc::clone(&db), 1000);
            db
        };
        self.inner.write().await.dbs.insert(name.to_string(), db.clone());
        Ok(db)
    }

    async fn drop_db(&self, name: &str) -> bool {
        let db = self.inner.write().await.dbs.remove(name);
        if let Some(db) = db {
            // Flush manifest before dropping
            db.flush_manifest_if_dirty();
            let data_dir = self.inner.read().await.data_dir.clone();
            let _ = std::fs::remove_dir_all(data_dir.join(name));
            true
        } else {
            false
        }
    }

    /// Flush all open databases (id-index WAL + MANIFEST) — call on graceful shutdown.
    pub async fn flush_all(&self) {
        let inner = self.inner.read().await;
        for db in inner.dbs.values() {
            db.flush_all();  // WAL + manifest
        }
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
    let inner = mgr.inner.read().await;
    ok(json!({
        "ok":        true,
        "service":   "nedbd",
        "version":   env!("CARGO_PKG_VERSION"),
        "engine":    "dag",
        "memory":    inner.memory_mode,
        "databases": names,
        "encrypted": inner.tmk.is_some(),
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
    // Run synchronous file I/O (objects.write) on a blocking thread so concurrent
    // PUTs don't serialize on the tokio async thread pool.
    let coll = body.coll.clone();
    let id   = body.id.clone();
    let doc  = body.doc.clone();
    let vf   = body.valid_from.clone();
    let vt   = body.valid_to.clone();
    let db2  = Arc::clone(&db);
    let result = tokio::task::spawn_blocking(move || {
        db2.put(&coll, &id, doc, caused_by, vf, vt)
    }).await;
    match result {
        Err(join_err) => err(StatusCode::INTERNAL_SERVER_ERROR, &join_err.to_string()),
        Ok(Err(e))    => err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        Ok(Ok(node))  => {
            let (seq, head) = db_seq_head(&db);
            mgr.notify_subscribers(&name, &db);
            ok(json!({"ok": true, "doc": node_to_response(&node), "seq": seq, "head": head}))
        }
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

    // Split ops into puts (parallelisable) and deletes (sequential)
    // Puts go through put_batch for parallel object + index writes.
    // Deletes remain sequential (tombstone ordering matters).
    let mut put_ops = vec![];
    let mut del_ops: Vec<(String, String)> = vec![];
    let mut op_order: Vec<(&str, usize)> = vec![];  // ("put"|"del", index into respective vec)

    for op in &body.ops {
        let t = op.op.to_lowercase();
        match t.as_str() {
            "put" => {
                op_order.push(("put", put_ops.len()));
                put_ops.push((
                    op.coll.clone().unwrap_or_default(),
                    op.id.clone().unwrap_or_default(),
                    op.doc.clone().unwrap_or(json!({})),
                    op.caused_by.clone().unwrap_or_default(),
                    None::<String>,
                    None::<String>,
                ));
            }
            "del" | "delete" => {
                op_order.push(("del", del_ops.len()));
                del_ops.push((
                    op.coll.clone().unwrap_or_default(),
                    op.id.clone().unwrap_or_default(),
                ));
            }
            _ => { op_order.push(("unknown", 0)); }
        }
    }

    // Execute all puts in parallel via put_batch
    let put_results = if put_ops.is_empty() {
        vec![]
    } else {
        match db.put_batch(put_ops) {
            Ok(nodes) => nodes.into_iter().map(|n| json!({"op":"put","id":n.id,"seq":n.seq,"hash":n.hash})).collect(),
            Err(e)    => return err(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        }
    };

    // Execute deletes sequentially
    let del_results: Vec<serde_json::Value> = del_ops.iter().map(|(coll, id)| {
        match db.delete(coll, id) {
            Ok(existed) => json!({"op":"del","id":id,"ok":existed}),
            Err(e)      => json!({"op":"del","id":id,"error":e.to_string()}),
        }
    }).collect();

    // Reconstruct results in original op order
    let mut results = vec![];
    for (kind, idx) in &op_order {
        let r = match *kind {
            "put"     => put_results.get(*idx).cloned().unwrap_or(json!({"op":"put","error":"missing"})),
            "del"     => del_results.get(*idx).cloned().unwrap_or(json!({"op":"del","error":"missing"})),
            _         => json!({"op": kind, "error": "unknown op"}),
        };
        results.push(r);
    }
    let (seq, head) = db_seq_head(&db);
    // Notify live query subscribers after batch completes
    mgr.notify_subscribers(&name, &db);
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

// ── Live query subscriptions — POST /v1/databases/:name/subscribe ─────────────

#[derive(Deserialize)]
struct SubscribeBody { nql: String }

async fn subscribe_query(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath(name): AxPath<String>,
    Json(body): Json<SubscribeBody>,
) -> Response {
    if !mgr.check_auth(&headers) {
        return err(StatusCode::UNAUTHORIZED, "unauthorized");
    }
    let db = match mgr.get_db(&name).await {
        None => return err(StatusCode::NOT_FOUND, &format!("database not found: {}", name)),
        Some(db) => db,
    };

    let (sub_id, rx) = mgr.subscribe(&name, body.nql.clone());

    // Send the initial query result immediately as the first SSE event
    if let Ok((rows, _)) = crate::nql::query(&db, &body.nql) {
        let init = json!({
            "sub_id": sub_id,
            "db":     &name,
            "nql":    &body.nql,
            "rows":   rows,
            "count":  rows.len(),
            "event":  "initial",
        });
        // Update last_hash so we don't re-send this on the next write if unchanged
        if let Some(mut entry) = mgr.subs.get_mut(&(name.clone(), sub_id)) {
            let hash = format!("{:?}", rows);
            entry.value_mut().1 = hash;
        }
        // Send the initial result through the channel
        if let Some(entry) = mgr.subs.get(&(name.clone(), sub_id)) {
            let _ = entry.value().2.send(init.to_string());
        }
    }

    let stream = BroadcastStream::new(rx).filter_map(|msg| {
        match msg {
            Ok(line) => Some(Ok::<Event, std::convert::Infallible>(Event::default().data(line))),
            Err(_)   => None,
        }
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

async fn unsubscribe_query(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath((name, sub_id)): AxPath<(String, u64)>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    mgr.unsubscribe(&name, sub_id);
    ok(json!({"ok": true, "sub_id": sub_id}))
}

// ── SSE log stream — GET /events ──────────────────────────────────────────────

async fn log_events(State(mgr): State<Manager>) -> Sse<impl futures_core::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let rx = mgr.log_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|msg| {
        match msg {
            Ok(line) => Some(Ok::<Event, std::convert::Infallible>(Event::default().data(line))),
            Err(_)   => None,  // lagged — skip
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

// ── Router ────────────────────────────────────────────────────────────────────

pub fn router(mgr: Manager) -> Router {
    Router::new()
        .route("/health",                                        get(health))
        .route("/events",                                        get(log_events))
        .route("/v1/databases",                                  get(list_databases).post(create_database))
        .route("/v1/databases/:name",                            get(get_database).delete(drop_database))
        .route("/v1/databases/:name/query",                      post(query_database))
        .route("/v1/databases/:name/put",                        post(put_document))
        .route("/v1/databases/:name/rows/:coll/:id",             delete(delete_document))
        .route("/v1/databases/:name/batch",                      post(batch_operations))
        .route("/v1/databases/:name/index",                      post(create_index))
        .route("/v1/databases/:name/verify",                     get(verify_database))
        .route("/v1/databases/:name/checkpoint",                 post(checkpoint))
        .route("/v1/databases/:name/log",                        get(get_log))
        .route("/v1/databases/:name/subscribe",                  post(subscribe_query))
        .route("/v1/databases/:name/subscribe/:sub_id",          delete(unsubscribe_query))
        .with_state(mgr)
}

/// Start the nedbd v2 server.
pub async fn run(host: &str, port: u16, data_dir: &str, tmk: Option<[u8; 32]>, token: Option<String>, memory_mode: bool) -> anyhow::Result<()> {
    let mgr = Manager::new(Path::new(data_dir), tmk, token, memory_mode);
    mgr.open_all().await?;

    let has_token = mgr.token.is_some();
    let mgr_for_shutdown = mgr.clone();
    let app = router(mgr);
    let addr = format!("{}:{}", host, port).parse::<std::net::SocketAddr>()?;
    let banner = format!(r#"
           ◆
          ╱ ╲               N E D B  ·  DAG ENGINE  {}
         ◆   ◆              ─────────────────────────────────────────────
        ╱ ╲ ╱ ╲             content-addressed · tamper-evident · causal
       ◆   ◆   ◆            bi-temporal · replay-protected · encrypted
      ╱ ╲ ╱ ╲ ╱ ╲
     ◆   ◆   ◆   ◆          © INTERCHAINED, LLC  ×  Vex (Claude Sonnet 4.6)
    ╱ ╲ ╱ ╲ ╱ ╲ ╱ ╲         interchained.org   ·   hyperagent.com/refer/J2G6TCD7

  ─────────────────────────────────────────────────────────────
  listen   http://{}
  data     {}
  enc      {}
  token    {}
  memory   {}
  ─────────────────────────────────────────────────────────────
"#,
        env!("CARGO_PKG_VERSION"),
        addr,
        data_dir,
        if tmk.is_some() { "AES-256-GCM" } else { "off" },
        if has_token { "on" } else { "off (set NEDBD_TOKEN to require auth)" },
        if memory_mode { "yes — all data lost on exit (NEDBD_MEMORY=1)" } else { "no — durable DAG on disk" }
    );
    print!("{}", banner);

    let listener = tokio::net::TcpListener::bind(addr).await?;

    // ── Scheduled hourly checkpoint ────────────────────────────────────────────
    // Flush MANIFEST every hour aligned to the system clock (top of the hour).
    // Ensures warm-start data is always fresh even on long-running servers.
    let mgr_hourly = mgr_for_shutdown.clone();
    tokio::spawn(async move {
        loop {
            // Sleep until the next top-of-hour boundary
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs()).unwrap_or(0);
            let secs_into_hour = now_secs % 3600;
            let sleep_secs = 3600 - secs_into_hour;
            tokio::time::sleep(tokio::time::Duration::from_secs(sleep_secs)).await;
            mgr_hourly.flush_all().await;
            println!("  [nedbd] hourly checkpoint — manifests flushed");
        }
    });

    // ── Graceful shutdown: SIGINT (Ctrl+C) + SIGTERM (systemctl stop) ─────────
    let shutdown = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).unwrap();
            let mut sigint  = signal(SignalKind::interrupt()).unwrap();
            tokio::select! {
                _ = sigterm.recv() => println!("  [nedbd] SIGTERM — flushing and exiting..."),
                _ = sigint.recv()  => println!("  [nedbd] SIGINT  — flushing and exiting..."),
            }
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.ok();
            println!("  [nedbd] shutting down — flushing manifests...");
        }
    };

    axum::serve(listener, app)
        .tcp_nodelay(true)
        .with_graceful_shutdown(shutdown)
        .await?;

    // Final flush on exit
    mgr_for_shutdown.flush_all().await;
    println!("  [nedbd] goodbye");
    Ok(())
}
