// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

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
    /// Natural-language planner. None unless built with --features cast AND
    /// enabled at runtime; the whole feature is opt-in so a default nedbd
    /// carries no model and no extra bytes.
    #[cfg(feature = "cast")]
    pub caster: Option<crate::cast::Caster>,
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
            #[cfg(feature = "cast")]
            caster: None,
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

// ── Natural-language planning (feature: cast) ─────────────────────────────────

// Both fields are read only by the `cast`-enabled handler. The
// `cfg(not(feature = "cast"))` stub still deserializes this body — so that a
// malformed request is rejected as 400 before the 501, keeping the two builds
// behaviourally consistent — but never looks at the values, which without this
// attribute produces a dead_code warning on every default build.
#[cfg_attr(not(feature = "cast"), allow(dead_code))]
#[derive(Deserialize)]
struct CastBody {
    prompt: String,
    /// Run the plan immediately. Defaults to FALSE on purpose: the endpoint hands
    /// back a plan for review rather than executing a guess. A planner that
    /// silently runs the wrong query is worse than one that admits uncertainty.
    #[serde(default)]
    execute: bool,
}

/// POST /v1/databases/:name/cast — turn a short English prompt into NQL.
///
/// The model only ever produces TEXT. Execution goes through the same
/// `nql::query` path a hand-typed query uses, so there is no second code path
/// with different validation.
#[cfg(feature = "cast")]
async fn cast_prompt(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath(name): AxPath<String>,
    Json(body): Json<CastBody>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }

    let caster = match &mgr.caster {
        Some(c) => c,
        None => return err(
            StatusCode::SERVICE_UNAVAILABLE,
            "cast is not enabled; start nedbd with --cast (or NEDBD_CAST=1) \
             and place model.cast in the data directory",
        ),
    };

    let db = match mgr.get_db(&name).await {
        None => return err(StatusCode::NOT_FOUND, &format!("database not found: {}", name)),
        Some(db) => db,
    };
    if body.prompt.trim().is_empty() {
        return err(StatusCode::BAD_REQUEST, "prompt is required");
    }

    // The engine knows the real schema, so constrain against it. This is the
    // whole reason the planner lives here instead of in a client.
    let collections = db.id_index.collections();
    let result = caster.cast_checked(&body.prompt, &collections);

    // Validate by PARSING, not by pattern-matching the text. The parser is the
    // only authority on whether something is runnable.
    let parse_err = match nql::parse(&result.nql) {
        Ok(_)  => None,
        Err(e) => Some(e.to_string()),
    };

    let (seq, head) = db_seq_head(&db);
    let mut out = json!({
        "prompt":            body.prompt,
        "nql":               result.nql,
        "valid":             parse_err.is_none(),
        "collection":        result.collection,
        "collection_known":  result.collection_known,
        "collections":       collections,
        "executed":          false,
        "seq":  seq,
        "head": head,
    });

    // A literal the model invented rather than copied. Advisory, not fatal —
    // the plan is well-formed and may be exactly right, so we surface it and
    // let the caller judge. Warned-about-and-correct is a cost worth paying to
    // avoid confidently-wrong-and-silent, which for an agent poisons every
    // subsequent step. Absent from the response when there is nothing to say,
    // so `"drift" in response` is a usable test.
    if let Some(d) = &result.drift {
        out["drift"] = json!(d);
    }

    if let Some(e) = parse_err {
        // Report the failure WITH the offending text. Never swallow it into an
        // empty result set — that reads as "no matching rows", which is a lie.
        out["error"] = json!(format!("NQL error: {}", e));
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(out)).into_response();
    }

    if !result.collection_known {
        // Parses fine, but names a collection this database does not have. That
        // is a model miss, not a user error, and it deserves to be said plainly
        // rather than returning zero rows.
        out["error"] = json!(format!(
            "collection {:?} does not exist in {:?}",
            result.collection.unwrap_or_default(), name
        ));
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(out)).into_response();
    }

    if !body.execute {
        return ok(out);
    }

    // Same executor as /query. No special path.
    let nql_text = out["nql"].as_str().unwrap_or("").to_string();
    match nql::query(&db, &nql_text) {
        Ok((rows, count)) => {
            out["executed"] = json!(true);
            out["rows"]     = json!(rows);
            out["count"]    = json!(count);
            ok(out)
        }
        Err(e) => {
            out["error"] = json!(format!("NQL error: {}", e));
            (StatusCode::BAD_REQUEST, Json(out)).into_response()
        }
    }
}

/// Stub so the route table compiles identically with the feature off. Callers get
/// a clear 501 instead of a 404, which would wrongly suggest the URL is wrong.
#[cfg(not(feature = "cast"))]
async fn cast_prompt(
    State(_mgr): State<Manager>,
    _headers: HeaderMap,
    AxPath(_name): AxPath<String>,
    Json(_body): Json<CastBody>,
) -> Response {
    err(
        StatusCode::NOT_IMPLEMENTED,
        "this nedbd was built without the `cast` feature; \
         rebuild with --features cast to enable natural-language planning",
    )
}

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
    caused_by:  Option<Vec<serde_json::Value>>,
    valid_from: Option<String>,
    valid_to:   Option<String>,
    #[allow(dead_code)] evidence:   Option<String>,
    #[allow(dead_code)] confidence: Option<f64>,
    #[allow(dead_code)] client:     Option<String>,
    #[allow(dead_code)] nonce:      Option<u64>,
    #[allow(dead_code)] idem:       Option<String>,
}

#[derive(Deserialize)]
struct LinkBody {
    frm: String,
    rel: String,
    to:  String,
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
    // Resolve caused_by items: accept hash strings (v2 native) OR seq integers (v1 compat).
    let caused_by: Vec<String> = body.caused_by.unwrap_or_default()
        .into_iter()
        .filter_map(|v| match v {
            serde_json::Value::String(s) => Some(s),
            serde_json::Value::Number(n) => {
                n.as_u64().and_then(|seq| db.get_hash_by_seq(seq))
            }
            _ => None,
        })
        .collect();
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

async fn link_document(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath(name): AxPath<String>,
    Json(body): Json<LinkBody>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let db = match mgr.get_db(&name).await {
        None => return err(StatusCode::NOT_FOUND, &format!("database not found: {}", name)),
        Some(db) => db,
    };
    if !db.startup_ready.load(std::sync::atomic::Ordering::SeqCst) {
        return err(StatusCode::SERVICE_UNAVAILABLE, "startup scan in progress");
    }
    match db.link(&body.frm, &body.rel, &body.to) {
        Ok(()) => {
            let (seq, head) = db_seq_head(&db);
            ok(json!({"ok": true, "frm": body.frm, "rel": body.rel, "to": body.to, "seq": seq, "head": head}))
        }
        Err(e) => err(StatusCode::BAD_REQUEST, &e.to_string()),
    }
}

/// `GET /v1/databases/:name/rows/:coll/:id` — fetch one document by id.
///
/// This route existed only for DELETE, so a client could remove a row by id
/// over HTTP but not READ one: it had to build `FROM coll WHERE _id = "..."`
/// and interpolate the id into a NQL string. That made every id containing a
/// double quote unreachable — `client.get()` returned None, meaning "no such
/// document", for a document `put()` had stored and `FROM coll` returned — and
/// an id ending in a backslash could not be escaped at all, because the lexer
/// collapses `\"` and would swallow the closing quote.
///
/// Taking the id from the URL path removes the string-building entirely: the
/// id arrives percent-decoded and byte-exact, with no quoting to get wrong and
/// no injection surface.
///
/// Returns the same flat row shape a query returns (`nql::node_to_json`), so
/// callers that previously used `rows[0]` from a query see no change.
///
/// `?as_of=N` resolves the version at or before sequence N, which is the
/// single-document form of time travel and previously had no HTTP surface at
/// all.
///
/// A missing row is `200 {"row": null}` rather than 404 — see the note in the
/// body for why that ambiguity had to go.
async fn get_document(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath((name, coll, id)): AxPath<(String, String, String)>,
    AxQuery(q): AxQuery<GetRowQuery>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let db = match mgr.get_db(&name).await {
        None => return err(StatusCode::NOT_FOUND, &format!("database not found: {}", name)),
        Some(db) => db,
    };
    let node = match q.as_of {
        Some(seq) => db.get_as_of(&coll, &id, seq),
        None      => db.get(&coll, &id),
    };
    // A MISSING ROW IS 200 WITH `row: null`, NOT 404.
    //
    // Deliberate, and it costs a little REST idiom to buy an unambiguous
    // client. A client must work against two server implementations (this one
    // and the Python AOF server) across several versions, and a server that
    // does not have this route at all also answers 404 — so a 404 here would
    // be indistinguishable from "route unavailable" and the client could not
    // tell "the row is absent" from "fall back to the query path". With this
    // shape: 200 means the route answered (row present or null), and any
    // 404/405 means the route is not there.
    let (seq, head) = db_seq_head(&db);
    let row = match node {
        None => Value::Null,
        Some(n) => crate::nql::node_to_json(&n),
    };
    ok(json!({"row": row, "seq": seq, "head": head}))
}

#[derive(Deserialize, Default)]
struct GetRowQuery {
    as_of: Option<u64>,
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
    caused_by: Option<Vec<serde_json::Value>>,
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
                // Resolve caused_by items: accept hash strings (v2 native) OR seq integers (v1 compat).
                let caused_by: Vec<String> = op.caused_by.clone().unwrap_or_default()
                    .into_iter()
                    .filter_map(|v| match v {
                        serde_json::Value::String(s) => Some(s),
                        serde_json::Value::Number(n) => {
                            n.as_u64().and_then(|seq| db.get_hash_by_seq(seq))
                        }
                        _ => None,
                    })
                    .collect();
                op_order.push(("put", put_ops.len()));
                put_ops.push((
                    op.coll.clone().unwrap_or_default(),
                    op.id.clone().unwrap_or_default(),
                    op.doc.clone().unwrap_or(json!({})),
                    caused_by,
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

// ── tip / since — GET /v1/databases/:name/{tip,since} ─────────────────────────
// tip()   = the most recent write (head of the log), O(1).
// since() = the changefeed: every write after ?after_seq (exclusive), ascending.
// Both return full nodes (Node: Serialize), alongside the current seq + head.

async fn tip_database(
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
    let tip = db.tip().map(|n| serde_json::to_value(&n).unwrap_or(Value::Null));
    ok(json!({"tip": tip, "seq": seq, "head": head}))
}

// Collection-local tip — GET /v1/databases/:name/collections/:coll/tip.
async fn tip_collection_database(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath((name, coll)): AxPath<(String, String)>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let db = match mgr.get_db(&name).await {
        None => return err(StatusCode::NOT_FOUND, &format!("database not found: {}", name)),
        Some(db) => db,
    };
    let (seq, head) = db_seq_head(&db);
    let tip = db.tip_collection(&coll).map(|n| serde_json::to_value(&n).unwrap_or(Value::Null));
    ok(json!({"coll": coll, "tip": tip, "seq": seq, "head": head}))
}

#[derive(Deserialize)]
struct SinceQuery { after_seq: Option<u64>, limit: Option<usize> }

async fn since_database(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath(name): AxPath<String>,
    AxQuery(q): AxQuery<SinceQuery>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let db = match mgr.get_db(&name).await {
        None => return err(StatusCode::NOT_FOUND, &format!("database not found: {}", name)),
        Some(db) => db,
    };
    let after = q.after_seq.unwrap_or(0);
    let b = db.since(after, q.limit.unwrap_or(0));
    let nodes: Vec<Value> = b.nodes.iter()
        .map(|n| serde_json::to_value(n).unwrap_or(Value::Null))
        .collect();
    let (seq, head) = db_seq_head(&db);
    ok(json!({
        "nodes": nodes, "count": nodes.len(),
        "from_seq": b.from_seq, "to_seq": b.to_seq, "head_seq": b.head_seq, "has_more": b.has_more,
        "seq": seq, "head": head
    }))
}

// Replication readiness — GET /v1/databases/:name/status. scan_complete is the
// hard gate for correctness-critical catch-up (see Db::scan_status).
async fn status_database(
    State(mgr): State<Manager>,
    headers: HeaderMap,
    AxPath(name): AxPath<String>,
) -> Response {
    if !mgr.check_auth(&headers) { return err(StatusCode::UNAUTHORIZED, "unauthorized"); }
    let db = match mgr.get_db(&name).await {
        None => return err(StatusCode::NOT_FOUND, &format!("database not found: {}", name)),
        Some(db) => db,
    };
    let s = db.scan_status();
    ok(json!({
        "ok": true,
        "scan_complete":   s.scan_complete,
        "tip_seq":         s.tip_seq,
        "indexed_seq_min": s.indexed_seq_min,
        "indexed_seq_max": s.indexed_seq_max,
        "indexed_count":   s.indexed_count
    }))
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
        .route("/v1/databases/:name/cast",                       post(cast_prompt))
        .route("/v1/databases/:name/put",                        post(put_document))
        .route("/v1/databases/:name/link",                       post(link_document))
        // GET was missing here: a row could be DELETEd by id over HTTP but not
        // READ by id, forcing clients to interpolate the id into a NQL string.
        .route("/v1/databases/:name/rows/:coll/:id",
               get(get_document).delete(delete_document))
        .route("/v1/databases/:name/batch",                      post(batch_operations))
        .route("/v1/databases/:name/index",                      post(create_index))
        .route("/v1/databases/:name/verify",                     get(verify_database))
        .route("/v1/databases/:name/checkpoint",                 post(checkpoint))
        .route("/v1/databases/:name/log",                        get(get_log))
        .route("/v1/databases/:name/tip",                        get(tip_database))
        .route("/v1/databases/:name/collections/:coll/tip",      get(tip_collection_database))
        .route("/v1/databases/:name/since",                      get(since_database))
        .route("/v1/databases/:name/status",                     get(status_database))
        .route("/v1/databases/:name/subscribe",                  post(subscribe_query))
        .route("/v1/databases/:name/subscribe/:sub_id",          delete(unsubscribe_query))
        .with_state(mgr)
}

/// Start the nedbd v2 server.
/// Lets the Postgres read endpoint share this process's already-open databases
/// instead of opening its own handles — which the exclusive data-dir LOCK would
/// refuse anyway, and rightly so.
impl crate::pgwire::DbResolver for Manager {
    fn resolve(&self, name: &str) -> Option<Arc<Db>> {
        // A blocking read on the manager map from the pgwire task. The lock is
        // only held across a HashMap lookup, never across I/O.
        let inner = self.inner.blocking_read();
        // An empty database name means the client did not send one; serve the
        // only database when that is unambiguous, which is the common case for
        // `psql -h host` against a single-database store.
        if name.is_empty() {
            if inner.dbs.len() == 1 {
                return inner.dbs.values().next().cloned();
            }
            return None;
        }
        inner.dbs.get(name).cloned()
    }
    fn token(&self) -> Option<String> {
        self.token.clone()
    }
}

pub async fn run(host: &str, port: u16, data_dir: &str, tmk: Option<[u8; 32]>, token: Option<String>, memory_mode: bool) -> anyhow::Result<()> {
    // `mut` is required by the cast block below, which assigns mgr.caster. With
    // the feature off nothing mutates it, so an unconditional `mut` warns on
    // every default build — and warnings people are used to seeing are warnings
    // people stop reading.
    #[cfg(feature = "cast")]
    let mut mgr = Manager::new(Path::new(data_dir), tmk, token, memory_mode);
    #[cfg(not(feature = "cast"))]
    let mgr = Manager::new(Path::new(data_dir), tmk, token, memory_mode);

    mgr.open_all().await?;

    // Load the natural-language planner if this build has the feature AND the
    // operator asked for it. Failure to load is reported loudly but is NOT fatal:
    // a missing model should not stop a database from serving queries.
    #[cfg(feature = "cast")]
    {
        let want = std::env::var("NEDBD_CAST").map(|v| v == "1").unwrap_or(false);
        if want {
            match crate::cast::Caster::load(Path::new(data_dir)) {
                Ok(c) => {
                    println!("  cast     enabled — {:.2}M params, vocab {}, {}",
                             c.n_params() as f64 / 1e6, c.vocab_size(), c.source());
                    mgr.caster = Some(c);
                }
                Err(e) => {
                    eprintln!("  cast     DISABLED — {}", e);
                }
            }
        }
    }
    // Freeze it: nothing past this point should mutate the manager. Only
    // meaningful in the cast build, where `mgr` was declared `mut` above.
    #[cfg(feature = "cast")]
    let mgr = mgr;

    let has_token = mgr.token.is_some();
    let mgr_for_shutdown = mgr.clone();
    // ── Postgres read endpoint ────────────────────────────────────────────────
    // Opt-in: nothing binds unless NEDBD_PG_PORT is set (or --pg-port passed).
    // Default-off is deliberate — a second listener is a second attack surface,
    // and it speaks cleartext, so the operator asks for it explicitly.
    if let Ok(raw) = std::env::var("NEDBD_PG_PORT") {
        match raw.trim().parse::<u16>() {
            Ok(pg_port) if pg_port > 0 => {
                let pg_host = host.to_string();
                let resolver: Arc<dyn crate::pgwire::DbResolver> = Arc::new(mgr.clone());
                tokio::spawn(async move {
                    if let Err(e) = crate::pgwire::run(&pg_host, pg_port, resolver).await {
                        eprintln!("  [pgwire] listener stopped: {}", e);
                    }
                });
            }
            _ => eprintln!("  [pgwire] ignoring NEDBD_PG_PORT={:?} — not a valid port", raw),
        }
    }

    let app = router(mgr);
    let addr = format!("{}:{}", host, port).parse::<std::net::SocketAddr>()?;
    let banner = format!(r#"
           ◆
          ╱ ╲               N E D B  ·  DAG ENGINE  {}
         ◆   ◆              ─────────────────────────────────────────────
        ╱ ╲ ╱ ╲             content-addressed · tamper-evident · causal
       ◆   ◆   ◆            bi-temporal · replay-protected · encrypted
      ╱ ╲ ╱ ╲ ╱ ╲
     ◆   ◆   ◆   ◆          © INTERCHAINED LLC × Vex (Interchained AI fleet: GLM · Claude · Opus · Fable · GPT-6)
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
