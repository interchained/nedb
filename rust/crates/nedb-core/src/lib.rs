//! nedb-core — the production speed core for NEDB.
//!
//! The OpLog is the single source of truth. Every mutation appends an Op; `apply`
//! deterministically folds an Op into the materialized state. State is a pure
//! function of the log — this gives crash recovery, time-travel, and determinism.

pub mod index;
pub mod log;
pub mod nql;
pub mod relations;
pub mod store;

pub use index::{IndexKind, Indexes, OrderedValue};
pub use log::{LogError, Op, OpLog, GENESIS};
pub use nql::{cmp, parse, Plan};
pub use relations::Relations;
pub use store::MvccStore;

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

use serde_json::Value;

// ── Apply ─────────────────────────────────────────────────────────────────────

fn apply(store: &mut MvccStore, rel: &mut Relations, idx: &mut Indexes, op: &Op) {
    match op.op.as_str() {
        "put" => {
            let key  = op.payload["key"].as_str().unwrap_or("");
            let coll = op.payload["coll"].as_str().unwrap_or("");
            let doc  = &op.payload["doc"];
            if let Some(old) = store.get(key, None).cloned() {
                idx.remove(coll, key, &old);
            }
            store.put(key, doc.clone(), op.seq);
            idx.add(coll, key, doc);
        }
        "delete" => {
            let key  = op.payload["key"].as_str().unwrap_or("");
            let coll = op.payload["coll"].as_str().unwrap_or("");
            if let Some(old) = store.get(key, None).cloned() {
                idx.remove(coll, key, &old);
            }
            store.delete(key, op.seq);
        }
        "link" => {
            let frm = op.payload["frm"].as_str().unwrap_or("");
            let r   = op.payload["rel"].as_str().unwrap_or("");
            let to  = op.payload["to"].as_str().unwrap_or("");
            rel.link(frm, r, to, op.seq);
        }
        "unlink" => {
            let frm = op.payload["frm"].as_str().unwrap_or("");
            let r   = op.payload["rel"].as_str().unwrap_or("");
            let to  = op.payload["to"].as_str().unwrap_or("");
            rel.unlink(frm, r, to, op.seq);
        }
        _ => {}
    }
}

// ── Db ────────────────────────────────────────────────────────────────────────

pub struct Db {
    pub log:     OpLog,
    pub store:   MvccStore,
    pub rel:     Relations,
    pub idx:     Indexes,
    nonce:       HashMap<String, u64>,
    // AOF persistence
    aof:         Option<File>,
    aof_path:    Option<PathBuf>,
}

impl Default for Db {
    fn default() -> Self { Self::new() }
}

impl Db {
    pub fn new() -> Self {
        Self {
            log:     OpLog::new(),
            store:   MvccStore::default(),
            rel:     Relations::default(),
            idx:     Indexes::default(),
            nonce:   HashMap::new(),
            aof:     None,
            aof_path: None,
        }
    }

    /// Open a durable database at `path`. Creates the directory if needed.
    /// Replays an existing `log.aof` on open.
    pub fn open(path: &str) -> std::io::Result<Self> {
        let dir = PathBuf::from(path);
        std::fs::create_dir_all(&dir)?;
        let aof_path = dir.join("log.aof");
        let mut db = Self::new();
        db.aof_path = Some(aof_path.clone());

        // Replay existing log
        if aof_path.exists() {
            let f = File::open(&aof_path)?;
            let reader = BufReader::new(f);
            let mut ops: Vec<Op> = Vec::new();
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() { continue; }
                if let Ok(op) = serde_json::from_str::<Op>(&line) {
                    ops.push(op);
                }
            }
            db.log.load(ops.clone());
            for op in &ops {
                // Skip checkpoint ops — they're chain entries, not data ops
                if op.op == "checkpoint" { continue; }
                apply(&mut db.store, &mut db.rel, &mut db.idx, op);
            }
            db.nonce = db.log.nonce_map();
        }

        // Open for appending
        let file = OpenOptions::new().append(true).create(true).open(&aof_path)?;
        db.aof = Some(file);
        Ok(db)
    }

    fn write_aof(&mut self, op: &Op) {
        if let Some(ref mut f) = self.aof {
            let _ = writeln!(f, "{}", serde_json::to_string(op).unwrap_or_default());
            let _ = f.flush();
            // fsync for durability
            #[cfg(unix)]
            { use std::os::unix::io::AsRawFd; unsafe { libc::fsync(f.as_raw_fd()); } }
        }
    }

    fn next_nonce(&mut self, client: &str) -> u64 {
        let n = self.nonce.get(client).copied().unwrap_or(0) + 1;
        self.nonce.insert(client.to_string(), n);
        n
    }

    fn append(
        &mut self,
        client: &str, nonce: u64,
        op: &str, payload: Value,
        idem: Option<String>,
    ) -> Result<(Op, bool), LogError> {
        let (rec, created) = self.log.append(client, nonce, op, payload, idem)?;
        if created {
            apply(&mut self.store, &mut self.rel, &mut self.idx, &rec);
            self.write_aof(&rec);
        }
        Ok((rec, created))
    }

    // ── Public API — mirrors Python reference ─────────────────────────────────

    pub fn create_index(&mut self, coll: &str, field: &str, kind: IndexKind) {
        self.idx.ensure(coll, field, kind.clone());
        // Backfill existing rows
        let prefix = format!("{coll}:");
        let keys: Vec<String> = self.store.keys(&prefix).into_iter().cloned().collect();
        for k in keys {
            if let Some(doc) = self.store.get(&k, None).cloned() {
                self.idx.add(coll, &k, &doc);
            }
        }
    }

    pub fn put(
        &mut self,
        coll: &str, id: &str, doc: Value,
        client: Option<&str>, nonce: Option<u64>, idem: Option<String>,
    ) -> Result<Value, LogError> {
        let client = client.unwrap_or("local");
        let nonce  = nonce.unwrap_or_else(|| self.next_nonce(client));
        let key    = format!("{coll}:{id}");
        let mut doc = match doc {
            Value::Object(mut m) => { m.insert("_id".into(), Value::String(id.to_string())); Value::Object(m) }
            other => other,
        };
        let payload = serde_json::json!({"key": key, "coll": coll, "id": id, "doc": doc});
        let (op, _) = self.append(client, nonce, "put", payload, idem)?;
        Ok(self.store.get(&key, None).cloned().unwrap_or(Value::Null))
    }

    pub fn delete(
        &mut self,
        coll: &str, id: &str,
        client: Option<&str>, nonce: Option<u64>, idem: Option<String>,
    ) -> Result<(), LogError> {
        let client = client.unwrap_or("local");
        let nonce  = nonce.unwrap_or_else(|| self.next_nonce(client));
        let key    = format!("{coll}:{id}");
        let payload = serde_json::json!({"key": key, "coll": coll, "id": id});
        self.append(client, nonce, "delete", payload, idem)?;
        Ok(())
    }

    pub fn get(&self, coll: &str, id: &str, as_of: Option<u64>) -> Option<Value> {
        self.store.get(&format!("{coll}:{id}"), as_of).cloned()
    }

    pub fn link(&mut self, frm: &str, rel: &str, to: &str, client: Option<&str>, nonce: Option<u64>) -> Result<(), LogError> {
        let client = client.unwrap_or("local");
        let nonce  = nonce.unwrap_or_else(|| self.next_nonce(client));
        let payload = serde_json::json!({"frm": frm, "rel": rel, "to": to});
        self.append(client, nonce, "link", payload, None)?;
        Ok(())
    }

    pub fn unlink(&mut self, frm: &str, rel: &str, to: &str, client: Option<&str>, nonce: Option<u64>) -> Result<(), LogError> {
        let client = client.unwrap_or("local");
        let nonce  = nonce.unwrap_or_else(|| self.next_nonce(client));
        let payload = serde_json::json!({"frm": frm, "rel": rel, "to": to});
        self.append(client, nonce, "unlink", payload, None)?;
        Ok(())
    }

    pub fn neighbors(&self, frm: &str, rel: &str, as_of: Option<u64>) -> Vec<String> {
        self.rel.neighbors(frm, rel, as_of)
    }

    pub fn inbound(&self, to: &str, rel: &str, as_of: Option<u64>) -> Vec<String> {
        self.rel.inbound(to, rel, as_of)
    }

    /// Execute a NQL query string. Returns matching documents as JSON Values.
    pub fn query(&self, nql: &str) -> Result<Vec<Value>, String> {
        let plan = parse(nql)?;
        self.execute(&plan)
    }

    pub fn execute(&self, plan: &Plan) -> Result<Vec<Value>, String> {
        let prefix = format!("{}:", plan.from);
        let as_of  = plan.as_of;

        // ── Candidate selection (index fast-paths) ────────────────────────────
        // IMPORTANT: index fast-paths are only valid for HEAD reads (as_of == None).
        // AS OF queries require a full scan because eq/search indexes reflect
        // the current HEAD state, not the historical state at the given seq.
        // (Mirrors the Python reference engine: `if candidates is None and as_of is None:`)
        let candidates: HashSet<String> = if let Some(text) = &plan.search {
            // Search index is also HEAD-only; fall through to scan for AS OF
            if as_of.is_none() {
                self.idx.search_all(&plan.from, text)
            } else {
                self.store.keys(&prefix).into_iter().cloned().collect()
            }
        } else if as_of.is_none() {
            if let Some(c) = plan.where_.iter().find(|c| c.op == nql::Op::Eq) {
                // Try eq index — only for HEAD reads
                if self.idx.has_eq(&plan.from, &c.field) {
                    // Numbers: store the integer form if the float is whole (e.g. 5.0 → "5")
                    // so it matches how serde_json serializes integer JSON numbers.
                    let val_s = match &c.value {
                        nql::Val::Str(s)  => s.clone(),
                        nql::Val::Num(n)  => {
                            if n.fract() == 0.0 && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                                (*n as i64).to_string()
                            } else {
                                n.to_string()
                            }
                        }
                        nql::Val::Bool(b) => b.to_string(),
                        nql::Val::Null    => "null".into(),
                    };
                    self.idx.eq_lookup(&plan.from, &c.field, &val_s)
                        .unwrap_or_default()
                } else {
                    self.store.keys(&prefix).into_iter().cloned().collect()
                }
            } else {
                self.store.keys(&prefix).into_iter().cloned().collect()
            }
        } else {
            // AS OF: always full scan (index reflects HEAD, not history)
            self.store.keys(&prefix).into_iter().cloned().collect()
        };

        // ── Load + filter ─────────────────────────────────────────────────────
        let mut rows: Vec<(String, Value)> = candidates
            .into_iter()
            .filter_map(|key| {
                let doc = self.store.get(&key, as_of)?;
                // Apply all WHERE predicates
                for c in &plan.where_ {
                    let dv = doc.get(&c.field).unwrap_or(&Value::Null);
                    if !cmp(dv, &c.op, &c.value) {
                        return None;
                    }
                }
                // Search double-check (for scan path)
                if let Some(text) = &plan.search {
                    let blob: String = doc.as_object()
                        .map(|o| o.values()
                            .filter_map(|v| v.as_str())
                            .collect::<Vec<_>>()
                            .join(" "))
                        .unwrap_or_default()
                        .to_lowercase();
                    let terms: Vec<&str> = text.split_whitespace().collect();
                    if !terms.iter().all(|t| blob.contains(t)) {
                        return None;
                    }
                }
                Some((key, doc.clone()))
            })
            .collect();

        // ── Sort ──────────────────────────────────────────────────────────────
        //
        // Sorted-index fast-path:
        //   ORDER BY field [ASC|DESC] LIMIT n   +  a Sorted index on `field`
        //   +  no WHERE / SEARCH / TRAVERSE / AS OF that narrowed the candidate set
        //   ⇒ pull the top-n keys straight out of the BTreeMap and skip the
        //     O(n log n) sort over the full collection.
        //
        // The fast-path is only safe when nothing else has filtered `rows`; if
        // WHERE narrowed the set we must sort the filtered rows the slow way.
        let _used_sorted_fast_path = if let Some((field, desc)) = &plan.order_by {
            let can_fast = as_of.is_none()
                && plan.search.is_none()
                && plan.traverse.is_none()
                && plan.where_.is_empty()
                && self.idx.has_sorted(&plan.from, field);
            if can_fast {
                let want = plan.limit.unwrap_or(usize::MAX);
                let keys = if plan.limit.is_some() {
                    self.idx.sorted_top_k(&plan.from, field, *desc, want)
                } else {
                    self.idx.sorted_all(&plan.from, field, *desc)
                };
                let mut out: Vec<(String, Value)> = Vec::with_capacity(keys.len());
                for k in keys {
                    if let Some(doc) = self.store.get(&k, None) {
                        out.push((k, doc.clone()));
                    }
                }
                rows = out;
                true
            } else {
                rows.sort_by(|(_, a), (_, b)| {
                    let av = a.get(field).unwrap_or(&Value::Null);
                    let bv = b.get(field).unwrap_or(&Value::Null);
                    let ord = index::OrderedValue(av.clone())
                        .cmp(&index::OrderedValue(bv.clone()));
                    if *desc { ord.reverse() } else { ord }
                });
                false
            }
        } else {
            false
        };

        // ── Traverse ──────────────────────────────────────────────────────────
        if let Some(rel) = &plan.traverse {
            let mut out = Vec::new();
            let mut seen = HashSet::new();
            for (key, _) in &rows {
                for nb in self.rel.neighbors(key, rel, as_of) {
                    if seen.insert(nb.clone()) {
                        if let Some(doc) = self.store.get(&nb, as_of) {
                            out.push((nb, doc.clone()));
                        }
                    }
                }
            }
            rows = out;
        }

        // ── Limit ─────────────────────────────────────────────────────────────
        if let Some(n) = plan.limit {
            rows.truncate(n);
        }

        let mut result: Vec<Value> = rows.into_iter().map(|(_, d)| d).collect();

        // ── GROUP BY ──────────────────────────────────────────────────────────
        if let Some(gb) = &plan.group_by {
            let mut groups: HashMap<String, Vec<Value>> = HashMap::new();
            for doc in result {
                let gkey = doc.get(gb)
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                groups.entry(gkey).or_default().push(doc);
            }
            result = groups.into_iter().map(|(gval, docs)| {
                let count = docs.len();
                let mut entry = serde_json::json!({gb.as_str(): gval.trim_matches('"'), "count": count});
                if let Some((fn_name, field_opt)) = &plan.agg {
                    if fn_name != "count" {
                        if let Some(field) = field_opt {
                            let nums: Vec<f64> = docs.iter()
                                .filter_map(|d| d.get(field)?.as_f64())
                                .collect();
                            let agg_val = match fn_name.as_str() {
                                "sum" => nums.iter().sum::<f64>(),
                                "avg" => if nums.is_empty() { 0.0 } else { nums.iter().sum::<f64>() / nums.len() as f64 },
                                "min" => nums.iter().cloned().fold(f64::INFINITY, f64::min),
                                "max" => nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                                _ => 0.0,
                            };
                            let key = format!("{fn_name}_{field}");
                            entry[key] = serde_json::json!(agg_val);
                        }
                    }
                }
                entry
            }).collect();
        }

        Ok(result)
    }

    pub fn verify(&self) -> bool { self.log.verify() }

    pub fn head(&self) -> String { self.log.head().to_string() }

    pub fn seq(&self) -> u64 {
        self.log.len().saturating_sub(1) as u64
    }

    pub fn flush(&mut self) {
        if let Some(ref mut f) = self.aof { let _ = f.flush(); }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> Db {
        let mut db = Db::new();
        db.create_index("users", "status", IndexKind::Eq);
        db.create_index("users", "age",    IndexKind::Ordered);
        db.create_index("users", "bio",    IndexKind::Search);
        db.put("users","alice", serde_json::json!({"name":"Alice","age":31,"status":"active","bio":"rust db"}), None, None, None).unwrap();
        db.put("users","bob",   serde_json::json!({"name":"Bob",  "age":24,"status":"active","bio":"python data"}), None, None, None).unwrap();
        db.put("users","carol", serde_json::json!({"name":"Carol","age":41,"status":"inactive","bio":"rust systems"}), None, None, None).unwrap();
        db
    }

    #[test]
    fn filter_sort() {
        let db = fresh();
        let rows = db.query(r#"FROM users WHERE status = "active" ORDER BY age DESC"#).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["name"], "Alice");
    }

    #[test]
    fn search() {
        let db = fresh();
        let rows = db.query(r#"FROM users SEARCH "rust""#).unwrap();
        let names: Vec<&str> = rows.iter().map(|r| r["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"Alice") && names.contains(&"Carol"));
    }

    #[test]
    fn time_travel() {
        let mut db = fresh();
        let s = db.seq();
        db.put("users","alice", serde_json::json!({"name":"Alice","age":32,"status":"active","city":"X"}), None, None, None).unwrap();
        assert_eq!(db.get("users","alice",None).unwrap()["city"], "X");
        assert!(db.get("users","alice",Some(s)).unwrap().get("city").is_none());
    }

    #[test]
    fn relations_time_travel() {
        let mut db = fresh();
        db.link("users:alice","follows","users:bob", None, None).unwrap();
        let s = db.seq();
        db.unlink("users:alice","follows","users:bob", None, None).unwrap();
        assert!(db.neighbors("users:alice","follows",None).is_empty());
        assert_eq!(db.neighbors("users:alice","follows",Some(s)), vec!["users:bob"]);
    }

    #[test]
    fn replay_protection() {
        let mut db = Db::new();
        db.put("k","1", serde_json::json!({"v":1}), Some("svc"), Some(10), None).unwrap();
        let err = db.put("k","1", serde_json::json!({"v":2}), Some("svc"), Some(5), None);
        assert!(err.is_err());
    }

    #[test]
    fn verify_detects_tamper() {
        let mut db = fresh();
        assert!(db.verify());
        db.log.ops[0].payload["doc"]["name"] = serde_json::json!("EVIL");
        assert!(!db.verify());
    }

    #[test]
    fn group_by_count() {
        let db = fresh();
        let rows = db.query("FROM users GROUP BY status COUNT").unwrap();
        assert_eq!(rows.len(), 2);
        let active = rows.iter().find(|r| r["status"].as_str() == Some("active")).unwrap();
        assert_eq!(active["count"], 2);
    }

    #[test]
    fn sorted_index_order_by_limit_fast_path() {
        // ORDER BY age ASC LIMIT 1 — should return Bob (youngest) without
        // scanning the full collection. With a sorted index in place, the
        // engine plucks the top entry straight out of the BTreeMap.
        let mut db = Db::new();
        db.create_index("users", "age", IndexKind::Sorted);
        db.put("users","alice", serde_json::json!({"name":"Alice","age":31}), None, None, None).unwrap();
        db.put("users","bob",   serde_json::json!({"name":"Bob",  "age":24}), None, None, None).unwrap();
        db.put("users","carol", serde_json::json!({"name":"Carol","age":41}), None, None, None).unwrap();
        db.put("users","dave",  serde_json::json!({"name":"Dave", "age":27}), None, None, None).unwrap();

        let asc = db.query("FROM users ORDER BY age ASC LIMIT 1").unwrap();
        assert_eq!(asc.len(), 1);
        assert_eq!(asc[0]["name"], "Bob");

        let desc = db.query("FROM users ORDER BY age DESC LIMIT 1").unwrap();
        assert_eq!(desc.len(), 1);
        assert_eq!(desc[0]["name"], "Carol");

        let top3 = db.query("FROM users ORDER BY age ASC LIMIT 3").unwrap();
        let names: Vec<&str> = top3.iter().map(|r| r["name"].as_str().unwrap()).collect();
        assert_eq!(names, vec!["Bob", "Dave", "Alice"]);
    }

    #[test]
    fn sorted_index_backfill_on_create() {
        // create_index after data was inserted must backfill from the store.
        let mut db = Db::new();
        db.put("users","alice", serde_json::json!({"name":"Alice","age":31}), None, None, None).unwrap();
        db.put("users","bob",   serde_json::json!({"name":"Bob",  "age":24}), None, None, None).unwrap();
        db.create_index("users", "age", IndexKind::Sorted);

        let asc = db.query("FROM users ORDER BY age ASC LIMIT 1").unwrap();
        assert_eq!(asc[0]["name"], "Bob");
    }

    #[test]
    fn sorted_index_updates_on_put_and_delete() {
        let mut db = Db::new();
        db.create_index("users", "age", IndexKind::Sorted);
        db.put("users","alice", serde_json::json!({"name":"Alice","age":31}), None, None, None).unwrap();
        db.put("users","bob",   serde_json::json!({"name":"Bob",  "age":24}), None, None, None).unwrap();

        // Update Bob to be older than Alice
        db.put("users","bob", serde_json::json!({"name":"Bob","age":99}), None, None, None).unwrap();
        let oldest = db.query("FROM users ORDER BY age DESC LIMIT 1").unwrap();
        assert_eq!(oldest[0]["name"], "Bob");

        // Delete Bob, Alice is oldest again
        db.delete("users","bob", None, None, None).unwrap();
        let oldest = db.query("FROM users ORDER BY age DESC LIMIT 1").unwrap();
        assert_eq!(oldest[0]["name"], "Alice");
    }

    #[test]
    fn sorted_index_with_where_still_correct() {
        // When WHERE is present, the fast-path is skipped and the engine
        // sorts the filtered rows using OrderedValue ordering — result must
        // still be correct.
        let mut db = Db::new();
        db.create_index("users", "status", IndexKind::Eq);
        db.create_index("users", "age",    IndexKind::Sorted);
        db.put("users","alice", serde_json::json!({"name":"Alice","age":31,"status":"active"}), None, None, None).unwrap();
        db.put("users","bob",   serde_json::json!({"name":"Bob",  "age":24,"status":"active"}), None, None, None).unwrap();
        db.put("users","carol", serde_json::json!({"name":"Carol","age":21,"status":"inactive"}), None, None, None).unwrap();

        let rows = db.query(r#"FROM users WHERE status = "active" ORDER BY age ASC LIMIT 1"#).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], "Bob");
    }

    #[test]
    fn persistence_roundtrip() {
        use std::fs;
        let tmp = "/tmp/nedb_rust_test";
        let _ = fs::remove_dir_all(tmp);
        {
            let mut db = Db::open(tmp).unwrap();
            db.create_index("users","status",IndexKind::Eq);
            db.put("users","alice", serde_json::json!({"name":"Alice","status":"active"}), None, None, None).unwrap();
            db.flush();
        }
        {
            let db = Db::open(tmp).unwrap();
            assert!(db.verify(), "chain broken after reload");
            let doc = db.get("users","alice",None).expect("alice missing after reload");
            assert_eq!(doc["name"], "Alice");
        }
        let _ = fs::remove_dir_all(tmp);
    }
}
