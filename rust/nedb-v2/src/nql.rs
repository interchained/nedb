//! NQL (NEDB Query Language) parser and executor for v2 DAG storage.
//!
//! Grammar:
//!   FROM coll
//!     [AS OF seq]
//!     [VALID AS OF "date"]
//!     [WHERE field op value [AND field op value]*]
//!     [SEARCH "text"]
//!     [ORDER BY field [DESC]]
//!     [LIMIT n]
//!     [GROUP BY field COUNT|SUM|AVG|MIN|MAX]
//!     [TRACE caused_by [REVERSE]]

use std::collections::HashMap;
use anyhow::{bail, Result};
use serde_json::{json, Value};

use crate::db::Db;
use crate::index::OrderedValue;
use crate::store::Node;

// ── Token types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Kw(String),     // uppercase keyword: FROM, WHERE, ORDER, BY, AS, OF, VALID, LIMIT, GROUP, TRACE, REVERSE, AND, DESC, COUNT, SUM, AVG, MIN, MAX, SEARCH
    Ident(String),  // field name or collection name (lowercase/mixed)
    Str(String),    // "quoted string"
    Num(f64),       // numeric literal
    Op(String),     // = != > < >= <=
    Eof,
}

struct Lexer<'a> {
    src:  &'a str,
    pos:  usize,
}

impl<'a> Lexer<'a> {
    fn new(src: &'a str) -> Self { Self { src, pos: 0 } }

    fn peek_char(&self) -> Option<char> { self.src[self.pos..].chars().next() }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek_char() {
            if c.is_whitespace() { self.pos += c.len_utf8(); } else { break; }
        }
    }

    fn next_tok(&mut self) -> Tok {
        self.skip_ws();
        if self.pos >= self.src.len() { return Tok::Eof; }

        let c = self.peek_char().unwrap();

        // Quoted string
        if c == '"' {
            self.pos += 1;
            let start = self.pos;
            while self.pos < self.src.len() && self.peek_char() != Some('"') {
                self.pos += self.peek_char().unwrap().len_utf8();
            }
            let s = self.src[start..self.pos].to_string();
            if self.peek_char() == Some('"') { self.pos += 1; }
            return Tok::Str(s);
        }

        // Two-char operators
        if self.pos + 1 < self.src.len() {
            let two = &self.src[self.pos..self.pos+2];
            if matches!(two, "!=" | ">=" | "<=") {
                self.pos += 2;
                return Tok::Op(two.to_string());
            }
        }

        // One-char operators
        if matches!(c, '=' | '>' | '<') {
            self.pos += 1;
            return Tok::Op(c.to_string());
        }

        // Number
        if c.is_ascii_digit() || (c == '-' && self.src[self.pos+1..].starts_with(|d: char| d.is_ascii_digit())) {
            let start = self.pos;
            if c == '-' { self.pos += 1; }
            while let Some(d) = self.peek_char() {
                if d.is_ascii_digit() || d == '.' { self.pos += 1; } else { break; }
            }
            let n: f64 = self.src[start..self.pos].parse().unwrap_or(0.0);
            return Tok::Num(n);
        }

        // Keyword or identifier
        if c.is_alphabetic() || c == '_' {
            let start = self.pos;
            while let Some(ch) = self.peek_char() {
                if ch.is_alphanumeric() || ch == '_' || ch == '.' || ch == ':' {
                    self.pos += ch.len_utf8();
                } else { break; }
            }
            let word = &self.src[start..self.pos];
            let upper = word.to_uppercase();
            let keywords = ["FROM","AS","OF","VALID","WHERE","AND","ORDER","BY",
                            "DESC","LIMIT","GROUP","COUNT","SUM","AVG","MIN","MAX",
                            "TRACE","TRAVERSE","REVERSE","SEARCH","NOT","NULL","TRUE","FALSE"];
            if keywords.contains(&upper.as_str()) {
                return Tok::Kw(upper);
            }
            return Tok::Ident(word.to_string());
        }

        // Skip unknown char
        self.pos += c.len_utf8();
        self.next_tok()
    }

    fn tokenize(&mut self) -> Vec<Tok> {
        let mut toks = vec![];
        loop {
            let t = self.next_tok();
            if t == Tok::Eof { break; }
            toks.push(t);
        }
        toks
    }
}

// ── AST ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub field: String,
    pub op:    String,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub enum GroupAgg { Count, Sum, Avg, Min, Max }

#[derive(Debug, Clone)]
pub struct Query {
    pub coll:       String,
    pub as_of:      Option<u64>,
    pub valid_as_of: Option<String>,
    pub wheres:     Vec<WhereClause>,
    pub search:     Option<String>,
    pub order_by:   Option<String>,
    pub order_desc: bool,
    pub limit:      Option<usize>,
    pub group_by:   Option<(String, GroupAgg)>,
    pub trace:      Option<String>,     // edge type (usually "caused_by")
    pub trace_rev:  bool,
    pub traverse:   Option<String>,     // named relation for TRAVERSE rel
}

// ── Parser ────────────────────────────────────────────────────────────────────

struct Parser { toks: Vec<Tok>, pos: usize }

impl Parser {
    fn new(toks: Vec<Tok>) -> Self { Self { toks, pos: 0 } }

    fn peek(&self) -> &Tok { self.toks.get(self.pos).unwrap_or(&Tok::Eof) }
    fn advance(&mut self) -> Tok { let t = self.peek().clone(); self.pos += 1; t }

    fn expect_kw(&mut self, kw: &str) -> Result<()> {
        match self.advance() {
            Tok::Kw(k) if k == kw => Ok(()),
            other => bail!("expected keyword {}, got {:?}", kw, other),
        }
    }

    fn parse_value(&mut self) -> Value {
        match self.advance() {
            Tok::Str(s)  => Value::String(s),
            Tok::Num(n)  => json!(n),
            Tok::Kw(k) if k == "NULL"  => Value::Null,
            Tok::Kw(k) if k == "TRUE"  => Value::Bool(true),
            Tok::Kw(k) if k == "FALSE" => Value::Bool(false),
            Tok::Ident(s) => Value::String(s),
            _ => Value::Null,
        }
    }

    fn parse(&mut self) -> Result<Query> {
        self.expect_kw("FROM")?;
        let coll = match self.advance() {
            Tok::Ident(s) | Tok::Kw(s) => s,
            other => bail!("expected collection name, got {:?}", other),
        };

        let mut q = Query {
            coll, as_of: None, valid_as_of: None,
            wheres: vec![], search: None,
            order_by: None, order_desc: false,
            limit: None, group_by: None,
            trace: None, trace_rev: false,
            traverse: None,
        };

        loop {
            match self.peek() {
                Tok::Eof => break,

                Tok::Kw(k) if k == "AS" => {
                    self.advance();
                    self.expect_kw("OF")?;
                    match self.advance() {
                        Tok::Num(n) => q.as_of = Some(n as u64),
                        other => bail!("AS OF expects sequence number, got {:?}", other),
                    }
                }

                Tok::Kw(k) if k == "VALID" => {
                    self.advance();
                    self.expect_kw("AS")?;
                    self.expect_kw("OF")?;
                    match self.advance() {
                        Tok::Str(s) => q.valid_as_of = Some(s),
                        other => bail!("VALID AS OF expects date string, got {:?}", other),
                    }
                }

                Tok::Kw(k) if k == "WHERE" => {
                    self.advance();
                    loop {
                        let field = match self.advance() {
                            Tok::Ident(s) | Tok::Kw(s) => s,
                            other => bail!("WHERE: expected field name, got {:?}", other),
                        };
                        let op = match self.advance() {
                            Tok::Op(s) => s,
                            other => bail!("WHERE: expected operator, got {:?}", other),
                        };
                        let value = self.parse_value();
                        q.wheres.push(WhereClause { field, op, value });
                        if let Tok::Kw(k) = self.peek() {
                            if k == "AND" { self.advance(); } else { break; }
                        } else { break; }
                    }
                }

                Tok::Kw(k) if k == "SEARCH" => {
                    self.advance();
                    match self.advance() {
                        Tok::Str(s) => q.search = Some(s),
                        other => bail!("SEARCH expects quoted string, got {:?}", other),
                    }
                }

                Tok::Kw(k) if k == "ORDER" => {
                    self.advance();
                    self.expect_kw("BY")?;
                    let field = match self.advance() {
                        Tok::Ident(s) | Tok::Kw(s) => s,
                        other => bail!("ORDER BY: expected field, got {:?}", other),
                    };
                    q.order_by = Some(field);
                    if let Tok::Kw(k) = self.peek() {
                        if k == "DESC" { self.advance(); q.order_desc = true; }
                    }
                }

                Tok::Kw(k) if k == "LIMIT" => {
                    self.advance();
                    match self.advance() {
                        Tok::Num(n) => q.limit = Some(n as usize),
                        other => bail!("LIMIT expects number, got {:?}", other),
                    }
                }

                Tok::Kw(k) if k == "GROUP" => {
                    self.advance();
                    self.expect_kw("BY")?;
                    let field = match self.advance() {
                        Tok::Ident(s) | Tok::Kw(s) => s,
                        other => bail!("GROUP BY: expected field, got {:?}", other),
                    };
                    let agg = match self.advance() {
                        Tok::Kw(a) if a == "COUNT" => GroupAgg::Count,
                        Tok::Kw(a) if a == "SUM"   => GroupAgg::Sum,
                        Tok::Kw(a) if a == "AVG"   => GroupAgg::Avg,
                        Tok::Kw(a) if a == "MIN"   => GroupAgg::Min,
                        Tok::Kw(a) if a == "MAX"   => GroupAgg::Max,
                        other => bail!("GROUP BY: expected aggregation, got {:?}", other),
                    };
                    q.group_by = Some((field, agg));
                }

                Tok::Kw(k) if k == "TRACE" => {
                    self.advance();
                    let edge = match self.advance() {
                        Tok::Ident(s) | Tok::Kw(s) => s,
                        other => bail!("TRACE: expected edge type, got {:?}", other),
                    };
                    q.trace = Some(edge);
                    if let Tok::Kw(k) = self.peek() {
                        if k == "REVERSE" { self.advance(); q.trace_rev = true; }
                    }
                }

                Tok::Kw(k) if k == "TRAVERSE" => {
                    self.advance();
                    let rel = match self.advance() {
                        Tok::Ident(s) | Tok::Kw(s) => s,
                        other => bail!("TRAVERSE: expected relation name, got {:?}", other),
                    };
                    q.traverse = Some(rel);
                }

                _ => { self.advance(); } // skip unrecognised
            }
        }

        Ok(q)
    }
}

// ── Executor ──────────────────────────────────────────────────────────────────

fn matches_where(node: &Node, w: &WhereClause) -> bool {
    let field_val = if w.field == "_id" {
        Value::String(node.id.clone())
    } else if w.field == "_coll" {
        Value::String(node.coll.clone())
    } else if w.field == "_hash" {
        Value::String(node.hash.clone())
    } else {
        node.data.get(&w.field).cloned().unwrap_or(Value::Null)
    };

    let a = OrderedValue::from(&field_val);
    let b = OrderedValue::from(&w.value);

    match w.op.as_str() {
        "="  => a == b,
        "!=" => a != b,
        ">"  => a >  b,
        "<"  => a <  b,
        ">=" => a >= b,
        "<=" => a <= b,
        _    => false,
    }
}

fn matches_valid_as_of(node: &Node, date: &str) -> bool {
    // A node is valid at `date` if:
    //   valid_from is None OR valid_from <= date
    //   AND (valid_to is None OR valid_to > date)
    let from_ok = node.valid_from.as_deref().map(|f| f <= date).unwrap_or(true);
    let to_ok   = node.valid_to.as_deref().map(|t| t > date).unwrap_or(true);
    from_ok && to_ok
}

fn node_contains_text(node: &Node, text: &str) -> bool {
    let s = node.data.to_string().to_lowercase();
    s.contains(&text.to_lowercase())
}

fn node_to_json(node: &Node) -> Value {
    let mut obj = if let Value::Object(m) = &node.data {
        m.clone()
    } else {
        serde_json::Map::new()
    };
    obj.insert("_id".to_string(),   Value::String(node.id.clone()));
    obj.insert("_hash".to_string(), Value::String(node.hash.clone()));
    obj.insert("_seq".to_string(),  json!(node.seq));
    obj.insert("_coll".to_string(), Value::String(node.coll.clone()));
    if let Some(ref vf) = node.valid_from {
        obj.insert("_valid_from".to_string(), Value::String(vf.clone()));
    }
    if let Some(ref vt) = node.valid_to {
        obj.insert("_valid_to".to_string(), Value::String(vt.clone()));
    }
    if !node.caused_by.is_empty() {
        obj.insert("_caused_by".to_string(), Value::Array(
            node.caused_by.iter().map(|h| Value::String(h.clone())).collect()
        ));
    }
    Value::Object(obj)
}

/// Execute a NQL query against the DAG database.
pub fn execute(db: &Db, nql: &str) -> Result<Vec<Value>> {
    let mut lexer = Lexer::new(nql);
    let toks = lexer.tokenize();
    let mut parser = Parser::new(toks);
    let q = parser.parse()?;

    // ── Candidate generation ──────────────────────────────────────────────────

    // Fast path: single equality filter on _id with no AS OF.
    // Skip the O(n) collection scan — go straight to the id index (O(1) file read).
    // This turns `FROM coll WHERE _id = "x" LIMIT 1` from a full-table-scan into
    // a single file read, giving orders-of-magnitude speedup for point lookups.
    let id_eq_fast_path: Option<String> = if q.as_of.is_none() && q.trace.is_none() {
        q.wheres.iter().find_map(|w| {
            if w.field == "_id" && w.op == "=" {
                if let Value::String(ref id) = w.value { Some(id.clone()) } else { None }
            } else { None }
        })
    } else { None };

    let candidates: Vec<Node> = if let Some(ref target_id) = id_eq_fast_path {
        // O(1) direct id-index lookup — skip full collection scan entirely
        db.get(&q.coll, target_id).into_iter().collect()
    } else if let Some(seq_target) = q.as_of {
        // AS OF: return each doc's version at or before target seq
        db.id_index.list_ids(&q.coll).into_iter()
            .filter_map(|id| db.get_as_of(&q.coll, &id, seq_target))
            .collect()
    } else if let Some(ref order_field) = q.order_by {
        // ORDER BY with optional sorted index — get candidates in order
        let limit = q.limit.unwrap_or(9_999_999);
        if q.order_desc {
            db.order_by_desc(&q.coll, order_field, limit)
        } else {
            db.order_by_asc(&q.coll, order_field, limit)
        }
    } else if let (Some(n), true) = (q.limit, q.wheres.is_empty()
            && q.search.is_none() && q.trace.is_none()
            && q.traverse.is_none() && q.group_by.is_none()
            && q.valid_as_of.is_none()) {
        // LIMIT-only fast path: no filters, no ordering, no trace.
        // Take only the first N IDs from the id-index and fetch those docs.
        // This makes `FROM coll LIMIT 1` O(N) not O(total) — critical for
        // the Studio "Preparing…" phase which samples every collection.
        db.id_index
            .list_ids(&q.coll)
            .into_iter()
            .take(n)
            .filter_map(|id| db.get(&q.coll, &id))
            .collect()
    } else {
        // Default: all docs in collection
        db.list(&q.coll)
    };

    // ── WHERE filter ──────────────────────────────────────────────────────────

    let mut rows: Vec<Node> = candidates.into_iter()
        .filter(|n| q.wheres.iter().all(|w| matches_where(n, w)))
        .filter(|n| q.valid_as_of.as_deref()
                       .map(|d| matches_valid_as_of(n, d))
                       .unwrap_or(true))
        .filter(|n| q.search.as_deref()
                       .map(|t| node_contains_text(n, t))
                       .unwrap_or(true))
        .collect();

    // ── TRACE ─────────────────────────────────────────────────────────────────

    if let Some(ref _edge_type) = q.trace {
        let limit = q.limit.unwrap_or(1000);
        let mut traced: Vec<Node> = vec![];
        for root in &rows {
            let chain = db.trace(&root.hash, q.trace_rev, limit);
            traced.extend(chain);
        }
        rows = traced;
    }

    // ── TRAVERSE rel — one-hop named-relation lookup ──────────────────────────

    if let Some(ref rel) = q.traverse {
        let mut traversed: Vec<Node> = vec![];
        for root in &rows {
            let frm = format!("{}:{}", root.coll, root.id);
            let neighbors = db.neighbors(&frm, rel);
            traversed.extend(neighbors);
        }
        rows = traversed;
    }

    // ── ORDER BY (post-filter sort if no sorted index was used) ───────────────

    if let Some(ref field) = q.order_by {
        if q.as_of.is_some() || !q.wheres.is_empty() || q.search.is_some() {
            // Re-sort after filtering
            rows.sort_by(|a, b| {
                let av = a.data.get(field).map(OrderedValue::from).unwrap_or(OrderedValue::Null);
                let bv = b.data.get(field).map(OrderedValue::from).unwrap_or(OrderedValue::Null);
                if q.order_desc { bv.cmp(&av) } else { av.cmp(&bv) }
            });
        }
    }

    // ── LIMIT ─────────────────────────────────────────────────────────────────

    if let Some(n) = q.limit {
        rows.truncate(n);
    }

    // ── GROUP BY ─────────────────────────────────────────────────────────────

    if let Some((ref group_field, ref agg)) = q.group_by {
        let mut groups: HashMap<String, Vec<f64>> = HashMap::new();
        for node in &rows {
            let key = node.data.get(group_field)
                .map(|v| v.to_string().trim_matches('"').to_string())
                .unwrap_or_else(|| "null".to_string());
            let val = node.data.get(group_field)
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0);
            groups.entry(key).or_default().push(val);
        }
        let result: Vec<Value> = groups.into_iter().map(|(k, vals)| {
            let agg_val = match agg {
                GroupAgg::Count => vals.len() as f64,
                GroupAgg::Sum   => vals.iter().sum(),
                GroupAgg::Avg   => vals.iter().sum::<f64>() / vals.len() as f64,
                GroupAgg::Min   => vals.iter().cloned().fold(f64::INFINITY, f64::min),
                GroupAgg::Max   => vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            };
            json!({group_field: k, "value": agg_val, "count": vals.len()})
        }).collect();
        return Ok(result);
    }

    // ── Serialize ─────────────────────────────────────────────────────────────

    Ok(rows.into_iter().map(|n| node_to_json(&n)).collect())
}

/// Parse and execute NQL, returning (rows, count).
pub fn query(db: &Db, nql: &str) -> Result<(Vec<Value>, usize)> {
    let rows = execute(db, nql)?;
    let count = rows.len();
    Ok((rows, count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use crate::db::Db;

    // Returns (TempDir, Db) — the TempDir guard MUST be kept alive by the caller
    // (`let (_tmp, db) = setup();`). If it dropped here, its Drop would delete the
    // database directory out from under the live Db, and every objects.read()
    // (loose object files live on disk) would fail → queries return 0 rows.
    fn setup() -> (tempfile::TempDir, Db) {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.create_sorted_index("blocks", "height");
        for h in 1u64..=5 {
            db.put("blocks", &h.to_string(),
                serde_json::json!({"height": h, "hash": format!("000{}", h), "n_tx": h * 2}),
                vec![], None, None).unwrap();
        }
        (dir, db)
    }

    #[test]
    fn from_all() {
        let (_tmp, db) = setup();
        let (rows, count) = query(&db, "FROM blocks").unwrap();
        assert_eq!(count, 5);
        let _ = rows;
    }

    #[test]
    fn where_eq() {
        let (_tmp, db) = setup();
        let (rows, count) = query(&db, r#"FROM blocks WHERE _id = "3""#).unwrap();
        assert_eq!(count, 1);
        assert_eq!(rows[0]["_id"], "3");
    }

    #[test]
    fn order_by_limit() {
        let (_tmp, db) = setup();
        let (rows, count) = query(&db, "FROM blocks ORDER BY height ASC LIMIT 3").unwrap();
        assert_eq!(count, 3);
        assert_eq!(rows[0]["height"], 1);
        assert_eq!(rows[2]["height"], 3);
    }

    #[test]
    fn order_by_desc() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks ORDER BY height DESC LIMIT 2").unwrap();
        assert_eq!(rows[0]["height"], 5);
    }

    #[test]
    fn where_gt() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks WHERE height > 3").unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn group_by_count() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks GROUP BY n_tx COUNT").unwrap();
        assert_eq!(rows.len(), 5); // all unique n_tx values
    }

    #[test]
    fn search() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, r#"FROM blocks SEARCH "0003""#).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn as_of() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        let v1 = db.put("docs", "x", serde_json::json!({"v": 1}), vec![], None, None).unwrap();
        db.put("docs", "x", serde_json::json!({"v": 2}), vec![], None, None).unwrap();
        let (rows, _) = query(&db, &format!("FROM docs AS OF {}", v1.seq)).unwrap();
        assert_eq!(rows[0]["v"], 1);
    }

    #[test]
    fn valid_as_of() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.put("events", "e1", serde_json::json!({"type": "a"}), vec![],
               Some("2025-01-01".to_string()), Some("2025-06-01".to_string())).unwrap();
        db.put("events", "e2", serde_json::json!({"type": "b"}), vec![],
               Some("2026-01-01".to_string()), None).unwrap();
        let (rows, _) = query(&db, r#"FROM events VALID AS OF "2025-03-01""#).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["type"], "a");
    }
}

#[cfg(test)]
mod tests_traverse {
    use super::*;
    use tempfile::tempdir;
    use crate::db::Db;

    #[test]
    fn traverse_one_hop() {
        let db = Db::in_memory();
        db.put("driver", "d1", serde_json::json!({"name": "Bob"}),   vec![], None, None).unwrap();
        db.put("driver", "d2", serde_json::json!({"name": "Carol"}), vec![], None, None).unwrap();
        db.put("trip",   "t1", serde_json::json!({"status": "req"}), vec![], None, None).unwrap();
        db.put("trip",   "t2", serde_json::json!({"status": "ok"}),  vec![], None, None).unwrap();

        db.link("driver:d1", "handles", "trip:t1").unwrap();
        db.link("driver:d1", "handles", "trip:t2").unwrap();

        let (rows, count) = query(&db, r#"FROM driver WHERE _id = "d1" TRAVERSE handles"#).unwrap();
        assert_eq!(count, 2);
        let ids: std::collections::HashSet<&str> = rows.iter()
            .filter_map(|r| r["_id"].as_str())
            .collect();
        assert!(ids.contains("t1") && ids.contains("t2"));
    }

    #[test]
    fn traverse_returns_empty_when_no_links() {
        let db = Db::in_memory();
        db.put("driver", "d1", serde_json::json!({"name": "Bob"}), vec![], None, None).unwrap();
        let (rows, count) = query(&db, r#"FROM driver WHERE _id = "d1" TRAVERSE handles"#).unwrap();
        assert_eq!(count, 0);
        assert!(rows.is_empty());
    }

    #[test]
    fn traverse_multi_source() {
        // When WHERE matches multiple rows, TRAVERSE unions all their neighbors
        let db = Db::in_memory();
        db.put("driver", "d1", serde_json::json!({"status": "active"}), vec![], None, None).unwrap();
        db.put("driver", "d2", serde_json::json!({"status": "active"}), vec![], None, None).unwrap();
        db.put("trip",   "t1", serde_json::json!({"n": 1}), vec![], None, None).unwrap();
        db.put("trip",   "t2", serde_json::json!({"n": 2}), vec![], None, None).unwrap();
        db.put("trip",   "t3", serde_json::json!({"n": 3}), vec![], None, None).unwrap();

        db.link("driver:d1", "handles", "trip:t1").unwrap();
        db.link("driver:d1", "handles", "trip:t2").unwrap();
        db.link("driver:d2", "handles", "trip:t3").unwrap();

        let (rows, count) = query(&db, r#"FROM driver WHERE status = "active" TRAVERSE handles"#).unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn traverse_nql_keyword_case_insensitive() {
        // Parser normalises to uppercase — "traverse" and "TRAVERSE" both work
        let db = Db::in_memory();
        db.put("driver", "d1", serde_json::json!({}), vec![], None, None).unwrap();
        db.put("trip",   "t1", serde_json::json!({}), vec![], None, None).unwrap();
        db.link("driver:d1", "handles", "trip:t1").unwrap();
        // uppercase
        let (r1, c1) = query(&db, r#"FROM driver WHERE _id = "d1" TRAVERSE handles"#).unwrap();
        assert_eq!(c1, 1);
        // lowercase (lexer uppercases keywords)
        let (r2, c2) = query(&db, r#"FROM driver WHERE _id = "d1" traverse handles"#).unwrap();
        assert_eq!(c2, 1);
        assert_eq!(r1[0]["_id"], r2[0]["_id"]);
    }

    #[test]
    fn traverse_durable() {
        let dir = tempdir().unwrap();
        {
            let db = Db::open(dir.path(), None).unwrap();
            db.put("driver", "d1", serde_json::json!({"name": "Bob"}),   vec![], None, None).unwrap();
            db.put("trip",   "t1", serde_json::json!({"status": "req"}), vec![], None, None).unwrap();
            db.link("driver:d1", "handles", "trip:t1").unwrap();
        }
        let db2 = Db::open(dir.path(), None).unwrap();
        db2.startup_ready.store(true, std::sync::atomic::Ordering::SeqCst);
        let (rows, count) = query(&db2, r#"FROM driver WHERE _id = "d1" TRAVERSE handles"#).unwrap();
        assert_eq!(count, 1);
        assert_eq!(rows[0]["_id"], "t1");
    }
}
