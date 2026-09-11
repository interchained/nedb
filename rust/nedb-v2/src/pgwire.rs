// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

//! A PostgreSQL wire-protocol endpoint for NEDB — reads **and** writes.
//!
//! # What this is
//!
//! A front door that speaks the PostgreSQL v3 wire protocol well enough that
//! tools built for Postgres — `psql`, DBeaver, Metabase, Grafana, psycopg, any
//! libpq client — can use a NEDB store with ordinary SQL. A documented subset
//! of SQL is translated to NQL and to engine writes; everything else is
//! refused with an error naming exactly what was not understood.
//!
//! It is **not** a claim of Postgres parity. It is a claim that the SQL people
//! actually type works, and that the boundary is stated rather than discovered.
//!
//! # Why writes belong here
//!
//! The first cut of this module was read-only, on the reasoning that a NEDB
//! write carries `caused_by`, valid-time bounds and idempotency, and none of
//! that has a natural SQL spelling. That reasoning was wrong, and looking at
//! the mapping is what made it obvious:
//!
//! | SQL | NEDB | and therefore |
//! |---|---|---|
//! | `INSERT` | a put | — |
//! | `UPDATE … WHERE` | a NEW VERSION of each match | the prior value stays readable |
//! | `DELETE … WHERE` | a tombstone | the deleted row stays in history |
//!
//! NEDB is append-only, so an `UPDATE` is *already* a versioned write and a
//! `DELETE` is *already* a tombstone. Nothing is bent to fit. The consequence
//! is the point of the whole endpoint:
//!
//! ```sql
//! UPDATE orders SET total = 999 WHERE _id = 'o1';
//! SELECT total FROM orders WHERE _id = 'o1';                  -- 999
//! SELECT total FROM orders AS OF SYSTEM TIME 0 WHERE _id = 'o1';  -- 120
//! ```
//!
//! Run the SQL you would run against Postgres, and the tamper-evident history
//! is free. No triggers, no audit table, no application code.
//!
//! Provenance is reachable too: `_caused_by`, `_valid_from` and `_valid_to` are
//! reserved INSERT columns, lifted out of the payload into the write itself.
//!
//! Writes are ON by default — that is the parity position. Set
//! `NEDBD_PG_READ_ONLY=1` for the deployment where this door must never mutate
//! anything.
//!
//! # Supported SQL
//!
//! ```sql
//! SELECT * | col [, col]* | COUNT(*) | <agg>(col)
//!   FROM <collection>
//!   [ AS OF SYSTEM TIME <seq> ]     -- bridges to NQL's AS OF
//!   [ WHERE <predicate> ]           -- the full NQL predicate surface
//!   [ GROUP BY <col> ] [ HAVING <predicate> ]
//!   [ ORDER BY <col> [ASC|DESC] (, ...) ] [ LIMIT <n> ] [ OFFSET <n> ]
//!
//! INSERT INTO <collection> (c1, c2) VALUES (v1, v2), (…) [RETURNING …]
//! UPDATE <collection> SET c = v [, …] [WHERE <predicate>] [RETURNING …]
//! DELETE FROM <collection> [WHERE <predicate>] [RETURNING …]
//! ```
//!
//! Single-quoted SQL literals are rewritten to NQL's double-quoted form and
//! `<>` to `!=`. Column projection is applied here, after NQL returns whole
//! documents, because NQL is FROM-first and has no projection clause.
//!
//! Not supported, each refused by name: JOIN, subqueries, CTEs, window
//! functions, DDL, `TRUNCATE`, `GRANT`/`REVOKE`. `INSERT` requires an explicit
//! column list, because NEDB is schemaless and there is no declared column
//! order to infer.
//!
//! # Protocol coverage
//!
//! **Both** protocols are implemented:
//!
//! * the **simple query protocol** (`Q`) — what `psql` and libpq's `PQexec`
//!   use, and therefore psycopg2, which interpolates parameters client-side;
//! * the **extended query protocol** (`Parse`/`Bind`/`Describe`/`Execute`/
//!   `Close`/`Sync`/`Flush`) — what psycopg3, asyncpg and the JDBC driver use
//!   for every parameterised statement. Without it those three could not run a
//!   single query, so "psql works" was a long way from "your framework works".
//!
//! Parameters arrive in text *and* binary format, prepared statements and
//! portals are per-connection, and a row-capped `Execute` suspends its portal
//! (`PortalSuspended`) so a JDBC `setFetchSize` pages instead of stalling.
//!
//! ## Parameter typing in a store with no schema
//!
//! The extended protocol needs types for `$1..$n`, which a relational server
//! reads out of its catalogue. NEDB has none — so the types are sampled from
//! the documents already stored, and the stored data *is* the schema. Where a
//! placeholder sits in a clause rather than beside a column
//! (`AS OF SYSTEM TIME $1`, `LIMIT $1`) the grammar supplies the type instead,
//! and an aggregate column is typed from what the aggregate means: a `COUNT` is
//! an integer, an `AVG` fractional.
//!
//! This is not polish. A client that declares its own parameter types
//! (psycopg3, JDBC) is believed and only its unspecified slots are inferred —
//! but asyncpg declares none, asks, and then **refuses the call client-side**
//! if the answer is wrong. Advertising "text" for everything does not degrade
//! gracefully there; it fails with `expected str, got int` before a query is
//! ever sent.
//!
//! SSL is declined (`N`), so connections are cleartext — hence the loopback
//! default.
//!
//! Authentication mirrors the HTTP surface: with `NEDBD_TOKEN` set the password
//! must equal it; otherwise any connection is accepted.
//!
//! Still outside the boundary, and refused by name: SQL-level cursors
//! (`DECLARE`/`FETCH`), `pg_catalog` introspection (so `\dt` and DBeaver's
//! schema browser stay empty), and binary *result* format for a column whose
//! stored values disagree about their type across documents.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::db::Db;

// ── Postgres type OIDs we hand out ──────────────────────────────────────────
const OID_BOOL: i32 = 16;
const OID_INT8: i32 = 20;
const OID_FLOAT8: i32 = 701;
const OID_TEXT: i32 = 25;

const PROTO_V3: i32 = 196_608; // 3.0 << 16
const SSL_REQUEST: i32 = 80_877_103;
const GSS_REQUEST: i32 = 80_877_104;
const CANCEL_REQUEST: i32 = 80_877_102;

/// How a caller resolves a database name to an open `Db`.
///
/// A trait object rather than a concrete handle so this module does not depend
/// on `server::Manager` — which keeps the protocol code unit-testable against a
/// plain `Db` with no HTTP stack in the way.
pub trait DbResolver: Send + Sync + 'static {
    /// Look up an open database by the name the client connected with.
    ///
    /// MAY BLOCK. The implementation is allowed to take a lock, so this is
    /// always called from `spawn_blocking` — never on an async worker. Taking
    /// a tokio `RwLock::blocking_read()` on a runtime thread panics outright
    /// ("Cannot block the current thread from within a runtime"), which is
    /// exactly how the first cut of this failed.
    fn resolve(&self, name: &str) -> Option<Arc<Db>>;
    /// The bearer token, when one is configured. `None` = open access.
    fn token(&self) -> Option<String> {
        None
    }
}

// ── wire encoding helpers ───────────────────────────────────────────────────

struct Out(Vec<u8>);

impl Out {
    fn msg(tag: u8) -> Self {
        // Tag, then a 4-byte length placeholder patched in `finish`.
        Out(vec![tag, 0, 0, 0, 0])
    }
    fn i16(&mut self, v: i16) { self.0.extend_from_slice(&v.to_be_bytes()); }
    fn i32(&mut self, v: i32) { self.0.extend_from_slice(&v.to_be_bytes()); }
    fn cstr(&mut self, s: &str) {
        // A NUL inside an identifier would truncate the field and desynchronise
        // the stream, so strip rather than trust.
        self.0.extend_from_slice(s.replace('\0', "").as_bytes());
        self.0.push(0);
    }
    fn bytes(&mut self, b: &[u8]) { self.0.extend_from_slice(b); }
    /// Patch the length prefix (which covers the length field itself, not the tag).
    fn finish(mut self) -> Vec<u8> {
        let len = (self.0.len() - 1) as i32;
        self.0[1..5].copy_from_slice(&len.to_be_bytes());
        self.0
    }
}

fn err_msg(code: &str, message: &str) -> Vec<u8> {
    let mut m = Out::msg(b'E');
    m.bytes(b"S"); m.cstr("ERROR");
    m.bytes(b"C"); m.cstr(code);
    m.bytes(b"M"); m.cstr(message);
    m.0.push(0);
    m.finish()
}

fn ready() -> Vec<u8> {
    let mut m = Out::msg(b'Z');
    m.bytes(b"I"); // idle, not in a transaction
    m.finish()
}

fn command_complete(tag: &str) -> Vec<u8> {
    let mut m = Out::msg(b'C');
    m.cstr(tag);
    m.finish()
}

// ── SQL → NQL translation ───────────────────────────────────────────────────

/// One output column: the key to read from the row, and the name to show.
///
/// The two differ for aggregates. NQL answers `SUM(total)` with a row holding
/// `sum_total` (plus `count` and a legacy `value` alias), while SQL callers
/// expect a single column called `sum`. Carrying both halves keeps NEDB's
/// internal key names off the wire — the first cut leaked `['count','value']`
/// out of a `SELECT COUNT(*)`, which is two columns where SQL promises one.
#[derive(Debug, PartialEq, Clone)]
pub struct Col {
    pub src: String,
    pub out: String,
}

impl Col {
    fn same(name: &str) -> Self {
        Col { src: name.to_string(), out: name.to_string() }
    }
    fn renamed(src: &str, out: &str) -> Self {
        Col { src: src.to_string(), out: out.to_string() }
    }
}

/// What a translated statement asks for.
///
/// The write variants exist because SQL's write semantics and NEDB's storage
/// model line up almost exactly, which was not obvious until it was written
/// down:
///
/// | SQL | NEDB |
/// |---|---|
/// | `INSERT` | a put |
/// | `UPDATE … WHERE` | a NEW VERSION of each matching document |
/// | `DELETE … WHERE` | a tombstone |
///
/// NEDB is append-only, so an `UPDATE` is *already* a versioned write and a
/// `DELETE` is *already* a tombstone. Nothing is being bent to fit. The
/// consequence is the thing worth selling: run the SQL you would run against
/// Postgres, and the tamper-evident history falls out for free — the prior
/// value is still readable with `AS OF SYSTEM TIME`.
#[derive(Debug, PartialEq)]
pub enum Stmt {
    /// Run this NQL, then project these columns (empty = all).
    Query { nql: String, project: Vec<Col> },
    /// `INSERT INTO coll (cols) VALUES (…), (…) [RETURNING …]`
    Insert { coll: String, rows: Vec<InsertRow>, returning: Vec<Col> },
    /// `UPDATE coll SET … [WHERE …] [RETURNING …]` — a new version per match.
    Update { coll: String, set: Vec<(String, Value)>, nql: String, returning: Vec<Col> },
    /// `DELETE FROM coll [WHERE …] [RETURNING …]` — a tombstone per match.
    Delete { coll: String, nql: String, returning: Vec<Col> },
    /// Answer from a fixed table — the handshake queries clients send on connect.
    Canned { cols: Vec<String>, row: Vec<String> },
    /// Nothing to do (empty statement, or a SET the client does not need honoured).
    Ok(&'static str),
}

/// One row of an `INSERT`: an explicit id when the statement supplied one, the
/// document body, and optional provenance lifted out of reserved columns.
#[derive(Debug, PartialEq, Clone)]
pub struct InsertRow {
    /// From an `_id` or `id` column. `None` means the server assigns one.
    pub id: Option<String>,
    pub doc: serde_json::Map<String, Value>,
    /// From a `_caused_by` column — the causal parents, so provenance is
    /// reachable from SQL rather than only from the HTTP API.
    pub caused_by: Vec<String>,
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
}

/// Strip SQL comments and collapse whitespace, so the matchers below can be
/// simple without being fragile about formatting.
fn normalise(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_s = false;
    while let Some(c) = chars.next() {
        if in_s {
            out.push(c);
            if c == '\'' { in_s = false; }
            continue;
        }
        match c {
            '\'' => { in_s = true; out.push(c); }
            '-' if chars.peek() == Some(&'-') => {
                // line comment
                for n in chars.by_ref() { if n == '\n' { break; } }
                out.push(' ');
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut prev = ' ';
                while let Some(n) = chars.next() {
                    if prev == '*' && n == '/' { break; }
                    prev = n;
                }
                out.push(' ');
            }
            _ => out.push(c),
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Rewrite SQL literal/operator spellings into NQL's.
///
/// Only `'…'` → `"…"` and `<>` → `!=`. Done with an explicit scan rather than a
/// regex so a quote inside a string cannot be mistaken for a delimiter: SQL
/// escapes an embedded quote by doubling it (`'it''s'`), and that has to become
/// a single character inside the NQL string rather than terminating it.
fn sql_literals_to_nql(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut it = s.chars().peekable();
    while let Some(c) = it.next() {
        match c {
            '\'' => {
                out.push('"');
                while let Some(ch) = it.next() {
                    if ch == '\'' {
                        if it.peek() == Some(&'\'') {
                            it.next();
                            out.push('\''); // doubled '' is one literal quote
                        } else {
                            break;
                        }
                    } else if ch == '"' {
                        // A double quote inside a SQL literal must be escaped
                        // for NQL, whose lexer collapses \" to a literal quote.
                        out.push('\\');
                        out.push('"');
                    } else {
                        out.push(ch);
                    }
                }
                out.push('"');
            }
            '<' if it.peek() == Some(&'>') => { it.next(); out.push_str("!="); }
            _ => out.push(c),
        }
    }
    out
}

fn strip_prefix_ci(s: &str, prefix: &str) -> Option<String> {
    if s.len() >= prefix.len() && s[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(s[prefix.len()..].trim_start().to_string())
    } else {
        None
    }
}

/// Find a top-level keyword (not inside quotes or parentheses), returning its
/// byte offset. Case-insensitive, and only matches on word boundaries.
fn find_kw(s: &str, kw: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let k = kw.as_bytes();
    let mut depth = 0i32;
    let mut in_s = false;
    let mut in_d = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if in_s { if c == b'\'' { in_s = false; } i += 1; continue; }
        if in_d { if c == b'"' { in_d = false; } i += 1; continue; }
        match c {
            b'\'' => { in_s = true; i += 1; continue; }
            b'"' => { in_d = true; i += 1; continue; }
            b'(' => { depth += 1; i += 1; continue; }
            b')' => { depth -= 1; i += 1; continue; }
            _ => {}
        }
        if depth == 0 && i + k.len() <= bytes.len()
            && bytes[i..i + k.len()].eq_ignore_ascii_case(k)
        {
            let before_ok = i == 0 || !(bytes[i - 1] as char).is_alphanumeric() && bytes[i - 1] != b'_';
            let after = i + k.len();
            let after_ok = after >= bytes.len()
                || !(bytes[after] as char).is_alphanumeric() && bytes[after] != b'_';
            if before_ok && after_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Split a comma-separated list at the TOP level, ignoring commas inside
/// quotes or parentheses — so `VALUES (1, 'a,b'), (2, 'c')` splits into two
/// groups and not four.
fn split_top(s: &str, sep: char) -> Vec<String> {
    let mut out = vec![];
    let mut cur = String::new();
    let mut depth = 0i32;
    let mut in_s = false;
    let mut it = s.chars().peekable();
    while let Some(c) = it.next() {
        if in_s {
            cur.push(c);
            if c == '\'' {
                // A doubled '' is an escaped quote, not the end of the literal.
                if it.peek() == Some(&'\'') { cur.push(it.next().unwrap()); } else { in_s = false; }
            }
            continue;
        }
        match c {
            '\'' => { in_s = true; cur.push(c); }
            '(' => { depth += 1; cur.push(c); }
            ')' => { depth -= 1; cur.push(c); }
            x if x == sep && depth == 0 => { out.push(cur.trim().to_string()); cur.clear(); }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() { out.push(cur.trim().to_string()); }
    out
}

/// Parse one SQL scalar literal into JSON.
///
/// Deliberately narrow: a string, a number, a boolean, or NULL. Anything else
/// — a function call, an expression, a cast — is refused by name rather than
/// coerced into a string that would silently store the wrong value.
fn sql_value(raw: &str) -> Result<Value, String> {
    let t = raw.trim();
    if t.is_empty() {
        return Err("empty value".into());
    }
    let up = t.to_uppercase();
    if up == "NULL" { return Ok(Value::Null); }
    if up == "TRUE" { return Ok(Value::Bool(true)); }
    if up == "FALSE" { return Ok(Value::Bool(false)); }
    if t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2 {
        // Unwrap, collapsing the SQL '' escape to one quote.
        let inner = &t[1..t.len() - 1];
        return Ok(Value::String(inner.replace("''", "'")));
    }
    if let Ok(i) = t.parse::<i64>() { return Ok(Value::from(i)); }
    if let Ok(f) = t.parse::<f64>() { return Ok(Value::from(f)); }
    Err(format!(
        "cannot use {:?} as a value — this endpoint accepts string literals, \
         numbers, TRUE/FALSE and NULL. Expressions, casts and function calls \
         are not evaluated, because storing an unevaluated expression as text \
         would be worse than refusing it", t))
}

/// Pull a trailing `RETURNING …` off a statement, returning (head, columns).
fn split_returning(tail: &str) -> (String, Vec<Col>) {
    let tu = tail.to_uppercase();
    match find_kw(&tu, "RETURNING") {
        None => (tail.to_string(), vec![]),
        Some(at) => {
            let head = tail[..at].trim().to_string();
            let list = tail[at + "RETURNING".len()..].trim();
            if list == "*" {
                return (head, vec![]);   // empty projection = every column
            }
            let cols = split_top(list, ',')
                .into_iter()
                .map(|p| {
                    let raw = p.split_whitespace().next().unwrap_or(&p).to_string();
                    let name = raw.rsplit('.').next().unwrap_or(&raw).trim_matches('"').to_string();
                    Col::same(&name)
                })
                .collect();
            (head, cols)
        }
    }
}

/// Columns whose names are reserved: they carry provenance rather than data.
fn take_reserved(doc: &mut serde_json::Map<String, Value>) -> (Option<String>, Vec<String>, Option<String>, Option<String>) {
    let id = doc.remove("_id").or_else(|| doc.remove("id"))
        .and_then(|v| match v {
            Value::String(s) => Some(s),
            Value::Null => None,
            other => Some(other.to_string()),   // a numeric key is a fine id
        });
    let caused_by = match doc.remove("_caused_by") {
        Some(Value::String(s)) => vec![s],
        Some(Value::Array(a)) => a.into_iter()
            .filter_map(|v| v.as_str().map(str::to_string)).collect(),
        _ => vec![],
    };
    let vf = doc.remove("_valid_from").and_then(|v| v.as_str().map(str::to_string));
    let vt = doc.remove("_valid_to").and_then(|v| v.as_str().map(str::to_string));
    (id, caused_by, vf, vt)
}

/// `INSERT INTO coll (c1, c2) VALUES (v1, v2), (…) [RETURNING …]`
fn translate_insert(sql: &str) -> Result<Stmt, String> {
    let rest = strip_prefix_ci(sql, "INSERT")
        .and_then(|r| strip_prefix_ci(&r, "INTO"))
        .ok_or("expected INSERT INTO")?;
    // Locate VALUES first. Everything before it is `coll (col, …)`; searching
    // for `(` without that bound finds the VALUES parenthesis instead and
    // swallows the keyword into the collection name.
    let ru = rest.to_uppercase();
    let values_at = find_kw(&ru, "VALUES").ok_or(
        "expected VALUES — `INSERT … SELECT` is not supported on this endpoint")?;
    let head = rest[..values_at].trim().to_string();
    let open = head.find('(').ok_or(
        "INSERT needs an explicit column list — `INSERT INTO t (a, b) VALUES (…)`. \
         NEDB is schemaless, so there is no declared column order to infer from")?;
    let coll = head[..open].trim().trim_matches('"');
    let coll = coll.rsplit('.').next().unwrap_or(coll).to_string();
    if coll.is_empty() {
        return Err("expected a collection name after INSERT INTO".into());
    }
    let close = head.rfind(')').ok_or("unterminated column list")?;
    if close < open {
        return Err("malformed column list".into());
    }
    let tail_from_values = rest[values_at..].to_string();
    let cols: Vec<String> = split_top(&head[open + 1..close], ',')
        .into_iter()
        .map(|c| c.trim().trim_matches('"').to_string())
        .collect();
    if cols.is_empty() {
        return Err("the column list is empty".into());
    }

    let after = strip_prefix_ci(&tail_from_values, "VALUES")
        .ok_or("expected VALUES after the column list")?;
    let (values_part, returning) = split_returning(&after);

    let mut rows = vec![];
    for group in split_top(&values_part, ',') {
        let g = group.trim();
        if !(g.starts_with('(') && g.ends_with(')')) {
            return Err(format!("expected a parenthesised row of values, got {:?}", g));
        }
        let vals = split_top(&g[1..g.len() - 1], ',');
        if vals.len() != cols.len() {
            return Err(format!(
                "{} values for {} columns — every row must match the column list",
                vals.len(), cols.len()));
        }
        let mut doc = serde_json::Map::new();
        for (c, v) in cols.iter().zip(vals.iter()) {
            doc.insert(c.clone(), sql_value(v)?);
        }
        let (id, caused_by, valid_from, valid_to) = take_reserved(&mut doc);
        rows.push(InsertRow { id, doc, caused_by, valid_from, valid_to });
    }
    if rows.is_empty() {
        return Err("INSERT with no rows".into());
    }
    Ok(Stmt::Insert { coll, rows, returning })
}

/// `UPDATE coll SET a = 1, b = 'x' [WHERE …] [RETURNING …]`
fn translate_update(sql: &str) -> Result<Stmt, String> {
    let rest = strip_prefix_ci(sql, "UPDATE").ok_or("expected UPDATE")?;
    let ru = rest.to_uppercase();
    let set_at = find_kw(&ru, "SET").ok_or("expected SET in UPDATE")?;
    let coll = rest[..set_at].trim().trim_matches('"');
    let coll = coll.rsplit('.').next().unwrap_or(coll).to_string();
    if coll.is_empty() {
        return Err("expected a collection name after UPDATE".into());
    }
    let after_set = rest[set_at + 3..].trim().to_string();
    let (after_set, returning) = split_returning(&after_set);

    // WHERE ends the assignment list; everything after it is a NQL predicate.
    let au = after_set.to_uppercase();
    let (assigns_raw, where_raw) = match find_kw(&au, "WHERE") {
        Some(at) => (after_set[..at].to_string(), after_set[at..].to_string()),
        None => (after_set.clone(), String::new()),
    };

    let mut set = vec![];
    for a in split_top(&assigns_raw, ',') {
        let eq = a.find('=').ok_or(format!("expected `col = value` in SET, got {:?}", a))?;
        let col = a[..eq].trim().trim_matches('"').to_string();
        if col.is_empty() {
            return Err("empty column name in SET".into());
        }
        set.push((col, sql_value(&a[eq + 1..])?));
    }
    if set.is_empty() {
        return Err("UPDATE with no assignments".into());
    }
    // The matching rows are found with an ordinary NQL read, so the whole
    // predicate surface (IN, BETWEEN, LIKE, OR, …) works in an UPDATE too.
    let nql = format!("FROM {} {}", coll, sql_literals_to_nql(where_raw.trim()))
        .trim().to_string();
    Ok(Stmt::Update { coll, set, nql, returning })
}

/// `DELETE FROM coll [WHERE …] [RETURNING …]`
fn translate_delete(sql: &str) -> Result<Stmt, String> {
    let rest = strip_prefix_ci(sql, "DELETE")
        .and_then(|r| strip_prefix_ci(&r, "FROM"))
        .ok_or("expected DELETE FROM")?;
    let (rest, returning) = split_returning(&rest);
    let end = rest.find(' ').unwrap_or(rest.len());
    let coll = rest[..end].trim().trim_matches('"');
    let coll = coll.rsplit('.').next().unwrap_or(coll).to_string();
    if coll.is_empty() {
        return Err("expected a collection name after DELETE FROM".into());
    }
    let where_raw = rest[end..].trim();
    let nql = format!("FROM {} {}", coll, sql_literals_to_nql(where_raw))
        .trim().to_string();
    Ok(Stmt::Delete { coll, nql, returning })
}

/// Translate one SQL statement into something executable, or explain why not.
pub fn translate(sql_raw: &str) -> Result<Stmt, String> {
    let sql = normalise(sql_raw);
    let sql = sql.trim().trim_end_matches(';').trim();
    if sql.is_empty() {
        return Ok(Stmt::Ok(""));
    }
    let upper = sql.to_uppercase();

    // ── the handshake. Clients issue these before anything useful; answering
    // them with plausible values is the difference between "connects" and
    // "hangs on startup". They are canned on purpose — NEDB has no pg_catalog
    // and pretending otherwise would be worse than a clear boundary.
    if upper.starts_with("SET ") || upper.starts_with("BEGIN") || upper.starts_with("COMMIT")
        || upper.starts_with("ROLLBACK") || upper.starts_with("DISCARD")
        || upper.starts_with("LISTEN ") || upper.starts_with("UNLISTEN ")
    {
        // Accepted and ignored: there is one implicit read-only transaction.
        return Ok(Stmt::Ok(if upper.starts_with("SET") { "SET" } else { "OK" }));
    }
    if upper.starts_with("SHOW ") {
        let name = sql[5..].trim().to_lowercase();
        let val = match name.as_str() {
            "transaction_isolation" | "default_transaction_isolation" => "read committed",
            "server_version" => SERVER_VERSION,
            "server_encoding" | "client_encoding" => "UTF8",
            "standard_conforming_strings" => "on",
            "is_superuser" => "off",
            _ => "",
        };
        return Ok(Stmt::Canned { cols: vec![name], row: vec![val.to_string()] });
    }
    if upper == "SELECT VERSION()" {
        return Ok(Stmt::Canned {
            cols: vec!["version".into()],
            row: vec![full_version_string()],
        });
    }
    if upper == "SELECT 1" || upper == "SELECT 1;" {
        return Ok(Stmt::Canned { cols: vec!["?column?".into()], row: vec!["1".into()] });
    }
    if upper.starts_with("SELECT CURRENT_SCHEMA") {
        return Ok(Stmt::Canned { cols: vec!["current_schema".into()], row: vec!["public".into()] });
    }
    if upper.starts_with("SELECT CURRENT_DATABASE") {
        return Ok(Stmt::Canned { cols: vec!["current_database".into()], row: vec!["nedb".into()] });
    }
    if upper.starts_with("SELECT CURRENT_USER") || upper.starts_with("SELECT USER") {
        return Ok(Stmt::Canned { cols: vec!["current_user".into()], row: vec!["nedb".into()] });
    }

    // ── writes ───────────────────────────────────────────────────────────────
    // SQL's write semantics and NEDB's append-only model line up, so these are
    // first-class rather than refused. See the `Stmt` doc comment.
    if upper.starts_with("INSERT") { return translate_insert(sql); }
    if upper.starts_with("UPDATE") { return translate_update(sql); }
    if upper.starts_with("DELETE") { return translate_delete(sql); }

    // ── the refusals that remain, each naming the boundary ──────────────────
    for (kw, why) in [
        ("CREATE", "DDL is not supported — collections are created implicitly by the first write to them, because NEDB is schemaless"),
        ("ALTER", "DDL is not supported — there is no schema to alter"),
        ("DROP", "DDL is not supported; drop a database with DELETE /v1/databases/<db>"),
        ("TRUNCATE", "not supported, and not an oversight: NEDB is append-only so that history cannot be discarded. That is the product"),
        ("COPY", "not supported; use GET /v1/databases/<db>/since for bulk export"),
        ("GRANT", "there is no SQL-level privilege system; auth is the bearer token"),
        ("REVOKE", "there is no SQL-level privilege system; auth is the bearer token"),
    ] {
        if upper.starts_with(kw) {
            return Err(format!("{} is not supported — {}", kw, why));
        }
    }
    if !upper.starts_with("SELECT") {
        return Err(format!(
            "only SELECT, INSERT, UPDATE and DELETE are supported on the Postgres \
             endpoint (got {:?})",
            sql.split_whitespace().next().unwrap_or("")
        ));
    }
    for (kw, why) in [
        (" JOIN ", "JOIN is not supported — NQL is single-collection; join in your client or model the relation with LINK/TRAVERSE"),
        (" UNION ", "UNION is not supported"),
        (" INTERSECT ", "INTERSECT is not supported"),
        (" EXCEPT ", "EXCEPT is not supported"),
        (" OVER (", "window functions are not supported"),
        ("DISTINCT ", "DISTINCT is not supported — GROUP BY <col> gives the distinct values with counts"),
    ] {
        if upper.contains(kw) {
            return Err(why.to_string());
        }
    }
    if find_kw(&upper, "FROM").is_none() {
        return Err("SELECT without FROM is not supported on this endpoint".into());
    }

    // ── SELECT <projection> FROM <rest> ──────────────────────────────────────
    let after_select = strip_prefix_ci(sql, "SELECT").ok_or("expected SELECT")?;
    let from_at = find_kw(&after_select.to_uppercase(), "FROM")
        .ok_or("expected FROM after the select list")?;
    let projection = after_select[..from_at].trim().to_string();
    let rest = after_select[from_at + 4..].trim().to_string();
    if rest.is_empty() {
        return Err("expected a collection name after FROM".into());
    }
    // A subquery in the FROM position, or a comma-separated table list (an
    // implicit cross join), are both out of scope — say which.
    if rest.starts_with('(') {
        return Err("subqueries in FROM are not supported".into());
    }
    let coll_end = rest.find(' ').unwrap_or(rest.len());
    let coll = &rest[..coll_end];
    if coll.contains(',') {
        return Err("selecting from more than one collection is not supported (no JOIN)".into());
    }
    // Postgres clients often qualify as schema.table; NEDB has one namespace.
    let coll = coll.rsplit('.').next().unwrap_or(coll).trim_matches('"');
    let tail = rest[coll_end..].trim();

    // ── the select list ──────────────────────────────────────────────────────
    let pu = projection.to_uppercase();
    let mut agg_clause = String::new();
    let mut project: Vec<Col> = vec![];

    if projection == "*" {
        // everything
    } else if pu.starts_with("COUNT(") {
        // COUNT(*) and COUNT(col) both become NQL's bare COUNT: NQL counts the
        // group, and a per-column non-null count is not expressible here.
        agg_clause = " COUNT".to_string();
        project.push(Col::same("count"));
    } else if let Some(agg) = ["SUM", "AVG", "MIN", "MAX"]
        .iter()
        .find(|a| pu.starts_with(&format!("{}(", a)))
    {
        let inner = projection[agg.len() + 1..]
            .trim_end_matches(')')
            .trim()
            .to_string();
        if inner.is_empty() || inner == "*" {
            return Err(format!("{}() needs a column", agg));
        }
        agg_clause = format!(" {} {}", agg, inner);
        // NQL emits `<agg>_<field>`; SQL names the column after the function.
        project.push(Col::renamed(
            &format!("{}_{}", agg.to_lowercase(), inner),
            &agg.to_lowercase(),
        ));
    } else {
        for part in projection.split(',') {
            let p = part.trim();
            if p.is_empty() {
                return Err("empty column in the select list".into());
            }
            if p.contains('(') {
                return Err(format!(
                    "expressions in the select list are not supported ({:?}) — \
                     supported: *, a column list, COUNT(*), or SUM/AVG/MIN/MAX(col)", p));
            }
            // strip an alias: `col AS x` / `col x`
            let raw = p.split_whitespace().next().unwrap_or(p);
            let name = raw.rsplit('.').next().unwrap_or(raw).trim_matches('"');
            project.push(Col::same(name));
        }
    }

    // ── clause tail: AS OF SYSTEM TIME → AS OF, then pass the rest through ──
    //
    // The clause keywords NQL shares with SQL (WHERE, GROUP BY, HAVING,
    // ORDER BY, LIMIT, OFFSET) are deliberately handed to the NQL parser
    // unchanged rather than re-parsed here. NQL is the authority on what is
    // valid; re-implementing its grammar would give two parsers to disagree.
    let mut tail = tail.to_string();
    let tu = tail.to_uppercase();
    if let Some(at) = find_kw(&tu, "AS OF SYSTEM TIME") {
        let before = tail[..at].to_string();
        let after = tail[at + "AS OF SYSTEM TIME".len()..].trim_start().to_string();
        // Take the sequence token; the rest of the tail follows it.
        let end = after.find(' ').unwrap_or(after.len());
        let seq = after[..end].trim().trim_matches('\'').trim_matches('"').to_string();
        if seq.parse::<u64>().is_err() {
            return Err(format!(
                "AS OF SYSTEM TIME takes a NEDB sequence number here, not a timestamp (got {:?}). \
                 NEDB's history is sequence-addressed and never garbage-collected, so a seq is \
                 exact where a wall-clock time would be approximate", seq));
        }
        tail = format!("{} AS OF {} {}", before.trim(), seq, after[end..].trim())
            .trim()
            .to_string();
    }

    // ── GROUP BY: refuse a bare column that SQL would refuse ─────────────────
    //
    // A grouped NQL row holds only the group key, `count` and the aggregate —
    // so projecting `total` from `GROUP BY region` found nothing and rendered
    // NULL. Silently answering NULL for a column the query cannot produce is
    // the exact failure shape this engine keeps getting bitten by, so it is an
    // error, using Postgres's own wording so the message is already familiar.
    let tu_all = tail.to_uppercase();
    if let Some(gb_at) = find_kw(&tu_all, "GROUP BY") {
        let after = tail[gb_at + "GROUP BY".len()..].trim_start();
        let key_end = after.find(|c: char| c == ' ' || c == ',').unwrap_or(after.len());
        let group_key = after[..key_end].trim().trim_matches('"').to_string();
        let is_agg = !agg_clause.is_empty();
        for c in &project {
            let ok = c.src == group_key
                || c.src == "count"
                || (is_agg && c.out == agg_clause.trim().split(' ').next()
                        .unwrap_or("").to_lowercase());
            if !ok {
                return Err(format!(
                    "column {:?} must appear in the GROUP BY clause or be used in an \
                     aggregate function — a grouped row carries the group key, `count`, \
                     and the aggregate, nothing else",
                    c.src));
            }
        }
    }

    let tail = sql_literals_to_nql(&tail);
    let nql = format!("FROM {}{}{}", coll,
                      if agg_clause.is_empty() { String::new() } else { agg_clause },
                      if tail.is_empty() { String::new() } else { format!(" {}", tail) });

    Ok(Stmt::Query { nql: nql.trim().to_string(), project })
}

const SERVER_VERSION: &str = "15.0";

fn full_version_string() -> String {
    format!(
        "PostgreSQL {} (NEDB {}) — tamper-evident, append-only, permanent \
         history. SELECT + INSERT/UPDATE/DELETE; an UPDATE is a new version, \
         so prior values stay readable with AS OF SYSTEM TIME.",
        SERVER_VERSION,
        env!("CARGO_PKG_VERSION")
    )
}

// ── result shaping ──────────────────────────────────────────────────────────

/// Pick the column order for a result set.
///
/// With an explicit projection, that order. Otherwise the union of keys across
/// the returned rows — `_`-prefixed provenance columns last, so `psql` shows
/// the user's own fields first and `_hash` does not push `status` off screen.
fn columns_for(rows: &[Value], project: &[Col]) -> Vec<Col> {
    if !project.is_empty() {
        return project.to_vec();
    }
    let mut plain: Vec<String> = vec![];
    let mut meta: Vec<String> = vec![];
    for r in rows {
        if let Value::Object(m) = r {
            for k in m.keys() {
                let target = if k.starts_with('_') { &mut meta } else { &mut plain };
                if !target.contains(k) {
                    target.push(k.clone());
                }
            }
        }
    }
    plain.sort();
    meta.sort();
    plain.extend(meta);
    plain.into_iter().map(|k| Col::same(&k)).collect()
}

/// The Postgres type of one JSON value.
fn oid_of_value(v: &Value) -> Option<i32> {
    match v {
        Value::Null => None,
        Value::Bool(_) => Some(OID_BOOL),
        Value::Number(n) => Some(if n.is_i64() || n.is_u64() { OID_INT8 } else { OID_FLOAT8 }),
        Value::String(_) => Some(OID_TEXT),
        // Arrays and objects render as their JSON text.
        _ => Some(OID_TEXT),
    }
}

/// Reconcile two observed types for the same column.
///
/// A relational column has one type by construction. A NEDB collection does
/// not: document 1 may hold `qty: 3` and document 2 `qty: "three"`. Widening
/// to `text` on a conflict is the only answer that can carry both, and mixed
/// integers and floats widen to float8 for the same reason.
fn unify_oid(a: i32, b: i32) -> i32 {
    if a == b {
        return a;
    }
    match (a, b) {
        (OID_INT8, OID_FLOAT8) | (OID_FLOAT8, OID_INT8) => OID_FLOAT8,
        _ => OID_TEXT,
    }
}

/// The type of `col` across EVERY row in the result, not just the first.
///
/// Taking the first non-null value's type was a latent wrong answer: a column
/// holding `3` in row one and `"n/a"` in row two was advertised as `int8`, and
/// a client that believes the description then fails parsing `"n/a"` as an
/// integer — or, on the binary path, cannot be sent the value at all.
fn oid_for(rows: &[Value], col: &str) -> i32 {
    let mut acc: Option<i32> = None;
    for r in rows {
        if let Some(o) = r.get(col).and_then(oid_of_value) {
            acc = Some(match acc {
                None => o,
                Some(prev) => unify_oid(prev, o),
            });
            if acc == Some(OID_TEXT) {
                break; // text absorbs everything; no need to look further
            }
        }
    }
    acc.unwrap_or(OID_TEXT)
}

/// Render one cell in the text format Postgres clients expect for format 0.
fn cell(v: Option<&Value>) -> Option<String> {
    match v {
        None | Some(Value::Null) => None, // NULL on the wire
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Bool(b)) => Some(if *b { "t".into() } else { "f".into() }),
        Some(other) => Some(other.to_string()),
    }
}

/// Render one cell in binary format for the type the column was advertised as.
///
/// Needed because asyncpg asks for binary results — it is not an optimisation
/// there, it is the only format it requests, so without this it cannot read a
/// single row. Text-format clients never reach this path.
///
/// A value that does not fit the advertised type is an error rather than a
/// coercion. The advertised type comes from sampling stored documents, so a
/// mismatch means the field is genuinely heterogeneous beyond the sample, and
/// quietly sending a zero (or the text bytes under a binary header) would
/// corrupt the value in a way the client cannot detect.
fn cell_binary(v: Option<&Value>, oid: i32) -> Result<Option<Vec<u8>>, String> {
    let v = match v {
        None | Some(Value::Null) => return Ok(None),
        Some(v) => v,
    };
    let as_f64 = |n: &serde_json::Number| n.as_f64()
        .ok_or_else(|| "a number too large to send as float8".to_string());
    Ok(Some(match (oid, v) {
        (OID_BOOL, Value::Bool(b)) => vec![u8::from(*b)],
        (OID_INT2, Value::Number(n)) => {
            let i = n.as_i64().ok_or("not an integer")?;
            i16::try_from(i).map_err(|_| format!("{} does not fit in int2", i))?
                .to_be_bytes().to_vec()
        }
        (OID_INT4, Value::Number(n)) => {
            let i = n.as_i64().ok_or("not an integer")?;
            i32::try_from(i).map_err(|_| format!("{} does not fit in int4", i))?
                .to_be_bytes().to_vec()
        }
        (OID_INT8, Value::Number(n)) => {
            n.as_i64().ok_or("not an integer")?.to_be_bytes().to_vec()
        }
        (OID_FLOAT4, Value::Number(n)) => (as_f64(n)? as f32).to_be_bytes().to_vec(),
        (OID_FLOAT8, Value::Number(n)) => as_f64(n)?.to_be_bytes().to_vec(),
        // For the text family, binary and text are the same bytes.
        (OID_TEXT | OID_VARCHAR | OID_NAME | OID_UNKNOWN | OID_JSON, _) => {
            cell(Some(v)).unwrap_or_default().into_bytes()
        }
        // jsonb is a one-byte version header then the JSON text.
        (OID_JSONB, _) => {
            let mut b = vec![1u8];
            b.extend_from_slice(cell(Some(v)).unwrap_or_default().as_bytes());
            b
        }
        (oid, val) => {
            let kind = match val {
                Value::Bool(_) => "a boolean",
                Value::Number(_) => "a number",
                Value::String(_) => "a string",
                Value::Array(_) => "an array",
                _ => "an object",
            };
            return Err(format!(
                "cannot send {} in binary format as type OID {} — the field holds \
                 more than one type across documents, so it cannot be described \
                 by a single Postgres type. Select it with a text cast, or use a \
                 text-format client",
                kind, oid
            ));
        }
    }))
}

/// A `RowDescription`, with a per-column wire format code.
fn row_description_fmt(cols: &[Col], oids: &[i32], fmts: &[i16]) -> Vec<u8> {
    let mut m = Out::msg(b'T');
    m.i16(cols.len() as i16);
    for (i, c) in cols.iter().enumerate() {
        m.cstr(&c.out);
        m.i32(0); // table OID — unknown
        m.i16((i + 1) as i16); // column attribute number
        m.i32(oids.get(i).copied().unwrap_or(OID_TEXT));
        m.i16(-1); // variable length
        m.i32(-1); // no type modifier
        m.i16(fmts.get(i).copied().unwrap_or(0));
    }
    m.finish()
}

fn row_description(cols: &[Col], oids: &[i32]) -> Vec<u8> {
    row_description_fmt(cols, oids, &[])
}

fn data_row_bytes(vals: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut m = Out::msg(b'D');
    m.i16(vals.len() as i16);
    for v in vals {
        match v {
            None => m.i32(-1),
            Some(b) => {
                m.i32(b.len() as i32);
                m.bytes(b);
            }
        }
    }
    m.finish()
}

fn data_row(vals: &[Option<String>]) -> Vec<u8> {
    let owned: Vec<Option<Vec<u8>>> =
        vals.iter().map(|v| v.as_ref().map(|s| s.as_bytes().to_vec())).collect();
    data_row_bytes(&owned)
}

/// Encode just the rows: `T` followed by one `D` per row, and NO
/// `CommandComplete`.
///
/// Split out because a write with `RETURNING` must emit `T`/`D`* and then its
/// OWN tag (`INSERT 0 3`, `UPDATE 1`). The first cut called `encode_result`
/// there, which appends `CommandComplete("SELECT n")` — so one statement sent
/// TWO CommandComplete messages. That is a protocol violation, and the visible
/// symptom was `RETURNING` silently yielding no rows at all: the client took
/// the first tag as the end of the statement and discarded the description.
pub fn encode_rows(rows: &[Value], project: &[Col]) -> Vec<u8> {
    let cols = columns_for(rows, project);
    let oids: Vec<i32> = cols.iter().map(|c| oid_for(rows, &c.src)).collect();
    let mut out = row_description(&cols, &oids);
    for r in rows {
        let vals: Vec<Option<String>> = cols.iter().map(|c| cell(r.get(&c.src))).collect();
        out.extend_from_slice(&data_row(&vals));
    }
    out
}

/// A complete SELECT response: rows plus `CommandComplete("SELECT n")`.
pub fn encode_result(rows: &[Value], project: &[Col]) -> Vec<u8> {
    let mut out = encode_rows(rows, project);
    out.extend_from_slice(&command_complete(&format!("SELECT {}", rows.len())));
    out
}

// ── the extended query protocol: Parse / Bind / Describe / Execute ──────────
//
// Why this exists at all: psycopg3, asyncpg and the JDBC driver do not speak
// the simple query protocol for parameterised statements. Without these six
// messages they cannot run a single query — psycopg3 hangs waiting for a
// `ParseComplete`, and asyncpg refuses before it ever sends a `Bind`. "psql
// works" is not the same as "the drivers your evaluators use work".
//
// Two facts about real drivers shaped everything below, and both were read off
// a wire transcript rather than assumed:
//
//   1. psycopg3 sends parameters in a MIXED format — a `str` as OID 0 in text
//      format, but an `int` as int2/int4/int8 in BINARY, a float as float8
//      binary, a bool as a single binary byte. A text-only decoder gets `\x00*`
//      where it expected `42`.
//
//   2. asyncpg declares NO parameter types in `Parse` and then asks
//      `Describe(statement)`, encoding its arguments from whatever OIDs come
//      back. Answering "text" for all of them does not degrade gracefully — it
//      makes asyncpg REFUSE the call client-side ("expected str, got int").
//
// (2) is the reason `infer_param_oids` exists. NEDB is schemaless, so there is
// no catalogue to read a column's type out of — the only honest source of truth
// is the data already stored, so the type is sampled from it.

/// Parameter/result type OIDs handled on the binary path.
const OID_INT2: i32 = 21;
const OID_INT4: i32 = 23;
const OID_OID: i32 = 26;
const OID_FLOAT4: i32 = 700;
const OID_VARCHAR: i32 = 1043;
const OID_NAME: i32 = 19;
const OID_UNKNOWN: i32 = 705;
const OID_JSON: i32 = 114;
const OID_JSONB: i32 = 3802;

/// How many `$n` placeholders a statement carries, and the highest index used.
///
/// Scans outside string literals so a `'$1'` inside a value is not mistaken for
/// a placeholder. Dollar-quoted bodies (`$tag$…$tag$`) are not recognised —
/// they need a procedural language NEDB does not have.
fn param_count(sql: &str) -> usize {
    let b = sql.as_bytes();
    let mut i = 0usize;
    let mut in_s = false;
    let mut max = 0usize;
    while i < b.len() {
        let c = b[i];
        if in_s {
            if c == b'\'' {
                in_s = false;
            }
            i += 1;
            continue;
        }
        if c == b'\'' {
            in_s = true;
            i += 1;
            continue;
        }
        if c == b'$' && i + 1 < b.len() && b[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            let mut n = 0usize;
            while j < b.len() && b[j].is_ascii_digit() {
                n = n * 10 + (b[j] - b'0') as usize;
                j += 1;
            }
            max = max.max(n);
            i = j;
            continue;
        }
        i += 1;
    }
    max
}

/// The JSON-shaped type of `field` as it is actually stored, sampled from the
/// collection, mapped onto the nearest Postgres OID.
///
/// This is the schemaless answer to "what type is this column?". A relational
/// server reads its catalogue; NEDB has none, so it reads the data. Sampling a
/// bounded number of rows keeps a `Describe` cheap, and the first row that
/// actually carries the field decides — a field missing from row one but
/// present in row nine still types correctly.
fn infer_field_oid(db: Option<&Arc<Db>>, coll: &str, field: &str) -> i32 {
    // `_`-prefixed names are engine metadata, not stored document fields, so
    // they type from the engine's own contract — no sampling, and no database
    // handle needed.
    match field {
        "_seq" => return OID_INT8,
        "_id" | "_hash" | "_prev" | "_collection" | "_valid_from" | "_valid_to" => return OID_TEXT,
        _ => {}
    }
    let db = match db {
        Some(db) => db,
        None => return OID_TEXT,
    };
    if coll.is_empty() || field.is_empty() {
        return OID_TEXT;
    }
    let rows = match crate::nql::query(db, &format!("FROM {} LIMIT {}", coll, TYPE_SAMPLE)) {
        Ok((rows, _)) => rows,
        Err(_) => return OID_TEXT,
    };
    // Unified over the sample, not taken from the first hit: a field that is a
    // number in one document and a string in another has to be advertised as
    // text or a client cannot decode every row of it.
    oid_for(&rows, field)
}

/// The type of an aggregate output column, which no document holds.
///
/// Sampling stored documents cannot type these: `COUNT(*)` produces a column
/// called `count` that exists in no document, so the sampler finds nothing and
/// falls back to text. A text-format client papers over that, but a binary
/// client is then handed the digits of a number under a text header and
/// `COUNT(*)` comes back as the string `"2"` instead of the integer `2`.
///
/// So aggregates are typed from what the aggregate MEANS: a count is always an
/// integer, an average is always fractional, and min/max/sum inherit the type
/// of the field they were computed over.
fn aggregate_oid(src: &str, db: Option<&Arc<Db>>, coll: &str) -> Option<i32> {
    if src == "count" {
        return Some(OID_INT8);
    }
    for (prefix, fixed) in [
        ("count_", Some(OID_INT8)),
        ("avg_", Some(OID_FLOAT8)),
        ("sum_", None),
        ("min_", None),
        ("max_", None),
    ] {
        if let Some(field) = src.strip_prefix(prefix) {
            return Some(match fixed {
                Some(oid) => oid,
                // SUM/MIN/MAX of an integer field is an integer; of a
                // fractional field, fractional.
                None => match infer_field_oid(db, coll, field) {
                    OID_INT8 => OID_INT8,
                    OID_FLOAT8 => OID_FLOAT8,
                    // Summing or ordering a non-numeric field is not
                    // meaningful; let the row-derived type answer.
                    other => other,
                },
            });
        }
    }
    None
}

/// How many documents to sample when typing a column.
///
/// Bounded so a `Describe` stays cheap. It is a sample, so a field that only
/// turns heterogeneous outside it can still surprise us — which is exactly why
/// `cell_binary` refuses a mismatch loudly instead of coercing.
const TYPE_SAMPLE: usize = 200;

/// The collection a statement reads from or writes to, for type sampling.
fn stmt_collection(sql: &str) -> String {
    let s = normalise(sql);
    let up = s.to_uppercase();
    let after = if let Some(at) = find_kw(&up, "FROM") {
        &s[at + 4..]
    } else if let Some(rest) = strip_prefix_ci(&s, "UPDATE") {
        return rest
            .split_whitespace()
            .next()
            .unwrap_or("")
            .rsplit('.')
            .next()
            .unwrap_or("")
            .trim_matches('"')
            .to_string();
    } else if let Some(rest) = strip_prefix_ci(&s, "INSERT INTO") {
        return rest
            .split(|c: char| c.is_whitespace() || c == '(')
            .find(|t| !t.is_empty())
            .unwrap_or("")
            .rsplit('.')
            .next()
            .unwrap_or("")
            .trim_matches('"')
            .to_string();
    } else {
        return String::new();
    };
    after
        .trim()
        .split(|c: char| c.is_whitespace())
        .find(|t| !t.is_empty())
        .unwrap_or("")
        .rsplit('.')
        .next()
        .unwrap_or("")
        .trim_matches('"')
        .to_string()
}

/// Which document field each `$n` is being compared against.
///
/// Three shapes cover essentially all driver-generated SQL:
///   `WHERE qty > $1`        → the identifier immediately left of the operator
///   `SET status = $1`       → same shape, inside the SET list
///   `INSERT INTO t (a,b) VALUES ($1,$2)` → positional against the column list
///
/// Anything it cannot read returns `None`, which types as `text`. Guessing
/// wrong here would make a driver encode a value the engine then fails to
/// match, so an unknown is left unknown on purpose.
fn param_fields(sql: &str, n_params: usize) -> Vec<Option<String>> {
    let s = normalise(sql);
    let mut out = vec![None; n_params];

    // The INSERT column list maps positionally, which is more reliable than
    // scanning leftwards through a VALUES tuple.
    let up = s.to_uppercase();
    if up.starts_with("INSERT") {
        if let (Some(open), Some(vals_at)) = (s.find('('), find_kw(&up, "VALUES")) {
            if open < vals_at {
                if let Some(close) = s[open..vals_at].rfind(')') {
                    let cols: Vec<String> = split_top(&s[open + 1..open + close], ',')
                        .into_iter()
                        .map(|c| c.trim().trim_matches('"').to_string())
                        .collect();
                    // `$1` is the first placeholder in the first tuple, and so on.
                    let tail = &s[vals_at..];
                    let mut seen = 0usize;
                    let b = tail.as_bytes();
                    let mut i = 0usize;
                    let mut in_s = false;
                    while i < b.len() {
                        if in_s {
                            if b[i] == b'\'' { in_s = false; }
                            i += 1;
                            continue;
                        }
                        if b[i] == b'\'' { in_s = true; i += 1; continue; }
                        if b[i] == b'$' && i + 1 < b.len() && b[i + 1].is_ascii_digit() {
                            let mut j = i + 1;
                            let mut num = 0usize;
                            while j < b.len() && b[j].is_ascii_digit() {
                                num = num * 10 + (b[j] - b'0') as usize;
                                j += 1;
                            }
                            if num >= 1 && num <= n_params {
                                if let Some(c) = cols.get(seen % cols.len().max(1)) {
                                    out[num - 1] = Some(c.clone());
                                }
                            }
                            seen += 1;
                            i = j;
                            continue;
                        }
                        i += 1;
                    }
                    return out;
                }
            }
        }
    }

    // Otherwise: for each `$n`, walk left past the operator to the identifier.
    let b = s.as_bytes();
    let mut i = 0usize;
    let mut in_s = false;
    while i < b.len() {
        if in_s {
            if b[i] == b'\'' { in_s = false; }
            i += 1;
            continue;
        }
        if b[i] == b'\'' { in_s = true; i += 1; continue; }
        if b[i] == b'$' && i + 1 < b.len() && b[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            let mut num = 0usize;
            while j < b.len() && b[j].is_ascii_digit() {
                num = num * 10 + (b[j] - b'0') as usize;
                j += 1;
            }
            if num >= 1 && num <= n_params {
                let left = &s[..i];
                // Skip the operator characters and whitespace sitting between
                // the identifier and the placeholder.
                let trimmed = left.trim_end_matches(|c: char| {
                    c.is_whitespace() || "=<>!+-*/%(,".contains(c)
                });
                // A word operator (`LIKE`, `IN`, `BETWEEN`, `AND`) also sits
                // between them; step over it to reach the real identifier.
                let mut tok = trimmed
                    .rsplit(|c: char| c.is_whitespace() || c == '(' || c == ',')
                    .find(|t| !t.is_empty())
                    .unwrap_or("")
                    .trim_matches('"');
                let mut before = trimmed;
                for _ in 0..4 {
                    let upper_tok = tok.to_uppercase();
                    // `BETWEEN $1 AND $2` puts BOTH a word operator and an
                    // earlier placeholder between `$2` and the column it
                    // constrains, so a placeholder has to be stepped over too —
                    // otherwise the upper bound of every range query types as
                    // text while the lower bound types correctly.
                    if upper_tok.starts_with('$')
                        || matches!(upper_tok.as_str(),
                        "LIKE" | "ILIKE" | "IN" | "BETWEEN" | "AND" | "OR" | "NOT" | "IS") {
                        before = before[..before.len() - tok.len()].trim_end_matches(|c: char| {
                            c.is_whitespace() || "=<>!(,".contains(c)
                        });
                        tok = before
                            .rsplit(|c: char| c.is_whitespace() || c == '(' || c == ',')
                            .find(|t| !t.is_empty())
                            .unwrap_or("")
                            .trim_matches('"');
                    } else {
                        break;
                    }
                }
                if !tok.is_empty()
                    && tok.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.')
                    && !tok.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(true)
                {
                    out[num - 1] = Some(tok.rsplit('.').next().unwrap_or(tok).to_string());
                }
            }
            i = j;
            continue;
        }
        i += 1;
    }
    out
}

/// The type of a placeholder sitting in a CLAUSE position rather than beside a
/// column.
///
/// `AS OF SYSTEM TIME $1` has no column to sample — the token to its left is
/// the word `TIME`. Its type comes from the grammar instead, which is both
/// cheaper and more certain than any inference: a system-time bound is a
/// sequence number, a valid-time bound is a date string, and a page bound is an
/// integer. Without this, a parameterised time-travel query typed as text and
/// asyncpg refused to send the integer at all.
fn clause_param_oids(sql: &str, n_params: usize) -> Vec<Option<i32>> {
    let s = normalise(sql);
    let mut out = vec![None; n_params];
    let b = s.as_bytes();
    let mut i = 0usize;
    let mut in_s = false;
    while i < b.len() {
        if in_s {
            if b[i] == b'\'' { in_s = false; }
            i += 1;
            continue;
        }
        if b[i] == b'\'' { in_s = true; i += 1; continue; }
        if b[i] == b'$' && i + 1 < b.len() && b[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            let mut num = 0usize;
            while j < b.len() && b[j].is_ascii_digit() {
                num = num * 10 + (b[j] - b'0') as usize;
                j += 1;
            }
            if num >= 1 && num <= n_params {
                let left = s[..i].trim_end().to_uppercase();
                // VALID AS OF is checked FIRST: it ends with "AS OF" too, and
                // its argument is a DATE STRING, not a sequence number.
                out[num - 1] = if left.ends_with("VALID AS OF") {
                    Some(OID_TEXT)
                } else if left.ends_with("AS OF SYSTEM TIME")
                    || left.ends_with("FOR SYSTEM_TIME AS OF")
                    || left.ends_with("AS OF")
                    || left.ends_with("LIMIT")
                    || left.ends_with("OFFSET")
                {
                    Some(OID_INT8)
                } else {
                    None
                };
            }
            i = j;
            continue;
        }
        i += 1;
    }
    out
}

/// The OIDs to advertise for `$1..$n`, sampled from stored data.
///
/// `declared` is what the client itself put in `Parse`. A client that states a
/// type is believed — it is about to encode its arguments that way, and second
///-guessing it would break the decode. Only the unspecified slots are inferred.
fn infer_param_oids(sql: &str, declared: &[i32], db: Option<&Arc<Db>>) -> Vec<i32> {
    let n = param_count(sql).max(declared.len());
    if n == 0 {
        return vec![];
    }
    let coll = stmt_collection(sql);
    let fields = param_fields(sql, n);
    let clauses = clause_param_oids(sql, n);
    (0..n)
        .map(|i| match declared.get(i) {
            Some(&oid) if oid != 0 => oid,
            // A clause position knows its own type from the grammar, so it
            // outranks sampling a column that is not even there.
            _ => match clauses[i] {
                Some(oid) => oid,
                None => match &fields[i] {
                    Some(f) => infer_field_oid(db, &coll, f),
                    None => OID_TEXT,
                },
            },
        })
        .collect()
}

/// Decode one bound parameter into the SQL literal text to splice into the
/// statement.
///
/// `None` means SQL NULL. Format 1 is binary — see the module note on psycopg3
/// sending small integers as int2.
fn decode_param(raw: Option<&[u8]>, oid: i32, format: i16) -> Result<Option<String>, String> {
    let bytes = match raw {
        None => return Ok(None),
        Some(b) => b,
    };
    let quote = |s: &str| format!("'{}'", s.replace('\'', "''"));

    if format == 0 {
        let s = String::from_utf8_lossy(bytes).to_string();
        return Ok(Some(match oid {
            OID_BOOL => {
                let t = matches!(s.as_str(), "t" | "true" | "TRUE" | "1" | "yes" | "on");
                if t { "TRUE".into() } else { "FALSE".into() }
            }
            OID_INT2 | OID_INT4 | OID_INT8 | OID_OID | OID_FLOAT4 | OID_FLOAT8 => {
                // Validate rather than trust: an unparseable "number" spliced
                // in bare would become a bare identifier in the NQL text and
                // produce a baffling error far from its cause.
                if s.parse::<f64>().is_ok() { s } else { quote(&s) }
            }
            // OID 0 with text format is psycopg3's `str`. Confirmed on the
            // wire: it declares a real numeric OID whenever the value is a
            // number, so an unspecified text parameter is genuinely a string
            // and quoting it is right rather than a guess.
            _ => quote(&s),
        }));
    }
    if format != 1 {
        return Err(format!("unsupported parameter format code {}", format));
    }

    // ── binary ──────────────────────────────────────────────────────────────
    let need = |n: usize| -> Result<(), String> {
        if bytes.len() == n {
            Ok(())
        } else {
            Err(format!(
                "binary parameter of type OID {} should be {} bytes, got {}",
                oid, n, bytes.len()
            ))
        }
    };
    Ok(Some(match oid {
        OID_BOOL => {
            need(1)?;
            if bytes[0] != 0 { "TRUE".into() } else { "FALSE".into() }
        }
        OID_INT2 => {
            need(2)?;
            i16::from_be_bytes([bytes[0], bytes[1]]).to_string()
        }
        OID_INT4 => {
            need(4)?;
            i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string()
        }
        OID_OID => {
            need(4)?;
            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string()
        }
        OID_INT8 => {
            need(8)?;
            i64::from_be_bytes(bytes[..8].try_into().unwrap()).to_string()
        }
        OID_FLOAT4 => {
            need(4)?;
            let f = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            fmt_float(f as f64)
        }
        OID_FLOAT8 => {
            need(8)?;
            fmt_float(f64::from_be_bytes(bytes[..8].try_into().unwrap()))
        }
        OID_TEXT | OID_VARCHAR | OID_NAME | OID_UNKNOWN | OID_JSON | 0 => {
            quote(&String::from_utf8_lossy(bytes))
        }
        OID_JSONB => {
            // jsonb binary is a 1-byte version header followed by the JSON text.
            let body = if bytes.first() == Some(&1) { &bytes[1..] } else { bytes };
            quote(&String::from_utf8_lossy(body))
        }
        other => {
            return Err(format!(
                "parameter type OID {} is not supported in binary format — \
                 the supported set is bool, int2/int4/int8, float4/float8, \
                 text/varchar/json/jsonb. Send it as text, or cast it in the \
                 statement",
                other
            ))
        }
    }))
}

/// Render a float without Rust's `inf`/`NaN` spellings leaking into SQL text.
fn fmt_float(f: f64) -> String {
    if f.is_nan() {
        "'NaN'".into()
    } else if f.is_infinite() {
        if f > 0.0 { "'Infinity'".into() } else { "'-Infinity'".into() }
    } else if f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{:.0}", f)
    } else {
        f.to_string()
    }
}

/// Splice decoded parameters into the statement text.
///
/// Textual substitution, deliberately: the whole SQL surface is already a text
/// translation into NQL, so one representation is simpler and cannot disagree
/// with itself. Every value arrives already rendered as a SQL literal by
/// `decode_param`, with embedded quotes doubled, so a parameter cannot break
/// out of its literal and alter the statement's shape.
fn substitute_params(sql: &str, params: &[Option<String>]) -> Result<String, String> {
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 16);
    let mut i = 0usize;
    let mut in_s = false;
    while i < b.len() {
        let c = b[i];
        if in_s {
            out.push(c as char);
            if c == b'\'' { in_s = false; }
            i += 1;
            continue;
        }
        if c == b'\'' {
            in_s = true;
            out.push('\'');
            i += 1;
            continue;
        }
        if c == b'$' && i + 1 < b.len() && b[i + 1].is_ascii_digit() {
            let mut j = i + 1;
            let mut n = 0usize;
            while j < b.len() && b[j].is_ascii_digit() {
                n = n * 10 + (b[j] - b'0') as usize;
                j += 1;
            }
            match params.get(n.wrapping_sub(1)) {
                Some(Some(lit)) => out.push_str(lit),
                Some(None) => out.push_str("NULL"),
                None => {
                    return Err(format!(
                        "bind message supplies {} parameter(s) but the statement uses ${}",
                        params.len(), n
                    ))
                }
            }
            i = j;
            continue;
        }
        out.push(c as char);
        i += 1;
    }
    Ok(out)
}

/// A parsed statement, held for the life of the connection (or until `Close`).
struct Prepared {
    sql: String,
    /// OIDs advertised for `$1..$n` — what `ParameterDescription` reports and
    /// what `Bind` values are decoded as.
    param_oids: Vec<i32>,
    /// The advertised output shape, computed on demand and then reused.
    ///
    /// Lazy because working it out samples stored documents, and a text-format
    /// client that never sends `Describe(statement)` should not pay for a scan
    /// on every `Parse` — psycopg3 parses once per query.
    ///
    /// `Some(None)` means "computed, and this statement returns no rows".
    out_shape: Option<Option<(Vec<Col>, Vec<i32>)>>,
}

/// The output columns and types a statement advertises, computed once.
fn prepared_shape<'a>(
    p: &'a mut Prepared,
    db: Option<&Arc<Db>>,
) -> &'a Option<(Vec<Col>, Vec<i32>)> {
    if p.out_shape.is_none() {
        p.out_shape = Some(describe_shape(&p.sql, db, p.param_oids.len()));
    }
    p.out_shape.as_ref().expect("just filled")
}

/// A bound statement: fully substituted SQL plus, once run, its result.
struct Portal {
    sql: String,
    /// Filled by the first `Describe` or `Execute` and reused afterwards.
    ///
    /// Executing once and streaming from the buffer is what makes a suspended
    /// portal safe: a second `Execute` on a partially-drained `INSERT` must
    /// continue the row stream, not perform the insert again.
    result: Option<PortalResult>,
    /// The output shape, frozen at the first `Describe`/`Execute`.
    ///
    /// A schemaless store derives `SELECT *`'s columns from the rows it found,
    /// which would let a `Describe` and a later `Execute` disagree about the
    /// column count — and a driver that was told three fields and handed two
    /// mis-decodes the row rather than failing loudly. Freezing the shape and
    /// projecting every row onto it makes the result set rectangular, as SQL
    /// promises. The simple protocol keeps the dynamic behaviour, where there
    /// is no `Describe` to contradict.
    frozen: Option<Vec<Col>>,
    /// Result-column format codes requested by `Bind`. Empty = all text.
    formats: Vec<i16>,
    /// The shape this portal's statement advertised, carried over from the
    /// prepared statement when any column is to be sent in BINARY.
    ///
    /// It has to be the ADVERTISED shape rather than one derived from the rows
    /// in hand: asyncpg built its decoders from `Describe`, so re-deriving a
    /// different type here would hand it bytes it cannot read.
    declared: Option<(Vec<Col>, Vec<i32>)>,
}

impl Portal {
    /// The format code for column `i`, following the protocol's shorthands:
    /// no codes means all-text, one code applies to every column.
    fn format_of(&self, i: usize) -> i16 {
        match self.formats.len() {
            0 => 0,
            1 => self.formats[0],
            _ => self.formats.get(i).copied().unwrap_or(0),
        }
    }
    /// The columns and types to advertise and encode with.
    fn shape(&self, r: &PortalResult) -> (Vec<Col>, Vec<i32>) {
        match &self.declared {
            Some((cols, oids)) if self.formats.iter().any(|f| *f == 1) => {
                (cols.clone(), oids.clone())
            }
            _ => {
                let cols = columns_for(&r.rows, &r.project);
                let oids = cols.iter().map(|c| oid_for(&r.rows, &c.src)).collect();
                (cols, oids)
            }
        }
    }
}

struct PortalResult {
    rows: Vec<Value>,
    project: Vec<Col>,
    has_rows: bool,
    tag: String,
    tag_counts_rows: bool,
    /// How many rows have gone out across all `Execute`s on this portal.
    sent: usize,
}

fn parse_complete() -> Vec<u8> { Out::msg(b'1').finish() }
fn bind_complete() -> Vec<u8> { Out::msg(b'2').finish() }
fn close_complete() -> Vec<u8> { Out::msg(b'3').finish() }
fn no_data() -> Vec<u8> { Out::msg(b'n').finish() }
fn portal_suspended() -> Vec<u8> { Out::msg(b's').finish() }

fn parameter_description(oids: &[i32]) -> Vec<u8> {
    let mut m = Out::msg(b't');
    m.i16(oids.len() as i16);
    for o in oids {
        m.i32(*o);
    }
    m.finish()
}

/// Split a NUL-terminated string off the front of a message body.
fn take_cstr(body: &[u8], at: &mut usize) -> String {
    let start = *at;
    while *at < body.len() && body[*at] != 0 {
        *at += 1;
    }
    let s = String::from_utf8_lossy(&body[start..*at]).to_string();
    if *at < body.len() {
        *at += 1; // step over the NUL
    }
    s
}

fn take_i16(body: &[u8], at: &mut usize) -> Result<i16, String> {
    if *at + 2 > body.len() {
        return Err("truncated message".into());
    }
    let v = i16::from_be_bytes([body[*at], body[*at + 1]]);
    *at += 2;
    Ok(v)
}

fn take_i32(body: &[u8], at: &mut usize) -> Result<i32, String> {
    if *at + 4 > body.len() {
        return Err("truncated message".into());
    }
    let v = i32::from_be_bytes([body[*at], body[*at + 1], body[*at + 2], body[*at + 3]]);
    *at += 4;
    Ok(v)
}

/// The field names a collection actually holds, sampled from stored documents.
///
/// The answer to `SELECT *` on a store with no schema. Sorted, because
/// `serde_json`'s map is ordered and both this and the row encoder must agree
/// on column order or the values land under the wrong headings.
fn sample_columns(db: Option<&Arc<Db>>, coll: &str) -> Vec<Col> {
    let db = match db {
        Some(db) => db,
        None => return vec![],
    };
    let rows = match crate::nql::query(db, &format!("FROM {} LIMIT 25", coll)) {
        Ok((rows, _)) => rows,
        Err(_) => return vec![],
    };
    let mut names: Vec<String> = vec![];
    for r in &rows {
        if let Value::Object(m) = r {
            for k in m.keys() {
                if !names.iter().any(|n| n == k) {
                    names.push(k.clone());
                }
            }
        }
    }
    names.sort();
    names.iter().map(|n| Col::same(n)).collect()
}

/// The result shape of a statement, worked out WITHOUT running it.
///
/// Needed for `Describe(statement)`, which arrives before any `Bind` — asyncpg
/// builds its row decoders from the answer. Only the select list is read off
/// the result; nothing touches storage except the type sampling.
///
/// Returns `None` when the statement returns no rows at all (`NoData`).
fn describe_shape(
    sql: &str,
    db: Option<&Arc<Db>>,
    n_params: usize,
) -> Option<(Vec<Col>, Vec<i32>)> {
    let probe = probe_sql(sql, n_params);
    let stmt = translate(&probe).ok()?;
    let coll = stmt_collection(sql);

    let cols = match stmt {
        Stmt::Ok(_) => return None,
        Stmt::Canned { cols, .. } => cols.iter().map(|c| Col::same(c)).collect(),
        Stmt::Query { project, .. } => {
            if project.is_empty() { sample_columns(db, &coll) } else { project }
        }
        Stmt::Insert { returning, .. } | Stmt::Update { returning, .. } | Stmt::Delete { returning, .. } => {
            if !wants_returning(sql) {
                return None;
            }
            if returning.is_empty() { sample_columns(db, &coll) } else { returning }
        }
    };
    if cols.is_empty() {
        // Nothing could be determined. `NoData` is a lie for a SELECT, but a
        // RowDescription with zero columns is a worse one — it tells the client
        // the query definitively has no output.
        return None;
    }
    let oids = cols
        .iter()
        .map(|c| {
            aggregate_oid(&c.src, db, &coll)
                .unwrap_or_else(|| infer_field_oid(db, &coll, &c.src))
        })
        .collect();
    Some((cols, oids))
}

/// A parse-only stand-in for a parameterised statement.
///
/// Substituting `NULL` was the obvious choice and the wrong one: a clause that
/// validates its argument rejects it, so `AS OF SYSTEM TIME $1` failed at
/// `Parse` — before the client ever bound a real sequence number. `0` parses
/// everywhere a literal can appear, and since only the SELECT list is read back
/// out, the stub's value never reaches an answer.
fn probe_sql(sql: &str, n_params: usize) -> String {
    let stub: Vec<Option<String>> = vec![Some("0".to_string()); n_params];
    substitute_params(sql, &stub).unwrap_or_else(|_| sql.to_string())
}

/// Run a portal's statement if it has not run yet, then report its shape.
fn ensure_executed(
    portal: &mut Portal,
    db_name: &str,
    db: Option<&Arc<Db>>,
    read_only: bool,
) -> Result<(), Vec<u8>> {
    if portal.result.is_some() {
        return Ok(());
    }
    let ex = execute_stmt(&portal.sql, db_name, db, read_only)?;
    // Freeze the output shape on first sight so `Describe` and every later
    // `Execute` describe the same rectangle.
    let project = if let Some(f) = &portal.frozen {
        f.clone()
    } else {
        let p = if ex.project.is_empty() {
            columns_for(&ex.rows, &[])
        } else {
            ex.project.clone()
        };
        portal.frozen = Some(p.clone());
        p
    };
    portal.result = Some(PortalResult {
        rows: ex.rows,
        project,
        has_rows: ex.has_rows,
        tag: ex.tag,
        tag_counts_rows: ex.tag_counts_rows,
        sent: 0,
    });
    Ok(())
}

// ── connection handling ─────────────────────────────────────────────────────

async fn read_exact(sock: &mut TcpStream, n: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    sock.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn read_i32(sock: &mut TcpStream) -> std::io::Result<i32> {
    let b = read_exact(sock, 4).await?;
    Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
}

fn parse_startup_params(body: &[u8]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let mut parts = body.split(|b| *b == 0).map(|s| String::from_utf8_lossy(s).to_string());
    while let (Some(k), Some(v)) = (parts.next(), parts.next()) {
        if k.is_empty() {
            break;
        }
        out.insert(k, v);
    }
    out
}

/// Serve one client connection to completion.
async fn handle(mut sock: TcpStream, resolver: Arc<dyn DbResolver>, read_only: bool) -> std::io::Result<()> {
    // ── startup, including the SSL negotiation clients try first ────────────
    let params = loop {
        let len = read_i32(&mut sock).await?;
        if len < 8 || len > 1 << 20 {
            return Ok(()); // nonsense framing — drop the connection
        }
        let code = read_i32(&mut sock).await?;
        let body = read_exact(&mut sock, (len - 8) as usize).await?;
        match code {
            SSL_REQUEST | GSS_REQUEST => {
                // Decline and let the client retry in the clear.
                sock.write_all(b"N").await?;
                continue;
            }
            CANCEL_REQUEST => return Ok(()), // nothing cancellable: reads are synchronous
            PROTO_V3 => break parse_startup_params(&body),
            other => {
                let major = other >> 16;
                sock.write_all(&err_msg(
                    "0A000",
                    &format!("unsupported frontend protocol {}.{} — this endpoint speaks 3.0",
                             major, other & 0xffff),
                )).await?;
                return Ok(());
            }
        }
    };

    let db_name = params.get("database").cloned().unwrap_or_default();

    // Resolve the database ONCE, here, on a blocking thread.
    //
    // A Postgres connection is bound to one database for its whole life, so
    // per-connection resolution is both correct and simpler than resolving per
    // statement — and it keeps the lock acquisition off the async worker.
    let resolved: Option<Arc<Db>> = {
        let r = Arc::clone(&resolver);
        let name = db_name.clone();
        tokio::task::spawn_blocking(move || r.resolve(&name))
            .await
            .unwrap_or(None)
    };

    // ── auth: mirror the HTTP surface ───────────────────────────────────────
    if let Some(expected) = resolver.token() {
        // AuthenticationCleartextPassword (3)
        let mut m = Out::msg(b'R');
        m.i32(3);
        sock.write_all(&m.finish()).await?;

        let tag = read_exact(&mut sock, 1).await?;
        if tag[0] != b'p' {
            sock.write_all(&err_msg("28000", "expected a password message")).await?;
            return Ok(());
        }
        let len = read_i32(&mut sock).await?;
        if len < 4 || len > 1 << 16 {
            return Ok(());
        }
        let body = read_exact(&mut sock, (len - 4) as usize).await?;
        let supplied = String::from_utf8_lossy(&body).trim_end_matches('\0').to_string();
        // Constant-time-ish: compare lengths and bytes without early return.
        let ok = supplied.len() == expected.len()
            && supplied.bytes().zip(expected.bytes()).fold(0u8, |a, (x, y)| a | (x ^ y)) == 0;
        if !ok {
            sock.write_all(&err_msg("28P01", "password authentication failed")).await?;
            return Ok(());
        }
    }

    let mut m = Out::msg(b'R');
    m.i32(0); // AuthenticationOk
    sock.write_all(&m.finish()).await?;

    for (k, v) in [
        ("server_version", SERVER_VERSION),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        ("application_name", "nedbd"),
    ] {
        let mut p = Out::msg(b'S');
        p.cstr(k);
        p.cstr(v);
        sock.write_all(&p.finish()).await?;
    }
    let mut k = Out::msg(b'K');
    k.i32(std::process::id() as i32);
    k.i32(0);
    sock.write_all(&k.finish()).await?;
    sock.write_all(&ready()).await?;

    // ── message loop ────────────────────────────────────────────────────────
    //
    // Prepared statements and portals live for the connection. `""` is the
    // unnamed statement/portal, which every driver reuses constantly — it is an
    // ordinary entry in the map rather than a special case.
    let mut prepared: HashMap<String, Prepared> = HashMap::new();
    let mut portals: HashMap<String, Portal> = HashMap::new();
    // After an error inside an extended-protocol sequence, everything up to the
    // next `Sync` is discarded. Skipping this is how a server ends up answering
    // a Bind the client has already abandoned, and the stream desynchronises.
    let mut failed = false;

    loop {
        let mut tag = [0u8; 1];
        if sock.read_exact(&mut tag).await.is_err() {
            return Ok(()); // client hung up
        }
        let len = read_i32(&mut sock).await?;
        if len < 4 || len > 64 << 20 {
            return Ok(());
        }
        let body = read_exact(&mut sock, (len - 4) as usize).await?;

        // `Sync` always clears the error state; `Terminate` always applies.
        if failed && tag[0] != b'S' && tag[0] != b'X' {
            continue;
        }

        match tag[0] {
            b'X' => return Ok(()), // Terminate

            b'Q' => {
                let sql = String::from_utf8_lossy(&body).trim_end_matches('\0').to_string();
                let out = run_simple_query(&sql, &db_name, resolved.as_ref(), read_only);
                sock.write_all(&out).await?;
                sock.write_all(&ready()).await?;
                // A simple query closes the unnamed portal, per the protocol.
                portals.remove("");
            }

            // ── Parse: name, SQL, declared parameter type OIDs ─────────────
            b'P' => {
                let mut at = 0usize;
                let name = take_cstr(&body, &mut at);
                let sql = take_cstr(&body, &mut at);
                let n = take_i16(&body, &mut at).unwrap_or(0).max(0) as usize;
                let mut declared = Vec::with_capacity(n);
                let mut bad = false;
                for _ in 0..n {
                    match take_i32(&body, &mut at) {
                        Ok(o) => declared.push(o),
                        Err(_) => { bad = true; break; }
                    }
                }
                if bad {
                    sock.write_all(&err_msg("08P01", "malformed Parse message")).await?;
                    failed = true;
                    continue;
                }
                // Reject unsupported SQL here rather than at Execute, so the
                // client learns at the point it asked — which is also where
                // Postgres reports it.
                if let Err(why) = translate(&probe_sql(&sql, param_count(&sql))) {
                    sock.write_all(&err_msg("0A000", &why)).await?;
                    failed = true;
                    continue;
                }
                let param_oids = infer_param_oids(&sql, &declared, resolved.as_ref());
                prepared.insert(name, Prepared { sql, param_oids, out_shape: None });
                sock.write_all(&parse_complete()).await?;
            }

            // ── Bind: portal, statement, formats, values, result formats ───
            b'B' => {
                let mut at = 0usize;
                let portal_name = take_cstr(&body, &mut at);
                let stmt_name = take_cstr(&body, &mut at);
                if !prepared.contains_key(&stmt_name) {
                    sock.write_all(&err_msg("26000", &format!(
                        "prepared statement {:?} does not exist", stmt_name))).await?;
                    failed = true;
                    continue;
                }
                let p = &prepared[&stmt_name];
                let mut want_formats: Vec<i16> = vec![];
                let res: Result<String, String> = (|| {
                    let nfmt = take_i16(&body, &mut at)? .max(0) as usize;
                    let mut fmts = Vec::with_capacity(nfmt);
                    for _ in 0..nfmt {
                        fmts.push(take_i16(&body, &mut at)?);
                    }
                    let nparam = take_i16(&body, &mut at)?.max(0) as usize;
                    let mut vals: Vec<Option<String>> = Vec::with_capacity(nparam);
                    for i in 0..nparam {
                        let l = take_i32(&body, &mut at)?;
                        let raw: Option<Vec<u8>> = if l < 0 {
                            None
                        } else {
                            let l = l as usize;
                            if at + l > body.len() {
                                return Err("truncated Bind parameter".into());
                            }
                            let v = body[at..at + l].to_vec();
                            at += l;
                            Some(v)
                        };
                        // Zero format codes means "all text"; one means "this
                        // format for every parameter"; otherwise one per value.
                        let f = match fmts.len() {
                            0 => 0,
                            1 => fmts[0],
                            _ => *fmts.get(i).unwrap_or(&0),
                        };
                        let oid = *p.param_oids.get(i).unwrap_or(&OID_TEXT);
                        vals.push(decode_param(raw.as_deref(), oid, f)?);
                    }
                    // Result format codes. asyncpg asks for binary on every
                    // column, so honouring these is not an optimisation — it
                    // is the difference between asyncpg reading rows and
                    // refusing the result outright.
                    let nres = take_i16(&body, &mut at)?.max(0) as usize;
                    for _ in 0..nres {
                        let f = take_i16(&body, &mut at)?;
                        if f != 0 && f != 1 {
                            return Err(format!("unknown result format code {}", f));
                        }
                        want_formats.push(f);
                    }
                    substitute_params(&p.sql, &vals)
                })();
                match res {
                    Ok(sql) => {
                        // Binary encoding must use the types the client was
                        // TOLD about, so pull the advertised shape across.
                        let declared = if want_formats.iter().any(|f| *f == 1) {
                            let p = prepared.get_mut(&stmt_name).expect("checked above");
                            prepared_shape(p, resolved.as_ref()).clone()
                        } else {
                            None
                        };
                        portals.insert(portal_name, Portal {
                            sql, result: None, frozen: None,
                            formats: want_formats, declared,
                        });
                        sock.write_all(&bind_complete()).await?;
                    }
                    Err(why) => {
                        sock.write_all(&err_msg("08P01", &why)).await?;
                        failed = true;
                    }
                }
            }

            // ── Describe: 'S' statement, or 'P' portal ─────────────────────
            b'D' => {
                let kind = body.first().copied().unwrap_or(b'S');
                let mut at = 1usize;
                let name = take_cstr(&body, &mut at);
                if kind == b'S' {
                    if !prepared.contains_key(&name) {
                        sock.write_all(&err_msg("26000", &format!(
                            "prepared statement {:?} does not exist", name))).await?;
                        failed = true;
                        continue;
                    }
                    let p = prepared.get_mut(&name).expect("checked above");
                    let oids = p.param_oids.clone();
                    // asyncpg encodes its arguments from this, so the count has
                    // to be right or it refuses the call before sending a Bind.
                    sock.write_all(&parameter_description(&oids)).await?;
                    // Describe(statement) happens before Bind, so the requested
                    // result format is not known yet; Postgres reports text
                    // here too and the client's own Bind decides the encoding.
                    let out = match prepared_shape(p, resolved.as_ref()) {
                        Some((cols, col_oids)) => row_description(cols, col_oids),
                        None => no_data(),
                    };
                    sock.write_all(&out).await?;
                } else {
                    let portal = match portals.get_mut(&name) {
                        Some(p) => p,
                        None => {
                            sock.write_all(&err_msg("34000", &format!(
                                "portal {:?} does not exist", name))).await?;
                            failed = true;
                            continue;
                        }
                    };
                    // A bound portal can be run: doing it here means the
                    // RowDescription reports the columns and types actually
                    // present, which is strictly better than a guess. psycopg3
                    // takes this path on every query.
                    match ensure_executed(portal, &db_name, resolved.as_ref(), read_only) {
                        Err(encoded) => {
                            sock.write_all(&encoded).await?;
                            failed = true;
                        }
                        Ok(()) => {
                            let r = portal.result.as_ref().expect("just executed");
                            if !r.has_rows {
                                sock.write_all(&no_data()).await?;
                            } else {
                                let (cols, oids) = portal.shape(r);
                                let fmts: Vec<i16> =
                                    (0..cols.len()).map(|i| portal.format_of(i)).collect();
                                sock.write_all(&row_description_fmt(&cols, &oids, &fmts)).await?;
                            }
                        }
                    }
                }
            }

            // ── Execute: portal, maximum rows (0 = all) ────────────────────
            b'E' => {
                let mut at = 0usize;
                let name = take_cstr(&body, &mut at);
                let max_rows = take_i32(&body, &mut at).unwrap_or(0);
                let portal = match portals.get_mut(&name) {
                    Some(p) => p,
                    None => {
                        sock.write_all(&err_msg("34000", &format!(
                            "portal {:?} does not exist", name))).await?;
                        failed = true;
                        continue;
                    }
                };
                if let Err(encoded) = ensure_executed(portal, &db_name, resolved.as_ref(), read_only) {
                    sock.write_all(&encoded).await?;
                    failed = true;
                    continue;
                }
                let r = portal.result.as_ref().expect("just executed");
                if !r.has_rows {
                    let tag = r.tag.clone();
                    sock.write_all(&command_complete(&tag)).await?;
                    continue;
                }
                let (cols, oids) = portal.shape(r);
                let limit = if max_rows > 0 {
                    (r.sent + max_rows as usize).min(r.rows.len())
                } else {
                    r.rows.len()
                };
                // Encode the whole batch BEFORE writing any of it. A value that
                // cannot be sent in the advertised binary type has to become an
                // error instead of a truncated row stream — half a result set
                // followed by an error is far harder to diagnose than an error.
                let mut encoded: Vec<Vec<u8>> = Vec::with_capacity(limit - r.sent);
                let mut fail: Option<String> = None;
                for row in &r.rows[r.sent..limit] {
                    let mut vals: Vec<Option<Vec<u8>>> = Vec::with_capacity(cols.len());
                    for (i, c) in cols.iter().enumerate() {
                        let v = row.get(&c.src);
                        let got = if portal.format_of(i) == 1 {
                            cell_binary(v, oids.get(i).copied().unwrap_or(OID_TEXT))
                                .map_err(|e| format!("column {:?}: {}", c.out, e))
                        } else {
                            Ok(cell(v).map(|s| s.into_bytes()))
                        };
                        match got {
                            Ok(b) => vals.push(b),
                            Err(e) => { fail = Some(e); break; }
                        }
                    }
                    if fail.is_some() {
                        break;
                    }
                    encoded.push(data_row_bytes(&vals));
                }
                if let Some(why) = fail {
                    sock.write_all(&err_msg("22P03", &why)).await?;
                    failed = true;
                    continue;
                }
                let mut out = vec![];
                for e in &encoded {
                    out.extend_from_slice(e);
                }
                let r = portal.result.as_mut().expect("just executed");
                r.sent = limit;
                // More rows left and the client capped the batch: suspend the
                // portal instead of completing it. This is what a JDBC
                // `setFetchSize` and a psycopg3 server-side cursor rely on.
                if max_rows > 0 && r.sent < r.rows.len() {
                    out.extend_from_slice(&portal_suspended());
                } else {
                    let tag = if r.tag_counts_rows {
                        format!("{} {}", r.tag, r.sent)
                    } else {
                        r.tag.clone()
                    };
                    out.extend_from_slice(&command_complete(&tag));
                }
                sock.write_all(&out).await?;
            }

            // ── Close: 'S' statement, or 'P' portal ───────────────────────
            b'C' => {
                let kind = body.first().copied().unwrap_or(b'S');
                let mut at = 1usize;
                let name = take_cstr(&body, &mut at);
                if kind == b'S' {
                    prepared.remove(&name);
                } else {
                    portals.remove(&name);
                }
                // Closing something that was never open is explicitly not an
                // error in the protocol.
                sock.write_all(&close_complete()).await?;
            }

            // Flush: everything is written unbuffered already, so this is a
            // no-op — but it must NOT produce a ReadyForQuery, or a client that
            // flushes mid-sequence (asyncpg does, after Describe) loses sync.
            b'H' => {}

            b'S' => {
                failed = false;
                sock.write_all(&ready()).await?;
            }

            other => {
                sock.write_all(&err_msg(
                    "08P01",
                    &format!("unexpected frontend message {:?}", other as char),
                )).await?;
                failed = true;
            }
        }
    }
}

const READ_ONLY_MSG: &str =
    "this endpoint is running read-only (NEDBD_PG_READ_ONLY=1). Writes are \
     implemented but disabled on this server — unset the flag to allow them.";

fn no_db(db_name: &str) -> Vec<u8> {
    err_msg("3D000", &format!(
        "database {:?} is not open on this server — create it first \
         (POST /v1/databases), or connect with -d <name>", db_name))
}

/// True when the statement carried a RETURNING clause. Checked against the raw
/// SQL because `RETURNING *` yields an EMPTY projection, which is otherwise
/// indistinguishable from "no RETURNING at all".
fn wants_returning(sql: &str) -> bool {
    find_kw(&sql.to_uppercase(), "RETURNING").is_some()
}

/// A unique key for a server-assigned INSERT id.
fn next_row_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0);
    format!("r{}{}", ts, n)
}

/// One executed statement, held apart from any wire encoding.
///
/// This type is why the simple and extended protocols share an execution path
/// rather than growing two copies of the SQL→NEDB semantics. The simple path
/// encodes it immediately; the extended path parks it in a portal and dribbles
/// the rows out across successive `Execute` messages. Both get identical
/// answers because both call `execute_stmt`.
pub struct Executed {
    /// The rows the client gets — a SELECT's result, or a write's `RETURNING`.
    pub rows: Vec<Value>,
    /// How to project them (empty = every key in the row).
    pub project: Vec<Col>,
    /// Whether the client asked for rows at all. Distinct from `rows.is_empty()`:
    /// a `SELECT` matching nothing still owes a `RowDescription`, while an
    /// `UPDATE` without `RETURNING` owes `NoData`.
    pub has_rows: bool,
    /// The command tag, already rendered — except for a SELECT, where the row
    /// count is only known once the rows have actually been sent.
    pub tag: String,
    /// True when `tag` is a SELECT-shaped tag whose count is the rows sent.
    pub tag_counts_rows: bool,
}

impl Executed {
    fn nothing(tag: &str) -> Self {
        Executed { rows: vec![], project: vec![], has_rows: false, tag: tag.to_string(), tag_counts_rows: false }
    }
    /// Render the final `CommandComplete` given how many rows went out.
    fn tag_for(&self, sent: usize) -> String {
        if self.tag_counts_rows { format!("{} {}", self.tag, sent) } else { self.tag.clone() }
    }
}

/// Run ONE statement. `Err` carries an already-encoded `ErrorResponse`.
///
/// Every SQL→NEDB decision lives here, which is the point: the extended query
/// protocol added below is then purely a matter of message framing, and cannot
/// drift from the simple path's semantics.
fn execute_stmt(
    stmt_sql: &str,
    db_name: &str,
    db: Option<&Arc<Db>>,
    read_only: bool,
) -> Result<Executed, Vec<u8>> {
    let stmt = translate(stmt_sql).map_err(|why| err_msg("0A000", &why))?;

    // Every arm below that touches storage needs a database; resolve the
    // "no such database" answer once instead of at each use.
    macro_rules! need_db {
        () => {
            match db {
                Some(db) => db,
                None => return Err(no_db(db_name)),
            }
        };
    }
    macro_rules! need_write {
        () => {
            if read_only {
                return Err(err_msg("25006", READ_ONLY_MSG));
            }
        };
    }

    match stmt {
        Stmt::Ok(tag) => Ok(Executed::nothing(if tag.is_empty() { "SELECT 0" } else { tag })),

        Stmt::Canned { cols, row } => {
            // Fold the canned answer into an ordinary row so the encoders,
            // the portal machinery and `Describe` all see one shape.
            let mut obj = serde_json::Map::new();
            for (c, v) in cols.iter().zip(row.iter()) {
                obj.insert(c.clone(), Value::String(v.clone()));
            }
            Ok(Executed {
                rows: vec![Value::Object(obj)],
                project: cols.iter().map(|c| Col::same(c)).collect(),
                has_rows: true,
                tag: "SELECT".into(),
                tag_counts_rows: true,
            })
        }

        Stmt::Query { nql, project } => {
            let db = need_db!();
            let (rows, _) = crate::nql::query(db, &nql).map_err(|e| {
                err_msg("42601", &format!("{} (translated to NQL: {})", e, nql))
            })?;
            Ok(Executed { rows, project, has_rows: true, tag: "SELECT".into(), tag_counts_rows: true })
        }

        Stmt::Insert { coll, rows, returning } => {
            let db = need_db!();
            need_write!();
            let mut written: Vec<Value> = vec![];
            for (i, r) in rows.iter().enumerate() {
                // The engine requires an id. When the statement did not supply
                // one, mint a unique key rather than silently overwriting a
                // shared default.
                let id = match &r.id {
                    Some(id) => id.clone(),
                    None => format!("{}-{}", next_row_id(), i),
                };
                let node = db
                    .put(&coll, &id, Value::Object(r.doc.clone()),
                         r.caused_by.clone(), r.valid_from.clone(), r.valid_to.clone())
                    .map_err(|e| err_msg("XX000", &format!("INSERT failed: {}", e)))?;
                written.push(crate::nql::node_to_json(&node));
            }
            let n = written.len();
            let has_rows = wants_returning(stmt_sql);
            Ok(Executed {
                rows: if has_rows { written } else { vec![] },
                project: returning,
                has_rows,
                // Postgres reports `INSERT <oid> <rows>`; the oid is always 0.
                tag: format!("INSERT 0 {}", n),
                tag_counts_rows: false,
            })
        }

        Stmt::Update { coll, set, nql, returning } => {
            let db = need_db!();
            need_write!();
            // Matching rows come from an ordinary NQL read, so the whole
            // predicate surface works inside an UPDATE.
            let (matched, _) = crate::nql::query(db, &nql).map_err(|e| {
                err_msg("42601", &format!("{} (translated to NQL: {})", e, nql))
            })?;
            let mut written: Vec<Value> = vec![];
            for row in &matched {
                let id = match row.get("_id").and_then(|v| v.as_str()) {
                    Some(id) => id.to_string(),
                    None => continue,
                };
                // Merge onto the CURRENT stored document, not onto the query
                // row: a query row carries injected `_`-prefixed metadata that
                // must never be written back into the payload.
                let mut doc = match db.get(&coll, &id) {
                    Some(n) => match n.data {
                        Value::Object(m) => m,
                        _ => serde_json::Map::new(),
                    },
                    None => continue,
                };
                for (k, v) in &set {
                    doc.insert(k.clone(), v.clone());
                }
                // An UPDATE is a NEW VERSION — the prior value stays readable
                // with AS OF SYSTEM TIME. That is the whole point.
                let node = db
                    .put(&coll, &id, Value::Object(doc), vec![], None, None)
                    .map_err(|e| err_msg("XX000", &format!("UPDATE failed: {}", e)))?;
                written.push(crate::nql::node_to_json(&node));
            }
            let n = written.len();
            let has_rows = wants_returning(stmt_sql);
            Ok(Executed {
                rows: if has_rows { written } else { vec![] },
                project: returning,
                has_rows,
                tag: format!("UPDATE {}", n),
                tag_counts_rows: false,
            })
        }

        Stmt::Delete { coll, nql, returning } => {
            let db = need_db!();
            need_write!();
            let (matched, _) = crate::nql::query(db, &nql).map_err(|e| {
                err_msg("42601", &format!("{} (translated to NQL: {})", e, nql))
            })?;
            // RETURNING must be captured BEFORE the delete: after the tombstone
            // the row is no longer readable by id.
            let returned = matched.clone();
            let mut n = 0usize;
            for row in &matched {
                if let Some(id) = row.get("_id").and_then(|v| v.as_str()) {
                    match db.delete(&coll, id) {
                        Ok(true) => n += 1,
                        Ok(false) => {}
                        Err(e) => return Err(err_msg("XX000", &format!("DELETE failed: {}", e))),
                    }
                }
            }
            let has_rows = wants_returning(stmt_sql);
            Ok(Executed {
                rows: if has_rows { returned } else { vec![] },
                project: returning,
                has_rows,
                tag: format!("DELETE {}", n),
                tag_counts_rows: false,
            })
        }
    }
}

/// Execute a simple-query payload, which may hold several `;`-separated statements.
fn run_simple_query(sql: &str, db_name: &str, db: Option<&Arc<Db>>, read_only: bool) -> Vec<u8> {
    let mut out = vec![];
    let statements = split_statements(sql);
    if statements.is_empty() {
        // EmptyQueryResponse
        return Out::msg(b'I').finish();
    }
    for stmt_sql in statements {
        match execute_stmt(&stmt_sql, db_name, db, read_only) {
            // Abandon the rest of the batch on the first error, as Postgres does.
            Err(encoded) => {
                out.extend_from_slice(&encoded);
                return out;
            }
            Ok(ex) => {
                if ex.has_rows {
                    out.extend_from_slice(&encode_rows(&ex.rows, &ex.project));
                }
                out.extend_from_slice(&command_complete(&ex.tag_for(ex.rows.len())));
            }
        }
    }
    out
}

/// Split on `;` at the top level, ignoring separators inside string literals.
fn split_statements(sql: &str) -> Vec<String> {
    let mut out = vec![];
    let mut cur = String::new();
    let mut in_s = false;
    for c in sql.chars() {
        match c {
            '\'' => { in_s = !in_s; cur.push(c); }
            ';' if !in_s => {
                if !cur.trim().is_empty() { out.push(cur.clone()); }
                cur.clear();
            }
            _ => cur.push(c),
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

/// Bind and serve the Postgres read endpoint until the process exits.
pub async fn run(host: &str, port: u16, resolver: Arc<dyn DbResolver>) -> anyhow::Result<()> {
    // Writes are ON by default — that is the parity position. An operator who
    // wants the "system of proof beside your database" deployment, where this
    // door must never mutate anything, sets NEDBD_PG_READ_ONLY=1.
    let read_only = std::env::var("NEDBD_PG_READ_ONLY")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let listener = TcpListener::bind((host, port)).await?;
    println!("  pgwire   postgres endpoint on {}:{} — psql / DBeaver / psycopg ({})",
             host, port,
             if read_only { "SELECT only — read-only mode" } else { "SELECT + INSERT/UPDATE/DELETE" });
    loop {
        let (sock, _peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("  [pgwire] accept failed: {}", e);
                continue;
            }
        };
        let r = Arc::clone(&resolver);
        tokio::spawn(async move {
            let _ = sock.set_nodelay(true);
            if let Err(e) = handle(sock, r, read_only).await {
                // A client disconnecting mid-message is routine, not an incident.
                if e.kind() != std::io::ErrorKind::UnexpectedEof
                    && e.kind() != std::io::ErrorKind::ConnectionReset
                {
                    eprintln!("  [pgwire] connection error: {}", e);
                }
            }
        });
    }
}

// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn q(sql: &str) -> String {
        match translate(sql) {
            Ok(Stmt::Query { nql, .. }) => nql,
            other => panic!("expected a query for {:?}, got {:?}", sql, other),
        }
    }
    /// Output column names, in order.
    fn proj(sql: &str) -> Vec<String> {
        match translate(sql) {
            Ok(Stmt::Query { project, .. }) => project.iter().map(|c| c.out.clone()).collect(),
            other => panic!("expected a query for {:?}, got {:?}", sql, other),
        }
    }
    /// (source key, output name) pairs, for the aggregate renaming.
    fn proj_pairs(sql: &str) -> Vec<(String, String)> {
        match translate(sql) {
            Ok(Stmt::Query { project, .. }) =>
                project.iter().map(|c| (c.src.clone(), c.out.clone())).collect(),
            other => panic!("expected a query for {:?}, got {:?}", sql, other),
        }
    }
    fn names(cols: &[Col]) -> Vec<String> { cols.iter().map(|c| c.out.clone()).collect() }

    #[test]
    fn select_star_becomes_bare_from() {
        assert_eq!(q("SELECT * FROM orders"), "FROM orders");
        assert_eq!(q("select * from orders;"), "FROM orders");
        assert_eq!(proj("SELECT * FROM orders"), Vec::<String>::new());
    }

    #[test]
    fn a_column_list_becomes_a_projection_not_a_clause() {
        // NQL has no projection, so the column list is carried separately and
        // applied to the returned rows.
        assert_eq!(q("SELECT status, total FROM orders"), "FROM orders");
        assert_eq!(proj("SELECT status, total FROM orders"), vec!["status", "total"]);
    }

    #[test]
    fn aliases_and_qualified_names_reduce_to_the_field() {
        assert_eq!(proj("SELECT o.status AS s, o.total total FROM orders o"),
                   vec!["status", "total"]);
        assert_eq!(q("SELECT * FROM public.orders"), "FROM orders");
        assert_eq!(q("SELECT * FROM \"orders\""), "FROM orders");
    }

    #[test]
    fn where_clauses_pass_through_with_sql_literals_rewritten() {
        assert_eq!(q("SELECT * FROM orders WHERE status = 'paid'"),
                   r#"FROM orders WHERE status = "paid""#);
        assert_eq!(q("SELECT * FROM orders WHERE status <> 'paid'"),
                   r#"FROM orders WHERE status != "paid""#);
        assert_eq!(q("SELECT * FROM orders WHERE status IN ('paid','open')"),
                   r#"FROM orders WHERE status IN ("paid","open")"#);
    }

    /// SQL escapes an embedded quote by doubling it. That must become ONE
    /// character inside the NQL string, not terminate it.
    #[test]
    fn a_doubled_sql_quote_is_one_literal_character() {
        assert_eq!(q("SELECT * FROM t WHERE name = 'it''s'"),
                   r#"FROM t WHERE name = "it's""#);
    }

    /// A double quote inside a SQL literal has to be escaped for NQL, whose
    /// lexer collapses \" — otherwise it would close the string early.
    #[test]
    fn a_double_quote_inside_a_sql_literal_is_escaped_for_nql() {
        assert_eq!(q(r#"SELECT * FROM t WHERE name = 'say "hi"'"#),
                   r#"FROM t WHERE name = "say \"hi\"""#);
    }

    #[test]
    fn the_shared_clauses_are_handed_to_nql_unchanged() {
        assert_eq!(q("SELECT * FROM orders ORDER BY total DESC LIMIT 10 OFFSET 5"),
                   "FROM orders ORDER BY total DESC LIMIT 10 OFFSET 5");
        assert_eq!(q("SELECT * FROM orders GROUP BY region"), "FROM orders GROUP BY region");
        assert_eq!(q("SELECT * FROM o WHERE total BETWEEN 1 AND 9 ORDER BY a, b DESC"),
                   "FROM o WHERE total BETWEEN 1 AND 9 ORDER BY a, b DESC");
    }

    /// An aggregate must surface as ONE column, named as SQL names it.
    ///
    /// NQL answers `SUM(total)` with `{count, sum_total, value}` — `value`
    /// being a back-compat alias. Passing that straight through gave
    /// `SELECT COUNT(*)` two columns (`count`, `value`) where SQL promises
    /// one, and leaked an internal key name onto the wire.
    #[test]
    fn an_aggregate_is_one_column_named_as_sql_names_it() {
        assert_eq!(proj_pairs("SELECT COUNT(*) FROM orders"),
                   vec![("count".to_string(), "count".to_string())]);
        assert_eq!(proj_pairs("SELECT SUM(total) FROM orders"),
                   vec![("sum_total".to_string(), "sum".to_string())]);
        assert_eq!(proj_pairs("SELECT avg(total) FROM orders"),
                   vec![("avg_total".to_string(), "avg".to_string())]);
        assert_eq!(proj_pairs("SELECT MIN(total) FROM orders"),
                   vec![("min_total".to_string(), "min".to_string())]);
        // And the encoded result really is one column with that name.
        let rows = vec![json!({"count": 4, "sum_total": 420, "value": 420})];
        let p = vec![Col::renamed("sum_total", "sum")];
        let cols = columns_for(&rows, &p);
        assert_eq!(names(&cols), vec!["sum"], "one column, SQL's name");
        assert_eq!(cell(rows[0].get(&cols[0].src)), Some("420".to_string()));
    }

    /// A grouped NQL row holds the group key, `count` and the aggregate —
    /// nothing else. Projecting another column found nothing and rendered
    /// NULL, which is a silent wrong answer. Postgres errors; so do we, in
    /// Postgres's own words.
    #[test]
    fn a_bare_column_with_group_by_is_refused_not_nulled() {
        let e = translate("SELECT region, total FROM orders GROUP BY region").unwrap_err();
        assert!(e.contains("must appear in the GROUP BY clause"), "{}", e);
        assert!(e.contains("total"), "the message names the offending column: {}", e);

        // The group key itself, and `count`, are both legitimate.
        assert!(translate("SELECT region FROM orders GROUP BY region").is_ok());
        assert!(translate("SELECT region, count FROM orders GROUP BY region").is_ok());
        // As is an aggregate over the grouped set.
        assert!(translate("SELECT SUM(total) FROM orders GROUP BY region").is_ok());
        // And `*` is unaffected — it returns whatever the grouped row holds.
        assert!(translate("SELECT * FROM orders GROUP BY region").is_ok());
    }

    #[test]
    fn count_star_becomes_nql_count() {
        assert_eq!(q("SELECT COUNT(*) FROM orders"), "FROM orders COUNT");
        assert_eq!(q("SELECT count(*) FROM orders WHERE total > 5"),
                   "FROM orders COUNT WHERE total > 5");
    }

    #[test]
    fn aggregates_carry_their_target_column() {
        assert_eq!(q("SELECT SUM(total) FROM orders"), "FROM orders SUM total");
        assert_eq!(q("SELECT avg(total) FROM orders WHERE region = 'eu'"),
                   r#"FROM orders AVG total WHERE region = "eu""#);
        assert!(translate("SELECT SUM(*) FROM orders").is_err());
    }

    /// The bridge worth having: Postgres spells time travel
    /// `AS OF SYSTEM TIME`, and NEDB's is sequence-addressed and permanent.
    #[test]
    fn as_of_system_time_bridges_to_nql_as_of() {
        assert_eq!(q("SELECT * FROM orders AS OF SYSTEM TIME 42"),
                   "FROM orders AS OF 42");
        assert_eq!(q("SELECT * FROM orders AS OF SYSTEM TIME 42 WHERE total > 1"),
                   "FROM orders AS OF 42 WHERE total > 1");
        // A wall-clock timestamp is refused with the reason, not silently ignored.
        let e = translate("SELECT * FROM orders AS OF SYSTEM TIME '2026-01-01'").unwrap_err();
        assert!(e.contains("sequence number"), "{}", e);
    }

    #[test]
    fn handshake_queries_are_answered_so_clients_can_connect() {
        assert!(matches!(translate("SELECT version()"), Ok(Stmt::Canned { .. })));
        assert!(matches!(translate("SHOW transaction_isolation"), Ok(Stmt::Canned { .. })));
        assert!(matches!(translate("SELECT current_schema()"), Ok(Stmt::Canned { .. })));
        assert!(matches!(translate("SET extra_float_digits = 3"), Ok(Stmt::Ok(_))));
        assert!(matches!(translate("BEGIN"), Ok(Stmt::Ok(_))));
        assert!(matches!(translate(""), Ok(Stmt::Ok(_))));
    }

    /// Every refusal has to name the boundary. "Syntax error" would send a
    /// developer hunting for a typo that is not there.
    #[test]
    fn unsupported_sql_is_refused_with_a_reason() {
        for (sql, expect) in [
            ("INSERT INTO t VALUES (1)", "explicit column list"),
            ("CREATE TABLE t (a int)", "DDL"),
            ("TRUNCATE t", "append-only"),
            ("GRANT ALL ON t TO x", "privilege system"),
            ("SELECT * FROM a JOIN b ON a.x = b.x", "JOIN is not supported"),
            ("SELECT * FROM a UNION SELECT * FROM b", "UNION"),
            ("SELECT DISTINCT region FROM orders", "GROUP BY"),
            ("SELECT * FROM (SELECT 1) x", "subqueries in FROM"),
            ("SELECT * FROM a, b", "more than one collection"),
            ("SELECT lower(status) FROM orders", "expressions in the select list"),
            ("VACUUM", "only SELECT"),
        ] {
            let e = translate(sql).unwrap_err();
            assert!(e.contains(expect), "for {:?} expected {:?} in {:?}", sql, expect, e);
        }
    }

    // ── writes ───────────────────────────────────────────────────────────────
    //
    // SQL's write semantics and NEDB's append-only model line up: INSERT is a
    // put, UPDATE is a new version, DELETE is a tombstone. These tests pin the
    // parse; tests/test_pgwire.py proves the behaviour against a live server,
    // including that the PRIOR value is still readable afterwards.

    fn ins(sql: &str) -> (String, Vec<InsertRow>, Vec<Col>) {
        match translate(sql) {
            Ok(Stmt::Insert { coll, rows, returning }) => (coll, rows, returning),
            other => panic!("expected INSERT for {:?}, got {:?}", sql, other),
        }
    }

    #[test]
    fn insert_becomes_a_put_per_row() {
        let (coll, rows, ret) = ins("INSERT INTO orders (_id, status, total) VALUES ('o1', 'paid', 120)");
        assert_eq!(coll, "orders");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id.as_deref(), Some("o1"));
        assert_eq!(rows[0].doc.get("status"), Some(&json!("paid")));
        assert_eq!(rows[0].doc.get("total"), Some(&json!(120)));
        // `_id` is the key, not a payload field.
        assert!(!rows[0].doc.contains_key("_id"));
        assert!(ret.is_empty());
    }

    #[test]
    fn a_multi_row_insert_yields_one_row_each() {
        let (_, rows, _) = ins(
            "INSERT INTO t (id, n) VALUES ('a', 1), ('b', 2), ('c', 3)");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1].id.as_deref(), Some("b"));
        assert_eq!(rows[2].doc.get("n"), Some(&json!(3)));
    }

    #[test]
    fn an_insert_without_an_id_column_lets_the_server_assign_one() {
        let (_, rows, _) = ins("INSERT INTO t (n) VALUES (1)");
        assert_eq!(rows[0].id, None, "the executor mints a unique key");
        assert_eq!(rows[0].doc.get("n"), Some(&json!(1)));
    }

    /// Provenance is reachable from SQL, not only from the HTTP API — which is
    /// the point of having writes here at all.
    #[test]
    fn insert_lifts_provenance_out_of_reserved_columns() {
        let (_, rows, _) = ins(
            "INSERT INTO audit (_id, _caused_by, _valid_from, kind) \
             VALUES ('e1', 'abc123', '2026-01-01', 'reprice')");
        assert_eq!(rows[0].caused_by, vec!["abc123".to_string()]);
        assert_eq!(rows[0].valid_from.as_deref(), Some("2026-01-01"));
        assert_eq!(rows[0].doc.get("kind"), Some(&json!("reprice")));
        // None of the reserved names leak into the stored payload.
        for k in ["_id", "_caused_by", "_valid_from"] {
            assert!(!rows[0].doc.contains_key(k), "{} leaked into the doc", k);
        }
    }

    #[test]
    fn insert_values_cover_the_scalar_types() {
        let (_, rows, _) = ins(
            "INSERT INTO t (s, i, f, b, n) VALUES ('x', 42, 1.5, TRUE, NULL)");
        assert_eq!(rows[0].doc.get("s"), Some(&json!("x")));
        assert_eq!(rows[0].doc.get("i"), Some(&json!(42)));
        assert_eq!(rows[0].doc.get("f"), Some(&json!(1.5)));
        assert_eq!(rows[0].doc.get("b"), Some(&json!(true)));
        assert_eq!(rows[0].doc.get("n"), Some(&Value::Null));
    }

    /// A doubled '' is one literal quote, and a comma inside a string is not a
    /// value separator.
    #[test]
    fn insert_literals_survive_quotes_and_commas() {
        let (_, rows, _) = ins("INSERT INTO t (a, b) VALUES ('it''s', 'x,y')");
        assert_eq!(rows[0].doc.get("a"), Some(&json!("it's")));
        assert_eq!(rows[0].doc.get("b"), Some(&json!("x,y")));
    }

    #[test]
    fn insert_refuses_what_it_cannot_store_faithfully() {
        // An unevaluated expression stored as text would be a wrong value.
        assert!(translate("INSERT INTO t (a) VALUES (1 + 1)").is_err());
        assert!(translate("INSERT INTO t (a) VALUES (now())").is_err());
        // Column/value count mismatch.
        let e = translate("INSERT INTO t (a, b) VALUES (1)").unwrap_err();
        assert!(e.contains("values for"), "{}", e);
        // No column list at all.
        let e2 = translate("INSERT INTO t VALUES (1)").unwrap_err();
        assert!(e2.contains("explicit column list"), "{}", e2);
    }

    #[test]
    fn update_finds_rows_with_the_full_predicate_surface() {
        match translate("UPDATE orders SET status = 'void' WHERE total < 50 AND region IN ('eu')") {
            Ok(Stmt::Update { coll, set, nql, .. }) => {
                assert_eq!(coll, "orders");
                assert_eq!(set, vec![("status".to_string(), json!("void"))]);
                // The WHERE became ordinary NQL, so IN/BETWEEN/LIKE all work.
                assert_eq!(nql, r#"FROM orders WHERE total < 50 AND region IN ("eu")"#);
            }
            other => panic!("expected UPDATE, got {:?}", other),
        }
    }

    #[test]
    fn update_without_where_targets_the_whole_collection() {
        // Postgres allows it, so parity allows it.
        match translate("UPDATE t SET a = 1") {
            Ok(Stmt::Update { nql, .. }) => assert_eq!(nql, "FROM t"),
            other => panic!("expected UPDATE, got {:?}", other),
        }
    }

    #[test]
    fn update_handles_several_assignments() {
        match translate("UPDATE t SET a = 1, b = 'x,y', c = NULL WHERE id = 'k'") {
            Ok(Stmt::Update { set, .. }) => {
                assert_eq!(set.len(), 3);
                assert_eq!(set[1], ("b".to_string(), json!("x,y")));
                assert_eq!(set[2], ("c".to_string(), Value::Null));
            }
            other => panic!("expected UPDATE, got {:?}", other),
        }
        assert!(translate("UPDATE t SET").is_err());
        assert!(translate("UPDATE t SET a").is_err());
    }

    #[test]
    fn delete_becomes_a_predicate_over_the_collection() {
        match translate("DELETE FROM orders WHERE status = 'void'") {
            Ok(Stmt::Delete { coll, nql, .. }) => {
                assert_eq!(coll, "orders");
                assert_eq!(nql, r#"FROM orders WHERE status = "void""#);
            }
            other => panic!("expected DELETE, got {:?}", other),
        }
        match translate("DELETE FROM t") {
            Ok(Stmt::Delete { nql, .. }) => assert_eq!(nql, "FROM t"),
            other => panic!("expected DELETE, got {:?}", other),
        }
    }

    #[test]
    fn returning_is_parsed_off_every_write() {
        let (_, _, ret) = ins("INSERT INTO t (a) VALUES (1) RETURNING a, _id");
        assert_eq!(ret.iter().map(|c| c.out.clone()).collect::<Vec<_>>(), vec!["a", "_id"]);
        // `RETURNING *` is an empty projection — every column — which is why
        // the executor checks the raw SQL for the keyword instead.
        let (_, _, star) = ins("INSERT INTO t (a) VALUES (1) RETURNING *");
        assert!(star.is_empty());
        assert!(wants_returning("INSERT INTO t (a) VALUES (1) RETURNING *"));
        assert!(!wants_returning("INSERT INTO t (a) VALUES (1)"));

        match translate("UPDATE t SET a = 1 WHERE id = 'k' RETURNING a") {
            Ok(Stmt::Update { nql, returning, .. }) => {
                assert_eq!(returning.len(), 1);
                // RETURNING must NOT leak into the predicate.
                assert!(!nql.to_uppercase().contains("RETURNING"), "{}", nql);
            }
            other => panic!("expected UPDATE, got {:?}", other),
        }
        match translate("DELETE FROM t WHERE id = 'k' RETURNING *") {
            Ok(Stmt::Delete { nql, .. }) =>
                assert!(!nql.to_uppercase().contains("RETURNING"), "{}", nql),
            other => panic!("expected DELETE, got {:?}", other),
        }
    }

    #[test]
    fn a_keyword_inside_a_value_is_not_a_clause() {
        match translate("UPDATE t SET note = 'where returning from' WHERE id = 'k'") {
            Ok(Stmt::Update { set, nql, .. }) => {
                assert_eq!(set[0].1, json!("where returning from"));
                assert_eq!(nql, r#"FROM t WHERE id = "k""#);
            }
            other => panic!("expected UPDATE, got {:?}", other),
        }
    }

    #[test]
    fn split_top_respects_quotes_and_nesting() {
        assert_eq!(split_top("a, b, c", ',').len(), 3);
        assert_eq!(split_top("(1, 2), (3, 4)", ',').len(), 2);
        assert_eq!(split_top("'a,b', c", ',').len(), 2);
        assert_eq!(split_top("'it''s, fine', c", ',').len(), 2);
    }

    #[test]
    fn comments_and_whitespace_do_not_confuse_the_translator() {
        assert_eq!(q("SELECT *\n  FROM orders  -- trailing note\n"), "FROM orders");
        assert_eq!(q("SELECT /* inline */ * FROM orders"), "FROM orders");
        // A keyword inside a string literal must not be treated as a clause.
        assert_eq!(q("SELECT * FROM t WHERE note = 'from here to JOIN'"),
                   r#"FROM t WHERE note = "from here to JOIN""#);
    }

    #[test]
    fn find_kw_ignores_quotes_parens_and_substrings() {
        assert_eq!(find_kw("SELECT A FROM B", "FROM"), Some(9));
        assert_eq!(find_kw("SELECT 'FROM' FROM B", "FROM"), Some(14));
        assert_eq!(find_kw("SELECT F(x FROM y) FROM B", "FROM"), Some(19));
        assert_eq!(find_kw("SELECT FROMAGE", "FROM"), None);
        assert_eq!(find_kw("SELECT X_FROM", "FROM"), None);
    }

    // ── result encoding ──────────────────────────────────────────────────────

    #[test]
    fn provenance_columns_sort_after_the_users_own_fields() {
        let rows = vec![json!({"_id":"1","_hash":"ab","status":"paid","total":9})];
        assert_eq!(names(&columns_for(&rows, &[])),
                   vec!["status", "total", "_hash", "_id"]);
    }

    #[test]
    fn an_explicit_projection_sets_the_column_order() {
        let rows = vec![json!({"a":1,"b":2})];
        let p = vec![Col::same("b"), Col::same("a")];
        assert_eq!(names(&columns_for(&rows, &p)), vec!["b", "a"]);
    }

    #[test]
    fn columns_are_the_union_across_sparse_rows() {
        // A document store has no schema, so row 2 may carry a field row 1 lacks.
        let rows = vec![json!({"a":1}), json!({"b":2})];
        assert_eq!(names(&columns_for(&rows, &[])), vec!["a", "b"]);
    }

    #[test]
    fn type_oids_follow_the_first_non_null_value() {
        let rows = vec![json!({"i":1,"f":1.5,"b":true,"s":"x","n":null})];
        assert_eq!(oid_for(&rows, "i"), OID_INT8);
        assert_eq!(oid_for(&rows, "f"), OID_FLOAT8);
        assert_eq!(oid_for(&rows, "b"), OID_BOOL);
        assert_eq!(oid_for(&rows, "s"), OID_TEXT);
        // All-null and absent columns fall back to text rather than guessing.
        assert_eq!(oid_for(&rows, "n"), OID_TEXT);
        assert_eq!(oid_for(&rows, "absent"), OID_TEXT);
    }

    #[test]
    fn a_column_that_is_null_in_the_first_row_still_gets_its_type() {
        let rows = vec![json!({"v": null}), json!({"v": 7})];
        assert_eq!(oid_for(&rows, "v"), OID_INT8);
    }

    #[test]
    fn cells_render_in_postgres_text_format() {
        assert_eq!(cell(Some(&json!("x"))), Some("x".to_string()));
        assert_eq!(cell(Some(&json!(true))), Some("t".to_string()));
        assert_eq!(cell(Some(&json!(false))), Some("f".to_string()));
        assert_eq!(cell(Some(&json!(42))), Some("42".to_string()));
        assert_eq!(cell(Some(&json!(null))), None);
        assert_eq!(cell(None), None);
        // Nested values render as JSON text rather than being dropped.
        assert_eq!(cell(Some(&json!({"a":1}))), Some("{\"a\":1}".to_string()));
    }

    /// The framing has to be exact or the client desynchronises and hangs.
    /// Length covers the length field itself but not the tag byte.
    #[test]
    fn message_framing_length_excludes_the_tag() {
        let mut m = Out::msg(b'Z');
        m.bytes(b"I");
        let bytes = m.finish();
        assert_eq!(bytes[0], b'Z');
        assert_eq!(i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]), 5);
        assert_eq!(bytes.len(), 6);
    }

    #[test]
    fn a_result_set_encodes_as_description_then_rows_then_complete() {
        let rows = vec![json!({"a": 1}), json!({"a": 2})];
        let out = encode_result(&rows, &[]);
        assert_eq!(out[0], b'T');
        let tags: Vec<u8> = {
            // Walk the message stream by its own length prefixes.
            let mut t = vec![];
            let mut i = 0usize;
            while i < out.len() {
                t.push(out[i]);
                let len = i32::from_be_bytes([out[i+1], out[i+2], out[i+3], out[i+4]]) as usize;
                i += 1 + len;
            }
            t
        };
        assert_eq!(tags, vec![b'T', b'D', b'D', b'C'],
                   "one description, one row each, one completion");
    }

    /// A statement must emit EXACTLY ONE CommandComplete. A write with
    /// RETURNING that reused the SELECT encoder sent two, and the visible
    /// symptom was RETURNING yielding no rows: the client took the first tag
    /// as the end of the statement and threw the description away.
    #[test]
    fn a_write_with_returning_emits_exactly_one_command_complete() {
        let rows = vec![json!({"_id": "o1", "total": 9})];
        let mut out = encode_rows(&rows, &[Col::same("_id")]);
        out.extend_from_slice(&command_complete("INSERT 0 1"));
        let mut tags = vec![];
        let mut i = 0usize;
        while i < out.len() {
            tags.push(out[i]);
            let len = i32::from_be_bytes([out[i+1], out[i+2], out[i+3], out[i+4]]) as usize;
            i += 1 + len;
        }
        assert_eq!(tags, vec![b'T', b'D', b'C'], "one description, one row, ONE tag");
        assert_eq!(tags.iter().filter(|t| **t == b'C').count(), 1);
        // encode_rows alone must not carry a tag at all.
        assert!(!encode_rows(&rows, &[]).contains(&b'C')
                || encode_rows(&rows, &[]).iter().filter(|b| **b == b'C').count() > 0);
        let bare = encode_rows(&rows, &[Col::same("_id")]);
        let mut bare_tags = vec![];
        let mut j = 0usize;
        while j < bare.len() {
            bare_tags.push(bare[j]);
            let len = i32::from_be_bytes([bare[j+1], bare[j+2], bare[j+3], bare[j+4]]) as usize;
            j += 1 + len;
        }
        assert_eq!(bare_tags, vec![b'T', b'D'], "encode_rows never appends a tag");
    }

    #[test]
    fn an_empty_result_still_sends_a_description() {
        let out = encode_result(&[], &[Col::same("a")]);
        assert_eq!(out[0], b'T', "clients need the shape even with no rows");
    }

    #[test]
    fn statements_split_on_top_level_semicolons_only() {
        assert_eq!(split_statements("SELECT 1; SELECT 2").len(), 2);
        assert_eq!(split_statements("SELECT ';'").len(), 1);
        assert_eq!(split_statements("SELECT 1;").len(), 1);
        assert_eq!(split_statements("   ").len(), 0);
    }

    #[test]
    fn an_error_names_its_sqlstate() {
        let e = String::from_utf8_lossy(&err_msg("0A000", "x")).to_string();
        assert!(e.contains("ERROR"));
        assert!(e.contains("0A000"));
    }

    // ── the extended query protocol ─────────────────────────────────────────

    #[test]
    fn placeholders_are_counted_outside_string_literals() {
        assert_eq!(param_count("SELECT a FROM t WHERE b = $1 AND c = $2"), 2);
        assert_eq!(param_count("SELECT a FROM t"), 0);
        // The highest index wins, because a parameter may be reused.
        assert_eq!(param_count("WHERE a = $2 OR b = $2 OR c = $1"), 2);
        assert_eq!(param_count("SELECT a FROM t WHERE b = '$1'"), 0,
                   "a placeholder inside a literal is data, not a parameter");
        assert_eq!(param_count("WHERE a = $10 AND b = $1"), 10,
                   "two-digit indexes must not be read as $1 followed by 0");
    }

    #[test]
    fn parameters_are_spliced_as_literals() {
        let out = substitute_params("WHERE a = $1 AND b = $2 AND c = $3",
            &[Some("'x'".into()), Some("42".into()), None]).unwrap();
        assert_eq!(out, "WHERE a = 'x' AND b = 42 AND c = NULL");
    }

    #[test]
    fn substitution_leaves_string_literals_alone() {
        let out = substitute_params("WHERE a = '$1' AND b = $1", &[Some("9".into())]).unwrap();
        assert_eq!(out, "WHERE a = '$1' AND b = 9");
    }

    #[test]
    fn too_few_parameters_is_an_error_not_a_silent_null() {
        // The alternative — treating a missing parameter as NULL — turns a
        // client bug into a wrong answer with a 200-shaped response.
        let e = substitute_params("WHERE a = $2", &[Some("1".into())]).unwrap_err();
        assert!(e.contains("$2"), "{}", e);
    }

    #[test]
    fn a_quote_in_a_parameter_cannot_escape_its_literal() {
        let lit = decode_param(Some(b"it's"), OID_TEXT, 0).unwrap().unwrap();
        assert_eq!(lit, "'it''s'");
        // And it survives a round trip through the splice unchanged.
        let out = substitute_params("WHERE a = $1", &[Some(lit)]).unwrap();
        assert_eq!(out, "WHERE a = 'it''s'");
    }

    #[test]
    fn binary_parameters_decode_in_every_width_psycopg_sends() {
        // These are the exact encodings read off a psycopg3 wire transcript:
        // a small int arrives as int2, a float as float8, a bool as one byte.
        assert_eq!(decode_param(Some(&[0x00, 0x2a]), OID_INT2, 1).unwrap().unwrap(), "42");
        assert_eq!(decode_param(Some(&[0, 0, 0, 7]), OID_INT4, 1).unwrap().unwrap(), "7");
        assert_eq!(
            decode_param(Some(&[0, 0, 0, 0, 0, 0, 0, 9]), OID_INT8, 1).unwrap().unwrap(), "9");
        assert_eq!(
            decode_param(Some(&0x400c_0000_0000_0000u64.to_be_bytes()), OID_FLOAT8, 1)
                .unwrap().unwrap(), "3.5");
        assert_eq!(decode_param(Some(&[1]), OID_BOOL, 1).unwrap().unwrap(), "TRUE");
        assert_eq!(decode_param(Some(&[0]), OID_BOOL, 1).unwrap().unwrap(), "FALSE");
    }

    #[test]
    fn a_negative_binary_integer_keeps_its_sign() {
        assert_eq!(decode_param(Some(&(-5i32).to_be_bytes()), OID_INT4, 1).unwrap().unwrap(), "-5");
        assert_eq!(decode_param(Some(&(-5i16).to_be_bytes()), OID_INT2, 1).unwrap().unwrap(), "-5");
    }

    #[test]
    fn a_binary_parameter_of_the_wrong_width_is_refused() {
        // Truncating or zero-extending would produce a plausible wrong number,
        // which is the failure mode worth engineering against.
        let e = decode_param(Some(&[0x2a]), OID_INT4, 1).unwrap_err();
        assert!(e.contains("4 bytes"), "{}", e);
    }

    #[test]
    fn an_unspecified_text_parameter_is_treated_as_a_string() {
        // psycopg3 declares OID 0 only for `str`; every number it sends carries
        // a real numeric OID. So quoting here is grounded, not a guess.
        assert_eq!(decode_param(Some(b"hello"), 0, 0).unwrap().unwrap(), "'hello'");
    }

    #[test]
    fn a_null_parameter_decodes_to_none_in_every_format() {
        assert_eq!(decode_param(None, OID_TEXT, 0).unwrap(), None);
        assert_eq!(decode_param(None, OID_INT8, 1).unwrap(), None);
    }

    #[test]
    fn an_unsupported_binary_type_says_so_by_name() {
        let e = decode_param(Some(&[0u8; 8]), 1114, 1).unwrap_err();
        assert!(e.contains("1114"), "{}", e);
        assert!(e.contains("text"), "the error should point at the way out: {}", e);
    }

    #[test]
    fn a_text_number_that_is_not_a_number_gets_quoted() {
        // Splicing it in bare would emit a naked identifier into the NQL text
        // and fail somewhere far away from the cause.
        assert_eq!(decode_param(Some(b"oops"), OID_INT8, 0).unwrap().unwrap(), "'oops'");
    }

    #[test]
    fn a_client_declared_type_is_believed_over_inference() {
        // The client is about to encode its argument that way; overriding it
        // would break the decode.
        let oids = infer_param_oids("SELECT a FROM t WHERE b = $1 AND c = $2", &[OID_INT4, 0], None);
        assert_eq!(oids, vec![OID_INT4, OID_TEXT]);
    }

    #[test]
    fn parameter_arity_is_taken_from_the_sql_when_the_client_declares_none() {
        // asyncpg declares nothing and then refuses the call if the count that
        // comes back is wrong, so this is the load-bearing path for it.
        let oids = infer_param_oids("SELECT a FROM t WHERE b = $1 AND c = $2", &[], None);
        assert_eq!(oids.len(), 2);
    }

    #[test]
    fn the_field_behind_each_placeholder_is_identified() {
        assert_eq!(
            param_fields("SELECT a FROM t WHERE qty > $1 AND status = $2", 2),
            vec![Some("qty".to_string()), Some("status".to_string())]);
    }

    #[test]
    fn word_operators_do_not_hide_the_field() {
        assert_eq!(param_fields("SELECT a FROM t WHERE name LIKE $1", 1),
                   vec![Some("name".to_string())]);
        assert_eq!(param_fields("SELECT a FROM t WHERE qty BETWEEN $1 AND $2", 2),
                   vec![Some("qty".to_string()), Some("qty".to_string())]);
        assert_eq!(param_fields("SELECT a FROM t WHERE region IN ($1, $2)", 2),
                   vec![Some("region".to_string()), Some("region".to_string())]);
    }

    #[test]
    fn a_clause_position_types_from_the_grammar_not_from_a_column() {
        // `AS OF SYSTEM TIME $1` has no column beside it — the token to its
        // left is the word TIME. Typing it text made asyncpg refuse to send
        // the sequence number at all.
        assert_eq!(
            infer_param_oids("SELECT a FROM t AS OF SYSTEM TIME $1 WHERE b = $2", &[], None),
            vec![OID_INT8, OID_TEXT]);
        assert_eq!(infer_param_oids("SELECT a FROM t AS OF $1", &[], None), vec![OID_INT8]);
        // VALID AS OF also ends with "AS OF", but its argument is a DATE
        // STRING. Checking the longer clause first is load-bearing.
        assert_eq!(
            infer_param_oids("SELECT a FROM t VALID AS OF $1", &[], None), vec![OID_TEXT]);
        assert_eq!(
            infer_param_oids("SELECT a FROM t LIMIT $1 OFFSET $2", &[], None),
            vec![OID_INT8, OID_INT8]);
    }

    #[test]
    fn an_aggregate_column_types_from_what_the_aggregate_means() {
        // No document holds a field called `count`, so sampling stored data
        // finds nothing and falls back to text — which hands a binary client
        // the string "2" for COUNT(*).
        assert_eq!(aggregate_oid("count", None, "t"), Some(OID_INT8));
        assert_eq!(aggregate_oid("avg_fee", None, "t"), Some(OID_FLOAT8),
                   "an average is fractional even over integers");
        // SUM/MIN/MAX inherit the field's type; with no database to sample,
        // that resolves to text, and `_seq` is known from the engine contract.
        assert_eq!(aggregate_oid("max__seq", None, "t"), Some(OID_INT8));
        assert_eq!(aggregate_oid("total", None, "t"), None, "not an aggregate");
    }

    #[test]
    fn the_parse_probe_uses_a_literal_that_every_clause_accepts() {
        // Stubbing with NULL was the obvious choice and the wrong one: clauses
        // that validate their argument rejected it, so `AS OF SYSTEM TIME $1`
        // failed at Parse before a real sequence was ever bound.
        let probe = probe_sql("SELECT a FROM t AS OF SYSTEM TIME $1 WHERE b = $2", 2);
        assert!(!probe.contains("NULL"), "{}", probe);
        assert!(translate(&probe).is_ok(), "the probe must parse: {}", probe);
    }

    #[test]
    fn a_column_with_mixed_types_across_documents_is_advertised_as_text() {
        // Taking the first non-null value's type told the client `int8` and
        // then sent it "n/a" — which fails to parse client-side, and on the
        // binary path cannot be encoded at all.
        let rows = vec![json!({"x": 3}), json!({"x": "n/a"})];
        assert_eq!(oid_for(&rows, "x"), OID_TEXT);
        // Integers and floats in one column widen rather than conflict.
        let rows = vec![json!({"x": 3}), json!({"x": 1.5})];
        assert_eq!(oid_for(&rows, "x"), OID_FLOAT8);
        // A leading null must not decide the type.
        let rows = vec![json!({"x": Value::Null}), json!({"x": 7})];
        assert_eq!(oid_for(&rows, "x"), OID_INT8);
    }

    #[test]
    fn binary_output_encodes_each_advertised_type() {
        assert_eq!(cell_binary(Some(&json!(true)), OID_BOOL).unwrap().unwrap(), vec![1]);
        assert_eq!(cell_binary(Some(&json!(42)), OID_INT8).unwrap().unwrap(),
                   42i64.to_be_bytes().to_vec());
        assert_eq!(cell_binary(Some(&json!(3.5)), OID_FLOAT8).unwrap().unwrap(),
                   3.5f64.to_be_bytes().to_vec());
        // For the text family, binary and text are the same bytes.
        assert_eq!(cell_binary(Some(&json!("hi")), OID_TEXT).unwrap().unwrap(), b"hi".to_vec());
        assert_eq!(cell_binary(Some(&Value::Null), OID_INT8).unwrap(), None);
        // A boolean renders as `t`/`f` in text but one byte in binary.
        assert_eq!(cell(Some(&json!(true))).unwrap(), "t");
    }

    #[test]
    fn a_value_that_does_not_fit_its_advertised_binary_type_is_refused() {
        // Advertised types come from a bounded sample, so a field that only
        // turns heterogeneous outside it lands here. Sending a zero, or the
        // text bytes under a binary header, would corrupt the value in a way
        // the client cannot detect — so it is an error instead.
        let e = cell_binary(Some(&json!("nope")), OID_INT8).unwrap_err();
        assert!(e.contains("a string"), "{}", e);
        assert!(e.contains("more than one type"), "the error should explain WHY: {}", e);
    }

    #[test]
    fn a_row_description_carries_the_requested_format_per_column() {
        let cols = [Col::same("a"), Col::same("b")];
        let m = row_description_fmt(&cols, &[OID_INT8, OID_TEXT], &[1, 0]);
        assert_eq!(m[0], b'T');
        // The trailing i16 of each field entry is its format code.
        assert_eq!(m[m.len() - 1], 0, "the last column was requested as text");
    }

    #[test]
    fn a_qualified_column_resolves_to_its_bare_name() {
        assert_eq!(param_fields("SELECT a FROM t WHERE t.qty = $1", 1),
                   vec![Some("qty".to_string())]);
    }

    #[test]
    fn insert_placeholders_map_positionally_to_the_column_list() {
        assert_eq!(
            param_fields("INSERT INTO t (_id, qty, status) VALUES ($1, $2, $3)", 3),
            vec![Some("_id".to_string()), Some("qty".to_string()), Some("status".to_string())]);
    }

    #[test]
    fn a_set_clause_placeholder_finds_its_column() {
        assert_eq!(param_fields("UPDATE t SET status = $1 WHERE _id = $2", 2),
                   vec![Some("status".to_string()), Some("_id".to_string())]);
    }

    #[test]
    fn the_target_collection_is_found_for_every_statement_kind() {
        assert_eq!(stmt_collection("SELECT a FROM inv WHERE b = $1"), "inv");
        assert_eq!(stmt_collection("UPDATE inv SET a = $1"), "inv");
        assert_eq!(stmt_collection("DELETE FROM inv WHERE a = $1"), "inv");
        assert_eq!(stmt_collection("INSERT INTO inv (a) VALUES ($1)"), "inv");
        // Clients qualify as schema.table; NEDB has one namespace.
        assert_eq!(stmt_collection("SELECT a FROM public.inv"), "inv");
        assert_eq!(stmt_collection("INSERT INTO inv(a) VALUES ($1)"), "inv");
    }

    #[test]
    fn engine_metadata_fields_type_without_touching_storage() {
        assert_eq!(infer_field_oid(None, "t", "_seq"), OID_INT8);
        assert_eq!(infer_field_oid(None, "t", "_id"), OID_TEXT);
    }

    #[test]
    fn the_protocol_acknowledgements_are_single_empty_messages() {
        // Each is a tag plus a 4-byte length of exactly 4.
        for (m, tag) in [
            (parse_complete(), b'1'), (bind_complete(), b'2'),
            (close_complete(), b'3'), (no_data(), b'n'), (portal_suspended(), b's'),
        ] {
            assert_eq!(m.len(), 5, "{:?}", tag as char);
            assert_eq!(m[0], tag);
            assert_eq!(i32::from_be_bytes([m[1], m[2], m[3], m[4]]), 4);
        }
    }

    #[test]
    fn parameter_description_reports_its_arity_and_types() {
        let m = parameter_description(&[OID_TEXT, OID_INT8]);
        assert_eq!(m[0], b't');
        assert_eq!(i16::from_be_bytes([m[5], m[6]]), 2);
        assert_eq!(i32::from_be_bytes([m[7], m[8], m[9], m[10]]), OID_TEXT);
        assert_eq!(i32::from_be_bytes([m[11], m[12], m[13], m[14]]), OID_INT8);
    }

    #[test]
    fn a_cstring_is_taken_without_its_terminator() {
        let body = b"one\0two\0".to_vec();
        let mut at = 0usize;
        assert_eq!(take_cstr(&body, &mut at), "one");
        assert_eq!(take_cstr(&body, &mut at), "two");
        assert_eq!(at, body.len());
    }

    #[test]
    fn truncated_integers_are_reported_rather_than_read_past_the_end() {
        let body = vec![0u8, 1];
        let mut at = 0usize;
        assert!(take_i32(&body, &mut at).is_err());
        let mut at = 0usize;
        assert!(take_i16(&body, &mut at).is_ok());
    }

    #[test]
    fn a_binary_result_format_request_is_refused_rather_than_faked() {
        // Sending text under a binary header corrupts every value silently,
        // which is far worse than an error naming the limitation.
        let out = encode_rows(&[], &[Col::same("a")]);
        let desc_format = &out[out.len() - 2..];
        assert_eq!(i16::from_be_bytes([desc_format[0], desc_format[1]]), 0,
                   "every column is advertised as text format");
    }

    #[test]
    fn a_float_parameter_does_not_render_as_rust_infinity() {
        assert_eq!(fmt_float(f64::INFINITY), "'Infinity'");
        assert_eq!(fmt_float(f64::NEG_INFINITY), "'-Infinity'");
        assert_eq!(fmt_float(f64::NAN), "'NaN'");
        assert_eq!(fmt_float(3.0), "3", "a whole float should not gain a .0 tail");
        assert_eq!(fmt_float(3.5), "3.5");
    }
}
