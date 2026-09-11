// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

//! NQL (NEDB Query Language) parser and executor for v2 DAG storage.
//!
//! Grammar:
//!   FROM coll
//!     [AS OF seq]
//!     [VALID AS OF "date"]
//!     [WHERE <predicate>]
//!     [SEARCH "text"]
//!     [ORDER BY field [ASC|DESC]]
//!     [LIMIT n]
//!     [GROUP BY field COUNT|SUM|AVG|MIN|MAX]
//!     [TRACE caused_by [REVERSE]]
//!
//! where <predicate> is a full boolean expression:
//!
//!   <predicate> := <or>
//!   <or>        := <and> [OR <and>]*
//!   <and>       := <not> [AND <not>]*
//!   <not>       := [NOT] <primary>
//!   <primary>   := "(" <predicate> ")" | <comparison>
//!   <comparison>:= field ( = | != | > | < | >= | <= ) value
//!                | field [NOT] IN "(" value [, value]* ")"
//!                | field [NOT] BETWEEN value AND value
//!                | field [NOT] LIKE|ILIKE "pattern"
//!                | field IS [NOT] NULL
//!
//! Until 3.3.0 the predicate surface was six operators wide (= != > < >= <=)
//! joined by an implicit AND, with no grouping, no negation and no set/range/
//! pattern tests. Every one of those is table stakes in SQL, and their absence
//! is the single most visible gap against a SQL engine: the queries people
//! actually type (`status IN ('open','pending')`, `height BETWEEN 100 AND 200`,
//! `name LIKE 'ac%'`) had to be decomposed by hand or filtered client-side.
//!
//! NOTE ON STRICTNESS. The old parser ended its clause loop with
//! `_ => { self.advance(); }` — "skip unrecognised". That is the same defect
//! class as a swallowed write error: a query containing a clause the engine
//! does not implement did not fail, it silently returned the results of a
//! DIFFERENT query. `FROM x WHERE a = 1 OFFSET 5` dropped both tokens and
//! answered without the offset; a misspelled `ORDRE BY height` answered
//! unsorted. Unknown tokens are now a parse error. This is deliberately
//! breaking for queries that were already being silently misread — there was
//! no correct behaviour to preserve.

use std::collections::HashMap;
use anyhow::{bail, Result};
use serde_json::{json, Value};

use crate::db::Db;
use crate::index::OrderedValue;
use crate::store::Node;

// ── Token types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// A reserved word: (UPPERCASED for matching, RAW as the user spelled it).
    ///
    /// The raw spelling has to survive. Field positions accept a keyword as a
    /// field name -- a document may legitimately have a field called `count`,
    /// `min`, `value` or `status` -- and using the uppercased form there looks
    /// up a key that does not exist. That made `HAVING count > 1` and
    /// `ORDER BY count DESC` silently match nothing, because they searched the
    /// row for "COUNT".
    Kw(String, String),
    Ident(String),  // field name or collection name (lowercase/mixed)
    Str(String),    // "quoted string"
    Num(f64),       // numeric literal
    Op(String),     // = != > < >= <=
    Punct(char),    // ( ) ,
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

        // Quoted string.
        //
        // A backslash escapes a following double-quote (\" -> a literal " that
        // does NOT end the string). This is purely additive: a literal quote
        // was previously impossible to express — the first " always closed the
        // string — so no existing query can rely on the old meaning of \" and
        // nothing breaks. Every OTHER backslash stays literal, so raw-backslash
        // values (e.g. a Windows path) keep matching exactly as before; a
        // regression test pins that. (A fully C-style scheme where \\ -> \
        // would instead change the meaning of every existing backslash query,
        // so it is deliberately NOT done here.)
        if c == '"' {
            self.pos += 1;
            let mut s = String::new();
            while let Some(ch) = self.peek_char() {
                if ch == '"' {
                    break;
                }
                if ch == '\\' {
                    // Look at the next char: only \" collapses to ". A trailing
                    // backslash (nothing after it) or \x for any other x stays
                    // a literal backslash, preserving prior behavior.
                    let next = self.src[self.pos + 1..].chars().next();
                    if next == Some('"') {
                        s.push('"');
                        self.pos += 1 + 1; // consume the backslash and the quote
                        continue;
                    }
                }
                s.push(ch);
                self.pos += ch.len_utf8();
            }
            if self.peek_char() == Some('"') {
                self.pos += 1;
            }
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

        // Punctuation: grouping for boolean predicates and IN-list separators.
        // These previously fell through to "skip unknown char", so `(`, `)` and
        // `,` were invisible to the parser — which is why the grammar could not
        // express either grouping or a value list.
        if matches!(c, '(' | ')' | ',') {
            self.pos += 1;
            return Tok::Punct(c);
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
            let keywords = ["FROM","AS","OF","VALID","WHERE","AND","OR","ORDER","BY",
                            "ASC","DESC","LIMIT","OFFSET","GROUP","HAVING",
                            "COUNT","SUM","AVG","MIN","MAX",
                            "TRACE","TRAVERSE","REVERSE","SEARCH","NOT","NULL","TRUE","FALSE",
                            "IN","BETWEEN","LIKE","ILIKE","IS"];
            if keywords.contains(&upper.as_str()) {
                return Tok::Kw(upper, word.to_string());
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

/// A boolean predicate tree.
///
/// The old representation was `Vec<WhereClause>` evaluated with `.all()`, which
/// can only ever express a conjunction of comparisons. A tree is required for
/// OR, for NOT, and for parenthesised grouping — `WHERE (a = 1 OR b = 2) AND
/// c != 3` has no encoding as a flat list.
#[derive(Debug, Clone)]
pub enum Pred {
    /// field <op> value, for op in = != > < >= <=
    Cmp { field: String, op: String, value: Value },
    /// field [NOT] IN (v1, v2, ...)
    In { field: String, values: Vec<Value>, negated: bool },
    /// field [NOT] BETWEEN low AND high — inclusive on both ends, as in SQL.
    Between { field: String, low: Value, high: Value, negated: bool },
    /// field [NOT] LIKE "pat" — SQL wildcards: % = any run, _ = any one char.
    /// `ci` is set by ILIKE (case-insensitive).
    Like { field: String, pattern: String, negated: bool, ci: bool },
    /// field IS [NOT] NULL — true when the field is JSON null OR absent.
    IsNull { field: String, negated: bool },
    And(Vec<Pred>),
    Or(Vec<Pred>),
    Not(Box<Pred>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum GroupAgg { Count, Sum, Avg, Min, Max }

impl GroupAgg {
    fn name(&self) -> &'static str {
        match self {
            GroupAgg::Count => "count", GroupAgg::Sum => "sum",
            GroupAgg::Avg   => "avg",   GroupAgg::Min => "min",
            GroupAgg::Max   => "max",
        }
    }
}

/// One `ORDER BY` key. A list of these replaces the old single
/// `Option<String>` + `bool` pair, because `ORDER BY status, fee DESC` — sort
/// by one column then break ties with another — has no encoding as a single
/// field plus a single direction.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderKey {
    pub field: String,
    pub desc:  bool,
}

/// An aggregate, grouped or ungrouped.
///
/// `group_field: None` is a whole-result aggregate — `FROM t COUNT`,
/// `FROM t SUM fee` — which returns exactly one row. That was previously
/// inexpressible: the aggregate keywords only existed after `GROUP BY`, so
/// "how many rows match this?" had to fetch every row and count client-side.
#[derive(Debug, Clone)]
pub struct Aggregate {
    pub group_field: Option<String>,
    pub agg:         GroupAgg,
    /// The field to aggregate. None for COUNT, which needs no target.
    pub agg_field:   Option<String>,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub coll:       String,
    pub as_of:      Option<u64>,
    pub valid_as_of: Option<String>,
    pub where_:     Option<Pred>,
    pub search:     Option<String>,
    pub order_by:   Vec<OrderKey>,
    pub limit:      Option<usize>,
    pub offset:     Option<usize>,
    pub aggregate:  Option<Aggregate>,
    /// `HAVING <predicate>` — filters the AGGREGATED rows, so it can test
    /// `count`, `sum_fee`, or the group key itself. Distinct from WHERE, which
    /// filters input rows before they are grouped.
    pub having:     Option<Pred>,
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
            Tok::Kw(k, _) if k == kw => Ok(()),
            other => bail!("expected keyword {}, got {:?}", kw, other),
        }
    }

    /// Parse a literal.
    ///
    /// Returns `Result` rather than defaulting to `Value::Null`: the old arm
    /// `_ => Value::Null` turned a syntax error into a comparison against null,
    /// so `WHERE height > )` quietly answered "nothing is greater than null"
    /// instead of reporting a malformed query.
    fn parse_value(&mut self) -> Result<Value> {
        Ok(match self.advance() {
            Tok::Str(s)  => Value::String(s),
            Tok::Num(n)  => json!(n),
            Tok::Kw(k, _) if k == "NULL"  => Value::Null,
            Tok::Kw(k, _) if k == "TRUE"  => Value::Bool(true),
            Tok::Kw(k, _) if k == "FALSE" => Value::Bool(false),
            Tok::Ident(s) => Value::String(s),
            other => bail!("expected a value (string, number, TRUE, FALSE or NULL), got {:?}", other),
        })
    }

    fn peek_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), Tok::Kw(k, _) if k == kw)
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.peek_kw(kw) { self.advance(); true } else { false }
    }

    fn expect_punct(&mut self, c: char) -> Result<()> {
        match self.advance() {
            Tok::Punct(p) if p == c => Ok(()),
            other => bail!("expected '{}', got {:?}", c, other),
        }
    }

    fn parse_agg_kw(&mut self) -> Result<GroupAgg> {
        Ok(match self.advance() {
            Tok::Kw(a, _) if a == "COUNT" => GroupAgg::Count,
            Tok::Kw(a, _) if a == "SUM"   => GroupAgg::Sum,
            Tok::Kw(a, _) if a == "AVG"   => GroupAgg::Avg,
            Tok::Kw(a, _) if a == "MIN"   => GroupAgg::Min,
            Tok::Kw(a, _) if a == "MAX"   => GroupAgg::Max,
            other => bail!("expected an aggregate (COUNT/SUM/AVG/MIN/MAX), got {:?}", other),
        })
    }

    fn parse_field(&mut self, ctx: &str) -> Result<String> {
        match self.advance() {
            Tok::Ident(s) | Tok::Kw(_, s) => Ok(s),
            other => bail!("{}: expected field name, got {:?}", ctx, other),
        }
    }

    // ── Predicate grammar: OR binds loosest, then AND, then NOT ──────────────

    fn parse_pred(&mut self) -> Result<Pred> { self.parse_or() }

    fn parse_or(&mut self) -> Result<Pred> {
        let mut terms = vec![self.parse_and()?];
        while self.eat_kw("OR") {
            terms.push(self.parse_and()?);
        }
        Ok(if terms.len() == 1 { terms.pop().unwrap() } else { Pred::Or(terms) })
    }

    fn parse_and(&mut self) -> Result<Pred> {
        let mut terms = vec![self.parse_not()?];
        while self.peek_kw("AND") {
            // `BETWEEN low AND high` owns its AND — it is consumed inside
            // parse_comparison, so any AND reaching here is a real conjunction.
            self.advance();
            terms.push(self.parse_not()?);
        }
        Ok(if terms.len() == 1 { terms.pop().unwrap() } else { Pred::And(terms) })
    }

    fn parse_not(&mut self) -> Result<Pred> {
        if self.eat_kw("NOT") {
            return Ok(Pred::Not(Box::new(self.parse_not()?)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Pred> {
        if matches!(self.peek(), Tok::Punct('(')) {
            self.advance();
            let inner = self.parse_pred()?;
            self.expect_punct(')')?;
            return Ok(inner);
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Pred> {
        let field = self.parse_field("WHERE")?;

        // field IS [NOT] NULL
        if self.eat_kw("IS") {
            let negated = self.eat_kw("NOT");
            if !self.eat_kw("NULL") {
                bail!("WHERE: expected NULL after IS{}", if negated { " NOT" } else { "" });
            }
            return Ok(Pred::IsNull { field, negated });
        }

        // A leading NOT applies to the operator that follows: IN / BETWEEN / LIKE.
        let negated = self.eat_kw("NOT");

        if self.eat_kw("IN") {
            self.expect_punct('(')?;
            let mut values = vec![];
            loop {
                values.push(self.parse_value()?);
                if matches!(self.peek(), Tok::Punct(',')) { self.advance(); continue; }
                break;
            }
            self.expect_punct(')')?;
            if values.is_empty() {
                bail!("WHERE: IN () needs at least one value");
            }
            return Ok(Pred::In { field, values, negated });
        }

        if self.eat_kw("BETWEEN") {
            let low = self.parse_value()?;
            if !self.eat_kw("AND") {
                bail!("WHERE: BETWEEN expects AND between its bounds");
            }
            let high = self.parse_value()?;
            return Ok(Pred::Between { field, low, high, negated });
        }

        let ci = self.peek_kw("ILIKE");
        if ci || self.peek_kw("LIKE") {
            self.advance();
            let pattern = match self.advance() {
                Tok::Str(s) => s,
                Tok::Ident(s) => s,
                other => bail!("WHERE: LIKE expects a pattern string, got {:?}", other),
            };
            return Ok(Pred::Like { field, pattern, negated, ci });
        }

        if negated {
            bail!("WHERE: NOT must be followed by IN, BETWEEN, LIKE or ILIKE \
                   (use `NOT (field = value)` or `field != value` to negate a comparison)");
        }

        let op = match self.advance() {
            Tok::Op(s) => s,
            other => bail!("WHERE: expected operator, got {:?}", other),
        };
        let value = self.parse_value()?;
        Ok(Pred::Cmp { field, op, value })
    }

    fn parse(&mut self) -> Result<Query> {
        self.expect_kw("FROM")?;
        let coll = match self.advance() {
            Tok::Ident(s) | Tok::Kw(_, s) => s,
            other => bail!("expected collection name, got {:?}", other),
        };

        let mut q = Query {
            coll, as_of: None, valid_as_of: None,
            where_: None, search: None,
            order_by: vec![],
            limit: None, offset: None,
            aggregate: None, having: None,
            trace: None, trace_rev: false,
            traverse: None,
        };

        loop {
            match self.peek() {
                Tok::Eof => break,

                Tok::Kw(k, _) if k == "AS" => {
                    self.advance();
                    self.expect_kw("OF")?;
                    match self.advance() {
                        Tok::Num(n) => q.as_of = Some(n as u64),
                        other => bail!("AS OF expects sequence number, got {:?}", other),
                    }
                }

                Tok::Kw(k, _) if k == "VALID" => {
                    self.advance();
                    self.expect_kw("AS")?;
                    self.expect_kw("OF")?;
                    match self.advance() {
                        Tok::Str(s) => q.valid_as_of = Some(s),
                        other => bail!("VALID AS OF expects date string, got {:?}", other),
                    }
                }

                Tok::Kw(k, _) if k == "WHERE" => {
                    self.advance();
                    let pred = self.parse_pred()?;
                    // Repeating WHERE is a conjunction, matching the old
                    // behaviour where every clause was ANDed together.
                    q.where_ = Some(match q.where_.take() {
                        None => pred,
                        Some(prev) => Pred::And(vec![prev, pred]),
                    });
                }

                Tok::Kw(k, _) if k == "SEARCH" => {
                    self.advance();
                    match self.advance() {
                        Tok::Str(s) => q.search = Some(s),
                        other => bail!("SEARCH expects quoted string, got {:?}", other),
                    }
                }

                Tok::Kw(k, _) if k == "ORDER" => {
                    self.advance();
                    self.expect_kw("BY")?;
                    // Comma-separated sort keys, each with its own direction:
                    // ORDER BY status, fee DESC
                    loop {
                        let field = self.parse_field("ORDER BY")?;
                        // ASC is a real keyword now. It used to lex as an Ident
                        // and survive only because the clause loop silently
                        // skipped tokens it did not recognise.
                        let desc = if self.eat_kw("DESC") {
                            true
                        } else {
                            self.eat_kw("ASC");
                            false
                        };
                        q.order_by.push(OrderKey { field, desc });
                        if matches!(self.peek(), Tok::Punct(',')) { self.advance(); continue; }
                        break;
                    }
                }

                Tok::Kw(k, _) if k == "LIMIT" => {
                    self.advance();
                    match self.advance() {
                        Tok::Num(n) if n >= 0.0 => q.limit = Some(n as usize),
                        other => bail!("LIMIT expects a non-negative number, got {:?}", other),
                    }
                }

                Tok::Kw(k, _) if k == "OFFSET" => {
                    self.advance();
                    match self.advance() {
                        Tok::Num(n) if n >= 0.0 => q.offset = Some(n as usize),
                        other => bail!("OFFSET expects a non-negative number, got {:?}", other),
                    }
                }

                Tok::Kw(k, _) if k == "HAVING" => {
                    self.advance();
                    let pred = self.parse_pred()?;
                    q.having = Some(match q.having.take() {
                        None => pred,
                        Some(prev) => Pred::And(vec![prev, pred]),
                    });
                }

                // A bare aggregate with no GROUP BY: `FROM t COUNT`,
                // `FROM t SUM fee`. Returns exactly one row.
                Tok::Kw(k, _) if matches!(k.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") => {
                    let agg = self.parse_agg_kw()?;
                    let agg_field = match agg {
                        GroupAgg::Count => None,
                        _ => Some(self.parse_field("aggregate")?),
                    };
                    if q.aggregate.is_some() {
                        bail!("only one aggregate per query");
                    }
                    q.aggregate = Some(Aggregate { group_field: None, agg, agg_field });
                }

                Tok::Kw(k, _) if k == "GROUP" => {
                    self.advance();
                    self.expect_kw("BY")?;
                    let field = match self.advance() {
                        Tok::Ident(s) | Tok::Kw(_, s) => s,
                        other => bail!("GROUP BY: expected field, got {:?}", other),
                    };
                    // The aggregate is OPTIONAL, matching the Python reference
                    // (query.py): `GROUP BY field` on its own yields per-group
                    // counts. Rust previously REQUIRED the keyword, so a bare
                    // GROUP BY was a parse error here and valid there.
                    let agg = if matches!(self.peek(),
                        Tok::Kw(a, _) if matches!(a.as_str(), "COUNT"|"SUM"|"AVG"|"MIN"|"MAX"))
                    {
                        self.parse_agg_kw()?
                    } else {
                        GroupAgg::Count
                    };
                    // SUM/AVG/MIN/MAX take the field to aggregate. Without it
                    // the executor fell back to aggregating the GROUP BY field
                    // itself, so `GROUP BY cat MAX price` reported the max
                    // *cat* — and since a non-numeric value coerced to 1.0,
                    // every group answered 1. The target field was lexed and
                    // then silently dropped by the unknown-token skip.
                    let agg_field = match agg {
                        GroupAgg::Count => None,
                        _ => Some(self.parse_field("GROUP BY aggregate")?),
                    };
                    if q.aggregate.is_some() {
                        bail!("only one aggregate per query");
                    }
                    q.aggregate = Some(Aggregate {
                        group_field: Some(field), agg, agg_field,
                    });
                }

                Tok::Kw(k, _) if k == "TRACE" => {
                    self.advance();
                    let edge = match self.advance() {
                        Tok::Ident(s) | Tok::Kw(_, s) => s,
                        other => bail!("TRACE: expected edge type, got {:?}", other),
                    };
                    q.trace = Some(edge);
                    if let Tok::Kw(k, _) = self.peek() {
                        if k == "REVERSE" { self.advance(); q.trace_rev = true; }
                    }
                }

                Tok::Kw(k, _) if k == "TRAVERSE" => {
                    self.advance();
                    let rel = match self.advance() {
                        Tok::Ident(s) | Tok::Kw(_, s) => s,
                        other => bail!("TRAVERSE: expected relation name, got {:?}", other),
                    };
                    q.traverse = Some(rel);
                }

                // Unknown token. This used to be `self.advance()` — a silent
                // skip that answered a different query than the one asked.
                other => bail!(
                    "unexpected {:?} in query. Expected one of: AS OF, VALID AS OF, \
                     WHERE, SEARCH, ORDER BY, LIMIT, OFFSET, GROUP BY, HAVING, \
                     COUNT, SUM, AVG, MIN, MAX, TRACE, TRAVERSE",
                    other
                ),
            }
        }

        Ok(q)
    }
}

// ── Executor ──────────────────────────────────────────────────────────────────

/// Resolve a field name against a node, including the `_`-prefixed metadata
/// fields that live on the node rather than in its data payload.
fn field_value(node: &Node, field: &str) -> Value {
    match field {
        "_id"   => Value::String(node.id.clone()),
        "_coll" => Value::String(node.coll.clone()),
        "_hash" => Value::String(node.hash.clone()),
        "_seq"  => json!(node.seq),
        _ => node.data.get(field).cloned().unwrap_or(Value::Null),
    }
}

fn cmp_op(a: &Value, op: &str, b: &Value) -> bool {
    // An ORDERING comparison against a null/missing field is never true.
    //
    // OrderedValue sorts Null below every number, so `<` and `<=` used to
    // report that a document with NO `fee` field at all satisfied
    // `WHERE fee < 5`. `>` and `>=` excluded it — the asymmetry was the tell.
    //
    // The Python reference has always excluded it (query.py: `if a is None:
    // return False`, placed deliberately AFTER the = / != arms), so this was
    // a live divergence between the two engines as well as a wrong answer:
    // asking for cheap jobs should not return jobs with no price.
    //
    // `=` and `!=` keep operating on null, exactly as Python does, so
    // `WHERE x != 5` still matches a row where x is absent and `WHERE x = NULL`
    // still works. `BETWEEN` is built from `>=` and `<=` and so inherits this.
    //
    // This also makes the scan path and the sorted-index path agree. A
    // document whose field is absent is not in that field's index, so an index
    // range scan could never have returned it — without this fix the two paths
    // answered the same query differently depending on whether an index
    // happened to exist.
    if matches!(op, "<" | "<=" | ">" | ">=") && a.is_null() {
        return false;
    }
    let a = OrderedValue::from(a);
    let b = OrderedValue::from(b);
    match op {
        "="  => a == b,
        "!=" => a != b,
        ">"  => a >  b,
        "<"  => a <  b,
        ">=" => a >= b,
        "<=" => a <= b,
        _    => false,
    }
}

/// Render a JSON scalar for text matching. Strings pass through unquoted so a
/// LIKE pattern is compared against the value a user sees, not against its
/// JSON encoding (`"abc"` with the quotes included).
fn as_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// SQL LIKE matching: `%` matches any run of characters (including empty),
/// `_` matches exactly one. Implemented as an iterative two-pointer scan with
/// backtracking to the last `%`, which is linear in practice and needs no
/// regex dependency. Operates on chars, so multi-byte values match correctly.
fn like_match(value: &str, pattern: &str, ci: bool) -> bool {
    let (v, p): (Vec<char>, Vec<char>) = if ci {
        (value.to_lowercase().chars().collect(), pattern.to_lowercase().chars().collect())
    } else {
        (value.chars().collect(), pattern.chars().collect())
    };

    let mut vi = 0usize;
    let mut pi = 0usize;
    // Position to resume from if the current `%` expansion turns out too short.
    let mut star: Option<(usize, usize)> = None;

    while vi < v.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == v[vi]) {
            vi += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star = Some((pi, vi));
            pi += 1;
        } else if let Some((sp, sv)) = star {
            // Backtrack: let the `%` swallow one more character.
            pi = sp + 1;
            vi = sv + 1;
            star = Some((sp, vi));
        } else {
            return false;
        }
    }
    // Trailing `%`s can still match the empty remainder.
    while pi < p.len() && p[pi] == '%' { pi += 1; }
    pi == p.len()
}

/// Evaluate a predicate against anything that can resolve a field name.
///
/// Generic over the row source so ONE implementation serves both `WHERE`
/// (over stored nodes) and `HAVING` (over aggregated rows, which are plain
/// JSON objects with no node behind them). Two copies would be two chances for
/// the operators to drift apart.
fn eval_pred_with(get: &dyn Fn(&str) -> Value, pred: &Pred) -> bool {
    match pred {
        Pred::Cmp { field, op, value } => cmp_op(&get(field), op, value),

        Pred::In { field, values, negated } => {
            let fv = get(field);
            let hit = values.iter().any(|v| cmp_op(&fv, "=", v));
            hit != *negated
        }

        Pred::Between { field, low, high, negated } => {
            let fv = get(field);
            // Inclusive on both ends, as in SQL.
            let hit = cmp_op(&fv, ">=", low) && cmp_op(&fv, "<=", high);
            hit != *negated
        }

        Pred::Like { field, pattern, negated, ci } => {
            let fv = get(field);
            // A missing/null field matches no pattern, and NOT LIKE on a null
            // field stays false — mirroring SQL's three-valued logic, where a
            // predicate over NULL is never true in either polarity.
            if fv.is_null() { return false; }
            let hit = like_match(&as_text(&fv), pattern, *ci);
            hit != *negated
        }

        Pred::IsNull { field, negated } => {
            // Absent and explicitly-null are both NULL here: a document store
            // has no schema, so "the field was never written" and "the field
            // holds null" are the same observable state.
            get(field).is_null() != *negated
        }

        Pred::And(terms) => terms.iter().all(|t| eval_pred_with(get, t)),
        Pred::Or(terms)  => terms.iter().any(|t| eval_pred_with(get, t)),
        Pred::Not(inner) => !eval_pred_with(get, inner),
    }
}

fn eval_pred(node: &Node, pred: &Pred) -> bool {
    eval_pred_with(&|f| field_value(node, f), pred)
}

/// `HAVING` evaluation, over an aggregated row.
fn eval_pred_json(obj: &Value, pred: &Pred) -> bool {
    eval_pred_with(&|f| obj.get(f).cloned().unwrap_or(Value::Null), pred)
}

/// Sort by a list of keys, each with its own direction. Earlier keys dominate;
/// later ones break ties.
fn sort_by_keys<T>(rows: &mut [T], keys: &[OrderKey], get: impl Fn(&T, &str) -> Value) {
    rows.sort_by(|a, b| {
        for k in keys {
            let av = OrderedValue::from(&get(a, &k.field));
            let bv = OrderedValue::from(&get(b, &k.field));
            let ord = if k.desc { bv.cmp(&av) } else { av.cmp(&bv) };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// Apply OFFSET then LIMIT, in that order.
///
/// SQL semantics: OFFSET skips rows of the RESULT, LIMIT caps what remains.
/// An offset past the end yields an empty page rather than an error.
fn paginate<T>(rows: Vec<T>, offset: Option<usize>, limit: Option<usize>) -> Vec<T> {
    let mut it = rows;
    if let Some(off) = offset {
        if off >= it.len() {
            return vec![];
        }
        it.drain(..off);
    }
    if let Some(n) = limit {
        it.truncate(n);
    }
    it
}

/// Collapse rows into aggregate rows.
///
/// With `group_field: Some(f)` this yields one row per distinct value of `f`;
/// with `None` it yields exactly one row aggregating the whole result set.
///
/// `count` is the group size, while the aggregate considers ONLY rows whose
/// target field is numeric. That split matters and matches the Python
/// reference, which computes `count` from the group and the aggregate from
/// `[d[af] for d in gdocs if isinstance(d[af], (int, float))]`: a group of 5
/// rows where 2 carry a numeric `price` reports `count: 5` and averages over
/// 2. Folding non-numeric values in as 1.0 — the behaviour before 3.3.0 —
/// silently corrupted every SUM and AVG.
fn aggregate_rows(rows: &[Node], spec: &Aggregate) -> Vec<Value> {
    // `ints` tracks whether EVERY contributing value was an integer.
    //
    // Aggregating exclusively in f64 was both a type divergence from the
    // Python reference (which returns `66`, not `66.0`, for a sum of integers)
    // and a precision bug: f64 cannot represent integers above 2^53 exactly,
    // so a SUM over satoshi amounts or block heights silently rounded. SUM /
    // MIN / MAX now stay in i64 when the inputs are integral. AVG is always
    // fractional — Python's `sum(nums) / len(nums)` is true division — so it
    // stays f64 in both engines.
    struct Group { count: usize, nums: Vec<f64>, ints: Vec<i64>, all_int: bool }

    // First-seen order, so results are stable run to run. HashMap iteration
    // order previously made the grouped output nondeterministic.
    let mut order: Vec<String> = vec![];
    let mut groups: HashMap<String, Group> = HashMap::new();

    // The ungrouped case is one group under a fixed key, so a single code path
    // serves both and they cannot disagree about the aggregate itself.
    const WHOLE: &str = "";

    for node in rows {
        // Resolved through `field_value`, not `node.data`, so the `_`-prefixed
        // metadata fields work here too. Reading the payload directly made
        // `SELECT MAX(_seq)` return NULL — with a 200 and a plausible-looking
        // single-row answer — even though `SELECT _seq` listed the values and
        // `WHERE _seq > 5` filtered on them. "What is the newest sequence?" is
        // the question replication and time travel are built on, so a silent
        // null there was the worst shape of wrong.
        let key = match spec.group_field {
            None => WHOLE.to_string(),
            Some(ref gf) => match field_value(node, gf) {
                Value::Null => "null".to_string(),
                v => as_text(&v),
            },
        };
        let entry = groups.entry(key.clone()).or_insert_with(|| {
            order.push(key.clone());
            Group { count: 0, nums: vec![], ints: vec![], all_int: true }
        });
        entry.count += 1;
        if let Some(ref af) = spec.agg_field {
            // A JSON bool is not a number here, matching Python's
            // `isinstance(x, (int, float)) and not isinstance(x, bool)`.
            if let Value::Number(n) = field_value(node, af) {
                if let Some(i) = n.as_i64() {
                    entry.ints.push(i);
                    entry.nums.push(i as f64);
                } else if let Some(f) = n.as_f64() {
                    entry.all_int = false;
                    entry.nums.push(f);
                }
            }
        }
    }

    // An ungrouped aggregate over ZERO rows still returns one row — COUNT of
    // an empty set is 0, not "no answer". A grouped aggregate over zero rows
    // correctly returns no groups.
    if spec.group_field.is_none() && order.is_empty() {
        order.push(WHOLE.to_string());
        groups.insert(WHOLE.to_string(),
                      Group { count: 0, nums: vec![], ints: vec![], all_int: true });
    }

    order.into_iter().map(|k| {
        let g = &groups[&k];
        let mut obj = serde_json::Map::new();
        if let Some(ref gf) = spec.group_field {
            obj.insert(gf.clone(), Value::String(k.clone()));
        }
        obj.insert("count".to_string(), json!(g.count));

        // Empty aggregate input yields null, not 0 and not +/-infinity — the
        // old fold seeded MIN with f64::INFINITY, which serialises to null
        // anyway but would report INFINITY through any non-JSON path.
        let int_path = g.all_int && !g.ints.is_empty();
        let agg_val: Value = match spec.agg {
            GroupAgg::Count => json!(g.count),
            _ if g.nums.is_empty() => Value::Null,
            // checked_add: an i64 overflow falls back to f64 rather than
            // panicking in release or wrapping to a negative sum.
            GroupAgg::Sum if int_path => {
                match g.ints.iter().try_fold(0i64, |a, &b| a.checked_add(b)) {
                    Some(t) => json!(t),
                    None => json!(g.nums.iter().sum::<f64>()),
                }
            }
            GroupAgg::Min if int_path => json!(g.ints.iter().min().copied().unwrap()),
            GroupAgg::Max if int_path => json!(g.ints.iter().max().copied().unwrap()),
            GroupAgg::Sum => json!(g.nums.iter().sum::<f64>()),
            // AVG is true division in both engines, so always fractional.
            GroupAgg::Avg => json!(g.nums.iter().sum::<f64>() / g.nums.len() as f64),
            GroupAgg::Min => json!(g.nums.iter().cloned().fold(f64::INFINITY, f64::min)),
            GroupAgg::Max => json!(g.nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max)),
        };

        // Python-parity key: sum_price / avg_score / min_price / max_price.
        if let Some(ref af) = spec.agg_field {
            obj.insert(format!("{}_{}", spec.agg.name(), af), agg_val.clone());
        }
        // `value` is retained as an alias. It was this engine's only aggregate
        // key before 3.3.0, so Studio and any existing caller still read it;
        // dropping it would be a silent breakage on a client we do not
        // control from here.
        obj.insert("value".to_string(), agg_val);
        Value::Object(obj)
    }).collect()
}

/// Find an `_id = "..."` equality usable as an O(1) index lookup.
///
/// Only descends through AND nodes. An equality sitting under an OR does not
/// constrain the result set — `WHERE _id = "a" OR height > 3` must still return
/// the height matches — so treating it as a point lookup would silently drop
/// rows. That is precisely the bug the pre-existing `where_order_limit` test
/// guards against in the ORDER BY path, one level up.
/// What an indexed field can be narrowed to, derived from the predicate.
#[derive(Debug, Clone)]
enum IndexPlan {
    /// A bounded (or half-bounded) range walk over the sorted index.
    Range {
        field: String,
        low: Option<Value>,
        high: Option<Value>,
        low_incl: bool,
        high_incl: bool,
    },
    /// A set of point lookups — `=` or `IN (...)`.
    Values { field: String, values: Vec<Value> },
}

impl IndexPlan {
    fn field(&self) -> &str {
        match self {
            IndexPlan::Range { field, .. } => field,
            IndexPlan::Values { field, .. } => field,
        }
    }
}

/// Collect every constraint an AND-reachable conjunct places on a field.
///
/// SAFETY PROPERTY that makes this whole path sound: the returned plan only
/// ever needs to describe a SUPERSET of the matching rows. The full predicate
/// is re-evaluated on whatever candidates come back, so an imprecise plan
/// costs time, never correctness. That is why it is fine to ignore constraints
/// this planner does not understand.
///
/// Only descends through `And`. A constraint under an `Or` does not restrict
/// the result set — `WHERE fee > 100 OR status = "open"` must still return the
/// status matches — so narrowing on one arm would silently drop rows. `Not` is
/// likewise never entered: a negated range is not a range.
fn collect_index_constraints(pred: &Pred, out: &mut Vec<IndexPlan>) {
    match pred {
        Pred::And(terms) => {
            for t in terms {
                collect_index_constraints(t, out);
            }
        }

        Pred::Cmp { field, op, value } => {
            // `_id` has its own O(1) path and is not in the sorted index.
            if field == "_id" {
                return;
            }
            match op.as_str() {
                "=" => out.push(IndexPlan::Values {
                    field: field.clone(),
                    values: vec![value.clone()],
                }),
                ">" | ">=" => out.push(IndexPlan::Range {
                    field: field.clone(),
                    low: Some(value.clone()),
                    high: None,
                    low_incl: op == ">=",
                    high_incl: true,
                }),
                "<" | "<=" => out.push(IndexPlan::Range {
                    field: field.clone(),
                    low: None,
                    high: Some(value.clone()),
                    low_incl: true,
                    high_incl: op == "<=",
                }),
                // `!=` matches almost everything; a range walk would be
                // slower than the scan it replaces.
                _ => {}
            }
        }

        Pred::Between { field, low, high, negated: false } => {
            out.push(IndexPlan::Range {
                field: field.clone(),
                low: Some(low.clone()),
                high: Some(high.clone()),
                low_incl: true,   // SQL BETWEEN is inclusive on both ends
                high_incl: true,
            });
        }

        Pred::In { field, values, negated: false } => {
            out.push(IndexPlan::Values {
                field: field.clone(),
                values: values.clone(),
            });
        }

        // NOT IN / NOT BETWEEN / LIKE / IS NULL cannot be served by a range
        // walk: they either match the complement of a range, or they are not
        // an ordering predicate at all. IS NULL specifically can NEVER use
        // this index — a document whose field is absent is not in the index,
        // so an index scan would return the exact opposite of the answer.
        _ => {}
    }
}

/// Merge same-field constraints and choose the most selective indexed plan.
///
/// `fee > 10 AND fee < 100` becomes ONE bounded walk rather than a half-open
/// one, and when several fields are indexed the planner asks the index how
/// many rows each range covers and takes the narrowest — rather than
/// committing to whichever field it happened to see first.
fn choose_index_plan(db: &Db, coll: &str, pred: &Pred) -> Option<IndexPlan> {
    let mut raw = vec![];
    collect_index_constraints(pred, &mut raw);
    raw.retain(|p| db.has_sorted_index(coll, p.field()));
    if raw.is_empty() {
        return None;
    }

    // Merge per field.
    let mut merged: Vec<IndexPlan> = vec![];
    for plan in raw {
        let field = plan.field().to_string();
        let existing = merged.iter().position(|m| m.field() == field);
        match (existing, plan) {
            (None, p) => merged.push(p),

            // Two ranges on the same field: intersect the bounds.
            (Some(i), IndexPlan::Range { low, high, low_incl, high_incl, .. }) => {
                if let IndexPlan::Range {
                    low: ref mut elow, high: ref mut ehigh,
                    low_incl: ref mut eli, high_incl: ref mut ehi, ..
                } = merged[i] {
                    if let Some(l) = low {
                        let tighter = match elow {
                            None => true,
                            Some(cur) => OrderedValue::from(&l) > OrderedValue::from(&*cur),
                        };
                        if tighter { *elow = Some(l); *eli = low_incl; }
                    }
                    if let Some(h) = high {
                        let tighter = match ehigh {
                            None => true,
                            Some(cur) => OrderedValue::from(&h) < OrderedValue::from(&*cur),
                        };
                        if tighter { *ehigh = Some(h); *ehi = high_incl; }
                    }
                }
                // A Range arriving where a Values plan already sits is
                // ignored: the point lookups are already at least as
                // selective, and the predicate re-runs regardless.
            }

            // An equality/IN beats a range on the same field.
            (Some(i), p @ IndexPlan::Values { .. }) => {
                if matches!(merged[i], IndexPlan::Range { .. }) {
                    merged[i] = p;
                }
            }
        }
    }

    // Pick the narrowest, measured against the index rather than guessed.
    // A Values plan costs one point lookup per arm, so its cardinality is
    // the sum of those buckets.
    let mut best: Option<(usize, IndexPlan)> = None;
    for plan in merged {
        let card = match &plan {
            IndexPlan::Range { field, low, high, low_incl, high_incl } => db
                .range_cardinality(coll, field, low.as_ref(), high.as_ref(),
                                   *low_incl, *high_incl)
                .unwrap_or(usize::MAX),
            IndexPlan::Values { field, values } => values
                .iter()
                .map(|v| db.range_cardinality(coll, field, Some(v), Some(v), true, true)
                          .unwrap_or(usize::MAX))
                .fold(0usize, |a, b| a.saturating_add(b)),
        };
        if best.as_ref().map(|(c, _)| card < *c).unwrap_or(true) {
            best = Some((card, plan));
        }
    }
    best.map(|(_, p)| p)
}

fn id_point_lookup(pred: &Pred) -> Option<String> {
    match pred {
        Pred::Cmp { field, op, value } if field == "_id" && op == "=" => {
            if let Value::String(s) = value { Some(s.clone()) } else { None }
        }
        Pred::And(terms) => terms.iter().find_map(id_point_lookup),
        _ => None,
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

/// A node as a flat query row: data fields at the top level plus the `_`-prefixed
/// metadata. Public so the HTTP single-row GET returns the SAME shape a query row
/// has — one definition, so the two surfaces cannot drift apart.
pub fn node_to_json(node: &Node) -> Value {
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
/// Parse NQL into a `Query` WITHOUT touching the database.
///
/// `execute` already does exactly this as its first step; exposing it separately
/// lets callers validate a query before deciding to run it. The natural-language
/// planner (`/v1/databases/:name/cast`) uses it to answer "is this runnable?"
/// without side effects — checking the text against the real grammar rather than
/// pattern-matching it, because the parser is the only authority on that.
pub fn parse(nql: &str) -> Result<Query> {
    let mut lexer = Lexer::new(nql);
    let toks = lexer.tokenize();
    let mut parser = Parser::new(toks);
    parser.parse()
}

pub fn execute(db: &Db, nql: &str) -> Result<Vec<Value>> {
    // One parse path, shared with the public `parse()` above — so validation and
    // execution can never disagree about what is well-formed.
    let q = parse(nql)?;

    // ── Candidate generation ──────────────────────────────────────────────────

    // Fast path: single equality filter on _id with no AS OF.
    // Skip the O(n) collection scan — go straight to the id index (O(1) file read).
    // This turns `FROM coll WHERE _id = "x" LIMIT 1` from a full-table-scan into
    // a single file read, giving orders-of-magnitude speedup for point lookups.
    let id_eq_fast_path: Option<String> = if q.as_of.is_none() && q.trace.is_none() {
        q.where_.as_ref().and_then(id_point_lookup)
    } else { None };

    let candidates: Vec<Node> = if let Some(ref target_id) = id_eq_fast_path {
        // O(1) direct id-index lookup — skip full collection scan entirely
        db.get(&q.coll, target_id).into_iter().collect()
    } else if let Some(seq_target) = q.as_of {
        // AS OF: return each doc's version at or before target seq.
        //
        // Over the live ids PLUS the deleted ones. Walking only `id_index`
        // meant a DELETED document was invisible at every sequence, including
        // sequences before the delete where it demonstrably existed — so
        // `AS OF` contradicted the promise that a delete is a tombstone rather
        // than an erasure. `get_as_of` reaches the chain through the graveyard
        // pointer, and returns nothing for a sequence at or after the
        // tombstone, where the document really is gone.
        db.list_ids_including_deleted(&q.coll).into_iter()
            .filter_map(|id| db.get_as_of(&q.coll, &id, seq_target))
            .collect()
    } else if let Some(plan) = q.where_.as_ref()
        // An indexed range or point-set scan, when a sorted index covers a
        // field the predicate constrains.
        //
        // Deliberately NOT attempted for AS OF: the sorted index holds current
        // versions only (a superseded hash is dropped on overwrite), so an
        // index scan would answer a historical query with present-day rows.
        // AS OF is handled by the branch above, which walks the id index and
        // resolves each document at the target seq.
        .filter(|_| q.as_of.is_none())
        .and_then(|p| choose_index_plan(db, &q.coll, p))
    {
        // The full predicate re-runs on these candidates below, so the plan
        // only has to be a superset — it can never make the answer wrong.
        let got = match &plan {
            IndexPlan::Range { field, low, high, low_incl, high_incl } => db.range_scan(
                &q.coll, field, low.as_ref(), high.as_ref(), *low_incl, *high_incl),
            IndexPlan::Values { field, values } => db.index_lookup(&q.coll, field, values),
        };
        match got {
            Some(nodes) => nodes,
            // The index vanished between planning and execution. Fall back
            // rather than answering from nothing.
            None => db.list(&q.coll),
        }
    } else if q.order_by.len() == 1 && q.aggregate.is_none() {
        // ORDER BY with optional sorted index — get candidates in order.
        //
        // Single key only: the sorted index is per-field, so a multi-key sort
        // cannot be served from it and falls through to the post-filter sort
        // below. Never used when aggregating either, because the sort then
        // applies to the GROUPED rows, which do not exist yet.
        //
        // Push LIMIT down into the index scan ONLY when nothing filters rows
        // after candidate generation. WHERE / SEARCH / VALID AS OF all run on
        // the candidate set below, so truncating to the top-k FIRST returns
        // incomplete results: `WHERE n_tx > 100 ORDER BY height LIMIT 10`
        // would fetch the 10 lowest blocks by height and then filter — losing
        // matches past the top-k window. The Python reference filters → sorts
        // → limits (engine.py execute()); this keeps the engines in agreement.
        let key = &q.order_by[0];
        let has_post_filters = q.where_.is_some()
            || q.search.is_some()
            || q.valid_as_of.is_some();
        let limit = if has_post_filters {
            9_999_999
        } else {
            // OFFSET is applied AFTER the sort, so the pushdown has to fetch
            // offset + limit rows and discard the prefix later. Fetching only
            // `limit` would return the first page for every page.
            match q.limit {
                Some(n) => n.saturating_add(q.offset.unwrap_or(0)),
                None => 9_999_999,
            }
        };
        if key.desc {
            db.order_by_desc(&q.coll, &key.field, limit)
        } else {
            db.order_by_asc(&q.coll, &key.field, limit)
        }
    } else if let (Some(n), true) = (q.limit, q.where_.is_none()
            && q.search.is_none() && q.trace.is_none()
            && q.traverse.is_none() && q.aggregate.is_none()
            && q.order_by.is_empty() && q.offset.is_none()
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
        .filter(|n| q.where_.as_ref().map(|p| eval_pred(n, p)).unwrap_or(true))
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

    // ── Aggregate → HAVING → ORDER BY → OFFSET → LIMIT ───────────────────────
    //
    // This is the SQL pipeline order, and getting it wrong was a live source
    // of silently-wrong answers. The old order was ORDER BY → LIMIT → GROUP BY,
    // which means:
    //
    //   `LIMIT 5 GROUP BY status COUNT` truncated the INPUT to five rows and
    //   then grouped them, so with twelve rows across three statuses the
    //   counts summed to 5 instead of 12. A confident wrong aggregate.
    //
    //   `ORDER BY count DESC GROUP BY status COUNT` sorted the raw documents
    //   on a field none of them carry (`count` only exists after grouping),
    //   so the grouped output came back in arbitrary order and the clause was
    //   silently inert.
    //
    // In SQL, LIMIT and ORDER BY apply to the RESULT. They now do here.

    if let Some(ref spec) = q.aggregate {
        let mut out = aggregate_rows(&rows, spec);

        // HAVING filters the aggregated rows, so it can test `count`,
        // `sum_fee` or the group key — none of which exist before this point.
        if let Some(ref pred) = q.having {
            out.retain(|row| eval_pred_json(row, pred));
        }

        if !q.order_by.is_empty() {
            sort_by_keys(&mut out, &q.order_by,
                         |row, f| row.get(f).cloned().unwrap_or(Value::Null));
        } else if let Some(ref gf) = spec.group_field {
            // Deterministic default: groups sorted by key.
            //
            // NOT first-seen order. The Python reference draws candidates from
            // a `set`, so its input row order is arbitrary; a first-seen
            // ordering would differ between the two engines even though both
            // are internally consistent. Sorting by the key gives one answer
            // they can agree on, which the cross-engine parity suite pins.
            let gf = gf.clone();
            out.sort_by(|a, b| {
                as_text(&a.get(&gf).cloned().unwrap_or(Value::Null))
                    .cmp(&as_text(&b.get(&gf).cloned().unwrap_or(Value::Null)))
            });
        }

        return Ok(paginate(out, q.offset, q.limit));
    }

    if q.having.is_some() {
        bail!("HAVING requires an aggregate — add GROUP BY <field>, or use WHERE \
               to filter individual rows");
    }

    // ── ORDER BY (post-filter sort if no sorted index was used) ──────────────

    if !q.order_by.is_empty() {
        // The single-key sorted-index path above already returned candidates
        // in order, but only when nothing filtered them afterwards. Re-sort
        // whenever a filter ran, or whenever the sort has more than one key.
        let index_path_held = q.order_by.len() == 1
            && q.as_of.is_none()
            && q.where_.is_none()
            && q.search.is_none()
            && q.valid_as_of.is_none()
            && q.trace.is_none()
            && q.traverse.is_none();
        if !index_path_held {
            sort_by_keys(&mut rows, &q.order_by,
                         |n, f| field_value(n, f));
        }
    }

    // ── OFFSET then LIMIT ────────────────────────────────────────────────────

    let rows = paginate(rows, q.offset, q.limit);

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

    /// Regression: WHERE + ORDER BY + LIMIT must not truncate candidates
    /// before the filter runs. setup() gives heights 1..=5 with n_tx = h*2;
    /// the predicate matches ONLY the two highest heights (4, 5). The old
    /// code passed LIMIT into the sorted-index top-k first: it fetched
    /// heights [1, 2], filtered on n_tx >= 8, and returned ZERO rows even
    /// though two matches exist. Python reference returns [4, 5].
    #[test]
    fn where_order_limit_does_not_truncate_before_filter() {
        let (_tmp, db) = setup();
        let (rows, count) =
            query(&db, "FROM blocks WHERE n_tx >= 8 ORDER BY height LIMIT 2").unwrap();
        assert_eq!(count, 2, "both matching rows must survive the limit");
        let heights: Vec<u64> = rows.iter()
            .filter_map(|r| r["height"].as_u64())
            .collect();
        assert_eq!(heights, vec![4, 5]);
        // And the same shape DESC — top match first.
        let (rows_d, _) =
            query(&db, "FROM blocks WHERE n_tx >= 8 ORDER BY height DESC LIMIT 1").unwrap();
        assert_eq!(rows_d.len(), 1);
        assert_eq!(rows_d[0]["height"], 5);
    }

    // ── Predicate parity (3.3.0) ─────────────────────────────────────────────
    //
    // setup() gives blocks 1..=5 with height = h, hash = "000{h}",
    // n_tx = h * 2. Every test below asserts against that fixture.

    /// A second fixture with string fields and a sparse column, for LIKE and
    /// IS NULL. `miner` is absent on one row on purpose.
    fn setup_text() -> (tempfile::TempDir, Db) {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        let rows = [
            ("1", serde_json::json!({"status": "open",    "miner": "Acme Pool", "fee": 10})),
            ("2", serde_json::json!({"status": "pending", "miner": "acme solo", "fee": 20})),
            ("3", serde_json::json!({"status": "closed",  "miner": "Zenith",    "fee": 30})),
            ("4", serde_json::json!({"status": "open",    "fee": 40})),
            ("5", serde_json::json!({"status": "voided",  "miner": Value::Null, "fee": 50})),
        ];
        for (id, data) in rows {
            db.put("jobs", id, data, vec![], None, None).unwrap();
        }
        (dir, db)
    }

    fn ids(rows: &[Value]) -> Vec<String> {
        let mut v: Vec<String> = rows.iter()
            .filter_map(|r| r["_id"].as_str().map(String::from))
            .collect();
        v.sort();
        v
    }

    #[test]
    fn where_in_list() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks WHERE height IN (2, 4)").unwrap();
        assert_eq!(ids(&rows), vec!["2", "4"]);
    }

    #[test]
    fn where_in_strings() {
        let (_tmp, db) = setup_text();
        let (rows, _) = query(&db, r#"FROM jobs WHERE status IN ("open", "closed")"#).unwrap();
        assert_eq!(ids(&rows), vec!["1", "3", "4"]);
    }

    #[test]
    fn where_not_in() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks WHERE height NOT IN (1, 2, 3)").unwrap();
        assert_eq!(ids(&rows), vec!["4", "5"]);
    }

    #[test]
    fn where_in_single_value_equals_eq() {
        let (_tmp, db) = setup();
        let (a, _) = query(&db, "FROM blocks WHERE height IN (3)").unwrap();
        let (b, _) = query(&db, "FROM blocks WHERE height = 3").unwrap();
        assert_eq!(ids(&a), ids(&b));
    }

    #[test]
    fn where_between_is_inclusive() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks WHERE height BETWEEN 2 AND 4").unwrap();
        // SQL BETWEEN includes both bounds — 2 and 4 must be present.
        assert_eq!(ids(&rows), vec!["2", "3", "4"]);
    }

    #[test]
    fn where_not_between() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks WHERE height NOT BETWEEN 2 AND 4").unwrap();
        assert_eq!(ids(&rows), vec!["1", "5"]);
    }

    /// The AND inside BETWEEN belongs to BETWEEN, not to the conjunction
    /// parser. If parse_and grabbed it first, this query would fail to parse
    /// or silently lose the second bound.
    #[test]
    fn between_and_does_not_swallow_the_conjunction() {
        let (_tmp, db) = setup();
        let (rows, _) = query(
            &db, "FROM blocks WHERE height BETWEEN 2 AND 4 AND n_tx > 4").unwrap();
        // heights 2,3,4 then n_tx > 4 (n_tx = h*2) leaves 3 and 4.
        assert_eq!(ids(&rows), vec!["3", "4"]);
    }

    #[test]
    fn where_like_prefix_suffix_and_infix() {
        let (_tmp, db) = setup_text();
        let (pre, _) = query(&db, r#"FROM jobs WHERE miner LIKE "Acme%""#).unwrap();
        assert_eq!(ids(&pre), vec!["1"]);
        let (suf, _) = query(&db, r#"FROM jobs WHERE miner LIKE "%Pool""#).unwrap();
        assert_eq!(ids(&suf), vec!["1"]);
        let (inf, _) = query(&db, r#"FROM jobs WHERE status LIKE "%pen%""#).unwrap();
        assert_eq!(ids(&inf), vec!["1", "2", "4"]);   // open, pending, open
    }

    #[test]
    fn where_like_underscore_matches_exactly_one_char() {
        let (_tmp, db) = setup_text();
        let (rows, _) = query(&db, r#"FROM jobs WHERE status LIKE "open_""#).unwrap();
        assert!(rows.is_empty(), "`open_` must not match the 4-char value `open`");
        let (rows2, _) = query(&db, r#"FROM jobs WHERE status LIKE "ope_""#).unwrap();
        assert_eq!(ids(&rows2), vec!["1", "4"]);
    }

    #[test]
    fn where_ilike_is_case_insensitive_and_like_is_not() {
        let (_tmp, db) = setup_text();
        let (ci, _) = query(&db, r#"FROM jobs WHERE miner ILIKE "acme%""#).unwrap();
        assert_eq!(ci.len(), 2, "ILIKE matches both `Acme Pool` and `acme solo`");
        let (cs, _) = query(&db, r#"FROM jobs WHERE miner LIKE "acme%""#).unwrap();
        assert_eq!(ids(&cs), vec!["2"], "LIKE stays case-sensitive");
    }

    /// The backtracking path: multiple `%` with literals between them, where a
    /// greedy first match must be given back for the pattern to succeed.
    #[test]
    fn like_backtracks_across_multiple_wildcards() {
        assert!(like_match("abcabcabd", "%abc%abd", false));
        assert!(like_match("aaa", "%a", false));
        assert!(like_match("", "%", false));
        assert!(like_match("x", "%%%", false));
        assert!(!like_match("abc", "%abd", false));
        assert!(!like_match("ab", "ab_", false));
        assert!(like_match("héllo wörld", "h_llo w%d", false));
    }

    #[test]
    fn where_not_like() {
        let (_tmp, db) = setup_text();
        let (rows, _) = query(&db, r#"FROM jobs WHERE status NOT LIKE "open""#).unwrap();
        assert_eq!(ids(&rows), vec!["2", "3", "5"]);
    }

    /// NOT LIKE over a NULL/absent field stays false, as in SQL: a predicate
    /// over NULL is never true in either polarity. Rows 4 (absent) and 5
    /// (explicit null) must appear in NEITHER `LIKE` nor `NOT LIKE`.
    #[test]
    fn like_over_null_is_false_in_both_polarities() {
        let (_tmp, db) = setup_text();
        let (pos, _) = query(&db, r#"FROM jobs WHERE miner LIKE "%""#).unwrap();
        let (neg, _) = query(&db, r#"FROM jobs WHERE miner NOT LIKE "%""#).unwrap();
        assert!(!ids(&pos).contains(&"4".to_string()));
        assert!(!ids(&neg).contains(&"4".to_string()));
        assert!(!ids(&pos).contains(&"5".to_string()));
        assert!(!ids(&neg).contains(&"5".to_string()));
    }

    /// Absent and explicitly-null are the same observable state in a
    /// schemaless store, so IS NULL must catch both.
    #[test]
    fn where_is_null_catches_absent_and_explicit_null() {
        let (_tmp, db) = setup_text();
        let (rows, _) = query(&db, "FROM jobs WHERE miner IS NULL").unwrap();
        assert_eq!(ids(&rows), vec!["4", "5"]);
    }

    #[test]
    fn where_is_not_null() {
        let (_tmp, db) = setup_text();
        let (rows, _) = query(&db, "FROM jobs WHERE miner IS NOT NULL").unwrap();
        assert_eq!(ids(&rows), vec!["1", "2", "3"]);
    }

    #[test]
    fn where_or() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks WHERE height = 1 OR height = 5").unwrap();
        assert_eq!(ids(&rows), vec!["1", "5"]);
    }

    /// AND binds tighter than OR, so this is `a OR (b AND c)` and NOT
    /// `(a OR b) AND c`. With the wrong precedence the result would be [5].
    #[test]
    fn and_binds_tighter_than_or() {
        let (_tmp, db) = setup();
        let (rows, _) = query(
            &db, "FROM blocks WHERE height = 1 OR height = 5 AND n_tx = 10").unwrap();
        assert_eq!(ids(&rows), vec!["1", "5"]);
        let (rows2, _) = query(
            &db, "FROM blocks WHERE height = 1 OR height = 5 AND n_tx = 99").unwrap();
        assert_eq!(ids(&rows2), vec!["1"], "the AND arm must not match");
    }

    /// Parentheses must be able to override that precedence.
    #[test]
    fn parens_override_precedence() {
        let (_tmp, db) = setup();
        let (rows, _) = query(
            &db, "FROM blocks WHERE (height = 1 OR height = 5) AND n_tx = 10").unwrap();
        assert_eq!(ids(&rows), vec!["5"]);
    }

    #[test]
    fn nested_parens() {
        let (_tmp, db) = setup();
        let (rows, _) = query(
            &db,
            "FROM blocks WHERE ((height >= 2 AND height <= 4) OR height = 1) AND n_tx != 6",
        ).unwrap();
        assert_eq!(ids(&rows), vec!["1", "2", "4"]);
    }

    #[test]
    fn not_negates_a_group() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks WHERE NOT (height > 2)").unwrap();
        assert_eq!(ids(&rows), vec!["1", "2"]);
    }

    /// Prefix NOT before a bare comparison, as SQL allows. Distinct from the
    /// INFIX `field NOT <op>` form, which is a syntax error — only NOT IN /
    /// NOT BETWEEN / NOT LIKE exist in that position.
    #[test]
    fn prefix_not_before_a_comparison() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks WHERE NOT height = 1").unwrap();
        assert_eq!(ids(&rows), vec!["2", "3", "4", "5"]);
        let (double, _) = query(&db, "FROM blocks WHERE NOT NOT height = 1").unwrap();
        assert_eq!(ids(&double), vec!["1"]);
        let (mixed, _) = query(&db, "FROM blocks WHERE NOT height = 1 AND height < 4").unwrap();
        assert_eq!(ids(&mixed), vec!["2", "3"]);
    }

    /// `_id = "x"` takes an O(1) index path. Under an OR it does not constrain
    /// the result set, so using it as a point lookup would drop every row the
    /// other arm matched. Guards the id_point_lookup AND-only descent.
    #[test]
    fn id_equality_under_or_does_not_become_a_point_lookup() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, r#"FROM blocks WHERE _id = "1" OR height > 3"#).unwrap();
        assert_eq!(ids(&rows), vec!["1", "4", "5"],
                   "the OR arm must survive the id fast path");
    }

    /// The fast path is still taken when the equality is a genuine conjunct.
    #[test]
    fn id_equality_under_and_still_point_looks_up() {
        let (_tmp, db) = setup();
        let (hit, _) = query(&db, r#"FROM blocks WHERE _id = "3" AND n_tx = 6"#).unwrap();
        assert_eq!(ids(&hit), vec!["3"]);
        let (miss, _) = query(&db, r#"FROM blocks WHERE _id = "3" AND n_tx = 999"#).unwrap();
        assert!(miss.is_empty(), "the second conjunct must still be applied");
    }

    #[test]
    fn metadata_fields_are_filterable() {
        let (_tmp, db) = setup();
        // _seq is 0-indexed — the first put lands at seq 0, so `> 0` drops it.
        let (rows, _) = query(&db, "FROM blocks WHERE _seq >= 0 AND _coll = blocks").unwrap();
        assert_eq!(rows.len(), 5);
        let (tail, _) = query(&db, "FROM blocks WHERE _seq > 0").unwrap();
        assert_eq!(tail.len(), 4);
    }

    #[test]
    fn combined_with_order_and_limit() {
        let (_tmp, db) = setup();
        let (rows, _) = query(
            &db,
            "FROM blocks WHERE height IN (1, 3, 5) ORDER BY height DESC LIMIT 2",
        ).unwrap();
        let heights: Vec<u64> = rows.iter().filter_map(|r| r["height"].as_u64()).collect();
        assert_eq!(heights, vec![5, 3]);
    }

    // ── Strictness: a query the engine cannot honour must FAIL, not lie ──────

    /// The headline regression. `_ => { self.advance(); }` meant an
    /// unimplemented or misspelled clause was dropped and a DIFFERENT query
    /// was answered. Each of these previously returned rows.
    #[test]
    fn unknown_clauses_are_errors_not_silent_skips() {
        let (_tmp, db) = setup();
        for bad in [
            "FROM blocks ORDRE BY height",       // typo
            "FROM blocks WHERE height > 3 JUNK", // trailing garbage
            "FROM blocks SELECT height",         // wrong dialect
            "FROM blocks LIMIT",                 // missing count
            "FROM blocks OFFSET",                // missing count
            "FROM blocks ORDER BY",              // missing key
            "FROM blocks ORDER BY height,",      // trailing comma
        ] {
            assert!(query(&db, bad).is_err(), "`{}` must be rejected, not silently reinterpreted", bad);
        }
    }

    #[test]
    fn malformed_predicates_are_errors() {
        let (_tmp, db) = setup();
        for bad in [
            "FROM blocks WHERE height >",            // missing value
            "FROM blocks WHERE height IN (",         // unterminated list
            "FROM blocks WHERE height IN ()",        // empty list
            "FROM blocks WHERE height BETWEEN 1",    // missing AND high
            "FROM blocks WHERE height BETWEEN 1 3",  // missing AND
            "FROM blocks WHERE (height = 1",         // unbalanced paren
            "FROM blocks WHERE height IS 3",         // IS without NULL
            "FROM blocks WHERE height NOT = 1",      // infix NOT before a comparison op
            "FROM blocks WHERE height LIKE",         // missing pattern
        ] {
            assert!(query(&db, bad).is_err(), "`{}` must be a parse error", bad);
        }
    }

    /// ASC used to survive only because unknown tokens were skipped. Now that
    /// skipping is gone it has to be a real keyword, and the pre-existing
    /// `order_by_limit` test above depends on it.
    #[test]
    fn asc_is_accepted_explicitly() {
        let (_tmp, db) = setup();
        let (asc, _) = query(&db, "FROM blocks ORDER BY height ASC").unwrap();
        let (plain, _) = query(&db, "FROM blocks ORDER BY height").unwrap();
        assert_eq!(asc[0]["height"], 1);
        assert_eq!(plain[0]["height"], 1);
    }

    /// Lowercase and mixed-case keywords must keep working — the lexer
    /// uppercases before matching, and the new keywords must be no different.
    #[test]
    fn new_keywords_are_case_insensitive() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "from blocks where height between 2 and 3").unwrap();
        assert_eq!(ids(&rows), vec!["2", "3"]);
        let (rows2, _) = query(&db, "FROM blocks Where height In (1) Or height In (2)").unwrap();
        assert_eq!(ids(&rows2), vec!["1", "2"]);
    }

    #[test]
    fn group_by_count() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks GROUP BY n_tx COUNT").unwrap();
        assert_eq!(rows.len(), 5); // all unique n_tx values
    }

    // ── Indexed range / point scans (3.3.0) ─────────────────────────────────
    //
    // The load-bearing property is EQUIVALENCE: an indexed query and the same
    // query without an index must return the same rows. The planner is allowed
    // to be imprecise (it only has to produce a superset — the full predicate
    // re-runs on the candidates) but it is never allowed to be wrong.
    //
    // Every test below therefore runs the same query against two databases
    // holding identical data, one indexed and one not, and compares.

    /// Build two identical databases, one with sorted indexes on `fields`.
    fn twin(fields: &[&str]) -> (tempfile::TempDir, tempfile::TempDir, Db, Db) {
        let d1 = tempdir().unwrap();
        let d2 = tempdir().unwrap();
        let indexed = Db::open(d1.path(), None).unwrap();
        let plain = Db::open(d2.path(), None).unwrap();
        for f in fields {
            indexed.create_sorted_index("t", f);
        }
        // Deliberately messy: duplicate fees, a missing field, a null, a
        // string column, and an out-of-order insert sequence.
        let rows: Vec<(String, Value)> = (0..40u64).map(|i| {
            let mut o = serde_json::Map::new();
            if i % 7 != 0 {
                o.insert("fee".into(), json!(i % 13));
            }
            if i % 11 == 0 {
                o.insert("note".into(), Value::Null);
            } else {
                o.insert("note".into(), json!(format!("n{}", i % 5)));
            }
            o.insert("rank".into(), json!(40 - i));
            (i.to_string(), Value::Object(o))
        }).collect();
        for (id, doc) in &rows {
            indexed.put("t", id, doc.clone(), vec![], None, None).unwrap();
            plain.put("t", id, doc.clone(), vec![], None, None).unwrap();
        }
        (d1, d2, indexed, plain)
    }

    fn same(a: &Db, b: &Db, nql: &str) -> (Vec<String>, Vec<String>) {
        let ga = {
            let (rows, _) = query(a, nql).unwrap();
            let mut v: Vec<String> = rows.iter()
                .filter_map(|r| r["_id"].as_str().map(String::from)).collect();
            v.sort(); v
        };
        let gb = {
            let (rows, _) = query(b, nql).unwrap();
            let mut v: Vec<String> = rows.iter()
                .filter_map(|r| r["_id"].as_str().map(String::from)).collect();
            v.sort(); v
        };
        (ga, gb)
    }

    #[test]
    fn indexed_and_unindexed_agree_on_every_predicate_shape() {
        let (_t1, _t2, idx, plain) = twin(&["fee", "note", "rank"]);
        for nql in [
            // ranges — the shapes the index now serves
            "FROM t WHERE fee > 5",
            "FROM t WHERE fee >= 5",
            "FROM t WHERE fee < 5",
            "FROM t WHERE fee <= 5",
            "FROM t WHERE fee = 5",
            "FROM t WHERE fee BETWEEN 3 AND 8",
            "FROM t WHERE fee NOT BETWEEN 3 AND 8",
            "FROM t WHERE fee IN (1, 5, 9)",
            "FROM t WHERE fee NOT IN (1, 5, 9)",
            "FROM t WHERE fee != 5",
            // merged bounds on one field
            "FROM t WHERE fee > 3 AND fee < 9",
            "FROM t WHERE fee >= 3 AND fee <= 9",
            "FROM t WHERE fee > 3 AND fee < 9 AND fee != 5",
            "FROM t WHERE fee BETWEEN 2 AND 10 AND fee > 6",
            // two indexed fields — the planner must pick one and stay correct
            "FROM t WHERE fee > 5 AND rank < 20",
            "FROM t WHERE fee IN (2, 3) AND rank > 10",
            "FROM t WHERE fee = 4 AND rank = 8",
            // the absent-field cases, where a naive index scan inverts the answer
            "FROM t WHERE fee IS NULL",
            "FROM t WHERE fee IS NOT NULL",
            "FROM t WHERE note IS NULL",
            "FROM t WHERE note IS NOT NULL",
            "FROM t WHERE fee IS NULL AND rank > 20",
            // predicates the index cannot serve, mixed with ones it can
            r#"FROM t WHERE note LIKE "n_""#,
            r#"FROM t WHERE fee > 5 AND note LIKE "n1""#,
            r#"FROM t WHERE note NOT LIKE "n1" AND fee < 4"#,
            // disjunction — must NOT be narrowed on one arm
            "FROM t WHERE fee > 11 OR rank > 38",
            "FROM t WHERE fee = 1 OR note IS NULL",
            "FROM t WHERE (fee > 11 OR rank > 38) AND rank < 39",
            "FROM t WHERE fee IN (1) OR fee IN (2)",
            // negation
            "FROM t WHERE NOT (fee > 5)",
            "FROM t WHERE NOT (fee IN (1, 2))",
            "FROM t WHERE NOT (fee > 5) AND rank < 30",
            // with shaping on top
            "FROM t WHERE fee > 5 ORDER BY rank DESC LIMIT 5",
            "FROM t WHERE fee BETWEEN 2 AND 8 ORDER BY fee, rank DESC",
            "FROM t WHERE fee > 5 GROUP BY note COUNT",
            "FROM t WHERE fee > 5 COUNT",
            "FROM t WHERE fee > 5 ORDER BY rank LIMIT 3 OFFSET 2",
            // empty results
            "FROM t WHERE fee > 9999",
            "FROM t WHERE fee IN (9999)",
            "FROM t WHERE fee BETWEEN 100 AND 200",
        ] {
            let (a, b) = same(&idx, &plain, nql);
            assert_eq!(a, b, "indexed and unindexed disagree on `{}`", nql);
        }
    }

    /// Ordering, not just membership, must survive the index path — the
    /// candidates arrive in index order of the PREDICATE field, which is not
    /// the requested sort order, so the post-filter sort has to still run.
    #[test]
    fn index_path_still_honours_order_by() {
        let (_t1, _t2, idx, plain) = twin(&["fee", "rank"]);
        for nql in [
            "FROM t WHERE fee > 4 ORDER BY rank",
            "FROM t WHERE fee > 4 ORDER BY rank DESC",
            "FROM t WHERE fee > 4 ORDER BY note, rank DESC",
            "FROM t WHERE fee BETWEEN 2 AND 9 ORDER BY rank LIMIT 4",
            "FROM t WHERE fee IN (3, 6) ORDER BY rank DESC LIMIT 2",
        ] {
            let ra = query(&idx, nql).unwrap().0;
            let rb = query(&plain, nql).unwrap().0;
            let ia: Vec<&str> = ra.iter().filter_map(|r| r["_id"].as_str()).collect();
            let ib: Vec<&str> = rb.iter().filter_map(|r| r["_id"].as_str()).collect();
            assert_eq!(ia, ib, "row ORDER differs on `{}`", nql);
        }
    }

    /// An ordering comparison against a missing field is never true.
    ///
    /// OrderedValue sorts Null below every number, so `<` and `<=` reported
    /// that a document with NO `fee` field satisfied `WHERE fee < 5` — while
    /// `>` and `>=` excluded it. That asymmetry was the tell. The Python
    /// reference has always excluded it, so this was a cross-engine
    /// divergence as well as a wrong answer, and it meant the scan path and
    /// the index path disagreed depending on whether an index existed.
    #[test]
    fn an_ordering_comparison_against_a_missing_field_is_false() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.put("t", "has", json!({"fee": 1}), vec![], None, None).unwrap();
        db.put("t", "none", json!({"other": 1}), vec![], None, None).unwrap();
        db.put("t", "null", json!({"fee": Value::Null}), vec![], None, None).unwrap();

        for nql in ["FROM t WHERE fee < 5", "FROM t WHERE fee <= 5"] {
            let (r, _) = query(&db, nql).unwrap();
            let ids: Vec<&str> = r.iter().filter_map(|x| x["_id"].as_str()).collect();
            assert_eq!(ids, vec!["has"],
                       "`{}` must not match a row whose fee is absent or null", nql);
        }
        for nql in ["FROM t WHERE fee > 0", "FROM t WHERE fee >= 0"] {
            let (r, _) = query(&db, nql).unwrap();
            let ids: Vec<&str> = r.iter().filter_map(|x| x["_id"].as_str()).collect();
            assert_eq!(ids, vec!["has"], "`{}`", nql);
        }
        // BETWEEN is built from >= and <=, so it inherits the rule.
        let (b, _) = query(&db, "FROM t WHERE fee BETWEEN 0 AND 9").unwrap();
        assert_eq!(b.len(), 1);

        // = and != keep operating on null, exactly as the Python reference
        // does — its None guard sits deliberately AFTER those two arms.
        let (ne, _) = query(&db, "FROM t WHERE fee != 5").unwrap();
        assert_eq!(ne.len(), 3, "!= still matches absent and null fields");
        let (isnull, _) = query(&db, "FROM t WHERE fee = NULL").unwrap();
        assert_eq!(isnull.len(), 2, "absent and explicit-null both equal NULL");

        // And the same answers with an index present — the two paths agreeing
        // is the reason this fix was required, not merely desirable.
        let d2 = tempdir().unwrap();
        let idx = Db::open(d2.path(), None).unwrap();
        idx.create_sorted_index("t", "fee");
        idx.put("t", "has", json!({"fee": 1}), vec![], None, None).unwrap();
        idx.put("t", "none", json!({"other": 1}), vec![], None, None).unwrap();
        idx.put("t", "null", json!({"fee": Value::Null}), vec![], None, None).unwrap();
        for nql in ["FROM t WHERE fee < 5", "FROM t WHERE fee <= 5",
                    "FROM t WHERE fee > 0", "FROM t WHERE fee BETWEEN 0 AND 9"] {
            let (a, _) = query(&db, nql).unwrap();
            let (b, _) = query(&idx, nql).unwrap();
            let ia: Vec<&str> = a.iter().filter_map(|x| x["_id"].as_str()).collect();
            let ib: Vec<&str> = b.iter().filter_map(|x| x["_id"].as_str()).collect();
            assert_eq!(ia, ib, "indexed and unindexed disagree on `{}`", nql);
        }
    }

    /// `IS NULL` must never touch this index. A document whose field is absent
    /// is not in the index for that field, so an index scan would return
    /// exactly the complement of the right answer — the worst possible failure
    /// for a filter, since it looks like a plausible result set.
    #[test]
    fn is_null_never_uses_the_index() {
        let (_t1, _t2, idx, plain) = twin(&["fee"]);
        let (a, b) = same(&idx, &plain, "FROM t WHERE fee IS NULL");
        assert_eq!(a, b);
        // 40 docs, every 7th missing `fee`: ids 0,7,14,21,28,35.
        assert_eq!(a, vec!["0", "14", "21", "28", "35", "7"]);
        assert!(!a.is_empty(), "the fixture must actually contain absent fields");
    }

    /// A constraint under an OR does not restrict the result set, so the
    /// planner must not narrow on it. Both arms have to survive.
    #[test]
    fn a_disjunct_is_never_used_to_narrow() {
        let (_t1, _t2, idx, plain) = twin(&["fee", "rank"]);
        let nql = "FROM t WHERE fee = 1 OR rank = 40";
        let (a, b) = same(&idx, &plain, nql);
        assert_eq!(a, b);
        // rank = 40 is doc 0, which has NO `fee` field at all — so if the
        // planner had narrowed on the `fee` arm it would have been dropped.
        assert!(a.contains(&"0".to_string()),
                "the OR arm matching a doc with no indexed field must survive: {:?}", a);
        assert!(a.len() > 1, "both arms must contribute: {:?}", a);
    }

    /// AS OF must not use the index: it holds CURRENT versions only, because a
    /// superseded hash is removed on overwrite. An index scan would answer a
    /// historical query with present-day rows.
    #[test]
    fn as_of_does_not_use_the_current_version_index() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.create_sorted_index("t", "fee");
        db.put("t", "a", json!({"fee": 5}), vec![], None, None).unwrap();
        let snap = db.put("t", "b", json!({"fee": 5}), vec![], None, None).unwrap().seq;
        // Move both out of the range the query asks for.
        db.put("t", "a", json!({"fee": 999}), vec![], None, None).unwrap();
        db.put("t", "b", json!({"fee": 999}), vec![], None, None).unwrap();

        // At HEAD nothing matches fee = 5 any more.
        let (now, _) = query(&db, "FROM t WHERE fee = 5").unwrap();
        assert!(now.is_empty(), "current versions have fee 999: {:?}", now);

        // AS OF the snapshot, both still had fee = 5. If the index served
        // this, it would return nothing.
        let (then, _) = query(&db, &format!("FROM t AS OF {} WHERE fee = 5", snap)).unwrap();
        let mut ids: Vec<&str> = then.iter().filter_map(|r| r["_id"].as_str()).collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "b"], "AS OF must see the historical values");

        // Same for a range and an IN.
        let (r, _) = query(&db, &format!("FROM t AS OF {} WHERE fee BETWEEN 1 AND 9", snap)).unwrap();
        assert_eq!(r.len(), 2);
        let (i, _) = query(&db, &format!("FROM t AS OF {} WHERE fee IN (5)", snap)).unwrap();
        assert_eq!(i.len(), 2);
    }

    /// An overwritten row must not come back from the index.
    #[test]
    fn the_index_path_returns_current_versions_only() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.create_sorted_index("t", "fee");
        for i in 0..5u64 {
            db.put("t", &i.to_string(), json!({"fee": i}), vec![], None, None).unwrap();
        }
        db.put("t", "0", json!({"fee": 100}), vec![], None, None).unwrap();

        let (low, _) = query(&db, "FROM t WHERE fee BETWEEN 0 AND 4").unwrap();
        let mut ids: Vec<&str> = low.iter().filter_map(|r| r["_id"].as_str()).collect();
        ids.sort();
        assert_eq!(ids, vec!["1", "2", "3", "4"],
                   "doc 0 moved to fee 100 and must not appear in 0..4");

        let (high, _) = query(&db, "FROM t WHERE fee = 100").unwrap();
        assert_eq!(high.len(), 1);
        assert_eq!(high[0]["_id"], "0");
        assert_eq!(high[0]["fee"], json!(100), "the CURRENT value, not the old one");
    }

    /// Duplicate values must not produce duplicate rows, and a value repeated
    /// across IN arms must be returned once.
    #[test]
    fn index_scans_do_not_duplicate_rows() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.create_sorted_index("t", "fee");
        for i in 0..6u64 {
            db.put("t", &i.to_string(), json!({"fee": i % 2}), vec![], None, None).unwrap();
        }
        let (dup, _) = query(&db, "FROM t WHERE fee IN (0, 0, 1, 1)").unwrap();
        assert_eq!(dup.len(), 6, "each row once despite repeated IN arms");
        let (r, _) = query(&db, "FROM t WHERE fee BETWEEN 0 AND 1").unwrap();
        assert_eq!(r.len(), 6);
        let mut ids: Vec<&str> = dup.iter().filter_map(|r| r["_id"].as_str()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 6, "no duplicate _ids");
    }

    /// A range over a string column, to prove the index is not numeric-only.
    #[test]
    fn index_ranges_work_on_strings() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.create_sorted_index("t", "name");
        for (i, n) in ["alpha", "bravo", "charlie", "delta", "echo"].iter().enumerate() {
            db.put("t", &i.to_string(), json!({"name": n}), vec![], None, None).unwrap();
        }
        let (r, _) = query(&db, r#"FROM t WHERE name BETWEEN "bravo" AND "delta""#).unwrap();
        let mut got: Vec<&str> = r.iter().filter_map(|x| x["name"].as_str()).collect();
        got.sort();
        assert_eq!(got, vec!["bravo", "charlie", "delta"]);
        let (gt, _) = query(&db, r#"FROM t WHERE name > "charlie""#).unwrap();
        assert_eq!(gt.len(), 2);
    }

    /// Bounds must be merged into one walk, and the tighter bound must win
    /// regardless of the order the conjuncts appear in.
    #[test]
    fn same_field_bounds_are_merged_tightest_wins() {
        let (_t1, _t2, idx, plain) = twin(&["fee"]);
        for (a_nql, b_nql) in [
            ("FROM t WHERE fee > 2 AND fee > 6", "FROM t WHERE fee > 6"),
            ("FROM t WHERE fee > 6 AND fee > 2", "FROM t WHERE fee > 6"),
            ("FROM t WHERE fee < 9 AND fee < 4", "FROM t WHERE fee < 4"),
            ("FROM t WHERE fee BETWEEN 0 AND 12 AND fee >= 5 AND fee <= 7",
             "FROM t WHERE fee >= 5 AND fee <= 7"),
        ] {
            let (ia, _) = same(&idx, &plain, a_nql);
            let (ib, _) = same(&idx, &plain, b_nql);
            assert_eq!(ia, ib, "`{}` should equal `{}`", a_nql, b_nql);
        }
    }

    /// The index only helps where it exists; an unindexed field must still
    /// answer correctly through the scan path.
    #[test]
    fn a_predicate_on_an_unindexed_field_still_answers() {
        let (_t1, _t2, idx, plain) = twin(&["fee"]);   // `rank` is NOT indexed
        for nql in [
            "FROM t WHERE rank > 30",
            "FROM t WHERE rank BETWEEN 10 AND 20",
            "FROM t WHERE rank IN (40, 39)",
            "FROM t WHERE rank > 30 AND fee > 2",
        ] {
            let (a, b) = same(&idx, &plain, nql);
            assert_eq!(a, b, "`{}`", nql);
        }
    }

    /// Cardinality is reported off the index without reading any rows, which
    /// is what lets the planner compare two candidate indexes.
    #[test]
    fn range_cardinality_counts_without_reading() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.create_sorted_index("t", "fee");
        for i in 0..20u64 {
            db.put("t", &i.to_string(), json!({"fee": i}), vec![], None, None).unwrap();
        }
        assert_eq!(db.range_cardinality("t", "fee", None, None, true, true), Some(20));
        assert_eq!(
            db.range_cardinality("t", "fee", Some(&json!(5)), Some(&json!(9)), true, true),
            Some(5), "5..=9 inclusive is five values");
        assert_eq!(
            db.range_cardinality("t", "fee", Some(&json!(5)), Some(&json!(9)), false, false),
            Some(3), "exclusive bounds drop both ends");
        assert_eq!(
            db.range_cardinality("t", "fee", Some(&json!(18)), None, true, true),
            Some(2));
        assert_eq!(
            db.range_cardinality("t", "fee", Some(&json!(999)), None, true, true),
            Some(0), "an empty range is 0, not an error");
        // No index on this field at all.
        assert_eq!(db.range_cardinality("t", "nope", None, None, true, true), None);
    }

    /// With two usable indexes the planner should choose the narrower range.
    /// Asserted through cardinality rather than by inspecting the plan, so the
    /// test pins the observable behaviour and not the implementation.
    #[test]
    fn the_narrower_index_is_preferred() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.create_sorted_index("t", "wide");
        db.create_sorted_index("t", "narrow");
        for i in 0..100u64 {
            db.put("t", &i.to_string(),
                   json!({"wide": i % 2, "narrow": i}), vec![], None, None).unwrap();
        }
        // `wide = 0` covers 50 rows; `narrow = 7` covers 1.
        let wide = db.range_cardinality("t", "wide", Some(&json!(0)), Some(&json!(0)), true, true);
        let narrow = db.range_cardinality("t", "narrow", Some(&json!(7)), Some(&json!(7)), true, true);
        assert_eq!(wide, Some(50));
        assert_eq!(narrow, Some(1));
        // The answer must be right whichever index is chosen.
        let (r, _) = query(&db, "FROM t WHERE wide = 0 AND narrow = 7").unwrap();
        assert!(r.is_empty(), "narrow 7 has wide 1, so nothing matches");
        let (r2, _) = query(&db, "FROM t WHERE wide = 0 AND narrow = 8").unwrap();
        assert_eq!(r2.len(), 1);
        assert_eq!(r2[0]["_id"], "8");
    }

    // ── Result shaping (3.3.0): OFFSET, multi-key ORDER BY, HAVING, ─────────
    // ── bare aggregates, and the SQL pipeline order ─────────────────────────

    fn heights(rows: &[Value]) -> Vec<u64> {
        rows.iter().filter_map(|r| r["height"].as_u64()).collect()
    }

    #[test]
    fn offset_skips_result_rows() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks ORDER BY height OFFSET 2").unwrap();
        assert_eq!(heights(&rows), vec![3, 4, 5]);
    }

    #[test]
    fn offset_with_limit_pages() {
        let (_tmp, db) = setup();
        // Page through 5 rows two at a time. Each page must be disjoint and
        // in order — the bug to catch is a pushdown that fetches only `limit`
        // rows and therefore returns page 1 for every page.
        let mut seen = vec![];
        for page in 0..3 {
            let (rows, _) = query(
                &db,
                &format!("FROM blocks ORDER BY height LIMIT 2 OFFSET {}", page * 2),
            ).unwrap();
            seen.extend(heights(&rows));
        }
        assert_eq!(seen, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn offset_past_the_end_is_an_empty_page() {
        let (_tmp, db) = setup();
        let (rows, count) = query(&db, "FROM blocks OFFSET 99").unwrap();
        assert!(rows.is_empty());
        assert_eq!(count, 0);
        let (zero, _) = query(&db, "FROM blocks OFFSET 0").unwrap();
        assert_eq!(zero.len(), 5, "OFFSET 0 skips nothing");
    }

    #[test]
    fn offset_applies_after_the_filter() {
        let (_tmp, db) = setup();
        // n_tx = h*2, so `>= 6` matches heights 3,4,5. Offsetting by one must
        // skip the first MATCH, not the first row of the collection.
        let (rows, _) = query(
            &db, "FROM blocks WHERE n_tx >= 6 ORDER BY height OFFSET 1").unwrap();
        assert_eq!(heights(&rows), vec![4, 5]);
    }

    #[test]
    fn order_by_multiple_keys() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        // Two statuses, each with several fees, so the second key has to do
        // real work to break the first key's ties.
        for (i, (s, f)) in [("open", 30), ("open", 10), ("closed", 20),
                            ("open", 20), ("closed", 5)].iter().enumerate() {
            db.put("t", &i.to_string(),
                serde_json::json!({"status": s, "fee": f}), vec![], None, None).unwrap();
        }
        let (rows, _) = query(&db, "FROM t ORDER BY status, fee DESC").unwrap();
        let got: Vec<(String, u64)> = rows.iter()
            .map(|r| (r["status"].as_str().unwrap().to_string(), r["fee"].as_u64().unwrap()))
            .collect();
        assert_eq!(got, vec![
            ("closed".into(), 20), ("closed".into(), 5),
            ("open".into(), 30), ("open".into(), 20), ("open".into(), 10),
        ]);
    }

    #[test]
    fn order_by_mixed_directions() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        for (i, (a, b)) in [(1, 1), (1, 2), (2, 1), (2, 2)].iter().enumerate() {
            db.put("t", &i.to_string(),
                serde_json::json!({"a": a, "b": b}), vec![], None, None).unwrap();
        }
        let (rows, _) = query(&db, "FROM t ORDER BY a DESC, b ASC").unwrap();
        let got: Vec<(u64, u64)> = rows.iter()
            .map(|r| (r["a"].as_u64().unwrap(), r["b"].as_u64().unwrap()))
            .collect();
        assert_eq!(got, vec![(2, 1), (2, 2), (1, 1), (1, 2)]);
    }

    /// The headline pipeline-order bug. In SQL, LIMIT applies to the RESULT.
    /// The old order was ORDER BY -> LIMIT -> GROUP BY, so LIMIT truncated the
    /// INPUT and the aggregate was computed over a fraction of the rows —
    /// reporting counts that summed to the limit instead of the true total.
    #[test]
    fn limit_applies_to_grouped_rows_not_to_the_input() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        for i in 0..12 {
            // Hoisted: json! cannot parse an indexing expression inline.
            let status = ["open", "closed", "void"][i % 3];
            db.put("t", &i.to_string(),
                serde_json::json!({"status": status, "fee": i}),
                vec![], None, None).unwrap();
        }
        let (all, _) = query(&db, "FROM t GROUP BY status COUNT").unwrap();
        assert_eq!(all.len(), 3);
        let total: u64 = all.iter().filter_map(|r| r["count"].as_u64()).sum();
        assert_eq!(total, 12, "every input row must be counted");

        // LIMIT 2 must return 2 GROUPS, each with its full count — not two
        // input rows regrouped.
        let (limited, _) = query(&db, "FROM t GROUP BY status COUNT LIMIT 2").unwrap();
        assert_eq!(limited.len(), 2, "LIMIT caps the number of groups");
        for r in &limited {
            assert_eq!(r["count"], json!(4),
                       "each group keeps its true count, got {:?}", r);
        }
    }

    /// The second pipeline-order bug: ORDER BY ran before grouping, so it
    /// sorted the raw documents on a field that only exists AFTER grouping
    /// (`count`, `sum_fee`) and the grouped output came back unordered. The
    /// clause was silently inert.
    #[test]
    fn order_by_sorts_the_grouped_rows() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        // Deliberately uneven: 1 x "a", 3 x "b", 2 x "c".
        for (i, s) in ["a", "b", "b", "b", "c", "c"].iter().enumerate() {
            db.put("t", &i.to_string(),
                serde_json::json!({"g": s, "n": i}), vec![], None, None).unwrap();
        }
        let (rows, _) = query(&db, "FROM t GROUP BY g COUNT ORDER BY count DESC").unwrap();
        let got: Vec<(String, u64)> = rows.iter()
            .map(|r| (r["g"].as_str().unwrap().to_string(), r["count"].as_u64().unwrap()))
            .collect();
        assert_eq!(got, vec![("b".into(), 3), ("c".into(), 2), ("a".into(), 1)]);

        // And the group key itself is sortable.
        let (by_key, _) = query(&db, "FROM t GROUP BY g COUNT ORDER BY g DESC").unwrap();
        let keys: Vec<&str> = by_key.iter().map(|r| r["g"].as_str().unwrap()).collect();
        assert_eq!(keys, vec!["c", "b", "a"]);
    }

    #[test]
    fn order_by_an_aggregate_key() {
        let (_tmp, db) = setup_items();
        let (rows, _) = query(
            &db, "FROM items GROUP BY cat SUM price ORDER BY sum_price DESC").unwrap();
        let cats: Vec<&str> = rows.iter().map(|r| r["cat"].as_str().unwrap()).collect();
        assert_eq!(cats, vec!["y", "x"], "y sums to 60, x to 15");
    }

    #[test]
    fn offset_and_limit_page_grouped_rows() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        for i in 0..9 {
            db.put("t", &i.to_string(),
                serde_json::json!({"g": format!("g{}", i % 3)}), vec![], None, None).unwrap();
        }
        let (page, _) = query(
            &db, "FROM t GROUP BY g COUNT ORDER BY g LIMIT 1 OFFSET 1").unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0]["g"], "g1");
    }

    // ── HAVING ──────────────────────────────────────────────────────────────

    #[test]
    fn having_filters_groups_by_count() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        for (i, s) in ["a", "b", "b", "b", "c", "c"].iter().enumerate() {
            db.put("t", &i.to_string(),
                serde_json::json!({"g": s, "n": i}), vec![], None, None).unwrap();
        }
        let (rows, _) = query(&db, "FROM t GROUP BY g COUNT HAVING count > 1").unwrap();
        let mut keys: Vec<&str> = rows.iter().map(|r| r["g"].as_str().unwrap()).collect();
        keys.sort();
        assert_eq!(keys, vec!["b", "c"], "the single-row group `a` is filtered out");
    }

    #[test]
    fn having_filters_on_the_aggregate_value() {
        let (_tmp, db) = setup_items();
        // x sums to 15, y to 60.
        let (rows, _) = query(
            &db, "FROM items GROUP BY cat SUM price HAVING sum_price > 20").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["cat"], "y");
    }

    /// HAVING gets the full predicate surface, because it runs through the
    /// same evaluator as WHERE rather than a second copy.
    #[test]
    fn having_supports_the_whole_predicate_surface() {
        let (_tmp, db) = setup_items();
        let (in_, _) = query(
            &db, r#"FROM items GROUP BY cat COUNT HAVING cat IN ("x")"#).unwrap();
        assert_eq!(in_.len(), 1);
        assert_eq!(in_[0]["cat"], "x");

        let (btw, _) = query(
            &db, "FROM items GROUP BY cat SUM price HAVING sum_price BETWEEN 10 AND 20").unwrap();
        assert_eq!(btw.len(), 1);
        assert_eq!(btw[0]["cat"], "x");

        let (like, _) = query(
            &db, r#"FROM items GROUP BY cat COUNT HAVING cat LIKE "y""#).unwrap();
        assert_eq!(like.len(), 1);

        let (or_, _) = query(
            &db, "FROM items GROUP BY cat SUM price HAVING sum_price < 20 OR count = 3").unwrap();
        assert_eq!(or_.len(), 2);
    }

    /// WHERE filters input rows, HAVING filters groups. Confusing them gives
    /// different answers, so the distinction must hold.
    #[test]
    fn where_and_having_are_different_stages() {
        let (_tmp, db) = setup_items();
        // WHERE drops rows BEFORE grouping, shrinking the sums.
        let (w, _) = query(
            &db, "FROM items WHERE price > 10 GROUP BY cat SUM price").unwrap();
        let x = w.iter().find(|r| r["cat"] == "x");
        assert!(x.is_none(), "x's rows (0,5,10) are all filtered out by WHERE");

        // HAVING keeps every row in the aggregate and filters the RESULT.
        let (h, _) = query(
            &db, "FROM items GROUP BY cat SUM price HAVING sum_price > 10").unwrap();
        assert_eq!(h.len(), 2, "both groups sum above 10 when nothing is pre-filtered");
    }

    #[test]
    fn having_without_an_aggregate_is_an_error() {
        let (_tmp, db) = setup();
        // HAVING is meaningless without grouping, and silently treating it as
        // a second WHERE would be exactly the kind of reinterpretation this
        // parser no longer does.
        assert!(query(&db, "FROM blocks HAVING height > 3").is_err());
    }

    // ── Bare aggregates, no GROUP BY ────────────────────────────────────────

    /// `FROM t COUNT` — "how many rows match?" without fetching them. The
    /// July engine note recorded `SELECT COUNT(*)` returning `[]` silently;
    /// this is the capability that was missing behind that silence.
    #[test]
    fn bare_count_returns_one_row() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks COUNT").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["count"], json!(5));
        assert_eq!(rows[0]["value"], json!(5));
    }

    #[test]
    fn bare_count_respects_the_filter() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks WHERE height > 3 COUNT").unwrap();
        assert_eq!(rows[0]["count"], json!(2));
    }

    #[test]
    fn bare_sum_avg_min_max() {
        let (_tmp, db) = setup();
        // heights 1..=5, n_tx = h*2 -> 2,4,6,8,10
        let (s, _) = query(&db, "FROM blocks SUM n_tx").unwrap();
        assert_eq!(s[0]["sum_n_tx"], json!(30), "integer inputs give an integer sum");
        let (a, _) = query(&db, "FROM blocks AVG n_tx").unwrap();
        assert_eq!(a[0]["avg_n_tx"], json!(6.0));
        let (mn, _) = query(&db, "FROM blocks MIN n_tx").unwrap();
        assert_eq!(mn[0]["min_n_tx"], json!(2));
        let (mx, _) = query(&db, "FROM blocks MAX n_tx").unwrap();
        assert_eq!(mx[0]["max_n_tx"], json!(10));
    }

    /// Integer inputs must produce integer aggregates.
    ///
    /// Aggregating exclusively in f64 was a type divergence from the Python
    /// reference (which returns `66`, not `66.0`) AND a precision bug: f64
    /// cannot represent integers above 2^53 exactly, so a SUM over satoshi
    /// amounts or block heights silently rounded. This engine stores exactly
    /// that kind of number.
    #[test]
    fn integer_aggregates_stay_integers_and_keep_full_precision() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        // Beyond 2^53 (9_007_199_254_740_992), where f64 starts skipping
        // integers. Their true sum ends in ...9, which a f64 round-trip loses.
        let big: [i64; 3] = [9_007_199_254_740_993, 9_007_199_254_740_995, 1];
        for (i, v) in big.iter().enumerate() {
            db.put("t", &i.to_string(), serde_json::json!({"v": v}),
                   vec![], None, None).unwrap();
        }
        let (s, _) = query(&db, "FROM t SUM v").unwrap();
        assert_eq!(s[0]["sum_v"], json!(18_014_398_509_481_989i64),
                   "exact i64 sum, not a rounded f64");
        assert!(s[0]["sum_v"].is_i64(), "must serialise as an integer");

        let (mx, _) = query(&db, "FROM t MAX v").unwrap();
        assert_eq!(mx[0]["max_v"], json!(9_007_199_254_740_995i64));
        let (mn, _) = query(&db, "FROM t MIN v").unwrap();
        assert_eq!(mn[0]["min_v"], json!(1));
    }

    /// A float anywhere in the column makes the whole aggregate fractional,
    /// which is what Python's arithmetic does too.
    #[test]
    fn a_single_float_makes_the_aggregate_fractional() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.put("t", "1", serde_json::json!({"v": 1}), vec![], None, None).unwrap();
        db.put("t", "2", serde_json::json!({"v": 2.5}), vec![], None, None).unwrap();
        let (s, _) = query(&db, "FROM t SUM v").unwrap();
        assert_eq!(s[0]["sum_v"], json!(3.5));
        // AVG is true division, so it is fractional even over pure integers.
        let (a, _) = query(&db, "FROM t AVG v").unwrap();
        assert_eq!(a[0]["avg_v"], json!(1.75));
    }

    /// A JSON bool is not a number, matching Python's explicit
    /// `not isinstance(x, bool)` guard.
    #[test]
    fn booleans_are_not_aggregated_as_numbers() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.put("t", "1", serde_json::json!({"v": true}), vec![], None, None).unwrap();
        db.put("t", "2", serde_json::json!({"v": 5}), vec![], None, None).unwrap();
        let (s, _) = query(&db, "FROM t SUM v").unwrap();
        assert_eq!(s[0]["sum_v"], json!(5), "the bool contributes nothing");
        assert_eq!(s[0]["count"], json!(2), "but it still counts toward the group");
    }

    /// COUNT over an empty result is 0, not "no rows". A caller asking "how
    /// many?" must get a number.
    #[test]
    fn bare_count_of_nothing_is_zero_not_empty() {
        let (_tmp, db) = setup();
        let (rows, count) = query(&db, "FROM blocks WHERE height > 999 COUNT").unwrap();
        assert_eq!(count, 1, "still exactly one row");
        assert_eq!(rows[0]["count"], json!(0));

        // A GROUPED aggregate over zero rows correctly has no groups.
        let (g, _) = query(&db, "FROM blocks WHERE height > 999 GROUP BY height COUNT").unwrap();
        assert!(g.is_empty());
    }

    #[test]
    fn bare_aggregate_over_an_empty_collection() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM nonexistent COUNT").unwrap();
        assert_eq!(rows[0]["count"], json!(0));
        let (s, _) = query(&db, "FROM nonexistent SUM n_tx").unwrap();
        assert_eq!(s[0]["sum_n_tx"], Value::Null, "sum of nothing is null, not 0");
    }

    #[test]
    fn bare_aggregate_carries_no_group_key() {
        let (_tmp, db) = setup();
        let (rows, _) = query(&db, "FROM blocks COUNT").unwrap();
        if let Value::Object(m) = &rows[0] {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            assert_eq!(keys, vec!["count", "value"]);
        } else {
            panic!("expected an object");
        }
    }

    /// A document field whose name collides with a reserved word must be
    /// addressable. The lexer uppercases keywords for matching, and field
    /// positions accept a keyword as a field name — but they used the
    /// UPPERCASED text, so `WHERE count > 1` searched the document for "COUNT"
    /// and matched nothing. Silent, and it hit real field names: count, min,
    /// max, sum, avg, value, search, group, order, limit, offset, trace.
    #[test]
    fn a_field_named_like_a_keyword_is_still_addressable() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.put("t", "1", serde_json::json!({
            "count": 5, "min": 1, "max": 9, "sum": 3, "avg": 2,
            "value": "keep", "limit": 7, "offset": 8, "group": "g1", "search": "s",
        }), vec![], None, None).unwrap();
        db.put("t", "2", serde_json::json!({
            "count": 1, "min": 0, "max": 2, "sum": 0, "avg": 0,
            "value": "drop", "limit": 0, "offset": 0, "group": "g2", "search": "t",
        }), vec![], None, None).unwrap();

        for (nql, want) in [
            ("FROM t WHERE count > 3", "1"),
            ("FROM t WHERE min = 1", "1"),
            ("FROM t WHERE max >= 9", "1"),
            ("FROM t WHERE sum = 3", "1"),
            ("FROM t WHERE avg = 2", "1"),
            (r#"FROM t WHERE value = "keep""#, "1"),
            ("FROM t WHERE limit = 7", "1"),
            ("FROM t WHERE offset = 8", "1"),
            (r#"FROM t WHERE group = "g1""#, "1"),
        ] {
            let (rows, _) = query(&db, nql).unwrap();
            assert_eq!(rows.len(), 1, "`{}` matched {} rows", nql, rows.len());
            assert_eq!(rows[0]["_id"], want, "`{}`", nql);
        }

        // Sorting and grouping on such a field too.
        let (ord, _) = query(&db, "FROM t ORDER BY count DESC").unwrap();
        assert_eq!(ord[0]["_id"], "1");
        let (grp, _) = query(&db, "FROM t GROUP BY group COUNT").unwrap();
        assert_eq!(grp.len(), 2);
        let keys: Vec<&str> = grp.iter().filter_map(|r| r["group"].as_str()).collect();
        assert!(keys.contains(&"g1") && keys.contains(&"g2"), "{:?}", grp);
    }

    /// The raw spelling is preserved, so a mixed-case field name round-trips
    /// while the keyword it collides with still matches case-insensitively.
    #[test]
    fn keyword_matching_stays_case_insensitive() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.put("t", "1", serde_json::json!({"Count": 5, "n": 1}),
               vec![], None, None).unwrap();
        // Field spelled `Count`, clause keywords in lower case.
        let (rows, _) = query(&db, "from t where Count = 5 order by n").unwrap();
        assert_eq!(rows.len(), 1);
        // And a differently-cased field name does NOT collide with it.
        let (miss, _) = query(&db, "FROM t WHERE count = 5").unwrap();
        assert!(miss.is_empty(), "`count` and `Count` are distinct field names");
    }

    #[test]
    fn two_aggregates_is_an_error() {
        let (_tmp, db) = setup();
        assert!(query(&db, "FROM blocks COUNT SUM n_tx").is_err());
        assert!(query(&db, "FROM blocks GROUP BY height COUNT SUM n_tx").is_err());
    }

    #[test]
    fn bare_aggregate_with_having() {
        let (_tmp, db) = setup();
        let (keep, _) = query(&db, "FROM blocks COUNT HAVING count > 3").unwrap();
        assert_eq!(keep.len(), 1);
        let (drop, _) = query(&db, "FROM blocks COUNT HAVING count > 99").unwrap();
        assert!(drop.is_empty());
    }

    // ── GROUP BY parity with the Python reference (query.py + engine.py) ────

    /// Fixture mirroring tests/test_v050.py::test_group_by_min_max exactly:
    /// six items, cat x for 0..2 and y for 3..5, price = i * 5.
    fn setup_items() -> (tempfile::TempDir, Db) {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        for i in 0..6 {
            db.put("items", &i.to_string(),
                serde_json::json!({"cat": if i < 3 {"x"} else {"y"}, "price": i * 5}),
                vec![], None, None).unwrap();
        }
        (dir, db)
    }

    fn group(rows: &[Value], field: &str, key: &str) -> Value {
        rows.iter()
            .find(|r| r[field] == Value::String(key.to_string()))
            .unwrap_or_else(|| panic!("no group {:?} in {:?}", key, rows))
            .clone()
    }

    /// The aggregate must read the TARGET field. Before 3.3.0 the executor
    /// aggregated the GROUP BY field itself and the target was silently
    /// dropped by the unknown-token skip, so `MAX price` returned the max of
    /// `cat` — a non-numeric value coerced to 1.0, making every group answer
    /// 1. Python returns x:0 and y:25 for MIN/MAX respectively.
    #[test]
    fn group_by_aggregates_the_target_field_not_the_group_field() {
        let (_tmp, db) = setup_items();

        let (mins, _) = query(&db, "FROM items GROUP BY cat MIN price").unwrap();
        assert_eq!(group(&mins, "cat", "x")["min_price"], json!(0));
        assert_eq!(group(&mins, "cat", "y")["min_price"], json!(15));

        let (maxs, _) = query(&db, "FROM items GROUP BY cat MAX price").unwrap();
        assert_eq!(group(&maxs, "cat", "y")["max_price"], json!(25));
        assert_eq!(group(&maxs, "cat", "x")["max_price"], json!(10));

        let (sums, _) = query(&db, "FROM items GROUP BY cat SUM price").unwrap();
        assert_eq!(group(&sums, "cat", "x")["sum_price"], json!(15));  // 0+5+10
        assert_eq!(group(&sums, "cat", "y")["sum_price"], json!(60));  // 15+20+25

        let (avgs, _) = query(&db, "FROM items GROUP BY cat AVG price").unwrap();
        assert_eq!(group(&avgs, "cat", "x")["avg_price"], json!(5.0));
        assert_eq!(group(&avgs, "cat", "y")["avg_price"], json!(20.0));
    }

    /// An aggregate over a `_`-prefixed metadata field must see it.
    ///
    /// `_seq` lives on the node, not in its data payload, and the aggregator
    /// read the payload directly — so `MAX _seq` answered NULL while
    /// `SELECT _seq` listed the values and `WHERE _seq > 5` filtered on them.
    /// It was also a live divergence: the Python engine builds its groups from
    /// projected dicts that already carry `_seq`, and answers correctly.
    ///
    /// "What is the newest sequence?" is the question replication and time
    /// travel are built on, so a confident null there is the worst shape of
    /// wrong answer this engine can give.
    #[test]
    fn aggregates_see_node_metadata_not_only_the_payload() {
        let (_tmp, db) = setup_items();
        let (all, _) = query(&db, "FROM items").unwrap();
        let want = all.iter().filter_map(|r| r.get("_seq")?.as_i64()).max().unwrap();

        let (rows, _) = query(&db, "FROM items MAX _seq").unwrap();
        assert_eq!(rows[0]["max__seq"], json!(want),
                   "MAX _seq must equal the highest sequence in the result");
        assert_ne!(rows[0]["max__seq"], Value::Null, "a null here is a silent wrong answer");

        let (rows, _) = query(&db, "FROM items MIN _seq").unwrap();
        assert_eq!(rows[0]["min__seq"], json!(
            all.iter().filter_map(|r| r.get("_seq")?.as_i64()).min().unwrap()));

        // Grouping by a metadata field works through the same resolver.
        let (rows, _) = query(&db, "FROM items GROUP BY _seq COUNT").unwrap();
        assert_eq!(rows.len(), all.len(), "one group per distinct sequence");
        assert!(rows.iter().all(|r| r["_seq"] != Value::Null),
                "the group key must be the sequence, not null");
    }

    /// Output key parity: Python's engine.py emits `<agg>_<field>` and a
    /// `count`. This engine additionally keeps `value` as the alias it has
    /// always emitted, so existing callers keep working.
    #[test]
    fn group_by_emits_python_parity_keys_and_the_value_alias() {
        let (_tmp, db) = setup_items();
        let (rows, _) = query(&db, "FROM items GROUP BY cat SUM price").unwrap();
        let x = group(&rows, "cat", "x");
        assert_eq!(x["sum_price"], json!(15), "python-parity key");
        assert_eq!(x["value"], json!(15), "back-compat alias must agree");
        assert_eq!(x["count"], json!(3), "count is the group size");
    }

    /// `count` is the group size; the aggregate only sees numeric targets.
    /// A group of 3 where one row has a non-numeric price must still report
    /// count=3 while averaging over 2 — matching Python's isinstance filter.
    #[test]
    fn count_is_group_size_while_aggregate_skips_non_numeric() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.put("t", "1", serde_json::json!({"g": "a", "n": 10}), vec![], None, None).unwrap();
        db.put("t", "2", serde_json::json!({"g": "a", "n": 20}), vec![], None, None).unwrap();
        db.put("t", "3", serde_json::json!({"g": "a", "n": "N/A"}), vec![], None, None).unwrap();

        let (rows, _) = query(&db, "FROM t GROUP BY g AVG n").unwrap();
        let a = group(&rows, "g", "a");
        assert_eq!(a["count"], json!(3), "every row counts toward the group");
        assert_eq!(a["avg_n"], json!(15.0), "only the two numeric rows average");
    }

    /// An aggregate with no numeric input is null, not 0 and not infinity.
    #[test]
    fn empty_aggregate_input_is_null() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.put("t", "1", serde_json::json!({"g": "a", "n": "x"}), vec![], None, None).unwrap();
        let (rows, _) = query(&db, "FROM t GROUP BY g MIN n").unwrap();
        assert_eq!(rows[0]["min_n"], Value::Null);
        assert_eq!(rows[0]["count"], json!(1));
    }

    /// Python makes the aggregate keyword optional — `GROUP BY field` alone
    /// yields counts. Rust used to reject it as a parse error.
    #[test]
    fn bare_group_by_without_an_aggregate_counts() {
        let (_tmp, db) = setup_items();
        let (rows, _) = query(&db, "FROM items GROUP BY cat").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(group(&rows, "cat", "x")["count"], json!(3));
        assert_eq!(group(&rows, "cat", "y")["count"], json!(3));
    }

    /// SUM/AVG/MIN/MAX require a target field, as in Python.
    #[test]
    fn aggregate_without_a_target_field_is_an_error() {
        let (_tmp, db) = setup_items();
        for bad in [
            "FROM items GROUP BY cat SUM",
            "FROM items GROUP BY cat AVG",
            "FROM items GROUP BY cat MIN",
        ] {
            assert!(query(&db, bad).is_err(), "`{}` must be rejected", bad);
        }
    }

    /// Group output order is first-seen, so repeated runs agree. HashMap
    /// iteration order previously made this nondeterministic.
    #[test]
    fn group_order_is_stable_across_runs() {
        let (_tmp, db) = setup_items();
        let first = query(&db, "FROM items GROUP BY cat SUM price").unwrap().0;
        for _ in 0..8 {
            let again = query(&db, "FROM items GROUP BY cat SUM price").unwrap().0;
            assert_eq!(first, again);
        }
    }

    /// GROUP BY composes with the new predicate surface.
    #[test]
    fn group_by_after_an_in_predicate() {
        let (_tmp, db) = setup_items();
        let (rows, _) = query(
            &db, "FROM items WHERE price IN (0, 5, 25) GROUP BY cat SUM price").unwrap();
        assert_eq!(group(&rows, "cat", "x")["sum_price"], json!(5));
        assert_eq!(group(&rows, "cat", "y")["sum_price"], json!(25));
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

    // ── String-literal escaping ──────────────────────────────────────────────

    #[test]
    fn escaped_quote_matches_a_value_containing_a_quote() {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.put("m", "q", serde_json::json!({ "name": "say \"hi\"" }), vec![], None, None)
            .unwrap();
        db.put("m", "p", serde_json::json!({ "name": "plain" }), vec![], None, None).unwrap();

        // \" inside the literal is a literal quote; the string does not end there.
        let (rows, count) = query(&db, r#"FROM m WHERE name = "say \"hi\"""#).unwrap();
        assert_eq!(count, 1, "the escaped-quote literal matches exactly one row");
        assert_eq!(rows[0]["_id"], "q");
    }

    #[test]
    fn raw_backslash_still_matches_literally() {
        // REGRESSION GUARD: a lone backslash stays literal, so pre-existing
        // backslash queries (Windows paths etc.) keep matching. This is the
        // property that makes the \" addition non-breaking.
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.put("m", "b", serde_json::json!({ "p": "back\\slash" }), vec![], None, None).unwrap();

        let (rows, count) = query(&db, r#"FROM m WHERE p = "back\slash""#).unwrap();
        assert_eq!(count, 1, "a raw backslash literal matches as before");
        assert_eq!(rows[0]["_id"], "b");
    }

    #[test]
    fn a_quote_can_no_longer_inject_trailing_clauses() {
        // The security motivation: previously a value of `x" LIMIT 1` would
        // terminate the literal and inject `LIMIT 1`. With \" the caller can
        // escape the quote so it stays part of the value and matches nothing
        // rogue. Here the escaped form matches the literal value verbatim.
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), None).unwrap();
        db.put("m", "x", serde_json::json!({ "v": "a\"b" }), vec![], None, None).unwrap();
        let (rows, count) = query(&db, r#"FROM m WHERE v = "a\"b""#).unwrap();
        assert_eq!(count, 1);
        assert_eq!(rows[0]["_id"], "x");
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

        let (_rows, count) = query(&db, r#"FROM driver WHERE status = "active" TRAVERSE handles"#).unwrap();
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
