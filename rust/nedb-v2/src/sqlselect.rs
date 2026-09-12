// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

//! A real SQL `SELECT` engine — expressions, aliases, `CASE`, functions, joins.
//!
//! # Why this module exists
//!
//! The pgwire layer translates SQL text into NQL text. That works beautifully
//! for `SELECT col FROM t WHERE x = 1`, and it cannot be stretched any
//! further: NQL has no expressions, no table aliases, no `CASE`, no scalar
//! functions and no joins. Those are not missing features of the translation —
//! they are things the target language cannot say.
//!
//! And they are exactly what catalogue introspection is made of. `psql`'s
//! `\dt` is one statement containing a two-table `LEFT JOIN`, a nine-branch
//! `CASE`, two scalar function calls, four qualified column references, an
//! `IN` list, a `!~` regex and `ORDER BY 1,2`. Every one of those has to work
//! or the command does not.
//!
//! So this is a small but genuine SQL evaluator: lexer, parser, expression
//! evaluator, nested-loop join. It operates over rows supplied by a callback,
//! which is what lets the same engine serve synthesised catalogue relations
//! today and stored collections later.
//!
//! # What it is NOT
//!
//! It is not a query planner and does not pretend to be. The join is a nested
//! loop, which is honest for catalogue relations (tens of rows) and would be
//! wrong to point at a large collection without an index strategy. That
//! boundary is enforced by the caller, not hidden here.
//!
//! # The rule this module follows
//!
//! Anything it cannot evaluate is REFUSED with an error naming the construct.
//! It never guesses. A catalogue query that silently returns the wrong rows
//! produces an empty or wrong table list, and a wrong table list is
//! indistinguishable from a correct one until somebody's data appears to be
//! missing.

use crate::sqljoin::{self, JoinExec, Strategy};
use crate::sqlplan::{Plan, Stage};
use crate::sqlpush::Pushdown;

use anyhow::{bail, Result};
use serde_json::{Map, Value};

// ─────────────────────────────────────────────────────────────────────────────
// Phase 1 — the lexer
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    /// A bare identifier or keyword, with its canonical UPPERCASE form and the
    /// raw spelling. Both are kept for the same reason NQL keeps both: a
    /// column may legitimately be called `count` or `value`, and folding case
    /// at the lexer would look up a key the data does not have.
    Word { upper: String, raw: String },
    /// A `"double quoted"` identifier. Case is significant and it is NEVER a
    /// keyword — `"select"` is a column named select.
    Quoted(String),
    /// A `'single quoted'` string literal, with `''` already collapsed.
    Str(String),
    Num(f64),
    Op(String),
    Punct(char),
    Eof,
}

impl Tok {
    fn is_kw(&self, kw: &str) -> bool {
        matches!(self, Tok::Word { upper, .. } if upper == kw)
    }
    /// The identifier text, for a token usable as a name.
    #[allow(dead_code)] // kept as the counterpart to `is_kw`; used by earlier phases
    fn ident(&self) -> Option<String> {
        match self {
            Tok::Word { raw, .. } => Some(raw.clone()),
            Tok::Quoted(s) => Some(s.clone()),
            _ => None,
        }
    }
}

/// Operators, longest first. Order is load-bearing: `!~*` must be matched
/// before `!~`, which must be matched before `!=`, or each longer operator
/// tokenises as a shorter one plus garbage.
const OPERATORS: &[&str] = &[
    "!~*", "!~", "~*", "<>", "!=", ">=", "<=", "||", "::",
    "=", "<", ">", "~", "+", "-", "*", "/", "%",
];

pub fn lex(src: &str) -> Result<Vec<Tok>> {
    let b: Vec<char> = src.chars().collect();
    let mut out = vec![];
    let mut i = 0usize;

    while i < b.len() {
        let c = b[i];

        // whitespace
        if c.is_whitespace() {
            i += 1;
            continue;
        }

        // `-- line comment`
        if c == '-' && b.get(i + 1) == Some(&'-') {
            while i < b.len() && b[i] != '\n' {
                i += 1;
            }
            continue;
        }

        // `/* block comment */`, which SQL allows to nest.
        if c == '/' && b.get(i + 1) == Some(&'*') {
            let mut depth = 1usize;
            i += 2;
            while i < b.len() && depth > 0 {
                if b[i] == '/' && b.get(i + 1) == Some(&'*') {
                    depth += 1;
                    i += 2;
                } else if b[i] == '*' && b.get(i + 1) == Some(&'/') {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            if depth > 0 {
                bail!("unterminated /* comment");
            }
            continue;
        }

        // 'string literal', where '' is one literal quote.
        if c == '\'' {
            i += 1;
            let mut s = String::new();
            loop {
                match b.get(i) {
                    None => bail!("unterminated string literal"),
                    Some('\'') if b.get(i + 1) == Some(&'\'') => {
                        s.push('\'');
                        i += 2;
                    }
                    Some('\'') => {
                        i += 1;
                        break;
                    }
                    Some(ch) => {
                        s.push(*ch);
                        i += 1;
                    }
                }
            }
            out.push(Tok::Str(s));
            continue;
        }

        // E'escape string' — Postgres spells a newline this way inside
        // catalogue queries (`array_to_string(d.datacl, E'\n')`).
        if (c == 'E' || c == 'e') && b.get(i + 1) == Some(&'\'') {
            i += 2;
            let mut s = String::new();
            loop {
                match b.get(i) {
                    None => bail!("unterminated E'' string literal"),
                    Some('\\') => {
                        // Only the escapes that appear in real catalogue SQL.
                        // An unknown escape keeps its literal character rather
                        // than being dropped, so nothing silently vanishes.
                        let esc = b.get(i + 1).copied().unwrap_or('\\');
                        s.push(match esc {
                            'n' => '\n',
                            't' => '\t',
                            'r' => '\r',
                            '0' => '\0',
                            other => other,
                        });
                        i += 2;
                    }
                    Some('\'') if b.get(i + 1) == Some(&'\'') => {
                        s.push('\'');
                        i += 2;
                    }
                    Some('\'') => {
                        i += 1;
                        break;
                    }
                    Some(ch) => {
                        s.push(*ch);
                        i += 1;
                    }
                }
            }
            out.push(Tok::Str(s));
            continue;
        }

        // "quoted identifier", where "" is one literal quote.
        if c == '"' {
            i += 1;
            let mut s = String::new();
            loop {
                match b.get(i) {
                    None => bail!("unterminated quoted identifier"),
                    Some('"') if b.get(i + 1) == Some(&'"') => {
                        s.push('"');
                        i += 2;
                    }
                    Some('"') => {
                        i += 1;
                        break;
                    }
                    Some(ch) => {
                        s.push(*ch);
                        i += 1;
                    }
                }
            }
            out.push(Tok::Quoted(s));
            continue;
        }

        // number — digits, an optional fraction, an optional exponent.
        if c.is_ascii_digit()
            || (c == '.' && b.get(i + 1).map(|d| d.is_ascii_digit()).unwrap_or(false))
        {
            let start = i;
            while i < b.len() && (b[i].is_ascii_digit() || b[i] == '.') {
                i += 1;
            }
            if i < b.len() && (b[i] == 'e' || b[i] == 'E') {
                let save = i;
                i += 1;
                if i < b.len() && (b[i] == '+' || b[i] == '-') {
                    i += 1;
                }
                if i < b.len() && b[i].is_ascii_digit() {
                    while i < b.len() && b[i].is_ascii_digit() {
                        i += 1;
                    }
                } else {
                    i = save; // `1e` is the number 1 followed by the name `e`
                }
            }
            let text: String = b[start..i].iter().collect();
            let n: f64 = text
                .parse()
                .map_err(|_| anyhow::anyhow!("not a number: {:?}", text))?;
            out.push(Tok::Num(n));
            continue;
        }

        // identifier / keyword. `$` is legal in a Postgres identifier.
        if c.is_alphabetic() || c == '_' {
            let start = i;
            while i < b.len() && (b[i].is_alphanumeric() || b[i] == '_' || b[i] == '$') {
                i += 1;
            }
            let raw: String = b[start..i].iter().collect();
            out.push(Tok::Word { upper: raw.to_uppercase(), raw });
            continue;
        }

        // operator — longest match wins.
        let rest: String = b[i..].iter().take(3).collect();
        if let Some(op) = OPERATORS.iter().find(|o| rest.starts_with(**o)) {
            i += op.chars().count();
            out.push(Tok::Op((*op).to_string()));
            continue;
        }

        if matches!(c, '(' | ')' | ',' | ';' | '.' | '[' | ']') {
            out.push(Tok::Punct(c));
            i += 1;
            continue;
        }

        // Refused rather than skipped. Skipping an unknown character is how a
        // parser silently reads a different query than the one it was given.
        bail!("unexpected character {:?} in SQL", c);
    }

    out.push(Tok::Eof);
    Ok(out)
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 2 — the AST
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// `nspname` or `n.nspname`. The qualifier is kept because a join makes
    /// bare names ambiguous, and resolving an ambiguous name by guessing is
    /// how a query silently reads the wrong table's column.
    Column { qual: Option<String>, name: String },
    Literal(Value),
    /// `*` in `count(*)`, and in a bare select list.
    Star,
    /// `alias.*`
    QualifiedStar(String),
    Func { name: String, args: Vec<Expr> },
    /// Both SQL spellings:
    ///   simple   — `CASE x WHEN 'r' THEN 'table' ... ELSE ... END`
    ///   searched — `CASE WHEN x = 'r' THEN 'table' ... ELSE ... END`
    /// psql's `\dt` uses the simple form with nine branches.
    Case {
        operand: Option<Box<Expr>>,
        whens: Vec<(Expr, Expr)>,
        else_: Option<Box<Expr>>,
    },
    Binary { op: String, left: Box<Expr>, right: Box<Expr> },
    Unary { op: String, expr: Box<Expr> },
    /// `x [NOT] IN (a, b, c)`
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool },
    /// `x IS [NOT] NULL`
    IsNull { expr: Box<Expr>, negated: bool },
    /// `x::type` — the cast is PARSED and then ignored at evaluation, because
    /// this engine is dynamically typed. Ignoring it is safe for the shapes
    /// catalogue SQL uses (`prattrs::int2[]`), and the alternative — refusing
    /// every cast — would reject queries whose result the cast cannot change.
    Cast { expr: Box<Expr>, ty: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectItem {
    pub expr: Expr,
    /// The name the client sees. `None` means it is derived from the
    /// expression, the way Postgres derives it.
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind { Inner, Left, Right, Full, Cross }

#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    /// The table name as written, minus quoting. A `pg_catalog.` qualifier is
    /// preserved here and resolved by the caller, because `information_schema`
    /// table names collide with plausible user collection names.
    pub name: String,
    pub alias: Option<String>,
}

impl TableRef {
    /// How this table's columns are addressed: the alias when given, else the
    /// table's own bare name, which is what SQL says.
    pub fn binding(&self) -> String {
        self.alias.clone().unwrap_or_else(|| {
            self.name.rsplit('.').next().unwrap_or(&self.name).to_string()
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub kind: JoinKind,
    pub table: TableRef,
    pub on: Option<Expr>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Dir { Asc, Desc }

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    /// `ORDER BY 1` is an ORDINAL into the select list, not the number 1.
    /// psql's `\dt` ends with `ORDER BY 1,2`, so reading it as a constant
    /// would silently produce an unordered listing.
    pub ordinal: Option<usize>,
    pub expr: Option<Expr>,
    pub dir: Dir,
    /// Postgres defaults NULLS LAST for ASC and NULLS FIRST for DESC.
    pub nulls_first: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub distinct: bool,
    pub items: Vec<SelectItem>,
    pub from: Option<TableRef>,
    pub joins: Vec<Join>,
    pub where_: Option<Expr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 2b — the parser
// ─────────────────────────────────────────────────────────────────────────────

/// Binding power for a binary operator. Higher binds tighter.
///
/// Written as a table rather than as nested recursive-descent functions so the
/// precedence is READABLE and auditable in one place — a hand-rolled cascade
/// is where operator precedence bugs hide, and a precedence bug in a WHERE
/// clause silently returns the wrong rows.
fn binding_power(op: &str) -> Option<u8> {
    Some(match op {
        "OR" => 1,
        "AND" => 2,
        // Comparison and pattern matching sit at the same level, and are
        // non-associative in Postgres. Left association here is harmless
        // because chaining them is a type error anyway.
        "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=" | "~" | "~*" | "!~" | "!~*"
        | "LIKE" | "ILIKE" | "NOT LIKE" | "NOT ILIKE" => 4,
        "||" => 5,
        "+" | "-" => 6,
        "*" | "/" | "%" => 7,
        _ => return None,
    })
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> &Tok {
        self.toks.get(self.pos).unwrap_or(&Tok::Eof)
    }
    fn peek_at(&self, n: usize) -> &Tok {
        self.toks.get(self.pos + n).unwrap_or(&Tok::Eof)
    }
    fn next(&mut self) -> Tok {
        let t = self.peek().clone();
        self.pos += 1;
        t
    }
    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.peek().is_kw(kw) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn expect_kw(&mut self, kw: &str) -> Result<()> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            bail!("expected {} , got {:?}", kw, self.peek())
        }
    }
    fn eat_punct(&mut self, c: char) -> bool {
        if matches!(self.peek(), Tok::Punct(p) if *p == c) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn expect_punct(&mut self, c: char) -> Result<()> {
        if self.eat_punct(c) {
            Ok(())
        } else {
            bail!("expected {:?}, got {:?}", c, self.peek())
        }
    }
    fn eat_op(&mut self, op: &str) -> bool {
        if matches!(self.peek(), Tok::Op(o) if o == op) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    // ── expressions ─────────────────────────────────────────────────────────

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_bin(0)
    }

    /// Precedence climbing. One loop, one table, no cascade of near-identical
    /// functions to keep in sync.
    fn parse_bin(&mut self, min_bp: u8) -> Result<Expr> {
        let mut left = self.parse_unary()?;

        loop {
            // A word operator (AND / OR / LIKE / NOT LIKE) and a symbol
            // operator are both binary here; normalise to one string.
            // `OPERATOR(pg_catalog.~)` — Postgres's explicit operator
            // qualification, which psql generates throughout `\d`. It names
            // exactly the operator it wraps, so the schema is dropped and the
            // symbol is used directly.
            if self.peek().is_kw("OPERATOR") && matches!(self.peek_at(1), Tok::Punct('(')) {
                let save = self.pos;
                self.pos += 2;
                // Skip any `schema.` qualification before the symbol.
                let mut sym = None;
                while sym.is_none() {
                    match self.next() {
                        Tok::Op(o) => sym = Some(o),
                        Tok::Word { .. } | Tok::Punct('.') => continue,
                        _ => break,
                    }
                }
                match sym {
                    Some(o) if binding_power(&o).is_some() && self.eat_punct(')') => {
                        let bp = binding_power(&o).unwrap();
                        if bp < min_bp {
                            self.pos = save;
                            break;
                        }
                        let right = self.parse_bin(bp + 1)?;
                        left = Expr::Binary {
                            op: o,
                            left: Box::new(left),
                            right: Box::new(right),
                        };
                        continue;
                    }
                    // Not an operator we know: rewind so the caller reports
                    // the real position rather than a half-consumed clause.
                    _ => {
                        self.pos = save;
                        break;
                    }
                }
            }

            let (op, width) = match self.peek() {
                Tok::Op(o) if binding_power(o).is_some() => (o.clone(), 1usize),
                Tok::Word { upper, .. } if upper == "AND" || upper == "OR" => (upper.clone(), 1),
                Tok::Word { upper, .. } if upper == "LIKE" || upper == "ILIKE" => (upper.clone(), 1),
                Tok::Word { upper, .. } if upper == "NOT" => {
                    // `NOT LIKE` / `NOT ILIKE` / `NOT IN` / `NOT BETWEEN`.
                    match self.peek_at(1) {
                        Tok::Word { upper: u2, .. } if u2 == "LIKE" || u2 == "ILIKE" => {
                            (format!("NOT {}", u2), 2)
                        }
                        _ => break,
                    }
                }
                _ => break,
            };

            let bp = match binding_power(&op) {
                Some(bp) if bp >= min_bp => bp,
                _ => break,
            };
            self.pos += width;
            // Left-associative: the right side binds tighter than this level.
            let right = self.parse_bin(bp + 1)?;
            left = Expr::Binary { op, left: Box::new(left), right: Box::new(right) };
        }

        Ok(left)
    }

    fn parse_postfix(&mut self, mut e: Expr) -> Result<Expr> {
        loop {
            // IS [NOT] NULL
            if self.peek().is_kw("IS") {
                self.pos += 1;
                let negated = self.eat_kw("NOT");
                if !self.eat_kw("NULL") {
                    // `IS TRUE` / `IS FALSE` are the other legal spellings.
                    if self.eat_kw("TRUE") {
                        e = Expr::Binary {
                            op: "=".into(),
                            left: Box::new(e),
                            right: Box::new(Expr::Literal(Value::Bool(!negated))),
                        };
                        continue;
                    }
                    if self.eat_kw("FALSE") {
                        e = Expr::Binary {
                            op: "=".into(),
                            left: Box::new(e),
                            right: Box::new(Expr::Literal(Value::Bool(negated))),
                        };
                        continue;
                    }
                    bail!("expected NULL, TRUE or FALSE after IS, got {:?}", self.peek());
                }
                e = Expr::IsNull { expr: Box::new(e), negated };
                continue;
            }

            // [NOT] IN (...)
            let negated_in = if self.peek().is_kw("NOT") && self.peek_at(1).is_kw("IN") {
                self.pos += 2;
                true
            } else if self.peek().is_kw("IN") {
                self.pos += 1;
                false
            } else {
                // [NOT] BETWEEN a AND b
                let negated_between =
                    if self.peek().is_kw("NOT") && self.peek_at(1).is_kw("BETWEEN") {
                        self.pos += 2;
                        true
                    } else if self.peek().is_kw("BETWEEN") {
                        self.pos += 1;
                        false
                    } else {
                        break;
                    };
                // BETWEEN's bounds bind tighter than AND, so the bounds are
                // parsed at a level above AND — otherwise `BETWEEN a AND b`
                // swallows the AND as a boolean operator.
                let low = self.parse_bin(3)?;
                self.expect_kw("AND")?;
                let high = self.parse_bin(3)?;
                let ge = Expr::Binary {
                    op: ">=".into(),
                    left: Box::new(e.clone()),
                    right: Box::new(low),
                };
                let le = Expr::Binary {
                    op: "<=".into(),
                    left: Box::new(e),
                    right: Box::new(high),
                };
                let both = Expr::Binary {
                    op: "AND".into(),
                    left: Box::new(ge),
                    right: Box::new(le),
                };
                e = if negated_between {
                    Expr::Unary { op: "NOT".into(), expr: Box::new(both) }
                } else {
                    both
                };
                continue;
            };

            self.expect_punct('(')?;
            let mut list = vec![];
            if !self.eat_punct(')') {
                loop {
                    list.push(self.parse_expr()?);
                    if self.eat_punct(',') {
                        continue;
                    }
                    self.expect_punct(')')?;
                    break;
                }
            }
            e = Expr::InList { expr: Box::new(e), list, negated: negated_in };
        }
        Ok(e)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        if self.peek().is_kw("NOT") {
            self.pos += 1;
            // NOT binds looser than comparison, so its operand is parsed at
            // the comparison level: `NOT a = b` is `NOT (a = b)`.
            let e = self.parse_bin(3)?;
            return Ok(Expr::Unary { op: "NOT".into(), expr: Box::new(e) });
        }
        if self.eat_op("-") {
            let e = self.parse_unary()?;
            return Ok(Expr::Unary { op: "-".into(), expr: Box::new(e) });
        }
        if self.eat_op("+") {
            return self.parse_unary();
        }
        let atom = self.parse_atom()?;
        let cast = self.parse_casts(atom)?;
        // Postfix forms (`IS NULL`, `IN (...)`, `BETWEEN a AND b`) bind to the
        // OPERAND, before any binary operator is considered.
        //
        // They used to be applied after the binary loop in `parse_bin`, which
        // meant that once `IN (...)` was consumed the loop had already exited
        // and the rest of the predicate was left unparsed. psql's `\dt` is
        // `WHERE c.relkind IN (...) AND n.nspname <> '...' AND ...`, so
        // everything after the IN list silently became "trailing tokens" — and
        // a WHERE clause that loses its later conjuncts returns TOO MANY rows,
        // confidently.
        self.parse_postfix(cast)
    }

    /// `expr::type`, possibly repeated and possibly `type[]`, and `COLLATE`.
    fn parse_casts(&mut self, mut e: Expr) -> Result<Expr> {
        loop {
            // `COLLATE "C"` — psql writes it throughout `\d`. NEDB has one
            // collation, so it cannot change the answer; it is consumed rather
            // than refused, because refusing a clause that provably has no
            // effect would reject a query whose result is already correct.
            if self.peek().is_kw("COLLATE") {
                self.pos += 1;
                match self.next() {
                    Tok::Word { .. } | Tok::Quoted(_) => {}
                    other => bail!("expected a collation name after COLLATE, got {:?}", other),
                }
                // A schema-qualified collation: `pg_catalog."C"`.
                while self.eat_punct('.') {
                    match self.next() {
                        Tok::Word { .. } | Tok::Quoted(_) => {}
                        other => bail!("expected a name after '.', got {:?}", other),
                    }
                }
                continue;
            }
            if !self.eat_op("::") {
                break;
            }
            let mut ty = match self.next() {
                Tok::Word { raw, .. } => raw,
                Tok::Quoted(s) => s,
                other => bail!("expected a type name after ::, got {:?}", other),
            };
            // A schema-qualified type: `pg_catalog.int2`.
            while self.eat_punct('.') {
                match self.next() {
                    Tok::Word { raw, .. } => ty = raw,
                    Tok::Quoted(s) => ty = s,
                    other => bail!("expected a type name after ., got {:?}", other),
                }
            }
            // An array type: `int2[]`.
            while self.eat_punct('[') {
                self.expect_punct(']')?;
                ty.push_str("[]");
            }
            e = Expr::Cast { expr: Box::new(e), ty };
        }
        Ok(e)
    }


    fn parse_atom(&mut self) -> Result<Expr> {
        // ( expr ) — or a SUBQUERY, which is named rather than reported as a
        // stray parenthesis.
        //
        // "expected ')', got SELECT" is a parser internal and tells the reader
        // nothing about what to change. `\d` and `\dp` both hinge on
        // subqueries, so this is the message somebody will actually read.
        if self.eat_punct('(') {
            if self.peek().is_kw("SELECT") {
                bail!("a subquery is not supported by this SELECT path");
            }
            let e = self.parse_expr()?;
            self.expect_punct(')')?;
            return Ok(e);
        }

        // `ARRAY(SELECT ...)` and `EXISTS (SELECT ...)` — both appear in
        // psql's \dp, and both are subqueries wearing a function's clothes.
        if self.peek().is_kw("ARRAY") {
            bail!("the ARRAY(...) constructor is not supported by this SELECT path");
        }
        if self.peek().is_kw("EXISTS") {
            bail!("EXISTS (...) is not supported by this SELECT path");
        }

        // CASE
        if self.peek().is_kw("CASE") {
            return self.parse_case();
        }

        match self.next() {
            Tok::Num(n) => Ok(Expr::Literal(from_f64(n))),
            Tok::Str(s) => Ok(Expr::Literal(Value::String(s))),
            Tok::Op(o) if o == "*" => Ok(Expr::Star),
            Tok::Quoted(name) => self.parse_name_tail(None, name),
            Tok::Word { upper, raw } => match upper.as_str() {
                "NULL" => Ok(Expr::Literal(Value::Null)),
                "TRUE" => Ok(Expr::Literal(Value::Bool(true))),
                "FALSE" => Ok(Expr::Literal(Value::Bool(false))),
                // `CURRENT_SCHEMA` and friends are functions spelled without
                // parentheses. Treated as zero-argument calls so one evaluator
                // handles both spellings.
                "CURRENT_SCHEMA" | "CURRENT_DATABASE" | "CURRENT_USER" | "SESSION_USER"
                | "CURRENT_CATALOG" | "USER" | "VERSION"
                    if !matches!(self.peek(), Tok::Punct('(')) =>
                {
                    Ok(Expr::Func { name: upper.to_lowercase(), args: vec![] })
                }
                _ => self.parse_name_tail(None, raw),
            },
            other => bail!("unexpected {:?} in an expression", other),
        }
    }

    /// After an identifier: `.more`, `(args)`, or nothing.
    ///
    /// This is where `pg_catalog.pg_get_userbyid(x)` and `n.nspname` and a
    /// bare `relname` all get told apart, and the rule is positional: the LAST
    /// dotted part before a `(` is the function name; before anything else it
    /// is the column, and the part before it is the qualifier.
    fn parse_name_tail(&mut self, _schema: Option<String>, first: String) -> Result<Expr> {
        let mut parts = vec![first];
        while self.eat_punct('.') {
            // `c.*`
            if self.eat_op("*") {
                return Ok(Expr::QualifiedStar(parts.pop().unwrap_or_default()));
            }
            match self.next() {
                Tok::Word { raw, .. } => parts.push(raw),
                Tok::Quoted(s) => parts.push(s),
                other => bail!("expected a name after '.', got {:?}", other),
            }
        }

        // A call: the last part is the function, any earlier parts are its
        // schema and are dropped — `pg_catalog.pg_get_userbyid` is the same
        // function as `pg_get_userbyid`.
        if matches!(self.peek(), Tok::Punct('(')) {
            self.pos += 1;
            let name = parts.pop().unwrap_or_default().to_lowercase();
            let mut args = vec![];
            if !self.eat_punct(')') {
                loop {
                    // `count(*)`
                    if self.eat_op("*") {
                        args.push(Expr::Star);
                    } else {
                        args.push(self.parse_expr()?);
                    }
                    if self.eat_punct(',') {
                        continue;
                    }
                    self.expect_punct(')')?;
                    break;
                }
            }
            return Ok(Expr::Func { name, args });
        }

        let name = parts.pop().unwrap_or_default();
        // Only the IMMEDIATE qualifier matters: in `public.orders.id` the
        // binding is `orders`, and the schema is not part of how a column is
        // addressed.
        let qual = parts.pop();
        Ok(Expr::Column { qual, name })
    }

    fn parse_case(&mut self) -> Result<Expr> {
        self.expect_kw("CASE")?;
        // A simple CASE has an operand; a searched CASE goes straight to WHEN.
        let operand = if self.peek().is_kw("WHEN") {
            None
        } else {
            Some(Box::new(self.parse_expr()?))
        };
        let mut whens = vec![];
        while self.eat_kw("WHEN") {
            let cond = self.parse_expr()?;
            self.expect_kw("THEN")?;
            let then = self.parse_expr()?;
            whens.push((cond, then));
        }
        if whens.is_empty() {
            bail!("CASE needs at least one WHEN branch");
        }
        let else_ = if self.eat_kw("ELSE") {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect_kw("END")?;
        Ok(Expr::Case { operand, whens, else_ })
    }

    // ── the statement ───────────────────────────────────────────────────────

    fn parse_table_ref(&mut self) -> Result<TableRef> {
        let mut parts = vec![match self.next() {
            Tok::Word { raw, .. } => raw,
            Tok::Quoted(s) => s,
            other => bail!("expected a table name, got {:?}", other),
        }];
        while self.eat_punct('.') {
            match self.next() {
                Tok::Word { raw, .. } => parts.push(raw),
                Tok::Quoted(s) => parts.push(s),
                other => bail!("expected a name after '.', got {:?}", other),
            }
        }
        let name = parts.join(".");

        // `AS alias`, or a bare alias. A bare alias must not swallow a
        // keyword that starts the next clause, or `FROM t WHERE x` reads `t`
        // aliased as `WHERE`.
        let alias = if self.eat_kw("AS") {
            match self.next() {
                Tok::Word { raw, .. } => Some(raw),
                Tok::Quoted(s) => Some(s),
                other => bail!("expected an alias after AS, got {:?}", other),
            }
        } else {
            match self.peek().clone() {
                Tok::Word { upper, raw } if !is_clause_keyword(&upper) => {
                    self.pos += 1;
                    Some(raw)
                }
                Tok::Quoted(s) => {
                    self.pos += 1;
                    Some(s)
                }
                _ => None,
            }
        };
        Ok(TableRef { name, alias })
    }

    fn parse_select(&mut self) -> Result<Select> {
        self.expect_kw("SELECT")?;
        let distinct = self.eat_kw("DISTINCT");
        if distinct && self.peek().is_kw("ON") {
            bail!("DISTINCT ON is not supported");
        }
        let _ = self.eat_kw("ALL");

        let mut items = vec![];
        loop {
            let expr = self.parse_expr()?;
            // `AS "Name"`, or a bare alias that is not a clause keyword.
            let alias = if self.eat_kw("AS") {
                match self.next() {
                    Tok::Word { raw, .. } => Some(raw),
                    Tok::Quoted(s) => Some(s),
                    other => bail!("expected an alias after AS, got {:?}", other),
                }
            } else {
                match self.peek().clone() {
                    Tok::Word { upper, raw } if !is_clause_keyword(&upper) => {
                        self.pos += 1;
                        Some(raw)
                    }
                    Tok::Quoted(s) => {
                        self.pos += 1;
                        Some(s)
                    }
                    _ => None,
                }
            };
            items.push(SelectItem { expr, alias });
            if self.eat_punct(',') {
                continue;
            }
            break;
        }

        let mut from = None;
        let mut joins = vec![];
        if self.eat_kw("FROM") {
            from = Some(self.parse_table_ref()?);
            // A comma-separated FROM list is an implicit CROSS JOIN.
            while self.eat_punct(',') {
                let table = self.parse_table_ref()?;
                joins.push(Join { kind: JoinKind::Cross, table, on: None });
            }
            loop {
                let kind = if self.peek().is_kw("JOIN") {
                    self.pos += 1;
                    JoinKind::Inner
                } else if self.peek().is_kw("INNER") && self.peek_at(1).is_kw("JOIN") {
                    self.pos += 2;
                    JoinKind::Inner
                } else if self.peek().is_kw("CROSS") && self.peek_at(1).is_kw("JOIN") {
                    self.pos += 2;
                    JoinKind::Cross
                } else if self.peek().is_kw("LEFT") {
                    self.pos += 1;
                    let _ = self.eat_kw("OUTER");
                    self.expect_kw("JOIN")?;
                    JoinKind::Left
                } else if self.peek().is_kw("RIGHT") {
                    self.pos += 1;
                    let _ = self.eat_kw("OUTER");
                    self.expect_kw("JOIN")?;
                    JoinKind::Right
                } else if self.peek().is_kw("FULL") {
                    self.pos += 1;
                    let _ = self.eat_kw("OUTER");
                    self.expect_kw("JOIN")?;
                    JoinKind::Full
                } else {
                    break;
                };
                let table = self.parse_table_ref()?;
                let on = if self.eat_kw("ON") {
                    Some(self.parse_expr()?)
                } else if self.peek().is_kw("USING") {
                    bail!("JOIN ... USING is not supported — write ON a.col = b.col");
                } else {
                    None
                };
                if on.is_none() && !matches!(kind, JoinKind::Cross) {
                    bail!("a {:?} JOIN needs an ON clause", kind);
                }
                joins.push(Join { kind, table, on });
            }
        }

        let where_ = if self.eat_kw("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };

        if self.peek().is_kw("GROUP") {
            bail!("GROUP BY is not supported by this SELECT path");
        }
        if self.peek().is_kw("HAVING") {
            bail!("HAVING is not supported by this SELECT path");
        }

        let mut order_by = vec![];
        if self.eat_kw("ORDER") {
            self.expect_kw("BY")?;
            loop {
                // `ORDER BY 1` is an ORDINAL into the select list, not the
                // literal 1. Reading it as a constant sorts every row equally
                // and silently yields an unordered result.
                let (ordinal, expr) = match self.peek().clone() {
                    Tok::Num(n)
                        if n.fract() == 0.0
                            && n >= 1.0
                            && !matches!(self.peek_at(1), Tok::Op(_)) =>
                    {
                        self.pos += 1;
                        (Some(n as usize), None)
                    }
                    _ => (None, Some(self.parse_expr()?)),
                };
                let dir = if self.eat_kw("DESC") {
                    Dir::Desc
                } else {
                    let _ = self.eat_kw("ASC");
                    Dir::Asc
                };
                // Postgres defaults NULLS LAST for ASC, NULLS FIRST for DESC.
                let mut nulls_first = matches!(dir, Dir::Desc);
                if self.eat_kw("NULLS") {
                    if self.eat_kw("FIRST") {
                        nulls_first = true;
                    } else if self.eat_kw("LAST") {
                        nulls_first = false;
                    } else {
                        bail!("expected FIRST or LAST after NULLS, got {:?}", self.peek());
                    }
                }
                order_by.push(OrderBy { ordinal, expr, dir, nulls_first });
                if self.eat_punct(',') {
                    continue;
                }
                break;
            }
        }

        let mut limit = None;
        let mut offset = None;
        // Either order, and either may appear alone.
        loop {
            if self.eat_kw("LIMIT") {
                if self.eat_kw("ALL") {
                    limit = None;
                } else {
                    limit = Some(self.parse_count("LIMIT")?);
                }
                continue;
            }
            if self.eat_kw("OFFSET") {
                offset = Some(self.parse_count("OFFSET")?);
                let _ = self.eat_kw("ROW") || self.eat_kw("ROWS");
                continue;
            }
            break;
        }

        let _ = self.eat_punct(';');
        if !matches!(self.peek(), Tok::Eof) {
            bail!("unexpected trailing tokens: {:?}", self.peek());
        }

        Ok(Select { distinct, items, from, joins, where_, order_by, limit, offset })
    }

    fn parse_count(&mut self, what: &str) -> Result<usize> {
        match self.next() {
            Tok::Num(n) if n >= 0.0 && n.fract() == 0.0 => Ok(n as usize),
            other => bail!("{} expects a non-negative integer, got {:?}", what, other),
        }
    }
}

/// Keywords that begin a clause, and so can never be a bare alias.
///
/// Without this, `FROM pg_class WHERE x` parses `pg_class` aliased as
/// `WHERE` — and then the predicate vanishes and every row comes back.
fn is_clause_keyword(upper: &str) -> bool {
    matches!(
        upper,
        "FROM" | "WHERE" | "GROUP" | "HAVING" | "ORDER" | "LIMIT" | "OFFSET"
            | "JOIN" | "LEFT" | "RIGHT" | "FULL" | "INNER" | "CROSS" | "OUTER"
            | "ON" | "USING" | "AND" | "OR" | "AS" | "UNION" | "INTERSECT"
            | "EXCEPT" | "FETCH" | "FOR" | "WINDOW" | "RETURNING" | "INTO"
            | "ASC" | "DESC" | "NULLS" | "IS" | "IN" | "NOT" | "LIKE" | "ILIKE"
            | "BETWEEN" | "THEN" | "WHEN" | "ELSE" | "END" | "CASE" | "DISTINCT"
            | "SELECT" | "WITH" | "ALL"
    )
}

/// Parse one `SELECT` statement.
pub fn parse(sql: &str) -> Result<Select> {
    let toks = lex(sql)?;
    let mut p = Parser { toks, pos: 0 };
    p.parse_select()
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 3 — the evaluator
// ─────────────────────────────────────────────────────────────────────────────

/// One row of a (possibly joined) result: an ordered list of
/// `(binding, row-or-NULL)`.
///
/// `None` is a LEFT JOIN's unmatched side. Keeping it as `None` rather than an
/// empty map is what makes `n.nspname IS NULL` answer correctly for a row
/// that had no match — an empty map would report the column as absent, which
/// looks identical but loses the distinction between "no such column" and "no
/// matching row".
pub struct Bound<'a> {
    pub parts: Vec<(String, Option<&'a Value>)>,
}

impl<'a> Bound<'a> {
    /// Resolve a column reference.
    ///
    /// A qualified name looks only at its own binding. A bare name scans the
    /// bindings in order and takes the first that actually HAS the key —
    /// which is how SQL resolves an unambiguous bare column across a join.
    fn column(&self, qual: Option<&str>, name: &str) -> Value {
        match qual {
            Some(q) => {
                for (binding, row) in &self.parts {
                    if binding.eq_ignore_ascii_case(q) {
                        return row
                            .and_then(|r| r.get(name))
                            .cloned()
                            .unwrap_or(Value::Null);
                    }
                }
                Value::Null
            }
            None => {
                for (_, row) in &self.parts {
                    if let Some(v) = row.and_then(|r| r.get(name)) {
                        return v.clone();
                    }
                }
                Value::Null
            }
        }
    }

    /// Is `qual` a binding in this row at all? Used to tell "unknown table
    /// alias" (a query bug, worth an error) from "column absent in this row"
    /// (ordinary schemaless behaviour, worth a NULL).
    fn has_binding(&self, qual: &str) -> bool {
        self.parts.iter().any(|(b, _)| b.eq_ignore_ascii_case(qual))
    }

    /// Every column of every bound row, for `SELECT *`.
    fn flatten(&self) -> Vec<(String, Value)> {
        let mut out = vec![];
        for (_, row) in &self.parts {
            if let Some(Value::Object(m)) = row {
                for (k, v) in m {
                    out.push((k.clone(), v.clone()));
                }
            }
        }
        out
    }

    fn flatten_binding(&self, qual: &str) -> Vec<(String, Value)> {
        let mut out = vec![];
        for (binding, row) in &self.parts {
            if binding.eq_ignore_ascii_case(qual) {
                if let Some(Value::Object(m)) = row {
                    for (k, v) in m {
                        out.push((k.clone(), v.clone()));
                    }
                }
            }
        }
        out
    }
}

/// SQL truth: three-valued. `None` is UNKNOWN.
///
/// This is not pedantry. A LEFT JOIN produces NULL columns, and a predicate
/// over NULL must be UNKNOWN rather than false — because `NOT UNKNOWN` is
/// UNKNOWN, not true. Collapsing UNKNOWN to false would make
/// `WHERE NOT (n.nspname = 'x')` include unmatched rows that Postgres
/// excludes, and the row counts would silently disagree.
type Truth = Option<bool>;

fn truthy(v: &Value) -> Truth {
    match v {
        Value::Null => None,
        Value::Bool(b) => Some(*b),
        // A predicate position holding a non-boolean is a query error in
        // Postgres. Being lenient here would let `WHERE 1` mean something
        // different than it does there, so it is treated as UNKNOWN.
        _ => None,
    }
}

/// Compare two values for ordering and equality.
///
/// Numbers compare numerically, strings lexicographically, booleans false <
/// true. A number and a numeric-looking string compare NUMERICALLY, because
/// catalogue rows carry oids as numbers while a client may quote them.
fn cmp_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Null, _) | (_, Value::Null) => None,
        (Value::Number(x), Value::Number(y)) => {
            x.as_f64().partial_cmp(&y.as_f64())
        }
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        // Mixed number/string: try numeric first, then fall back to text, so
        // `oid = '16384'` behaves the way a Postgres client expects.
        (Value::Number(x), Value::String(y)) => match y.parse::<f64>() {
            Ok(n) => x.as_f64().partial_cmp(&Some(n)),
            Err(_) => Some(as_text(a).cmp(&as_text(b))),
        },
        (Value::String(x), Value::Number(y)) => match x.parse::<f64>() {
            Ok(n) => Some(n).partial_cmp(&y.as_f64()),
            Err(_) => Some(as_text(a).cmp(&as_text(b))),
        },
        _ => {
            let (x, y) = (as_text(a), as_text(b));
            if x == y { Some(Ordering::Equal) } else { Some(x.cmp(&y)) }
        }
    }
}

/// The text a user sees for a value — not its JSON encoding.
fn as_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        Value::Bool(b) => (if *b { "t" } else { "f" }).to_string(),
        other => other.to_string(),
    }
}

fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

/// Every number this engine PRODUCES goes through here, so that one rule
/// decides how numbers render.
///
/// An integral value becomes a JSON integer. Without this, the lexer's `f64`
/// leaked into the output and `SELECT 1` answered `1.0` — which a client reads
/// as the TEXT "1.0", where PostgreSQL says "1". The liveness probe every
/// driver opens with was the most visible casualty.
///
/// Note what this rule cannot do: PostgreSQL distinguishes `1` (integer) from
/// `1.0` (numeric with scale 1), and JSON has no numeric-with-scale type at
/// all, so that distinction is unrepresentable here whatever we choose.
/// Rendering integral values as integers is the only self-consistent option
/// available, and it is the one that matches the common case.
fn from_f64(f: f64) -> Value {
    if f.is_finite() && f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
        return Value::Number((f as i64).into());
    }
    serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(Value::Null)
}

/// Evaluate an expression against one bound row.
pub fn eval(e: &Expr, row: &Bound) -> Result<Value> {
    Ok(match e {
        Expr::Literal(v) => v.clone(),

        Expr::Column { qual, name } => {
            // An unknown ALIAS is a query bug and is reported. An unknown
            // COLUMN in a known binding is NULL, because a schemaless
            // document may legitimately omit any field.
            if let Some(q) = qual {
                if !row.has_binding(q) {
                    bail!("no table or alias named {:?} in this query", q);
                }
            }
            row.column(qual.as_deref(), name)
        }

        Expr::Cast { expr, .. } => eval(expr, row)?,

        Expr::Star | Expr::QualifiedStar(_) => {
            bail!("`*` is only valid in a select list or as count(*)")
        }

        Expr::Unary { op, expr } => {
            let v = eval(expr, row)?;
            match op.as_str() {
                "NOT" => match truthy(&v) {
                    // NOT UNKNOWN is UNKNOWN, not true.
                    None => Value::Null,
                    Some(b) => Value::Bool(!b),
                },
                "-" => match num(&v) {
                    Some(n) => from_f64(-n),
                    None => Value::Null,
                },
                other => bail!("unsupported unary operator {:?}", other),
            }
        }

        Expr::Binary { op, left, right } => {
            // AND / OR short-circuit on the value that decides the result, and
            // follow SQL's three-valued truth tables:
            //   false AND unknown = false      true  OR unknown = true
            //   true  AND unknown = unknown    false OR unknown = unknown
            if op == "AND" {
                let l = truthy(&eval(left, row)?);
                if l == Some(false) {
                    return Ok(Value::Bool(false));
                }
                let r = truthy(&eval(right, row)?);
                return Ok(match (l, r) {
                    (_, Some(false)) => Value::Bool(false),
                    (Some(true), Some(true)) => Value::Bool(true),
                    _ => Value::Null,
                });
            }
            if op == "OR" {
                let l = truthy(&eval(left, row)?);
                if l == Some(true) {
                    return Ok(Value::Bool(true));
                }
                let r = truthy(&eval(right, row)?);
                return Ok(match (l, r) {
                    (_, Some(true)) => Value::Bool(true),
                    (Some(false), Some(false)) => Value::Bool(false),
                    _ => Value::Null,
                });
            }

            let l = eval(left, row)?;
            let r = eval(right, row)?;

            // Every comparison over NULL is UNKNOWN — including `NULL = NULL`.
            let compare = |ord: fn(std::cmp::Ordering) -> bool| -> Value {
                match cmp_values(&l, &r) {
                    None => Value::Null,
                    Some(o) => Value::Bool(ord(o)),
                }
            };

            match op.as_str() {
                "=" => compare(|o| o.is_eq()),
                "!=" | "<>" => compare(|o| o.is_ne()),
                "<" => compare(|o| o.is_lt()),
                "<=" => compare(|o| o.is_le()),
                ">" => compare(|o| o.is_gt()),
                ">=" => compare(|o| o.is_ge()),

                "~" | "~*" | "!~" | "!~*" => {
                    if l.is_null() || r.is_null() {
                        Value::Null
                    } else {
                        let pat = as_text(&r);
                        if let Some(bad) = crate::nql::unsupported_regex_char_pub(&pat) {
                            bail!(
                                "regex {:?} uses {:?}, which this engine does not \
                                 implement. The supported subset is ^ $ . and \
                                 literal text",
                                pat, bad
                            );
                        }
                        let hit = crate::nql::regex_match_pub(
                            &as_text(&l), &pat, op.ends_with('*'));
                        Value::Bool(hit != op.starts_with('!'))
                    }
                }

                "LIKE" | "ILIKE" | "NOT LIKE" | "NOT ILIKE" => {
                    if l.is_null() || r.is_null() {
                        Value::Null
                    } else {
                        let hit = crate::nql::like_match_pub(
                            &as_text(&l), &as_text(&r), op.ends_with("ILIKE"));
                        Value::Bool(hit != op.starts_with("NOT"))
                    }
                }

                // String concatenation. NULL propagates, as in Postgres.
                "||" => {
                    if l.is_null() || r.is_null() {
                        Value::Null
                    } else {
                        Value::String(format!("{}{}", as_text(&l), as_text(&r)))
                    }
                }

                "+" | "-" | "*" | "/" | "%" => match (num(&l), num(&r)) {
                    (Some(a), Some(b)) => match op.as_str() {
                        "+" => from_f64(a + b),
                        "-" => from_f64(a - b),
                        "*" => from_f64(a * b),
                        // Division by zero is an ERROR in Postgres, not
                        // infinity. Returning inf would be a wrong number.
                        "/" if b == 0.0 => bail!("division by zero"),
                        "/" => from_f64(a / b),
                        "%" if b == 0.0 => bail!("division by zero"),
                        "%" => from_f64(a % b),
                        _ => unreachable!(),
                    },
                    _ => Value::Null,
                },

                other => bail!("unsupported operator {:?}", other),
            }
        }

        Expr::IsNull { expr, negated } => {
            let v = eval(expr, row)?;
            // `IS NULL` is the one predicate that is never UNKNOWN — it always
            // answers true or false, which is exactly why it exists.
            Value::Bool(v.is_null() != *negated)
        }

        Expr::InList { expr, list, negated } => {
            let v = eval(expr, row)?;
            if v.is_null() {
                return Ok(Value::Null);
            }
            let mut any_null = false;
            let mut found = false;
            for item in list {
                let iv = eval(item, row)?;
                if iv.is_null() {
                    any_null = true;
                    continue;
                }
                if matches!(cmp_values(&v, &iv), Some(std::cmp::Ordering::Equal)) {
                    found = true;
                    break;
                }
            }
            // `x NOT IN (1, NULL)` is UNKNOWN rather than true when x is not
            // 1 — because x might equal the NULL. Postgres agrees, and this
            // is the classic NOT IN trap.
            if found {
                Value::Bool(!*negated)
            } else if any_null {
                Value::Null
            } else {
                Value::Bool(*negated)
            }
        }

        Expr::Case { operand, whens, else_ } => {
            let subject = match operand {
                Some(o) => Some(eval(o, row)?),
                None => None,
            };
            for (cond, then) in whens {
                let hit = match &subject {
                    // simple CASE: compare the operand to each WHEN value.
                    Some(sv) => {
                        let cv = eval(cond, row)?;
                        matches!(cmp_values(sv, &cv), Some(std::cmp::Ordering::Equal))
                    }
                    // searched CASE: each WHEN is a predicate, and UNKNOWN
                    // does not match.
                    None => truthy(&eval(cond, row)?) == Some(true),
                };
                if hit {
                    return eval(then, row);
                }
            }
            match else_ {
                Some(e) => eval(e, row)?,
                // A CASE with no matching branch and no ELSE is NULL, which is
                // exactly what psql's \dt relies on for an unknown relkind.
                None => Value::Null,
            }
        }

        Expr::Func { name, args } => eval_func(name, args, row)?,
    })
}

/// Scalar functions.
///
/// Only what real clients actually call. An unknown function is REFUSED by
/// name rather than returning NULL — a NULL would flow into a result set as a
/// blank column and look like missing data rather than a missing feature.
fn eval_func(name: &str, args: &[Expr], row: &Bound) -> Result<Value> {
    // Evaluated lazily per arm, because `coalesce` must not error on a later
    // argument once an earlier one is non-null.
    let arg = |i: usize| -> Result<Value> {
        match args.get(i) {
            Some(e) => eval(e, row),
            None => Ok(Value::Null),
        }
    };

    Ok(match name {
        // ── identity / session ──────────────────────────────────────────────
        // NEDB presents a single role and a single schema; reporting them
        // consistently is what lets a client's "who am I" probe succeed.
        "pg_get_userbyid" | "current_user" | "session_user" | "user" => {
            Value::String("nedb".into())
        }
        "current_schema" => Value::String("public".into()),
        "current_database" | "current_catalog" => Value::String("nedb".into()),
        "version" => Value::String(crate::pgwire::version_string()),

        // ── visibility ──────────────────────────────────────────────────────
        // Every relation NEDB reports is in `public` and reachable on the
        // search path, so visibility is unconditionally true. Returning false
        // would hide every table from `\dt`.
        "pg_table_is_visible" | "pg_type_is_visible" | "pg_function_is_visible"
        | "pg_opclass_is_visible" | "pg_conversion_is_visible" => Value::Bool(true),

        // ── encoding ────────────────────────────────────────────────────────
        "pg_encoding_to_char" => Value::String("UTF8".into()),
        "pg_get_expr" | "pg_get_indexdef" | "pg_get_constraintdef"
        | "pg_get_viewdef" | "pg_get_partkeydef" | "obj_description"
        | "col_description" | "shobj_description" => Value::Null,

        // ── text ────────────────────────────────────────────────────────────
        "lower" => match arg(0)? {
            Value::Null => Value::Null,
            v => Value::String(as_text(&v).to_lowercase()),
        },
        "upper" => match arg(0)? {
            Value::Null => Value::Null,
            v => Value::String(as_text(&v).to_uppercase()),
        },
        "length" | "char_length" | "character_length" => match arg(0)? {
            Value::Null => Value::Null,
            v => from_f64(as_text(&v).chars().count() as f64),
        },
        "format_type" => match arg(0)? {
            Value::Null => Value::Null,
            v => Value::String(crate::pgcatalog::type_name_pub(
                num(&v).unwrap_or(25.0) as i32).to_string()),
        },
        "array_to_string" | "pg_catalog.array_to_string" => {
            // NEDB stores no arrays in the catalogue, so an ACL column is
            // NULL and joining it yields NULL — the same as Postgres for a
            // relation with default privileges.
            match arg(0)? {
                Value::Array(items) => {
                    let sep = as_text(&arg(1)?);
                    Value::String(
                        items.iter().map(as_text).collect::<Vec<_>>().join(&sep),
                    )
                }
                _ => Value::Null,
            }
        }
        "quote_ident" => Value::String(as_text(&arg(0)?)),

        // ── null handling ───────────────────────────────────────────────────
        "coalesce" => {
            let mut out = Value::Null;
            for a in args {
                let v = eval(a, row)?;
                if !v.is_null() {
                    out = v;
                    break;
                }
            }
            out
        }
        "nullif" => {
            let a = arg(0)?;
            let b = arg(1)?;
            if matches!(cmp_values(&a, &b), Some(std::cmp::Ordering::Equal)) {
                Value::Null
            } else {
                a
            }
        }

        // ── casts spelled as functions ──────────────────────────────────────
        "int4" | "int8" | "int2" => match num(&arg(0)?) {
            Some(n) => from_f64(n.trunc()),
            None => Value::Null,
        },
        "text" => match arg(0)? {
            Value::Null => Value::Null,
            v => Value::String(as_text(&v)),
        },

        other => bail!(
            "the function {}() is not implemented. It is refused rather than \
             answered with NULL, because a NULL column reads as missing DATA \
             rather than a missing feature",
            other
        ),
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 4 — execution
// ─────────────────────────────────────────────────────────────────────────────

/// Every relation in a query must be addressable by a DISTINCT name.
///
/// PostgreSQL rejects `FROM a JOIN a` with "table name a specified more than
/// once". This engine used to accept it and answer WRONGLY: a qualified
/// reference scans the bindings in order and takes the first match, so both
/// `a.x` and `a.y` read the same row, and `FROM emp JOIN emp ON emp.mgr =
/// emp.id` compared every row to ITSELF and returned no rows at all.
///
/// A silently empty result is the worst possible answer — it is
/// indistinguishable from "there is no such data". Refusing is strictly
/// better, and the supported spelling is one alias per relation.
fn validate_bindings(sel: &Select) -> Result<()> {
    let mut seen: Vec<String> = vec![];
    if let Some(f) = &sel.from {
        seen.push(f.binding());
    }
    for j in &sel.joins {
        seen.push(j.table.binding());
    }
    for (i, b) in seen.iter().enumerate() {
        if let Some(prev) = seen[..i].iter().find(|p| p.eq_ignore_ascii_case(b)) {
            bail!(
                "ambiguous relation binding: {:?} appears more than once; use \
                 aliases (for example `FROM {} JOIN {} AS {}2 ...`)",
                prev, prev, prev, prev
            );
        }
    }
    Ok(())
}

/// One output column: the key it is stored under, and the name the client sees.
///
/// These are NOT always the same, and that is the whole point. PostgreSQL
/// permits duplicate output names — `SELECT e.name, e2.name` legitimately
/// returns two columns both called `name`, and generated SQL relies on it.
/// Rows here are JSON objects, so two columns sharing a key would share a
/// VALUE: the second write silently overwrote the first, and the query above
/// returned the same value twice while reporting two columns.
///
/// So the key is made unique and the display name is left alone. Renaming the
/// column instead would be worse — generated SQL asks for the name it wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutCol {
    pub key: String,
    pub name: String,
}

/// A key no user field can collide with, for the second and later columns
/// sharing a display name. `\u{1}` is not producible in a JSON field name by
/// any sane writer, and the index disambiguates even if one managed it.
fn unique_key(taken: &[OutCol], name: &str) -> String {
    if !taken.iter().any(|c| c.key == name) {
        return name.to_string();
    }
    format!("{name}\u{1}{}", taken.len())
}

/// A joined row, owned: `(binding, row-or-NULL)` per source table.
type JoinedRow = Vec<(String, Option<Value>)>;

fn bind<'a>(row: &'a JoinedRow) -> Bound<'a> {
    Bound {
        parts: row.iter().map(|(b, v)| (b.clone(), v.as_ref())).collect(),
    }
}

/// The name a client sees for a select item, when no `AS` was given.
///
/// Postgres derives it: a bare column keeps its column name, a function call
/// takes the function's name, and anything else becomes `?column?`. Matching
/// that matters because clients index result columns BY NAME — psycopg's
/// `RealDictCursor` and every ORM do — so inventing a different name breaks
/// code that would work against Postgres.
fn derived_name(e: &Expr) -> String {
    match e {
        Expr::Column { name, .. } => name.clone(),
        Expr::Func { name, .. } => name.clone(),
        Expr::Cast { expr, .. } => derived_name(expr),
        Expr::Case { .. } => "case".to_string(),
        _ => "?column?".to_string(),
    }
}

/// A relation, delivered one row at a time.
///
/// # The smallest interface that permits early termination
///
/// The previous contract handed back an owned `Vec<Value>`, which forced the
/// whole relation to exist before any work could start. That is fine until
/// execution can stop early — and once `LIMIT` can stop a join, a contract
/// that insists on materialising 8000 rows to return 20 becomes the
/// bottleneck. It was measured as exactly that: after the filter fusion in
/// #120, the hash path's remaining time was dominated by cloning relations
/// rather than probing them.
///
/// So this is deliberately two methods, not an async stream and not a
/// borrowing iterator with a lifetime parameter threaded through the whole
/// evaluator. Pull a row; stop whenever you like by dropping it.
///
/// [`size_hint`](Relation::size_hint) exists only so the join planner can
/// keep choosing a strategy from relation sizes. A source that genuinely does
/// not know returns `None`, and the planner then decides from what it does
/// know rather than pretending.
pub trait Relation {
    /// The next row, or `None` when exhausted.
    fn next_row(&mut self) -> Result<Option<Value>>;

    /// Exact row count when the source knows it, `None` when it does not.
    fn size_hint(&self) -> Option<usize> {
        None
    }
}

/// A relation backed by an already-materialised `Vec`.
///
/// Every current caller uses this, so the interface change on its own alters
/// no behaviour — it is what lets the executor become demand-driven ahead of
/// the storage layer, rather than requiring both to move at once.
pub struct VecRelation {
    iter: std::vec::IntoIter<Value>,
    len: usize,
}

impl Relation for VecRelation {
    fn next_row(&mut self) -> Result<Option<Value>> {
        Ok(self.iter.next())
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.len)
    }
}

/// Wrap a materialised relation.
pub fn from_vec(rows: Vec<Value>) -> Box<dyn Relation> {
    let len = rows.len();
    Box::new(VecRelation { iter: rows.into_iter(), len })
}

/// Everything one execution needs from the outside world.
///
/// A callback rather than a concrete store, which is what lets this engine
/// serve synthesised catalogue relations today and stored collections later
/// without knowing the difference.
pub type Resolver<'r> = dyn Fn(&str) -> Result<Option<Box<dyn Relation>>> + 'r;

/// Run a parsed `SELECT`, returning `(column names, rows)`.
///
/// Rows come back as JSON objects keyed by output column name, which is the
/// shape the wire encoder already consumes.
pub fn execute(sel: &Select, resolve: &Resolver) -> Result<(Vec<OutCol>, Vec<Value>)> {
    let (cols, rows, _) = execute_explain(sel, resolve, JoinExec::Auto)?;
    Ok((cols, rows))
}

/// Run a parsed `SELECT`, also reporting how each join was executed.
///
/// `exec` forces a join strategy, which exists so that differential tests can
/// drive the SAME query down BOTH paths — and so a benchmark can prove it
/// measured the path it claims to have measured rather than silently timing
/// the other one twice.
pub fn execute_explain(
    sel: &Select,
    resolve: &Resolver,
    exec: JoinExec,
) -> Result<(Vec<OutCol>, Vec<Value>, Plan)> {
    execute_with(sel, resolve, exec, true)
}

/// Execution options. Every switch exists so a differential test can run the
/// SAME query with the optimisation on and off and compare — without that, a
/// test believing it exercised an optimisation could be measuring the
/// unoptimised path, and the equivalence suite would prove nothing.
#[derive(Debug, Clone, Copy)]
pub struct Opts {
    pub exec: JoinExec,
    pub pushdown: bool,
    /// Evaluate the `WHERE` clause inside the final join rather than as a
    /// separate pass. Semantically identical; it is what lets the row budget
    /// apply to a filtered join.
    pub fuse_filter: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Opts { exec: JoinExec::Auto, pushdown: true, fuse_filter: true }
    }
}

impl Opts {
    pub fn exec(exec: JoinExec) -> Self {
        Opts { exec, ..Default::default() }
    }
}

/// As [`execute_explain`], with predicate pushdown switchable.
///
/// The switch exists so differential tests can run the SAME query with and
/// without the rewrite and compare. Without it, a test believing it exercised
/// pushdown could be measuring the unoptimised path, and the equivalence suite
/// would prove nothing — the same reason `JoinExec` can force a strategy.
pub fn execute_with(
    sel: &Select,
    resolve: &Resolver,
    exec: JoinExec,
    pushdown: bool,
) -> Result<(Vec<OutCol>, Vec<Value>, Plan)> {
    execute_opts(sel, resolve, Opts { exec, pushdown, ..Default::default() })
}

/// The full form.
pub fn execute_opts(
    sel: &Select,
    resolve: &Resolver,
    opts: Opts,
) -> Result<(Vec<OutCol>, Vec<Value>, Plan)> {
    let exec = opts.exec;
    let pushdown = opts.pushdown;
    let mut plan = Plan::default();

    // ── 0a. semantic validation, before any work ────────────────────────────
    validate_bindings(sel)?;

    // ── 0. the row budget ───────────────────────────────────────────────────
    //
    // The only safe rewrite available without a streaming executor: when the
    // final answer is a PREFIX of the join's output, the join may stop as soon
    // as it has produced enough rows.
    //
    // Every one of these conditions is load-bearing, and each corresponds to
    // an operation that can REDUCE the row count after the join — capping the
    // join's output early would then starve it:
    //
    //   * `ORDER BY` — the prefix depends on the sort, not on emission order.
    //   * `DISTINCT` — deduplication can shrink 100 rows to 3.
    //   * a `WHERE` clause — filtering happens after the join here.
    //   * more than one join — an intermediate cap can starve a later join.
    //
    // `OFFSET` is added to the budget rather than disqualifying it, because
    // the rows skipped still have to be produced.
    //
    // This is narrow on purpose. `SELECT ... JOIN ... LIMIT n` is the shape an
    // interactive client sends constantly, and it was measured taking 32ms to
    // return 20 rows out of an 8000-row join. A wider rewrite needs a
    // streaming executor, not a cleverer predicate.
    // Fusing the `WHERE` into the final join is what makes a filtered query
    // eligible: the join's own output is then already filtered, so its length
    // is a real count of final rows and stopping early keeps a true prefix.
    // Without the fusion a `WHERE` had to disqualify the budget entirely.
    let fuse = opts.fuse_filter && sel.where_.is_some() && !sel.joins.is_empty();

    let budget: Option<usize> = match sel.limit {
        Some(lim)
            if sel.order_by.is_empty()
                && !sel.distinct
                && !sel.joins.is_empty()
                && (sel.where_.is_none() || fuse) =>
        {
            Some(lim.saturating_add(sel.offset.unwrap_or(0)))
        }
        _ => None,
    };
    plan.budget = budget;

    // ── 0b. predicate pushdown ──────────────────────────────────────────────
    // Conjuncts of the WHERE clause that read exactly one relation are COPIED
    // to pre-filter that relation before the join. The WHERE clause below is
    // untouched and still runs afterwards — a copy, never a move, which is
    // what keeps this safe for outer joins. See `sqlpush` for the argument.
    let all_bindings: Vec<String> = sel
        .from
        .iter()
        .map(|t| t.binding())
        .chain(sel.joins.iter().map(|j| j.table.binding()))
        .collect();
    let nullable = crate::sqlpush::nullable_bindings(sel);
    let push = if pushdown {
        crate::sqlpush::plan(sel.where_.as_ref(), &all_bindings, &nullable)
    } else {
        Pushdown::default()
    };
    plan.refusals = push.refusals.clone();

    let mut base_scan_at: Option<usize> = None;
    let mut base_prefilter_at: Option<usize> = None;

    // ── 1. source rows, and the join ────────────────────────────────────────
    //
    // The driving relation is STREAMED when there is a join to feed it into,
    // so a query that stops early never asks the source for the rest. The
    // inner side of each join is materialised, because it genuinely has to
    // be: a hash join builds its table before probing, and a nested loop
    // re-scans it per left row.
    let mut left_src: Box<dyn LeftSource> = match &sel.from {
        None => {
            // `SELECT 1` with no FROM is one row with no columns — which is
            // how a client's liveness probe is written.
            Box::new(VecLeft { rows: vec![vec![]], at: 0 })
        }
        Some(t) => {
            let rel = fetch(&t.name, resolve)?;
            let binding = t.binding();
            // Placeholder counts, patched once the pull is over. A streamed
            // relation cannot report its `actual rows` before it is read, and
            // inventing a number would be exactly the kind of plausible
            // fiction `EXPLAIN` must never contain.
            base_scan_at = Some(plan.stages.len());
            plan.push(Stage::Scan {
                table: t.name.clone(),
                binding: binding.clone(),
                rows: 0,
            });
            let preds = push.for_binding(&binding).cloned().unwrap_or_default();
            if !preds.is_empty() {
                base_prefilter_at = Some(plan.stages.len());
                plan.push(Stage::Prefilter {
                    binding: binding.clone(),
                    predicates: preds.len(),
                    in_rows: 0,
                    out_rows: 0,
                });
            }
            Box::new(StreamLeft { rel, binding, preds, pulled: 0, kept: 0 })
        }
    };

    // The bindings accumulated so far, tracked explicitly rather than read off
    // the first row. Reading a row cannot describe the shape when there are no
    // rows — which is exactly the case a `RIGHT JOIN` onto an EMPTY left
    // relation produces, and it made those rows come back missing their left
    // bindings entirely instead of carrying them as NULL.
    let mut left_bindings: Vec<String> = match &sel.from {
        None => vec![],
        Some(t) => vec![t.binding()],
    };
    let last = sel.joins.len().saturating_sub(1);
    let mut rows: Vec<JoinedRow> = vec![];
    let mut base_pulled: Option<usize> = None;
    let mut base_kept: Option<usize> = None;

    for (ji, join) in sel.joins.iter().enumerate() {
        let right_rel = fetch(&join.table.name, resolve)?;
        let rb = join.table.binding();
        let right_all = drain(right_rel)?;
        plan.push(Stage::Scan {
            table: join.table.name.clone(),
            binding: rb.clone(),
            rows: right_all.len(),
        });
        let right_rows = prefilter(right_all, &rb, &push, &mut plan)?;

        // The filter can only be evaluated once every binding it reads is
        // bound, so it fuses into the FINAL join and nowhere earlier. The
        // budget likewise applies only there: capping an intermediate join
        // can starve a later one of rows it needed.
        let is_last = ji == last;
        let post = if fuse && is_last { sel.where_.as_ref() } else { None };
        let join_budget = if is_last { budget } else { None };

        // The planner proposes; sizes decide. A join with no provable equality
        // key has nothing to hash on and stays on the reference path.
        let keys = sqljoin::hash_keys(join.on.as_ref(), &left_bindings, &rb);
        let left_hint = left_src.hint().unwrap_or(usize::MAX);
        let strategy = sqljoin::choose(exec, keys.len(), left_hint, right_rows.len());

        let (out, removed, consumed) = match strategy {
            Strategy::NestedLoop => join_nested_loop(
                left_src.as_mut(), &left_bindings, join, &right_rows, &rb,
                join_budget, post,
            )?,
            Strategy::Hash => join_hash(
                left_src.as_mut(), &left_bindings, join, &right_rows, &rb, &keys,
                join_budget, post,
            )?,
        };

        plan.push(Stage::Join {
            kind: join.kind,
            table: join.table.name.clone(),
            binding: rb.clone(),
            strategy,
            keys: keys.len(),
            left_rows: consumed,
            right_rows: right_rows.len(),
            out_rows: out.len(),
            early_stopped: join_budget.is_some_and(|b| out.len() >= b),
            post_filter_removed: post.map(|_| removed),
        });
        left_bindings.push(rb);
        // Read the streamed base's counts BEFORE the source is replaced.
        if ji == 0 {
            if let Some((pulled, kept)) = left_src.stats() {
                base_pulled = Some(pulled);
                base_kept = Some(kept);
            }
        }
        rows = out;
        // The next join reads this join's output, which is already whole.
        left_src = Box::new(VecLeft { rows: std::mem::take(&mut rows), at: 0 });
    }

    // Recover the rows from the last source, and record what the streamed
    // base relation actually delivered.
    rows = left_src.take_rows();
    if let Some(i) = base_scan_at {
        if let (Some(pulled), Some(kept)) = (base_pulled, base_kept) {
            if let Some(Stage::Scan { rows: r, .. }) = plan.stages.get_mut(i) {
                *r = pulled;
            }
            if let Some(j) = base_prefilter_at {
                if let Some(Stage::Prefilter { in_rows, out_rows, .. }) =
                    plan.stages.get_mut(j)
                {
                    *in_rows = pulled;
                    *out_rows = kept;
                }
            }
        }
    }

    // ── 2. WHERE ────────────────────────────────────────────────────────────
    if let Some(pred) = sel.where_.as_ref().filter(|_| !fuse) {
        let in_rows = rows.len();
        let mut kept = Vec::with_capacity(rows.len());
        for r in rows {
            // Only TRUE keeps a row. UNKNOWN excludes it, which is what makes
            // `WHERE n.nspname <> 'x'` drop a LEFT JOIN's unmatched rows the
            // way Postgres does.
            if truthy(&eval(pred, &bind(&r))?) == Some(true) {
                kept.push(r);
            }
        }
        rows = kept;
        plan.push(Stage::Filter { in_rows, out_rows: rows.len() });
    }

    // ── 3. the output shape ─────────────────────────────────────────────────
    // Resolved from the FIRST row when the select list contains a `*`,
    // because only a row knows what columns a schemaless source has. With no
    // rows at all a `*` yields no columns, which is the honest answer.
    //
    // `spans` records which output columns each select ITEM owns, so the
    // projection below never has to guess. The previous version walked a
    // single counter through both stages, and a `*` that skipped an
    // already-named column left the counter pointing at the wrong name — a
    // drift that happened to be masked by a fallback.
    let mut cols: Vec<OutCol> = vec![];
    let mut spans: Vec<(usize, usize)> = Vec::with_capacity(sel.items.len());
    for item in &sel.items {
        let start = cols.len();
        match &item.expr {
            Expr::Star => {
                if let Some(first) = rows.first() {
                    for (n, _) in bind(first).flatten() {
                        // A star never emits the same column twice.
                        if !cols.iter().any(|c| c.name == n) {
                            cols.push(OutCol { key: n.clone(), name: n });
                        }
                    }
                }
            }
            Expr::QualifiedStar(q) => {
                if let Some(first) = rows.first() {
                    for (n, _) in bind(first).flatten_binding(q) {
                        if !cols.iter().any(|c| c.name == n) {
                            cols.push(OutCol { key: n.clone(), name: n });
                        }
                    }
                }
            }
            _ => {
                let name = item.alias.clone().unwrap_or_else(|| derived_name(&item.expr));
                // Postgres permits duplicate output names and clients index
                // positionally as well as by name, so a collision is NOT
                // renamed — silently renaming a column is worse than a
                // duplicate, because generated SQL looks for the name it asked
                // for. Only the internal KEY is disambiguated.
                let key = unique_key(&cols, &name);
                cols.push(OutCol { key, name });
            }
        }
        spans.push((start, cols.len()));
    }

    // ── 4. project ──────────────────────────────────────────────────────────
    // The source row is kept beside each projected row, because ORDER BY may
    // sort on an expression over columns that are NOT in the select list.
    let mut projected: Vec<(Map<String, Value>, JoinedRow)> = Vec::with_capacity(rows.len());
    for r in rows {
        let b = bind(&r);
        let mut obj = Map::new();
        for (i, item) in sel.items.iter().enumerate() {
            let (start, end) = spans[i];
            match &item.expr {
                Expr::Star => {
                    for (n, v) in b.flatten() {
                        if let Some(c) = cols[start..end].iter().find(|c| c.name == n) {
                            obj.entry(c.key.clone()).or_insert(v);
                        }
                    }
                }
                Expr::QualifiedStar(q) => {
                    for (n, v) in b.flatten_binding(q) {
                        if let Some(c) = cols[start..end].iter().find(|c| c.name == n) {
                            obj.entry(c.key.clone()).or_insert(v);
                        }
                    }
                }
                _ => {
                    let v = eval(&item.expr, &b)?;
                    if let Some(c) = cols.get(start) {
                        obj.insert(c.key.clone(), v);
                    }
                }
            }
        }
        projected.push((obj, r));
    }

    plan.push(Stage::Project { columns: cols.len(), out_rows: projected.len() });

    // ── 5. DISTINCT ─────────────────────────────────────────────────────────
    if sel.distinct {
        let in_rows = projected.len();
        let mut seen: Vec<String> = vec![];
        let mut kept = vec![];
        for (obj, src) in projected {
            // Keyed on the PROJECTED values in output order, which is what
            // DISTINCT means — not on the source rows.
            let key = cols
                .iter()
                .map(|c| format!("{:?}", obj.get(&c.key).unwrap_or(&Value::Null)))
                .collect::<Vec<_>>()
                .join("\u{1}");
            if !seen.contains(&key) {
                seen.push(key);
                kept.push((obj, src));
            }
        }
        projected = kept;
        plan.push(Stage::Distinct { in_rows, out_rows: projected.len() });
    }

    // ── 6. ORDER BY ─────────────────────────────────────────────────────────
    if !sel.order_by.is_empty() {
        // Sort keys are precomputed so the comparator cannot fail halfway
        // through a sort — an error raised inside `sort_by` would leave the
        // rows in an arbitrary order and still return them.
        let mut keyed: Vec<(Vec<Value>, (Map<String, Value>, JoinedRow))> = vec![];
        for (obj, src) in projected {
            let mut key = vec![];
            for ob in &sel.order_by {
                let v = match (ob.ordinal, &ob.expr) {
                    (Some(n), _) => {
                        let c = cols.get(n - 1).ok_or_else(|| {
                            anyhow::anyhow!(
                                "ORDER BY {} is out of range: the select list has {} \
                                 column(s)", n, cols.len())
                        })?;
                        obj.get(&c.key).cloned().unwrap_or(Value::Null)
                    }
                    (None, Some(e)) => {
                        // An ORDER BY expression may name a column that is not
                        // in the select list, so it is evaluated against the
                        // SOURCE row.
                        eval(e, &bind(&src))?
                    }
                    (None, None) => Value::Null,
                };
                key.push(v);
            }
            keyed.push((key, (obj, src)));
        }

        keyed.sort_by(|a, b| {
            for (i, ob) in sel.order_by.iter().enumerate() {
                let (x, y) = (&a.0[i], &b.0[i]);
                let ord = match (x.is_null(), y.is_null()) {
                    (true, true) => std::cmp::Ordering::Equal,
                    // NULL placement is a direction-independent choice, so it
                    // is applied BEFORE the DESC reversal rather than being
                    // flipped by it.
                    (true, false) => {
                        return if ob.nulls_first {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Greater
                        }
                    }
                    (false, true) => {
                        return if ob.nulls_first {
                            std::cmp::Ordering::Greater
                        } else {
                            std::cmp::Ordering::Less
                        }
                    }
                    (false, false) => cmp_values(x, y).unwrap_or(std::cmp::Ordering::Equal),
                };
                let ord = if matches!(ob.dir, Dir::Desc) { ord.reverse() } else { ord };
                if !ord.is_eq() {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });

        projected = keyed.into_iter().map(|(_, row)| row).collect();
        plan.push(Stage::Sort { keys: sel.order_by.len(), rows: projected.len() });
    }

    // ── 7. OFFSET / LIMIT ───────────────────────────────────────────────────
    let mut out: Vec<Value> = projected
        .into_iter()
        .map(|(obj, _)| Value::Object(obj))
        .collect();
    let in_rows = out.len();
    if let Some(off) = sel.offset {
        out = if off >= out.len() { vec![] } else { out.split_off(off) };
    }
    if let Some(lim) = sel.limit {
        out.truncate(lim);
    }
    if sel.limit.is_some() || sel.offset.is_some() {
        plan.push(Stage::Limit {
            limit: sel.limit,
            offset: sel.offset,
            in_rows,
            out_rows: out.len(),
        });
    }

    Ok((cols, out, plan))
}

// ─────────────────────────────────────────────────────────────────────────────
// The two join implementations
// ─────────────────────────────────────────────────────────────────────────────

/// Apply the post-join filter to one produced row.
///
/// # An `ON` predicate and a post-join `WHERE` predicate are NOT the same thing
///
/// The physical join evaluates both inside one loop, which is where the
/// performance comes from. It does NOT merge them, and the difference is
/// semantic law rather than a matter of taste:
///
/// ```text
///   LEFT JOIN ... ON a.x = b.x AND b.tag = 'q'     keeps every left row
///   LEFT JOIN ... ON a.x = b.x WHERE b.tag = 'q'   discards the outer rows
/// ```
///
/// So the order is fixed and each step sees only what it should:
///
/// 1. form the candidate pair
/// 2. evaluate `ON` — and this ALONE decides whether the row counts as
///    matched, for both the left row and the right row
/// 3. synthesise NULLs if the outer join requires it
/// 4. evaluate the post-join filter
/// 5. count the survivor toward the row budget
///
/// Step 2 is the load-bearing one. If the filter were allowed to influence
/// "matched", a left row whose only partner fails the filter would be
/// NULL-extended — and a filter like `WHERE b.tag IS NULL` would then ACCEPT
/// that synthesised row, inventing output that the unfused pipeline never
/// produces. It is the same trap that made the first predicate-pushdown
/// attempt wrong, in a different place.
fn keep_row(cand: &JoinedRow, post: Option<&Expr>, removed: &mut usize) -> Result<bool> {
    let Some(p) = post else { return Ok(true) };
    // Only TRUE keeps a row, exactly as a standalone `WHERE` stage does.
    if truthy(&eval(p, &bind(cand))?) == Some(true) {
        Ok(true)
    } else {
        *removed += 1;
        Ok(false)
    }
}

/// `RIGHT`/`FULL`: every right row that found no partner survives, with every
/// left binding NULL.
///
/// Shared by both strategies so the two cannot drift apart on the subtlest
/// part of outer-join semantics.
fn emit_unmatched_right(
    out: &mut Vec<JoinedRow>,
    kind: JoinKind,
    left_bindings: &[String],
    right_rows: &[Value],
    right_matched: &[bool],
    rb: &str,
    post: Option<&Expr>,
    removed: &mut usize,
) -> Result<()> {
    if !matches!(kind, JoinKind::Right | JoinKind::Full) {
        return Ok(());
    }
    for (ri, right) in right_rows.iter().enumerate() {
        if right_matched[ri] {
            continue;
        }
        let mut cand: JoinedRow = left_bindings.iter().map(|b| (b.clone(), None)).collect();
        cand.push((rb.to_string(), Some(right.clone())));
        // Outer rows face the post-join filter too — it is a `WHERE`, and a
        // `WHERE` applies to every row the join produced.
        if keep_row(&cand, post, removed)? {
            out.push(cand);
        }
    }
    Ok(())
}

/// The reference strategy: consider every pair.
///
/// Quadratic, and kept forever anyway. It is the semantic fallback for
/// predicates the hash path cannot key on, the implementation of record for
/// non-equality joins, and the oracle the differential tests compare against.
fn join_nested_loop(
    left_src: &mut dyn LeftSource,
    left_bindings: &[String],
    join: &Join,
    right_rows: &[Value],
    rb: &str,
    budget: Option<usize>,
    post: Option<&Expr>,
) -> Result<(Vec<JoinedRow>, usize, usize)> {
    let mut out: Vec<JoinedRow> = vec![];
    let mut removed = 0usize;
    // Which right rows found a partner — only needed for RIGHT and FULL.
    let mut right_matched = vec![false; right_rows.len()];

    let mut consumed = 0usize;
    while let Some(left) = {
        if budget.is_some_and(|b| out.len() >= b) {
            // Stop ASKING. With a streaming left side this is what keeps the
            // source from producing rows nobody will look at.
            None
        } else {
            left_src.next_left()?
        }
    } {
        consumed += 1;
        let left = &left;
        // Decided by the ON clause ALONE. See `keep_row` for why the
        // post-join filter must not touch this.
        let mut matched = false;
        for (ri, right) in right_rows.iter().enumerate() {
            let mut cand: JoinedRow = left.clone();
            cand.push((rb.to_string(), Some(right.clone())));
            let joins_here = match &join.on {
                // CROSS JOIN has no predicate: every pair survives.
                None => true,
                // An ON that evaluates to UNKNOWN does NOT join, exactly
                // as in SQL. Treating UNKNOWN as a match would invent
                // pairings out of missing data.
                Some(on) => truthy(&eval(on, &bind(&cand))?) == Some(true),
            };
            if joins_here {
                matched = true;
                right_matched[ri] = true;
                if keep_row(&cand, post, &mut removed)? {
                    out.push(cand);
                }
            }
        }
        // LEFT/FULL: an unmatched left row survives with a NULL right.
        if !matched && matches!(join.kind, JoinKind::Left | JoinKind::Full) {
            let mut cand: JoinedRow = left.clone();
            cand.push((rb.to_string(), None));
            if keep_row(&cand, post, &mut removed)? {
                out.push(cand);
            }
        }
    }

    // Right-outer rows are appended AFTER every left row, so once the budget
    // is met they sit beyond the prefix `LIMIT` will keep and cannot affect the
    // answer. Skipping them is the point of the budget; emitting them would be
    // correct but pointless work.
    if !budget.is_some_and(|b| out.len() >= b) {
        emit_unmatched_right(
            &mut out, join.kind, left_bindings, right_rows, &right_matched, rb, post,
            &mut removed,
        )?;
    }
    Ok((out, removed, consumed))
}

/// The fast strategy: bucket the right relation, probe it with the left.
///
/// The hash table is used ONLY to narrow the candidate set. Every surviving
/// pair is then evaluated against the complete, unmodified `ON` expression —
/// the same call the nested loop makes — so the two strategies answer with the
/// same expression evaluated on the same rows. See [`crate::sqljoin`] for why
/// bucketing alone would be unsound here.
fn join_hash(
    left_src: &mut dyn LeftSource,
    left_bindings: &[String],
    join: &Join,
    right_rows: &[Value],
    rb: &str,
    keys: &[(Expr, Expr)],
    budget: Option<usize>,
    post: Option<&Expr>,
) -> Result<(Vec<JoinedRow>, usize, usize)> {
    debug_assert!(!keys.is_empty(), "the planner must not choose Hash with no keys");

    // ── build: the right relation, keyed ────────────────────────────────────
    let side = sqljoin::HashSide::build(right_rows.len(), |i| {
        // A right key reads only the right binding — that is what the planner
        // proved — so binding the row alone is sufficient and correct.
        let one: JoinedRow = vec![(rb.to_string(), Some(right_rows[i].clone()))];
        let b = bind(&one);
        let mut k = Vec::with_capacity(keys.len());
        for (_, right_expr) in keys {
            match sqljoin::hkey(&eval(right_expr, &b)?) {
                Some(h) => k.push(h),
                // A NULL anywhere in the key means this row joins nothing.
                None => return Ok(None),
            }
        }
        Ok(Some(k))
    })?;

    // ── probe: the accumulated left rows ────────────────────────────────────
    let mut out: Vec<JoinedRow> = vec![];
    let mut removed = 0usize;
    let mut right_matched = vec![false; right_rows.len()];

    let mut consumed = 0usize;
    while let Some(left) = {
        if budget.is_some_and(|b| out.len() >= b) {
            None
        } else {
            left_src.next_left()?
        }
    } {
        consumed += 1;
        let left = &left;
        let lb = bind(left);
        let mut lk = Vec::with_capacity(keys.len());
        let mut null_key = false;
        for (left_expr, _) in keys {
            match sqljoin::hkey(&eval(left_expr, &lb)?) {
                Some(h) => lk.push(h),
                None => {
                    null_key = true;
                    break;
                }
            }
        }

        let mut matched = false;
        // A NULL key matches nothing, so the bucket is not consulted. A
        // shortcut, not a safeguard: the confirm step below would reject
        // those pairs anyway, since `NULL = NULL` is UNKNOWN.
        if !null_key {
            for &ri in side.probe(&lk) {
                let mut cand: JoinedRow = left.clone();
                cand.push((rb.to_string(), Some(right_rows[ri].clone())));
                // Confirm. The bucket only suggested this pair.
                let joins_here = match &join.on {
                    None => true,
                    Some(on) => truthy(&eval(on, &bind(&cand))?) == Some(true),
                };
                if joins_here {
                    matched = true;
                    right_matched[ri] = true;
                    if keep_row(&cand, post, &mut removed)? {
                        out.push(cand);
                    }
                }
            }
        }
        if !matched && matches!(join.kind, JoinKind::Left | JoinKind::Full) {
            let mut cand: JoinedRow = left.clone();
            cand.push((rb.to_string(), None));
            if keep_row(&cand, post, &mut removed)? {
                out.push(cand);
            }
        }
    }

    // See the note in `join_nested_loop`: beyond the budget these rows cannot
    // survive the `LIMIT` prefix.
    if !budget.is_some_and(|b| out.len() >= b) {
        emit_unmatched_right(
            &mut out, join.kind, left_bindings, right_rows, &right_matched, rb, post,
            &mut removed,
        )?;
    }
    Ok((out, removed, consumed))
}

/// Apply the pushed conjuncts for one relation, before it reaches the join.
///
/// Evaluated against the relation's own binding alone, which is exactly what
/// the planner proved is sufficient: a pushed conjunct references only this
/// relation, so binding it alone gives the same answer the post-join `WHERE`
/// will give for the same row.
fn prefilter(
    rows: Vec<Value>,
    binding: &str,
    push: &Pushdown,
    plan: &mut Plan,
) -> Result<Vec<Value>> {
    let Some(preds) = push.for_binding(binding) else { return Ok(rows) };
    if preds.is_empty() {
        return Ok(rows);
    }
    let in_rows = rows.len();
    let mut kept = Vec::with_capacity(rows.len());
    for row in rows {
        let one: JoinedRow = vec![(binding.to_string(), Some(row))];
        let b = bind(&one);
        let mut keep = true;
        for p in preds {
            // Only TRUE keeps a row, exactly as in `WHERE`. Treating UNKNOWN
            // as a keep would make the pre-filter weaker than the filter it
            // duplicates, which is harmless; treating it as a drop when the
            // real filter would keep it would not be — so the two must agree,
            // and they do because this is the same evaluator call.
            if truthy(&eval(p, &b)?) != Some(true) {
                keep = false;
                break;
            }
        }
        if keep {
            // Unwrap the row back out of the single-binding wrapper.
            if let Some((_, Some(v))) = one.into_iter().next() {
                kept.push(v);
            }
        }
    }
    plan.push(Stage::Prefilter {
        binding: binding.to_string(),
        predicates: preds.len(),
        in_rows,
        out_rows: kept.len(),
    });
    Ok(kept)
}

fn fetch(name: &str, resolve: &Resolver) -> Result<Box<dyn Relation>> {
    match resolve(name)? {
        Some(rel) => Ok(rel),
        // Named rather than silently empty: an unknown table that answered
        // with no rows would look exactly like an empty one.
        None => bail!("relation {:?} does not exist", name),
    }
}

/// Where a join reads its LEFT rows from.
///
/// Both strategies consume the left side in a SINGLE forward pass — the
/// nested loop iterates it once, and the hash join probes with it once — so an
/// iterator is a natural fit and no rewinding is needed. That is what makes
/// the driving relation streamable while the inner side stays materialised.
trait LeftSource {
    fn next_left(&mut self) -> Result<Option<JoinedRow>>;
    /// Best guess at the row count, for the strategy planner.
    fn hint(&self) -> Option<usize>;
    /// Whatever rows remain, for a query with no join at all.
    fn take_rows(&mut self) -> Vec<JoinedRow>;
    /// `(pulled, kept)` when this is a streamed base relation.
    fn stats(&self) -> Option<(usize, usize)> {
        None
    }
}

/// The driving relation, pulled on demand and pre-filtered inline.
///
/// Pulling lazily is the whole point: with a row budget, a `LIMIT 20` over a
/// join stops asking for rows long before the source is exhausted, so the
/// source never has to produce the rest.
struct StreamLeft {
    rel: Box<dyn Relation>,
    binding: String,
    preds: Vec<Expr>,
    /// Rows actually requested from the source. Reported as the scan's
    /// `actual rows`, which for a streamed relation is the honest number —
    /// the total is not merely unknown, it is irrelevant to what happened.
    pulled: usize,
    kept: usize,
}

impl LeftSource for StreamLeft {
    fn next_left(&mut self) -> Result<Option<JoinedRow>> {
        while let Some(row) = self.rel.next_row()? {
            self.pulled += 1;
            let one: JoinedRow = vec![(self.binding.clone(), Some(row))];
            if !self.preds.is_empty() {
                let b = bind(&one);
                let mut keep = true;
                for p in &self.preds {
                    if truthy(&eval(p, &b)?) != Some(true) {
                        keep = false;
                        break;
                    }
                }
                if !keep {
                    continue;
                }
            }
            self.kept += 1;
            return Ok(Some(one));
        }
        Ok(None)
    }
    fn hint(&self) -> Option<usize> {
        // The source's own count, BEFORE the inline pre-filter. An
        // over-estimate, which only ever biases the planner toward the hash
        // path — and the two paths are proven equivalent, so a biased choice
        // costs time at worst and never correctness.
        self.rel.size_hint()
    }
    fn take_rows(&mut self) -> Vec<JoinedRow> {
        // Only reached when there is no join, and the base is materialised in
        // that case, so this drains what is left for completeness.
        let mut out = vec![];
        while let Ok(Some(r)) = self.next_left() {
            out.push(r);
        }
        out
    }
    fn stats(&self) -> Option<(usize, usize)> {
        Some((self.pulled, self.kept))
    }
}

/// An already-materialised left side: the output of a previous join, or a
/// base relation in a query the streaming path does not cover.
struct VecLeft {
    rows: Vec<JoinedRow>,
    at: usize,
}

impl LeftSource for VecLeft {
    fn next_left(&mut self) -> Result<Option<JoinedRow>> {
        let r = self.rows.get(self.at).cloned();
        if r.is_some() {
            self.at += 1;
        }
        Ok(r)
    }
    fn hint(&self) -> Option<usize> {
        Some(self.rows.len().saturating_sub(self.at))
    }
    fn take_rows(&mut self) -> Vec<JoinedRow> {
        let mut v = std::mem::take(&mut self.rows);
        if self.at > 0 {
            v = v.split_off(self.at);
        }
        self.at = 0;
        v
    }
}

/// Pull a relation completely into memory.
///
/// Used for the INNER side of a join, which genuinely has to be whole: a hash
/// join must build its table before probing, and a nested loop re-scans it for
/// every left row. Streaming it would save nothing, so this says plainly that
/// it is being materialised on purpose rather than by omission.
fn drain(mut rel: Box<dyn Relation>) -> Result<Vec<Value>> {
    let mut out = Vec::with_capacity(rel.size_hint().unwrap_or(0));
    while let Some(row) = rel.next_row()? {
        out.push(row);
    }
    Ok(out)
}

/// Parse and run in one call.
pub fn run(sql: &str, resolve: &Resolver) -> Result<(Vec<OutCol>, Vec<Value>)> {
    let sel = parse(sql)?;
    execute(&sel, resolve)
}

#[cfg(test)]
mod lexer_tests {
    use super::*;

    fn kinds(src: &str) -> Vec<Tok> {
        let mut t = lex(src).expect("lexes");
        t.pop(); // drop Eof
        t
    }

    #[test]
    fn a_word_keeps_both_its_canonical_and_raw_spelling() {
        // A column may legitimately be called `count` or `value`; folding case
        // in the lexer would later look up a key the data does not have.
        assert_eq!(
            kinds("Select"),
            vec![Tok::Word { upper: "SELECT".into(), raw: "Select".into() }]
        );
    }

    #[test]
    fn a_quoted_identifier_is_never_a_keyword() {
        assert_eq!(kinds(r#""select""#), vec![Tok::Quoted("select".into())]);
        // …and keeps its case, which is the whole point of quoting it.
        assert_eq!(kinds(r#""Name""#), vec![Tok::Quoted("Name".into())]);
    }

    #[test]
    fn a_doubled_quote_is_one_literal_quote() {
        assert_eq!(kinds("'it''s'"), vec![Tok::Str("it's".into())]);
        assert_eq!(kinds(r#""a""b""#), vec![Tok::Quoted("a\"b".into())]);
    }

    #[test]
    fn an_E_string_decodes_the_escapes_catalogue_sql_uses() {
        // `array_to_string(d.datacl, E'\n')` appears verbatim in psql's \l.
        assert_eq!(kinds(r"E'\n'"), vec![Tok::Str("\n".into())]);
        assert_eq!(kinds(r"E'a\tb'"), vec![Tok::Str("a\tb".into())]);
        // An unknown escape keeps its character rather than vanishing.
        assert_eq!(kinds(r"E'\q'"), vec![Tok::Str("q".into())]);
    }

    #[test]
    fn operators_match_longest_first() {
        // Order is load-bearing: `!~*` must not tokenise as `!~` plus `*`.
        assert_eq!(kinds("!~*"), vec![Tok::Op("!~*".into())]);
        assert_eq!(kinds("!~"), vec![Tok::Op("!~".into())]);
        assert_eq!(kinds("~*"), vec![Tok::Op("~*".into())]);
        assert_eq!(kinds("<>"), vec![Tok::Op("<>".into())]);
        assert_eq!(kinds("!="), vec![Tok::Op("!=".into())]);
        assert_eq!(kinds(">="), vec![Tok::Op(">=".into())]);
        assert_eq!(kinds("::"), vec![Tok::Op("::".into())]);
        assert_eq!(kinds("||"), vec![Tok::Op("||".into())]);
        assert_eq!(kinds("~"), vec![Tok::Op("~".into())]);
    }

    #[test]
    fn comments_are_skipped_including_nested_block_comments() {
        assert_eq!(kinds("1 -- trailing\n"), vec![Tok::Num(1.0)]);
        assert_eq!(kinds("1 /* a */ 2"), vec![Tok::Num(1.0), Tok::Num(2.0)]);
        // SQL block comments nest, unlike C's.
        assert_eq!(kinds("1 /* a /* b */ c */ 2"), vec![Tok::Num(1.0), Tok::Num(2.0)]);
        assert!(lex("1 /* unterminated").is_err());
    }

    #[test]
    fn numbers_parse_including_fractions_and_exponents() {
        assert_eq!(kinds("42"), vec![Tok::Num(42.0)]);
        assert_eq!(kinds("4.5"), vec![Tok::Num(4.5)]);
        assert_eq!(kinds(".5"), vec![Tok::Num(0.5)]);
        assert_eq!(kinds("1e3"), vec![Tok::Num(1000.0)]);
        assert_eq!(kinds("1e-2"), vec![Tok::Num(0.01)]);
        // `1e` is the number 1 followed by an identifier, not a broken number.
        assert_eq!(
            kinds("1e"),
            vec![Tok::Num(1.0), Tok::Word { upper: "E".into(), raw: "e".into() }]
        );
    }

    #[test]
    fn an_unterminated_literal_is_an_error_not_a_truncation() {
        assert!(lex("'abc").is_err());
        assert!(lex(r#""abc"#).is_err());
    }

    #[test]
    fn an_unknown_character_is_REFUSED_rather_than_skipped() {
        // Skipping is how a parser silently reads a different query than the
        // one it was handed.
        let e = lex("SELECT 1 @ 2").unwrap_err().to_string();
        assert!(e.contains('@'), "{}", e);
    }

    #[test]
    fn the_real_dn_query_lexes() {
        let sql = r#"SELECT n.nspname AS "Name",
          pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
        FROM pg_catalog.pg_namespace n
        WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
        ORDER BY 1;"#;
        let toks = lex(sql).expect("psql's \\dn must lex");
        assert!(toks.contains(&Tok::Quoted("Name".into())));
        assert!(toks.contains(&Tok::Op("!~".into())));
        assert!(toks.contains(&Tok::Op("<>".into())));
        assert!(toks.contains(&Tok::Str("^pg_".into())));
    }

    #[test]
    fn the_real_dt_query_lexes() {
        let sql = r#"SELECT n.nspname as "Schema", c.relname as "Name",
          CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' END as "Type",
          pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
        FROM pg_catalog.pg_class c
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
        WHERE c.relkind IN ('r','p','')
              AND n.nspname <> 'pg_catalog'
              AND n.nspname !~ '^pg_toast'
          AND pg_catalog.pg_table_is_visible(c.oid)
        ORDER BY 1,2;"#;
        let toks = lex(sql).expect("psql's \\dt must lex");
        assert!(toks.iter().any(|t| t.is_kw("CASE")));
        assert!(toks.iter().any(|t| t.is_kw("LEFT")));
        assert!(toks.iter().any(|t| t.is_kw("JOIN")));
        // The empty string in `IN ('r','p','')` must survive as a real value.
        assert!(toks.contains(&Tok::Str(String::new())));
    }
}

#[cfg(test)]
mod parser_tests {
    use super::*;
    use serde_json::json;

    fn col(qual: Option<&str>, name: &str) -> Expr {
        Expr::Column { qual: qual.map(str::to_string), name: name.to_string() }
    }

    #[test]
    fn a_bare_select_list_and_from() {
        let s = parse("SELECT a, b FROM t").unwrap();
        assert_eq!(s.items.len(), 2);
        assert_eq!(s.items[0].expr, col(None, "a"));
        assert_eq!(s.from.unwrap().name, "t");
    }

    #[test]
    fn a_clause_keyword_is_never_read_as_a_bare_alias() {
        // Without the guard, `FROM t WHERE x = 1` parses `t` aliased as
        // `WHERE`, the predicate vanishes, and EVERY row comes back — a
        // silently wrong answer of the worst kind.
        let s = parse("SELECT a FROM t WHERE a = 1").unwrap();
        assert_eq!(s.from.clone().unwrap().alias, None);
        assert!(s.where_.is_some(), "the WHERE clause must survive");
        let s = parse("SELECT a FROM t ORDER BY a").unwrap();
        assert_eq!(s.from.unwrap().alias, None);
        assert_eq!(s.order_by.len(), 1);
    }

    #[test]
    fn a_real_alias_is_kept_in_both_spellings() {
        assert_eq!(parse("SELECT a FROM t x").unwrap().from.unwrap().alias,
                   Some("x".to_string()));
        assert_eq!(parse("SELECT a FROM t AS x").unwrap().from.unwrap().alias,
                   Some("x".to_string()));
    }

    #[test]
    fn a_tables_binding_is_its_alias_else_its_bare_name() {
        let t = TableRef { name: "pg_catalog.pg_class".into(), alias: Some("c".into()) };
        assert_eq!(t.binding(), "c");
        let t = TableRef { name: "pg_catalog.pg_class".into(), alias: None };
        assert_eq!(t.binding(), "pg_class", "the schema is not how a column is addressed");
    }

    #[test]
    fn a_qualified_column_keeps_only_its_immediate_qualifier() {
        assert_eq!(parse("SELECT n.nspname FROM x").unwrap().items[0].expr,
                   col(Some("n"), "nspname"));
        // In `public.orders.id` the binding is `orders`; the schema is not
        // part of how a column is addressed.
        assert_eq!(parse("SELECT public.orders.id FROM x").unwrap().items[0].expr,
                   col(Some("orders"), "id"));
    }

    #[test]
    fn an_alias_may_be_a_quoted_string_with_significant_case() {
        let s = parse(r#"SELECT n.nspname AS "Name" FROM x"#).unwrap();
        assert_eq!(s.items[0].alias, Some("Name".to_string()));
    }

    #[test]
    fn a_schema_qualified_function_drops_its_schema() {
        // `pg_catalog.pg_get_userbyid` is the same function as
        // `pg_get_userbyid`; the schema is not part of its identity here.
        let s = parse("SELECT pg_catalog.pg_get_userbyid(n.nspowner) FROM x").unwrap();
        match &s.items[0].expr {
            Expr::Func { name, args } => {
                assert_eq!(name, "pg_get_userbyid");
                assert_eq!(args.len(), 1);
                assert_eq!(args[0], col(Some("n"), "nspowner"));
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn operator_precedence_matches_sql() {
        // AND binds tighter than OR: `a OR b AND c` is `a OR (b AND c)`.
        // Getting this backwards silently returns the wrong rows.
        let s = parse("SELECT 1 FROM t WHERE a = 1 OR b = 2 AND c = 3").unwrap();
        match s.where_.unwrap() {
            Expr::Binary { op, right, .. } => {
                assert_eq!(op, "OR");
                assert!(matches!(*right, Expr::Binary { ref op, .. } if op == "AND"),
                        "AND must bind tighter than OR");
            }
            other => panic!("{:?}", other),
        }
        // Comparison binds tighter than AND.
        let s = parse("SELECT 1 FROM t WHERE a = 1 AND b = 2").unwrap();
        assert!(matches!(s.where_.unwrap(), Expr::Binary { ref op, .. } if op == "AND"));
        // Multiplication binds tighter than addition.
        let s = parse("SELECT 1 + 2 * 3 FROM t").unwrap();
        match &s.items[0].expr {
            Expr::Binary { op, right, .. } => {
                assert_eq!(op, "+");
                assert!(matches!(**right, Expr::Binary { ref op, .. } if op == "*"));
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn parentheses_override_precedence() {
        let s = parse("SELECT 1 FROM t WHERE (a = 1 OR b = 2) AND c = 3").unwrap();
        match s.where_.unwrap() {
            Expr::Binary { op, left, .. } => {
                assert_eq!(op, "AND");
                assert!(matches!(*left, Expr::Binary { ref op, .. } if op == "OR"));
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn in_and_is_null_and_between_parse_in_both_polarities() {
        let s = parse("SELECT 1 FROM t WHERE k IN ('r','p','')").unwrap();
        match s.where_.unwrap() {
            Expr::InList { list, negated, .. } => {
                assert_eq!(list.len(), 3);
                assert!(!negated);
                // The empty string in psql's `IN ('r','p','')` is a REAL value.
                assert_eq!(list[2], Expr::Literal(json!("")));
            }
            other => panic!("{:?}", other),
        }
        assert!(matches!(parse("SELECT 1 FROM t WHERE k NOT IN (1)").unwrap().where_.unwrap(),
                         Expr::InList { negated: true, .. }));
        assert!(matches!(parse("SELECT 1 FROM t WHERE k IS NULL").unwrap().where_.unwrap(),
                         Expr::IsNull { negated: false, .. }));
        assert!(matches!(parse("SELECT 1 FROM t WHERE k IS NOT NULL").unwrap().where_.unwrap(),
                         Expr::IsNull { negated: true, .. }));
        // BETWEEN's bounds must not let AND escape as a boolean operator.
        let s = parse("SELECT 1 FROM t WHERE n BETWEEN 1 AND 5").unwrap();
        assert!(matches!(s.where_.unwrap(), Expr::Binary { ref op, .. } if op == "AND"));
    }

    #[test]
    fn both_case_spellings_parse() {
        // simple CASE — what psql's \dt uses, with nine branches.
        let s = parse("SELECT CASE k WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' \
                       ELSE 'other' END FROM t").unwrap();
        match &s.items[0].expr {
            Expr::Case { operand, whens, else_ } => {
                assert!(operand.is_some());
                assert_eq!(whens.len(), 2);
                assert!(else_.is_some());
            }
            other => panic!("{:?}", other),
        }
        // searched CASE
        let s = parse("SELECT CASE WHEN k = 'r' THEN 1 END FROM t").unwrap();
        match &s.items[0].expr {
            Expr::Case { operand, whens, else_ } => {
                assert!(operand.is_none());
                assert_eq!(whens.len(), 1);
                assert!(else_.is_none());
            }
            other => panic!("{:?}", other),
        }
        // A CASE with no WHEN is malformed and must be refused.
        assert!(parse("SELECT CASE k END FROM t").is_err());
    }

    #[test]
    fn every_join_flavour_parses_and_an_inner_join_demands_ON() {
        for (sql, kind) in [
            ("SELECT 1 FROM a JOIN b ON a.x = b.x", JoinKind::Inner),
            ("SELECT 1 FROM a INNER JOIN b ON a.x = b.x", JoinKind::Inner),
            ("SELECT 1 FROM a LEFT JOIN b ON a.x = b.x", JoinKind::Left),
            ("SELECT 1 FROM a LEFT OUTER JOIN b ON a.x = b.x", JoinKind::Left),
            ("SELECT 1 FROM a RIGHT JOIN b ON a.x = b.x", JoinKind::Right),
            ("SELECT 1 FROM a FULL OUTER JOIN b ON a.x = b.x", JoinKind::Full),
            ("SELECT 1 FROM a CROSS JOIN b", JoinKind::Cross),
        ] {
            let s = parse(sql).unwrap_or_else(|e| panic!("{}: {}", sql, e));
            assert_eq!(s.joins.len(), 1, "{}", sql);
            assert_eq!(s.joins[0].kind, kind, "{}", sql);
        }
        // A comma FROM list is an implicit cross join.
        let s = parse("SELECT 1 FROM a, b").unwrap();
        assert_eq!(s.joins[0].kind, JoinKind::Cross);
        // A join that needs a predicate must not silently become a cross
        // product — that turns two tables into n*m confidently wrong rows.
        assert!(parse("SELECT 1 FROM a LEFT JOIN b").is_err());
        assert!(parse("SELECT 1 FROM a JOIN b USING (x)").is_err());
    }

    #[test]
    fn order_by_reads_a_number_as_an_ORDINAL() {
        // psql's \dt ends with `ORDER BY 1,2`. Reading those as the constants
        // 1 and 2 sorts every row equally and silently yields an unordered
        // listing that looks fine.
        let s = parse("SELECT a, b FROM t ORDER BY 1, 2 DESC").unwrap();
        assert_eq!(s.order_by.len(), 2);
        assert_eq!(s.order_by[0].ordinal, Some(1));
        assert_eq!(s.order_by[0].dir, Dir::Asc);
        assert_eq!(s.order_by[1].ordinal, Some(2));
        assert_eq!(s.order_by[1].dir, Dir::Desc);
        // An expression still parses as an expression.
        let s = parse("SELECT a FROM t ORDER BY lower(a) ASC").unwrap();
        assert!(s.order_by[0].ordinal.is_none());
        assert!(s.order_by[0].expr.is_some());
    }

    #[test]
    fn null_ordering_defaults_the_way_postgres_defaults() {
        let s = parse("SELECT a FROM t ORDER BY a").unwrap();
        assert!(!s.order_by[0].nulls_first, "ASC defaults to NULLS LAST");
        let s = parse("SELECT a FROM t ORDER BY a DESC").unwrap();
        assert!(s.order_by[0].nulls_first, "DESC defaults to NULLS FIRST");
        let s = parse("SELECT a FROM t ORDER BY a NULLS FIRST").unwrap();
        assert!(s.order_by[0].nulls_first, "an explicit clause wins");
    }

    #[test]
    fn limit_and_offset_parse_in_either_order() {
        let s = parse("SELECT a FROM t LIMIT 5 OFFSET 2").unwrap();
        assert_eq!((s.limit, s.offset), (Some(5), Some(2)));
        let s = parse("SELECT a FROM t OFFSET 2 LIMIT 5").unwrap();
        assert_eq!((s.limit, s.offset), (Some(5), Some(2)));
        let s = parse("SELECT a FROM t LIMIT ALL").unwrap();
        assert_eq!(s.limit, None);
    }

    #[test]
    fn casts_parse_and_are_recorded_rather_than_rejected() {
        // `pr.prattrs::pg_catalog.int2[]` appears verbatim in psql's \d.
        let s = parse("SELECT x::int2 FROM t").unwrap();
        assert!(matches!(s.items[0].expr, Expr::Cast { .. }));
        let s = parse("SELECT x::pg_catalog.int2[] FROM t").unwrap();
        match &s.items[0].expr {
            Expr::Cast { ty, .. } => assert_eq!(ty, "int2[]"),
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn star_and_qualified_star_parse() {
        assert_eq!(parse("SELECT * FROM t").unwrap().items[0].expr, Expr::Star);
        assert_eq!(parse("SELECT c.* FROM t c").unwrap().items[0].expr,
                   Expr::QualifiedStar("c".into()));
        match &parse("SELECT count(*) FROM t").unwrap().items[0].expr {
            Expr::Func { name, args } => {
                assert_eq!(name, "count");
                assert_eq!(args, &vec![Expr::Star]);
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn a_parenthesis_free_function_parses_as_a_zero_arg_call() {
        // `current_schema` is legal without parentheses.
        match &parse("SELECT current_schema FROM t").unwrap().items[0].expr {
            Expr::Func { name, args } => {
                assert_eq!(name, "current_schema");
                assert!(args.is_empty());
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn unsupported_clauses_are_refused_by_name() {
        for (sql, needle) in [
            ("SELECT a FROM t GROUP BY a", "GROUP BY"),
            ("SELECT a FROM t HAVING count(*) > 1", "HAVING"),
            ("SELECT DISTINCT ON (a) a FROM t", "DISTINCT ON"),
        ] {
            let e = parse(sql).unwrap_err().to_string();
            assert!(e.contains(needle), "{} -> {}", sql, e);
        }
        // Trailing garbage is an error, not something to ignore.
        assert!(parse("SELECT a FROM t JUNK JUNK2").is_err());
    }

    #[test]
    fn THE_dn_QUERY_parses_completely() {
        let s = parse(
            r#"SELECT n.nspname AS "Name",
                 pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
               FROM pg_catalog.pg_namespace n
               WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
               ORDER BY 1;"#,
        )
        .expect("psql's \\dn must parse");

        assert_eq!(s.items.len(), 2);
        assert_eq!(s.items[0].alias, Some("Name".into()));
        assert_eq!(s.items[1].alias, Some("Owner".into()));
        let from = s.from.unwrap();
        assert_eq!(from.name, "pg_catalog.pg_namespace");
        assert_eq!(from.binding(), "n");
        assert!(s.where_.is_some());
        assert_eq!(s.order_by[0].ordinal, Some(1));
    }

    #[test]
    fn THE_dt_QUERY_parses_completely() {
        let s = parse(
            r#"SELECT n.nspname as "Schema",
                 c.relname as "Name",
                 CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view'
                   WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index'
                   WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table'
                   WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table'
                   WHEN 'I' THEN 'partitioned index' END as "Type",
                 pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
               FROM pg_catalog.pg_class c
                    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
               WHERE c.relkind IN ('r','p','')
                     AND n.nspname <> 'pg_catalog'
                     AND n.nspname !~ '^pg_toast'
                     AND n.nspname <> 'information_schema'
                 AND pg_catalog.pg_table_is_visible(c.oid)
               ORDER BY 1,2;"#,
        )
        .expect("psql's \\dt must parse");

        assert_eq!(s.items.len(), 4);
        assert_eq!(s.items[2].alias, Some("Type".into()));
        match &s.items[2].expr {
            Expr::Case { whens, .. } => assert_eq!(whens.len(), 9, "all nine branches"),
            other => panic!("{:?}", other),
        }
        assert_eq!(s.joins.len(), 2);
        assert!(s.joins.iter().all(|j| j.kind == JoinKind::Left && j.on.is_some()));
        assert_eq!(s.from.unwrap().binding(), "c");
        assert_eq!(s.order_by.len(), 2);
        assert_eq!(
            (s.order_by[0].ordinal, s.order_by[1].ordinal),
            (Some(1), Some(2))
        );
    }
}

#[cfg(test)]
mod eval_tests {
    use super::*;
    use serde_json::json;

    /// One binding named `t` holding `row`.
    fn one(row: &Value) -> Bound<'_> {
        Bound { parts: vec![("t".to_string(), Some(row))] }
    }

    fn ev(sql_expr: &str, row: &Value) -> Result<Value> {
        let s = parse(&format!("SELECT {} FROM t", sql_expr))?;
        eval(&s.items[0].expr, &one(row))
    }

    fn v(sql_expr: &str, row: &Value) -> Value {
        ev(sql_expr, row).unwrap_or_else(|e| panic!("{}: {}", sql_expr, e))
    }

    #[test]
    fn literals_and_columns_resolve() {
        let r = json!({"a": 1, "s": "x", "b": true, "n": null});
        assert_eq!(v("42", &r), json!(42));
        assert_eq!(v("'hi'", &r), json!("hi"));
        assert_eq!(v("NULL", &r), Value::Null);
        assert_eq!(v("TRUE", &r), json!(true));
        assert_eq!(v("a", &r), json!(1));
        assert_eq!(v("t.a", &r), json!(1));
        assert_eq!(v("s", &r), json!("x"));
        // An absent column is NULL, because a schemaless document may omit
        // any field — that is data, not an error.
        assert_eq!(v("nosuch", &r), Value::Null);
    }

    #[test]
    fn an_unknown_table_ALIAS_is_an_error_while_an_unknown_column_is_null() {
        // The distinction matters: a typo'd alias is a query bug worth
        // reporting, while a missing field is ordinary schemaless behaviour.
        let r = json!({"a": 1});
        assert_eq!(v("t.nosuch", &r), Value::Null);
        let e = ev("zz.a", &r).unwrap_err().to_string();
        assert!(e.contains("zz"), "{}", e);
    }

    // ── SQL's three-valued logic. The subtle, dangerous part. ───────────────

    #[test]
    fn every_comparison_over_NULL_is_UNKNOWN_including_null_equals_null() {
        let r = json!({"n": null, "a": 1});
        assert_eq!(v("n = 1", &r), Value::Null);
        assert_eq!(v("n != 1", &r), Value::Null);
        assert_eq!(v("n < 1", &r), Value::Null);
        // The one everybody gets wrong: NULL = NULL is UNKNOWN, not true.
        assert_eq!(v("n = n", &r), Value::Null);
        assert_eq!(v("n = NULL", &r), Value::Null);
    }

    #[test]
    fn NOT_UNKNOWN_is_UNKNOWN_not_true() {
        // Collapsing UNKNOWN to false here would make
        // `WHERE NOT (n.nspname = 'x')` include the unmatched rows of a LEFT
        // JOIN that Postgres excludes — the counts would silently disagree.
        let r = json!({"n": null});
        assert_eq!(v("NOT (n = 1)", &r), Value::Null);
        assert_eq!(v("NOT TRUE", &r), json!(false));
        assert_eq!(v("NOT FALSE", &r), json!(true));
    }

    #[test]
    fn AND_and_OR_follow_the_three_valued_truth_tables() {
        let r = json!({"n": null});
        // false AND unknown = FALSE (the false decides it)
        assert_eq!(v("FALSE AND n = 1", &r), json!(false));
        // true AND unknown = unknown
        assert_eq!(v("TRUE AND n = 1", &r), Value::Null);
        // true OR unknown = TRUE (the true decides it)
        assert_eq!(v("TRUE OR n = 1", &r), json!(true));
        // false OR unknown = unknown
        assert_eq!(v("FALSE OR n = 1", &r), Value::Null);
        // and the ordinary cases
        assert_eq!(v("TRUE AND TRUE", &r), json!(true));
        assert_eq!(v("TRUE AND FALSE", &r), json!(false));
        assert_eq!(v("FALSE OR FALSE", &r), json!(false));
    }

    #[test]
    fn IS_NULL_is_the_one_predicate_that_is_never_unknown() {
        let r = json!({"n": null, "a": 1});
        assert_eq!(v("n IS NULL", &r), json!(true));
        assert_eq!(v("n IS NOT NULL", &r), json!(false));
        assert_eq!(v("a IS NULL", &r), json!(false));
        assert_eq!(v("a IS NOT NULL", &r), json!(true));
        // An absent column is indistinguishable from an explicit null, which
        // is the honest answer for a schemaless store.
        assert_eq!(v("nosuch IS NULL", &r), json!(true));
    }

    #[test]
    fn NOT_IN_with_a_NULL_in_the_list_is_UNKNOWN_the_classic_trap() {
        let r = json!({"a": 2});
        assert_eq!(v("a IN (1, 2)", &r), json!(true));
        assert_eq!(v("a IN (1, 3)", &r), json!(false));
        assert_eq!(v("a NOT IN (1, 3)", &r), json!(true));
        // `2 NOT IN (1, NULL)` is UNKNOWN, not true — 2 MIGHT equal the null.
        // Postgres agrees, and getting this wrong silently includes rows.
        assert_eq!(v("a NOT IN (1, NULL)", &r), Value::Null);
        // A match still decides it even with a null present.
        assert_eq!(v("a IN (2, NULL)", &r), json!(true));
        // NULL on the left is unknown regardless.
        assert_eq!(v("nosuch IN (1)", &r), Value::Null);
    }

    // ── operators ───────────────────────────────────────────────────────────

    #[test]
    fn comparisons_work_across_numbers_strings_and_booleans() {
        let r = json!({"n": 5, "s": "b", "t": true});
        assert_eq!(v("n > 3", &r), json!(true));
        assert_eq!(v("n <= 5", &r), json!(true));
        assert_eq!(v("s < 'c'", &r), json!(true));
        assert_eq!(v("s > 'c'", &r), json!(false));
        // A number and a numeric-looking string compare NUMERICALLY, because
        // a catalogue oid is a number while a client may quote it.
        assert_eq!(v("n = '5'", &r), json!(true));
        assert_eq!(v("n = '5.0'", &r), json!(true));
        // And a non-numeric string falls back to text comparison rather than
        // erroring.
        assert_eq!(v("n = 'five'", &r), json!(false));
    }

    #[test]
    fn the_regex_operators_use_the_SAME_matcher_as_NQL() {
        // Two implementations would be two chances for the SQL surface and
        // the NQL surface to disagree about the same operator.
        let r = json!({"s": "pg_catalog"});
        assert_eq!(v("s ~ '^pg_'", &r), json!(true));
        assert_eq!(v("s !~ '^pg_'", &r), json!(false));
        assert_eq!(v("s ~ '^PG_'", &r), json!(false));
        assert_eq!(v("s ~* '^PG_'", &r), json!(true));
        assert_eq!(v("s !~ '^zz'", &r), json!(true));
        // NULL propagates.
        assert_eq!(v("nosuch ~ '^x'", &r), Value::Null);
        // And an unsupported metacharacter is refused, not approximated.
        assert!(ev("s ~ 'a+b'", &r).is_err());
    }

    #[test]
    fn like_works_in_all_four_spellings() {
        let r = json!({"s": "Acme Pool"});
        assert_eq!(v("s LIKE 'Acme%'", &r), json!(true));
        assert_eq!(v("s LIKE 'acme%'", &r), json!(false));
        assert_eq!(v("s ILIKE 'acme%'", &r), json!(true));
        assert_eq!(v("s NOT LIKE 'zz%'", &r), json!(true));
        assert_eq!(v("nosuch LIKE 'x'", &r), Value::Null);
    }

    #[test]
    fn arithmetic_and_concatenation_propagate_null_and_refuse_div_by_zero() {
        let r = json!({"a": 7, "b": 2});
        assert_eq!(v("a + b", &r), json!(9));
        assert_eq!(v("a - b", &r), json!(5));
        assert_eq!(v("a * b", &r), json!(14));
        assert_eq!(v("a / b", &r), json!(3.5));
        assert_eq!(v("a % b", &r), json!(1));
        assert_eq!(v("-a", &r), json!(-7));
        // A non-integral result stays a float — the rule is about how INTEGRAL
        // values render, not about collapsing every number to an integer.
        assert_eq!(v("a / b", &r), json!(3.5));
        assert_eq!(v("b / a", &r), json!(2.0 / 7.0));
        assert_eq!(v("'x' || 'y'", &r), json!("xy"));
        assert_eq!(v("'x' || nosuch", &r), Value::Null);
        assert_eq!(v("a + nosuch", &r), Value::Null);
        // Division by zero is an ERROR in Postgres, not infinity. Returning
        // inf would be a confidently wrong number.
        assert!(ev("a / 0", &r).is_err());
        assert!(ev("a % 0", &r).is_err());
    }

    #[test]
    fn a_cast_is_transparent_rather_than_rejected() {
        // `pr.prattrs::pg_catalog.int2[]` appears in real catalogue SQL, and
        // the cast cannot change the answer for the shapes it is used on.
        let r = json!({"a": 7});
        assert_eq!(v("a::int2", &r), json!(7));
        assert_eq!(v("a::pg_catalog.int2[]", &r), json!(7));
    }

    // ── CASE ────────────────────────────────────────────────────────────────

    #[test]
    fn a_simple_CASE_picks_the_matching_branch() {
        // This is psql's \dt shape, with the real relkind values.
        let expr = "CASE k WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' \
                    WHEN 'i' THEN 'index' END";
        assert_eq!(v(expr, &json!({"k": "r"})), json!("table"));
        assert_eq!(v(expr, &json!({"k": "v"})), json!("view"));
        assert_eq!(v(expr, &json!({"k": "i"})), json!("index"));
        // No branch and no ELSE is NULL — which is exactly what \dt relies on
        // for a relkind it does not name.
        assert_eq!(v(expr, &json!({"k": "z"})), Value::Null);
    }

    #[test]
    fn a_searched_CASE_evaluates_predicates_and_UNKNOWN_does_not_match() {
        let expr = "CASE WHEN n > 5 THEN 'big' WHEN n > 0 THEN 'small' ELSE 'none' END";
        assert_eq!(v(expr, &json!({"n": 9})), json!("big"));
        assert_eq!(v(expr, &json!({"n": 2})), json!("small"));
        assert_eq!(v(expr, &json!({"n": -1})), json!("none"));
        // An UNKNOWN condition must not match — it falls through to ELSE.
        assert_eq!(v(expr, &json!({"other": 1})), json!("none"));
    }

    #[test]
    fn an_ELSE_branch_is_used_when_nothing_matches() {
        assert_eq!(
            v("CASE k WHEN 'r' THEN 'table' ELSE 'other' END", &json!({"k": "z"})),
            json!("other")
        );
    }

    // ── functions ───────────────────────────────────────────────────────────

    #[test]
    fn the_catalogue_functions_psql_calls_all_answer() {
        let r = json!({"o": 10, "enc": 6});
        // \dn and \dt both call this for the "Owner" column.
        assert_eq!(v("pg_get_userbyid(o)", &r), json!("nedb"));
        assert_eq!(v("pg_catalog.pg_get_userbyid(o)", &r), json!("nedb"));
        // \dt filters on this. Returning false would hide EVERY table.
        assert_eq!(v("pg_table_is_visible(o)", &r), json!(true));
        assert_eq!(v("pg_encoding_to_char(enc)", &r), json!("UTF8"));
        assert_eq!(v("current_schema", &r), json!("public"));
        assert_eq!(v("current_database()", &r), json!("nedb"));
        assert_eq!(v("current_user", &r), json!("nedb"));
        // The definition-printing functions return NULL rather than invented
        // DDL — NEDB has no DDL to print.
        assert_eq!(v("pg_get_expr(o, o)", &r), Value::Null);
        assert_eq!(v("obj_description(o)", &r), Value::Null);
    }

    #[test]
    fn text_and_null_handling_functions_work() {
        let r = json!({"s": "AbC", "n": null});
        assert_eq!(v("lower(s)", &r), json!("abc"));
        assert_eq!(v("upper(s)", &r), json!("ABC"));
        assert_eq!(v("length(s)", &r), json!(3));
        assert_eq!(v("lower(n)", &r), Value::Null);
        assert_eq!(v("coalesce(n, 'fallback')", &r), json!("fallback"));
        assert_eq!(v("coalesce(s, 'fallback')", &r), json!("AbC"));
        assert_eq!(v("coalesce(n, n)", &r), Value::Null);
        assert_eq!(v("nullif(s, 'AbC')", &r), Value::Null);
        assert_eq!(v("nullif(s, 'zz')", &r), json!("AbC"));
        // format_type names the type the same way information_schema does.
        assert_eq!(v("format_type(20, NULL)", &r), json!("bigint"));
    }

    #[test]
    fn coalesce_does_not_evaluate_past_its_first_non_null() {
        // `a / 0` would error; coalesce must never reach it.
        let r = json!({"a": 1});
        assert_eq!(v("coalesce(a, a / 0)", &r), json!(1));
    }

    #[test]
    fn an_unknown_function_is_REFUSED_rather_than_answered_with_NULL() {
        // A NULL column reads as missing DATA rather than a missing feature,
        // and somebody would file a data-loss bug against it.
        let e = ev("pg_stat_get_numscans(1)", &json!({})).unwrap_err().to_string();
        assert!(e.contains("pg_stat_get_numscans"), "{}", e);
        assert!(e.contains("refused"), "{}", e);
    }

    // ── join bindings ───────────────────────────────────────────────────────

    #[test]
    fn a_qualified_column_reads_only_its_OWN_binding() {
        // Both rows have `name`. Without qualifier isolation a join would
        // silently read the wrong table's column.
        let a = json!({"name": "left", "x": 1});
        let b = json!({"name": "right", "y": 2});
        let row = Bound {
            parts: vec![("a".into(), Some(&a)), ("b".into(), Some(&b))],
        };
        let get = |e: &str| {
            let s = parse(&format!("SELECT {} FROM x", e)).unwrap();
            eval(&s.items[0].expr, &row).unwrap()
        };
        assert_eq!(get("a.name"), json!("left"));
        assert_eq!(get("b.name"), json!("right"));
        // A bare name takes the first binding that HAS the key.
        assert_eq!(get("name"), json!("left"));
        assert_eq!(get("y"), json!(2), "a bare name still finds a later binding");
    }

    #[test]
    fn an_unmatched_LEFT_JOIN_side_reads_as_NULL_not_as_a_missing_column() {
        // The distinction is what makes `n.nspname IS NULL` answer correctly
        // for a row that found no match.
        let a = json!({"x": 1});
        let row = Bound {
            parts: vec![("a".into(), Some(&a)), ("b".into(), None)],
        };
        let get = |e: &str| {
            let s = parse(&format!("SELECT {} FROM x", e)).unwrap();
            eval(&s.items[0].expr, &row).unwrap()
        };
        assert_eq!(get("b.anything"), Value::Null);
        assert_eq!(get("b.anything IS NULL"), json!(true));
        assert_eq!(get("a.x"), json!(1));
    }
}

#[cfg(test)]
mod exec_tests {
    use super::*;
    use serde_json::json;

    /// A resolver over a fixed set of named tables.
    fn tables(defs: Vec<(&str, Vec<Value>)>) -> impl Fn(&str) -> Result<Option<Box<dyn Relation>>> {
        let owned: Vec<(String, Vec<Value>)> =
            defs.into_iter().map(|(n, r)| (n.to_string(), r)).collect();
        move |name: &str| {
            // Match on the bare name so `pg_catalog.pg_class` finds `pg_class`.
            let bare = name.rsplit('.').next().unwrap_or(name);
            Ok(owned
                .iter()
                .find(|(n, _)| n == name || n == bare)
                .map(|(_, r)| from_vec(r.clone())))
        }
    }

    fn go(sql: &str, r: &Resolver) -> (Vec<String>, Vec<Value>) {
        let (cols, rows) = run(sql, r).unwrap_or_else(|e| panic!("{}\n  -> {}", sql, e));
        (cols.into_iter().map(|c| c.name).collect(), rows)
    }

    fn col(rows: &[Value], name: &str) -> Vec<Value> {
        rows.iter().map(|r| r.get(name).cloned().unwrap_or(Value::Null)).collect()
    }

    // ── the basics, over one table ───────────────────────────────────────────

    #[test]
    fn select_columns_where_order_limit_offset() {
        let t = tables(vec![(
            "t",
            vec![json!({"a": 3, "s": "c"}), json!({"a": 1, "s": "a"}), json!({"a": 2, "s": "b"})],
        )]);
        let (names, rows) = go("SELECT a, s FROM t ORDER BY a", &t);
        assert_eq!(names, vec!["a", "s"]);
        assert_eq!(col(&rows, "a"), vec![json!(1), json!(2), json!(3)]);

        let (_, rows) = go("SELECT a FROM t ORDER BY a DESC", &t);
        assert_eq!(col(&rows, "a"), vec![json!(3), json!(2), json!(1)]);

        let (_, rows) = go("SELECT a FROM t WHERE a > 1 ORDER BY a", &t);
        assert_eq!(col(&rows, "a"), vec![json!(2), json!(3)]);

        let (_, rows) = go("SELECT a FROM t ORDER BY a LIMIT 2", &t);
        assert_eq!(col(&rows, "a"), vec![json!(1), json!(2)]);

        let (_, rows) = go("SELECT a FROM t ORDER BY a OFFSET 1", &t);
        assert_eq!(col(&rows, "a"), vec![json!(2), json!(3)]);

        let (_, rows) = go("SELECT a FROM t ORDER BY a LIMIT 1 OFFSET 1", &t);
        assert_eq!(col(&rows, "a"), vec![json!(2)]);

        // Past the end is an empty page, not an error.
        let (_, rows) = go("SELECT a FROM t OFFSET 99", &t);
        assert!(rows.is_empty());
    }

    #[test]
    fn an_output_column_takes_its_alias_or_a_derived_name() {
        // Clients index result columns BY NAME, so inventing a different name
        // breaks code that works against Postgres.
        let t = tables(vec![("t", vec![json!({"a": 1})])]);
        assert_eq!(go(r#"SELECT a AS "Name" FROM t"#, &t).0, vec!["Name"]);
        assert_eq!(go("SELECT a FROM t", &t).0, vec!["a"]);
        assert_eq!(go("SELECT lower('X') FROM t", &t).0, vec!["lower"]);
        assert_eq!(go("SELECT 1 + 1 FROM t", &t).0, vec!["?column?"]);
        assert_eq!(go("SELECT CASE a WHEN 1 THEN 'x' END FROM t", &t).0, vec!["case"]);
    }

    #[test]
    fn star_expands_from_the_rows_and_a_qualified_star_from_one_binding() {
        let t = tables(vec![
            ("a", vec![json!({"x": 1, "y": 2})]),
            ("b", vec![json!({"z": 3})]),
        ]);
        let (names, rows) = go("SELECT * FROM a", &t);
        assert_eq!(names, vec!["x", "y"]);
        assert_eq!(rows.len(), 1);

        let (names, _) = go("SELECT a.* FROM a CROSS JOIN b", &t);
        assert_eq!(names, vec!["x", "y"], "a qualified star takes ONE binding");

        // With no rows a `*` yields no columns, which is the honest answer for
        // a schemaless source: only a row knows what columns exist.
        let empty = tables(vec![("e", vec![])]);
        assert_eq!(go("SELECT * FROM e", &empty).0, Vec::<String>::new());
    }

    #[test]
    fn distinct_dedupes_on_the_projected_values() {
        let t = tables(vec![(
            "t",
            vec![json!({"g": "x"}), json!({"g": "x"}), json!({"g": "y"})],
        )]);
        let (_, rows) = go("SELECT DISTINCT g FROM t ORDER BY 1", &t);
        assert_eq!(col(&rows, "g"), vec![json!("x"), json!("y")]);
        let (_, rows) = go("SELECT g FROM t", &t);
        assert_eq!(rows.len(), 3, "without DISTINCT every row survives");
    }

    #[test]
    fn order_by_an_ORDINAL_sorts_the_projected_column() {
        let t = tables(vec![(
            "t",
            vec![json!({"a": 2, "b": "z"}), json!({"a": 1, "b": "y"})],
        )]);
        let (_, rows) = go("SELECT a, b FROM t ORDER BY 1", &t);
        assert_eq!(col(&rows, "a"), vec![json!(1), json!(2)]);
        let (_, rows) = go("SELECT a, b FROM t ORDER BY 2 DESC", &t);
        assert_eq!(col(&rows, "b"), vec![json!("z"), json!("y")]);
        // Out of range is an error naming the range, not a silent no-sort.
        let e = run("SELECT a FROM t ORDER BY 3", &t).unwrap_err().to_string();
        assert!(e.contains("out of range"), "{}", e);
    }

    #[test]
    fn order_by_an_expression_may_use_a_column_NOT_in_the_select_list() {
        let t = tables(vec![(
            "t",
            vec![json!({"a": 1, "hidden": 9}), json!({"a": 2, "hidden": 1})],
        )]);
        let (_, rows) = go("SELECT a FROM t ORDER BY hidden", &t);
        assert_eq!(col(&rows, "a"), vec![json!(2), json!(1)]);
    }

    #[test]
    fn null_ordering_follows_the_direction_defaults() {
        let t = tables(vec![(
            "t",
            vec![json!({"a": 2}), json!({"a": null}), json!({"a": 1})],
        )]);
        // ASC defaults to NULLS LAST.
        assert_eq!(col(&go("SELECT a FROM t ORDER BY a", &t).1, "a"),
                   vec![json!(1), json!(2), Value::Null]);
        // DESC defaults to NULLS FIRST.
        assert_eq!(col(&go("SELECT a FROM t ORDER BY a DESC", &t).1, "a"),
                   vec![Value::Null, json!(2), json!(1)]);
        // An explicit clause overrides the default.
        assert_eq!(col(&go("SELECT a FROM t ORDER BY a NULLS FIRST", &t).1, "a"),
                   vec![Value::Null, json!(1), json!(2)]);
    }

    #[test]
    fn a_where_clause_that_is_UNKNOWN_excludes_the_row() {
        let t = tables(vec![(
            "t",
            vec![json!({"a": 1}), json!({"a": null}), json!({"other": 1})],
        )]);
        // Only the row where the comparison is TRUE survives; UNKNOWN drops.
        let (_, rows) = go("SELECT a FROM t WHERE a = 1", &t);
        assert_eq!(rows.len(), 1);
        // And NOT over UNKNOWN is still UNKNOWN, so it drops too.
        let (_, rows) = go("SELECT a FROM t WHERE NOT (a = 1)", &t);
        assert_eq!(rows.len(), 0, "NOT UNKNOWN must not resurrect a null row");
    }

    #[test]
    fn select_with_no_FROM_returns_exactly_one_row() {
        // A client's liveness probe is written this way.
        let t = tables(vec![]);
        let (names, rows) = go("SELECT 1", &t);
        assert_eq!(rows.len(), 1);
        assert_eq!(names, vec!["?column?"]);
        assert_eq!(go("SELECT current_schema", &t).1.len(), 1);
    }

    #[test]
    fn an_unknown_relation_is_NAMED_rather_than_answered_with_no_rows() {
        // An unknown table that returned zero rows would look exactly like an
        // empty one, which is how "where did my data go" starts.
        let t = tables(vec![("t", vec![])]);
        let e = run("SELECT a FROM nosuchtable", &t).unwrap_err().to_string();
        assert!(e.contains("nosuchtable"), "{}", e);
        assert!(e.contains("does not exist"), "{}", e);
    }

    // ── joins ────────────────────────────────────────────────────────────────

    #[test]
    fn an_inner_join_keeps_only_matching_pairs() {
        let t = tables(vec![
            ("l", vec![json!({"id": 1, "n": "a"}), json!({"id": 2, "n": "b"})]),
            ("r", vec![json!({"lid": 1, "v": "x"})]),
        ]);
        let (_, rows) = go("SELECT l.n, r.v FROM l JOIN r ON r.lid = l.id", &t);
        assert_eq!(rows.len(), 1);
        assert_eq!(col(&rows, "n"), vec![json!("a")]);
    }

    #[test]
    fn a_LEFT_join_keeps_unmatched_left_rows_with_NULLs() {
        // This is the shape psql's \dt uses twice.
        let t = tables(vec![
            ("l", vec![json!({"id": 1, "n": "a"}), json!({"id": 2, "n": "b"})]),
            ("r", vec![json!({"lid": 1, "v": "x"})]),
        ]);
        let (_, rows) = go("SELECT l.n, r.v FROM l LEFT JOIN r ON r.lid = l.id ORDER BY 1", &t);
        assert_eq!(rows.len(), 2);
        assert_eq!(col(&rows, "n"), vec![json!("a"), json!("b")]);
        assert_eq!(col(&rows, "v"), vec![json!("x"), Value::Null]);
    }

    #[test]
    fn a_RIGHT_join_keeps_unmatched_right_rows_and_FULL_keeps_both() {
        let t = tables(vec![
            ("l", vec![json!({"id": 1})]),
            ("r", vec![json!({"lid": 1}), json!({"lid": 9})]),
        ]);
        let (_, rows) = go("SELECT l.id, r.lid FROM l RIGHT JOIN r ON r.lid = l.id", &t);
        assert_eq!(rows.len(), 2);
        assert!(col(&rows, "id").contains(&Value::Null), "the unmatched right row keeps NULLs on the left");

        let t2 = tables(vec![
            ("l", vec![json!({"id": 1}), json!({"id": 5})]),
            ("r", vec![json!({"lid": 1}), json!({"lid": 9})]),
        ]);
        let (_, rows) = go("SELECT l.id, r.lid FROM l FULL OUTER JOIN r ON r.lid = l.id", &t2);
        assert_eq!(rows.len(), 3, "one match plus one orphan on each side");
    }

    #[test]
    fn a_cross_join_is_the_cartesian_product() {
        let t = tables(vec![
            ("a", vec![json!({"x": 1}), json!({"x": 2})]),
            ("b", vec![json!({"y": 1}), json!({"y": 2}), json!({"y": 3})]),
        ]);
        assert_eq!(go("SELECT a.x, b.y FROM a CROSS JOIN b", &t).1.len(), 6);
        // A comma FROM list means the same thing.
        assert_eq!(go("SELECT a.x, b.y FROM a, b", &t).1.len(), 6);
    }

    #[test]
    fn an_ON_clause_that_is_UNKNOWN_does_not_join() {
        // Treating UNKNOWN as a match would invent pairings out of missing
        // data — rows that exist in neither table.
        let t = tables(vec![
            ("l", vec![json!({"id": null})]),
            ("r", vec![json!({"lid": null})]),
        ]);
        let (_, rows) = go("SELECT l.id FROM l JOIN r ON r.lid = l.id", &t);
        assert!(rows.is_empty(), "NULL = NULL is UNKNOWN, so nothing joins");
        // …and on a LEFT JOIN the left row survives with NULLs.
        let (_, rows) = go("SELECT l.id FROM l LEFT JOIN r ON r.lid = l.id", &t);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn two_joins_chain() {
        let t = tables(vec![
            ("a", vec![json!({"id": 1, "bid": 10, "cid": 100})]),
            ("b", vec![json!({"id": 10, "bn": "B"})]),
            ("c", vec![json!({"id": 100, "cn": "C"})]),
        ]);
        let (_, rows) = go(
            "SELECT a.id, b.bn, c.cn FROM a \
             LEFT JOIN b ON b.id = a.bid \
             LEFT JOIN c ON c.id = a.cid",
            &t,
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(col(&rows, "bn"), vec![json!("B")]);
        assert_eq!(col(&rows, "cn"), vec![json!("C")]);
    }

    // ── THE acceptance tests ─────────────────────────────────────────────────

    /// The catalogue rows psql's `\dn` and `\dt` actually read.
    fn catalog() -> impl Fn(&str) -> Result<Option<Box<dyn Relation>>> {
        tables(vec![
            (
                "pg_namespace",
                vec![
                    json!({"oid": 2200, "nspname": "public", "nspowner": 10}),
                    json!({"oid": 11, "nspname": "pg_catalog", "nspowner": 10}),
                    json!({"oid": 13000, "nspname": "information_schema", "nspowner": 10}),
                ],
            ),
            (
                "pg_class",
                vec![
                    json!({"oid": 16401, "relname": "orders", "relnamespace": 2200,
                           "relkind": "r", "relowner": 10, "relam": 2}),
                    json!({"oid": 16402, "relname": "drivers", "relnamespace": 2200,
                           "relkind": "r", "relowner": 10, "relam": 2}),
                ],
            ),
            ("pg_am", vec![json!({"oid": 2, "amname": "heap"})]),
        ])
    }

    #[test]
    fn THE_dn_QUERY_RUNS_AND_RETURNS_THE_RIGHT_ROWS() {
        let (names, rows) = go(
            r#"SELECT n.nspname AS "Name",
                 pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
               FROM pg_catalog.pg_namespace n
               WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
               ORDER BY 1;"#,
            &catalog(),
        );

        assert_eq!(names, vec!["Name", "Owner"], "psql reads these BY NAME");
        // `pg_catalog` is excluded by the regex, `information_schema` by the
        // `<>` — leaving exactly the one schema a user cares about.
        assert_eq!(col(&rows, "Name"), vec![json!("public")]);
        assert_eq!(col(&rows, "Owner"), vec![json!("nedb")]);
    }

    #[test]
    fn THE_dt_QUERY_RUNS_AND_RETURNS_THE_RIGHT_ROWS() {
        let (names, rows) = go(
            r#"SELECT n.nspname as "Schema",
                 c.relname as "Name",
                 CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view'
                   WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index'
                   WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table'
                   WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table'
                   WHEN 'I' THEN 'partitioned index' END as "Type",
                 pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
               FROM pg_catalog.pg_class c
                    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                    LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
               WHERE c.relkind IN ('r','p','')
                     AND n.nspname <> 'pg_catalog'
                     AND n.nspname !~ '^pg_toast'
                     AND n.nspname <> 'information_schema'
                 AND pg_catalog.pg_table_is_visible(c.oid)
               ORDER BY 1,2;"#,
            &catalog(),
        );

        assert_eq!(names, vec!["Schema", "Name", "Type", "Owner"]);
        // ORDER BY 1,2 — schema then name, so `drivers` precedes `orders`.
        assert_eq!(col(&rows, "Name"), vec![json!("drivers"), json!("orders")]);
        assert_eq!(col(&rows, "Schema"), vec![json!("public"), json!("public")]);
        // The nine-branch CASE resolves relkind 'r'.
        assert_eq!(col(&rows, "Type"), vec![json!("table"), json!("table")]);
        assert_eq!(col(&rows, "Owner"), vec![json!("nedb"), json!("nedb")]);
    }

    #[test]
    fn the_dt_query_still_filters_correctly_with_a_system_relation_present() {
        // A relation in pg_catalog must be excluded by the `<>`, and one with
        // an unlisted relkind by the IN list. If either filter were dropped —
        // the bug the parser restructure fixed — \dt would list internals.
        let t = tables(vec![
            (
                "pg_namespace",
                vec![
                    json!({"oid": 2200, "nspname": "public", "nspowner": 10}),
                    json!({"oid": 11, "nspname": "pg_catalog", "nspowner": 10}),
                ],
            ),
            (
                "pg_class",
                vec![
                    json!({"oid": 1, "relname": "mine", "relnamespace": 2200,
                           "relkind": "r", "relowner": 10, "relam": 2}),
                    json!({"oid": 2, "relname": "pg_internal", "relnamespace": 11,
                           "relkind": "r", "relowner": 10, "relam": 2}),
                    json!({"oid": 3, "relname": "an_index", "relnamespace": 2200,
                           "relkind": "i", "relowner": 10, "relam": 2}),
                ],
            ),
            ("pg_am", vec![json!({"oid": 2, "amname": "heap"})]),
        ]);
        let (_, rows) = go(
            r#"SELECT c.relname as "Name" FROM pg_catalog.pg_class c
                 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
               WHERE c.relkind IN ('r','p','') AND n.nspname <> 'pg_catalog'
               ORDER BY 1"#,
            &t,
        );
        assert_eq!(col(&rows, "Name"), vec![json!("mine")],
                   "a system relation and an index must both be filtered out");
    }
}

#[cfg(test)]
mod operator_syntax_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_OPERATOR_qualification_psql_generates_is_understood() {
        // `\d` writes every operator this way:
        //   c.relname OPERATOR(pg_catalog.~) '^(orders)$'
        // It names exactly the operator it wraps, so the schema is dropped.
        let s = parse(
            "SELECT a FROM t WHERE n OPERATOR(pg_catalog.~) '^x' \
             AND m OPERATOR(pg_catalog.=) 1",
        )
        .expect("psql's OPERATOR() form must parse");
        match s.where_.unwrap() {
            Expr::Binary { op, left, .. } => {
                assert_eq!(op, "AND");
                assert!(matches!(*left, Expr::Binary { ref op, .. } if op == "~"));
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn an_OPERATOR_qualified_comparison_EVALUATES() {
        let t = |_: &str| -> Result<Option<Box<dyn Relation>>> {
            Ok(Some(from_vec(vec![json!({"n": "orders"}), json!({"n": "pg_toast_1"})])))
        };
        let (_, rows) = run(
            "SELECT n FROM pg_class WHERE n OPERATOR(pg_catalog.~) '^ord'",
            &t,
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["n"], json!("orders"));
    }

    #[test]
    fn a_subquery_an_ARRAY_constructor_and_EXISTS_are_all_refused_BY_NAME() {
        // "expected ')', got SELECT" is a parser internal and tells the reader
        // nothing about what to change. These are the constructs `\d` and
        // `\dp` actually hinge on, so these are the messages someone reads.
        for (sql, needle) in [
            ("SELECT a FROM t WHERE x = (SELECT 1)", "subquery"),
            ("SELECT array_to_string(ARRAY(SELECT a FROM b), ',') FROM t", "ARRAY"),
            ("SELECT a FROM t WHERE EXISTS (SELECT 1)", "EXISTS"),
        ] {
            let e = parse(sql).unwrap_err().to_string();
            assert!(e.contains(needle), "{} -> {}", sql, e);
        }
    }
}

#[cfg(test)]
mod collate_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn COLLATE_is_consumed_because_it_cannot_change_the_answer() {
        // psql writes `COLLATE pg_catalog."C"` throughout `\d`. NEDB has one
        // collation, so refusing a clause that provably has no effect would
        // reject a query whose result is already correct.
        for sql in [
            r#"SELECT a FROM t ORDER BY a COLLATE "C""#,
            r#"SELECT a COLLATE "C" FROM t"#,
            r#"SELECT a FROM t WHERE a COLLATE pg_catalog."C" = 'x'"#,
        ] {
            parse(sql).unwrap_or_else(|e| panic!("{} -> {}", sql, e));
        }
        // A malformed COLLATE is still an error rather than silently skipped.
        assert!(parse("SELECT a FROM t ORDER BY a COLLATE").is_err());
    }

    #[test]
    fn a_COLLATE_annotated_comparison_still_evaluates() {
        let t = |_: &str| -> Result<Option<Box<dyn Relation>>> {
            Ok(Some(from_vec(vec![json!({"n": "b"}), json!({"n": "a"})])))
        };
        let (_, rows) = run(r#"SELECT n FROM pg_class ORDER BY n COLLATE "C""#, &t).unwrap();
        assert_eq!(rows[0]["n"], json!("a"), "the ORDER BY still sorts");
    }
}
