// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

// nedb-core — NQL (the NEDB Query Language) parser.
//
// Matches the Python reference grammar exactly. Returns a `Plan` struct that the
// engine executes against the MvccStore + Indexes + Relations.

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Eq, Ne, Lt, Le, Gt, Ge,
}

impl Op {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "="  | "==" => Some(Op::Eq),
            "!=" | "<>" => Some(Op::Ne),
            "<"  => Some(Op::Lt),
            "<=" => Some(Op::Le),
            ">"  => Some(Op::Gt),
            ">=" => Some(Op::Ge),
            _    => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Val {
    Str(String),
    Num(f64),
    Bool(bool),
    Null,
}

impl Val {
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Val::Str(s)  => serde_json::Value::String(s.clone()),
            Val::Num(n)  => serde_json::json!(n),
            Val::Bool(b) => serde_json::Value::Bool(*b),
            Val::Null    => serde_json::Value::Null,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Condition {
    pub field: String,
    pub op:    Op,
    pub value: Val,
}

#[derive(Clone, Debug, Default)]
pub struct Plan {
    pub from:     String,
    pub as_of:    Option<u64>,
    pub where_:   Vec<Condition>,
    pub search:   Option<String>,
    pub order_by: Option<(String, bool)>, // (field, desc)
    pub traverse: Option<String>,
    pub limit:    Option<usize>,
    pub group_by: Option<String>,
    pub agg:      Option<(String, Option<String>)>, // (fn, field)
}

// ── Tokeniser ────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Kw(String),
    Word(String),
    Str(String),
    Num(f64),
    Op(String),
}

fn lex(s: &str) -> Result<Vec<Tok>, String> {
    let s = s.trim();
    let bytes = s.as_bytes();
    let mut i = 0;
    let mut toks = Vec::new();

    const KEYWORDS: &[&str] = &[
        "from","as","of","where","and","search","order","by","asc","desc",
        "traverse","limit","true","false","null","group","count","sum","avg","min","max",
    ];

    while i < bytes.len() {
        match bytes[i] {
            b' ' | b'\t' | b'\n' | b'\r' => { i += 1; }
            b'"' | b'\'' => {
                let q = bytes[i];
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i] != q { i += 1; }
                let v = &s[start..i];
                i += 1;
                toks.push(Tok::Str(v.to_string()));
            }
            b'<' | b'>' | b'!' | b'=' => {
                let start = i;
                i += 1;
                if i < bytes.len() && (bytes[i] == b'=' || bytes[i] == b'>') {
                    i += 1;
                }
                toks.push(Tok::Op(s[start..i].to_string()));
            }
            b'0'..=b'9' | b'-' => {
                let start = i;
                if bytes[i] == b'-' { i += 1; }
                while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') { i += 1; }
                let n: f64 = s[start..i].parse().map_err(|_| format!("bad number at {start}"))?;
                toks.push(Tok::Num(n));
            }
            b if b.is_ascii_alphabetic() || b == b'_' => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let word = &s[start..i];
                let lw = word.to_lowercase();
                if KEYWORDS.contains(&lw.as_str()) {
                    toks.push(Tok::Kw(lw));
                } else {
                    toks.push(Tok::Word(word.to_string()));
                }
            }
            c => return Err(format!("unexpected char '{}'", c as char)),
        }
    }
    Ok(toks)
}

// ── Parser ────────────────────────────────────────────────────────────────────

pub fn parse(nql: &str) -> Result<Plan, String> {
    let toks = lex(nql)?;
    let mut i = 0;
    let mut plan = Plan::default();

    macro_rules! peek {
        () => { toks.get(i) }
    }
    macro_rules! eat_kw {
        ($kw:expr) => {{
            match toks.get(i) {
                Some(Tok::Kw(k)) if k.as_str() == $kw => { i += 1; }
                other => return Err(format!("expected {}, got {:?}", $kw, other)),
            }
        }}
    }
    macro_rules! ident {
        () => {
            match toks.get(i) {
                Some(Tok::Word(w)) => { i += 1; w.clone() }
                Some(Tok::Kw(k))  => { i += 1; k.clone() }
                other => return Err(format!("expected identifier, got {:?}", other)),
            }
        }
    }
    macro_rules! value {
        () => {
            match toks.get(i) {
                Some(Tok::Str(s))           => { i += 1; Val::Str(s.clone()) }
                Some(Tok::Num(n))           => { i += 1; Val::Num(*n) }
                Some(Tok::Kw(k)) if k == "true"  => { i += 1; Val::Bool(true) }
                Some(Tok::Kw(k)) if k == "false" => { i += 1; Val::Bool(false) }
                Some(Tok::Kw(k)) if k == "null"  => { i += 1; Val::Null }
                Some(Tok::Word(w)) => { i += 1; Val::Str(w.clone()) }
                other => return Err(format!("expected value, got {:?}", other)),
            }
        }
    }

    eat_kw!("from");
    plan.from = ident!();

    // AS OF
    if matches!(peek!(), Some(Tok::Kw(k)) if k == "as") {
        i += 1; eat_kw!("of");
        match toks.get(i) {
            Some(Tok::Num(n)) => { plan.as_of = Some(*n as u64); i += 1; }
            other => return Err(format!("AS OF expects integer, got {:?}", other)),
        }
    }

    // WHERE
    if matches!(peek!(), Some(Tok::Kw(k)) if k == "where") {
        i += 1;
        loop {
            let field = ident!();
            let op_tok = match toks.get(i) {
                Some(Tok::Op(o)) => { i += 1; Op::from_str(o).ok_or(format!("unknown op {o}"))? }
                other => return Err(format!("expected operator, got {:?}", other)),
            };
            let val = value!();
            plan.where_.push(Condition { field, op: op_tok, value: val });
            if matches!(peek!(), Some(Tok::Kw(k)) if k == "and") {
                i += 1;
            } else {
                break;
            }
        }
    }

    // SEARCH
    if matches!(peek!(), Some(Tok::Kw(k)) if k == "search") {
        i += 1;
        match toks.get(i) {
            Some(Tok::Str(s)) => { plan.search = Some(s.clone()); i += 1; }
            other => return Err(format!("SEARCH expects quoted string, got {:?}", other)),
        }
    }

    // ORDER BY
    if matches!(peek!(), Some(Tok::Kw(k)) if k == "order") {
        i += 1; eat_kw!("by");
        let field = ident!();
        let desc = if matches!(peek!(), Some(Tok::Kw(k)) if k == "desc") {
            i += 1; true
        } else {
            if matches!(peek!(), Some(Tok::Kw(k)) if k == "asc") { i += 1; }
            false
        };
        plan.order_by = Some((field, desc));
    }

    // TRAVERSE
    if matches!(peek!(), Some(Tok::Kw(k)) if k == "traverse") {
        i += 1;
        plan.traverse = Some(ident!());
    }

    // LIMIT
    if matches!(peek!(), Some(Tok::Kw(k)) if k == "limit") {
        i += 1;
        match toks.get(i) {
            Some(Tok::Num(n)) => { plan.limit = Some(*n as usize); i += 1; }
            other => return Err(format!("LIMIT expects integer, got {:?}", other)),
        }
    }

    // GROUP BY [COUNT | SUM f | AVG f | MIN f | MAX f]
    if matches!(peek!(), Some(Tok::Kw(k)) if k == "group") {
        i += 1; eat_kw!("by");
        plan.group_by = Some(ident!());
        if let Some(Tok::Kw(agg)) = toks.get(i) {
            let agg = agg.clone();
            if ["count","sum","avg","min","max"].contains(&agg.as_str()) {
                i += 1;
                if agg == "count" {
                    plan.agg = Some(("count".into(), None));
                } else {
                    let f = ident!();
                    plan.agg = Some((agg, Some(f)));
                }
            }
        }
    }

    if i != toks.len() {
        return Err(format!("unexpected trailing tokens: {:?}", &toks[i..]));
    }
    Ok(plan)
}

// ── Comparator ────────────────────────────────────────────────────────────────

pub fn cmp(doc_val: &serde_json::Value, op: &Op, query_val: &Val) -> bool {
    // For numeric equality/inequality, compare f64 values rather than the
    // serde_json::Value directly. `serde_json::json!(3.0f64)` stores the
    // number as N::Float(3.0), but a JSON integer `3` in a document is stored
    // as N::PosInt(3). serde_json's PartialEq considers these NOT equal even
    // though they represent the same number — causing WHERE n = 3 to reject
    // documents where n was stored as an integer.
    let qv = query_val.to_json();
    match op {
        Op::Eq => {
            // Numeric equality: compare as f64 so integer 3 == float 3.0
            match (doc_val.as_f64(), qv.as_f64()) {
                (Some(a), Some(b)) => (a - b).abs() < f64::EPSILON * a.abs().max(b.abs()).max(1.0),
                // Fall back to exact serde_json equality for non-numeric types
                _ => doc_val == &qv,
            }
        }
        Op::Ne => {
            match (doc_val.as_f64(), qv.as_f64()) {
                (Some(a), Some(b)) => (a - b).abs() >= f64::EPSILON * a.abs().max(b.abs()).max(1.0),
                _ => doc_val != &qv,
            }
        }
        _ => {
            // Numeric or string comparison
            let a = doc_val.as_f64().or_else(|| doc_val.as_str().map(|_| f64::NAN));
            let b = qv.as_f64();
            match (a, b) {
                (Some(a), Some(b)) => match op {
                    Op::Lt => a < b,
                    Op::Le => a <= b,
                    Op::Gt => a > b,
                    Op::Ge => a >= b,
                    _      => false,
                },
                _ => {
                    let sa = doc_val.as_str().unwrap_or("");
                    let sb = qv.as_str().unwrap_or("");
                    match op {
                        Op::Lt => sa < sb,
                        Op::Le => sa <= sb,
                        Op::Gt => sa > sb,
                        Op::Ge => sa >= sb,
                        _      => false,
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic() {
        let p = parse(r#"FROM users WHERE status = "active" ORDER BY age DESC LIMIT 10"#).unwrap();
        assert_eq!(p.from, "users");
        assert_eq!(p.where_.len(), 1);
        assert_eq!(p.where_[0].field, "status");
        assert_eq!(p.where_[0].value, Val::Str("active".into()));
        assert_eq!(p.order_by, Some(("age".into(), true)));
        assert_eq!(p.limit, Some(10));
    }

    #[test]
    fn parse_as_of() {
        let p = parse("FROM events AS OF 42").unwrap();
        assert_eq!(p.as_of, Some(42));
    }

    #[test]
    fn parse_group_by() {
        let p = parse("FROM orders GROUP BY status COUNT").unwrap();
        assert_eq!(p.group_by, Some("status".into()));
        assert_eq!(p.agg, Some(("count".into(), None)));
    }
}
