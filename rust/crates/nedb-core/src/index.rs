// nedb-core — secondary indexes: equality, ordered, sorted, and full-text search.
//
// - Eq:      HashMap<field_value_as_string, HashSet<key>> — O(1) equality lookup.
// - Ordered: sorted Vec<(string, key)> — legacy stringly-typed sort (kept for
//            back-compat with existing on-disk index configs).
// - Sorted:  BTreeMap<OrderedValue, Vec<key>> — JSON-typed total ordering, used
//            by NQL `ORDER BY field [ASC|DESC] LIMIT n` to return the top-n keys
//            in milliseconds without scanning the whole collection (v1.3.0+).
// - Search:  inverted token → HashSet<key> for AND-of-tokens text search.
//
// All indexes are maintained incrementally on every `put`/`delete` and rebuilt
// from the AOF on startup (in-memory only — never persisted to disk).

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::Value;

/// Which kind of index was created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexKind {
    Eq,
    Ordered,
    /// JSON-typed sorted index backed by BTreeMap<OrderedValue, Vec<key>>.
    /// Used by NQL ORDER BY ... LIMIT n for top-n queries without a full sort.
    Sorted,
    Search,
}

/// A total ordering over `serde_json::Value` for use as a `BTreeMap` key.
///
/// JSON has no canonical ordering — we adopt the convention:
///     `null  <  bool  <  number  <  string  <  array  <  object`
///
/// Within each type:
/// - bools: false < true
/// - numbers: compared as f64 (integer JSON numbers are promoted to f64); NaN
///   is treated as Equal to NaN and Greater than everything else so that the
///   ordering stays total and stable.
/// - strings: lexicographic (Rust's default `Ord` on `str`)
/// - arrays/objects: serialized to canonical JSON and compared lexicographically
///   — rare in indexed fields, but the impl stays total.
#[derive(Clone, Debug)]
pub struct OrderedValue(pub Value);

impl OrderedValue {
    fn type_rank(&self) -> u8 {
        match &self.0 {
            Value::Null      => 0,
            Value::Bool(_)   => 1,
            Value::Number(_) => 2,
            Value::String(_) => 3,
            Value::Array(_)  => 4,
            Value::Object(_) => 5,
        }
    }
}

impl PartialEq for OrderedValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for OrderedValue {}

impl PartialOrd for OrderedValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedValue {
    fn cmp(&self, other: &Self) -> Ordering {
        let (a, b) = (self.type_rank(), other.type_rank());
        if a != b { return a.cmp(&b); }
        match (&self.0, &other.0) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
            (Value::Number(x), Value::Number(y)) => {
                let xf = x.as_f64().unwrap_or(f64::NAN);
                let yf = y.as_f64().unwrap_or(f64::NAN);
                match xf.partial_cmp(&yf) {
                    Some(o) => o,
                    None => {
                        // NaN handling: NaN == NaN, NaN > non-NaN.
                        match (xf.is_nan(), yf.is_nan()) {
                            (true, true)   => Ordering::Equal,
                            (true, false)  => Ordering::Greater,
                            (false, true)  => Ordering::Less,
                            (false, false) => Ordering::Equal,
                        }
                    }
                }
            }
            (Value::String(x), Value::String(y)) => x.cmp(y),
            (Value::Array(_),  Value::Array(_))  |
            (Value::Object(_), Value::Object(_)) => {
                let sa = serde_json::to_string(&self.0).unwrap_or_default();
                let sb = serde_json::to_string(&other.0).unwrap_or_default();
                sa.cmp(&sb)
            }
            // Different concrete types within the same rank can't actually happen
            // (rank is a function of the variant) but be defensive.
            _ => Ordering::Equal,
        }
    }
}

/// One index: collection, field, kind, and the underlying data structure.
pub struct Index {
    pub coll:  String,
    pub field: String,
    pub kind:  IndexKind,
    /// Eq: field-value → {key, ...}
    eq: HashMap<String, HashSet<String>>,
    /// Ordered (legacy stringly-typed): sorted Vec<(field_value_as_string, key)>
    ordered: Vec<(String, String)>,
    /// Sorted (JSON-typed): BTreeMap<OrderedValue, Vec<key>> — supports
    /// O(log n + k) top-k iteration for ORDER BY ... LIMIT k.
    sorted: BTreeMap<OrderedValue, Vec<String>>,
    /// Reverse lookup so deletes can find a key's previous indexed value
    /// without rescanning the whole BTreeMap.
    sorted_by_key: HashMap<String, OrderedValue>,
    /// Search: token → {key, ...}
    inv: HashMap<String, HashSet<String>>,
}

fn tokenize(s: &str) -> Vec<String> {
    s.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(String::from)
        .collect()
}

fn val_str(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b)   => Some(b.to_string()),
        _ => None,
    }
}

impl Index {
    pub fn new(coll: &str, field: &str, kind: IndexKind) -> Self {
        Self {
            coll: coll.to_string(),
            field: field.to_string(),
            kind,
            eq: HashMap::new(),
            ordered: Vec::new(),
            sorted: BTreeMap::new(),
            sorted_by_key: HashMap::new(),
            inv: HashMap::new(),
        }
    }

    pub fn add(&mut self, key: &str, doc: &Value) {
        let Some(fval) = doc.get(&self.field) else { return };
        match self.kind {
            IndexKind::Eq => {
                if let Some(s) = val_str(fval) {
                    self.eq.entry(s).or_default().insert(key.to_string());
                }
            }
            IndexKind::Ordered => {
                if let Some(s) = val_str(fval) {
                    // Remove stale entry first (update path)
                    self.ordered.retain(|(_, k)| k != key);
                    let pos = self.ordered.partition_point(|(v, _)| v.as_str() <= s.as_str());
                    self.ordered.insert(pos, (s, key.to_string()));
                }
            }
            IndexKind::Sorted => {
                // Remove any prior entry for this key (update path) before inserting.
                if let Some(prev) = self.sorted_by_key.remove(key) {
                    if let Some(bucket) = self.sorted.get_mut(&prev) {
                        bucket.retain(|k| k != key);
                        if bucket.is_empty() {
                            self.sorted.remove(&prev);
                        }
                    }
                }
                let ov = OrderedValue(fval.clone());
                self.sorted.entry(ov.clone()).or_default().push(key.to_string());
                self.sorted_by_key.insert(key.to_string(), ov);
            }
            IndexKind::Search => {
                if let Some(s) = fval.as_str() {
                    for tok in tokenize(s) {
                        self.inv.entry(tok).or_default().insert(key.to_string());
                    }
                }
            }
        }
    }

    pub fn remove(&mut self, key: &str, doc: &Value) {
        let Some(fval) = doc.get(&self.field) else { return };
        match self.kind {
            IndexKind::Eq => {
                if let Some(s) = val_str(fval) {
                    if let Some(set) = self.eq.get_mut(&s) {
                        set.remove(key);
                    }
                }
            }
            IndexKind::Ordered => {
                self.ordered.retain(|(_, k)| k != key);
            }
            IndexKind::Sorted => {
                let prev = self.sorted_by_key.remove(key);
                let target = prev.unwrap_or_else(|| OrderedValue(fval.clone()));
                if let Some(bucket) = self.sorted.get_mut(&target) {
                    bucket.retain(|k| k != key);
                    if bucket.is_empty() {
                        self.sorted.remove(&target);
                    }
                }
            }
            IndexKind::Search => {
                if let Some(s) = fval.as_str() {
                    for tok in tokenize(s) {
                        if let Some(set) = self.inv.get_mut(&tok) {
                            set.remove(key);
                        }
                    }
                }
            }
        }
    }

    /// Equality lookup — returns matching store keys.
    pub fn eq_lookup(&self, value: &str) -> Option<HashSet<String>> {
        self.eq.get(value).cloned()
    }

    /// Full-text lookup for a single token — returns matching store keys.
    pub fn search_lookup(&self, token: &str) -> Option<HashSet<String>> {
        self.inv.get(token).cloned()
    }

    /// Full-text lookup: AND of all tokens in the query string.
    pub fn search_all(&self, text: &str) -> HashSet<String> {
        let tokens = tokenize(text);
        if tokens.is_empty() {
            return HashSet::new();
        }
        let mut result: Option<HashSet<String>> = None;
        for tok in &tokens {
            let hits = self.inv.get(tok).cloned().unwrap_or_default();
            result = Some(match result {
                None    => hits,
                Some(r) => r.intersection(&hits).cloned().collect(),
            });
        }
        result.unwrap_or_default()
    }

    /// Top-k keys by sorted-index order. `desc == false` → ASC, else DESC.
    /// Returns the first `limit` keys in index order, with stable tie-break
    /// (insertion order within a bucket).
    pub fn sorted_top_k(&self, desc: bool, limit: usize) -> Vec<String> {
        if self.kind != IndexKind::Sorted || limit == 0 {
            return Vec::new();
        }
        let mut out: Vec<String> = Vec::with_capacity(limit);
        if desc {
            for (_, bucket) in self.sorted.iter().rev() {
                for k in bucket {
                    out.push(k.clone());
                    if out.len() == limit { return out; }
                }
            }
        } else {
            for (_, bucket) in self.sorted.iter() {
                for k in bucket {
                    out.push(k.clone());
                    if out.len() == limit { return out; }
                }
            }
        }
        out
    }

    /// Total ordering walk (no limit). Used as a fast pre-sort when ORDER BY
    /// exists but LIMIT does not — still saves the O(n log n) sort.
    pub fn sorted_all(&self, desc: bool) -> Vec<String> {
        if self.kind != IndexKind::Sorted {
            return Vec::new();
        }
        let mut out: Vec<String> = Vec::with_capacity(self.sorted_by_key.len());
        if desc {
            for (_, bucket) in self.sorted.iter().rev() {
                out.extend(bucket.iter().cloned());
            }
        } else {
            for (_, bucket) in self.sorted.iter() {
                out.extend(bucket.iter().cloned());
            }
        }
        out
    }
}

/// The full collection of indexes for a database.
#[derive(Default)]
pub struct Indexes {
    /// (coll, field, kind) — for persistence / round-trip
    pub config: Vec<(String, String, IndexKind)>,
    /// Inner map keyed by (coll, field)
    map: HashMap<(String, String), Index>,
}

impl Indexes {
    pub fn ensure(&mut self, coll: &str, field: &str, kind: IndexKind) {
        let k = (coll.to_string(), field.to_string());
        if !self.map.contains_key(&k) {
            self.config.push((coll.to_string(), field.to_string(), kind.clone()));
            self.map.insert(k, Index::new(coll, field, kind));
        }
    }

    pub fn add(&mut self, coll: &str, key: &str, doc: &Value) {
        for idx in self.map.values_mut() {
            if idx.coll == coll {
                idx.add(key, doc);
            }
        }
    }

    pub fn remove(&mut self, coll: &str, key: &str, doc: &Value) {
        for idx in self.map.values_mut() {
            if idx.coll == coll {
                idx.remove(key, doc);
            }
        }
    }

    pub fn has_eq(&self, coll: &str, field: &str) -> bool {
        self.map
            .get(&(coll.to_string(), field.to_string()))
            .map_or(false, |i| i.kind == IndexKind::Eq)
    }

    pub fn has_sorted(&self, coll: &str, field: &str) -> bool {
        self.map
            .get(&(coll.to_string(), field.to_string()))
            .map_or(false, |i| i.kind == IndexKind::Sorted)
    }

    pub fn eq_lookup(&self, coll: &str, field: &str, value: &str) -> Option<HashSet<String>> {
        self.map
            .get(&(coll.to_string(), field.to_string()))
            .and_then(|i| i.eq_lookup(value))
    }

    /// Top-k key list from a sorted index (returns empty Vec if no such index).
    pub fn sorted_top_k(&self, coll: &str, field: &str, desc: bool, limit: usize) -> Vec<String> {
        self.map
            .get(&(coll.to_string(), field.to_string()))
            .map(|i| i.sorted_top_k(desc, limit))
            .unwrap_or_default()
    }

    /// All keys in sorted-index order (returns empty Vec if no such index).
    pub fn sorted_all(&self, coll: &str, field: &str, desc: bool) -> Vec<String> {
        self.map
            .get(&(coll.to_string(), field.to_string()))
            .map(|i| i.sorted_all(desc))
            .unwrap_or_default()
    }

    pub fn search_all(&self, coll: &str, text: &str) -> HashSet<String> {
        let mut result: Option<HashSet<String>> = None;
        for idx in self.map.values() {
            if idx.coll == coll && idx.kind == IndexKind::Search {
                let hits = idx.search_all(text);
                result = Some(match result {
                    None    => hits,
                    Some(r) => r.union(&hits).cloned().collect(),
                });
            }
        }
        result.unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ov(v: Value) -> OrderedValue { OrderedValue(v) }

    #[test]
    fn ordered_value_total_order_across_types() {
        // null < bool < number < string < array < object
        let nulls  = ov(Value::Null);
        let boolf  = ov(json!(false));
        let boolt  = ov(json!(true));
        let num0   = ov(json!(0));
        let num42  = ov(json!(42));
        let numpi  = ov(json!(3.14));
        let stra   = ov(json!("apple"));
        let strz   = ov(json!("zebra"));
        let arr    = ov(json!([1, 2, 3]));
        let obj    = ov(json!({"x": 1}));

        let mut all = vec![
            obj.clone(), strz.clone(), num42.clone(), nulls.clone(),
            boolt.clone(), arr.clone(), stra.clone(), boolf.clone(),
            num0.clone(), numpi.clone(),
        ];
        all.sort();

        // Expected order
        let expected = vec![
            nulls, boolf, boolt, num0, numpi, num42, stra, strz, arr, obj,
        ];
        for (got, exp) in all.iter().zip(expected.iter()) {
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn ordered_value_integer_float_equal() {
        // JSON 5 (PosInt) and 5.0 (Float) must sort as equal numbers.
        let i = ov(json!(5));
        let f = ov(json!(5.0));
        assert_eq!(i, f);
        assert_eq!(i.cmp(&f), Ordering::Equal);
    }

    #[test]
    fn sorted_index_top_k_asc_desc() {
        let mut idx = Index::new("users", "age", IndexKind::Sorted);
        idx.add("u:a", &json!({"age": 31}));
        idx.add("u:b", &json!({"age": 24}));
        idx.add("u:c", &json!({"age": 41}));
        idx.add("u:d", &json!({"age": 27}));

        let asc1 = idx.sorted_top_k(false, 1);
        assert_eq!(asc1, vec!["u:b".to_string()]); // youngest

        let desc1 = idx.sorted_top_k(true, 1);
        assert_eq!(desc1, vec!["u:c".to_string()]); // oldest

        let asc3 = idx.sorted_top_k(false, 3);
        assert_eq!(asc3, vec!["u:b", "u:d", "u:a"]);

        let desc3 = idx.sorted_top_k(true, 3);
        assert_eq!(desc3, vec!["u:c", "u:a", "u:d"]);
    }

    #[test]
    fn sorted_index_update_and_delete() {
        let mut idx = Index::new("users", "age", IndexKind::Sorted);
        idx.add("u:a", &json!({"age": 30}));
        idx.add("u:b", &json!({"age": 40}));

        // Update u:a to be the oldest
        idx.add("u:a", &json!({"age": 50}));
        let top = idx.sorted_top_k(true, 1);
        assert_eq!(top, vec!["u:a".to_string()]);

        // Delete u:a
        idx.remove("u:a", &json!({"age": 50}));
        let top = idx.sorted_top_k(true, 1);
        assert_eq!(top, vec!["u:b".to_string()]);
    }

    #[test]
    fn sorted_index_strings() {
        let mut idx = Index::new("books", "title", IndexKind::Sorted);
        idx.add("k:1", &json!({"title": "Zebra"}));
        idx.add("k:2", &json!({"title": "Apple"}));
        idx.add("k:3", &json!({"title": "Mango"}));
        let asc = idx.sorted_top_k(false, 2);
        assert_eq!(asc, vec!["k:2", "k:3"]);
    }
}
