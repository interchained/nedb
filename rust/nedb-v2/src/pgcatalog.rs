// SPDX-FileCopyrightText: 2026 INTERCHAINED LLC
// SPDX-License-Identifier: BUSL-1.1
// NEDB · © 2026 INTERCHAINED LLC × Eth-Interchained × Vex (Claude Opus 5)

//! `pg_catalog` and `information_schema` as REAL QUERYABLE TABLES.
//!
//! # Why this exists
//!
//! `\dt` in psql and the schema browser in DBeaver came back **empty**. That
//! is an evaluator's first ten minutes, and an empty table list does not read
//! as "unsupported" — it reads as "this database is broken" or "my data is
//! gone".
//!
//! # Why it is built this way
//!
//! The cheap implementation is to recognise psql's exact query text and answer
//! it from a fixed table. Several pgwire-compatible engines do that. It is the
//! wrong choice here for a specific reason: **it breaks silently.** psql
//! changes its catalogue queries between versions, and when the pattern stops
//! matching, the result is an empty table list — indistinguishable from a
//! database that genuinely has no tables. That is the exact class of
//! confidently-wrong answer this engine has spent its life removing.
//!
//! So the catalogue is a set of real tables, synthesised from the live
//! database, and queried through the ordinary predicate path
//! (`nql::query_rows`). `WHERE`, `ORDER BY`, `LIMIT` and the `~`/`!~`
//! operators all work on them because they are the same operators, not a
//! second implementation.
//!
//! # What a schemaless engine can honestly report
//!
//! NEDB has no schema, so the catalogue is *derived*, and that derivation is
//! stated rather than hidden:
//!
//! * a **collection** is a table in `pg_class` / `information_schema.tables`;
//! * a **field observed in a sampled document** is a column in
//!   `pg_attribute` / `information_schema.columns`, typed the way the pgwire
//!   layer types it on the wire;
//! * everything Postgres tracks that NEDB does not have — owners, tablespaces,
//!   ACLs, statistics — reports a fixed, plainly-wrong-if-you-look value
//!   (`10`, `0`, empty) rather than a fabricated plausible one.
//!
//! Column order and the sampled field set come from the same code the wire
//! protocol uses, so a client is never told about a column the data does not
//! produce.

use std::sync::Arc;

use serde_json::{json, Map, Value};

use crate::db::Db;

/// The fixed OID NEDB reports for anything Postgres owns and NEDB does not.
///
/// `10` is Postgres's own `bootstrap superuser` OID. Reporting a real-looking
/// owner is less misleading than reporting `0`, which some clients render as a
/// missing row rather than an unknown one.
const OWNER_OID: i64 = 10;

/// The one schema NEDB presents. A collection has no namespace of its own, so
/// inventing several would be inventing structure.
const PUBLIC_NS_OID: i64 = 2200; // Postgres's own oid for `public`
const CATALOG_NS_OID: i64 = 11;
const INFO_NS_OID: i64 = 13000;

/// How many documents to sample when deriving a table's columns.
///
/// Bounded, because `\d` on a large collection must not turn into a scan. It
/// is a sample, and the module doc says so — a field that appears only outside
/// it is absent from the catalogue, which is the honest failure for a store
/// with no declared schema.
const COLUMN_SAMPLE: usize = 200;

/// Is `table` a catalogue relation this module serves?
///
/// Matched on the BARE name, because the pgwire layer strips the schema
/// qualification before it gets here (`pg_catalog.pg_class` → `pg_class`).
/// An `information_schema.` prefix is kept on those names by the caller, since
/// `tables` and `columns` are words a user could plausibly name a collection.
pub fn is_catalog(table: &str) -> bool {
    matches!(
        table,
        "pg_class" | "pg_namespace" | "pg_attribute" | "pg_type" | "pg_database"
            | "pg_am" | "pg_roles" | "pg_user" | "pg_settings" | "pg_index"
            | "pg_description" | "pg_constraint" | "pg_tablespace"
            | "information_schema.tables"
            | "information_schema.columns"
            | "information_schema.schemata"
            | "information_schema.key_column_usage"
            | "information_schema.table_constraints"
    )
}

/// A stable synthetic OID for a name.
///
/// Postgres clients use an OID to correlate rows between catalogue tables
/// (`pg_attribute.attrelid` → `pg_class.oid`), so it has to be *consistent*
/// within a connection, not globally meaningful. Derived from the name so the
/// same collection gets the same OID on every query without any state to keep.
/// Offset above Postgres's own reserved range so a synthetic OID cannot
/// collide with a real one a client has hard-coded.
fn oid_for(name: &str) -> i64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in name.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    // Keep it comfortably inside i32 (Postgres OIDs are 32-bit unsigned) and
    // above the reserved floor.
    16_384 + (h % 2_000_000_000) as i64
}

/// The collections in the database, sorted so a listing is stable run to run.
fn collections(db: Option<&Arc<Db>>) -> Vec<String> {
    let mut out = match db {
        Some(db) => db.id_index.collections(),
        None => vec![],
    };
    // Internal bookkeeping is not a user table. `__links__` holds relation
    // edges; showing it in `\dt` would invite someone to query or trust it.
    out.retain(|c| !c.starts_with("__") && !c.is_empty());
    out.sort();
    out
}

/// Field name → wire type, derived from a bounded sample of the collection.
///
/// Uses the pgwire layer's own typing so the catalogue cannot disagree with
/// what the wire actually sends: a column reported as `bigint` here is a
/// column the protocol advertises as `int8`.
fn columns_of(db: Option<&Arc<Db>>, coll: &str) -> Vec<(String, i32)> {
    let db = match db {
        Some(db) => db,
        None => return vec![],
    };
    let rows = match crate::nql::query(db, &format!("FROM {} LIMIT {}", coll, COLUMN_SAMPLE)) {
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
    names
        .into_iter()
        .map(|n| {
            let oid = crate::pgwire::oid_for_column(&rows, &n);
            (n, oid)
        })
        .collect()
}

/// The Postgres type name for an OID we hand out — the `data_type` a client
/// reads in `information_schema.columns`.
/// Public alias so `format_type()` in the SQL engine names a type the same
/// way `information_schema.columns` does.
pub fn type_name_pub(oid: i32) -> &'static str {
    type_name(oid)
}

fn type_name(oid: i32) -> &'static str {
    match oid {
        16 => "boolean",
        20 => "bigint",
        21 => "smallint",
        23 => "integer",
        700 => "real",
        701 => "double precision",
        1043 => "character varying",
        _ => "text",
    }
}

fn row(pairs: Vec<(&str, Value)>) -> Value {
    let mut m = Map::new();
    for (k, v) in pairs {
        m.insert(k.to_string(), v);
    }
    Value::Object(m)
}

/// The rows of a catalogue relation, or `None` if it is not one.
///
/// Every column a real Postgres exposes is present where a client might read
/// it, because a missing column is a hard error in the middle of someone
/// else's generated SQL — far worse than a column reporting a fixed value.
pub fn rows(table: &str, db: Option<&Arc<Db>>) -> Option<Vec<Value>> {
    let colls = collections(db);

    Some(match table {
        // ── pg_namespace ────────────────────────────────────────────────────
        // What `\dn` reads. The one table that needs no JOIN, which is why it
        // was the first milestone.
        "pg_namespace" | "information_schema.schemata" => {
            let is_info = table.starts_with("information_schema");
            [("public", PUBLIC_NS_OID), ("pg_catalog", CATALOG_NS_OID),
             ("information_schema", INFO_NS_OID)]
                .iter()
                .map(|(name, oid)| {
                    if is_info {
                        row(vec![
                            ("catalog_name", json!("nedb")),
                            ("schema_name", json!(name)),
                            ("schema_owner", json!("nedb")),
                            ("default_character_set_catalog", Value::Null),
                            ("default_character_set_schema", Value::Null),
                            ("default_character_set_name", Value::Null),
                            ("sql_path", Value::Null),
                        ])
                    } else {
                        row(vec![
                            ("oid", json!(oid)),
                            ("nspname", json!(name)),
                            ("nspowner", json!(OWNER_OID)),
                            ("nspacl", Value::Null),
                        ])
                    }
                })
                .collect()
        }

        // ── pg_class — one row per collection ───────────────────────────────
        "pg_class" => colls
            .iter()
            .map(|c| {
                row(vec![
                    ("oid", json!(oid_for(c))),
                    ("relname", json!(c)),
                    ("relnamespace", json!(PUBLIC_NS_OID)),
                    // 'r' = ordinary table. A collection is writable and has
                    // rows, so any other relkind would be a lie.
                    ("relkind", json!("r")),
                    ("relowner", json!(OWNER_OID)),
                    ("relam", json!(2)),          // heap
                    ("reltuples", json!(-1.0)),   // -1 = never analysed, which is true
                    ("relhasindex", json!(false)),
                    ("relpersistence", json!("p")),
                    ("reltablespace", json!(0)),
                    ("relispartition", json!(false)),
                    ("reltoastrelid", json!(0)),
                    ("relnatts", json!(columns_of(db, c).len() as i64)),
                ])
            })
            .collect(),

        // ── pg_attribute — one row per observed field ───────────────────────
        "pg_attribute" => colls
            .iter()
            .flat_map(|c| {
                let rel = oid_for(c);
                columns_of(db, c)
                    .into_iter()
                    .enumerate()
                    .map(move |(i, (name, oid))| {
                        row(vec![
                            ("attrelid", json!(rel)),
                            ("attname", json!(name)),
                            // 1-based, as Postgres numbers them.
                            ("attnum", json!(i as i64 + 1)),
                            ("atttypid", json!(oid as i64)),
                            ("attlen", json!(-1)),
                            ("atttypmod", json!(-1)),
                            // Nothing is NOT NULL in a schemaless store: any
                            // document may omit any field.
                            ("attnotnull", json!(false)),
                            ("atthasdef", json!(false)),
                            ("attisdropped", json!(false)),
                            ("attidentity", json!("")),
                            ("attgenerated", json!("")),
                        ])
                    })
                    .collect::<Vec<_>>()
            })
            .collect(),

        // ── information_schema.tables / .columns — the standard-SQL view ────
        // What JDBC's DatabaseMetaData and most BI tools read first.
        "information_schema.tables" => colls
            .iter()
            .map(|c| {
                row(vec![
                    ("table_catalog", json!("nedb")),
                    ("table_schema", json!("public")),
                    ("table_name", json!(c)),
                    ("table_type", json!("BASE TABLE")),
                    ("self_referencing_column_name", Value::Null),
                    ("reference_generation", Value::Null),
                    ("user_defined_type_catalog", Value::Null),
                    ("user_defined_type_schema", Value::Null),
                    ("user_defined_type_name", Value::Null),
                    ("is_insertable_into", json!("YES")),
                    ("is_typed", json!("NO")),
                    ("commit_action", Value::Null),
                ])
            })
            .collect(),

        "information_schema.columns" => colls
            .iter()
            .flat_map(|c| {
                columns_of(db, c)
                    .into_iter()
                    .enumerate()
                    .map(move |(i, (name, oid))| {
                        row(vec![
                            ("table_catalog", json!("nedb")),
                            ("table_schema", json!("public")),
                            ("table_name", json!(c)),
                            ("column_name", json!(name)),
                            ("ordinal_position", json!(i as i64 + 1)),
                            ("column_default", Value::Null),
                            // Always YES: any document may omit any field.
                            ("is_nullable", json!("YES")),
                            ("data_type", json!(type_name(oid))),
                            ("character_maximum_length", Value::Null),
                            ("numeric_precision", Value::Null),
                            ("numeric_scale", Value::Null),
                            ("datetime_precision", Value::Null),
                            ("udt_catalog", json!("nedb")),
                            ("udt_schema", json!("pg_catalog")),
                            ("udt_name", json!(type_name(oid))),
                            ("is_updatable", json!("YES")),
                        ])
                    })
                    .collect::<Vec<_>>()
            })
            .collect(),

        // ── pg_type — only the types this endpoint actually hands out ───────
        // Listing Postgres's full type table would be inventing support for
        // types the wire layer cannot encode.
        "pg_type" => [
            (16, "bool"), (20, "int8"), (21, "int2"), (23, "int4"),
            (25, "text"), (700, "float4"), (701, "float8"), (1043, "varchar"),
        ]
        .iter()
        .map(|(oid, name)| {
            row(vec![
                ("oid", json!(*oid as i64)),
                ("typname", json!(name)),
                ("typnamespace", json!(CATALOG_NS_OID)),
                ("typowner", json!(OWNER_OID)),
                ("typlen", json!(-1)),
                ("typtype", json!("b")),
                ("typcategory", json!("S")),
                ("typelem", json!(0)),
                ("typrelid", json!(0)),
            ])
        })
        .collect(),

        // ── pg_database — the databases the server has open ─────────────────
        "pg_database" => vec![row(vec![
            ("oid", json!(oid_for("nedb"))),
            ("datname", json!("nedb")),
            ("datdba", json!(OWNER_OID)),
            ("encoding", json!(6)), // 6 = UTF8 in Postgres's encoding table
            ("datcollate", json!("C")),
            ("datctype", json!("C")),
            ("datlocprovider", json!("c")),
            ("daticulocale", Value::Null),
            ("daticurules", Value::Null),
            ("datistemplate", json!(false)),
            ("datallowconn", json!(true)),
            ("datconnlimit", json!(-1)),
            ("datacl", Value::Null),
        ])],

        "pg_am" => vec![row(vec![
            ("oid", json!(2)),
            ("amname", json!("heap")),
            ("amhandler", json!(0)),
            ("amtype", json!("t")),
        ])],

        "pg_roles" | "pg_user" => vec![row(vec![
            ("oid", json!(OWNER_OID)),
            ("rolname", json!("nedb")),
            ("usename", json!("nedb")),
            ("rolsuper", json!(true)),
            ("usesuper", json!(true)),
            ("rolcanlogin", json!(true)),
            ("rolcreatedb", json!(true)),
            ("rolvaliduntil", Value::Null),
        ])],

        // ── the ones that are genuinely EMPTY, and should say so ────────────
        // An empty catalogue table is the truthful answer here: NEDB has no
        // secondary indexes visible to SQL, no constraints, no comments and
        // no tablespaces. Returning rows would fabricate structure; refusing
        // the query would break generated SQL that only wants to find none.
        "pg_index" | "pg_description" | "pg_constraint" | "pg_tablespace"
        | "pg_settings" | "information_schema.key_column_usage"
        | "information_schema.table_constraints" => vec![],

        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn db_with(colls: &[(&str, &str, Value)]) -> (tempfile::TempDir, Arc<Db>) {
        let dir = tempdir().unwrap();
        let db = Arc::new(Db::open(dir.path(), None).unwrap());
        for (coll, id, doc) in colls {
            db.put(coll, id, doc.clone(), vec![], None, None).unwrap();
        }
        (dir, db)
    }

    fn names(rows: &[Value], field: &str) -> Vec<String> {
        let mut v: Vec<String> = rows
            .iter()
            .filter_map(|r| r.get(field)?.as_str().map(str::to_string))
            .collect();
        v.sort();
        v
    }

    #[test]
    fn a_collection_appears_as_a_table_in_every_place_a_client_looks() {
        // JDBC reads information_schema; psql reads pg_class. A collection
        // visible in one and not the other is a database that looks half
        // empty depending on the tool.
        let (_t, db) = db_with(&[
            ("orders", "1", json!({"total": 1})),
            ("drivers", "d1", json!({"name": "Bob"})),
        ]);
        let d = Some(&db);
        assert_eq!(names(&rows("pg_class", d).unwrap(), "relname"),
                   vec!["drivers", "orders"]);
        assert_eq!(names(&rows("information_schema.tables", d).unwrap(), "table_name"),
                   vec!["drivers", "orders"]);
    }

    #[test]
    fn internal_bookkeeping_is_not_presented_as_a_user_table() {
        // `__links__` holds relation edges. Listing it in `\dt` would invite
        // someone to query or trust it as their own data.
        let (_t, db) = db_with(&[("orders", "1", json!({"a": 1}))]);
        let db2 = Arc::clone(&db);
        db2.link("orders:1", "rel", "orders:1").ok();
        let got = names(&rows("pg_class", Some(&db)).unwrap(), "relname");
        assert!(!got.iter().any(|n| n.starts_with("__")), "{:?}", got);
    }

    #[test]
    fn a_columns_reported_type_is_the_type_the_WIRE_sends() {
        // The catalogue and the protocol go through one typing function, so
        // they cannot disagree. A column reported `bigint` here that arrived
        // as text on the wire would be a self-contradiction a client is
        // entitled to trust.
        let (_t, db) = db_with(&[
            ("t", "1", json!({"n": 7, "s": "x", "b": true, "f": 1.5})),
        ]);
        let cols = rows("information_schema.columns", Some(&db)).unwrap();
        let by = |name: &str| -> String {
            cols.iter()
                .find(|r| r["column_name"] == json!(name))
                .and_then(|r| r["data_type"].as_str())
                .unwrap_or("<missing>").to_string()
        };
        assert_eq!(by("n"), "bigint");
        assert_eq!(by("s"), "text");
        assert_eq!(by("b"), "boolean");
        assert_eq!(by("f"), "double precision");
    }

    #[test]
    fn everything_is_nullable_because_a_schemaless_document_may_omit_anything() {
        let (_t, db) = db_with(&[("t", "1", json!({"a": 1}))]);
        let cols = rows("information_schema.columns", Some(&db)).unwrap();
        assert!(cols.iter().all(|r| r["is_nullable"] == json!("YES")), "{:?}", cols);
        let attrs = rows("pg_attribute", Some(&db)).unwrap();
        assert!(attrs.iter().all(|r| r["attnotnull"] == json!(false)));
    }

    #[test]
    fn pg_attribute_correlates_with_pg_class_by_oid() {
        // A client JOINs these two on oid. If the synthetic oids did not
        // match, every `\d`-style query would silently return no columns.
        let (_t, db) = db_with(&[("orders", "1", json!({"total": 1}))]);
        let d = Some(&db);
        let rel = rows("pg_class", d).unwrap();
        let oid = rel.iter().find(|r| r["relname"] == json!("orders")).unwrap()["oid"].clone();
        let attrs = rows("pg_attribute", d).unwrap();
        assert!(attrs.iter().any(|r| r["attrelid"] == oid),
                "no pg_attribute row points at pg_class.oid {:?}", oid);
        // …and attnum is 1-based, as Postgres numbers columns.
        assert!(attrs.iter().all(|r| r["attnum"].as_i64().unwrap_or(0) >= 1));
    }

    #[test]
    fn an_oid_is_stable_across_calls_so_a_join_holds() {
        assert_eq!(oid_for("orders"), oid_for("orders"));
        assert_ne!(oid_for("orders"), oid_for("drivers"));
        // Above Postgres's reserved floor, so a synthetic oid cannot collide
        // with a real one a client has hard-coded.
        assert!(oid_for("orders") >= 16_384);
        // And inside i32, because a Postgres OID is 32-bit.
        assert!(oid_for("orders") < i32::MAX as i64);
    }

    #[test]
    fn pg_namespace_answers_without_any_database_open() {
        // psql sends catalogue queries on startup, sometimes before a database
        // is selected. Refusing there is how "psql cannot connect" begins.
        let ns = rows("pg_namespace", None).unwrap();
        assert_eq!(names(&ns, "nspname"),
                   vec!["information_schema", "pg_catalog", "public"]);
    }

    #[test]
    fn the_tables_nedb_genuinely_has_nothing_for_are_EMPTY_not_absent() {
        // Empty is the truthful answer: no SQL-visible indexes, no
        // constraints, no comments, no tablespaces. Returning rows would
        // fabricate structure; returning None would break generated SQL that
        // only wants to find none.
        for t in ["pg_index", "pg_constraint", "pg_description", "pg_tablespace",
                  "information_schema.key_column_usage",
                  "information_schema.table_constraints"] {
            let r = rows(t, None).unwrap_or_else(|| panic!("{} must be served", t));
            assert!(r.is_empty(), "{} should be empty, got {:?}", t, r);
        }
    }

    #[test]
    fn a_name_that_is_not_a_catalogue_relation_is_not_claimed() {
        assert!(!is_catalog("orders"));
        assert!(!is_catalog("tables"), "a bare `tables` is a user collection");
        assert!(rows("orders", None).is_none());
        // information_schema keeps its qualifier precisely so a user
        // collection called `tables` cannot be shadowed by the catalogue.
        assert!(is_catalog("information_schema.tables"));
    }

    #[test]
    fn pg_type_lists_only_types_this_endpoint_can_actually_send() {
        let t = names(&rows("pg_type", None).unwrap(), "typname");
        assert!(t.contains(&"int8".to_string()));
        assert!(t.contains(&"text".to_string()));
        // Listing Postgres's whole type table would advertise encoders the
        // wire layer does not have.
        assert!(!t.contains(&"tsvector".to_string()));
        assert!(!t.contains(&"jsonb".to_string()));
    }
}
