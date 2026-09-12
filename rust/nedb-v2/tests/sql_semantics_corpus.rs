// SPDX-License-Identifier: BUSL-1.1
// SPDX-FileCopyrightText: © 2026 INTERCHAINED LLC × Claude Sonnet 4.6

//! The frozen semantic corpus for the SQL evaluator.
//!
//! # This file is a contract, not a test of convenience
//!
//! Every answer below is what NEDB's SQL surface MEANS. They were established
//! by building the evaluator against real PostgreSQL behaviour and a real
//! `psql` binary, and they are now fixed.
//!
//! **An optimisation may not change an answer in this file.** If a change to
//! execution makes a case here fail, the change is wrong — the expectation is
//! not to be "updated" to match new behaviour. That rule is the entire point
//! of writing them down: the failure mode this codebase has actually suffered
//! is not a crash, it is a confident wrong answer that passed its own audit.
//!
//! Should a case here ever be found to disagree with PostgreSQL, the fix is a
//! separate, deliberate, documented change — never a quiet edit folded into a
//! performance commit.
//!
//! # Every case runs on every execution strategy
//!
//! Each query is executed three times: with the planner choosing freely, with
//! the nested loop forced, and with the hash join forced. All three must give
//! byte-identical columns AND rows, in order. That is what stops a fast path
//! from being fast and wrong.

use nedb_engine::sqljoin::JoinExec;
use nedb_engine::sqlselect::{execute_explain, parse};
use serde_json::{json, Value};

/// The fixture. Deliberately hostile:
///
/// - `emp.dept_id` contains NULLs, so outer joins and `UNKNOWN` filtering are
///   exercised rather than assumed.
/// - `dept` contains a row nothing points at, so `RIGHT`/`FULL` have work.
/// - `emp` has two employees in one department, so duplicate matches are real.
/// - one `dept.id` is the STRING `"4"` while `emp.dept_id` holds the NUMBER 4,
///   because this engine compares those numerically and a hash join must not
///   lose the match.
fn relation(name: &str) -> Option<Vec<Value>> {
    Some(match name {
        "emp" => vec![
            json!({"id": 1, "name": "ada",   "dept_id": 10, "salary": 100}),
            json!({"id": 2, "name": "grace", "dept_id": 10, "salary": 200}),
            json!({"id": 3, "name": "linus", "dept_id": 20, "salary": 150}),
            json!({"id": 4, "name": "ken",   "dept_id": null, "salary": 175}),
            json!({"id": 5, "name": "rob",   "dept_id": 4,  "salary": 125}),
        ],
        "dept" => vec![
            json!({"id": 10,  "dname": "eng"}),
            json!({"id": 20,  "dname": "ops"}),
            json!({"id": 30,  "dname": "ghost"}),
            json!({"id": "4", "dname": "legal"}),
            json!({"id": null, "dname": "limbo"}),
        ],
        "proj" => vec![
            json!({"pid": 1, "dept_id": 10, "pname": "apollo"}),
            json!({"pid": 2, "dept_id": 20, "pname": "gemini"}),
        ],
        // Matches ONLY `legal`, which is the LAST row the emp-dept join emits.
        // That is what makes it able to detect an intermediate join being
        // capped: cap the first join at all and this relation joins nothing.
        "proj_late" => vec![json!({"pid": 9, "dept_id": 4, "pname": "zeta"})],
        _ => return None,
    })
}

fn resolve(name: &str) -> anyhow::Result<Option<Box<dyn nedb_engine::sqlselect::Relation>>> {
    Ok(relation(name).map(nedb_engine::sqlselect::from_vec))
}

/// Run one query under every strategy and assert they agree, returning the
/// single answer they agreed on.
fn run_all_strategies(sql: &str) -> (Vec<String>, Vec<Value>) {
    let sel = parse(sql).unwrap_or_else(|e| panic!("{sql}\n  failed to parse: {e:#}"));

    let mut answers = vec![];
    for exec in [JoinExec::Auto, JoinExec::NestedLoop, JoinExec::Hash] {
        let (names, rows, choices) = execute_explain(&sel, &resolve, exec)
            .unwrap_or_else(|e| panic!("{sql}\n  failed under {exec:?}: {e:#}"));
        answers.push((exec, names, rows, choices));
    }

    let (_, base_names, base_rows, _) = &answers[0];
    for (exec, names, rows, _) in &answers[1..] {
        assert_eq!(
            base_names, names,
            "{sql}\n  column names differ between Auto and {exec:?}"
        );
        assert_eq!(
            base_rows, rows,
            "{sql}\n  ROWS DIFFER between Auto and {exec:?} — an execution \
             strategy changed the answer"
        );
    }
    (
        base_names.iter().map(|c| c.name.clone()).collect(),
        base_rows.clone(),
    )
}

/// Assert the exact, ordered answer.
#[track_caller]
fn expect(sql: &str, want: Value) {
    let (_, rows) = run_all_strategies(sql);
    assert_eq!(Value::Array(rows), want, "{sql}");
}

/// Assert the exact output column names.
#[track_caller]
fn expect_cols(sql: &str, want: &[&str]) {
    let (names, _) = run_all_strategies(sql);
    assert_eq!(names, want, "{sql}");
}

/// Assert the query is refused, and that the message names the real reason.
#[track_caller]
fn expect_refused(sql: &str, needle: &str) {
    let err = match parse(sql) {
        Err(e) => format!("{e:#}"),
        Ok(sel) => match execute_explain(&sel, &resolve, JoinExec::Auto) {
            Err(e) => format!("{e:#}"),
            Ok((_, rows, _)) => panic!("{sql}\n  was ACCEPTED, returning {rows:?}"),
        },
    };
    assert!(
        err.to_lowercase().contains(&needle.to_lowercase()),
        "{sql}\n  refused, but the reason did not mention {needle:?}: {err}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Three-valued logic
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn null_equals_null_is_unknown_not_true() {
    // If this were TRUE, `ken` (dept_id NULL) would join `limbo` (id NULL).
    expect(
        "SELECT e.name FROM emp e JOIN dept d ON e.dept_id = d.id \
         WHERE e.name = 'ken'",
        json!([]),
    );
}

#[test]
fn not_unknown_is_unknown_so_the_row_is_still_dropped() {
    // The trap: collapsing UNKNOWN to false would make NOT(...) true here and
    // admit `ken`.
    expect(
        "SELECT e.name FROM emp e WHERE NOT (e.dept_id = 10) ORDER BY 1",
        json!([{"name": "linus"}, {"name": "rob"}]),
    );
}

#[test]
fn only_true_keeps_a_row() {
    expect("SELECT e.name FROM emp e WHERE e.dept_id = 10 ORDER BY 1",
        json!([{"name": "ada"}, {"name": "grace"}]));
}

#[test]
fn and_or_truth_tables() {
    // false AND unknown = false; true AND unknown = unknown;
    // true OR unknown = true; false OR unknown = unknown.
    expect(
        "SELECT e.name FROM emp e WHERE e.id = 99 AND e.dept_id = 10 ORDER BY 1",
        json!([]),
    );
    expect(
        "SELECT e.name FROM emp e WHERE e.id = 4 AND e.dept_id = 10 ORDER BY 1",
        json!([]),
    );
    expect(
        "SELECT e.name FROM emp e WHERE e.id = 4 OR e.dept_id = 10 ORDER BY 1",
        json!([{"name": "ada"}, {"name": "grace"}, {"name": "ken"}]),
    );
    expect(
        "SELECT e.name FROM emp e WHERE e.id = 99 OR e.dept_id = 10 ORDER BY 1",
        json!([{"name": "ada"}, {"name": "grace"}]),
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Joins
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn inner_join_drops_null_keys_and_keeps_duplicates() {
    expect(
        "SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept_id = d.id \
         ORDER BY 1",
        json!([
            {"name": "ada",   "dname": "eng"},
            {"name": "grace", "dname": "eng"},
            {"name": "linus", "dname": "ops"},
            // 4 = '4': the engine compares a number and a numeric string
            // numerically, so this match is real and must survive hashing.
            {"name": "rob",   "dname": "legal"},
        ]),
    );
}

#[test]
fn left_join_keeps_the_unmatched_left_row_with_nulls() {
    expect(
        "SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept_id = d.id \
         ORDER BY 1",
        json!([
            {"name": "ada",   "dname": "eng"},
            {"name": "grace", "dname": "eng"},
            {"name": "ken",   "dname": null},
            {"name": "linus", "dname": "ops"},
            {"name": "rob",   "dname": "legal"},
        ]),
    );
}

#[test]
fn right_join_keeps_the_unmatched_right_rows() {
    expect(
        "SELECT e.name, d.dname FROM emp e RIGHT JOIN dept d ON e.dept_id = d.id \
         ORDER BY 2",
        json!([
            {"name": "ada",   "dname": "eng"},
            {"name": "grace", "dname": "eng"},
            {"name": null,    "dname": "ghost"},
            // 4 = '4' is TRUE here, so `legal` is matched, not outer.
            {"name": "rob",   "dname": "legal"},
            // `limbo` has a NULL id, so it matches nothing and survives as a
            // right-outer row rather than joining `ken`.
            {"name": null,    "dname": "limbo"},
            {"name": "linus", "dname": "ops"},
        ]),
    );
}

#[test]
fn full_join_keeps_both_sides_unmatched() {
    let (_, rows) = run_all_strategies(
        "SELECT e.name, d.dname FROM emp e FULL JOIN dept d ON e.dept_id = d.id",
    );
    assert_eq!(rows.len(), 7, "4 matched + ken + ghost + limbo");
    assert!(rows.contains(&json!({"name": "ken", "dname": null})));
    assert!(rows.contains(&json!({"name": null, "dname": "ghost"})));
    assert!(rows.contains(&json!({"name": null, "dname": "limbo"})));
}

#[test]
fn right_join_onto_an_empty_left_relation_still_carries_left_bindings() {
    // The shape of the result cannot be read off a row when there are no rows.
    // This returned rows MISSING the left binding before it was tracked
    // explicitly — so `e.name` had nothing to resolve against.
    expect(
        "SELECT e.name, d.dname FROM emp e RIGHT JOIN dept d ON e.dept_id = d.id \
         WHERE d.dname = 'ghost'",
        json!([{"name": null, "dname": "ghost"}]),
    );
}

#[test]
fn cross_join_is_the_full_product() {
    let (_, rows) = run_all_strategies("SELECT e.id, d.id AS did FROM emp e CROSS JOIN dept d");
    assert_eq!(rows.len(), 25);
}

#[test]
fn a_predicate_on_the_nullable_side_in_where_filters_after_the_join() {
    // This is precisely the transformation a careless optimiser would push
    // below the join. In WHERE it discards the outer rows; in ON it keeps
    // them. The two are NOT interchangeable.
    expect(
        "SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept_id = d.id \
         WHERE d.dname = 'eng' ORDER BY 1",
        json!([
            {"name": "ada",   "dname": "eng"},
            {"name": "grace", "dname": "eng"},
        ]),
    );
}

#[test]
fn the_same_predicate_in_on_keeps_every_left_row() {
    expect(
        "SELECT e.name, d.dname FROM emp e LEFT JOIN dept d \
         ON e.dept_id = d.id AND d.dname = 'eng' ORDER BY 1",
        json!([
            {"name": "ada",   "dname": "eng"},
            {"name": "grace", "dname": "eng"},
            {"name": "ken",   "dname": null},
            {"name": "linus", "dname": null},
            {"name": "rob",   "dname": null},
        ]),
    );
}

#[test]
fn a_non_equality_conjunct_is_applied_not_merely_keyed_on() {
    // `e.salary > 120` rides along in the ON clause. A hash join keys on the
    // equality and must still apply the rest.
    expect(
        "SELECT e.name FROM emp e JOIN dept d \
         ON e.dept_id = d.id AND e.salary > 120 ORDER BY 1",
        json!([{"name": "grace"}, {"name": "linus"}, {"name": "rob"}]),
    );
}

#[test]
fn a_pure_non_equality_join_still_works() {
    // No equality anywhere: there is nothing to hash on, and the answer must
    // be the same as it always was.
    let (_, rows) = run_all_strategies(
        "SELECT e.name, d.dname FROM emp e JOIN dept d ON e.salary > 160",
    );
    // ken (175) and grace (200) against the 5 dept rows.
    assert_eq!(rows.len(), 10);
}

#[test]
fn an_or_join_predicate_is_never_split_into_keys() {
    let (_, rows) = run_all_strategies(
        "SELECT e.id, d.dname FROM emp e JOIN dept d \
         ON e.dept_id = d.id OR d.dname = 'ghost'",
    );
    // 4 real matches + every employee against `ghost`.
    assert_eq!(rows.len(), 9);
}

#[test]
fn two_joins_chain_and_the_second_keys_off_the_first() {
    expect(
        "SELECT e.name, p.pname FROM emp e \
         JOIN dept d ON e.dept_id = d.id \
         JOIN proj p ON d.id = p.dept_id ORDER BY 1",
        json!([
            {"name": "ada",   "pname": "apollo"},
            {"name": "grace", "pname": "apollo"},
            {"name": "linus", "pname": "gemini"},
        ]),
    );
}

#[test]
fn an_expression_join_key_works_on_both_paths() {
    expect(
        "SELECT e.name FROM emp e JOIN dept d ON lower(d.dname) = lower('ENG') \
         AND e.dept_id = d.id ORDER BY 1",
        json!([{"name": "ada"}, {"name": "grace"}]),
    );
}

#[test]
fn a_compound_key_requires_every_column() {
    // ada is (dept_id 10, id 1) and apollo is (dept_id 10, pid 1), so both
    // key columns agree and the pair joins.
    expect(
        "SELECT e.name, p.pname FROM emp e JOIN proj p \
         ON e.dept_id = p.dept_id AND e.id = p.pid ORDER BY 1",
        json!([{"name": "ada", "pname": "apollo"}]),
    );
    // The same first key column, a second that agrees for nobody: a compound
    // key that matched on a subset would wrongly return ada here.
    expect(
        "SELECT e.name, p.pname FROM emp e JOIN proj p \
         ON e.dept_id = p.dept_id AND e.id = p.dept_id ORDER BY 1",
        json!([]),
    );
}

#[test]
fn a_bare_column_join_key_resolves_at_evaluation_time() {
    // `dept_id` is unqualified and exists in BOTH relations, so it resolves to
    // the first binding that has it. Refusing to hash on it is what keeps this
    // answer stable.
    let (_, rows) = run_all_strategies("SELECT e.id FROM emp e JOIN proj p ON dept_id = 10");
    assert_eq!(rows.len(), 4, "ada and grace, against both proj rows");
}

// ─────────────────────────────────────────────────────────────────────────────
// Postfix operators, and their composition with later predicates
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn in_list_composes_with_a_following_conjunct() {
    // The bug this pins: postfix operators were applied AFTER the binary
    // loop, so everything following an `IN (...)` was silently dropped — and a
    // WHERE clause that loses its later conjuncts returns TOO MANY rows.
    expect(
        "SELECT e.name FROM emp e WHERE e.dept_id IN (10, 20) AND e.salary > 120 \
         ORDER BY 1",
        json!([{"name": "grace"}, {"name": "linus"}]),
    );
}

#[test]
fn not_in_with_a_null_in_the_list_is_unknown_for_every_row() {
    // `x NOT IN (10, NULL)` can never be TRUE. Postgres returns nothing here,
    // and so must this.
    expect(
        "SELECT e.name FROM emp e WHERE e.salary NOT IN (100, NULL) ORDER BY 1",
        json!([]),
    );
}

#[test]
fn in_over_a_null_column_is_unknown() {
    expect(
        "SELECT e.name FROM emp e WHERE e.dept_id IN (10, 20) ORDER BY 1",
        json!([{"name": "ada"}, {"name": "grace"}, {"name": "linus"}]),
    );
}

#[test]
fn is_null_and_is_not_null_compose() {
    expect(
        "SELECT e.name FROM emp e WHERE e.dept_id IS NULL AND e.salary > 100",
        json!([{"name": "ken"}]),
    );
    expect(
        "SELECT e.name FROM emp e WHERE e.dept_id IS NOT NULL AND e.salary < 130 \
         ORDER BY 1",
        json!([{"name": "ada"}, {"name": "rob"}]),
    );
}

#[test]
fn between_is_inclusive_on_both_bounds_and_composes() {
    expect(
        "SELECT e.name FROM emp e WHERE e.salary BETWEEN 100 AND 150 \
         AND e.dept_id IS NOT NULL ORDER BY 1",
        json!([{"name": "ada"}, {"name": "linus"}, {"name": "rob"}]),
    );
    expect(
        "SELECT e.name FROM emp e WHERE e.salary NOT BETWEEN 100 AND 150 ORDER BY 1",
        json!([{"name": "grace"}, {"name": "ken"}]),
    );
}

#[test]
fn is_null_after_a_left_join_sees_the_synthesised_null() {
    expect(
        "SELECT e.name FROM emp e LEFT JOIN dept d ON e.dept_id = d.id \
         WHERE d.dname IS NULL ORDER BY 1",
        json!([{"name": "ken"}]),
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// CASE, aliases, scalar functions, ordinals
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn searched_case_returns_the_first_true_branch() {
    expect(
        "SELECT e.name, CASE WHEN e.salary >= 200 THEN 'high' \
         WHEN e.salary >= 150 THEN 'mid' ELSE 'low' END AS band \
         FROM emp e ORDER BY 1",
        json!([
            {"name": "ada",   "band": "low"},
            {"name": "grace", "band": "high"},
            {"name": "ken",   "band": "mid"},
            {"name": "linus", "band": "mid"},
            {"name": "rob",   "band": "low"},
        ]),
    );
}

#[test]
fn simple_case_compares_against_the_operand() {
    expect(
        "SELECT CASE e.dept_id WHEN 10 THEN 'eng' WHEN 20 THEN 'ops' \
         ELSE 'other' END AS d FROM emp e WHERE e.id IN (1, 3, 4) ORDER BY 1",
        json!([{"d": "eng"}, {"d": "ops"}, {"d": "other"}]),
    );
}

#[test]
fn case_with_no_else_and_no_match_is_null() {
    expect(
        "SELECT CASE WHEN e.salary > 1000 THEN 'rich' END AS r FROM emp e \
         WHERE e.id = 1",
        json!([{"r": null}]),
    );
}

#[test]
fn a_case_operand_that_is_null_matches_no_branch() {
    // `CASE NULL WHEN 10 ...` compares NULL = 10, which is UNKNOWN.
    expect(
        "SELECT CASE e.dept_id WHEN 10 THEN 'eng' ELSE 'other' END AS d \
         FROM emp e WHERE e.id = 4",
        json!([{"d": "other"}]),
    );
}

#[test]
fn output_names_follow_postgres_derivation() {
    expect_cols("SELECT e.name FROM emp e", &["name"]);
    expect_cols("SELECT e.name AS who FROM emp e", &["who"]);
    expect_cols("SELECT lower(e.name) FROM emp e", &["lower"]);
    expect_cols("SELECT e.salary + 1 FROM emp e", &["?column?"]);
    expect_cols("SELECT CASE WHEN 1 = 1 THEN 'y' END FROM emp e", &["case"]);
    expect_cols("SELECT e.salary::text FROM emp e", &["salary"]);
}

#[test]
fn duplicate_output_names_are_not_silently_renamed() {
    // Postgres permits them and clients index positionally too; renaming would
    // break generated SQL that asks for the name it wrote.
    expect_cols("SELECT e.name, d.dname AS name FROM emp e JOIN dept d ON e.dept_id = d.id",
        &["name", "name"]);
}

#[test]
fn duplicate_output_names_still_carry_DIFFERENT_values() {
    // The version of the test above checked only the NAMES, and passed for
    // months while the VALUES were broken: rows are JSON objects, so two
    // columns sharing a name shared a key, and the second write silently
    // overwrote the first. `SELECT e.name, e2.name` reported two columns and
    // returned one value twice.
    //
    // Checking names without checking values is exactly the decorative-test
    // failure mode. Asserting both is what makes this evidence.
    let (names, rows) = run_all_strategies(
        "SELECT e.name, d.dname AS name FROM emp e JOIN dept d ON e.dept_id = d.id \
         WHERE e.name = 'ada'",
    );
    assert_eq!(names, ["name", "name"]);
    assert_eq!(rows.len(), 1);
    let vals: Vec<&Value> = rows[0].as_object().expect("an object").values().collect();
    assert_eq!(vals.len(), 2, "two columns must occupy two slots, not one");
    assert_eq!(vals[0], &json!("ada"));
    assert_eq!(vals[1], &json!("eng"));
}

// ─────────────────────────────────────────────────────────────────────────────
// Ambiguous relation bindings are REFUSED, not answered
//
// Closing a wrong-answer surface takes priority over any optimisation: a
// silently empty result is indistinguishable from "there is no such data".
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn a_relation_used_twice_without_aliases_is_refused() {
    // This used to return NOTHING. A qualified reference scans the bindings in
    // order and takes the first match, so `emp.dept_id = emp.id` compared
    // every row to ITSELF. PostgreSQL says "table name emp specified more than
    // once"; answering it at all was the bug.
    expect_refused(
        "SELECT emp.name FROM emp JOIN emp ON emp.dept_id = emp.id",
        "ambiguous relation binding",
    );
    // The message has to name the relation and point at the fix.
    expect_refused(
        "SELECT emp.name FROM emp JOIN emp ON emp.dept_id = emp.id",
        "use aliases",
    );
}

#[test]
fn two_aliases_that_collide_are_refused_too() {
    // Different tables, same binding: equally ambiguous.
    expect_refused(
        "SELECT x.name FROM emp x JOIN dept x ON x.dept_id = x.id",
        "ambiguous relation binding",
    );
    // And a bare name colliding with an alias.
    expect_refused(
        "SELECT dept.id FROM emp dept JOIN dept ON dept.id = dept.id",
        "ambiguous relation binding",
    );
}

#[test]
fn aliasing_the_second_use_is_the_supported_spelling() {
    // `emp.dept_id` points at `dept.id` for ada/grace/linus/rob. Self-joining
    // emp to emp on id needs the alias, and with it the query means what it
    // says.
    expect(
        "SELECT e.name, e2.name AS other FROM emp e JOIN emp e2 ON e.dept_id = e2.dept_id \
         WHERE e.id = 1 ORDER BY 2",
        json!([
            {"name": "ada", "other": "ada"},
            {"name": "ada", "other": "grace"},
        ]),
    );
}

#[test]
fn a_relation_used_once_is_never_affected() {
    // The guard must not fire on ordinary queries — including three distinct
    // relations and a case-different alias.
    expect(
        "SELECT e.name, p.pname FROM emp e \
         JOIN dept d ON e.dept_id = d.id \
         JOIN proj p ON d.id = p.dept_id ORDER BY 1",
        json!([
            {"name": "ada",   "pname": "apollo"},
            {"name": "grace", "pname": "apollo"},
            {"name": "linus", "pname": "gemini"},
        ]),
    );
}

#[test]
fn binding_collision_is_case_insensitive_because_resolution_is() {
    // `Bound::column` matches bindings with `eq_ignore_ascii_case`, so `E` and
    // `e` are the SAME binding. The guard has to agree with the resolver, or
    // it would let through exactly the ambiguity it exists to stop.
    expect_refused(
        "SELECT e.name FROM emp e JOIN emp E ON e.dept_id = E.id",
        "ambiguous relation binding",
    );
}

#[test]
fn scalar_functions_evaluate() {
    expect(
        "SELECT upper(e.name) AS u, length(e.name) AS n FROM emp e WHERE e.id = 1",
        json!([{"u": "ADA", "n": 3}]),
    );
    expect(
        "SELECT coalesce(e.dept_id, -1) AS d FROM emp e WHERE e.id = 4",
        json!([{"d": -1}]),
    );
    expect(
        "SELECT coalesce(e.dept_id, -1) AS d FROM emp e WHERE e.id = 3",
        json!([{"d": 20}]),
    );
    expect(
        "SELECT nullif(e.salary, 100) AS s FROM emp e WHERE e.id = 1",
        json!([{"s": null}]),
    );
}

#[test]
fn order_by_ordinal_sorts_by_select_list_position() {
    // `ORDER BY 1` is an ordinal, not the constant 1. psql's `\dt` ends with
    // `ORDER BY 1,2`, so reading it as a constant returns an UNORDERED
    // listing that looks plausible.
    expect(
        "SELECT e.name, e.salary FROM emp e ORDER BY 2 DESC",
        json!([
            {"name": "grace", "salary": 200},
            {"name": "ken",   "salary": 175},
            {"name": "linus", "salary": 150},
            {"name": "rob",   "salary": 125},
            {"name": "ada",   "salary": 100},
        ]),
    );
}

#[test]
fn order_by_puts_nulls_last_ascending_and_first_descending() {
    expect(
        "SELECT e.id FROM emp e ORDER BY e.dept_id ASC, e.id ASC",
        json!([{"id": 5}, {"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]),
    );
    expect(
        "SELECT e.id FROM emp e ORDER BY e.dept_id DESC, e.id ASC",
        json!([{"id": 4}, {"id": 3}, {"id": 1}, {"id": 2}, {"id": 5}]),
    );
}

#[test]
fn order_by_an_expression_not_in_the_select_list() {
    expect(
        "SELECT e.name FROM emp e ORDER BY e.salary ASC LIMIT 2",
        json!([{"name": "ada"}, {"name": "rob"}]),
    );
}

#[test]
fn limit_and_offset_apply_after_ordering() {
    expect(
        "SELECT e.name FROM emp e ORDER BY 1 LIMIT 2 OFFSET 1",
        json!([{"name": "grace"}, {"name": "ken"}]),
    );
    expect("SELECT e.name FROM emp e ORDER BY 1 OFFSET 99", json!([]));
}

#[test]
fn distinct_removes_duplicate_projected_rows() {
    expect(
        "SELECT DISTINCT e.dept_id FROM emp e ORDER BY 1",
        json!([{"dept_id": 4}, {"dept_id": 10}, {"dept_id": 20}, {"dept_id": null}]),
    );
}

#[test]
fn select_with_no_from_is_one_row() {
    expect("SELECT 1 AS one", json!([{"one": 1}]));
}

#[test]
fn star_expands_from_the_rows_themselves() {
    expect_cols("SELECT * FROM proj", &["pid", "dept_id", "pname"]);
    expect_cols("SELECT p.* FROM proj p", &["pid", "dept_id", "pname"]);
}

#[test]
fn a_missing_field_reads_as_null_because_documents_are_schemaless() {
    // Absent and NULL are one state in a schemaless store. This is a design
    // decision, not an omission.
    expect("SELECT p.nope AS n FROM proj p WHERE p.pid = 1", json!([{"n": null}]));
}

// ─────────────────────────────────────────────────────────────────────────────
// The row budget — early termination must not change any answer
//
// When the final answer is a PREFIX of a join's output, the join is allowed to
// stop as soon as it has produced enough rows. The invariant that makes this
// safe is simple and is asserted directly below:
//
//     `q LIMIT n`  ==  the first n rows of `q`
//
// Every join kind is checked, because outer joins append their unmatched rows
// AFTER the main pass — so an early stop has to be proven not to drop rows
// that belong inside the prefix.
// ─────────────────────────────────────────────────────────────────────────────

/// `q LIMIT n` must equal the first n rows of `q`, for every join kind and
/// every n from 0 past the end of the result.
#[test]
fn a_limit_is_always_a_prefix_of_the_unlimited_answer() {
    for kind in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN"] {
        let on = if kind == "CROSS JOIN" { "" } else { " ON e.dept_id = d.id" };
        let base = format!("SELECT e.name, d.dname FROM emp e {kind} dept d{on}");
        let (_, all) = run_all_strategies(&base);

        for n in 0..=all.len() + 2 {
            let (_, got) = run_all_strategies(&format!("{base} LIMIT {n}"));
            let want: Vec<Value> = all.iter().take(n).cloned().collect();
            assert_eq!(
                got, want,
                "{base} LIMIT {n}\n  early termination changed the answer"
            );
        }
    }
}

/// The same invariant with an OFFSET, which must be ADDED to the budget rather
/// than disqualifying it — the skipped rows still have to be produced.
#[test]
fn limit_with_offset_is_always_the_right_window() {
    for kind in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"] {
        let base = format!(
            "SELECT e.name, d.dname FROM emp e {kind} dept d ON e.dept_id = d.id"
        );
        let (_, all) = run_all_strategies(&base);

        for off in 0..=all.len() + 1 {
            for lim in 0..4 {
                let (_, got) =
                    run_all_strategies(&format!("{base} LIMIT {lim} OFFSET {off}"));
                let want: Vec<Value> =
                    all.iter().skip(off).take(lim).cloned().collect();
                assert_eq!(got, want, "{base} LIMIT {lim} OFFSET {off}");
            }
        }
    }
}

/// A `WHERE` clause disqualifies the budget, because filtering happens AFTER
/// the join here — capping the join would starve the filter.
#[test]
fn a_filter_after_the_join_still_sees_every_row() {
    // Only `rob` survives, and he is the LAST matching row emitted. A join
    // capped at one row would return nothing.
    expect(
        "SELECT e.name FROM emp e JOIN dept d ON e.dept_id = d.id \
         WHERE d.dname = 'legal' LIMIT 1",
        json!([{"name": "rob"}]),
    );
}

/// `ORDER BY` disqualifies it, because the prefix depends on the sort rather
/// than on emission order.
#[test]
fn a_sort_after_the_join_still_sees_every_row() {
    // The highest salary belongs to `grace`, emitted second. Capping the join
    // at one row would answer `ada`.
    expect(
        "SELECT e.name FROM emp e JOIN dept d ON e.dept_id = d.id \
         ORDER BY e.salary DESC LIMIT 1",
        json!([{"name": "grace"}]),
    );
}

/// `DISTINCT` disqualifies it, because deduplication can shrink the row count.
#[test]
fn distinct_after_the_join_still_sees_every_row() {
    // Two employees are in `eng`, so DISTINCT collapses them. A join capped at
    // 2 rows would yield only `eng` where 2 distinct departments exist.
    expect(
        "SELECT DISTINCT d.dname FROM emp e JOIN dept d ON e.dept_id = d.id \
         ORDER BY 1 LIMIT 2",
        json!([{"dname": "eng"}, {"dname": "legal"}]),
    );
}

/// More than one join disqualifies it, because capping an intermediate result
/// can starve a later join of the rows it needed.
///
/// `proj_late` matches only `legal`, and `legal` is the LAST row the emp-dept
/// join emits. So capping that first join at ANY size below its full output
/// makes this query return nothing at all.
///
/// Written this way on purpose: an earlier version of this test used `proj`
/// and an `ORDER BY`, and a deliberate mutation that removed the multi-join
/// guard still passed it — the rows it needed happened to survive the cap.
/// A test that cannot fail is not evidence.
#[test]
fn a_second_join_still_sees_every_row_from_the_first() {
    expect(
        "SELECT p.pname FROM emp e \
         JOIN dept d ON e.dept_id = d.id \
         JOIN proj_late p ON d.id = p.dept_id \
         LIMIT 1",
        json!([{"pname": "zeta"}]),
    );
    // And with the budget-eligible shape, for completeness: a single join to a
    // relation whose only match is last.
    expect(
        "SELECT d.dname FROM dept d JOIN proj_late p ON d.id = p.dept_id LIMIT 1",
        json!([{"dname": "legal"}]),
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Predicate pushdown is invisible in the answers
//
// A conjunct reading exactly one relation is COPIED to pre-filter that
// relation before the join — but only when the relation is never
// NULL-synthesised. These cases pin both halves of that rule.
// ─────────────────────────────────────────────────────────────────────────────

/// The case that disproved the first version of the pushdown safety argument.
///
/// The original reasoning was that copying a predicate (rather than moving it)
/// made pre-filtering safe for any join type, because newly-unmatched rows
/// would be NULL-extended and then dropped by the retained `WHERE`. But a
/// predicate can be SATISFIED by a synthesised NULL: no `dept` row has a NULL
/// `dname`, so pre-filtering `dept` empties it, every `emp` row becomes
/// unmatched, and `IS NULL` is then TRUE for all five.
///
/// The answer below is 1 row. It went to 5. That is why the rule is about NULL
/// synthesis rather than about retention.
#[test]
fn a_predicate_satisfied_by_a_synthesised_null_is_not_pushed() {
    expect(
        "SELECT e.name FROM emp e LEFT JOIN dept d ON e.dept_id = d.id \
         WHERE d.dname IS NULL ORDER BY 1",
        json!([{"name": "ken"}]),
    );
    // The same shape with NOT, which is equally satisfied by a NULL becoming
    // UNKNOWN under negation... except it is NOT: `NOT UNKNOWN` is UNKNOWN, so
    // this one drops the outer rows. Both directions pinned.
    expect(
        "SELECT e.name FROM emp e LEFT JOIN dept d ON e.dept_id = d.id \
         WHERE NOT (d.dname = 'eng') ORDER BY 1",
        json!([{"name": "linus"}, {"name": "rob"}]),
    );
}

/// A `RIGHT`/`FULL` join synthesises NULLs across the whole LEFT side, so a
/// predicate on the driving relation cannot be pushed either.
#[test]
fn a_right_join_protects_the_left_relation_from_pushdown() {
    expect(
        "SELECT e.name, d.dname FROM emp e RIGHT JOIN dept d ON e.dept_id = d.id \
         WHERE e.name IS NULL ORDER BY 2",
        json!([
            {"name": null, "dname": "ghost"},
            {"name": null, "dname": "limbo"},
        ]),
    );
}

/// The non-nullable side is pushed, and the answer is unchanged — which is the
/// case that actually matters for performance.
#[test]
fn the_non_nullable_side_is_pushed_without_changing_the_answer() {
    expect(
        "SELECT e.name, d.dname FROM emp e LEFT JOIN dept d ON e.dept_id = d.id \
         WHERE e.salary > 140 ORDER BY 1",
        json!([
            {"name": "grace", "dname": "eng"},
            {"name": "ken",   "dname": null},
            {"name": "linus", "dname": "ops"},
        ]),
    );
    // An inner join can push both sides.
    expect(
        "SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept_id = d.id \
         WHERE e.salary > 110 AND d.dname <> 'ops' ORDER BY 1",
        json!([
            {"name": "grace", "dname": "eng"},
            {"name": "rob",   "dname": "legal"},
        ]),
    );
}

/// A predicate reading two relations is never pushed, because neither
/// relation can evaluate it alone.
#[test]
fn a_predicate_spanning_two_relations_is_left_above_the_join() {
    expect(
        "SELECT e.name FROM emp e JOIN dept d ON e.dept_id = d.id \
         WHERE e.salary > d.id ORDER BY 1",
        json!([
            {"name": "ada"}, {"name": "grace"}, {"name": "linus"}, {"name": "rob"},
        ]),
    );
}

/// An `OR` is never split, so a predicate whose halves read different
/// relations stays whole.
#[test]
fn an_or_across_relations_is_not_split_for_pushdown() {
    expect(
        "SELECT e.name FROM emp e JOIN dept d ON e.dept_id = d.id \
         WHERE e.salary > 190 OR d.dname = 'legal' ORDER BY 1",
        json!([{"name": "grace"}, {"name": "rob"}]),
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Known divergences from PostgreSQL
//
// These are NOT part of the contract above. They are recorded because writing
// the corpus is what exposed them, and a divergence that is written down can
// be fixed on purpose — whereas one absorbed into an expectation as though it
// were correct is how a wart becomes a feature nobody dares touch.
//
// Each is scheduled for its own PR, so the expectation below visibly FLIPS
// when it is fixed. Deliberately not folded into a performance change.
// ─────────────────────────────────────────────────────────────────────────────

/// An integral value renders as an integer, a fractional one as a float.
///
/// This was a real divergence, found by writing this corpus: the lexer holds
/// every number as `f64`, so `SELECT 1` answered `1.0` — which a client reads
/// as the TEXT "1.0" where PostgreSQL says "1". The liveness probe that every
/// driver opens with was the most visible casualty.
///
/// What the rule cannot do: PostgreSQL distinguishes `1` from `1.0` (numeric
/// with scale), and JSON has no numeric-with-scale type, so `SELECT 1.0` also
/// renders as `1` here. That distinction is unrepresentable either way.
#[test]
fn integral_values_render_as_integers() {
    let (_, rows) = run_all_strategies(
        "SELECT 1 AS a, length('ada') AS b, 7 / 2 AS c, 6 / 2 AS d, 2 + 2 AS e \
         FROM proj LIMIT 1",
    );
    assert_eq!(rows[0], json!({"a": 1, "b": 3, "c": 3.5, "d": 3, "e": 4}));
    assert!(rows[0]["a"].is_i64(), "an integral value must be a JSON integer");
    assert!(!rows[0]["c"].is_i64(), "a fractional value must stay a float");
}

/// `SELECT *` lists columns in the DOCUMENT's own order.
///
/// This was the second divergence the corpus exposed. Rows are
/// `serde_json::Map`, which defaults to a `BTreeMap` and discards insertion
/// order on the way in — so column order came out alphabetical, and no fix
/// was possible inside this engine because the order was already gone before
/// a row reached it.
///
/// The fix is the `preserve_order` feature on `serde_json`, which makes that
/// map order-preserving crate-wide. Safe for the content-addressed store
/// because a node's hash is taken over the BYTES AS WRITTEN and verification
/// re-hashes those same bytes — it never re-serialises a parsed node, so key
/// order cannot invalidate an existing hash. Verified against the real daemon
/// rather than assumed.
#[test]
fn star_column_order_follows_the_document() {
    let (names, _) = run_all_strategies("SELECT * FROM proj");
    assert_eq!(names, ["pid", "dept_id", "pname"], "the document's own order");
}

// ─────────────────────────────────────────────────────────────────────────────
// Explicit refusals — an honest error beats a plausible answer
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn unsupported_constructs_are_refused_by_name() {
    expect_refused("SELECT count(*) FROM emp GROUP BY dept_id", "GROUP BY");
    expect_refused("SELECT 1 FROM emp HAVING 1 = 1", "HAVING");
    expect_refused("SELECT DISTINCT ON (id) id FROM emp", "DISTINCT ON");
    expect_refused("SELECT 1 FROM emp e JOIN dept d USING (id)", "USING");
    expect_refused("SELECT 1 FROM emp e LEFT JOIN dept d", "ON clause");
    expect_refused("SELECT 1 FROM nosuch", "does not exist");
    expect_refused("SELECT x.id FROM emp e", "no table or alias named");
}

#[test]
fn a_refusal_names_the_construct_it_actually_choked_on() {
    // A misleading error is worse than a blunt one: it sends the reader to fix
    // the wrong thing. `\d` was told "JOIN is not supported" long after joins
    // worked.
    let err = match parse("SELECT 1 FROM emp e JOIN dept d ON e.id = d.id") {
        Ok(_) => String::new(),
        Err(e) => format!("{e:#}"),
    };
    assert!(err.is_empty(), "joins work now, so nothing should mention them: {err}");
}
