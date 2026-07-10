// M4b slice 1: Zero v51 AST validation + SQLite compilation. every supported
// shape compiles and runs against a real table; every unsupported shape is a
// deterministic rejection. the compiled SQL uses positional `?` binds only.
mod common;

use common::TestDb;
use serde_json::{Value, json};

use sync_core::query::{compile, parse_ast};
use sync_core::schema::TableSpec;
use sync_core::value::ZeroColumnType;
use sync_core::{SqlValue, SyncDb, Tables};

fn schema() -> Tables {
    use ZeroColumnType::*;
    Tables::new()
        .with(
            "issue",
            TableSpec {
                columns: vec![
                    ("id".into(), String),
                    ("title".into(), String),
                    ("closed".into(), Boolean),
                    ("priority".into(), Number),
                    ("ownerId".into(), String),
                ],
                primary_key: vec!["id".into()],
            },
        )
        .with(
            "comment",
            TableSpec {
                columns: vec![
                    ("id".into(), String),
                    ("issueId".into(), String),
                    ("body".into(), String),
                ],
                primary_key: vec!["id".into()],
            },
        )
}

fn seeded_db() -> TestDb {
    let mut db = TestDb::memory();
    db.exec(
        "CREATE TABLE issue (id TEXT PRIMARY KEY, title TEXT, closed INTEGER, priority INTEGER, ownerId TEXT)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE comment (id TEXT PRIMARY KEY, issueId TEXT, body TEXT)",
        &[],
    )
    .unwrap();
    let rows = [
        ("i1", "alpha", 0, 5, "u1"),
        ("i2", "beta", 1, 3, "u1"),
        ("i3", "gamma", 0, 1, "u2"),
        ("i4", "delta", 0, 3, "u2"),
    ];
    for (id, title, closed, priority, owner) in rows {
        db.exec(
            "INSERT INTO issue VALUES (?, ?, ?, ?, ?)",
            &[
                SqlValue::Text(id.into()),
                SqlValue::Text(title.into()),
                SqlValue::Integer(closed),
                SqlValue::Integer(priority),
                SqlValue::Text(owner.into()),
            ],
        )
        .unwrap();
    }
    // comments only on i1 and i3
    for (id, issue) in [("c1", "i1"), ("c2", "i1"), ("c3", "i3")] {
        db.exec(
            "INSERT INTO comment VALUES (?, ?, 'x')",
            &[SqlValue::Text(id.into()), SqlValue::Text(issue.into())],
        )
        .unwrap();
    }
    db
}

// compile the AST json and run it, returning the matched issue ids in order
fn run_ids(db: &mut TestDb, ast_json: Value) -> Vec<String> {
    let ast = parse_ast(&ast_json).expect("valid AST");
    let compiled = compile(&ast, &schema()).expect("compiles");
    let rows = db.query(&compiled.sql, &compiled.params).unwrap();
    rows.iter()
        .map(|r| match r.get("id") {
            Some(SqlValue::Text(s)) => s.clone(),
            other => panic!("unexpected id {other:?}"),
        })
        .collect()
}

fn simple(op: &str, col: &str, value: Value) -> Value {
    json!({ "type": "simple", "op": op, "left": { "type": "column", "name": col }, "right": { "type": "literal", "value": value } })
}

#[test]
fn equality_condition() {
    let mut db = seeded_db();
    let ids = run_ids(
        &mut db,
        json!({ "table": "issue", "where": simple("=", "closed", json!(false)) }),
    );
    assert_eq!(sorted(ids), vec!["i1", "i3", "i4"]); // open issues
}

#[test]
fn comparison_condition() {
    let mut db = seeded_db();
    let ids = run_ids(
        &mut db,
        json!({ "table": "issue", "where": simple(">=", "priority", json!(3)) }),
    );
    assert_eq!(sorted(ids), vec!["i1", "i2", "i4"]);
}

#[test]
fn conjunction_and_disjunction() {
    let mut db = seeded_db();
    let and = json!({ "table": "issue", "where": { "type": "and", "conditions": [
        simple("=", "closed", json!(false)), simple(">=", "priority", json!(3))
    ] } });
    assert_eq!(sorted(run_ids(&mut db, and)), vec!["i1", "i4"]);

    let or = json!({ "table": "issue", "where": { "type": "or", "conditions": [
        simple("=", "priority", json!(5)), simple("=", "priority", json!(1))
    ] } });
    assert_eq!(sorted(run_ids(&mut db, or)), vec!["i1", "i3"]);
}

#[test]
fn correlated_exists_and_not_exists() {
    let mut db = seeded_db();
    let exists = json!({ "table": "issue", "where": {
        "type": "correlatedSubquery", "op": "EXISTS",
        "related": {
            "correlation": { "parentField": ["id"], "childField": ["issueId"] },
            "subquery": { "table": "comment" }
        }
    } });
    assert_eq!(sorted(run_ids(&mut db, exists)), vec!["i1", "i3"]); // have comments

    let not_exists = json!({ "table": "issue", "where": {
        "type": "correlatedSubquery", "op": "NOT EXISTS",
        "related": {
            "correlation": { "parentField": ["id"], "childField": ["issueId"] },
            "subquery": { "table": "comment" }
        }
    } });
    assert_eq!(sorted(run_ids(&mut db, not_exists)), vec!["i2", "i4"]);
}

#[test]
fn exists_with_nested_where() {
    let mut db = seeded_db();
    // issues owned by u2 that have a comment
    let q = json!({ "table": "issue", "where": { "type": "and", "conditions": [
        simple("=", "ownerId", json!("u2")),
        { "type": "correlatedSubquery", "op": "EXISTS", "related": {
            "correlation": { "parentField": ["id"], "childField": ["issueId"] },
            "subquery": { "table": "comment" }
        } }
    ] } });
    assert_eq!(sorted(run_ids(&mut db, q)), vec!["i3"]);
}

#[test]
fn order_by_with_pk_tiebreak_and_limit() {
    let mut db = seeded_db();
    // priority desc, then id asc; limit 2
    let q = json!({ "table": "issue", "orderBy": [["priority", "desc"]], "limit": 2 });
    let ids = run_ids(&mut db, q);
    // priorities: i1=5, i2=3, i4=3, i3=1 -> desc, ties broken by id asc:
    // i1(5), i2(3) [i2 < i4]
    assert_eq!(ids, vec!["i1", "i2"]);
}

#[test]
fn start_cursor_paginates() {
    let mut db = seeded_db();
    // order by id asc, start exclusively after i2 -> i3, i4
    let q = json!({
        "table": "issue",
        "orderBy": [["id", "asc"]],
        "start": { "row": { "id": "i2" }, "exclusive": true }
    });
    assert_eq!(run_ids(&mut db, q), vec!["i3", "i4"]);

    // inclusive start at i2 -> i2, i3, i4
    let q2 = json!({
        "table": "issue",
        "orderBy": [["id", "asc"]],
        "start": { "row": { "id": "i2" }, "exclusive": false }
    });
    assert_eq!(run_ids(&mut db, q2), vec!["i2", "i3", "i4"]);
}

#[test]
fn start_cursor_multi_key() {
    let mut db = seeded_db();
    // order priority desc, id asc; start exclusively after (priority=3, id=i2)
    // remaining in order: i4(3), i3(1)
    let q = json!({
        "table": "issue",
        "orderBy": [["priority", "desc"], ["id", "asc"]],
        "start": { "row": { "priority": 3, "id": "i2" }, "exclusive": true }
    });
    assert_eq!(run_ids(&mut db, q), vec!["i4", "i3"]);
}

#[test]
fn dependency_tables_collected() {
    let ast = parse_ast(&json!({ "table": "issue", "where": {
        "type": "correlatedSubquery", "op": "EXISTS",
        "related": { "correlation": { "parentField": ["id"], "childField": ["issueId"] },
                     "subquery": { "table": "comment" } }
    } }))
    .unwrap();
    let compiled = compile(&ast, &schema()).unwrap();
    assert_eq!(
        compiled.dependency_tables,
        vec!["comment".to_string(), "issue".to_string()]
    );
    assert_eq!(compiled.primary_key, vec!["id".to_string()]);
    // positional binds only — no interpolated literal ever appears in the SQL
    assert!(
        !compiled.sql.contains("'"),
        "sql must not interpolate literals: {}",
        compiled.sql
    );
}

// ---- deterministic rejections ---------------------------------------------

fn reject(ast_json: Value) -> u16 {
    parse_ast(&ast_json)
        .and_then(|ast| compile(&ast, &schema()).map(|_| ()))
        .expect_err("should reject")
        .status
}

#[test]
fn like_and_in_and_is_null() {
    let mut db = seeded_db();
    // LIKE on title: titles starting with a consonant-then-'l' — 'al%' matches
    // "alpha" only
    let like = json!({ "table": "issue", "where": simple("LIKE", "title", json!("al%")) });
    assert_eq!(sorted(run_ids(&mut db, like)), vec!["i1"]); // alpha

    // IN over priorities
    let in_q = json!({ "table": "issue", "where": {
        "type": "simple", "op": "IN",
        "left": { "type": "column", "name": "priority" },
        "right": { "type": "literal", "value": [1, 5] }
    } });
    assert_eq!(sorted(run_ids(&mut db, in_q)), vec!["i1", "i3"]);

    // NOT IN
    let not_in = json!({ "table": "issue", "where": {
        "type": "simple", "op": "NOT IN",
        "left": { "type": "column", "name": "priority" },
        "right": { "type": "literal", "value": [1, 5] }
    } });
    assert_eq!(sorted(run_ids(&mut db, not_in)), vec!["i2", "i4"]);

    // IS NULL via IS op with a null literal (ownerId is never null here, use a
    // column that can be null: reuse priority with a crafted row)
    db.exec("INSERT INTO issue VALUES ('i5', 'eps', 0, NULL, 'u1')", &[])
        .unwrap();
    let is_null = json!({ "table": "issue", "where": {
        "type": "simple", "op": "IS",
        "left": { "type": "column", "name": "priority" },
        "right": { "type": "literal", "value": null }
    } });
    assert_eq!(run_ids(&mut db, is_null), vec!["i5"]);
}

#[test]
fn rejects_unsupported_shapes() {
    // IN with a scalar (non-array) operand
    assert_eq!(
        reject(json!({ "table": "issue", "where": simple("IN", "priority", json!(2)) })),
        400
    );
    // array literal (IN operand)
    assert_eq!(
        reject(json!({ "table": "issue", "where": simple("=", "priority", json!([1, 2])) })),
        400
    );
    // unknown table
    assert_eq!(reject(json!({ "table": "nope" })), 400);
    // unknown column
    assert_eq!(
        reject(json!({ "table": "issue", "where": simple("=", "nope", json!(1)) })),
        400
    );
    // unknown top-level field
    assert_eq!(reject(json!({ "table": "issue", "bogus": 1 })), 400);
    // static parameter left unresolved
    assert_eq!(
        reject(json!({ "table": "issue", "where": {
            "type": "simple", "op": "=", "left": { "type": "column", "name": "ownerId" },
            "right": { "type": "static", "anchor": "authData", "field": "sub" }
        } })),
        400
    );
    // unknown condition type
    assert_eq!(
        reject(json!({ "table": "issue", "where": { "type": "between" } })),
        400
    );
    // unknown orderBy direction
    assert_eq!(
        reject(json!({ "table": "issue", "orderBy": [["id", "sideways"]] })),
        400
    );
    // EXISTS onto an unknown child table
    assert_eq!(
        reject(json!({ "table": "issue", "where": {
            "type": "correlatedSubquery", "op": "EXISTS",
            "related": { "correlation": { "parentField": ["id"], "childField": ["x"] },
                         "subquery": { "table": "ghost" } }
        } })),
        400
    );
}

#[test]
fn compiles_bounded_child_subqueries() {
    // GAP-2: a related-output child carrying a per-parent orderBy/limit/start is
    // COMPILED (per-parent window), not rejected -- Chat's inventory needs the
    // windowed "last N per parent" shape (and .one()). correctness of the produced
    // membership is proved in query_related.rs; here we only assert they compile.
    let rel = |child: Value| {
        json!({ "table": "issue", "related": [{
            "correlation": { "parentField": ["id"], "childField": ["issueId"] },
            "subquery": child }] })
    };
    let ok = |ast: Value| {
        parse_ast(&ast)
            .and_then(|a| compile(&a, &schema()).map(|_| ()))
            .is_ok()
    };

    assert!(ok(rel(json!({ "table": "comment", "limit": 1 })))); // .one()
    assert!(ok(rel(
        json!({ "table": "comment", "orderBy": [["id", "desc"]], "limit": 2 })
    ))); // top-2 per parent
    assert!(ok(rel(
        json!({ "table": "comment", "orderBy": [["id", "asc"]],
        "start": { "row": { "id": "c1" }, "exclusive": true }, "limit": 3 })
    )));
    // start with no limit compiles (cursor-bounded set)
    assert!(ok(rel(
        json!({ "table": "comment", "orderBy": [["id", "asc"]],
        "start": { "row": { "id": "c1" }, "exclusive": true } })
    )));
    // nested: a bounded grandchild under a related child compiles
    assert!(ok(rel(json!({ "table": "comment", "related": [{
        "correlation": { "parentField": ["issueId"], "childField": ["issueId"] },
        "subquery": { "table": "comment", "limit": 1 } }] }))));
    // orderBy-only and plain children still compile
    assert!(ok(rel(
        json!({ "table": "comment", "orderBy": [["id", "asc"]] })
    )));
    assert!(ok(rel(json!({ "table": "comment" }))));
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}
