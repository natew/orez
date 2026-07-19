// M4c compiler-gate items from plans/chat-query-inventory.md §7: the exact
// shapes Chat's production query traffic exercises beyond the plan's baseline
// AST list. each is confirmed to compile and run correctly here.
mod common;

use common::TestDb;
use serde_json::{Value, json};

use sync_core::query::{compile, parse_ast};
use sync_core::schema::TableSpec;
use sync_core::value::ZeroColumnType;
use sync_core::{SqlValue, SyncDb, Tables};

fn tables() -> Tables {
    use ZeroColumnType::*;
    let s = |n: &str| (n.to_string(), String);
    Tables::new()
        .with(
            "user",
            TableSpec {
                columns: vec![s("id"), s("name")],
                primary_key: vec!["id".into()],
                encrypted_columns: Default::default(),
                encrypted_physical_columns: Default::default(),
            },
        )
        .with(
            "channel",
            TableSpec {
                columns: vec![s("id"), s("serverId"), ("private".into(), Boolean)],
                primary_key: vec!["id".into()],
                encrypted_columns: Default::default(),
                encrypted_physical_columns: Default::default(),
            },
        )
        .with(
            "channelUserRole",
            TableSpec {
                columns: vec![s("id"), s("channelId"), s("userId")],
                primary_key: vec!["id".into()],
                encrypted_columns: Default::default(),
                encrypted_physical_columns: Default::default(),
            },
        )
        // composite primary key (serverId, userId) — 9 of Chat's 51 tables have these
        .with(
            "member",
            TableSpec {
                columns: vec![s("serverId"), s("userId"), s("role")],
                primary_key: vec!["serverId".into(), "userId".into()],
                encrypted_columns: Default::default(),
                encrypted_physical_columns: Default::default(),
            },
        )
}

fn db() -> TestDb {
    let mut db = TestDb::memory();
    db.exec("CREATE TABLE user (id TEXT PRIMARY KEY, name TEXT)", &[])
        .unwrap();
    db.exec(
        "CREATE TABLE channel (id TEXT PRIMARY KEY, serverId TEXT, private INTEGER)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE channelUserRole (id TEXT PRIMARY KEY, channelId TEXT, userId TEXT)",
        &[],
    )
    .unwrap();
    db.exec("CREATE TABLE member (serverId TEXT, userId TEXT, role TEXT, PRIMARY KEY (serverId, userId))", &[]).unwrap();
    db
}

fn ids(db: &mut TestDb, ast_json: Value) -> Vec<String> {
    let ast = parse_ast(&ast_json).expect("valid AST");
    let compiled = compile(&ast, &tables()).expect("compiles");
    db.query(&compiled.sql, &compiled.params)
        .unwrap()
        .iter()
        .map(|r| match r.get("id") {
            Some(SqlValue::Text(s)) => s.clone(),
            _ => String::new(),
        })
        .collect()
}

fn simple(op: &str, col: &str, value: Value) -> Value {
    json!({ "type": "simple", "op": op, "left": { "type": "column", "name": col }, "right": { "type": "literal", "value": value } })
}

#[test]
fn ilike_is_case_insensitive() {
    let mut db = db();
    db.exec(
        "INSERT INTO user VALUES ('u1','Alice'), ('u2','alBERT'), ('u3','Bob')",
        &[],
    )
    .unwrap();
    // ILIKE '%al%' folds case on both sides -> Alice, alBERT (both contain al/AL)
    let mut got = ids(
        &mut db,
        json!({ "table": "user", "where": simple("ILIKE", "name", json!("%al%")) }),
    );
    got.sort();
    assert_eq!(got, vec!["u1", "u2"]);

    // exact ILIKE match ignoring case
    assert_eq!(
        ids(
            &mut db,
            json!({ "table": "user", "where": simple("ILIKE", "name", json!("alice")) })
        ),
        vec!["u1"]
    );
}

#[test]
fn like_pattern_matches() {
    let mut db = db();
    db.exec(
        "INSERT INTO user VALUES ('u1','alice'), ('u2','alfred'), ('u3','bob'), ('u4','ALICE')",
        &[],
    )
    .unwrap();
    let mut got = ids(
        &mut db,
        json!({ "table": "user", "where": simple("LIKE", "name", json!("al%")) }),
    );
    got.sort();
    assert_eq!(got, vec!["u1", "u2"]);
}

#[test]
fn in_empty_array_is_constant_false() {
    let mut db = db();
    db.exec("INSERT INTO user VALUES ('u1','a'), ('u2','b')", &[])
        .unwrap();
    // IN [] -> no rows (constant false, not a SQL error)
    let in_empty = json!({ "table": "user", "where": {
        "type": "simple", "op": "IN", "left": { "type": "column", "name": "id" },
        "right": { "type": "literal", "value": [] } } });
    assert!(ids(&mut db, in_empty).is_empty());
    // NOT IN [] -> all rows
    let not_in_empty = json!({ "table": "user", "where": {
        "type": "simple", "op": "NOT IN", "left": { "type": "column", "name": "id" },
        "right": { "type": "literal", "value": [] } } });
    let mut got = ids(&mut db, not_in_empty);
    got.sort();
    assert_eq!(got, vec!["u1", "u2"]);
}

#[test]
fn is_null_and_is_not_null() {
    let mut db = db();
    db.exec(
        "INSERT INTO user (id, name) VALUES ('u1','a'), ('u2', NULL)",
        &[],
    )
    .unwrap();
    // IS NULL via a bound null literal
    assert_eq!(
        ids(
            &mut db,
            json!({ "table": "user", "where": simple("IS", "name", json!(null)) })
        ),
        vec!["u2"]
    );
    // IS NOT NULL
    assert_eq!(
        ids(
            &mut db,
            json!({ "table": "user", "where": simple("IS NOT", "name", json!(null)) })
        ),
        vec!["u1"]
    );
}

#[test]
fn two_hop_junction_exists_in_a_permission() {
    // channel -> channelUserRole (junction) -> user: channels where a user named
    // 'alice' holds a role. this is the security-relevant channelUserRoles
    // junction shape, expressed as a nested EXISTS.
    let mut db = db();
    db.exec(
        "INSERT INTO channel VALUES ('c1','s1',1), ('c2','s1',1)",
        &[],
    )
    .unwrap();
    db.exec("INSERT INTO user VALUES ('ua','alice'), ('ub','bob')", &[])
        .unwrap();
    // c1 has alice via the junction; c2 has bob
    db.exec(
        "INSERT INTO channelUserRole VALUES ('r1','c1','ua'), ('r2','c2','ub')",
        &[],
    )
    .unwrap();

    let q = json!({ "table": "channel", "where": {
        "type": "correlatedSubquery", "op": "EXISTS",
        "related": {
            "correlation": { "parentField": ["id"], "childField": ["channelId"] },
            "subquery": { "table": "channelUserRole", "where": {
                "type": "correlatedSubquery", "op": "EXISTS",
                "related": {
                    "correlation": { "parentField": ["userId"], "childField": ["id"] },
                    "subquery": { "table": "user", "where": simple("=", "name", json!("alice")) }
                }
            } }
        }
    } });
    assert_eq!(ids(&mut db, q), vec!["c1"]);
}

#[test]
fn composite_pk_ordering_tiebreak() {
    let mut db = db();
    // same role for all -> the tie-breaker (serverId asc, userId asc) decides order
    db.exec(
        "INSERT INTO member VALUES ('s2','ua','m'), ('s1','ub','m'), ('s1','ua','m')",
        &[],
    )
    .unwrap();
    let ast = parse_ast(&json!({ "table": "member", "orderBy": [["role", "asc"]] })).unwrap();
    let compiled = compile(&ast, &tables()).unwrap();
    // the compiled ORDER BY carries both composite-PK columns as the tie-break
    assert!(
        compiled.sql.contains("\"serverId\" ASC"),
        "sql: {}",
        compiled.sql
    );
    assert!(
        compiled.sql.contains("\"userId\" ASC"),
        "sql: {}",
        compiled.sql
    );
    // rows come out ordered by (serverId, userId): (s1,ua),(s1,ub),(s2,ua)
    let rows = db.query(&compiled.sql, &compiled.params).unwrap();
    let order: Vec<String> = rows
        .iter()
        .map(|r| {
            let sid = match r.get("serverId") {
                Some(SqlValue::Text(s)) => s.clone(),
                _ => String::new(),
            };
            let uid = match r.get("userId") {
                Some(SqlValue::Text(s)) => s.clone(),
                _ => String::new(),
            };
            format!("{sid}/{uid}")
        })
        .collect();
    assert_eq!(order, vec!["s1/ua", "s1/ub", "s2/ua"]);
}

#[test]
fn runtime_dynamic_orderby_column() {
    // dataSearch/jobsSorted pass a sort column chosen at runtime; any real schema
    // column must be orderable (not an enumerated allow-list).
    let mut db = db();
    db.exec(
        "INSERT INTO user VALUES ('u1','carol'), ('u2','anna'), ('u3','bob')",
        &[],
    )
    .unwrap();
    let by_name = ids(
        &mut db,
        json!({ "table": "user", "orderBy": [["name", "asc"]] }),
    );
    assert_eq!(by_name, vec!["u2", "u3", "u1"]); // anna, bob, carol
}

#[test]
fn limit_zero_returns_no_rows() {
    let mut db = db();
    db.exec("INSERT INTO user VALUES ('u1','a'), ('u2','b')", &[])
        .unwrap();
    // topPrivateChats uses limit(0)
    assert!(ids(&mut db, json!({ "table": "user", "limit": 0 })).is_empty());
    // limit 1 (the .one() shape) returns exactly one
    assert_eq!(
        ids(
            &mut db,
            json!({ "table": "user", "orderBy": [["id", "asc"]], "limit": 1 })
        ),
        vec!["u1"]
    );
}
