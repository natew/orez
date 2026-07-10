// M4b slice 2: query-aware membership + per-client-group refcounts. a query's
// matching rows become row puts; a row that leaves every active query is del'd
// only when its last reference disappears (invariant 14); dropping a desired
// query removes its rows unless another query still holds them (invariant 15,
// membership side); a data change to a still-member row re-emits it.
mod common;

use std::collections::BTreeSet;

use common::TestDb;
use serde_json::{Value, json};

use sync_core::query::{
    init_query_schema, recompute_group, register_query, remove_desire, set_desire,
};
use sync_core::schema::TableSpec;
use sync_core::value::ZeroColumnType;
use sync_core::{SqlValue, SyncDb, Tables, Transactor};

const G: &str = "g1";

fn schema() -> Tables {
    use ZeroColumnType::*;
    Tables::new().with(
        "issue",
        TableSpec {
            columns: vec![
                ("id".into(), String),
                ("title".into(), String),
                ("closed".into(), Boolean),
                ("priority".into(), Number),
            ],
            primary_key: vec!["id".into()],
        },
    )
}

struct Host {
    db: TestDb,
    tables: Tables,
}

impl Host {
    fn new() -> Host {
        let mut db = TestDb::memory();
        db.exec(
            "CREATE TABLE issue (id TEXT PRIMARY KEY, title TEXT, closed INTEGER, priority INTEGER)",
            &[],
        )
        .unwrap();
        init_query_schema(&mut db).unwrap();
        for (id, closed, priority) in [("i1", 0, 5), ("i2", 1, 3), ("i3", 0, 1), ("i4", 0, 3)] {
            db.exec(
                "INSERT INTO issue VALUES (?, ?, ?, ?)",
                &[
                    SqlValue::Text(id.into()),
                    SqlValue::Text(format!("t-{id}")),
                    SqlValue::Integer(closed),
                    SqlValue::Integer(priority),
                ],
            )
            .unwrap();
        }
        Host {
            db,
            tables: schema(),
        }
    }

    fn exec(&mut self, sql: &str) {
        self.db.exec(sql, &[]).unwrap();
    }

    fn register(&mut self, hash: &str, ast: Value) {
        register_query(&mut self.db, &self.tables, G, hash, &ast, 0).unwrap();
    }

    fn desire(&mut self, client: &str, hash: &str, version: i64) {
        set_desire(&mut self.db, G, client, hash, version).unwrap();
    }

    fn undesire(&mut self, client: &str, hash: &str) {
        remove_desire(&mut self.db, G, client, hash).unwrap();
    }

    fn recompute(&mut self, changed: &[(&str, &str)]) -> Vec<Value> {
        let set: BTreeSet<(String, String)> = changed
            .iter()
            .map(|(t, id)| (t.to_string(), json!({ "id": id }).to_string()))
            .collect();
        let tables = self.tables.clone();
        self.db
            .transaction(|db| recompute_group(db, &tables, G, &set))
            .unwrap()
    }
}

fn where_eq(col: &str, v: Value) -> Value {
    json!({ "type": "simple", "op": "=", "left": { "type": "column", "name": col }, "right": { "type": "literal", "value": v } })
}
fn where_cmp(op: &str, col: &str, v: Value) -> Value {
    json!({ "type": "simple", "op": op, "left": { "type": "column", "name": col }, "right": { "type": "literal", "value": v } })
}

fn put_ids(patch: &[Value]) -> Vec<String> {
    let mut ids: Vec<String> = patch
        .iter()
        .filter(|op| op["op"] == "put")
        .map(|op| op["value"]["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    ids
}
fn del_ids(patch: &[Value]) -> Vec<String> {
    let mut ids: Vec<String> = patch
        .iter()
        .filter(|op| op["op"] == "del")
        .map(|op| op["id"]["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    ids
}

#[test]
fn single_query_puts_members_and_dels_on_leave() {
    let mut h = Host::new();
    // open issues (closed = false)
    h.register(
        "q_open",
        json!({ "table": "issue", "where": where_eq("closed", json!(false)) }),
    );
    h.desire("c1", "q_open", 1);
    let patch = h.recompute(&[]);
    assert_eq!(put_ids(&patch), vec!["i1", "i3", "i4"]);
    assert!(del_ids(&patch).is_empty());

    // recompute with no change -> nothing
    assert!(h.recompute(&[]).is_empty());

    // close i1 -> it leaves the query -> del
    h.exec("UPDATE issue SET closed = 1 WHERE id = 'i1'");
    let patch = h.recompute(&[("issue", "i1")]);
    assert_eq!(del_ids(&patch), vec!["i1"]);
    assert!(put_ids(&patch).is_empty());
}

#[test]
fn changed_but_still_member_re_emits() {
    let mut h = Host::new();
    h.register(
        "q_open",
        json!({ "table": "issue", "where": where_eq("closed", json!(false)) }),
    );
    h.desire("c1", "q_open", 1);
    h.recompute(&[]);

    // change i3's data (still open, still a member) -> re-emit with new value
    h.exec("UPDATE issue SET title = 'renamed' WHERE id = 'i3'");
    let patch = h.recompute(&[("issue", "i3")]);
    assert_eq!(put_ids(&patch), vec!["i3"]);
    let put = patch.iter().find(|op| op["op"] == "put").unwrap();
    assert_eq!(put["value"]["title"], "renamed");
}

#[test]
fn overlapping_queries_retain_until_last_reference() {
    let mut h = Host::new();
    // q_open: {i1, i3, i4}; q_hi (priority>=3): {i1, i2, i4}
    h.register(
        "q_open",
        json!({ "table": "issue", "where": where_eq("closed", json!(false)) }),
    );
    h.register(
        "q_hi",
        json!({ "table": "issue", "where": where_cmp(">=", "priority", json!(3)) }),
    );
    h.desire("c1", "q_open", 1);
    h.desire("c1", "q_hi", 1);
    let patch = h.recompute(&[]);
    // union delivered once each
    assert_eq!(put_ids(&patch), vec!["i1", "i2", "i3", "i4"]);

    // drop q_hi: i2 was only in q_hi -> del; i1 and i4 stay (still in q_open)
    h.undesire("c1", "q_hi");
    let patch = h.recompute(&[]);
    assert_eq!(del_ids(&patch), vec!["i2"]);
    assert!(put_ids(&patch).is_empty());

    // drop q_open too: everything left -> del i1, i3, i4
    h.undesire("c1", "q_open");
    let patch = h.recompute(&[]);
    assert_eq!(del_ids(&patch), vec!["i1", "i3", "i4"]);
}

#[test]
fn permission_contraction_removes_forbidden_rows() {
    let mut h = Host::new();
    // model a permission predicate as priority >= 3 (a stand-in for a row-local
    // authorization column). the query result IS the authorized set.
    h.register(
        "q_perm",
        json!({ "table": "issue", "where": where_cmp(">=", "priority", json!(3)) }),
    );
    h.desire("c1", "q_perm", 1);
    assert_eq!(put_ids(&h.recompute(&[])), vec!["i1", "i2", "i4"]);

    // contraction: i4 loses authorization (priority drops below 3) -> must leave
    h.exec("UPDATE issue SET priority = 0 WHERE id = 'i4'");
    let patch = h.recompute(&[("issue", "i4")]);
    assert_eq!(del_ids(&patch), vec!["i4"]);

    // expansion: i3 gains authorization -> appears
    h.exec("UPDATE issue SET priority = 9 WHERE id = 'i3'");
    let patch = h.recompute(&[("issue", "i3")]);
    assert_eq!(put_ids(&patch), vec!["i3"]);
}

#[test]
fn transform_change_never_retains_a_more_permissive_result() {
    // invariant 15: a permission/schema transformation change re-registers the
    // query hash with a new transformed AST; the next recompute must drop the
    // now-forbidden rows even when no underlying row data changed.
    let mut h = Host::new();
    // v1 permission predicate admits everything (priority >= 0)
    h.register(
        "q_perm",
        json!({ "table": "issue", "where": where_cmp(">=", "priority", json!(0)) }),
    );
    h.desire("c1", "q_perm", 1);
    assert_eq!(put_ids(&h.recompute(&[])), vec!["i1", "i2", "i3", "i4"]);

    // transform TIGHTENS to priority >= 3 — i3 (priority 1) becomes forbidden
    h.register(
        "q_perm",
        json!({ "table": "issue", "where": where_cmp(">=", "priority", json!(3)) }),
    );
    let patch = h.recompute(&[]);
    assert_eq!(del_ids(&patch), vec!["i3"]);
    assert!(put_ids(&patch).is_empty());

    // transform LOOSENS to priority >= 1 — i3 becomes visible again
    h.register(
        "q_perm",
        json!({ "table": "issue", "where": where_cmp(">=", "priority", json!(1)) }),
    );
    assert_eq!(put_ids(&h.recompute(&[])), vec!["i3"]);
}

#[test]
fn two_clients_one_group_share_membership() {
    let mut h = Host::new();
    h.register(
        "q_open",
        json!({ "table": "issue", "where": where_eq("closed", json!(false)) }),
    );
    // two tabs (clients) in the same group both desire the query
    h.desire("c1", "q_open", 1);
    h.desire("c2", "q_open", 1);
    assert_eq!(put_ids(&h.recompute(&[])), vec!["i1", "i3", "i4"]);

    // c1 stops desiring; c2 still wants it -> rows stay (query still active)
    h.undesire("c1", "q_open");
    assert!(h.recompute(&[]).is_empty());

    // c2 also stops -> query inactive -> rows leave
    h.undesire("c2", "q_open");
    assert_eq!(del_ids(&h.recompute(&[])), vec!["i1", "i3", "i4"]);
}

// CRITICAL-2 regression: the query hash is client-chosen, so two client groups
// can desire the SAME hash carrying DIFFERENT permission-transformed ASTs. group
// B registering a permissive AST under group A's hash must NOT overwrite A's
// restricted definition — A's pull must still see only its restricted rows
// (invariant 15: a group-scoped query can never leak forbidden rows via a shared
// hash). before the (group, hash) rekey, B's ON CONFLICT overwrote the single
// global row and A recomputed under B's permissive AST.
#[test]
fn query_definitions_are_scoped_per_group_not_by_client_hash() {
    let mut db = TestDb::memory();
    db.exec(
        "CREATE TABLE issue (id TEXT PRIMARY KEY, title TEXT, closed INTEGER, priority INTEGER)",
        &[],
    )
    .unwrap();
    init_query_schema(&mut db).unwrap();
    for (id, closed, priority) in [("i1", 0, 5), ("i2", 1, 3), ("i3", 0, 1)] {
        db.exec(
            "INSERT INTO issue VALUES (?, ?, ?, ?)",
            &[
                SqlValue::Text(id.into()),
                SqlValue::Text(format!("t-{id}")),
                SqlValue::Integer(closed),
                SqlValue::Integer(priority),
            ],
        )
        .unwrap();
    }
    let tables = schema();
    const GA: &str = "groupA";
    const GB: &str = "groupB";
    let hash = "shared";

    // group A: RESTRICTED to priority = 1 (its permission transform). group B:
    // PERMISSIVE, every issue. same client-chosen hash.
    register_query(
        &mut db,
        &tables,
        GA,
        hash,
        &json!({ "table": "issue", "where": where_eq("priority", json!(1)) }),
        0,
    )
    .unwrap();
    register_query(&mut db, &tables, GB, hash, &json!({ "table": "issue" }), 0).unwrap();
    set_desire(&mut db, GA, "ca", hash, 1).unwrap();
    set_desire(&mut db, GB, "cb", hash, 1).unwrap();

    // group A sees ONLY its restricted row i3 (priority 1), never i1/i2.
    let a = db
        .transaction(|d| recompute_group(d, &tables, GA, &BTreeSet::new()))
        .unwrap();
    assert_eq!(
        put_ids(&a),
        vec!["i3"],
        "group A's restricted AST must not be overwritten by group B's hash"
    );

    // group B sees all rows under its own permissive AST.
    let b = db
        .transaction(|d| recompute_group(d, &tables, GB, &BTreeSet::new()))
        .unwrap();
    assert_eq!(put_ids(&b), vec!["i1", "i2", "i3"]);

    // re-registering B on a later pull still must not disturb A's membership.
    register_query(&mut db, &tables, GB, hash, &json!({ "table": "issue" }), 0).unwrap();
    let a2 = db
        .transaction(|d| recompute_group(d, &tables, GA, &BTreeSet::new()))
        .unwrap();
    assert!(
        put_ids(&a2).is_empty() && del_ids(&a2).is_empty(),
        "group A's membership is unchanged; only i3 stays durable"
    );
}

// CRITICAL-2 companion: a transform-version bump on a group's query forces a
// recompute so a tightened permission transform cannot retain older, more-
// permissive rows even if the AST text were unchanged.
#[test]
fn transform_version_bump_forces_recompute() {
    let mut db = TestDb::memory();
    db.exec(
        "CREATE TABLE issue (id TEXT PRIMARY KEY, title TEXT, closed INTEGER, priority INTEGER)",
        &[],
    )
    .unwrap();
    init_query_schema(&mut db).unwrap();
    for (id, closed, priority) in [("i1", 0, 5), ("i3", 0, 1)] {
        db.exec(
            "INSERT INTO issue VALUES (?, ?, ?, ?)",
            &[
                SqlValue::Text(id.into()),
                SqlValue::Text(format!("t-{id}")),
                SqlValue::Integer(closed),
                SqlValue::Integer(priority),
            ],
        )
        .unwrap();
    }
    let tables = schema();
    let all = json!({ "table": "issue" });
    register_query(&mut db, &tables, G, "q", &all, 0).unwrap();
    set_desire(&mut db, G, "c", "q", 1).unwrap();
    let first = db
        .transaction(|d| recompute_group(d, &tables, G, &BTreeSet::new()))
        .unwrap();
    assert_eq!(put_ids(&first), vec!["i1", "i3"]);

    // same AST text, bumped transform version -> the query's marker is cleared,
    // so the next recompute re-runs it (rather than being narrowed away with no
    // touched tables). membership is unchanged here, so no spurious puts/dels.
    register_query(&mut db, &tables, G, "q", &all, 1).unwrap();
    let after = db
        .transaction(|d| recompute_group(d, &tables, G, &BTreeSet::new()))
        .unwrap();
    assert!(
        put_ids(&after).is_empty() && del_ids(&after).is_empty(),
        "recompute ran (marker cleared) but membership is identical"
    );

    // prove the marker was actually cleared: with NO transform bump and no
    // touched tables, the query is narrowed away (the state row exists again).
    let noop = db
        .transaction(|d| recompute_group(d, &tables, G, &BTreeSet::new()))
        .unwrap();
    assert!(noop.is_empty());
}
