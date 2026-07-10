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
        register_query(&mut self.db, &self.tables, hash, &ast).unwrap();
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
