// M4b related-output subqueries: a query's `related` child rows are part of its
// result. child rows follow their parents — a parent entering/leaving the query
// pulls its children in/out even when the child table itself was not touched,
// and a child-table change to an included parent's children flows through.
mod common;

use std::collections::BTreeSet;

use common::TestDb;
use serde_json::{Value, json};

use sync_core::query::{init_query_schema, recompute_group, register_query, set_desire};
use sync_core::schema::TableSpec;
use sync_core::value::ZeroColumnType;
use sync_core::{SyncDb, Tables, Transactor, init_schema};

const G: &str = "g1";

fn schema() -> Tables {
    use ZeroColumnType::*;
    Tables::new()
        .with(
            "issue",
            TableSpec {
                columns: vec![("id".into(), String), ("closed".into(), Boolean)],
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

struct Host {
    db: TestDb,
    tables: Tables,
}

impl Host {
    fn new() -> Host {
        let mut db = TestDb::memory();
        db.exec(
            "CREATE TABLE issue (id TEXT PRIMARY KEY, closed INTEGER)",
            &[],
        )
        .unwrap();
        db.exec(
            "CREATE TABLE comment (id TEXT PRIMARY KEY, issueId TEXT, body TEXT)",
            &[],
        )
        .unwrap();
        // i1 open (with c1, c2), i2 closed (with c3)
        db.exec("INSERT INTO issue VALUES ('i1', 0), ('i2', 1)", &[])
            .unwrap();
        db.exec(
            "INSERT INTO comment VALUES ('c1','i1','a'), ('c2','i1','b'), ('c3','i2','c')",
            &[],
        )
        .unwrap();
        let tables = schema();
        init_schema(&mut db, &tables).unwrap();
        init_query_schema(&mut db).unwrap();
        Host { db, tables }
    }

    fn exec(&mut self, sql: &str) {
        self.db.exec(sql, &[]).unwrap();
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

// members of a patch as "table:id" strings
fn puts(patch: &[Value]) -> Vec<String> {
    let mut v: Vec<String> = patch
        .iter()
        .filter(|op| op["op"] == "put")
        .map(|op| {
            format!(
                "{}:{}",
                op["tableName"].as_str().unwrap(),
                op["value"]["id"].as_str().unwrap()
            )
        })
        .collect();
    v.sort();
    v
}
fn dels(patch: &[Value]) -> Vec<String> {
    let mut v: Vec<String> = patch
        .iter()
        .filter(|op| op["op"] == "del")
        .map(|op| {
            format!(
                "{}:{}",
                op["tableName"].as_str().unwrap(),
                op["id"]["id"].as_str().unwrap()
            )
        })
        .collect();
    v.sort();
    v
}

// open issues with their comments (related output)
fn open_with_comments() -> Value {
    json!({
        "table": "issue",
        "where": { "type": "simple", "op": "=", "left": { "type": "column", "name": "closed" },
                   "right": { "type": "literal", "value": false } },
        "related": [{
            "correlation": { "parentField": ["id"], "childField": ["issueId"] },
            "subquery": { "table": "comment" }
        }]
    })
}

#[test]
fn related_output_includes_child_rows() {
    let mut h = Host::new();
    register_query(&mut h.db, &schema(), "q", &open_with_comments()).unwrap();
    set_desire(&mut h.db, G, "cl", "q", 1).unwrap();

    // i1 is open -> issue:i1 plus its comments c1, c2. i2 closed -> excluded,
    // and c3 (i2's comment) is NOT included.
    let patch = h.recompute(&[]);
    assert_eq!(puts(&patch), vec!["comment:c1", "comment:c2", "issue:i1"]);
}

#[test]
fn adding_a_child_to_an_included_parent_flows_through() {
    let mut h = Host::new();
    register_query(&mut h.db, &schema(), "q", &open_with_comments()).unwrap();
    set_desire(&mut h.db, G, "cl", "q", 1).unwrap();
    h.recompute(&[]);

    // a new comment on the open issue i1 -> it appears (child-table change, the
    // parent issue itself was untouched)
    h.exec("INSERT INTO comment VALUES ('c4', 'i1', 'd')");
    let patch = h.recompute(&[("comment", "c4")]);
    assert_eq!(puts(&patch), vec!["comment:c4"]);
    assert!(dels(&patch).is_empty());
}

#[test]
fn a_parent_leaving_pulls_its_children_out() {
    let mut h = Host::new();
    register_query(&mut h.db, &schema(), "q", &open_with_comments()).unwrap();
    set_desire(&mut h.db, G, "cl", "q", 1).unwrap();
    assert_eq!(
        puts(&h.recompute(&[])),
        vec!["comment:c1", "comment:c2", "issue:i1"]
    );

    // close i1 -> the issue AND its comments leave, even though the comment table
    // was not touched (the parent membership change pulls the children out)
    h.exec("UPDATE issue SET closed = 1 WHERE id = 'i1'");
    let patch = h.recompute(&[("issue", "i1")]);
    assert_eq!(dels(&patch), vec!["comment:c1", "comment:c2", "issue:i1"]);
    assert!(puts(&patch).is_empty());
}

#[test]
fn a_new_parent_brings_its_children() {
    let mut h = Host::new();
    register_query(&mut h.db, &schema(), "q", &open_with_comments()).unwrap();
    set_desire(&mut h.db, G, "cl", "q", 1).unwrap();
    h.recompute(&[]);

    // reopen i2 -> i2 enters, and its existing comment c3 is pulled in with it
    h.exec("UPDATE issue SET closed = 0 WHERE id = 'i2'");
    let patch = h.recompute(&[("issue", "i2")]);
    assert_eq!(puts(&patch), vec!["comment:c3", "issue:i2"]);
}
