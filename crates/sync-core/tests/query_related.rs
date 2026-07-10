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
        .with(
            "reaction",
            TableSpec {
                columns: vec![
                    ("id".into(), String),
                    ("commentId".into(), String),
                    ("emoji".into(), String),
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
        db.exec(
            "CREATE TABLE reaction (id TEXT PRIMARY KEY, commentId TEXT, emoji TEXT)",
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
        // reactions: r1 on c1 (i1's comment), r2 on c3 (i2's comment)
        db.exec(
            "INSERT INTO reaction VALUES ('r1','c1','+1'), ('r2','c3','heart')",
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
    register_query(&mut h.db, &schema(), G, "q", &open_with_comments(), 0).unwrap();
    set_desire(&mut h.db, G, "cl", "q", 1).unwrap();

    // i1 is open -> issue:i1 plus its comments c1, c2. i2 closed -> excluded,
    // and c3 (i2's comment) is NOT included.
    let patch = h.recompute(&[]);
    assert_eq!(puts(&patch), vec!["comment:c1", "comment:c2", "issue:i1"]);
}

#[test]
fn adding_a_child_to_an_included_parent_flows_through() {
    let mut h = Host::new();
    register_query(&mut h.db, &schema(), G, "q", &open_with_comments(), 0).unwrap();
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
    register_query(&mut h.db, &schema(), G, "q", &open_with_comments(), 0).unwrap();
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
    register_query(&mut h.db, &schema(), G, "q", &open_with_comments(), 0).unwrap();
    set_desire(&mut h.db, G, "cl", "q", 1).unwrap();
    h.recompute(&[]);

    // reopen i2 -> i2 enters, and its existing comment c3 is pulled in with it
    h.exec("UPDATE issue SET closed = 0 WHERE id = 'i2'");
    let patch = h.recompute(&[("issue", "i2")]);
    assert_eq!(puts(&patch), vec!["comment:c3", "issue:i2"]);
}

// a whereExists FILTER: issues that HAVE a comment with body 'a'. the stock
// Zero client re-runs this query against its synced rows, so the engine must
// sync BOTH the matching issue rows AND the comment rows that satisfy the
// EXISTS, or the client's local EXISTS is false and the result collapses.
#[test]
fn exists_filter_syncs_subquery_rows_for_local_reevaluation() {
    let mut h = Host::new();
    let q = json!({ "table": "issue", "where": {
        "type": "correlatedSubquery", "op": "EXISTS",
        "related": {
            "correlation": { "parentField": ["id"], "childField": ["issueId"] },
            "subquery": { "table": "comment", "where": {
                "type": "simple", "op": "=", "left": { "type": "column", "name": "body" },
                "right": { "type": "literal", "value": "a" } } }
        }
    } });
    register_query(&mut h.db, &schema(), G, "q", &q, 0).unwrap();
    set_desire(&mut h.db, G, "cl", "q", 1).unwrap();
    // i1 has c1 (body 'a') so it matches; the response must carry issue:i1 AND
    // comment:c1 (the EXISTS witness the client needs to re-evaluate the filter).
    let patch = h.recompute(&[]);
    assert_eq!(puts(&patch), vec!["comment:c1", "issue:i1"]);

    // when the witness comment's body changes so it no longer matches, the issue
    // leaves and the comment is no longer synced
    h.exec("UPDATE comment SET body = 'z' WHERE id = 'c1'");
    let patch = h.recompute(&[("comment", "c1")]);
    assert_eq!(dels(&patch), vec!["comment:c1", "issue:i1"]);
}

// open issues -> comments -> reactions (nested related-of-related, the shape
// Chat's queryMessageItemRelations uses several levels deep)
fn open_comments_reactions() -> Value {
    json!({
        "table": "issue",
        "where": { "type": "simple", "op": "=", "left": { "type": "column", "name": "closed" },
                   "right": { "type": "literal", "value": false } },
        "related": [{
            "correlation": { "parentField": ["id"], "childField": ["issueId"] },
            "subquery": {
                "table": "comment",
                "related": [{
                    "correlation": { "parentField": ["id"], "childField": ["commentId"] },
                    "subquery": { "table": "reaction" }
                }]
            }
        }]
    })
}

#[test]
fn nested_related_of_related_includes_grandchildren() {
    let mut h = Host::new();
    register_query(&mut h.db, &schema(), G, "q", &open_comments_reactions(), 0).unwrap();
    set_desire(&mut h.db, G, "cl", "q", 1).unwrap();

    // i1 open -> its comments c1,c2 -> c1's reaction r1. i2 closed excludes its
    // comment c3 and grandchild reaction r2.
    let patch = h.recompute(&[]);
    assert_eq!(
        puts(&patch),
        vec!["comment:c1", "comment:c2", "issue:i1", "reaction:r1"]
    );

    // a reaction added to an included comment flows through (grandchild-table
    // change, parents untouched)
    h.exec("INSERT INTO reaction VALUES ('r3','c2','tada')");
    let patch = h.recompute(&[("reaction", "r3")]);
    assert_eq!(puts(&patch), vec!["reaction:r3"]);

    // closing i1 pulls out the whole subtree: issue, its comments, and their
    // reactions (grandchildren follow even though only the issue table changed)
    h.exec("UPDATE issue SET closed = 1 WHERE id = 'i1'");
    let patch = h.recompute(&[("issue", "i1")]);
    assert_eq!(
        dels(&patch),
        vec![
            "comment:c1",
            "comment:c2",
            "issue:i1",
            "reaction:r1",
            "reaction:r3"
        ]
    );
}

// GAP-2: a related child with a per-parent orderBy+limit is windowed PER PARENT,
// not widened to every child. `issue related comment (orderBy id desc, limit 1)`
// -> each issue keeps only its single newest comment.
fn issues_with_newest_comment() -> Value {
    json!({
        "table": "issue",
        "related": [{
            "correlation": { "parentField": ["id"], "childField": ["issueId"] },
            "subquery": { "table": "comment", "orderBy": [["id", "desc"]], "limit": 1 }
        }]
    })
}

#[test]
fn windowed_related_output_keeps_top_n_per_parent() {
    let mut h = Host::new();
    register_query(
        &mut h.db,
        &schema(),
        G,
        "q",
        &issues_with_newest_comment(),
        0,
    )
    .unwrap();
    set_desire(&mut h.db, G, "cl", "q", 1).unwrap();
    // i1 has c1,c2 -> top-1 by id desc = c2 (c1 is NOT synced); i2 has only c3.
    assert_eq!(
        puts(&h.recompute(&[])),
        vec!["comment:c2", "comment:c3", "issue:i1", "issue:i2"]
    );
}

#[test]
fn windowed_child_rank_alias_avoids_column_collision() {
    // GAP-2c: a child column named like the ROW_NUMBER rank alias must not shadow
    // it. here the child carries both the legacy `_zrn` and the current `_zsync_rn`
    // as real columns (value 99); a top-1 window must still return the top row, not
    // zero rows (the collision made `_zrn <= 1` compare the app column, 99 <= 1).
    use sync_core::value::ZeroColumnType::{Number, String as S};
    let mut db = TestDb::memory();
    db.exec("CREATE TABLE parent (id TEXT PRIMARY KEY)", &[])
        .unwrap();
    db.exec(
        "CREATE TABLE item (id TEXT PRIMARY KEY, parentId TEXT, rank INTEGER, _zrn INTEGER, _zsync_rn INTEGER)",
        &[],
    )
    .unwrap();
    db.exec("INSERT INTO parent VALUES ('p')", &[]).unwrap();
    db.exec(
        "INSERT INTO item VALUES ('i1','p',1,99,99), ('i2','p',2,99,99)",
        &[],
    )
    .unwrap();
    let tables = Tables::new()
        .with(
            "parent",
            TableSpec {
                columns: vec![("id".into(), S)],
                primary_key: vec!["id".into()],
            },
        )
        .with(
            "item",
            TableSpec {
                columns: vec![
                    ("id".into(), S),
                    ("parentId".into(), S),
                    ("rank".into(), Number),
                    ("_zrn".into(), Number),
                    ("_zsync_rn".into(), Number),
                ],
                primary_key: vec!["id".into()],
            },
        );
    init_schema(&mut db, &tables).unwrap();
    init_query_schema(&mut db).unwrap();

    let q = json!({ "table": "parent", "related": [{
        "correlation": { "parentField": ["id"], "childField": ["parentId"] },
        "subquery": { "table": "item", "orderBy": [["rank", "desc"]], "limit": 1 } }] });
    register_query(&mut db, &tables, G, "q", &q, 0).unwrap();
    set_desire(&mut db, G, "cl", "q", 1).unwrap();
    let patch = db
        .transaction(|d| recompute_group(d, &tables, G, &BTreeSet::new()))
        .unwrap();
    assert_eq!(puts(&patch), vec!["item:i2", "parent:p"]);
}

#[test]
fn windowed_child_rank_alias_avoids_case_insensitive_collision() {
    // RESIDUAL-2c: SQLite identifiers are ASCII case-insensitive, so a child column
    // `_ZSYNC_RN` collides with the `_zsync_rn` rank alias. the absence check must
    // fold case; otherwise the top-1 window returns zero rows.
    use sync_core::value::ZeroColumnType::{Number, String as S};
    let mut db = TestDb::memory();
    db.exec("CREATE TABLE parent (id TEXT PRIMARY KEY)", &[])
        .unwrap();
    db.exec(
        "CREATE TABLE item (id TEXT PRIMARY KEY, parentId TEXT, rank INTEGER, _ZSYNC_RN INTEGER)",
        &[],
    )
    .unwrap();
    db.exec("INSERT INTO parent VALUES ('p')", &[]).unwrap();
    db.exec(
        "INSERT INTO item VALUES ('i1','p',1,99), ('i2','p',2,99)",
        &[],
    )
    .unwrap();
    let tables = Tables::new()
        .with(
            "parent",
            TableSpec {
                columns: vec![("id".into(), S)],
                primary_key: vec!["id".into()],
            },
        )
        .with(
            "item",
            TableSpec {
                columns: vec![
                    ("id".into(), S),
                    ("parentId".into(), S),
                    ("rank".into(), Number),
                    ("_ZSYNC_RN".into(), Number),
                ],
                primary_key: vec!["id".into()],
            },
        );
    init_schema(&mut db, &tables).unwrap();
    init_query_schema(&mut db).unwrap();

    let q = json!({ "table": "parent", "related": [{
        "correlation": { "parentField": ["id"], "childField": ["parentId"] },
        "subquery": { "table": "item", "orderBy": [["rank", "desc"]], "limit": 1 } }] });
    register_query(&mut db, &tables, G, "q", &q, 0).unwrap();
    set_desire(&mut db, G, "cl", "q", 1).unwrap();
    let patch = db
        .transaction(|d| recompute_group(d, &tables, G, &BTreeSet::new()))
        .unwrap();
    assert_eq!(puts(&patch), vec!["item:i2", "parent:p"]);
}

#[test]
fn windowed_related_output_shifts_incrementally() {
    let mut h = Host::new();
    register_query(
        &mut h.db,
        &schema(),
        G,
        "q",
        &issues_with_newest_comment(),
        0,
    )
    .unwrap();
    set_desire(&mut h.db, G, "cl", "q", 1).unwrap();
    assert_eq!(
        puts(&h.recompute(&[])),
        vec!["comment:c2", "comment:c3", "issue:i1", "issue:i2"]
    );

    // a newer comment on i1 shifts its window: c4 enters, the old top (c2) leaves.
    // the incremental recompute must equal a fresh evaluation.
    h.exec("INSERT INTO comment VALUES ('c4','i1','d')");
    let patch = h.recompute(&[("comment", "c4")]);
    assert_eq!(puts(&patch), vec!["comment:c4"]);
    assert_eq!(dels(&patch), vec!["comment:c2"]);
}
