// M4b slice 3: the query-aware pull entry point end to end. drives
// handle_query_pull with the wire body ({queries:{version,patch}}) and asserts
// the response ({cookie, lastMutationIDChanges, rowsPatch, gotQueries}): the
// desired-query lifecycle, membership-driven puts/dels, fresh-client full
// re-sync, the unchanged fast path, and that the query ack never leads its row
// effects (invariant 13).
mod common;

use common::TestDb;
use serde_json::{Value, json};

use sync_core::query::{handle_query_pull, init_query_schema};
use sync_core::schema::TableSpec;
use sync_core::value::ZeroColumnType;
use sync_core::{SqlValue, SyncDb, Tables, Transactor, init_schema};

fn schema() -> Tables {
    use ZeroColumnType::*;
    Tables::new().with(
        "issue",
        TableSpec {
            columns: vec![
                ("id".into(), String),
                ("title".into(), String),
                ("closed".into(), Boolean),
            ],
            primary_key: vec!["id".into()],
        },
    )
}

struct QHost {
    db: TestDb,
    tables: Tables,
}

impl QHost {
    fn new() -> QHost {
        let mut db = TestDb::memory();
        db.exec(
            "CREATE TABLE issue (id TEXT PRIMARY KEY, title TEXT, closed INTEGER)",
            &[],
        )
        .unwrap();
        for (id, closed) in [("i1", 0), ("i2", 1), ("i3", 0)] {
            db.exec(
                "INSERT INTO issue VALUES (?, ?, ?)",
                &[
                    SqlValue::Text(id.into()),
                    SqlValue::Text(format!("t-{id}")),
                    SqlValue::Integer(closed),
                ],
            )
            .unwrap();
        }
        let tables = schema();
        init_schema(&mut db, &tables).unwrap(); // M1 change-log triggers
        init_query_schema(&mut db).unwrap();
        QHost { db, tables }
    }

    fn exec(&mut self, sql: &str) {
        self.db.exec(sql, &[]).unwrap();
    }

    fn pull(&mut self, client: &str, cookie: Value, queries: Option<Value>) -> Value {
        let mut body = json!({ "clientID": client, "clientGroupID": "g1", "cookie": cookie });
        if let Some(q) = queries {
            body["queries"] = q;
        }
        let tables = self.tables.clone();
        self.db
            .transaction(|db| handle_query_pull(db, &tables, 4096, &body, "u1"))
            .unwrap()
    }
}

fn open_query() -> Value {
    json!({ "table": "issue", "where": {
        "type": "simple", "op": "=",
        "left": { "type": "column", "name": "closed" },
        "right": { "type": "literal", "value": false }
    } })
}

fn put_ids(resp: &Value) -> Vec<String> {
    let mut ids: Vec<String> = resp["rowsPatch"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|op| op["op"] == "put")
        .map(|op| op["value"]["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    ids
}
fn del_ids(resp: &Value) -> Vec<String> {
    let mut ids: Vec<String> = resp["rowsPatch"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|op| op["op"] == "del")
        .map(|op| op["id"]["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    ids
}
fn has_clear(resp: &Value) -> bool {
    resp["rowsPatch"].as_array().unwrap().first() == Some(&json!({ "op": "clear" }))
}

#[test]
fn desire_pull_then_data_change_flows_as_membership_delta() {
    let mut h = QHost::new();
    // first pull desires q_open (put with the transformed AST) at version 1
    let queries =
        json!({ "version": 1, "patch": [{ "op": "put", "hash": "q_open", "ast": open_query() }] });
    let r1 = h.pull("c1", json!(null), Some(queries));
    // fresh client: clear + puts for the open issues (i1, i3)
    assert!(has_clear(&r1));
    assert_eq!(put_ids(&r1), vec!["i1", "i3"]);
    // gotQueries acks the desired query at version 1
    assert_eq!(
        r1["gotQueries"],
        json!({ "version": 1, "patch": [{ "op": "put", "hash": "q_open" }] })
    );
    let c1 = r1["cookie"].clone();

    // no change -> unchanged fast path
    let r_same = h.pull("c1", c1.clone(), None);
    assert_eq!(r_same, json!({ "cookie": c1, "unchanged": true }));

    // close i1 (a change-logged write) -> next pull dels i1
    h.exec("UPDATE issue SET closed = 1 WHERE id = 'i1'");
    let r2 = h.pull("c1", c1, None);
    assert!(!has_clear(&r2));
    assert_eq!(del_ids(&r2), vec!["i1"]);
    assert!(put_ids(&r2).is_empty());
    let c2 = r2["cookie"].clone();

    // reopen i3-as-new: insert a new open issue -> it appears
    h.exec("INSERT INTO issue VALUES ('i4', 't-i4', 0)");
    let r3 = h.pull("c1", c2, None);
    assert_eq!(put_ids(&r3), vec!["i4"]);
}

#[test]
fn fresh_client_full_resync_clears_and_resends() {
    let mut h = QHost::new();
    let queries =
        json!({ "version": 1, "patch": [{ "op": "put", "hash": "q_open", "ast": open_query() }] });
    let r1 = h.pull("c1", json!(null), Some(queries));
    assert_eq!(put_ids(&r1), vec!["i1", "i3"]);

    // a brand-new client in the same group with a null cookie is re-synced from
    // scratch: clear + all current members again
    let r2 = h.pull("c1", json!(null), None);
    assert!(has_clear(&r2));
    assert_eq!(put_ids(&r2), vec!["i1", "i3"]);
    // and it still acks the group's desired query
    assert_eq!(
        r2["gotQueries"]["patch"],
        json!([{ "op": "put", "hash": "q_open" }])
    );
}

#[test]
fn deleting_a_desired_query_removes_its_rows() {
    let mut h = QHost::new();
    let put =
        json!({ "version": 1, "patch": [{ "op": "put", "hash": "q_open", "ast": open_query() }] });
    let r1 = h.pull("c1", json!(null), Some(put));
    assert_eq!(put_ids(&r1), vec!["i1", "i3"]);
    let c1 = r1["cookie"].clone();

    // delete the desired query -> its rows leave, gotQueries empties
    let del = json!({ "version": 2, "patch": [{ "op": "del", "hash": "q_open" }] });
    let r2 = h.pull("c1", c1, Some(del));
    assert_eq!(del_ids(&r2), vec!["i1", "i3"]);
    assert_eq!(r2["gotQueries"], json!({ "version": 2, "patch": [] }));
}

#[test]
fn gotqueries_version_never_regresses_after_del_or_clear() {
    // MEDIUM-6: only `put` recorded a version, so after a higher-versioned
    // del/clear the ack fell back to the max over remaining desires (a lower
    // number), churning the client (replayed gotQueries). the acked version must
    // be monotonic per client, independent of which desires remain.
    let mut h = QHost::new();
    // put A@1, then put B@2 -> two desires at increasing versions
    let r1 = h.pull(
        "c",
        json!(0),
        Some(json!({ "version": 1, "patch": [{ "op": "put", "hash": "A", "ast": open_query() }] })),
    );
    assert_eq!(r1["gotQueries"]["version"], json!(1));
    let r2 = h.pull(
        "c",
        r1["cookie"].clone(),
        Some(json!({ "version": 2, "patch": [{ "op": "put", "hash": "B", "ast": open_query() }] })),
    );
    assert_eq!(r2["gotQueries"]["version"], json!(2));

    // del B at version 3 -> ack 3
    let r3 = h.pull(
        "c",
        r2["cookie"].clone(),
        Some(json!({ "version": 3, "patch": [{ "op": "del", "hash": "B" }] })),
    );
    assert_eq!(r3["gotQueries"]["version"], json!(3));

    // a data change makes the next pull non-caught-up; a pull WITHOUT a queries
    // patch must still ack 3, not the max over the remaining desire A@1.
    h.exec("INSERT INTO issue VALUES ('i4', 't-i4', 0)");
    let r4 = h.pull("c", r3["cookie"].clone(), None);
    assert_eq!(
        r4["gotQueries"]["version"],
        json!(3),
        "ack regressed after del"
    );

    // clear at version 4 -> ack 4 even though no desire remains
    let r5 = h.pull(
        "c",
        r4["cookie"].clone(),
        Some(json!({ "version": 4, "patch": [{ "op": "clear" }] })),
    );
    assert_eq!(r5["gotQueries"], json!({ "version": 4, "patch": [] }));

    // another non-caught-up pull without queries still acks 4, not 0
    h.exec("INSERT INTO issue VALUES ('i5', 't-i5', 0)");
    let r6 = h.pull("c", r5["cookie"].clone(), None);
    assert_eq!(
        r6["gotQueries"]["version"],
        json!(4),
        "ack regressed after clear"
    );
}

#[test]
fn query_ack_never_leads_row_effects() {
    // invariant 13: the gotQueries version is acknowledged in the SAME response
    // (same transaction) that carries the query's row effects, so a client can
    // never see its query acknowledged before the rows arrive.
    let mut h = QHost::new();
    let put =
        json!({ "version": 5, "patch": [{ "op": "put", "hash": "q_open", "ast": open_query() }] });
    let r = h.pull("c1", json!(null), Some(put));
    // the ack (version 5) and the row effects (puts for i1,i3) are in one response
    assert_eq!(r["gotQueries"]["version"], json!(5));
    assert_eq!(put_ids(&r), vec!["i1", "i3"]);
    assert!(!r["rowsPatch"].as_array().unwrap().is_empty());
}

#[test]
fn future_cookie_is_409() {
    let mut h = QHost::new();
    let err = {
        let body = json!({ "clientID": "c1", "clientGroupID": "g1", "cookie": 999 });
        let tables = h.tables.clone();
        h.db.transaction(|db| handle_query_pull(db, &tables, 4096, &body, "u1"))
            .unwrap_err()
    };
    assert_eq!(err.status, 409);
}
