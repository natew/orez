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
use sync_core::{SqlValue, SyncDb, Tables, Transactor, init_schema, settle_delegated_push};

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
            encrypted_columns: Default::default(),
            encrypted_physical_columns: Default::default(),
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
fn settled_update_migrating_between_queries_is_reemitted_without_snapshot() {
    let mut h = QHost::new();
    let closed_query = json!({ "table": "issue", "where": {
        "type": "simple", "op": "=",
        "left": { "type": "column", "name": "closed" },
        "right": { "type": "literal", "value": true }
    } });
    let moved_query = json!({ "table": "issue", "where": {
        "type": "simple", "op": "=",
        "left": { "type": "column", "name": "title" },
        "right": { "type": "literal", "value": "moved" }
    } });
    let queries = json!({
        "version": 1,
        "patch": [
            { "op": "put", "hash": "open", "ast": open_query() },
            { "op": "put", "hash": "closed", "ast": closed_query },
            { "op": "put", "hash": "moved", "ast": moved_query },
        ]
    });
    let first = h.pull("client-b", json!(null), Some(queries));

    // i1 leaves `open` and enters both `closed` and `moved`: its group
    // refcount stays positive while its net reference delta is nonzero.
    h.exec("UPDATE issue SET title = 'moved', closed = 1 WHERE id = 'i1'");
    h.exec("INSERT INTO issue VALUES ('i4', 'new', 0)");
    let push = json!({
        "clientGroupID": "g1",
        "mutations": [{
            "type": "custom",
            "id": 3,
            "clientID": "client-a",
            "name": "issue.update",
            "args": [{ "id": "i1" }],
            "timestamp": 0,
        }],
        "pushVersion": 1,
    });
    let response = json!({
        "pushResponse": { "mutations": [{
            "id": { "clientID": "client-a", "id": 3 },
            "result": {},
        }] }
    });
    h.db.transaction(|db| settle_delegated_push(db, &push, &response, "u1"))
        .unwrap();

    let next = h.pull("client-b", first["cookie"].clone(), None);
    assert_eq!(next["lastMutationIDChanges"]["client-a"], json!(3));
    assert_eq!(put_ids(&next), vec!["i1", "i4"]);
    let updated = next["rowsPatch"]
        .as_array()
        .unwrap()
        .iter()
        .find(|op| op["op"] == "put" && op["value"]["id"] == "i1")
        .expect("the incremental patch must carry the updated existing row");
    assert_eq!(updated["value"]["title"], json!("moved"));
    assert_eq!(updated["value"]["closed"], json!(true));
}

#[test]
fn membership_flips_emit_exactly_one_operation_per_changed_row() {
    let mut h = QHost::new();
    let queries =
        json!({ "version": 1, "patch": [{ "op": "put", "hash": "open", "ast": open_query() }] });
    let first = h.pull("c1", json!(null), Some(queries));

    h.exec("UPDATE issue SET closed = 1 WHERE id = 'i1'");
    h.exec("INSERT INTO issue VALUES ('i4', 'new', 0)");
    let next = h.pull("c1", first["cookie"].clone(), None);

    assert_eq!(put_ids(&next), vec!["i4"]);
    assert_eq!(del_ids(&next), vec!["i1"]);
    assert_eq!(next["rowsPatch"].as_array().unwrap().len(), 2);
}

#[test]
fn changed_row_with_stable_membership_reemits_current_content() {
    let mut h = QHost::new();
    let queries =
        json!({ "version": 1, "patch": [{ "op": "put", "hash": "open", "ast": open_query() }] });
    let first = h.pull("c1", json!(null), Some(queries));

    h.exec("UPDATE issue SET title = 'renamed' WHERE id = 'i1'");
    let next = h.pull("c1", first["cookie"].clone(), None);

    assert_eq!(put_ids(&next), vec!["i1"]);
    assert!(del_ids(&next).is_empty());
    assert_eq!(next["rowsPatch"].as_array().unwrap().len(), 1);
    assert_eq!(next["rowsPatch"][0]["value"]["title"], json!("renamed"));
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
fn newly_desiring_client_rehydrates_existing_group_query() {
    let mut h = QHost::new();
    let query = || json!({ "version": 1, "patch": [{ "op": "put", "hash": "q_open", "ast": open_query() }] });
    let first = h.pull("c1", json!(null), Some(query()));
    assert_eq!(put_ids(&first), vec!["i1", "i3"]);

    // a restarted client keeps the group cookie but has a new client id and may
    // have evicted ttl=0 rows from its local store. the group membership is
    // already computed, so the new desire must re-send the members without a
    // clear or a refcount transition.
    let restarted = h.pull("c2", first["cookie"].clone(), Some(query()));
    assert!(!has_clear(&restarted));
    assert_eq!(put_ids(&restarted), vec!["i1", "i3"]);

    // replaying the same put for the same client is only a version update and
    // does not resend the full result repeatedly.
    let replay = h.pull(
        "c2",
        restarted["cookie"].clone(),
        Some(json!({ "version": 2, "patch": [{ "op": "put", "hash": "q_open", "ast": open_query() }] })),
    );
    assert!(put_ids(&replay).is_empty());
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

// reproduces the differential's `allProjects` (project.related('members'),
// unbounded) end to end through handle_query_pull: a fresh pull must return the
// project rows AND their related member rows (clear + puts), and the caught-up
// follow-up pull is unchanged.
#[test]
fn unbounded_related_fresh_pull_includes_child_rows() {
    use ZeroColumnType::String as S;
    let mut db = TestDb::memory();
    db.exec("CREATE TABLE project (id TEXT PRIMARY KEY, name TEXT)", &[])
        .unwrap();
    db.exec(
        "CREATE TABLE member (id TEXT PRIMARY KEY, projectId TEXT, userId TEXT)",
        &[],
    )
    .unwrap();
    db.exec("INSERT INTO project VALUES ('p0','P0'), ('p1','P1')", &[])
        .unwrap();
    db.exec(
        "INSERT INTO member VALUES ('m0','p0','u0'), ('m1','p0','u1'), ('m2','p1','u2')",
        &[],
    )
    .unwrap();
    let tables = Tables::new()
        .with(
            "project",
            TableSpec {
                columns: vec![("id".into(), S), ("name".into(), S)],
                primary_key: vec!["id".into()],
                encrypted_columns: Default::default(),
                encrypted_physical_columns: Default::default(),
            },
        )
        .with(
            "member",
            TableSpec {
                columns: vec![
                    ("id".into(), S),
                    ("projectId".into(), S),
                    ("userId".into(), S),
                ],
                primary_key: vec!["id".into()],
                encrypted_columns: Default::default(),
                encrypted_physical_columns: Default::default(),
            },
        );
    init_schema(&mut db, &tables).unwrap();
    init_query_schema(&mut db).unwrap();

    let all_projects = json!({ "table": "project", "related": [{
        "correlation": { "parentField": ["id"], "childField": ["projectId"] },
        "subquery": { "table": "member" } }] });
    let body = json!({ "clientID": "c", "clientGroupID": "g", "cookie": null,
        "queries": { "version": 1, "patch": [{ "op": "put", "hash": "allProjects", "ast": all_projects }] } });
    let resp = db
        .transaction(|d| handle_query_pull(d, &tables, 4096, &body, "u"))
        .unwrap();

    let rows = resp["rowsPatch"].as_array().unwrap();
    let by_table = |t: &str| -> Vec<String> {
        let mut v: Vec<String> = rows
            .iter()
            .filter(|op| op["op"] == "put" && op["tableName"] == t)
            .map(|op| op["value"]["id"].as_str().unwrap().to_string())
            .collect();
        v.sort();
        v
    };
    assert_eq!(by_table("project"), vec!["p0", "p1"]);
    assert_eq!(
        by_table("member"),
        vec!["m0", "m1", "m2"],
        "fresh pull must sync the related member rows"
    );
    assert_eq!(resp["gotQueries"]["version"], json!(1));
}
