// soot's production composition semantics (httpPullProject.test.ts, 13 tests)
// ported to the engine's dedicated-log model: byte/row caps with a
// last-included-watermark cookie cut at a change-row boundary before pk dedup,
// log-derived prefix LMIDs (own group only), the explicit skip/throw table
// classifier, and row-local per-user visibility on diffs and snapshots.
//
// soot runs these against a transactionless pg/DO backend, so several of its
// tests pin ordering rules that a single host transaction gives for free here
// (a concurrent append is invisible to a pull's snapshot; a purge racing the
// read cannot happen mid-pull). those are ported as the equivalent invariant
// the transactional model still must satisfy, noted per test.
mod common;

use common::TestDb;
use serde_json::{json, Value};

use sync_core::pull::{Caps, VisibleFilter, Visibility};
use sync_core::schema::TableSpec;
use sync_core::value::ZeroColumnType;
use sync_core::{handle_pull, init_schema, SqlValue, SyncDb, Tables, Transactor};

const PROJECT: &str = "proj1";
const USER: &str = "u1";
const GROUP: &str = "g1";

fn col(name: &str, ty: ZeroColumnType) -> (String, ZeroColumnType) {
    (name.to_string(), ty)
}

fn soot_tables() -> Tables {
    use ZeroColumnType::*;
    Tables::new()
        .with(
            "file",
            TableSpec {
                columns: vec![
                    col("id", String),
                    col("projectId", String),
                    col("path", String),
                    col("size", Number),
                ],
                primary_key: vec!["id".into()],
            },
        )
        .with(
            "attachCommand",
            TableSpec {
                columns: vec![
                    col("id", String),
                    col("projectId", String),
                    col("userId", String),
                    col("command", String),
                ],
                primary_key: vec!["id".into()],
            },
        )
}

struct Soot {
    db: TestDb,
    tables: Tables,
    caps: Caps,
    retain: i64,
}

impl Soot {
    fn new() -> Soot {
        let mut db = TestDb::memory();
        db.exec(
            "CREATE TABLE file (id TEXT PRIMARY KEY, projectId TEXT, path TEXT, size REAL)",
            &[],
        )
        .unwrap();
        db.exec(
            "CREATE TABLE attachCommand (id TEXT PRIMARY KEY, projectId TEXT, userId TEXT, command TEXT)",
            &[],
        )
        .unwrap();
        let tables = soot_tables();
        init_schema(&mut db, &tables).unwrap();
        Soot { db, tables, caps: Caps::default(), retain: 4096 }
    }

    fn exec(&mut self, sql: &str) {
        self.db.exec(sql, &[]).unwrap();
    }

    fn pull(&mut self, cookie: Value) -> Result<Value, sync_core::EngineError> {
        self.pull_full("c1", GROUP, cookie, None, USER)
    }

    fn pull_full(
        &mut self,
        client: &str,
        group: &str,
        cookie: Value,
        visible: Option<&Visibility>,
        user: &str,
    ) -> Result<Value, sync_core::EngineError> {
        let body = json!({ "clientID": client, "clientGroupID": group, "cookie": cookie });
        let tables = self.tables.clone();
        let retain = self.retain;
        let caps = self.caps;
        self.db
            .transaction(|db| handle_pull(db, &tables, retain, visible, caps, &body, user))
    }

    fn watermark(&mut self) -> i64 {
        self.db.transaction(|db| sync_core::watermark(db)).unwrap()
    }

    // insert an lmid change row directly (models the clients table riding the
    // log in soot; here it is what finalize() writes)
    fn append_lmid(&mut self, group: &str, client: &str, lmid: i64) {
        self.db
            .exec(
                "INSERT INTO _zsync_changes (tableName, op, pk)
                 VALUES ('_zsync_clients', 'lmid',
                         json_object('clientGroupID', ?, 'clientID', ?, 'lmid', ?))",
                &[
                    SqlValue::Text(group.to_string()),
                    SqlValue::Text(client.to_string()),
                    SqlValue::Text(lmid.to_string()),
                ],
            )
            .unwrap();
    }
}

fn patch(resp: &Value) -> Vec<Value> {
    resp["rowsPatch"].as_array().cloned().unwrap_or_default()
}
fn puts(resp: &Value) -> Vec<Value> {
    patch(resp).into_iter().filter(|op| op["op"] == "put").collect()
}
fn dels(resp: &Value) -> Vec<Value> {
    patch(resp).into_iter().filter(|op| op["op"] == "del").collect()
}
fn cookie(resp: &Value) -> i64 {
    resp["cookie"].as_i64().unwrap()
}

// attachCommand own-row visibility (userId = the requesting user), row-local
fn attach_visibility<'a>() -> Visibility<'a> {
    Visibility {
        row_local: true,
        filter: Box::new(|table: &str, user: &str| {
            if table == "attachCommand" {
                Some(VisibleFilter {
                    sql: "\"userId\" = ?".into(),
                    params: vec![SqlValue::Text(user.to_string())],
                })
            } else {
                None
            }
        }),
    }
}

// ---- cursorPull semantics --------------------------------------------------

#[test]
fn fresh_client_snapshots_the_surface_with_live_values() {
    let mut s = Soot::new();
    s.exec(&format!("INSERT INTO file VALUES ('f1', '{PROJECT}', 'a.ts', 1)"));
    let res = s.pull(json!(null)).unwrap();
    assert_eq!(patch(&res)[0], json!({ "op": "clear" }));
    let p = puts(&res);
    assert_eq!(p.len(), 1);
    assert_eq!(p[0]["tableName"], "file");
    assert_eq!(p[0]["value"]["id"], "f1");
    assert_eq!(cookie(&res), s.watermark());
}

#[test]
fn diff_resolves_touched_pks_against_live_state_and_advances_cookie() {
    let mut s = Soot::new();
    s.exec(&format!("INSERT INTO file VALUES ('f0', '{PROJECT}', 'z.ts', 0)"));
    let c = cookie(&s.pull(json!(null)).unwrap());
    // f1 inserted then updated -> coalesces to one put with the LIVE row
    s.exec(&format!("INSERT INTO file VALUES ('f1', '{PROJECT}', 'a.ts', 1)"));
    s.exec("UPDATE file SET size = 2 WHERE id = 'f1'");
    // 'gone' inserted then deleted -> a del by its pk
    s.exec(&format!("INSERT INTO file VALUES ('gone', '{PROJECT}', 'g.ts', 9)"));
    s.exec("DELETE FROM file WHERE id = 'gone'");
    let res = s.pull(json!(c)).unwrap();
    assert_eq!(cookie(&res), s.watermark());
    let p = puts(&res);
    assert_eq!(p.len(), 1);
    // integral REAL serializes with JS parity: 2.0 -> "2" (not serde's "2.0")
    assert_eq!(p[0]["value"]["size"], json!(2));
    assert_eq!(dels(&res), vec![json!({ "op": "del", "tableName": "file", "id": { "id": "gone" } })]);
}

#[test]
fn last_mutation_id_changes_derive_from_the_included_prefix_own_group_only() {
    let mut s = Soot::new();
    let c = cookie(&s.pull(json!(null)).unwrap());
    s.append_lmid(GROUP, "c1", 7);
    s.append_lmid("other-group", "x1", 99);
    let res = s.pull(json!(c)).unwrap();
    assert_eq!(res["lastMutationIDChanges"], json!({ "c1": 7 }));
    assert_eq!(patch(&res), Vec::<Value>::new());
    assert_eq!(cookie(&res), s.watermark());
}

#[test]
fn unchanged_when_cookie_equals_watermark_409_when_ahead() {
    let mut s = Soot::new();
    s.exec(&format!("INSERT INTO file VALUES ('f1', '{PROJECT}', 'a.ts', 1)"));
    let wm = s.watermark();
    assert_eq!(s.pull(json!(wm)).unwrap(), json!({ "cookie": wm, "unchanged": true }));
    let err = s.pull(json!(wm + 5)).unwrap_err();
    assert_eq!(err.status, 409);
}

#[test]
fn a_pruned_gap_above_the_cookie_degrades_to_snapshot() {
    let mut s = Soot::new();
    s.retain = 2;
    s.exec(&format!("INSERT INTO file VALUES ('f0', '{PROJECT}', 'z.ts', 0)"));
    let ancient = cookie(&s.pull(json!(null)).unwrap());
    // churn past the retention window so `ancient` falls below the floor
    for i in 0..6 {
        s.exec(&format!("INSERT INTO file VALUES ('r{i}', '{PROJECT}', 'p{i}.ts', 1)"));
    }
    // a pull prunes; the ancient cookie is now below the floor -> snapshot
    let res = s.pull(json!(ancient)).unwrap();
    assert_eq!(patch(&res)[0], json!({ "op": "clear" }));
    assert!(cookie(&res) == s.watermark());
}

#[test]
fn byte_cap_cuts_at_a_change_row_boundary_and_returns_last_included_watermark() {
    let mut s = Soot::new();
    s.caps = Caps { max_change_rows: 10_000, max_change_bytes: 2_000 };
    let c = cookie(&s.pull(json!(null)).unwrap());
    // a big row (oversize resolved value) then a small row
    let big_path = "x".repeat(50_000);
    s.exec(&format!("INSERT INTO file VALUES ('big', '{PROJECT}', '{big_path}', 1)"));
    let big_wm = s.watermark();
    s.exec(&format!("INSERT INTO file VALUES ('small', '{PROJECT}', 's.ts', 1)"));
    let res = s.pull(json!(c)).unwrap();
    // one oversize row admitted, the rest deferred; cookie is the last included
    assert_eq!(cookie(&res), big_wm);
    assert_eq!(
        puts(&res).iter().map(|p| p["value"]["id"].clone()).collect::<Vec<_>>(),
        vec![json!("big")]
    );
    // the remainder ships on the next poll
    let res2 = s.pull(json!(cookie(&res))).unwrap();
    assert_eq!(cookie(&res2), s.watermark());
    assert_eq!(
        puts(&res2).iter().map(|p| p["value"]["id"].clone()).collect::<Vec<_>>(),
        vec![json!("small")]
    );
}

#[test]
fn returned_cookie_never_advances_past_an_excluded_change() {
    // soot's "an append during the pull never advances the returned cookie past
    // itself": a single host transaction makes a concurrent append invisible,
    // so the invariant is realized through the cap — the cookie is the last
    // INCLUDED watermark and a later change is never covered by it.
    let mut s = Soot::new();
    s.caps = Caps { max_change_rows: 1, max_change_bytes: 2_000_000 };
    let c = cookie(&s.pull(json!(null)).unwrap());
    s.exec(&format!("INSERT INTO file VALUES ('a', '{PROJECT}', 'a.ts', 1)"));
    let first_wm = s.watermark();
    s.exec(&format!("INSERT INTO file VALUES ('b', '{PROJECT}', 'b.ts', 1)"));
    let later_wm = s.watermark();
    let res = s.pull(json!(c)).unwrap();
    assert_eq!(cookie(&res), first_wm);
    assert!(cookie(&res) < later_wm);
    // the next poll picks the excluded change up
    let res2 = s.pull(json!(cookie(&res))).unwrap();
    let ids: Vec<Value> = puts(&res2).iter().map(|p| p["value"]["id"].clone()).collect();
    assert!(ids.contains(&json!("b")));
}

#[test]
fn a_cookie_below_the_floor_degrades_to_snapshot() {
    // soot's "a purge between the size scan and the image read degrades to a
    // snapshot": a single host transaction cannot purge mid-pull, so the
    // equivalent invariant is the floor check — a cookie the retention floor has
    // risen above can no longer be served as a diff.
    let mut s = Soot::new();
    s.retain = 1;
    s.exec(&format!("INSERT INTO file VALUES ('f0', '{PROJECT}', 'z.ts', 0)"));
    let old = cookie(&s.pull(json!(null)).unwrap());
    for i in 0..4 {
        s.exec(&format!("INSERT INTO file VALUES ('g{i}', '{PROJECT}', 'p{i}.ts', 1)"));
    }
    let res = s.pull(json!(old)).unwrap();
    assert_eq!(patch(&res)[0], json!({ "op": "clear" }));
}

#[test]
fn attach_command_visibility_puts_own_rows_dels_others() {
    let mut s = Soot::new();
    let vis = attach_visibility();
    let c = cookie(&s.pull_full("c1", GROUP, json!(null), Some(&vis), USER).unwrap());
    s.exec(&format!("INSERT INTO attachCommand VALUES ('mine', '{PROJECT}', '{USER}', 'ls')"));
    s.exec(&format!("INSERT INTO attachCommand VALUES ('theirs', '{PROJECT}', 'u2', 'env')"));
    let res = s.pull_full("c1", GROUP, json!(c), Some(&vis), USER).unwrap();
    assert_eq!(
        puts(&res).iter().map(|p| p["value"]["id"].clone()).collect::<Vec<_>>(),
        vec![json!("mine")]
    );
    assert_eq!(
        dels(&res),
        vec![json!({ "op": "del", "tableName": "attachCommand", "id": { "id": "theirs" } })]
    );
    // the snapshot path applies the same row-local filter
    let snap = s.pull_full("c2", GROUP, json!(null), Some(&vis), USER).unwrap();
    let snap_ids: Vec<Value> = puts(&snap)
        .iter()
        .filter(|p| p["tableName"] == "attachCommand")
        .map(|p| p["value"]["id"].clone())
        .collect();
    assert_eq!(snap_ids, vec![json!("mine")]);
}

#[test]
fn internal_ops_are_skipped_unknown_row_tables_throw() {
    let mut s = Soot::new();
    let c = cookie(&s.pull(json!(null)).unwrap());
    // internal ops: an lmid row (own group) and an epoch marker — no row patch
    s.append_lmid(GROUP, "c1", 3);
    s.exec("INSERT INTO _zsync_changes (tableName, op, pk) VALUES ('_zsync_meta', 'marker', NULL)");
    let res = s.pull(json!(c)).unwrap();
    assert_eq!(puts(&res), Vec::<Value>::new());
    assert_eq!(dels(&res), Vec::<Value>::new());

    // an unmapped table appearing as a 'row' change must fail loudly (invariant 10)
    s.exec(
        "INSERT INTO _zsync_changes (tableName, op, pk)
         VALUES ('mysteryTable', 'row', json_object('id', 'x'))",
    );
    let err = s.pull(json!(cookie(&res))).unwrap_err();
    assert_eq!(err.status, 500);
    assert!(err.message.contains("unmapped table 'mysteryTable'"), "got: {}", err.message);
}

#[test]
fn group_ownership_is_enforced_before_any_data() {
    let mut s = Soot::new();
    s.db
        .exec(
            "INSERT INTO _zsync_clients VALUES (?, 'c1', 3, 'someone-else')",
            &[SqlValue::Text(GROUP.to_string())],
        )
        .unwrap();
    let err = s.pull_full("c2", GROUP, json!(null), None, USER).unwrap_err();
    assert_eq!(err.status, 403);
    assert!(err.message.contains("different user"));
}

// ---- node-mode semantics (mapped to the cursor model) ----------------------

#[test]
fn a_delete_of_a_non_newest_row_still_moves_the_cookie() {
    // soot's node WAL cookie must advance on ANY committed write, including a
    // delete of an old row while another client holds the max lmid. the engine's
    // change-log watermark advances on every trigger, so the delete both moves
    // the cookie and ships as a del — the derived-clock bug cannot exist here.
    let mut s = Soot::new();
    s.exec(&format!("INSERT INTO file VALUES ('old', '{PROJECT}', 'old.ts', 1)"));
    s.exec(&format!("INSERT INTO file VALUES ('new', '{PROJECT}', 'new.ts', 1)"));
    // a peer client cA advanced its lmid to 7 long ago
    s.append_lmid(GROUP, "cA", 7);
    let first = s.pull(json!(null)).unwrap();
    assert_eq!(puts(&first).len(), 2); // snapshot of both files
    let c = cookie(&first);

    // cB deletes the OLD (non-newest) row and advances 2 -> 3
    s.exec("DELETE FROM file WHERE id = 'old'");
    s.append_lmid(GROUP, "cB", 3);

    let second = s.pull(json!(c)).unwrap();
    assert!(cookie(&second) > c);
    assert_eq!(
        dels(&second),
        vec![json!({ "op": "del", "tableName": "file", "id": { "id": "old" } })]
    );
    // the prefix ack carries cB's advance
    assert_eq!(second["lastMutationIDChanges"]["cB"], json!(3));

    // and with no further writes the fast path settles
    let third = s.pull(json!(cookie(&second))).unwrap();
    assert_eq!(third, json!({ "cookie": s.watermark(), "unchanged": true }));
}

#[test]
fn snapshot_applies_row_local_visibility_across_tables() {
    // soot's node snapshot scopes by projectId + applies attachCommand own-row
    // visibility. the engine runs one namespace per database, so projectId
    // scoping is a deployment concern, not the engine's; the row-local
    // visibility it must apply on snapshots is what this pins.
    let mut s = Soot::new();
    let vis = attach_visibility();
    s.exec(&format!("INSERT INTO file VALUES ('mine', '{PROJECT}', 'a.ts', 1)"));
    s.exec(&format!("INSERT INTO attachCommand VALUES ('mine', '{PROJECT}', '{USER}', 'ls')"));
    s.exec(&format!("INSERT INTO attachCommand VALUES ('theirs', '{PROJECT}', 'u2', 'env')"));
    let res = s.pull_full("c1", GROUP, json!(null), Some(&vis), USER).unwrap();
    let ids: Vec<String> = puts(&res)
        .iter()
        .map(|p| format!("{}:{}", p["tableName"].as_str().unwrap(), p["value"]["id"].as_str().unwrap()))
        .collect();
    assert!(ids.contains(&"file:mine".to_string()));
    assert!(ids.contains(&"attachCommand:mine".to_string()));
    assert!(!ids.contains(&"attachCommand:theirs".to_string()));
}
