// the original TypeScript delta correctness suite, ported verbatim: every
// named test and every table-driven case. drives the
// engine through the synchronous test host (rusqlite + the push-step driver).
mod common;

use common::{Host, item_tables};
use serde_json::{Value, json};

use sync_core::query::handle_query_pull;
use sync_core::{EngineError, SqlValue, SyncDb, Transactor, invalidate, push_validate};

// ---- helpers mirroring the TS suite's push()/pull()/patchOf() -------------

fn setup() -> Host {
    let mut h = Host::new(true);
    // seed before init_schema: the seed row stays out of the change log
    h.exec(
        "INSERT INTO item (id, label, rank, done, meta)
         VALUES ('seed1', 'first', 1.5, 0, '{\"tag\":\"a\"}')",
    );
    h.init();
    h
}

fn cookie_of(resp: &Value) -> i64 {
    resp["cookie"].as_i64().unwrap()
}

fn patch_of(resp: &Value) -> &Vec<Value> {
    resp["rowsPatch"].as_array().expect("rowsPatch present")
}

fn puts(patch: &[Value]) -> Vec<&Value> {
    patch.iter().filter(|op| op["op"] == "put").collect()
}

// ---- request validation ---------------------------------------------------

#[test]
fn rejects_invalid_pull_cookies() {
    // the reference's table was [undefined, "0", -1, 1.5, NaN, +Infinity].
    // under the pinned wire decision a CANONICAL decimal string ("0", "5") is a
    // valid cookie, so the reference's string-"0" rejection is replaced by
    // non-canonical string rejection; NaN/Infinity can't exist in parsed JSON.
    let cases = vec![
        json!(-1),
        json!(1.5),
        json!({}),
        json!([]),
        json!(true),
        json!("abc"), // non-numeric string
        json!("01"),  // non-canonical (leading zero)
        json!("-5"),  // signed
        json!("1.5"), // fractional string
    ];
    for cookie in cases {
        let mut h = setup();
        let tables = item_tables();
        let body = json!({ "clientID": "c1", "clientGroupID": "g1", "cookie": cookie });
        let err =
            h.db.transaction(|db| handle_query_pull(db, &tables, 4096, &body, "u1"))
                .unwrap_err();
        assert_eq!(err.status, 400, "cookie {cookie} should 400");
    }

    // undefined (missing cookie field) is malformed, like the reference
    let mut h = setup();
    let tables = item_tables();
    let body = json!({ "clientID": "c1", "clientGroupID": "g1" });
    let err =
        h.db.transaction(|db| handle_query_pull(db, &tables, 4096, &body, "u1"))
            .unwrap_err();
    assert_eq!(err.status, 400, "missing cookie should 400");
}

#[test]
fn canonical_string_cookie_is_accepted() {
    // the pinned wire decision: a canonical unsigned base-10 string is a valid
    // cookie (sol-m0's precision-safe boundary format). "0" == watermark 0.
    let mut h = setup();
    let tables = item_tables();
    let body = json!({ "clientID": "c1", "clientGroupID": "g1", "cookie": "0" });
    let resp =
        h.db.transaction(|db| handle_query_pull(db, &tables, 4096, &body, "u1"))
            .unwrap();
    assert_eq!(resp, json!({ "cookie": 0, "unchanged": true }));
}

#[test]
fn rejects_malformed_pull_body() {
    let cases = vec![
        json!({ "clientID": 1, "clientGroupID": "g1", "cookie": null }),
        json!({ "clientID": "c1", "clientGroupID": 1, "cookie": null }),
        json!(null),
    ];
    for body in cases {
        let mut h = setup();
        let tables = item_tables();
        let err =
            h.db.transaction(|db| handle_query_pull(db, &tables, 4096, &body, "u1"))
                .unwrap_err();
        assert_eq!(err.status, 400);
    }
}

fn valid_push_body(over: Value) -> Value {
    let mut mutation = json!({
        "type": "custom", "id": 1, "clientID": "c1", "name": "item.put",
        "args": [{ "id": "validated", "label": "ok", "rank": 1, "done": false, "meta": null }],
        "timestamp": 0,
    });
    if let Value::Object(o) = over {
        for (k, v) in o {
            mutation[k] = v;
        }
    }
    json!({ "clientGroupID": "g1", "mutations": [mutation], "pushVersion": 1, "requestID": "validated-request" })
}

#[test]
fn rejects_malformed_mutations() {
    let cases = vec![
        json!({ "id": 1.5 }),
        json!({ "id": 0 }),
        json!({ "clientID": 7 }),
        json!({ "name": 7 }),
        json!({ "args": { "id": "x" } }),
        json!({ "type": "crud" }),
    ];
    for over in cases {
        let body = valid_push_body(over.clone());
        let err = push_validate(&body).err().expect("should 400");
        assert_eq!(err.status, 400, "mutation override {over} should 400");
    }
}

#[test]
fn rejects_malformed_push_body() {
    let cases = vec![
        json!(null),
        json!({ "clientGroupID": 1, "mutations": [], "pushVersion": 1 }),
        json!({ "clientGroupID": "g1", "mutations": {}, "pushVersion": 1 }),
        json!({ "clientGroupID": "g1", "mutations": [], "pushVersion": "1" }),
    ];
    for body in cases {
        assert_eq!(push_validate(&body).err().map(|e| e.status), Some(400));
    }
}

#[test]
fn validates_entire_push_before_processing_first_mutation() {
    let mut h = setup();
    let mut body = valid_push_body(json!({}));
    let extra = json!({ "type": "custom", "id": 2, "clientID": "c1", "name": 42, "args": [{}], "timestamp": 0 });
    body["mutations"].as_array_mut().unwrap().push(extra);

    let err = h.push_from_body(&body, "u1").unwrap_err();
    assert_eq!(err.status, 400);
    assert!(h.query_item("validated").is_none());
    assert_eq!(h.watermark(), 0);
}

#[test]
fn returns_unsupported_push_version_without_processing() {
    let mut h = setup();
    let mut body = valid_push_body(json!({}));
    body["pushVersion"] = json!(2);
    let resp = h.push_from_body(&body, "u1").unwrap();
    assert_eq!(
        resp,
        json!({ "pushResponse": { "error": "unsupportedPushVersion", "mutationIDs": [{ "clientID": "c1", "id": 1 }] } })
    );
    assert!(h.query_item("validated").is_none());
    assert_eq!(h.watermark(), 0);
}

// ---- snapshot and unchanged -----------------------------------------------

#[test]
fn fresh_pull_is_clear_puts_snapshot_with_typed_values() {
    let mut h = setup();
    let resp = h.pull(json!(null), "u1").unwrap();
    let patch = patch_of(&resp);
    assert_eq!(patch[0], json!({ "op": "clear" }));
    assert_eq!(
        patch[1],
        json!({ "op": "put", "tableName": "item_record",
                "value": { "item_id": "seed1", "item_label": "first", "sort_rank": 1.5, "is_done": false, "metadata_json": { "tag": "a" } } })
    );
    assert_eq!(cookie_of(&resp), 0);
}

#[test]
fn same_cookie_pull_is_unchanged() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    assert_eq!(
        h.pull(json!(cookie), "u1").unwrap(),
        json!({ "cookie": cookie, "unchanged": true })
    );
}

#[test]
fn future_cookie_is_409() {
    let mut h = setup();
    let err = h.pull(json!(99), "u1").unwrap_err();
    assert_eq!(err.status, 409);
}

// ---- cursor diffs ---------------------------------------------------------

#[test]
fn insert_arrives_as_put_diff_without_clear_floats_exact() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    let rank = 0.1 + 0.2; // 0.30000000000000004 — 17 significant digits
    h.put(
        "i2",
        json!({ "id": "i2", "label": "two", "rank": rank, "done": true, "meta": [1, "x"] }),
        1,
    );
    let resp = h.pull(json!(cookie), "u1").unwrap();
    let patch = patch_of(&resp);
    assert!(!patch.iter().any(|op| op["op"] == "clear"));
    let put = patch.iter().find(|op| op["op"] == "put").unwrap();
    assert_eq!(
        put["value"],
        json!({
            "item_id": "i2",
            "item_label": "two",
            "sort_rank": rank,
            "is_done": true,
            "metadata_json": [1, "x"],
        })
    );
    // exact, not sqlite json's 15-digit form
    assert_eq!(put["value"]["sort_rank"].as_f64().unwrap(), rank);
}

#[test]
fn update_arrives_as_put_of_only_the_touched_row() {
    let mut h = setup();
    h.put(
        "i2",
        json!({ "id": "i2", "label": "two", "rank": 2, "done": false, "meta": null }),
        1,
    );
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    h.put(
        "i2",
        json!({ "id": "i2", "label": "renamed", "rank": 2, "done": false, "meta": null }),
        2,
    );
    let patch = patch_of(&h.pull(json!(cookie), "u1").unwrap()).clone();
    let ps = puts(&patch);
    assert_eq!(ps.len(), 1);
    assert_eq!(ps[0]["value"]["item_id"], json!("i2"));
    assert_eq!(ps[0]["value"]["item_label"], json!("renamed"));
}

#[test]
fn a_missing_first_packed_segment_fails_loud() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    h.put(
        "i2",
        json!({ "id": "i2", "label": "two", "rank": 2, "done": false, "meta": null }),
        1,
    );
    let (end, payload, pending, capture_mode): (i64, String, String, i64) =
        h.db.conn
            .query_row(
                "SELECT endVersion, payload, pending, captureMode
             FROM _zsync_log_segments ORDER BY startVersion LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
    h.db.conn
        .execute("DELETE FROM _zsync_log_segments", [])
        .unwrap();
    h.db.conn
        .execute(
            "INSERT INTO _zsync_log_segments
               (startVersion, endVersion, payload, pending, captureMode)
             VALUES (3, ?, ?, ?, ?)",
            rusqlite::params![end, payload, pending, capture_mode],
        )
        .unwrap();

    let error = h.pull(json!(cookie), "u1").unwrap_err();
    assert_eq!(error.status, 500);
    assert_eq!(
        error.message,
        "packed ledger segment chain has a leading gap"
    );

    h.db.conn
        .execute("DELETE FROM _zsync_log_segments", [])
        .unwrap();
    h.db.conn
        .execute(
            "INSERT INTO _zsync_log_segments
               (startVersion, endVersion, payload, pending, captureMode)
             VALUES (1, ?, ?, ?, ?)",
            rusqlite::params![end, payload, pending, capture_mode],
        )
        .unwrap();
    let response = h.pull(json!(cookie), "u1").unwrap();
    assert_eq!(puts(patch_of(&response)).len(), 1);
}

#[test]
fn raw_trigger_rotation_preserves_the_incremental_boundary() {
    for (label, filler_bytes, expected_segments) in [
        ("below threshold", 760_000, 1),
        ("above threshold", 790_000, 2),
    ] {
        let mut h = setup();
        h.pull(json!(null), "u1").unwrap();
        let payload = serde_json::to_string(&json!({
            "format": 2,
            "transactions": [{
                "version": "1",
                "changes": [["item", { "id": "x".repeat(filler_bytes) }]],
            }],
        }))
        .unwrap();
        h.db.conn
            .execute(
                "UPDATE _zsync_log_segments SET endVersion = 1, payload = ?",
                rusqlite::params![payload],
            )
            .unwrap();

        h.exec(
            "INSERT INTO item (id, label, rank, done, meta)
             VALUES ('rotated', 'visible', 1, 0, NULL)",
        );
        let segments: i64 =
            h.db.conn
                .query_row("SELECT COUNT(*) FROM _zsync_log_segments", [], |row| {
                    row.get(0)
                })
                .unwrap();
        assert_eq!(segments, expected_segments, "{label}");
        let response = h.pull(json!(1), "u1").unwrap();
        assert_eq!(puts(patch_of(&response))[0]["value"]["item_id"], "rotated");
    }
}

#[test]
fn a_large_lmid_checkpoint_does_not_wedge_the_next_mutation() {
    let mut h = setup();
    h.put(
        "first",
        json!({ "id": "first", "label": "first", "rank": 1, "done": false, "meta": null }),
        1,
    );

    // the wedge shape a namespace reaches once its lmid map alone crosses
    // ROTATE_AT_BYTES: the active segment is EMPTY (start == end + 1) and still
    // oversized, so both rotate paths insert at the active row's own
    // startVersion and every write dies on the primary key forever.
    let historical = 18_000;
    let lmid_of = |index: usize| 1 + (index % 3) as i64;
    let mut lmids = serde_json::Map::new();
    lmids.insert("g1".into(), json!({ "c1": "1" }));
    for index in 0..historical {
        lmids.insert(
            format!("historical-group-{index:05}"),
            json!({ format!("historical-client-{index:05}"): lmid_of(index).to_string() }),
        );
    }
    let payload = serde_json::to_string(&json!({
        "format": 1,
        "lmids": lmids,
        "transactions": [],
    }))
    .unwrap();
    assert!(payload.len() >= 768 * 1_024);
    h.db.conn
        .execute(
            "UPDATE _zsync_log_segments
             SET endVersion = startVersion - 1, payload = ?",
            rusqlite::params![payload],
        )
        .unwrap();
    let active = |h: &mut Host| -> (i64, i64, i64) {
        h.db.conn
            .query_row(
                "SELECT startVersion, endVersion, length(CAST(payload AS BLOB))
                 FROM _zsync_log_segments ORDER BY startVersion DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    };
    let (start, end, bytes) = active(&mut h);
    assert_eq!(start, end + 1, "the wedged segment is empty");
    assert!(
        bytes >= 768 * 1_024,
        "the wedged segment is still oversized"
    );

    let before = h.db.conn.total_changes() as i64;
    h.init();
    let migration_cost = h.db.conn.total_changes() as i64 - before;
    let before = h.db.conn.total_changes() as i64;
    h.init();
    let settled_cost = h.db.conn.total_changes() as i64 - before;
    // the migration owns one upsert per client in the active map plus one
    // rewrite of the single retained segment, and a namespace this size pays
    // that bill exactly once — a durable object re-runs the schema pass on
    // every hibernation wake.
    assert_eq!(settled_cost, 0, "a settled schema pass must write nothing");
    assert_eq!(migration_cost, historical as i64 + 1 + 1);

    // the map moved out of the payload, which is what takes the segment back
    // under the rotation threshold and unwedges the namespace
    let (healed_start, healed_end, healed_bytes) = active(&mut h);
    assert_eq!((healed_start, healed_end), (start, end));
    assert!(healed_bytes < 768 * 1_024);

    // every client's lastMutationID survives the move, exactly
    let lmid = |h: &mut Host, group: String, client: String| -> i64 {
        h.db.conn
            .query_row(
                "SELECT lastMutationID FROM _zsync_clients
                 WHERE clientGroupID = ?1 AND clientID = ?2",
                [group, client],
                |row| row.get(0),
            )
            .unwrap()
    };
    for index in 0..historical {
        let actual = lmid(
            &mut h,
            format!("historical-group-{index:05}"),
            format!("historical-client-{index:05}"),
        );
        assert_eq!(actual, lmid_of(index), "historical client {index}");
    }
    assert_eq!(lmid(&mut h, "g1".into(), "c1".into()), 1);

    h.put(
        "second",
        json!({ "id": "second", "label": "second", "rank": 2, "done": false, "meta": null }),
        2,
    );
    assert!(h.query_item("second").is_some());
    assert_eq!(
        lmid(&mut h, "g1".into(), "c1".into()),
        2,
        "the next mutation advances from the migrated lmid"
    );
}

#[test]
fn orphaned_capture_mode_is_cleared_by_the_schema_pass() {
    let mut h = setup();
    h.put(
        "first",
        json!({ "id": "first", "label": "first", "rank": 1, "done": false, "meta": null }),
        1,
    );
    let capture_mode = |h: &mut Host| -> i64 {
        h.db.conn
            .query_row(
                "SELECT captureMode FROM _zsync_log_segments
                 ORDER BY startVersion DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap()
    };
    // a delegated push abandoned between the capture toggle and its commit
    // leaves the column set with no writer behind it
    h.db.conn
        .execute(
            "UPDATE _zsync_log_segments SET captureMode = 1
             WHERE startVersion = (SELECT MAX(startVersion) FROM _zsync_log_segments)",
            [],
        )
        .unwrap();

    h.init();
    assert_eq!(
        capture_mode(&mut h),
        0,
        "the schema pass clears the residue"
    );
    let before = h.db.conn.total_changes() as i64;
    h.init();
    assert_eq!(
        h.db.conn.total_changes() as i64 - before,
        0,
        "a settled schema pass must write nothing"
    );

    // the trigger bodies are gated on captureMode = 0, so an uncleared column
    // wrote the row while silently dropping its change envelope: the row went
    // live and no client could ever pull it.
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    h.put(
        "second",
        json!({ "id": "second", "label": "second", "rank": 2, "done": false, "meta": null }),
        2,
    );
    assert!(h.query_item("second").is_some());
    let puts = puts(patch_of(&h.pull(json!(cookie), "u1").unwrap())).len();
    assert_eq!(puts, 1, "the first write after the pass must be pullable");
}

#[test]
fn capture_mode_is_left_alone_while_a_transaction_is_journaled() {
    let mut h = setup();
    h.put(
        "first",
        json!({ "id": "first", "label": "first", "rank": 1, "done": false, "meta": null }),
        1,
    );
    // the journal owns the toggle until its transaction commits or rolls back;
    // a schema pass that clobbered it would strand a live writer.
    h.db.conn
        .execute_batch(
            "CREATE TABLE _orez_tx_manifest (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, tx_id TEXT NOT NULL,
                owner TEXT NOT NULL DEFAULT 'default', original TEXT NOT NULL,
                snapshot TEXT);
             INSERT INTO _orez_tx_manifest (tx_id, original)
             VALUES ('live-tx', '_zsync_log_segments');
             UPDATE _zsync_log_segments SET captureMode = 1
             WHERE startVersion = (SELECT MAX(startVersion) FROM _zsync_log_segments);",
        )
        .unwrap();
    h.init();
    let capture_mode: i64 =
        h.db.conn
            .query_row(
                "SELECT captureMode FROM _zsync_log_segments
                 ORDER BY startVersion DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
    assert_eq!(capture_mode, 1, "an in-flight transaction keeps its toggle");

    // once that transaction is gone the next pass clears it
    h.db.conn
        .execute("DELETE FROM _orez_tx_manifest", [])
        .unwrap();
    h.init();
    let capture_mode: i64 =
        h.db.conn
            .query_row(
                "SELECT captureMode FROM _zsync_log_segments
                 ORDER BY startVersion DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
    assert_eq!(capture_mode, 0);
}

#[test]
fn a_previous_trigger_generation_is_dropped_by_the_schema_pass() {
    let mut h = setup();
    // a stale-generation trigger (versioned or unversioned) left installed
    // would fire beside the current set on every raw write. the canary table
    // records any such firing.
    h.db.conn
        .execute_batch(
            "CREATE TABLE trigger_canary (n INTEGER);
             CREATE TRIGGER _zsync_tr_item_record_i_v2 AFTER INSERT ON item_record
             BEGIN INSERT INTO trigger_canary VALUES (1); END;
             CREATE TRIGGER _zsync_tr_item_record_i AFTER INSERT ON item_record
             BEGIN INSERT INTO trigger_canary VALUES (1); END;",
        )
        .unwrap();
    h.init();
    h.put(
        "first",
        json!({ "id": "first", "label": "first", "rank": 1, "done": false, "meta": null }),
        1,
    );
    let fired: i64 =
        h.db.conn
            .query_row("SELECT count(*) FROM trigger_canary", [], |row| row.get(0))
            .unwrap();
    assert_eq!(
        fired, 0,
        "stale trigger generations must be dropped by init"
    );
    let current: i64 =
        h.db.conn
            .query_row(
                "SELECT count(*) FROM sqlite_schema
                 WHERE type = 'trigger' AND name GLOB '_zsync_tr_*'",
                [],
                |row| row.get(0),
            )
            .unwrap();
    assert_eq!(
        current, 3,
        "current-generation triggers must stay installed"
    );
}

#[test]
fn format_1_migration_rewrites_every_retained_segment() {
    let mut h = setup();
    h.put(
        "first",
        json!({ "id": "first", "label": "first", "rank": 1, "done": false, "meta": null }),
        1,
    );

    let segment = |versions: &[(i64, &str)], lmids: Value| {
        serde_json::to_string(&json!({
            "format": 1,
            "lmids": lmids,
            "transactions": versions
                .iter()
                .map(|(version, id)| json!({
                    "version": version.to_string(),
                    "changes": [["item", { "id": id }]],
                }))
                .collect::<Vec<_>>(),
        }))
        .unwrap()
    };
    h.db.conn
        .execute("DELETE FROM _zsync_log_segments", [])
        .unwrap();
    let insert = "INSERT INTO _zsync_log_segments
                    (startVersion, endVersion, payload, pending, captureMode)
                  VALUES (?, ?, ?, '[]', 0)";
    // non-active maps are rotation-time copies and must be ignored: only the
    // active segment's map is canonical at migration time.
    h.db.conn
        .execute(
            insert,
            rusqlite::params![
                1,
                2,
                segment(&[(1, "a"), (2, "b")], json!({ "g1": { "c1": "999" } }))
            ],
        )
        .unwrap();
    h.db.conn
        .execute(
            insert,
            rusqlite::params![
                3,
                4,
                segment(&[(3, "c"), (4, "d")], json!({ "g1": { "c1": "999" } }))
            ],
        )
        .unwrap();
    h.db.conn
        .execute(
            insert,
            rusqlite::params![
                5,
                5,
                segment(
                    &[(5, "e")],
                    json!({ "g1": { "c1": "7" }, "historical-group": { "historical-client": "3" } })
                )
            ],
        )
        .unwrap();

    let before = h.db.conn.total_changes() as i64;
    h.init();
    let migration_cost = h.db.conn.total_changes() as i64 - before;
    let before = h.db.conn.total_changes() as i64;
    h.init();
    let settled_cost = h.db.conn.total_changes() as i64 - before;
    // the migration owns exactly one rewrite per retained segment plus one
    // upsert per client in the active map, over whatever a settled pass costs.
    assert_eq!(settled_cost, 0, "a settled schema pass must write nothing");
    assert_eq!(migration_cost, settled_cost + 3 + 2);

    let rows =
        h.db.query(
            "SELECT payload FROM _zsync_log_segments ORDER BY startVersion",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 3);
    let mut versions = Vec::new();
    for row in &rows {
        let Some(SqlValue::Text(payload)) = row.values.first() else {
            panic!("payload is not text");
        };
        let payload: Value = serde_json::from_str(payload).unwrap();
        assert_eq!(payload["format"], json!(2));
        assert!(payload.get("lmids").is_none());
        for transaction in payload["transactions"].as_array().unwrap() {
            versions.push(transaction["version"].as_str().unwrap().to_string());
            assert_eq!(transaction["changes"].as_array().unwrap().len(), 1);
        }
    }
    assert_eq!(versions, ["1", "2", "3", "4", "5"]);

    let lmid = |group: &str, client: &str| -> i64 {
        h.db.conn
            .query_row(
                "SELECT lastMutationID FROM _zsync_clients
                 WHERE clientGroupID = ?1 AND clientID = ?2",
                [group, client],
                |row| row.get(0),
            )
            .unwrap()
    };
    assert_eq!(
        lmid("g1", "c1"),
        7,
        "the active map must overwrite a stale row"
    );
    assert_eq!(lmid("historical-group", "historical-client"), 3);

    // the migrated ledger accepts the next mutation for the imported lmid
    h.put(
        "second",
        json!({ "id": "second", "label": "second", "rank": 2, "done": false, "meta": null }),
        8,
    );
    assert!(h.query_item("second").is_some());
}

#[test]
fn an_oversized_raw_envelope_rolls_back_the_application_write() {
    let mut h = setup();
    let oversized = "x".repeat(1_050_000);
    let error =
        h.db.exec(
            "INSERT INTO item_record
             (item_id, item_label, sort_rank, is_done, metadata_json)
             VALUES (?, 'too large', 1, 0, NULL)",
            &[sync_core::SqlValue::Text(oversized)],
        )
        .unwrap_err();
    assert!(
        error
            .0
            .contains("packed ledger transaction exceeds the 1 MiB limit")
    );
    assert_eq!(h.watermark(), 0);
    let rows: i64 =
        h.db.conn
            .query_row("SELECT COUNT(*) FROM item_record", [], |row| row.get(0))
            .unwrap();
    assert_eq!(rows, 1, "only the pre-init seed row remains");

    h.exec(
        "INSERT INTO item (id, label, rank, done, meta)
         VALUES ('small', 'fits', 1, 0, NULL)",
    );
    assert_eq!(h.watermark(), 1);
}

#[test]
fn delete_arrives_as_del_with_primary_key() {
    let mut h = setup();
    h.put(
        "i2",
        json!({ "id": "i2", "label": "two", "rank": 2, "done": false, "meta": null }),
        1,
    );
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    h.del("i2", 2);
    let patch = patch_of(&h.pull(json!(cookie), "u1").unwrap()).clone();
    assert_eq!(
        patch,
        vec![json!({ "op": "del", "tableName": "item_record", "id": { "item_id": "i2" } })]
    );
}

#[test]
fn delete_then_recreate_collapses_to_put() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    h.del("seed1", 1);
    h.put(
        "seed1",
        json!({ "id": "seed1", "label": "reborn", "rank": 9, "done": false, "meta": null }),
        2,
    );
    let patch = patch_of(&h.pull(json!(cookie), "u1").unwrap()).clone();
    assert_eq!(
        patch,
        vec![json!({ "op": "put", "tableName": "item_record",
                     "value": { "item_id": "seed1", "item_label": "reborn", "sort_rank": 9, "is_done": false, "metadata_json": null } })]
    );
}

#[test]
fn insert_then_delete_collapses_to_nothing() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    h.put(
        "ephemeral",
        json!({ "id": "ephemeral", "label": "x", "rank": 0, "done": false, "meta": null }),
        1,
    );
    h.del("ephemeral", 2);
    // the client never held the row (it was never a member at any pull), so
    // the membership diff correctly emits nothing, not a del for a row the
    // client cannot have. the baseline change-log diff used to re-emit it.
    let patch = patch_of(&h.pull(json!(cookie), "u1").unwrap()).clone();
    assert_eq!(patch, Vec::<Value>::new());
}

#[test]
fn upstream_sql_outside_push_advances_watermark() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    h.exec("UPDATE item SET label = 'edited behind zero' WHERE id = 'seed1'");
    let resp = h.pull(json!(cookie), "u1").unwrap();
    assert!(cookie_of(&resp) > cookie);
    assert_eq!(
        patch_of(&resp).clone(),
        vec![json!({ "op": "put", "tableName": "item_record",
                     "value": { "item_id": "seed1", "item_label": "edited behind zero", "sort_rank": 1.5, "is_done": false, "metadata_json": { "tag": "a" } } })]
    );
}

#[test]
fn pk_changing_update_dels_old_and_puts_new() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    h.exec("UPDATE item SET id = 'seed1-renamed' WHERE id = 'seed1'");
    let patch = patch_of(&h.pull(json!(cookie), "u1").unwrap()).clone();
    assert!(patch.contains(
        &json!({ "op": "del", "tableName": "item_record", "id": { "item_id": "seed1" } })
    ));
    assert!(patch.contains(&json!({ "op": "put", "tableName": "item_record",
        "value": { "item_id": "seed1-renamed", "item_label": "first", "sort_rank": 1.5, "is_done": false, "metadata_json": { "tag": "a" } } })));
}

// ---- push semantics -------------------------------------------------------

#[test]
fn app_error_advances_lmid_and_watermark_but_no_rows() {
    let mut h = setup();
    let cookie = cookie_of(&h.pull(json!(null), "u1").unwrap());
    let resp = h
        .push_one("item.reject", json!({}), "c1", "g1", 1, "u1")
        .unwrap();
    assert_eq!(
        resp["pushResponse"]["mutations"][0]["result"],
        json!({ "error": "app", "message": "nope", "details": "nope" })
    );
    assert!(h.query_item("rejected").is_none());
    let next = h.pull(json!(cookie), "u1").unwrap();
    assert!(cookie_of(&next) > cookie);
    assert_eq!(next["lastMutationIDChanges"]["c1"], json!(1));
    assert_eq!(patch_of(&next).clone(), Vec::<Value>::new());
}

#[test]
fn replayed_mutation_acks_idempotently() {
    let mut h = setup();
    h.push_one(
        "item.put",
        json!({ "id": "i2", "label": "once", "rank": 1, "done": false, "meta": null }),
        "c1",
        "g1",
        1,
        "u1",
    )
    .unwrap();
    let replay = h
        .push_one(
            "item.put",
            json!({ "id": "i2", "label": "twice?", "rank": 1, "done": false, "meta": null }),
            "c1",
            "g1",
            1,
            "u1",
        )
        .unwrap();
    assert_eq!(
        replay["pushResponse"]["mutations"][0]["result"],
        json!({ "error": "alreadyProcessed",
                "details": "Ignoring mutation from c1 with ID 1 as it was already processed. Expected: 2" })
    );
    assert_eq!(
        h.query_item("i2").unwrap()["label"].as_str().unwrap(),
        "once"
    );
}

#[test]
fn out_of_order_mutation_id_is_400() {
    let mut h = setup();
    let err = h
        .push_one("item.put", json!({ "id": "x" }), "c1", "g1", 5, "u1")
        .unwrap_err();
    assert_eq!(err.status, 400);
    assert!(err.message.contains("skips lmid"));
}

#[test]
fn two_tabs_settle_through_last_mutation_id_changes() {
    let mut h = setup();
    h.push_one(
        "item.put",
        json!({ "id": "a", "label": "a", "rank": 0, "done": false, "meta": null }),
        "tab1",
        "g1",
        1,
        "u1",
    )
    .unwrap();
    h.push_one(
        "item.put",
        json!({ "id": "b", "label": "b", "rank": 0, "done": false, "meta": null }),
        "tab2",
        "g1",
        1,
        "u1",
    )
    .unwrap();
    let resp = h.pull_as("tab1", "g1", json!(null), "u1").unwrap();
    assert_eq!(
        resp["lastMutationIDChanges"],
        json!({ "tab1": 1, "tab2": 1 })
    );
}

#[test]
fn client_group_claimed_by_one_user_rejects_another() {
    let mut h = setup();
    h.pull(json!(null), "u1").unwrap();
    let err = h.pull(json!(null), "intruder").unwrap_err();
    assert_eq!(err.status, 403);
    assert!(err.message.contains("different user"));
}

// ---- retention floor ------------------------------------------------------

#[test]
fn pull_prunes_upstream_churn_before_unchanged() {
    let mut h = Host::new(true);
    h.retain = 2;
    h.exec("INSERT INTO item (id, label, rank, done, meta) VALUES ('seed1', 'first', 1.5, 0, '{\"tag\":\"a\"}')");
    h.init();
    let ancient = cookie_of(&h.pull(json!(null), "u1").unwrap());
    for i in 0..6 {
        h.exec(&format!(
            "INSERT INTO item (id, label, rank, done, meta) VALUES ('upstream-{i}', 'upstream {i}', {i}, 0, NULL)"
        ));
    }
    let current = h.watermark();
    assert_eq!(h.retained_version_count(), 6);
    assert_eq!(
        h.pull(json!(current), "u1").unwrap(),
        json!({ "cookie": current, "unchanged": true })
    );
    assert_eq!(h.retained_version_count(), 2);
    assert_eq!(h.floor(), current - 2);
    let stale = h.pull(json!(ancient), "u1").unwrap();
    assert_eq!(patch_of(&stale)[0], json!({ "op": "clear" }));
}

#[test]
fn cookie_below_floor_snapshots_recent_cookies_still_diff() {
    let mut h = Host::new(true);
    h.retain = 2;
    h.exec("INSERT INTO item (id, label, rank, done, meta) VALUES ('seed1', 'first', 1.5, 0, '{\"tag\":\"a\"}')");
    h.init();
    let ancient = cookie_of(&h.pull(json!(null), "u1").unwrap());
    for i in 0..6 {
        h.push_one("item.put", json!({ "id": format!("i{i}"), "label": format!("l{i}"), "rank": i, "done": false, "meta": null }), "c1", "g1", i + 1, "u1").unwrap();
    }
    let recent = cookie_of(&h.pull_as("c2", "g1", json!(null), "u1").unwrap());
    h.push_one(
        "item.put",
        json!({ "id": "last", "label": "last", "rank": 99, "done": false, "meta": null }),
        "c1",
        "g1",
        7,
        "u1",
    )
    .unwrap();

    let stale = h.pull(json!(ancient), "u1").unwrap();
    let stale_patch = patch_of(&stale).clone();
    assert_eq!(stale_patch[0], json!({ "op": "clear" })); // snapshot fallback
    assert!(puts(&stale_patch).len() >= 8);

    let fresh = h.pull_as("c2", "g1", json!(recent), "u1").unwrap();
    let fresh_patch = patch_of(&fresh).clone();
    assert!(!fresh_patch.iter().any(|op| op["op"] == "clear")); // still a diff
    assert_eq!(
        fresh_patch,
        vec![json!({ "op": "put", "tableName": "item_record",
                     "value": { "item_id": "last", "item_label": "last", "sort_rank": 99, "is_done": false, "metadata_json": null } })]
    );
}

// ---- epoch invalidation ---------------------------------------------------

#[test]
fn invalidate_forces_one_snapshot_then_diffs_resume() {
    let mut h = setup();
    let c1 = cookie_of(&h.pull(json!(null), "u1").unwrap());
    // a client that is fully caught up would otherwise answer `unchanged`
    let tables = item_tables();
    h.db.transaction(|db| invalidate(db)).unwrap();
    let after = h.pull(json!(c1), "u1").unwrap();
    assert!(cookie_of(&after) > c1);
    assert_eq!(patch_of(&after)[0], json!({ "op": "clear" })); // full snapshot
    let _ = tables;
    // after re-snapshotting, incremental diffs resume
    h.put(
        "post",
        json!({ "id": "post", "label": "post", "rank": 1, "done": false, "meta": null }),
        1,
    );
    let diff = h.pull(json!(cookie_of(&after)), "u1").unwrap();
    let diff_patch = patch_of(&diff).clone();
    assert!(!diff_patch.iter().any(|op| op["op"] == "clear"));
    assert_eq!(
        diff_patch,
        vec![json!({ "op": "put", "tableName": "item_record",
                     "value": { "item_id": "post", "item_label": "post", "sort_rank": 1, "is_done": false, "metadata_json": null } })]
    );
}

// ---- interleaved churn converges ------------------------------------------

#[test]
fn interleaved_pushes_and_upstream_converge() {
    let mut h = setup();
    use std::collections::HashMap;
    let mut stores: HashMap<&str, HashMap<String, Value>> = HashMap::new();
    let mut cookies: HashMap<&str, Value> = HashMap::new();
    cookies.insert("c1", json!(null));
    cookies.insert("c2", json!(null));

    fn apply_pull(
        h: &mut Host,
        client: &'static str,
        stores: &mut std::collections::HashMap<&str, std::collections::HashMap<String, Value>>,
        cookies: &mut std::collections::HashMap<&str, Value>,
    ) {
        let resp = h
            .pull_as(client, "g1", cookies[client].clone(), "u1")
            .unwrap();
        cookies.insert(client, resp["cookie"].clone());
        if resp.get("unchanged") == Some(&json!(true)) {
            return;
        }
        let store = stores.entry(client).or_default();
        for op in resp["rowsPatch"].as_array().unwrap() {
            match op["op"].as_str() {
                Some("clear") => store.clear(),
                Some("put") => {
                    store.insert(
                        op["value"]["item_id"].as_str().unwrap().to_string(),
                        op["value"].clone(),
                    );
                }
                Some("del") => {
                    store.remove(op["id"]["item_id"].as_str().unwrap());
                }
                _ => {}
            }
        }
    }

    apply_pull(&mut h, "c1", &mut stores, &mut cookies);
    let mut ids: HashMap<&str, i64> = HashMap::new();
    ids.insert("c1", 0);
    for round in 0..20 {
        let id = {
            let e = ids.entry("c1").or_insert(0);
            *e += 1;
            *e
        };
        h.push_one(
            "item.put",
            json!({ "id": format!("r{}", round % 7), "label": format!("round {round}"),
                    "rank": round as f64 + 0.1, "done": round % 2 == 1,
                    "meta": if round % 3 == 0 { json!({ "round": round }) } else { json!(null) } }),
            "c1",
            "g1",
            id,
            "u1",
        )
        .unwrap();
        if round % 4 == 0 {
            h.exec(&format!(
                "DELETE FROM item WHERE id = 'r{}'",
                (round + 3) % 7
            ));
        }
        if round % 5 == 2 {
            apply_pull(&mut h, "c1", &mut stores, &mut cookies);
        }
        if round % 3 == 1 {
            apply_pull(&mut h, "c2", &mut stores, &mut cookies);
        }
    }
    apply_pull(&mut h, "c1", &mut stores, &mut cookies);
    apply_pull(&mut h, "c2", &mut stores, &mut cookies);

    // oracle: a fresh client's full snapshot
    let oracle_resp = h.pull_as("c3", "g1", json!(null), "u1").unwrap();
    let mut oracle: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    for op in oracle_resp["rowsPatch"].as_array().unwrap() {
        if op["op"] == "put" {
            oracle.insert(
                op["value"]["item_id"].as_str().unwrap().to_string(),
                op["value"].clone(),
            );
        }
    }
    for client in ["c1", "c2"] {
        assert_eq!(
            &stores[client], &oracle,
            "client {client} diverged from oracle"
        );
    }
}

// exercised indirectly above but keep the type imports honest
#[allow(dead_code)]
fn _uses(_: EngineError) {}
