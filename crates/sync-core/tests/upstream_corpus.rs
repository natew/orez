// Behavioral CDC contracts adapted from Turso's pinned CDC integration suite
// and Electric's transaction-fragmentation suite. Orez uses database-scoped
// triggers rather than a connection-scoped CDC pragma, so these assert the
// equivalent durable effects instead of copying Turso's internal row format.
mod common;

use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use common::{item_tables, Host, TestDb};
use rusqlite::Connection;
use serde_json::{json, Map, Value};
use sync_core::pull::Caps;
use sync_core::{
    apply_upstream, init_schema, EngineError, SqlValue, SyncDb, Transactor, UpstreamBatch,
    UpstreamChange,
};

fn row(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn item(id: &str, label: &str) -> Value {
    json!({ "id": id, "label": label, "rank": 1, "done": false, "meta": null })
}

fn change(watermark: i64, id: &str) -> UpstreamChange {
    UpstreamChange {
        watermark,
        table_name: "item".into(),
        op: "INSERT".into(),
        row_data: Some(row(item(id, id))),
        old_data: None,
    }
}

fn setup() -> (TestDb, sync_core::Tables) {
    let mut db = TestDb::memory();
    db.exec(
        "CREATE TABLE item (id TEXT PRIMARY KEY, label TEXT NOT NULL, rank REAL NOT NULL, done INTEGER NOT NULL, meta TEXT)",
        &[],
    )
    .unwrap();
    let tables = item_tables();
    init_schema(&mut db, &tables).unwrap();
    (db, tables)
}

fn count(db: &mut TestDb, table: &str) -> i64 {
    let rows = db
        .query(
            &format!("SELECT CAST(COUNT(*) AS TEXT) AS n FROM {table}"),
            &[],
        )
        .unwrap();
    match rows[0].get("n") {
        Some(SqlValue::Text(value)) => value.parse().unwrap(),
        other => panic!("unexpected count {other:?}"),
    }
}

// turso.cdc.failed-operation + zero.change-processor.rollback
#[test]
fn failed_upstream_transaction_rolls_back_rows_log_and_cursor() {
    let (mut db, tables) = setup();
    let batch = UpstreamBatch {
        watermark: 3,
        changes: vec![
            change(1, "would-have-been-written"),
            UpstreamChange {
                watermark: 2,
                table_name: "item".into(),
                op: "INSERT".into(),
                row_data: Some(row(json!({
                    "id": "missing-full-image",
                    "label": "bad",
                    "rank": 1,
                    "done": false
                }))),
                old_data: None,
            },
        ],
    };

    let result = db.transaction(|tx| apply_upstream(tx, &tables, &batch));
    assert!(result.is_err());
    assert_eq!(count(&mut db, "item"), 0);
    assert_eq!(count(&mut db, "_zsync_changes"), 0);
    assert_eq!(sync_core::upstream_watermark(&mut db).unwrap(), 0);
}

// turso.cdc.transaction-boundary
#[test]
fn successful_upstream_batch_commits_rows_log_and_cursor_together() {
    let (mut db, tables) = setup();
    let batch = UpstreamBatch {
        watermark: 3,
        changes: vec![change(1, "a"), change(2, "b"), change(3, "c")],
    };

    let result = db
        .transaction(|tx| apply_upstream(tx, &tables, &batch))
        .unwrap();
    assert_eq!(result.applied, 3);
    assert_eq!(count(&mut db, "item"), 3);
    assert_eq!(count(&mut db, "_zsync_changes"), 3);
    assert_eq!(sync_core::upstream_watermark(&mut db).unwrap(), 3);
}

// turso.cdc.no-op-and-rollback
#[test]
fn no_op_commit_and_rolled_back_write_emit_no_cdc_and_connection_stays_usable() {
    let (mut db, _) = setup();
    db.transaction::<_, EngineError>(|_| Ok(())).unwrap();
    let rolled_back = db.transaction(|tx| {
        tx.exec(
            "INSERT INTO item (id, label, rank, done, meta) VALUES ('rolled-back', 'x', 0, 0, NULL)",
            &[],
        )?;
        Err::<(), _>(EngineError::bad_request("force rollback"))
    });
    assert!(rolled_back.is_err());
    assert_eq!(count(&mut db, "item"), 0);
    assert_eq!(count(&mut db, "_zsync_changes"), 0);

    db.exec(
        "INSERT INTO item (id, label, rank, done, meta) VALUES ('after', 'usable', 0, 0, NULL)",
        &[],
    )
    .unwrap();
    assert_eq!(count(&mut db, "item"), 1);
    assert_eq!(count(&mut db, "_zsync_changes"), 1);
}

// turso.cdc.recursion-guard
#[test]
fn trigger_cdc_does_not_capture_its_own_metadata_writes() {
    let (mut db, tables) = setup();
    let batch = UpstreamBatch {
        watermark: 1,
        changes: vec![change(1, "one")],
    };
    db.transaction(|tx| apply_upstream(tx, &tables, &batch))
        .unwrap();

    // The application write is captured once. Updating _zsync_meta and writing
    // the log row itself do not recursively append more log rows.
    assert_eq!(count(&mut db, "_zsync_changes"), 1);
    db.exec(
        "UPDATE _zsync_meta SET upstream_watermark = upstream_watermark WHERE lock = 1",
        &[],
    )
    .unwrap();
    assert_eq!(count(&mut db, "_zsync_changes"), 1);
}

// turso.cdc.connection-scope. Turso enables capture per connection; Orez's
// installed triggers are intentionally database-scoped and durable.
#[test]
fn trigger_cdc_is_visible_across_independent_connections() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("orez-upstream-corpus-{nonce}.sqlite"));
    let mut first = TestDb {
        conn: Connection::open(&path).unwrap(),
    };
    first
        .exec(
            "CREATE TABLE item (id TEXT PRIMARY KEY, label TEXT NOT NULL, rank REAL NOT NULL, done INTEGER NOT NULL, meta TEXT)",
            &[],
        )
        .unwrap();
    init_schema(&mut first, &item_tables()).unwrap();

    let mut second = TestDb {
        conn: Connection::open(&path).unwrap(),
    };
    second
        .exec(
            "INSERT INTO item (id, label, rank, done, meta) VALUES ('second', 'connection', 0, 0, NULL)",
            &[],
        )
        .unwrap();
    assert_eq!(count(&mut first, "_zsync_changes"), 1);
    first
        .exec(
            "INSERT INTO item (id, label, rank, done, meta) VALUES ('first', 'connection', 0, 0, NULL)",
            &[],
        )
        .unwrap();
    assert_eq!(count(&mut second, "_zsync_changes"), 2);

    drop(second);
    drop(first);
    std::fs::remove_file(path).unwrap();
}

// turso.cdc.schema-version-evolution + zero.change-processor.schema-metadata
#[test]
fn schema_drift_rolls_back_and_legacy_metadata_upgrade_is_idempotent() {
    let mut db = TestDb::memory();
    db.exec(
        "CREATE TABLE item (id TEXT PRIMARY KEY, label TEXT NOT NULL, rank REAL NOT NULL, done INTEGER NOT NULL, meta TEXT)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE _zsync_meta (lock INTEGER PRIMARY KEY, floor INTEGER NOT NULL)",
        &[],
    )
    .unwrap();
    db.exec("INSERT INTO _zsync_meta (lock, floor) VALUES (1, 0)", &[])
        .unwrap();
    let tables = item_tables();
    init_schema(&mut db, &tables).unwrap();
    init_schema(&mut db, &tables).unwrap();

    let mut drifted = item("drift", "unknown column");
    drifted["newColumn"] = json!(true);
    let batch = UpstreamBatch {
        watermark: 1,
        changes: vec![UpstreamChange {
            watermark: 1,
            table_name: "item".into(),
            op: "INSERT".into(),
            row_data: Some(row(drifted)),
            old_data: None,
        }],
    };
    let error = db
        .transaction(|tx| apply_upstream(tx, &tables, &batch))
        .unwrap_err();
    assert_eq!(error.status, 409);
    assert_eq!(sync_core::upstream_watermark(&mut db).unwrap(), 0);
    assert_eq!(count(&mut db, "item"), 0);
}

fn patch_id_list(response: &Value) -> Vec<String> {
    response["rowsPatch"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|op| op["op"] == "put")
        .filter_map(|op| op["value"]["id"].as_str().map(str::to_owned))
        .collect()
}

fn patch_ids(response: &Value) -> BTreeSet<String> {
    patch_id_list(response).into_iter().collect()
}

// electric.replication.transaction-fragmentation
#[test]
fn transaction_larger_than_pull_cap_converges_without_loss_or_duplication() {
    let mut host = Host::new(true);
    host.init();
    host.db
        .transaction::<_, EngineError>(|tx| {
            for i in 0..37 {
                tx.exec(
                    "INSERT INTO item (id, label, rank, done, meta) VALUES (?, ?, ?, 0, NULL)",
                    &[
                        SqlValue::Text(format!("fragment-{i:02}")),
                        SqlValue::Text(format!("row {i}")),
                        SqlValue::Integer(i),
                    ],
                )?;
            }
            Ok(())
        })
        .unwrap();

    host.caps = Caps {
        max_change_rows: 5,
        max_change_bytes: 1_000_000,
    };
    let mut cookie = json!(null);
    let mut seen = BTreeSet::new();
    let mut put_count = 0;
    for _ in 0..64 {
        let response = host.pull(cookie.clone(), "u1").unwrap();
        let fragment_ids = patch_id_list(&response);
        put_count += fragment_ids.len();
        seen.extend(fragment_ids);
        let next = response["cookie"].clone();
        if response.get("unchanged") == Some(&json!(true)) {
            break;
        }
        assert_ne!(next, cookie, "a non-empty fragment must advance the cookie");
        cookie = next;
    }
    let expected = (0..37)
        .map(|i| format!("fragment-{i:02}"))
        .collect::<BTreeSet<_>>();
    assert_eq!(seen, expected);
    assert_eq!(put_count, expected.len(), "fragmentation duplicated a put");

    host.caps = Caps::default();
    let fresh = host
        .pull_as("fresh", "fresh-group", json!(null), None, "u1")
        .unwrap();
    assert_eq!(patch_ids(&fresh), expected);
}
