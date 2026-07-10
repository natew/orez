mod common;

use serde_json::{Map, Value, json};
use sync_core::{SyncDb, Transactor, apply_upstream, apply_upstream_snapshot, init_schema};
use sync_core::{UpstreamBatch, UpstreamChange, UpstreamSnapshot};

use common::{TestDb, item_tables};

fn row(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn change(
    watermark: i64,
    op: &str,
    row_data: Option<Value>,
    old_data: Option<Value>,
) -> UpstreamChange {
    UpstreamChange {
        watermark,
        table_name: "item".into(),
        op: op.into(),
        row_data: row_data.map(row),
        old_data: old_data.map(row),
    }
}

fn setup() -> (TestDb, sync_core::Tables) {
    let mut db = TestDb::memory();
    let tables = item_tables();
    db.exec(
        "CREATE TABLE item (id TEXT PRIMARY KEY, label TEXT NOT NULL, rank REAL NOT NULL, done INTEGER NOT NULL, meta TEXT)",
        &[],
    )
    .unwrap();
    init_schema(&mut db, &tables).unwrap();
    (db, tables)
}

fn item(id: &str, label: &str) -> Value {
    json!({ "id": id, "label": label, "rank": 1, "done": false, "meta": null })
}

#[test]
fn applies_ordered_pages_and_is_watermark_idempotent() {
    let (mut db, tables) = setup();
    let batch = UpstreamBatch {
        watermark: 2,
        changes: vec![
            change(1, "INSERT", Some(item("a", "one")), None),
            change(2, "INSERT", Some(item("b", "two")), None),
        ],
    };
    let first = apply_upstream(&mut db, &tables, &batch).unwrap();
    assert_eq!(first.applied, 2);
    assert!(first.caught_up);
    let changes = db
        .query(
            "SELECT watermark FROM _zsync_changes ORDER BY watermark",
            &[],
        )
        .unwrap();
    assert_eq!(changes.len(), 2);

    let replay = apply_upstream(&mut db, &tables, &batch).unwrap();
    assert_eq!(replay.applied, 0);
    assert_eq!(replay.watermark, 2);
    assert_eq!(
        db.query("SELECT watermark FROM _zsync_changes", &[])
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn update_and_delete_use_full_images_and_advance_the_change_log() {
    let (mut db, tables) = setup();
    apply_upstream(
        &mut db,
        &tables,
        &UpstreamBatch {
            watermark: 1,
            changes: vec![change(1, "INSERT", Some(item("a", "one")), None)],
        },
    )
    .unwrap();
    apply_upstream(
        &mut db,
        &tables,
        &UpstreamBatch {
            watermark: 3,
            changes: vec![
                change(
                    2,
                    "UPDATE",
                    Some(item("a", "updated")),
                    Some(item("a", "one")),
                ),
                change(3, "DELETE", None, Some(item("a", "updated"))),
            ],
        },
    )
    .unwrap();
    assert!(db.query("SELECT id FROM item", &[]).unwrap().is_empty());
    // insert + update old/new + delete
    assert_eq!(
        db.query("SELECT watermark FROM _zsync_changes", &[])
            .unwrap()
            .len(),
        4
    );
}

#[test]
fn rejects_out_of_order_and_classifies_schema_drift_as_refresh() {
    let (mut db, tables) = setup();
    let ordered_batch = UpstreamBatch {
        watermark: 2,
        changes: vec![
            change(2, "INSERT", Some(item("b", "two")), None),
            change(1, "INSERT", Some(item("a", "one")), None),
        ],
    };
    let err = db
        .transaction(|db| apply_upstream(db, &tables, &ordered_batch))
        .unwrap_err();
    assert_eq!(err.status, 400);
    assert!(db.query("SELECT id FROM item", &[]).unwrap().is_empty());

    let mut unknown = item("x", "drift");
    unknown
        .as_object_mut()
        .unwrap()
        .insert("newColumn".into(), json!(1));
    let err = apply_upstream(
        &mut db,
        &tables,
        &UpstreamBatch {
            watermark: 1,
            changes: vec![change(1, "INSERT", Some(unknown), None)],
        },
    )
    .unwrap_err();
    assert_eq!(err.status, 409);
    assert!(err.message.starts_with("schema refresh required:"));
}

#[test]
fn init_migrates_legacy_engine_meta() {
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
    init_schema(&mut db, &item_tables()).unwrap();
    assert_eq!(sync_core::upstream_watermark(&mut db).unwrap(), 0);
}

#[test]
fn retention_gap_snapshot_replaces_rows_atomically() {
    let (mut db, tables) = setup();
    apply_upstream(
        &mut db,
        &tables,
        &UpstreamBatch {
            watermark: 1,
            changes: vec![change(1, "INSERT", Some(item("stale", "old")), None)],
        },
    )
    .unwrap();
    let snapshot = UpstreamSnapshot {
        watermark: 50,
        tables: [("item".into(), vec![row(item("fresh", "snapshot"))])]
            .into_iter()
            .collect(),
    };
    db.transaction(|db| apply_upstream_snapshot(db, &tables, &snapshot))
        .unwrap();
    let rows = db.query("SELECT id, label FROM item", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("id"),
        Some(&sync_core::SqlValue::Text("fresh".into()))
    );
    assert_eq!(sync_core::upstream_watermark(&mut db).unwrap(), 50);
}
