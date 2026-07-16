mod common;

use serde_json::{Map, Value, json};
use sync_core::value::zero_row;
use sync_core::{SyncDb, Transactor, apply_upstream, apply_upstream_snapshot, init_schema};
use sync_core::{UpstreamBatch, UpstreamChange, UpstreamSnapshot};

use common::{TestDb, item_sql, item_tables};

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
        &item_sql("CREATE TABLE item (id TEXT PRIMARY KEY, label TEXT NOT NULL, rank REAL NOT NULL, done INTEGER NOT NULL, meta TEXT)"),
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
fn upstream_json_values_persist_and_hydrate_with_their_original_types() {
    let (mut db, tables) = setup();
    let json_values = [
        json!({ "nested": { "tags": ["a", 2, true] } }),
        json!([1, "two", null]),
        json!("42"),
        json!("true"),
        json!("null"),
        json!("{\"looks\":\"encoded\"}"),
        json!(42.5),
        json!(true),
    ];
    let changes = json_values
        .iter()
        .enumerate()
        .map(|(index, meta)| {
            let mut value = item(&format!("json-{index}"), "json round trip");
            value["meta"] = meta.clone();
            change(index as i64 + 1, "INSERT", Some(value), None)
        })
        .collect();

    apply_upstream(
        &mut db,
        &tables,
        &UpstreamBatch {
            watermark: json_values.len() as i64,
            changes,
        },
    )
    .unwrap();

    let rows = db
        .query(
            "SELECT item_id AS id, item_label AS label, sort_rank AS rank,
             is_done AS done, metadata_json AS meta
             FROM item_record ORDER BY item_id",
            &[],
        )
        .unwrap();
    let spec = tables.get("item").unwrap();
    for (row, expected) in rows.iter().zip(json_values) {
        assert_eq!(
            row.get("meta"),
            Some(&sync_core::SqlValue::Text(
                serde_json::to_string(&expected).unwrap()
            ))
        );
        assert_eq!(zero_row(spec, row).unwrap()["meta"], expected);
    }
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
    assert!(
        db.query(&item_sql("SELECT id FROM item"), &[])
            .unwrap()
            .is_empty()
    );
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
    assert!(
        db.query(&item_sql("SELECT id FROM item"), &[])
            .unwrap()
            .is_empty()
    );

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
        &item_sql("CREATE TABLE item (id TEXT PRIMARY KEY, label TEXT NOT NULL, rank REAL NOT NULL, done INTEGER NOT NULL, meta TEXT)"),
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
    let rows = db
        .query(
            "SELECT item_id AS id, item_label AS label FROM item_record",
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("id"),
        Some(&sync_core::SqlValue::Text("fresh".into()))
    );
    assert_eq!(sync_core::upstream_watermark(&mut db).unwrap(), 50);
}

#[test]
fn snapshot_ignores_tables_absent_from_host_schema() {
    let (mut db, tables) = setup();
    // the upstream is authoritative for the full app schema; this host models
    // only `item`. a server-only table present in the snapshot must be ignored
    // so a subset replica can still rebuild, instead of failing the rebuild.
    let snapshot = UpstreamSnapshot {
        watermark: 42,
        tables: [
            ("item".into(), vec![row(item("fresh", "snapshot"))]),
            (
                "user".into(),
                vec![row(json!({ "id": "u1", "email": "a@b.c" }))],
            ),
        ]
        .into_iter()
        .collect(),
    };
    let result = db
        .transaction(|db| apply_upstream_snapshot(db, &tables, &snapshot))
        .unwrap();
    // only the modeled table's row counts as applied; `user` is skipped.
    assert_eq!(result.applied, 1);
    let rows = db
        .query("SELECT item_id AS id FROM item_record", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("id"),
        Some(&sync_core::SqlValue::Text("fresh".into()))
    );
    assert_eq!(sync_core::upstream_watermark(&mut db).unwrap(), 42);
}

#[test]
fn changes_skip_tables_absent_from_host_schema() {
    let (mut db, tables) = setup();
    // a change for a table this host does not model is consumed (the watermark
    // advances past it) but not materialized, so ingest is never blocked by a
    // server-only table like `user` flowing through the feed.
    let unknown = UpstreamChange {
        watermark: 1,
        table_name: "user".into(),
        op: "INSERT".into(),
        row_data: Some(row(json!({ "id": "u1", "email": "a@b.c" }))),
        old_data: None,
    };
    let result = apply_upstream(
        &mut db,
        &tables,
        &UpstreamBatch {
            watermark: 1,
            changes: vec![unknown],
        },
    )
    .unwrap();
    assert_eq!(result.applied, 0);
    assert!(result.caught_up);
    // the watermark advanced past the skipped change, so ingest keeps flowing.
    assert_eq!(sync_core::upstream_watermark(&mut db).unwrap(), 1);
    // and a later change on a modeled table still applies normally.
    apply_upstream(
        &mut db,
        &tables,
        &UpstreamBatch {
            watermark: 2,
            changes: vec![change(2, "INSERT", Some(item("a", "one")), None)],
        },
    )
    .unwrap();
    assert_eq!(
        db.query(&item_sql("SELECT id FROM item"), &[])
            .unwrap()
            .len(),
        1
    );
    assert_eq!(sync_core::upstream_watermark(&mut db).unwrap(), 2);
}
