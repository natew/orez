mod common;

use serde_json::{Map, Value, json};
use sync_core::pull::Caps;
use sync_core::{
    SnapshotState, SqlValue, SyncDb, TableSpec, Tables, Transactor, UpstreamBatch, UpstreamChange,
    ZeroColumnType, apply_snapshot_changes, apply_snapshot_page, begin_snapshot_generation,
    finalize_snapshot_generation, handle_pull, init_schema, read_snapshot_progress,
};

use common::{TestDb, item_sql, item_tables};

fn row(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

fn item(id: &str, label: &str) -> Map<String, Value> {
    row(json!({ "id": id, "label": label, "rank": 1, "done": false, "meta": null }))
}

fn setup() -> (TestDb, Tables) {
    let mut db = TestDb::memory();
    let tables = item_tables();
    db.exec(
        &item_sql(
            "CREATE TABLE item (
            id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            rank REAL NOT NULL,
            done INTEGER NOT NULL,
            meta TEXT,
            CHECK (rank >= 0)
        ) STRICT",
        ),
        &[],
    )
    .unwrap();
    db.exec(
        &item_sql("CREATE UNIQUE INDEX item_label_unique ON item(label)"),
        &[],
    )
    .unwrap();
    init_schema(&mut db, &tables).unwrap();
    (db, tables)
}

fn pull(db: &mut TestDb, tables: &Tables, cookie: Value) -> Value {
    let body = json!({ "clientID": "c1", "clientGroupID": "g1", "cookie": cookie });
    db.transaction(|db| handle_pull(db, tables, 4096, None, Caps::default(), &body, "u1"))
        .unwrap()
}

fn integer(db: &mut TestDb, sql: &str) -> i64 {
    match db.query(&item_sql(sql), &[]).unwrap()[0].values[0] {
        SqlValue::Integer(value) => value,
        ref value => panic!("expected integer, got {value:?}"),
    }
}

#[test]
fn pages_commit_progress_and_resume_from_the_opaque_cursor() {
    let (mut db, tables) = setup();
    let begun = db
        .transaction(|db| begin_snapshot_generation(db, &tables, 50))
        .unwrap();
    assert_eq!(begun.generation, 1);
    assert_eq!(begun.start_watermark, 50);
    assert_eq!(begun.table.as_deref(), Some("item"));
    assert_eq!(begun.cursor, None);
    assert_eq!(begun.state, SnapshotState::Paging);

    let opaque = "eyJpZCI6ImIifQ==";
    let progress = db
        .transaction(|db| {
            apply_snapshot_page(
                db,
                &tables,
                begun.generation,
                "item",
                &[item("a", "one"), item("b", "two")],
                Some(opaque),
            )
        })
        .unwrap();
    assert_eq!(progress.cursor.as_deref(), Some(opaque));
    assert_eq!(progress.table.as_deref(), Some("item"));

    let resumed = read_snapshot_progress(&mut db).unwrap().unwrap();
    assert_eq!(resumed, progress);
    assert_eq!(integer(&mut db, "SELECT COUNT(*) FROM item"), 0);
    assert_eq!(
        integer(&mut db, "SELECT COUNT(*) FROM _zsync_stage_1_item"),
        2
    );

    let progress = db
        .transaction(|db| {
            apply_snapshot_page(
                db,
                &tables,
                begun.generation,
                "item",
                &[item("c", "three")],
                None,
            )
        })
        .unwrap();
    assert_eq!(progress.state, SnapshotState::CatchingUp);
    assert_eq!(progress.table, None);
    assert_eq!(progress.cursor, None);
    assert_eq!(progress.catchup_watermark, 50);
}

#[test]
fn progress_reads_fail_closed_when_the_progress_table_is_unreadable() {
    let (mut db, _) = setup();
    db.exec("DROP TABLE _zsync_snapshot_progress", &[]).unwrap();
    let error = read_snapshot_progress(&mut db).unwrap_err();
    assert_eq!(error.status, 500);
    assert!(error.message.contains("_zsync_snapshot_progress"));
}

#[test]
fn corrupt_incomplete_progress_is_an_error_instead_of_no_progress() {
    let (mut db, tables) = setup();
    db.transaction(|db| begin_snapshot_generation(db, &tables, 5))
        .unwrap();
    db.exec("PRAGMA ignore_check_constraints = ON", &[])
        .unwrap();
    db.exec(
        "UPDATE _zsync_snapshot_progress SET active = NULL WHERE generation = 1",
        &[],
    )
    .unwrap();
    let error = read_snapshot_progress(&mut db).unwrap_err();
    assert_eq!(error.status, 500);
    assert!(error.message.contains("not marked active"));
}

#[test]
fn begin_rejects_foreign_keys_without_mutating_the_live_schema() {
    let mut db = TestDb::memory();
    db.exec("PRAGMA foreign_keys = ON", &[]).unwrap();
    db.exec("CREATE TABLE parent (id TEXT PRIMARY KEY)", &[])
        .unwrap();
    db.exec(
        "CREATE TABLE child (
            id TEXT PRIMARY KEY,
            parentId TEXT NOT NULL REFERENCES parent(id)
        )",
        &[],
    )
    .unwrap();
    let tables = Tables::new()
        .with(
            "parent",
            TableSpec {
                columns: vec![("id".into(), ZeroColumnType::String)],
                primary_key: vec!["id".into()],
            },
        )
        .with(
            "child",
            TableSpec {
                columns: vec![
                    ("id".into(), ZeroColumnType::String),
                    ("parentId".into(), ZeroColumnType::String),
                ],
                primary_key: vec!["id".into()],
            },
        );
    init_schema(&mut db, &tables).unwrap();

    let error = db
        .transaction(|db| begin_snapshot_generation(db, &tables, 10))
        .unwrap_err();
    assert_eq!(error.status, 409);
    assert!(error.message.contains("child"));
    assert!(error.message.contains("remove REFERENCES constraints"));
    assert!(read_snapshot_progress(&mut db).unwrap().is_none());
    assert!(
        db.query(
            "SELECT name FROM sqlite_schema WHERE name GLOB '_zsync_stage_*'",
            &[],
        )
        .unwrap()
        .is_empty()
    );
    assert!(
        db.exec(
            "INSERT INTO child (id, parentId) VALUES ('orphan', 'missing')",
            &[],
        )
        .is_err(),
        "rejecting the rebuild must leave live FK enforcement unchanged"
    );
}

#[test]
fn invalid_page_rolls_back_rows_and_cursor_together() {
    let (mut db, tables) = setup();
    let generation = db
        .transaction(|db| begin_snapshot_generation(db, &tables, 5))
        .unwrap()
        .generation;
    let mut invalid = item("bad", "bad");
    invalid.insert("unknown".into(), json!(true));
    let error = db
        .transaction(|db| {
            apply_snapshot_page(
                db,
                &tables,
                generation,
                "item",
                &[item("good", "good"), invalid],
                Some("must-not-commit"),
            )
        })
        .unwrap_err();
    assert_eq!(error.status, 409);
    assert_eq!(
        integer(&mut db, "SELECT COUNT(*) FROM _zsync_stage_1_item"),
        0
    );
    assert_eq!(
        read_snapshot_progress(&mut db).unwrap().unwrap().cursor,
        None
    );
}

#[test]
fn catchup_overlap_and_unseen_delete_converge_then_invalidate_old_clients() {
    let (mut db, tables) = setup();
    db.exec(
        &item_sql(
            "INSERT INTO item (id, label, rank, done, meta) VALUES ('old', 'live-old', 1, 0, NULL)",
        ),
        &[],
    )
    .unwrap();
    let old_pull = pull(&mut db, &tables, Value::Null);
    let old_cookie = old_pull["cookie"].clone();
    let changes_before_paging = integer(&mut db, "SELECT COUNT(*) FROM _zsync_changes");

    let generation = db
        .transaction(|db| begin_snapshot_generation(db, &tables, 100))
        .unwrap()
        .generation;
    db.transaction(|db| {
        apply_snapshot_page(
            db,
            &tables,
            generation,
            "item",
            &[item("a", "page-old")],
            None,
        )
    })
    .unwrap();
    assert_eq!(
        integer(&mut db, "SELECT COUNT(*) FROM _zsync_changes"),
        changes_before_paging,
        "staging writes must not pollute the live client change log"
    );

    let batch = UpstreamBatch {
        watermark: 102,
        changes: vec![
            UpstreamChange {
                watermark: 101,
                table_name: "item".into(),
                op: "UPDATE".into(),
                row_data: Some(item("a", "catchup-new")),
                old_data: Some(item("a", "page-old")),
            },
            UpstreamChange {
                watermark: 102,
                table_name: "item".into(),
                op: "DELETE".into(),
                row_data: None,
                old_data: Some(item("never-staged", "gone")),
            },
        ],
    };
    let caught_up = db
        .transaction(|db| apply_snapshot_changes(db, &tables, generation, &batch))
        .unwrap();
    assert!(caught_up.caught_up);
    assert_eq!(caught_up.watermark, 102);
    assert_eq!(sync_core::upstream_watermark(&mut db).unwrap(), 0);
    assert_eq!(integer(&mut db, "SELECT COUNT(*) FROM item"), 1);
    assert_eq!(
        integer(
            &mut db,
            "SELECT COUNT(*) FROM _zsync_stage_1_item WHERE id = 'never-staged'"
        ),
        0
    );

    db.transaction(|db| finalize_snapshot_generation(db, &tables, generation, 102))
        .unwrap();
    assert!(read_snapshot_progress(&mut db).unwrap().is_none());
    assert_eq!(sync_core::upstream_watermark(&mut db).unwrap(), 102);
    assert_eq!(
        db.query(&item_sql("SELECT label FROM item WHERE id = 'a'"), &[])
            .unwrap()[0]
            .values[0],
        SqlValue::Text("catchup-new".into())
    );

    let refreshed = pull(&mut db, &tables, old_cookie);
    let patch = refreshed["rowsPatch"].as_array().unwrap();
    assert_eq!(patch[0], json!({ "op": "clear" }));
    assert_eq!(patch.len(), 2);
    assert_eq!(patch[1]["value"]["id"], "a");
    assert_eq!(patch[1]["value"]["label"], "catchup-new");

    db.exec(
        &item_sql("INSERT INTO item (id, label, rank, done, meta) VALUES ('after', 'after-cutover', 1, 0, NULL)"),
        &[],
    )
    .unwrap();
    assert!(
        integer(&mut db, "SELECT COUNT(*) FROM _zsync_changes") > changes_before_paging + 1,
        "live-table triggers must be restored after rename"
    );
    let duplicate = db.exec(
        &item_sql("INSERT INTO item (id, label, rank, done, meta) VALUES ('duplicate', 'after-cutover', 1, 0, NULL)"),
        &[],
    );
    assert!(
        duplicate.is_err(),
        "the staged secondary UNIQUE index must survive cutover"
    );
}

fn two_tables() -> Tables {
    let string_column = |name: &str| (name.to_string(), ZeroColumnType::String);
    Tables::new()
        .with(
            "a",
            TableSpec {
                columns: vec![string_column("id"), string_column("value")],
                primary_key: vec!["id".into()],
            },
        )
        .with(
            "b",
            TableSpec {
                columns: vec![string_column("id"), string_column("value")],
                primary_key: vec!["id".into()],
            },
        )
}

#[test]
fn failed_cutover_rolls_back_every_table_and_the_epoch_change() {
    let mut db = TestDb::memory();
    let tables = two_tables();
    db.exec(
        "CREATE TABLE a (id TEXT PRIMARY KEY, value TEXT NOT NULL)",
        &[],
    )
    .unwrap();
    db.exec(
        "CREATE TABLE b (id TEXT PRIMARY KEY, value TEXT NOT NULL)",
        &[],
    )
    .unwrap();
    init_schema(&mut db, &tables).unwrap();
    db.exec("INSERT INTO a VALUES ('old-a', 'old')", &[])
        .unwrap();
    db.exec("INSERT INTO b VALUES ('old-b', 'old')", &[])
        .unwrap();
    let floor_before = sync_core::pull::floor(&mut db).unwrap();

    let generation = db
        .transaction(|db| begin_snapshot_generation(db, &tables, 10))
        .unwrap()
        .generation;
    db.transaction(|db| {
        apply_snapshot_page(
            db,
            &tables,
            generation,
            "a",
            &[row(json!({ "id": "new-a", "value": "new" }))],
            None,
        )
    })
    .unwrap();
    db.transaction(|db| {
        apply_snapshot_page(
            db,
            &tables,
            generation,
            "b",
            &[row(json!({ "id": "new-b", "value": "new" }))],
            None,
        )
    })
    .unwrap();
    db.exec("DROP TABLE _zsync_stage_1_b", &[]).unwrap();

    let error = db
        .transaction(|db| finalize_snapshot_generation(db, &tables, generation, 10))
        .unwrap_err();
    assert_eq!(error.status, 500);
    assert_eq!(
        db.query("SELECT id FROM a", &[]).unwrap()[0].values[0],
        SqlValue::Text("old-a".into())
    );
    assert_eq!(
        db.query("SELECT id FROM b", &[]).unwrap()[0].values[0],
        SqlValue::Text("old-b".into())
    );
    assert_eq!(
        db.query("SELECT id FROM _zsync_stage_1_a", &[]).unwrap()[0].values[0],
        SqlValue::Text("new-a".into())
    );
    assert_eq!(sync_core::pull::floor(&mut db).unwrap(), floor_before);
    assert!(read_snapshot_progress(&mut db).unwrap().is_some());
}

#[test]
fn abandoned_generations_are_cleaned_in_bounded_batches() {
    let (mut db, tables) = setup();
    let first = db
        .transaction(|db| begin_snapshot_generation(db, &tables, 1))
        .unwrap();
    let rows = (0..2001)
        .map(|index| item(&format!("id-{index:04}"), &format!("label-{index:04}")))
        .collect::<Vec<_>>();
    db.transaction(|db| {
        apply_snapshot_page(
            db,
            &tables,
            first.generation,
            "item",
            &rows,
            Some("not-done"),
        )
    })
    .unwrap();

    let second = db
        .transaction(|db| begin_snapshot_generation(db, &tables, 2))
        .unwrap();
    assert_ne!(first.generation, second.generation);
    assert_eq!(
        integer(&mut db, "SELECT COUNT(*) FROM _zsync_stage_1_item"),
        2001
    );

    db.transaction(|db| {
        apply_snapshot_page(
            db,
            &tables,
            second.generation,
            "item",
            &[],
            Some("still-paging"),
        )
    })
    .unwrap();
    assert_eq!(
        integer(&mut db, "SELECT COUNT(*) FROM _zsync_stage_1_item"),
        1,
        "one engine call sweeps only one bounded cleanup batch"
    );

    db.transaction(|db| {
        apply_snapshot_page(
            db,
            &tables,
            second.generation,
            "item",
            &[],
            Some("still-paging-2"),
        )
    })
    .unwrap();
    assert!(
        db.query(
            "SELECT name FROM sqlite_schema WHERE name = '_zsync_stage_1_item'",
            &[],
        )
        .unwrap()
        .is_empty()
    );
    assert!(
        db.query(
            "SELECT generation FROM _zsync_snapshot_progress WHERE generation = 1",
            &[],
        )
        .unwrap()
        .is_empty()
    );
}
