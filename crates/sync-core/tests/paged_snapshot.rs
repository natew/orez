mod common;

use serde_json::{Map, Value, json};
use sync_core::{
    SnapshotState, SqlValue, SyncDb, TableSpec, Tables, Transactor, UpstreamBatch, UpstreamChange,
    ZeroColumnType, apply_snapshot_changes, apply_snapshot_page, begin_snapshot_generation,
    finalize_snapshot_generation, init_schema, read_snapshot_progress,
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
    sync_core::query::init_query_schema(&mut db).unwrap();
    (db, tables)
}

fn pull(db: &mut TestDb, tables: &Tables, cookie: Value) -> Value {
    // every pull re-desires the all-items query; re-puts of the same hash and
    // version are idempotent, so this serves the whole `item` surface.
    let body = json!({
        "clientID": "c1", "clientGroupID": "g1", "cookie": cookie,
        "queries": { "version": 1, "patch": [
            { "op": "put", "hash": "q_all", "ast": { "table": "item" } },
        ]},
    });
    db.transaction(|db| sync_core::query::handle_query_pull(db, tables, 4096, &body, "u1"))
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
                &Default::default(),
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
                &Default::default(),
            )
        })
        .unwrap();
    assert_eq!(progress.state, SnapshotState::CatchingUp);
    assert_eq!(progress.table, None);
    assert_eq!(progress.cursor, None);
    assert_eq!(progress.catchup_watermark, 50);
}

#[test]
fn schema_change_abandons_the_active_generation_before_resume() {
    let (mut db, tables) = setup();
    let original = db
        .transaction(|db| begin_snapshot_generation(db, &tables, 50))
        .unwrap();
    init_schema(&mut db, &tables).unwrap();
    assert_eq!(read_snapshot_progress(&mut db).unwrap(), Some(original));

    db.exec("ALTER TABLE item_record ADD COLUMN owner_name TEXT", &[])
        .unwrap();
    let expanded_tables = Tables::from_zero_schema(&json!({
        "tables": {
            "item": {
                "serverName": "item_record",
                "columns": {
                    "id": { "type": "string", "serverName": "item_id" },
                    "label": { "type": "string", "serverName": "item_label" },
                    "rank": { "type": "number", "serverName": "sort_rank" },
                    "done": { "type": "boolean", "serverName": "is_done" },
                    "meta": { "type": "json", "serverName": "metadata_json" },
                    "owner": { "type": "string", "serverName": "owner_name", "optional": true },
                },
                "primaryKey": ["id"],
            }
        }
    }))
    .unwrap();

    init_schema(&mut db, &expanded_tables).unwrap();

    assert!(read_snapshot_progress(&mut db).unwrap().is_none());
    let abandoned = db
        .query(
            "SELECT state, active FROM _zsync_snapshot_progress WHERE generation = 1",
            &[],
        )
        .unwrap();
    assert_eq!(
        abandoned[0].get("state"),
        Some(&SqlValue::Text("abandoned".into()))
    );
    assert_eq!(abandoned[0].get("active"), Some(&SqlValue::Null));

    let replacement = db
        .transaction(|db| begin_snapshot_generation(db, &expanded_tables, 60))
        .unwrap();
    assert_eq!(replacement.generation, 2);
    assert_eq!(
        db.query(
            "SELECT name FROM pragma_table_info('_zsync_stage_2_item')
             WHERE name = 'owner_name'",
            &[],
        )
        .unwrap()
        .len(),
        1
    );
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
fn abandoned_generation_cleanup_crossing_a_decimal_width_stays_numeric() {
    let (mut db, tables) = setup();
    db.exec(
        "INSERT INTO _zsync_snapshot_cleanup (generation, stageName)
         VALUES (9, 'missing-nine'), (10, 'missing-ten')",
        &[],
    )
    .unwrap();

    db.transaction(|db| begin_snapshot_generation(db, &tables, 5))
        .unwrap();

    let remaining = db
        .query(
            "SELECT generation FROM _zsync_snapshot_cleanup ORDER BY generation",
            &[],
        )
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].values[0], SqlValue::Integer(10));
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
                encrypted_columns: Default::default(),
                encrypted_physical_columns: Default::default(),
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
                encrypted_columns: Default::default(),
                encrypted_physical_columns: Default::default(),
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
                &Default::default(),
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
    let watermark_before_paging = sync_core::watermark(&mut db).unwrap();

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
            &Default::default(),
        )
    })
    .unwrap();
    assert_eq!(
        sync_core::watermark(&mut db).unwrap(),
        watermark_before_paging,
        "staging writes must not pollute the live client change log"
    );

    let batch = UpstreamBatch {
        watermark: 102,
        optional_columns: Default::default(),
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
    assert_eq!(patch[1]["value"]["item_id"], "a");
    assert_eq!(patch[1]["value"]["item_label"], "catchup-new");

    let before_live_insert = sync_core::watermark(&mut db).unwrap();
    db.exec(
        &item_sql("INSERT INTO item (id, label, rank, done, meta) VALUES ('after', 'after-cutover', 1, 0, NULL)"),
        &[],
    )
    .unwrap();
    assert!(
        sync_core::watermark(&mut db).unwrap() > before_live_insert,
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

fn live_index_names(db: &mut TestDb) -> Vec<String> {
    db.query(
        "SELECT name FROM sqlite_schema
         WHERE type = 'index' AND tbl_name = 'item_record' AND sql IS NOT NULL
         ORDER BY name",
        &[],
    )
    .unwrap()
    .iter()
    .map(|row| match &row.values[0] {
        SqlValue::Text(name) => name.clone(),
        value => panic!("expected index name, got {value:?}"),
    })
    .collect()
}

#[test]
fn cutover_restores_live_index_names_so_generations_never_compound_them() {
    let (mut db, tables) = setup();
    // a clone an older engine left under its stage name after cutover
    db.exec(
        &item_sql("CREATE INDEX _zsync_stage_9_item_idx_0 ON item(label)"),
        &[],
    )
    .unwrap();
    for start in [10, 20] {
        let generation = db
            .transaction(|db| begin_snapshot_generation(db, &tables, start))
            .unwrap()
            .generation;
        db.transaction(|db| {
            apply_snapshot_page(
                db,
                &tables,
                generation,
                "item",
                &[item("a", "one")],
                None,
                &Default::default(),
            )
        })
        .unwrap();
        db.transaction(|db| finalize_snapshot_generation(db, &tables, generation, start))
            .unwrap();
        assert_eq!(
            live_index_names(&mut db),
            vec!["item_label_unique".to_string()],
            "generation {generation} must leave exactly the live index set"
        );
    }
    let duplicate = db.exec(
        &item_sql(
            "INSERT INTO item (id, label, rank, done, meta) VALUES ('dup', 'one', 1, 0, NULL)",
        ),
        &[],
    );
    assert!(
        duplicate.is_err(),
        "the rebuilt UNIQUE index must still enforce"
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
                encrypted_columns: Default::default(),
                encrypted_physical_columns: Default::default(),
            },
        )
        .with(
            "b",
            TableSpec {
                columns: vec![string_column("id"), string_column("value")],
                primary_key: vec!["id".into()],
                encrypted_columns: Default::default(),
                encrypted_physical_columns: Default::default(),
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
            &Default::default(),
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
            &Default::default(),
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
            &Default::default(),
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
            &Default::default(),
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
            &Default::default(),
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

fn finish_table_bootstrap(
    db: &mut TestDb,
    tables: &Tables,
    selected: &[String],
    generation: i64,
    watermark: i64,
) {
    for _ in 0..16 {
        init_schema(db, tables).unwrap();
        if db
            .transaction(|db| {
                sync_core::finalize_table_bootstrap(db, tables, selected, generation, watermark)
            })
            .unwrap()
            .caught_up
        {
            return;
        }
    }
    panic!("small fixture bootstrap did not finish within its bounded batches");
}

#[test]
fn restored_table_bootstrap_preserves_existing_clients_and_catches_concurrent_changes() {
    use sync_core::{apply_upstream, finalize_table_bootstrap};
    let (mut db, mut tables) = setup();
    let baseline = UpstreamBatch {
        watermark: 10,
        changes: vec![UpstreamChange {
            watermark: 10,
            table_name: "item".into(),
            op: "INSERT".into(),
            row_data: Some(item("existing", "unchanged")),
            old_data: None,
        }],
        optional_columns: Default::default(),
    };
    db.transaction(|db| apply_upstream(db, &tables, &baseline))
        .unwrap();
    let first = pull(&mut db, &tables, Value::Null);
    db.exec(
        "CREATE TABLE counter (id TEXT PRIMARY KEY, value REAL NOT NULL, owner TEXT NOT NULL)",
        &[],
    )
    .unwrap();
    tables.push(
        "counter",
        TableSpec {
            columns: vec![
                ("id".into(), ZeroColumnType::String),
                ("value".into(), ZeroColumnType::Number),
                ("owner".into(), ZeroColumnType::String),
            ],
            primary_key: vec!["id".into()],
            encrypted_columns: Default::default(),
            encrypted_physical_columns: Default::default(),
        },
    );
    init_schema(&mut db, &tables).unwrap();
    let counter =
        |id: &str, value: i64, owner: &str| row(json!({"id": id, "value": value, "owner": owner}));
    let selected = vec!["counter".into()];
    let begun = db
        .transaction(|db| begin_snapshot_generation(db, &tables, 10))
        .unwrap();
    db.transaction(|db| {
        apply_snapshot_page(
            db,
            &tables,
            begun.generation,
            "counter",
            &[counter("a", 42, "u1")],
            None,
            &Default::default(),
        )
    })
    .unwrap();
    // an eviction runs the normal full-schema initializer before resuming.
    init_schema(&mut db, &tables).unwrap();
    assert!(read_snapshot_progress(&mut db).unwrap().is_some());
    db.transaction(|db| {
        apply_snapshot_page(
            db,
            &tables,
            begun.generation,
            "item",
            &[],
            None,
            &Default::default(),
        )
    })
    .unwrap();
    let first_batch = db
        .transaction(|db| finalize_table_bootstrap(db, &tables, &selected, begun.generation, 10))
        .unwrap();
    assert!(!first_batch.caught_up);
    init_schema(&mut db, &tables).unwrap();
    finish_table_bootstrap(&mut db, &tables, &selected, begun.generation, 10);
    let reader = |db: &mut TestDb, cookie: Value| {
        let mut body = json!({"clientID":"c2","clientGroupID":"g2","cookie":cookie});
        if cookie.is_null() {
            body["queries"] = json!({"version":1,"patch":[{"op":"put","hash":"counter-own","ast":{"table":"counter","where":{
                "type":"simple","op":"=","left":{"type":"column","name":"owner"},"right":{"type":"literal","value":"u1"}
            }}}]});
        }
        db.transaction(|db| sync_core::query::handle_query_pull(db, &tables, 4096, &body, "u1"))
            .unwrap()
    };
    let initial = reader(&mut db, Value::Null);
    assert!(
        initial["rowsPatch"]
            .as_array()
            .unwrap()
            .iter()
            .any(|patch| patch["value"]["value"] == 42)
    );
    let reconnected = reader(&mut db, initial["cookie"].clone());
    assert_eq!(
        reconnected,
        json!({"cookie": initial["cookie"], "unchanged": true})
    );
    let existing_reader = |db: &mut TestDb, cookie: Value| {
        let body = json!({"clientID":"c1","clientGroupID":"g1","cookie":cookie});
        db.transaction(|db| sync_core::query::handle_query_pull(db, &tables, 4096, &body, "u1"))
            .unwrap()
    };
    let existing = existing_reader(&mut db, first["cookie"].clone());
    assert!(existing["rowsPatch"].as_array().unwrap().is_empty());
    let next = db
        .transaction(|db| begin_snapshot_generation(db, &tables, 10))
        .unwrap();
    db.transaction(|db| {
        apply_snapshot_page(
            db,
            &tables,
            next.generation,
            "counter",
            &[counter("a", 42, "u1")],
            None,
            &Default::default(),
        )
    })
    .unwrap();
    db.transaction(|db| {
        apply_snapshot_page(
            db,
            &tables,
            next.generation,
            "item",
            &[],
            None,
            &Default::default(),
        )
    })
    .unwrap();
    let change = |watermark, op: &str, new, old| UpstreamChange {
        watermark,
        table_name: "counter".into(),
        op: op.into(),
        row_data: new,
        old_data: old,
    };
    let raced = UpstreamBatch {
        watermark: 14,
        changes: vec![
            change(
                11,
                "UPDATE",
                Some(counter("a", 43, "u1")),
                Some(counter("a", 42, "u1")),
            ),
            change(
                12,
                "UPDATE",
                Some(counter("renamed", 43, "u1")),
                Some(counter("a", 43, "u1")),
            ),
            change(13, "INSERT", Some(counter("temporary", 1, "u1")), None),
            change(14, "DELETE", None, Some(counter("temporary", 1, "u1"))),
        ],
        optional_columns: Default::default(),
    };
    db.transaction(|db| {
        let live = apply_upstream(db, &tables, &raced)?;
        let staged = apply_snapshot_changes(db, &tables, next.generation, &raced)?;
        assert!(live.caught_up && staged.caught_up);
        finalize_table_bootstrap(db, &tables, &selected, next.generation, 14)
    })
    .unwrap();
    finish_table_bootstrap(&mut db, &tables, &selected, next.generation, 14);
    assert_eq!(integer(&mut db, "SELECT COUNT(*) FROM item"), 1);
    let changes = reader(&mut db, reconnected["cookie"].clone());
    assert!(
        !changes["rowsPatch"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["op"] == "clear")
    );
    assert!(
        changes["rowsPatch"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["value"]["id"] == "renamed")
    );
    let existing_after = existing_reader(&mut db, existing["cookie"].clone());
    assert!(
        !existing_after["rowsPatch"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["op"] == "clear")
    );
    assert!(existing_after["rowsPatch"].as_array().unwrap().is_empty());
    let revoke = UpstreamBatch {
        watermark: 15,
        changes: vec![change(
            15,
            "UPDATE",
            Some(counter("renamed", 43, "u2")),
            Some(counter("renamed", 43, "u1")),
        )],
        optional_columns: Default::default(),
    };
    db.transaction(|db| apply_upstream(db, &tables, &revoke))
        .unwrap();
    let revoked = reader(&mut db, changes["cookie"].clone());
    assert!(
        revoked["rowsPatch"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["op"] == "del")
    );
    let final_delete = UpstreamBatch {
        watermark: 16,
        changes: vec![change(
            16,
            "DELETE",
            None,
            Some(counter("renamed", 43, "u2")),
        )],
        optional_columns: Default::default(),
    };
    db.transaction(|db| apply_upstream(db, &tables, &final_delete))
        .unwrap();
    assert_eq!(integer(&mut db, "SELECT COUNT(*) FROM counter"), 0);
}

#[test]
fn table_bootstrap_publication_bounds_writes_and_resumes_without_rewriting_equal_rows() {
    use sync_core::{apply_upstream, finalize_table_bootstrap};
    let (mut db, tables) = setup();
    let selected = vec!["item".into()];
    let source = (0..2_501)
        .map(|i| item(&format!("id-{i:04}"), &format!("label-{i:04}")))
        .collect::<Vec<_>>();
    db.transaction(|db| {
        apply_upstream(
            db,
            &tables,
            &UpstreamBatch {
                watermark: 10,
                changes: vec![UpstreamChange {
                    watermark: 10,
                    table_name: "item".into(),
                    op: "INSERT".into(),
                    row_data: Some(source[0].clone()),
                    old_data: None,
                }],
                optional_columns: Default::default(),
            },
        )
    })
    .unwrap();
    let begun = db
        .transaction(|db| begin_snapshot_generation(db, &tables, 10))
        .unwrap();
    for (index, page) in source.chunks(1_000).enumerate() {
        db.transaction(|db| {
            apply_snapshot_page(
                db,
                &tables,
                begun.generation,
                "item",
                page,
                if index < 2 { Some("next") } else { None },
                &Default::default(),
            )
        })
        .unwrap();
    }
    let before = db.conn.total_changes();
    let first = db
        .transaction(|db| finalize_table_bootstrap(db, &tables, &selected, begun.generation, 10))
        .unwrap();
    assert_eq!(first.applied, 0);
    assert!(!first.caught_up);
    assert_eq!(
        db.conn.total_changes() - before,
        1,
        "an equal live row writes only the publication cursor"
    );
    let mut counts = Vec::new();
    for _ in 0..3 {
        // reconstruct the engine between every publication transaction.
        init_schema(&mut db, &tables).unwrap();
        let before = db.conn.total_changes();
        let result = db
            .transaction(|db| {
                finalize_table_bootstrap(db, &tables, &selected, begun.generation, 10)
            })
            .unwrap();
        let written = db.conn.total_changes() - before;
        assert!(result.applied <= 1_000);
        assert!(
            written <= 3 * result.applied as u64 + 4,
            "{written} physical changes for {} published rows",
            result.applied
        );
        counts.push(result.applied);
        assert!(!result.caught_up);
    }
    assert_eq!(counts, vec![999, 1_000, 501]);
    for (written_rows, complete) in [(2_000, false), (503, true)] {
        init_schema(&mut db, &tables).unwrap();
        let before = db.conn.total_changes();
        let result = db
            .transaction(|db| {
                finalize_table_bootstrap(db, &tables, &selected, begun.generation, 10)
            })
            .unwrap();
        assert_eq!(result.applied, 0);
        assert_eq!(result.caught_up, complete);
        assert_eq!(db.conn.total_changes() - before, written_rows);
    }
    assert_eq!(
        integer(
            &mut db,
            "SELECT COUNT(*) FROM sqlite_schema WHERE name GLOB '_zsync_stage_*'"
        ),
        0
    );
    assert_eq!(
        integer(&mut db, "SELECT COUNT(*) FROM _zsync_snapshot_cleanup"),
        0
    );
    assert_eq!(integer(&mut db, "SELECT COUNT(*) FROM item"), 2_501);
    assert!(read_snapshot_progress(&mut db).unwrap().is_none());
}
