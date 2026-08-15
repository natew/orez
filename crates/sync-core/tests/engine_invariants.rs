mod common;

use std::time::{SystemTime, UNIX_EPOCH};

use common::{TestDb, item_tables};
use rusqlite::Connection;
use sync_core::{SyncDb, Transactor, init_schema, prune, watermark};

#[test]
fn watermark_survives_full_prune_and_reopen() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("orez-watermark-{nonce}.sqlite"));
    let tables = item_tables();

    let before = {
        let mut db = TestDb {
            conn: Connection::open(&path).unwrap(),
        };
        db.exec(
            "CREATE TABLE item_record (
                item_id TEXT PRIMARY KEY,
                item_label TEXT NOT NULL,
                sort_rank REAL NOT NULL,
                is_done INTEGER NOT NULL,
                metadata_json TEXT
            )",
            &[],
        )
        .unwrap();
        init_schema(&mut db, &tables).unwrap();
        db.exec(
            "INSERT INTO item_record VALUES ('durable', 'durable', 1, 0, NULL)",
            &[],
        )
        .unwrap();

        let before = db.transaction(watermark).unwrap();
        assert!(before > 0);
        db.transaction(|db| prune(db, 0)).unwrap();
        assert_eq!(
            db.query(
                "SELECT 1 FROM _zsync_log_segments WHERE endVersion = ?",
                &[sync_core::SqlValue::Integer(before)],
            )
            .unwrap()
            .len(),
            1
        );
        before
    };

    let after = {
        let mut db = TestDb {
            conn: Connection::open(&path).unwrap(),
        };
        init_schema(&mut db, &tables).unwrap();
        db.transaction(watermark).unwrap()
    };
    std::fs::remove_file(path).unwrap();

    assert_eq!(
        after, before,
        "watermark regressed after pruning and reopen"
    );
}

#[test]
fn init_retires_legacy_change_journal_without_regressing_its_watermark() {
    let mut db = TestDb {
        conn: Connection::open_in_memory().unwrap(),
    };
    db.exec(
        "CREATE TABLE item_record (
            item_id TEXT PRIMARY KEY,
            item_label TEXT NOT NULL,
            sort_rank REAL NOT NULL,
            is_done INTEGER NOT NULL,
            metadata_json TEXT
        )",
        &[],
    )
    .unwrap();
    init_schema(&mut db, &item_tables()).unwrap();
    db.exec(
        "CREATE TABLE _zsync_changes (
            watermark INTEGER PRIMARY KEY AUTOINCREMENT,
            tableName TEXT NOT NULL,
            op TEXT NOT NULL,
            pk TEXT
        )",
        &[],
    )
    .unwrap();
    db.exec(
        "INSERT INTO _zsync_changes (watermark, tableName, op, pk)
         VALUES (7, 'item', 'row', '{\"id\":\"legacy\"}')",
        &[],
    )
    .unwrap();

    init_schema(&mut db, &item_tables()).unwrap();

    assert!(
        db.query(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '_zsync_changes'",
            &[],
        )
        .unwrap()
        .is_empty()
    );
    assert_eq!(db.transaction(watermark).unwrap(), 7);
    assert_eq!(sync_core::pull::floor(&mut db).unwrap(), 7);
}
