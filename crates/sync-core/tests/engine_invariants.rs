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
            db.query("SELECT 1 FROM _zsync_changes", &[]).unwrap().len(),
            0
        );
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
