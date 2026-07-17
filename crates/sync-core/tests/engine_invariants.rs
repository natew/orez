mod common;

use std::time::{SystemTime, UNIX_EPOCH};

use common::{Host, TestDb, item_tables};
use rusqlite::Connection;
use serde_json::{Value, json};
use sync_core::pull::Caps;
use sync_core::{SyncDb, Transactor, init_schema, prune, watermark};

fn cookie(response: &Value) -> i64 {
    response["cookie"].as_i64().unwrap()
}

fn patch(response: &Value) -> Vec<Value> {
    response["rowsPatch"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

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
fn capped_diff_does_not_ack_before_the_mutation_effect() {
    let mut host = Host::new(true);
    host.init();
    let start = cookie(
        &host
            .pull_as("observer", "g1", json!(null), None, "u1")
            .unwrap(),
    );

    host.push_one(
        "item.put",
        json!({
            "id": "effect",
            "label": "confirmed",
            "rank": 1,
            "done": false,
            "meta": null,
        }),
        "writer",
        "g1",
        1,
        "u1",
    )
    .unwrap();
    host.caps = Caps {
        max_change_rows: 1,
        max_change_bytes: usize::MAX,
    };

    let effect = host
        .pull_as("observer", "g1", json!(start), None, "u1")
        .unwrap();
    assert_eq!(effect["lastMutationIDChanges"], json!({}));
    assert_eq!(
        patch(&effect),
        vec![json!({
            "op": "put",
            "tableName": "item_record",
            "value": {
                "item_id": "effect",
                "item_label": "confirmed",
                "sort_rank": 1,
                "is_done": false,
                "metadata_json": null,
            },
        })]
    );

    let ack = host
        .pull_as("observer", "g1", effect["cookie"].clone(), None, "u1")
        .unwrap();
    assert_eq!(ack["lastMutationIDChanges"], json!({ "writer": 1 }));
    assert_eq!(patch(&ack), Vec::<Value>::new());
}

#[test]
fn capped_lmid_only_push_moves_the_cookie() {
    let mut host = Host::new(true);
    host.init();
    let start = cookie(
        &host
            .pull_as("observer", "g1", json!(null), None, "u1")
            .unwrap(),
    );

    let rejected = host
        .push_one("item.reject", json!({}), "writer", "g1", 1, "u1")
        .unwrap();
    assert_eq!(
        rejected["pushResponse"]["mutations"][0]["result"]["error"],
        json!("app")
    );
    host.exec(
        "INSERT INTO item (id, label, rank, done, meta)
         VALUES ('later', 'later', 2, 0, NULL)",
    );
    let current = host.watermark();
    host.caps = Caps {
        max_change_rows: 1,
        max_change_bytes: usize::MAX,
    };

    let ack = host
        .pull_as("observer", "g1", json!(start), None, "u1")
        .unwrap();
    assert!(cookie(&ack) > start);
    assert!(cookie(&ack) < current, "the row cap did not cut the diff");
    assert_eq!(ack["lastMutationIDChanges"], json!({ "writer": 1 }));
    assert_eq!(patch(&ack), Vec::<Value>::new());

    let effect = host
        .pull_as("observer", "g1", ack["cookie"].clone(), None, "u1")
        .unwrap();
    assert_eq!(cookie(&effect), current);
    assert_eq!(
        patch(&effect),
        vec![json!({
            "op": "put",
            "tableName": "item_record",
            "value": {
                "item_id": "later",
                "item_label": "later",
                "sort_rank": 2,
                "is_done": false,
                "metadata_json": null,
            },
        })]
    );
}
