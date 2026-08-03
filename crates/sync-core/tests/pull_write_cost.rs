// What a pull COSTS in rows written, metered with sqlite total_changes() so
// trigger writes count too (Cloudflare bills Durable Object SQLite per row
// written, and a trigger's rows bill like any other).
//
// The load-bearing invariant is that a caught-up client's pull writes NOTHING.
// A client that is refused and retries -- which is what a spend circuit or any
// 429 with Retry-After produces -- then costs nothing per retry, however long
// it keeps retrying. If this ever regresses to a nonzero number, every retrying
// client starts billing on a timer, which is the failure mode this guards.
mod common;

use common::Host;
use serde_json::json;
use sync_core::{SqlValue, SyncDb};

fn writes<T>(h: &mut Host, work: impl FnOnce(&mut Host) -> T) -> (i64, T) {
    let before = h.db.conn.total_changes() as i64;
    let out = work(h);
    ((h.db.conn.total_changes() as i64) - before, out)
}

#[test]
fn a_caught_up_pull_writes_nothing_however_often_it_retries() {
    let mut h = Host::new(true);
    h.init();
    for i in 0..200 {
        h.exec(&format!(
            "INSERT INTO item_record VALUES ('i{i}','label{i}',{i}.0,0,NULL)"
        ));
    }

    let (_, first) = writes(&mut h, |h| {
        h.pull_as("c1", "g1", json!(null), "u1").unwrap()
    });
    let cookie = first.get("cookie").cloned().unwrap_or(json!(null));

    // ten retries in a row, exactly what a client backing off against a
    // refusing server does
    for attempt in 0..10 {
        let (rows, _) = writes(&mut h, |h| {
            h.pull_as("c1", "g1", cookie.clone(), "u1").unwrap()
        });
        assert_eq!(
            rows, 0,
            "a caught-up pull must write nothing (retry {attempt} wrote {rows} rows)"
        );
    }
}

#[test]
fn a_pull_with_no_cookie_preserves_unchanged_group_membership() {
    let mut h = Host::new(true);
    h.init();
    for i in 0..1_000 {
        h.exec(&format!(
            "INSERT INTO item_record VALUES ('i{i}','l{i}',{i}.0,0,NULL)"
        ));
    }
    h.pull_as("c1", "g1", json!(null), "u1").unwrap();

    let (written, response) = writes(&mut h, |h| {
        h.pull_as("c1", "g1", json!(null), "u1").unwrap()
    });

    assert_eq!(
        written, 0,
        "unchanged durable membership must not be rewritten"
    );
    let patch = response["rowsPatch"].as_array().unwrap();
    assert_eq!(patch.first().unwrap()["op"], "clear");
    assert_eq!(
        patch.iter().filter(|op| op["op"] == "put").count(),
        1_000,
        "the requesting replica still needs the complete current union"
    );
    println!("cookieless unchanged pull wrote {written} rows and sent 1,000 rows");
}

#[test]
fn a_pull_with_no_cookie_writes_only_a_real_membership_delta() {
    let mut h = Host::new(true);
    h.init();
    for i in 0..1_000 {
        h.exec(&format!(
            "INSERT INTO item_record VALUES ('i{i}','l{i}',{i}.0,0,NULL)"
        ));
    }
    h.pull_as("c1", "g1", json!(null), "u1").unwrap();
    h.exec("DELETE FROM item_record WHERE item_id = 'i999'");

    let (written, response) = writes(&mut h, |h| {
        h.pull_as("c1", "g1", json!(null), "u1").unwrap()
    });

    assert_eq!(
        written, 2,
        "one departed row updates membership plus its group ref; the trigger already advanced the packed head"
    );
    let patch = response["rowsPatch"].as_array().unwrap();
    assert_eq!(patch.first().unwrap()["op"], "clear");
    assert_eq!(
        patch.iter().filter(|op| op["op"] == "put").count(),
        999,
        "the full response reflects current membership"
    );
    assert_eq!(patch.iter().filter(|op| op["op"] == "del").count(), 0);
    let stale_membership =
        h.db.query(
            "SELECT 1 FROM _zsync_query_rows
             WHERE clientGroupID = ? AND rowPk = ?",
            &[
                SqlValue::Text("g1".into()),
                SqlValue::Text(r#"{"id":"i999"}"#.into()),
            ],
        )
        .unwrap();
    assert!(
        stale_membership.is_empty(),
        "the negative control must remove the departed durable member"
    );
    println!(
        "cookieless one-row membership delta wrote {written} rows and sent 999 rows \
         (1 query membership, 1 group ref; the trigger advanced the packed head before pull)"
    );
}

#[test]
fn a_below_floor_cookie_preserves_unchanged_group_membership() {
    let mut h = Host::new(true);
    h.retain = 2;
    h.init();
    for i in 0..1_000 {
        h.exec(&format!(
            "INSERT INTO item_record VALUES ('i{i}','l{i}',{i}.0,0,NULL)"
        ));
    }
    let first = h.pull_as("c1", "g1", json!(null), "u1").unwrap();
    let ancient = first["cookie"].clone();

    for i in 0..6 {
        h.exec(&format!(
            "UPDATE item_record SET item_label = 'changed-{i}' WHERE item_id = 'i0'"
        ));
    }
    let current = h.watermark();
    h.pull_as("c2", "g1", json!(current), "u1").unwrap();
    assert!(ancient.as_i64().unwrap() < h.floor());

    let (written, response) = writes(&mut h, |h| h.pull_as("c1", "g1", ancient, "u1").unwrap());

    assert_eq!(
        written, 0,
        "an old cookie must not invalidate correct membership"
    );
    let patch = response["rowsPatch"].as_array().unwrap();
    assert_eq!(patch.first().unwrap()["op"], "clear");
    assert_eq!(patch.iter().filter(|op| op["op"] == "put").count(), 1_000);
    assert!(patch.iter().any(|op| {
        op["op"] == "put"
            && op["value"]["item_id"] == "i0"
            && op["value"]["item_label"] == "changed-5"
    }));
    println!("below-floor unchanged pull wrote {written} rows and sent 1,000 rows");
}
