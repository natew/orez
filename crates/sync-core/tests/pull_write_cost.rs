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
fn a_pull_with_no_cookie_rebuilds_the_whole_group() {
    // A client that lost its cookie (a respawned process, or one whose cookie
    // fell below the retention floor) is `fresh`, so qpull resets the GROUP's
    // durable membership and recomputes it. That is the expensive pull, and it
    // scales with the group's membership rather than with what changed -- so a
    // restart loop is far more expensive than a retry loop. Asserted as a
    // shape, not a constant, so it documents the scaling without pinning a
    // number that ordinary changes would churn.
    let mut small = Host::new(true);
    small.init();
    for i in 0..100 {
        small.exec(&format!(
            "INSERT INTO item_record VALUES ('i{i}','l{i}',{i}.0,0,NULL)"
        ));
    }
    small.pull_as("c1", "g1", json!(null), "u1").unwrap();
    let (small_rebuild, _) = writes(&mut small, |h| {
        h.pull_as("c1", "g1", json!(null), "u1").unwrap()
    });

    let mut big = Host::new(true);
    big.init();
    for i in 0..400 {
        big.exec(&format!(
            "INSERT INTO item_record VALUES ('i{i}','l{i}',{i}.0,0,NULL)"
        ));
    }
    big.pull_as("c1", "g1", json!(null), "u1").unwrap();
    let (big_rebuild, _) = writes(&mut big, |h| {
        h.pull_as("c1", "g1", json!(null), "u1").unwrap()
    });

    assert!(
        small_rebuild > 0 && big_rebuild > 3 * small_rebuild,
        "a cookieless pull should scale with group membership \
         (100 rows wrote {small_rebuild}, 400 rows wrote {big_rebuild})"
    );
}
