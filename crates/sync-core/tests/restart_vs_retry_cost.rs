// What a RESTART costs versus a RETRY, for a client the server is refusing.
//
// pull_write_cost.rs pins what a pull writes. This pins the other half: what a
// pull SENDS, and what changes when the retrying client is a fresh process
// rather than the same one. Cloudflare bills Durable Object SQLite per row read
// and per row written, so a pull that writes nothing can still be expensive.
//
// The three shapes a refused client can take, in rising cost:
//
//   retry   same client, same cookie          nothing written, nothing sent
//   restart new client, same group and cookie 4 rows of per-client bookkeeping
//                                             written, EVERY row sent
//   cold    new client, same group, no cookie 4 rows of per-client bookkeeping
//                                             written, EVERY row sent
//
// A restart looks cheap on a write meter and is not: a client that has never
// desired these queries before gets a full rehydrate, because the group's
// durable membership is current but that client's local cache is empty. So a
// supervisor that answers a refusal by exiting and respawning pays a full
// re-send on a timer for as long as the refusal lasts, while a process that
// stays alive and retries pays nothing.
mod common;

use common::Host;
use serde_json::json;

fn writes<T>(h: &mut Host, work: impl FnOnce(&mut Host) -> T) -> (i64, T) {
    let before = h.db.conn.total_changes() as i64;
    let out = work(h);
    ((h.db.conn.total_changes() as i64) - before, out)
}

// row puts only: a fresh pull prepends a `clear` op, which is not a row.
fn rows_sent(response: &serde_json::Value) -> usize {
    response
        .get("rowsPatch")
        .and_then(|p| p.as_array())
        .map(|patch| {
            patch
                .iter()
                .filter(|op| op.get("op").and_then(|o| o.as_str()) == Some("put"))
                .count()
        })
        .unwrap_or(0)
}

fn seeded(rows: usize) -> Host {
    let mut h = Host::new(true);
    h.init();
    for i in 0..rows {
        h.exec(&format!(
            "INSERT INTO item_record VALUES ('i{i}','label{i}',{i}.0,0,NULL)"
        ));
    }
    h
}

#[test]
fn a_restart_resends_every_row_while_a_retry_sends_nothing() {
    const MEMBERSHIP: usize = 1_000;
    let mut h = seeded(MEMBERSHIP);

    let first = h.pull_as("c1", "g1", json!(null), "u1").unwrap();
    let cookie = first.get("cookie").cloned().unwrap();
    assert_eq!(
        rows_sent(&first),
        MEMBERSHIP,
        "the initial pull sends the whole membership"
    );

    // (1) the same process retrying: what an in-process backoff does.
    for attempt in 0..10 {
        let (written, response) = writes(&mut h, |h| {
            h.pull_as("c1", "g1", cookie.clone(), "u1").unwrap()
        });
        assert_eq!(written, 0, "retry {attempt} wrote {written} rows");
        assert_eq!(
            rows_sent(&response),
            0,
            "retry {attempt} sent {} rows",
            rows_sent(&response)
        );
    }

    // (2) a respawned process: a NEW client joining the SAME group, carrying
    // the group's still-valid cookie from its persistent store. Writes stay
    // flat, because the group's membership refcounts do not move and only this
    // client's own desired-query bookkeeping is written. The whole membership
    // still goes out on the wire, because the client's desired-query state did
    // not survive the restart. That re-send, plus the full recompute behind it,
    // is what a restart costs and a retry does not.
    let mut restarts = Vec::new();
    for restart in 0..10 {
        let client = format!("c-restart-{restart}");
        let (written, response) = writes(&mut h, |h| {
            h.pull_as(&client, "g1", cookie.clone(), "u1").unwrap()
        });
        restarts.push((written, rows_sent(&response)));
    }
    println!("10 restarts, each (rows written, rows sent): {restarts:?}");
    assert!(
        restarts.iter().all(|(sent, _)| *sent < 100),
        "a restart's writes are per-client bookkeeping, not per-row, so they \
         must not scale with the {MEMBERSHIP}-row membership: {restarts:?}"
    );
    assert!(
        restarts.iter().all(|(_, sent)| *sent == MEMBERSHIP),
        "every restart re-sends the whole membership: {restarts:?}"
    );
}

#[test]
fn a_restart_that_lost_its_cookie_preserves_group_membership() {
    let mut h = seeded(1_000);
    h.pull_as("c1", "g1", json!(null), "u1").unwrap();

    let (written, response) = writes(&mut h, |h| {
        h.pull_as("c-cold", "g1", json!(null), "u1").unwrap()
    });
    assert_eq!(
        written, 4,
        "only the new client's bookkeeping should be written"
    );
    assert_eq!(rows_sent(&response), 1_000);
    println!(
        "cookieless restart wrote {written} rows and sent {}",
        rows_sent(&response)
    );
}
