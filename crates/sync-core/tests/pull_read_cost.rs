// What a pull COSTS in SQL reads: statement count and rows returned per pull,
// metered with a counting SyncDb decorator. Cloudflare bills Durable Object
// SQLite per row read, and every engine-issued statement also crosses the
// wasm-to-JS boundary, so both numbers drive production cost and latency.
//
// Workload ("ten slices"): 2,000 rows in `item`, 10 range queries of ~200 rows
// each (the shape of per-channel / per-view queries over one shared table).
// The load-bearing scenario is the STEADY pull: one row changed since the
// cookie, nine of ten queries provably unaffected.
mod common;

use common::{TestDb, item_tables};
use serde_json::{Value, json};
use sync_core::query::{handle_query_pull, init_query_schema};
use sync_core::{DbError, Row, SqlValue, SyncDb, Transactor, init_schema};

struct Meter {
    db: TestDb,
    pub stmts: u64,
    pub rows: u64,
    pub log: Vec<String>,
}

impl Meter {
    fn new() -> Meter {
        let mut db = TestDb::memory();
        db.exec(
            "CREATE TABLE item_record (item_id TEXT PRIMARY KEY, item_label TEXT NOT NULL,
             sort_rank REAL NOT NULL, is_done INTEGER NOT NULL, metadata_json TEXT)",
            &[],
        )
        .unwrap();
        Meter {
            db,
            stmts: 0,
            rows: 0,
            log: Vec::new(),
        }
    }

    fn reset(&mut self) {
        self.stmts = 0;
        self.rows = 0;
        self.log.clear();
    }

    fn short(line: &str) -> String {
        line.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(120)
            .collect()
    }
}

impl SyncDb for Meter {
    fn exec(&mut self, sql: &str, params: &[SqlValue]) -> Result<(), DbError> {
        self.stmts += 1;
        self.log.push(format!("exec {sql}"));
        self.db.exec(sql, params)
    }

    fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<Vec<Row>, DbError> {
        self.stmts += 1;
        self.log.push(format!("query {sql}"));
        let rows = self.db.query(sql, params)?;
        self.rows += rows.len() as u64;
        Ok(rows)
    }
}

impl Transactor for Meter {
    fn transaction<T, E>(
        &mut self,
        body: impl FnOnce(&mut dyn SyncDb) -> Result<T, E>,
    ) -> Result<T, E> {
        // counting must observe the engine's statements, so run the body
        // against this decorator inside the inner transaction.
        self.db.conn.execute_batch("BEGIN").expect("BEGIN");
        let result = body(self);
        match &result {
            Ok(_) => self.db.conn.execute_batch("COMMIT").expect("COMMIT"),
            Err(_) => self.db.conn.execute_batch("ROLLBACK").expect("ROLLBACK"),
        }
        result
    }
}

fn slice_ast(lo: i64, hi: i64) -> Value {
    json!({
        "table": "item",
        "where": { "type": "and", "conditions": [
            { "type": "simple", "op": ">=", "left": { "type": "column", "name": "rank" },
              "right": { "type": "literal", "value": lo } },
            { "type": "simple", "op": "<", "left": { "type": "column", "name": "rank" },
              "right": { "type": "literal", "value": hi } },
        ]},
    })
}

// ten-slice host with all queries registered and one steady-state change
// pending: returns (meter, cookie before the change).
fn steady_host() -> (Meter, Value) {
    steady_host_with_retain(4096)
}

fn steady_host_with_retain(retain: i64) -> (Meter, Value) {
    let mut h = Meter::new();
    let tables = item_tables();
    init_schema(&mut h, &tables).unwrap();
    init_query_schema(&mut h).unwrap();
    for i in 0..2000 {
        h.db.exec(
            "INSERT INTO item_record VALUES (?, ?, ?, 0, NULL)",
            &[
                SqlValue::Text(format!("i{i}")),
                SqlValue::Text(format!("label{i}")),
                SqlValue::Real(i as f64),
            ],
        )
        .unwrap();
    }
    let patch: Vec<Value> = (0..10)
        .map(|k| {
            json!({ "op": "put", "hash": format!("q{k}"), "ast": slice_ast(k * 200, k * 200 + 200) })
        })
        .collect();
    let body = json!({
        "clientID": "c1", "clientGroupID": "g1", "cookie": Value::Null,
        "queries": { "version": 1, "patch": patch },
    });
    let first = h
        .transaction(|db| handle_query_pull(db, &tables, retain, &body, "u1"))
        .unwrap();
    let cookie = first["cookie"].clone();
    assert_eq!(
        first["rowsPatch"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|op| op["op"] == "put")
            .count(),
        2000,
        "setup must deliver all 2,000 rows"
    );
    // one steady-state change inside slice 3.
    h.db.exec(
        "UPDATE item_record SET item_label = 'changed' WHERE item_id = 'i612'",
        &[],
    )
    .unwrap();
    (h, cookie)
}

#[test]
fn steady_pull_read_cost() {
    let (mut h, cookie) = steady_host();
    let tables = item_tables();
    h.reset();
    let body = json!({ "clientID": "c1", "clientGroupID": "g1", "cookie": cookie });
    let resp = h
        .transaction(|db| handle_query_pull(db, &tables, 4096, &body, "u1"))
        .unwrap();
    assert_eq!(resp["rowsPatch"].as_array().unwrap().len(), 1);
    println!("steady pull: {} statements, {} rows read", h.stmts, h.rows);
    let mut counts = std::collections::BTreeMap::new();
    for line in &h.log {
        *counts.entry(line.clone()).or_insert(0u64) += 1;
    }
    let mut counts: Vec<_> = counts.into_iter().collect();
    counts.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
    for (sql, n) in counts.iter().take(15) {
        println!("  {n:4} x {}", Meter::short(sql));
    }
    assert!(h.stmts < 24, "steady pull issued {} statements", h.stmts);
    assert!(h.rows < 500, "steady pull read {} rows", h.rows);
}

#[test]
fn caught_up_pull_read_cost() {
    let (mut h, cookie) = steady_host();
    let tables = item_tables();
    // drain the steady change first so the second pull is fully caught up.
    let body = json!({ "clientID": "c1", "clientGroupID": "g1", "cookie": cookie });
    let drained = h
        .transaction(|db| handle_query_pull(db, &tables, 4096, &body, "u1"))
        .unwrap();
    h.reset();
    let body2 =
        json!({ "clientID": "c1", "clientGroupID": "g1", "cookie": drained["cookie"].clone() });
    let resp = h
        .transaction(|db| handle_query_pull(db, &tables, 4096, &body2, "u1"))
        .unwrap();
    assert_eq!(resp["unchanged"], json!(true));
    println!(
        "caught-up pull: {} statements, {} rows read",
        h.stmts, h.rows
    );
    assert!(h.stmts < 10, "caught-up pull issued {} statements", h.stmts);
    assert!(h.rows < 20, "caught-up pull read {} rows", h.rows);
}

// The pull path must reach engine bookkeeping by key, never by scan:
// `_zsync_log_segments` is WITHOUT ROWID with up to ~16 KiB payload rows, so
// a scan pages every retained payload on each pull; the group-seen probe and
// the desires filters scale with namespace size (groups, clients x queries)
// on every pull. Cloudflare bills every row scanned, so each of these is
// production money. Exercises the real engine, captures the statements it
// actually issues, and asserts none plans a scan of these tables.
#[test]
fn pull_path_never_scans_engine_tables() {
    let (mut h, cookie) = steady_host_with_retain(10);
    let tables = item_tables();
    // the log spans the setup's cookieless pull (which advances the floor and
    // drains stale segments) and the steady pull below (which scans for
    // changes). both phases' ledger statements are checked.
    let steady = h
        .transaction(|db| {
            handle_query_pull(
                db,
                &tables,
                10,
                &json!({ "clientID": "c1", "clientGroupID": "g1", "cookie": cookie }),
                "u1",
            )
        })
        .unwrap();
    assert_eq!(steady["rowsPatch"].as_array().unwrap().len(), 1);
    let mut checked = 0;
    for line in &h.log {
        if !line.contains("_zsync_log_segments")
            && !line.contains("_zsync_client_group_seen")
            && !line.contains("_zsync_desires")
        {
            continue;
        }
        let sql = line.split_once(' ').map(|(_, s)| s).unwrap_or(line);
        // EXPLAIN needs bound literals; 0 preserves each statement's index
        // choice (all binds here are version comparisons or key equalities).
        let plan =
            h.db.query(&format!("EXPLAIN QUERY PLAN {sql}").replace('?', "0"), &[])
                .unwrap();
        for row in &plan {
            if let Some(SqlValue::Text(detail)) = row.get("detail") {
                for table in [
                    "_zsync_log_segments",
                    "_zsync_client_group_seen",
                    "_zsync_desires",
                ] {
                    assert!(
                        !detail.contains(&format!("SCAN {table}")),
                        "engine statement plans a scan: {sql} -> {detail}"
                    );
                }
            }
        }
        checked += 1;
    }
    assert!(
        checked >= 4,
        "expected watermark, scan, and prune statements to be checked, saw {checked}"
    );
    // the setup's 2,000 writes must have rotated into several segments, so the
    // drain path above actually ran more than once; otherwise this check is
    // vacuous (e.g. after a rotation-threshold change).
    let drains = h
        .log
        .iter()
        .filter(|line| line.starts_with("exec DELETE FROM _zsync_log_segments"))
        .count();
    assert!(
        drains >= 2,
        "expected the prune drain to remove several stale segments, saw {drains}"
    );
    println!("checked {checked} ledger statements, none scans the segments table");
}

// A bulk membership flip must not cost a statement per row: 500 departures
// arrive as tiled multi-row writes, and the tombstones they stamp must replay
// to a lagging client of the same group (otherwise its rows ghost forever).
// The replay half is what would fail if the batched refcount upsert stamped
// addedVersion/removedVersion wrong.
#[test]
fn bulk_departure_write_cost_and_tombstone_replay() {
    let mut h = Meter::new();
    let tables = item_tables();
    init_schema(&mut h, &tables).unwrap();
    init_query_schema(&mut h).unwrap();
    for i in 0..2000 {
        h.db.exec(
            "INSERT INTO item_record VALUES (?, ?, ?, 0, NULL)",
            &[
                SqlValue::Text(format!("i{i}")),
                SqlValue::Text(format!("l{i}")),
                SqlValue::Real(i as f64),
            ],
        )
        .unwrap();
    }
    let full = json!({
        "clientID": "c1", "clientGroupID": "g1", "cookie": Value::Null,
        "queries": { "version": 1, "patch": [{ "op": "put", "hash": "q", "ast": { "table": "item" } }] },
    });
    let first = h
        .transaction(|db| handle_query_pull(db, &tables, 4096, &full, "u1"))
        .unwrap();
    let cookie0 = first["cookie"].clone();
    // a second client of the same group, fully synced, then left behind.
    let c2body = json!({
        "clientID": "c2", "clientGroupID": "g1", "cookie": cookie0,
        "queries": { "version": 1, "patch": [{ "op": "put", "hash": "q", "ast": { "table": "item" } }] },
    });
    let c2first = h
        .transaction(|db| handle_query_pull(db, &tables, 4096, &c2body, "u1"))
        .unwrap();
    assert_eq!(
        c2first["rowsPatch"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|op| op["op"] == "put")
            .count(),
        2000
    );
    for i in 0..500 {
        h.db.exec(
            &format!("DELETE FROM item_record WHERE item_id = 'i{i}'"),
            &[],
        )
        .unwrap();
    }
    h.reset();
    let before = h.db.conn.total_changes();
    let flip = h
        .transaction(|db| {
            handle_query_pull(
                db,
                &tables,
                4096,
                &json!({ "clientID": "c1", "clientGroupID": "g1", "cookie": cookie0 }),
                "u1",
            )
        })
        .unwrap();
    let written = h.db.conn.total_changes() as i64 - before as i64;
    let dels = flip["rowsPatch"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|op| op["op"] == "del")
        .count();
    assert_eq!(dels, 500, "the flip must depart exactly the deleted rows");
    println!(
        "bulk 500-departure pull: {} statements, {} rows read, {written} rows written",
        h.stmts, h.rows
    );
    assert!(
        h.stmts < 120,
        "bulk departure issued {} statements (one per row would be ~1,500)",
        h.stmts
    );
    assert_eq!(
        written, 1000,
        "500 membership deletes plus 500 refcount tombstones, no more"
    );
    // the lagging client replays the same 500 departures from the tombstones.
    let replay = h
        .transaction(|db| {
            handle_query_pull(
                db,
                &tables,
                4096,
                &json!({ "clientID": "c2", "clientGroupID": "g1", "cookie": cookie0 }),
                "u1",
            )
        })
        .unwrap();
    let replay_dels = replay["rowsPatch"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|op| op["op"] == "del")
        .count();
    assert_eq!(
        replay_dels, 500,
        "a lagging client must replay every tombstoned departure or rows ghost"
    );
}
