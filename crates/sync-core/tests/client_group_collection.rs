// Abandoned client groups are collected when the retained-ledger floor passes
// them.
//
// Nothing could collect a group before `_zsync_client_group_seen` existed:
// `delete_clients` only runs when a live client TELLS the server which of its
// clients it deleted, so a client that simply never returns leaves its whole
// membership behind forever. A real store reached 607 groups and 640k
// membership rows while serving three clients.
//
// The rule under test is not a timeout. A group last served below the floor can
// no longer be sent a diff — its next pull is a full snapshot regardless — so
// its cached membership has already lost the only thing it is for.
mod common;

use common::TestDb;
use serde_json::{Value, json};

use sync_core::query::{handle_query_pull, init_query_schema};
use sync_core::schema::TableSpec;
use sync_core::value::ZeroColumnType;
use sync_core::{SqlValue, SyncDb, Tables, Transactor, init_schema, prune};

// small enough that a handful of writes strands a group that stopped pulling
const RETAIN: i64 = 4;

fn schema() -> Tables {
    use ZeroColumnType::*;
    Tables::new().with(
        "issue",
        TableSpec {
            columns: vec![("id".into(), String), ("closed".into(), Boolean)],
            primary_key: vec!["id".into()],
            encrypted_columns: Default::default(),
            encrypted_physical_columns: Default::default(),
        },
    )
}

fn open_query() -> Value {
    json!({ "table": "issue", "where": {
        "type": "simple", "op": "=",
        "left": { "type": "column", "name": "closed" },
        "right": { "type": "literal", "value": false }
    } })
}

struct Host {
    db: TestDb,
    tables: Tables,
}

impl Host {
    fn new() -> Host {
        let mut db = TestDb::memory();
        db.exec(
            "CREATE TABLE issue (id TEXT PRIMARY KEY, closed INTEGER)",
            &[],
        )
        .unwrap();
        for id in ["i1", "i2", "i3"] {
            db.exec(
                "INSERT INTO issue VALUES (?, 0)",
                &[SqlValue::Text(id.into())],
            )
            .unwrap();
        }
        let tables = schema();
        init_schema(&mut db, &tables).unwrap();
        init_query_schema(&mut db).unwrap();
        Host { db, tables }
    }

    /// A first pull for `group`: registers the open query, so the group ends up
    /// with membership rows to collect.
    fn first_pull(&mut self, group: &str) -> Value {
        let body = json!({
            "clientID": format!("{group}-c"),
            "clientGroupID": group,
            "cookie": Value::Null,
            "queries": { "version": 1, "patch": [
                { "op": "put", "hash": "h-open", "ast": open_query() }
            ]},
        });
        let tables = self.tables.clone();
        self.db
            .transaction(|db| handle_query_pull(db, &tables, RETAIN, &body, "u1"))
            .unwrap()
    }

    /// A later pull carrying whatever cookie the group last received.
    fn pull(&mut self, group: &str, cookie: Value) -> Value {
        let body = json!({
            "clientID": format!("{group}-c"),
            "clientGroupID": group,
            "cookie": cookie,
        });
        let tables = self.tables.clone();
        self.db
            .transaction(|db| handle_query_pull(db, &tables, RETAIN, &body, "u1"))
            .unwrap()
    }

    fn write(&mut self, id: &str, closed: i64) {
        self.db
            .exec(
                "INSERT INTO issue VALUES (?, ?)
                 ON CONFLICT (id) DO UPDATE SET closed = excluded.closed",
                &[SqlValue::Text(id.into()), SqlValue::Integer(closed)],
            )
            .unwrap();
    }

    fn count(&mut self, table: &str, group: &str) -> i64 {
        let rows = self
            .db
            .query(
                &format!("SELECT count(*) FROM {table} WHERE clientGroupID = ?"),
                &[SqlValue::Text(group.into())],
            )
            .unwrap();
        match rows[0].values[0] {
            SqlValue::Integer(n) => n,
            SqlValue::Text(ref s) => s.parse().unwrap(),
            ref other => panic!("count is not an integer: {other:?}"),
        }
    }

    fn prune(&mut self) {
        self.db.transaction(|db| prune(db, RETAIN)).unwrap();
    }

    fn seen_watermark(&mut self, group: &str) -> i64 {
        let rows = self
            .db
            .query(
                "SELECT CAST(watermark AS TEXT) FROM _zsync_client_group_seen
                 WHERE clientGroupID = ?",
                &[SqlValue::Text(group.into())],
            )
            .unwrap();
        match rows.first().and_then(|r| r.values.first()) {
            Some(SqlValue::Text(s)) => s.parse().unwrap(),
            Some(SqlValue::Integer(n)) => *n,
            _ => -1,
        }
    }
}

#[test]
fn a_group_that_stops_pulling_is_collected_and_one_that_keeps_pulling_is_not() {
    let mut host = Host::new();

    // both groups sync, so both hold membership.
    let staying = host.first_pull("g-stays");
    host.first_pull("g-leaves");
    assert!(
        host.count("_zsync_query_rows", "g-leaves") > 0,
        "the fixture must have membership to collect"
    );
    assert!(host.count("_zsync_queries", "g-leaves") > 0);

    // history moves well past the retained window. only one group keeps up.
    let mut cookie = staying["cookie"].clone();
    for i in 0..(RETAIN * 3) {
        host.write(&format!("n{i}"), 0);
        cookie = host.pull("g-stays", cookie)["cookie"].clone();
    }

    host.prune();

    // the group that kept pulling is untouched…
    assert!(
        host.count("_zsync_query_rows", "g-stays") > 0,
        "a group still pulling must keep its membership"
    );
    assert!(host.count("_zsync_queries", "g-stays") > 0);
    assert_eq!(
        host.count("_zsync_client_group_seen", "g-stays"),
        1,
        "and must still be recorded as seen"
    );

    // …and the one that stopped is collected.
    assert_eq!(
        host.count("_zsync_query_rows", "g-leaves"),
        0,
        "an abandoned group's membership rows must be collected"
    );
    assert_eq!(host.count("_zsync_row_refs", "g-leaves"), 0);
    assert_eq!(host.count("_zsync_query_state", "g-leaves"), 0);
    assert_eq!(host.count("_zsync_queries", "g-leaves"), 0);
    assert_eq!(host.count("_zsync_client_group_seen", "g-leaves"), 0);

    // lastMutationID outlives the cache: it is mutation dedup, not membership.
    assert_eq!(
        host.count("_zsync_clients", "g-leaves"),
        1,
        "collecting a group must not forget its lastMutationID"
    );

    // the surviving group is not merely present, it still syncs.
    let next = host.pull("g-stays", cookie);
    assert!(next.get("cookie").is_some(), "g-stays can still pull");
}

/// Tracking liveness must not put a row on every pull.
///
/// A caught-up pull writes nothing, which is what makes a client retrying
/// against a refusing server free however long it retries (`pull_write_cost`).
/// A blind upsert recording "seen" would have written one row per pull and
/// turned every retrying client into a billing timer, so the recording is
/// conditional and this pins it.
#[test]
fn tracking_liveness_does_not_write_on_a_caught_up_pull() {
    let mut host = Host::new();
    let mut cookie = host.first_pull("g-quiet")["cookie"].clone();

    for attempt in 0..10 {
        let before = host.db.conn.total_changes() as i64;
        cookie = host.pull("g-quiet", cookie)["cookie"].clone();
        let wrote = host.db.conn.total_changes() as i64 - before;
        assert_eq!(
            wrote, 0,
            "a caught-up pull must write nothing (retry {attempt} wrote {wrote})"
        );
    }
}

/// And a busy group pays once per retention window, not once per pull: the
/// refresh only fires when the group would otherwise fall below the floor.
#[test]
fn a_busy_group_refreshes_at_most_once_per_retention_window() {
    let mut host = Host::new();
    let mut cookie = host.first_pull("g-busy")["cookie"].clone();

    let mut refreshes = 0;
    let rounds = RETAIN * 8;
    for i in 0..rounds {
        host.write(&format!("n{i}"), 0);
        let before = host.count("_zsync_client_group_seen", "g-busy");
        let seen_before = host.seen_watermark("g-busy");
        cookie = host.pull("g-busy", cookie)["cookie"].clone();
        assert_eq!(before, 1);
        if host.seen_watermark("g-busy") != seen_before {
            refreshes += 1;
        }
    }
    assert!(
        refreshes > 0,
        "a group outrunning the window must refresh at least once"
    );
    assert!(
        refreshes < rounds,
        "but not on every pull: {refreshes} refreshes over {rounds} pulls"
    );
}

#[test]
fn a_collected_group_that_comes_back_resyncs_from_scratch() {
    let mut host = Host::new();
    host.first_pull("g-returns");

    let mut cookie = host.first_pull("g-other")["cookie"].clone();
    for i in 0..(RETAIN * 3) {
        host.write(&format!("n{i}"), 0);
        cookie = host.pull("g-other", cookie)["cookie"].clone();
    }
    host.prune();
    assert_eq!(host.count("_zsync_query_rows", "g-returns"), 0);

    // it registers its queries again and gets a full set back, which is what
    // "collectable" means here: it was already going to re-sync.
    let back = host.first_pull("g-returns");
    let puts = back["rowsPatch"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|op| op["op"] == "put")
        .count();
    assert!(puts > 0, "a returning group must be served its rows again");
    assert!(host.count("_zsync_query_rows", "g-returns") > 0);
    assert_eq!(
        host.count("_zsync_client_group_seen", "g-returns"),
        1,
        "and is tracked again from here"
    );
}
