// M4b/M5 recomputation narrowing (plan algorithm step 2): a query is recomputed
// only when the touched tables intersect its dependency set (or it was never
// computed). this proves the narrowing is active and correct, and benchmarks
// the before/after in the style of the dataset report.
mod common;

use std::collections::BTreeSet;
use std::time::Instant;

use common::TestDb;
use serde_json::{Value, json};

use sync_core::query::{init_query_schema, recompute_group, register_query, set_desire};
use sync_core::schema::TableSpec;
use sync_core::value::ZeroColumnType;
use sync_core::{SyncDb, Tables, Transactor, init_schema};

const G: &str = "g1";

fn tables(n_tables: usize) -> Tables {
    use ZeroColumnType::*;
    let mut t = Tables::new();
    for i in 0..n_tables {
        t.push(
            format!("t{i}"),
            TableSpec {
                columns: vec![("id".into(), String), ("v".into(), Number)],
                primary_key: vec!["id".into()],
            },
        );
    }
    t
}

fn changed(items: &[(&str, &str)]) -> BTreeSet<(String, String)> {
    items
        .iter()
        .map(|(t, id)| (t.to_string(), json!({ "id": id }).to_string()))
        .collect()
}

fn put_keys(patch: &[Value]) -> Vec<String> {
    let mut v: Vec<String> = patch
        .iter()
        .filter(|op| op["op"] == "put")
        .map(|op| {
            format!(
                "{}:{}",
                op["tableName"].as_str().unwrap(),
                op["value"]["id"].as_str().unwrap()
            )
        })
        .collect();
    v.sort();
    v
}

#[test]
fn narrowing_skips_queries_whose_deps_were_not_touched() {
    let mut db = TestDb::memory();
    db.exec("CREATE TABLE t0 (id TEXT PRIMARY KEY, v INTEGER)", &[])
        .unwrap();
    db.exec("CREATE TABLE t1 (id TEXT PRIMARY KEY, v INTEGER)", &[])
        .unwrap();
    let tables = tables(2);
    init_schema(&mut db, &tables).unwrap();
    init_query_schema(&mut db).unwrap();
    db.exec("INSERT INTO t0 VALUES ('a', 1)", &[]).unwrap();
    db.exec("INSERT INTO t1 VALUES ('x', 1)", &[]).unwrap();

    // query A over t0, query B over t1
    register_query(&mut db, &tables, "qa", &json!({ "table": "t0" })).unwrap();
    register_query(&mut db, &tables, "qb", &json!({ "table": "t1" })).unwrap();
    set_desire(&mut db, G, "c", "qa", 1).unwrap();
    set_desire(&mut db, G, "c", "qb", 1).unwrap();
    let first = db
        .transaction(|d| recompute_group(d, &tables, G, &changed(&[])))
        .unwrap();
    assert_eq!(put_keys(&first), vec!["t0:a", "t1:x"]);

    // insert into BOTH tables, but report only t1 as touched. qa (over t0) is
    // narrowed away, so the new t0 row is NOT picked up; qb (over t1) recomputes.
    db.exec("INSERT INTO t0 VALUES ('b', 1)", &[]).unwrap();
    db.exec("INSERT INTO t1 VALUES ('y', 1)", &[]).unwrap();
    let narrowed = db
        .transaction(|d| recompute_group(d, &tables, G, &changed(&[("t1", "y")])))
        .unwrap();
    assert_eq!(
        put_keys(&narrowed),
        vec!["t1:y"],
        "qa should have been skipped"
    );

    // now report t0 touched -> qa recomputes and the earlier t0 insert appears
    let caught_up = db
        .transaction(|d| recompute_group(d, &tables, G, &changed(&[("t0", "b")])))
        .unwrap();
    assert_eq!(put_keys(&caught_up), vec!["t0:b"]);
}

#[test]
fn narrowing_respects_related_and_exists_dependency_tables() {
    // a query over t0 that EXISTS-filters on t1 must recompute when t1 is touched
    // (its dependency set includes t1), even though its root table is t0.
    let mut db = TestDb::memory();
    db.exec("CREATE TABLE t0 (id TEXT PRIMARY KEY, v INTEGER)", &[])
        .unwrap();
    db.exec("CREATE TABLE t1 (id TEXT PRIMARY KEY, v INTEGER)", &[])
        .unwrap();
    let tables = tables(2);
    init_schema(&mut db, &tables).unwrap();
    init_query_schema(&mut db).unwrap();
    db.exec("INSERT INTO t0 VALUES ('a', 1)", &[]).unwrap();

    // t0 rows that have a matching t1 row (t1.v = t0.v)
    let q = json!({ "table": "t0", "where": {
        "type": "correlatedSubquery", "op": "EXISTS",
        "related": { "correlation": { "parentField": ["v"], "childField": ["v"] },
                     "subquery": { "table": "t1" } }
    } });
    register_query(&mut db, &tables, "q", &q).unwrap();
    set_desire(&mut db, G, "c", "q", 1).unwrap();
    // no matching t1 yet -> empty
    assert!(
        put_keys(
            &db.transaction(|d| recompute_group(d, &tables, G, &changed(&[])))
                .unwrap()
        )
        .is_empty()
    );

    // add a matching t1 row; report ONLY t1 touched -> q must still recompute
    // because t1 is in its dependency set, and t0:a now matches (+ the t1 witness)
    db.exec("INSERT INTO t1 VALUES ('m', 1)", &[]).unwrap();
    let patch = db
        .transaction(|d| recompute_group(d, &tables, G, &changed(&[("t1", "m")])))
        .unwrap();
    assert_eq!(put_keys(&patch), vec!["t0:a", "t1:m"]);
}

fn where_v(v: i64) -> Value {
    json!({ "table": "t0", "where": {
        "type": "simple", "op": "=", "left": { "type": "column", "name": "v" },
        "right": { "type": "literal", "value": v } } })
}

#[test]
fn touched_pk_narrowing_skips_same_table_queries_that_do_not_match() {
    // two queries on the SAME table t0: v=1 and v=2. a change to a v=1 row must
    // not recompute the v=2 query (touched-pk narrowing — the touched row is
    // neither a member of nor a match for the v=2 query). this is the "one new
    // message recomputes one channel's window" case: same table, different filter.
    let mut db = TestDb::memory();
    db.exec("CREATE TABLE t0 (id TEXT PRIMARY KEY, v INTEGER)", &[])
        .unwrap();
    db.exec("CREATE TABLE t1 (id TEXT PRIMARY KEY, v INTEGER)", &[])
        .unwrap();
    let tables = tables(2);
    init_schema(&mut db, &tables).unwrap();
    init_query_schema(&mut db).unwrap();
    db.exec("INSERT INTO t0 VALUES ('a', 1), ('b', 2)", &[])
        .unwrap();

    register_query(&mut db, &tables, "q1", &where_v(1)).unwrap();
    register_query(&mut db, &tables, "q2", &where_v(2)).unwrap();
    set_desire(&mut db, G, "c", "q1", 1).unwrap();
    set_desire(&mut db, G, "c", "q2", 1).unwrap();
    assert_eq!(
        put_keys(
            &db.transaction(|d| recompute_group(d, &tables, G, &changed(&[])))
                .unwrap()
        ),
        vec!["t0:a", "t0:b"]
    );

    // insert a new v=1 row AND a new v=2 row; report only the v=1 row touched.
    // q1 recomputes (the row matches v=1); q2 is narrowed away (the touched row
    // is not a v=2 member and does not match v=2), so the new v=2 row is not seen.
    db.exec("INSERT INTO t0 VALUES ('c', 1), ('d', 2)", &[])
        .unwrap();
    let narrowed = db
        .transaction(|d| recompute_group(d, &tables, G, &changed(&[("t0", "c")])))
        .unwrap();
    assert_eq!(
        put_keys(&narrowed),
        vec!["t0:c"],
        "q2 should have been narrowed away by touched-pk"
    );

    // reporting the v=2 row touched recomputes q2
    let caught = db
        .transaction(|d| recompute_group(d, &tables, G, &changed(&[("t0", "d")])))
        .unwrap();
    assert_eq!(put_keys(&caught), vec!["t0:d"]);
}

#[test]
#[ignore = "benchmark — run with --ignored --nocapture"]
fn narrowing_benchmark() {
    // 40 queries, one per table, each table with 500 rows. touching ONE table
    // should recompute ONE query (narrowed) vs all 40 (unnarrowed baseline).
    const N: usize = 40;
    const ROWS: usize = 500;
    let mut db = TestDb::memory();
    for i in 0..N {
        db.exec(
            &format!("CREATE TABLE t{i} (id TEXT PRIMARY KEY, v INTEGER)"),
            &[],
        )
        .unwrap();
    }
    let tables = tables(N);
    init_schema(&mut db, &tables).unwrap();
    init_query_schema(&mut db).unwrap();
    for i in 0..N {
        for r in 0..ROWS {
            db.exec(&format!("INSERT INTO t{i} VALUES ('r{r}', {r})"), &[])
                .unwrap();
        }
        register_query(
            &mut db,
            &tables,
            &format!("q{i}"),
            &json!({ "table": format!("t{i}") }),
        )
        .unwrap();
        set_desire(&mut db, G, "c", &format!("q{i}"), 1).unwrap();
    }
    // prime all memberships
    db.transaction(|d| recompute_group(d, &tables, G, &changed(&[])))
        .unwrap();

    // unnarrowed baseline: touch every table -> all 40 queries recompute
    let all: Vec<(&str, &str)> = (0..N).map(|_| ("t0", "r0")).collect(); // stand-in; use full set below
    let _ = all;
    let full_touch: BTreeSet<(String, String)> = (0..N)
        .map(|i| (format!("t{i}"), json!({ "id": "r0" }).to_string()))
        .collect();
    let t0 = Instant::now();
    db.transaction(|d| recompute_group(d, &tables, G, &full_touch))
        .unwrap();
    let full_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // narrowed: touch ONE table -> one query recomputes
    let one_touch = changed(&[("t7", "r0")]);
    let t1 = Instant::now();
    db.transaction(|d| recompute_group(d, &tables, G, &one_touch))
        .unwrap();
    let one_ms = t1.elapsed().as_secs_f64() * 1000.0;

    println!(
        "\n=== (a) dependency-intersection narrowing ===\n{N} queries on {N} distinct tables x {ROWS} rows | ALL tables touched (unnarrowed): {full_ms:.1}ms | ONE table touched (narrowed): {one_ms:.2}ms | speedup {:.0}x",
        full_ms / one_ms.max(0.001)
    );

    // (b) same-table narrowing: many windowed queries over ONE table (like open
    // channels over `message`), each filtered to a distinct value. a change to
    // one value's row must recompute only that query.
    let mut db2 = TestDb::memory();
    db2.exec(
        "CREATE TABLE m (id TEXT PRIMARY KEY, ch INTEGER, v INTEGER)",
        &[],
    )
    .unwrap();
    let mtables = Tables::new().with(
        "m",
        TableSpec {
            columns: vec![
                ("id".into(), ZeroColumnType::String),
                ("ch".into(), ZeroColumnType::Number),
                ("v".into(), ZeroColumnType::Number),
            ],
            primary_key: vec!["id".into()],
        },
    );
    init_schema(&mut db2, &mtables).unwrap();
    init_query_schema(&mut db2).unwrap();
    for ch in 0..N {
        for r in 0..ROWS {
            db2.exec(
                &format!("INSERT INTO m VALUES ('c{ch}r{r}', {ch}, {r})"),
                &[],
            )
            .unwrap();
        }
        // channelMessages(ch) with a window: m where ch=ch orderBy v desc limit 100
        let q = json!({ "table": "m", "where": {
            "type": "simple", "op": "=", "left": { "type": "column", "name": "ch" },
            "right": { "type": "literal", "value": ch as i64 } },
            "orderBy": [["v", "desc"]], "limit": 100 });
        register_query(&mut db2, &mtables, &format!("ch{ch}"), &q).unwrap();
        set_desire(&mut db2, G, "c", &format!("ch{ch}"), 1).unwrap();
    }
    db2.transaction(|d| recompute_group(d, &mtables, G, &changed(&[])))
        .unwrap();

    // baseline: without touched-pk narrowing every ch-query depends on table `m`,
    // so a new message would recompute all N windows. approximate that by
    // touching one row per channel so every query is relevant.
    db2.exec("INSERT INTO m VALUES ('new0', 0, 9999)", &[])
        .unwrap();
    let all_ch: BTreeSet<(String, String)> = (0..N)
        .map(|_| ("m".to_string(), json!({ "id": "new0" }).to_string()))
        .collect();
    // (new0 matches only ch=0; to force all, touch a matching row per channel)
    let all_ch: BTreeSet<(String, String)> = {
        let mut s = all_ch;
        for ch in 0..N {
            db2.exec(&format!("INSERT INTO m VALUES ('n{ch}', {ch}, 9999)"), &[])
                .unwrap();
            s.insert((
                "m".to_string(),
                json!({ "id": format!("n{ch}") }).to_string(),
            ));
        }
        s
    };
    let tb = Instant::now();
    db2.transaction(|d| recompute_group(d, &mtables, G, &all_ch))
        .unwrap();
    let all_ch_ms = tb.elapsed().as_secs_f64() * 1000.0;

    // narrowed: one new message in channel 3 only -> only ch3's window recomputes
    db2.exec("INSERT INTO m VALUES ('msg', 3, 10000)", &[])
        .unwrap();
    let one_ch = changed(&[("m", "msg")]);
    let tc = Instant::now();
    db2.transaction(|d| recompute_group(d, &mtables, G, &one_ch))
        .unwrap();
    let one_ch_ms = tc.elapsed().as_secs_f64() * 1000.0;

    println!(
        "=== (b) touched-pk narrowing ===\n{N} windowed queries over ONE table (open channels) | new msg in ALL channels: {all_ch_ms:.1}ms | new msg in ONE channel (narrowed): {one_ch_ms:.2}ms | speedup {:.0}x",
        all_ch_ms / one_ch_ms.max(0.001)
    );
}
