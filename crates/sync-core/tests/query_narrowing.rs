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
        "\n=== recomputation narrowing (dependency-intersection) ===\n{N} queries x {ROWS} rows | ALL touched (unnarrowed): {full_ms:.1}ms | ONE table touched (narrowed): {one_ms:.2}ms | speedup {:.0}x",
        full_ms / one_ms.max(0.001)
    );
}
