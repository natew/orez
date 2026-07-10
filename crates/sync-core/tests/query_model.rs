// M4b randomized model test: over many seeds, interleave data changes with
// query desire/undesire and recompute, applying each membership patch to a
// simulated client store, and assert after every step that the store equals the
// union of the group's active queries' live results — i.e. no missed row, no
// forbidden row lingering (invariants 9, 14, 15), and refcounts stay consistent.
mod common;

use std::collections::{BTreeSet, HashMap};

use common::TestDb;
use serde_json::{Value, json};

use sync_core::query::{
    compile, init_query_schema, parse_ast, recompute_group, register_query, remove_desire,
    set_desire,
};
use sync_core::schema::TableSpec;
use sync_core::value::ZeroColumnType;
use sync_core::{SqlValue, SyncDb, Tables, Transactor, init_schema};

const G: &str = "g1";

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn schema() -> Tables {
    use ZeroColumnType::*;
    Tables::new().with(
        "issue",
        TableSpec {
            columns: vec![
                ("id".into(), String),
                ("closed".into(), Boolean),
                ("priority".into(), Number),
                ("ownerId".into(), String),
            ],
            primary_key: vec!["id".into()],
        },
    )
}

// the candidate query pool (stable hashes -> transformed ASTs)
fn query_pool() -> Vec<(&'static str, Value)> {
    fn cmp(op: &str, col: &str, v: Value) -> Value {
        json!({ "type": "simple", "op": op, "left": { "type": "column", "name": col }, "right": { "type": "literal", "value": v } })
    }
    vec![
        (
            "q_open",
            json!({ "table": "issue", "where": cmp("=", "closed", json!(false)) }),
        ),
        (
            "q_hi",
            json!({ "table": "issue", "where": cmp(">=", "priority", json!(5)) }),
        ),
        (
            "q_u1",
            json!({ "table": "issue", "where": cmp("=", "ownerId", json!("u1")) }),
        ),
        (
            "q_open_hi",
            json!({ "table": "issue", "where": { "type": "and", "conditions": [
                cmp("=", "closed", json!(false)), cmp(">=", "priority", json!(3))
            ] } }),
        ),
        (
            "q_limit",
            json!({ "table": "issue", "orderBy": [["priority", "desc"]], "limit": 3 }),
        ),
    ]
}

fn live_ids(db: &mut TestDb, tables: &Tables, ast_json: &Value) -> BTreeSet<String> {
    let ast = parse_ast(ast_json).unwrap();
    let compiled = compile(&ast, tables).unwrap();
    db.query(&compiled.sql, &compiled.params)
        .unwrap()
        .iter()
        .map(|r| match r.get("id") {
            Some(SqlValue::Text(s)) => s.clone(),
            other => panic!("bad id {other:?}"),
        })
        .collect()
}

fn run_trace(seed: u64, steps: u64) {
    let mut rng = Rng(seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(7));
    let mut db = TestDb::memory();
    db.exec(
        "CREATE TABLE issue (id TEXT PRIMARY KEY, closed INTEGER, priority INTEGER, ownerId TEXT)",
        &[],
    )
    .unwrap();
    let tables = schema();
    init_schema(&mut db, &tables).unwrap();
    init_query_schema(&mut db).unwrap();

    let pool = query_pool();
    for (hash, ast) in &pool {
        register_query(&mut db, &tables, G, hash, ast, 0).unwrap();
    }

    let mut store: BTreeSet<String> = BTreeSet::new();
    let mut desired: BTreeSet<usize> = BTreeSet::new();
    let mut changed: BTreeSet<(String, String)> = BTreeSet::new();
    let mut ids_seq = 0u64;

    let recompute =
        |db: &mut TestDb, tables: &Tables, changed: &BTreeSet<(String, String)>| -> Vec<Value> {
            db.transaction(|d| recompute_group(d, tables, G, changed))
                .unwrap()
        };

    for step in 0..steps {
        match rng.below(5) {
            0 | 1 => {
                // data change: insert/update a random issue id in a small pool
                let id = format!("k{}", rng.below(6));
                let closed = rng.below(2) as i64;
                let priority = rng.below(8) as i64;
                let owner = if rng.below(2) == 0 { "u1" } else { "u2" };
                ids_seq += 1;
                db.exec(
                    "INSERT INTO issue (id, closed, priority, ownerId) VALUES (?, ?, ?, ?)
                     ON CONFLICT (id) DO UPDATE SET closed = excluded.closed,
                       priority = excluded.priority, ownerId = excluded.ownerId",
                    &[
                        SqlValue::Text(id.clone()),
                        SqlValue::Integer(closed),
                        SqlValue::Integer(priority),
                        SqlValue::Text(owner.into()),
                    ],
                )
                .unwrap();
                changed.insert(("issue".into(), json!({ "id": id }).to_string()));
                let _ = ids_seq;
            }
            2 => {
                // delete a random issue id
                let id = format!("k{}", rng.below(6));
                db.exec(
                    "DELETE FROM issue WHERE id = ?",
                    &[SqlValue::Text(id.clone())],
                )
                .unwrap();
                changed.insert(("issue".into(), json!({ "id": id }).to_string()));
            }
            3 => {
                // desire a random pool query
                let q = rng.below(pool.len() as u64) as usize;
                set_desire(&mut db, G, "c1", pool[q].0, 1).unwrap();
                desired.insert(q);
            }
            _ => {
                // undesire a random currently-desired query (if any)
                if !desired.is_empty() {
                    let picks: Vec<usize> = desired.iter().copied().collect();
                    let q = picks[rng.below(picks.len() as u64) as usize];
                    remove_desire(&mut db, G, "c1", pool[q].0).unwrap();
                    desired.remove(&q);
                }
            }
        }

        // recompute + apply the patch to the store
        let patch = recompute(&mut db, &tables, &changed);
        changed.clear();
        for op in &patch {
            match op["op"].as_str() {
                Some("put") => {
                    store.insert(op["value"]["id"].as_str().unwrap().to_string());
                }
                Some("del") => {
                    store.remove(op["id"]["id"].as_str().unwrap());
                }
                _ => {}
            }
        }

        // expected = union of active queries' live results
        let mut expected: BTreeSet<String> = BTreeSet::new();
        for &q in &desired {
            expected.extend(live_ids(&mut db, &tables, &pool[q].1));
        }
        assert_eq!(
            store, expected,
            "seed {seed} step {step}: client store diverged from the union of active queries"
        );
    }

    // sanity: refcounts are exactly the number of active queries containing each row
    let mut expected_refs: HashMap<String, i64> = HashMap::new();
    for &q in &desired {
        for id in live_ids(&mut db, &tables, &pool[q].1) {
            *expected_refs.entry(id).or_insert(0) += 1;
        }
    }
    let ref_rows = db
        .query("SELECT rowPk, CAST(refcount AS TEXT) AS c FROM _zsync_row_refs WHERE clientGroupID = ?", &[SqlValue::Text(G.into())])
        .unwrap();
    let mut actual_refs: HashMap<String, i64> = HashMap::new();
    for row in &ref_rows {
        let pk = match row.get("rowPk") {
            Some(SqlValue::Text(s)) => s.clone(),
            _ => continue,
        };
        let id: Value = serde_json::from_str(&pk).unwrap();
        let count: i64 = match row.get("c") {
            Some(SqlValue::Text(s)) => s.parse().unwrap(),
            _ => 0,
        };
        actual_refs.insert(id["id"].as_str().unwrap().to_string(), count);
    }
    assert_eq!(
        actual_refs, expected_refs,
        "seed {seed}: refcounts drifted from the active-query cover"
    );
}

#[test]
fn randomized_query_membership_converges() {
    for seed in 0..24u64 {
        run_trace(seed, 300);
    }
}
