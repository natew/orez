// generated operation-trace differential against the TypeScript reference core.
// a deterministic PRNG generates a trace of high-level ops; the Rust engine and
// the TS core (src/sync-server/sync-server.ts, run by ts-oracle/run-oracle.ts
// under bun) each execute the SAME trace with identical per-client id/cookie
// bookkeeping, and their pull responses are compared. The same trace also
// drives a multi-table query-aware pull fixture against a pure TypeScript ZQL
// evaluator, including named-query membership and transform changes.
//
// comparison is SEMANTIC where the two cores legitimately differ:
// - cookie: exact (both are the change-log watermark)
// - unchanged flag: exact
// - rowsPatch: order-independent (a clear must lead; the rest is a set) — both
//   resolve the same touched pks against the same live rows
// - lastMutationIDChanges: the Rust core derives diff acks from the INCLUDED log
//   prefix (soot semantics), the reference core from a full clients read, so the
//   Rust map is a subset of the reference map with identical values — every
//   (client,lmid) the Rust core reports must equal the reference's. this is the
//   representational difference (a client already knows its unchanged peers'
//   lmids); it can never ack ahead of effects.
//
// REQUIRES bun on PATH (documented in the crate README / ts-oracle). run with a
// normal `cargo test -p sync-core`.
mod common;

use std::collections::HashMap;
use std::io::Write;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use common::{Host, TestDb};
use proptest::prelude::*;
use proptest::test_runner::{Config, FileFailurePersistence};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use sync_core::pull::Caps;
use sync_core::query::{handle_query_pull, init_query_schema};
use sync_core::{SqlValue, SyncDb, Tables, Transactor, init_schema};

const CLIENTS: [&str; 3] = ["c1", "c2", "c3"];
const POOL: u8 = 6;
static TRACE_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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

    fn boolean(&mut self) -> bool {
        self.next() & 1 == 1
    }
}

// Commands are symbolic: both runners assign mutation ids and cookies from
// their current state. This makes every generated command valid while still
// exercising state-dependent duplicate deletes, rejected transactions, stale
// pulls, invalidation, and writes outside the mutation path.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
enum Op {
    Put {
        client: String,
        item: String,
        label: String,
        rank: f64,
        done: bool,
        meta: Value,
    },
    Del {
        client: String,
        item: String,
    },
    Reject {
        client: String,
    },
    Upstream {
        sql: String,
    },
    Pull {
        client: String,
    },
    Invalidate,
    QueryPut {
        hash: String,
        ast: Value,
        transform_version: i64,
    },
    QueryDel {
        hash: String,
    },
    QueryClear,
    QueryPull,
    QueryProject {
        id: String,
        owner_id: String,
        name: String,
    },
    QueryMember {
        id: String,
        project_id: String,
        user_id: String,
    },
    QueryTask {
        id: String,
        project_id: String,
        title: String,
        rank: i64,
        done: bool,
        due_at: Option<i64>,
    },
    QueryUser {
        id: String,
        name: String,
    },
    QueryDelete {
        table: String,
        id: String,
    },
}

fn cmp(op: &str, column: &str, value: Value) -> Value {
    json!({
        "type": "simple",
        "op": op,
        "left": { "type": "column", "name": column },
        "right": { "type": "literal", "value": value },
    })
}

// Named, already-transformed ZQL ASTs. The repeated `permission` hash models
// the trusted host replacing a query's permission transform in place.
fn query_specs() -> Vec<(String, Value, i64)> {
    let member_relation = || {
        json!({
            "correlation": { "parentField": ["id"], "childField": ["projectId"] },
            "subquery": { "table": "member", "related": [{
                "correlation": { "parentField": ["userId"], "childField": ["id"] },
                "subquery": { "table": "user", "limit": 1 },
            }] },
        })
    };
    vec![
        (
            "and_or".into(),
            json!({
                "table": "project",
                "where": { "type": "and", "conditions": [
                    cmp("=", "ownerId", json!("u0")),
                    { "type": "or", "conditions": [
                        cmp("=", "name", json!("A")),
                        cmp("=", "name", json!("C")),
                    ] },
                ] },
                "orderBy": [["name", "asc"]],
            }),
            0,
        ),
        (
            "top_tasks".into(),
            json!({ "table": "task", "orderBy": [["rank", "desc"]], "limit": 2 }),
            0,
        ),
        (
            "first_project".into(),
            json!({ "table": "project", "orderBy": [["name", "asc"]], "limit": 1 }),
            0,
        ),
        (
            "project_page".into(),
            json!({
                "table": "project",
                "orderBy": [["name", "asc"]],
                "limit": 2,
                "related": [
                    member_relation(),
                    {
                        "correlation": { "parentField": ["id"], "childField": ["projectId"] },
                        "subquery": {
                            "table": "task",
                            "orderBy": [["rank", "desc"]],
                            "limit": 2,
                        },
                    },
                ],
            }),
            0,
        ),
        (
            "done_projects".into(),
            json!({
                "table": "project",
                "where": {
                    "type": "correlatedSubquery",
                    "op": "EXISTS",
                    "related": {
                        "correlation": { "parentField": ["id"], "childField": ["projectId"] },
                        "subquery": { "table": "task", "where": cmp("=", "done", json!(true)) },
                    },
                },
                "orderBy": [["id", "asc"]],
            }),
            0,
        ),
        (
            "nullable_page".into(),
            json!({
                "table": "task",
                "orderBy": [["dueAt", "asc"], ["id", "asc"]],
                "start": { "row": { "dueAt": null, "id": "t0" }, "exclusive": true },
                "limit": 3,
            }),
            0,
        ),
        (
            "permission".into(),
            json!({ "table": "project", "where": cmp("=", "ownerId", json!("u0")) }),
            1,
        ),
        (
            "permission".into(),
            json!({ "table": "project", "where": cmp("=", "ownerId", json!("u1")) }),
            2,
        ),
    ]
}

fn query_op() -> impl Strategy<Value = Op> {
    let query_put =
        prop::sample::select(query_specs()).prop_map(|(hash, ast, transform_version)| {
            Op::QueryPut {
                hash,
                ast,
                transform_version,
            }
        });
    prop_oneof![
        5 => query_put,
        2 => prop::sample::select(query_specs()).prop_map(|(hash, _, _)| Op::QueryDel { hash }),
        1 => Just(Op::QueryClear),
        4 => Just(Op::QueryPull),
        2 => (0u8..4, 0u8..3, 0u16..100).prop_map(|(id, owner, name)| Op::QueryProject {
            id: format!("p{id}"),
            owner_id: format!("u{owner}"),
            name: format!("P{name}"),
        }),
        2 => (0u8..6, 0u8..4, 0u8..3).prop_map(|(id, project, user)| Op::QueryMember {
            id: format!("m{id}"),
            project_id: format!("p{project}"),
            user_id: format!("u{user}"),
        }),
        3 => (0u8..8, 0u8..4, -2i16..12, any::<bool>(), prop::option::of(0i16..30)).prop_map(
            |(id, project, rank, done, due_at)| Op::QueryTask {
                id: format!("t{id}"),
                project_id: format!("p{project}"),
                title: format!("T{id}"),
                rank: i64::from(rank),
                done,
                due_at: due_at.map(i64::from),
            },
        ),
        1 => (0u8..4, 0u16..100).prop_map(|(id, name)| Op::QueryUser {
            id: format!("u{id}"),
            name: format!("U{name}"),
        }),
        2 => (prop::sample::select(vec!["project", "member", "task", "user"]), 0u8..8)
            .prop_map(|(table, id)| {
                let prefix = match table {
                    "project" => "p",
                    "member" => "m",
                    "task" => "t",
                    "user" => "u",
                    _ => unreachable!(),
                };
                Op::QueryDelete { table: table.into(), id: format!("{prefix}{id}") }
            }),
    ]
}

fn client() -> impl Strategy<Value = String> {
    prop::sample::select(&CLIENTS).prop_map(str::to_owned)
}

fn item() -> impl Strategy<Value = String> {
    (0..POOL).prop_map(|id| format!("k{id}"))
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (client(), item(), 0u16..1000, any::<bool>(), any::<bool>()).prop_map(
            |(client, item, n, done, with_meta)| Op::Put {
                client,
                item,
                label: format!("l{n}"),
                rank: f64::from(n) / 7.0,
                done,
                meta: if with_meta { json!({ "s": n }) } else { Value::Null },
            },
        ),
        2 => (client(), item()).prop_map(|(client, item)| Op::Del { client, item }),
        1 => client().prop_map(|client| Op::Reject { client }),
        2 => (item(), 0u8..50, any::<bool>()).prop_map(|(item, n, put)| {
            let sql = if put {
                format!(
                    "INSERT INTO item (id,label,rank,done,meta) VALUES ('{item}','u{n}',{},0,NULL) ON CONFLICT (id) DO UPDATE SET label=excluded.label",
                    f64::from(n) / 3.0
                )
            } else {
                format!("DELETE FROM item WHERE id='{item}'")
            };
            Op::Upstream { sql }
        }),
        3 => client().prop_map(|client| Op::Pull { client }),
        1 => Just(Op::Invalidate),
        8 => query_op(),
    ]
}

fn trace() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(op(), 1..=64).prop_map(|mut ops| {
        // Observe late state and verify the unchanged response after convergence.
        for client in CLIENTS {
            ops.push(Op::Pull {
                client: client.into(),
            });
            ops.push(Op::Pull {
                client: client.into(),
            });
        }
        // Always leave two independently useful query observations in the
        // minimized trace. These make AND and order/limit mutants shrink to a
        // handful of operations instead of relying on a lucky random suffix.
        let specs = query_specs();
        for index in [0, 1] {
            let (hash, ast, transform_version) = specs[index].clone();
            ops.push(Op::QueryPut {
                hash,
                ast,
                transform_version,
            });
        }
        ops.push(Op::QueryPull);
        ops.push(Op::QueryPull);
        ops
    })
}

fn fixed_client(rng: &mut Rng) -> String {
    CLIENTS[rng.below(3) as usize].to_owned()
}

fn fixed_item(rng: &mut Rng) -> String {
    format!("k{}", rng.below(u64::from(POOL)))
}

// Preserve the original stable corpus alongside proptest. These long traces
// provide coverage-neutral migration while the property lane adds shrinking.
fn fixed_trace(seed: u64, steps: u64) -> Vec<Op> {
    let mut rng = Rng(seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(0xABC));
    let mut ops = Vec::new();
    let mut seq = 0u64;
    for step in 0..steps {
        match rng.below(7) {
            0 | 1 => {
                seq += 1;
                // Keep PRNG consumption byte-for-byte compatible with the
                // pre-proptest fixed corpus.
                let rank = rng.below(1000) as f64 / 7.0;
                let client = fixed_client(&mut rng);
                let item = fixed_item(&mut rng);
                let done = rng.boolean();
                let meta = if rng.boolean() {
                    json!({ "s": seq })
                } else {
                    Value::Null
                };
                ops.push(Op::Put {
                    client,
                    item,
                    label: format!("l{seq}"),
                    rank,
                    done,
                    meta,
                });
            }
            2 => ops.push(Op::Del {
                client: fixed_client(&mut rng),
                item: fixed_item(&mut rng),
            }),
            3 => ops.push(Op::Reject {
                client: fixed_client(&mut rng),
            }),
            4 => {
                let item = fixed_item(&mut rng);
                let sql = if rng.boolean() {
                    seq += 1;
                    format!(
                        "INSERT INTO item (id,label,rank,done,meta) VALUES ('{item}','u{seq}',{},0,NULL) ON CONFLICT (id) DO UPDATE SET label=excluded.label",
                        rng.below(50) as f64 / 3.0
                    )
                } else {
                    format!("DELETE FROM item WHERE id='{item}'")
                };
                ops.push(Op::Upstream { sql });
            }
            5 => ops.push(Op::Pull {
                client: fixed_client(&mut rng),
            }),
            _ if step % 40 == 39 => ops.push(Op::Invalidate),
            _ => ops.push(Op::Pull {
                client: fixed_client(&mut rng),
            }),
        }
    }
    for client in CLIENTS {
        ops.push(Op::Pull {
            client: client.into(),
        });
        ops.push(Op::Pull {
            client: client.into(),
        });
    }
    ops
}

fn fixed_query_trace() -> Vec<Op> {
    // SQLite's default BINARY collation sorts uppercase P before lowercase o.
    // Keep this in the stable corpus so the independent TS comparator cannot
    // drift back to locale-sensitive ordering.
    let mut ops = vec![
        Op::QueryProject {
            id: "p0".into(),
            owner_id: "u0".into(),
            name: "P0".into(),
        },
        Op::QueryProject {
            id: "p1".into(),
            owner_id: "u0".into(),
            name: "P0".into(),
        },
    ];
    for (hash, ast, transform_version) in query_specs() {
        ops.push(Op::QueryPut {
            hash,
            ast,
            transform_version,
        });
    }
    ops.extend([
        Op::QueryTask {
            id: "t4".into(),
            project_id: "p0".into(),
            title: "new top".into(),
            rank: 10,
            done: true,
            due_at: None,
        },
        Op::QueryPull,
        Op::QueryDelete {
            table: "task".into(),
            id: "t4".into(),
        },
        Op::QueryPull,
        Op::QueryDel {
            hash: "top_tasks".into(),
        },
        Op::QueryClear,
        Op::QueryPull,
        Op::QueryPull,
    ]);
    ops
}

fn query_tables() -> Tables {
    Tables::from_zero_schema(&json!({
        "tables": {
            "project": {
                "columns": {
                    "id": { "type": "string" },
                    "ownerId": { "type": "string" },
                    "name": { "type": "string" },
                },
                "primaryKey": ["id"],
            },
            "member": {
                "columns": {
                    "id": { "type": "string" },
                    "projectId": { "type": "string" },
                    "userId": { "type": "string" },
                },
                "primaryKey": ["id"],
            },
            "task": {
                "columns": {
                    "id": { "type": "string" },
                    "projectId": { "type": "string" },
                    "title": { "type": "string" },
                    "rank": { "type": "number" },
                    "done": { "type": "boolean" },
                    "dueAt": { "type": "number" },
                },
                "primaryKey": ["id"],
            },
            "user": {
                "columns": {
                    "id": { "type": "string" },
                    "name": { "type": "string" },
                },
                "primaryKey": ["id"],
            },
        },
    }))
    .unwrap()
}

struct QueryHost {
    db: TestDb,
    tables: Tables,
    cookie: Value,
    version: i64,
}

impl QueryHost {
    fn new() -> Self {
        let mut db = TestDb::memory();
        for ddl in [
            "CREATE TABLE project (id TEXT PRIMARY KEY, ownerId TEXT, name TEXT)",
            "CREATE TABLE member (id TEXT PRIMARY KEY, projectId TEXT, userId TEXT)",
            "CREATE TABLE task (id TEXT PRIMARY KEY, projectId TEXT, title TEXT, rank INTEGER, done INTEGER, dueAt INTEGER)",
            "CREATE TABLE user (id TEXT PRIMARY KEY, name TEXT)",
        ] {
            db.exec(ddl, &[]).unwrap();
        }
        db.exec(
            "INSERT INTO project VALUES
             ('p0','u0','A'),('p1','u1','B'),('p2','u0','C'),('p3','u0','outside')",
            &[],
        )
        .unwrap();
        db.exec(
            "INSERT INTO member VALUES ('m0','p0','u0'),('m1','p0','u1'),('m2','p2','u2')",
            &[],
        )
        .unwrap();
        db.exec(
            "INSERT INTO task VALUES
             ('t0','p0','T0',1,0,NULL),('t1','p0','T1',2,1,10),
             ('t2','p0','T2',3,0,20),('t3','p2','T3',5,1,NULL)",
            &[],
        )
        .unwrap();
        db.exec(
            "INSERT INTO user VALUES ('u0','U0'),('u1','U1'),('u2','U2')",
            &[],
        )
        .unwrap();
        let tables = query_tables();
        init_schema(&mut db, &tables).unwrap();
        init_query_schema(&mut db).unwrap();
        QueryHost {
            db,
            tables,
            cookie: Value::Null,
            version: 0,
        }
    }

    fn pull(&mut self, patch: Option<Vec<Value>>) -> Value {
        let mut body = json!({
            "clientID": "query-client",
            "clientGroupID": "query-group",
            "cookie": self.cookie,
        });
        if let Some(patch) = patch {
            self.version += 1;
            body["queries"] = json!({ "version": self.version, "patch": patch });
        }
        let tables = self.tables.clone();
        let response = self
            .db
            .transaction(|db| handle_query_pull(db, &tables, 4096, &body, "u1"))
            .unwrap();
        self.cookie = response["cookie"].clone();
        response
    }

    fn contains(&mut self, table: &str, id: &str) -> bool {
        let sql = match table {
            "project" => "SELECT id FROM project WHERE id = ?",
            "member" => "SELECT id FROM member WHERE id = ?",
            "task" => "SELECT id FROM task WHERE id = ?",
            "user" => "SELECT id FROM user WHERE id = ?",
            _ => unreachable!("generated query table"),
        };
        !self
            .db
            .query(sql, &[SqlValue::Text(id.into())])
            .unwrap()
            .is_empty()
    }

    fn upsert_project(&mut self, id: &str, owner_id: &str, name: &str) {
        let sql = if self.contains("project", id) {
            "UPDATE project SET ownerId = ?, name = ? WHERE id = ?"
        } else {
            "INSERT INTO project (ownerId, name, id) VALUES (?, ?, ?)"
        };
        self.db
            .exec(
                sql,
                &[
                    SqlValue::Text(owner_id.into()),
                    SqlValue::Text(name.into()),
                    SqlValue::Text(id.into()),
                ],
            )
            .unwrap();
    }

    fn upsert_member(&mut self, id: &str, project_id: &str, user_id: &str) {
        let sql = if self.contains("member", id) {
            "UPDATE member SET projectId = ?, userId = ? WHERE id = ?"
        } else {
            "INSERT INTO member (projectId, userId, id) VALUES (?, ?, ?)"
        };
        self.db
            .exec(
                sql,
                &[
                    SqlValue::Text(project_id.into()),
                    SqlValue::Text(user_id.into()),
                    SqlValue::Text(id.into()),
                ],
            )
            .unwrap();
    }

    fn upsert_task(
        &mut self,
        id: &str,
        project_id: &str,
        title: &str,
        rank: i64,
        done: bool,
        due_at: Option<i64>,
    ) {
        let sql = if self.contains("task", id) {
            "UPDATE task SET projectId = ?, title = ?, rank = ?, done = ?, dueAt = ? WHERE id = ?"
        } else {
            "INSERT INTO task (projectId, title, rank, done, dueAt, id) VALUES (?, ?, ?, ?, ?, ?)"
        };
        self.db
            .exec(
                sql,
                &[
                    SqlValue::Text(project_id.into()),
                    SqlValue::Text(title.into()),
                    SqlValue::Integer(rank),
                    SqlValue::Integer(i64::from(done)),
                    due_at.map_or(SqlValue::Null, SqlValue::Integer),
                    SqlValue::Text(id.into()),
                ],
            )
            .unwrap();
    }

    fn upsert_user(&mut self, id: &str, name: &str) {
        let sql = if self.contains("user", id) {
            "UPDATE user SET name = ? WHERE id = ?"
        } else {
            "INSERT INTO user (name, id) VALUES (?, ?)"
        };
        self.db
            .exec(
                sql,
                &[SqlValue::Text(name.into()), SqlValue::Text(id.into())],
            )
            .unwrap();
    }

    fn delete(&mut self, table: &str, id: &str) {
        let sql = match table {
            "project" => "DELETE FROM project WHERE id = ?",
            "member" => "DELETE FROM member WHERE id = ?",
            "task" => "DELETE FROM task WHERE id = ?",
            "user" => "DELETE FROM user WHERE id = ?",
            _ => unreachable!("generated query table"),
        };
        self.db.exec(sql, &[SqlValue::Text(id.into())]).unwrap();
    }
}

// run the trace through the Rust engine, returning the pull responses in order
fn run_rust(trace: &[Op]) -> Vec<Value> {
    let mut h = Host::new(true);
    let mut query = QueryHost::new();
    h.init();
    // uncapped, to mirror the reference core (which has no caps)
    h.caps = Caps {
        max_change_rows: 1_000_000,
        max_change_bytes: usize::MAX,
    };
    let mut next_id: HashMap<String, i64> = HashMap::new();
    let mut cookies: HashMap<String, Value> = HashMap::new();
    let mut pulls = Vec::new();

    for op in trace {
        match op {
            Op::Put {
                client,
                item,
                label,
                rank,
                done,
                meta,
            } => {
                let client = client.clone();
                let id = {
                    let e = next_id.entry(client.clone()).or_insert(0);
                    *e += 1;
                    *e
                };
                h.push_one(
                    "item.put",
                    json!({ "id": item, "label": label, "rank": rank, "done": done, "meta": meta }),
                    &client,
                    "g1",
                    id,
                    "u1",
                )
                .unwrap();
            }
            Op::Del { client, item } => {
                let client = client.clone();
                let id = {
                    let e = next_id.entry(client.clone()).or_insert(0);
                    *e += 1;
                    *e
                };
                h.push_one("item.del", json!({ "id": item }), &client, "g1", id, "u1")
                    .unwrap();
            }
            Op::Reject { client } => {
                let client = client.clone();
                let id = {
                    let e = next_id.entry(client.clone()).or_insert(0);
                    *e += 1;
                    *e
                };
                h.push_one("item.reject", json!({}), &client, "g1", id, "u1")
                    .unwrap();
            }
            Op::Upstream { sql } => h.exec(sql),
            Op::Invalidate => {
                h.db.transaction(|db| sync_core::invalidate(db)).unwrap();
            }
            Op::Pull { client } => {
                let client = client.clone();
                let cookie = cookies.get(&client).cloned().unwrap_or(json!(null));
                let resp = h.pull_as(&client, "g1", cookie, None, "u1").unwrap();
                cookies.insert(client, resp["cookie"].clone());
                pulls.push(json!({ "lane": "base", "response": resp }));
            }
            Op::QueryPut {
                hash,
                ast,
                transform_version,
            } => pulls.push(json!({
                "lane": "query",
                "response": query.pull(Some(vec![json!({
                    "op": "put",
                    "hash": hash,
                    "ast": ast,
                    "transformVersion": transform_version,
                })])),
            })),
            Op::QueryDel { hash } => pulls.push(json!({
                "lane": "query",
                "response": query.pull(Some(vec![json!({ "op": "del", "hash": hash })])),
            })),
            Op::QueryClear => pulls.push(json!({
                "lane": "query",
                "response": query.pull(Some(vec![json!({ "op": "clear" })])),
            })),
            Op::QueryPull => pulls.push(json!({
                "lane": "query",
                "response": query.pull(None),
            })),
            Op::QueryProject { id, owner_id, name } => query.upsert_project(id, owner_id, name),
            Op::QueryMember {
                id,
                project_id,
                user_id,
            } => query.upsert_member(id, project_id, user_id),
            Op::QueryTask {
                id,
                project_id,
                title,
                rank,
                done,
                due_at,
            } => query.upsert_task(id, project_id, title, *rank, *done, *due_at),
            Op::QueryUser { id, name } => query.upsert_user(id, name),
            Op::QueryDelete { table, id } => query.delete(table, id),
        }
    }
    pulls
}

fn run_ts(trace: &[Op]) -> Vec<Value> {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let oracle = format!("{manifest}/ts-oracle/run-oracle.ts");
    let sequence = TRACE_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let trace_path = std::env::temp_dir().join(format!(
        "sync-core-diff-{}-{sequence}.json",
        std::process::id(),
    ));
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&trace_path)
        .expect("create unique trace file");
    f.write_all(serde_json::to_string(trace).unwrap().as_bytes())
        .unwrap();
    drop(f);

    let output = Command::new("bun")
        .arg(&oracle)
        .arg(&trace_path)
        .output()
        .expect("run bun oracle — is bun on PATH? the differential test requires it");
    if !output.status.success() {
        panic!(
            "oracle failed; trace preserved at {}: {}\n{}",
            trace_path.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let parsed = serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "parse oracle json; trace preserved at {}: {error}",
            trace_path.display()
        )
    });
    std::fs::remove_file(&trace_path).expect("remove successful oracle trace");
    parsed
}

// (has_clear, sorted non-clear ops) — rowsPatch order is not semantic
fn normalize_patch(resp: &Value, base_lane: bool) -> (bool, Vec<String>) {
    let patch = resp["rowsPatch"].as_array().cloned().unwrap_or_default();
    let has_clear = patch.first() == Some(&json!({ "op": "clear" }));
    let mut ops: Vec<String> = patch
        .iter()
        .filter(|op| op["op"] != "clear")
        // the TS reference core fixture is identity-named while the Rust
        // differential fixture deliberately exercises serverName mappings.
        // canonicalize the reference response to the physical downstream wire
        // before comparing row semantics.
        .map(|op| {
            let op = if base_lane {
                physical_item_op(op.clone())
            } else {
                op.clone()
            };
            serde_json::to_string(&canonical_json(op)).unwrap()
        })
        .collect();
    ops.sort();
    (has_clear, ops)
}

fn physical_item_op(mut op: Value) -> Value {
    if op["tableName"] == "item" {
        op["tableName"] = json!("item_record");
    }
    for field in ["value", "id", "merge"] {
        let Some(row) = op.get_mut(field).and_then(Value::as_object_mut) else {
            continue;
        };
        for (logical, physical) in [
            ("id", "item_id"),
            ("label", "item_label"),
            ("rank", "sort_rank"),
            ("done", "is_done"),
            ("meta", "metadata_json"),
        ] {
            if let Some(value) = row.remove(logical) {
                row.insert(physical.to_string(), value);
            }
        }
    }
    if let Some(columns) = op.get_mut("constrain").and_then(Value::as_array_mut) {
        for column in columns {
            *column = match column.as_str() {
                Some("id") => json!("item_id"),
                Some("label") => json!("item_label"),
                Some("rank") => json!("sort_rank"),
                Some("done") => json!("is_done"),
                Some("meta") => json!("metadata_json"),
                _ => column.clone(),
            };
        }
    }
    op
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut entries = object.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut canonical = serde_json::Map::new();
            for (key, value) in entries {
                canonical.insert(key, canonical_json(value));
            }
            Value::Object(canonical)
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_json).collect()),
        scalar => scalar,
    }
}

fn compare(rust: &[Value], ts: &[Value]) -> Result<(), String> {
    if rust.len() != ts.len() {
        return Err(format!(
            "pull count differs: rust={} ts={}",
            rust.len(),
            ts.len()
        ));
    }
    for (i, (r, t)) in rust.iter().zip(ts.iter()).enumerate() {
        if r["lane"] != t["lane"] {
            return Err(format!("observation {i}: lane differs\nrust={r}\nts={t}"));
        }
        let base_lane = r["lane"] == "base";
        let r = &r["response"];
        let t = &t["response"];
        if r["cookie"] != t["cookie"] {
            return Err(format!("pull {i}: cookie differs\nrust={r}\nts={t}"));
        }
        if r.get("unchanged").is_some() != t.get("unchanged").is_some() {
            return Err(format!(
                "pull {i}: unchanged flag differs\nrust={r}\nts={t}"
            ));
        }
        if r.get("unchanged").is_some() {
            continue;
        }
        let rust_patch = normalize_patch(r, base_lane);
        let ts_patch = normalize_patch(t, base_lane);
        if rust_patch != ts_patch {
            return Err(format!(
                "pull {i}: rowsPatch differs\nrust={r}\nts={t}\nnormalized rust={rust_patch:?}\nnormalized ts={ts_patch:?}"
            ));
        }
        // rust lmids must be a subset of ts lmids with identical values
        let rl = r["lastMutationIDChanges"].as_object().unwrap();
        let tl = t["lastMutationIDChanges"].as_object().unwrap();
        for (client, lmid) in rl {
            if tl.get(client) != Some(lmid) {
                return Err(format!(
                    "pull {i}: rust acks {client}->{lmid} not matched by ts {t}"
                ));
            }
        }
        if !base_lane && r["gotQueries"] != t["gotQueries"] {
            return Err(format!("pull {i}: gotQueries differs\nrust={r}\nts={t}"));
        }
    }
    Ok(())
}

fn property_config() -> Config {
    let default = Config::default();
    // Each case launches the black-box Bun oracle. Keep pull requests bounded;
    // nightly can raise this without changing code, e.g. PROPTEST_CASES=256.
    let cases = if std::env::var_os("PROPTEST_CASES").is_none() {
        12
    } else {
        default.cases
    };
    Config {
        cases,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/proptest-regressions/differential.txt"
        )))),
        ..default
    }
}

fn failure_envelope(reason: String, ops: &[Op], process_id: u32) -> Value {
    let cases = std::env::var("PROPTEST_CASES").unwrap_or_else(|_| "12".into());
    let seed = std::env::var("PROPTEST_RNG_SEED").ok();
    // The producer gives this envelope a stable basename under a process-id
    // directory. Discovering that suffix beneath cwd works both locally and
    // after upload-artifact strips the results-tree root during extraction.
    let replay_path = format!(
        "$(find . -type f -path '*/{process_id}/sync-core-differential-minimized.json' -exec realpath {{}} \\; -quit)"
    );
    json!({
        "schemaVersion": 1,
        "kind": "sync-core-differential",
        "seed": {
            "value": seed.as_deref().unwrap_or("persisted by proptest after failure"),
            "source": if seed.is_some() { "PROPTEST_RNG_SEED" } else { "proptest runner" },
        },
        "generator": {
            "name": "sync-core-operation-state-machine",
            "version": 2,
            "cases": cases,
        },
        "replay": {
            "command": format!(
                "SYNC_CORE_REPLAY=\"{}\" cargo test -p sync-core --test differential replay_saved_differential_trace -- --ignored --exact --nocapture",
                replay_path
            ),
            "env": {},
        },
        "input": { "trace": ops },
        "failure": { "message": reason },
    })
}

fn persist_failure_envelope(reason: String, ops: &[Op]) -> (std::path::PathBuf, Value) {
    // Proptest invokes the body repeatedly while shrinking. Replacing this
    // process-scoped file means the final write is the latest (minimized)
    // failing input instead of leaving one artifact per shrink attempt.
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("sync-core is nested under workspace/crates");
    let process_id = std::process::id();
    let path = workspace
        .join("target/sync-core-differential")
        .join(process_id.to_string())
        .join("sync-core-differential-minimized.json");
    std::fs::create_dir_all(path.parent().unwrap()).expect("create differential results dir");
    let staging = path.with_extension("json.writing");
    let envelope = failure_envelope(reason, ops, process_id);
    std::fs::write(&staging, serde_json::to_vec_pretty(&envelope).unwrap())
        .expect("write differential failure artifact");
    std::fs::rename(&staging, &path).expect("publish differential failure artifact");
    (path, envelope)
}

#[test]
#[ignore = "set SYNC_CORE_REPLAY to a saved differential envelope"]
fn replay_saved_differential_trace() {
    let path = std::env::var_os("SYNC_CORE_REPLAY").expect("SYNC_CORE_REPLAY artifact path");
    let bytes = std::fs::read(&path).expect("read SYNC_CORE_REPLAY artifact");
    let envelope: Value = serde_json::from_slice(&bytes).expect("parse replay envelope");
    assert_eq!(envelope["schemaVersion"], 1, "unsupported replay schema");
    assert_eq!(
        envelope["kind"], "sync-core-differential",
        "wrong replay artifact kind"
    );
    let ops: Vec<Op> = serde_json::from_value(envelope["input"]["trace"].clone())
        .expect("deserialize replay input.trace");
    let rust = run_rust(&ops);
    let ts = run_ts(&ops);
    if let Err(reason) = compare(&rust, &ts) {
        panic!(
            "replayed differential failure from {}: {reason}",
            std::path::Path::new(&path).display()
        );
    }
}

#[test]
fn rust_matches_the_ts_reference_core_on_fixed_traces() {
    for seed in 0..8 {
        let mut ops = fixed_trace(seed, 200);
        ops.extend(fixed_query_trace());
        let rust = run_rust(&ops);
        let ts = run_ts(&ops);
        if let Err(reason) = compare(&rust, &ts) {
            panic!(
                "fixed seed {seed}: {reason}\ntrace={}",
                serde_json::to_string_pretty(&ops).unwrap()
            );
        }
    }
}

proptest! {
    #![proptest_config(property_config())]

    #[test]
    fn rust_matches_the_ts_reference_core_on_generated_traces(ops in trace()) {
        let rust = run_rust(&ops);
        let ts = run_ts(&ops);
        if let Err(reason) = compare(&rust, &ts) {
            let (artifact_path, envelope) = persist_failure_envelope(reason, &ops);
            let artifact = serde_json::to_string_pretty(&envelope).unwrap();
            prop_assert!(false,
                "minimized failure artifact saved at {}:\n{artifact}\n\nReplay with replay.command in the artifact. The minimized seed is also saved in crates/sync-core/proptest-regressions/differential.txt; to replay the original RNG stream, use the seed printed by proptest as PROPTEST_RNG_SEED with PROPTEST_CASES=1.",
                artifact_path.display()
            );
        }
    }
}
