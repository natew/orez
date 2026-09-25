// wake targeting must name every client group whose next pull carries a row
// change (superset), while leaving untouched groups asleep. the oracle drives
// random writes against a control-plane-shaped fixture (projects gated by
// grants, accounts reached through members, related children, a global table
// and a NOT EXISTS), then pulls every group and fails if any group that
// received rows was not targeted.
mod common;

use std::collections::BTreeSet;

use common::TestDb;
use serde_json::{Value, json};

use sync_core::query::{WakeTargets, handle_query_pull, init_query_schema, wake_targets};
use sync_core::schema::TableSpec;
use sync_core::value::ZeroColumnType;
use sync_core::{SyncDb, Tables, Transactor, init_schema, watermark};

fn tables() -> Tables {
    use ZeroColumnType::*;
    let spec = |columns: &[(&str, ZeroColumnType)]| TableSpec {
        columns: columns.iter().map(|(n, t)| (n.to_string(), *t)).collect(),
        primary_key: vec!["id".into()],
        encrypted_columns: Default::default(),
        encrypted_physical_columns: Default::default(),
    };
    Tables::new()
        .with(
            "project",
            spec(&[
                ("id", String),
                ("accountId", String),
                ("state", String),
                ("createdAt", Number),
            ]),
        )
        .with(
            "grant",
            spec(&[
                ("id", String),
                ("resourceType", String),
                ("resourceId", String),
                ("userId", String),
            ]),
        )
        .with("account", spec(&[("id", String), ("name", String)]))
        .with(
            "member",
            spec(&[
                ("id", String),
                ("accountId", String),
                ("userId", String),
                ("role", String),
            ]),
        )
        .with("userPublic", spec(&[("id", String), ("name", String)]))
        .with("flag", spec(&[("id", String), ("on", Boolean)]))
}

const DDL: &[&str] = &[
    "CREATE TABLE project (id TEXT PRIMARY KEY, accountId TEXT, state TEXT, createdAt INTEGER)",
    "CREATE TABLE \"grant\" (id TEXT PRIMARY KEY, resourceType TEXT, resourceId TEXT, userId TEXT)",
    "CREATE TABLE account (id TEXT PRIMARY KEY, name TEXT)",
    "CREATE TABLE member (id TEXT PRIMARY KEY, accountId TEXT, userId TEXT, role TEXT)",
    "CREATE TABLE userPublic (id TEXT PRIMARY KEY, name TEXT)",
    "CREATE TABLE flag (id TEXT PRIMARY KEY, \"on\" INTEGER)",
];

fn eq(col: &str, value: Value) -> Value {
    json!({ "type": "simple", "op": "=", "left": { "type": "column", "name": col },
            "right": { "type": "literal", "value": value } })
}
fn ne(col: &str, value: Value) -> Value {
    json!({ "type": "simple", "op": "!=", "left": { "type": "column", "name": col },
            "right": { "type": "literal", "value": value } })
}
fn and(conditions: Vec<Value>) -> Value {
    json!({ "type": "and", "conditions": conditions })
}
fn or(conditions: Vec<Value>) -> Value {
    json!({ "type": "or", "conditions": conditions })
}
fn exists(parent: &str, child: &str, subquery: Value, negated: bool) -> Value {
    json!({ "type": "correlatedSubquery", "op": if negated { "NOT EXISTS" } else { "EXISTS" },
            "related": { "correlation": { "parentField": [parent], "childField": [child] },
                         "subquery": subquery } })
}
fn grant_of(user: &str) -> Value {
    exists(
        "id",
        "resourceId",
        json!({ "table": "grant", "where": and(vec![eq("resourceType", json!("project")), eq("userId", json!(user))]) }),
        false,
    )
}
fn admin_account(user: &str) -> Value {
    exists(
        "accountId",
        "id",
        json!({ "table": "account", "where": exists("id", "accountId", json!({
            "table": "member",
            "where": and(vec![eq("userId", json!(user)), or(vec![eq("role", json!("owner")), eq("role", json!("admin"))])]),
        }), false) }),
        false,
    )
}

// the queries one user's group desires, shaped like Contrast's control plane
fn queries(user: &str) -> Vec<(String, Value)> {
    vec![
        (
            "projectsByUser".into(),
            json!({ "table": "project",
                    "where": and(vec![ne("state", json!("deleted")), grant_of(user)]),
                    "orderBy": [["createdAt", "desc"], ["id", "asc"]], "limit": 3 }),
        ),
        (
            "projectById".into(),
            json!({ "table": "project",
                    "where": and(vec![grant_of(user), eq("id", json!("p1"))]), "limit": 1 }),
        ),
        (
            "accountProjects".into(),
            json!({ "table": "project",
                    "where": or(vec![admin_account(user), grant_of(user)]) }),
        ),
        (
            "accountMembers".into(),
            json!({ "table": "member",
                    "where": exists("accountId", "id", json!({ "table": "account",
                        "where": exists("id", "accountId", json!({ "table": "member",
                            "where": eq("userId", json!(user)) }), false) }), false),
                    "related": [{ "correlation": { "parentField": ["userId"], "childField": ["id"] },
                                  "subquery": { "table": "userPublic" } }] }),
        ),
        (
            "flags".into(),
            json!({ "table": "flag", "where": eq("on", json!(true)) }),
        ),
        (
            "unsharedProjects".into(),
            json!({ "table": "project",
                    "where": exists("id", "resourceId", json!({ "table": "grant",
                        "where": eq("userId", json!(user)) }), true) }),
        ),
    ]
}

struct Group {
    user: String,
    client: String,
    cookie: Value,
}

struct Fixture {
    db: TestDb,
    tables: Tables,
    groups: Vec<Group>,
}

impl Fixture {
    fn new(users: usize) -> Fixture {
        let mut db = TestDb::memory();
        for ddl in DDL {
            db.exec(ddl, &[]).unwrap();
        }
        let tables = tables();
        init_schema(&mut db, &tables).unwrap();
        init_query_schema(&mut db).unwrap();
        let groups = (0..users)
            .map(|i| Group {
                user: format!("u{i}"),
                client: format!("c{i}"),
                cookie: Value::Null,
            })
            .collect();
        let mut fixture = Fixture { db, tables, groups };
        for i in 0..users {
            let patch: Vec<Value> = queries(&fixture.groups[i].user)
                .into_iter()
                .map(|(hash, ast)| json!({ "op": "put", "hash": hash, "ast": ast }))
                .collect();
            fixture.pull(i, Some(json!({ "version": 1, "patch": patch })));
        }
        fixture
    }

    // pull one group; true when its response carried row changes
    fn pull(&mut self, index: usize, queries: Option<Value>) -> bool {
        let group = &self.groups[index];
        let mut body = json!({
            "clientID": group.client,
            "clientGroupID": format!("g-{}", group.user),
            "cookie": group.cookie,
        });
        if let Some(queries) = queries {
            body["queries"] = queries;
        }
        let user = group.user.clone();
        let tables = self.tables.clone();
        let response = self
            .db
            .transaction(|db| handle_query_pull(db, &tables, 4096, &body, &user))
            .unwrap();
        self.groups[index].cookie = response["cookie"].clone();
        response["rowsPatch"]
            .as_array()
            .is_some_and(|patch| !patch.is_empty())
    }

    fn targets(&mut self, since: i64) -> WakeTargets {
        let tables = self.tables.clone();
        self.db
            .transaction(|db| wake_targets(db, &tables, since))
            .unwrap()
    }

    fn watermark(&mut self) -> i64 {
        watermark(&mut self.db).unwrap()
    }
}

// a small deterministic generator, so a failure reproduces from its seed
struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
    fn pick<'a>(&mut self, items: &[&'a str]) -> &'a str {
        items[(self.next() as usize) % items.len()]
    }
}

fn random_write(rng: &mut Lcg, users: usize) -> String {
    let user = format!("u{}", rng.next() as usize % users);
    let id = |rng: &mut Lcg, prefix: &str| format!("{prefix}{}", rng.next() % 6);
    match rng.next() % 6 {
        0 => {
            let project = id(rng, "p");
            let account = id(rng, "a");
            let state = rng.pick(&["active", "active", "deleted"]);
            let created = rng.next() % 50;
            format!(
                "INSERT INTO project VALUES ('{project}', '{account}', '{state}', {created})
                 ON CONFLICT (id) DO UPDATE SET accountId = excluded.accountId,
                 state = excluded.state, createdAt = excluded.createdAt"
            )
        }
        1 => {
            let grant = id(rng, "g");
            let project = id(rng, "p");
            let kind = rng.pick(&["project", "project", "repo"]);
            format!(
                "INSERT INTO \"grant\" VALUES ('{grant}', '{kind}', '{project}', '{user}')
                 ON CONFLICT (id) DO UPDATE SET resourceType = excluded.resourceType,
                 resourceId = excluded.resourceId, userId = excluded.userId"
            )
        }
        2 => {
            let member = id(rng, "m");
            let account = id(rng, "a");
            let role = rng.pick(&["owner", "admin", "viewer"]);
            format!(
                "INSERT INTO member VALUES ('{member}', '{account}', '{user}', '{role}')
                 ON CONFLICT (id) DO UPDATE SET accountId = excluded.accountId,
                 userId = excluded.userId, role = excluded.role"
            )
        }
        3 => {
            let name = rng.next() % 1000;
            format!(
                "INSERT INTO userPublic VALUES ('{user}', 'n{name}')
                 ON CONFLICT (id) DO UPDATE SET name = excluded.name"
            )
        }
        4 => {
            let table = rng.pick(&["project", "\"grant\"", "member", "account", "userPublic"]);
            let prefix = match table {
                "project" => "p",
                "\"grant\"" => "g",
                "member" => "m",
                "account" => "a",
                _ => "u",
            };
            let victim = id(rng, prefix);
            format!("DELETE FROM {table} WHERE id = '{victim}'")
        }
        _ => {
            if rng.next().is_multiple_of(2) {
                let account = id(rng, "a");
                format!("INSERT OR REPLACE INTO account VALUES ('{account}', 'x')")
            } else {
                let flag = id(rng, "f");
                let on = rng.next() % 2;
                format!("INSERT OR REPLACE INTO flag VALUES ('{flag}', {on})")
            }
        }
    }
}

#[test]
fn every_group_whose_pull_changes_is_targeted() {
    let users = 6;
    let mut fixture = Fixture::new(users);
    let mut rng = Lcg(0x5eed);
    let mut woke = 0usize;
    let mut changed = 0usize;
    let mut offered = 0usize;
    for round in 0..600 {
        let since = fixture.watermark();
        let writes: Vec<String> = (0..=rng.next() % 3)
            .map(|_| random_write(&mut rng, users))
            .collect();
        for sql in &writes {
            fixture.db.exec(sql, &[]).unwrap();
        }
        let targets = fixture.targets(since);
        for index in 0..users {
            let client = fixture.groups[index].client.clone();
            let targeted = match &targets {
                WakeTargets::All => true,
                WakeTargets::Clients(clients) => clients.contains(&client),
            };
            let got_rows = fixture.pull(index, None);
            offered += 1;
            woke += usize::from(targeted);
            changed += usize::from(got_rows);
            assert!(
                targeted || !got_rows,
                "round {round}: group {client} received rows but was not woken by {writes:?}"
            );
        }
    }
    // the point of targeting: most group-rounds stay asleep. the NOT EXISTS
    // query registers `all` on project and grant, so this fixture cannot get
    // near zero; it must still wake well under every group every time.
    assert!(
        changed > 0 && woke < offered,
        "woke {woke} of {offered}, {changed} changed"
    );
}

#[test]
fn a_users_own_writes_wake_only_that_users_group() {
    let users = 20;
    let mut fixture = Fixture::new(users);
    // make every group's NOT EXISTS query irrelevant to grants of other users
    // by exercising only tables and rows scoped to one user
    let since = fixture.watermark();
    fixture
        .db
        .exec(
            "INSERT INTO member VALUES ('m-new', 'a-new', 'u7', 'owner')",
            &[],
        )
        .unwrap();
    fixture
        .db
        .exec("INSERT INTO userPublic VALUES ('u7', 'seven')", &[])
        .unwrap();
    let WakeTargets::Clients(clients) = fixture.targets(since) else {
        panic!("a two-row write fell back to waking everyone");
    };
    assert_eq!(clients, BTreeSet::from(["c7".to_string()]));
}

#[test]
fn global_and_negated_shapes_fall_back_to_their_groups() {
    let mut fixture = Fixture::new(4);
    let since = fixture.watermark();
    fixture
        .db
        .exec("INSERT INTO flag VALUES ('f-new', 1)", &[])
        .unwrap();
    let WakeTargets::Clients(clients) = fixture.targets(since) else {
        panic!("one flag row fell back to waking everyone");
    };
    // the flag query has an anchor (on = true): every group registered it
    assert_eq!(clients.len(), 4);
    let since = fixture.watermark();
    fixture
        .db
        .exec("INSERT INTO flag VALUES ('f-off', 0)", &[])
        .unwrap();
    assert_eq!(
        fixture.targets(since),
        WakeTargets::Clients(BTreeSet::new())
    );
}

#[test]
fn plans_registered_before_the_wake_tables_are_rebuilt() {
    // enough groups that the stored queries span several backfill pages, and
    // the rebuild must reproduce every key the registrations stored
    let mut fixture = Fixture::new(300);
    let keys = |db: &mut TestDb| {
        db.query(
            "SELECT clientGroupID, hash, wakeTable, wakeKey FROM _zsync_wake_keys
             ORDER BY clientGroupID, hash, wakeTable, wakeKey",
            &[],
        )
        .unwrap()
        .into_iter()
        .map(|row| format!("{row:?}"))
        .collect::<Vec<_>>()
    };
    let queries = match fixture
        .db
        .query("SELECT COUNT(*) AS n FROM _zsync_queries", &[])
        .unwrap()[0]
        .get("n")
    {
        Some(sync_core::SqlValue::Integer(n)) => *n,
        _ => 0,
    };
    assert!(
        queries > 1_000,
        "{queries} stored queries fit one backfill page"
    );
    let registered = keys(&mut fixture.db);
    for table in [
        "_zsync_wake_keys",
        "_zsync_wake_recipes",
        "_zsync_wake_meta",
    ] {
        fixture
            .db
            .exec(&format!("DELETE FROM {table}"), &[])
            .unwrap();
    }
    let since = fixture.watermark();
    fixture
        .db
        .exec(
            "INSERT INTO member VALUES ('m-x', 'a-x', 'u1', 'owner')",
            &[],
        )
        .unwrap();
    assert_eq!(
        fixture.targets(since),
        WakeTargets::Clients(BTreeSet::from(["c1".to_string()]))
    );
    let rebuilt = keys(&mut fixture.db);
    assert_eq!(rebuilt.len(), registered.len());
    assert!(
        rebuilt == registered,
        "rebuilt keys differ from the registered ones"
    );
    let recipes = match fixture
        .db
        .query("SELECT COUNT(*) AS n FROM _zsync_wake_recipes", &[])
        .unwrap()[0]
        .get("n")
    {
        Some(sync_core::SqlValue::Integer(n)) => *n,
        _ => 0,
    };
    assert!(recipes > 0);
}

// targeting cost is per changed row, never per connected group: the same
// user-scoped write issues the same statements with 5 groups or 60
#[test]
fn targeting_cost_does_not_grow_with_connected_groups() {
    struct Counting<'a> {
        db: &'a mut dyn SyncDb,
        statements: usize,
    }
    impl SyncDb for Counting<'_> {
        fn exec(
            &mut self,
            sql: &str,
            params: &[sync_core::SqlValue],
        ) -> Result<(), sync_core::DbError> {
            self.statements += 1;
            self.db.exec(sql, params)
        }
        fn query(
            &mut self,
            sql: &str,
            params: &[sync_core::SqlValue],
        ) -> Result<Vec<sync_core::Row>, sync_core::DbError> {
            self.statements += 1;
            self.db.query(sql, params)
        }
    }
    let statements = |users: usize| {
        let mut fixture = Fixture::new(users);
        let tables = fixture.tables.clone();
        // build the plans once so the measured call is a steady-state one
        let warm = fixture.watermark();
        fixture.targets(warm);
        let since = fixture.watermark();
        fixture
            .db
            .exec(
                "INSERT INTO member VALUES ('m-cost', 'a-cost', 'u1', 'owner')",
                &[],
            )
            .unwrap();
        let mut counting = Counting {
            db: &mut fixture.db,
            statements: 0,
        };
        let targets = wake_targets(&mut counting, &tables, since).unwrap();
        assert_eq!(
            targets,
            WakeTargets::Clients(BTreeSet::from(["c1".to_string()]))
        );
        counting.statements
    };
    assert_eq!(statements(5), statements(60));
}
