// the fixture the native host serves: table spec, DDL, deterministic seed
// install, the built-in mutators, and the optional per-user visibility policy.
// ported from harness/src/fixture-data.ts (mutators + DDL + tables) and
// harness/src/permissions.ts (fixtureVisibility) so rust-local lanes need no
// app-server sidecar. these are plain host functions; engine.rs wraps them in
// sync-core's Mutator/Visibility traits.

use serde_json::Value;
use std::collections::HashSet;

use sync_core::{DbError, SqlValue, SyncDb};

use crate::seed::seed_rows;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ColType {
    String,
    Number,
    Boolean,
    Json,
}

pub struct TableSpec {
    pub name: &'static str,
    pub columns: &'static [(&'static str, ColType)],
    pub primary_key: &'static [&'static str],
}

// build sync-core Tables from the fixture TableSpecs. used by the binary
// and by tests that need the standard fixture schema.
pub fn build_tables() -> sync_core::schema::Tables {
    use sync_core::schema::TableSpec as CoreSpec;
    use sync_core::value::ZeroColumnType;
    let mut tables = sync_core::schema::Tables::new();
    for spec in TABLES {
        let columns = spec
            .columns
            .iter()
            .map(|(name, ct)| {
                let zt = match ct {
                    ColType::String => ZeroColumnType::String,
                    ColType::Number => ZeroColumnType::Number,
                    ColType::Boolean => ZeroColumnType::Boolean,
                    ColType::Json => ZeroColumnType::Json,
                };
                (name.to_string(), zt)
            })
            .collect();
        let primary_key = spec.primary_key.iter().map(|s| s.to_string()).collect();
        tables.push(
            spec.name,
            CoreSpec {
                columns,
                primary_key,
                encrypted_columns: Default::default(),
                encrypted_physical_columns: Default::default(),
            },
        );
    }
    tables
}

// mirror of the zero schema (fixture.ts guards TABLES against drift). order is
// deterministic so snapshot patches are stable across hosts.
pub const TABLES: &[TableSpec] = &[
    TableSpec {
        name: "user",
        columns: &[("id", ColType::String), ("name", ColType::String)],
        primary_key: &["id"],
    },
    TableSpec {
        name: "project",
        columns: &[
            ("id", ColType::String),
            ("ownerId", ColType::String),
            ("name", ColType::String),
        ],
        primary_key: &["id"],
    },
    TableSpec {
        name: "member",
        columns: &[
            ("id", ColType::String),
            ("projectId", ColType::String),
            ("userId", ColType::String),
        ],
        primary_key: &["id"],
    },
    TableSpec {
        name: "task",
        columns: &[
            ("id", ColType::String),
            ("projectId", ColType::String),
            ("title", ColType::String),
            ("rank", ColType::Number),
            ("done", ColType::Boolean),
            ("meta", ColType::Json),
            ("dueAt", ColType::Number),
        ],
        primary_key: &["id"],
    },
];

// valid in both postgres and sqlite — every target runs the same statements.
// rank is `double precision` (REAL affinity), not `real`, so it round-trips
// as an 8-byte double instead of pg float4 noise.
pub const DDL: &[&str] = &[
    r#"CREATE TABLE "user" (id text PRIMARY KEY, name text NOT NULL)"#,
    r#"CREATE TABLE project (id text PRIMARY KEY, "ownerId" text NOT NULL, name text NOT NULL)"#,
    r#"CREATE TABLE member (id text PRIMARY KEY, "projectId" text NOT NULL, "userId" text NOT NULL)"#,
    r#"CREATE TABLE task (id text PRIMARY KEY, "projectId" text NOT NULL, title text NOT NULL,
    rank double precision NOT NULL, done boolean NOT NULL, meta jsonb, "dueAt" bigint)"#,
];

// run the DDL + insert the deterministic seed, once. idempotent across process
// restarts (checks for the `project` table first) so a hard-kill + reopen on
// the same file keeps its data. runs BEFORE the engine installs its triggers,
// so seed rows stay out of the change log (fresh clients snapshot anyway).
pub fn install_app_tables_and_seed(db: &mut dyn SyncDb) -> Result<(), DbError> {
    let existing = db.query(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'project'",
        &[],
    )?;
    if !existing.is_empty() {
        return Ok(());
    }
    for stmt in DDL {
        db.exec(stmt, &[])?;
    }
    for (table, columns, rows) in seed_rows() {
        let col_list = columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = columns.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let sql = format!("INSERT INTO \"{table}\" ({col_list}) VALUES ({placeholders})");
        for row in rows {
            db.exec(&sql, &row)?;
        }
    }
    Ok(())
}

// mutator outcome: an app error rolls back the mutation's row changes but the
// engine still advances the LMID; a db/unknown error is an infra failure.
#[derive(Debug)]
pub enum MutateError {
    App(String),
    Db(DbError),
    Unknown(String),
}

impl From<DbError> for MutateError {
    fn from(e: DbError) -> Self {
        MutateError::Db(e)
    }
}

fn text(args: &Value, key: &str) -> Result<String, MutateError> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| MutateError::Unknown(format!("missing string arg {key}")))
}

// the built-in mutators — same names + semantics as fixture-data.ts
// executeMutator + the client registry in fixture.ts (e.g. project.delete does
// NOT cascade, matching the client mutator). args is the first push arg object.
pub fn run_mutator(
    db: &mut dyn SyncDb,
    name: &str,
    args: &Value,
    _user_id: &str,
) -> Result<(), MutateError> {
    match name {
        "exactlyOnce.incrementProbe" => {
            let id = text(args, "id")?;
            let rows = db.query(
                "SELECT rank FROM task WHERE id = ?",
                &[SqlValue::Text(id.clone())],
            )?;
            if rows.len() != 1 {
                return Err(MutateError::App("probe-not-found".into()));
            }
            db.exec(
                "UPDATE task SET rank = rank + 1 WHERE id = ?",
                &[SqlValue::Text(id)],
            )?;
            Ok(())
        }
        "exactlyOnce.incrementThenReject" => {
            let id = text(args, "id")?;
            let rows = db.query(
                "SELECT rank FROM task WHERE id = ?",
                &[SqlValue::Text(id.clone())],
            )?;
            if rows.len() != 1 {
                return Err(MutateError::App("probe-not-found".into()));
            }
            db.exec(
                "UPDATE task SET rank = rank + 1 WHERE id = ?",
                &[SqlValue::Text(id)],
            )?;
            Err(MutateError::App("intentional-reject".into()))
        }
        "atomicVisibility.appendGroup" => {
            let effects = args
                .get("effects")
                .and_then(Value::as_array)
                .filter(|effects| effects.len() >= 2)
                .ok_or_else(|| {
                    MutateError::Unknown("atomic append requires at least two effects".into())
                })?;
            let mut ids = HashSet::new();
            let mut identities = HashSet::new();
            let mut validated = Vec::with_capacity(effects.len());
            for effect in effects {
                let id = text(effect, "id")?;
                let project_id = text(effect, "projectId")?;
                let rank = effect
                    .get("rank")
                    .and_then(Value::as_f64)
                    .filter(|rank| {
                        rank.is_finite()
                            && rank.fract() == 0.0
                            && rank.abs() <= 9_007_199_254_740_991.0
                            && !(*rank == 0.0 && rank.is_sign_negative())
                    })
                    .ok_or_else(|| MutateError::Unknown("invalid atomic append rank".into()))?;
                if !ids.insert(id.clone())
                    || !identities.insert((project_id.clone(), rank.to_bits()))
                {
                    return Err(MutateError::Unknown(
                        "atomic append effects must be unique".into(),
                    ));
                }
                validated.push((id, project_id, rank));
            }
            for (id, project_id, rank) in validated {
                let title = format!("atomic-visibility:{id}");
                db.exec(
                    r#"INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt")
                       VALUES (?, ?, ?, ?, 0, NULL, NULL)"#,
                    &[
                        SqlValue::Text(id),
                        SqlValue::Text(project_id),
                        SqlValue::Text(title),
                        SqlValue::Real(rank),
                    ],
                )?;
            }
            Ok(())
        }
        "project.create" => {
            let id = text(args, "id")?;
            let owner = text(args, "ownerId")?;
            let name = text(args, "name")?;
            let exists = db.query(
                "SELECT 1 FROM project WHERE id = ?",
                &[SqlValue::Text(id.clone())],
            )?;
            if !exists.is_empty() {
                return Err(MutateError::App("exists".into()));
            }
            db.exec(
                r#"INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)"#,
                &[
                    SqlValue::Text(id),
                    SqlValue::Text(owner),
                    SqlValue::Text(name),
                ],
            )?;
            Ok(())
        }
        "project.rename" => {
            let id = text(args, "id")?;
            let name = text(args, "name")?;
            db.exec(
                "UPDATE project SET name = ? WHERE id = ?",
                &[SqlValue::Text(name), SqlValue::Text(id)],
            )?;
            Ok(())
        }
        "project.delete" => {
            let id = text(args, "id")?;
            db.exec("DELETE FROM project WHERE id = ?", &[SqlValue::Text(id)])?;
            Ok(())
        }
        "member.add" => {
            let id = text(args, "id")?;
            let project = text(args, "projectId")?;
            let user = text(args, "userId")?;
            db.exec(
                r#"INSERT INTO member (id, "projectId", "userId") VALUES (?, ?, ?)"#,
                &[
                    SqlValue::Text(id),
                    SqlValue::Text(project),
                    SqlValue::Text(user),
                ],
            )?;
            Ok(())
        }
        "member.remove" => {
            let id = text(args, "id")?;
            db.exec("DELETE FROM member WHERE id = ?", &[SqlValue::Text(id)])?;
            Ok(())
        }
        "task.create" => {
            let id = text(args, "id")?;
            let project = text(args, "projectId")?;
            let title = text(args, "title")?;
            let rank = args
                .get("rank")
                .and_then(Value::as_f64)
                .ok_or_else(|| MutateError::Unknown("missing number arg rank".into()))?;
            let done = args.get("done").and_then(Value::as_bool).unwrap_or(false);
            let meta = match args.get("meta") {
                None | Some(Value::Null) => SqlValue::Null,
                Some(v) => SqlValue::Text(v.to_string()),
            };
            let due_at = match args.get("dueAt") {
                None | Some(Value::Null) => SqlValue::Null,
                Some(v) => v
                    .as_i64()
                    .map(SqlValue::Integer)
                    .or_else(|| v.as_f64().map(SqlValue::Real))
                    .unwrap_or(SqlValue::Null),
            };
            db.exec(
                r#"INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt")
                   VALUES (?, ?, ?, ?, ?, ?, ?)"#,
                &[
                    SqlValue::Text(id),
                    SqlValue::Text(project),
                    SqlValue::Text(title),
                    SqlValue::Real(rank),
                    SqlValue::Integer(if done { 1 } else { 0 }),
                    meta,
                    due_at,
                ],
            )?;
            Ok(())
        }
        "task.toggle" => {
            let id = text(args, "id")?;
            let existing = db.query(
                "SELECT done FROM task WHERE id = ?",
                &[SqlValue::Text(id.clone())],
            )?;
            let Some(row) = existing.first() else {
                return Err(MutateError::App("not-found".into()));
            };
            let done = matches!(row.values.first(), Some(SqlValue::Integer(1)));
            db.exec(
                "UPDATE task SET done = ? WHERE id = ?",
                &[
                    SqlValue::Integer(if done { 0 } else { 1 }),
                    SqlValue::Text(id),
                ],
            )?;
            Ok(())
        }
        "task.setRank" => {
            let id = text(args, "id")?;
            let rank = args
                .get("rank")
                .and_then(Value::as_f64)
                .ok_or_else(|| MutateError::Unknown("missing number arg rank".into()))?;
            db.exec(
                "UPDATE task SET rank = ? WHERE id = ?",
                &[SqlValue::Real(rank), SqlValue::Text(id)],
            )?;
            Ok(())
        }
        other => Err(MutateError::Unknown(format!("unknown mutator: {other}"))),
    }
}

// optional per-user row visibility (permissions lane). returns a WHERE FRAGMENT
// (+ positional params) selecting the user's visible rows of `table`; sync-core
// exposes logical table and column names through CTEs before applying it.
// semantically identical to permissions.ts fixtureVisibility, rewritten from
// aliased full SELECTs to fragments to match sync-core's Visibility contract.
pub fn fixture_visible(table: &str, user_id: &str) -> Option<(String, Vec<SqlValue>)> {
    // project access predicate against the project table by two references: the
    // unaliased `project` row (used when filtering the project table itself)
    // and a `p`-aliased project subquery (used inside member/task EXISTS).
    let access = |project_ref: &str| {
        format!(
            r#"({project_ref}."ownerId" = ? OR EXISTS (
                SELECT 1 FROM member access
                WHERE access."projectId" = {project_ref}.id AND access."userId" = ?
            ))"#
        )
    };
    let uid = || SqlValue::Text(user_id.to_string());
    match table {
        "user" => Some(("id = ?".to_string(), vec![uid()])),
        "project" => Some((access("project"), vec![uid(), uid()])),
        "member" => Some((
            format!(
                r#"EXISTS (
                    SELECT 1 FROM project p
                    WHERE p.id = member."projectId" AND {}
                )"#,
                access("p")
            ),
            vec![uid(), uid()],
        )),
        "task" => Some((
            format!(
                r#"EXISTS (
                    SELECT 1 FROM project p
                    WHERE p.id = task."projectId" AND {}
                )"#,
                access("p")
            ),
            vec![uid(), uid()],
        )),
        _ => None,
    }
}
