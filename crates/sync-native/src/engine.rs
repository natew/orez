// the single integration seam between the native host and the sync-core
// engine. the host owns transaction begin/commit/rollback here (sync-core
// never emits BEGIN/COMMIT) and adapts the fixture mutators + visibility into
// sync-core's Mutator / Visibility / Transactor traits. every engine call runs
// on a namespace's writer thread, so the Connection is single-threaded and a
// plain BEGIN/COMMIT is the whole transaction story.

use rusqlite::Connection;
use serde_json::Value;

use sync_core::error::EngineError;
use sync_core::pull::{Caps, Visibility, VisibleFilter, handle_pull};
use sync_core::push::{Mutator, Transactor, handle_push};
use sync_core::schema::{TableSpec, Tables, init_schema};
use sync_core::value::ZeroColumnType;
use sync_core::{DbError, SyncDb};

use crate::db::RusqliteDb;
use crate::fixture::{self, ColType};

// process-wide engine configuration shared by every namespace worker.
pub struct EngineContext {
    pub tables: Tables,
    pub retain_changes: i64,
    pub visibility_enabled: bool,
    // query-aware mode: pulls carry desired queries and go through the
    // query-aware engine (membership/refcount) instead of the baseline
    // full-namespace pull. a namespace serves one consumer kind, not a mix.
    pub query_aware: bool,
}

impl EngineContext {
    pub fn new(retain_changes: i64, visibility_enabled: bool, query_aware: bool) -> Self {
        Self {
            tables: build_tables(),
            retain_changes,
            visibility_enabled,
            query_aware,
        }
    }

    // the fixture visibility policy is cross-table (membership), so it is NOT
    // row-local: a permission flip can revoke a row without touching it, which
    // no diff can express, so every pull falls back to a snapshot. matches the
    // reference core's `visible` behavior (permissions lane).
    fn visibility(&self) -> Option<Visibility<'static>> {
        if !self.visibility_enabled {
            return None;
        }
        Some(Visibility {
            row_local: false,
            filter: Box::new(|table, user| {
                fixture::fixture_visible(table, user)
                    .map(|(sql, params)| VisibleFilter { sql, params })
            }),
        })
    }
}

fn build_tables() -> Tables {
    let mut tables = Tables::new();
    for spec in fixture::TABLES {
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
            TableSpec {
                columns,
                primary_key,
            },
        );
    }
    tables
}

// worker init: install the fixture app tables + seed (host), then the engine's
// _zsync_* schema + triggers. triggers install AFTER the seed so seed rows
// stay out of the change log. idempotent across restart.
pub fn init_namespace(db: &mut dyn SyncDb, ctx: &EngineContext) -> Result<(), String> {
    fixture::install_app_tables_and_seed(db).map_err(|e| e.0)?;
    init_schema(db, &ctx.tables).map_err(|e| e.0)?;
    // the query-aware tables are idempotent + unused in baseline mode, so
    // install them always so a namespace can serve query-aware pulls.
    sync_core::query::init_query_schema(db).map_err(|e| e.message)?;
    Ok(())
}

// pull runs inside one host-entered transaction, matching the CF host's
// transactionSync. commit on Ok, roll back on Err (a 409 undoes the claim).
pub fn pull(
    conn: &Connection,
    ctx: &EngineContext,
    body: &Value,
    user_id: &str,
) -> Result<Value, EngineError> {
    conn.execute_batch("BEGIN").expect("BEGIN failed");
    let result = {
        let mut db = RusqliteDb::new(conn);
        if ctx.query_aware {
            // query-aware pull: desired queries in the body drive membership;
            // no whole-namespace visibility filter (permissions ride the AST).
            sync_core::query::handle_query_pull(
                &mut db,
                &ctx.tables,
                ctx.retain_changes,
                body,
                user_id,
            )
        } else {
            let visibility = ctx.visibility();
            handle_pull(
                &mut db,
                &ctx.tables,
                ctx.retain_changes,
                visibility.as_ref(),
                Caps::default(),
                body,
                user_id,
            )
        }
    };
    finish(conn, result.is_ok());
    result
}

// push drives the per-mutation tx1/tx2 steps through the Transactor; the CF
// host runs the same steps around its async JS mutator inside ctx.storage
// .transaction. row effects + LMID advance commit atomically per mutation.
pub fn push(
    conn: &Connection,
    ctx: &EngineContext,
    body: &Value,
    user_id: &str,
) -> Result<Value, EngineError> {
    let mut txor = ConnTransactor { conn };
    handle_push(
        &mut txor,
        &ctx.tables,
        ctx.retain_changes,
        &FixtureMutator,
        body,
        user_id,
    )
}

// epoch invalidation (harness invalidate hook): force every client's next pull
// to a full snapshot.
pub fn invalidate(conn: &Connection) -> Result<(), EngineError> {
    conn.execute_batch("BEGIN").expect("BEGIN failed");
    let result = {
        let mut db = RusqliteDb::new(conn);
        sync_core::pull::invalidate(&mut db)
    };
    finish(conn, result.is_ok());
    result
}

// harness reset-cursor fault hook: wipe the change log + floor + durable
// high-water mark to simulate a restored/behind server, so a persisted client
// whose cookie is ahead gets the 409 future-cookie reset path.
pub fn reset_cursor(conn: &Connection) -> Result<(), DbError> {
    conn.execute_batch(
        "BEGIN;
         DELETE FROM _zsync_changes;
         UPDATE _zsync_meta SET floor = 0;
         UPDATE _zsync_watermark SET high = 0;
         COMMIT;",
    )
    .map_err(|e| DbError(e.to_string()))
}

fn finish(conn: &Connection, ok: bool) {
    if ok {
        conn.execute_batch("COMMIT").expect("COMMIT failed");
    } else {
        let _ = conn.execute_batch("ROLLBACK");
    }
}

// host-owned transaction boundary for the synchronous native push path.
struct ConnTransactor<'c> {
    conn: &'c Connection,
}

impl<'c> Transactor for ConnTransactor<'c> {
    fn transaction<T, E>(
        &mut self,
        body: impl FnOnce(&mut dyn SyncDb) -> Result<T, E>,
    ) -> Result<T, E> {
        self.conn.execute_batch("BEGIN").expect("BEGIN failed");
        let outcome = {
            let mut db = RusqliteDb::new(self.conn);
            body(&mut db)
        };
        match outcome {
            Ok(value) => {
                self.conn.execute_batch("COMMIT").expect("COMMIT failed");
                Ok(value)
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }
}

// adapts the fixture mutators into sync-core's Mutator trait: an app-level
// rejection maps to MutateError::App (LMID still advances), anything else to
// Other (infra failure, the whole push fails and retries).
struct FixtureMutator;

impl Mutator for FixtureMutator {
    fn mutate(
        &self,
        db: &mut dyn SyncDb,
        name: &str,
        args: &Value,
        user_id: &str,
    ) -> Result<(), sync_core::error::MutateError> {
        use fixture::MutateError as F;
        match fixture::run_mutator(db, name, args, user_id) {
            Ok(()) => Ok(()),
            Err(F::App(details)) => Err(sync_core::error::MutateError::app(details)),
            Err(F::Db(e)) => Err(sync_core::error::MutateError::Other(e.0)),
            Err(F::Unknown(m)) => Err(sync_core::error::MutateError::Other(m)),
        }
    }
}
