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
use crate::fault::{FaultKind, FaultPoint, FaultRegistry};
use crate::fixture::{self, ColType};

// the EngineError an Error/Quota fault injects (Kill never reaches here — it exits).
fn injected_error(kind: FaultKind, point: FaultPoint) -> EngineError {
    match kind {
        FaultKind::Quota => EngineError::new(
            507,
            format!("storage quota exceeded (injected at {})", point.as_str()),
        ),
        _ => EngineError::new(500, format!("injected fault at {}", point.as_str())),
    }
}

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

// a completed pull/push plus the engine state the host needs for its structured
// telemetry: the response the client sees, and the retention floor (before + after)
// and durable watermark so the host can log floor/watermark and detect a prune
// (floor advanced) without the engine surfacing either.
pub struct Observed {
    pub result: Result<Value, EngineError>,
    pub floor_before: i64,
    pub floor: i64,
    pub watermark: i64,
}

// the retention floor: the oldest change watermark still retained. advances only
// when old changes are pruned, so a jump signals a retention run.
pub fn read_floor(conn: &Connection) -> i64 {
    conn.query_row("SELECT floor FROM _zsync_meta WHERE lock = 1", [], |row| {
        row.get(0)
    })
    .unwrap_or(0)
}

// the durable high-water mark: the max watermark ever assigned in this namespace.
pub fn read_watermark(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT high FROM _zsync_watermark WHERE lock = 1",
        [],
        |row| row.get(0),
    )
    .unwrap_or(0)
}

// pull runs inside one host-entered transaction, matching the CF host's
// transactionSync. commit on Ok, roll back on Err (a 409 undoes the claim).
pub fn pull(
    conn: &Connection,
    ctx: &EngineContext,
    faults: &FaultRegistry,
    ns: &str,
    body: &Value,
    user_id: &str,
) -> Observed {
    let floor_before = read_floor(conn);
    conn.execute_batch("BEGIN").expect("BEGIN failed");
    let mut result = {
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
    // fault: mid pull transaction, before COMMIT. Kill exits (SIGKILL-shaped, so
    // the tx never commits); Error/Quota roll back via finish() below.
    if result.is_ok()
        && let Some(kind) = faults.take(ns, FaultPoint::PullDuringTx)
    {
        match kind {
            FaultKind::Kill => std::process::exit(137),
            _ => result = Err(injected_error(kind, FaultPoint::PullDuringTx)),
        }
    }
    finish(conn, result.is_ok());
    // fault: after the pull committed, before the client sees the response.
    if result.is_ok()
        && let Some(kind) = faults.take(ns, FaultPoint::PullAfterCommit)
    {
        match kind {
            FaultKind::Kill => std::process::exit(137),
            _ => result = Err(injected_error(kind, FaultPoint::PullAfterCommit)),
        }
    }
    Observed {
        result,
        floor_before,
        floor: read_floor(conn),
        watermark: read_watermark(conn),
    }
}

// push drives the per-mutation tx1/tx2 steps through the Transactor; the CF
// host runs the same steps around its async JS mutator inside ctx.storage
// .transaction. row effects + LMID advance commit atomically per mutation.
pub fn push(
    conn: &Connection,
    ctx: &EngineContext,
    faults: &FaultRegistry,
    ns: &str,
    body: &Value,
    user_id: &str,
) -> Observed {
    let floor_before = read_floor(conn);
    // fault: before any mutation is applied
    if let Some(kind) = faults.take(ns, FaultPoint::PushBeforeMutation) {
        match kind {
            FaultKind::Kill => std::process::exit(137),
            _ => {
                return Observed {
                    result: Err(injected_error(kind, FaultPoint::PushBeforeMutation)),
                    floor_before,
                    floor: floor_before,
                    watermark: read_watermark(conn),
                };
            }
        }
    }
    let mut txor = ConnTransactor { conn, faults, ns };
    let mut result = handle_push(
        &mut txor,
        &ctx.tables,
        ctx.retain_changes,
        &FixtureMutator,
        body,
        user_id,
    );
    // fault: the push committed durably, before the client sees the ack. Kill
    // exits after the commit (durability probe); Error/Quota return a failure the
    // client must reconcile against already-committed state.
    if result.is_ok()
        && let Some(kind) = faults.take(ns, FaultPoint::PushAfterCommitBeforeResponse)
    {
        match kind {
            FaultKind::Kill => std::process::exit(137),
            _ => {
                result = Err(injected_error(
                    kind,
                    FaultPoint::PushAfterCommitBeforeResponse,
                ))
            }
        }
    }
    Observed {
        result,
        floor_before,
        floor: read_floor(conn),
        watermark: read_watermark(conn),
    }
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
    faults: &'c FaultRegistry,
    ns: &'c str,
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
        // fault: the mutation's app rows are written, before this per-mutation
        // COMMIT. Kill-only: exiting here leaves the writes uncommitted in the WAL,
        // so they are gone on restart (durability probe). the generic tx error type
        // cannot be forged, so Error/Quota are rejected for this point at arm time.
        if outcome.is_ok()
            && self
                .faults
                .take(self.ns, FaultPoint::PushAfterWriteBeforeCommit)
                == Some(FaultKind::Kill)
        {
            std::process::exit(137);
        }
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
