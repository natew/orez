// the single integration seam between the native host and the sync-core
// engine. the host owns transaction begin/commit/rollback here (sync-core
// never emits BEGIN/COMMIT) and adapts the consumer's init/mutator/visibility
// closures into sync-core's Mutator / Visibility / Transactor traits. every
// engine call runs on a namespace's worker thread, so the Connection is
// single-threaded and a plain BEGIN/COMMIT is the whole transaction story.

use std::sync::Arc;

use rusqlite::Connection;
use serde_json::Value;

use sync_core::error::{EngineError, MutateError};
use sync_core::pull::{Caps, Visibility, VisibleFilter, handle_pull};
use sync_core::push::{Mutator, Transactor, handle_push};
use sync_core::schema::Tables;
use sync_core::{DbError, SqlValue, SyncDb};

use crate::db::RusqliteDb;
use crate::fault::{FaultKind, FaultPoint, FaultRegistry};

// ---- config types -------------------------------------------------------

/// Called whenever a namespace database is opened to install or migrate app
/// DDL and optional seed data. The callback must be idempotent. It runs inside
/// a transaction before the engine installs its _zsync_* schema. Return
/// Err(String) to fail opening the namespace.
pub type InitFn = Arc<dyn Fn(&mut dyn SyncDb) -> Result<(), String> + Send + Sync>;

/// Runs a named mutator inside the push transaction. Return Ok(()) for
/// success, Err(MutateError::app(msg)) for an app-level rejection (LMID
/// still advances), or Err(MutateError::Other(msg)) for an infra failure
/// that rolls back the entire push.
pub type MutateFn =
    Arc<dyn Fn(&mut dyn SyncDb, &str, &Value, &str) -> Result<(), MutateError> + Send + Sync>;

/// Optional per-user row visibility. Returns a WHERE fragment (without the
/// WHERE keyword) and positional parameters, or None for a table with no
/// visibility filter.
pub type VisibleFn = Arc<dyn Fn(&str, &str) -> Option<(String, Vec<SqlValue>)> + Send + Sync>;

// ---- EngineContext -------------------------------------------------------

// process-wide engine configuration shared by every namespace worker.
pub struct EngineContext {
    pub tables: Tables,
    pub retain_changes: i64,
    // baseline-pull change-row cap. one diff ships at most this many change
    // rows, cutting at a row boundary before pk dedup, so effects and their
    // lmid ack ride separate pulls when the cap is small (see Caps).
    pub max_change_rows: usize,
    pub visibility_enabled: bool,
    // query-aware mode: pulls carry desired queries and go through the
    // query-aware engine (membership/refcount) instead of the baseline
    // full-namespace pull. a namespace serves one consumer kind, not a mix.
    pub query_aware: bool,
    // consumer-provided callbacks
    pub(crate) init_fn: InitFn,
    pub(crate) mutate_fn: MutateFn,
    pub(crate) visible_fn: Option<VisibleFn>,
}

impl EngineContext {
    fn visibility(&self) -> Option<Visibility<'_>> {
        let visible_fn = self.visible_fn.as_ref()?;
        if !self.visibility_enabled {
            return None;
        }
        Some(Visibility {
            row_local: false,
            filter: Box::new(|table, user| {
                visible_fn(table, user).map(|(sql, params)| VisibleFilter { sql, params })
            }),
        })
    }
}

// ---- helpers -------------------------------------------------------------

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

// ---- worker init ---------------------------------------------------------

// worker init: install or migrate the app tables (consumer), then the engine's
// _zsync_* schema + triggers. on a fresh database the triggers install after
// seed data, so those initial rows stay out of the change log.
pub fn init_namespace(db: &mut dyn SyncDb, ctx: &EngineContext) -> Result<(), String> {
    (ctx.init_fn)(db)?;
    sync_core::schema::init_schema(db, &ctx.tables).map_err(|e| e.0)?;
    // the query-aware tables are idempotent + unused in baseline mode, so
    // install them always so a namespace can serve query-aware pulls.
    sync_core::query::init_query_schema(db).map_err(|e| e.message)?;
    Ok(())
}

// ---- pull / push / invalidate / reset-cursor ----------------------------

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
                Caps {
                    max_change_rows: ctx.max_change_rows,
                    ..Caps::default()
                },
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
    let mutator = CallbackMutator {
        f: ctx.mutate_fn.clone(),
    };
    let mut result = handle_push(
        &mut txor,
        &ctx.tables,
        ctx.retain_changes,
        &mutator,
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

// settle an application-owned push in a new transaction after its app effects
// committed to this shared sqlite file. this ordering makes the lmid served by
// the next pull a truthful acknowledgement of effects already in the log.
pub fn settle_delegated_push(
    conn: &Connection,
    ctx: &EngineContext,
    push: &Value,
    response: &Value,
    user_id: &str,
) -> Result<usize, EngineError> {
    conn.execute_batch("BEGIN").expect("BEGIN failed");
    let result = (|| {
        let mut db = RusqliteDb::new(conn);
        let settled = sync_core::settle_delegated_push(&mut db, push, response, user_id)?;
        if settled > 0 {
            sync_core::prune(&mut db, ctx.retain_changes)?;
        }
        Ok(settled)
    })();
    finish(conn, result.is_ok());
    result
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

// harness full-prune hook: bump the durable high-water, then prune the entire
// change log (retain 0), raising the floor to the head so _zsync_changes is
// emptied. the durable high-water must keep the served cookie monotonic across
// a reopen of the same sqlite file (invariant 7 / mutant O1). the state machine
// arms this before a server restart to exercise O1 end to end at the system
// level, which no other lane does (it empties the log AND reopens the store).
pub fn prune_to_head(conn: &Connection) -> Result<(), EngineError> {
    conn.execute_batch("BEGIN").expect("BEGIN failed");
    let result = (|| {
        let mut db = RusqliteDb::new(conn);
        sync_core::watermark(&mut db)?;
        sync_core::prune(&mut db, 0)
    })();
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

// ---- Transactor (host-owned tx boundary) ---------------------------------

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

// ---- Mutator adapter -----------------------------------------------------

// adapts a consumer's MutateFn closure into sync-core's Mutator trait.
struct CallbackMutator {
    f: MutateFn,
}

impl Mutator for CallbackMutator {
    fn mutate(
        &self,
        db: &mut dyn SyncDb,
        name: &str,
        args: &Value,
        user_id: &str,
    ) -> Result<(), MutateError> {
        (self.f)(db, name, args, user_id)
    }
}
