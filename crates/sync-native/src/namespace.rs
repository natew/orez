// one sqlite file per namespace under --data-dir, each owned by a dedicated
// worker thread. every operation for a namespace (pull, push, admin sql,
// invalidate) is submitted to that one thread and runs serially on its
// Connection, so the plan's "one writer per namespace" invariant is
// structural. there is exactly one thread that can touch a namespace's db, so
// no lock and no SQLITE_BUSY. WAL + synchronous=FULL make committed cookies
// durable through SIGKILL, so reopening the same file resumes monotonically.
//
// admin transactions (multi-request BEGIN/.../COMMIT over /admin/sql) are owned
// by the worker, not the client. the worker runs the actual BEGIN, COMMIT, and
// ROLLBACK; the scheduler's active_transaction flips only after that SQL
// succeeds, and it releases the namespace only once the connection is back in
// autocommit, so scheduler state and the connection's real transaction state
// cannot drift apart. steps that name the wrong transaction, a duplicate begin,
// a begin while another transaction owns the namespace, or a step with no
// transaction open are rejected instead of run. a lost admin client (its reply
// channel closes, or it stalls past the lease) is rolled back and unblocks the
// namespace.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusqlite::Connection;
use tokio::sync::oneshot;

use sync_core::{DbError, Row, SyncDb};

use crate::db::RusqliteDb;

// how an admin transaction ends. the worker runs the real COMMIT or ROLLBACK;
// the client only names the outcome (never the raw SQL), so scheduler state and
// the connection's transaction state cannot drift apart.
#[derive(Clone, Copy)]
pub enum TxEnd {
    Commit,
    Rollback,
}

// an admin-transaction step the worker refused to run or that failed: a
// duplicate/foreign begin, a step naming a transaction that does not own the
// namespace, or the step's SQL failing. status is the HTTP status the
// /admin/sql handler returns (409 for a protocol conflict, 500 for SQL failure).
#[derive(Debug)]
pub struct AdminTxError {
    pub status: u16,
    pub message: String,
}

impl AdminTxError {
    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: 409,
            message: message.into(),
        }
    }

    fn sql(err: DbError) -> Self {
        Self {
            status: 500,
            message: err.0,
        }
    }
}

// a unit of work for a namespace's worker thread.
enum Job {
    // a non-transactional operation (pull, push, one-shot admin sql, invalidate,
    // reset). runs only when no admin transaction owns the namespace. the closure
    // captures its own reply channel, so it stays generic over the return type.
    Plain(Box<dyn FnOnce(&Connection) + Send>),
    // open a server-owned admin transaction keyed by id.
    Begin {
        id: String,
        reply: oneshot::Sender<Result<(), AdminTxError>>,
    },
    // run one statement inside the admin transaction id.
    Query {
        id: String,
        run: TxQueryFn,
        reply: oneshot::Sender<Result<Vec<Row>, AdminTxError>>,
    },
    // end the admin transaction id with a commit or rollback.
    End {
        id: String,
        end: TxEnd,
        reply: oneshot::Sender<Result<(), AdminTxError>>,
    },
}

// the boxed statement a Query step runs inside its admin transaction.
type TxQueryFn = Box<dyn FnOnce(&Connection) -> Result<Vec<Row>, DbError> + Send>;

// runs once on a fresh worker connection to install the app tables + seed and
// the engine's _zsync_* schema/triggers. injected by main.rs so this module
// stays free of the fixture and engine.
pub type InitFn = Arc<dyn Fn(&mut dyn SyncDb) -> Result<(), String> + Send + Sync>;

pub struct Namespace {
    sender: std::sync::mpsc::Sender<Job>,
}

impl Namespace {
    fn send(&self, job: Job) {
        self.sender
            .send(job)
            .unwrap_or_else(|_| panic!("namespace worker thread is gone"));
    }

    // run a closure on the namespace's writer thread and await its result. the
    // closure gets the raw Connection; callers wrap it in RusqliteDb and own
    // transaction begin/commit/rollback themselves. waits its turn behind any
    // active admin transaction.
    pub async fn run<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&Connection) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.send(Job::Plain(Box::new(move |conn| {
            let _ = tx.send(f(conn));
        })));
        rx.await.expect("namespace worker dropped the reply")
    }

    // open a server-owned admin transaction. the worker runs BEGIN and only marks
    // the namespace owned by id once it succeeds, so a failed begin never wedges
    // pull/push traffic. a begin while any transaction is active is rejected
    // (duplicate id, or another transaction already owns the namespace). once this
    // returns Ok, every other namespace operation waits until the matching End, so
    // pull/push cannot be folded into the transaction.
    pub async fn tx_begin(&self, id: String) -> Result<(), AdminTxError> {
        let (reply, rx) = oneshot::channel();
        self.send(Job::Begin { id, reply });
        rx.await.expect("namespace worker dropped the reply")
    }

    // run one statement inside the admin transaction id. rejected (not run) if id
    // is not the transaction that currently owns the namespace.
    pub async fn tx_query<F>(&self, id: String, f: F) -> Result<Vec<Row>, AdminTxError>
    where
        F: FnOnce(&Connection) -> Result<Vec<Row>, DbError> + Send + 'static,
    {
        let (reply, rx) = oneshot::channel();
        self.send(Job::Query {
            id,
            run: Box::new(f),
            reply,
        });
        rx.await.expect("namespace worker dropped the reply")
    }

    // end the admin transaction id. the worker runs the real COMMIT/ROLLBACK and
    // releases the namespace only once the connection is back in autocommit, so a
    // failed commit cannot leave a transaction open. rejected if id does not own
    // the namespace.
    pub async fn tx_end(&self, id: String, end: TxEnd) -> Result<(), AdminTxError> {
        let (reply, rx) = oneshot::channel();
        self.send(Job::End { id, end, reply });
        rx.await.expect("namespace worker dropped the reply")
    }
}

fn open_connection(path: &Path) -> Result<Connection, String> {
    let conn = Connection::open(path).map_err(|e| e.to_string())?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = FULL;
         PRAGMA busy_timeout = 5000;
         PRAGMA foreign_keys = OFF;",
    )
    .map_err(|e| e.to_string())?;
    Ok(conn)
}

// return the connection to autocommit, rolling back if a transaction is still
// open. returns true once no transaction is open. used to guarantee the worker
// never releases the namespace while the connection is still mid-transaction.
fn force_autocommit(conn: &Connection) -> bool {
    if conn.is_autocommit() {
        return true;
    }
    let _ = conn.execute_batch("ROLLBACK");
    conn.is_autocommit()
}

// the worker loop: pull one job at a time off the channel and run it on the one
// connection. active_transaction is the id of the admin transaction that owns
// the namespace, or None. it is set only after BEGIN succeeds and cleared only
// after the connection is back in autocommit, so it always mirrors the
// connection's real transaction state.
fn worker_loop(conn: Connection, receiver: std::sync::mpsc::Receiver<Job>, lease: Duration) {
    let mut active_transaction: Option<String> = None;
    // plain jobs that arrived while a transaction owned the namespace. drained in
    // arrival order once it frees. begins are never deferred (they are rejected
    // while a transaction is active), so this only ever holds plain work.
    let mut deferred = VecDeque::<Job>::new();
    // while an admin transaction is active, the wall-clock instant its lease
    // expires. only meaningful while active_transaction is Some.
    let mut lease_deadline = Instant::now();

    loop {
        match active_transaction.clone() {
            // an admin transaction owns the connection: run only its own steps,
            // defer unrelated plain work, reject begins and steps that name a
            // different transaction, and reclaim the namespace if the lease
            // expires with no next step.
            Some(active_id) => {
                // enforce the lease before receiving. recv_timeout(0) returns an
                // already-queued job rather than Timeout, so once the deadline has
                // passed a sustained backlog of plain jobs (deferred one per loop)
                // could otherwise starve the timeout forever after the owner is
                // lost. checking the deadline first guarantees reclaim regardless of
                // backlog.
                let now = Instant::now();
                let job = if now >= lease_deadline {
                    None
                } else {
                    match receiver.recv_timeout(lease_deadline.saturating_duration_since(now)) {
                        Ok(job) => Some(job),
                        Err(RecvTimeoutError::Timeout) => None,
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                };
                let Some(job) = job else {
                    // the lease expired (deadline passed, or the wait timed out) with
                    // no next step: the admin client stalled or disconnected. roll
                    // back and unblock; if the rollback itself cannot clear the
                    // transaction, keep ownership and retry next lease rather than
                    // resume pull/push on a dirty connection.
                    if force_autocommit(&conn) {
                        active_transaction = None;
                    } else {
                        lease_deadline = Instant::now() + lease;
                    }
                    continue;
                };
                match job {
                    Job::Query { id, run, reply } if id == active_id => {
                        let outcome = run(&conn);
                        if reply.send(outcome.map_err(AdminTxError::sql)).is_ok() {
                            lease_deadline = Instant::now() + lease;
                        } else if force_autocommit(&conn) {
                            // the admin client disconnected mid-transaction: roll
                            // back now instead of holding the namespace to the lease.
                            active_transaction = None;
                        } else {
                            lease_deadline = Instant::now() + lease;
                        }
                    }
                    Job::End { id, end, reply } if id == active_id => {
                        let sql = match end {
                            TxEnd::Commit => "COMMIT",
                            TxEnd::Rollback => "ROLLBACK",
                        };
                        let outcome = conn.execute_batch(sql);
                        // a failed commit (e.g. a deferred FK violation) leaves the
                        // transaction open; force it closed before releasing, and
                        // if even that fails keep ownership so pull/push never runs
                        // on a dirty connection.
                        if force_autocommit(&conn) {
                            active_transaction = None;
                        } else {
                            lease_deadline = Instant::now() + lease;
                        }
                        let result = outcome
                            .map(|_| ())
                            .map_err(|e| AdminTxError::sql(DbError(e.to_string())));
                        let _ = reply.send(result);
                    }
                    // a step naming a transaction that does not own the namespace.
                    // reject rather than defer, or the client would hang forever.
                    Job::Query { id, reply, .. } => {
                        let _ = reply.send(Err(AdminTxError::conflict(format!(
                            "no active transaction {id}"
                        ))));
                    }
                    Job::End { id, reply, .. } => {
                        let _ = reply.send(Err(AdminTxError::conflict(format!(
                            "no active transaction {id}"
                        ))));
                    }
                    // a second begin while a transaction owns the namespace: a
                    // duplicate of the owner, or a foreign transaction. both are
                    // ownership conflicts, not queued work.
                    Job::Begin { id, reply } => {
                        let message = if id == active_id {
                            format!("transaction {id} is already active")
                        } else {
                            format!("transaction {active_id} already owns this namespace")
                        };
                        let _ = reply.send(Err(AdminTxError::conflict(message)));
                    }
                    // plain work waits its turn.
                    plain => deferred.push_back(plain),
                }
            }
            // no admin transaction: serve deferred plain work first (preserving
            // arrival order), then block on the channel.
            None => {
                let job = match deferred.pop_front() {
                    Some(job) => job,
                    None => match receiver.recv() {
                        Ok(job) => job,
                        Err(_) => break,
                    },
                };
                match job {
                    Job::Plain(run) => run(&conn),
                    Job::Begin { id, reply } => match conn.execute_batch("BEGIN") {
                        Ok(()) => {
                            if reply.send(Ok(())).is_ok() {
                                // state changes only after BEGIN succeeded and the
                                // client is still there to own the transaction.
                                active_transaction = Some(id);
                                lease_deadline = Instant::now() + lease;
                            } else {
                                // client vanished during begin: undo it so the
                                // namespace is never blocked on a dead session.
                                if !force_autocommit(&conn) {
                                    active_transaction = Some(id);
                                    lease_deadline = Instant::now() + lease;
                                }
                            }
                        }
                        Err(e) => {
                            // a failed BEGIN should normally leave autocommit on. if
                            // the connection was already dirty, force it clean before
                            // releasing plain work; keep ownership and retry on the
                            // lease if SQLite cannot roll it back yet.
                            if !force_autocommit(&conn) {
                                active_transaction = Some(id);
                                lease_deadline = Instant::now() + lease;
                            }
                            let _ = reply.send(Err(AdminTxError::sql(DbError(e.to_string()))));
                        }
                    },
                    // a query/end step with no transaction open to belong to.
                    Job::Query { id, reply, .. } => {
                        let _ = reply.send(Err(AdminTxError::conflict(format!(
                            "no active transaction {id}"
                        ))));
                    }
                    Job::End { id, reply, .. } => {
                        let _ = reply.send(Err(AdminTxError::conflict(format!(
                            "no active transaction {id}"
                        ))));
                    }
                }
            }
        }
    }
}

fn spawn(name: &str, path: PathBuf, init: InitFn, lease: Duration) -> Result<Namespace, String> {
    let (sender, receiver) = std::sync::mpsc::channel::<Job>();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();
    std::thread::Builder::new()
        .name(format!("ns-{name}"))
        .spawn(move || {
            let conn = match open_connection(&path) {
                Ok(c) => c,
                Err(e) => {
                    let _ = ready_tx.send(Err(e));
                    return;
                }
            };
            {
                let mut db = RusqliteDb::new(&conn);
                if let Err(e) = init(&mut db) {
                    let _ = ready_tx.send(Err(e));
                    return;
                }
            }
            if ready_tx.send(Ok(())).is_err() {
                return;
            }
            worker_loop(conn, receiver, lease);
        })
        .map_err(|e| e.to_string())?;
    ready_rx
        .recv()
        .map_err(|e| format!("namespace worker failed to start: {e}"))??;
    Ok(Namespace { sender })
}

// a namespace name must be a safe filename component (no path traversal, no
// separators). harness namespaces are `rust-<ts>-<rand>` so this only rejects
// hostile input.
fn sanitize(ns: &str) -> Result<String, String> {
    if ns.is_empty() || ns.len() > 128 {
        return Err(format!("invalid namespace {ns:?}"));
    }
    if ns
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        Ok(ns.to_string())
    } else {
        Err(format!("invalid namespace {ns:?}"))
    }
}

pub struct Manager {
    data_dir: PathBuf,
    namespaces: Mutex<HashMap<String, Arc<Namespace>>>,
    init: InitFn,
    // idle-between-steps budget for an admin transaction before the worker
    // reclaims the namespace (see worker_loop).
    admin_tx_lease: Duration,
}

impl Manager {
    pub fn new(data_dir: PathBuf, init: InitFn, admin_tx_lease: Duration) -> Self {
        Self {
            data_dir,
            namespaces: Mutex::new(HashMap::new()),
            init,
            admin_tx_lease,
        }
    }

    // get-or-create the namespace's worker. creation opens the file and seeds
    // it, which is quick and only happens once per namespace per process.
    pub fn get(&self, ns: &str) -> Result<Arc<Namespace>, String> {
        let key = sanitize(ns)?;
        let mut map = self.namespaces.lock().unwrap();
        if let Some(existing) = map.get(&key) {
            return Ok(existing.clone());
        }
        let path = self.data_dir.join(format!("{key}.sqlite"));
        let namespace = Arc::new(spawn(&key, path, self.init.clone(), self.admin_tx_lease)?);
        map.insert(key, namespace.clone());
        Ok(namespace)
    }
}

#[cfg(test)]
mod tests {
    // direct coverage of the admin-transaction scheduler on a bare namespace
    // worker, where the lease and the connection are controllable in ways the
    // HTTP surface cannot reach. the library_api.rs integration suite covers the
    // same protocol end to end through /admin/sql.
    use super::*;

    use sync_core::SqlValue;

    fn test_namespace(lease: Duration) -> (tempfile::TempDir, Namespace) {
        namespace_with_init(
            lease,
            Arc::new(|db: &mut dyn SyncDb| {
                db.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &[])
                    .map_err(|e| e.0)
            }),
        )
    }

    fn namespace_with_init(lease: Duration, init: InitFn) -> (tempfile::TempDir, Namespace) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        let ns = spawn("test", path, init, lease).unwrap();
        (dir, ns)
    }

    fn count(rows: &[Row]) -> i64 {
        match rows[0].get("n") {
            Some(SqlValue::Integer(n)) => *n,
            other => panic!("expected integer count, got {other:?}"),
        }
    }

    async fn row_count(ns: &Namespace, table: &str) -> i64 {
        let sql = format!("SELECT count(*) AS n FROM {table}");
        let rows = ns
            .run(move |c| {
                let mut db = RusqliteDb::new(c);
                db.query(&sql, &[]).unwrap()
            })
            .await;
        count(&rows)
    }

    #[tokio::test]
    async fn admin_tx_commit_persists_and_rollback_discards() {
        let (_dir, ns) = test_namespace(Duration::from_secs(5));
        // commit path
        ns.tx_begin("t1".into()).await.unwrap();
        ns.tx_query("t1".into(), |c| {
            let mut db = RusqliteDb::new(c);
            db.query("INSERT INTO t (id, v) VALUES (1, 'a')", &[])
        })
        .await
        .unwrap();
        ns.tx_end("t1".into(), TxEnd::Commit).await.unwrap();
        assert_eq!(row_count(&ns, "t").await, 1);

        // rollback path leaves the committed row alone and drops its own write
        ns.tx_begin("t2".into()).await.unwrap();
        ns.tx_query("t2".into(), |c| {
            let mut db = RusqliteDb::new(c);
            db.query("INSERT INTO t (id, v) VALUES (2, 'b')", &[])
        })
        .await
        .unwrap();
        ns.tx_end("t2".into(), TxEnd::Rollback).await.unwrap();
        assert_eq!(row_count(&ns, "t").await, 1);
    }

    #[tokio::test]
    async fn admin_tx_wrong_id_is_rejected() {
        let (_dir, ns) = test_namespace(Duration::from_secs(5));
        ns.tx_begin("t1".into()).await.unwrap();
        let err = ns
            .tx_query("t2".into(), |c| {
                let mut db = RusqliteDb::new(c);
                db.query("INSERT INTO t (id, v) VALUES (9, 'leak')", &[])
            })
            .await
            .unwrap_err();
        assert_eq!(err.status, 409);
        let err = ns.tx_end("t2".into(), TxEnd::Commit).await.unwrap_err();
        assert_eq!(err.status, 409);
        // the real transaction is untouched and still commits its own write.
        ns.tx_query("t1".into(), |c| {
            let mut db = RusqliteDb::new(c);
            db.query("INSERT INTO t (id, v) VALUES (1, 'a')", &[])
        })
        .await
        .unwrap();
        ns.tx_end("t1".into(), TxEnd::Commit).await.unwrap();
        assert_eq!(row_count(&ns, "t").await, 1);
    }

    #[tokio::test]
    async fn admin_tx_begin_while_active_is_rejected() {
        let (_dir, ns) = test_namespace(Duration::from_secs(5));
        ns.tx_begin("t1".into()).await.unwrap();
        // duplicate id
        let err = ns.tx_begin("t1".into()).await.unwrap_err();
        assert_eq!(err.status, 409);
        // foreign id while owned
        let err = ns.tx_begin("t2".into()).await.unwrap_err();
        assert_eq!(err.status, 409);
        // the first transaction is still the owner: its work commits normally.
        ns.tx_query("t1".into(), |c| {
            let mut db = RusqliteDb::new(c);
            db.query("INSERT INTO t (id, v) VALUES (1, 'a')", &[])
        })
        .await
        .unwrap();
        ns.tx_end("t1".into(), TxEnd::Commit).await.unwrap();
        assert_eq!(row_count(&ns, "t").await, 1);
    }

    #[tokio::test]
    async fn admin_tx_step_without_begin_is_rejected() {
        let (_dir, ns) = test_namespace(Duration::from_secs(5));
        let err = ns
            .tx_query("t1".into(), |c| {
                let mut db = RusqliteDb::new(c);
                db.query("SELECT 1", &[])
            })
            .await
            .unwrap_err();
        assert_eq!(err.status, 409);
        let err = ns.tx_end("t1".into(), TxEnd::Commit).await.unwrap_err();
        assert_eq!(err.status, 409);
    }

    #[tokio::test]
    async fn admin_tx_failed_begin_does_not_wedge() {
        let (_dir, ns) = test_namespace(Duration::from_secs(10));
        // open a raw transaction the scheduler does not track, so the next
        // server-owned BEGIN fails with "cannot start a transaction within a
        // transaction".
        ns.run(|c| c.execute_batch("BEGIN").unwrap()).await;
        let err = ns.tx_begin("t1".into()).await.unwrap_err();
        assert_eq!(err.status, 500, "failed begin should surface a 500");
        // the worker must clean the unexpected transaction before releasing
        // plain work, rather than merely clearing its scheduler state.
        let probe = tokio::time::timeout(Duration::from_millis(500), ns.run(|c| c.is_autocommit()));
        assert!(probe.await.expect("failed begin wedged the namespace"));
    }

    #[tokio::test]
    async fn admin_tx_failed_commit_unblocks() {
        // a deferred foreign-key violation makes COMMIT (not the insert) fail,
        // leaving the transaction open. the worker must force it closed and
        // release the namespace.
        let (_dir, ns) = namespace_with_init(
            Duration::from_secs(10),
            Arc::new(|db: &mut dyn SyncDb| {
                db.exec("CREATE TABLE parent (id INTEGER PRIMARY KEY)", &[])
                    .map_err(|e| e.0)?;
                db.exec(
                    "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER \
                     REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)",
                    &[],
                )
                .map_err(|e| e.0)
            }),
        );
        // open_connection defaults foreign_keys off; enable enforcement outside any
        // transaction so the deferred constraint fires at commit.
        ns.run(|c| c.execute_batch("PRAGMA foreign_keys = ON").unwrap())
            .await;
        ns.tx_begin("t1".into()).await.unwrap();
        ns.tx_query("t1".into(), |c| {
            let mut db = RusqliteDb::new(c);
            db.query("INSERT INTO child (id, parent_id) VALUES (1, 999)", &[])
        })
        .await
        .unwrap();
        let err = ns.tx_end("t1".into(), TxEnd::Commit).await.unwrap_err();
        assert_eq!(
            err.status, 500,
            "deferred FK violation should fail the commit"
        );
        // forced rollback ran, so the namespace is usable at once and nothing
        // persisted.
        let probe = tokio::time::timeout(Duration::from_millis(500), row_count(&ns, "child"));
        assert_eq!(probe.await.expect("failed commit wedged the namespace"), 0);
    }

    #[tokio::test]
    async fn admin_tx_lease_reclaims_lost_client() {
        // a lost client (disconnect / timeout / session loss all reduce to "no end
        // step arrives") must not wedge the namespace: the lease rolls the
        // transaction back and resumes pull/push.
        let (_dir, ns) = test_namespace(Duration::from_millis(150));
        ns.tx_begin("t1".into()).await.unwrap();
        ns.tx_query("t1".into(), |c| {
            let mut db = RusqliteDb::new(c);
            db.query("INSERT INTO t (id, v) VALUES (1, 'pending')", &[])
        })
        .await
        .unwrap();
        // never send end. a plain job queued now waits for the transaction, then
        // runs once the lease reclaims it.
        let probe = tokio::time::timeout(Duration::from_secs(5), row_count(&ns, "t"));
        assert_eq!(
            probe.await.expect("lease did not unblock the namespace"),
            0,
            "the pending insert must be rolled back on reclaim"
        );
        // a late step for the reclaimed transaction is rejected, not accepted into
        // whatever transaction runs next.
        let err = ns
            .tx_query("t1".into(), |c| {
                let mut db = RusqliteDb::new(c);
                db.query("SELECT 1", &[])
            })
            .await
            .unwrap_err();
        assert_eq!(err.status, 409);
    }

    #[tokio::test]
    async fn admin_tx_disconnect_mid_step_recovers() {
        // the admin client drops its request future mid-transaction. whether the
        // worker notices at reply time or at the lease, the namespace must recover
        // and the pending write must be rolled back.
        let (_dir, ns) = test_namespace(Duration::from_millis(150));
        let ns = Arc::new(ns);
        ns.tx_begin("t1".into()).await.unwrap();
        {
            // this future sends its Query job, then is dropped before awaiting the
            // reply, closing the reply channel.
            let ns = ns.clone();
            let dropped = ns.tx_query("t1".into(), |c| {
                let mut db = RusqliteDb::new(c);
                db.query("INSERT INTO t (id, v) VALUES (1, 'pending')", &[])
            });
            drop(tokio::time::timeout(Duration::from_millis(1), dropped).await);
        }
        // the namespace recovers and the insert did not survive.
        let probe = tokio::time::timeout(Duration::from_secs(5), row_count(&ns, "t"));
        assert_eq!(probe.await.expect("disconnect wedged the namespace"), 0);
    }

    #[tokio::test]
    async fn admin_tx_disconnect_during_begin_recovers() {
        let (_dir, ns) = test_namespace(Duration::from_millis(150));
        let ns = Arc::new(ns);
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        // hold the worker so the begin request is definitely queued before its
        // caller disappears.
        let blocker = {
            let ns = ns.clone();
            tokio::spawn(async move {
                ns.run(move |_| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .await
            })
        };
        entered_rx.await.unwrap();

        let begin = {
            let ns = ns.clone();
            tokio::spawn(async move { ns.tx_begin("gone".into()).await })
        };
        tokio::task::yield_now().await;
        begin.abort();
        release_tx.send(()).unwrap();
        blocker.await.unwrap();

        let probe = tokio::time::timeout(Duration::from_secs(5), ns.run(|c| c.is_autocommit()));
        assert!(probe.await.expect("begin disconnect wedged the namespace"));
    }

    #[tokio::test]
    async fn admin_tx_lease_not_starved_by_plain_flood() {
        // a sustained backlog of plain jobs must not starve the lease. with a
        // continuous feed, recv_timeout never sees an empty channel, so the reclaim
        // has to come from the pre-receive deadline check, not a recv timeout.
        let (_dir, ns) = test_namespace(Duration::from_millis(100));
        let ns = Arc::new(ns);
        ns.tx_begin("t1".into()).await.unwrap();
        ns.tx_query("t1".into(), |c| {
            let mut db = RusqliteDb::new(c);
            db.query("INSERT INTO t (id, v) VALUES (1, 'pending')", &[])
        })
        .await
        .unwrap();

        // feed plain reads continuously for longer than the lease. each is deferred
        // while the now-ownerless transaction holds the namespace.
        let mut floods = Vec::new();
        for _ in 0..150 {
            let ns = ns.clone();
            floods.push(tokio::spawn(async move { row_count(&ns, "t").await }));
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        // every flood read completes (the lease reclaimed the namespace despite the
        // backlog) and sees the rolled-back state.
        for flood in floods {
            let seen = tokio::time::timeout(Duration::from_secs(5), flood)
                .await
                .expect("plain flood starved the lease")
                .unwrap();
            assert_eq!(seen, 0, "the pending insert must be rolled back on reclaim");
        }
    }
}
