// one sqlite file per namespace under --data-dir, each owned by a dedicated
// worker thread. every operation for a namespace (pull, push, admin sql,
// invalidate) is submitted to that one thread and runs serially on its
// Connection, so the plan's "one writer per namespace" invariant is
// structural — there is exactly one thread that can touch a namespace's db, so
// no lock and no SQLITE_BUSY. WAL + synchronous=FULL make committed cookies
// durable through SIGKILL, so reopening the same file resumes monotonically.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use tokio::sync::oneshot;

use sync_core::SyncDb;

use crate::db::RusqliteDb;

type Run = Box<dyn FnOnce(&Connection) + Send>;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TransactionStep {
    Begin,
    Query,
    End,
}

struct Job {
    transaction_id: Option<String>,
    step: TransactionStep,
    run: Run,
}

// runs once on a fresh worker connection to install the app tables + seed and
// the engine's _zsync_* schema/triggers. injected by main.rs so this module
// stays free of the fixture and engine.
pub type InitFn = Arc<dyn Fn(&mut dyn SyncDb) -> Result<(), String> + Send + Sync>;

pub struct Namespace {
    sender: std::sync::mpsc::Sender<Job>,
}

impl Namespace {
    // run a closure on the namespace's writer thread and await its result. the
    // closure gets the raw Connection; callers wrap it in RusqliteDb and own
    // transaction begin/commit/rollback themselves.
    pub async fn run<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&Connection) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let job = Job {
            transaction_id: None,
            step: TransactionStep::Query,
            run: Box::new(move |conn| {
                let _ = tx.send(f(conn));
            }),
        };
        self.sender
            .send(job)
            .unwrap_or_else(|_| panic!("namespace worker thread is gone"));
        rx.await.expect("namespace worker dropped the reply")
    }

    // run one request in an admin transaction. once Begin reaches the worker,
    // every other namespace operation waits until that transaction's End, so
    // pull/push traffic cannot be folded into the app server's transaction.
    pub async fn run_transaction<T, F>(
        &self,
        transaction_id: String,
        step: TransactionStep,
        f: F,
    ) -> T
    where
        F: FnOnce(&Connection) -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let job = Job {
            transaction_id: Some(transaction_id),
            step,
            run: Box::new(move |conn| {
                let _ = tx.send(f(conn));
            }),
        };
        self.sender
            .send(job)
            .unwrap_or_else(|_| panic!("namespace worker thread is gone"));
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

fn spawn(name: &str, path: PathBuf, init: InitFn) -> Result<Namespace, String> {
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
            let mut active_transaction: Option<String> = None;
            let mut deferred = VecDeque::<Job>::new();
            loop {
                let job = if let Some(active) = active_transaction.as_deref() {
                    if let Some(index) = deferred
                        .iter()
                        .position(|job| job.transaction_id.as_deref() == Some(active))
                    {
                        deferred.remove(index).expect("deferred job disappeared")
                    } else {
                        match receiver.recv() {
                            Ok(job) => job,
                            Err(_) => break,
                        }
                    }
                } else if let Some(job) = deferred.pop_front() {
                    job
                } else {
                    match receiver.recv() {
                        Ok(job) => job,
                        Err(_) => break,
                    }
                };

                if let Some(active) = active_transaction.as_deref()
                    && job.transaction_id.as_deref() != Some(active)
                {
                    deferred.push_back(job);
                    continue;
                }

                if job.step == TransactionStep::Begin {
                    active_transaction = job.transaction_id.clone();
                }
                let ends_transaction = job.step == TransactionStep::End;
                (job.run)(&conn);
                if ends_transaction {
                    active_transaction = None;
                }
            }
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
}

impl Manager {
    pub fn new(data_dir: PathBuf, init: InitFn) -> Self {
        Self {
            data_dir,
            namespaces: Mutex::new(HashMap::new()),
            init,
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
        let namespace = Arc::new(spawn(&key, path, self.init.clone())?);
        map.insert(key, namespace.clone());
        Ok(namespace)
    }
}
