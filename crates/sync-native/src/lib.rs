// sync-native: a native axum + rusqlite host for the sync-core engine.
//
// this crate is both a library (embed the sync host in your Rust process)
// and a binary (run a standalone sync server with fixture data for harness
// testing). downstream Rust consumers add `sync-native` as a dependency,
// populate a `SyncNativeConfig`, and call `SyncNativeHost::run()`.
//
// the engine lives in sync-core; this crate is the native host shell:
// per-namespace sqlite files (WAL, one serialized writer each), the
// rusqlite SyncDb adapter, and the axum HTTP surface. it owns transaction
// begin/commit/rollback and drives the engine steps (see engine.rs).

pub mod db;
pub mod engine;
pub mod fault;
pub mod fixture;
pub mod namespace;
pub mod obs;
pub mod seed;
pub mod server;
pub mod wake;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::http::HeaderMap;
use engine::{EngineContext, InitFn, MutateFn, VisibleFn};
use namespace::Manager;
use sync_core::schema::Tables;

/// Authentication callback. Receives HTTP request headers, returns
/// `Some(user_id)` for an authenticated user, or `None` to reject.
pub type AuthFn = Arc<dyn Fn(&HeaderMap) -> Option<String> + Send + Sync>;

/// Default idle-between-steps budget for a server-owned admin transaction. If
/// an admin client sends no next step within this window the namespace worker
/// rolls the transaction back and unblocks pull/push, so a lost admin client
/// cannot wedge a namespace.
pub const DEFAULT_ADMIN_TX_LEASE: Duration = Duration::from_secs(30);

// ---- public config -------------------------------------------------------

/// Complete configuration for a sync-native host. Populate this with your
/// schema, DDL/seed, mutators, visibility rules, and auth, then pass it to
/// `SyncNativeHost::new`.
pub struct SyncNativeConfig {
    /// Application tables with Zero column types and primary keys.
    /// Usually derived from your Zero `createSchema()` result.
    pub tables: Tables,

    /// Called once per namespace at creation to install app DDL and
    /// optional seed data. Runs inside a transaction before the engine
    /// installs its `_zsync_*` schema. Return `Err(String)` to fail
    /// namespace creation.
    ///
    /// The engine's internal schema and triggers are installed
    /// automatically after this runs; you do not need to manage them.
    pub initialize: InitFn,

    /// Runs a named mutator inside the push transaction.
    ///
    /// Return `Ok(())` for success. Return
    /// `Err(MutateError::app("reason"))` for an app-level rejection (the
    /// engine still advances the last-mutation-id for the rejected
    /// mutation, matching zero-cache semantics). Return
    /// `Err(MutateError::Other("reason"))` for an infra failure that
    /// rolls back the entire push and retries.
    pub mutate: MutateFn,

    /// Optional per-user row visibility. `None` means every table is
    /// fully visible to every authenticated user.
    ///
    /// When set: for each application table, the engine calls this
    /// function. Return `None` if the table has no visibility filter
    /// (fully visible). Return `Some((where_clause, params))` with a
    /// SQL WHERE fragment (without the `WHERE` keyword) and positional
    /// parameters. The engine composes it as `SELECT * FROM "<table>"
    /// WHERE <fragment>` for snapshots and `... AND (<fragment>)` for
    /// diff point-reads.
    ///
    /// Any visibility config forces every pull to a full snapshot
    /// because a permission flip can revoke rows without changing them,
    /// which a diff cannot express.
    pub visible: Option<VisibleFn>,

    /// Authenticate an incoming request. Receives the raw HTTP headers.
    /// Return `Some(user_id)` for an authenticated user, or `None` to
    /// reject with HTTP 401.
    pub authenticate: AuthFn,

    /// Change-log retention rows. A client whose cookie falls below the
    /// pruned floor gets a full snapshot on its next pull. Default: 4096.
    pub retain_changes: i64,

    /// Whether visibility filtering is active at boot. When false the
    /// visibility callback is ignored. Can be toggled at runtime via the
    /// admin route.
    pub visibility_enabled: bool,

    /// Whether query-aware mode is active at boot. In query-aware mode
    /// pulls carry desired queries and go through the membership/refcount
    /// engine instead of the baseline full-namespace pull. Can be toggled
    /// at runtime via the admin route.
    pub query_aware: bool,

    /// Idle-between-steps budget for a server-owned admin transaction (the
    /// multi-request BEGIN/.../COMMIT protocol on `/admin/sql`). Each step
    /// refreshes it; if it elapses with no next step the namespace worker
    /// rolls the transaction back and unblocks pull/push. Use
    /// [`DEFAULT_ADMIN_TX_LEASE`] unless you have a reason to tune it.
    pub admin_tx_lease: Duration,
}

// ---- host ----------------------------------------------------------------

/// The native sync host. Owns the axum Router and all runtime state
/// (namespace workers, wake channels, counters, fault injection).
///
/// Construct with `SyncNativeHost::new(config, data_dir)`, then either
/// call `run(port)` to start serving or `into_router()` to nest the
/// router inside your own axum application.
pub struct SyncNativeHost {
    router: Router,
}

impl SyncNativeHost {
    /// Build a host from the given config and data directory.
    /// Namespace sqlite files are stored under `data_dir` with one file
    /// per namespace (`<name>.sqlite`).
    pub fn new(config: SyncNativeConfig, data_dir: PathBuf) -> Self {
        std::fs::create_dir_all(&data_dir).expect("failed to create data dir");

        let ctx = Arc::new(EngineContext::new(
            config.tables,
            config.retain_changes,
            config.visibility_enabled,
            config.query_aware,
            config.initialize,
            config.mutate,
            config.visible,
        ));

        let init_ctx = ctx.clone();
        let init: namespace::InitFn =
            Arc::new(move |db: &mut dyn sync_core::SyncDb| engine::init_namespace(db, &init_ctx));
        let manager = Arc::new(Manager::new(data_dir.clone(), init, config.admin_tx_lease));

        let state = Arc::new(server::AppState::new(
            manager,
            wake::WakeRegistry::new(),
            ctx,
            config.authenticate,
        ));

        let router = server::build_router(state.clone());

        Self { router }
    }

    /// Consume the host and return the axum Router for nesting inside
    /// another axum application.
    pub fn into_router(self) -> Router {
        self.router
    }

    /// Start serving on the given port (binds 127.0.0.1). Blocks until
    /// SIGINT or a fatal error.
    pub async fn run(self, port: u16) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("failed to bind");
        let addr = listener.local_addr().expect("no local addr");
        println!("sync-native listening on {addr}");

        axum::serve(listener, self.router)
            .with_graceful_shutdown(server::shutdown_signal())
            .await
            .expect("server error");
    }
}
