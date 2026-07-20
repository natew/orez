// sync-native: a native axum + rusqlite host for the sync-core engine.
//
// this crate is both a library (embed the sync host in your Rust process)
// and a binary (run a standalone sync server around application-owned HTTP
// callbacks). downstream Rust consumers add `sync-native` as a dependency,
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
pub mod retain;
pub mod seed;
pub mod server;
pub mod standalone;
pub mod wake;

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::Router;
use axum::http::HeaderMap;
use engine::{EngineContext, InitFn, MutateFn, VisibleFn};
use namespace::Manager;
use serde_json::Value;
use sync_core::schema::Tables;

/// An authentication failure returned to the pull or push caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthError {
    pub status: u16,
    pub message: String,
}

impl AuthError {
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: 401,
            message: message.into(),
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: 403,
            message: message.into(),
        }
    }

    pub fn upstream(message: impl Into<String>) -> Self {
        Self {
            status: 502,
            message: message.into(),
        }
    }
}

/// Application-authenticated claims. `userID` is the only field interpreted by
/// the engine; the full object remains available to query policy callbacks.
#[derive(Clone, Debug, PartialEq)]
pub struct AuthClaims {
    value: Value,
    user_id: String,
}

impl AuthClaims {
    pub fn new(user_id: impl Into<String>) -> Self {
        let user_id = user_id.into();
        assert!(!user_id.is_empty(), "auth claims userID must not be empty");
        Self {
            value: serde_json::json!({ "userID": user_id }),
            user_id,
        }
    }

    pub fn from_value(value: Value) -> Result<Self, String> {
        let user_id = value
            .as_object()
            .and_then(|claims| claims.get("userID"))
            .and_then(Value::as_str)
            .filter(|user_id| !user_id.is_empty())
            .ok_or_else(|| "normalized claims require a non-empty userID".to_string())?
            .to_string();
        Ok(Self { value, user_id })
    }

    pub fn user_id(&self) -> &str {
        &self.user_id
    }

    pub fn value(&self) -> &Value {
        &self.value
    }
}

pub type AuthFuture = Pin<Box<dyn Future<Output = Result<AuthClaims, AuthError>> + Send>>;

/// Authenticate an incoming request for one namespace. The callback is async
/// so standalone hosts can delegate policy to their application server.
pub type AuthFn = Arc<dyn Fn(HeaderMap, String) -> AuthFuture + Send + Sync>;

pub type WakeAuthorizeFuture = Pin<Box<dyn Future<Output = Result<(), AuthError>> + Send>>;

/// Validate a short-lived wake capability for one namespace before upgrading
/// its advisory WebSocket.
pub type AuthorizeWakeFn = Arc<dyn Fn(String, Option<String>) -> WakeAuthorizeFuture + Send + Sync>;

/// One named desired query forwarded by a client for server-side resolution.
#[derive(Clone, Debug, PartialEq)]
pub struct NamedQuery {
    pub name: String,
    pub args: Vec<Value>,
}

/// A query resolver failure returned to the pull caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryResolveError {
    pub status: u16,
    pub message: String,
}

impl QueryResolveError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: 400,
            message: message.into(),
        }
    }

    pub fn upstream(message: impl Into<String>) -> Self {
        Self {
            status: 502,
            message: message.into(),
        }
    }
}

pub type ResolveQueriesFuture =
    Pin<Box<dyn Future<Output = Result<ResolvedQueries, QueryResolveError>> + Send>>;

/// Permission-checked ASTs and the application-owned version that participates
/// in each desired-query cache key.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedQueries {
    pub asts: Vec<Value>,
    pub transform_version: u64,
}

/// Resolve a batch of named desired queries into permission-checked Zero ASTs.
/// The returned ASTs must match the input order and length.
pub type ResolveQueriesFn = Arc<
    dyn Fn(Vec<NamedQuery>, HeaderMap, AuthClaims, String) -> ResolveQueriesFuture + Send + Sync,
>;

/// Server-owned query resolution and invalidation policy.
pub struct QueryResolution {
    pub resolve: ResolveQueriesFn,
}

/// Default idle-between-steps budget for a server-owned admin transaction. If
/// an admin client sends no next step within this window the namespace worker
/// rolls the transaction back and unblocks pull/push, so a lost admin client
/// cannot wedge a namespace.
pub const DEFAULT_ADMIN_TX_LEASE: Duration = Duration::from_secs(30);

/// HTTP security for a native sync process.
///
/// Admin routes always require `x-admin-key` and never accept requests carrying
/// a browser `Origin` header. Pull, push, and wake requests carrying an origin
/// must match one of `allowed_origins`; originless native/server requests remain
/// available. Use [`SyncNativeSecurity::process_random`] unless a supervisor
/// needs to share a process-scoped token with a trusted local SQL client.
pub struct SyncNativeSecurity {
    admin_token: String,
    allowed_origins: Vec<String>,
}

impl SyncNativeSecurity {
    /// Generate a new 256-bit process-local admin token and deny every browser
    /// origin until one is explicitly allowed.
    pub fn process_random() -> Self {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes).expect("failed to generate sync-native admin token");
        let mut admin_token = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write;
            write!(&mut admin_token, "{byte:02x}").expect("writing to a String cannot fail");
        }
        Self {
            admin_token,
            allowed_origins: Vec::new(),
        }
    }

    /// Use a supervisor-provided process token. Empty tokens are rejected.
    pub fn with_admin_token(admin_token: impl Into<String>) -> Self {
        let admin_token = admin_token.into();
        assert!(
            admin_token.len() >= 32,
            "sync-native admin token must contain at least 32 bytes"
        );
        Self {
            admin_token,
            allowed_origins: Vec::new(),
        }
    }

    /// Allow one exact HTTP(S) browser origin for pull, push, and wake traffic.
    pub fn allow_origin(mut self, origin: impl Into<String>) -> Self {
        let origin = origin.into();
        assert!(
            valid_origin(&origin),
            "invalid sync-native browser origin: {origin}"
        );
        if !self.allowed_origins.contains(&origin) {
            self.allowed_origins.push(origin);
        }
        self
    }
}

fn valid_origin(origin: &str) -> bool {
    let Ok(uri) = origin.parse::<axum::http::Uri>() else {
        return false;
    };
    matches!(uri.scheme_str(), Some("http" | "https"))
        && uri.authority().is_some()
        && uri.path() == "/"
        && uri.query().is_none()
}

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
    /// parameters. Table and column references in this callback use logical
    /// Zero schema names. The engine projects physical `serverName` columns
    /// to logical aliases before applying the fragment.
    ///
    /// Any visibility config forces every pull to a full snapshot
    /// because a permission flip can revoke rows without changing them,
    /// which a diff cannot express.
    pub visible: Option<VisibleFn>,

    /// Authenticate an incoming request. Receives the raw HTTP headers and
    /// namespace. Return the authenticated user ID or an [`AuthError`].
    pub authenticate: AuthFn,

    /// Authorize a WebSocket wake token for one namespace. Browser WebSockets
    /// cannot attach an Authorization header, so applications mint a
    /// short-lived namespace capability and validate it here.
    pub authorize_wake: AuthorizeWakeFn,

    /// Change-log retention rows. A client whose cookie falls below the
    /// pruned floor gets a full snapshot on its next pull. Default: 4096.
    pub retain_changes: i64,

    /// Baseline-pull change-row cap. One diff response ships at most this many
    /// change rows, cutting at a row boundary before pk dedup; the remainder
    /// ships on the next poll. A small cap forces a mutation's row effects and
    /// its lmid ack onto separate pulls, exercising the capped-diff cut path.
    /// Use [`sync_core::Caps::default().max_change_rows`] for production budgets.
    pub max_change_rows: usize,

    /// Whether visibility filtering is active at boot. When false the
    /// visibility callback is ignored. Can be toggled at runtime via the
    /// admin route.
    pub visibility_enabled: bool,

    /// Whether query-aware mode is active at boot. In query-aware mode
    /// pulls carry desired queries and go through the membership/refcount
    /// engine instead of the baseline full-namespace pull. Can be toggled
    /// at runtime via the admin route.
    pub query_aware: bool,

    /// Optional server-side named-query resolver. When configured, every query
    /// put must carry a name and args, and any client-authored AST is discarded.
    /// The request headers are included so the consumer can apply the same auth
    /// context as its application query endpoint.
    pub query_resolution: Option<QueryResolution>,

    /// Idle-between-steps budget for a server-owned admin transaction (the
    /// multi-request BEGIN/.../COMMIT protocol on `/admin/sql`). Each step
    /// refreshes it; if it elapses with no next step the namespace worker
    /// rolls the transaction back and unblocks pull/push. Use
    /// [`DEFAULT_ADMIN_TX_LEASE`] unless you have a reason to tune it.
    pub admin_tx_lease: Duration,

    /// On-disk retention for per-namespace replica files. Disabled by default.
    /// Enable it only for derived replicas that no other process opens; shared or
    /// authoritative SQLite files must remain disabled. When explicitly enabled,
    /// the host evicts idle namespace workers and deletes replicas that are stale
    /// or over budget, once at startup and then on a background timer. The timer
    /// only runs when the host is started with [`SyncNativeHost::run`] or
    /// [`SyncNativeHost::run_on`].
    pub retention: retain::RetentionPolicy,
}

// ---- host ----------------------------------------------------------------

/// The native sync host. Owns the axum Router and all runtime state
/// (namespace workers, wake channels, counters, fault injection).
///
/// Construct with `SyncNativeHost::new(config, data_dir)`, then either
/// call `run(port)` to start serving or `into_router()` to nest the router
/// inside an axum application that installs TCP peer metadata.
pub struct SyncNativeHost {
    router: Router,
    state: Arc<server::AppState>,
    admin_token: String,
    manager: Arc<Manager>,
    retention: retain::RetentionPolicy,
}

impl SyncNativeHost {
    /// Build a host from the given config and data directory.
    /// Namespace sqlite files are stored under `data_dir` with one file
    /// per namespace (`<name>.sqlite`).
    pub fn new(config: SyncNativeConfig, data_dir: PathBuf) -> Self {
        Self::new_with_security(config, data_dir, SyncNativeSecurity::process_random())
    }

    /// Build a host with an explicit process security policy.
    pub fn new_with_security(
        config: SyncNativeConfig,
        data_dir: PathBuf,
        security: SyncNativeSecurity,
    ) -> Self {
        std::fs::create_dir_all(&data_dir).expect("failed to create data dir");

        let ctx = Arc::new(EngineContext {
            tables: config.tables,
            retain_changes: config.retain_changes,
            max_change_rows: config.max_change_rows,
            visibility_enabled: config.visibility_enabled,
            query_aware: config.query_aware,
            init_fn: config.initialize,
            mutate_fn: config.mutate,
            visible_fn: config.visible,
        });

        let init_ctx = ctx.clone();
        let init: namespace::InitFn =
            Arc::new(move |db: &mut dyn sync_core::SyncDb| engine::init_namespace(db, &init_ctx));
        let manager = Arc::new(Manager::new(data_dir.clone(), init, config.admin_tx_lease));

        // startup sweep: no namespaces are open yet, so this is a pure on-disk
        // reclamation with nothing to race. it clears replicas left behind by
        // prior runs (abandoned or over budget) before serving begins.
        let retention = config.retention;
        manager.retain(&retention, SystemTime::now()).emit();

        let admin_token = security.admin_token.clone();
        let state = Arc::new(server::AppState::new(
            manager.clone(),
            wake::WakeRegistry::new(),
            ctx,
            config.authenticate,
            config.authorize_wake,
            config.query_resolution,
            security,
        ));

        let router = server::build_router(state.clone());

        Self {
            router,
            state,
            admin_token,
            manager,
            retention,
        }
    }

    /// Return the process-scoped token required in `x-admin-key`.
    ///
    /// Keep this value in the supervising process. It must never be embedded in
    /// browser code or written to ordinary application logs.
    pub fn admin_token(&self) -> &str {
        &self.admin_token
    }

    /// Consume the host and return the axum Router for nesting inside another
    /// axum application. Admin routes fail closed when peer metadata is absent.
    pub fn into_router(self) -> Router {
        self.router
    }

    /// Consume the host as an in-process router whose admin routes may be
    /// called without TCP peer metadata. Non-loopback peers remain forbidden.
    /// Use this only when the embedding process is the admin trust boundary.
    pub fn into_router_trusted(self) -> Router {
        self.state.trust_missing_admin_peer();
        self.router
    }

    /// Start serving on the given port (binds 127.0.0.1). Blocks until
    /// SIGINT or a fatal error.
    pub async fn run(self, port: u16) {
        self.run_on("127.0.0.1".parse().expect("valid loopback IP"), port)
            .await;
    }

    /// Start serving on an explicit IP address. Standalone supervisors may bind
    /// a public container interface while keeping every policy callback on
    /// loopback. Admin routes still reject every non-loopback TCP peer.
    pub async fn run_on(self, host: std::net::IpAddr, port: u16) {
        let listener = tokio::net::TcpListener::bind((host, port))
            .await
            .expect("failed to bind");

        // background retention: on each tick, evict idle namespace workers and
        // reclaim disk. the startup sweep already ran in the constructor; this
        // keeps a long-lived process bounded without waiting for a restart.
        if self.retention.is_enabled() {
            let manager = self.manager.clone();
            let policy = self.retention;
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(policy.interval());
                ticker.tick().await; // the first tick fires immediately; skip it
                loop {
                    ticker.tick().await;
                    manager.retain(&policy, SystemTime::now()).emit();
                }
            });
        }

        axum::serve(
            listener,
            self.router
                .into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(server::shutdown_signal())
        .await
        .expect("server error");
    }
}
