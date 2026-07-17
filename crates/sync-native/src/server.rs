// the axum HTTP surface: router construction, handlers for pull/push/wake
// and admin routes, observability, and graceful shutdown. this module is
// the generic server shell — all consumer-specific behaviour comes from
// the SyncNativeConfig passed at construction time.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Bytes;
use axum::extract::Request;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, ORIGIN};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;
use tower_http::cors::{AllowOrigin, CorsLayer};

use sync_core::value::f64_to_json;
use sync_core::{Row, SqlValue, SyncDb, WireValue};

use crate::db::RusqliteDb;
use crate::engine::{self, EngineContext};
use crate::fault::{FaultKind, FaultPoint, FaultRegistry};
use crate::namespace::{Manager, TxEnd};
use crate::obs::{self, Counters};
use crate::wake::WakeRegistry;

// ---- AppState ------------------------------------------------------------

pub struct AppState {
    pub manager: Arc<Manager>,
    pub wake: Arc<WakeRegistry>,
    pub ctx: Arc<EngineContext>,
    pub authenticate: crate::AuthFn,
    query_resolution: Option<crate::QueryResolution>,
    query_pull_locks: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    admin_token: String,
    allowed_origins: HashSet<String>,
    boot_id: String,
    // process-wide aggregate telemetry (mirrors the CF host's counters)
    counters: Counters,
    // harness/operator fault injection, armed per namespace (M6)
    faults: Arc<FaultRegistry>,
    // namespaces with a one-shot "drop the next push response" fault armed
    drop_push: Mutex<HashSet<String>>,
}

impl AppState {
    pub fn new(
        manager: Arc<Manager>,
        wake: Arc<WakeRegistry>,
        ctx: Arc<EngineContext>,
        authenticate: crate::AuthFn,
        query_resolution: Option<crate::QueryResolution>,
        admin_token: String,
        allowed_origins: Vec<String>,
    ) -> Self {
        Self {
            manager,
            wake,
            ctx,
            authenticate,
            query_resolution,
            query_pull_locks: Mutex::new(HashMap::new()),
            admin_token,
            allowed_origins: allowed_origins.into_iter().collect(),
            boot_id: boot_id(),
            counters: Counters::default(),
            faults: FaultRegistry::new(),
            drop_push: Mutex::new(HashSet::new()),
        }
    }

    fn arm_drop_push(&self, ns: &str) {
        self.drop_push.lock().unwrap().insert(ns.to_string());
    }
    fn take_drop_push(&self, ns: &str) -> bool {
        self.drop_push.lock().unwrap().remove(ns)
    }
}

fn query_pull_lock(
    locks: &Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    ns: &str,
) -> Arc<tokio::sync::Mutex<()>> {
    let mut locks = locks.lock().unwrap();
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(ns).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(tokio::sync::Mutex::new(()));
    locks.insert(ns.to_string(), Arc::downgrade(&lock));
    lock
}

fn boot_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{:x}-{:x}", nanos, std::process::id())
}

// ---- router --------------------------------------------------------------

pub fn build_router(state: Arc<AppState>) -> Router {
    let cors_origins = state.allowed_origins.clone();
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(move |origin, _| {
            origin
                .to_str()
                .ok()
                .is_some_and(|origin| cors_origins.contains(origin))
        }))
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([AUTHORIZATION, CONTENT_TYPE]);

    let admin = Router::new()
        .route("/admin/health", get(health))
        .route("/{ns}/admin/sql", post(admin_sql))
        .route("/{ns}/admin/settle-push", post(admin_settle_push))
        .route("/{ns}/admin/status", get(admin_status))
        .route("/{ns}/admin/invalidate", post(admin_invalidate))
        .route("/{ns}/admin/prune-to-head", post(admin_prune_to_head))
        .route("/{ns}/admin/reset-cursor", post(admin_reset_cursor))
        .route("/{ns}/admin/drop-next-push-response", post(admin_drop_push))
        .route("/{ns}/admin/fault", post(admin_fault))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_admin));

    Router::new()
        .route("/{ns}/pull", post(pull))
        .route("/{ns}/push", post(push))
        .route("/{ns}/wake", get(wake_ws))
        .layer(cors)
        .merge(admin)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_allowed_origin,
        ))
        .with_state(state)
}

pub async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

// ---- helpers -------------------------------------------------------------

fn json_status(status: u16, body: Value) -> Response {
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        axum::Json(body),
    )
        .into_response()
}

async fn require_allowed_origin(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    if let Some(origin) = request.headers().get(ORIGIN) {
        let allowed = origin
            .to_str()
            .ok()
            .is_some_and(|origin| state.allowed_origins.contains(origin));
        if !allowed {
            return json_status(403, json!({ "error": "origin not allowed" }));
        }
    }
    next.run(request).await
}

async fn require_admin(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    // Admin is a machine-only surface. This execution guard is independent of
    // CORS response headers and applies even to an otherwise allowed origin.
    if request.headers().contains_key(ORIGIN) {
        return json_status(
            403,
            json!({ "error": "browser admin requests are forbidden" }),
        );
    }
    let authenticated = request
        .headers()
        .get("x-admin-key")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|provided| {
            provided
                .as_bytes()
                .ct_eq(state.admin_token.as_bytes())
                .into()
        });
    if !authenticated {
        return json_status(401, json!({ "error": "invalid admin token" }));
    }
    next.run(request).await
}

fn row_to_json(row: &Row) -> Value {
    let mut object = serde_json::Map::new();
    for (i, col) in row.columns.iter().enumerate() {
        let value = match &row.values[i] {
            SqlValue::Null => Value::Null,
            SqlValue::Integer(n) => json!(n),
            SqlValue::Real(f) => f64_to_json(*f),
            SqlValue::Text(s) => Value::String(s.clone()),
            SqlValue::Blob(b) => Value::Array(b.iter().map(|byte| json!(byte)).collect()),
        };
        object.insert(col.clone(), value);
    }
    Value::Object(object)
}

fn admin_sql_params(value: &Value) -> Result<Vec<SqlValue>, String> {
    let Some(params) = value.get("params") else {
        return Ok(Vec::new());
    };
    let params: Vec<WireValue> = serde_json::from_value(params.clone())
        .map_err(|error| format!("invalid params: {error}"))?;
    params
        .into_iter()
        .map(SqlValue::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("invalid params: {error}"))
}

// ---- http handlers -------------------------------------------------------

async fn health() -> Response {
    json_status(200, json!({ "ok": true, "pid": std::process::id() }))
}

async fn pull(
    State(state): State<Arc<AppState>>,
    Path(ns): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let Some(user_id) = (state.authenticate)(&headers) else {
        return json_status(401, json!({ "error": "missing auth" }));
    };
    let Ok(mut value) = serde_json::from_slice::<Value>(&body) else {
        return json_status(400, json!({ "error": "invalid json" }));
    };
    if !value.is_object() {
        return json_status(400, json!({ "error": "invalid pull body" }));
    }
    let namespace = match state.manager.get(&ns) {
        Ok(n) => n,
        Err(e) => return json_status(400, json!({ "error": e })),
    };
    let _query_pull_guard = if state.ctx.query_aware && state.query_resolution.is_some() {
        Some(
            query_pull_lock(&state.query_pull_locks, &ns)
                .lock_owned()
                .await,
        )
    } else {
        None
    };
    if state.ctx.query_aware
        && let Some(resolution) = &state.query_resolution
    {
        if let Err(error) = resolve_named_queries(
            &mut value,
            &headers,
            &user_id,
            &resolution.resolve,
            resolution.transform_version,
        )
        .await
        {
            return json_status(error.status, json!({ "error": error.message }));
        }
        value["_serverQueryTransformVersion"] = json!(resolution.transform_version);
    }
    let ns_hash = obs::namespace_hash(&ns);
    let input_cookie = value.get("cookie").cloned().unwrap_or(Value::Null);
    let query_puts = obs::count_query_puts(&value);
    let ctx = state.ctx.clone();
    let faults = state.faults.clone();
    let fault_ns = ns.clone();
    let tx_started = Instant::now();
    let observed = namespace
        .run(move |conn| engine::pull(conn, &ctx, &faults, &fault_ns, &value, &user_id))
        .await;
    let transaction_ms = tx_started.elapsed().as_millis() as u64;
    Counters::bump(&state.counters.pulls);

    let mut result_class = "success";
    let mut output_cookie = Value::Null;
    let mut row_puts = 0;
    let mut row_deletes = 0;
    let mut reset_reason = Value::Null;
    match &observed.result {
        Ok(response) => {
            if response.get("unchanged").and_then(Value::as_bool) == Some(true) {
                result_class = "unchanged";
            }
            output_cookie = response.get("cookie").cloned().unwrap_or(Value::Null);
            let (puts, deletes) = obs::count_patch(response);
            row_puts = puts;
            row_deletes = deletes;
            // each desired-query put drives a recompile, mirroring CF's counter
            Counters::add(&state.counters.query_recompilations, query_puts);
        }
        Err(e) => {
            if e.status == 409 {
                result_class = "reset";
                reset_reason = Value::String(e.message.clone());
                Counters::bump(&state.counters.resets);
            } else {
                result_class = "error";
                if e.status == 500 {
                    Counters::bump(&state.counters.invariant_failures);
                }
            }
        }
    }
    // floor advancing means old changes were pruned: a retention run
    if observed.floor > observed.floor_before {
        Counters::bump(&state.counters.retention_runs);
    }
    obs::RequestEvent {
        namespace_hash: &ns_hash,
        request_kind: "pull",
        result_class,
        input_cookie,
        output_cookie,
        retained_floor: observed.floor,
        current_watermark: observed.watermark,
        change_rows_included: row_puts + row_deletes,
        queries_recomputed: query_puts,
        row_puts,
        row_deletes,
        lmid_advances: 0,
        transaction_ms,
        total_ms: started.elapsed().as_millis() as u64,
        reset_reason,
    }
    .emit();

    match observed.result {
        Ok(v) => json_status(200, v),
        Err(e) => json_status(e.status, json!({ "error": e.message })),
    }
}

async fn resolve_named_queries(
    body: &mut Value,
    headers: &HeaderMap,
    user_id: &str,
    resolver: &crate::ResolveQueriesFn,
    transform_version: u64,
) -> Result<(), crate::QueryResolveError> {
    const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
    if transform_version > MAX_SAFE_INTEGER {
        return Err(crate::QueryResolveError::upstream(
            "query transform version exceeds the JSON safe integer range",
        ));
    }

    let Some(patch) = body
        .get_mut("queries")
        .and_then(|queries| queries.get_mut("patch"))
        .and_then(Value::as_array_mut)
    else {
        return Ok(());
    };

    let mut named_queries = Vec::new();
    let mut put_indices = Vec::new();
    for (index, operation) in patch.iter().enumerate() {
        if operation.get("op").and_then(Value::as_str) != Some("put") {
            continue;
        }
        if operation.get("hash").and_then(Value::as_str).is_none() {
            return Err(crate::QueryResolveError::bad_request(
                "query put requires a hash",
            ));
        }
        let Some(name) = operation.get("name").and_then(Value::as_str) else {
            return Err(crate::QueryResolveError::bad_request(
                "query put requires a server-resolved named query",
            ));
        };
        let Some(args) = operation.get("args").and_then(Value::as_array) else {
            return Err(crate::QueryResolveError::bad_request(
                "named query args must be an array",
            ));
        };
        named_queries.push(crate::NamedQuery {
            name: name.to_string(),
            args: args.clone(),
        });
        put_indices.push(index);
    }

    if named_queries.is_empty() {
        return Ok(());
    }

    let asts = resolver(named_queries, headers.clone(), user_id.to_string()).await?;
    if asts.len() != put_indices.len() {
        return Err(crate::QueryResolveError::upstream(format!(
            "query resolver returned {} ASTs for {} queries",
            asts.len(),
            put_indices.len()
        )));
    }

    for (index, ast) in put_indices.into_iter().zip(asts) {
        let hash = patch[index]["hash"].clone();
        patch[index] = json!({
            "op": "put",
            "hash": hash,
            "ast": ast,
            "transformVersion": transform_version,
        });
    }
    Ok(())
}

async fn push(
    State(state): State<Arc<AppState>>,
    Path(ns): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let Some(user_id) = (state.authenticate)(&headers) else {
        return json_status(401, json!({ "error": "missing auth" }));
    };
    let Ok(value) = serde_json::from_slice::<Value>(&body) else {
        return json_status(400, json!({ "error": "invalid json" }));
    };
    // the pushing client (to exclude from the wake fan-out). best-effort: the
    // first mutation's clientID, which is the connected client in the normal
    // case (a recovery push carries a previous clientID, harmless to wake).
    let pusher = value
        .get("mutations")
        .and_then(|m| m.get(0))
        .and_then(|m0| m0.get("clientID"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let namespace = match state.manager.get(&ns) {
        Ok(n) => n,
        Err(e) => return json_status(400, json!({ "error": e })),
    };
    let ns_hash = obs::namespace_hash(&ns);
    let ctx = state.ctx.clone();
    let faults = state.faults.clone();
    let fault_ns = ns.clone();
    let tx_started = Instant::now();
    let observed = namespace
        .run(move |conn| engine::push(conn, &ctx, &faults, &fault_ns, &value, &user_id))
        .await;
    let transaction_ms = tx_started.elapsed().as_millis() as u64;
    Counters::bump(&state.counters.pushes);

    let mut result_class = "success";
    let mut lmid_advances = 0;
    let mut reset_reason = Value::Null;
    match &observed.result {
        Ok(response) => {
            let (advances, app_errors) = obs::count_push_mutations(response);
            lmid_advances = advances;
            Counters::add(&state.counters.application_errors, app_errors);
        }
        Err(e) => {
            result_class = "error";
            if e.status == 409 {
                reset_reason = Value::String(e.message.clone());
            } else if e.status == 500 {
                Counters::bump(&state.counters.invariant_failures);
            }
        }
    }
    if observed.floor > observed.floor_before {
        Counters::bump(&state.counters.retention_runs);
    }
    obs::RequestEvent {
        namespace_hash: &ns_hash,
        request_kind: "push",
        result_class,
        input_cookie: Value::Null,
        output_cookie: Value::Null,
        retained_floor: observed.floor,
        current_watermark: observed.watermark,
        change_rows_included: 0,
        queries_recomputed: 0,
        row_puts: 0,
        row_deletes: 0,
        lmid_advances,
        transaction_ms,
        total_ms: started.elapsed().as_millis() as u64,
        reset_reason,
    }
    .emit();

    match observed.result {
        Ok(v) => {
            // wake the namespace's other clients post-commit
            state.wake.wake(&ns, &pusher);
            if state.take_drop_push(&ns) {
                // fault: the mutation committed, but the client never sees the
                // ack — it must reconnect and settle via replay/LMID recovery.
                return json_status(500, json!({ "error": "dropped push response" }));
            }
            json_status(200, v)
        }
        Err(e) => json_status(e.status, json!({ "error": e.message })),
    }
}

async fn admin_sql(
    State(state): State<Arc<AppState>>,
    Path(ns): Path<String>,
    body: Bytes,
) -> Response {
    let Ok(value) = serde_json::from_slice::<Value>(&body) else {
        return json_status(400, json!({ "error": "invalid json" }));
    };
    let Some(query) = value
        .get("query")
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        return json_status(400, json!({ "error": "missing query" }));
    };
    let namespace = match state.manager.get(&ns) {
        Ok(n) => n,
        Err(e) => return json_status(400, json!({ "error": e })),
    };
    let transaction_id = value
        .get("transactionId")
        .and_then(Value::as_str)
        .map(str::to_string);
    let transaction_step = value.get("transactionStep").and_then(Value::as_str);
    let params = match admin_sql_params(&value) {
        Ok(params) => params,
        Err(error) => return json_status(400, json!({ "error": error })),
    };

    // an admin transaction step carries both an id and a step; a one-shot query
    // carries neither. anything half-specified is malformed. the transaction is
    // server-owned: begin validates the declared SQL and runs BEGIN, query runs
    // the client statement inside it, and end runs COMMIT/ROLLBACK named by the
    // query.
    match (transaction_id, transaction_step) {
        (Some(id), Some("begin")) => {
            // the transaction is server-owned: accept only a canonical BEGIN as the
            // step's declared intent, then run the real BEGIN in the worker.
            if !is_canonical_begin(&query) {
                return json_status(
                    400,
                    json!({ "error": "transaction begin requires a canonical BEGIN statement" }),
                );
            }
            if !params.is_empty() {
                return json_status(400, json!({ "error": "transaction begin forbids params" }));
            }
            match namespace.tx_begin(id).await {
                Ok(()) => json_status(200, json!({ "rows": [] })),
                Err(e) => json_status(e.status, json!({ "error": e.message })),
            }
        }
        (Some(id), Some("query")) => {
            // the worker owns begin/commit/rollback; a query step may not smuggle
            // transaction-control SQL, which would desync scheduler and connection.
            if is_transaction_control(&query) {
                return json_status(
                    400,
                    json!({ "error": "transaction control is not allowed in a query step" }),
                );
            }
            let result = namespace
                .tx_query(id, move |conn| {
                    let mut db = RusqliteDb::new(conn);
                    db.query(&query, &params)
                })
                .await;
            match result {
                Ok(rows) => {
                    // uncommitted: do not wake clients until the transaction ends.
                    let rows: Vec<Value> = rows.iter().map(row_to_json).collect();
                    json_status(200, json!({ "rows": rows }))
                }
                Err(e) => json_status(e.status, json!({ "error": e.message })),
            }
        }
        (Some(id), Some("end")) => {
            let Some(end) = parse_tx_end(&query) else {
                return json_status(
                    400,
                    json!({ "error": "transaction end requires COMMIT or ROLLBACK" }),
                );
            };
            if !params.is_empty() {
                return json_status(400, json!({ "error": "transaction end forbids params" }));
            }
            match namespace.tx_end(id, end).await {
                Ok(()) => {
                    // a committed transaction publishes its rows; wake every
                    // namespace client so they pull without waiting for a push.
                    if matches!(end, TxEnd::Commit) {
                        state.wake.wake(&ns, "");
                    }
                    json_status(200, json!({ "rows": [] }))
                }
                Err(e) => json_status(e.status, json!({ "error": e.message })),
            }
        }
        (Some(_), Some(_)) => json_status(
            400,
            json!({ "error": "transaction step must be begin, query, or end" }),
        ),
        (None, Some(_)) => json_status(400, json!({ "error": "missing transaction id" })),
        (Some(_), None) => json_status(400, json!({ "error": "missing transaction step" })),
        (None, None) => {
            let result = namespace
                .run(move |conn| {
                    let mut db = RusqliteDb::new(conn);
                    db.query(&query, &params)
                })
                .await;
            match result {
                Ok(rows) => {
                    // admin SQL is the upstream-write seam for embedded consumers.
                    // triggers captured the committed rows; wake every namespace
                    // client so they pull those changes without waiting for a push.
                    state.wake.wake(&ns, "");
                    let rows: Vec<Value> = rows.iter().map(row_to_json).collect();
                    json_status(200, json!({ "rows": rows }))
                }
                Err(e) => json_status(500, json!({ "error": e.0 })),
            }
        }
    }
}

async fn admin_settle_push(
    State(state): State<Arc<AppState>>,
    Path(ns): Path<String>,
    body: Bytes,
) -> Response {
    let Ok(value) = serde_json::from_slice::<Value>(&body) else {
        return json_status(400, json!({ "error": "invalid json" }));
    };
    let Some(push) = value.get("push").cloned() else {
        return json_status(400, json!({ "error": "missing push" }));
    };
    let Some(response) = value.get("response").cloned() else {
        return json_status(400, json!({ "error": "missing response" }));
    };
    let Some(user_id) = value
        .get("userID")
        .and_then(Value::as_str)
        .filter(|user_id| !user_id.is_empty())
        .map(str::to_string)
    else {
        return json_status(400, json!({ "error": "missing userID" }));
    };
    let namespace = match state.manager.get(&ns) {
        Ok(namespace) => namespace,
        Err(error) => return json_status(400, json!({ "error": error })),
    };
    let ctx = state.ctx.clone();
    let result = namespace
        .run(move |conn| engine::settle_delegated_push(conn, &ctx, &push, &response, &user_id))
        .await;
    match result {
        Ok(settled) => {
            if settled > 0 {
                state.wake.wake(&ns, "");
            }
            json_status(200, json!({ "settled": settled }))
        }
        Err(error) => json_status(error.status, json!({ "error": error.message })),
    }
}

// normalize a transaction-control token: trim whitespace and a trailing `;`,
// then uppercase, so "begin;", "  BEGIN " and "BEGIN" compare equal.
fn normalize_control(query: &str) -> String {
    query
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_ascii_uppercase()
}

// the begin step declares its intent as SQL text; only a canonical BEGIN is
// accepted, so malformed or mismatched SQL cannot open the transaction.
fn is_canonical_begin(query: &str) -> bool {
    matches!(
        normalize_control(query).as_str(),
        "BEGIN" | "BEGIN TRANSACTION"
    )
}

// the first real SQL keyword, skipping leading whitespace, empty statements
// (`;`), and SQL comments (`-- line` and `/* block */`). rusqlite executes only
// the first statement of a query and skips comments, so this matches what the
// connection would actually run, closing the "hide control SQL behind a comment"
// bypass.
fn first_sql_keyword(query: &str) -> String {
    let mut rest = query;
    loop {
        let trimmed = rest.trim_start_matches(|c: char| c.is_whitespace() || c == ';');
        if let Some(after) = trimmed.strip_prefix("--") {
            // line comment: skip through the newline (or to end of input).
            rest = after.find('\n').map_or("", |nl| &after[nl + 1..]);
            continue;
        }
        if let Some(after) = trimmed.strip_prefix("/*") {
            // block comment: skip past the close (unterminated consumes the rest).
            rest = after.find("*/").map_or("", |end| &after[end + 2..]);
            continue;
        }
        rest = trimmed;
        break;
    }
    rest.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .next()
        .unwrap_or("")
        .to_ascii_uppercase()
}

// reject any statement whose first keyword drives transaction state, so a query
// step cannot open, commit, or roll back the worker-owned transaction.
fn is_transaction_control(query: &str) -> bool {
    matches!(
        first_sql_keyword(query).as_str(),
        "BEGIN" | "COMMIT" | "END" | "ROLLBACK" | "SAVEPOINT" | "RELEASE"
    )
}

// the end step names its outcome as SQL text (COMMIT/ROLLBACK, END is a commit
// synonym); the worker runs the real statement. anything else is a malformed end.
fn parse_tx_end(query: &str) -> Option<TxEnd> {
    match normalize_control(query).as_str() {
        "COMMIT" | "END" => Some(TxEnd::Commit),
        "ROLLBACK" => Some(TxEnd::Rollback),
        _ => None,
    }
}

async fn admin_status(State(state): State<Arc<AppState>>, Path(_ns): Path<String>) -> Response {
    // diagnostics surface: process-wide aggregate counters alongside boot info.
    // the HTTP guard limits admin/* to token-authenticated, originless clients.
    json_status(
        200,
        json!({
            "ok": true,
            "bootID": state.boot_id,
            "pid": std::process::id(),
            "hostVersion": obs::HOST_VERSION,
            "engineVersion": obs::ENGINE_VERSION,
            "counters": state.counters.snapshot(),
        }),
    )
}

async fn admin_invalidate(State(state): State<Arc<AppState>>, Path(ns): Path<String>) -> Response {
    let namespace = match state.manager.get(&ns) {
        Ok(n) => n,
        Err(e) => return json_status(400, json!({ "error": e })),
    };
    let result = namespace.run(engine::invalidate).await;
    match result {
        Ok(()) => json_status(200, json!({ "ok": true })),
        Err(e) => json_status(e.status, json!({ "error": e.message })),
    }
}

// harness full-prune hook: empty the change log to the head over the same
// sqlite file (see engine::prune_to_head). the state machine pairs this with a
// server restart to prove the durable high-water keeps the served cookie
// monotonic (mutant O1).
async fn admin_prune_to_head(
    State(state): State<Arc<AppState>>,
    Path(ns): Path<String>,
) -> Response {
    let namespace = match state.manager.get(&ns) {
        Ok(n) => n,
        Err(e) => return json_status(400, json!({ "error": e })),
    };
    let result = namespace.run(engine::prune_to_head).await;
    match result {
        Ok(()) => json_status(200, json!({ "ok": true })),
        Err(e) => json_status(e.status, json!({ "error": e.message })),
    }
}

async fn admin_reset_cursor(
    State(state): State<Arc<AppState>>,
    Path(ns): Path<String>,
) -> Response {
    let namespace = match state.manager.get(&ns) {
        Ok(n) => n,
        Err(e) => return json_status(400, json!({ "error": e })),
    };
    let result = namespace.run(engine::reset_cursor).await;
    match result {
        Ok(()) => json_status(200, json!({ "ok": true })),
        Err(e) => json_status(500, json!({ "error": e.0 })),
    }
}

async fn admin_drop_push(State(state): State<Arc<AppState>>, Path(ns): Path<String>) -> Response {
    state.arm_drop_push(&ns);
    json_status(200, json!({ "ok": true }))
}

// arm a one-shot fault for a namespace at a precise pull/push lifecycle point (M6).
// body: { "point": <point>, "kind": "kill" | "error" | "quota" }, or { "clear": true }
// to disarm. operator/harness-only: the HTTP guard limits admin/* to
// token-authenticated, originless clients.
async fn admin_fault(
    State(state): State<Arc<AppState>>,
    Path(ns): Path<String>,
    body: Bytes,
) -> Response {
    let Ok(value) = serde_json::from_slice::<Value>(&body) else {
        return json_status(400, json!({ "error": "invalid json" }));
    };
    if value.get("clear").and_then(Value::as_bool) == Some(true) {
        state.faults.clear(&ns);
        return json_status(200, json!({ "ok": true, "armed": false }));
    }
    let Some(point) = value
        .get("point")
        .and_then(Value::as_str)
        .and_then(FaultPoint::parse)
    else {
        return json_status(
            400,
            json!({ "error": "point must be one of push_before_mutation, push_after_write_before_commit, push_after_commit_before_response, pull_during_tx, pull_after_commit" }),
        );
    };
    let Some(kind) = value
        .get("kind")
        .and_then(Value::as_str)
        .and_then(FaultKind::parse)
    else {
        return json_status(
            400,
            json!({ "error": "kind must be kill, error, or quota" }),
        );
    };
    // the after-write point is kill-only: the engine's generic per-mutation tx error
    // type cannot be forged to inject an Error/Quota return mid-transaction.
    if point == FaultPoint::PushAfterWriteBeforeCommit && kind != FaultKind::Kill {
        return json_status(
            400,
            json!({ "error": "push_after_write_before_commit supports only kind=kill" }),
        );
    }
    state.faults.arm(&ns, point, kind);
    json_status(
        200,
        json!({ "ok": true, "armed": true, "point": point.as_str() }),
    )
}

async fn wake_ws(
    State(state): State<Arc<AppState>>,
    Path(ns): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    ws: WebSocketUpgrade,
) -> Response {
    let client_id = params.get("clientID").cloned().unwrap_or_default();
    ws.on_upgrade(move |socket| wake_socket(socket, state, ns, client_id))
}

async fn wake_socket(mut socket: WebSocket, state: Arc<AppState>, ns: String, client_id: String) {
    let subscription = state.wake.subscribe(&ns, &client_id);
    loop {
        tokio::select! {
            _ = subscription.waked() => {
                if socket.send(Message::Text("wake".into())).await.is_err() {
                    break;
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_pull_locks_share_active_namespaces_and_collect_finished_ones() {
        let locks = Mutex::new(HashMap::new());
        let first = query_pull_lock(&locks, "first");
        let first_again = query_pull_lock(&locks, "first");
        assert!(Arc::ptr_eq(&first, &first_again));
        assert_eq!(locks.lock().unwrap().len(), 1);

        drop(first);
        drop(first_again);
        let second = query_pull_lock(&locks, "second");
        let locks = locks.lock().unwrap();
        assert!(!locks.contains_key("first"));
        assert!(locks.contains_key("second"));
        drop(locks);
        drop(second);
    }
}
