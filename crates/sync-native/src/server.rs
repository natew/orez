// the axum HTTP surface: router construction, handlers for pull/push/wake
// and admin routes, observability, and graceful shutdown. this module is
// the generic server shell — all consumer-specific behaviour comes from
// the SyncNativeConfig passed at construction time.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::{Value, json};

use sync_core::value::f64_to_json;
use sync_core::{Row, SqlValue, SyncDb};

use crate::db::RusqliteDb;
use crate::engine::{self, EngineContext};
use crate::fault::{FaultKind, FaultPoint, FaultRegistry};
use crate::namespace::Manager;
use crate::obs::{self, Counters};
use crate::wake::WakeRegistry;

// ---- AppState ------------------------------------------------------------

pub struct AppState {
    pub manager: Arc<Manager>,
    pub wake: Arc<WakeRegistry>,
    pub ctx: Arc<EngineContext>,
    pub authenticate: crate::AuthFn,
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
    ) -> Self {
        Self {
            manager,
            wake,
            ctx,
            authenticate,
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

fn boot_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{:x}-{:x}", nanos, std::process::id())
}

// ---- router --------------------------------------------------------------

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/admin/health", get(health))
        .route("/{ns}/pull", post(pull))
        .route("/{ns}/push", post(push))
        .route("/{ns}/wake", get(wake_ws))
        .route("/{ns}/admin/sql", post(admin_sql))
        .route("/{ns}/admin/status", get(admin_status))
        .route("/{ns}/admin/invalidate", post(admin_invalidate))
        .route("/{ns}/admin/reset-cursor", post(admin_reset_cursor))
        .route("/{ns}/admin/drop-next-push-response", post(admin_drop_push))
        .route("/{ns}/admin/fault", post(admin_fault))
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
    let Ok(value) = serde_json::from_slice::<Value>(&body) else {
        return json_status(400, json!({ "error": "invalid json" }));
    };
    let namespace = match state.manager.get(&ns) {
        Ok(n) => n,
        Err(e) => return json_status(400, json!({ "error": e })),
    };
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
    let result = namespace
        .run(move |conn| {
            let mut db = RusqliteDb::new(conn);
            db.query(&query, &[])
        })
        .await;
    match result {
        Ok(rows) => {
            let rows: Vec<Value> = rows.iter().map(row_to_json).collect();
            json_status(200, json!({ "rows": rows }))
        }
        Err(e) => json_status(500, json!({ "error": e.0 })),
    }
}

async fn admin_status(State(state): State<Arc<AppState>>, Path(_ns): Path<String>) -> Response {
    // diagnostics surface: process-wide aggregate counters alongside boot info.
    // this host binds 127.0.0.1 only and admin/* is the operator/harness surface,
    // so it is not reachable by sync clients.
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
// to disarm. operator/harness-only: this host is 127.0.0.1-bound and admin/* is not
// reachable by sync clients.
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
