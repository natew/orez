// sync-native: the native axum host for the sync engine (plan M2).
//
// usage: sync-native --data-dir <dir> --port <port>
//                    [--retain-changes <n>] [--visible]
//
// routes (namespace = one sqlite file under --data-dir):
//   POST /<ns>/pull, /<ns>/push        the http-pull dialect (engine)
//   GET  /<ns>/wake                     wake WebSocket ("pull now" only)
//   POST /<ns>/admin/sql                oracle reads + upstream writes
//   GET  /<ns>/admin/status            { ok, bootID, pid }
//   POST /<ns>/admin/invalidate         epoch bump
//   POST /<ns>/admin/reset-cursor       restored/behind-server fault
//   POST /<ns>/admin/drop-next-push-response  lost-response fault
//   GET  /admin/health                  process readiness (no namespace)

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::{Value, json};
use tokio::net::TcpListener;

use sync_core::value::f64_to_json;
use sync_core::{Row, SqlValue, SyncDb};

use sync_native::db::RusqliteDb;
use sync_native::engine::{self, EngineContext};
use sync_native::namespace::{InitFn, Manager};
use sync_native::wake::WakeRegistry;

struct AppState {
    manager: Arc<Manager>,
    wake: Arc<WakeRegistry>,
    ctx: Arc<EngineContext>,
    boot_id: String,
    // namespaces with a one-shot "drop the next push response" fault armed
    drop_push: Mutex<HashSet<String>>,
}

impl AppState {
    fn arm_drop_push(&self, ns: &str) {
        self.drop_push.lock().unwrap().insert(ns.to_string());
    }
    fn take_drop_push(&self, ns: &str) -> bool {
        self.drop_push.lock().unwrap().remove(ns)
    }
}

struct Config {
    data_dir: PathBuf,
    port: u16,
    retain_changes: i64,
    visible: bool,
    query_aware: bool,
}

fn parse_args() -> Config {
    let mut data_dir: Option<PathBuf> = None;
    let mut port: Option<u16> = None;
    let mut retain_changes: i64 = 4096;
    let mut visible = false;
    let mut query_aware = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-dir" => data_dir = Some(PathBuf::from(expect_value(&mut args, "--data-dir"))),
            "--port" => {
                port = Some(
                    expect_value(&mut args, "--port")
                        .parse()
                        .expect("--port must be a u16"),
                )
            }
            "--retain-changes" => {
                retain_changes = expect_value(&mut args, "--retain-changes")
                    .parse()
                    .expect("--retain-changes must be an integer")
            }
            "--visible" => visible = true,
            "--query-aware" => query_aware = true,
            other => panic!("unknown argument {other}"),
        }
    }

    Config {
        data_dir: data_dir.expect("--data-dir is required"),
        port: port.expect("--port is required"),
        retain_changes,
        visible,
        query_aware,
    }
}

fn expect_value(args: &mut impl Iterator<Item = String>, flag: &str) -> String {
    args.next()
        .unwrap_or_else(|| panic!("{flag} needs a value"))
}

fn boot_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{:x}-{:x}", nanos, std::process::id())
}

#[tokio::main]
async fn main() {
    let config = parse_args();
    std::fs::create_dir_all(&config.data_dir).expect("failed to create data dir");

    let ctx = Arc::new(EngineContext::new(
        config.retain_changes,
        config.visible,
        config.query_aware,
    ));
    let init_ctx = ctx.clone();
    let init: InitFn = Arc::new(move |db: &mut dyn SyncDb| engine::init_namespace(db, &init_ctx));
    let manager = Arc::new(Manager::new(config.data_dir, init));

    let state = Arc::new(AppState {
        manager,
        wake: WakeRegistry::new(),
        ctx,
        boot_id: boot_id(),
        drop_push: Mutex::new(HashSet::new()),
    });

    let app = Router::new()
        .route("/admin/health", get(health))
        .route("/{ns}/pull", post(pull))
        .route("/{ns}/push", post(push))
        .route("/{ns}/wake", get(wake_ws))
        .route("/{ns}/admin/sql", post(admin_sql))
        .route("/{ns}/admin/status", get(admin_status))
        .route("/{ns}/admin/invalidate", post(admin_invalidate))
        .route("/{ns}/admin/reset-cursor", post(admin_reset_cursor))
        .route("/{ns}/admin/drop-next-push-response", post(admin_drop_push))
        .with_state(state);

    let listener = TcpListener::bind(("127.0.0.1", config.port))
        .await
        .expect("failed to bind");
    let addr = listener.local_addr().expect("no local addr");
    println!("sync-native listening on {addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn auth(headers: &HeaderMap) -> Option<String> {
    let value = headers.get("authorization")?.to_str().ok()?;
    value.strip_prefix("Bearer token-").map(str::to_string)
}

fn json_status(status: u16, body: Value) -> Response {
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        axum::Json(body),
    )
        .into_response()
}

async fn health() -> Response {
    json_status(200, json!({ "ok": true, "pid": std::process::id() }))
}

async fn pull(
    State(state): State<Arc<AppState>>,
    Path(ns): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(user_id) = auth(&headers) else {
        return json_status(401, json!({ "error": "missing auth" }));
    };
    let Ok(value) = serde_json::from_slice::<Value>(&body) else {
        return json_status(400, json!({ "error": "invalid json" }));
    };
    let namespace = match state.manager.get(&ns) {
        Ok(n) => n,
        Err(e) => return json_status(400, json!({ "error": e })),
    };
    let ctx = state.ctx.clone();
    let result = namespace
        .run(move |conn| engine::pull(conn, &ctx, &value, &user_id))
        .await;
    match result {
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
    let Some(user_id) = auth(&headers) else {
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
    let ctx = state.ctx.clone();
    let result = namespace
        .run(move |conn| engine::push(conn, &ctx, &value, &user_id))
        .await;
    match result {
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
    json_status(
        200,
        json!({ "ok": true, "bootID": state.boot_id, "pid": std::process::id() }),
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
