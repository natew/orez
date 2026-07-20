// integration tests for the sync-native library API.
//
// verifies that:
// 1. SyncNativeHost can be constructed with a custom config
// 2. the health endpoint responds
// 3. pull/push work with a custom schema
// 4. explicit trusted-router construction works for in-process embedding

use std::sync::{Arc, Mutex};

use axum::extract::connect_info::ConnectInfo;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use tower::ServiceExt;

use sync_core::error::MutateError;
use sync_core::schema::{TableSpec, Tables};
use sync_core::value::ZeroColumnType;
use sync_core::{SqlValue, SyncDb};

use sync_native::AuthClaims;
use sync_native::AuthError;
use sync_native::AuthFn;
use sync_native::NamedQuery;
use sync_native::QueryResolution;
use sync_native::ResolveQueriesFn;
use sync_native::ResolvedQueries;
use sync_native::SyncNativeConfig;
use sync_native::SyncNativeHost;
use sync_native::SyncNativeSecurity;
use sync_native::engine::{InitFn, MutateFn};

// ---- helpers -----------------------------------------------------------

const ADMIN_TOKEN: &str = "sync-native-library-test-admin-token-0000000000000000000000000000";
const ALLOWED_ORIGIN: &str = "http://localhost:7878";

fn custom_tables() -> Tables {
    let mut tables = Tables::new();
    tables.push(
        "item",
        TableSpec {
            columns: vec![
                ("id".to_string(), ZeroColumnType::String),
                ("label".to_string(), ZeroColumnType::String),
            ],
            primary_key: vec!["id".to_string()],
            encrypted_columns: Default::default(),
            encrypted_physical_columns: Default::default(),
        },
    );
    tables
}

fn custom_init() -> InitFn {
    Arc::new(|db: &mut dyn SyncDb| {
        db.exec(
            "CREATE TABLE item (id text PRIMARY KEY, label text NOT NULL)",
            &[],
        )
        .map_err(|e| e.0)?;
        // seed one row so fresh pulls are not empty
        db.exec("INSERT INTO item (id, label) VALUES ('i1', 'hello')", &[])
            .map_err(|e| e.0)?;
        Ok(())
    })
}

fn custom_mutate() -> MutateFn {
    Arc::new(
        |db: &mut dyn SyncDb, name: &str, args: &serde_json::Value, _user_id: &str| {
            match name {
                "item.create" => {
                    let id = args
                        .get("id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| MutateError::Other("missing id".into()))?;
                    let label = args
                        .get("label")
                        .and_then(Value::as_str)
                        .ok_or_else(|| MutateError::Other("missing label".into()))?;
                    // app-level validation: empty label is a client error
                    if label.is_empty() {
                        return Err(MutateError::app("label must not be empty"));
                    }
                    db.exec(
                        "INSERT INTO item (id, label) VALUES (?, ?)",
                        &[SqlValue::Text(id.into()), SqlValue::Text(label.into())],
                    )
                    .map_err(|e| MutateError::Other(e.0))?;
                    Ok(())
                }
                "item.delete" => {
                    let id = args
                        .get("id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| MutateError::Other("missing id".into()))?;
                    db.exec(
                        "DELETE FROM item WHERE id = ?",
                        &[SqlValue::Text(id.into())],
                    )
                    .map_err(|e| MutateError::Other(e.0))?;
                    Ok(())
                }
                other => Err(MutateError::Other(format!("unknown mutator: {other}"))),
            }
        },
    )
}

fn custom_auth() -> AuthFn {
    Arc::new(|headers, _namespace| {
        Box::pin(async move {
            let token = headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
                .filter(|token| !token.is_empty())
                .ok_or_else(|| AuthError::unauthorized("missing auth"))?;
            Ok(AuthClaims::new(token))
        })
    })
}

fn custom_config() -> SyncNativeConfig {
    custom_config_with_lease(sync_native::DEFAULT_ADMIN_TX_LEASE)
}

fn custom_config_with_lease(admin_tx_lease: std::time::Duration) -> SyncNativeConfig {
    SyncNativeConfig {
        tables: custom_tables(),
        initialize: custom_init(),
        mutate: custom_mutate(),
        visible: None,
        authenticate: custom_auth(),
        authorize_wake: Arc::new(|_, _| Box::pin(async { Ok(()) })),
        retain_changes: 4096,
        max_change_rows: sync_core::Caps::default().max_change_rows,
        visibility_enabled: false,
        query_aware: false,
        query_resolution: None,
        admin_tx_lease,
        // tests drive retention directly (namespace.rs unit tests); keep it off
        // here so no background sweep races the fixtures.
        retention: sync_native::retain::RetentionPolicy::disabled(),
    }
}

fn item_query(label: Option<&str>) -> Value {
    match label {
        Some(label) => json!({
            "table": "item",
            "where": {
                "type": "simple",
                "op": "=",
                "left": { "type": "column", "name": "label" },
                "right": { "type": "literal", "value": label },
            },
        }),
        None => json!({ "table": "item" }),
    }
}

fn resolved(asts: Vec<Value>, transform_version: u64) -> ResolvedQueries {
    ResolvedQueries {
        asts,
        transform_version,
    }
}

fn test_host(config: SyncNativeConfig, data_dir: std::path::PathBuf) -> SyncNativeHost {
    SyncNativeHost::new_with_security(
        config,
        data_dir,
        SyncNativeSecurity::with_admin_token(ADMIN_TOKEN).allow_origin(ALLOWED_ORIGIN),
    )
}

// send a request against a cloned router (oneshot takes ownership).
async fn send(router: &axum::Router, req: Request<axum::body::Body>) -> (StatusCode, Value) {
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let limit = 262_144;
    let body_bytes = axum::body::to_bytes(resp.into_body(), limit).await.unwrap();
    let body_str = String::from_utf8_lossy(&body_bytes);
    if !status.is_success() {
        eprintln!("HTTP {status}: {body_str}");
    }
    let v: Value = serde_json::from_slice(&body_bytes).unwrap_or(Value::Null);
    (status, v)
}

fn pull_req(ns: &str, body: &Value, token: &str) -> Request<axum::body::Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/{ns}/pull"))
        .header("authorization", token)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap()
}

fn push_req(ns: &str, body: &Value, token: &str) -> Request<axum::body::Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/{ns}/push"))
        .header("authorization", token)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap()
}

fn admin_sql_req(
    ns: &str,
    query: &str,
    transaction_id: Option<&str>,
    transaction_step: Option<&str>,
) -> Request<axum::body::Body> {
    admin_sql_params_req(ns, query, None, transaction_id, transaction_step)
}

fn admin_sql_params_req(
    ns: &str,
    query: &str,
    params: Option<Value>,
    transaction_id: Option<&str>,
    transaction_step: Option<&str>,
) -> Request<axum::body::Body> {
    let mut body = json!({ "query": query });
    if let Some(params) = params {
        body["params"] = params;
    }
    if let Some(transaction_id) = transaction_id {
        body["transactionId"] = json!(transaction_id);
    }
    if let Some(transaction_step) = transaction_step {
        body["transactionStep"] = json!(transaction_step);
    }
    Request::builder()
        .method("POST")
        .uri(format!("/{ns}/admin/sql"))
        .header("content-type", "application/json")
        .header("x-admin-key", ADMIN_TOKEN)
        .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

fn admin_settle_push_req(
    ns: &str,
    push: &Value,
    response: &Value,
    user_id: &str,
) -> Request<axum::body::Body> {
    Request::builder()
        .method("POST")
        .uri(format!("/{ns}/admin/settle-push"))
        .header("content-type", "application/json")
        .header("x-admin-key", ADMIN_TOKEN)
        .body(axum::body::Body::from(
            serde_json::to_vec(&json!({
                "push": push,
                "response": response,
                "userID": user_id,
            }))
            .unwrap(),
        ))
        .unwrap()
}

fn patch_count(resp: &Value, table: &str, op: &str) -> usize {
    resp["rowsPatch"]
        .as_array()
        .map(|patch| {
            patch
                .iter()
                .filter(|entry| {
                    entry["op"].as_str() == Some(op) && entry["tableName"].as_str() == Some(table)
                })
                .count()
        })
        .unwrap_or(0)
}

fn pull_body(cookie: Option<&Value>) -> Value {
    json!({
        "clientID": "test-client",
        "clientGroupID": "test-group",
        "cookie": cookie.unwrap_or(&Value::Null),
    })
}

fn push_body(mutations: Value) -> Value {
    json!({
        "clientGroupID": "test-group",
        "mutations": mutations,
        "pushVersion": 1,
    })
}

fn mutation(id: u64, name: &str, args: Value, client_id: &str) -> Value {
    json!({
        "id": id,
        "name": name,
        "args": args,
        "clientID": client_id,
        "type": "custom",
    })
}

// ---- tests -------------------------------------------------------------

#[tokio::test]
async fn named_queries_are_batched_and_resolved_before_sqlite() {
    let seen = Arc::new(Mutex::new(Vec::<NamedQuery>::new()));
    let captured = seen.clone();
    let resolver: ResolveQueriesFn = Arc::new(move |queries, headers, claims, _namespace| {
        assert_eq!(
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer user-1")
        );
        assert_eq!(claims.user_id(), "user-1");
        *captured.lock().unwrap() = queries;
        Box::pin(async {
            Ok(resolved(
                vec![item_query(None), item_query(Some("missing"))],
                7,
            ))
        })
    });

    let mut config = custom_config();
    config.query_aware = true;
    config.query_resolution = Some(QueryResolution { resolve: resolver });
    let tmp = tempfile::tempdir().unwrap();
    let router = test_host(config, tmp.path().to_path_buf()).into_router_trusted();

    let body = json!({
        "clientID": "query-client",
        "clientGroupID": "query-group",
        "cookie": null,
        "queries": {
            "version": 1,
            "patch": [
                {
                    "op": "put",
                    "hash": "q-all",
                    "name": "item.all",
                    "args": [],
                    "ast": { "table": "client-forged" },
                    "transformVersion": 999,
                },
                {
                    "op": "put",
                    "hash": "q-missing",
                    "name": "item.byLabel",
                    "args": [{ "label": "missing" }],
                },
            ],
        },
    });
    let (status, response) = send(&router, pull_req("query-ns", &body, "Bearer user-1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(patch_count(&response, "item", "put"), 1);
    assert_eq!(
        response["gotQueries"],
        json!({
            "version": 1,
            "patch": [
                { "op": "put", "hash": "q-all" },
                { "op": "put", "hash": "q-missing" },
            ],
        })
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            NamedQuery {
                name: "item.all".into(),
                args: vec![],
            },
            NamedQuery {
                name: "item.byLabel".into(),
                args: vec![json!({ "label": "missing" })],
            },
        ]
    );

    let (status, stored) = send(
        &router,
        admin_sql_req(
            "query-ns",
            "SELECT hash, ast, transformVersion FROM _zsync_queries ORDER BY hash",
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stored["rows"][0]["hash"], json!("q-all"));
    assert_eq!(
        stored["rows"][0]["ast"],
        json!(item_query(None).to_string())
    );
    assert_eq!(stored["rows"][0]["transformVersion"], json!(7));
    assert_eq!(stored["rows"][1]["hash"], json!("q-missing"));
    assert_eq!(
        stored["rows"][1]["ast"],
        json!(item_query(Some("missing")).to_string())
    );
    assert_eq!(stored["rows"][1]["transformVersion"], json!(7));
}

#[tokio::test]
async fn configured_query_resolver_rejects_client_authored_ast() {
    let calls = Arc::new(Mutex::new(0));
    let captured = calls.clone();
    let resolver: ResolveQueriesFn = Arc::new(move |_queries, _headers, _user_id, _namespace| {
        *captured.lock().unwrap() += 1;
        Box::pin(async { Ok(resolved(vec![item_query(None)], 0)) })
    });
    let mut config = custom_config();
    config.query_aware = true;
    config.query_resolution = Some(QueryResolution { resolve: resolver });
    let tmp = tempfile::tempdir().unwrap();
    let router = test_host(config, tmp.path().to_path_buf()).into_router_trusted();

    let (status, response) = send(&router, pull_req("query-ns", &json!([]), "Bearer user-1")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(response["error"], json!("invalid pull body"));

    for operation in [
        json!({ "op": "put", "hash": "forged", "ast": { "table": "item" } }),
        json!({ "op": "put", "hash": "missing-args", "name": "item.all" }),
    ] {
        let body = json!({
            "clientID": "query-client",
            "clientGroupID": "query-group",
            "cookie": null,
            "queries": { "version": 1, "patch": [operation] },
        });
        let (status, _) = send(&router, pull_req("query-ns", &body, "Bearer user-1")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
    assert_eq!(*calls.lock().unwrap(), 0);
}

#[tokio::test]
async fn query_resolution_preserves_pull_arrival_order() {
    let started = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let signal_started = started.clone();
    let wait_for_resume = resume.clone();
    let resolver: ResolveQueriesFn = Arc::new(move |queries, _headers, _user_id, _namespace| {
        let signal_started = signal_started.clone();
        let wait_for_resume = wait_for_resume.clone();
        Box::pin(async move {
            if queries.is_empty() {
                return Ok(resolved(Vec::new(), 1));
            }
            signal_started.notify_one();
            wait_for_resume.notified().await;
            Ok(resolved(vec![item_query(None)], 1))
        })
    });
    let mut config = custom_config();
    config.query_aware = true;
    config.query_resolution = Some(QueryResolution { resolve: resolver });
    let tmp = tempfile::tempdir().unwrap();
    let router = test_host(config, tmp.path().to_path_buf()).into_router_trusted();

    let first_router = router.clone();
    let first = tokio::spawn(async move {
        let body = json!({
            "clientID": "query-client",
            "clientGroupID": "query-group",
            "cookie": null,
            "queries": {
                "version": 1,
                "patch": [{
                    "op": "put",
                    "hash": "q-all",
                    "name": "item.all",
                    "args": [],
                }],
            },
        });
        send(
            &first_router,
            pull_req("ordered-ns", &body, "Bearer user-1"),
        )
        .await
    });
    started.notified().await;

    let second_router = router.clone();
    let second = tokio::spawn(async move {
        let body = json!({
            "clientID": "query-client",
            "clientGroupID": "query-group",
            "cookie": null,
            "queries": {
                "version": 2,
                "patch": [{ "op": "clear" }],
            },
        });
        send(
            &second_router,
            pull_req("ordered-ns", &body, "Bearer user-1"),
        )
        .await
    });
    tokio::task::yield_now().await;
    resume.notify_one();

    assert_eq!(first.await.unwrap().0, StatusCode::OK);
    assert_eq!(second.await.unwrap().0, StatusCode::OK);
    let (status, stored) = send(
        &router,
        admin_sql_req(
            "ordered-ns",
            "SELECT (SELECT count(*) FROM _zsync_desires) AS desires, \
                    (SELECT version FROM _zsync_query_ack WHERE clientGroupID = 'query-group' \
                     AND clientID = 'query-client') AS version",
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stored["rows"][0]["desires"], json!(0));
    assert_eq!(stored["rows"][0]["version"], json!(2));
}

#[tokio::test]
async fn slow_query_transform_does_not_block_another_client_group() {
    let blocked_started = Arc::new(tokio::sync::Notify::new());
    let blocked_resume = Arc::new(tokio::sync::Notify::new());
    let independent_started = Arc::new(tokio::sync::Notify::new());
    let resolver: ResolveQueriesFn = Arc::new({
        let blocked_started = blocked_started.clone();
        let blocked_resume = blocked_resume.clone();
        let independent_started = independent_started.clone();
        move |queries, _headers, _claims, _namespace| {
            let blocked_started = blocked_started.clone();
            let blocked_resume = blocked_resume.clone();
            let independent_started = independent_started.clone();
            Box::pin(async move {
                if queries[0].name == "item.blocked" {
                    blocked_started.notify_one();
                    blocked_resume.notified().await;
                } else {
                    independent_started.notify_one();
                }
                Ok(resolved(vec![item_query(None)], 1))
            })
        }
    });
    let mut config = custom_config();
    config.query_aware = true;
    config.query_resolution = Some(QueryResolution { resolve: resolver });
    let tmp = tempfile::tempdir().unwrap();
    let router = test_host(config, tmp.path().to_path_buf()).into_router_trusted();

    let request = |client: &str, group: &str, name: &str, hash: &str| {
        json!({
            "clientID": client,
            "clientGroupID": group,
            "cookie": null,
            "queries": {
                "version": 1,
                "patch": [{
                    "op": "put",
                    "hash": hash,
                    "name": name,
                    "args": [],
                }],
            },
        })
    };
    let first_router = router.clone();
    let first_body = request("client-one", "group-one", "item.blocked", "q-one");
    let first = tokio::spawn(async move {
        send(
            &first_router,
            pull_req("parallel-group-ns", &first_body, "Bearer user-1"),
        )
        .await
    });
    blocked_started.notified().await;

    let second_router = router.clone();
    let second_body = request("client-two", "group-two", "item.all", "q-two");
    let second = tokio::spawn(async move {
        send(
            &second_router,
            pull_req("parallel-group-ns", &second_body, "Bearer user-1"),
        )
        .await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        independent_started.notified(),
    )
    .await
    .expect("another client group should resolve independently");
    blocked_resume.notify_one();

    assert_eq!(first.await.unwrap().0, StatusCode::OK);
    assert_eq!(second.await.unwrap().0, StatusCode::OK);
}

#[tokio::test]
async fn transform_version_bump_revokes_stored_query_without_a_client_patch() {
    let tmp = tempfile::tempdir().unwrap();
    let resolver: ResolveQueriesFn = Arc::new(|queries, _headers, _claims, _namespace| {
        Box::pin(async move {
            Ok(resolved(
                queries.iter().map(|_| item_query(None)).collect(),
                1,
            ))
        })
    });
    let mut first_config = custom_config();
    first_config.query_aware = true;
    first_config.query_resolution = Some(QueryResolution { resolve: resolver });
    let first_router = test_host(first_config, tmp.path().to_path_buf()).into_router_trusted();
    let body = json!({
        "clientID": "query-client",
        "clientGroupID": "query-group",
        "cookie": null,
        "queries": {
            "version": 1,
            "patch": [{
                "op": "put",
                "hash": "q-all",
                "name": "item.all",
                "args": [],
            }],
        },
    });
    let (status, first_response) = send(
        &first_router,
        pull_req("versioned-ns", &body, "Bearer user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(patch_count(&first_response, "item", "put"), 1);
    assert_eq!(
        first_response["gotQueries"],
        json!({ "version": 1, "patch": [{ "op": "put", "hash": "q-all" }] })
    );
    let cookie = first_response["cookie"].clone();
    drop(first_router);

    let resolver: ResolveQueriesFn = Arc::new(|queries, _headers, _claims, _namespace| {
        Box::pin(async move {
            Ok(resolved(
                queries
                    .iter()
                    .map(|_| item_query(Some("missing")))
                    .collect(),
                2,
            ))
        })
    });
    let mut second_config = custom_config();
    second_config.initialize = Arc::new(|db| {
        db.exec(
            "CREATE TABLE IF NOT EXISTS item (id text PRIMARY KEY, label text NOT NULL)",
            &[],
        )
        .map_err(|error| error.0)?;
        db.exec(
            "INSERT OR IGNORE INTO item (id, label) VALUES ('i1', 'hello')",
            &[],
        )
        .map_err(|error| error.0)?;
        Ok(())
    });
    second_config.query_aware = true;
    second_config.query_resolution = Some(QueryResolution { resolve: resolver });
    let second_router = test_host(second_config, tmp.path().to_path_buf()).into_router_trusted();
    let body = json!({
        "clientID": "query-client",
        "clientGroupID": "query-group",
        "cookie": cookie,
    });
    let (status, response) = send(
        &second_router,
        pull_req("versioned-ns", &body, "Bearer user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["rowsPatch"][0], json!({ "op": "clear" }));
    assert_eq!(patch_count(&response, "item", "put"), 0);
    assert_eq!(
        response["gotQueries"],
        json!({ "version": 1, "patch": [{ "op": "del", "hash": "q-all" }] })
    );

    let body = json!({
        "clientID": "query-client",
        "clientGroupID": "query-group",
        "cookie": response["cookie"],
        "queries": {
            "version": 2,
            "patch": [{
                "op": "put",
                "hash": "q-all",
                "name": "item.all",
                "args": [],
            }],
        },
    });
    let (status, resent) = send(
        &second_router,
        pull_req("versioned-ns", &body, "Bearer user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(patch_count(&resent, "item", "put"), 0);
    assert_eq!(
        resent["gotQueries"],
        json!({ "version": 2, "patch": [{ "op": "put", "hash": "q-all" }] })
    );
}

#[tokio::test]
async fn unchanged_pulls_observe_query_transform_version_changes() {
    let resolver_state = Arc::new(Mutex::new((1_u64, 0_usize)));
    let captured = resolver_state.clone();
    let resolver: ResolveQueriesFn = Arc::new(move |queries, _headers, _claims, _namespace| {
        let mut state = captured.lock().unwrap();
        state.1 += 1;
        let version = state.0;
        Box::pin(async move {
            Ok(resolved(
                queries.iter().map(|_| item_query(None)).collect(),
                version,
            ))
        })
    });
    let mut config = custom_config();
    config.query_aware = true;
    config.query_resolution = Some(QueryResolution { resolve: resolver });
    let tmp = tempfile::tempdir().unwrap();
    let router = test_host(config, tmp.path().to_path_buf()).into_router_trusted();

    let initial = json!({
        "clientID": "query-client",
        "clientGroupID": "query-group",
        "cookie": null,
        "queries": {
            "version": 1,
            "patch": [{
                "op": "put",
                "hash": "q-all",
                "name": "item.all",
                "args": [],
            }],
        },
    });
    let (status, initial_response) = send(
        &router,
        pull_req("live-version-ns", &initial, "Bearer user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(*resolver_state.lock().unwrap(), (1, 1));

    let unchanged = json!({
        "clientID": "query-client",
        "clientGroupID": "query-group",
        "cookie": initial_response["cookie"],
    });
    let (status, unchanged_response) = send(
        &router,
        pull_req("live-version-ns", &unchanged, "Bearer user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(*resolver_state.lock().unwrap(), (1, 2));

    resolver_state.lock().unwrap().0 = 2;
    let after_version_change = json!({
        "clientID": "query-client",
        "clientGroupID": "query-group",
        "cookie": unchanged_response["cookie"],
    });
    let (status, response) = send(
        &router,
        pull_req("live-version-ns", &after_version_change, "Bearer user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(*resolver_state.lock().unwrap(), (2, 3));
    assert_eq!(
        response["gotQueries"],
        json!({ "version": 1, "patch": [{ "op": "del", "hash": "q-all" }] })
    );
}

#[tokio::test]
async fn transform_version_bump_invalidates_each_client_when_it_checks_in() {
    let tmp = tempfile::tempdir().unwrap();
    let resolver: ResolveQueriesFn = Arc::new(|queries, _headers, _user_id, _namespace| {
        Box::pin(async move {
            Ok(resolved(
                queries.iter().map(|_| item_query(None)).collect(),
                1,
            ))
        })
    });
    let mut first_config = custom_config();
    first_config.query_aware = true;
    first_config.query_resolution = Some(QueryResolution { resolve: resolver });
    let first_router = test_host(first_config, tmp.path().to_path_buf()).into_router_trusted();

    let mut cookies = Vec::new();
    for (client, hash) in [("client-one", "q-one"), ("client-two", "q-two")] {
        let body = json!({
            "clientID": client,
            "clientGroupID": "query-group",
            "cookie": null,
            "queries": {
                "version": 1,
                "patch": [{
                    "op": "put",
                    "hash": hash,
                    "name": "item.all",
                    "args": [],
                }],
            },
        });
        let (status, response) = send(
            &first_router,
            pull_req("multi-client-versioned-ns", &body, "Bearer user-1"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        cookies.push(response["cookie"].clone());
    }
    drop(first_router);

    let resolver: ResolveQueriesFn = Arc::new(|queries, _headers, _user_id, _namespace| {
        Box::pin(async move {
            Ok(resolved(
                queries
                    .iter()
                    .map(|_| item_query(Some("missing")))
                    .collect(),
                2,
            ))
        })
    });
    let mut second_config = custom_config();
    second_config.initialize = Arc::new(|db| {
        db.exec(
            "CREATE TABLE IF NOT EXISTS item (id text PRIMARY KEY, label text NOT NULL)",
            &[],
        )
        .map_err(|error| error.0)?;
        db.exec(
            "INSERT OR IGNORE INTO item (id, label) VALUES ('i1', 'hello')",
            &[],
        )
        .map_err(|error| error.0)?;
        Ok(())
    });
    second_config.query_aware = true;
    second_config.query_resolution = Some(QueryResolution { resolve: resolver });
    let second_router = test_host(second_config, tmp.path().to_path_buf()).into_router_trusted();

    let first_body = json!({
        "clientID": "client-one",
        "clientGroupID": "query-group",
        "cookie": cookies[0],
    });
    let (status, first_response) = send(
        &second_router,
        pull_req("multi-client-versioned-ns", &first_body, "Bearer user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        first_response["gotQueries"],
        json!({ "version": 1, "patch": [{ "op": "del", "hash": "q-one" }] })
    );

    let (status, stored) = send(
        &second_router,
        admin_sql_req(
            "multi-client-versioned-ns",
            "SELECT clientID, hash FROM _zsync_desires ORDER BY clientID",
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        stored["rows"],
        json!([{ "clientID": "client-two", "hash": "q-two" }])
    );

    let second_body = json!({
        "clientID": "client-two",
        "clientGroupID": "query-group",
        "cookie": cookies[1],
    });
    let (status, second_response) = send(
        &second_router,
        pull_req("multi-client-versioned-ns", &second_body, "Bearer user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        second_response["gotQueries"],
        json!({ "version": 1, "patch": [{ "op": "del", "hash": "q-two" }] })
    );
}

#[tokio::test]
async fn health_endpoint_responds() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();

    let req = Request::builder()
        .uri("/admin/health")
        .header("x-admin-key", ADMIN_TOKEN)
        .body(axum::body::Body::empty())
        .unwrap();
    let (_status, v) = send(&router, req).await;
    assert_eq!(v["ok"], json!(true));
    assert!(v["pid"].is_number());
}

#[tokio::test]
async fn admin_lists_persisted_namespaces_in_lexical_order() {
    let tmp = tempfile::tempdir().unwrap();
    let router = test_host(custom_config(), tmp.path().to_path_buf()).into_router_trusted();

    for namespace in ["project-z", "control", "project-a"] {
        let (status, _) = send(&router, admin_sql_req(namespace, "SELECT 1", None, None)).await;
        assert_eq!(status, StatusCode::OK);
    }

    let request = Request::builder()
        .uri("/admin/namespaces")
        .header("x-admin-key", ADMIN_TOKEN)
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, response) = send(&router, request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        response,
        json!({ "namespaces": ["control", "project-a", "project-z"] })
    );
}

#[tokio::test]
async fn auth_rejection_cannot_create_a_namespace_replica() {
    let tmp = tempfile::tempdir().unwrap();
    let mut config = custom_config();
    config.authenticate = Arc::new(|_headers, namespace| {
        Box::pin(async move {
            if namespace == "forbidden" {
                return Err(AuthError::forbidden("namespace forbidden"));
            }
            Ok(AuthClaims::new("user-1"))
        })
    });
    let router = test_host(config, tmp.path().to_path_buf()).into_router_trusted();

    let body = json!({
        "clientID": "client",
        "clientGroupID": "group",
        "cookie": null,
    });
    let (status, _) = send(&router, pull_req("forbidden", &body, "Bearer user-1")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let request = Request::builder()
        .uri("/admin/namespaces")
        .header("x-admin-key", ADMIN_TOKEN)
        .body(axum::body::Body::empty())
        .unwrap();
    let (status, response) = send(&router, request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response, json!({ "namespaces": [] }));
}

#[tokio::test]
async fn admin_requires_a_loopback_peer_or_explicit_in_process_trust() {
    let tmp = tempfile::tempdir().unwrap();
    let router = test_host(custom_config(), tmp.path().to_path_buf()).into_router();
    let request = || {
        Request::builder()
            .uri("/admin/health")
            .header("x-admin-key", ADMIN_TOKEN)
            .body(axum::body::Body::empty())
            .unwrap()
    };

    let (status, _) = send(&router, request()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let mut remote = request();
    remote.extensions_mut().insert(ConnectInfo(
        "203.0.113.10:4000".parse::<std::net::SocketAddr>().unwrap(),
    ));
    let (status, _) = send(&router, remote).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let mut loopback = request();
    loopback.extensions_mut().insert(ConnectInfo(
        "127.0.0.1:4000".parse::<std::net::SocketAddr>().unwrap(),
    ));
    let (status, _) = send(&router, loopback).await;
    assert_eq!(status, StatusCode::OK);

    let trusted_tmp = tempfile::tempdir().unwrap();
    let trusted =
        test_host(custom_config(), trusted_tmp.path().to_path_buf()).into_router_trusted();
    let (status, _) = send(&trusted, request()).await;
    assert_eq!(status, StatusCode::OK);

    let mut trusted_remote = request();
    trusted_remote.extensions_mut().insert(ConnectInfo(
        "203.0.113.10:4000".parse::<std::net::SocketAddr>().unwrap(),
    ));
    let (status, _) = send(&trusted, trusted_remote).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[test]
fn default_hosts_generate_distinct_process_admin_tokens() {
    let first_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let first = SyncNativeHost::new(custom_config(), first_dir.path().to_path_buf());
    let second = SyncNativeHost::new(custom_config(), second_dir.path().to_path_buf());
    assert_eq!(first.admin_token().len(), 64);
    assert_eq!(second.admin_token().len(), 64);
    assert_ne!(first.admin_token(), second.admin_token());
}

#[tokio::test]
async fn admin_requires_token_and_rejects_browser_origins() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();

    let mut missing = admin_sql_req("test-ns", "SELECT 1 AS value", None, None);
    missing.headers_mut().remove("x-admin-key");
    let (status, _) = send(&router, missing).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let mut wrong = admin_sql_req("test-ns", "SELECT 1 AS value", None, None);
    wrong
        .headers_mut()
        .insert("x-admin-key", "wrong-admin-token".parse().unwrap());
    let (status, _) = send(&router, wrong).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, body) = send(
        &router,
        admin_sql_req("test-ns", "SELECT 1 AS value", None, None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"][0]["value"], json!(1));

    let push = push_body(json!([mutation(
        1,
        "item.create",
        json!([{"id": "settle-auth", "label": "settle-auth"}]),
        "client-a",
    )]));
    let response = json!({
        "pushResponse": {
            "mutations": [{
                "id": { "clientID": "client-a", "id": 1 },
                "result": {},
            }],
        },
    });
    let mut missing = admin_settle_push_req("test-ns", &push, &response, "user-1");
    missing.headers_mut().remove("x-admin-key");
    let (status, _) = send(&router, missing).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let mut browser_settle = admin_settle_push_req("test-ns", &push, &response, "user-1");
    browser_settle
        .headers_mut()
        .insert("origin", ALLOWED_ORIGIN.parse().unwrap());
    let (status, _) = send(&router, browser_settle).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let mut browser = admin_sql_req(
        "test-ns",
        "INSERT INTO item (id, label) VALUES ('browser-write', 'blocked')",
        None,
        None,
    );
    browser
        .headers_mut()
        .insert("origin", ALLOWED_ORIGIN.parse().unwrap());
    let (status, _) = send(&router, browser).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let preflight = Request::builder()
        .method("OPTIONS")
        .uri("/test-ns/admin/sql")
        .header("origin", ALLOWED_ORIGIN)
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", "content-type,x-admin-key")
        .body(axum::body::Body::empty())
        .unwrap();
    let response = router.clone().oneshot(preflight).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(
        response
            .headers()
            .get("access-control-allow-origin")
            .is_none()
    );

    let (status, body) = send(
        &router,
        admin_sql_req(
            "test-ns",
            "SELECT count(*) AS count FROM item WHERE id = 'browser-write'",
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"][0]["count"], json!(0));
}

#[tokio::test]
async fn admin_sql_binds_typed_params_and_rejects_ambiguous_values() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();

    let (status, values) = send(
        &router,
        admin_sql_params_req(
            "test-ns",
            "SELECT ? AS integer_value, ? AS real_value, ? AS text_value, ? AS null_value, hex(?) AS blob_value",
            Some(json!([
                { "kind": "integer", "value": "42" },
                { "kind": "real", "value": 1.5 },
                { "kind": "text", "value": "bound" },
                { "kind": "null" },
                { "kind": "blob", "value": [0, 255] },
            ])),
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        values["rows"],
        json!([{
            "integer_value": 42,
            "real_value": 1.5,
            "text_value": "bound",
            "null_value": null,
            "blob_value": "00FF",
        }])
    );

    let transaction_id = "tx-params";
    let (status, invalid_begin) = send(
        &router,
        admin_sql_params_req(
            "test-ns",
            "BEGIN",
            Some(json!([{ "kind": "null" }])),
            Some(transaction_id),
            Some("begin"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(invalid_begin["error"], "transaction begin forbids params");

    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "BEGIN", Some(transaction_id), Some("begin")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &router,
        admin_sql_params_req(
            "test-ns",
            "INSERT INTO item (id, label) VALUES (?, ?)",
            Some(json!([
                { "kind": "text", "value": "bound-item" },
                { "kind": "text", "value": "committed" },
            ])),
            Some(transaction_id),
            Some("query"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "COMMIT", Some(transaction_id), Some("end")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, stored) = send(
        &router,
        admin_sql_params_req(
            "test-ns",
            "SELECT label FROM item WHERE id = ?",
            Some(json!([{ "kind": "text", "value": "bound-item" }])),
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stored["rows"], json!([{ "label": "committed" }]));

    let (status, invalid) = send(
        &router,
        admin_sql_params_req("test-ns", "SELECT ?", Some(json!([1])), None, None),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        invalid["error"]
            .as_str()
            .unwrap()
            .contains("invalid params")
    );

    let (status, invalid) = send(
        &router,
        admin_sql_params_req(
            "test-ns",
            "SELECT ?",
            Some(json!([{ "kind": "integer", "value": "not-an-integer" }])),
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        invalid["error"]
            .as_str()
            .unwrap()
            .contains("invalid params")
    );
}

#[tokio::test]
async fn admin_sql_binds_params_through_transaction_rollback() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();
    let transaction_id = "tx-bound-rollback";

    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "BEGIN", Some(transaction_id), Some("begin")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &router,
        admin_sql_params_req(
            "test-ns",
            "INSERT INTO item (id, label) VALUES (?, ?)",
            Some(json!([
                { "kind": "text", "value": "bound-rolled-back" },
                { "kind": "text", "value": "pending" },
            ])),
            Some(transaction_id),
            Some("query"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, invalid_end) = send(
        &router,
        admin_sql_params_req(
            "test-ns",
            "ROLLBACK",
            Some(json!([{ "kind": "null" }])),
            Some(transaction_id),
            Some("end"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(invalid_end["error"], "transaction end forbids params");

    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "ROLLBACK", Some(transaction_id), Some("end")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, stored) = send(
        &router,
        admin_sql_params_req(
            "test-ns",
            "SELECT count(*) AS count FROM item WHERE id = ?",
            Some(json!([{ "kind": "text", "value": "bound-rolled-back" }])),
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stored["rows"][0]["count"], json!(0));
}

#[tokio::test]
async fn browser_sync_requires_an_exact_allowed_origin() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();
    let body = pull_body(None);

    let mut denied = pull_req("test-ns", &body, "Bearer user-1");
    denied
        .headers_mut()
        .insert("origin", "https://attacker.example".parse().unwrap());
    let (status, _) = send(&router, denied).await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let mut allowed = pull_req("test-ns", &body, "Bearer user-1");
    allowed
        .headers_mut()
        .insert("origin", ALLOWED_ORIGIN.parse().unwrap());
    let response = router.clone().oneshot(allowed).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .and_then(|value| value.to_str().ok()),
        Some(ALLOWED_ORIGIN)
    );

    let preflight = Request::builder()
        .method("OPTIONS")
        .uri("/test-ns/pull")
        .header("origin", ALLOWED_ORIGIN)
        .header("access-control-request-method", "POST")
        .header(
            "access-control-request-headers",
            "authorization,content-type",
        )
        .body(axum::body::Body::empty())
        .unwrap();
    let response = router.clone().oneshot(preflight).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .and_then(|value| value.to_str().ok()),
        Some(ALLOWED_ORIGIN)
    );
}

#[tokio::test]
async fn fresh_pull_returns_seed_data() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();

    let body = pull_body(None);
    let req = pull_req("test-ns", &body, "Bearer user-1");
    let (status, v) = send(&router, req).await;
    assert_eq!(status, StatusCode::OK);

    // first pull is a snapshot with a clear op + puts
    let patch = v["rowsPatch"].as_array().unwrap();
    let has_clear = patch.iter().any(|op| op["op"] == "clear");
    assert!(has_clear, "fresh pull should have a clear op");

    let item_puts = patch_count(&v, "item", "put");
    assert!(item_puts > 0, "seed row should be in the snapshot");

    // cookie should be present and non-null
    assert!(v["cookie"].is_number() || v["cookie"].is_string());
}

#[tokio::test]
async fn push_applies_mutation_and_pull_sees_it() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();

    // push: create a new item
    let push_body = push_body(json!([mutation(
        1,
        "item.create",
        json!([{"id": "i2", "label": "world"}]),
        "c1"
    )]));
    let req = push_req("test-ns", &push_body, "Bearer user-1");
    let (status, push_resp) = send(&router, req).await;
    assert_eq!(status, StatusCode::OK);

    let mutations = push_resp["pushResponse"]["mutations"].as_array().unwrap();
    assert_eq!(mutations.len(), 1);
    assert!(
        mutations[0]["result"]["error"].is_null(),
        "expected successful mutation"
    );

    // pull: should see both items
    let body = pull_body(None);
    let req = pull_req("test-ns", &body, "Bearer user-1");
    let (_status, pull_resp) = send(&router, req).await;
    let item_puts = patch_count(&pull_resp, "item", "put");
    assert_eq!(item_puts, 2, "should see both seed + pushed item");
}

#[tokio::test]
async fn delegated_push_settlement_is_pull_visible_after_its_effects_and_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();

    let client_b_pull = json!({
        "clientID": "client-b",
        "clientGroupID": "shared-group",
        "cookie": null,
    });
    let (status, before) = send(
        &router,
        pull_req("settle-ns", &client_b_pull, "Bearer user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(
        &router,
        admin_sql_req(
            "settle-ns",
            "INSERT INTO item (id, label) VALUES ('delegated', 'committed by app')",
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let push = json!({
        "clientGroupID": "shared-group",
        "mutations": [mutation(
            3,
            "item.create",
            json!([{"id": "delegated", "label": "committed by app"}]),
            "client-a",
        )],
        "pushVersion": 1,
    });
    let response = json!({
        "pushResponse": {
            "mutations": [{
                "id": { "clientID": "client-a", "id": 3 },
                "result": {
                    "error": "alreadyProcessed",
                    "details": "application lmid is already past mutation 3",
                },
            }],
        },
    });
    let (status, settled) = send(
        &router,
        admin_settle_push_req("settle-ns", &push, &response, "user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(settled, json!({ "settled": 1 }));

    let diff_pull = json!({
        "clientID": "client-b",
        "clientGroupID": "shared-group",
        "cookie": before["cookie"],
    });
    let (status, diff) = send(&router, pull_req("settle-ns", &diff_pull, "Bearer user-1")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(patch_count(&diff, "item", "put"), 1);
    assert_eq!(diff["lastMutationIDChanges"]["client-a"], json!(3));

    let snapshot_pull = json!({
        "clientID": "client-c",
        "clientGroupID": "shared-group",
        "cookie": null,
    });
    let (status, snapshot) = send(
        &router,
        pull_req("settle-ns", &snapshot_pull, "Bearer user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(snapshot["lastMutationIDChanges"]["client-a"], json!(3));

    let (status, replayed) = send(
        &router,
        admin_settle_push_req("settle-ns", &push, &response, "user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replayed, json!({ "settled": 0 }));

    let (status, stored) = send(
        &router,
        admin_sql_req(
            "settle-ns",
            "SELECT c.lastMutationID, \
                    (SELECT count(*) FROM _zsync_changes z \
                     WHERE z.tableName = '_zsync_clients' AND z.op = 'lmid') AS lmidRows, \
                    (SELECT watermark FROM _zsync_changes z \
                     WHERE z.tableName = 'item' ORDER BY watermark DESC LIMIT 1) AS effectWatermark, \
                    (SELECT watermark FROM _zsync_changes z \
                     WHERE z.tableName = '_zsync_clients' AND z.op = 'lmid' \
                     ORDER BY watermark DESC LIMIT 1) AS lmidWatermark \
             FROM _zsync_clients c \
             WHERE c.clientGroupID = 'shared-group' AND c.clientID = 'client-a'",
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stored["rows"][0]["lastMutationID"], json!(3));
    assert_eq!(stored["rows"][0]["lmidRows"], json!(1));
    assert!(
        stored["rows"][0]["effectWatermark"].as_i64().unwrap()
            < stored["rows"][0]["lmidWatermark"].as_i64().unwrap(),
        "the app effect must be journaled before its lmid"
    );
}

#[tokio::test]
async fn delegated_push_settlement_ignores_unacknowledged_cleanup_mutations() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();
    let push = json!({
        "clientGroupID": "cleanup-group",
        "mutations": [
            mutation(
                3,
                "item.create",
                json!([{"id": "delegated", "label": "committed by app"}]),
                "client-a",
            ),
            mutation(
                0,
                "_zero_cleanupResults",
                json!([{
                    "type": "single",
                    "clientGroupID": "cleanup-group",
                    "clientID": "client-a",
                    "upToMutationID": 2,
                }]),
                "client-a",
            ),
        ],
        "pushVersion": 1,
    });
    let response = json!({
        "pushResponse": {
            "mutations": [{
                "id": { "clientID": "client-a", "id": 3 },
                "result": {},
            }],
        },
    });

    let (status, settled) = send(
        &router,
        admin_settle_push_req("mixed-cleanup-ns", &push, &response, "user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(settled, json!({ "settled": 1 }));

    let (status, stored) = send(
        &router,
        admin_sql_req(
            "mixed-cleanup-ns",
            "SELECT lastMutationID FROM _zsync_clients \
             WHERE clientGroupID = 'cleanup-group' AND clientID = 'client-a'",
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stored["rows"], json!([{ "lastMutationID": 3 }]));
}

#[tokio::test]
async fn delegated_cleanup_only_settlement_needs_no_acknowledgement() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();
    let push = json!({
        "clientGroupID": "cleanup-group",
        "mutations": [mutation(
            0,
            "_zero_cleanupResults",
            json!([{
                "type": "single",
                "clientGroupID": "cleanup-group",
                "clientID": "client-a",
                "upToMutationID": 3,
            }]),
            "client-a",
        )],
        "pushVersion": 1,
    });
    let response = json!({ "pushResponse": { "mutations": [] } });

    let (status, settled) = send(
        &router,
        admin_settle_push_req("cleanup-only-ns", &push, &response, "user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(settled, json!({ "settled": 0 }));

    let (status, stored) = send(
        &router,
        admin_sql_req(
            "cleanup-only-ns",
            "SELECT (SELECT count(*) FROM _zsync_clients) AS clients, \
                    (SELECT count(*) FROM _zsync_changes WHERE op = 'lmid') AS lmidRows",
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stored["rows"][0], json!({ "clients": 0, "lmidRows": 0 }));
}

#[tokio::test]
async fn delegated_push_settlement_rejects_a_mismatched_ack_without_advancing_lmid() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();
    let push = json!({
        "clientGroupID": "shared-group",
        "mutations": [mutation(
            1,
            "item.create",
            json!([{"id": "delegated", "label": "committed by app"}]),
            "client-a",
        )],
        "pushVersion": 1,
    });
    let response = json!({
        "pushResponse": {
            "mutations": [{
                "id": { "clientID": "different-client", "id": 1 },
                "result": {},
            }],
        },
    });
    let (status, rejected) = send(
        &router,
        admin_settle_push_req("settle-validation-ns", &push, &response, "user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        rejected["error"],
        json!("push response mutation at index 0 does not match the original push")
    );

    let (status, stored) = send(
        &router,
        admin_sql_req(
            "settle-validation-ns",
            "SELECT (SELECT count(*) FROM _zsync_clients) AS clients, \
                    (SELECT count(*) FROM _zsync_changes WHERE op = 'lmid') AS lmidRows",
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stored["rows"][0]["clients"], json!(0));
    assert_eq!(stored["rows"][0]["lmidRows"], json!(0));
}

#[tokio::test]
async fn app_error_advances_lmid_no_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();

    // baseline pull to obtain a cookie
    let body = pull_body(None);
    let req = pull_req("test-ns", &body, "Bearer user-1");
    let (_status, before) = send(&router, req).await;
    let cookie = before["cookie"].clone();

    // push: create an item with an empty label — app-level validation error
    let push_body = push_body(json!([mutation(
        1,
        "item.create",
        json!([{"id": "bad", "label": ""}]),
        "c1"
    )]));
    let req = push_req("test-ns", &push_body, "Bearer user-1");
    let (status, push_resp) = send(&router, req).await;
    assert_eq!(status, StatusCode::OK);

    // app-level errors are reported in the mutation result, with LMID advanced
    let result_error = &push_resp["pushResponse"]["mutations"][0]["result"]["error"];
    assert!(
        result_error.as_str().is_some(),
        "app-level rejection should produce an error on the mutation result"
    );

    // pull with the cookie: should be unchanged since no mutation succeeded
    let body = pull_body(Some(&cookie));
    let req = pull_req("test-ns", &body, "Bearer user-1");
    let (_status, after) = send(&router, req).await;
    // unchanged or a diff with no row changes
    if after["unchanged"] != json!(true) {
        let put_count = patch_count(&after, "item", "put");
        assert_eq!(put_count, 0, "failed push should not produce row changes");
    }
}

#[tokio::test]
async fn two_namespaces_are_independent() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();

    // push to ns-a only
    let push_body = push_body(json!([mutation(
        1,
        "item.create",
        json!([{"id": "a-only", "label": "in-a"}]),
        "ca"
    )]));
    let req = push_req("ns-a", &push_body, "Bearer user-1");
    let (status, _resp) = send(&router, req).await;
    assert_eq!(status, StatusCode::OK);

    // pull ns-b: should NOT see the ns-a item
    let body = pull_body(None);
    let req = pull_req("ns-b", &body, "Bearer user-1");
    let (_status, pull_resp) = send(&router, req).await;
    let item_puts = patch_count(&pull_resp, "item", "put");
    assert_eq!(
        item_puts, 1,
        "ns-b should only have the seed item, not a-only"
    );
}

#[tokio::test]
async fn application_initialize_runs_once_for_a_persisted_namespace() {
    let tmp = tempfile::tempdir().unwrap();
    let make_config = || {
        let mut config = custom_config();
        config.initialize = Arc::new(|db| {
            db.exec(
                "CREATE TABLE item (id text PRIMARY KEY, label text NOT NULL)",
                &[],
            )
            .map_err(|error| error.0)?;
            db.exec(
                "INSERT INTO item (id, label) VALUES ('once', 'initialized once')",
                &[],
            )
            .map_err(|error| error.0)
        });
        config
    };

    let first = test_host(make_config(), tmp.path().to_path_buf()).into_router_trusted();
    let (status, _) = send(
        &first,
        pull_req("persisted", &pull_body(None), "Bearer user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    drop(first);

    let second = test_host(make_config(), tmp.path().to_path_buf()).into_router_trusted();
    let (status, _) = send(
        &second,
        pull_req("persisted", &pull_body(None), "Bearer user-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = send(
        &second,
        admin_sql_req(
            "persisted",
            "SELECT count(*) AS count FROM item WHERE id = 'once'",
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"][0]["count"], 1);
}

#[tokio::test]
async fn admin_transaction_rolls_back_and_excludes_namespace_work() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();
    let transaction_id = "tx-rollback";

    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "BEGIN", Some(transaction_id), Some("begin")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &router,
        admin_sql_req(
            "test-ns",
            "INSERT INTO item (id, label) VALUES ('rolled-back', 'pending')",
            Some(transaction_id),
            Some("query"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let blocked_router = router.clone();
    let blocked_pull = tokio::spawn(async move {
        send(
            &blocked_router,
            pull_req("test-ns", &pull_body(None), "Bearer user-1"),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    assert!(
        !blocked_pull.is_finished(),
        "pull must wait outside the active admin transaction"
    );

    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "ROLLBACK", Some(transaction_id), Some("end")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = blocked_pull.await.unwrap();
    assert_eq!(status, StatusCode::OK);

    let (status, result) = send(
        &router,
        admin_sql_req(
            "test-ns",
            "SELECT count(*) AS count FROM item WHERE id = 'rolled-back'",
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["rows"][0]["count"], json!(0));
}

#[tokio::test]
async fn admin_transaction_commits_and_excludes_namespace_work() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();
    let transaction_id = "tx-commit";

    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "BEGIN", Some(transaction_id), Some("begin")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &router,
        admin_sql_req(
            "test-ns",
            "INSERT INTO item (id, label) VALUES ('committed', 'kept')",
            Some(transaction_id),
            Some("query"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // a pull must wait outside the active admin transaction.
    let blocked_router = router.clone();
    let blocked_pull = tokio::spawn(async move {
        send(
            &blocked_router,
            pull_req("test-ns", &pull_body(None), "Bearer user-1"),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    assert!(
        !blocked_pull.is_finished(),
        "pull must wait outside the active admin transaction"
    );

    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "COMMIT", Some(transaction_id), Some("end")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = blocked_pull.await.unwrap();
    assert_eq!(status, StatusCode::OK);

    // the committed row survived.
    let (status, result) = send(
        &router,
        admin_sql_req(
            "test-ns",
            "SELECT count(*) AS count FROM item WHERE id = 'committed'",
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["rows"][0]["count"], json!(1));
}

#[tokio::test]
async fn admin_transaction_wrong_id_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();

    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "BEGIN", Some("tx-a"), Some("begin")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // a query naming a different transaction is a conflict, not silently run.
    let (status, body) = send(
        &router,
        admin_sql_req("test-ns", "SELECT 1", Some("tx-b"), Some("query")),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(body["error"].is_string());

    // the real transaction is intact and can still end.
    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "ROLLBACK", Some("tx-a"), Some("end")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn admin_transaction_duplicate_begin_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();

    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "BEGIN", Some("tx-a"), Some("begin")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "BEGIN", Some("tx-a"), Some("begin")),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "ROLLBACK", Some("tx-a"), Some("end")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn admin_transaction_malformed_steps_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();

    // a begin step whose SQL is not a canonical BEGIN.
    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "SELECT 1", Some("tx-a"), Some("begin")),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // a step with an id but no step name.
    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "SELECT 1", Some("tx-a"), None),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // a step name with no id.
    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "SELECT 1", None, Some("query")),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // now open a real transaction and probe the query/end malformed cases.
    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "BEGIN", Some("tx-a"), Some("begin")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // a query step may not run transaction-control SQL.
    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "ROLLBACK", Some("tx-a"), Some("query")),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // leading comments, empty statements, and whitespace must not hide control
    // SQL from the guard (rusqlite skips them and would run the control stmt).
    for sneaky in [
        "-- oops\nROLLBACK",
        "/* oops */ COMMIT",
        "  ;\t-- x\n  rollback",
        "/* a */ /* b */ END",
    ] {
        let (status, _) = send(
            &router,
            admin_sql_req("test-ns", sneaky, Some("tx-a"), Some("query")),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "comment-hidden control must be rejected: {sneaky:?}"
        );
    }

    // an end step whose SQL is neither COMMIT nor ROLLBACK.
    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "MAYBE", Some("tx-a"), Some("end")),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // the transaction survived every rejected step and still ends cleanly.
    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "ROLLBACK", Some("tx-a"), Some("end")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn admin_transaction_disconnect_recovers_namespace() {
    // a short lease so a lost admin client is reclaimed within the test.
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(
        custom_config_with_lease(std::time::Duration::from_millis(150)),
        tmp.path().to_path_buf(),
    );
    let router = host.into_router_trusted();

    let (status, _) = send(
        &router,
        admin_sql_req("test-ns", "BEGIN", Some("tx-gone"), Some("begin")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &router,
        admin_sql_req(
            "test-ns",
            "INSERT INTO item (id, label) VALUES ('orphan', 'pending')",
            Some("tx-gone"),
            Some("query"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // the client vanishes without ending. a pull queued now must eventually run
    // once the lease reclaims the namespace.
    let (status, _) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        send(
            &router,
            pull_req("test-ns", &pull_body(None), "Bearer user-1"),
        ),
    )
    .await
    .expect("lease did not unblock the namespace");
    assert_eq!(status, StatusCode::OK);

    // the pending insert was rolled back on reclaim.
    let (status, result) = send(
        &router,
        admin_sql_req(
            "test-ns",
            "SELECT count(*) AS count FROM item WHERE id = 'orphan'",
            None,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["rows"][0]["count"], json!(0));
}

#[tokio::test]
async fn auth_rejection_returns_401() {
    let tmp = tempfile::tempdir().unwrap();
    let host = test_host(custom_config(), tmp.path().to_path_buf());
    let router = host.into_router_trusted();

    let req = Request::builder()
        .method("POST")
        .uri("/test-ns/pull")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            serde_json::to_vec(&pull_body(None)).unwrap(),
        ))
        .unwrap();
    let (status, _v) = send(&router, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let req = pull_req("test-ns", &pull_body(None), "Bearer ");
    let (status, _v) = send(&router, req).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn fixture_config_still_works() {
    // verify the fixture tables work through the library API — the binary path
    use sync_native::fixture;

    let fixture_init: InitFn =
        Arc::new(|db| fixture::install_app_tables_and_seed(db).map_err(|e| e.0));

    let fixture_mutate: MutateFn = Arc::new(|db, name, args, _user_id| {
        use fixture::MutateError as F;
        match fixture::run_mutator(db, name, args, _user_id) {
            Ok(()) => Ok(()),
            Err(F::App(d)) => Err(MutateError::app(d)),
            Err(F::Db(e)) => Err(MutateError::Other(e.0)),
            Err(F::Unknown(m)) => Err(MutateError::Other(m)),
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let config = SyncNativeConfig {
        tables: fixture::build_tables(),
        initialize: fixture_init,
        mutate: fixture_mutate,
        visible: None,
        authenticate: Arc::new(|headers, _namespace| {
            Box::pin(async move {
                headers
                    .get("authorization")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.strip_prefix("Bearer token-"))
                    .map(AuthClaims::new)
                    .ok_or_else(|| AuthError::unauthorized("missing auth"))
            })
        }),
        authorize_wake: Arc::new(|_, _| Box::pin(async { Ok(()) })),
        retain_changes: 4096,
        max_change_rows: sync_core::Caps::default().max_change_rows,
        visibility_enabled: false,
        query_aware: false,
        query_resolution: None,
        admin_tx_lease: sync_native::DEFAULT_ADMIN_TX_LEASE,
        retention: sync_native::retain::RetentionPolicy::disabled(),
    };
    let host = test_host(config, tmp.path().to_path_buf());
    let router = host.into_router_trusted();

    // pull: should get the fixture seed (users, projects, members, tasks)
    let body = pull_body(None);
    let req = pull_req("test-ns", &body, "Bearer token-u1");
    let (_status, pull_resp) = send(&router, req).await;

    // four fixture tables should each have put rows
    for table in &["user", "project", "member", "task"] {
        let count = patch_count(&pull_resp, table, "put");
        assert!(
            count > 0,
            "fixture table {table} should have seed rows, got {count}"
        );
    }

    // push: create a project
    let push_body = push_body(json!([mutation(
        1,
        "project.create",
        json!([{"id": "p-new", "ownerId": "u1", "name": "fresh"}]),
        "c1"
    )]));
    let req = push_req("test-ns", &push_body, "Bearer token-u1");
    let (status, _resp) = send(&router, req).await;
    assert_eq!(status, StatusCode::OK);
}
