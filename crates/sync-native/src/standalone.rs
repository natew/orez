use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::http::header::{
    CONNECTION, CONTENT_LENGTH, HOST, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION, TE, TRAILER,
    TRANSFER_ENCODING, UPGRADE,
};
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sync_core::error::MutateError;
use sync_core::schema::Tables;

use crate::engine::{InitFn, MutateFn};
use crate::retain::RetentionPolicy;
use crate::{
    AuthClaims, AuthError, AuthFn, AuthorizeWakeFn, QueryResolution, QueryResolveError,
    ResolveQueriesFn, ResolvedQueries, SyncNativeConfig, SyncNativeHost, SyncNativeSecurity,
};

pub const USAGE: &str = "sync-native

Usage:
  sync-native serve --schema <zero-schema.json> --init-sql <statements.json> \\
    --data-dir <dir> --port <port> --admin-token-env <name> \\
    --auth-url <loopback-url> --wake-authorize-url <loopback-url> \\
    --query-transform-url <loopback-url> [options]

Options:
  --host <IP>                       Default: 127.0.0.1
  --allow-origin <origin>           Repeat for each browser origin
  --retain-changes <rows>           Default: 4096
  --max-change-rows <rows>          Default: engine limit
  -h, --help
  -V, --version";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServeConfig {
    pub schema: PathBuf,
    pub init_sql: PathBuf,
    pub data_dir: PathBuf,
    pub host: IpAddr,
    pub port: u16,
    pub admin_token_env: String,
    pub auth_url: Url,
    pub wake_authorize_url: Url,
    pub query_transform_url: Url,
    pub allowed_origins: Vec<String>,
    pub retain_changes: i64,
    pub max_change_rows: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Help,
    Version,
    Serve(Box<ServeConfig>),
}

pub fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Command, String> {
    let mut args = args.into_iter();
    let Some(command) = args.next() else {
        return Ok(Command::Help);
    };
    if matches!(command.as_str(), "-h" | "--help") {
        return Ok(Command::Help);
    }
    if matches!(command.as_str(), "-V" | "--version") {
        return Ok(Command::Version);
    }
    if command != "serve" {
        return Err(format!("unknown command '{command}'"));
    }

    let mut values = HashMap::<String, String>::new();
    let mut allowed_origins = Vec::new();
    while let Some(flag) = args.next() {
        if matches!(flag.as_str(), "-h" | "--help") {
            return Ok(Command::Help);
        }
        if matches!(flag.as_str(), "-V" | "--version") {
            return Ok(Command::Version);
        }
        let supported = matches!(
            flag.as_str(),
            "--schema"
                | "--init-sql"
                | "--data-dir"
                | "--host"
                | "--port"
                | "--admin-token-env"
                | "--auth-url"
                | "--wake-authorize-url"
                | "--query-transform-url"
                | "--allow-origin"
                | "--retain-changes"
                | "--max-change-rows"
        );
        if !supported {
            return Err(format!("unknown option '{flag}'"));
        }
        let value = args
            .next()
            .ok_or_else(|| format!("{flag} requires a value"))?;
        if flag == "--allow-origin" {
            let url =
                Url::parse(&value).map_err(|error| format!("invalid --allow-origin: {error}"))?;
            if !matches!(url.scheme(), "http" | "https")
                || url.origin().ascii_serialization() != value
            {
                return Err(
                    "--allow-origin must be an exact HTTP(S) origin without a path or trailing slash"
                        .to_string(),
                );
            }
            allowed_origins.push(value);
        } else if values.insert(flag.clone(), value).is_some() {
            return Err(format!("{flag} may only be provided once"));
        }
    }

    let required = |flag: &str| {
        values
            .get(flag)
            .cloned()
            .ok_or_else(|| format!("missing required {flag}"))
    };
    let port = parse_number::<u16>(&required("--port")?, "--port")?;
    if port == 0 {
        return Err("--port must be greater than zero".to_string());
    }
    let retain_changes = values
        .get("--retain-changes")
        .map(|value| parse_number::<i64>(value, "--retain-changes"))
        .transpose()?
        .unwrap_or(4096);
    if retain_changes < 1 {
        return Err("--retain-changes must be greater than zero".to_string());
    }
    let max_change_rows = values
        .get("--max-change-rows")
        .map(|value| parse_number::<usize>(value, "--max-change-rows"))
        .transpose()?
        .unwrap_or_else(|| sync_core::Caps::default().max_change_rows);
    if max_change_rows == 0 {
        return Err("--max-change-rows must be greater than zero".to_string());
    }
    Ok(Command::Serve(Box::new(ServeConfig {
        schema: PathBuf::from(required("--schema")?),
        init_sql: PathBuf::from(required("--init-sql")?),
        data_dir: PathBuf::from(required("--data-dir")?),
        host: values
            .get("--host")
            .map(|value| {
                value
                    .parse::<IpAddr>()
                    .map_err(|_| format!("invalid value for --host: '{value}'"))
            })
            .transpose()?
            .unwrap_or_else(|| "127.0.0.1".parse().expect("valid loopback IP")),
        port,
        admin_token_env: required("--admin-token-env")?,
        auth_url: loopback_url(&required("--auth-url")?, "--auth-url")?,
        wake_authorize_url: loopback_url(
            &required("--wake-authorize-url")?,
            "--wake-authorize-url",
        )?,
        query_transform_url: loopback_url(
            &required("--query-transform-url")?,
            "--query-transform-url",
        )?,
        allowed_origins,
        retain_changes,
        max_change_rows,
    })))
}

fn parse_number<T>(value: &str, flag: &str) -> Result<T, String>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| format!("invalid value for {flag}: '{value}'"))
}

fn loopback_url(value: &str, flag: &str) -> Result<Url, String> {
    let url = Url::parse(value).map_err(|error| format!("invalid {flag}: {error}"))?;
    if url.scheme() != "http"
        || !matches!(
            url.host_str(),
            Some("localhost" | "127.0.0.1" | "::1" | "[::1]")
        )
        || url.port().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(format!(
            "{flag} must be an explicit-port HTTP URL on localhost, 127.0.0.1, or [::1]"
        ));
    }
    Ok(url)
}

pub async fn serve(config: ServeConfig) -> Result<(), String> {
    let schema = read_json(&config.schema, "schema")?;
    let tables = Tables::from_zero_schema(&schema)
        .map_err(|error| format!("invalid schema {}: {error}", config.schema.display()))?;
    let init_sql: Vec<String> = serde_json::from_value(read_json(&config.init_sql, "init SQL")?)
        .map_err(|error| {
            format!(
                "invalid init SQL {}: expected a JSON array of SQL strings: {error}",
                config.init_sql.display()
            )
        })?;
    if init_sql.iter().any(|statement| statement.trim().is_empty()) {
        return Err("init SQL statements must not be empty".to_string());
    }
    let admin_token = std::env::var(&config.admin_token_env)
        .map_err(|_| format!("environment variable {} is not set", config.admin_token_env))?;
    if admin_token.len() < 32 {
        return Err(format!(
            "environment variable {} must contain at least 32 bytes",
            config.admin_token_env
        ));
    }
    let admin_header = HeaderValue::from_str(&admin_token).map_err(|_| {
        format!(
            "environment variable {} is not a valid HTTP header value",
            config.admin_token_env
        )
    })?;
    prepare_data_dir(&config.data_dir)?;

    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| format!("failed to build callback client: {error}"))?;
    let authenticate = callback_auth(
        client.clone(),
        config.auth_url.clone(),
        admin_header.clone(),
    );
    let authorize_wake = callback_wake(
        client.clone(),
        config.wake_authorize_url.clone(),
        admin_header.clone(),
    );
    let resolve = callback_queries(client, config.query_transform_url.clone(), admin_header);
    let initialize: InitFn = Arc::new(move |db| {
        for statement in &init_sql {
            db.exec(statement, &[]).map_err(|error| error.to_string())?;
        }
        Ok(())
    });
    let mutate: MutateFn = Arc::new(|_, _, _, _| {
        Err(MutateError::Other(
            "direct native pushes are disabled; use the application push endpoint".to_string(),
        ))
    });
    let mut security = SyncNativeSecurity::with_admin_token(admin_token);
    for origin in config.allowed_origins {
        security = security.allow_origin(origin);
    }
    let host = SyncNativeHost::new_with_security(
        SyncNativeConfig {
            tables,
            initialize,
            mutate,
            visible: None,
            authenticate,
            authorize_wake,
            retain_changes: config.retain_changes,
            max_change_rows: config.max_change_rows,
            visibility_enabled: false,
            query_aware: true,
            query_resolution: Some(QueryResolution { resolve }),
            admin_tx_lease: crate::DEFAULT_ADMIN_TX_LEASE,
            retention: RetentionPolicy::workers(Duration::from_secs(30), Duration::from_secs(5)),
        },
        config.data_dir,
        security,
    );
    host.run_on(config.host, config.port).await;
    Ok(())
}

fn prepare_data_dir(path: &Path) -> Result<(), String> {
    std::fs::create_dir_all(path).map_err(|error| {
        format!(
            "failed to create data directory {}: {error}",
            path.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(
            |error| {
                format!(
                    "failed to secure data directory {}: {error}",
                    path.display()
                )
            },
        )?;
    }
    Ok(())
}

fn read_json(path: &Path, label: &str) -> Result<Value, String> {
    let bytes = std::fs::read(path)
        .map_err(|error| format!("failed to read {label} {}: {error}", path.display()))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid JSON in {label} {}: {error}", path.display()))
}

fn callback_auth(client: Client, url: Url, admin_token: HeaderValue) -> AuthFn {
    Arc::new(move |headers, namespace| {
        let client = client.clone();
        let url = url.clone();
        let callback_headers = callback_headers(&headers, &admin_token);
        Box::pin(async move {
            let response = client
                .post(url)
                .headers(callback_headers)
                .json(&AuthRequest { namespace })
                .send()
                .await
                .map_err(|error| AuthError::upstream(format!("auth callback failed: {error}")))?;
            if response.status() == reqwest::StatusCode::UNAUTHORIZED {
                return Err(AuthError::unauthorized("unauthorized"));
            }
            if response.status() == reqwest::StatusCode::FORBIDDEN {
                return Err(AuthError::forbidden("forbidden"));
            }
            if !response.status().is_success() {
                return Err(AuthError::upstream(format!(
                    "auth callback returned {}",
                    response.status()
                )));
            }
            let body: Value = response.json().await.map_err(|error| {
                AuthError::upstream(format!("auth callback returned invalid JSON: {error}"))
            })?;
            AuthClaims::from_value(body).map_err(AuthError::upstream)
        })
    })
}

fn callback_queries(client: Client, url: Url, admin_token: HeaderValue) -> ResolveQueriesFn {
    Arc::new(move |queries, headers, claims, namespace| {
        let client = client.clone();
        let url = url.clone();
        let mut callback_headers = callback_headers(&headers, &admin_token);
        Box::pin(async move {
            let expected = queries.len();
            callback_headers.insert(
                "x-orez-namespace",
                namespace.parse().map_err(|_| {
                    QueryResolveError::upstream("namespace is not a valid callback header value")
                })?,
            );
            callback_headers.insert(
                "x-orez-user-id",
                claims.user_id().parse().map_err(|_| {
                    QueryResolveError::upstream("userID is not a valid callback header value")
                })?,
            );
            let requests: Vec<CallbackQuery> = queries
                .into_iter()
                .enumerate()
                .map(|(index, query)| CallbackQuery {
                    id: index.to_string(),
                    name: query.name,
                    args: query.args,
                })
                .collect();
            let request = serde_json::json!(["transform", requests]);
            let response = client
                .post(url)
                .headers(callback_headers)
                .json(&request)
                .send()
                .await
                .map_err(|error| {
                    QueryResolveError::upstream(format!("query transform callback failed: {error}"))
                })?;
            let status = response.status();
            if !status.is_success() {
                let message = format!("query transform callback returned {status}");
                return Err(if status.is_client_error() {
                    QueryResolveError::bad_request(message)
                } else {
                    QueryResolveError::upstream(message)
                });
            }
            let body: QueryResponse = response.json().await.map_err(|error| {
                QueryResolveError::upstream(format!(
                    "query transform callback returned invalid JSON: {error}"
                ))
            })?;
            if body.queries.len() != expected {
                return Err(QueryResolveError::upstream(format!(
                    "query transform callback returned {} results for {expected} queries",
                    body.queries.len()
                )));
            }
            let mut ordered = vec![None; expected];
            for result in body.queries {
                let index = result.id.parse::<usize>().map_err(|_| {
                    QueryResolveError::upstream(format!(
                        "query transform callback returned invalid id '{}'",
                        result.id
                    ))
                })?;
                if index >= expected || ordered[index].is_some() {
                    return Err(QueryResolveError::upstream(format!(
                        "query transform callback returned unexpected id '{}'",
                        result.id
                    )));
                }
                if let Some(error) = result.error {
                    return Err(QueryResolveError::bad_request(format!(
                        "query '{}' failed: {error}: {}",
                        result.id,
                        result.message.unwrap_or_default()
                    )));
                }
                let ast = result.ast.ok_or_else(|| {
                    QueryResolveError::upstream(format!(
                        "query transform callback returned no AST for id '{}'",
                        result.id
                    ))
                })?;
                ordered[index] = Some(ast);
            }
            Ok(ResolvedQueries {
                asts: ordered
                    .into_iter()
                    .map(|ast| ast.expect("result count and ids were validated"))
                    .collect(),
                transform_version: body.query_transform_version,
            })
        })
    })
}

fn callback_wake(client: Client, url: Url, admin_token: HeaderValue) -> AuthorizeWakeFn {
    Arc::new(move |namespace, token| {
        let client = client.clone();
        let url = url.clone();
        let admin_token = admin_token.clone();
        Box::pin(async move {
            let token = token.ok_or_else(|| AuthError::unauthorized("missing wake capability"))?;
            let response = client
                .post(url)
                .header("x-admin-key", admin_token)
                .json(&WakeAuthorizeRequest { namespace, token })
                .send()
                .await
                .map_err(|error| {
                    AuthError::upstream(format!("wake authorization callback failed: {error}"))
                })?;
            if response.status() == reqwest::StatusCode::NO_CONTENT {
                return Ok(());
            }
            if response.status().is_client_error() {
                return Err(AuthError::unauthorized("invalid wake capability"));
            }
            Err(AuthError::upstream(format!(
                "wake authorization callback returned {}",
                response.status()
            )))
        })
    })
}

fn callback_headers(source: &HeaderMap, admin_token: &HeaderValue) -> HeaderMap {
    static OMIT: [HeaderName; 9] = [
        HOST,
        CONNECTION,
        CONTENT_LENGTH,
        TRANSFER_ENCODING,
        UPGRADE,
        TE,
        TRAILER,
        PROXY_AUTHENTICATE,
        PROXY_AUTHORIZATION,
    ];
    let connection_headers = source
        .get_all(CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect::<Vec<_>>();
    let mut headers = HeaderMap::new();
    for (name, value) in source {
        if name == "x-admin-key" || OMIT.contains(name) || connection_headers.contains(name) {
            continue;
        }
        headers.append(name, value.clone());
    }
    headers.insert("x-admin-key", admin_token.clone());
    headers
}

#[derive(Serialize)]
struct AuthRequest {
    namespace: String,
}

#[derive(Serialize)]
struct WakeAuthorizeRequest {
    namespace: String,
    token: String,
}

#[derive(Serialize)]
struct CallbackQuery {
    id: String,
    name: String,
    args: Vec<Value>,
}

#[derive(Deserialize)]
struct QueryResponse {
    #[serde(rename = "queryTransformVersion")]
    query_transform_version: u64,
    queries: Vec<CallbackQueryResult>,
}

#[derive(Deserialize)]
struct CallbackQueryResult {
    id: String,
    ast: Option<Value>,
    error: Option<String>,
    message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::post;
    use serde_json::json;
    use std::sync::Mutex;

    fn required_args() -> Vec<String> {
        [
            "serve",
            "--schema",
            "schema.json",
            "--init-sql",
            "init.json",
            "--data-dir",
            "data",
            "--port",
            "4848",
            "--admin-token-env",
            "OREZ_ADMIN_TOKEN",
            "--auth-url",
            "http://127.0.0.1:3000/auth",
            "--wake-authorize-url",
            "http://127.0.0.1:3000/wake-authorize",
            "--query-transform-url",
            "http://localhost:3000/query",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    }

    #[test]
    fn parses_required_serve_contract() {
        let Command::Serve(config) = parse_args(required_args()).unwrap() else {
            panic!("expected serve command");
        };
        assert_eq!(config.port, 4848);
        assert_eq!(config.host, "127.0.0.1".parse::<IpAddr>().unwrap());
        assert_eq!(config.retain_changes, 4096);
    }

    #[test]
    fn rejects_non_loopback_callbacks() {
        for value in [
            "https://127.0.0.1:3000/auth",
            "http://example.com:3000/auth",
            "http://127.0.0.1/auth",
            "http://user@127.0.0.1:3000/auth",
            "http://127.0.0.1:3000/auth#fragment",
        ] {
            let mut args = required_args();
            let index = args.iter().position(|arg| arg == "--auth-url").unwrap() + 1;
            args[index] = value.to_string();
            assert!(
                parse_args(args)
                    .unwrap_err()
                    .contains("explicit-port HTTP URL")
            );
        }
    }

    #[test]
    fn accepts_ipv6_loopback_callbacks() {
        let mut args = required_args();
        let index = args.iter().position(|arg| arg == "--auth-url").unwrap() + 1;
        args[index] = "http://[::1]:3000/auth".to_string();
        assert!(matches!(parse_args(args), Ok(Command::Serve(_))));
    }

    #[cfg(unix)]
    #[test]
    fn data_directory_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("nested/data");
        prepare_data_dir(&data_dir).unwrap();
        assert_eq!(
            std::fs::metadata(data_dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    #[test]
    fn accepts_only_canonical_browser_origins() {
        let mut args = required_args();
        args.extend([
            "--allow-origin".to_string(),
            "https://example.com".to_string(),
            "--allow-origin".to_string(),
            "http://localhost:3000".to_string(),
        ]);
        let Command::Serve(config) = parse_args(args).unwrap() else {
            panic!("expected serve command");
        };
        assert_eq!(
            config.allowed_origins,
            ["https://example.com", "http://localhost:3000"]
        );

        for invalid in [
            "https://example.com/",
            "https://example.com/path",
            "https://example.com?query",
            "file://example.com",
        ] {
            let mut args = required_args();
            args.extend(["--allow-origin".to_string(), invalid.to_string()]);
            assert!(
                parse_args(args)
                    .unwrap_err()
                    .contains("exact HTTP(S) origin")
            );
        }
    }

    #[derive(Default)]
    struct SeenCallbacks {
        auth: Mutex<Option<(HeaderMap, Value)>>,
        query: Mutex<Option<(HeaderMap, Value)>>,
        wake: Mutex<Option<(HeaderMap, Value)>>,
    }

    #[tokio::test]
    async fn callbacks_preserve_zero_wire_and_replace_admin_identity() {
        let seen = Arc::new(SeenCallbacks::default());
        let app = Router::new()
            .route(
                "/auth",
                post(
                    |State(seen): State<Arc<SeenCallbacks>>,
                     headers: HeaderMap,
                     axum::Json(body): axum::Json<Value>| async move {
                        *seen.auth.lock().unwrap() = Some((headers, body));
                        axum::Json(json!({
                            "userID": "user-1",
                            "email": "user@example.com",
                            "role": "member",
                        }))
                    },
                ),
            )
            .route(
                "/query",
                post(
                    |State(seen): State<Arc<SeenCallbacks>>,
                     headers: HeaderMap,
                     axum::Json(body): axum::Json<Value>| async move {
                        *seen.query.lock().unwrap() = Some((headers, body));
                        axum::Json(json!({
                            "queryTransformVersion": 9,
                            "queries": [
                                { "id": "1", "ast": { "table": "second" } },
                                { "id": "0", "ast": { "table": "first" } },
                            ],
                        }))
                    },
                ),
            )
            .route(
                "/wake",
                post(
                    |State(seen): State<Arc<SeenCallbacks>>,
                     headers: HeaderMap,
                     axum::Json(body): axum::Json<Value>| async move {
                        *seen.wake.lock().unwrap() = Some((headers, body));
                        StatusCode::NO_CONTENT
                    },
                ),
            )
            .route("/forbidden", post(|| async { StatusCode::FORBIDDEN }))
            .with_state(seen.clone());
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer original".parse().unwrap());
        headers.insert("cookie", "session=original".parse().unwrap());
        headers.insert("x-admin-key", "attacker-value".parse().unwrap());
        headers.insert("x-orez-namespace", "attacker-namespace".parse().unwrap());
        headers.insert("x-orez-user-id", "attacker-user".parse().unwrap());
        headers.insert("connection", "x-remove-me".parse().unwrap());
        headers.insert("x-remove-me", "hop-by-hop".parse().unwrap());
        let admin_token = "process-admin-token-000000000000000000000000000000";

        let authenticate = callback_auth(
            client.clone(),
            Url::parse(&format!("http://127.0.0.1:{port}/auth")).unwrap(),
            admin_token.parse().unwrap(),
        );
        let claims = authenticate(headers.clone(), "project-one".to_string())
            .await
            .unwrap();
        assert_eq!(claims.user_id(), "user-1");
        assert_eq!(claims.value()["role"], "member");

        let resolve = callback_queries(
            client.clone(),
            Url::parse(&format!("http://127.0.0.1:{port}/query")).unwrap(),
            admin_token.parse().unwrap(),
        );
        let resolved = resolve(
            vec![
                crate::NamedQuery {
                    name: "first.all".to_string(),
                    args: vec![],
                },
                crate::NamedQuery {
                    name: "second.byID".to_string(),
                    args: vec![json!({ "id": "two" })],
                },
            ],
            headers,
            claims,
            "project-one".to_string(),
        )
        .await
        .unwrap();
        assert_eq!(resolved.transform_version, 9);
        assert_eq!(resolved.asts[0]["table"], "first");
        assert_eq!(resolved.asts[1]["table"], "second");

        let authorize_wake = callback_wake(
            client.clone(),
            Url::parse(&format!("http://127.0.0.1:{port}/wake")).unwrap(),
            admin_token.parse().unwrap(),
        );
        assert_eq!(
            authorize_wake("project-one".to_string(), None)
                .await
                .unwrap_err()
                .status,
            401
        );
        authorize_wake(
            "project-one".to_string(),
            Some("signed-capability".to_string()),
        )
        .await
        .unwrap();

        let (auth_headers, auth_body) = seen.auth.lock().unwrap().take().unwrap();
        assert_eq!(auth_headers["authorization"], "Bearer original");
        assert_eq!(auth_headers["cookie"], "session=original");
        assert_eq!(auth_headers["x-admin-key"], admin_token);
        assert!(!auth_headers.contains_key("x-remove-me"));
        assert_eq!(auth_body, json!({ "namespace": "project-one" }));

        let (query_headers, query_body) = seen.query.lock().unwrap().take().unwrap();
        assert_eq!(query_headers["authorization"], "Bearer original");
        assert_eq!(query_headers["x-admin-key"], admin_token);
        assert_eq!(query_headers["x-orez-namespace"], "project-one");
        assert_eq!(query_headers["x-orez-user-id"], "user-1");
        assert!(!query_headers.contains_key("x-remove-me"));
        assert_eq!(query_body[0], "transform");
        assert_eq!(query_body[1][0]["id"], "0");
        assert_eq!(query_body[1][1]["name"], "second.byID");

        let (wake_headers, wake_body) = seen.wake.lock().unwrap().take().unwrap();
        assert_eq!(wake_headers["x-admin-key"], admin_token);
        assert_eq!(
            wake_body,
            json!({ "namespace": "project-one", "token": "signed-capability" })
        );

        let forbidden_auth = callback_auth(
            client,
            Url::parse(&format!("http://127.0.0.1:{port}/forbidden")).unwrap(),
            admin_token.parse().unwrap(),
        );
        assert_eq!(
            forbidden_auth(HeaderMap::new(), "project-two".to_string())
                .await
                .unwrap_err()
                .status,
            403
        );
        server.abort();
    }
}
