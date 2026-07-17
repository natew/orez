// sync-native binary: the fixture-data harness server.
//
// this is a thin consumer of the sync-native library. it populates a
// SyncNativeConfig with the standard fixture tables, mutators, visibility,
// and auth, then starts the axum host.
//
// usage: sync-native --data-dir <dir> --port <port>
//                    [--admin-token <token>] [--allow-origin <origin>]
//                    [--retain-changes <n>] [--max-change-rows <n>]
//                    [--visible] [--query-aware]
//
// routes (namespace = one sqlite file under --data-dir):
//   POST /<ns>/pull, /<ns>/push        the http-pull dialect (engine)
//   GET  /<ns>/wake                     wake WebSocket ("pull now" only)
//   POST /<ns>/admin/sql                oracle reads + upstream writes
//   POST /<ns>/admin/settle-push        settle an app-owned committed push
//   GET  /<ns>/admin/status            { ok, bootID, pid, versions, counters }
//   POST /<ns>/admin/invalidate         epoch bump
//   POST /<ns>/admin/reset-cursor       restored/behind-server fault
//   POST /<ns>/admin/drop-next-push-response  lost-response fault
//   POST /<ns>/admin/fault              arm a one-shot pull/push lifecycle fault (M6)
//   GET  /admin/health                  process readiness (no namespace)

use std::path::PathBuf;
use std::sync::Arc;

use axum::http::HeaderMap;
use sync_native::AuthFn;
use sync_native::SyncNativeConfig;
use sync_native::SyncNativeHost;
use sync_native::SyncNativeSecurity;
use sync_native::engine::{InitFn, MutateFn, VisibleFn};
use sync_native::fixture;

struct CliConfig {
    data_dir: PathBuf,
    port: u16,
    retain_changes: i64,
    max_change_rows: usize,
    visible: bool,
    query_aware: bool,
    admin_token: Option<String>,
    allowed_origins: Vec<String>,
}

fn parse_args() -> CliConfig {
    let mut data_dir: Option<PathBuf> = None;
    let mut port: Option<u16> = None;
    let mut retain_changes: i64 = 4096;
    let mut max_change_rows: usize = sync_core::pull::Caps::default().max_change_rows;
    let mut visible = false;
    let mut query_aware = false;
    let mut admin_token = None;
    let mut allowed_origins = Vec::new();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-dir" => {
                data_dir = Some(PathBuf::from(expect_value(&mut args, "--data-dir")));
            }
            "--port" => {
                port = Some(
                    expect_value(&mut args, "--port")
                        .parse()
                        .expect("--port must be a u16"),
                );
            }
            "--retain-changes" => {
                retain_changes = expect_value(&mut args, "--retain-changes")
                    .parse()
                    .expect("--retain-changes must be an integer");
            }
            "--max-change-rows" => {
                max_change_rows = expect_value(&mut args, "--max-change-rows")
                    .parse()
                    .expect("--max-change-rows must be a non-negative integer");
            }
            "--visible" => visible = true,
            "--query-aware" => query_aware = true,
            "--admin-token" => {
                admin_token = Some(expect_value(&mut args, "--admin-token"));
            }
            "--allow-origin" => {
                allowed_origins.push(expect_value(&mut args, "--allow-origin"));
            }
            other => panic!("unknown argument {other}"),
        }
    }

    CliConfig {
        data_dir: data_dir.expect("--data-dir is required"),
        port: port.expect("--port is required"),
        retain_changes,
        max_change_rows,
        visible,
        query_aware,
        admin_token,
        allowed_origins,
    }
}

fn expect_value(args: &mut impl Iterator<Item = String>, flag: &str) -> String {
    args.next()
        .unwrap_or_else(|| panic!("{flag} needs a value"))
}

#[tokio::main]
async fn main() {
    let cli = parse_args();

    // fixture authenticate: Bearer token-<userID>
    let authenticate: AuthFn = Arc::new(|headers: &HeaderMap| {
        let value = headers.get("authorization")?.to_str().ok()?;
        value
            .strip_prefix("Bearer token-")
            .filter(|user_id| !user_id.is_empty())
            .map(str::to_string)
    });

    // fixture initialize: install app tables + seed
    let initialize: InitFn =
        Arc::new(|db| fixture::install_app_tables_and_seed(db).map_err(|e| e.0));

    // fixture mutate: forward to the built-in mutator set
    let mutate: MutateFn = Arc::new(|db, name, args, _user_id| {
        use fixture::MutateError as F;
        match fixture::run_mutator(db, name, args, _user_id) {
            Ok(()) => Ok(()),
            Err(F::App(details)) => Err(sync_core::error::MutateError::app(details)),
            Err(F::Db(e)) => Err(sync_core::error::MutateError::Other(e.0)),
            Err(F::Unknown(m)) => Err(sync_core::error::MutateError::Other(m)),
        }
    });

    // fixture visibility: per-user row filtering
    let visible: Option<VisibleFn> = if cli.visible {
        Some(Arc::new(|table: &str, user_id: &str| {
            fixture::fixture_visible(table, user_id)
        }))
    } else {
        None
    };

    let config = SyncNativeConfig {
        tables: fixture::build_tables(),
        initialize,
        mutate,
        visible,
        authenticate,
        retain_changes: cli.retain_changes,
        max_change_rows: cli.max_change_rows,
        visibility_enabled: cli.visible,
        query_aware: cli.query_aware,
        query_resolution: None,
        admin_tx_lease: sync_native::DEFAULT_ADMIN_TX_LEASE,
        // the fixture harness owns short-lived data dirs, so it does not need a
        // background file-deletion policy.
        retention: sync_native::retain::RetentionPolicy::disabled(),
    };

    let mut security = cli
        .admin_token
        .or_else(|| std::env::var("SYNC_NATIVE_ADMIN_TOKEN").ok())
        .map(SyncNativeSecurity::with_admin_token)
        .unwrap_or_else(SyncNativeSecurity::process_random);
    for origin in cli.allowed_origins {
        security = security.allow_origin(origin);
    }

    SyncNativeHost::new_with_security(config, cli.data_dir, security)
        .run(cli.port)
        .await;
}
