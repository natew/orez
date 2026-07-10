//! Narrow wasm-bindgen boundary between `sync-core` and Durable Object SQL.
//!
//! The JavaScript object passed to these exports implements the same synchronous
//! `exec`/`query` shape as [`sync_core::SyncDb`]. Transactions deliberately stay
//! in JavaScript: neither this crate nor `sync-core` emits transaction SQL.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use sync_core::{DbError, Row, SqlValue, SyncDb};
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    pub type JsSyncDb;

    #[wasm_bindgen(method, catch, js_name = exec)]
    fn js_exec(this: &JsSyncDb, sql: &str, params: JsValue) -> Result<(), JsValue>;

    #[wasm_bindgen(method, catch, js_name = query)]
    fn js_query(this: &JsSyncDb, sql: &str, params: JsValue) -> Result<JsValue, JsValue>;
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum WireValue {
    Null,
    Integer(String),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<&SqlValue> for WireValue {
    fn from(value: &SqlValue) -> Self {
        match value {
            SqlValue::Null => Self::Null,
            SqlValue::Integer(value) => Self::Integer(value.to_string()),
            SqlValue::Real(value) => Self::Real(*value),
            SqlValue::Text(value) => Self::Text(value.clone()),
            SqlValue::Blob(value) => Self::Blob(value.clone()),
        }
    }
}

impl TryFrom<WireValue> for SqlValue {
    type Error = DbError;

    fn try_from(value: WireValue) -> Result<Self, Self::Error> {
        match value {
            WireValue::Null => Ok(Self::Null),
            WireValue::Integer(value) => value
                .parse()
                .map(Self::Integer)
                .map_err(|error| DbError(format!("invalid i64 {value:?}: {error}"))),
            WireValue::Real(value) => Ok(Self::Real(value)),
            WireValue::Text(value) => Ok(Self::Text(value)),
            WireValue::Blob(value) => Ok(Self::Blob(value)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct WireRow {
    columns: Vec<String>,
    values: Vec<WireValue>,
}

struct WasmDb<'a>(&'a JsSyncDb);

impl WasmDb<'_> {
    fn js_error(error: impl std::fmt::Debug) -> DbError {
        DbError(format!("JavaScript adapter error: {error:?}"))
    }
}

impl SyncDb for WasmDb<'_> {
    fn exec(&mut self, sql: &str, params: &[SqlValue]) -> Result<(), DbError> {
        let params: Vec<_> = params.iter().map(WireValue::from).collect();
        let params = serde_wasm_bindgen::to_value(&params).map_err(Self::js_error)?;
        self.0.js_exec(sql, params).map_err(Self::js_error)
    }

    fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<Vec<Row>, DbError> {
        let params: Vec<_> = params.iter().map(WireValue::from).collect();
        let params = serde_wasm_bindgen::to_value(&params).map_err(Self::js_error)?;
        let rows = self.0.js_query(sql, params).map_err(Self::js_error)?;
        let rows: Vec<WireRow> = serde_wasm_bindgen::from_value(rows).map_err(Self::js_error)?;

        rows.into_iter()
            .map(|row| {
                if row.columns.len() != row.values.len() {
                    return Err(DbError(format!(
                        "adapter row has {} columns but {} values",
                        row.columns.len(),
                        row.values.len()
                    )));
                }
                Ok(Row {
                    columns: Arc::from(row.columns),
                    values: row
                        .values
                        .into_iter()
                        .map(SqlValue::try_from)
                        .collect::<Result<_, _>>()?,
                })
            })
            .collect()
    }
}

fn js_err(error: impl std::fmt::Display) -> JsValue {
    js_sys::Error::new(&error.to_string()).into()
}

#[cfg(feature = "platform-probes")]
fn one_text(row: &Row, name: &str) -> Result<String, JsValue> {
    match row.get(name) {
        Some(SqlValue::Text(value)) => Ok(value.clone()),
        value => Err(js_err(format!(
            "expected text column {name}, got {value:?}"
        ))),
    }
}

/// Initialize only probe tables. The host wraps this in `transactionSync`.
#[cfg(feature = "platform-probes")]
#[wasm_bindgen]
pub fn init_probe_schema(db: &JsSyncDb) -> Result<(), JsValue> {
    let mut db = WasmDb(db);
    for sql in [
        "CREATE TABLE IF NOT EXISTS probe_state (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), lmid INTEGER NOT NULL)",
        "CREATE TABLE IF NOT EXISTS accounts (id TEXT PRIMARY KEY, balance REAL NOT NULL)",
        "CREATE TABLE IF NOT EXISTS ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT NOT NULL, amount REAL NOT NULL, note TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT NOT NULL, payload TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS mutation_log (mutation_id TEXT PRIMARY KEY, lmid INTEGER NOT NULL)",
        "CREATE TABLE IF NOT EXISTS value_probe (id INTEGER PRIMARY KEY, integer_value INTEGER NOT NULL, real_value REAL NOT NULL, text_value TEXT NOT NULL, blob_value BLOB NOT NULL, null_value TEXT, json_value TEXT NOT NULL, boolean_value INTEGER NOT NULL, boundary_value INTEGER NOT NULL)",
    ] {
        db.exec(sql, &[]).map_err(js_err)?;
    }
    db.exec(
        "INSERT OR IGNORE INTO probe_state (singleton, lmid) VALUES (1, 0)",
        &[],
    )
    .map_err(js_err)?;
    db.exec(
        "INSERT OR IGNORE INTO accounts (id, balance) VALUES (?, ?)",
        &[SqlValue::Text("primary".into()), SqlValue::Real(100.0)],
    )
    .map_err(js_err)
}

#[cfg(feature = "platform-probes")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PullSnapshot {
    lmid: String,
    balance: f64,
}

/// Representative synchronous pull read performed wholly inside the host tx.
#[cfg(feature = "platform-probes")]
#[wasm_bindgen]
pub fn pull_snapshot(db: &JsSyncDb) -> Result<JsValue, JsValue> {
    let mut db = WasmDb(db);
    let rows = db
        .query(
            "SELECT CAST(s.lmid AS TEXT) AS lmid, a.balance AS balance FROM probe_state s JOIN accounts a ON a.id = ? WHERE s.singleton = 1",
            &[SqlValue::Text("primary".into())],
        )
        .map_err(js_err)?;
    let row = rows.first().ok_or_else(|| js_err("missing probe state"))?;
    let lmid = one_text(row, "lmid")?;
    let balance = match row.get("balance") {
        Some(SqlValue::Real(value)) => *value,
        Some(SqlValue::Integer(value)) => *value as f64,
        value => return Err(js_err(format!("expected numeric balance, got {value:?}"))),
    };
    serde_wasm_bindgen::to_value(&PullSnapshot { lmid, balance }).map_err(js_err)
}

/// Ordering/replay preflight. LMIDs cross JS as decimal strings, never numbers.
#[cfg(feature = "platform-probes")]
#[wasm_bindgen]
pub fn push_preflight(db: &JsSyncDb, mutation_id: &str) -> Result<String, JsValue> {
    let mut db = WasmDb(db);
    if !db
        .query(
            "SELECT mutation_id FROM mutation_log WHERE mutation_id = ?",
            &[SqlValue::Text(mutation_id.into())],
        )
        .map_err(js_err)?
        .is_empty()
    {
        return Err(js_err(format!("mutation {mutation_id:?} already applied")));
    }
    let row = db
        .query(
            "SELECT CAST(lmid AS TEXT) AS lmid FROM probe_state WHERE singleton = 1",
            &[],
        )
        .map_err(js_err)?
        .into_iter()
        .next()
        .ok_or_else(|| js_err("missing probe state"))?;
    one_text(&row, "lmid")
}

/// Finalize ordering only after the JavaScript mutator has succeeded.
#[cfg(feature = "platform-probes")]
#[wasm_bindgen]
pub fn push_finalize(
    db: &JsSyncDb,
    mutation_id: &str,
    expected_lmid: &str,
) -> Result<String, JsValue> {
    let mut db = WasmDb(db);
    db.exec(
        "UPDATE probe_state SET lmid = lmid + 1 WHERE singleton = 1 AND CAST(lmid AS TEXT) = ?",
        &[SqlValue::Text(expected_lmid.into())],
    )
    .map_err(js_err)?;
    let row = db
        .query(
            "SELECT CAST(lmid AS TEXT) AS lmid FROM probe_state WHERE singleton = 1",
            &[],
        )
        .map_err(js_err)?
        .into_iter()
        .next()
        .ok_or_else(|| js_err("missing probe state"))?;
    let lmid = one_text(&row, "lmid")?;
    let expected_next = expected_lmid
        .parse::<i64>()
        .map_err(js_err)?
        .checked_add(1)
        .ok_or_else(|| js_err("LMID overflow"))?
        .to_string();
    if lmid != expected_next {
        return Err(js_err(format!(
            "LMID compare-and-swap failed: expected {expected_next}, got {lmid}"
        )));
    }
    db.exec(
        "INSERT INTO mutation_log (mutation_id, lmid) VALUES (?, ?)",
        &[
            SqlValue::Text(mutation_id.into()),
            SqlValue::Integer(lmid.parse().map_err(js_err)?),
        ],
    )
    .map_err(js_err)?;
    Ok(lmid)
}

/// Mutate both application data and the counter, then trap. The host must roll
/// back both writes when the panic crosses the wasm/JavaScript boundary.
#[cfg(feature = "platform-probes")]
#[wasm_bindgen]
pub fn rust_panic_after_writes(db: &JsSyncDb) -> Result<(), JsValue> {
    let mut db = WasmDb(db);
    db.exec(
        "INSERT INTO ledger (account_id, amount, note) VALUES (?, ?, ?)",
        &[
            SqlValue::Text("primary".into()),
            SqlValue::Real(999.0),
            SqlValue::Text("must roll back after Rust panic".into()),
        ],
    )
    .map_err(js_err)?;
    db.exec(
        "UPDATE probe_state SET lmid = lmid + 1 WHERE singleton = 1",
        &[],
    )
    .map_err(js_err)?;
    panic!("intentional M0 Rust panic after SQL writes")
}

#[cfg(feature = "platform-probes")]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ValueRoundTrip {
    integer: String,
    real: f64,
    text: String,
    blob: Vec<u8>,
    null: Option<String>,
    json: serde_json::Value,
    boolean: bool,
    boundary: String,
}

/// JS -> wasm -> SQLite -> wasm -> JS fidelity probe. Integer fields are
/// decimal strings on the JS/wire side; SQL uses INTEGER and exact CAST reads.
#[cfg(feature = "platform-probes")]
#[wasm_bindgen]
pub fn value_round_trip(db: &JsSyncDb, input: JsValue) -> Result<JsValue, JsValue> {
    let input: ValueRoundTrip = serde_wasm_bindgen::from_value(input).map_err(js_err)?;
    let integer = input.integer.parse::<i64>().map_err(js_err)?;
    let boundary = input.boundary.parse::<i64>().map_err(js_err)?;
    let json = serde_json::to_string(&input.json).map_err(js_err)?;
    let mut db = WasmDb(db);
    db.exec("DELETE FROM value_probe", &[]).map_err(js_err)?;
    db.exec(
        "INSERT INTO value_probe (id, integer_value, real_value, text_value, blob_value, null_value, json_value, boolean_value, boundary_value) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)",
        &[
            SqlValue::Integer(integer),
            SqlValue::Real(input.real),
            SqlValue::Text(input.text),
            SqlValue::Blob(input.blob),
            SqlValue::Null,
            SqlValue::Text(json),
            SqlValue::Integer(i64::from(input.boolean)),
            // Binding as decimal text lets SQLite's INTEGER affinity parse the
            // exact i64 without first passing through a JavaScript Number.
            SqlValue::Text(boundary.to_string()),
        ],
    )
    .map_err(js_err)?;
    let row = db
        .query(
            "SELECT CAST(integer_value AS TEXT) AS integer_value, real_value, text_value, blob_value, null_value, json_value, CAST(boolean_value AS TEXT) AS boolean_value, CAST(boundary_value AS TEXT) AS boundary_value FROM value_probe WHERE id = 1",
            &[],
        )
        .map_err(js_err)?
        .into_iter()
        .next()
        .ok_or_else(|| js_err("value probe row missing"))?;
    let real = match row.get("real_value") {
        Some(SqlValue::Real(value)) => *value,
        value => return Err(js_err(format!("expected real value, got {value:?}"))),
    };
    let blob = match row.get("blob_value") {
        Some(SqlValue::Blob(value)) => value.clone(),
        value => return Err(js_err(format!("expected blob value, got {value:?}"))),
    };
    let null = match row.get("null_value") {
        Some(SqlValue::Null) => None,
        value => return Err(js_err(format!("expected null value, got {value:?}"))),
    };
    let output = ValueRoundTrip {
        integer: one_text(&row, "integer_value")?,
        real,
        text: one_text(&row, "text_value")?,
        blob,
        null,
        json: serde_json::from_str(&one_text(&row, "json_value")?).map_err(js_err)?,
        boolean: one_text(&row, "boolean_value")? == "1",
        boundary: one_text(&row, "boundary_value")?,
    };
    output
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(js_err)
}

// ---- production sync-core boundary ---------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CapsWire {
    max_change_rows: usize,
    max_change_bytes: usize,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VisibilityWire {
    row_local: bool,
    filters: Vec<VisibilityFilterWire>,
}

#[derive(Clone, Deserialize)]
struct VisibilityFilterWire {
    table: String,
    sql: String,
    params: Vec<serde_json::Value>,
}

fn from_js<T: for<'de> Deserialize<'de>>(value: JsValue) -> Result<T, JsValue> {
    serde_wasm_bindgen::from_value(value).map_err(js_err)
}

fn to_js<T: Serialize>(value: &T) -> Result<JsValue, JsValue> {
    value
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(js_err)
}

fn engine_error(error: sync_core::EngineError) -> JsValue {
    let js_error = js_sys::Error::new(&error.message);
    let _ = js_sys::Reflect::set(
        js_error.as_ref(),
        &JsValue::from_str("status"),
        &JsValue::from_f64(f64::from(error.status)),
    );
    js_error.into()
}

fn tables_from_js(schema: JsValue) -> Result<sync_core::Tables, JsValue> {
    let schema: serde_json::Value = from_js(schema)?;
    sync_core::Tables::from_zero_schema(&schema).map_err(js_err)
}

fn parse_counter(value: &str, name: &str) -> Result<i64, JsValue> {
    value
        .parse::<i64>()
        .map_err(|error| js_err(format!("invalid {name} {value:?}: {error}")))
}

fn sql_value_from_json(value: serde_json::Value) -> Result<SqlValue, JsValue> {
    match value {
        serde_json::Value::Null => Ok(SqlValue::Null),
        serde_json::Value::Bool(value) => Ok(SqlValue::Integer(i64::from(value))),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(SqlValue::Integer(value))
            } else {
                value
                    .as_f64()
                    .map(SqlValue::Real)
                    .ok_or_else(|| js_err("visibility parameter is not a finite number"))
            }
        }
        serde_json::Value::String(value) => Ok(SqlValue::Text(value)),
        value => Err(js_err(format!(
            "visibility parameter must be a scalar, got {value}"
        ))),
    }
}

/// Initialize the sync engine's durable metadata and triggers. The host calls
/// this inside startup `transactionSync` after application DDL and seed.
#[wasm_bindgen]
pub fn engine_init_schema(db: &JsSyncDb, schema: JsValue) -> Result<(), JsValue> {
    let mut db = WasmDb(db);
    let tables = tables_from_js(schema)?;
    sync_core::init_schema(&mut db, &tables).map_err(js_err)
}

#[derive(Serialize)]
struct CompiledQueryWire {
    sql: String,
    params: Vec<WireValue>,
}

/// Compile a validated Zero query AST for a consumer mutator's transactional
/// `tx.run(...)`. Execution remains in the host-owned application transaction.
#[wasm_bindgen]
pub fn engine_compile_query(schema: JsValue, ast: JsValue) -> Result<JsValue, JsValue> {
    let tables = tables_from_js(schema)?;
    let ast: serde_json::Value = from_js(ast)?;
    let ast = sync_core::query::parse_ast(&ast).map_err(engine_error)?;
    let compiled = sync_core::query::compile(&ast, &tables).map_err(engine_error)?;
    to_js(&CompiledQueryWire {
        sql: compiled.sql,
        params: compiled.params.iter().map(WireValue::from).collect(),
    })
}

/// Production pull entry. The TypeScript host owns `transactionSync`.
#[wasm_bindgen]
pub fn engine_handle_pull(
    db: &JsSyncDb,
    schema: JsValue,
    visibility: JsValue,
    caps: JsValue,
    retain_changes: &str,
    body: JsValue,
    user_id: &str,
) -> Result<JsValue, JsValue> {
    let mut db = WasmDb(db);
    let tables = tables_from_js(schema)?;
    let visibility: Option<VisibilityWire> = from_js(visibility)?;
    if let Some(visibility) = &visibility {
        for filter in &visibility.filters {
            for param in &filter.params {
                sql_value_from_json(param.clone())?;
            }
        }
    }
    let visibility = visibility.map(|visibility| {
        let filters = visibility.filters;
        sync_core::Visibility {
            row_local: visibility.row_local,
            filter: Box::new(move |table, _user_id| {
                let filter = filters.iter().find(|filter| filter.table == table)?;
                Some(sync_core::VisibleFilter {
                    sql: filter.sql.clone(),
                    params: filter
                        .params
                        .clone()
                        .into_iter()
                        .map(sql_value_from_json)
                        .collect::<Result<Vec<_>, _>>()
                        .expect("visibility params validated before engine call"),
                })
            }),
        }
    });
    let caps: CapsWire = from_js(caps)?;
    let body: serde_json::Value = from_js(body)?;
    let result = sync_core::handle_pull(
        &mut db,
        &tables,
        parse_counter(retain_changes, "retention count")?,
        visibility.as_ref(),
        sync_core::Caps {
            max_change_rows: caps.max_change_rows,
            max_change_bytes: caps.max_change_bytes,
        },
        &body,
        user_id,
    )
    .map_err(engine_error)?;
    to_js(&result)
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum PushPlanWire {
    Respond {
        response: serde_json::Value,
    },
    Process {
        #[serde(rename = "clientGroupID")]
        client_group_id: String,
        mutations: Vec<PushMutationWire>,
    },
}

#[derive(Serialize)]
struct PushMutationWire {
    id: String,
    #[serde(rename = "clientID")]
    client_id: String,
    name: String,
    args: Vec<serde_json::Value>,
}

/// Validate an entire push before the host opens the first mutation tx.
#[wasm_bindgen]
pub fn engine_push_validate(body: JsValue) -> Result<JsValue, JsValue> {
    let body: serde_json::Value = from_js(body)?;
    let plan = match sync_core::push_validate(&body).map_err(engine_error)? {
        sync_core::PushPlan::Respond(response) => PushPlanWire::Respond { response },
        sync_core::PushPlan::Process(body) => PushPlanWire::Process {
            client_group_id: body.client_group_id,
            mutations: body
                .mutations
                .into_iter()
                .map(|mutation| PushMutationWire {
                    id: mutation.id.to_string(),
                    client_id: mutation.client_id,
                    name: mutation.name,
                    args: mutation.args,
                })
                .collect(),
        },
    };
    to_js(&plan)
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum PreflightWire {
    Applied,
    Replay { expected: String },
}

#[wasm_bindgen]
pub fn engine_preflight(
    db: &JsSyncDb,
    client_group_id: &str,
    client_id: &str,
    mutation_id: &str,
    user_id: &str,
) -> Result<JsValue, JsValue> {
    let mut db = WasmDb(db);
    let result = sync_core::preflight(
        &mut db,
        client_group_id,
        client_id,
        parse_counter(mutation_id, "mutation id")?,
        user_id,
    )
    .map_err(engine_error)?;
    to_js(&match result {
        sync_core::Preflight::Applied => PreflightWire::Applied,
        sync_core::Preflight::Replay { expected } => PreflightWire::Replay {
            expected: expected.to_string(),
        },
    })
}

#[wasm_bindgen]
pub fn engine_finalize(
    db: &JsSyncDb,
    client_group_id: &str,
    client_id: &str,
    mutation_id: &str,
) -> Result<(), JsValue> {
    let mut db = WasmDb(db);
    sync_core::finalize(
        &mut db,
        client_group_id,
        client_id,
        parse_counter(mutation_id, "mutation id")?,
    )
    .map_err(engine_error)
}

#[wasm_bindgen]
pub fn engine_record_app_error(
    db: &JsSyncDb,
    client_group_id: &str,
    client_id: &str,
    mutation_id: &str,
    user_id: &str,
) -> Result<(), JsValue> {
    let mut db = WasmDb(db);
    sync_core::record_app_error(
        &mut db,
        client_group_id,
        client_id,
        parse_counter(mutation_id, "mutation id")?,
        user_id,
    )
    .map_err(engine_error)
}

#[wasm_bindgen]
pub fn engine_prune(db: &JsSyncDb, retain_changes: &str) -> Result<(), JsValue> {
    let mut db = WasmDb(db);
    sync_core::prune(&mut db, parse_counter(retain_changes, "retention count")?)
        .map_err(engine_error)
}

#[wasm_bindgen]
pub fn engine_invalidate(db: &JsSyncDb) -> Result<(), JsValue> {
    let mut db = WasmDb(db);
    sync_core::invalidate(&mut db).map_err(engine_error)
}

#[derive(Deserialize)]
struct MutationResultWire {
    #[serde(rename = "clientID")]
    client_id: String,
    id: String,
    result: serde_json::Value,
}

#[wasm_bindgen]
pub fn engine_assemble_push_response(results: JsValue) -> Result<JsValue, JsValue> {
    let results: Vec<MutationResultWire> = from_js(results)?;
    let results = results
        .into_iter()
        .map(|result| {
            Ok(sync_core::MutationResult {
                client_id: result.client_id,
                id: parse_counter(&result.id, "mutation id")?,
                result: result.result,
            })
        })
        .collect::<Result<Vec<_>, JsValue>>()?;
    to_js(&sync_core::assemble_push_response(results))
}

#[derive(Serialize)]
struct EngineStateWire {
    watermark: String,
    floor: String,
}

#[wasm_bindgen]
pub fn engine_state(db: &JsSyncDb) -> Result<JsValue, JsValue> {
    let mut db = WasmDb(db);
    to_js(&EngineStateWire {
        watermark: sync_core::watermark(&mut db)
            .map_err(engine_error)?
            .to_string(),
        floor: sync_core::pull::floor(&mut db)
            .map_err(engine_error)?
            .to_string(),
    })
}

#[wasm_bindgen]
pub fn engine_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
