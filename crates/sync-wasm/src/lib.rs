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

fn one_text(row: &Row, name: &str) -> Result<String, JsValue> {
    match row.get(name) {
        Some(SqlValue::Text(value)) => Ok(value.clone()),
        value => Err(js_err(format!(
            "expected text column {name}, got {value:?}"
        ))),
    }
}

/// Initialize only probe tables. The host wraps this in `transactionSync`.
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PullSnapshot {
    lmid: String,
    balance: f64,
}

/// Representative synchronous pull read performed wholly inside the host tx.
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
