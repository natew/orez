//! Apply ordered full-row images from the ZeroSqlDO `/changes` feed.
//!
//! The host owns the surrounding transaction. Application-table triggers are
//! deliberately left enabled, so upstream writes enter `_zsync_changes` by the
//! exact same path as local mutator writes and existing pull/CVR logic remains
//! unaware of the upstream watermark domain.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::db::{SqlValue, SyncDb};
use crate::error::EngineError;
use crate::schema::{TableSpec, Tables, quote_ident};
use crate::store;
use crate::value::ZeroColumnType;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpstreamChange {
    pub watermark: i64,
    pub table_name: String,
    pub op: String,
    pub row_data: Option<Map<String, Value>>,
    pub old_data: Option<Map<String, Value>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpstreamBatch {
    /// Current head reported by the feed. It is informational: a limited page
    /// only advances to its final applied change, never past unseen changes.
    pub watermark: i64,
    pub changes: Vec<UpstreamChange>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ApplyUpstreamResult {
    pub watermark: i64,
    pub applied: usize,
    pub caught_up: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpstreamSnapshot {
    pub watermark: i64,
    pub tables: BTreeMap<String, Vec<Map<String, Value>>>,
}

pub fn upstream_watermark(db: &mut dyn SyncDb) -> Result<i64, EngineError> {
    let rows = db.query(
        "SELECT CAST(upstream_watermark AS TEXT) FROM _zsync_meta WHERE lock = 1",
        &[],
    )?;
    match rows.first().and_then(|row| row.values.first()) {
        Some(SqlValue::Text(value)) => value
            .parse()
            .map_err(|_| EngineError::internal("upstream watermark is not an integer")),
        Some(SqlValue::Integer(value)) => Ok(*value),
        _ => Ok(0),
    }
}

fn schema_refresh(message: impl Into<String>) -> EngineError {
    EngineError::conflict(format!("schema refresh required: {}", message.into()))
}

fn sql_value(ty: ZeroColumnType, value: &Value) -> Result<SqlValue, EngineError> {
    // upstream rowData already contains decoded JSON values. sqlite stores a
    // zero json column as encoded text so scalar strings remain distinguishable
    // from numbers, booleans, null, and object-looking text during hydration.
    if ty == ZeroColumnType::Json && !value.is_null() {
        return serde_json::to_string(value)
            .map(SqlValue::Text)
            .map_err(|error| EngineError::bad_request(error.to_string()));
    }
    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::Bool(value) => SqlValue::Integer(i64::from(*value)),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                SqlValue::Integer(value)
            } else {
                SqlValue::Real(value.as_f64().ok_or_else(|| {
                    EngineError::bad_request("upstream row contains an invalid number")
                })?)
            }
        }
        Value::String(value) => SqlValue::Text(value.clone()),
        Value::Array(_) | Value::Object(_) => SqlValue::Text(
            serde_json::to_string(value)
                .map_err(|error| EngineError::bad_request(error.to_string()))?,
        ),
    })
}

fn validate_columns(
    table: &str,
    spec: &TableSpec,
    row: &Map<String, Value>,
) -> Result<(), EngineError> {
    for column in row.keys() {
        if spec.column_type(column).is_none() {
            return Err(schema_refresh(format!("unknown column {table}.{column}")));
        }
    }
    Ok(())
}

fn delete_row(
    db: &mut dyn SyncDb,
    table: &str,
    spec: &TableSpec,
    row: &Map<String, Value>,
) -> Result<(), EngineError> {
    validate_columns(table, spec, row)?;
    let mut params = Vec::with_capacity(spec.primary_key.len());
    for column in &spec.primary_key {
        let value = row.get(column).ok_or_else(|| {
            EngineError::bad_request(format!(
                "upstream delete for {table} is missing primary key {column}"
            ))
        })?;
        params.push(sql_value(
            spec.column_type(column).unwrap_or(ZeroColumnType::String),
            value,
        )?);
    }
    let predicate = spec
        .primary_key
        .iter()
        .map(|column| format!("{} IS ?", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(" AND ");
    db.exec(
        &format!("DELETE FROM {} WHERE {predicate}", quote_ident(table)),
        &params,
    )?;
    Ok(())
}

fn same_key(spec: &TableSpec, a: &Map<String, Value>, b: &Map<String, Value>) -> bool {
    spec.primary_key
        .iter()
        .all(|column| a.get(column) == b.get(column))
}

fn upsert_row(
    db: &mut dyn SyncDb,
    table: &str,
    spec: &TableSpec,
    row: &Map<String, Value>,
) -> Result<(), EngineError> {
    validate_columns(table, spec, row)?;
    let columns = spec
        .columns
        .iter()
        .map(|(column, _)| column)
        .collect::<Vec<_>>();
    let mut params = Vec::with_capacity(columns.len());
    for (column, ty) in &spec.columns {
        let value = row.get(column).ok_or_else(|| {
            EngineError::bad_request(format!(
                "upstream full row for {table} is missing column {column}"
            ))
        })?;
        params.push(sql_value(*ty, value)?);
    }
    let quoted = columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>();
    let updates = columns
        .iter()
        .filter(|column| !spec.primary_key.contains(column))
        .map(|column| {
            let column = quote_ident(column);
            format!("{column} = excluded.{column}")
        })
        .collect::<Vec<_>>();
    let conflict = if updates.is_empty() {
        "DO NOTHING".to_string()
    } else {
        format!("DO UPDATE SET {}", updates.join(", "))
    };
    db.exec(
        &format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {conflict}",
            quote_ident(table),
            quoted.join(", "),
            vec!["?"; columns.len()].join(", "),
            spec.primary_key
                .iter()
                .map(|column| quote_ident(column))
                .collect::<Vec<_>>()
                .join(", "),
        ),
        &params,
    )?;
    Ok(())
}

/// Apply one feed page. The caller must wrap this call in one transaction.
pub fn apply_upstream(
    db: &mut dyn SyncDb,
    tables: &Tables,
    batch: &UpstreamBatch,
) -> Result<ApplyUpstreamResult, EngineError> {
    let mut cursor = upstream_watermark(db)?;
    let initial_cursor = cursor;
    let mut applied = 0;
    let mut last_batch_watermark = None;
    for change in &batch.changes {
        if last_batch_watermark.is_some_and(|previous| change.watermark <= previous) {
            return Err(EngineError::bad_request("upstream changes are not ordered"));
        }
        last_batch_watermark = Some(change.watermark);
        if change.watermark <= cursor {
            continue;
        }
        // subset replica: consume changes for tables this host does not model.
        // advance the cursor so ingest makes durable progress past them; the row
        // is not materialized. mirrors apply_upstream_snapshot's table skip.
        let Some(spec) = tables.get(&change.table_name) else {
            cursor = change.watermark;
            continue;
        };
        match change.op.as_str() {
            "INSERT" => upsert_row(
                db,
                &change.table_name,
                spec,
                change.row_data.as_ref().ok_or_else(|| {
                    EngineError::bad_request("upstream insert is missing rowData")
                })?,
            )?,
            "UPDATE" => {
                let row = change.row_data.as_ref().ok_or_else(|| {
                    EngineError::bad_request("upstream update is missing rowData")
                })?;
                if let Some(old) = &change.old_data
                    && !same_key(spec, old, row)
                {
                    delete_row(db, &change.table_name, spec, old)?;
                }
                upsert_row(db, &change.table_name, spec, row)?;
            }
            "DELETE" => delete_row(
                db,
                &change.table_name,
                spec,
                change.old_data.as_ref().ok_or_else(|| {
                    EngineError::bad_request("upstream delete is missing oldData")
                })?,
            )?,
            op => {
                return Err(EngineError::bad_request(format!(
                    "unknown upstream op {op}"
                )));
            }
        }
        cursor = change.watermark;
        applied += 1;
    }
    if cursor > initial_cursor {
        db.exec(
            "UPDATE _zsync_meta SET upstream_watermark = ? WHERE lock = 1",
            &[store::counter(cursor)],
        )?;
    }
    Ok(ApplyUpstreamResult {
        watermark: cursor,
        applied,
        caught_up: cursor >= batch.watermark,
    })
}

/// Atomically replace application rows after the feed's retained floor passes
/// this replica. Normal triggers populate the ordinary engine change log.
pub fn apply_upstream_snapshot(
    db: &mut dyn SyncDb,
    tables: &Tables,
    snapshot: &UpstreamSnapshot,
) -> Result<ApplyUpstreamResult, EngineError> {
    // the upstream feed is authoritative for the whole application schema, but
    // a host may model only a subset of it (server-only tables like `user` are
    // deliberately excluded from client sync). ignore snapshot tables this host
    // does not model rather than rejecting the rebuild; every modeled table
    // still rebuilds atomically. column drift on a modeled table stays a hard
    // refresh via upsert_row's validation.
    for (table, _) in tables.iter() {
        db.exec(&format!("DELETE FROM {}", quote_ident(table)), &[])?;
    }
    let mut applied = 0;
    for (table, rows) in &snapshot.tables {
        let Some(spec) = tables.get(table) else {
            continue;
        };
        for row in rows {
            upsert_row(db, table, spec, row)?;
            applied += 1;
        }
    }
    db.exec(
        "UPDATE _zsync_meta SET upstream_watermark = ? WHERE lock = 1",
        &[store::counter(snapshot.watermark)],
    )?;
    Ok(ApplyUpstreamResult {
        watermark: snapshot.watermark,
        applied,
        caught_up: true,
    })
}
