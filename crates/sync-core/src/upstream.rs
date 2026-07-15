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

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotState {
    Paging,
    CatchingUp,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotProgress {
    pub generation: i64,
    pub start_watermark: i64,
    pub table: Option<String>,
    pub cursor: Option<String>,
    pub state: SnapshotState,
    pub catchup_watermark: i64,
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

fn delete_row_from(
    db: &mut dyn SyncDb,
    table: &str,
    target: &str,
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
        &format!("DELETE FROM {} WHERE {predicate}", quote_ident(target)),
        &params,
    )?;
    Ok(())
}

fn same_key(spec: &TableSpec, a: &Map<String, Value>, b: &Map<String, Value>) -> bool {
    spec.primary_key
        .iter()
        .all(|column| a.get(column) == b.get(column))
}

fn upsert_row_into(
    db: &mut dyn SyncDb,
    table: &str,
    target: &str,
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
            quote_ident(target),
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

fn upsert_row(
    db: &mut dyn SyncDb,
    table: &str,
    spec: &TableSpec,
    row: &Map<String, Value>,
) -> Result<(), EngineError> {
    upsert_row_into(db, table, table, spec, row)
}

fn apply_change_to(
    db: &mut dyn SyncDb,
    target: &str,
    spec: &TableSpec,
    change: &UpstreamChange,
) -> Result<(), EngineError> {
    match change.op.as_str() {
        "INSERT" => upsert_row_into(
            db,
            &change.table_name,
            target,
            spec,
            change
                .row_data
                .as_ref()
                .ok_or_else(|| EngineError::bad_request("upstream insert is missing rowData"))?,
        ),
        "UPDATE" => {
            let row = change
                .row_data
                .as_ref()
                .ok_or_else(|| EngineError::bad_request("upstream update is missing rowData"))?;
            if let Some(old) = &change.old_data
                && !same_key(spec, old, row)
            {
                delete_row_from(db, &change.table_name, target, spec, old)?;
            }
            upsert_row_into(db, &change.table_name, target, spec, row)
        }
        "DELETE" => delete_row_from(
            db,
            &change.table_name,
            target,
            spec,
            change
                .old_data
                .as_ref()
                .ok_or_else(|| EngineError::bad_request("upstream delete is missing oldData"))?,
        ),
        op => Err(EngineError::bad_request(format!(
            "unknown upstream op {op}"
        ))),
    }
}

const SNAPSHOT_CLEANUP_BATCH_ROWS: usize = 2_000;

fn sorted_table_names(tables: &Tables) -> Vec<&str> {
    let mut names = tables.iter().map(|(name, _)| name).collect::<Vec<_>>();
    names.sort_unstable();
    names
}

fn stage_table_name(generation: i64, table: &str) -> String {
    format!("_zsync_stage_{generation}_{table}")
}

fn required_text<'a>(row: &'a crate::db::Row, column: &str) -> Result<&'a str, EngineError> {
    match row.get(column) {
        Some(SqlValue::Text(value)) => Ok(value),
        _ => Err(EngineError::internal(format!(
            "snapshot progress column {column} is unreadable"
        ))),
    }
}

fn optional_text(row: &crate::db::Row, column: &str) -> Result<Option<String>, EngineError> {
    match row.get(column) {
        Some(SqlValue::Text(value)) => Ok(Some(value.clone())),
        Some(SqlValue::Null) => Ok(None),
        _ => Err(EngineError::internal(format!(
            "snapshot progress column {column} is unreadable"
        ))),
    }
}

fn progress_from_rows(rows: Vec<crate::db::Row>) -> Result<Option<SnapshotProgress>, EngineError> {
    if rows.len() > 1 {
        return Err(EngineError::internal(
            "multiple active snapshot generations exist",
        ));
    }
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    if !matches!(row.get("active"), Some(SqlValue::Text(active)) if active == "1") {
        return Err(EngineError::internal(
            "incomplete snapshot generation is not marked active",
        ));
    }
    let parse_counter = |column: &str| -> Result<i64, EngineError> {
        required_text(row, column)?
            .parse()
            .map_err(|_| EngineError::internal(format!("snapshot {column} is not an integer")))
    };
    let state = match required_text(row, "state")? {
        "paging" => SnapshotState::Paging,
        "catching_up" => SnapshotState::CatchingUp,
        state => {
            return Err(EngineError::internal(format!(
                "active snapshot generation has invalid state {state:?}"
            )));
        }
    };
    Ok(Some(SnapshotProgress {
        generation: parse_counter("generation")?,
        start_watermark: parse_counter("startWatermark")?,
        table: optional_text(row, "tableName")?,
        cursor: optional_text(row, "cursor")?,
        state,
        catchup_watermark: parse_counter("catchupWatermark")?,
    }))
}

const PROGRESS_SELECT: &str = "SELECT
    CAST(generation AS TEXT) AS generation,
    CAST(startWatermark AS TEXT) AS startWatermark,
    tableName,
    cursor,
    state,
    CAST(catchupWatermark AS TEXT) AS catchupWatermark,
    CAST(active AS TEXT) AS active
FROM _zsync_snapshot_progress";

/// Strictly read the active resumable generation. A storage or shape error is
/// never treated as an absent generation.
pub fn read_snapshot_progress(
    db: &mut dyn SyncDb,
) -> Result<Option<SnapshotProgress>, EngineError> {
    progress_from_rows(db.query(
        &format!(
            "{PROGRESS_SELECT}
             WHERE active IS NOT NULL OR state IN ('paging', 'catching_up')"
        ),
        &[],
    )?)
}

fn generation_progress(
    db: &mut dyn SyncDb,
    generation: i64,
) -> Result<SnapshotProgress, EngineError> {
    progress_from_rows(db.query(
        &format!("{PROGRESS_SELECT} WHERE generation = ? AND active = 1"),
        &[store::counter(generation)],
    )?)?
    .ok_or_else(|| {
        EngineError::conflict(format!(
            "snapshot generation {generation} is not the active generation"
        ))
    })
}

fn finish_cleanup_generation(db: &mut dyn SyncDb, generation: i64) -> Result<(), EngineError> {
    let pending = db.query(
        "SELECT 1 FROM _zsync_snapshot_cleanup WHERE generation = ? LIMIT 1",
        &[store::counter(generation)],
    )?;
    if pending.is_empty() {
        db.exec(
            "DELETE FROM _zsync_snapshot_progress
             WHERE generation = ? AND state = 'abandoned'",
            &[store::counter(generation)],
        )?;
    }
    Ok(())
}

fn cleanup_one_snapshot_batch(db: &mut dyn SyncDb) -> Result<(), EngineError> {
    let rows = db.query(
        "SELECT CAST(generation AS TEXT) AS generation, stageName
         FROM _zsync_snapshot_cleanup
         ORDER BY generation, stageName
         LIMIT 1",
        &[],
    )?;
    let Some(row) = rows.first() else {
        return Ok(());
    };
    let generation = required_text(row, "generation")?
        .parse::<i64>()
        .map_err(|_| EngineError::internal("cleanup generation is not an integer"))?;
    let stage = required_text(row, "stageName")?;
    let exists = !db
        .query(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?",
            &[SqlValue::Text(stage.to_string())],
        )?
        .is_empty();
    if !exists {
        db.exec(
            "DELETE FROM _zsync_snapshot_cleanup WHERE generation = ? AND stageName = ?",
            &[
                store::counter(generation),
                SqlValue::Text(stage.to_string()),
            ],
        )?;
        return finish_cleanup_generation(db, generation);
    }

    let mut primary_key = db
        .query(&format!("PRAGMA table_info({})", quote_ident(stage)), &[])?
        .into_iter()
        .filter_map(|row| match (row.get("name"), row.get("pk")) {
            (Some(SqlValue::Text(name)), Some(SqlValue::Integer(position))) if *position > 0 => {
                Some((*position, name.clone()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    primary_key.sort_by_key(|(position, _)| *position);
    let stage_quoted = quote_ident(stage);
    let predicate = if primary_key.is_empty() {
        format!("rowid IN (SELECT rowid FROM {stage_quoted} LIMIT {SNAPSHOT_CLEANUP_BATCH_ROWS})")
    } else if primary_key.len() == 1 {
        let column = quote_ident(&primary_key[0].1);
        format!(
            "{column} IN (SELECT {column} FROM {stage_quoted} LIMIT {SNAPSHOT_CLEANUP_BATCH_ROWS})"
        )
    } else {
        let columns = primary_key
            .iter()
            .map(|(_, name)| quote_ident(name))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "({columns}) IN (SELECT {columns} FROM {stage_quoted} LIMIT {SNAPSHOT_CLEANUP_BATCH_ROWS})"
        )
    };
    db.exec(
        &format!("DELETE FROM {stage_quoted} WHERE {predicate}"),
        &[],
    )?;
    if db
        .query(&format!("SELECT 1 FROM {stage_quoted} LIMIT 1"), &[])?
        .is_empty()
    {
        db.exec(&format!("DROP TABLE {stage_quoted}"), &[])?;
        db.exec(
            "DELETE FROM _zsync_snapshot_cleanup WHERE generation = ? AND stageName = ?",
            &[
                store::counter(generation),
                SqlValue::Text(stage.to_string()),
            ],
        )?;
        finish_cleanup_generation(db, generation)?;
    }
    Ok(())
}

fn abandon_active_generation(db: &mut dyn SyncDb) -> Result<(), EngineError> {
    let Some(progress) = read_snapshot_progress(db)? else {
        return Ok(());
    };
    db.exec(
        "UPDATE _zsync_snapshot_progress
         SET state = 'abandoned', active = NULL
         WHERE generation = ? AND active = 1",
        &[store::counter(progress.generation)],
    )?;
    let stages = db.query(
        "SELECT name FROM sqlite_schema
         WHERE type = 'table' AND name GLOB ?
         ORDER BY name",
        &[SqlValue::Text(format!(
            "_zsync_stage_{}_*",
            progress.generation
        ))],
    )?;
    for row in &stages {
        db.exec(
            "INSERT INTO _zsync_snapshot_cleanup (generation, stageName) VALUES (?, ?)
             ON CONFLICT (generation, stageName) DO NOTHING",
            &[
                store::counter(progress.generation),
                SqlValue::Text(required_text(row, "name")?.to_string()),
            ],
        )?;
    }
    finish_cleanup_generation(db, progress.generation)
}

fn next_generation(db: &mut dyn SyncDb) -> Result<i64, EngineError> {
    let rows = db.query(
        "SELECT CAST(nextGeneration AS TEXT) AS nextGeneration
         FROM _zsync_snapshot_generation WHERE lock = 1",
        &[],
    )?;
    if rows.len() != 1 {
        return Err(EngineError::internal(
            "snapshot generation counter is missing or duplicated",
        ));
    }
    let generation = required_text(&rows[0], "nextGeneration")?
        .parse::<i64>()
        .map_err(|_| EngineError::internal("snapshot generation counter is not an integer"))?;
    let next = generation
        .checked_add(1)
        .ok_or_else(|| EngineError::internal("snapshot generation counter overflow"))?;
    db.exec(
        "UPDATE _zsync_snapshot_generation SET nextGeneration = ? WHERE lock = 1",
        &[store::counter(next)],
    )?;
    Ok(generation)
}

fn table_create_sql(db: &mut dyn SyncDb, table: &str) -> Result<String, EngineError> {
    let rows = db.query(
        "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?",
        &[SqlValue::Text(table.to_string())],
    )?;
    if rows.len() != 1 {
        return Err(schema_refresh(format!(
            "modeled table {table} is missing from the live database"
        )));
    }
    match rows[0].get("sql") {
        Some(SqlValue::Text(sql)) => Ok(sql.clone()),
        _ => Err(EngineError::internal(format!(
            "modeled table {table} has unreadable CREATE TABLE SQL"
        ))),
    }
}

fn clone_index_sql(sql: &str, stage: &str, stage_index: &str) -> Result<String, EngineError> {
    let upper = sql.to_ascii_uppercase();
    let on = upper
        .find(" ON ")
        .ok_or_else(|| EngineError::internal("stored index SQL has no ON clause"))?;
    let suffix_start = sql[on + 4..]
        .find('(')
        .map(|offset| on + 4 + offset)
        .ok_or_else(|| EngineError::internal("stored index SQL has no column expression"))?;
    let unique = if upper[..on].contains("UNIQUE") {
        "UNIQUE "
    } else {
        ""
    };
    Ok(format!(
        "CREATE {unique}INDEX {} ON {} {}",
        quote_ident(stage_index),
        quote_ident(stage),
        &sql[suffix_start..]
    ))
}

fn clone_live_table(db: &mut dyn SyncDb, generation: i64, table: &str) -> Result<(), EngineError> {
    let sql = table_create_sql(db, table)?;
    let body = sql.find('(').ok_or_else(|| {
        EngineError::internal(format!(
            "modeled table {table} has invalid CREATE TABLE SQL"
        ))
    })?;
    let stage = stage_table_name(generation, table);
    db.exec(
        &format!("CREATE TABLE {} {}", quote_ident(&stage), &sql[body..]),
        &[],
    )?;

    let indexes = db.query(
        "SELECT sql FROM sqlite_schema
         WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL
         ORDER BY name",
        &[SqlValue::Text(table.to_string())],
    )?;
    for (index, row) in indexes.iter().enumerate() {
        let sql = required_text(row, "sql")?;
        let stage_index = format!("{stage}_idx_{index}");
        db.exec(&clone_index_sql(sql, &stage, &stage_index)?, &[])?;
    }
    Ok(())
}

fn reject_foreign_keys(db: &mut dyn SyncDb, tables: &Tables) -> Result<(), EngineError> {
    let mut foreign_key_tables = Vec::new();
    for table in sorted_table_names(tables) {
        if !db
            .query(
                &format!("PRAGMA foreign_key_list({})", quote_ident(table)),
                &[],
            )?
            .is_empty()
        {
            foreign_key_tables.push(table);
        }
    }
    if foreign_key_tables.is_empty() {
        return Ok(());
    }
    Err(EngineError::conflict(format!(
        "paged snapshots do not support foreign keys in sync replica tables: {}; remove REFERENCES constraints from the sync-host DDL",
        foreign_key_tables.join(", ")
    )))
}

/// Create a fresh staged generation. `start_watermark` is the source current
/// watermark captured from the first snapshot-page response. The caller owns
/// one transaction around this call.
pub fn begin_snapshot_generation(
    db: &mut dyn SyncDb,
    tables: &Tables,
    start_watermark: i64,
) -> Result<SnapshotProgress, EngineError> {
    if start_watermark < 0 {
        return Err(EngineError::bad_request(
            "snapshot start watermark must be non-negative",
        ));
    }
    cleanup_one_snapshot_batch(db)?;
    reject_foreign_keys(db, tables)?;
    abandon_active_generation(db)?;
    let generation = next_generation(db)?;
    let names = sorted_table_names(tables);
    for table in &names {
        clone_live_table(db, generation, table)?;
    }
    let (table, state) = match names.first() {
        Some(table) => (SqlValue::Text((*table).to_string()), "paging"),
        None => (SqlValue::Null, "catching_up"),
    };
    db.exec(
        "INSERT INTO _zsync_snapshot_progress
         (generation, startWatermark, tableName, cursor, state, catchupWatermark, active)
         VALUES (?, ?, ?, NULL, ?, ?, 1)",
        &[
            store::counter(generation),
            store::counter(start_watermark),
            table,
            SqlValue::Text(state.to_string()),
            store::counter(start_watermark),
        ],
    )?;
    generation_progress(db, generation)
}

/// Apply one bounded source page to its stage table and durably record the
/// opaque source cursor in the same host-owned transaction.
pub fn apply_snapshot_page(
    db: &mut dyn SyncDb,
    tables: &Tables,
    generation: i64,
    table: &str,
    rows: &[Map<String, Value>],
    next_cursor: Option<&str>,
) -> Result<SnapshotProgress, EngineError> {
    cleanup_one_snapshot_batch(db)?;
    let progress = generation_progress(db, generation)?;
    if progress.state != SnapshotState::Paging {
        return Err(EngineError::conflict(format!(
            "snapshot generation {generation} is not paging"
        )));
    }
    if progress.table.as_deref() != Some(table) {
        return Err(EngineError::conflict(format!(
            "snapshot generation {generation} expects table {:?}, got {table:?}",
            progress.table
        )));
    }
    let spec = tables
        .get(table)
        .ok_or_else(|| schema_refresh(format!("snapshot table {table} is not modeled")))?;
    let stage = stage_table_name(generation, table);
    for row in rows {
        upsert_row_into(db, table, &stage, spec, row)?;
    }

    if let Some(cursor) = next_cursor {
        db.exec(
            "UPDATE _zsync_snapshot_progress SET cursor = ?
             WHERE generation = ? AND active = 1",
            &[
                SqlValue::Text(cursor.to_string()),
                store::counter(generation),
            ],
        )?;
    } else {
        let names = sorted_table_names(tables);
        let position = names
            .iter()
            .position(|name| *name == table)
            .ok_or_else(|| schema_refresh(format!("snapshot table {table} is not modeled")))?;
        if let Some(next_table) = names.get(position + 1) {
            db.exec(
                "UPDATE _zsync_snapshot_progress
                 SET tableName = ?, cursor = NULL
                 WHERE generation = ? AND active = 1",
                &[
                    SqlValue::Text((*next_table).to_string()),
                    store::counter(generation),
                ],
            )?;
        } else {
            db.exec(
                "UPDATE _zsync_snapshot_progress
                 SET tableName = NULL, cursor = NULL, state = 'catching_up'
                 WHERE generation = ? AND active = 1",
                &[store::counter(generation)],
            )?;
        }
    }
    generation_progress(db, generation)
}

/// Replay one bounded change-feed page into staging. This is deliberately a
/// separate path from live `apply_upstream`: it advances only the generation
/// cursor and never touches the live table set or live upstream watermark.
pub fn apply_snapshot_changes(
    db: &mut dyn SyncDb,
    tables: &Tables,
    generation: i64,
    batch: &UpstreamBatch,
) -> Result<ApplyUpstreamResult, EngineError> {
    cleanup_one_snapshot_batch(db)?;
    let progress = generation_progress(db, generation)?;
    if progress.state != SnapshotState::CatchingUp {
        return Err(EngineError::conflict(format!(
            "snapshot generation {generation} has not finished paging"
        )));
    }
    let mut cursor = progress.catchup_watermark;
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
        let Some(spec) = tables.get(&change.table_name) else {
            cursor = change.watermark;
            continue;
        };
        let stage = stage_table_name(generation, &change.table_name);
        apply_change_to(db, &stage, spec, change)?;
        cursor = change.watermark;
        applied += 1;
    }
    if cursor > initial_cursor {
        db.exec(
            "UPDATE _zsync_snapshot_progress SET catchupWatermark = ?
             WHERE generation = ? AND active = 1",
            &[store::counter(cursor), store::counter(generation)],
        )?;
    }
    Ok(ApplyUpstreamResult {
        watermark: cursor,
        applied,
        caught_up: cursor >= batch.watermark,
    })
}

fn live_trigger_sql(db: &mut dyn SyncDb, table: &str) -> Result<Vec<String>, EngineError> {
    db.query(
        "SELECT sql FROM sqlite_schema
         WHERE type = 'trigger' AND tbl_name = ? AND sql IS NOT NULL
         ORDER BY name",
        &[SqlValue::Text(table.to_string())],
    )?
    .iter()
    .map(|row| required_text(row, "sql").map(str::to_string))
    .collect()
}

/// Atomically swap every completed stage table into the live namespace, bump
/// the client epoch, and advance the live upstream watermark. The supplied
/// watermark must equal the drained generation-local catch-up cursor.
pub fn finalize_snapshot_generation(
    db: &mut dyn SyncDb,
    tables: &Tables,
    generation: i64,
    watermark: i64,
) -> Result<ApplyUpstreamResult, EngineError> {
    cleanup_one_snapshot_batch(db)?;
    let progress = generation_progress(db, generation)?;
    if progress.state != SnapshotState::CatchingUp {
        return Err(EngineError::conflict(format!(
            "snapshot generation {generation} has not finished paging"
        )));
    }
    if watermark != progress.catchup_watermark {
        return Err(EngineError::conflict(format!(
            "snapshot generation {generation} catch-up is at {}, not drain watermark {watermark}",
            progress.catchup_watermark
        )));
    }

    let names = sorted_table_names(tables);
    let triggers = names
        .iter()
        .map(|table| live_trigger_sql(db, table))
        .collect::<Result<Vec<_>, _>>()?;
    for (table, trigger_sql) in names.iter().zip(triggers) {
        db.exec(&format!("DROP TABLE {}", quote_ident(table)), &[])?;
        db.exec(
            &format!(
                "ALTER TABLE {} RENAME TO {}",
                quote_ident(&stage_table_name(generation, table)),
                quote_ident(table)
            ),
            &[],
        )?;
        for sql in trigger_sql {
            db.exec(&sql, &[])?;
        }
    }
    store::invalidate(db)?;
    db.exec(
        "UPDATE _zsync_meta SET upstream_watermark = ? WHERE lock = 1",
        &[store::counter(watermark)],
    )?;
    db.exec(
        "UPDATE _zsync_snapshot_progress
         SET state = 'complete', active = NULL
         WHERE generation = ? AND active = 1",
        &[store::counter(generation)],
    )?;
    db.exec(
        "DELETE FROM _zsync_snapshot_progress WHERE generation = ? AND state = 'complete'",
        &[store::counter(generation)],
    )?;
    Ok(ApplyUpstreamResult {
        watermark,
        applied: 0,
        caught_up: true,
    })
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
        apply_change_to(db, &change.table_name, spec, change)?;
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
