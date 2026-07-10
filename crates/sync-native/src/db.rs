// rusqlite adapter for sync-core's SyncDb boundary.
//
// one of these wraps the single per-namespace connection owned by the
// namespace worker thread (see namespace.rs), so every method runs on that
// one writer — the plan's "one writer per namespace" invariant is structural,
// not lock-based. positional `?` bindings only (matches the DO SqlStorage
// constraint the engine is written against). the SyncDb boundary is
// exec/query only; the HOST owns transaction begin/commit/rollback (see
// engine.rs), because the CF host must orchestrate the same steps around an
// async JS mutator and so the engine can't drive tx boundaries.

use rusqlite::Connection;
use rusqlite::types::{Value, ValueRef};
use std::sync::Arc;

use sync_core::{DbError, Row, SqlValue, SyncDb};

pub struct RusqliteDb<'c> {
    conn: &'c Connection,
}

impl<'c> RusqliteDb<'c> {
    pub fn new(conn: &'c Connection) -> Self {
        Self { conn }
    }
}

fn to_value(v: &SqlValue) -> Value {
    match v {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(i) => Value::Integer(*i),
        SqlValue::Real(r) => Value::Real(*r),
        SqlValue::Text(s) => Value::Text(s.clone()),
        SqlValue::Blob(b) => Value::Blob(b.clone()),
    }
}

fn from_ref(v: ValueRef<'_>) -> SqlValue {
    match v {
        ValueRef::Null => SqlValue::Null,
        ValueRef::Integer(i) => SqlValue::Integer(i),
        ValueRef::Real(r) => SqlValue::Real(r),
        ValueRef::Text(t) => SqlValue::Text(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(b) => SqlValue::Blob(b.to_vec()),
    }
}

fn map_err(e: rusqlite::Error) -> DbError {
    DbError(e.to_string())
}

impl<'c> SyncDb for RusqliteDb<'c> {
    fn exec(&mut self, sql: &str, params: &[SqlValue]) -> Result<(), DbError> {
        let bound: Vec<Value> = params.iter().map(to_value).collect();
        self.conn
            .execute(sql, rusqlite::params_from_iter(bound.iter()))
            .map(|_| ())
            .map_err(map_err)
    }

    fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<Vec<Row>, DbError> {
        let bound: Vec<Value> = params.iter().map(to_value).collect();
        let mut stmt = self.conn.prepare(sql).map_err(map_err)?;
        let columns: Arc<[String]> = stmt
            .column_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .into();
        let col_count = columns.len();
        let mut rows = stmt
            .query(rusqlite::params_from_iter(bound.iter()))
            .map_err(map_err)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(map_err)? {
            let mut values = Vec::with_capacity(col_count);
            for i in 0..col_count {
                values.push(from_ref(row.get_ref(i).map_err(map_err)?));
            }
            out.push(Row {
                columns: columns.clone(),
                values,
            });
        }
        Ok(out)
    }
}
