// the narrow synchronous database boundary every host adapts:
// rusqlite on native, a wasm-bindgen javascript adapter over
// ctx.storage.sql on cloudflare. deliberately tiny.

use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Row {
    pub columns: Arc<[String]>,
    pub values: Vec<SqlValue>,
}

impl Row {
    pub fn get(&self, name: &str) -> Option<&SqlValue> {
        self.columns
            .iter()
            .position(|c| c == name)
            .map(|i| &self.values[i])
    }
}

#[derive(Debug, thiserror::Error)]
#[error("db error: {0}")]
pub struct DbError(pub String);

// synchronous, positional-`?`-only. the host opens/commits the transaction
// around engine calls; implementations must reject transaction statements.
pub trait SyncDb {
    fn exec(&mut self, sql: &str, params: &[SqlValue]) -> Result<(), DbError>;
    fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<Vec<Row>, DbError>;
}
