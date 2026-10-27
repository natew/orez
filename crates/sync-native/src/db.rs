// rusqlite adapter for sync-core's SyncDb boundary.
//
// one of these wraps the single per-namespace connection owned by the
// namespace worker thread (see namespace.rs), so every method runs on that
// one writer — the plan's "one writer per namespace" invariant is structural,
// not lock-based. positional `?` bindings only (matches the DO SqlStorage
// constraint the engine is written against). statements come from the
// connection's statement cache (sized in namespace.rs open_connection): the
// engine replays the same few hundred statements, and re-parsing the
// trigger-bearing writes on every call took about 15% of a busy worker
// thread. the SyncDb boundary is
// exec/query only; the HOST owns transaction begin/commit/rollback (see
// engine.rs), because the CF host must orchestrate the same steps around an
// async JS mutator and so the engine can't drive tx boundaries.

use rusqlite::Connection;
use rusqlite::types::{Value, ValueRef};
use std::sync::Arc;

use sync_core::schema::{
    Tables, init_number_storage_guards, invalid_number_cells, validate_number_storage_changes,
};
use sync_core::{DbError, Row, SqlValue, SyncDb};

pub struct RusqliteDb<'c> {
    conn: &'c Connection,
    tables: Option<&'c Tables>,
}

impl<'c> RusqliteDb<'c> {
    pub fn new(conn: &'c Connection) -> Self {
        Self { conn, tables: None }
    }

    pub fn guarded(conn: &'c Connection, tables: &'c Tables) -> Self {
        Self {
            conn,
            tables: Some(tables),
        }
    }

    fn schema_write<T>(
        &mut self,
        sql: &str,
        run: impl FnOnce(&mut Self) -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        let Some(tables) = self.tables else {
            return run(self);
        };
        let upper = sql.to_ascii_uppercase();
        if !upper
            .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
            .any(|word| matches!(word, "CREATE" | "ALTER" | "DROP" | "REINDEX"))
        {
            return run(self);
        }
        if upper.contains("_ZSYNC_NUMBER_") {
            return Err(DbError("sync storage guards are host-owned".to_string()));
        }
        self.conn
            .execute_batch("SAVEPOINT _orez_guard_schema")
            .map_err(map_err)?;
        let result = (|| {
            let mut raw = RusqliteDb::new(self.conn);
            let before = invalid_number_cells(&mut raw, tables)?;
            let result = run(&mut raw)?;
            validate_number_storage_changes(&mut raw, tables, &before)?;
            init_number_storage_guards(&mut raw, tables)?;
            Ok(result)
        })();
        match result {
            Ok(value) => {
                self.conn
                    .execute_batch("RELEASE _orez_guard_schema")
                    .map_err(map_err)?;
                Ok(value)
            }
            Err(error) => {
                self.conn
                    .execute_batch("ROLLBACK TO _orez_guard_schema; RELEASE _orez_guard_schema")
                    .map_err(map_err)?;
                Err(error)
            }
        }
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
        self.schema_write(sql, |db| db.exec_raw(sql, params))
    }

    fn query(&mut self, sql: &str, params: &[SqlValue]) -> Result<Vec<Row>, DbError> {
        self.schema_write(sql, |db| db.query_raw(sql, params))
    }
}

impl RusqliteDb<'_> {
    fn exec_raw(&mut self, sql: &str, params: &[SqlValue]) -> Result<(), DbError> {
        let bound: Vec<Value> = params.iter().map(to_value).collect();
        self.conn
            .prepare_cached(sql)
            .and_then(|mut stmt| stmt.execute(rusqlite::params_from_iter(bound.iter())))
            .map(|_| ())
            .map_err(map_err)
    }

    fn query_raw(&mut self, sql: &str, params: &[SqlValue]) -> Result<Vec<Row>, DbError> {
        let bound: Vec<Value> = params.iter().map(to_value).collect();
        let mut stmt = self.conn.prepare_cached(sql).map_err(map_err)?;
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

#[cfg(test)]
mod guard_tests {
    use super::*;

    #[test]
    fn schema_writes_cannot_remove_guards_or_create_invalid_cells() {
        let conn = Connection::open_in_memory().unwrap();
        let tables = Tables::from_zero_schema(&serde_json::json!({"tables": {
            "item": {"columns": {"id": {"type": "string"}, "amount": {"type": "number"}}, "primaryKey": ["id"]},
            "other": {"columns": {"id": {"type": "string"}, "amount": {"type": "number"}}, "primaryKey": ["id"]}
        }})).unwrap();
        conn.execute_batch("CREATE TABLE item (id TEXT PRIMARY KEY, amount NUMERIC); INSERT INTO item VALUES ('legacy', 'bad');").unwrap();
        init_number_storage_guards(&mut RusqliteDb::new(&conn), &tables).unwrap();
        let guard: String = conn.query_row("SELECT name FROM sqlite_schema WHERE type = 'trigger' AND name LIKE '%_insert' AND name GLOB '_zsync_number_*'", [], |row| row.get(0)).unwrap();
        let mut db = RusqliteDb::guarded(&conn, &tables);
        assert!(db.exec(&format!("DROP TRIGGER \"{guard}\""), &[]).is_err());
        assert!(
            db.exec("INSERT INTO item VALUES ('new', 'bad')", &[])
                .is_err()
        );
        assert!(
            db.exec(
                "CREATE TABLE other AS SELECT 'new' AS id, 'bad' AS amount",
                &[]
            )
            .is_err()
        );
        assert!(db.query("SELECT * FROM other", &[]).is_err());
        assert_eq!(
            db.query("SELECT amount FROM item WHERE id = 'legacy'", &[])
                .unwrap()[0]
                .values[0],
            SqlValue::Text("bad".into())
        );
    }
}
