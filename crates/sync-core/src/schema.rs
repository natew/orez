// the durable `_zsync_*` schema and the application-table triggers that feed
// the change log. mirrors createSyncServer()'s DDL in the reference core
// (src/sync-server/sync-server.ts) plus the plan's durable-watermark table.
//
// invariants encoded here:
// - the change log stores WHICH primary keys were touched, never row values:
//   SQLite json_object formats REAL at 15 significant digits, so row images
//   would corrupt float columns. diffs re-read live rows instead (value.rs).
// - triggers capture EVERY write path (mutators and upstream/admin sql alike)
//   and are installed AFTER any seed so the initial dataset stays out of the
//   log (fresh clients snapshot anyway).
// - op 'row' carries a touched pk; op 'lmid' carries {clientID,lmid} so a
//   capped diff can derive acknowledgements from the INCLUDED prefix only
//   (never acking a mutation whose row effects were cut); op 'marker' carries
//   nothing and only advances the watermark (epoch invalidation).

use std::collections::BTreeSet;

use crate::db::{DbError, SqlValue, SyncDb};
use crate::error::EngineError;
use crate::value::ZeroColumnType;

#[derive(Debug, Clone)]
pub struct TableSpec {
    // ordered so snapshot column emission is deterministic
    pub columns: Vec<(String, ZeroColumnType)>,
    pub primary_key: Vec<String>,
    pub encrypted_columns: BTreeSet<String>,
    pub encrypted_physical_columns: BTreeSet<String>,
}

impl TableSpec {
    pub fn column_type(&self, name: &str) -> Option<ZeroColumnType> {
        self.columns
            .iter()
            .find(|(c, _)| c == name)
            .map(|(_, t)| *t)
    }

    pub fn is_encrypted(&self, name: &str) -> bool {
        self.encrypted_columns.contains(name) || self.encrypted_physical_columns.contains(name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnUse {
    Index,
    Predicate,
    Order,
    Cursor,
    Correlation,
    Visibility,
}

impl ColumnUse {
    pub fn as_str(self) -> &'static str {
        match self {
            ColumnUse::Index => "index",
            ColumnUse::Predicate => "predicate",
            ColumnUse::Order => "order",
            ColumnUse::Cursor => "cursor",
            ColumnUse::Correlation => "correlation",
            ColumnUse::Visibility => "visibility",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ResolvedColumn<'a> {
    pub logical_table: &'a str,
    pub physical_table: &'a str,
    pub logical_column: &'a str,
    pub physical_column: &'a str,
    pub column_type: ZeroColumnType,
    pub encrypted: bool,
}

#[derive(Debug, Clone)]
struct TableMapping {
    physical_name: String,
    physical_columns: Vec<(String, String)>,
}

// an ordered map of logical sync table name -> spec + physical SQLite mapping.
// order is preserved so snapshot row emission matches the reference core's
// Object.entries() iteration.
#[derive(Debug, Clone, Default)]
pub struct Tables(Vec<(String, TableSpec, TableMapping)>);

impl Tables {
    pub fn new() -> Self {
        Tables(Vec::new())
    }

    pub fn with(mut self, name: impl Into<String>, spec: TableSpec) -> Self {
        self.push(name, spec);
        self
    }

    pub fn push(&mut self, name: impl Into<String>, mut spec: TableSpec) {
        let name = name.into();
        spec.encrypted_physical_columns = spec.encrypted_columns.clone();
        let mapping = TableMapping {
            physical_name: name.clone(),
            physical_columns: spec
                .columns
                .iter()
                .map(|(column, _)| (column.clone(), column.clone()))
                .collect(),
        };
        self.0.push((name, spec, mapping));
    }

    fn push_mapped(
        &mut self,
        logical_name: String,
        physical_name: String,
        spec: TableSpec,
        physical_columns: Vec<(String, String)>,
    ) {
        self.0.push((
            logical_name,
            spec,
            TableMapping {
                physical_name,
                physical_columns,
            },
        ));
    }

    pub fn get(&self, name: &str) -> Option<&TableSpec> {
        self.0
            .iter()
            .find(|(logical, _, _)| logical == name)
            .map(|(_, spec, _)| spec)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &TableSpec)> {
        self.0
            .iter()
            .map(|(logical, spec, _)| (logical.as_str(), spec))
    }

    pub fn physical_name(&self, logical_name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(logical, _, _)| logical == logical_name)
            .map(|(_, _, mapping)| mapping.physical_name.as_str())
    }

    pub fn physical_column(&self, logical_table: &str, logical_column: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(logical, _, _)| logical == logical_table)
            .and_then(|(_, _, mapping)| {
                mapping
                    .physical_columns
                    .iter()
                    .find(|(logical, _)| logical == logical_column)
                    .map(|(_, physical)| physical.as_str())
            })
    }

    pub fn resolve_column(
        &self,
        table: &str,
        column: &str,
    ) -> Result<ResolvedColumn<'_>, EngineError> {
        let (logical_table, spec, mapping) = self
            .0
            .iter()
            .find(|(logical, _, mapping)| {
                logical.eq_ignore_ascii_case(table)
                    || mapping.physical_name.eq_ignore_ascii_case(table)
            })
            .ok_or_else(|| EngineError::bad_request(format!("unknown table '{table}'")))?;
        let (logical_column, physical_column) = mapping
            .physical_columns
            .iter()
            .find(|(logical, physical)| {
                logical.eq_ignore_ascii_case(column) || physical.eq_ignore_ascii_case(column)
            })
            .ok_or_else(|| {
                EngineError::bad_request(format!("unknown column '{table}.{column}'"))
            })?;
        let column_type = spec
            .column_type(logical_column)
            .expect("column mapping belongs to table spec");
        Ok(ResolvedColumn {
            logical_table,
            physical_table: &mapping.physical_name,
            logical_column,
            physical_column,
            column_type,
            encrypted: spec.encrypted_columns.contains(logical_column),
        })
    }

    pub fn validate_column_usage(
        &self,
        table: &str,
        column: &str,
        usage: ColumnUse,
    ) -> Result<ResolvedColumn<'_>, EngineError> {
        let resolved = self.resolve_column(table, column)?;
        if resolved.encrypted {
            return Err(EngineError::bad_request(format!(
                "encrypted column '{}.{}' has forbidden use '{}'",
                resolved.logical_table,
                resolved.logical_column,
                usage.as_str()
            )));
        }
        Ok(resolved)
    }

    pub fn has_encrypted_columns(&self) -> bool {
        self.0
            .iter()
            .any(|(_, spec, _)| !spec.encrypted_columns.is_empty())
    }

    pub(crate) fn projected_columns(
        &self,
        logical_table: &str,
        source_alias: Option<&str>,
    ) -> Option<String> {
        let spec = self.get(logical_table)?;
        spec.columns
            .iter()
            .map(|(logical_column, _)| {
                let physical_column = self.physical_column(logical_table, logical_column)?;
                let source = match source_alias {
                    Some(alias) => {
                        format!("{}.{}", quote_ident(alias), quote_ident(physical_column))
                    }
                    None => quote_ident(physical_column),
                };
                Some(format!("{source} AS {}", quote_ident(logical_column)))
            })
            .collect::<Option<Vec<_>>>()
            .map(|columns| columns.join(", "))
    }

    pub(crate) fn logical_ctes(&self) -> String {
        self.iter()
            .map(|(logical_table, _)| {
                let projection = self
                    .projected_columns(logical_table, None)
                    .expect("iterated table has projected columns");
                let physical_table = self
                    .physical_name(logical_table)
                    .expect("iterated table has physical mapping");
                format!(
                    "{} AS (SELECT {projection} FROM {}.{})",
                    quote_ident(logical_table),
                    quote_ident("main"),
                    quote_ident(physical_table)
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    // build the table set from a zero createSchema() result serialized to JSON:
    //   { "tables": { "<name>": { "columns": { "<col>": { "type": "<t>" }, ... },
    //                             "primaryKey": ["<col>", ...] }, ... } }
    // mirrors the reference core's tablesFromZeroSchema. this is how a host (the
    // wasm CF host, the native host) constructs Tables without touching the
    // internal representation. column/table order follows the JSON map's
    // iteration order, which is non-semantic on the wire (patch values are
    // objects and clients converge regardless of key/row order); tests that
    // assert positional patch order use the ordered `with()` builder instead.
    pub fn from_zero_schema(schema: &serde_json::Value) -> Result<Tables, String> {
        let tables_obj = schema
            .get("tables")
            .and_then(|t| t.as_object())
            .ok_or_else(|| "schema.tables must be an object".to_string())?;
        let mut tables = Tables::new();
        let mut logical_tables = BTreeSet::new();
        let mut physical_tables = BTreeSet::new();
        for (name, table) in tables_obj {
            // the consumer schema is trusted, but a malformed or hostile name
            // reaches trigger DDL, so reject anything that is not a plain SQL
            // identifier at ingest (defense in depth alongside quote_ident).
            if !is_valid_identifier(name) {
                return Err(format!("table name '{name}' is not a valid identifier"));
            }
            let logical_table_key = name.to_ascii_lowercase();
            if physical_tables.contains(&logical_table_key) {
                return Err(format!("ambiguous logical/physical table mapping '{name}'"));
            }
            if !logical_tables.insert(logical_table_key.clone()) {
                return Err(format!("duplicate logical table name '{name}'"));
            }
            let physical_name = match table.get("serverName") {
                None => name.clone(),
                Some(serde_json::Value::String(value)) => value.clone(),
                Some(_) => return Err(format!("table '{name}'.serverName must be a string")),
            };
            if !is_valid_identifier(&physical_name) {
                return Err(format!(
                    "physical table name '{physical_name}' for '{name}' is not a valid identifier"
                ));
            }
            let physical_table_key = physical_name.to_ascii_lowercase();
            if physical_table_key != logical_table_key
                && logical_tables.contains(&physical_table_key)
            {
                return Err(format!(
                    "ambiguous logical/physical table mapping '{physical_name}'"
                ));
            }
            if !physical_tables.insert(physical_table_key) {
                return Err(format!(
                    "duplicate physical table mapping '{physical_name}'"
                ));
            }
            let columns_obj = table
                .get("columns")
                .and_then(|c| c.as_object())
                .ok_or_else(|| format!("table '{name}'.columns must be an object"))?;
            let mut columns = Vec::new();
            let mut physical_columns = Vec::new();
            let mut encrypted_columns = BTreeSet::new();
            let mut encrypted_physical_columns = BTreeSet::new();
            let mut seen_logical_columns = BTreeSet::new();
            let mut seen_physical_columns = BTreeSet::new();
            for (col, spec) in columns_obj {
                if !is_valid_identifier(col) {
                    return Err(format!("column '{name}.{col}' is not a valid identifier"));
                }
                let logical_column_key = col.to_ascii_lowercase();
                if seen_physical_columns.contains(&logical_column_key) {
                    return Err(format!(
                        "ambiguous logical/physical column mapping '{name}.{col}'"
                    ));
                }
                if !seen_logical_columns.insert(logical_column_key.clone()) {
                    return Err(format!("duplicate logical column name '{name}.{col}'"));
                }
                let ty = spec
                    .get("type")
                    .and_then(|t| t.as_str())
                    .ok_or_else(|| format!("column '{name}.{col}'.type must be a string"))?;
                let physical_column = match spec.get("serverName") {
                    None => col.clone(),
                    Some(serde_json::Value::String(value)) => value.clone(),
                    Some(_) => {
                        return Err(format!("column '{name}.{col}'.serverName must be a string"));
                    }
                };
                if !is_valid_identifier(&physical_column) {
                    return Err(format!(
                        "physical column name '{physical_column}' for '{name}.{col}' is not a valid identifier"
                    ));
                }
                let physical_column_key = physical_column.to_ascii_lowercase();
                if physical_column_key != logical_column_key
                    && seen_logical_columns.contains(&physical_column_key)
                {
                    return Err(format!(
                        "ambiguous logical/physical column mapping '{name}.{physical_column}'"
                    ));
                }
                if !seen_physical_columns.insert(physical_column_key) {
                    return Err(format!(
                        "duplicate physical column mapping '{name}.{physical_column}'"
                    ));
                }
                let encrypted = match spec.get("encrypted") {
                    None => false,
                    Some(serde_json::Value::Bool(true)) => true,
                    Some(_) => {
                        return Err(format!(
                            "column '{name}.{col}'.encrypted must be true when present"
                        ));
                    }
                };
                if encrypted && !matches!(ty, "string" | "json") {
                    return Err(format!(
                        "encrypted column '{name}.{col}' has unsupported logical type '{ty}'"
                    ));
                }
                if encrypted {
                    encrypted_columns.insert(col.clone());
                    encrypted_physical_columns.insert(physical_column.clone());
                }
                columns.push((col.clone(), ZeroColumnType::from_type_str(ty)));
                physical_columns.push((col.clone(), physical_column));
            }
            let primary_key = table
                .get("primaryKey")
                .and_then(|p| p.as_array())
                .ok_or_else(|| format!("table '{name}'.primaryKey must be an array"))?
                .iter()
                .map(|v| {
                    let col = v.as_str().map(str::to_string).ok_or_else(|| {
                        format!("table '{name}'.primaryKey entries must be strings")
                    })?;
                    if !is_valid_identifier(&col) {
                        return Err(format!(
                            "table '{name}'.primaryKey entry '{col}' is not a valid identifier"
                        ));
                    }
                    Ok(col)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if primary_key.is_empty() {
                return Err(format!("table '{name}'.primaryKey must not be empty"));
            }
            let mut seen_primary_key = BTreeSet::new();
            for column in &primary_key {
                if !seen_primary_key.insert(column.as_str()) {
                    return Err(format!(
                        "table '{name}'.primaryKey contains duplicate '{column}'"
                    ));
                }
                if !columns.iter().any(|(candidate, _)| candidate == column) {
                    return Err(format!(
                        "table '{name}'.primaryKey references unknown column '{column}'"
                    ));
                }
                if encrypted_columns.contains(column) {
                    return Err(format!(
                        "encrypted column '{name}.{column}' has forbidden use 'primary-key'"
                    ));
                }
            }
            tables.push_mapped(
                name.clone(),
                physical_name,
                TableSpec {
                    columns,
                    primary_key,
                    encrypted_columns,
                    encrypted_physical_columns,
                },
                physical_columns,
            );
        }
        Ok(tables)
    }
}

fn text_column<'a>(row: &'a crate::db::Row, name: &str) -> Result<&'a str, DbError> {
    match row.get(name) {
        Some(SqlValue::Text(value)) => Ok(value),
        value => Err(DbError(format!(
            "SQLite metadata column '{name}' must be text, got {value:?}"
        ))),
    }
}

fn integer_column(row: &crate::db::Row, name: &str) -> Result<i64, DbError> {
    match row.get(name) {
        Some(SqlValue::Integer(value)) => Ok(*value),
        value => Err(DbError(format!(
            "SQLite metadata column '{name}' must be an integer, got {value:?}"
        ))),
    }
}

fn validate_encrypted_indexes(db: &mut dyn SyncDb, tables: &Tables) -> Result<(), DbError> {
    for (logical_table, spec) in tables.iter() {
        if spec.encrypted_columns.is_empty() {
            continue;
        }
        let physical_table = tables
            .physical_name(logical_table)
            .expect("iterated table has physical mapping");
        let indexes = db.query(
            "SELECT name, \"unique\" AS is_unique, origin, partial
             FROM pragma_index_list(?)",
            &[SqlValue::Text(physical_table.to_string())],
        )?;
        for index in indexes {
            let index_name = text_column(&index, "name")?;
            let unique = integer_column(&index, "is_unique")? != 0;
            let partial = integer_column(&index, "partial")? != 0;
            let origin = text_column(&index, "origin")?;
            let columns = db.query(
                "SELECT cid, name FROM pragma_index_info(?)",
                &[SqlValue::Text(index_name.to_string())],
            )?;
            let expression = columns.iter().any(|column| {
                matches!(column.get("cid"), Some(SqlValue::Integer(-2)))
                    || matches!(column.get("name"), None | Some(SqlValue::Null))
            });
            let kind = if expression {
                "expression"
            } else if origin == "pk" {
                "primary"
            } else if unique {
                "unique"
            } else if partial {
                "partial"
            } else {
                "ordinary"
            };

            if expression {
                let encrypted_columns = spec
                    .encrypted_columns
                    .iter()
                    .map(|column| format!("{logical_table}.{column}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(DbError(format!(
                    "encrypted columns [{encrypted_columns}] have forbidden use 'index' in {kind} SQLite index '{index_name}' because its referenced columns cannot be proven clear"
                )));
            }

            for column in columns {
                let physical_column = text_column(&column, "name")?;
                let resolved = tables
                    .resolve_column(physical_table, physical_column)
                    .map_err(|error| DbError(error.message))?;
                if resolved.encrypted {
                    return Err(DbError(format!(
                        "encrypted column '{}.{}' has forbidden use '{}' in {kind} SQLite index '{index_name}'",
                        resolved.logical_table,
                        resolved.logical_column,
                        ColumnUse::Index.as_str()
                    )));
                }
            }
        }
    }
    Ok(())
}

// SQLite identifier quoting: double-quote and escape embedded quotes. table
// and column names come from the trusted consumer schema, but quoting keeps
// reserved words and mixed-case identifiers correct.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

// a plain SQL identifier: non-empty, starts with a letter or underscore, then
// letters/digits/underscores. every real Zero schema name (camelCase tables and
// columns) satisfies this; anything else (embedded quotes, whitespace, dots) is
// rejected at schema ingest so an injection-shaped name never reaches DDL.
fn is_valid_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

// SQLite string literal quoting for the trigger bodies (single quotes).
fn quote_str(name: &str) -> String {
    format!("'{}'", name.replace('\'', "''"))
}

// json_object('<pk1>', <REF>."<pk1>", ...) expression capturing a row's pk
fn pk_object(tables: &Tables, table: &str, spec: &TableSpec, reference: &str) -> String {
    let parts: Vec<String> = spec
        .primary_key
        .iter()
        .map(|col| {
            let physical = tables
                .physical_column(table, col)
                .expect("primary key column in table mapping");
            format!(
                "{}, {}.{}",
                quote_str(col),
                reference,
                quote_ident(physical)
            )
        })
        .collect();
    format!("json_object({})", parts.join(", "))
}

// the durable metadata tables + one trigger set per application table. safe to
// call on every startup (all statements are IF NOT EXISTS / idempotent).
pub fn init_schema(db: &mut dyn SyncDb, tables: &Tables) -> Result<(), DbError> {
    validate_encrypted_indexes(db, tables)?;
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_clients (
            clientGroupID TEXT NOT NULL,
            clientID TEXT NOT NULL,
            lastMutationID INTEGER NOT NULL,
            userID TEXT,
            PRIMARY KEY (clientGroupID, clientID)
        )",
        &[],
    )?;
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_meta (
            lock INTEGER PRIMARY KEY CHECK (lock = 1),
            floor INTEGER NOT NULL,
            upstream_watermark INTEGER NOT NULL DEFAULT 0
        )",
        &[],
    )?;
    // Additive migration for stores created before upstream ingest existed.
    let has_upstream_watermark = !db
        .query(
            "SELECT name FROM pragma_table_info('_zsync_meta') WHERE name = 'upstream_watermark'",
            &[],
        )?
        .is_empty();
    if !has_upstream_watermark {
        db.exec(
            "ALTER TABLE _zsync_meta ADD COLUMN upstream_watermark INTEGER NOT NULL DEFAULT 0",
            &[],
        )?;
    }
    db.exec(
        "INSERT INTO _zsync_meta (lock, floor) VALUES (1, 0)
         ON CONFLICT (lock) DO NOTHING",
        &[],
    )?;
    // durable high-water mark: max watermark ever assigned. survives full
    // pruning and restart, so cookies never regress even if the log is emptied
    // (invariant 7: durable watermark = max(state, MAX(log))).
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_watermark (
            lock INTEGER PRIMARY KEY CHECK (lock = 1),
            high INTEGER NOT NULL
        )",
        &[],
    )?;
    db.exec(
        "INSERT INTO _zsync_watermark (lock, high) VALUES (1, 0)
         ON CONFLICT (lock) DO NOTHING",
        &[],
    )?;
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_changes (
            watermark INTEGER PRIMARY KEY AUTOINCREMENT,
            tableName TEXT NOT NULL,
            op TEXT NOT NULL CHECK (op IN ('row', 'lmid', 'marker')),
            pk TEXT
        )",
        &[],
    )?;
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_snapshot_generation (
            lock INTEGER PRIMARY KEY CHECK (lock = 1),
            nextGeneration INTEGER NOT NULL CHECK (nextGeneration > 0)
        )",
        &[],
    )?;
    db.exec(
        "INSERT INTO _zsync_snapshot_generation (lock, nextGeneration) VALUES (1, 1)
         ON CONFLICT (lock) DO NOTHING",
        &[],
    )?;
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_snapshot_progress (
            generation INTEGER PRIMARY KEY,
            startWatermark INTEGER NOT NULL CHECK (startWatermark >= 0),
            tableName TEXT,
            cursor TEXT,
            state TEXT NOT NULL CHECK (state IN ('paging', 'catching_up', 'complete', 'abandoned')),
            catchupWatermark INTEGER NOT NULL CHECK (catchupWatermark >= 0),
            active INTEGER UNIQUE CHECK (active IS NULL OR active = 1)
        )",
        &[],
    )?;
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_snapshot_cleanup (
            generation INTEGER NOT NULL,
            stageName TEXT NOT NULL,
            PRIMARY KEY (generation, stageName)
        )",
        &[],
    )?;

    for sql in trigger_ddl(tables) {
        db.exec(&sql, &[])?;
    }
    Ok(())
}

// the CREATE TRIGGER statements that append touched pks to _zsync_changes for
// every INSERT/UPDATE/DELETE. an UPDATE logs OLD and NEW pks so a pk-changing
// update dels the old row and puts the new one.
pub fn trigger_ddl(tables: &Tables) -> Vec<String> {
    let mut out = Vec::new();
    for (table, spec) in tables.iter() {
        let tq = quote_ident(
            tables
                .physical_name(table)
                .expect("iterated table has physical mapping"),
        );
        let tl = quote_str(table);
        // the trigger NAME embeds the table name, so it must be quoted/escaped
        // just like the target identifier — a raw interpolation lets a table
        // named `x" AFTER INSERT ON "victim" ...` break out of the quoted name
        // and install an injected trigger (from_zero_schema also rejects such
        // names, but the quoting is the actual barrier for any Tables source).
        let trigger_key = tables
            .physical_name(table)
            .expect("iterated table has physical mapping");
        let tr_i = quote_ident(&format!("_zsync_tr_{trigger_key}_i"));
        let tr_u = quote_ident(&format!("_zsync_tr_{trigger_key}_u"));
        let tr_d = quote_ident(&format!("_zsync_tr_{trigger_key}_d"));
        let new_pk = pk_object(tables, table, spec, "NEW");
        let old_pk = pk_object(tables, table, spec, "OLD");
        out.push(format!(
            "CREATE TRIGGER IF NOT EXISTS {tr_i} AFTER INSERT ON {tq} BEGIN
                INSERT INTO _zsync_changes (tableName, op, pk) VALUES ({tl}, 'row', {new_pk});
            END"
        ));
        out.push(format!(
            "CREATE TRIGGER IF NOT EXISTS {tr_u} AFTER UPDATE ON {tq} BEGIN
                INSERT INTO _zsync_changes (tableName, op, pk) VALUES ({tl}, 'row', {old_pk});
                INSERT INTO _zsync_changes (tableName, op, pk) VALUES ({tl}, 'row', {new_pk});
            END"
        ));
        out.push(format!(
            "CREATE TRIGGER IF NOT EXISTS {tr_d} AFTER DELETE ON {tq} BEGIN
                INSERT INTO _zsync_changes (tableName, op, pk) VALUES ({tl}, 'row', {old_pk});
            END"
        ));
    }
    out
}
