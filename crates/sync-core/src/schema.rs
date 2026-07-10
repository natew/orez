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

use crate::db::{DbError, SyncDb};
use crate::value::ZeroColumnType;

#[derive(Debug, Clone)]
pub struct TableSpec {
    // ordered so snapshot column emission is deterministic
    pub columns: Vec<(String, ZeroColumnType)>,
    pub primary_key: Vec<String>,
}

impl TableSpec {
    pub fn column_type(&self, name: &str) -> Option<ZeroColumnType> {
        self.columns
            .iter()
            .find(|(c, _)| c == name)
            .map(|(_, t)| *t)
    }
}

// an ordered map of sync table name -> spec. order is preserved so snapshot
// row emission matches the reference core's Object.entries() iteration.
#[derive(Debug, Clone, Default)]
pub struct Tables(Vec<(String, TableSpec)>);

impl Tables {
    pub fn new() -> Self {
        Tables(Vec::new())
    }

    pub fn with(mut self, name: impl Into<String>, spec: TableSpec) -> Self {
        self.0.push((name.into(), spec));
        self
    }

    pub fn push(&mut self, name: impl Into<String>, spec: TableSpec) {
        self.0.push((name.into(), spec));
    }

    pub fn get(&self, name: &str) -> Option<&TableSpec> {
        self.0.iter().find(|(n, _)| n == name).map(|(_, s)| s)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &TableSpec)> {
        self.0.iter().map(|(n, s)| (n.as_str(), s))
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
        for (name, table) in tables_obj {
            // the consumer schema is trusted, but a malformed or hostile name
            // reaches trigger DDL, so reject anything that is not a plain SQL
            // identifier at ingest (defense in depth alongside quote_ident).
            if !is_valid_identifier(name) {
                return Err(format!("table name '{name}' is not a valid identifier"));
            }
            let columns_obj = table
                .get("columns")
                .and_then(|c| c.as_object())
                .ok_or_else(|| format!("table '{name}'.columns must be an object"))?;
            let mut columns = Vec::new();
            for (col, spec) in columns_obj {
                if !is_valid_identifier(col) {
                    return Err(format!("column '{name}.{col}' is not a valid identifier"));
                }
                let ty = spec
                    .get("type")
                    .and_then(|t| t.as_str())
                    .ok_or_else(|| format!("column '{name}.{col}'.type must be a string"))?;
                columns.push((col.clone(), ZeroColumnType::from_type_str(ty)));
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
            tables.push(
                name.clone(),
                TableSpec {
                    columns,
                    primary_key,
                },
            );
        }
        Ok(tables)
    }
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
fn pk_object(spec: &TableSpec, reference: &str) -> String {
    let parts: Vec<String> = spec
        .primary_key
        .iter()
        .map(|col| format!("{}, {}.{}", quote_str(col), reference, quote_ident(col)))
        .collect();
    format!("json_object({})", parts.join(", "))
}

// the durable metadata tables + one trigger set per application table. safe to
// call on every startup (all statements are IF NOT EXISTS / idempotent).
pub fn init_schema(db: &mut dyn SyncDb, tables: &Tables) -> Result<(), DbError> {
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
            floor INTEGER NOT NULL
        )",
        &[],
    )?;
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
        let tq = quote_ident(table);
        let tl = quote_str(table);
        // the trigger NAME embeds the table name, so it must be quoted/escaped
        // just like the target identifier — a raw interpolation lets a table
        // named `x" AFTER INSERT ON "victim" ...` break out of the quoted name
        // and install an injected trigger (from_zero_schema also rejects such
        // names, but the quoting is the actual barrier for any Tables source).
        let tr_i = quote_ident(&format!("_zsync_tr_{table}_i"));
        let tr_u = quote_ident(&format!("_zsync_tr_{table}_u"));
        let tr_d = quote_ident(&format!("_zsync_tr_{table}_d"));
        let new_pk = pk_object(spec, "NEW");
        let old_pk = pk_object(spec, "OLD");
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
