// durable query-aware state and the recomputation + membership-diff algorithm
// (the plan's "First algorithm: recomputation and membership diff (CVR-lite)").
// created only when a host enables the feature (init_query_schema), so the
// baseline M1 surface is untouched. all functions assume the host has a
// transaction open; none emits BEGIN/COMMIT.
//
// model (everything scoped per client GROUP, since a replica is shared across a
// group's tabs):
// - _zsync_queries:    (group, hash) -> transformed AST + root table + dependency
//                      tables + transform version. scoped by group because the
//                      hash is client-chosen: a global row would let one group's
//                      permission-transformed AST overwrite another group's under
//                      the same hash and leak forbidden rows (invariant 15).
// - _zsync_desires:    (group, client, hash) -> the client's query-state version
// - _zsync_query_rows: (group, hash) -> the rows currently in that query's result
// - _zsync_row_refs:   (group, rowTable, rowPk) -> how many of the group's active
//                      queries reference the row; the row is delivered while the
//                      count is positive and deleted only when it reaches zero
//                      (invariant 14).

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use crate::db::{SqlValue, SyncDb};
use crate::error::EngineError;
use crate::schema::{TableSpec, Tables};
use crate::value::{zero_pk_id, zero_row};

use super::ast::{Ast, Condition, CorrelatedSubquery};
use super::compile::{compile_predicate_probe, compile_related_of};
use super::{compile, parse_ast};

// bind a pk column (parsed from a canonical pk json) as a sqlite value
fn json_pk_to_sql(v: Option<&Value>) -> SqlValue {
    match v {
        None | Some(Value::Null) => SqlValue::Null,
        Some(Value::Bool(b)) => SqlValue::Integer(if *b { 1 } else { 0 }),
        Some(Value::Number(n)) => n
            .as_i64()
            .map(SqlValue::Integer)
            .unwrap_or_else(|| SqlValue::Real(n.as_f64().unwrap_or(0.0))),
        Some(Value::String(s)) => SqlValue::Text(s.clone()),
        Some(other) => SqlValue::Text(other.to_string()),
    }
}

fn text(s: impl Into<String>) -> SqlValue {
    SqlValue::Text(s.into())
}

// canonical string key for a primary-key JSON object (deterministic within a
// build; pk column order is fixed by the schema)
fn canonical_pk(pk: &Value) -> String {
    serde_json::to_string(pk).unwrap_or_default()
}

// raw (non-zero-typed) primary-key object of a live row, for keying membership
// and for building del ids
fn raw_pk(spec: &TableSpec, row: &crate::db::Row) -> Value {
    let mut pk = serde_json::Map::new();
    for col in &spec.primary_key {
        let v = match row.get(col) {
            Some(SqlValue::Null) | None => Value::Null,
            Some(SqlValue::Integer(i)) => json!(i),
            Some(SqlValue::Real(f)) => crate::value::f64_to_json(*f),
            Some(SqlValue::Text(s)) => json!(s),
            Some(SqlValue::Blob(b)) => json!(String::from_utf8_lossy(b)),
        };
        pk.insert(col.clone(), v);
    }
    Value::Object(pk)
}

// the current query-aware schema version. bump when a query-aware table changes
// shape, and add the forward migration in migrate_query_schema.
const QUERY_SCHEMA_VERSION: i64 = 2;

fn table_exists(db: &mut dyn SyncDb, name: &str) -> Result<bool, EngineError> {
    let rows = db.query(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        &[text(name)],
    )?;
    Ok(!rows.is_empty())
}

fn read_query_meta_version(db: &mut dyn SyncDb) -> Result<i64, EngineError> {
    let rows = db.query(
        "SELECT CAST(version AS TEXT) AS v FROM _zsync_query_meta WHERE lock = 1",
        &[],
    )?;
    match rows.first().and_then(|r| r.get("v")) {
        Some(SqlValue::Text(s)) => Ok(s.parse().unwrap_or(0)),
        _ => Ok(0),
    }
}

// forward-only migration for the query-aware tables, driven by the stored schema
// version. CREATE TABLE IF NOT EXISTS cannot alter an existing table, so a shape
// change (e.g. the pre-CRITICAL-2 hash-only _zsync_queries with no clientGroupID)
// needs an explicit migration. the query-aware state is fully reconstructable from
// clients' next desiredQueriesPatch + recompute and the old rows carry no client
// group to backfill, so an out-of-date version RESETS the query-aware tables (init
// recreates them at the current version). a version NEWER than this engine fails
// loud rather than silently downgrading. baseline _zsync_* tables are untouched.
fn migrate_query_schema(db: &mut dyn SyncDb) -> Result<(), EngineError> {
    // determine the stored version: from _zsync_query_meta if present, else infer
    // from the _zsync_queries shape (pre-versioning installs have no meta table —
    // a group-scoped _zsync_queries is v1, a hash-only one is v0). no query tables
    // at all means a fresh install with nothing to migrate.
    let stored_version = if table_exists(db, "_zsync_query_meta")? {
        read_query_meta_version(db)?
    } else {
        let queries_sql = db
            .query(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '_zsync_queries'",
                &[],
            )?
            .first()
            .and_then(|r| match r.get("sql") {
                Some(SqlValue::Text(s)) => Some(s.clone()),
                _ => None,
            });
        match queries_sql {
            Some(sql) if sql.contains("clientGroupID") => 1,
            Some(_) => 0,
            None => return Ok(()), // fresh install
        }
    };

    if stored_version > QUERY_SCHEMA_VERSION {
        return Err(EngineError::internal(format!(
            "query-schema version {stored_version} is newer than this engine's {QUERY_SCHEMA_VERSION}; refusing to downgrade"
        )));
    }
    if stored_version == QUERY_SCHEMA_VERSION {
        return Ok(());
    }

    // out of date: reset the query-aware tables (init recreates them at the current
    // version) AND bump the epoch, so every client's cookie is stale and it
    // full-resyncs — re-sending its desired queries. without the epoch bump a
    // caught-up client fast-paths to {unchanged} forever and never re-sends its
    // already-acked patch, and the wiped membership can no longer delete its stale
    // rows (GAP-3a).
    for table in [
        "_zsync_queries",
        "_zsync_desires",
        "_zsync_query_ack",
        "_zsync_query_rows",
        "_zsync_row_refs",
        "_zsync_query_state",
        "_zsync_query_transform_group",
        "_zsync_query_transform_client",
        "_zsync_query_meta",
    ] {
        db.exec(&format!("DROP TABLE IF EXISTS {table}"), &[])?;
    }
    crate::store::invalidate(db)?;
    Ok(())
}

pub fn init_query_schema(db: &mut dyn SyncDb) -> Result<(), EngineError> {
    migrate_query_schema(db)?;
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_query_meta (
            lock INTEGER PRIMARY KEY CHECK (lock = 1),
            version INTEGER NOT NULL
        )",
        &[],
    )?;
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_queries (
            clientGroupID TEXT NOT NULL,
            hash TEXT NOT NULL,
            ast TEXT NOT NULL,
            rootTable TEXT NOT NULL,
            deps TEXT NOT NULL,
            transformVersion INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (clientGroupID, hash)
        )",
        &[],
    )?;
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_desires (
            clientGroupID TEXT NOT NULL,
            clientID TEXT NOT NULL,
            hash TEXT NOT NULL,
            clientVersion INTEGER NOT NULL,
            PRIMARY KEY (clientGroupID, clientID, hash)
        )",
        &[],
    )?;
    // the monotonic acknowledged query-state version per (group, client). the
    // gotQueries ack is read from here, NOT from the max over current desire rows:
    // a del/clear removes desires but must never regress the acked version
    // (MEDIUM-6). every applied desiredQueriesPatch advances it to max(stored,
    // patch.version).
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_query_ack (
            clientGroupID TEXT NOT NULL,
            clientID TEXT NOT NULL,
            version INTEGER NOT NULL,
            PRIMARY KEY (clientGroupID, clientID)
        )",
        &[],
    )?;
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_query_transform_group (
            clientGroupID TEXT PRIMARY KEY,
            version INTEGER NOT NULL
        )",
        &[],
    )?;
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_query_transform_client (
            clientGroupID TEXT NOT NULL,
            clientID TEXT NOT NULL,
            version INTEGER NOT NULL,
            PRIMARY KEY (clientGroupID, clientID)
        )",
        &[],
    )?;
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_query_rows (
            clientGroupID TEXT NOT NULL,
            hash TEXT NOT NULL,
            rowTable TEXT NOT NULL,
            rowPk TEXT NOT NULL,
            PRIMARY KEY (clientGroupID, hash, rowTable, rowPk)
        )",
        &[],
    )?;
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_row_refs (
            clientGroupID TEXT NOT NULL,
            rowTable TEXT NOT NULL,
            rowPk TEXT NOT NULL,
            refcount INTEGER NOT NULL,
            PRIMARY KEY (clientGroupID, rowTable, rowPk)
        )",
        &[],
    )?;
    // marks a (group, query) whose membership has been computed at least once.
    // recomputation narrowing skips a query whose dependency tables were not
    // touched, EXCEPT one with no marker yet (a newly desired query, or one
    // whose AST changed and had its marker cleared), which always recomputes.
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_query_state (
            clientGroupID TEXT NOT NULL,
            hash TEXT NOT NULL,
            PRIMARY KEY (clientGroupID, hash)
        )",
        &[],
    )?;
    // record the schema version so a future shape change can migrate forward.
    db.exec(
        "INSERT INTO _zsync_query_meta (lock, version) VALUES (1, ?)
         ON CONFLICT (lock) DO UPDATE SET version = excluded.version",
        &[SqlValue::Integer(QUERY_SCHEMA_VERSION)],
    )?;
    Ok(())
}

// register (or replace) a query for a client GROUP by its stable hash. validates
// + compiles the transformed AST so an invalid query is rejected here, not at
// pull time, and records the root table + dependency tables recomputation
// narrows on, plus the consumer-supplied transformation version.
//
// scoping by (group, hash) is a security boundary: the hash is client-chosen, so
// a globally-keyed row would let group B register a permissive AST under group
// A's hash and overwrite A's restricted (permission-transformed) definition,
// leaking forbidden rows on A's next pull (invariant 15). each group keeps its
// own definition; a change to the AST OR the transformation version clears just
// this group's computed markers so its next pull recomputes and can never retain
// a more-permissive older result.
pub fn register_query(
    db: &mut dyn SyncDb,
    tables: &Tables,
    group: &str,
    hash: &str,
    ast_json: &Value,
    transform_version: i64,
) -> Result<(), EngineError> {
    let ast = parse_ast(ast_json)?;
    compile(&ast, tables)?; // validate + reject unsupported shapes here
    // the dependency set includes related-output and EXISTS child tables (not
    // just the root query's), so touched-table narrowing never skips a query
    // whose child rows changed.
    let mut dep_set = BTreeSet::new();
    super::ast::collect_dependency_tables(&ast, &mut dep_set);
    let deps = serde_json::to_string(&dep_set).unwrap();
    let ast_text = serde_json::to_string(ast_json).unwrap();

    let prev = db.query(
        "SELECT ast, CAST(transformVersion AS TEXT) AS tv FROM _zsync_queries
         WHERE clientGroupID = ? AND hash = ?",
        &[text(group), text(hash)],
    )?;
    let (prev_ast, prev_tv) = match prev.first() {
        Some(r) => (
            str_col(r.get("ast")).ok(),
            match r.get("tv") {
                Some(SqlValue::Text(s)) => s.parse::<i64>().unwrap_or(0),
                _ => 0,
            },
        ),
        None => (None, 0),
    };
    let changed = prev_ast.as_deref() != Some(ast_text.as_str()) || prev_tv != transform_version;

    db.exec(
        "INSERT INTO _zsync_queries (clientGroupID, hash, ast, rootTable, deps, transformVersion)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT (clientGroupID, hash) DO UPDATE SET ast = excluded.ast,
             rootTable = excluded.rootTable, deps = excluded.deps,
             transformVersion = excluded.transformVersion",
        &[
            text(group),
            text(hash),
            text(ast_text),
            text(&ast.table),
            text(deps),
            SqlValue::Text(transform_version.to_string()),
        ],
    )?;
    if changed {
        db.exec(
            "DELETE FROM _zsync_query_state WHERE clientGroupID = ? AND hash = ?",
            &[text(group), text(hash)],
        )?;
    }
    Ok(())
}

pub fn set_desire(
    db: &mut dyn SyncDb,
    group: &str,
    client: &str,
    hash: &str,
    client_version: i64,
) -> Result<bool, EngineError> {
    let existed = !db
        .query(
            "SELECT 1 FROM _zsync_desires
             WHERE clientGroupID = ? AND clientID = ? AND hash = ?",
            &[text(group), text(client), text(hash)],
        )?
        .is_empty();
    db.exec(
        "INSERT INTO _zsync_desires (clientGroupID, clientID, hash, clientVersion)
         VALUES (?, ?, ?, ?)
         ON CONFLICT (clientGroupID, clientID, hash) DO UPDATE SET clientVersion = excluded.clientVersion",
        &[text(group), text(client), text(hash), SqlValue::Text(client_version.to_string())],
    )?;
    Ok(!existed)
}

pub fn remove_desire(
    db: &mut dyn SyncDb,
    group: &str,
    client: &str,
    hash: &str,
) -> Result<(), EngineError> {
    db.exec(
        "DELETE FROM _zsync_desires WHERE clientGroupID = ? AND clientID = ? AND hash = ?",
        &[text(group), text(client), text(hash)],
    )?;
    Ok(())
}

// clear all of a client's desired queries (a desiredQueriesPatch 'clear')
pub fn clear_desires(db: &mut dyn SyncDb, group: &str, client: &str) -> Result<(), EngineError> {
    db.exec(
        "DELETE FROM _zsync_desires WHERE clientGroupID = ? AND clientID = ?",
        &[text(group), text(client)],
    )?;
    Ok(())
}

// the hashes a client currently desires (for the gotQueries acknowledgement)
pub(crate) fn desired_hashes(
    db: &mut dyn SyncDb,
    group: &str,
    client: &str,
) -> Result<Vec<String>, EngineError> {
    let rows = db.query(
        "SELECT hash FROM _zsync_desires WHERE clientGroupID = ? AND clientID = ? ORDER BY hash",
        &[text(group), text(client)],
    )?;
    Ok(rows
        .iter()
        .filter_map(|r| str_col(r.get("hash")).ok())
        .collect())
}

// advance a client's acknowledged query-state version monotonically. called for
// every applied desiredQueriesPatch (put/del/clear alike), so the ack tracks the
// patch version independently of which desire rows currently exist and never
// regresses when del/clear shrink the desire set (MEDIUM-6).
pub(crate) fn advance_query_ack(
    db: &mut dyn SyncDb,
    group: &str,
    client: &str,
    version: i64,
) -> Result<(), EngineError> {
    db.exec(
        "INSERT INTO _zsync_query_ack (clientGroupID, clientID, version)
         VALUES (?, ?, ?)
         ON CONFLICT (clientGroupID, clientID) DO UPDATE SET version = max(version, excluded.version)",
        &[
            text(group),
            text(client),
            SqlValue::Text(version.to_string()),
        ],
    )?;
    Ok(())
}

// the client's acknowledged query-state version: the monotonic value recorded by
// advance_query_ack (0 if it has never sent a desiredQueriesPatch). read from the
// durable ack row, NOT the max over current desires, so a del/clear cannot make
// the gotQueries ack regress.
pub(crate) fn client_query_version(
    db: &mut dyn SyncDb,
    group: &str,
    client: &str,
) -> Result<i64, EngineError> {
    let rows = db.query(
        "SELECT CAST(version AS TEXT) AS v FROM _zsync_query_ack
         WHERE clientGroupID = ? AND clientID = ?",
        &[text(group), text(client)],
    )?;
    match rows.first().and_then(|r| r.get("v")) {
        Some(SqlValue::Text(s)) => Ok(s.parse().unwrap_or(0)),
        _ => Ok(0),
    }
}

// drop a group's entire durable membership + reference counts. used when a fresh
// or reset client (null / below-floor cookie) must be re-synced from scratch.
pub(crate) fn reset_group(db: &mut dyn SyncDb, group: &str) -> Result<(), EngineError> {
    db.exec(
        "DELETE FROM _zsync_query_rows WHERE clientGroupID = ?",
        &[text(group)],
    )?;
    db.exec(
        "DELETE FROM _zsync_row_refs WHERE clientGroupID = ?",
        &[text(group)],
    )?;
    // clear the computed markers so a fresh re-sync recomputes every query
    db.exec(
        "DELETE FROM _zsync_query_state WHERE clientGroupID = ?",
        &[text(group)],
    )?;
    Ok(())
}

pub(crate) fn prepare_transform_version(
    db: &mut dyn SyncDb,
    group: &str,
    client: &str,
    version: i64,
) -> Result<bool, EngineError> {
    let group_rows = db.query(
        "SELECT CAST(version AS TEXT) AS v FROM _zsync_query_transform_group
         WHERE clientGroupID = ?",
        &[text(group)],
    )?;
    let group_version =
        group_rows
            .first()
            .and_then(|row| row.get("v"))
            .and_then(|value| match value {
                SqlValue::Text(value) => value.parse::<i64>().ok(),
                _ => None,
            });

    if group_version.is_some_and(|stored| stored != version) {
        // keep per-client desires until each client checks in so qpull can
        // acknowledge targeted dels. a gotQueries clear drops Zero's local
        // named-query mapping and leaves correctly returned rows invisible.
        db.exec(
            "DELETE FROM _zsync_queries WHERE clientGroupID = ?",
            &[text(group)],
        )?;
        reset_group(db, group)?;
    }
    db.exec(
        "INSERT INTO _zsync_query_transform_group (clientGroupID, version) VALUES (?, ?)
         ON CONFLICT (clientGroupID) DO UPDATE SET version = excluded.version",
        &[text(group), SqlValue::Integer(version)],
    )?;

    let client_rows = db.query(
        "SELECT CAST(version AS TEXT) AS v FROM _zsync_query_transform_client
         WHERE clientGroupID = ? AND clientID = ?",
        &[text(group), text(client)],
    )?;
    let client_version = client_rows
        .first()
        .and_then(|row| row.get("v"))
        .and_then(|value| match value {
            SqlValue::Text(value) => value.parse::<i64>().ok(),
            _ => None,
        });
    let reset_client = client_version != Some(version);
    db.exec(
        "INSERT INTO _zsync_query_transform_client (clientGroupID, clientID, version)
         VALUES (?, ?, ?)
         ON CONFLICT (clientGroupID, clientID) DO UPDATE SET version = excluded.version",
        &[text(group), text(client), SqlValue::Integer(version)],
    )?;
    Ok(reset_client)
}

// canonicalize a change-log pk (json_object text) to the membership key form
pub(crate) fn canonical_pk_text(pk_text: &str) -> String {
    match serde_json::from_str::<Value>(pk_text) {
        Ok(v) => canonical_pk(&v),
        Err(_) => pk_text.to_string(),
    }
}

struct ActiveQuery {
    hash: String,
    ast_json: Value,
    root_table: String,
    deps: Vec<String>,
}

// the distinct queries any client in the group currently desires
fn active_queries(db: &mut dyn SyncDb, group: &str) -> Result<Vec<ActiveQuery>, EngineError> {
    let rows = db.query(
        "SELECT DISTINCT q.hash AS hash, q.ast AS ast, q.rootTable AS rootTable, q.deps AS deps
         FROM _zsync_desires d JOIN _zsync_queries q
           ON q.clientGroupID = d.clientGroupID AND q.hash = d.hash
         WHERE d.clientGroupID = ?",
        &[text(group)],
    )?;
    let mut out = Vec::new();
    for row in &rows {
        let hash = str_col(row.get("hash"))?;
        let ast_text = str_col(row.get("ast"))?;
        let root_table = str_col(row.get("rootTable"))?;
        let deps: Vec<String> =
            serde_json::from_str(&str_col(row.get("deps"))?).unwrap_or_default();
        let ast_json: Value = serde_json::from_str(&ast_text)
            .map_err(|e| EngineError::internal(format!("stored query ast is not json: {e}")))?;
        out.push(ActiveQuery {
            hash,
            ast_json,
            root_table,
            deps,
        });
    }
    Ok(out)
}

// the set of query hashes already computed at least once for the group
fn read_query_state(db: &mut dyn SyncDb, group: &str) -> Result<BTreeSet<String>, EngineError> {
    let rows = db.query(
        "SELECT hash FROM _zsync_query_state WHERE clientGroupID = ?",
        &[text(group)],
    )?;
    Ok(rows
        .iter()
        .filter_map(|r| str_col(r.get("hash")).ok())
        .collect())
}

fn set_query_state(db: &mut dyn SyncDb, group: &str, hash: &str) -> Result<(), EngineError> {
    db.exec(
        "INSERT OR IGNORE INTO _zsync_query_state (clientGroupID, hash) VALUES (?, ?)",
        &[text(group), text(hash)],
    )?;
    Ok(())
}

fn str_col(v: Option<&SqlValue>) -> Result<String, EngineError> {
    match v {
        Some(SqlValue::Text(s)) => Ok(s.clone()),
        _ => Err(EngineError::internal("expected a text column")),
    }
}

// a query's durable member rows, as (rowTable, rowPk) keys (a query with
// related output has members in more than one table)
fn read_query_row_keys(
    db: &mut dyn SyncDb,
    group: &str,
    hash: &str,
) -> Result<BTreeSet<(String, String)>, EngineError> {
    let rows = db.query(
        "SELECT rowTable, rowPk FROM _zsync_query_rows WHERE clientGroupID = ? AND hash = ?",
        &[text(group), text(hash)],
    )?;
    let mut out = BTreeSet::new();
    for r in &rows {
        if let (Ok(t), Ok(pk)) = (str_col(r.get("rowTable")), str_col(r.get("rowPk"))) {
            out.insert((t, pk));
        }
    }
    Ok(out)
}

fn read_ref(db: &mut dyn SyncDb, group: &str, table: &str, pk: &str) -> Result<i64, EngineError> {
    let rows = db.query(
        "SELECT CAST(refcount AS TEXT) AS c FROM _zsync_row_refs
         WHERE clientGroupID = ? AND rowTable = ? AND rowPk = ?",
        &[text(group), text(table), text(pk)],
    )?;
    match rows.first().and_then(|r| r.get("c")) {
        Some(SqlValue::Text(s)) => Ok(s.parse().unwrap_or(0)),
        _ => Ok(0),
    }
}

fn set_ref(
    db: &mut dyn SyncDb,
    group: &str,
    table: &str,
    pk: &str,
    count: i64,
) -> Result<(), EngineError> {
    if count <= 0 {
        db.exec(
            "DELETE FROM _zsync_row_refs WHERE clientGroupID = ? AND rowTable = ? AND rowPk = ?",
            &[text(group), text(table), text(pk)],
        )?;
    } else {
        db.exec(
            "INSERT INTO _zsync_row_refs (clientGroupID, rowTable, rowPk, refcount)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (clientGroupID, rowTable, rowPk) DO UPDATE SET refcount = excluded.refcount",
            &[text(group), text(table), text(pk), SqlValue::Text(count.to_string())],
        )?;
    }
    Ok(())
}

// the correlated subqueries whose rows a stock client needs to reproduce the
// query locally: every related-OUTPUT subquery, plus every positive
// correlated-EXISTS FILTER subquery. the client re-runs the whole query against
// its synced rows, so it must have the member rows that make each EXISTS true;
// without them its local EXISTS is false and the query collapses to empty.
// NOT EXISTS is skipped: a matching parent has no matching child by definition,
// so there are no rows to sync, and the client re-runs over the matching parent
// set the server already sent.
fn dependent_subqueries(ast: &Ast) -> Vec<&CorrelatedSubquery> {
    let mut subs: Vec<&CorrelatedSubquery> = Vec::new();
    if let Some(cond) = &ast.where_ {
        collect_positive_exists(cond, &mut subs);
    }
    for rel in &ast.related {
        subs.push(rel);
    }
    subs
}

fn collect_positive_exists<'a>(cond: &'a Condition, out: &mut Vec<&'a CorrelatedSubquery>) {
    match cond {
        Condition::Exists {
            negated: false,
            related,
        } => out.push(related),
        Condition::Exists { negated: true, .. } | Condition::Simple { .. } => {}
        Condition::And(conds) | Condition::Or(conds) => {
            for c in conds {
                collect_positive_exists(c, out);
            }
        }
    }
}

// walk a query's dependent subtree (related output + positive-EXISTS filter
// subqueries), adding every correlated child (and grandchild, recursively) row
// to the query's live member set. `parent_sql` is the compiled SQL of the
// parent row-set at this level; each dependent subquery is joined to it and its
// own SQL becomes the parent for the next level down.
#[allow(clippy::too_many_arguments)]
fn collect_dependent_rows(
    db: &mut dyn SyncDb,
    tables: &Tables,
    parent_sql: &str,
    parent_params: &[SqlValue],
    parent_table: &str,
    ast: &Ast,
    depth: usize,
    live: &mut BTreeSet<(String, String)>,
    values: &mut BTreeMap<(String, String), Value>,
) -> Result<(), EngineError> {
    for sub in dependent_subqueries(ast) {
        let cr = compile_related_of(parent_sql, parent_params, parent_table, sub, tables, depth)?;
        let child_spec = tables.get(&cr.child_table).ok_or_else(|| {
            EngineError::internal(format!(
                "dependent child table '{}' missing",
                cr.child_table
            ))
        })?;
        for row in &db.query(&cr.sql, &cr.params)? {
            let pk_obj = raw_pk(child_spec, row);
            let key = canonical_pk(&pk_obj);
            live.insert((cr.child_table.clone(), key.clone()));
            values.insert(
                (cr.child_table.clone(), key),
                zero_row(tables, &cr.child_table, child_spec, row)?,
            );
        }
        // the child row-set is the parent for the child's own dependent subqueries
        collect_dependent_rows(
            db,
            tables,
            &cr.sql,
            &cr.params,
            &cr.child_table,
            &sub.subquery,
            depth + 1,
            live,
            values,
        )?;
    }
    Ok(())
}

// touched-pk narrowing: is any touched row actually relevant to this query? a
// touched row in a non-root dependency table (an EXISTS/related child) is
// conservatively relevant (its effect on parents is not cheaply localized). a
// touched ROOT row is relevant only if it is a current member (it may leave or
// its data changed) or it matches the query predicate (it may enter, possibly
// shifting a limited window). so a new message in channel Y probes as a
// non-member non-match for channel X's query and is skipped.
fn query_relevant(
    db: &mut dyn SyncDb,
    tables: &Tables,
    group: &str,
    q: &ActiveQuery,
    changed: &BTreeSet<(String, String)>,
) -> Result<bool, EngineError> {
    let mut root_pks: Vec<&String> = Vec::new();
    for (table, pk) in changed {
        if !q.deps.iter().any(|d| d == table) {
            continue;
        }
        if table != &q.root_table {
            return Ok(true); // non-root dependency change
        }
        root_pks.push(pk);
    }
    if root_pks.is_empty() {
        return Ok(false);
    }
    let durable = read_query_row_keys(db, group, &q.hash)?;
    let ast = parse_ast(&q.ast_json)?;
    let (sql, base_params, pk_cols) = compile_predicate_probe(&ast, tables)?;
    for pk in root_pks {
        if durable.contains(&(q.root_table.clone(), pk.clone())) {
            return Ok(true);
        }
        let pk_obj: Value = serde_json::from_str(pk).unwrap_or(Value::Null);
        let mut params = base_params.clone();
        for col in &pk_cols {
            params.push(json_pk_to_sql(pk_obj.get(col)));
        }
        if !db.query(&sql, &params)?.is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}

// recompute every active query for the group, diff each against durable
// membership, net the per-row reference changes, and emit row puts (a row's
// first reference in the group) and dels (its last reference gone). `changed`
// carries the (table, pk) touched since the client's cookie so a row whose DATA
// changed but whose membership did not is re-emitted with its current value.
pub fn recompute_group(
    db: &mut dyn SyncDb,
    tables: &Tables,
    group: &str,
    changed: &BTreeSet<(String, String)>,
) -> Result<Vec<Value>, EngineError> {
    recompute_group_with_rehydrate(db, tables, group, changed, &BTreeSet::new())
}

pub(crate) fn recompute_group_with_rehydrate(
    db: &mut dyn SyncDb,
    tables: &Tables,
    group: &str,
    changed: &BTreeSet<(String, String)>,
    rehydrate: &BTreeSet<String>,
) -> Result<Vec<Value>, EngineError> {
    let queries = active_queries(db, group)?;

    // clear computed markers for queries this group no longer desires, so a
    // later re-desire recomputes from scratch instead of being narrowed away.
    db.exec(
        "DELETE FROM _zsync_query_state WHERE clientGroupID = ?
         AND hash NOT IN (SELECT DISTINCT hash FROM _zsync_desires WHERE clientGroupID = ?)",
        &[text(group), text(group)],
    )?;

    // recomputation narrowing (plan algorithm step 2): a query is recomputed only
    // when the touched tables intersect its dependency set, or it has never been
    // computed for this group (newly desired, or its AST changed and its marker
    // was cleared). an untouched query's membership cannot have changed. the
    // computed-marker set is loaded once so a skip costs no SQL.
    let touched: BTreeSet<&str> = changed.iter().map(|(t, _)| t.as_str()).collect();
    let mut computed: BTreeSet<String> = read_query_state(db, group)?;

    // net reference delta per (table, pk), and the live value cache for puts
    let mut ref_delta: BTreeMap<(String, String), i64> = BTreeMap::new();
    let mut values: BTreeMap<(String, String), Value> = BTreeMap::new();
    let mut rehydrate_rows: BTreeSet<(String, String)> = BTreeSet::new();

    for q in &queries {
        if computed.contains(&q.hash) && !rehydrate.contains(&q.hash) {
            // (a) dependency-intersection: skip if no dependency table was touched
            let dep_touched = q.deps.iter().any(|t| touched.contains(t.as_str()));
            if !dep_touched {
                continue;
            }
            // (b) touched-pk narrowing: skip if no touched row is relevant
            if !query_relevant(db, tables, group, q, changed)? {
                continue;
            }
        } else {
            // never computed for this group (newly desired, or AST changed): compute
            computed.insert(q.hash.clone());
            set_query_state(db, group, &q.hash)?;
        }
        let spec = tables.get(&q.root_table).ok_or_else(|| {
            EngineError::internal(format!("query root table '{}' missing", q.root_table))
        })?;
        let ast = parse_ast(&q.ast_json)?;

        // the query's live members = root rows + related-output child rows, keyed
        // by (table, pk). collecting into one set lets the diff/refcount logic
        // treat every member uniformly regardless of which table it came from.
        let mut live: BTreeSet<(String, String)> = BTreeSet::new();

        let compiled = compile(&ast, tables)?;
        for row in &db.query(&compiled.sql, &compiled.params)? {
            let pk_obj = raw_pk(spec, row);
            let key = canonical_pk(&pk_obj);
            live.insert((q.root_table.clone(), key.clone()));
            values.insert(
                (q.root_table.clone(), key),
                zero_row(tables, &q.root_table, spec, row)?,
            );
        }

        // dependent rows (related output + positive-EXISTS filter subqueries),
        // walked recursively. a parent membership change re-runs these joins, so
        // children follow their parents even when the child table was untouched.
        // syncing the EXISTS-filter rows is what lets the stock client re-run the
        // query locally and reproduce the server's membership.
        collect_dependent_rows(
            db,
            tables,
            &compiled.sql,
            &compiled.params,
            &ast.table,
            &ast,
            0,
            &mut live,
            &mut values,
        )?;

        if rehydrate.contains(&q.hash) {
            rehydrate_rows.extend(live.iter().cloned());
        }

        let durable = read_query_row_keys(db, group, &q.hash)?;
        for key in live.difference(&durable) {
            db.exec(
                "INSERT OR IGNORE INTO _zsync_query_rows (clientGroupID, hash, rowTable, rowPk)
                 VALUES (?, ?, ?, ?)",
                &[text(group), text(&q.hash), text(&key.0), text(&key.1)],
            )?;
            *ref_delta.entry(key.clone()).or_insert(0) += 1;
        }
        for key in durable.difference(&live) {
            db.exec(
                "DELETE FROM _zsync_query_rows
                 WHERE clientGroupID = ? AND hash = ? AND rowTable = ? AND rowPk = ?",
                &[text(group), text(&q.hash), text(&key.0), text(&key.1)],
            )?;
            *ref_delta.entry(key.clone()).or_insert(0) -= 1;
        }
    }

    // deactivated queries: a query no client desires any more still has durable
    // membership rows. drop them and release their references (invariant 15:
    // dropping a desired query removes its rows unless another query holds them).
    // find just the deactivated hashes first, so a recompute with no deactivation
    // never scans the whole membership.
    let deactivated: Vec<String> = db
        .query(
            "SELECT DISTINCT hash FROM _zsync_query_rows WHERE clientGroupID = ?
             AND hash NOT IN (SELECT DISTINCT hash FROM _zsync_desires WHERE clientGroupID = ?)",
            &[text(group), text(group)],
        )?
        .iter()
        .filter_map(|r| str_col(r.get("hash")).ok())
        .collect();
    for hash in &deactivated {
        let rows = read_query_row_keys(db, group, hash)?;
        for (table, pk) in rows {
            db.exec(
                "DELETE FROM _zsync_query_rows
                 WHERE clientGroupID = ? AND hash = ? AND rowTable = ? AND rowPk = ?",
                &[text(group), text(hash), text(&table), text(&pk)],
            )?;
            *ref_delta.entry((table, pk)).or_insert(0) -= 1;
        }
    }

    // apply net deltas; emit a put on 0 -> positive, a del on positive -> 0
    let mut patch: Vec<Value> = Vec::new();
    let mut emitted: BTreeSet<(String, String)> = BTreeSet::new();
    for ((table, pk), delta) in &ref_delta {
        if *delta == 0 {
            continue;
        }
        let old = read_ref(db, group, table, pk)?;
        let new = old + delta;
        set_ref(db, group, table, pk, new)?;
        if old == 0 && new > 0 {
            let value = values
                .get(&(table.clone(), pk.clone()))
                .cloned()
                .ok_or_else(|| EngineError::internal("added row missing live value"))?;
            let physical_table = tables
                .physical_name(table)
                .expect("member table has physical mapping");
            patch.push(json!({ "op": "put", "tableName": physical_table, "value": value }));
            emitted.insert((table.clone(), pk.clone()));
        } else if old > 0 && new == 0 {
            let spec = tables.get(table).unwrap();
            let pk_obj: Value = serde_json::from_str(pk).unwrap_or(Value::Null);
            let physical_table = tables
                .physical_name(table)
                .expect("member table has physical mapping");
            patch.push(json!({
                "op": "del",
                "tableName": physical_table,
                "id": zero_pk_id(tables, table, spec, &pk_obj)?,
            }));
            emitted.insert((table.clone(), pk.clone()));
        }
    }

    // re-emit rows whose data changed but whose membership did not (still
    // referenced, without duplicating a put/del from a membership flip)
    for (table, pk) in changed {
        let key = (table.clone(), pk.clone());
        if emitted.contains(&key) {
            continue;
        }
        if read_ref(db, group, table, pk)? > 0
            && let Some(value) = values.get(&key)
        {
            let physical_table = tables
                .physical_name(table)
                .expect("member table has physical mapping");
            patch.push(json!({ "op": "put", "tableName": physical_table, "value": value }));
            emitted.insert(key);
        }
    }

    // a newly-desiring client in an existing group may have an empty local
    // cache even though the group's durable membership and cookie are current
    // (for example after a ttl=0 client restart). membership refcounts do not
    // change, but that client still needs the live rows for its query. duplicate
    // puts are idempotent in the shared client-group store.
    for key in rehydrate_rows {
        if emitted.contains(&key) {
            continue;
        }
        let value = values
            .get(&key)
            .cloned()
            .ok_or_else(|| EngineError::internal("rehydrated row missing live value"))?;
        let physical_table = tables
            .physical_name(&key.0)
            .expect("member table has physical mapping");
        patch.push(json!({ "op": "put", "tableName": physical_table, "value": value }));
    }

    Ok(patch)
}
