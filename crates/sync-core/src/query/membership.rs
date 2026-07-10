// durable query-aware state and the recomputation + membership-diff algorithm
// (the plan's "First algorithm: recomputation and membership diff (CVR-lite)").
// created only when a host enables the feature (init_query_schema), so the
// baseline M1 surface is untouched. all functions assume the host has a
// transaction open; none emits BEGIN/COMMIT.
//
// model (everything scoped per client GROUP, since a replica is shared across a
// group's tabs):
// - _zsync_queries:    hash -> transformed AST + root table + dependency tables
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
use super::compile::compile_related_of;
use super::{compile, parse_ast};

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

pub fn init_query_schema(db: &mut dyn SyncDb) -> Result<(), EngineError> {
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_queries (
            hash TEXT PRIMARY KEY,
            ast TEXT NOT NULL,
            rootTable TEXT NOT NULL,
            deps TEXT NOT NULL,
            transformVersion INTEGER NOT NULL DEFAULT 0
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
    Ok(())
}

// register (or replace) a query by its stable hash. validates + compiles the
// transformed AST so an invalid query is rejected here, not at pull time, and
// records the root table + dependency tables recomputation narrows on.
pub fn register_query(
    db: &mut dyn SyncDb,
    tables: &Tables,
    hash: &str,
    ast_json: &Value,
) -> Result<(), EngineError> {
    let ast = parse_ast(ast_json)?;
    let compiled = compile(&ast, tables)?;
    let deps = serde_json::to_string(&compiled.dependency_tables).unwrap();
    let ast_text = serde_json::to_string(ast_json).unwrap();
    db.exec(
        "INSERT INTO _zsync_queries (hash, ast, rootTable, deps, transformVersion)
         VALUES (?, ?, ?, ?, 0)
         ON CONFLICT (hash) DO UPDATE SET ast = excluded.ast,
             rootTable = excluded.rootTable, deps = excluded.deps",
        &[text(hash), text(ast_text), text(&ast.table), text(deps)],
    )?;
    Ok(())
}

pub fn set_desire(
    db: &mut dyn SyncDb,
    group: &str,
    client: &str,
    hash: &str,
    client_version: i64,
) -> Result<(), EngineError> {
    db.exec(
        "INSERT INTO _zsync_desires (clientGroupID, clientID, hash, clientVersion)
         VALUES (?, ?, ?, ?)
         ON CONFLICT (clientGroupID, clientID, hash) DO UPDATE SET clientVersion = excluded.clientVersion",
        &[text(group), text(client), text(hash), SqlValue::Text(client_version.to_string())],
    )?;
    Ok(())
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

// the client's current query-state version = the max version it has recorded
// across its desires (0 if it desires nothing)
pub(crate) fn client_query_version(
    db: &mut dyn SyncDb,
    group: &str,
    client: &str,
) -> Result<i64, EngineError> {
    let rows = db.query(
        "SELECT CAST(COALESCE(MAX(clientVersion), 0) AS TEXT) AS v FROM _zsync_desires
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
    Ok(())
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
}

// the distinct queries any client in the group currently desires
fn active_queries(db: &mut dyn SyncDb, group: &str) -> Result<Vec<ActiveQuery>, EngineError> {
    let rows = db.query(
        "SELECT DISTINCT q.hash AS hash, q.ast AS ast, q.rootTable AS rootTable
         FROM _zsync_desires d JOIN _zsync_queries q ON q.hash = d.hash
         WHERE d.clientGroupID = ?",
        &[text(group)],
    )?;
    let mut out = Vec::new();
    for row in &rows {
        let hash = str_col(row.get("hash"))?;
        let ast_text = str_col(row.get("ast"))?;
        let root_table = str_col(row.get("rootTable"))?;
        let ast_json: Value = serde_json::from_str(&ast_text)
            .map_err(|e| EngineError::internal(format!("stored query ast is not json: {e}")))?;
        out.push(ActiveQuery {
            hash,
            ast_json,
            root_table,
        });
    }
    Ok(out)
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
            values.insert((cr.child_table.clone(), key), zero_row(child_spec, row));
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
    let queries = active_queries(db, group)?;

    // net reference delta per (table, pk), and the live value cache for puts
    let mut ref_delta: BTreeMap<(String, String), i64> = BTreeMap::new();
    let mut values: BTreeMap<(String, String), Value> = BTreeMap::new();

    for q in &queries {
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
            values.insert((q.root_table.clone(), key), zero_row(spec, row));
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
    let active_hashes: BTreeSet<&str> = queries.iter().map(|q| q.hash.as_str()).collect();
    let lingering = db.query(
        "SELECT hash, rowTable, rowPk FROM _zsync_query_rows WHERE clientGroupID = ?",
        &[text(group)],
    )?;
    for row in &lingering {
        let hash = str_col(row.get("hash"))?;
        if active_hashes.contains(hash.as_str()) {
            continue;
        }
        let table = str_col(row.get("rowTable"))?;
        let pk = str_col(row.get("rowPk"))?;
        db.exec(
            "DELETE FROM _zsync_query_rows
             WHERE clientGroupID = ? AND hash = ? AND rowTable = ? AND rowPk = ?",
            &[text(group), text(&hash), text(&table), text(&pk)],
        )?;
        *ref_delta.entry((table, pk)).or_insert(0) -= 1;
    }

    // apply net deltas; emit a put on 0 -> positive, a del on positive -> 0
    let mut patch: Vec<Value> = Vec::new();
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
            patch.push(json!({ "op": "put", "tableName": table, "value": value }));
        } else if old > 0 && new == 0 {
            let spec = tables.get(table).unwrap();
            let pk_obj: Value = serde_json::from_str(pk).unwrap_or(Value::Null);
            patch.push(json!({ "op": "del", "tableName": table, "id": zero_pk_id(spec, &pk_obj) }));
        }
    }

    // re-emit rows whose data changed but whose membership did not (still
    // referenced, not part of the net delta this round)
    for (table, pk) in changed {
        let key = (table.clone(), pk.clone());
        if ref_delta.get(&key).copied().unwrap_or(0) != 0 {
            continue; // already emitted a put/del (or a no-op net) above
        }
        if read_ref(db, group, table, pk)? > 0
            && let Some(value) = values.get(&key)
        {
            patch.push(json!({ "op": "put", "tableName": table, "value": value }));
        }
    }

    Ok(patch)
}
