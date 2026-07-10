// pull: cursor-diff and snapshot responses over the trigger-fed change log.
// ported from the reference core's handlePull + soot's cursorPull composition
// (byte/row caps with a last-included-watermark cookie, log-derived prefix
// LMIDs). MUST be called inside one host-entered transaction: this module
// issues no BEGIN/COMMIT (durable object SQL rejects them) and relies on the
// host's transaction for a single consistent view.

use std::collections::{BTreeMap, HashSet};

use serde_json::{Map, Value, json};

use crate::db::{Row, SqlValue, SyncDb};
use crate::error::EngineError;
use crate::schema::{TableSpec, Tables, quote_ident};
use crate::store;
use crate::value::{to_zero_value, to_zero_value_json};
use crate::wire;

// per-user row visibility. `filter` returns a WHERE fragment + params selecting
// the user's visible rows of a table (or None for "the whole table"). applied
// to snapshot reads always, and to diff point-reads when `row_local` is true.
//
// `row_local` declares whether the predicate depends ONLY on the row's own
// columns. a row-local predicate is safe to serve through cursor diffs (a
// flip always touches the row, so the change log sees it). a non-row-local
// predicate can flip without touching the row, which no diff can express, so
// it forces every pull to a full snapshot (the reference core's `visible`
// behavior). see invariants 8, 9, 15.
pub struct Visibility<'a> {
    pub row_local: bool,
    pub filter: Box<dyn Fn(&str, &str) -> Option<VisibleFilter> + 'a>,
}

pub struct VisibleFilter {
    pub sql: String,
    pub params: Vec<SqlValue>,
}

impl Visibility<'_> {
    fn of(&self, table: &str, user: &str) -> Option<VisibleFilter> {
        (self.filter)(table, user)
    }
}

// caps bound one diff response. the cut happens at a change-row boundary
// BEFORE primary-key deduplication and the returned cookie is the last
// INCLUDED watermark, so the remainder ships on the next poll and retention
// can never delete beyond the last included prefix (invariants 5, 6).
#[derive(Debug, Clone, Copy)]
pub struct Caps {
    pub max_change_rows: usize,
    pub max_change_bytes: usize,
}

impl Default for Caps {
    fn default() -> Self {
        // soot's production budgets: a backlog-pacing budget, not a platform
        // limit. one oversize row is always admitted so progress is possible.
        Caps {
            max_change_rows: 10_000,
            max_change_bytes: 2_000_000,
        }
    }
}

// the single pull entry point. `body` is the raw {clientID, clientGroupID,
// cookie} object; the response is {cookie, unchanged} | {cookie,
// lastMutationIDChanges, rowsPatch}, byte-compatible with the vendored
// http-pull transport.
pub fn handle_pull(
    db: &mut dyn SyncDb,
    tables: &Tables,
    retain_changes: i64,
    visible: Option<&Visibility>,
    caps: Caps,
    body: &Value,
    user_id: &str,
) -> Result<Value, EngineError> {
    // validate the pull body (matches the reference core's validatePullBody):
    // clientID/clientGroupID are strings and the cookie field is PRESENT and is
    // null or a valid counter. a missing cookie (undefined) is malformed.
    let client_id = body.get("clientID").and_then(Value::as_str);
    let group = body.get("clientGroupID").and_then(Value::as_str);
    let cookie_present = body.get("cookie").is_some();
    let cookie = wire::parse_cookie(body.get("cookie"));
    let (client_id, group, cookie) = match (client_id, group, cookie) {
        (Some(c), Some(g), Ok(cookie)) if cookie_present => (c, g, cookie),
        _ => return Err(EngineError::bad_request("invalid pull body")),
    };

    // one host transaction = one consistent view. claim first, then enforce
    // retention (upstream/admin writes also feed the log, so a read-only
    // workload must not depend on a later push to prune).
    store::claim_client(db, group, client_id, user_id)?;
    store::prune(db, retain_changes)?;
    let current = store::watermark(db)?;

    if let Some(c) = cookie {
        if c > current {
            return Err(EngineError::conflict(format!(
                "future cookie {c} is ahead of watermark {current}"
            )));
        }
        if c == current {
            return Ok(json!({ "cookie": wire::counter_to_json(current), "unchanged": true }));
        }
    }

    // diff pulls need uniform (or row-local) visibility: a non-row-local
    // visible() filter can revoke rows without any row change, which no diff
    // can express, so those configs always snapshot. below the floor also
    // snapshots (the prefix was pruned).
    let can_diff = match cookie {
        Some(c) => c >= store::floor(db)? && visible.map(|v| v.row_local).unwrap_or(true),
        None => false,
    };

    let (cookie_out, lmids, rows_patch) = if can_diff {
        diff(
            db,
            tables,
            group,
            cookie.unwrap(),
            current,
            caps,
            visible,
            user_id,
        )?
    } else {
        let lmids = store::all_lmids(db, group)?;
        (current, lmids, snapshot(db, tables, visible, user_id)?)
    };

    Ok(json!({
        "cookie": wire::counter_to_json(cookie_out),
        "lastMutationIDChanges": lmid_json(&lmids),
        "rowsPatch": rows_patch,
    }))
}

fn lmid_json(lmids: &BTreeMap<String, i64>) -> Value {
    let mut map = Map::new();
    for (client, lmid) in lmids {
        map.insert(client.clone(), wire::counter_to_json(*lmid));
    }
    Value::Object(map)
}

// snapshot: clear + one put per visible row of every table, live values.
fn snapshot(
    db: &mut dyn SyncDb,
    tables: &Tables,
    visible: Option<&Visibility>,
    user_id: &str,
) -> Result<Vec<Value>, EngineError> {
    let mut patch = vec![json!({ "op": "clear" })];
    // collect statements first so the mutable db borrow is not held across the
    // visibility closure call
    let plans: Vec<(String, String, Vec<SqlValue>)> = tables
        .iter()
        .map(|(table, _)| {
            let (sql, params) = match visible.and_then(|v| v.of(table, user_id)) {
                Some(filter) => (
                    format!("SELECT * FROM {} WHERE {}", quote_ident(table), filter.sql),
                    filter.params,
                ),
                None => (format!("SELECT * FROM {}", quote_ident(table)), Vec::new()),
            };
            (table.to_string(), sql, params)
        })
        .collect();

    for (table, sql, params) in plans {
        let spec = tables.get(&table).expect("table in spec");
        for row in db.query(&sql, &params)? {
            patch.push(json!({ "op": "put", "tableName": table, "value": row_value(spec, &row) }));
        }
    }
    Ok(patch)
}

// build a zero-typed row object from a live sqlite row
fn row_value(spec: &TableSpec, row: &Row) -> Value {
    let mut value = Map::new();
    for (col, ty) in &spec.columns {
        let raw = row.get(col).cloned().unwrap_or(SqlValue::Null);
        value.insert(col.clone(), to_zero_value(*ty, &raw));
    }
    Value::Object(value)
}

// one change-log row the diff scan walks
struct Change {
    watermark: i64,
    table_name: String,
    op: String,
    pk: Option<Value>,
}

// pks touched since the cookie, capped at a change-row boundary before pk
// dedup, resolved against LIVE table state. returns (cookie, prefix lmids,
// rowsPatch). the cookie is the last INCLUDED watermark when capped, else the
// current watermark (matching the reference core's uncapped behavior).
fn diff(
    db: &mut dyn SyncDb,
    tables: &Tables,
    group: &str,
    cookie: i64,
    current: i64,
    caps: Caps,
    visible: Option<&Visibility>,
    user_id: &str,
) -> Result<(i64, BTreeMap<String, i64>, Vec<Value>), EngineError> {
    // read one extra row to detect "there is more beyond the row cap"
    let raw = db.query(
        "SELECT CAST(watermark AS TEXT) AS w, tableName, op, pk FROM _zsync_changes
         WHERE watermark > ? ORDER BY watermark LIMIT ?",
        &[
            store::counter(cookie),
            SqlValue::Integer(caps.max_change_rows as i64 + 1),
        ],
    )?;
    let row_capped = raw.len() > caps.max_change_rows;
    let mut changes: Vec<Change> = Vec::with_capacity(raw.len().min(caps.max_change_rows));
    for row in raw.iter().take(caps.max_change_rows) {
        changes.push(Change {
            watermark: counter_col(row.get("w")),
            table_name: text_col(row.get("tableName")),
            op: text_col(row.get("op")),
            pk: match row.get("pk") {
                Some(SqlValue::Text(s)) => Some(
                    serde_json::from_str(s)
                        .map_err(|e| EngineError::internal(format!("bad change pk json: {e}")))?,
                ),
                _ => None,
            },
        });
    }

    // walk change rows in watermark order, deduping touched pks and collecting
    // prefix lmids, applying the byte budget at change-row boundaries. include a
    // row only if it keeps the response within budget (always admit the first
    // change row so progress is possible). because effects always precede their
    // lmid row in watermark order, a cut that excludes an lmid row leaves the
    // ack for the next poll — an ack never precedes its effects (invariant 3).
    let mut seen: HashSet<String> = HashSet::new();
    let mut ops: Vec<Value> = Vec::new();
    let mut lmids: BTreeMap<String, i64> = BTreeMap::new();
    let mut bytes: usize = 0;
    let mut included = 0usize;
    let mut cut_watermark = cookie;
    let mut byte_capped = false;

    for change in &changes {
        // compute this change row's contribution WITHOUT committing it, so a
        // budget overflow can cut before it.
        let mut pending_op: Option<Value> = None;
        let mut pending_key: Option<String> = None;
        let mut pending_lmid: Option<(String, i64)> = None;
        let mut delta = 0usize;

        match change.op.as_str() {
            "row" => {
                let spec = tables.get(&change.table_name).ok_or_else(|| {
                    // invariant 10: a synced-looking change for a table the
                    // engine does not know must fail loudly, never drop silently.
                    EngineError::internal(format!(
                        "change log row for unmapped table '{}'",
                        change.table_name
                    ))
                })?;
                let pk = change
                    .pk
                    .as_ref()
                    .ok_or_else(|| EngineError::internal("row change missing pk".to_string()))?;
                let key = dedup_key(&change.table_name, spec, pk);
                if !seen.contains(&key) {
                    let op = resolve_row(db, &change.table_name, spec, pk, visible, user_id)?;
                    delta = serde_json::to_string(&op).map(|s| s.len()).unwrap_or(0);
                    pending_op = Some(op);
                    pending_key = Some(key);
                }
                // an already-seen pk contributes no bytes (identical live
                // resolution) and is free to include.
            }
            "lmid" => {
                // acks for THIS group only — never leak a peer group's lmid
                if let Some(pk) = &change.pk {
                    let same_group = pk.get("clientGroupID").and_then(Value::as_str) == Some(group);
                    if same_group {
                        if let (Some(client), Some(lmid)) = (
                            pk.get("clientID").and_then(Value::as_str),
                            pk.get("lmid").and_then(parse_lmid_field),
                        ) {
                            if lmid > lmids.get(client).copied().unwrap_or(0) {
                                pending_lmid = Some((client.to_string(), lmid));
                                delta = client.len() + 24; // approx bytes of the ack entry
                            }
                        }
                    }
                }
            }
            // markers (epoch invalidation) advance the watermark only
            _ => {}
        }

        if included >= 1 && bytes + delta > caps.max_change_bytes {
            byte_capped = true;
            break;
        }

        if let Some(op) = pending_op {
            ops.push(op);
        }
        if let Some(key) = pending_key {
            seen.insert(key);
        }
        if let Some((client, lmid)) = pending_lmid {
            lmids.insert(client, lmid);
        }
        bytes += delta;
        included += 1;
        cut_watermark = change.watermark;
    }

    let capped = row_capped || byte_capped;
    let cookie_out = if capped { cut_watermark } else { current };
    Ok((cookie_out, lmids, ops))
}

// resolve one touched pk against live state: present -> put with current
// values, gone (or filtered out by row-local visibility) -> del by pk.
fn resolve_row(
    db: &mut dyn SyncDb,
    table: &str,
    spec: &TableSpec,
    pk: &Value,
    visible: Option<&Visibility>,
    user_id: &str,
) -> Result<Value, EngineError> {
    let where_pk = spec
        .primary_key
        .iter()
        .map(|col| format!("{} = ?", quote_ident(col)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let mut params: Vec<SqlValue> = spec
        .primary_key
        .iter()
        .map(|col| json_pk_to_sql(pk.get(col)))
        .collect();
    let mut sql = format!("SELECT * FROM {} WHERE {}", quote_ident(table), where_pk);
    // diff only runs under row-local (or absent) visibility, so applying the
    // filter to the point read is safe: an invisible row emits del.
    if let Some(filter) = visible.and_then(|v| v.of(table, user_id)) {
        sql.push_str(&format!(" AND ({})", filter.sql));
        params.extend(filter.params);
    }
    let rows = db.query(&sql, &params)?;
    match rows.first() {
        Some(row) => Ok(json!({ "op": "put", "tableName": table, "value": row_value(spec, row) })),
        None => Ok(json!({ "op": "del", "tableName": table, "id": pk_id(spec, pk) })),
    }
}

// del ids carry zero-typed primary-key columns
fn pk_id(spec: &TableSpec, pk: &Value) -> Value {
    let mut id = Map::new();
    for col in &spec.primary_key {
        let ty = spec
            .column_type(col)
            .unwrap_or(crate::value::ZeroColumnType::String);
        let raw = pk.get(col).cloned().unwrap_or(Value::Null);
        id.insert(col.clone(), to_zero_value_json(ty, raw));
    }
    Value::Object(id)
}

// dedup key = client table name + the pk column values, JSON-normalized (the
// reference core's `${table} ${JSON.stringify(primaryKey.map(pk))}`)
fn dedup_key(table: &str, spec: &TableSpec, pk: &Value) -> String {
    let vals: Vec<Value> = spec
        .primary_key
        .iter()
        .map(|c| pk.get(c).cloned().unwrap_or(Value::Null))
        .collect();
    format!(
        "{} {}",
        table,
        serde_json::to_string(&vals).unwrap_or_default()
    )
}

// bind a pk column (parsed from the change log's json_object) as a sqlite value
fn json_pk_to_sql(v: Option<&Value>) -> SqlValue {
    match v {
        None | Some(Value::Null) => SqlValue::Null,
        Some(Value::Bool(b)) => SqlValue::Integer(if *b { 1 } else { 0 }),
        Some(Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                SqlValue::Integer(i)
            } else {
                SqlValue::Real(n.as_f64().unwrap_or(0.0))
            }
        }
        Some(Value::String(s)) => SqlValue::Text(s.clone()),
        Some(other) => SqlValue::Text(other.to_string()),
    }
}

// the lmid value rides the json as text ("5") to avoid any number path
fn parse_lmid_field(v: &Value) -> Option<i64> {
    match v {
        Value::String(s) => s.parse::<i64>().ok(),
        Value::Number(n) => n.as_i64(),
        _ => None,
    }
}

fn counter_col(v: Option<&SqlValue>) -> i64 {
    match v {
        Some(SqlValue::Text(s)) => s.parse::<i64>().unwrap_or(0),
        Some(SqlValue::Integer(i)) => *i,
        _ => 0,
    }
}

fn text_col(v: Option<&SqlValue>) -> String {
    match v {
        Some(SqlValue::Text(s)) => s.clone(),
        _ => String::new(),
    }
}

// the change log's high watermark = max(durable high, MAX(log)). this is the
// cookie. monotonic through restart, eviction, and pruning (invariant 7).
pub fn watermark(db: &mut dyn SyncDb) -> Result<i64, EngineError> {
    store::watermark(db)
}

// the retained-log floor: cookies at or above it can be served as a diff,
// below it fall back to snapshot.
pub fn floor(db: &mut dyn SyncDb) -> Result<i64, EngineError> {
    store::floor(db)
}

// size-bounded retention: prune change rows below (watermark - retain),
// raising the floor. never prunes the top, so MAX(log) stays monotonic.
pub fn prune(db: &mut dyn SyncDb, retain_changes: i64) -> Result<(), EngineError> {
    store::prune(db, retain_changes)
}

// epoch invalidation: force every client's next pull to a full snapshot (for
// changes no row diff can express — visibility revocation, table-set change).
// appends a marker (advances the watermark past every cookie) and raises the
// floor past every prior watermark. host wraps this in a transaction.
pub fn invalidate(db: &mut dyn SyncDb) -> Result<(), EngineError> {
    store::invalidate(db)
}
