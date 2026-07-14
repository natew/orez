// the query-aware pull entry point: the additive, versioned extension of the
// baseline pull. it applies the client's desiredQueriesPatch, recomputes the
// group's query membership, and returns membership-driven row puts/dels plus a
// gotQueries acknowledgement — all inside the one host transaction, so the query
// acknowledgement can never precede its row effects (invariant 13).
//
// the baseline (no `queries` key) request/response stays byte-identical to
// handle_pull; this is a SEPARATE entry point a host routes to only for
// query-aware consumers.

use std::collections::BTreeSet;

use serde_json::{Map, Value, json};

use crate::db::{SqlValue, SyncDb};
use crate::error::EngineError;
use crate::schema::Tables;
use crate::store;
use crate::wire;

use super::membership::{
    advance_query_ack, canonical_pk_text, clear_desires, client_query_version, desired_hashes,
    prepare_transform_version, recompute_group_with_rehydrate, register_query, remove_desire,
    reset_group, set_desire,
};

// apply the desiredQueriesPatch and return queries newly desired by this client.
// an existing group can already have
// durable membership for those hashes, but the new client's local store may not
// have the rows and needs an idempotent re-send.
fn apply_desired_patch(
    db: &mut dyn SyncDb,
    tables: &Tables,
    group: &str,
    client: &str,
    queries: &Value,
) -> Result<BTreeSet<String>, EngineError> {
    let obj = queries
        .as_object()
        .ok_or_else(|| EngineError::bad_request("queries must be an object"))?;
    let version = obj
        .get("version")
        .and_then(wire::non_negative_safe_int)
        .ok_or_else(|| {
            EngineError::bad_request("queries.version must be a non-negative integer")
        })?;
    let patch = obj
        .get("patch")
        .and_then(Value::as_array)
        .ok_or_else(|| EngineError::bad_request("queries.patch must be an array"))?;
    let mut rehydrate = BTreeSet::new();
    for op in patch {
        let kind = op.get("op").and_then(Value::as_str);
        match kind {
            Some("put") => {
                let hash = op
                    .get("hash")
                    .and_then(Value::as_str)
                    .ok_or_else(|| EngineError::bad_request("query put requires a hash"))?;
                let ast = op
                    .get("ast")
                    .ok_or_else(|| EngineError::bad_request("query put requires an ast"))?;
                // a SERVER-OWNED permission/schema transformation version the host
                // attaches to the resolved put op (after resolveQuery, alongside
                // the ast). NEVER client-trusted: the host strips client fields and
                // re-emits {op, hash, ast[, transformVersion]}, so a client cannot
                // set it. a bump forces this group to recompute even when the AST
                // text is unchanged, so a tightened transform can never retain
                // older, more-permissive rows. absent -> 0, which is still safe
                // because any AST-content change already forces a recompute.
                let transform_version = match op.get("transformVersion") {
                    None | Some(Value::Null) => 0,
                    Some(v) => wire::non_negative_safe_int(v).ok_or_else(|| {
                        EngineError::bad_request("transformVersion must be a non-negative integer")
                    })?,
                };
                register_query(db, tables, group, hash, ast, transform_version)?;
                if set_desire(db, group, client, hash, version)? {
                    rehydrate.insert(hash.to_string());
                }
            }
            Some("del") => {
                let hash = op
                    .get("hash")
                    .and_then(Value::as_str)
                    .ok_or_else(|| EngineError::bad_request("query del requires a hash"))?;
                remove_desire(db, group, client, hash)?;
            }
            Some("clear") => clear_desires(db, group, client)?,
            _ => return Err(EngineError::bad_request("unknown desiredQueriesPatch op")),
        }
    }
    // record the applied version monotonically so a later del/clear cannot make
    // the gotQueries ack regress (MEDIUM-6). done once per patch, after every op,
    // covering put/del/clear and an empty patch that only bumps the version.
    advance_query_ack(db, group, client, version)?;
    Ok(rehydrate)
}

// (table, canonical pk) touched since the cookie — for the membership
// recompute's phase-3 re-emit of changed-but-still-member rows
fn scan_changed(
    db: &mut dyn SyncDb,
    cookie: i64,
) -> Result<BTreeSet<(String, String)>, EngineError> {
    let rows = db.query(
        "SELECT tableName, pk FROM _zsync_changes WHERE watermark > ? AND op = 'row'",
        &[store::counter(cookie)],
    )?;
    let mut out = BTreeSet::new();
    for row in &rows {
        let table = match row.get("tableName") {
            Some(SqlValue::Text(s)) => s.clone(),
            _ => continue,
        };
        let pk = match row.get("pk") {
            Some(SqlValue::Text(s)) => canonical_pk_text(s),
            _ => continue,
        };
        out.insert((table, pk));
    }
    Ok(out)
}

pub fn handle_query_pull(
    db: &mut dyn SyncDb,
    tables: &Tables,
    retain_changes: i64,
    body: &Value,
    user_id: &str,
) -> Result<Value, EngineError> {
    let client_id = body.get("clientID").and_then(Value::as_str);
    let group = body.get("clientGroupID").and_then(Value::as_str);
    let cookie_present = body.get("cookie").is_some();
    let cookie = wire::parse_cookie(body.get("cookie"));
    let (client_id, group, cookie) = match (client_id, group, cookie) {
        (Some(c), Some(g), Ok(cookie)) if cookie_present => (c, g, cookie),
        _ => return Err(EngineError::bad_request("invalid pull body")),
    };

    store::claim_client(db, group, client_id, user_id)?;

    let transform_reset = match body.get("_serverQueryTransformVersion") {
        None => false,
        Some(version) => {
            let version = wire::non_negative_safe_int(version).ok_or_else(|| {
                EngineError::bad_request(
                    "_serverQueryTransformVersion must be a non-negative integer",
                )
            })?;
            prepare_transform_version(db, group, client_id, version)?
        }
    };
    // preserve the old hashes long enough to send targeted gotQueries dels.
    // this makes Zero resend each named query without clearing its local
    // custom-query mapping.
    let invalidated_hashes = if transform_reset {
        let hashes = desired_hashes(db, group, client_id)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        clear_desires(db, group, client_id)?;
        hashes
    } else {
        BTreeSet::new()
    };

    // apply the desired-query lifecycle before recomputing
    let applied_queries = match body.get("queries") {
        None | Some(Value::Null) => None,
        Some(queries) => Some(apply_desired_patch(db, tables, group, client_id, queries)?),
    };

    store::prune(db, retain_changes)?;
    let current = store::watermark(db)?;
    if let Some(c) = cookie
        && c > current
    {
        return Err(EngineError::conflict(format!(
            "future cookie {c} is ahead of watermark {current}"
        )));
    }

    // a fresh or reset client (null / below-floor cookie) is re-synced from
    // scratch: reset the group membership, clear the client store, re-send all
    // current members. otherwise diff incrementally.
    let below_floor = match cookie {
        Some(c) => c < store::floor(db)?,
        None => true,
    };
    let fresh = cookie.is_none() || below_floor || transform_reset;

    // fast path: caught up and no desired-query change -> unchanged
    if !fresh && cookie == Some(current) && applied_queries.is_none() {
        return Ok(json!({ "cookie": wire::counter_to_json(current)?, "unchanged": true }));
    }

    if fresh {
        reset_group(db, group)?;
    }
    let changed = if fresh {
        BTreeSet::new()
    } else {
        scan_changed(db, cookie.unwrap())?
    };
    let rehydrate = applied_queries.as_ref().cloned().unwrap_or_default();
    let mut rows_patch = recompute_group_with_rehydrate(db, tables, group, &changed, &rehydrate)?;
    if fresh {
        // wipe the client store before the full re-send
        rows_patch.insert(0, json!({ "op": "clear" }));
    }

    // gotQueries: acknowledge the client's currently-desired queries at its
    // query-state version. built AFTER the recompute, in the same transaction,
    // so the ack never precedes the row effects (invariant 13).
    let desired_hashes = desired_hashes(db, group, client_id)?;
    let desired_set = desired_hashes.iter().cloned().collect::<BTreeSet<_>>();
    let mut got_patch = Vec::new();
    for hash in invalidated_hashes.difference(&desired_set) {
        got_patch.push(json!({ "op": "del", "hash": hash }));
    }
    for hash in desired_hashes {
        got_patch.push(json!({ "op": "put", "hash": hash }));
    }
    // the version the client is now synced to: its durable, monotonic query-state
    // version (advanced above when this request carried a patch), acknowledged
    // only now that the row effects are durable. reading the stored value — not
    // this request's version — keeps the ack from regressing across del/clear.
    let ack_version = client_query_version(db, group, client_id)?;

    let lmids = store::all_lmids(db, group)?;
    let mut lmid_map = Map::new();
    for (client, lmid) in &lmids {
        lmid_map.insert(client.clone(), wire::counter_to_json(*lmid)?);
    }

    Ok(json!({
        "cookie": wire::counter_to_json(current)?,
        "lastMutationIDChanges": Value::Object(lmid_map),
        "rowsPatch": rows_patch,
        "gotQueries": { "version": wire::counter_to_json(ack_version)?, "patch": got_patch },
    }))
}
