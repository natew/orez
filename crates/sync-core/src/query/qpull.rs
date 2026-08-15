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

use crate::db::SyncDb;
use crate::error::EngineError;
use crate::ledger;
use crate::schema::Tables;
use crate::store;
use crate::wire;

use super::membership::{
    advance_query_ack, canonical_pk_text, clear_desires, client_query_version, delete_clients,
    desired_hashes, prepare_transform_version, recompute_group_with_rehydrate, register_query,
    remove_desire, set_desire, validate_active_queries,
};

const MAX_DELETED_CLIENTS_PER_PULL: usize = 64;

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
    tables: &Tables,
    cookie: i64,
) -> Result<BTreeSet<(String, String)>, EngineError> {
    let mut out = BTreeSet::new();
    for (table, pk) in ledger::scan_since(db, cookie)?.changes {
        let Some(pk) = canonical_pk_text(tables, &table, &pk) else {
            continue;
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
    let deleted_client_ids = match body.get("deletedClientIDs") {
        None => BTreeSet::new(),
        Some(Value::Array(ids)) if ids.len() <= MAX_DELETED_CLIENTS_PER_PULL => {
            let mut parsed = BTreeSet::new();
            for id in ids {
                let Some(id) = id.as_str().filter(|id| !id.is_empty()) else {
                    return Err(EngineError::bad_request(
                        "deletedClientIDs must contain non-empty strings",
                    ));
                };
                if id == client_id {
                    return Err(EngineError::bad_request(
                        "deletedClientIDs cannot contain the requesting client",
                    ));
                }
                parsed.insert(id.to_string());
            }
            parsed
        }
        Some(Value::Array(_)) => {
            return Err(EngineError::bad_request(format!(
                "deletedClientIDs cannot contain more than {MAX_DELETED_CLIENTS_PER_PULL} entries"
            )));
        }
        Some(_) => {
            return Err(EngineError::bad_request(
                "deletedClientIDs must be an array",
            ));
        }
    };

    store::claim_client(db, group, client_id, user_id)?;
    delete_clients(db, group, &deleted_client_ids)?;

    let transform_client_reset = match body.get("_serverQueryTransformVersion") {
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
    let invalidated_hashes = if transform_client_reset {
        let hashes = desired_hashes(db, group, client_id)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        clear_desires(db, group, client_id)?;
        hashes
    } else {
        BTreeSet::new()
    };

    // hashes desired before this request's patch: any that end up no longer
    // desired after it get a targeted gotQueries del below. without the del the
    // client's got-state keeps the hash forever, and a later re-add of the same
    // query completes from an empty local store before its rows arrive.
    let prior_hashes = match body.get("queries") {
        None | Some(Value::Null) => BTreeSet::new(),
        Some(_) => desired_hashes(db, group, client_id)?
            .into_iter()
            .collect::<BTreeSet<_>>(),
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

    // a replica with no usable cookie needs a full response, but that says
    // nothing about the group's durable membership. preserve the membership,
    // recompute every active query against it, and write only genuine deltas.
    // a server transform reset remains a fail-closed group invalidation.
    let below_floor = match cookie {
        Some(c) => c < store::floor(db)?,
        None => true,
    };
    let full_response = cookie.is_none() || below_floor || transform_client_reset;

    // fast path: caught up and no desired-query change -> unchanged
    if !full_response
        && cookie == Some(current)
        && applied_queries.is_none()
        && deleted_client_ids.is_empty()
    {
        validate_active_queries(db, tables, group)?;
        return Ok(json!({ "cookie": wire::counter_to_json(current)?, "unchanged": true }));
    }

    let changed = if full_response {
        BTreeSet::new()
    } else {
        scan_changed(db, tables, cookie.unwrap())?
    };
    let rehydrate = applied_queries.as_ref().cloned().unwrap_or_default();
    let mut rows_patch =
        recompute_group_with_rehydrate(db, tables, group, &changed, &rehydrate, full_response)?;
    if full_response {
        // membership departures were already applied durably. after a clear,
        // only the current union belongs in the snapshot response.
        rows_patch.retain(|op| op.get("op").and_then(Value::as_str) != Some("del"));
        // wipe the client store before the full re-send
        rows_patch.insert(0, json!({ "op": "clear" }));
    }

    // gotQueries: acknowledge the client's currently-desired queries at its
    // query-state version. built AFTER the recompute, in the same transaction,
    // so the ack never precedes the row effects (invariant 13).
    let desired_hashes = desired_hashes(db, group, client_id)?;
    let desired_set = desired_hashes.iter().cloned().collect::<BTreeSet<_>>();
    let mut got_patch = Vec::new();
    for hash in invalidated_hashes.union(&prior_hashes) {
        if !desired_set.contains(hash) {
            got_patch.push(json!({ "op": "del", "hash": hash }));
        }
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

    let mut response = json!({
        "cookie": wire::counter_to_json(current)?,
        "lastMutationIDChanges": Value::Object(lmid_map),
        "rowsPatch": rows_patch,
        "gotQueries": { "version": wire::counter_to_json(ack_version)?, "patch": got_patch },
    });
    if !deleted_client_ids.is_empty() {
        response["deletedClientIDs"] = json!(deleted_client_ids);
    }
    Ok(response)
}
