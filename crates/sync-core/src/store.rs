// shared access to the durable `_zsync_*` tables. every counter (watermark,
// floor, high-water, last-mutation-id) is read with CAST(x AS TEXT) and parsed
// to i64, and written as decimal text into INTEGER-affinity columns, so a value
// never passes through a float on any host (sol-m0's precision contract). all
// functions assume the host has a transaction open; none emits BEGIN/COMMIT.

use std::collections::{BTreeMap, BTreeSet};

use crate::db::{SqlValue, SyncDb};
use crate::error::EngineError;
use crate::ledger;

// bind an i64 counter as decimal text; INTEGER affinity coerces it losslessly
pub(crate) fn counter(value: i64) -> SqlValue {
    SqlValue::Text(value.to_string())
}

fn text(s: impl Into<String>) -> SqlValue {
    SqlValue::Text(s.into())
}

// parse the first column of the first row as an i64 counter (missing -> 0)
fn read_i64(db: &mut dyn SyncDb, sql: &str, params: &[SqlValue]) -> Result<i64, EngineError> {
    let rows = db.query(sql, params)?;
    match rows.first().and_then(|r| r.values.first()) {
        None | Some(SqlValue::Null) => Ok(0),
        Some(SqlValue::Text(s)) => s
            .parse::<i64>()
            .map_err(|_| EngineError::internal(format!("counter is not an integer: {s}"))),
        Some(SqlValue::Integer(i)) => Ok(*i),
        Some(other) => Err(EngineError::internal(format!(
            "counter has wrong type: {other:?}"
        ))),
    }
}

// the packed ledger's high watermark, bounded below by the durable high-water
// imported from retired journals during schema initialization.
pub(crate) fn watermark(db: &mut dyn SyncDb) -> Result<i64, EngineError> {
    let high = read_i64(
        db,
        "SELECT CAST(high AS TEXT) FROM _zsync_watermark WHERE lock = 1",
        &[],
    )?;
    let segment = ledger::watermark(db)?;
    Ok(high.max(segment))
}

pub(crate) fn floor(db: &mut dyn SyncDb) -> Result<i64, EngineError> {
    read_i64(
        db,
        "SELECT CAST(floor AS TEXT) FROM _zsync_meta WHERE lock = 1",
        &[],
    )
}

// size-bounded retention: prune packed segments at or below the retained
// cutoff, then raise the floor.
pub(crate) fn prune(db: &mut dyn SyncDb, retain_changes: i64) -> Result<(), EngineError> {
    let cutoff = watermark(db)? - retain_changes;
    if cutoff > floor(db)? {
        ledger::prune(db, cutoff)?;
        db.exec(
            "UPDATE _zsync_meta SET floor = ? WHERE lock = 1",
            &[counter(cutoff)],
        )?;
    }
    Ok(())
}

// epoch bump: append a marker (advances the watermark past every cookie) and
// raise the floor past every prior watermark, so every client's next pull is a
// full snapshot.
pub(crate) fn invalidate(db: &mut dyn SyncDb) -> Result<(), EngineError> {
    let version = ledger::finalize(db, None, true)?;
    db.exec(
        "UPDATE _zsync_meta SET floor = ? WHERE lock = 1",
        &[counter(version)],
    )?;
    Ok(())
}

// guarded client-group claim (the reference core's claimClient / soot's
// claimStatement): bind the group to this user unless another user already owns
// it, adopting any userID-less rows. positional `?` only (DO has no ?N), so the
// repeated params are passed twice. 403 if a different user owns the group.
pub(crate) fn claim_client(
    db: &mut dyn SyncDb,
    client_group_id: &str,
    client_id: &str,
    user_id: &str,
) -> Result<(), EngineError> {
    db.exec(
        "INSERT INTO _zsync_clients (clientGroupID, clientID, lastMutationID, userID)
         SELECT ?, ?, 0, ?
         WHERE NOT EXISTS (
           SELECT 1 FROM _zsync_clients
           WHERE clientGroupID = ? AND userID IS NOT NULL AND userID <> ?
         )
         ON CONFLICT (clientGroupID, clientID)
         DO UPDATE SET userID = excluded.userID WHERE userID IS NULL",
        &[
            text(client_group_id),
            text(client_id),
            text(user_id),
            text(client_group_id),
            text(user_id),
        ],
    )?;
    let owners = db.query(
        "SELECT DISTINCT userID FROM _zsync_clients
         WHERE clientGroupID = ? AND userID IS NOT NULL",
        &[text(client_group_id)],
    )?;
    for row in &owners {
        if let Some(SqlValue::Text(owner)) = row.values.first()
            && owner != user_id
        {
            return Err(EngineError::forbidden(
                "client group belongs to a different user",
            ));
        }
    }
    Ok(())
}

pub(crate) fn delete_clients(
    db: &mut dyn SyncDb,
    client_group_id: &str,
    client_ids: &BTreeSet<String>,
) -> Result<(), EngineError> {
    if client_ids.is_empty() {
        return Ok(());
    }
    let placeholders = std::iter::repeat_n("?", client_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let mut params = Vec::with_capacity(client_ids.len() + 1);
    params.push(text(client_group_id));
    params.extend(client_ids.iter().map(text));
    db.exec(
        &format!(
            "DELETE FROM _zsync_clients
             WHERE clientGroupID = ? AND clientID IN ({placeholders})"
        ),
        &params,
    )?;
    Ok(())
}

pub(crate) fn read_lmid(
    db: &mut dyn SyncDb,
    client_group_id: &str,
    client_id: &str,
) -> Result<i64, EngineError> {
    read_i64(
        db,
        "SELECT CAST(lastMutationID AS TEXT) FROM _zsync_clients
         WHERE clientGroupID = ? AND clientID = ?",
        &[text(client_group_id), text(client_id)],
    )
}

// the full current lmid map for a group (snapshot responses read it directly)
pub(crate) fn all_lmids(
    db: &mut dyn SyncDb,
    client_group_id: &str,
) -> Result<BTreeMap<String, i64>, EngineError> {
    let rows = db.query(
        "SELECT clientID, CAST(lastMutationID AS TEXT) AS lmid FROM _zsync_clients
         WHERE clientGroupID = ?",
        &[text(client_group_id)],
    )?;
    let mut out = BTreeMap::new();
    for row in rows {
        let Some(SqlValue::Text(client)) = row.get("clientID") else {
            return Err(EngineError::internal("client identity is invalid"));
        };
        let lmid = match row.get("lmid") {
            Some(SqlValue::Text(value)) => value
                .parse::<i64>()
                .ok()
                .filter(|value| *value >= 0)
                .ok_or_else(|| EngineError::internal("client lmid is invalid"))?,
            Some(SqlValue::Integer(value)) if *value >= 0 => *value,
            _ => return Err(EngineError::internal("client lmid is invalid")),
        };
        out.insert(client.clone(), lmid);
    }
    Ok(out)
}

// advance a client's lmid beside an empty change envelope, so an lmid-only
// push still moves the cookie and settles on the next pull.
pub(crate) fn advance_lmid(
    db: &mut dyn SyncDb,
    client_group_id: &str,
    client_id: &str,
    mutation_id: i64,
) -> Result<(), EngineError> {
    ledger::finalize(db, Some((client_group_id, client_id, mutation_id)), false)?;
    Ok(())
}
