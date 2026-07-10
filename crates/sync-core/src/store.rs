// shared access to the durable `_zsync_*` tables. every counter (watermark,
// floor, high-water, last-mutation-id) is read with CAST(x AS TEXT) and parsed
// to i64, and written as decimal text into INTEGER-affinity columns, so a value
// never passes through a float on any host (sol-m0's precision contract). all
// functions assume the host has a transaction open; none emits BEGIN/COMMIT.

use std::collections::BTreeMap;

use crate::db::{SqlValue, SyncDb};
use crate::error::EngineError;

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
        Some(other) => Err(EngineError::internal(format!("counter has wrong type: {other:?}"))),
    }
}

// the change log's high watermark = max(durable high-water, MAX(log)). durable
// so it never regresses even if the log is emptied (invariant 7). bumps the
// stored high-water when the log has advanced past it.
pub(crate) fn watermark(db: &mut dyn SyncDb) -> Result<i64, EngineError> {
    let max_log = read_i64(
        db,
        "SELECT CAST(COALESCE(MAX(watermark), 0) AS TEXT) FROM _zsync_changes",
        &[],
    )?;
    let high = read_i64(db, "SELECT CAST(high AS TEXT) FROM _zsync_watermark WHERE lock = 1", &[])?;
    let wm = max_log.max(high);
    if wm > high {
        db.exec("UPDATE _zsync_watermark SET high = ? WHERE lock = 1", &[counter(wm)])?;
    }
    Ok(wm)
}

pub(crate) fn floor(db: &mut dyn SyncDb) -> Result<i64, EngineError> {
    read_i64(db, "SELECT CAST(floor AS TEXT) FROM _zsync_meta WHERE lock = 1", &[])
}

// size-bounded retention: prune change rows at or below (watermark - retain),
// raising the floor. never prunes the top row, so MAX(log) stays monotonic.
pub(crate) fn prune(db: &mut dyn SyncDb, retain_changes: i64) -> Result<(), EngineError> {
    let cutoff = watermark(db)? - retain_changes;
    if cutoff > floor(db)? {
        db.exec("DELETE FROM _zsync_changes WHERE watermark <= ?", &[counter(cutoff)])?;
        db.exec("UPDATE _zsync_meta SET floor = ? WHERE lock = 1", &[counter(cutoff)])?;
    }
    Ok(())
}

// epoch bump: append a marker (advances the watermark past every cookie) and
// raise the floor past every prior watermark, so every client's next pull is a
// full snapshot.
pub(crate) fn invalidate(db: &mut dyn SyncDb) -> Result<(), EngineError> {
    db.exec(
        "INSERT INTO _zsync_changes (tableName, op, pk) VALUES ('_zsync_meta', 'marker', NULL)",
        &[],
    )?;
    db.exec(
        "UPDATE _zsync_meta SET floor = (SELECT COALESCE(MAX(watermark), 0) FROM _zsync_changes)
         WHERE lock = 1",
        &[],
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
        if let Some(SqlValue::Text(owner)) = row.values.first() {
            if owner != user_id {
                return Err(EngineError::forbidden("client group belongs to a different user"));
            }
        }
    }
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
    let mut map = BTreeMap::new();
    for row in &rows {
        let client = match row.get("clientID") {
            Some(SqlValue::Text(s)) => s.clone(),
            _ => continue,
        };
        let lmid = match row.get("lmid") {
            Some(SqlValue::Text(s)) => s.parse::<i64>().unwrap_or(0),
            Some(SqlValue::Integer(i)) => *i,
            _ => 0,
        };
        map.insert(client, lmid);
    }
    Ok(map)
}

// advance a client's lmid and append an 'lmid' change row carrying the new
// value, so a capped diff can derive this ack from the included prefix only
// (invariant 3) and an lmid-only push still moves the cookie (invariant 4).
pub(crate) fn advance_lmid(
    db: &mut dyn SyncDb,
    client_group_id: &str,
    client_id: &str,
    mutation_id: i64,
) -> Result<(), EngineError> {
    db.exec(
        "UPDATE _zsync_clients SET lastMutationID = ?
         WHERE clientGroupID = ? AND clientID = ?",
        &[counter(mutation_id), text(client_group_id), text(client_id)],
    )?;
    // lmid value stored as text in the json so it too avoids any number path
    db.exec(
        "INSERT INTO _zsync_changes (tableName, op, pk)
         VALUES ('_zsync_clients', 'lmid', json_object('clientID', ?, 'lmid', ?))",
        &[text(client_id), counter(mutation_id)],
    )?;
    Ok(())
}
