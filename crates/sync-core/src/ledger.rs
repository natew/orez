use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::db::{DbError, SqlValue, SyncDb};
use crate::error::EngineError;

const ROTATE_AT_BYTES: usize = 768 * 1_024;
const MAX_PAYLOAD_BYTES: usize = 1_024 * 1_024;

// bump when the packed payload shape changes; part of schema_revision so hosts
// re-run the schema pass exactly once when a new format ships.
pub const LEDGER_FORMAT: u8 = 2;

fn is_false(value: &bool) -> bool {
    !value
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerTransaction {
    version: String,
    #[serde(default)]
    changes: Vec<(String, Value)>,
    #[serde(default, skip_serializing_if = "is_false")]
    reset: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerPayload {
    format: u8,
    transactions: Vec<LedgerTransaction>,
}

#[derive(Debug, Clone, Deserialize)]
struct LegacyLedgerTransaction {
    version: String,
    #[serde(default)]
    changes: Vec<(String, Value)>,
    #[serde(default, rename = "lmid")]
    _lmid: Option<Value>,
    #[serde(default)]
    reset: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct LegacyLedgerPayload {
    format: u8,
    lmids: BTreeMap<String, BTreeMap<String, String>>,
    transactions: Vec<LegacyLedgerTransaction>,
}

struct ActiveSegment {
    start: i64,
    end: i64,
    payload: LedgerPayload,
    pending: Vec<(String, Value)>,
    capture_mode: i64,
}

fn text(value: impl Into<String>) -> SqlValue {
    SqlValue::Text(value.into())
}

fn parse_counter(value: Option<&SqlValue>, name: &str) -> Result<i64, EngineError> {
    match value {
        Some(SqlValue::Integer(value)) if *value >= 0 => Ok(*value),
        Some(SqlValue::Text(value)) => value
            .parse::<i64>()
            .ok()
            .filter(|value| *value >= 0)
            .ok_or_else(|| EngineError::internal(format!("packed ledger {name} is invalid"))),
        _ => Err(EngineError::internal(format!(
            "packed ledger {name} is invalid"
        ))),
    }
}

fn parse_payload(value: Option<&SqlValue>) -> Result<LedgerPayload, EngineError> {
    let Some(SqlValue::Text(value)) = value else {
        return Err(EngineError::internal("packed ledger payload is not text"));
    };
    let payload: LedgerPayload = serde_json::from_str(value)
        .map_err(|_| EngineError::internal("packed ledger payload is invalid JSON"))?;
    if payload.format != LEDGER_FORMAT {
        return Err(EngineError::internal("packed ledger format is unsupported"));
    }
    Ok(payload)
}

fn active(db: &mut dyn SyncDb) -> Result<ActiveSegment, EngineError> {
    let rows = db.query(
        "SELECT CAST(startVersion AS TEXT) AS startVersion,
                CAST(endVersion AS TEXT) AS endVersion,
                payload, pending, CAST(captureMode AS TEXT) AS captureMode
         FROM _zsync_log_segments ORDER BY startVersion DESC LIMIT 1",
        &[],
    )?;
    let Some(row) = rows.first() else {
        return Err(EngineError::internal("packed ledger has no active segment"));
    };
    let pending = match row.get("pending") {
        Some(SqlValue::Text(value)) => serde_json::from_str(value)
            .map_err(|_| EngineError::internal("packed ledger pending keys are invalid"))?,
        _ => {
            return Err(EngineError::internal(
                "packed ledger pending keys are not text",
            ));
        }
    };
    Ok(ActiveSegment {
        start: parse_counter(row.get("startVersion"), "start version")?,
        end: parse_counter(row.get("endVersion"), "end version")?,
        payload: parse_payload(row.get("payload"))?,
        pending,
        capture_mode: parse_counter(row.get("captureMode"), "capture mode")?,
    })
}

fn encoded_payload(payload: &LedgerPayload) -> Result<String, EngineError> {
    serde_json::to_string(payload)
        .map_err(|_| EngineError::internal("packed ledger payload cannot be encoded"))
}

fn rotate_if_full(
    db: &mut dyn SyncDb,
    segment: ActiveSegment,
) -> Result<ActiveSegment, EngineError> {
    if encoded_payload(&segment.payload)?.len() < ROTATE_AT_BYTES {
        return Ok(segment);
    }
    if segment.capture_mode != 0 || !segment.pending.is_empty() {
        return Err(EngineError::internal(
            "packed ledger cannot rotate with uncommitted capture state",
        ));
    }
    if segment.end == i64::MAX {
        return Err(EngineError::internal(
            "packed ledger version space is exhausted",
        ));
    }
    let payload = encoded_payload(&LedgerPayload {
        format: LEDGER_FORMAT,
        transactions: Vec::new(),
    })?;
    db.exec(
        "INSERT INTO _zsync_log_segments
           (startVersion, endVersion, payload, pending, captureMode)
         VALUES (?, ?, ?, '[]', 0)",
        &[
            SqlValue::Integer(segment.end + 1),
            SqlValue::Integer(segment.end),
            text(payload),
        ],
    )?;
    active(db)
}

pub(crate) fn init(db: &mut dyn SyncDb) -> Result<(), DbError> {
    let legacy_table = !db
        .query(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = '_zsync_changes'",
            &[],
        )?
        .is_empty();
    let legacy_head = if legacy_table {
        let rows = db.query(
            "SELECT CAST(COALESCE(MAX(watermark), 0) AS TEXT) AS watermark
             FROM _zsync_changes",
            &[],
        )?;
        match rows.first().and_then(|row| row.get("watermark")) {
            Some(SqlValue::Text(value)) => value.parse::<i64>().map_err(|_| {
                DbError("legacy watermark is invalid while creating packed ledger".into())
            })?,
            Some(SqlValue::Integer(value)) => *value,
            _ => 0,
        }
    } else {
        0
    };
    db.exec(
        "CREATE TABLE IF NOT EXISTS _zsync_log_segments (
            startVersion INTEGER PRIMARY KEY,
            endVersion INTEGER NOT NULL,
            payload TEXT NOT NULL,
            pending TEXT NOT NULL,
            captureMode INTEGER NOT NULL CHECK (captureMode IN (0, 1))
         ) WITHOUT ROWID",
        &[],
    )?;
    let segments = db.query(
        "SELECT CAST(startVersion AS TEXT) AS startVersion, payload
         FROM _zsync_log_segments ORDER BY startVersion",
        &[],
    )?;
    if segments.is_empty() {
        let durable_head = db.query(
            "SELECT CAST(high AS TEXT) AS watermark FROM _zsync_watermark WHERE lock = 1",
            &[],
        )?;
        let durable_head = match durable_head.first().and_then(|row| row.get("watermark")) {
            Some(SqlValue::Text(value)) => value.parse::<i64>().map_err(|_| {
                DbError("durable watermark is invalid while creating packed ledger".into())
            })?,
            Some(SqlValue::Integer(value)) => *value,
            _ => 0,
        };
        let head = legacy_head.max(durable_head);
        let payload = serde_json::to_string(&LedgerPayload {
            format: LEDGER_FORMAT,
            transactions: Vec::new(),
        })
        .map_err(|error| DbError(error.to_string()))?;
        db.exec(
            "INSERT INTO _zsync_log_segments
           (startVersion, endVersion, payload, pending, captureMode)
         VALUES (?, ?, ?, '[]', 0)",
            &[
                SqlValue::Integer(head + 1),
                SqlValue::Integer(head),
                text(payload),
            ],
        )?;
    } else {
        let active_payload = match segments.last().and_then(|row| row.get("payload")) {
            Some(SqlValue::Text(value)) => value,
            _ => return Err(DbError("packed ledger payload is not text".into())),
        };
        let format = serde_json::from_str::<Value>(active_payload)
            .ok()
            .and_then(|payload| payload.get("format").and_then(Value::as_u64))
            .ok_or_else(|| DbError("packed ledger payload has an unknown shape".into()))?;
        match format {
            format if format == u64::from(LEDGER_FORMAT) => {}
            1 => {
                let active: LegacyLedgerPayload = serde_json::from_str(active_payload)
                    .map_err(|_| DbError("packed ledger payload has an unknown shape".into()))?;
                if active.format != 1 {
                    return Err(DbError("packed ledger payload has an unknown shape".into()));
                }
                for (group, clients) in active.lmids {
                    for (client, mutation) in clients {
                        let mutation = mutation
                            .parse::<i64>()
                            .ok()
                            .filter(|value| *value >= 0)
                            .ok_or_else(|| DbError("legacy client lmid is invalid".into()))?;
                        db.exec(
                            "INSERT INTO _zsync_clients
                               (clientGroupID, clientID, lastMutationID, userID)
                             VALUES (?, ?, ?, NULL)
                             ON CONFLICT (clientGroupID, clientID)
                             DO UPDATE SET lastMutationID = excluded.lastMutationID",
                            &[
                                text(group.clone()),
                                text(client),
                                SqlValue::Integer(mutation),
                            ],
                        )?;
                    }
                }
                for row in &segments {
                    let start = parse_counter(row.get("startVersion"), "start version")
                        .map_err(|error| DbError(error.message))?;
                    let payload = match row.get("payload") {
                        Some(SqlValue::Text(value)) => value,
                        _ => return Err(DbError("packed ledger payload is not text".into())),
                    };
                    let legacy: LegacyLedgerPayload =
                        serde_json::from_str(payload).map_err(|_| {
                            DbError("packed ledger payload has an unknown shape".into())
                        })?;
                    if legacy.format != 1 {
                        return Err(DbError("packed ledger payload has an unknown shape".into()));
                    }
                    let payload = serde_json::to_string(&LedgerPayload {
                        format: LEDGER_FORMAT,
                        transactions: legacy
                            .transactions
                            .into_iter()
                            .map(|transaction| LedgerTransaction {
                                version: transaction.version,
                                changes: transaction.changes,
                                reset: transaction.reset,
                            })
                            .collect(),
                    })
                    .map_err(|error| DbError(error.to_string()))?;
                    db.exec(
                        "UPDATE _zsync_log_segments SET payload = ? WHERE startVersion = ?",
                        &[text(payload), SqlValue::Integer(start)],
                    )?;
                }
            }
            _ => return Err(DbError("packed ledger format is unsupported".into())),
        }
    }
    if legacy_table {
        db.exec(
            "UPDATE _zsync_watermark SET high = MAX(high, ?) WHERE lock = 1",
            &[SqlValue::Integer(legacy_head)],
        )?;
        db.exec(
            "UPDATE _zsync_meta SET floor = MAX(floor, ?) WHERE lock = 1",
            &[SqlValue::Integer(legacy_head)],
        )?;
        db.exec("DROP TABLE _zsync_changes", &[])?;
    }
    Ok(())
}

pub(crate) fn watermark(db: &mut dyn SyncDb) -> Result<i64, EngineError> {
    let segment = db.query(
        "SELECT CAST(COALESCE(MAX(endVersion), 0) AS TEXT) AS watermark
         FROM _zsync_log_segments",
        &[],
    )?;
    parse_counter(
        segment.first().and_then(|row| row.get("watermark")),
        "watermark",
    )
}

pub(crate) fn finalize(
    db: &mut dyn SyncDb,
    identity: Option<(&str, &str, i64)>,
    reset: bool,
) -> Result<i64, EngineError> {
    let mut segment = active(db)?;
    if segment.end == i64::MAX {
        return Err(EngineError::internal(
            "packed ledger version space is exhausted",
        ));
    }
    let mut changes = BTreeMap::new();
    for (table, key) in segment.pending.drain(..) {
        let encoded = serde_json::to_string(&(table.clone(), &key))
            .map_err(|_| EngineError::internal("packed ledger key is invalid"))?;
        changes.insert(encoded, (table, key));
    }
    if identity.is_none() && changes.is_empty() && !reset {
        if segment.capture_mode != 0 {
            db.exec(
                "UPDATE _zsync_log_segments SET captureMode = 0
                 WHERE startVersion = ?",
                &[SqlValue::Integer(segment.start)],
            )?;
        }
        return Ok(segment.end);
    }
    segment = rotate_if_full(db, segment)?;
    let version = segment.end + 1;
    if let Some((group, client, mutation)) = identity {
        db.exec(
            "UPDATE _zsync_clients SET lastMutationID = ?
             WHERE clientGroupID = ? AND clientID = ?",
            &[SqlValue::Integer(mutation), text(group), text(client)],
        )?;
    }
    segment.payload.transactions.push(LedgerTransaction {
        version: version.to_string(),
        changes: changes.into_values().collect(),
        reset,
    });
    let payload = encoded_payload(&segment.payload)?;
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(EngineError::internal(
            "packed ledger transaction exceeds the 1 MiB limit",
        ));
    }
    db.exec(
        "UPDATE _zsync_log_segments
         SET endVersion = ?, payload = ?, pending = '[]', captureMode = 0
         WHERE startVersion = ?",
        &[
            SqlValue::Integer(version),
            text(payload),
            SqlValue::Integer(segment.start),
        ],
    )?;
    Ok(version)
}

pub(crate) struct ScannedLedger {
    pub changes: BTreeSet<(String, String)>,
}

pub(crate) fn scan_since(db: &mut dyn SyncDb, cookie: i64) -> Result<ScannedLedger, EngineError> {
    let rows = db.query(
        "SELECT CAST(startVersion AS TEXT) AS startVersion,
                CAST(endVersion AS TEXT) AS endVersion, payload
         FROM _zsync_log_segments
         WHERE endVersion > ? ORDER BY startVersion",
        &[SqlValue::Integer(cookie)],
    )?;
    let mut changes = BTreeSet::new();
    let mut previous_end: Option<i64> = None;
    for row in rows {
        let start = parse_counter(row.get("startVersion"), "start version")?;
        let end = parse_counter(row.get("endVersion"), "end version")?;
        if start > end.saturating_add(1) {
            return Err(EngineError::internal(
                "packed ledger segment bounds are invalid",
            ));
        }
        if let Some(previous_end) = previous_end {
            let expected = previous_end
                .checked_add(1)
                .ok_or_else(|| EngineError::internal("packed ledger version space is exhausted"))?;
            if start != expected {
                return Err(EngineError::internal(
                    "packed ledger segment chain has a gap",
                ));
            }
        } else if start > cookie.saturating_add(1) {
            return Err(EngineError::internal(
                "packed ledger segment chain has a leading gap",
            ));
        }
        let payload = parse_payload(row.get("payload"))?;
        let had_transactions = !payload.transactions.is_empty();
        let mut previous_version = start.saturating_sub(1);
        for transaction in payload.transactions {
            let version = transaction
                .version
                .parse::<i64>()
                .map_err(|_| EngineError::internal("packed ledger version is invalid"))?;
            if version < start || version > end || version <= previous_version {
                return Err(EngineError::internal(
                    "packed ledger transaction order is invalid",
                ));
            }
            previous_version = version;
            if version <= cookie {
                continue;
            }
            for (table, key) in transaction.changes {
                let key = serde_json::to_string(&key)
                    .map_err(|_| EngineError::internal("packed ledger key is invalid"))?;
                changes.insert((table, key));
            }
        }
        if had_transactions && previous_version != end {
            return Err(EngineError::internal(
                "packed ledger segment end does not match its last transaction",
            ));
        }
        previous_end = Some(end);
    }
    Ok(ScannedLedger { changes })
}

pub(crate) fn prune(db: &mut dyn SyncDb, cutoff: i64) -> Result<(), EngineError> {
    db.exec(
        "DELETE FROM _zsync_log_segments
         WHERE endVersion <= ?
           AND startVersion <> (SELECT MAX(startVersion) FROM _zsync_log_segments)",
        &[SqlValue::Integer(cutoff)],
    )?;
    Ok(())
}
