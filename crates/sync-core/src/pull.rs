// pull: cursor-diff and snapshot responses over the trigger-fed change log.
// ported from the reference core's handlePull + soot's cursorPull composition
// (byte/row caps with a last-included-watermark cookie, log-derived prefix
// LMIDs). MUST be called inside one host-entered transaction: this module
// issues no BEGIN/COMMIT (durable object SQL rejects them) and relies on the
// host's transaction for a single consistent view.

use serde_json::Value;

use crate::db::SyncDb;
use crate::error::EngineError;
use crate::schema::Tables;

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
    pub params: Vec<crate::db::SqlValue>,
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
        Caps { max_change_rows: 10_000, max_change_bytes: 2_000_000 }
    }
}

// the single pull entry point. `body` is the raw {clientID, clientGroupID,
// cookie} object; the response is {cookie, unchanged} | {cookie,
// lastMutationIDChanges, rowsPatch}, byte-compatible with the vendored
// http-pull transport.
pub fn handle_pull(
    _db: &mut dyn SyncDb,
    _tables: &Tables,
    _retain_changes: i64,
    _visible: Option<&Visibility>,
    _caps: Caps,
    _body: &Value,
    _user_id: &str,
) -> Result<Value, EngineError> {
    unimplemented!("handle_pull: implemented in the M1 port")
}

// the change log's high watermark = max(durable high, MAX(log)). this is the
// cookie. monotonic through restart, eviction, and pruning (invariant 7).
pub fn watermark(_db: &mut dyn SyncDb) -> Result<i64, EngineError> {
    unimplemented!("watermark")
}

// the retained-log floor: cookies at or above it can be served as a diff,
// below it fall back to snapshot.
pub fn floor(_db: &mut dyn SyncDb) -> Result<i64, EngineError> {
    unimplemented!("floor")
}

// size-bounded retention: prune change rows below (watermark - retain),
// raising the floor. never prunes the top, so MAX(log) stays monotonic.
pub fn prune(_db: &mut dyn SyncDb, _retain_changes: i64) -> Result<(), EngineError> {
    unimplemented!("prune")
}

// epoch invalidation: force every client's next pull to a full snapshot (for
// changes no row diff can express — visibility revocation, table-set change).
// appends a marker (advances the watermark past every cookie) and raises the
// floor past every prior watermark. host wraps this in a transaction.
pub fn invalidate(_db: &mut dyn SyncDb) -> Result<(), EngineError> {
    unimplemented!("invalidate")
}
