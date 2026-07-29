// change-log lifecycle entry points shared by every pull path. the pull
// itself lives in query::handle_query_pull: desired queries drive membership,
// and a client receives exactly the rows its registered queries select.

use crate::db::SyncDb;
use crate::error::EngineError;
use crate::store;

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
// changes no membership diff can express — permission-transform revocation,
// table-set change). appends a marker (advances the watermark past every
// cookie) and raises the floor past every prior watermark. host wraps this in
// a transaction.
pub fn invalidate(db: &mut dyn SyncDb) -> Result<(), EngineError> {
    store::invalidate(db)
}
