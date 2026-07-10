// sync-core: the deterministic sqlite zero sync engine (rust port of the
// executable spec at src/sync-server/sync-server.ts — see
// plans/rust-sync-server-final-plan.md).
//
// hard boundary rules (pinned by the plan, do not relax):
// - the host owns transaction entry/exit; this crate NEVER emits
//   BEGIN/COMMIT/SAVEPOINT (durable object sql rejects them)
// - positional `?` bindings only (DO SqlStorage has no `?N`)
// - no network or filesystem i/o in this crate
// - patch values come from live rows read inside the pull transaction,
//   never from logged images

pub mod db;

pub use db::{DbError, Row, SqlValue, SyncDb};
