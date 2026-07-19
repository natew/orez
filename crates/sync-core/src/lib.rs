// sync-core: the deterministic sqlite zero sync engine (rust port of the
// executable spec at src/sync-server/sync-server.ts and soot's production
// composition src/zero/httpPullProject.server.ts — see
// plans/rust-sync-server-final-plan.md).
//
// hard boundary rules (pinned by the plan, do not relax):
// - the host owns transaction entry/exit; this crate NEVER emits
//   BEGIN/COMMIT/SAVEPOINT (durable object sql rejects them). every engine
//   entry point documents the transaction the host must have open around it.
// - positional `?` bindings only (DO SqlStorage has no `?N`)
// - no network or filesystem i/o in this crate; stays wasm-compilable
//   (rusqlite is a DEV-dependency only, for the test host)
// - patch values come from live rows read inside the pull transaction, never
//   from logged images (SQLite json_object rounds REAL to 15 sig figs)
//
// counter representation (coordinated with M0/sol-m0): watermarks, cookies,
// and last-mutation-ids are i64 end to end. reads use CAST(x AS TEXT) and parse
// to i64 so a value never passes through a float. inbound cookies accept a
// non-negative safe-integer JSON number (the vendored transport) or a canonical
// base-10 string in 0..=i64::MAX (sol-m0's boundary format); outbound counters
// are emitted through `wire::counter_to_json` (currently JSON numbers, the
// single flip point if the HTTP wire ever moves to strings).
//
// the push API is a set of STEP functions the host orchestrates around its
// (possibly async) mutator; see push.rs. a synchronous convenience
// `push::handle_push` composes those exact steps for the native host and tests.

pub mod db;
pub mod error;
pub mod pull;
pub mod push;
pub mod query;
pub mod schema;
mod store;
pub mod upstream;
pub mod value;
pub mod visibility;
pub mod wire;

pub use db::{DbError, Row, SqlValue, SyncDb};
pub use error::{EngineError, MutateError};
pub use pull::{Caps, Visibility, VisibleFilter, handle_pull, invalidate, prune, watermark};
pub use push::{
    MutationResult, Mutator, Preflight, PushBody, PushMutation, PushPlan, Transactor,
    assemble_push_response, finalize, handle_push, preflight, push_validate, record_app_error,
    settle_delegated_push,
};
pub use schema::{TableSpec, Tables, init_schema, trigger_ddl};
pub use upstream::{
    ApplyUpstreamResult, SnapshotProgress, SnapshotState, UpstreamBatch, UpstreamChange,
    UpstreamSnapshot, apply_snapshot_changes, apply_snapshot_page, apply_upstream,
    apply_upstream_snapshot, begin_snapshot_generation, finalize_snapshot_generation,
    read_snapshot_progress, upstream_watermark,
};
pub use value::{ZeroColumnType, to_zero_value, to_zero_value_json};
pub use visibility::{VisibilityExpression, compile_visibility_filter};
pub use wire::WireValue;
