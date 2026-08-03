// sync-core: the deterministic sqlite zero sync engine behind Orez hosts and
// the original executor-backed TypeScript conformance host; see
// plans/rust-sync-server-final-plan.md.
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
mod ledger;
pub mod pull;
pub mod push;
pub mod query;
pub mod schema;
mod store;
pub mod upstream;
pub mod value;
pub mod wire;

pub use db::{DbError, Row, SqlValue, SyncDb};
pub use error::{EngineError, MutateError};
pub use pull::{invalidate, prune, watermark};
pub use push::{
    MutationResult, Mutator, Preflight, PushBody, PushMutation, PushPlan, Transactor,
    assemble_push_response, finalize, handle_push, preflight, push_validate, record_app_error,
    settle_delegated_push,
};
pub use schema::{TableSpec, Tables, init_schema, trigger_ddl};

/// identity of every durable DDL surface this engine installs. a host stores
/// it (with its own inputs) after a schema pass and skips the pass while the
/// stored value matches, so the constants composed here MUST change whenever
/// init_schema, trigger_ddl, init_query_schema, or the packed-ledger payload
/// change what they would install.
pub fn schema_revision() -> String {
    format!(
        "core{version}:q{query}:t{trigger}:l{ledger}",
        version = env!("CARGO_PKG_VERSION"),
        query = query::membership::QUERY_SCHEMA_VERSION,
        trigger = schema::TRIGGER_VERSION,
        ledger = ledger::LEDGER_FORMAT,
    )
}
pub use upstream::{
    ApplyUpstreamResult, SnapshotProgress, SnapshotState, UpstreamBatch, UpstreamChange,
    UpstreamSnapshot, apply_snapshot_changes, apply_snapshot_page, apply_upstream,
    apply_upstream_snapshot, begin_snapshot_generation, finalize_snapshot_generation,
    read_snapshot_progress, upstream_watermark,
};
pub use value::{
    ZeroColumnType, canonical_pk, canonical_pk_text, to_zero_value, to_zero_value_json,
};
pub use wire::WireValue;
