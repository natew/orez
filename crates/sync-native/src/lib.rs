// sync-native: axum + rusqlite host for the sync engine (plan M2).
//
// the engine lives in sync-core; this crate is the native host shell:
// per-namespace sqlite files (WAL, one serialized writer each), the rusqlite
// SyncDb adapter, the fixture mutators/visibility/seed, the harness admin
// routes, and the wake channel. it owns transaction begin/commit/rollback and
// drives the engine steps (see engine.rs).

pub mod db;
pub mod engine;
pub mod fault;
pub mod fixture;
pub mod namespace;
pub mod obs;
pub mod seed;
pub mod wake;
