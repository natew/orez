// push: v51 custom-mutator processing with LMID bookkeeping, ported from the
// reference core's handlePush. exported as STEP functions the host orchestrates
// around its (possibly async) mutator, because a Durable Object runs push
// inside `ctx.storage.transaction(async () => ...)` with the consumer's async
// TypeScript mutator mid-transaction: the synchronous engine cannot drive or
// span that, so the host owns transaction entry/exit and calls these steps.
//
// per-mutation lifecycle the host drives:
//   tx1: preflight(db) -> Applied | Replay | (Err 400 out-of-order)
//        if Applied: run mutator; on success finalize(db); commit
//        if the mutator raises an app error: roll back tx1
//   tx2 (app error only): record_app_error(db) advances the LMID + marker
// crash between tx1 and tx2 is safe: nothing committed, replay re-executes and
// hits the same app error (invariant 8). replay is idempotent (invariant 12).

use serde_json::Value;

use crate::db::SyncDb;
use crate::error::{EngineError, MutateError};
use crate::schema::Tables;

// a validated push mutation (type:'custom' only)
#[derive(Debug, Clone)]
pub struct PushMutation {
    pub id: i64,
    pub client_id: String,
    pub name: String,
    pub args: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct PushBody {
    pub client_group_id: String,
    pub mutations: Vec<PushMutation>,
}

// push_validate outcome: either a ready-to-return response (the stock
// unsupportedPushVersion error, which prevents any mutation processing), or a
// validated body to process.
pub enum PushPlan {
    Respond(Value),
    Process(PushBody),
}

// preflight decision for one mutation, taken inside tx1 after the ownership,
// replay, and ordering checks. `Replay` carries the expected next id for the
// idempotent already-processed ack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Preflight {
    Applied,
    Replay { expected: i64 },
}

// one processed mutation's result, assembled into the pushResponse
#[derive(Debug, Clone)]
pub struct MutationResult {
    pub client_id: String,
    pub id: i64,
    pub result: Value,
}

// validate the whole push body before processing its first mutation (so a
// malformed later mutation can't half-apply an earlier one). rejects malformed
// bodies/mutations with 400; returns the stock unsupportedPushVersion response
// for pushVersion != 1.
pub fn push_validate(_body: &Value) -> Result<PushPlan, EngineError> {
    unimplemented!("push_validate")
}

// tx1 step: claim the client group (403 if owned by another user), read the
// LMID, and decide. out-of-order (id > lmid+1) is a 400. no row effects yet —
// the caller runs the mutator, then calls finalize.
pub fn preflight(
    _db: &mut dyn SyncDb,
    _client_group_id: &str,
    _client_id: &str,
    _mutation_id: i64,
    _user_id: &str,
) -> Result<Preflight, EngineError> {
    unimplemented!("preflight")
}

// tx1 step, after the mutator succeeds: advance the LMID and append an 'lmid'
// change row (so peers' pulls become non-unchanged and mutation recovery
// settles — invariant 4 — and a capped diff can derive this ack from the
// included prefix — invariant 3).
pub fn finalize(
    _db: &mut dyn SyncDb,
    _client_group_id: &str,
    _client_id: &str,
    _mutation_id: i64,
) -> Result<(), EngineError> {
    unimplemented!("finalize")
}

// tx2 step: the second transaction after an application error rolls back tx1.
// advances the LMID + marker with NO row effects (invariant 8).
pub fn record_app_error(
    _db: &mut dyn SyncDb,
    _client_group_id: &str,
    _client_id: &str,
    _mutation_id: i64,
    _user_id: &str,
) -> Result<(), EngineError> {
    unimplemented!("record_app_error")
}

// build the final {pushResponse: {mutations: [...]}} from processed results
pub fn assemble_push_response(_results: Vec<MutationResult>) -> Value {
    unimplemented!("assemble_push_response")
}

// ---- native / test convenience -------------------------------------------
// the CF host orchestrates the steps above around its async mutator directly.
// synchronous hosts (native fixture mutators, unit tests) use handle_push,
// which is a TRIVIAL composition of the exact same steps with no separate
// semantics: a Transactor supplies host-owned transaction boundaries and a
// Mutator runs the consumer's SQL. SyncDb itself has no transaction method by
// design (see the crate docs).

pub trait Mutator {
    fn mutate(
        &self,
        db: &mut dyn SyncDb,
        name: &str,
        args: &Value,
        user_id: &str,
    ) -> Result<(), MutateError>;
}

// host-owned transaction boundary for synchronous hosts. commits when `body`
// returns Ok, rolls back on Err. generic so the app-error signal can ride the
// Err channel. NOT object-safe and NOT used by the wasm/CF host.
pub trait Transactor {
    fn transaction<T, E>(
        &mut self,
        body: impl FnOnce(&mut dyn SyncDb) -> Result<T, E>,
    ) -> Result<T, E>;
}

pub fn handle_push(
    _txor: &mut impl Transactor,
    _tables: &Tables,
    _retain_changes: i64,
    _mutator: &dyn Mutator,
    _body: &Value,
    _user_id: &str,
) -> Result<Value, EngineError> {
    unimplemented!("handle_push: trivial composition of the push steps")
}
