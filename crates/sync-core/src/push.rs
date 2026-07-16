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

use serde_json::{Value, json};

use crate::db::SyncDb;
use crate::error::{EngineError, MutateError};
use crate::schema::Tables;
use crate::store;
use crate::wire;

// a validated push mutation (type:'custom' only)
#[derive(Debug, Clone)]
pub struct PushMutation {
    pub id: i64,
    pub client_id: String,
    pub name: String,
    pub args: Vec<Value>,
}

impl PushMutation {
    // the single arg object the mutator receives (reference core: args[0])
    pub fn arg(&self) -> Value {
        self.args.first().cloned().unwrap_or(Value::Null)
    }
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

fn is_record(v: &Value) -> bool {
    v.is_object()
}

// validate the whole push body before processing its first mutation (so a
// malformed later mutation can't half-apply an earlier one). rejects malformed
// bodies/mutations with 400; returns the stock unsupportedPushVersion response
// for pushVersion != 1.
pub fn push_validate(body: &Value) -> Result<PushPlan, EngineError> {
    if !is_record(body)
        || !body
            .get("clientGroupID")
            .map(Value::is_string)
            .unwrap_or(false)
        || !body.get("mutations").map(Value::is_array).unwrap_or(false)
        || !body
            .get("pushVersion")
            .map(|v| v.is_number() && v.as_f64().map(f64::is_finite).unwrap_or(false))
            .unwrap_or(false)
    {
        return Err(EngineError::bad_request("invalid push body"));
    }

    let client_group_id = body["clientGroupID"].as_str().unwrap().to_string();
    let raw_mutations = body["mutations"].as_array().unwrap();

    let mut mutations = Vec::with_capacity(raw_mutations.len());
    for (index, m) in raw_mutations.iter().enumerate() {
        let id = m.get("id").and_then(wire::non_negative_safe_int);
        let valid = is_record(m)
            && m.get("type").and_then(Value::as_str) == Some("custom")
            && matches!(id, Some(id) if id != 0)
            && m.get("clientID").map(Value::is_string).unwrap_or(false)
            && m.get("name").map(Value::is_string).unwrap_or(false)
            && m.get("args").map(Value::is_array).unwrap_or(false);
        if !valid {
            return Err(EngineError::bad_request(format!(
                "invalid mutation at index {index}"
            )));
        }
        mutations.push(PushMutation {
            id: id.unwrap(),
            client_id: m["clientID"].as_str().unwrap().to_string(),
            name: m["name"].as_str().unwrap().to_string(),
            args: m["args"].as_array().unwrap().clone(),
        });
    }

    // the pinned client still accepts this legacy pushResponse error form: the
    // direct-transport equivalent of zero-cache's unsupportedPushVersion, which
    // prevents any mutation processing.
    if body["pushVersion"].as_f64() != Some(1.0) {
        let ids: Vec<Value> = mutations
            .iter()
            .map(|m| json!({ "clientID": m.client_id, "id": m.id }))
            .collect();
        return Ok(PushPlan::Respond(json!({
            "pushResponse": { "error": "unsupportedPushVersion", "mutationIDs": ids }
        })));
    }

    Ok(PushPlan::Process(PushBody {
        client_group_id,
        mutations,
    }))
}

// tx1 step: claim the client group (403 if owned by another user), read the
// LMID, and decide. out-of-order (id > lmid+1) is a 400. no row effects yet —
// the caller runs the mutator, then calls finalize.
pub fn preflight(
    db: &mut dyn SyncDb,
    client_group_id: &str,
    client_id: &str,
    mutation_id: i64,
    user_id: &str,
) -> Result<Preflight, EngineError> {
    store::claim_client(db, client_group_id, client_id, user_id)?;
    let lmid = store::read_lmid(db, client_group_id, client_id)?;
    if mutation_id <= lmid {
        return Ok(Preflight::Replay { expected: lmid + 1 });
    }
    if mutation_id > lmid + 1 {
        return Err(EngineError::bad_request(format!(
            "mutation id {mutation_id} skips lmid {lmid} (out of order)"
        )));
    }
    Ok(Preflight::Applied)
}

// tx1 step, after the mutator succeeds: advance the LMID and append an 'lmid'
// change row (invariants 3, 4).
pub fn finalize(
    db: &mut dyn SyncDb,
    client_group_id: &str,
    client_id: &str,
    mutation_id: i64,
) -> Result<(), EngineError> {
    store::advance_lmid(db, client_group_id, client_id, mutation_id)
}

// tx2 step: the second transaction after an application error rolls back tx1.
// re-runs the ownership + ordering checks (unchanged after the rollback) and
// advances the LMID + marker with NO row effects (invariant 8).
pub fn record_app_error(
    db: &mut dyn SyncDb,
    client_group_id: &str,
    client_id: &str,
    mutation_id: i64,
    user_id: &str,
) -> Result<(), EngineError> {
    match preflight(db, client_group_id, client_id, mutation_id, user_id)? {
        Preflight::Applied => finalize(db, client_group_id, client_id, mutation_id),
        // unreachable in practice (tx1 rolled back, so the lmid is unchanged and
        // this is still an Applied): a replay means the ack already landed.
        Preflight::Replay { .. } => Ok(()),
    }
}

// build the final {pushResponse: {mutations: [...]}} from processed results
pub fn assemble_push_response(results: Vec<MutationResult>) -> Value {
    let mutations: Vec<Value> = results
        .into_iter()
        .map(|r| json!({ "id": { "clientID": r.client_id, "id": r.id }, "result": r.result }))
        .collect();
    json!({ "pushResponse": { "mutations": mutations } })
}

// settle an application-owned push after its database transaction committed.
// the response must acknowledge exactly the mutations in the original push,
// in order, before any lmid is touched. callers must run this in a transaction
// that starts after the application effects committed, so their trigger rows
// precede the lmid rows in the shared change log.
pub fn settle_delegated_push(
    db: &mut dyn SyncDb,
    push: &Value,
    response: &Value,
    user_id: &str,
) -> Result<usize, EngineError> {
    let plan = match push_validate(push)? {
        PushPlan::Process(plan) => plan,
        PushPlan::Respond(_) => {
            return Err(EngineError::bad_request(
                "unsupported push version cannot be settled",
            ));
        }
    };
    let response = response.get("pushResponse").unwrap_or(response);
    let acknowledged = response
        .get("mutations")
        .and_then(Value::as_array)
        .ok_or_else(|| EngineError::bad_request("push response has no mutation results"))?;
    if acknowledged.len() != plan.mutations.len() {
        return Err(EngineError::bad_request(format!(
            "push response acknowledged {} mutations, expected {}",
            acknowledged.len(),
            plan.mutations.len()
        )));
    }
    for (index, (mutation, acknowledgement)) in plan.mutations.iter().zip(acknowledged).enumerate()
    {
        let acknowledged_client_id = acknowledgement
            .get("id")
            .and_then(|id| id.get("clientID"))
            .and_then(Value::as_str);
        let acknowledged_mutation_id = acknowledgement
            .get("id")
            .and_then(|id| id.get("id"))
            .and_then(|id| wire::parse_cookie(Some(id)).ok().flatten());
        if acknowledged_client_id != Some(mutation.client_id.as_str())
            || acknowledged_mutation_id != Some(mutation.id)
            || acknowledgement.get("result").is_none()
        {
            return Err(EngineError::bad_request(format!(
                "push response mutation at index {index} does not match the original push"
            )));
        }
    }

    let mut settled = 0;
    for mutation in &plan.mutations {
        store::claim_client(db, &plan.client_group_id, &mutation.client_id, user_id)?;
        let lmid = store::read_lmid(db, &plan.client_group_id, &mutation.client_id)?;
        // the application store is authoritative for delegated pushes. it may
        // already be ahead when a host first adopts an existing sqlite file, so
        // catch up monotonically to the acknowledged id without manufacturing
        // intermediate settlements. an equal or older replay is a no-op.
        if mutation.id > lmid {
            finalize(db, &plan.client_group_id, &mutation.client_id, mutation.id)?;
            settled += 1;
        }
    }
    Ok(settled)
}

// the idempotent already-processed result for a replayed mutation
fn replay_result(client_id: &str, id: i64, expected: i64) -> Value {
    json!({
        "error": "alreadyProcessed",
        "details": format!(
            "Ignoring mutation from {client_id} with ID {id} as it was already processed. Expected: {expected}"
        ),
    })
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

// the error channel the tx1 closure uses so handle_push can tell an application
// rejection (roll back tx1, then run tx2) apart from a real failure (propagate).
enum Tx1Error {
    App { details: String, message: String },
    Engine(EngineError),
    Other(String),
}

impl From<EngineError> for Tx1Error {
    fn from(e: EngineError) -> Self {
        Tx1Error::Engine(e)
    }
}

enum Tx1Ok {
    Applied,
    Replay { expected: i64 },
}

pub fn handle_push(
    txor: &mut impl Transactor,
    _tables: &Tables,
    retain_changes: i64,
    mutator: &dyn Mutator,
    body: &Value,
    user_id: &str,
) -> Result<Value, EngineError> {
    let plan = match push_validate(body)? {
        PushPlan::Respond(response) => return Ok(response),
        PushPlan::Process(body) => body,
    };
    let group = plan.client_group_id.as_str();

    let mut results = Vec::with_capacity(plan.mutations.len());
    for m in &plan.mutations {
        let arg = m.arg();
        // tx1: preflight, then (on Applied) the mutator + finalize, all atomic
        let tx1 = txor.transaction(|db| -> Result<Tx1Ok, Tx1Error> {
            match preflight(db, group, &m.client_id, m.id, user_id)? {
                Preflight::Replay { expected } => Ok(Tx1Ok::Replay { expected }),
                Preflight::Applied => match mutator.mutate(db, &m.name, &arg, user_id) {
                    Ok(()) => {
                        finalize(db, group, &m.client_id, m.id)?;
                        Ok(Tx1Ok::Applied)
                    }
                    Err(MutateError::App { details, message }) => {
                        Err(Tx1Error::App { details, message })
                    }
                    Err(MutateError::Other(e)) => Err(Tx1Error::Other(e)),
                },
            }
        });

        match tx1 {
            Ok(Tx1Ok::Applied) => {
                results.push(MutationResult {
                    client_id: m.client_id.clone(),
                    id: m.id,
                    result: json!({}),
                });
            }
            Ok(Tx1Ok::Replay { expected }) => {
                results.push(MutationResult {
                    client_id: m.client_id.clone(),
                    id: m.id,
                    result: replay_result(&m.client_id, m.id, expected),
                });
            }
            Err(Tx1Error::App { details, message }) => {
                // tx2: advance the LMID for the rejected mutation, no row effects
                txor.transaction(|db| record_app_error(db, group, &m.client_id, m.id, user_id))?;
                results.push(MutationResult {
                    client_id: m.client_id.clone(),
                    id: m.id,
                    result: json!({ "error": "app", "message": message, "details": details }),
                });
            }
            Err(Tx1Error::Engine(e)) => return Err(e),
            Err(Tx1Error::Other(e)) => return Err(EngineError::internal(e)),
        }
    }

    // size-bounded retention: pruned changes raise the floor; clients whose
    // cookie fell below it get one snapshot on their next pull
    if !plan.mutations.is_empty() {
        txor.transaction(|db| store::prune(db, retain_changes))?;
    }

    Ok(assemble_push_response(results))
}
