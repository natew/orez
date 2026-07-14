// harness/operator fault injection (M6). One-shot faults armed per namespace at a
// precise point in the pull/push lifecycle, checked by the engine path. This is a
// diagnostics surface: it is armed via the token-gated, browser-denied admin
// route. It mirrors the intent of the CF host's transaction/storage fault hooks
// (sol mirrors the CF side).
//
// The points bracket the durability-critical transitions: a Kill at a point
// simulates SIGKILL there (does committed state survive? does uncommitted state
// stay gone?), while Error/Quota inject an infra failure the client must recover
// from.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// the precise lifecycle point a fault fires at.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    // before any mutation in a push is applied
    PushBeforeMutation,
    // a mutation's app rows are written, before its per-mutation COMMIT (Kill only:
    // the host cannot forge the engine's generic tx error to inject Error/Quota here)
    PushAfterWriteBeforeCommit,
    // the push committed durably, before the client sees the ack
    PushAfterCommitBeforeResponse,
    // a pull is assembled, before its COMMIT
    PullDuringTx,
    // a pull committed, before the client sees the response
    PullAfterCommit,
}

impl FaultPoint {
    pub fn parse(value: &str) -> Option<FaultPoint> {
        match value {
            "push_before_mutation" => Some(FaultPoint::PushBeforeMutation),
            "push_after_write_before_commit" => Some(FaultPoint::PushAfterWriteBeforeCommit),
            "push_after_commit_before_response" => Some(FaultPoint::PushAfterCommitBeforeResponse),
            "pull_during_tx" => Some(FaultPoint::PullDuringTx),
            "pull_after_commit" => Some(FaultPoint::PullAfterCommit),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            FaultPoint::PushBeforeMutation => "push_before_mutation",
            FaultPoint::PushAfterWriteBeforeCommit => "push_after_write_before_commit",
            FaultPoint::PushAfterCommitBeforeResponse => "push_after_commit_before_response",
            FaultPoint::PullDuringTx => "pull_during_tx",
            FaultPoint::PullAfterCommit => "pull_after_commit",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    // std::process::exit(137): simulate SIGKILL at this point (durability probe)
    Kill,
    // return a 500 at this point (transaction failure)
    Error,
    // return a 507 at this point (storage exhausted)
    Quota,
}

impl FaultKind {
    pub fn parse(value: &str) -> Option<FaultKind> {
        match value {
            "kill" => Some(FaultKind::Kill),
            "error" => Some(FaultKind::Error),
            "quota" => Some(FaultKind::Quota),
            _ => None,
        }
    }
}

// the process-wide register of armed faults, one per namespace. shared (Arc) so
// the axum handler arms and the namespace worker thread checks the same state.
#[derive(Default)]
pub struct FaultRegistry {
    armed: Mutex<HashMap<String, (FaultPoint, FaultKind)>>,
}

impl FaultRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn arm(&self, ns: &str, point: FaultPoint, kind: FaultKind) {
        self.armed
            .lock()
            .unwrap()
            .insert(ns.to_string(), (point, kind));
    }

    pub fn clear(&self, ns: &str) {
        self.armed.lock().unwrap().remove(ns);
    }

    // one-shot: return and disarm the fault iff it targets this exact point.
    pub fn take(&self, ns: &str, point: FaultPoint) -> Option<FaultKind> {
        let mut map = self.armed.lock().unwrap();
        match map.get(ns) {
            Some((armed_point, kind)) if *armed_point == point => {
                let kind = *kind;
                map.remove(ns);
                Some(kind)
            }
            _ => None,
        }
    }
}
