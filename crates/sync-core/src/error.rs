// deterministic error classification. `EngineError` is the Rust equivalent of
// the reference core's `SyncHttpError` (src/sync-server/sync-server.ts): one
// status + message per class, so every host renders the same wire error.
//
//   400 malformed / unsupported / out-of-order push
//   401 missing authentication (host resolves auth; engine surfaces the class)
//   403 authenticated user forbidden from claiming a client group
//   409 cookie above the retained floor / incompatible epoch (reset)
//   500 internal invariant failure or unknown change-log table (fail loud)

use crate::db::DbError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineError {
    pub status: u16,
    pub message: String,
}

impl EngineError {
    pub fn new(status: u16, message: impl Into<String>) -> Self {
        Self { status, message: message.into() }
    }
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(400, message)
    }
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(401, message)
    }
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(403, message)
    }
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(409, message)
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(500, message)
    }
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.status, self.message)
    }
}

impl std::error::Error for EngineError {}

// a DbError is always an internal (500) invariant failure from the engine's
// point of view: the host's SQL adapter failed underneath a statement the
// engine knows is well-formed.
impl From<DbError> for EngineError {
    fn from(err: DbError) -> Self {
        EngineError::internal(err.0)
    }
}

// what a consumer mutator returns. `App` is an application-level rejection:
// the mutation's row effects roll back, but the LMID still advances in a
// second transaction and the client rolls back its optimistic layer (the
// reference core's `MutationAppError`). `Other` is an infrastructure failure:
// the whole push fails, the LMID does NOT advance, and the client retries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutateError {
    App { details: String, message: String },
    Other(String),
}

impl MutateError {
    pub fn app(details: impl Into<String>) -> Self {
        let details = details.into();
        MutateError::App { message: details.clone(), details }
    }
}
