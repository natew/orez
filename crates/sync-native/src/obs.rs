// structured observability for the native host, mirroring the Cloudflare DO host
// (packages/sync-cf-host/src/host.ts) so an operator can diff the two hosts'
// telemetry field-for-field. Two surfaces:
//   1. a `sync_request` JSON line on stderr for non-routine request results.
//   2. process-wide aggregate Counters, surfaced on the local admin/status route.
//
// CF-only fields are intentionally dropped: there is no wasm boundary here, so
// wasmBoundaryCalls and the per-call sql-stats block do not apply.

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Value, json};

// hostVersion/engineVersion mirror the CF host's version fields. ENGINE_VERSION
// tracks sync-core's crate version by hand (sync-core exposes no version const);
// keep it in step if sync-core's Cargo version bumps.
pub const HOST_VERSION: &str = concat!("sync-native-", env!("CARGO_PKG_VERSION"));
pub const ENGINE_VERSION: &str = "0.1.0";

// process-wide aggregate counters, the native analogues of the CF host's
// Counters. Diagnostics-only; read via the local admin/status route.
#[derive(Default)]
pub struct Counters {
    pub pulls: AtomicU64,
    pub pushes: AtomicU64,
    pub resets: AtomicU64,
    pub application_errors: AtomicU64,
    pub invariant_failures: AtomicU64,
    pub retention_runs: AtomicU64,
    pub query_recompilations: AtomicU64,
}

impl Counters {
    pub fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Value {
        json!({
            "pulls": self.pulls.load(Ordering::Relaxed),
            "pushes": self.pushes.load(Ordering::Relaxed),
            "resets": self.resets.load(Ordering::Relaxed),
            "applicationErrors": self.application_errors.load(Ordering::Relaxed),
            "invariantFailures": self.invariant_failures.load(Ordering::Relaxed),
            "retentionRuns": self.retention_runs.load(Ordering::Relaxed),
            "queryRecompilations": self.query_recompilations.load(Ordering::Relaxed),
        })
    }
}

// a stable 64-bit FNV-1a of the namespace as 16 hex chars, mirroring the CF host's
// namespaceHash field. native namespaces are raw strings (not DO id hashes), so
// we hash here to keep the field shape parallel and avoid logging raw names.
pub fn namespace_hash(ns: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in ns.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x00000100000001b3);
    }
    format!("{hash:016x}")
}

// count of desired-query `put` ops in a pull body (the query-aware requests that
// drive a recompile), mirroring CF's `queryRecompilations += queryPuts`.
pub fn count_query_puts(body: &Value) -> u64 {
    body.get("queries")
        .and_then(|q| q.get("patch"))
        .and_then(Value::as_array)
        .map(|patch| {
            patch
                .iter()
                .filter(|op| op.get("op").and_then(Value::as_str) == Some("put"))
                .count() as u64
        })
        .unwrap_or(0)
}

// (rowPuts, rowDeletes) over a pull response's rowsPatch (the `clear` op is not
// counted as either).
pub fn count_patch(response: &Value) -> (u64, u64) {
    let mut puts = 0;
    let mut deletes = 0;
    if let Some(patch) = response.get("rowsPatch").and_then(Value::as_array) {
        for entry in patch {
            match entry.get("op").and_then(Value::as_str) {
                Some("put") => puts += 1,
                Some("del") => deletes += 1,
                _ => {}
            }
        }
    }
    (puts, deletes)
}

// count of mutations acknowledged in a push response (each advances its client's
// LMID) and, of those, how many carried an app-level rejection.
pub fn count_push_mutations(response: &Value) -> (u64, u64) {
    let mut advances = 0;
    let mut app_errors = 0;
    if let Some(mutations) = response
        .get("pushResponse")
        .and_then(|p| p.get("mutations"))
        .and_then(Value::as_array)
    {
        for mutation in mutations {
            advances += 1;
            if mutation
                .get("result")
                .and_then(|r| r.get("error"))
                .and_then(Value::as_str)
                == Some("app")
            {
                app_errors += 1;
            }
        }
    }
    (advances, app_errors)
}

// one per-request structured event. routine traffic stays silent; resets and
// failures are emitted as a single JSON line on stderr.
pub struct RequestEvent<'a> {
    pub namespace_hash: &'a str,
    pub request_kind: &'a str,
    pub result_class: &'a str,
    pub input_cookie: Value,
    pub output_cookie: Value,
    pub retained_floor: i64,
    pub current_watermark: i64,
    pub change_rows_included: u64,
    pub queries_recomputed: u64,
    pub row_puts: u64,
    pub row_deletes: u64,
    pub lmid_advances: u64,
    pub transaction_ms: u64,
    pub total_ms: u64,
    pub reset_reason: Value,
}

impl RequestEvent<'_> {
    fn payload(&self) -> Option<Value> {
        if matches!(self.result_class, "success" | "unchanged") {
            return None;
        }

        // floor/watermark as strings, matching the CF host (its cookies/counters
        // can exceed the JS safe-int range; kept parallel here). changeRowsScanned
        // is null: the host does not see the engine's internal change scan.
        Some(json!({
            "event": "sync_request",
            "hostVersion": HOST_VERSION,
            "engineVersion": ENGINE_VERSION,
            "namespaceHash": self.namespace_hash,
            "requestKind": self.request_kind,
            "resultClass": self.result_class,
            "inputCookie": self.input_cookie,
            "outputCookie": self.output_cookie,
            "retainedFloor": self.retained_floor.to_string(),
            "currentWatermark": self.current_watermark.to_string(),
            "changeRowsScanned": Value::Null,
            "changeRowsIncluded": self.change_rows_included,
            "queriesRecomputed": self.queries_recomputed,
            "rowPuts": self.row_puts,
            "rowDeletes": self.row_deletes,
            "lmidAdvances": self.lmid_advances,
            "transactionMs": self.transaction_ms,
            "totalMs": self.total_ms,
            "resetReason": self.reset_reason,
        }))
    }

    pub fn emit(&self) {
        if let Some(event) = self.payload() {
            eprintln!("{event}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(result_class: &'static str) -> RequestEvent<'static> {
        RequestEvent {
            namespace_hash: "602ee718f0b5c8d4",
            request_kind: "pull",
            result_class,
            input_cookie: json!(16087),
            output_cookie: json!(16087),
            retained_floor: 11991,
            current_watermark: 16087,
            change_rows_included: 0,
            queries_recomputed: 0,
            row_puts: 0,
            row_deletes: 0,
            lmid_advances: 0,
            transaction_ms: 42,
            total_ms: 42,
            reset_reason: Value::Null,
        }
    }

    #[test]
    fn routine_requests_are_silent() {
        assert!(event("success").payload().is_none());
        assert!(event("unchanged").payload().is_none());
    }

    #[test]
    fn non_routine_requests_remain_visible() {
        assert!(event("reset").payload().is_some());
        assert!(event("error").payload().is_some());
    }
}
