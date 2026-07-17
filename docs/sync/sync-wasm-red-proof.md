# sync-wasm and single-writer red proof

Recorded on 2026-07-16 with Node 25.9.0, wasm-pack 0.14.0, and the repository Rust toolchain. Both mutations below were temporary, produced the expected failure, and were reverted before the final green runs.

## Wasm error-status mutation

The production wasm error mapper was temporarily changed to replace every engine status with 500:

```diff
diff --git a/crates/sync-wasm/src/lib.rs b/crates/sync-wasm/src/lib.rs
--- a/crates/sync-wasm/src/lib.rs
+++ b/crates/sync-wasm/src/lib.rs
@@ -352,7 +352,7 @@ fn engine_error(error: sync_core::EngineError) -> JsValue {
     let _ = js_sys::Reflect::set(
         js_error.as_ref(),
         &JsValue::from_str("status"),
-        &JsValue::from_f64(f64::from(error.status)),
+        &JsValue::from_f64(500.0),
     );
```

Command:

```text
wasm-pack test --node crates/sync-wasm
```

RED output:

```text
running 3 tests
test push_and_pull_round_trip_through_wasm_exports ... ok
test preflight_then_application_write_then_finalize_preserves_journal_order ... ok
test engine_errors_keep_400_and_403_statuses_without_panicking ... FAIL

---- engine_errors_keep_400_and_403_statuses_without_panicking output ----
panicked at crates/sync-wasm/tests/wasm_surface.rs:148:5:
assertion `left == right` failed
  left: 500
 right: 400

test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out
Error: Node failed with exit_code 1
```

After reverting the mutation, the same command was GREEN:

```text
running 3 tests
test push_and_pull_round_trip_through_wasm_exports ... ok
test preflight_then_application_write_then_finalize_preserves_journal_order ... ok
test engine_errors_keep_400_and_403_statuses_without_panicking ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

## Native single-writer scheduling mutation

The production `Manager::get` reuse branch was temporarily removed. Every concurrent caller therefore spawned a separate worker thread and SQLite connection for the same namespace file. SQLite's busy timeout could still serialize the transactions, so the test records the threads that actually execute the database closures instead of relying only on final rows:

```diff
--- a/crates/sync-native/src/namespace.rs
+++ b/crates/sync-native/src/namespace.rs
@@ -455,10 +455,6 @@ pub fn get(&self, ns: &str) -> Result<Arc<Namespace>, String> {
         let key = sanitize(ns)?;
         let mut map = self.namespaces.lock().unwrap();
-        if let Some(entry) = map.get_mut(&key) {
-            entry.last_access = Instant::now();
-            return Ok(entry.ns.clone());
-        }
         let path = self.data_dir.join(format!("{key}.sqlite"));
```

Command:

```text
cargo test -p sync-native concurrent_writers_are_serialized_with_consistent_lmids_and_change_log -- --nocapture
```

RED output:

```text
running 1 test

thread 'namespace::tests::concurrent_writers_are_serialized_with_consistent_lmids_and_change_log' panicked at crates/sync-native/src/namespace.rs:827:9:
assertion `left == right` failed: one namespace must execute every closure on one worker thread
  left: 8
 right: 1
test namespace::tests::concurrent_writers_are_serialized_with_consistent_lmids_and_change_log ... FAILED

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 28 filtered out
```

After reverting the mutation, the same command was GREEN:

```text
running 1 test
test namespace::tests::concurrent_writers_are_serialized_with_consistent_lmids_and_change_log ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 28 filtered out
```
