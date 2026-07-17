# Consistency lane red proof

Both native consistency lanes were run against a deliberately broken
`sync-core` release build on 2026-07-16. Each command exited 1 with a checker
verdict tied to the injected engine defect. The mutants were applied one at a
time and reverted after the captured run.

## Atomic visibility

Command:

```sh
cd harness
bun src/atomic-visibility-lane.ts --target rust-local --seed=red-proof-atomic
```

Mutant: admit only the first row operation in each incremental pull while still
advancing the pull cookie over the remaining changes.

```diff
diff --git a/crates/sync-core/src/pull.rs b/crates/sync-core/src/pull.rs
index 691faa9..ef79712 100644
--- a/crates/sync-core/src/pull.rs
+++ b/crates/sync-core/src/pull.rs
@@ -341,7 +341,9 @@ fn diff(
         }

         if let Some(op) = pending_op {
-            ops.push(op);
+            if ops.is_empty() {
+                ops.push(op);
+            }
         }
         if let Some(key) = pending_key {
             seen.insert(key);
```

Red output:

```text
[atomic-visibility] FAIL target/consistency/atomic-visibility/atomic-visibility-0eb27ca64f4ed8d0
[atomic-visibility] replay: bun src/atomic-visibility-lane.ts --target rust-local --seed=red-proof-atomic --replay
error: atomic visibility violations:
atomic group atomic-visibility-0eb27ca64f4ed8d0-mutation is partially visible in read atomic-visibility-0eb27ca64f4ed8d0-read-after-1; missing effects: p1=1010899278
atomic visibility requires at least one eligible pair observing a complete group
```

The failed read was a complete materialized query on `atomic-reader`, a client
that never issued a mutation. The authority check still contained both rows,
so the verdict isolates a torn client patch rather than a failed write.

## Exactly-once LMID

Command:

```sh
cd harness
bun src/exactly-once-lmid-lane.ts --target rust-local --seed=red-proof-lmid
```

Mutant: commit mutation rows and the LMID change-log marker without advancing
the authoritative client LMID.

```diff
diff --git a/crates/sync-core/src/store.rs b/crates/sync-core/src/store.rs
index 63245cc..d5d4f0f 100644
--- a/crates/sync-core/src/store.rs
+++ b/crates/sync-core/src/store.rs
@@ -190,11 +190,6 @@ pub(crate) fn advance_lmid(
     client_id: &str,
     mutation_id: i64,
 ) -> Result<(), EngineError> {
-    db.exec(
-        "UPDATE _zsync_clients SET lastMutationID = ?
-         WHERE clientGroupID = ? AND clientID = ?",
-        &[counter(mutation_id), text(client_group_id), text(client_id)],
-    )?;
     // the lmid row carries the group so a diff derives acks for its OWN group
     // only (never leaking a peer group's lmid); the lmid value rides as text so
     // it too avoids any number path.
```

Red output:

```text
[exactly-once-lmid] FAIL target/consistency/exactly-once-lmid/exactly-once-5025cb1ef8cd61d4
[exactly-once-lmid] replay: bun src/exactly-once-lmid-lane.ts --target rust-local --seed=red-proof-lmid --replay
[exactly-once-lmid] failure: Error: after authority does not show one application and LMID 1
stock retry 2 was not already processed
final harness replay was not already processed
error: after authority does not show one application and LMID 1
stock retry 2 was not already processed
final harness replay was not already processed
```

The final counts came from direct authenticated authority SQL, after the lost
response, stock retry, client quiescence, and byte-identical harness replay.
The precondition client was a separate non-writing Zero client. All scheduled
fault arm, fire, and heal receipts were present and anchored in the history.

## Elle list-append on the recorded atomic-visibility history

`scripts/elle/check-history.sh` projects a recorded atomic-visibility
`history.jsonl` and runs the pinned elle-cli 0.1.9 jar with `--model
list-append --consistency-models serializable --verbose`, failing on anything
other than `valid?: true`. Proven both directions on 2026-07-16 against a real
history recorded by `bun src/atomic-visibility-lane.ts --target rust-local
--seed elle-proof`.

Green: the unmodified recorded history returns `{"valid?":true}`, exit 0. The
observed lists are restricted to the appended ranks, so the reader observes an
empty list before the append and `[<rank>]` after it (a serial order exists).

Red: the after-read observation for one key in the recorded `history.jsonl` was
edited to drop that key's appended rank, fabricating the same partial-visibility
torn read the `pull.rs` mutant above produces (the reader sees the append on
`p0` but not on `p1`). The history stays structurally valid
(`validateHistory` does not constrain read values), so it flows through the
projection into elle:

```text
elle did not return valid=true (got False); anomalies: ['G-single-item']
```

elle's `also-not` list includes `serializable`, and the runner exits 1. The
G-single cycle is exactly `wr` on the visible key and `rw` on the missing key,
which is the dependency signature of a non-atomic client observation.
