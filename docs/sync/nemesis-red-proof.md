# Composed nemesis red proof

Verified on 2026-07-16 from branch `test/coverage-nemesis`. The composed mode is
the existing generated lifecycle state machine with `armEngineFault` operations
added to its seeded vocabulary. It boots `rust-local` with `retainChanges: 2`.
Normal state-machine runs remain at `retainChanges: 8` and do not generate engine
faults.

Each successful arm stores the admin route's confirmation in the executed trace.
The same receipt is resolved as `fired` from the matching server response or killed
request, or as `not-fired` when another arm replaces it, the server restarts, or the
run ends. A generated nemesis run with no fired receipt is invalid: the required
prefix pairs each arm with a firing write, so zero fires means injection itself
broke. Replays and shrink candidates are judged on execution alone, because
receipts canceled by restart, replacement, or run-end are legitimate there and a
whole-run coverage property shrinks to a vacuous trace. The final tally and the
result JSON report both outcomes.

## Clean-engine stability

Five different seeds passed this exact invocation:

```sh
cd harness
for seed in 1 2 3 4 5; do
  bun src/state-machine.ts --against rust-local --nemesis --seed "$seed" --steps 24
done
```

The five runs armed 3, 4, 3, 4, and 3 faults. They fired 3, 3, 2, 4, and 3.
Seeds 2 and 3 each resolved a still-armed random kill fault as `not-fired` when the
run ended. This proves the generator does not serialize every arm immediately next
to a firing operation. Every run also fired the required
`push_after_write_before_commit/kill`, restarted the native process over the same
SQLite file, continued through response loss, and completed.

## L1: prune without a retained floor

Applied with:

```sh
git apply harness/mutants/patches/L1-prune-no-floor.patch
cd harness
bun src/state-machine.ts --against rust-local --nemesis --seed 101 --steps 24 --shrink-runs 50
```

The mutant removed the floor update while leaving change deletion intact:

```diff
diff --git a/crates/sync-core/src/store.rs b/crates/sync-core/src/store.rs
@@
         db.exec(
             "DELETE FROM _zsync_changes WHERE watermark <= ?",
             &[counter(cutoff)],
         )?;
-        db.exec(
-            "UPDATE _zsync_meta SET floor = ? WHERE lock = 1",
-            &[counter(cutoff)],
-        )?;
```

The 24-operation run failed because the server-confirmed `_zsync_meta.floor`
did not advance. Delta debugging took 15 replays and reduced the composed failure
to three operations. At the time the in-execution vacuity check forced every
accepted candidate to retain a fired fault; since shrink candidates are judged
on execution alone (2026-07-16), the same mutant minimizes to the single `prune`
operation, which still replays red on the mutant and green on a clean engine:

```json
[
  {
    "kind": "armEngineFault",
    "point": "push_after_commit_before_response",
    "faultKind": "error",
    "faultReceipt": {
      "id": "fault-101-1",
      "arm": {
        "step": 0,
        "point": "push_after_commit_before_response",
        "kind": "error",
        "confirmed": true
      },
      "resolution": {
        "status": "fired",
        "step": 1,
        "operation": "write",
        "reason": "server confirmed injected 500 during push"
      }
    }
  },
  {
    "kind": "write",
    "id": "sm-101-postcommit",
    "projectID": "p0",
    "rank": 4.25
  },
  { "kind": "prune", "epoch": 0 }
]
```

Final tally: `armed=1 fired=1 not-fired=0`.

## M1: skip mutation finalization

Applied with:

```sh
git apply harness/mutants/patches/M1-skip-finalize.patch
cd harness
bun src/state-machine.ts --against rust-local --nemesis --seed 202 --steps 24 --shrink-runs 50
```

The mutant committed app rows without advancing the LMID:

```diff
diff --git a/crates/sync-core/src/push.rs b/crates/sync-core/src/push.rs
@@
-                    Ok(()) => {
-                        finalize(db, group, &m.client_id, m.id)?;
-                        Ok(Tx1Ok::Applied)
-                    }
+                    Ok(()) => Ok(Tx1Ok::Applied),
```

The schedule armed `push_after_commit_before_response/error`. Its receipt fired,
the response was lost after the row commit, and recovery could not settle because
the LMID had not advanced. The run failed while waiting for the server-confirmed
write. Delta debugging took 11 replays and reduced 24 operations to the causal
pair:

```json
[
  {
    "kind": "armEngineFault",
    "point": "push_after_commit_before_response",
    "faultKind": "error",
    "faultReceipt": {
      "id": "fault-202-1",
      "arm": {
        "step": 0,
        "point": "push_after_commit_before_response",
        "kind": "error",
        "confirmed": true
      },
      "resolution": {
        "status": "fired",
        "step": 1,
        "operation": "write",
        "reason": "server confirmed injected 500 during push"
      }
    }
  },
  {
    "kind": "write",
    "id": "sm-202-postcommit",
    "projectID": "p0",
    "rank": 4.25
  }
]
```

Final tally: `armed=1 fired=1 not-fired=0`.

Both patches were reversed after their proof. The requested plain-lane check then
passed at baseline and caught both mutants:

```sh
cd harness
bun scripts/mutation-matrix.ts \
  --mutants L1-prune-no-floor,M1-skip-finalize \
  --lanes state-machine
```

Run `2026-07-17T01-26-01-549Z` reported baseline `state-machine: pass`, then
`state-machine: CAUGHT` for M1 and L1. This confirms the opt-in nemesis path did
not regress the plain lane.

## CI schedule

The `rust-local-faults` PR job runs one bounded 24-operation nemesis trace with
`github.run_id` as its fresh, replayable seed. The nightly `rust-local-heavy` job
runs one 80-operation trace with the same fresh seed source. Both jobs already
upload `harness/regressions/` and `harness/results/`, including receipts, tallies,
and minimized traces.
