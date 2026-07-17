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

## 2026-07-16 update: O1 system lane, composed overlap, offline-past-retention

Three reliability pieces landed on top of the composed nemesis (this session,
against main after the 123d7ee restructure):

- **O1 at the system level (`fullPruneRestart` op).** Before this, no system
  lane emptied the change log AND reopened the same store, so mutant O1
  (non-durable watermark) was caught only by `cargo test -p sync-core`. The new
  op reads the server-confirmed watermark via a raw null-cookie pull, POSTs the
  new `/{ns}/admin/prune-to-head` admin route (`engine::prune_to_head`: bump the
  durable high-water, then `prune(db, 0)` to empty the log to the head),
  restarts the native process over the same SQLite file, and re-reads the served
  watermark. A regression fails the run. It sits in both the lifecycle prefix
  (so the mutation-matrix `state-machine` lane catches O1) and the nemesis
  prefix. Details below.
- **Composed overlap (`pausePulls`/`resumePulls` ops).** A second, independent
  fault class: a client-side transport pause that gates this one client's pulls
  at the fetch seam. The nemesis prefix holds it open across an engine-fault arm
  and a server restart, so two fault classes are active at once, then heals it on
  resume and requires the client to reconverge with no silent loss. It carries
  its own arm/fire/heal receipts, validated on generated schedules exactly like
  the engine-fault receipts.
- **Client offline past retention: already covered, not duplicated.**
  `harness/src/reconnect.ts` (`retainChanges: 2`, wired per-PR against both
  `rust-local` and `rust-cf` in `ci.yml`) already proves it: the client closes,
  eight upstream writes land past the retained floor, the host restarts over the
  same SQLite file, and the resumed client carries its persisted **non-null**
  cookie, is forced into a **snapshot** because the stale cookie is below the
  floor (an explicit reset, not silent loss), and its view converges with both
  the pre-close row and every offline row. The future-cookie direction (server
  behind the client) is covered in the same lane via `resetCursor` → 409 →
  explicit `onClientStateNotFound` → fresh reload. No new op was added; the suite
  stays lean.

## Clean-engine stability

Five different seeds passed this exact invocation (2026-07-16, merged main):

```sh
cd harness
for seed in 1 2 3 4 5; do
  bun src/state-machine.ts --against rust-local --nemesis --seed "$seed" --steps 24
done
```

The five runs armed 4, 3, 3, 4, and 4 faults and fired 3, 2, 2, 3, and 3. Each
resolved one still-armed kill fault as `not-fired` (the engine fault armed inside
the transport-pause window, canceled by the composed restart). Every run also
reported `transport pauses armed=1 fired=1 healed=1`: the held-open pause blocked
at least one real pull and healed on resume. Every run fired the required
`push_after_write_before_commit/kill`, restarted the native process over the same
SQLite file, ran a full prune + reopen through `fullPruneRestart`, continued
through response loss, and completed.

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

## O1: non-durable watermark, caught at the system level

`fullPruneRestart` closes mutation-matrix finding 1. Applied with:

```sh
git apply harness/mutants/patches/O1-nondurable-watermark.patch
cargo build --release -p sync-native
cd harness
# matrix's plain state-machine lane (non-nemesis):
bun src/state-machine.ts --against rust-local --seed 7 --steps 24 --no-shrink
# and inside the composed schedule:
bun src/state-machine.ts --against rust-local --nemesis --seed 1 --steps 24 --no-shrink
```

O1 drops the durable high-water fallback in `store::watermark`, returning
`max_log` instead of `max(max_log, high)`. While the change log is populated the
two are equal, so every existing system lane passed the mutant. Once
`fullPruneRestart` empties the log to the head and reopens the same file,
`max_log` is 0 while the durable high-water is unchanged. Both lanes failed:

```
[state-machine] FAIL seed=7: Error: served watermark regressed across full prune + restart: 20 -> 0
[state-machine] FAIL seed=1: Error: served watermark regressed across full prune + restart: 18 -> 0
```

The client also surfaces the downstream symptom in the logs: its persisted
cookie is now ahead of the regressed watermark, so its next pull returns
`409 InvalidConnectionRequestBaseCookie`. The probe catches the fault directly
from the server-confirmed pull cookie, before the client's silent reset can mask
it. Reverting the patch, the identical minimized trace replays **green** at
baseline and **red** under O1, so the check is specific, not flaky:

```sh
# red under O1, green after revert — same artifact both times
bun src/state-machine.ts --against rust-local --nemesis --seed 1 \
  --replay regressions/state-machine-rust-local-seed-1.json --no-shrink
```

## Composed overlap: sabotaged heal fails schedule validation

The overlap's receipt validation is proved by a deliberately sabotaged heal
(`resumePulls` left without its `healTransport` call), then restored. A generated
nemesis run then fails top-level validation rather than passing vacuously:

```
[state-machine] FAIL seed=1: Error: INVALID transport schedule: armed 1 pauses but healed 0
[state-machine] transport pauses armed=1 fired=1 healed=0
```

Like the fired-fault gate, this coverage check runs only on generated
(non-replay) nemesis schedules: a shrink candidate may legitimately drop the
pause/resume pair, and the check has its own fingerprint
(`transport-schedule-invalid`) so it never masquerades as the failure under
minimization. The composed schedule also fails end to end under a real engine
mutant: the seed-1 nemesis trace above (which contains the overlap) goes red at
`fullPruneRestart` under O1.

## CI schedule

The `rust-local-faults` PR job runs one bounded 24-operation nemesis trace with
`github.run_id` as its fresh, replayable seed. The nightly `rust-local-heavy` job
runs one 80-operation trace with the same fresh seed source. Both jobs already
upload `harness/regressions/` and `harness/results/`, including receipts, tallies,
and minimized traces.
