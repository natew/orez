# orez 0.5.16 — issues found from soot prod example deploys (for the orez agent)

Reporter: soot-side agent. Runtime-proven, not static speculation. Nate asked me
to hand you everything I found so you can fix at the orez layer. I diagnosed
only — I did not edit any orez files.

## ISSUE 1 (primary, blocking): initial sync livelocks on CF DO storage reset / OOM

### verdict

orez 0.5.16 embedded zero-cache **never completes initial sync** on Cloudflare.
It is a **0.5.16 regression that hits every fresh initial sync**, not one app's
data — proven on two independent example apps. The `fix(worker): survive durable
object storage resets` work (`f87cf06`, shipped IN 0.5.16) is present in the
running dist but **does not resolve this** — sync still livelocks.

### runtime proof

Captured with `wrangler tail <worker> --format json` during initial sync, on
workers built against npm `orez@0.5.16` (verified: `f87cf06` markers
`orez-cf-storage-burst-cap`, `MAX_BUFFERED_ROWS`, `runtime-abandon-after-do-reset`,
`doSqliteStorageIncarnation` are all present in the installed `node_modules/orez/dist`).

| app                     | resets in window | window | MTBF  | boot restarts | error                                                                |
| ----------------------- | ---------------- | ------ | ----- | ------------- | -------------------------------------------------------------------- |
| pennywise (app-finance) | 4                | 40.6s  | ~10s  | 4             | `Internal error in Durable Object storage caused object to be reset` |
| travelo (app-travel)    | 19               | 71s    | ~3.7s | 19            | same                                                                 |

- Every reset lands on a `ZeroCacheDO` **scheduled (alarm)** event and is
  immediately followed by a fresh boot from the top:
  `zero-cache embed boot: starting → migrations done → replica tag checked →
changelog checked → cdc checked, starting embed`.
- Between resets the embed does real sync work: heavy `ZeroSqlDO` traffic to
  `orez-do-backend.local/exec` (90 in one 40s window), `/batch`, `/commit-tx`,
  `/snapshot-tx-schema`, `/recover-txs`.
- Consumer symptom: the deploy warm step throws `zero-cache embed did not become
ready within 900s`. Live `/keepalive?deploy=1` sits on `202 booting` forever.
- Write volume is modest — pennywise ~111k rows/hr (ZeroCacheDO 63.7k + ZeroSqlDO
  47.3k). This is **not** a fast write-amplification loop (chat staging's ~1.47M/hr
  burst is a different thing); it's a slow restart-livelock.

### root cause (code-level, your own comment names it)

`src/worker/zero-cache-replica-repair.ts`, `clearChangeStreamerStateIfReplicaUninitialized`:

```
// a replica without its init marker must not reuse the cdc subscription state,
// or initial sync never re-runs. the change-streamer's subscription state lives
// in the SQL DO and SURVIVES a replica wipe (the resets above, or an OOM
// eviction); a wiped replica + surviving subscription state makes zero-cache
// skip initial sync ("already synced") ... when the replica has no init marker,
// clear the cdc state so the embed re-runs initial sync from scratch.
```

The livelock:

1. CF resets/OOM-evicts the DO **during the initial COPY** → replica DO storage
   is wiped, init marker gone.
2. `clearChangeStreamerStateIfReplicaUninitialized` sees no init marker → clears
   CDC subscription state → **forces initial sync to re-run from scratch**.
3. The heavy initial COPY runs again and OOM-resets again **before it finishes and
   durably writes the init marker**.
4. GOTO 1. Forever. Reset cadence (~3.7–10s) is far shorter than the time a full
   initial COPY needs, so it can never win the race.

The recovery logic is correct for a _rare_ reset. It is fatal when the initial
COPY _itself_ is what triggers the reset — recovery just re-arms the trigger.

### why f87cf06 doesn't fix it

- `MAX_BUFFERED_ROWS = 256` (`cf-patches.ts`, `orez-cf-storage-burst-cap`) caps
  the per-stream-callback write burst. The reset still fires, so **the reset is
  not caused by per-callback write-burst size** — something else accumulates in
  memory across the COPY until CF OOM-resets the object. The burst cap addressed
  a different theory than the actual OOM source.
- `runtime-abandon-after-do-reset` / `doSqliteStorageIncarnation` make a reset
  _survivable_ (clean abandon of the dead storage handle, no crash) but **not
  progress-preserving** — the surviving path is "re-run initial sync from
  scratch," which is the loop.

### fix directions (your call — I did not implement)

- **(A) Stop the reset during initial COPY (the real root).** Find what grows
  unboundedly in the DO isolate during the initial COPY and OOMs it — candidates:
  the in-memory replica/SQLite page cache, accumulating snapshot tables, the COPY
  read buffer, or a single large transaction that never flushes mid-COPY. Bound
  that so a full initial sync fits inside one DO incarnation. The comment already
  suspects OOM eviction — chase that, not write-burst size.
- **(B) Make initial sync resumable across resets (robust safety net).** Persist
  COPY progress durably (per-table copied-cursor / partial-init marker) so a
  post-reset re-run RESUMES instead of re-copying from zero. Then sync makes
  monotonic forward progress and completes even if resets keep happening. Today
  step 2 deliberately throws progress away.

Ideally both: (A) removes the trigger, (B) guarantees convergence if it ever
fires again.

## ISSUE 2 (informational, already handled soot-side): DoBackend is now a split chunk

Not an orez code bug — a packaging consequence consumers must handle. In 0.5.16
the CF embed dynamic-imports `DoBackend` from a separate root chunk
(`pg-proxy-do-backend-*.js`) instead of inlining it into `zero-cache-embed-cf-*`.
soot's per-package wrangler ESModule `globs` allowlist didn't attach that chunk,
so the worker uploaded clean and then 500'd on first boot with
`No such module "pg-proxy-do-backend-KT3VC33E.js"` (dynamic specifiers aren't
validated at upload). We fixed it soot-side with a general `*-*.js` root-chunk
glob so any future dependency chunk split auto-attaches.

Suggestion: note in orez's CF-deploy guidance that consumers must attach **all**
emitted root chunks in their wrangler modules rules, or list the chunks the embed
dynamic-imports, so the next split doesn't silently break a different consumer.

## what I did NOT touch

- No orez files edited. Diagnosis only.
- soot side: the glob fix + a version-gated boot-failure reset are already landed
  on soot main (they let the DO boot far enough to expose Issue 1). Phase-2 example
  refreshes and any fresh user-app deploy on 0.5.16 are HELD until Issue 1 is fixed
  — they would all livelock on fresh initial sync.
- Travelo was temporarily deployed on 0.5.16 to prove the regression, confirmed it
  livelocks identically, then reverted to its 0.5.11 bundle (`e689d88b`). No demo
  left broken, no prod DB mutated.
