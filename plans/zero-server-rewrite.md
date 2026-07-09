# zero server rewrite: purpose-built sync server for sqlite/cloudflare

status: consolidated plan, 2026-07-09. this merges the prior decision docs and
the passed zero-http spike into one execution plan. the direction is the
"clean-room zero-compatible engine" (soot's
`plans/sootbean/zero-compatible-sync-engine.md`, option 2), scoped to what is
provable and shippable: keep the stock `@rocicorp/zero` client, replace the
server half on cloudflare with a native DO-sqlite sync server, one plane at a
time. no embedded zero-cache, no pg wire, no fake replication on the CF path
when this is done.

**where we actually are (verified 2026-07-09):** phase 1 (control plane on
http-pull) is SHIPPED and validated in prod. on-zero ships
`transport: 'http-pull'` (`~/takeout/packages/on-zero/src/httpPullTransport.ts`
plus the ported spike suite in `src/httpPull/`), soot serves
`/zero-http/pull|push` (`~/soot/src/zero/httpPull.server.ts`), the control
instance is flipped (`~/soot/src/zero/client.tsx`), and the flip's five launch
gates were proven (soot commit 70fb7efd26; the shipped integration design was
pruned from soot plans as done, recover it via
`git show fe4b345577~1:plans/sootbean/zero/zero-http-integration.md`).
**phase 2 (project plane) is the active work.**

## prior art this consolidates (read these before changing course)

- `~/soot/plans/sootbean/zero-compatible-sync-engine.md`: option taxonomy +
  the measured motivation. the 128MB `ZeroCacheDO` singleton dies on
  control-plane connections alone (<50 concurrent users, two load runs
  2026-06-12); per-project sharding shipped and did not move the wall,
  because the cost is per-client resident view-syncer state, structural to
  zero-cache's connection contract.
- `~/orez/plans/zero-http.md`: the spike, PASSED 2026-06-12. a stock 1.6.1
  zero client runs on an HTTP transport (fake-WebSocket seam) against a
  stateless server: full-snapshot pulls, HTTP push, LMID bookkeeping, rebase,
  rollback, relations, auth parity all proven. 26 tests in `src/zero-http/`
  pin nine wire-level discoveries; that VERDICT section is the transport
  contract. its "step 2" (cursor-diff pulls) is phase 2 below.
- `~/soot/src/zero/core.ts`: the account/project split, live in prod. control
  instance owns the small account-level graph (user, userState, project
  directory, subscription, planGrant, tokenUsage, workspace, communityListing,
  deploySlug); project instance owns the wave-heavy per-project data
  (message, thread, agentEvent, file, sootSession, ...) and routes to the
  project's OWN DO pair. a table lives in exactly one instance, enforced at
  module eval by `assertZeroInstancePartition`.
- `~/soot/docs/orez-architecture.md` §5: everything the current CF path costs
  us: the embed, 5 cf-patches, ~30 workerd shims, the 7.7k-line PG-wire
  translator feeding zero-cache, replica reset machinery, two split-brain
  self-healing fixes.
- `~/soot/plans/deploy-research-planetscale-hyperdrive-vs-cf-zero.md`
  (2026-07-08): confirms soot's own stack stays on CF DO + orez. managed
  postgres is a future paid add-on for user apps only. the rewrite target is
  not moving.

## why the split is the core design lever

the account/project split is not just load isolation. it makes CVR-free sync
tractable, which is what lets us delete the server half of zero:

- **account (control) plane**: KB-scale per user, visibility is per-user row
  filtering. full-snapshot pulls (clear + puts) are cheap, idempotent, and
  need zero per-client server state. spike-proven end to end.
- **project plane**: MB-scale per project, but visibility inside a project DO
  is near-uniform (members see the project's data; the already-filtered
  `*Public` query shapes handle the exceptions). that means cursor-diff sync
  does not need per-row CVR bookkeeping: a watermark cursor over the change
  log is a correct delta source for every member, and membership changes are
  rare enough to handle with a per-(project,user) epoch that forces a fresh
  snapshot on join/revoke.

zero-cache needs CVR + resident IVM precisely because it serves arbitrary
per-client query subsets from one shared stream. the split removes that
requirement plane by plane, which is why the rewrite is small enough to own.

## end state

```
client (stock @rocicorp/zero, zql, optimistic mutations, rebase: unchanged)
  │  on-zero transport: 'http-pull' (fake-WebSocket seam, v51 pokes)
  │  note: base must be ONE path component (zero server-option validation),
  │  so soot uses /zero-http, project routes get their own single-component
  │  base (e.g. /p-<projectId> prefix routing already exists)
  ▼
app worker
  ├── /zero-http/pull (control) ──► per-user full snapshot   [SHIPPED]
  ├── project pull ───────────────► cursor-diff from project DO change log
  └── /zero-http/push ────────────► on-zero PushProcessor (LMID in soot_0.clients)
  ▼
ZeroSqlDO (per project + control): DO sqlite, _zero_changes + watermark +
tx-journal. the authoritative store. no ZeroCacheDO, no replica, no CVR/CDB,
no pgoutput stream, no pg-wire upstream connection on the CF path.
```

per-client server state: durable LMID bookkeeping only. memory scales with
in-flight requests, not connected users.

## phase 1: control plane on http-pull [SHIPPED]

done and validated in prod. what shipped (evidence, not aspiration):

- on-zero `transport: 'http-pull'` mode with the full spike test suite
  ported (`packages/on-zero/src/httpPullTransport.ts` + `src/httpPull/`),
  plus production hardening the spike didn't have: transient-reconnect
  backoff, 409 → InvalidConnectionRequestBaseCookie reset, per-client push
  result filtering, rehydrated baseCookie suffix resume, bound fetch.
- soot endpoints at `/zero-http/pull|push` (`src/zero/httpPull.server.ts`):
  full per-user snapshot with schema-typed value conversion, cookie derived
  from control-row change clocks + client-group max LMID, group→user
  binding via a nullable `userID` column on `soot_0.clients`, push through
  zero's own `PushProcessor`.
- control instance flipped in `src/zero/client.tsx`
  (`transport: 'http-pull'`, server `${APP_ORIGIN}/zero-http`).
- launch gates proven (soot 70bd/70b commit chain, `zero-http-flip` handoff);
  the shipped design doc was pruned as done, recover via
  `git show fe4b345577~1:plans/sootbean/zero/zero-http-integration.md`.

known caveats recorded at ship time: admin cross-user control queries are
not in the snapshot; tokenUsage snapshot is newest-200 per user. extend the
snapshot when a control UI needs more, not before.

consequence already banked: control-plane pushes no longer route through
`ZeroCacheDO` re-entry. the `Subrequest depth limit exceeded` class
(`~/soot/plans/sootbean/zero/custom-mutator-depth-seam-2026-07-09.md`)
survives only on the project plane, which still rides the embed. that seam
spec is the interim fire fix; phase 2 is the structural fix. coordinate with
its owner so effort isn't duplicated.

## phase 2: project plane (cursor-diff pulls) [THE ACTIVE WORK]

design first, then measure, then build.

**progress 2026-07-09: the cursor-diff protocol is implemented and green in
the reference core** (`src/sync-server/sync-server.ts`, the harness's
orez-local/orez-cf server). what's proven there:

- change log `_zsync_changes` (watermark autoincrement) fed by per-table
  sqlite triggers — captures mutator AND upstream/admin sql writes; stores
  touched pks ONLY, and the diff pull re-reads live rows in its transaction.
  row images through sqlite's json functions are a trap: json_object formats
  REAL at 15 significant digits (0.1+0.2 → 0.3), which corrupts float
  columns. live-read sidesteps float mangling and op-coalescing entirely.
- cookie = change-log high watermark. unchanged/409 semantics as phase 1.
- LMID-only pushes (app errors) append an 'lmid' marker row: mutation
  RECOVERY settles via lastMutationIDChanges in a NON-unchanged pull, so an
  LMID advance must move the cookie even with zero row changes.
- retention is size-bounded (`retainChanges`, default 4096): pruning raises
  a floor; cookie below floor → full snapshot (clear+puts), the single
  recovery path. per-user `visible()` configs always snapshot (a visibility
  filter can revoke rows without a row change, which no diff can express —
  the project plane's uniform-visibility assumption is what enables diffs).
- pk-changing UPDATEs log OLD and NEW pks (del old, put new).
- validated: 18-test delta suite (`src/sync-server/sync-server.test.ts`:
  churn convergence, floor fallback, recreate/ephemeral collapse, two tabs
  one group, float exactness), harness smoke/shapes/bench on bun:sqlite and
  the real CF DO. measured on the DO at 10 clients 3 writers x 5/s: ack p50
  1169→173ms, propagation p50 1538→551ms vs full-snapshot pulls (500ms poll
  interval now dominates propagation).

remaining for phase 2 proper: compose the same protocol over soot's
production `_zero_changes` (per-(project,user) epochs for membership
revocation, per-table window policy, prod DO size measurements below), then
the soot validators + transport flip.

### protocol

- pull request carries the client cookie = last-seen watermark + epoch.
- server reads committed rows from `_zero_changes` (tx-journal already
  guarantees only-committed visibility) where `watermark > cookie`, maps to
  put/del row patches, returns new cookie. dels come from the change log's
  old-row data.
- epoch mismatch (membership change, retention floor passed, table-set
  change) → full snapshot response (clear + puts), same shape the control
  plane uses. one recovery path, no special cases.
- push is unchanged from phase 1 (on-zero server already executes project
  mutators over HTTP).

### retention replaces the ack protocol

today `_zero_changes` is retained until zero-cache proves durable consumption
(`plans/cf-durable-stream-progress.md`, the WAL-style contract that the july
cost incident forced). with cursor pulls, retention becomes purely
size/time-bounded: keep N days or M rows, and any client whose cookie falls
below the floor gets a snapshot. this deletes the re-stream write-amplification
class on the CF path entirely. the durable-stream-progress work stays correct
for the node/pglite backends and for however long the embed still runs; do not
stall it on this plan.

### bounding what syncs (the honest open question)

full-project sync is fine for most projects, but factory-shaped projects grow
append-heavy tables (agentEvent, message) without bound. options, simplest
first:

1. per-table window policy: append-log tables sync only the recent window
   (server-side `where watermark/createdAt > floor`), older rows age out of
   the client store via the same epoch/snapshot path. no per-query server
   state.
2. server-evaluated named queries per pull (soot already uses named/grouped
   queries exclusively, so the query set is enumerable and server-known).
   more machinery; only if (1) measurably fails.

decision gate: measure real per-table row counts and byte sizes across prod
project DOs before committing. if p95 project full-sync is single-digit MB,
option 1 with generous windows wins.

**MEASURED 2026-07-09** (soot `scripts/ops/cf-project-do-sizes.ts`, offline
parse of the 6-hourly R2 backups, 1040 project DOs): project logical size
p50 0.23MB / p90 0.37MB / p95 0.40MB / max 1.15MB; rows p50 664 / max 1165.
app data is tiny — the byte budget is dominated by embed-support state:
`_orez_pg_metadata` 193.6MB fleet-wide (76% of all bytes),
`soot_0_replicas` + `soot_0_publishedSchema` 24.6MB, `_zero_changes`
22.6MB. `file` is the largest app table at 13.1MB fleet / 0.07MB max per
project; `message` maxes at 20 rows in any project. **decision: no window
policy for now — full-project snapshots are sub-MB even at max, so option
1 isn't needed yet; re-run the measurement when a factory-shaped project
appears. bonus: phase 3 deletes the reason most of those bytes exist.**

### acceptance

- the conformance harness lanes (`plans/zero-conformance-harness.md`
  EXECUTION PLAN) run green against the new server: query-shape backbone +
  sweep, load lanes, on both the pure-sqlite local host and the DO host.
  the harness's `orez-local` target IS this server core; they are built
  together.
- delta correctness suite in orez mirroring the spike style: interleaved
  push/pull churn, visibility revocation via epoch, cursor-below-floor
  snapshot fallback, two tabs one client group, multi-client convergence.
- soot validators: `validate-cf-do-runtime.ts` (flicker detector included),
  factory-wave load run on a project DO, chat e2e suite.
- project instance flips transport; project namespaces stop accepting
  `/sync` sockets.

## phase 3: delete the embed on the CF path

once both planes are on http-pull, remove from the CF deploy:

- `worker/zero-cache-embed-cf.ts`, `worker/cf-patches.ts`, the workerd shim
  surface used only by the embed
- soot's `ZeroCacheDO` class, replica reset machinery
  (`resetReplicaIfTableSetChanged`), the `SchemaVersionNotSupported`
  self-healing pair, the embed-generation recycling
- the pgoutput streaming leg into zero-cache on CF (change tracking itself
  stays: it feeds cursor pulls now)
- `DoBackend`'s zero-cache-upstream role (verify what the app SQL path still
  uses of it before cutting; the app-side pg→sqlite translation is a
  separate concern from the zero-cache upstream connection)

`cloudflareDoDeploy.ts` should shrink by the majority of its hard parts. the
guard tests that assert embed internals get replaced by guards asserting the
new endpoints. one path: after this phase there is no flag, env toggle, or
fallback that can boot zero-cache in a DO.

## phase 4 (directional, explicitly not now)

node and web runtimes keep their current paths (node is on the real-postgres
backend and healthy; web serves the in-IDE dev env). when the sqlite-native
server is proven in prod, they can adopt the same server for dev/prod parity
and orez sheds the pglite/pgoutput machinery everywhere. build nothing for
this yet.

## conformance strategy (from the option doc, still the discipline)

- the client is stock and pinned (`@rocicorp/zero@1.6.1`); compatibility is
  with the v51 wire protocol the spike tests pin, not with zero-cache
  internals. a zero version bump moves one surface (the client transport
  contract) instead of five patch layers.
- inventory the zql/named-query subset soot + takeout actually use before
  phase 2 lands (step 3 of the option doc). compatibility with our usage is
  the target, not all of zero.
- soot's e2e suites (access-denied, chat e2e, cf-load-longevity, runtime
  validators) are the ongoing conformance harness.

## working agreements

- code placement follows the phase 1 precedent: transport in on-zero
  (`~/takeout`), endpoints + app wiring in soot, reusable primitives in
  orez. for phase 2 that means the delta machinery (change-log → row-patch
  mapping, epoch bookkeeping, retention/compaction over `_zero_changes`)
  belongs in orez next to `cf-do/watermark.ts` + `cf-do/tx-journal.ts`;
  soot's data worker composes it into the project pull endpoint. the orez
  spike dir `src/zero-http/` stays as the frozen wire-contract reference
  (its port now lives in on-zero `src/httpPull/`).
- releases beyond the local tree need explicit approval, per repo rules.
- ~/soot is a shared multi-agent checkout: explicit-pathspec commits only,
  and coordinate with the depth-seam spec owner (phase 1 changes that
  problem's scope).
