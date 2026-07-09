# zero server rewrite: purpose-built sync server for sqlite/cloudflare

status: consolidated plan, 2026-07-09. this merges the prior decision docs and
the passed zero-http spike into one execution plan. the direction is the
"clean-room zero-compatible engine" (soot's
`plans/sootbean/zero-compatible-sync-engine.md`, option 2), scoped to what is
provable and shippable: keep the stock `@rocicorp/zero` client, replace the
server half on cloudflare with a native DO-sqlite sync server, one plane at a
time. no embedded zero-cache, no pg wire, no fake replication on the CF path
when this is done.

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
  ▼
app worker
  ├── /api/zero/pull-control ──► per-user full snapshot from control storage
  ├── /api/zero/pull-project ──► cursor-diff from project DO change log
  └── /api/zero/push ─────────► on-zero server mutators (already HTTP)
  ▼
ZeroSqlDO (per project + control): DO sqlite, _zero_changes + watermark +
tx-journal. the authoritative store. no ZeroCacheDO, no replica, no CVR/CDB,
no pgoutput stream, no pg-wire upstream connection on the CF path.
```

per-client server state: durable LMID bookkeeping only. memory scales with
in-flight requests, not connected users.

## phase 1: control plane (the zero-http follow-on, already sequenced)

this is `plans/zero-http.md` "follow-on" verbatim; the spike code in
`src/zero-http/` is the reference implementation and its tests are the
contract.

1. lift the transport into on-zero (`~/takeout`) as a
   `transport: 'http-pull'` mode on `createZeroClient`, preserving all nine
   wire discoveries (fixed-width lexicographic cookies, gotQueriesPatch
   ordering, push FIFO serialization, updateAuth, 401→Unauthorized frame,
   group→user binding, ackMutationResponses pruning, teardown drain).
2. real pull/push endpoints in soot's data worker backed by orez storage.
   the fixture server pins semantics: clear+puts snapshots, monotonic
   version cookie, LMID bookkeeping, app-error-still-advances-LMID.
3. release on-zero (local iteration via `bun release --into ~/soot`; npm
   publish only with explicit approval), flip the control instance, stop
   accepting `/sync` sockets on the control namespace, delete the
   dual-instance workarounds (`useControlConnectionState`, the
   `run(…, 'complete')` reads in `useAccessDeniedCheck` /
   `useAnonProjectRedirectOnLogin`).
4. acceptance: access-denied playwright suite green + a 50-user
   `cf-load-longevity` run with zero control-DO memory events.

side effect worth naming: control-plane pushes stop routing through
`ZeroCacheDO` re-entry, so the `Subrequest depth limit exceeded` class
(`~/soot/plans/sootbean/zero/custom-mutator-depth-seam-2026-07-09.md`) is
deleted for the control plane rather than worked around. the project plane
keeps the embed until phase 2, so that seam spec stays relevant short-term;
coordinate with whoever owns it before either side ships.

## phase 2: project plane (cursor-diff pulls)

the new work. design first, then measure, then build.

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

### acceptance

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

- server code lives in orez (`src/zero-http/` graduates from spike to the
  real module); transport lives in on-zero (`~/takeout`); soot wires
  endpoints and flips instances.
- releases beyond the local tree need explicit approval, per repo rules.
- ~/soot is a shared multi-agent checkout: explicit-pathspec commits only,
  and coordinate with the depth-seam spec owner (phase 1 changes that
  problem's scope).
