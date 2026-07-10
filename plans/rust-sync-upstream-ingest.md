# rust sync engine: upstream-ingest mode (ZERO_MUTATE_URL parity)

Status: design, 2026-07-10. Follow-up to the chat staging rollout
(chat plans/cf-orez-migration-run.md "Rust sync host staging deploy").

## The gap, named

The engine (crates/sync-core) owns its store: client pushes are applied by
the host's mutator adapter inside the engine's own SQLite, and pulls read
that same store. That works, but it forces every consumer to bundle its
mutation logic INTO the sync host and leaves anything Node-bound or
app-worker-bound (chat's server actions, soot's projections and jobs)
either fail-closed or on a different database than the one clients read.

zero-cache never had this problem because it splits the roles: the app's
real push endpoint (ZERO_MUTATE_URL) owns writes against the upstream
database, and the sync server replicates from that database. The user's
directive: the rust engine must support the same split — "it should just
work the same as zero, with an api endpoint it can hit for push/pull."

Chat worked around half of it today: query transform is already delegated
to the app's real on-zero endpoint over an APP service binding (no gap
there). Push is the missing half.

## The seams already exist

- Upstream change feed: orez's ZeroSqlDO exposes a watermark-cursored feed
  today — `GET/POST /changes {watermark, limit} -> {watermark, changes}`
  (src/cf-do/worker.ts:569 handleChanges / readChangesSince, backed by
  src/do-sql-tracking.ts) plus `/notify` wakes. This is exactly what the
  embedded zero-cache (ZeroCacheDO) consumes now; the rust host can consume
  the same feed and replace ZeroCacheDO one-for-one, with app writes
  (pg-wire through DoBackend) completely unchanged.
- Engine write path: push application already funnels through one
  transactional apply that advances the engine's change log and produces
  pokes. Ingest needs the same funnel with rows sourced from the feed
  instead of mutators.
- Host config: SyncHostConfig (packages/sync-cf-host/src/config.ts) already
  carries per-consumer wiring (authenticate, namespace, resolveQuery,
  mutators); ingest adds `upstream` + `mutateUrl` members and makes
  `mutators` optional.

## Design

### P1 — engine: apply_upstream API

New wasm/core entry: `apply_upstream(batch)` where batch is the /changes
payload (ordered row images: table, op insert|update|delete, key, values,
watermark). Semantics:

- applies the batch in ONE engine transaction; idempotent by watermark
  (store `upstream_watermark` in engine meta; drop already-applied rows)
- advances the engine change log exactly as a push does, so CVR diffing,
  pokes, and cookies work unchanged
- schema drift: unknown table/column in a change triggers the schema
  refresh path (same classifier family as the replica-drift reset we
  shipped for zero-cache) rather than a crash

### P2 — host: ingest loop + push delegation

- Ingest loop per namespace DO: on wake (`/notify` fan-out or the existing
  wake channel) and on a safety-net interval, pull `/changes?watermark=N`
  from the paired ZeroSqlDO over a DATA service binding and feed
  `apply_upstream`. Backpressure = the limit/cursor loop that already
  exists in the feed contract.
- Push delegation mode: when config has `mutateUrl` (an APP-binding path,
  e.g. /api/zero/push), the host forwards the client push body verbatim,
  returns the app's per-mutation results in the push response, and does NOT
  apply rows locally — row visibility arrives via ingest, and the poke
  fires when the ingest transaction lands (zero's own confirmation model).
  LMID bookkeeping: record client lastMutationID when the delegated push
  response acknowledges it; the engine treats LMID advance + row arrival as
  independent, like zero-cache.
- Config shape: `mutators` XOR `mutateUrl` (one path per deployment, no
  fallback chain); `upstream: { binding, namespacePath }` required for
  mutateUrl mode.

### P3 — consumers

- chat: flip chat-host config from DO-local mutators to
  mutateUrl=/api/zero/push + ingest from start-chat-orez-data. This makes
  server actions (unfurls, notifications, agents) work on the rust path
  for free, because the app's real push endpoint runs them. Cookie-domain
  note: staging already runs the rust cookie domain, fresh-start there is
  fine; the apex cutover inherits the reset-on-cutover client story.
- soot: same shape against soot-cf-orez-data-demo namespaces; soot's
  projections/jobs keep writing pg-wire as today. Migration = restore the
  __soot_export dump into the namespace (existing machinery), start ingest
  at the restore watermark.

### P4 — native host parity

The axum host gets the same two modes with the upstream feed sourced from
orez node's change tracking (shared WAL LSN domain) instead of the DO
binding. Same engine API, different transport. Not needed for the CF
cutovers; keeps one engine story across deployments.

## Non-goals

- No dual-apply mode (local mutators AND delegation) — one path per
  deployment.
- No new cookie/watermark domain: ingest advances the engine's existing
  change log; clients never see upstream watermarks.

## Open questions

- /changes row images: confirm they carry full post-image values for
  updates (engine needs full rows or must read-through; full images
  preferred — check do-sql-tracking payloads).
- Feed retention: readChangesSince window vs an engine that was offline
  long enough to miss it — needs a "watermark too old -> full resync from
  upstream tables" path (mirror of zero-cache initial sync).
- Delegated push auth: the app push endpoint must authenticate the host's
  forwarded request (service-binding origin + forwarded client token, same
  pattern as the chat transform delegation).
