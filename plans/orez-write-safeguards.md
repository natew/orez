# Orez write safeguards

## Why this exists

On 2026-07-10, `soot-rust-sync-data-preview.ZeroSqlDO` wrote 468,198 rows in
five minutes (about 5.62M/hour). The known legitimate five-minute peak is
70–100k rows. The burn resumed without a local probe, application/data
trigger, or a writing Rust host; the strongest remaining signature was a reused
ZeroCacheDO repeatedly doing a partial replica boot/re-stream. The sticky
circuit eventually refused lazy `CREATE TABLE IF NOT EXISTS _zero_changes`.
The exact upstream cursor progression was not captured before containment.

These safeguards stop writes in the source Durable Object and slow feedback
loops in the sync host. Billing analytics remain a backstop, not the primary
stop.

## ZeroDO row-write circuit

`src/cf-do/worker.ts` wraps the singleton ZeroDO's SQL handle. Mutating SQL is
classified before execution; successful statements contribute their
`rowsWritten` count to a rolling window. Reads remain available while tripped.

Defaults:

- `OREZ_DO_WRITE_BUDGET_ROWS=150000`
- `OREZ_DO_WRITE_BUDGET_WINDOW_MS=300000` (five minutes)
- `OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN` authorizes a manual reopen
- `OREZ_DO_WRITE_BUDGET_DISABLED=1` is the only opt-out and emits a loud
  `orez_do_write_budget_disabled` error log at object construction

The budget is measured in Cloudflare billable SQLite rows, not application
rows. Cloudflare includes every affected index row, and `SqlStorageCursor` can
increase `rowsWritten` while a `RETURNING` cursor is consumed. Orez therefore
meters monotonic cursor deltas through `toArray()`, `one()`, `next()`, normal
iteration, and `raw()` iteration. The 150k default leaves headroom below soot's
200k/5min external alert. Invalid/non-positive numeric env values fall back to
the defaults.

When `windowRows > budget`, the circuit becomes sticky and mutating endpoints
return HTTP 429:

```json
{ "error": "writeBudgetExceeded", "windowRows": 150001, "budget": 150000 }
```

The rolling counter uses conservative one-second buckets, bounding memory to
roughly one sample per second even for single-row hot loops. The trip emits one
structured `orez_do_write_budget_tripped` line. Further
rejected requests do not log. Only the sticky trip timestamp is persisted in
DO storage; the hot-path rolling counter is in the TypeScript worker layer.
This deliberately avoids updating a meter table for every application write,
which would add billed writes and amplify an incident. A busy runaway keeps the
isolate resident, while persisted sticky state prevents a quiet/eviction cycle
from reopening it.

Monitor and recovery routes (the outer worker forwards them to the singleton):

- `GET /_orez/write-budget` returns `windowRows`/`billableRows`, observed
  application `logicalRows`, `budget`, `windowMs`, window bounds, `tripped`,
  and `trippedAt`. It performs no SQL mutation.
- `POST /_orez/write-budget/reopen` clears the sticky trip and rolling samples.
  Supply `x-orez-admin-token: <token>` or `Authorization: Bearer <token>`
  matching `OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN`.

The existing `src/worker/zero-sql-write-circuit.ts` remains the embedded
ZeroSqlDO defense used by downstream worker shims. The raw CF data worker now
has an independent source-side budget instead of relying on downstream setup.

## Sync host ingest circuit

`packages/sync-cf-host/src/host.ts` uses one breaker for two signatures:

1. More than `ingestBudgetRows` billable SQLite rows in the rolling
   `ingestBudgetWindowMs` interval.
2. A non-empty `/changes` response or snapshot application that leaves the
   engine's upstream cursor unchanged. This catches repeated partial
   boot/replay pages even when a single page is individually valid.

`SyncHostConfig.upstream` knobs and defaults:

- `ingestBudgetRows: 150000`
- `ingestBudgetWindowMs: 300000`
- `ingestBackoffMs: 1000`
- `ingestMaxBackoffMs: 60000`

The host's SQL adapter drains the same final `rowsWritten` counter during WASM
engine application. Its status exposes both applied `logicalRows` and exact
`billableRows`. The breaker returns a structured 429 with its reason
(`ingestBudgetExceeded` or `ingestCursorStalled`), rolling rows, budget, and
`retryAfterMs`. Repeated trips use capped exponential cooldown. Alarms are
scheduled no sooner than the larger of the normal ingest interval and breaker
cooldown. A complete successful ingest resets consecutive-trip backoff; rolling
row samples still age normally.

Only cooldown reason/deadline/attempt state is persisted in the existing host
control table, and only on trip/recovery transitions. This preserves backoff
across eviction without turning metering into a per-page write amplifier.

`GET /admin/status` includes `ingestBreaker`. Authenticated
`GET /admin/ingest-breaker` returns just that state, and
`POST /admin/ingest-breaker` clears it. These routes use the sync worker's
existing admin authorization.

Authenticated `GET /admin/upstream-write-budget` proxies the source ZeroDO's
cheap status route through the configured private DATA binding. The binding's
service entrypoint must permit `/<namespace>/_orez/write-budget` alongside
`changes` and `snapshot`; the route remains unreachable from the public DATA
worker unless the consumer explicitly exposes it.

## Delegated push bounds

Delegated app pushes retry only transport failures, HTTP 429, and HTTP 5xx.
HTTP 4xx responses other than 429 are returned immediately. Defaults:

- `delegatedPushRetry.maxAttempts: 3` (including the first request)
- `initialBackoffMs: 100`
- `maxBackoffMs: 1000`
- `timeoutMs: 5000` per service-binding attempt

Every retry emits `sync_delegated_push_retry` with attempt, cap, status, delay,
and transport error. The attempt count and delay are both bounded, so a failing
application endpoint cannot create an internal hot loop.

## Semantics adopted from soot's proven monitoring

Read-only review of `~/soot/scripts/ops/cf-do-write-monitor.ts` and
`~/soot/docs/cloudflare-do-deploy.md` established these useful semantics:

- compare a five-minute bucket against a known legitimate 70–100k peak;
- keep trips sticky and reads open;
- provide an admin-token recovery path;
- emit one transition log, not one log per rejected write;
- prefer a non-destructive circuit for data workers (never force-delete the
  worker holding the only DO data);
- retain external rate/cost monitoring as a delayed backstop.

Soot's external alert is 200k/5min, while its separate embedded circuit uses
200k/min sustained for three minutes plus a 1M/min hard stop. Orez adopts the
incident investigator's 479,550 billable rows for 10k control pushes exposed a
0.4.53 bug: the initial implementation sampled `rowsWritten` before lazy
`RETURNING` cursor consumption. The corrected default is 150k billable rows per
five minutes for both ZeroDO and host ingest, below the external alert. Logical
application/applied rows remain a separate observability counter.

## Tests

- `src/do-sql-tracking.test.ts` injects a deterministic clock and proves rolling
  expiry, exact-boundary behavior, sticky trip, restore, reopen, and SQL
  classification.
- `packages/sync-cf-host/write-safeguards.test.mjs` proves rolling ingest limits,
  shared cursor-stall cooldown, recovery, and bounded retry math.
- `packages/sync-cf-host/ingest-test.mjs` drives a real worker harness whose feed
  repeats a non-advancing change, proves the 429 trip, removes the bad feed,
  reopens through the admin route, and proves recovery. It also proves delegated
  push success after two transient failures and exactly three attempts for a
  persistently failing endpoint.
