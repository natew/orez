# Prod monitor — 2026-07-11 (orez rust sync engine on sootbean.com)

Monitor agent: log-monitor for coordinator `ab-mreeh1ah-69466`. Wave driver:
`ab-mrg82jbd-98468`. All times UTC.

Tails (read-only `wrangler tail --format json`, account
`aa20b480cc813f2131bc005e2b7fd140`), output under `/tmp/prod-wave-logs/`:

| role | worker                        | note                                                            |
| ---- | ----------------------------- | --------------------------------------------------------------- |
| app  | `soot-cf-demo`                | deployed name is `soot-cf-demo`, not `soot`                     |
| data | `soot-cf-orez-data-demo`      | ZeroSqlDO per namespace; service-binding-only (no public route) |
| host | `soot-rust-sync-host-prod-v2` | public at `soot-rust-sync-host-prod-v2.natewienert.workers.dev` |

## Disposition: NO WAVES RUN

The wave driver ran **zero** waves and is holding them. Prod was already in an
active incident when tails came up, so there is no clean baseline and no wave
load was applied. **Every observation below is pre-existing incident state, not
wave traffic.** Recovery is a gentle keepalive drain owned by the wave driver;
this monitor is read-only and does not reopen the budget.

## Headline finding — ~12,000x write amplification tripped the soot-ns budget

At **10:31:30 UTC** the `soot` namespace ZeroSqlDO write-budget circuit tripped:

```json
{
  "event": "orez_do_write_budget_tripped",
  "windowRows": 301642,
  "billableRows": 301642,
  "logicalRows": 25,
  "budget": 300000,
  "windowMs": 300000,
  "trippedAt": 1783765890883
}
```

301,642 billable SQLite rows written for **25** logical application rows =
~12,000x amplification. This is the 2026-07-10 partial-boot / re-stream
signature (a reused ZeroCacheDO repeatedly doing partial replica boot/re-stream,
each pass rewriting index rows). Prod budget override is **300k/5min** (not the
150k doc default). The new rust engine surfaced its own showstopper with no wave
needed.

### The circuit is holding — single trip, not actively re-burning

Across a 348-second capture window there is exactly **one** distinct `trippedAt`
(`1783765890883`) and **one** trip log line. The circuit is sticky and is now
**rejecting** further mutating writes rather than re-burning. So the
amplification is a past event; the safeguard is doing its job (writes stopped,
reads meant to stay open). The live 429s are the sticky rejections, not new
burn.

## Collateral: sticky trip is breaking reads too

The sticky circuit refuses the lazy `CREATE TABLE IF NOT EXISTS`, so queries
that touch a not-yet-created table now 500 instead of degrading gracefully:

```
[exec-500] no such table: file: SQLITE_ERROR :: SQL=SELECT 1 FROM file WHERE "projectId" = ? LIMIT 1
```

`POST /__soot_pg` + `POST /__soot_query` 500s land on the `soot`/`singleton`
instance (7 + 7 in-window) and one `proj-default_anon-zdiisik7` (2). This means
the `soot` namespace is currently unusable for both writes (429) and any read
that hits an uncreated table (500), i.e. the namespace is effectively down until
reopened.

## Live symptom counts (first ~5.5 min of tailing, ~10:31–10:37 UTC)

Data worker `soot-cf-orez-data-demo` (844 events in window):

- outcomes: ok 475, canceled 19 (in first 90s sample); statuses incl. 429 and 500
- **429 writeBudgetExceeded**: ~51 on `/changes` + `/soot/changes` (sticky rejects)
- **500**: 16 on `/__soot_pg` + `/__soot_query` (`no such table: file`)
- write-budget trips: 1 (see above)

Host worker `soot-rust-sync-host-prod-v2`:

- **500**: 12 on `POST /soot/pull` (public workers.dev URL)
- 7 `responseStreamDisconnected`
- **ingest breaker: NOT tripped** (0 `ingestBudgetExceeded`/`ingestCursorStalled`
  events). Only the source ZeroDO budget tripped; the host breaker never fired.

App worker `soot-cf-demo`: clean in window (outcomes all ok; 200/204/302 only).

## Timeline

- 10:31:04 UTC — tails begin capturing (data worker events already flowing)
- 10:31:30 UTC — `soot` ns write budget trips at 301,642 billable / 25 logical
- 10:31:30+ — sticky 429 on `/changes` + `/soot/changes`; 500 `no such table:
file` on pg-wire/query; host `/soot/pull` 500s
- 10:36:51 UTC — end of first capture window; still a single trip, no re-burn

## Recovery watch (in progress)

Wave driver drains gently (GET /keepalive off-peak) per
`plans/incident-2026-07-07-do-rows-written-burn.md`; explicitly NOT reopen +
restress, NOT export/import. Monitor is timestamping:

- billable-counter decay (window rolls off at 5 min; but trip is **sticky** and
  persisted — see risk note)
- 429s on `/changes` clearing
- first green `POST /soot/pull` on the host
- **ALERT trigger**: any re-stream re-burst approaching the 2026-07-08 profile
  (~540k rows / 5 min) — page coordinator + wave driver immediately

### Risk note for recovery

The write-budget trip is **sticky and persisted in DO storage**; the rolling
5-minute window rolling off does **not** auto-clear it. Per the safeguard doc,
only `POST /_orez/write-budget/reopen` (admin token) clears the sticky trip.
Keepalive pings keep the isolate warm but do not by themselves clear a sticky
trip, so the 429/500s on `soot` will persist until an explicit reopen. That
reopen is the exact action the wave driver wants to avoid before RCA, because
reopening without fixing the amplifier risks an immediate re-burn. Net: the
`soot` namespace likely stays degraded until the amplification is root-caused on
a CF preview / local 2-worker split and a reopen is done deliberately. Flagged
to coordinator; monitor is not reopening.

## Reopen mechanics (documented, not executed)

If/when a deliberate reopen is authorized: token
`OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN` = value of `BETTER_AUTH_SECRET` in soot
`.env.production` (deploy maps them 1:1 in `scripts/ops/cf-demo/deploy-cf.ts`
`CF_DATA_SECRET_ENV`). Route: `POST /soot/_orez/write-budget/reopen` on the data
worker, reachable only via a bound worker (data worker has no public route); the
data shim in `src/deploy/cloudflareDoDeploy.ts` routes
`/<ns>/_orez/write-budget/reopen` → ZeroSqlDO. Status (read-only, no mutation):
`GET /soot/_orez/write-budget`.
