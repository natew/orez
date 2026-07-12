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

## Update ~10:53 UTC — soot symptom self-cleared (on old code, no reopen)

soot-ns `/changes` flipped from 429 → steady **200** around 10:40–10:48 UTC. By
10:53, the last 90s showed 48 soot `/changes` all 200, zero 429, zero 5xx, and no
re-trip in 20+ min. This happened with the data worker **still on scriptVersion
`46dbfea4`** (no redeploy) and **no reopen log line**.

Reading: the write-**attempts** ceased, not a reopen. The initial re-stream burst
that wrote 301k rows got rejected repeatedly; once it stopped attempting the large
write, `/changes` returns cheap incremental 200s that write nothing, so they no
longer hit the budget. Reads stay open while tripped, so the **sticky trip flag is
very likely still set** — the tail cannot show the flag; only
`GET /soot/_orez/write-budget` (`tripped`) confirms it. Consequence: polling is now
cheap, so the soot ZeroSqlDO can idle-evict once the host stops polling it.

## Recovery plan (superseded twice) → now: engine-fix-gated

No keepalive drain. The trip is persisted and a drain would keep the stale-code
isolate resident, blocking the idle-evict needed to boot fixed code. Coordinator's
sequence: land orez engine fix → release `--into` the deploy tree → redeploy data
worker (+ host if the fix touches the WASM engine) → **delete host** briefly to stop
`/changes` polling so the soot ZeroSqlDO idle-evicts → one admin reopen → minimal
probe write to confirm ~1:1 billable:logical (not 12,000×). Engine RCA (per driver
runbook): the tracked transaction copies all of `_zero_changes` per push and
watermark discovery treats the rollback snapshot as a live sequence → triangular
growth; fix measured −94% to −96%.

## Monitor arming for recovery execution

- Tails switched to **supervised auto-reconnecting** wrappers
  (`supervise-tail.sh`, append mode) so they survive the data redeploy and the
  host delete→recreate without a blind gap. Supervisor pids in `super-pids.txt`.
- Watcher (`watch.py`, recovery phase) exits + re-invokes the monitor on the first
  of: RE_BURST (new `trippedAt` → instant page), DATA_DEPLOY (data scriptVersion ≠
  `46dbfea4` = fixed code live; ping before reopen), HOST_DEPLOY, HOST_SILENT (host
  events → 0 = deletion; confirm `/changes` stops), SOOT_CHANGES_STOP, and
  DATA_TAIL_STALL (blind-guard). Baselines: data `46dbfea4-8d78-4ce6-91bb-963ee7e9a8f9`,
  host `2aa41010-eea9-486e-90ad-38b00d196f30`.
- Probe billable:logical: captured from the data tail; authoritative `tripped` +
  `billableRows`/`logicalRows` via `GET /admin/upstream-write-budget` on the host
  (gated by prod host `ADMIN_KEY`) — read by the driver/coordinator during
  reopen+probe, not by this read-only monitor unilaterally. The reopen itself is
  independently confirmable from the data tail: the ZeroSqlDO logs
  `orez_do_write_budget_reopened` (`reopenedAt`) — watcher keys on it.

## Recovery execution timeline (live)

- ~10:58:59 / 10:59:08 UTC — **data worker redeployed** twice for 0.4.61
  (scriptVersion `f14c6fe3` then `413cb780`). `413cb780` is current/latest;
  old `46dbfea4` drained out. soot `/changes` stayed clean 200 throughout.
- 11:01:46–11:02:31 UTC — **host `soot-rust-sync-host-prod-v2` deleted** (driver,
  coordinator GO). Confirmed: host inbound events → 0; soot `/changes` dropped
  ~85% (≈8/15s → ≈1–2/15s). Polling stopped so the soot ZeroSqlDO can idle-evict.
  Planned all-namespace sync gap begins; tails supervised so they reconnect when
  the host is redeployed.
- ~11:10 UTC — after the idle-evict, the driver read the soot budget status on
  fixed `413cb780`: **`tripped: FALSE`, decayed to 0 rows**. The DO came up
  **un-tripped** on the fresh boot, so **no reopen was needed**. Observed ratio
  ~6:1 (a `12:2` billable:logical window before roll-off), not 12,000×.
- 11:12:15–11:12:58 UTC — driver ran a deliberate **prod-login probe**. Data-tail
  confirmation: `POST /__soot_pg` and `POST /__soot_query` back to **200** (were
  500 `no such table: file` during the incident — collateral resolved as the DO
  booted fresh schema), `/changes` 200, budget-status GET 200, **no new trip**,
  **zero exceptions / 5xx** in the 90s window (only benign `canceled` queries).
- Outcome: **recovery successful on the soot ns.** The fix reduced amplification
  from ~12,000× to ~6:1, the namespace serves writes+reads cleanly, no re-burst.

### Open follow-ups (flagged, not monitor-actionable)

- The data worker was deployed with a **dev `BETTER_AUTH_SECRET` (len 36)** vs the
  prod len-64 value (driver flagged to coordinator). That secret is also
  `OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN` and the auth secret, so it should be
  corrected.
- Worth explaining **why the DO came up un-tripped** after the evict, given the
  safeguard doc says the sticky `trippedAt` is persisted in DO storage: either the
  fixed code no longer restores/keeps the sticky flag, or the persisted flag did
  not survive the fresh boot. Not resolved here.

### Why the DO came up un-tripped (coordinator's answer)

The sticky flag is written via `ctx.waitUntil(storage.put(...))` at the instant the
budget error **throws** (`orez src/cf-do/worker.ts:196`). The `put` is a buffered
write inside a request that ends in an uncaught throw, so the DO reset discards it —
stickiness is unreliable by construction. Queued as a round-2 engine fix (await the
persist before throwing). Secret defect already corrected (prod len-64 values re-put;
uploads confirmed).

### Host redeploy + close-out (~11:15–11:22 UTC)

- ~11:14:46 UTC — data worker redeployed again for the secret fix (scriptVersion
  `bc11e838`, now current). Verified healthy: pg/query/changes all 200, 0 trips,
  0 exceptions.
- Host `soot-rust-sync-host-prod-v2` redeployed. Driver cited `f05253d6` + root
  GET 200 + reopen POST `{ok:true,tripped:false}` with the re-put prod len-64 token
  (was 403 on the dev secret). **My host tail sees scriptVersion `2d6477c0`** —
  reconciliation flagged to the driver (deployment-id vs version-id, or a second
  redeploy).
- Host tail required manual recovery: the wrangler-tail session zombied on the
  deleted worker (single connect marker, never exited), then left stale children
  after recreation. Cycled the children so the supervisor attached a fresh session
  to the live host (jsonl fresh again 11:19:28 UTC). **Lesson: `wrangler tail`
  does not reliably follow a worker across delete→recreate; a supervised tail must
  be force-cycled when the target is recreated.**
- **Confirmation (2) /changes polling resumed — CONFIRMED.** soot `/changes` on
  the data tail climbed out of the ~1–2/15s trough to steady 5–7/25s (host
  ingesting again; its DO alarms fire on schedule).
- **Confirmation (1) `POST /soot/pull` → 200 — not yet observable.** No organic
  client pull in the window (host tail shows only DO alarm firings, outcome ok);
  the driver's root-GET-200 landed before the tail reconnected. Watcher now armed
  to report the first `/soot/pull` (any status) the instant it appears.

## Wave 1 (real, interp C — playwright drives prod product path)

- First attempt **aborted**: the `debug:bossbean` driver crashed at its local pg
  observer (`127.0.0.1:7432` ECONNREFUSED) right after a successful prod login,
  before creating a project. No sync load reached prod. Superseded by interp C
  (headless playwright against sootbean.com: login → create project → factory tab
  → "Build Pace Mobile" prompt → prod factory runs agents server-side ~25 min).
- Division of labor: driver self-polls `GET /soot/admin/upstream-write-budget`
  (host `ADMIN_KEY`) for exact `billableRows`; monitor runs the namespace-agnostic
  trip/429-surge pager + confirms `/soot/pull`.
- **Real wave 1 (~11:33 UTC): STALLED at project-create — no factory load ran.**
  The playwright harness logged in but `project|insert` failed on a **new engine
  bug**: `Mutator 'project|insert' → 'Cannot compare values of different types:
string and number'` in poke processing (i64/string wire-format class, client +
  server). The harness then timed out on the create-redirect and exited. No
  `proj_<id>` factory namespace was ever created; the factory never started.
- **Traffic attribution (corrected).** The ~338 data-events/60s + `/soot/pull`
  burst + `/changes` 40/window at 11:36–11:38 were the harness shell + default
  project (`proj-default_Owwi…`) Zero-client pulling/retrying after the failed
  mutation, **plus organic anon prod visitors** (`proj-default_anon-*`) — **not**
  factory-agent load. Do not count it as wave throughput.
- **Server-tail localization of the new bug** (create window 11:33–11:38): the
  `project|insert` push **succeeded at every HTTP layer** — app `/api/zero/push`
  200 (4×), host `/<ns>/push` 200 (8×), data `/__soot_pg`+`/__soot_query` 200
  (217× each), `/__soot_migrate` 200 (22×). The `Cannot compare…` error appears in
  **none** of the three tails (no console log, no `exceptions` entry, no HTTP
  4xx/5xx) and there were **zero exceptions** on any worker in-window. So the write
  path is healthy and the failure is in **poke/diff processing below the HTTP
  surface** (the i64-vs-string compare when applying the poke), not the write path.
  A different bug class from the amplification.
- **Confirmations that stand (real recovery proof, even without factory load):**
  `POST /soot/pull` → **200** under load (burst at 11:36:20 UTC, 14+ in 60s, no
  500s — the incident's `/soot/pull` 500s are fully resolved), and **zero trips /
  zero 429 / zero amplification** across all namespaces — the 0.4.61 amplifier fix
  holds under real product load.
- Product observation (minor, not amplification): each brand-new anon namespace
  (`proj-default_anon-*`, organic visitors) throws 2× HTTP 500 `no such table:
file` on `POST /__soot_pg` + `/__soot_query` at creation — a `SELECT 1 FROM file
WHERE projectId=?` racing ahead of schema provisioning on the fresh DO.
  Self-resolves after migration; distinct from the incident's sticky-trip cascade.
  Worth an orez ticket.

## Round-2 attempts + where the real fix lives

- Host redeployed to `da5e3874` (0.4.62) at 11:49:28 UTC — **no-op for the poke
  bug** (coordinator + driver confirmed). Data unchanged (`bc11e838`).
- Create-repro at 11:53 UTC still failed (`create_ok=false, compareError=true`).
  Data-tail corroboration: the new namespace `proj-proj_mrgb24n4_0ewn98` appeared
  with exactly **1 event, 0s span, then silent** — the write landed (namespace
  created) but no sustained sync followed = poke broke / stall. Write-healthy,
  poke-broken, consistent throughout.
- **The real fix is app/soot-side**: `toZeroValue` in `httpPull.server.ts`,
  shipped by a **`soot-cf-demo` app-worker deploy** (not host WASM, not data).
  Monitor now watches for the app-worker scriptVersion bump off baseline
  `1ef662d1` (APP_DEPLOY signal) and will confirm on the driver's next repro when
  `create_ok` flips true.

## Verdict

The new orez rust sync engine's write-budget safeguard **worked**: it caught a
~12,000× amplification at 301,642 billable rows and held (single sticky trip, no
re-burn), turning a potential 2026-07-08-style multi-hundred-k burn into a
contained, reads-degraded incident. Recovery was clean — engine fix (0.4.61)
redeploy → idle-evict → un-tripped fresh boot → probe verified ~6:1 (7 billable /
0 logical), collateral 500s resolved.

What is **confirmed**: the 0.4.61 amplifier fix holds under real prod load — zero
trips, zero 429, zero amplification across all namespaces during real product/sync
traffic (organic anon visitors + the wave harness's shell/default-project
Zero-client retries). The host `/soot/pull` path is healthy under that load (200
burst, no 500s — the incident's `/soot/pull` 500s are gone).

What is **not yet tested**: actual **factory-agent write load**. Wave 1 stalled at
`project|insert` before the factory started (new engine bug — `Cannot compare
values of different types: string and number` in poke/diff processing, i64/string
wire format; write path healthy, poke path broken — a different class from the
amplification). So the factory-load stress remains pending the round-2 fix + wave
retry; do not claim the amplifier fix is proven under factory load until then.

Engine follow-ups surfaced: (a) sticky-flag persistence is unreliable (buffered
`put` in a throwing request) — round-2 fix queued (already on orez main);
(b) a dev `BETTER_AUTH_SECRET` shipped to the data worker — corrected; (c) the new
`project|insert` string/number poke bug — RCA/fix in flight (engine agent
`ab-mrgal2dq-98705`); (d) minor: fresh anon namespaces 500 on `no such table:
file` (pg-wire query races schema provisioning) — orez ticket candidate.

No factory waves ran; prod surfaced and recovered from its own showstopper, and the
one attempted product-path wave found a second, unrelated engine bug at create.

## Wave 1 retry — 2026-07-11 ~13:50 UTC (monitor ab-mreeh1ah-adopted)

Monitor handed off from ab-mrg82jhn-98583 to this session; all 3 supervised tails
adopted live (supervisors 78099/78100/78101, wrangler children 78111/78112/63868).

- **New engine live:** host `soot-rust-sync-host-prod-v2` rolled `da5e3874`
  (0.4.62) → **`488840dc` (orez 0.4.63, timestamp-coercion fix)** at **13:46:03 UTC**.
  Fresh durableObjectId isolates booted; rollout converged (only 488840dc in the
  last 90s; transient `3997c2cf` during propagation, gone). App worker unchanged
  at baseline `1ef662d1` — the poke/`Cannot compare` fix shipped in the orez engine
  (timestamps now JSON numbers), not app-side `toZeroValue`.
- **Gate PASS** (driver ab-mrgei5ef-41125): create_ok=true, single gate create did
  not trip; authoritative budget read `tripped=false billable=21482/300000 (~7%)
logical=67 (~320:1) writerEnabled=true`, window resets ~13:51:39 UTC.
- **Wave 1 launched ~13:50 UTC:** real product path on sootbean.com — login e2e-\* →
  create project → factory prompt (Pace Mobile running tracker) → built-app-or-stall
  (~20 min). Single e2e account, one project namespace, factory server-side. Driver
  snapshots budget every 30s and STOPS on any trip.
- **Monitor arming:** tail-based danger scanner `wave-danger.py` (pid in
  `wave-danger.pid`) watches all three tails from wave-launch EOF and pages the
  coordinator `--urgent` on write-budget trip / ≥5 429s-per-60s / ≥5 non-anon
  5xx-per-60s. Benign fresh-anon `no such table: file` 500s tracked separately, not
  paged. Coordinator declined to share ADMIN_KEY over the bus (correct); tail
  detection + driver phase snapshots cover paging. Heartbeat in
  `wave-danger.heartbeat`.
- **Pre-load baseline (since 13:46 boot):** ZERO trips / 429 / 5xx / exceptions
  across data, host, and app. Data tail connection-alive but idle (no data traffic
  since 13:15 UTC) — the wave's proj namespace will be the first data-worker load.

### Reliability finding — silent data-tail blind (13:15–14:01 UTC)

The `soot-cf-orez-data-demo` wrangler tail went **silently blind**: its jsonl froze
at 13:15 (file size unchanged) while the driver's authoritative budget read proved
~12k billable rows were written to that worker's ZeroSqlDO in-window. wrangler's
tail WebSocket dropped on the long-idle worker, but the `wrangler tail` **process
stayed alive**, so `supervise-tail.sh` (which only reconnects when the child exits)
never saw a disconnect and never reconnected. The supervisor's liveness model —
"child process alive == tail healthy" — is wrong for this failure mode.

Fixed by cycling the wrangler child (`kill` the tail process); the supervisor
reconnected in 3s (new child, file appending again). No trip occurred during the
blind window (billable stayed 12–21k, far under 300k), and the driver's 30s budget
poll was independent coverage throughout.

**Supervisor improvement to make durable:** add a per-tail freshness heartbeat —
if a tail emits no lines for N seconds _while the worker is known to be receiving
traffic_ (or unconditionally past a max-idle), proactively recycle the child rather
than trusting process liveness. Idle workers are the ones whose tail sockets get
reaped, so the longest-quiet tail is the most likely to be blind exactly when load
finally arrives.

### Wave-1 attempts 1 & 2 (pre-factory)

- **Attempt 1** (proj_mrgfbxmw_y751df): project created (no trip), but the driver's
  harness boss-composer selector matched a hidden instance and timed out **before**
  sending the factory prompt. Zero factory load. Not a finding.
- **Attempt 2** (proj_mrgfkhtj_hw5cvv): re-driven via the sanctioned
  `window.SootBean.bridges.harness.ui.sendBossMessage` bridge. Create wrote through;
  4 transient host `/push` 500s at 13:59:08–11 (fresh-namespace schema race, empty
  logs, self-resolved to 200) then steady pull 200s. Still pre-factory-load at
  handoff of this note.

### Host push-500 RCA (localization) + unified provisioning-race story

**Symptom (driver + monitor):** first push(es) to a brand-new project namespace on
the HOST DO return HTTP 500, intermittently — `SootSyncDurableObjectV2` 488840dc,
`exceptions:[] logs:[] outcome:ok` (a deliberate/caught 500, no crash). It blocks
the boss composer from initializing (`bossInputCount=0`), so the factory prompt
can't be sent. Intermittent: attempt-1 (proj_mrgfbxmw) had zero push-500 and
mounted the factory shell; attempt-1b (proj_mrgfkhtj) hit it.

**Localization (`packages/sync-cf-host/src/host.ts` `#push`):** two push paths
exist. Soot prod uses the **delegated** path (`config.mutateUrl` → data worker,
lines 1029-1108). Its catch (1104-1107) returns `json(errorBody, statusOf)` with
**no `#log`, no re-throw**; `statusOf()` defaults to 500 for a non-HTTP error — an
exact match for the empty-logs/empty-exceptions signature. The **local** path's
catch (1266+) _does_ `#log(resultClass:'error')` and bumps `invariantFailures`, so
the missing log rules it out. Likely throw inside the delegated catch: line
1064-1065 `if(!Array.isArray(acknowledged)) throw 'delegated push returned no
mutation results'` (data returns 200 but a body without `pushResponse.mutations`
while its ZeroSqlDO provisions), or `#fetchDelegatedPush` (690) throwing after
`delegateMaxAttempts` during data cold-start.

**Unified root cause with the data 5xx:** on attempt-1c the data worker shows the
fresh-namespace provisioning race live — `no such table: file: SELECT 1 FROM file
WHERE "projectId"=? LIMIT 1` 500s on `/__soot_query`+`/__soot_pg` at 14:07:13-31,
self-resolving post-migration. Same class: first writes/queries race schema + DO
provisioning on a brand-new project. The host push-500 is very likely the
push-path face of the same race. Engine fix direction: make the first push/query
on a fresh namespace provision-then-retry rather than 500. Data tail now live, so a
repeat on 1c can be captured to confirm delegated-push-body vs data-500-passthrough.

### Wave 1d + scanner hardening (~14:16-14:19 UTC)

- **Delegated-push target is APP, not data.** soot host config:
  `mutateUrl='/api/zero/push?schema=soot_0&appID=soot'`, `mutateBinding='APP'`. So
  the host `#push` delegated path fetches the **APP worker** (`soot-cf-demo`)
  `/api/zero/push`, and the `!Array.isArray(acknowledged)` throw is on APP's
  response body — not the data worker. A host push-500 correlates to APP
  `/api/zero/push`: APP 200 ⇒ the array-less-body throw; APP 5xx ⇒ passthrough.
  Correlator: `/tmp/prod-wave-logs/push500-correlate.py` → `push500.findings.log`.
- **Scanner false-page fixed.** The 5xx-storm pager was counting the fresh-ns
  provisioning race (query-face `/__soot_query` 500s don't self-carry the
  `no such table` text — it rides the paired `/__soot_pg`/`/batch` event), so 5 of
  them tripped an --urgent page with no real trip. Classifier is now race-aware:
  `RACE_ROUTES = __soot_pg/__soot_query/__soot_migrate/batch/push` and any
  `no such table` blob go to a non-paging `5xx_race` bucket. Trip + 429 paging
  unchanged — those remain the real danger channel.
- **Data worker → 0.4.64 (cb6b7857)** with the no-such-table provisioning retry;
  tail captured the new isolate cleanly across the redeploy (no blind gap). Host
  0.4.64 (push barrier, 299d0d7) held until wave-1 terminal to avoid restarting
  host DOs under the live factory build.

### Wave 1 terminal outcome (~14:28 UTC) — factory load exercised, engine held

**HEADLINE (the thing the whole round was gated on): real factory-agent write load
was finally exercised and the write-budget engine HELD.** The boss-planning +
4-bean + 4-task creation phase drove the upstream write budget to a **peak of
212k/300k (71%, ~700:1 amplification)**, which then **receded** as the 5-min
rolling window aged out — **NO trip, NO 429, no amplification runaway**. This
closes the prior monitor's open item ("factory-agent stress remains untested"):
the 0.4.61 amplifier fix + 0.4.63 timestamp fix hold under genuine factory load.

**Build outcome: STALL after bean-assignment (confounded).** readState on
proj_mrgg5igp: `parked=true`, all 4 beans (Onyx/Sage/Rivet/Cinder) `status=idle
currentTaskId="" tasks=[]`, writes flatlined to 8/300k for ~7 min. The factory
reached boss-planning + bean/task creation, then parked; beans never executed, no
app built. **Confound (driver was upfront):** the driver tab was swapped at
14:20:51 right after bean assignment, and `headlessRuntimeEnabled=false` means the
browser tab _is_ the factory driver — so the swap may have caused the park. But a
stable tab was then present ~7 min without un-parking, and `tasks=[]` despite boss
chat claiming 4 in-progress — so a genuine stall (tasks not persisting) can't be
ruled out. Driver is re-running with a single stable tab, readState-based watch,
for an uncontaminated built-app-or-stall. **This is a factory/harness question, not
a sync/engine failure** — all sync layers stayed clean throughout.

### Clean re-run push-500 = real two-bug cascade (14:32 UTC, proj_mrggrc0y)

The push-500 correlator (host `/push` 5xx → paired APP `/api/zero/push`) fired on
the clean single-tab re-run and captured the actual cause. Host push-500 at
14:32:26 with **all 9 paired APP pushes = 200**, confirming the host
`!Array.isArray(acknowledged)` body-shape throw (hypothesis (a)), not a
passthrough. The APP console logs show a two-bug cascade — distinct from the
fresh-ns provisioning race and **not** covered by 299d0d7:

1. **PermissionError on the factory's own inserts.** `[permission] Not Allowed:
thread with auth id cLX8Hw0h…` on `thread|insert#1` (id
   `proj_mrggrc0y_qy8v83-icon-designer`) and `message with auth id …` on
   `message|insert#1` (`threadId=thread_mainbean_proj_mrggrc0y`). The beans'
   thread/message inserts are rejected by `runServerPermissionCheck` /
   `ensurePermission`. **Hypothesis: this is the genuine stall cause** — beans that
   can't persist threads/messages/tasks park with `tasks=[]`, independent of the
   driver's tab swap.
2. **UNIQUE-constraint 500 in the error-record path.** After the permission error,
   the retry-without-mutator path calls `writeMutationResult` →
   `INSERT INTO soot_0_mutations (clientGroupID, clientID, mutationID, result)` and
   hits `UNIQUE constraint failed … SQLITE_CONSTRAINT_PRIMARYKEY` (that mutation id
   is already recorded) → `[zero] push failed kind=PushFailed reason=database`. The
   PushFailed body carries no mutations array → host throws → the 500.

Also observed: `alreadyProcessed` replays (`sootSession|createWithWorktree#1`
expected 2 got 1; `thread|insert#1`) — client resending mutation id 1. Host push
recovered partially (push 200×6 / 500×2, pulls 200). No trip, budget clean.

Engine/app follow-ups: (a) why the factory bean `thread|insert`/`message|insert`
fail server permission for the project owner's auth id; (b) `writeMutationResult`
idempotency — recording a result for an already-present
(clientGroupID, clientID, mutationID) should upsert/ignore, not 500 the push.
Full evidence: `/tmp/prod-wave-logs/push500.findings.log`.

**CONFIRMED (driver): the permission denial is a REAL bug.** `cLX8Hw0h...` is the
project OWNER's userId AND the 1e logged-in USER_ID — so the factory's server
inserts run AS the owner, with the logged-in user == owner == the denied auth id,
and are STILL permission-denied on the owner's own `thread|insert`/`message|insert`.
Not an agent-wrong-identity issue and not the driver's tab-swap confound. This is
the genuine stall cause: beans can't persist their threads/messages/tasks →
`tasks=[]`, beans park, no app built. Residual (for a real-login repro to settle,
not more static analysis): whether `/api/test-login` sets a session/auth CONTEXT
claim that the on-zero permission rule evaluates differently than a real login. The
permission logic is in the on-zero mutator/permission layer, not soot's
`src/zero`/`src/database` schema. Driver is relaying the full cascade to the
coordinator. Sync/engine layers stayed clean throughout (no trip, budget clean).

## Wave 2 (post-fix, ~15:32-15:40 UTC) — mirror fix worked for main project; STALL moved one level deeper to per-bean branch sessions

Fix deploys: APP 3ba77d5c (mirror-ordering barrier), host 68de9353 (0.4.65+secrets),
data cb6b7857 (0.4.64). Gate verified served ENTIRELY by 3ba77d5c (gate-version
correlator: all 6 gate-ns pushes = 3ba77d5c, first push project|insert≈ = 3ba77d5c,
zero 1ef662d1) — so the result counts.

**The mirror-ordering fix WORKED for the create/main-project path:** create burst
clean, grant committed, timestamps numeric, `push500=0`, `permDenied=0`, and
`sootAgent|insert` SUCCEEDED server-side (no error lines). The wave-1 owner-denied
cascade and the UNIQUE-constraint 500 (bug #2) did NOT recur.

**But the build still STALLED — the denial moved one level deeper.** After the boss
spawned 4 beans, `sootTask|insert` was permission-denied 4× (mutations
#133/136/139/142 at 15:37:41-46, client 4t41t4r1a94tqjc9su): structured app-level
`ApplicationError [permission] Not Allowed: sootTask with auth id Z3Yb…` via
`linkedProjectAccessCondition`, returned **http 200 with mutation result
error='app'** (clean rejection, NOT a 500 — no cascade). Beans then gave up →
tasks=0 → idle stall.

**Root cause (why sootAgent passes but sootTask fails on the same condition): the
linked entity differs.** Every denied `sootTask` routes through a **per-bean BRANCH
session** (`routeKey=sess_branch_mrgj2itl/2j9q/2jp3/2k17`, one per bean), while
create used `routeKey=main` and `sootAgent` links to the project directly.
`linkedProjectAccessCondition` resolves project access _through_ the branch
session's routeKey, and the per-bean branch sessions have **no access grant/mirror
seeded**. So 3ba77d5c fixed the main-project grant seeding, but the **branch-session
access path is a second, unpatched seeding gap** — same async-mirror bug class, one
level deeper. Same pattern in wave-1 proj_mrgg5igp (14:18 sootTask also
routeKey=sess_branch_*). Fix direction: seed the access grant/mirror for each
per-bean branch session (or have `linkedProjectAccessCondition` fall back to the
parent project's grant) before tasks insert. **Sync/engine clean throughout wave 2:
no trip, no 429, budget clean.**

### Wave 2 FINAL RCA (engine ab-mrgjbzn1-14233) — corrects the grant-gap hypothesis

The `sootTask` permission-denied is a **SYMPTOM, not a grant bug** — the grant IS
present (verified in the owner pull). Real chain: `sootTask.insert` on
`routeKey=sess_branch_*` fails the **ACTIVE-ROUTE gate** (`isTaskRouteActive`: no
active `sootSession` for that routeKey visible in-transaction) → the mutator's
`if (routeError) return` **skips the write** → on-zero's post-insert `ctx.can`
permission check **by primary key** then throws `PermissionError` because the row
was never written. `sootAgent` passes because it has no route gate. So the branch
session's routeKey being the distinguisher (my correlation) is correct, but the
mechanism is the active-route gate + a post-insert permission check on a skipped
write — **not** a missing branch-session grant. Round-3's grant barrier cannot fix
the task path; the fix is in the active-route gate / the skip-then-permission-check
ordering. ns proj_mrgiwzkh_9oq7z2 preserved server-side for the sootSession query.
Terminal wave-2 state: PARKED-STALL-NO-EXEC, no app built. Sync/engine clean
throughout (no trip, no 429, budget clean).

## Wave 3 (~16:25 UTC) — 3199e13c REGRESSED the create-path owner grant fix

Gate verified served entirely by 3199e13c (gate-version correlator: 4/4 gate-window
pushes = 3199e13c, first-push = 3199e13c, zero stale). But the built-app wave failed
immediately with a **regression**: the boss's OWN thread/message inserts are
persistently permission-denied (owner 8ImAiplL), so it can't write planning messages
→ agents=[] → no beans → no build.

**Tail ground truth (definitive, version is the only variable):**

- proj_mrgkscyt (wave 3, served ENTIRELY by 3199e13c, 834 app events, zero stale):
  thread denied 6×, **message denied 597×**, window 16:25:16→16:29:13 and still
  climbing (owner retry storm, not resolving over 4+ min → not an async-seed race).
- proj_mrgiwzkh (wave 2 gate, served entirely by 3ba77d5c, 732 events): thread
  denied 0, message denied 0 — CLEAN.

So `3ba77d5c`'s mirror-ordering barrier fixed the owner create-path thread/message
grant; **`3199e13c` (branch-session response barrier) regressed it** — the
wave-1-class owner-denied-on-own-inserts bug is back. accountResourceGrant mirror is
not surfaced in app push logs (grant_refs=0 on BOTH namespaces, including the working
one), so grant-log absence is not the signal; the behavioral diff (same owner-insert
path: denied on 3199e13c, allowed on 3ba77d5c) is. **Diagnosis: 3199e13c lost/
regressed the 3ba77d5c create-path barrier — likely branched off a pre-3ba77d5c base
or the two barriers conflict. Fix must be ADDITIVE: 3199e13c needs BOTH the
mirror-ordering create-path grant barrier AND the branch-session barrier.** No trip/
429 (app-permission denials, not budget writes); engine still clean. Driver aborting
wave 3.

### Wave 3 regression — CORRECTION + create-time runtime probe

My "3199e13c lost/regressed the 3ba77d5c create-path barrier" diagnosis was WRONG.
Coordinator's bundle-hash comparison is authoritative: 3199e13c's deployed bundle
contains the IDENTICAL response-barrier + provisionNamespace code as 3ba77d5c (same
content-hashed chunks). So the behavioral difference is NOT app-logic.

Create-time probe (proj_mrgkscyt, 16:24-16:26):

- **No provisioning errors:** zero app-tail lines mentioning provisionNamespace /
  zeroAsyncActions / async-task-failed / projectNamespaceAccessMirror / __soot_migrate,
  zero exceptions; data tail zero 5xx and zero __soot_migrate in-window. The barrier
  did not visibly error.
- **Create push was SLOW, not fast** (refutes "acked fast / backgrounded"): host
  wallTime for the wave-3 create pushes = 2184ms + 2627ms; the working wave-2
  (3ba77d5c) create = 605/720ms. So the barrier appears to have AWAITED ~2.2-2.6s.
  Caveat: some may be fresh-DO cold provisioning, but it is not a fast ack.
- **Denials are persistent ~4 min** (16:25:16→16:29:13, 597 message denials) — not a
  transient race that clears after the ~2.6s create.
  Implication: barrier awaited, no errors, yet owner thread/message denied for 4 min,
  on a bundle IDENTICAL to the working 3ba77d5c → points to ENVIRONMENT/PAIRING, not
  app logic. Prime suspects: (a) the post-deploy secret-put on 3199e13c feeding the
  permission/grant path a wrong/rotated secret; (b) the data/host version the
  3199e13c app was paired with vs 3ba77d5c's pairing. Engine clean throughout (no
  trip/429; denials aren't budget writes). Wave 3 aborted.

## Decisive run (~16:41-16:47 UTC) — rollback to 9ce1f207: grant regression CLEARED, branch-session stall persists

Coordinator rolled the app BACK to 9ce1f207 (3ba77d5c-equivalent) to isolate
version-linked vs data-tier reader-staleness. Gate ns proj_mrgldp5a_ghf2my.

**Second silent-blind-tail incident:** the APP tail (soot-cf-demo) went silently
blind 16:34:19→16:44 (socket dropped during the rollback redeploy, process alive,
supervisor didn't reconnect). It corrupted an early read — I wrongly warned the gate
was "likely 3199e13c → abort" off a stale aggregate. Cycled the app wrangler child →
reconnected. **Added a tail-freshness watchdog to the heartbeat** (flags any tail
jsonl silent >150s) so this can't silently corrupt signal again. Root fix still owed
upstream: supervise-tail.sh must not treat process-liveness as tail-liveness.

**Ground truth (post-reconnect, corrected):**

- proj_mrgldp5a served **100% by 9ce1f207** (74/74 host-push-correlated app pushes,
  zero 3199e13c) — the clean rolled-back test.
- **thread/message: CLEAN (0 denials) on 9ce1f207.** Contrast wave-3 on 3199e13c:
  597 message + 6 thread denials. ⇒ the thread/message **grant-path denial is
  VERSION-LINKED to 3199e13c**, NOT reader-staleness (a pure reader-staleness bug
  would deny on 9ce1f207 too). The rollback CLEARED the wave-3 grant regression.
- **sootTask: DENIED 4× on 9ce1f207** (mut #91/94/97/100 at 16:46:35-44, each a
  distinct branch routeKey sess_branch_mrglizp2/mrglj01l/mrglj0dt/mrglj0qz, one per
  bean) ⇒ the branch-session sootTask stall is **version-INDEPENDENT** (reproduces on
  the baseline lacking d889f2ac67), the same wave-2 behavior.

**Refinement of the unifying reader-staleness hypothesis:** the two denials behave
DIFFERENTLY across app versions, so they are likely NOT one bug. sootTask/
branch-session: version-independent (fits reader-staleness or the unfixed
active-route gate). thread/message/grant: 3199e13c-specific regression (clean on
9ce1f207). Commit-vs-read delta not extractable from wrangler tail (successful
sootSession/branch commits don't log); structural evidence (branch sessions created,
sootTask read still denies) is consistent with reader-staleness for the
branch-session path specifically. Engine clean throughout: no trip/429.
