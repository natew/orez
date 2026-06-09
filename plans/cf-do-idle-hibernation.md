# CF DO idle hibernation — sleep the zero-cache embed once no one is connected

status: in progress (2026-06-08)

## goal

Let a deployed orez CF-DO project's `ZeroCacheDO` **hibernate when no sync
client is connected**, so an idle / single-player project stops accruing
Durable Object GB-s. Today the embedded real zero-cache keeps a live
in-process bridge + internal timers for the connection lifetime, so the DO
stays resident and billed for every wall-clock hour a tab is open — even when
the project is effectively single-player and idle.

This is the "free / smaller use cases" cost lever from
`~/soot/.../plans/deploy-tiers.md` (§"the hibernation gotchas"). The validated
cost model: a resident cache DO ≈ $0.0058/active-hour; hibernating when idle
drops that toward $0.

## what we are NOT doing (rejected)

- **No bespoke/fake Zero handler for free mode.** `src/cf-do/worker.ts`'s WS
  handler is a partial reimpl (no real IVM/CVR) — it breaks on real apps.
  Free mode must run the SAME real zero-cache as paid. (user: "no we dont want
  the fake client / it will break / not an option".)
- **No "avoid websocket / HTTP-only transport".** That's a client-transport
  rewrite — the ball of yarn. Keep Zero's WS unchanged.
- **No ping-auto-response / hibernation-WS-API on the embed (yet).** That tries
  to hibernate _while a client is connected_ — harder, fights the live bridge.
  Out of scope for this pass; revisit only if "idle-but-connected" cost matters.

## the design (one path)

"Hibernate **once no one is connected.**" While ≥1 sync WS is live, zero-cache
runs exactly as today. When the last client disconnects, tear the embed down so
nothing keeps the DO resident; it gets evicted and billing stops. First request
after that lazily cold-starts zero-cache again, rehydrating its replica from DO
SQLite (already durable — no migrations/seeds to replay, so reconnect is far
cheaper than first boot).

### moving parts

1. **orez — live connection count.** `DurableObjectWebSocketHandoff` already
   tracks live bridges in `#bridges` (registered on accept, deleted on close).
   Expose `activeConnections` and an optional `onConnectionsChanged` callback.
   `startZeroCacheEmbedCF()` re-exposes `connectionCount` on its handle.
   (`src/worker/durable-object-websocket-handoff.ts`,
   `src/worker/zero-cache-embed-cf.ts`.)

2. **orez — idle decision (pure, unit-tested).** `shouldHibernate({
connectionCount, msSinceActive, graceMs })` → boolean. No I/O, deterministic.

3. **soot shim — alarm-driven teardown.** `ZeroCacheDO`
   (`src/deploy/cloudflareDoDeploy.ts` CLOUDFLARE_DO_SHIM_SOURCE):
   - `fetch()` stamps `lastActiveAt = Date.now()`.
   - `ensureReady()` schedules the first idle-check alarm after start.
   - `alarm()`: if no embed → return. If `connectionCount > 0` OR within grace
     of `lastActiveAt` → reschedule alarm, return. Else `await
zeroCache.stop(); zeroCache = undefined; ready = undefined` and DON'T
     reschedule → DO goes idle → evicted.
     Alarm cadence `IDLE_CHECK_MS` (≈30s) + `IDLE_GRACE_MS` (≈30s). Single alarm,
     no other timers; after teardown there is no pending alarm.

   Alarm chosen over reacting to the sync close callback: the callback fires
   outside a request context (risky for storage writes); the periodic alarm is
   always (re)scheduled from inside `fetch`/`alarm` and is self-healing if a
   disconnect is missed. Cost while connected: one cheap wake / IDLE_CHECK_MS on
   an already-billed active DO. Cost when idle: zero (no alarm pending).

## the load-bearing risk (must runtime-verify)

`stop()` must leave **zero** pending timers/handles or the DO never evicts
(deploy-tiers gotcha #2: "zero-cache has internal timers; the embed shims must
cancel them when idle"). `stop()` today: SIGTERM → await runWorker (≤5s) →
close proxy + backends → restore globals. Whether every zero-cache internal
timer (HeartbeatMonitor, change-streamer poll, syncer) is gone after SIGTERM is
the empirical unknown. Probe it: start embed → stop → assert no residual timers
(instrument setInterval/setTimeout, or observe real eviction in wrangler). If it
leaks, fix the embed shutdown — that fix IS the feature.

## validation gates (must stay green)

- orez unit tests (`bun run test`) incl. new handoff/idle tests.
- orez prod tests in soot (`bun test:orez:quick`, cloudflare-do-deploy.test.ts).
- chat e2e against the DO backend (the hard gate; run ONCE near the end).
- a wrangler-dev reconnect test: connect Zero client → disconnect → wait for
  teardown → reconnect → assert sync still works.

## progress (2026-06-08)

done + green:

- orez `DurableObjectWebSocketHandoff.activeConnections` getter + 2 unit tests.
- orez `ZeroCacheEmbedCF.connectionCount` getter on the embed handle.
- orez `src/worker/zero-cache-do-idle.ts`: pure `shouldHibernateIdleZeroCache`
  - `ZERO_CACHE_IDLE_CHECK_MS`/`GRACE_MS` constants + 3 unit tests.
- orez `bun run build` clean; new module exported via `./worker/*`.
- soot shim (`cloudflareDoDeploy.ts` CLOUDFLARE_DO_SHIM_SOURCE): `lastActiveAt`
  stamp in fetch, arm idle alarm after start, `alarm()` that tears the embed
  down under `blockConcurrencyWhile` once `connectionCount===0` past grace, then
  doesn't re-arm (so the DO evicts). re-arms while still up.
- soot `test/cloudflare-do-deploy.test.ts` 23/23 pass.
- soot `scripts/dev/test-cf-do-bundle.ts` green — the new import resolves and
  the full zero-cache CF overlay bundles (4.57MB).
- static check of zero-cache shutdown: `HeartbeatMonitor.stop()` clears its
  interval, `life-cycle` drains every service on SIGTERM — shutdown is designed
  to clear timers.

shipping dependency (NEEDS USER OK):

- deploy bundles orez from `node_modules/orez` (currently published 0.4.1).
  the embed `connectionCount` + the new idle module aren't in 0.4.1, so this
  ships only after an **orez 0.4.2 release + soot `orez` dep bump**. publishing
  needs explicit permission. (local node_modules/orez/dist synced by hand for
  testing only — not a publish.)

also done:

- soot shim idle timing is env-tunable: `idleCheckMs`/`idleGraceMs` read
  `ZERO_CACHE_IDLE_CHECK_MS` / `ZERO_CACHE_IDLE_GRACE_MS` from env (default to the
  orez constants). lets a throwaway deploy set both low and observe teardown fast.

runtime proof — local Miniflare path is BLOCKED (pre-existing, orthogonal):

- wrote `soot/scripts/dev/test-cf-do-hibernation.ts` (Miniflare boot→cycle). it
  gets past config but the cf-do worker bundle won't boot under Miniflare:
  `node_modules/pg/lib/index.js` evals at init and does `class BoundPool extends
Pool4` where `Pool4 = __toCommonJS(pg-pool esm)` — an ESM namespace object, not
  the class → workerd "Class extends value #<Object>". On real CF the top-level
  `pg` specifier is aliased to a virtual module (orezCfAliasPlugin,
  'orez-cf-virtual') so prod never evals it; Miniflare does. This is a
  bundle/Miniflare interop gap, NOT the hibernation change (which touches no
  pg/events/bundling). Fixing it = making the bundle pg-pool CJS-interop-safe,
  a separate task that risks the prod deploy — deferred.
- so the authoritative runtime proof is a **real CF deploy**: deploy with
  `ZERO_CACHE_IDLE_CHECK_MS`/`GRACE_MS` low, idle → watch the cache DO's GB-s
  drop in CF dashboard / `wrangler tail`, then reconnect → confirm sync resumes.
  needs the orez 0.4.2 publish (or a manual deploy from this machine, which
  bundles the locally-synced node_modules/orez) + the user's CF account.
- low regression risk meanwhile: while a client is connected the change is inert
  (alarm reschedules, never tears down with connectionCount>0). teardown only
  fires after grace with zero live sync bridges.

## tracks

- **A (this doc): idle hibernation.** BUILD DONE + green. parked awaiting user
  greenlight on (1) deploy-to-validate and (2) orez 0.4.2 publish + soot bump.
- **B: cf-do perf stress harness + DRY cleanup.** DONE this pass:
  - `perf/scripts/bench-cf-do.ts` — real perf + conformance harness over the DO
    SQL backend (/exec, /batch, /changes). throughput + p50/p95/p99 + conformance
    (roundtrip, change capture, monotonic watermark, batch atomicity, delete).
    validated live against `wrangler dev`. report → `perf/reports/cf-do-*.json`.
    full writeup: `perf/CF-DO-FINDINGS.md`.
  - baseline (wrangler dev --local, CONC=4, N=1000, 2026-06-08):

    | scenario                    |  ops/s | mean |  p50 |   p95 |   p99 |
    | --------------------------- | -----: | ---: | ---: | ----: | ----: |
    | exec INSERT (tracked)       |  1,541 | 2.59 | 2.62 |  4.07 |  5.15 |
    | exec SELECT (point)         |  1,756 | 2.28 | 2.05 |  3.89 |  7.36 |
    | batch x20 INSERT (per-stmt) | 15,233 | 5.13 | 4.95 | 11.04 | 11.56 |

  - finding: the DO SQL path is HTTP-round-trip-bound (~2.5ms/call, read or write
    alike — DO SQLite itself is sub-ms); **/batch is ~9× per-statement** (1.5k →
    15k stmt/s). this is CHAT_E2E.md §8's batching lever, now measured. all
    conformance checks green.
  - safe DRY cleanup: `src/cf-do/worker.ts` outer `export default` collapsed from
    a 35-line duplicated route table to a 4-line forward-all to the singleton DO;
    also fixed CORS `OPTIONS` (was 404 at the outer worker, now 200). validated
    by bench + worker-schema/watermark unit tests.
  - NOT done (flagged risky, needs chat e2e + CPU): DoBackend coalescing
    same-shape seed inserts into /batch — the actual chat-boot speed win. the
    harness is the gate for it.
