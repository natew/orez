# Prod wave driver — 2026-07-11 (orez rust sync engine on sootbean.com)

Driver agent: wave driver for coordinator `ab-mreeh1ah-69466`. Monitor:
`ab-mrg82jhn-98583`. All times UTC. Sibling doc: `prod-wave-2026-07-11-monitor.md`
(monitor's live capture + reopen mechanics research).

## Disposition: ZERO waves run. Prod is in a self-cleared incident; recovery is gated on the engine fix.

The wave brief asked for factory design waves against real prod. That was held
and the hold was approved by the coordinator, because it collides with the hard
repo rule _"never use a real prod cloud factory to RCA/stress prod"_
(`~/soot/CLAUDE.md:263`; incident `~/soot/plans/incident-2026-07-07-do-rows-written-burn.md`).
Prod then surfaced its own showstopper with no wave load.

## Situation snapshot (from monitor, ~10:40 UTC)

- `soot` namespace ZeroSqlDO write-budget circuit tripped once at 10:31:30 UTC:
  `windowRows=301642, billableRows=301642, logicalRows=25, budget=300000,
windowMs=300000` → **~12,000x write amplification** for 25 logical rows.
- Root cause (engine agent, validated locally, per coordinator): the tracked
  transaction copies all of `_zero_changes` per push **and** watermark discovery
  treats the rollback snapshot as a live sequence → triangular growth (measured
  29,402 rows for 241 deletes). Fix measures: push 281→18, create 582→144,
  cascade 32,594→1,477 (−94% to −96%). Local re-RCA is skipped; the repro matches.
- **Write source is quiet.** Exactly one trip, zero re-trips in 8+ min. All
  mutating writes rejected → successful write rate ~0. `/changes` 429s flat at
  ~8/30s (gentle client retry, not a storm). Host `/soot/pull` 500s and data
  `/__soot_pg` + `/__soot_query` 500s (`no such table: file`) both self-cleared
  ~10:34 UTC. Host ingest breaker never tripped.
- The 5-min rolling window has fully elapsed, so the live counter decayed to ~0,
  **but the trip flag is sticky and persisted in DO storage** — it survives DO
  eviction and only an explicit reopen clears it.

## Why no keepalive drain

The July runbook's gentle keepalive drain does **not** apply here and is
superseded:

1. It was written for the old zero-1.6 embed (`confirmStreamedBatches` purging a
   `_zero_changes` re-stream backlog). Prod now runs the new split Rust host +
   orez cf-do data tier.
2. The trip is persisted; a drain cannot clear it (monitor confirmed; only an
   admin reopen clears it).
3. Keeping the isolate warm would **fight** step 4 below — the resident ZeroSqlDO
   must idle-evict so a fresh isolate boots the fixed engine code. A drain keeps
   the stale-code isolate resident.

Since the source is quiet with no active burn, there is nothing to drain. Recovery
is: land the fix → release → redeploy → evict → reopen → verify.

---

## Recovery sequence (steps 1-2 owned by engine agent + coordinator)

1. **Engine fix lands in orez** (`~/orez`). Local RCA already validated.
2. **orez release into the deploy tree.** `~/orez` publishes into a downstream
   node_modules without hitting npm:

   ```sh
   cd ~/orez && bun scripts/release.ts --patch --into <deploy-tree>
   ```

   `<deploy-tree>` = the worktree the coordinator deploys from (NOT the shared
   `~/soot` checkout — soot is read-only for this driver). Soot pins `orez@0.4.60`
   (`~/soot/package.json:319`); `--into` overwrites `node_modules/orez/dist` in
   the target so the data-worker bundle picks up the fixed engine.

   > Confirm with the engine agent whether the fix is orez-TS only (cf-do /
   > do-sql-tracking / watermark — bundled into the **data worker**) or also
   > touches the Rust/WASM engine (`crates/sync-*` → `sync_wasm` — bundled into
   > the **host**). That determines whether step 3b (host redeploy) is required.

---

## Step 3 — redeploy (PREPARED; execute the moment the release exists)

Deploy from a `~/.worktrees` worktree, never the shared `~/soot` main checkout
(`fast-deploy.ts` header warns: a shared-tree data build transiently rewrites
`src/zero/server.ts` + writes `data/`+`database/`, breaking other sessions' local
zero). Coordinator owns / closely directs this.

### 3a. Data worker (REQUIRED — carries the orez engine fix)

`soot-cf-orez-data-demo`, `workers_dev:false`, ~3s build:

```sh
# from the deploy worktree, with the fixed orez already --into'd
bun src/env.ts -- bun scripts/ops/cf-demo/fast-deploy.ts data
```

`bun src/env.ts --` injects `CLOUDFLARE_API_TOKEN` + `CLOUDFLARE_ACCOUNT_ID`. The
data deploy also re-syncs the data-worker secrets, which re-asserts
`OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN` (= `BETTER_AUTH_SECRET`) — needed for the reopen
in step 5 (`deploy-cf.ts` `CF_DATA_SECRET_ENV`, lines 201-203).

App worker (`soot-cf-demo`, ~10min) does **not** carry the engine fix; only
redeploy it if the coordinator has an app-side change to ship.

### 3b. Rust host (CONDITIONAL — only if the fix touches the WASM engine)

Production host = `soot-rust-sync-host-prod-v2`, app binding `soot-cf-demo`, data
binding `soot-cf-orez-data-demo` (`~/soot/integrations/soot-rust-sync/src/deployment-target.ts`).
The host crate `~/orez/packages/sync-cf-host` builds the WASM engine and its
`wrangler.toml` default name is `orez-rust-sync` (dev, `workers_dev:true`), so the
**prod** deploy overrides name + prod bindings. This is the exact command the
coordinator's M3 lane already used — reuse it verbatim; do not hand-roll the
name/binding override. (`sync-cf-host` deploy = `build:wasm` then `wrangler deploy`
without `--dry-run`.)

### 3c. Force the resident ZeroSqlDO to pick up new code (DO idle-evict)

The `soot`/`singleton` ZeroSqlDO is a resident Durable Object running the OLD
(amplifying) code; the persisted write-circuit trip survives eviction. A new
deployment does not instantly swap a live DO's code — it evicts on idle, then the
next request boots the new isolate. **Do not reopen or probe until the fixed code
is confirmed live**, or a probe write re-amplifies on the old isolate and re-trips.

- Let the DATA ZeroSqlDO go idle long enough to evict (the shim hibernates via
  `ZERO_CACHE_IDLE_GRACE_MS`, imported in the data shim). The quiet source helps.
- Confirm fixed code is resident before reopen — coordinate with the monitor,
  who is tailing the data worker, to see the fresh-boot log signature after the
  deploy (or a build/version marker). Treat "reopen then immediate re-trip on a
  tiny write" as proof the old isolate was still resident (or the fix is
  incomplete) — abort, do not re-reopen in a loop.

---

## Step 4 — controlled reopen (PREPARED)

The DATA worker has **no public ingress** (`workers_dev:false`; host proxies only
the GET status via `/admin/upstream-write-budget`; the APP shim does not forward
`_orez/*`). The reopen must be issued from a worker **bound to
`soot-cf-orez-data-demo`**.

- Route (in the data worker): `POST /soot/_orez/write-budget/reopen`. The data
  shim strips `/soot/` → `/_orez/write-budget/reopen` on the ZeroSqlDO
  (`~/soot/src/deploy/cloudflareDoDeploy.ts:260,271`).
- Handler (`~/orez/src/cf-do/worker.ts:293`): checks
  `x-orez-admin-token: <token>` (or `Authorization: Bearer <token>`) against
  `OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN`, then `ctx.storage.delete(WRITE_BUDGET_TRIPPED_KEY)`
  - `writeBudget.reopen()` (samples/billable/logical reset, `trippedAt=null`).
    Returns `{ ok:true, enabled, windowRows:0, billableRows:0, tripped:false, ... }`
    and logs `orez_do_write_budget_reopened`.
- Token value: `BETTER_AUTH_SECRET` from `~/soot/.env.production`.

**Ingress method — one-off bound worker** (coordinator confirms; this is the only
path that reaches a `workers_dev:false` worker). Minimal wrangler worker with a
service binding to the data worker:

```jsonc
// reopen-tmp/wrangler.jsonc
{
  "name": "soot-writebudget-reopen-tmp",
  "main": "reopen.ts",
  "compatibility_date": "2024-11-01",
  "workers_dev": true,
  "services": [{ "binding": "DATA", "service": "soot-cf-orez-data-demo" }],
}
```

```ts
// reopen-tmp/reopen.ts
export default {
  async fetch(_req: Request, env: { DATA: Fetcher }): Promise<Response> {
    const r = await env.DATA.fetch(
      'https://data.invalid/soot/_orez/write-budget/reopen',
      {
        method: 'POST',
        headers: { 'x-orez-admin-token': (env as any).TOKEN },
      }
    )
    return new Response(await r.text(), { status: r.status })
  },
}
```

Deploy it, set `TOKEN` = `BETTER_AUTH_SECRET`, hit its `workers.dev` URL once, read
the JSON (expect `tripped:false, billableRows:0`), then **delete the temp worker**.
If the coordinator's deploy env already exposes a sanctioned bound-worker reopen
(an ops route the M3 lane wired), prefer that and skip the temp worker.

Read-only status any time (public, host admin key) to watch the flag:
`GET https://soot-rust-sync-host-prod-v2.natewienert.workers.dev/soot/admin/upstream-write-budget`.

---

## Step 5 — verify amplification is gone (PREPARED)

Immediately after reopen, exercise the real push→data-write path with the
**smallest** possible mutation, then read the billable delta. This is the
falsifiable check the whole recovery rests on.

1. Snapshot budget: `GET /soot/admin/upstream-write-budget` → record `billableRows`
   (should be ~0 post-reopen) and `logicalRows`.
2. One minimal owner mutation via the normal app path (mint owner session with
   `~/soot/scripts/ops/prod-login.ts --email natewienert@gmail.com`, then a single
   tiny mutation — engine agent picks the smallest real one). This boots the
   fixed isolate and writes through the fixed tracked-tx + watermark path.
3. Re-read `GET /soot/admin/upstream-write-budget`. **Pass:** `billableRows` delta
   is in the low tens and roughly tracks `logicalRows` (ratio near 1:1, not
   ~12,000:1); `tripped:false`. **Fail:** billable delta is thousands+ or it
   re-trips → the fix is not live on this isolate (eviction didn't happen) or is
   incomplete. On fail: STOP, do not reopen-loop, report to coordinator.

Report to coordinator when: (a) reopen returned `tripped:false`, and (b) the probe
write showed a near-1:1 billable:logical ratio and no re-trip. That is "breaker
recovered and service green." Confirm the collateral is gone too: `/soot/pull` on
the host returns 200 and reads that hit lazy `CREATE TABLE IF NOT EXISTS` no longer 500.

---

## Step 6 — waves (only after step 5 passes)

Not before. Per the load rule, check `sysctl -n vm.loadavg` and free memory before
each wave, ask the monitor for write-budget headroom between waves, and treat any
re-trip as a finding (record, let monitor reopen, continue). If prod visibly
breaks for real users, stop immediately and report urgent.

## Execution record (what actually happened — 2026-07-11, ~11:00-11:20 UTC)

Fix scope was **orez-TS only** (`cf-do/watermark.ts`, `cf-do/worker.ts`,
`pg-proxy-do-backend.ts`; landed `909b4db`, released 0.4.61). The refined evict
dance was used instead of a passive idle-evict: the host continuously polls the
data DO's `/changes`, so the DO never idles while the host exists.

Sequence as executed:

1. Coordinator: `release --into ~/.worktrees/soot-deploy-fix`, then
   `fast-deploy.ts data` → data worker on fixed 0.4.61, **Version `413cb780`**
   (old baseline `46dbfea4`), smoke green.
2. Driver: `wrangler delete --name soot-rust-sync-host-prod-v2` (stops polling;
   monitor confirmed host inbound=0, `/changes` −85%). Deleting the host removes
   only its derived `SYNC_DO`; the authoritative data worker is untouched.
3. DO evicted during a ~2min quiet window and **came up UN-tripped on the fixed
   code** — the sticky/persisted trip did NOT carry into the fixed-code isolate,
   so **no manual reopen was needed**.
4. Driver: temp bound worker (`services`→`soot-cf-orez-data-demo`, self-gated by
   `x-reopen-key`) served the status GET + reopen POST during the host-deleted
   window (host status proxy was gone).
5. **Verify — PASS.** GET status: `tripped:false`, decayed to 0 rows. Ambient
   post-boot window: `billableRows 12 / logicalRows 2` (~6:1). Deliberate probe
   (owner session mint via `prod-login`): budget delta **7 billable / 0 logical,
   no re-trip**. vs the incident's `301642 / 25` (~12,000:1). Monitor tail: the
   collateral `/__soot_pg` + `/__soot_query` 500s (`no such table: file`) are
   **resolved** (now 200), no new `orez_do_write_budget_tripped`, no 5xx.
6. Coordinator: host redeploy → `soot-rust-sync-host-prod-v2` `f05253d6`, root GET
   200, admin route alive. Reopen capability then **proven**: POST reopen with the
   prod len-64 token returned `{ok:true,tripped:false}`. Temp worker deleted.

### Lesson for the runbook: deploy the data worker with PROD env

`fast-deploy.ts data` from the worktree sourced the **default dev env**, so the
data worker's `BETTER_AUTH_SECRET` (and its `OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN`
alias) was written as the **dev value (len 36)** instead of prod (len 64). Symptom:
the operator reopen with the prod token 403'd (`{"error":"forbidden"}` from the
data worker). This also risks data-tier better-auth session validation if it uses
`BETTER_AUTH_SECRET`. Fix applied: `wrangler secret put BETTER_AUTH_SECRET` +
`OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN` = prod value on `soot-cf-orez-data-demo`
(a new version; the resident DO serves the old env until its next boot).
**Always source `~/soot/.env.production` for a worktree data deploy** (the worktree
has no `.env.production`; the CF creds are missing there too).

---

## Step 6 — waves (owner override; incident closed)

The owner explicitly authorized the deepseek waves this round, overriding
`~/soot/CLAUDE.md:263`. The amplification fix landing first is what makes them
safe. Guards: check `sysctl -n vm.loadavg` + free memory before each wave (16
cores; skip if 1-min load > ~2x or <6GB free), ask the monitor for write-budget
headroom between waves, capture sync-engine symptoms per wave (push failures, pull
stalls, poke gaps, reload divergence, 410 watermarkTooOld, breaker trips, latency),
treat any re-trip as a finding (record, monitor reopens, continue), and stop + alert
if prod visibly breaks for real users. Per-wave sections appended below.

### Prod-wave mechanism (learned the hard way)

The stock `debug:bossbean` driver is a LOCAL-factory harness: even with `--origin
https://sootbean.com --login test` (+ `E2E_ADMIN_TOKEN`, which mints an isolated
`e2e-<run>@test.local` prod account), it crashes at `connect ECONNREFUSED
127.0.0.1:7432` — its agent-event observer hard-requires the local `dev:factory`
Postgres. It cannot observe a prod wave. The working mechanism (coordinator interp
C) is to drive prod's REAL product path with headless playwright and observe from
outside (no pg): login → `/beta?skipWelcome` → navigate
`/project/default_<userId>/main?createProject=app&name=<n>` → FACTORY tab →
`[data-testid="boss-chat-input"]` (gate on `__sootFactoryRuntime().projectReady`
first) → send. Harness: `<driver-scratch>/wave-c-driver.ts` (needs
`NODE_PATH=~/soot/node_modules`; retry the first navigations for a fresh-Chrome
`ERR_CERT_VERIFIER_CHANGED` race).

### Wave 1 (interp C) — RESULT: STALL at project-create; found a sync-engine bug

Wave 1 could not create a project. Login + f2c shell OK on prod (isolated e2e
account). `createProjectFromTemplate` failed with a poke type-comparison:
`Cannot compare values of different types: number and string` (client IVM `rX`,
server "Poke processing error"). Monitor's server tails: the push/write succeeded
at every HTTP layer (push/pull/`__soot_pg`/`__soot_query` all 200) — the failure
is below HTTP, in poke/diff processing. **No amplification** (zero trips/429 across
all namespaces — the 0.4.61 fix holds under load; the `/soot/pull` 200 burst also
confirmed the host pull path healthy). This bug is a different class from the
amplification.

RCA (sent to engine agent `ab-mrgal2dq-98705`, HIGH confidence): failing columns
`project.createdAt` / `project.updatedAt` (drizzle `timestamp`, zero-schema type
`number`, epoch-ms i64 — the only numeric columns in the `project|insert` row).
The SNAPSHOT path coerces every `number`-typed column string→number via
`toZeroValue` (`src/zero/httpPull.server.ts:117-145`; invariant at line 114:
"rowsPatch values must match the ZERO schema's column types … timestamp → epoch
ms"). The incremental POKE/changes rowsPatch path does NOT run through `toZeroRow`,
so it streams the data-worker's i64 timestamps as CAST-AS-TEXT strings; the client
IVM then compares a poke-string `createdAt` against the snapshot/optimistic-number
`createdAt` → "number and string". Scope = all `CHANGE_CLOCK_FIELDS`
(`createdAt, updatedAt, firstSeenAt, lastActiveAt, billingPeriodStart`;
`src/zero/httpPullWatermark.ts`) across every table in the incremental poke. Fix =
run the poke rowsPatch through the same `toZeroValue` coercion the snapshot uses.
Waves HELD until this deploys. Repro + captured payloads:
`<driver-scratch>/wave-c-repro.ts` → `repro-out/{push-payloads.json,pull-bodies.txt}`.

### Round-2 ticket: fresh-namespace "no such table: file" race

Each brand-new namespace throws 2× HTTP 500 on `POST /__soot_pg` + `/__soot_query`
at creation: `SELECT 1 FROM file WHERE "projectId" = ? LIMIT 1` runs before the
`file` table is provisioned in the fresh DO. Self-resolves once the namespace is
migrated (distinct from the incident's sticky-trip-refuses-CREATE-TABLE). Not
blocking, but a real schema-provisioning race — orez round-2 ticket. Observed on
organic anon prod visitors' fresh `proj-default_anon-*` namespaces during wave 1.

## Wave 1 execution + terminal findings ledger (replacement driver, ~14:00–14:40 UTC)

Ran the real product path on the fixed stack (orez `0.4.63`, host `488840dc`, data
`cb6b7857`/`0.4.64`): `/api/test-login` E2E account → `createProjectFromTemplate`
(app template) → factory boss prompt ("Pace Mobile" running tracker) → watch to
built-app-or-stall, with between-phase write-budget checks via the host admin route
(`/soot/admin/upstream-write-budget` + `/status`, `x-admin-key`).

### 1. Timestamp compare bug — FIXED (gate passed)

Gate = `create_ok=true` AND `project.createdAt`/`updatedAt` arrive as JSON numbers
in the cold `/soot/pull` snapshot. Verbatim post-fix (run `gate-063-034635`):
`"createdAt":1783777602000,"updatedAt":1783777602000` (bare numbers; pre-fix was
the string `"2026-07-11 13:34:46"`). `accountResourceGrant.createdAt` was numeric
before and after. Served by host `soot-rust-sync-host-prod-v2` (`488840dc`). No
compare error; create-from-template succeeds end-to-end. Residency confirmed
(numbers ⇒ new isolate serving). The round-1 poke-coercion RCA above is resolved by
the sync-core epoch-ms coercion in `0.4.63`.

### 2. Wave outcome: STALL (no app built) — namespace mirror ordering race

Factory reaches the boss turn (BossBean plans, writes the ProductSpec and
strips/relays the template — a real ~200k-row write burst) but the worker beans
cannot persist their work; the factory parks with `tasks=[]` and agents
idle/absent. Confirmed across two runs (1d = mid-build driver-tab swap; 1e = single
stable tab, no swap). Ground truth via harness `ui.readState()`: `parked:true`,
agents `status:"idle"`, `currentTaskId:""`, `tasks:[]`.

**Root cause (engine RCA via coordinator + monitor APP-log RCA):** the project
namespace's access-mirror rows (`project` + `accountResourceGrant`, which carry the
owner's admin grant) seed **asynchronously** after create. The beans' first
`thread`/`message`/`task` inserts race ahead of the mirror rows, so the server
permission check's EXISTS-grant predicate is false on real data absence and denies
them — even though actor == row-owner == the project owner
(`cLX8Hw0hfOZV4ZmOWxNPkmlTj0rYzxJS` in run 1e). A real (non-impersonation) login
hits it identically; no impersonation quirk. Wave-1d's boss got further only by
timing luck (it created 4 beans + 4 tasks in chat, but `readState.tasks` was still
`[]`).

**Cascade to HTTP 500 (bug #2, real regardless of identity):** after the permission
error, the retry path calls `writeMutationResult` →
`INSERT INTO soot_0_mutations (clientGroupID,clientID,mutationID,result)` → hits
`UNIQUE constraint failed … SQLITE_CONSTRAINT_PRIMARYKEY` (that mutation id was
already recorded) → `[zero] push failed kind=PushFailed reason=database`. That
`PushFailed` body has no `mutations` array → the host's delegated-push catch
(`packages/sync-cf-host/src/host.ts`: `!Array.isArray(acknowledged)` throw
~1064-1065; catch ~1104-1107 returns 500 without `#log()`, matching the observed
`exceptions:[] logs:[]` signature) → HTTP 500 on `POST /proj-<ns>/push`. The host's
delegated target is the APP worker (`soot-cf-demo /api/zero/push`,
`mutateBinding=APP`), which returned 200 with the malformed body — so the 500 is
the host body-shape throw, not an APP passthrough.

**Fixes in flight (coordinator/engine):** (a) move soot-side access-mirror seeding
into namespace provisioning so the first inserts authorize; (b) orez host tolerance
for a structured `PushFailed`; (c) make `writeMutationResult` idempotent
(upsert/ignore an already-recorded `(clientGroupID,clientID,mutationID)` instead of
the UNIQUE constraint). **Waves HELD until (a)+(b) deploy** — every wave parks until
then; the next wave (after the mirror fix) is the built-app attempt.

### 3. Write-budget under a real build — no trip (concurrency ceiling note)

One real factory build's boss file-gen burst peaked `upstream-write-budget` at
**212,198 / 300,000 billable (71%)** in the 300 s window (logical ~320, ~660:1
amplification), then receded as the window slid. **No breaker trip**; ingest
breaker stayed at 0. The `0.4.61` amplification fix holds under a real build.
Concurrency ceiling: a single first-time build reaches ~71% of the 300k/300s
budget, so a few concurrent first-time builds could pressure or trip it — worth a
headroom review before multi-user factory load. Budget checked each 30 s tick;
trace in the driver scratchpad `budget-trace.json`.

### 4. Fresh-namespace provisioning race — both faces corroborated (round-2 ticket)

- **query-face** (the round-2 ticket above): `no such table: file` 500s on
  `/__soot_query` + `/__soot_pg` at fresh-ns creation, self-resolving
  post-migration (seen on 1c, 14:07:13-31 UTC).
- **push-face:** intermittent HTTP 500 on a fresh ns's first push(es) to the host
  (1b `proj_mrgfkhtj` push 500 ×2; also recurred on later writes for 1d
  `proj_mrgg5igp`). Same delegated-push catch as bug #2 but triggered by the
  provisioning race (APP returns a provisioning-shaped 200 body → host throw).
- data `cb6b7857`/`0.4.64` adds a no-such-table retry (query-face should decay);
  host `0.4.64` fresh-ns push barrier deploying at wave-1 terminal.

### 5. Harness notes for the next driver

- Deployed factory boss composer testid is **`sootbean-chat-input`** (aria "Message
  Soot"), NOT `boss-chat-input`; the in-page bridge `harness.ui.sendBossMessage`
  queries the stale `boss-chat-input` and always returns `{ok:false}`. Send via
  `textarea[data-testid="sootbean-chat-input"]` + native value setter + `input`
  event + plain `Enter` keydown (`src/features/chat/threadChatInput.tsx:355` —
  enter submits, shift+enter = newline).
- `window.__sootFactoryRuntime` exposes only `drivingAgentCount` (local-driver
  runner count → **0 in prod**, server/cloud-driven; NOT a stall signal;
  `headlessRuntimeEnabled:false`). Use `harness.ui.readState()`
  (`agents[].status`, `tasks`, `parked`) for real progress. `parked:true` is the
  default local state, not a stall by itself.
- Hold a **single stable driver tab** the whole build; do not swap mid-build (a
  swap looked like it might cause the park until the clean single-tab 1e run
  reproduced the same stall and the engine RCA identified the real cause).
- Driver/repro scripts (driver scratchpad): `wave1-driver.ts`
  (create→boss→readState watch), `wire-repro.ts` (timestamp gate), `bean-inspect.ts`
  (readState/bean-thread state). Coordinator: `ab-mreeh1ah-69466`; monitor:
  `ab-mrgf4lhh-94638` (push-500 correlator + trip/429 pager).
