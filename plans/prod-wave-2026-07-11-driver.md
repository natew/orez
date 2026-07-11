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

## Open items needing coordinator confirmation

1. Does the fix touch the WASM engine (host redeploy 3b required) or orez-TS only
   (data worker 3a suffices)?
2. Exact prod host deploy command from the M3 lane (name + prod bindings override
   of `orez-rust-sync` → `soot-rust-sync-host-prod-v2`).
3. Is there already a sanctioned bound-worker reopen path in the deploy env, or do
   we use the one-off temp worker in step 4?
4. DO-evict confirmation signal on the data-worker tail (with the monitor) so we
   only reopen/probe against fixed code.
