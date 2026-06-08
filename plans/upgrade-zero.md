# Upgrading `@rocicorp/zero` in orez — runbook

orez does not just _depend_ on `@rocicorp/zero`; it **patches zero-cache's
compiled internals** at startup (node, browser, and Cloudflare paths) and speaks
zero's wire/sync protocol directly. That makes a Zero bump "a whole process,"
not a one-line version change. This doc is the reusable runbook: the exact
coupling points to re-validate, the staged cross-repo validation pipeline, and
the per-version gotchas.

> Keep this updated on every upgrade. Add a new entry under **Worked examples**
> each time, and fix any coupling point whose file:line drifted.

---

## 0. TL;DR runbook

1. **Find the new protocol version.** `PROTOCOL_VERSION` lives in
   `@rocicorp/zero/out/zero-protocol/src/protocol-version.d.ts`. orez's
   `/sync/v<N>/connect` strings must match it. (1.5 = 50, 1.6 = 51.)
2. **Find the matching `@rocicorp/zero-sqlite3`.** `npm view @rocicorp/zero@<v>
dependencies.@rocicorp/zero-sqlite3`. Pin orez's _direct_ devDep to that
   **exact** version so there is a single copy in the tree (orez patches the
   one it resolves; zero-cache loads the one _it_ resolves — they must be the
   same instance).
3. **Bump `package.json`** (`@rocicorp/zero`, `@rocicorp/zero-sqlite3`) and
   `bun install`. See **§2 release-age gotcha** if bun blocks a fresh version.
4. **Rebuild the native binding:** `bun run native:bootstrap` (see **§5** — it
   has a downgrade footgun under the release-age policy).
5. **Bump every protocol string** (see **§3 coupling map**, category A).
6. **Re-validate every patch anchor** against the freshly installed compiled
   code (see **§3**, categories B–E). The fastest pre-check is
   `bun run cf-patches.test.ts` + the litestream-patch path. Each patch _fails
   loud_ if its anchor is gone — trust those warnings.
7. **Run the staged pipeline (§4):** orez unit → orez integration (native) →
   chat e2e → orez-node ✅ → orez-web → orez-cf (+ ~/soot).

---

## 1. Why a Zero bump is invasive here

zero-cache ships as compiled ESM under `@rocicorp/zero/out/`. orez:

- **Rewrites compiled files** to neutralize behavior that assumes a full
  zero-cache deployment (litestream restore, worker auto-start, `node:worker_threads`).
- **Reimplements an internal module inline** (the write-worker) so it runs
  without a real worker thread on Cloudflare.
- **Talks the sync protocol** directly in tests/benches and the CF DO worker.
- **Shims `@rocicorp/zero-sqlite3`** (native binary build, or a wasm/bedrock
  shim for browser).

Every one of those is coupled to zero's _compiled shape_, which can change on
any release even with "no breaking changes" in the release notes. The patches
are written to **throw or warn loudly** when their anchor disappears — treat
that as the signal to update this doc.

---

## 2. The bun release-age gotcha

`~/.bunfig.toml` sets `minimumReleaseAge = 259200` (3-day supply-chain
cooldown). A Zero release younger than 3 days is **blocked** by `bun install`:

```
error: No version matching "@rocicorp/zero" found for specifier "1.6.1"
       (blocked by minimum-release-age: 259200 seconds)
```

Workaround for a single upgrade — a **temporary** project-local `bunfig.toml`
that excludes only the two trusted first-party packages, then delete it:

```toml
# TEMPORARY — delete after install
[install]
minimumReleaseAge = 259200
minimumReleaseAgeExcludes = ["@rocicorp/zero", "@rocicorp/zero-sqlite3"]
```

Do **not** leave it committed — it weakens the policy for the repo. Once the
versions age past 3 days, plain `bun install` works and CI is unaffected. Note
`@rocicorp/zero-sqlite3` and `@rocicorp/zero` age out independently; check both.

---

## 3. Coupling map — re-validate ALL of these every upgrade

### A. Sync protocol version strings (must equal `PROTOCOL_VERSION`)

| file                                                  | what                                           |
| ----------------------------------------------------- | ---------------------------------------------- |
| `src/integration/integration.test.ts`                 | `const SYNC_PROTOCOL_VERSION`                  |
| `src/integration/replication-latency.test.ts`         | `const SYNC_PROTOCOL_VERSION`                  |
| `src/integration/restore-live-stress.test.ts`         | `const SYNC_PROTOCOL_VERSION`                  |
| `src/integration/restore-reset.test.ts`               | `const PROTOCOL_VERSION` (historically lagged) |
| `src/worker/embed-integration.test.ts`                | `const SYNC_PROTOCOL_VERSION`                  |
| `src/bench/serial-mutations.bench.ts`                 | `const SYNC_PROTOCOL_VERSION`                  |
| `src/cf-do/worker.ts`                                 | header comment `WS /sync/v<N>/connect`         |
| `src/worker/durable-object-websocket-handoff.test.ts` | literal `/sync/v<N>/connect`                   |

Routing code uses `startsWith('/sync/v')` (version-agnostic) — only the
constants/comments above need bumping. The PG-wire `196608` (protocol 3.0)
literals in `pg-proxy*.ts` / `proxy-throughput.bench.ts` /
`tcp-replication.test.ts` are **PostgreSQL**, not Zero — leave them.

### B. Litestream restore patch — `src/zero-litestream-patch.ts`

- **Target:** `out/zero-cache/src/services/litestream/commands.js`
- **Anchor:** `async function restoreReplica(lc, config, replicaConstraints) {`
- **Why:** zero-cache's dedicated change-streamer calls `restoreReplica()`
  unconditionally on restart once the change-log has rows; orez has no
  litestream backup, so it would error + wastefully resync. The patch injects
  an early-return guard when `config.litestream?.backupURL` is unset.
- **On upgrade:** if the anchor moved, the patch **throws** at startup with a
  pointer to update it. Also re-confirm `config.litestream?.backupURL` is still
  the correct gate (zero's `main.js` dispatcher gates real backup/restore behind
  the same field — mirror it).

### C. Cloudflare overlay patches — `src/worker/cf-patches.ts`

Copies `out/` into a generated overlay and applies 5 patches. Each warns if its
anchor is gone. Exercised by `src/worker/cf-patches.test.ts` — **run it first.**

1. **`patchWorkerUrls`** — overwrites `server/worker-urls.js` with
   `zero-worker://` identifier URLs. Re-check the **exported URL set** matches
   upstream's (`MAIN/CHANGE_STREAMER/REAPER/REPLICATOR/SHADOW_SYNCER/SYNCER/WRITE_WORKER`).
2. **`patchWorkerEntrypoints`** — neutralizes each worker's import-time
   auto-start. Anchors on the **stable prefix** `if (!singleProcessMode())
exitAfter(` and flips the condition to `false` (robust to the call
   arguments, which vary per worker and per version — 1.6 added an `lc` arg and
   a `.catch()` wrapper on change-streamer). If the prefix ever changes, update
   `WORKER_AUTOSTART_PREFIX`.
3. **`patchProcesses`** — replaces the dynamic `import(moduleUrl.href)` in
   `types/processes.js` with a static `__zc_workers` lookup. Anchor:
   `import(moduleUrl.href).then(async ({ default: runWorker })`.
4. **`patchWriteWorkerClient`** — **overwrites**
   `services/replicator/write-worker-client.js` with an inline (no
   `node:worker_threads`) reimplementation. This is the most fragile patch: it
   hardcodes relative imports and the internal `createAPI` shape from
   `write-worker.js`. On upgrade, **diff orez's inline body against the real
   `write-worker.js`**: verify `createLogContext`/`ChangeProcessor`/
   `StatementRunner`/`getSubscriptionState`/`applyPragmas` signatures + the
   `export { ... }` surface still match. (1.6 changed `createLogContext`'s 2nd
   arg to a string and added a `serializeError` export — both had to be mirrored.)
5. **`patchPgsqlParserWasm`** — embeds `libpg-query` wasm bytes. Coupled to
   `pgsql-parser`/`libpg-query`, **not** Zero — only revisit if those bump.

### D. In-process / browser embed import — `src/worker/zero-cache-embed.ts`

- Dynamically imports `@rocicorp/zero/out/zero-cache/src/server/runner/run-worker.js`
  (also a static import in `zero-cache-embed-cf.ts` and `browser-embed.ts`).
- On upgrade: confirm the path still exists and exports `runWorker`. A failure
  here often surfaces as **"failed to import zero-cache runWorker … ensure
  @rocicorp/zero is installed"** — usually the _native binding isn't built_
  (run `native:bootstrap`), not a moved path. Verify which before chasing it.

### E. Crash/recovery error signatures — `src/recovery.ts`

String-matches zero-cache's crash output to pick restart-vs-full-reset:
`changeLog_pkey`+`duplicate key`, `23505`+`watermark`, `RowsVersionBehindError`,
`max attempts exceeded waiting for CVR`, `replica db must be in wal2 mode`,
`SQLITE_CANTOPEN`, `Unable to read watermark from replica`+`_zero.replicationState`,
statement/query-timeout phrasing. **Validate at runtime** (`perf:crash`, the
integration suite) — release notes that touch the change-streamer (1.6:
"startup errors during change-streamer init now published") can change the
wording. `src/recovery.test.ts` covers the classifier with sample tails.

### F. SQLite mode shim — `src/sqlite-mode/`

- `apply-mode.ts` finds `lib/index.js` in `@rocicorp/zero-sqlite3` and, for
  **native** mode, restores the original (no coupling to zero-sqlite3
  internals). For **wasm** mode it writes a bedrock-backed shim
  (`shim-template.ts`) that must expose every method zero-cache calls on the
  `Database` (`pragma`, `prepare`, plus no-op `scanStatus*`). Wasm-shim coverage
  is an **orez-web** concern — re-check `shim-template.ts` if a release adds new
  SQLite API usage (1.6's SQLiteStore batching / STAT4 sampling are candidates).
- `JOURNAL_MODE = wal2` is required by zero-cache (`BEGIN CONCURRENT`). If a
  release changes that, update `sqlite-mode/types.ts` + the recovery signature.

---

## 4. Staged validation pipeline (do in order)

Each stage gates the next. Don't skip ahead — a green unit suite does not mean
sync works end-to-end.

1. **orez unit** — `bun run test` (excludes `src/integration/`, wasm). Fast.
2. **orez integration (native)** — build the native binding then run the
   integration suite, which spawns real zero-cache 1.x and drives the sync
   protocol: `bun run native:bootstrap && bun run test:integration`
   (`test:integration:native` covers the native-startup guard). Also worth:
   `perf:correctness`, `perf:crash`.
3. **chat e2e** — orez's `test:chat:e2e` runs ~/chat against orez. It requires:
   1. **Upgrade `~/chat`** to the same Zero version first.
   2. **Start OrbStack/Docker** (chat's reference stack runs real Zero +
      Postgres in Docker).
   3. Confirm **~/chat's own integration tests pass against real
      zero/docker** — i.e. chat is properly upgraded on its own terms.
   4. _Only then_ run this repo's chat e2e: `bun run test:chat:e2e`.
      ➜ Passing this is **orez-node ready.** ✅ done for 1.6 (incl. the cf-do
      DO-backend e2e — see §6). Note: that e2e drives chat through orez's cf-do
      _translation layer_ against a local `wrangler dev`; it is a strong lower
      bound but **not** the authoritative orez-cf validation (§7, stage 5).
4. **orez-web** — soot bundles orez into browser workers and tests cross-surface
   sync (`test/orez-web-sync.test.ts`). Validate in `~/soot`. **See §7, stage 4.**
   ✅ done for 1.6.
5. **orez-cf** — the Cloudflare Durable Object backend (`src/cf-do/`,
   `src/worker/` overlay). **Requires coordination with `~/soot`** (soot is the
   downstream CF consumer). Validate the overlay build + a real DO deploy.
   **See §7, stage 5.** ✅ done for 1.6 — both legs validated.

---

## 5. Native sqlite bootstrap footgun

`scripts/setup-native-sqlite.ts` rebuilds the binding when missing by running a
**bare `bun i @rocicorp/zero-sqlite3`**. Under the release-age policy that
resolves to the newest _un-blocked_ version, which can **downgrade your pinned
devDep** (e.g. pin 1.1.2 → bun installs 1.1.1) while zero-cache still pulls the
pinned one for itself — leaving two copies and a patch/runtime version split.

Mitigation during an upgrade window:

- Keep the temporary `bunfig.toml` exclude (§2) in place _while_ running
  `native:bootstrap`, then `bun install` once more to restore the exact pin, and
  confirm a single version: `ls node_modules/.bun/@rocicorp+zero-sqlite3@*` and
  `grep version node_modules/@rocicorp/zero-sqlite3/package.json`.
- After the versions age past 3 days, the bare `bun i` resolves correctly and
  the footgun disappears.

Verify the binary exists for the resolved version:
`find node_modules/.bun/@rocicorp+zero-sqlite3@<v>/node_modules/@rocicorp/zero-sqlite3/build/Release/better_sqlite3.node`.

---

## 6. Worked examples

### 1.5 → 1.6 (June 2026)

- **Versions:** `@rocicorp/zero` 1.5.0 → **1.6.1**; `@rocicorp/zero-sqlite3`
  1.0.18 → **1.1.2** (zero 1.6.1 requires `^1.1.2`; zero 1.6.0 only `^1.0.18`).
  **Protocol 50 → 51** (min supported still 30). Release notes: _no breaking
  API changes_; headline PlanetScale failover (persistent slots — N/A, orez
  owns its replica); change-streamer init now publishes startup errors.
- **Release-age:** 1.6.1 and zero-sqlite3 1.1.2 were both <3 days old at upgrade
  time → used the temporary `bunfig.toml` exclude (§2).
- **What broke / changed (the real work):**
  - Protocol strings 50 → 51 across the 8 sites in §3.A (and the stale
    `restore-reset.test.ts` 45 → 51).
  - `patchWorkerEntrypoints` regex no longer matched — 1.6 added `lc` as
    `exitAfter`'s first arg and wrapped change-streamer's `runWorker` in a
    `.catch()` that calls `publishCriticalEvent`. **Rewrote it to neutralize the
    stable guard prefix** instead of matching the (now-variadic) call.
  - `patchWriteWorkerClient` overwrite drifted from upstream: `createLogContext`
    now takes a **string** worker name (`"write-worker"`) not `{ worker: ... }`,
    and `write-worker-client.js` now exports **`serializeError`**. Mirrored both.
  - Native binding rebuilt for 1.1.2 (`native:bootstrap`); hit the §5 downgrade
    footgun and corrected the pin back to 1.1.2.
  - Two reset bugs surfaced by `restore-live-stress` (full reset + live
    frontend): the crash-watcher misfire and a stuck proxy mutex on CVR/CDB
    recreation. Both fixed — see the `restore-live-stress` section below.
- **Validated OK without change:** litestream `restoreReplica` anchor (§3.B),
  `worker-urls.js` export set, `processes.js` dynamic-import anchor,
  `run-worker.js` import path, `lib/index.js` location for the sqlite shim.
- **Pre-existing, fixed alongside (not caused by the bump):** two
  `pg-proxy-do-backend.test.ts` assertions left stale by commit `129ca3d`
  (sqlite keyword quoting of `key`/`current`) — `obj.key` → `obj."key"`,
  `( current )`/`SET current = excluded.current` → quoted forms. (These were
  failing main's CI at `aaf5ad0`; main was green at `13b685b`.)

#### Validation status after the bump

- `bun run test` (unit): **655/655** (embed-integration moved to the integration
  suite — see below; +1 for the cf-do BIGSERIAL test). build / lint / format /
  tsc all green.
- `bun run test:integration`: **24/24** (incl. embed + the two reset fixes below).
- `bun run test:wasm`: **22/22**. `bun run test:compiler`: **49/49**.
- **orez-cf chat e2e** (stage 3, `test:chat:e2e` against the cf-do DO backend):
  **48 passed / 1 flaky / 2 skipped**, all channel-unseen tests green — after
  the two cf-do fixes below.

#### `restore-live-stress.test.ts` — FIXED (two distinct reset bugs)

Symptom: a SIGUSR1 full reset _while a WS sync client is connected_ fails;
the client's reconnect gets `ECONNREFUSED` and orez logs
"reset failed: zero-cache exited with code 0". Found by runtime probing
(life-cycle `runUntilKilled`/`exitAfter`/`#startDrain`, `terminateProcessTree`
caller stacks, the proxy mutex hold-time watchdog, and a frontend-close A/B).
Two independent causes, both fixed:

1. **Crash-watcher misfire** — `src/index.ts` `installCrashWatcher`. The
   watcher's `zeroCacheProcess.on('exit')` handler read the _current_
   `zeroCacheProcess`. A reset swaps in a new process, but the OLD process's
   `exit` event can arrive _after_ the swap (1.6's graceful drain is slower, and
   `zeroStopExpected` is cleared the moment `killZeroCache` returns — before the
   late event fires), so the handler ran recovery against the **new** process.
   Fix: capture the watched process; bail if `watched !== zeroCacheProcess`.
2. **Stuck proxy mutex on instance recreation** — `src/pg-proxy.ts` +
   `src/index.ts`. orez's pg-proxy serializes each pglite instance behind a
   `Mutex`. A connected frontend's syncer is mid-query on the CVR instance
   (holding that mutex) when the full reset `close()`s + recreates CVR/CDB; the
   in-flight query on the now-closed instance never returns, so its mutex is
   **never released**. The next zero-cache's CVR/CDB connections then block on
   the stuck mutex forever → it never becomes healthy → "exited with code 0".
   Confirmed by: closing the frontend _before_ the reset makes the test pass,
   and the mutex watchdog showing one instance held until teardown. Fix: the
   proxy exposes `resetDbState(dbName)` which swaps in a **fresh mutex +
   txState** for the recreated instance (abandoning the stuck one); the reset
   calls it for `zero_cvr`/`zero_cdb` right after recreating them. Independent
   of data size (tiny stress data reproduced it too). Now passes 3/3 with the
   default stress config and is **CI-gated**: the `native-integration` job now
   runs the full `test:integration` (`native:bootstrap && test:integration`),
   not just the native-startup guard.

#### Resolved: `embed-integration` "insert triggers poke" load flake

Was a full-39-file-`bun run test` flake: the in-process change-streamer is
CPU-starved late in the heavy unit suite (and worse on a loaded machine), so
the poke drifts past 30s. It is an integration test, not a unit test. Fix:
**moved `embed-integration.test.ts` → `src/integration/`** (8-file suite, light
load) where it passes reliably (poke ~2s), and CI now runs the full
`test:integration` (see above) so it stays covered. unit suite is now 654/654;
integration 24/24.

#### orez-cf (cf-do) chat-e2e follow-ons — both FIXED

Stage 3 runs ~/chat's playwright e2e in `E2E_LITE` mode but routed at the
PG-protocol layer to the Cloudflare Durable Object backend
(`src/pg-proxy-do-backend.ts` → `src/cf-do/worker.ts` over HTTP `/exec`).
Two zero-1.6-exposed cf-do bugs blocked it; both fixed:

1. **`replicas.rank BIGSERIAL` left NULL → "Expected bigint at rank. Got null"**
   (`src/pg-proxy-do-backend.ts`). Zero 1.6 changed the shard `replicas` table
   to `"rank" BIGSERIAL` (a **non-PK** auto-increment;
   `node_modules/@rocicorp/zero/out/.../change-source/pg/schema/shard.js`).
   SQLite only auto-increments an `INTEGER PRIMARY KEY`, so the DO backend's
   pg→sqlite type map turns `bigserial` into a plain nullable `integer` and
   `createReplica` (which omits `rank`) leaves it NULL; zero's change-streamer
   then reads `rank` expecting a bigint and the cache exits 255 (port never
   binds). Fix: in the `CreateStmt` rewrite path, detect serial/bigserial
   columns **before** `normalizeCreateTable` rewrites the type, and emit a
   sequence-emulating companion statement —
   `CREATE TRIGGER … AFTER INSERT … WHEN NEW.<col> IS NULL
BEGIN UPDATE … SET <col> = (SELECT coalesce(max(<col>),0)+1 FROM …) … END`.
   Inline-PK serials are skipped (they become `INTEGER PRIMARY KEY` = rowid,
   already auto-incrementing). Covered by a new unit test.

2. **`track is not defined` on `INSERT INTO message … RETURNING *`**
   (`src/cf-do/worker.ts`). Latent ReferenceError introduced 11 days earlier in
   `d2dbb0b fix(cf-do): track chat trigger side effects`:
   `appendRowsAsUpdates(...)` referenced `track.transactionID` but `track` was
   never in its scope. The derived-tracking path (a `message` insert that bumps
   a channel's latest-order or a thread's reply-count) is exactly what the
   channel-unseen / thread-lifecycle e2e exercise, so it surfaced now. Fix:
   thread `transactionID` through `appendRowsAsUpdates` as a parameter. No test
   previously covered this method.

**Port-coordination note:** orez's cf-do `wrangler dev` defaults to `:8799`,
which **collides with ~/soot's** dev DO worker. When other agents are running
soot, start orez's worker on a dedicated port and point the e2e at it:
`cd src/cf-do && bunx wrangler dev --port 8798 --local --no-show-interactive-dev-session`,
then `DO_BACKEND_URL=http://127.0.0.1:8798 RETRY=1 bun run test:chat:e2e`.
Reset DO state between runs by trashing `src/cf-do/.wrangler/state/v3/do`.

---

## 7. Stages 4 & 5 — `~/soot` validation plan (orez-web + orez-cf)

> Status: **PLAN ONLY — not yet executed.** These are the last two legs of the
> 1.6 upgrade and both are significant. Authored 2026-06-07.

### 7.0 How soot consumes orez (the coupling, verified)

Everything soot runs resolves orez from **`~/soot/node_modules/orez/dist`** —
both legs share one installed copy:

- **orez-web** (`packages/orez-web/`): bundles browser workers from
  `orez/dist`. `scripts/build-zero-cache.ts` resolves
  `orez/dist/worker/shims/*` and `orez/dist/worker/browser-embed.js`; the
  worker sources import `orez/worker/browser-embed`,
  `orez/worker/shims/ws-browser`, `orez/change-tracking`,
  `orez/pg-proxy-browser`. Built via soot's `build:orez`
  (`scripts/build-orez.ts` → emits `orez-web-zc/pg-proxy/pglite.worker.js`
  into `soot/public/`). Tested by `test/orez-web-sync.test.ts` (npm scripts
  `test:orez`, `:quick`, `:smoke`, `:robust`, `:prod`).
- **orez-cf / cf-do** (`src/deploy/cloudflareDoDeploy.ts`): imports
  `ZeroDO from 'orez/cf-do'` and `prepareZeroCacheForCF from 'orez/worker/cf-patches'`,
  esbuild-bundled **at deploy time** (`bundleCloudflareDoWorker`), then
  `wrangler deploy`. There is **no** `packages/orez-cf` — the entire DO backend
  comes from the npm `orez` package.

**The no-publish ship path:** from `~/orez`, `bun release --into ~/soot`
runs `bun run build`, packs `orez` + `bedrock-sqlite`, and unpacks them into
`~/soot/node_modules/<name>` — **but only replaces packages already present**
(orez 0.3.9 is present → its `dist` becomes the 1.6.1-based build). It does
**not** touch soot's `package.json` pin. Good enough for testing; a real ship
needs publishing orez to npm + bumping soot's pin (user's call).

### 7.1 The blocking prerequisite — soot's own Zero is still 1.5

This is the heart of "coordination with ~/soot". soot's **client** Zero is
pinned to **1.5.0 / protocol 50**, orez is now **1.6.1 / protocol 51**. A 1.5
client against a 1.6-embedded zero-cache is a **protocol mismatch** — sync will
not connect. Every Zero surface must move to 1.6.1 together:

| site         | file                                                        | current   | →        |
| ------------ | ----------------------------------------------------------- | --------- | -------- |
| client dep   | `~/soot/package.json:234` `@rocicorp/zero`                  | `1.5.0`   | `1.6.1`  |
| native dep   | `~/soot/package.json:235` `@rocicorp/zero-sqlite3`          | `^1.0.18` | `^1.1.2` |
| orez-web dep | `~/soot/packages/orez-web/package.json:18` `@rocicorp/zero` | `1.5.0`   | `1.6.1`  |

(soot's `overrides` block does **not** pin zero — only RN packages — so nothing
to change there. `@rocicorp/zero-sqlite3` is in `trustedDependencies`; leave it.)

Reinstall under the 1.6.1 release-age cooldown (clears ~2026-06-09): drop a
**temporary** `~/soot/bunfig.toml` with
`minimumReleaseAgeExcludes = ["@rocicorp/zero", "@rocicorp/zero-sqlite3"]`
(see §2), `bun install`, rebuild the native binding, then **delete the
bunfig.toml**. After 06-09 the exclude is unnecessary.

### 7.2 Coordination & safety (soot is a live multi-agent repo)

`~/soot` has **many** concurrent agent sessions (seen via `agentbus list`).
Before doing anything that binds ports, mutates the working tree, or deploys:

- Announce intent via `agentbus mail` to the soot manager session; check the
  roster first.
- Pick a non-default `PORT_OFFSET` for the playwright tests; orez's own cf-do
  `wrangler dev` must avoid soot's `:8799` (use `:8798`).
- **Never** kill another session's processes or stash their uncommitted work.
- soot needs **explicit commit permission** — same rule as ~/chat. Leave the
  dep bumps uncommitted and flag them.

### 7.3 Stage 4 — orez-web validation — ✅ DONE for 1.6 (2026-06-08)

Validated in an isolated worktree `~/.worktrees/soot-zero-16` (branch
`chore/upgrade-zero-1.6`) on `PORT_OFFSET=500`. **Result: `test:orez` full
(11/11), `:smoke` (3/3), `:robust` (5/5) all green; warmup ready ~4–8s
(faster than the 60s budget → parity with 1.5, no timeouts loosened).**

Steps (the real ones — they diverged from the original plan above):

1. Bump Zero deps to 1.6.1. `bun update --latest @rocicorp/zero @rocicorp/zero-sqlite3`
   only bumps **root**; the workspace members (`packages/orez-web`, `templates/*`)
   keep `1.5.0` and bun's filtered update won't move exact pins — **hand-align**
   those manifests to `1.6.1` and `bun install` so the lockfile + frozen-install
   gate stay clean. soot's committed `bunfig.toml` already sets
   `minimumReleaseAge = 0`, so **no temp bunfig is needed** (the §2/§7.1 workaround
   does not apply to soot).
2. **Native `@rocicorp/zero-sqlite3` 1.1.2 is prebuilt-only.** Its npm tarball
   omits the generated `unicode_case_data.h`, so `bun tko run ensure-zero-sqlite`
   (node-gyp source rebuild) **fails** (`fatal error: 'unicode_case_data.h' file
not found`) and silently falls back to wasm — but the soot **node backend**
   hard-requires the native binding (`assertNativeNodeRuntime`), so the stack
   won't boot on wasm. Fix: use the prebuilt binary (what a plain `bun install`
   fetches via `prebuild-install`). Pragmatic unblock used here: copy the
   resolved binary from another checkout that has it, e.g.
   `cp ~/orez/node_modules/.bun/@rocicorp+zero-sqlite3@1.1.2/node_modules/@rocicorp/zero-sqlite3/build/Release/better_sqlite3.node`
   `   ~/soot/node_modules/@rocicorp/zero-sqlite3/build/Release/`. **TODO (soot):**
   make `ensure-zero-sqlite` prefer `prebuild-install` over node-gyp for 1.1.2+.
3. Ship orez: `bun release --into <soot-worktree>` (build + unpack 1.6 dist).
4. Apply vxrn's built-in dep patches **before first dev boot**:
   `bun -e "const m=await import('./node_modules/vxrn/dist/utils/patches.mjs'); await m.applyBuiltInPatches({root:process.cwd()})"`.
   On a fresh checkout the `one dev` optimizer races vxrn's
   `applyBuiltInPatchesPlugin`, so the `@react-navigation/core` exports patch
   (`./lib/module/EnsureSingleNavigator` etc.) isn't applied yet →
   `"… is not exported under conditions ['vxrn-web','import']"` and no hydration.
   Pre-applying (idempotent, persists in node_modules) avoids it. Pre-existing
   soot/vxrn cold-start race; not a Zero issue.
5. Build the test prereqs (`ci-dev` skips them): `bun run build:prereqs:validate`
   (generate + `build:sootsim:cli` → `build:sootsim` (`sootsim/sdk`) + `build:deps`
   - `build:tool-runtime`). Without `sootsim/sdk` built, the project route 500s
     on SSR (`Cannot find module 'sootsim/sdk'`).
6. Rebuild browser workers: `bun run build:orez`. Confirm protocol **v51**
   (`grep -o 'protocolVersion: [0-9]*' public/orez-web-zc.worker.js` → 51).
7. Boot the stack the **CI way** — `bun scripts/test-stack.ts start` (NOT bare
   `ci-dev start`): test-stack forces `NODE_ENV=development`, without which One's
   `DevHead` skips injecting `/@one/dev.js` → no hydration → warmup never ready.
8. Run `test:orez:smoke` → `test:orez` (full) → `test:orez:robust`, all on the
   same `PORT_OFFSET`. Logs in `/tmp/orez-sync-logs/`. `bun scripts/test-stack.ts stop`
   when done.

**Two real zero-1.6 orez-web bugs found + fixed** (both surfaced only via a
playwright probe capturing the in-page worker console — the test's truncated
warmup dump hid them):

1. **Worker auto-start guard not neutralized** → in-browser zero-cache crashes
   at startup with `Error: Unexpected undefined value` (`must(parentWorker)` in
   `orez-web-zc.worker.js`). soot's `packages/orez-web/scripts/build-zero-cache.ts`
   `zcPatchPlugin` strips zero's `if (!singleProcessMode()) exitAfter(...)`
   self-start, but its two regexes only matched the single-line `;`-terminated
   form and the older brace form. Zero 1.6 made it **multi-line, no braces**
   (`exitAfter(lc, () => runWorker(must(parentWorker), …).catch(…))`), so neither
   matched and the guard shipped (same class as §3.C-2 for the CF overlay).
   Fix: anchor on the stable prefix `if (!singleProcessMode()) exitAfter(` →
   `if (false) exitAfter(` (variadic/multiline-safe), plus a **fail-loud
   post-build assertion** that the bundle no longer contains
   `!singleProcessMode()) exitAfter`.
2. **Initial-sync deadlock on the faked replication session** → after "opening
   replication session" the change-streamer hangs forever (no `ready`). Zero 1.6's
   `createReplicaAndSlot` (`change-source/pg/replication-slots.js`) creates the
   slot from a **separate replication session opened inside the main connection's
   advisory-lock transaction** (`runTx(sql, … createReplicationSlot(session) …)`),
   and that session issues `SET lock_timeout = <n>` then
   `CREATE_REPLICATION_SLOT … (FAILOVER)` (new in 1.6, gated on `pgVersion>=17e4`
   — orez reports PG 17.0.4). orez-web's pg-proxy uses a **transaction-aware
   mutex** (`acquireForOwner`, owner=null), so the faked repl session waits for
   the main transaction to commit — which can't, until slot creation returns.
   Deadlock. orez-node avoids it with a plain per-statement mutex. Fix (orez):
   - `src/pg-proxy-browser.ts` — the repl protocol-command branch uses the **raw**
     `mutex.acquire()` (statement-level, like orez-node), not `acquireForOwner`,
     so the slot row lands in the main connection's open tx and commits with it.
   - `src/replication/handler.ts` — `handleReplicationQuery` answers **any
     `SET …`** (not just `SET TRANSACTION/SESSION`) with a synthetic `SET`
     CommandComplete, so `SET lock_timeout` never round-trips through pglite.

Watch-items still valid: protocol-version handshake, SAB→MessagePort handoff
(known IPC reload-sync bug if SAB disabled), reload persistence, multi-surface
(web/native) convergence.
➜ Green here = **orez-web ready. ✅**

### 7.4 Stage 5 — orez-cf / cf-do validation (the big one) — ✅ DONE for 1.6 (2026-06-08)

**Result: all gates green on zero 1.6 — no compiler-gap tail materialized.**
Validated from `~/.worktrees/soot-zero-16` against the real soot CF account
(`natewienert.workers.dev`), unique slug prefix `zero16-cfdo`, every deploy
auto-torn-down (verified zero orphan workers/D1 via the CF REST API afterwards):

- **Bundle check** (`test-cf-do-bundle.ts`): ok, 4.57 MB — all 9 cf-patches
  applied (worker-urls, auto-start disabled on main/change-streamer/reaper/
  replicator/syncer, static processes imports, inline write-worker, embedded
  libpg-query wasm). The §3.C anchors held for 1.6. (Note: the CF overlay
  **correctly** neutralizes the worker auto-start — the §7.3 bug-1 guard
  regression was orez-web's separate copy in `build-zero-cache.ts`, not here.)
- **Unit guard** (`cloudflare-do-deploy.test.ts`): **23/23** against the
  shipped 1.6 `node_modules/orez/dist`.
- **Live deploy + smoke** (`test-cf-do-deploy.ts <t> zero16-cfdo`): **todo /
  app / flights all PASS** — home 200, push endpoint parses (200, expected
  `Missing clientGroupID` on the empty probe body), **sync websocket 101
  (in-DO zero-cache accepts sync)**, `/exec` 404. (app hit a one-off transient
  `ECONNRESET` on the trailing `/exec` probe; clean on re-run — not schema.)
- **Live deploy + browser runtime** (`--runtime`, todo): **`[runtime] ✓ todo
runtime validated`** — anonymous realtime + durable sync across two isolated
  browser contexts against the deployed worker.

CF creds: `CLOUDFLARE_ACCOUNT_ID` + `CLOUDFLARE_API_TOKEN` are NOT in env nor in
the repo — they live in soot **main's** untracked `.env`/`.env.development`,
which a fresh worktree does not inherit. Copy both into the worktree (they're
gitignored) to run the deploys. The deploy genuinely needs the API **token**
(not just wrangler's stored OAuth): it makes direct `api.cloudflare.com` REST
calls for D1 create/delete + workers subdomain, and passes the token to wrangler.

➜ Green here = **orez-cf ready. ✅**

#### original plan (for reference)

Run cheapest→most-authoritative; each gates the next:

1. **Bundle check (no deploy):** `bun scripts/dev/test-cf-do-bundle.ts` — proves
   esbuild can bundle the Zero CF overlay + orez 1.6 cf-do shim for workerd.
   Fastest signal that 1.6 doesn't break the overlay (`cf-patches.ts` — re-verify
   the §3.C patch anchors held for 1.6).
2. **Deploy-infra unit guard:** `bun test test/cloudflare-do-deploy.test.ts` —
   asserts the **installed** `node_modules/orez/dist` still carries the
   transaction barrier (`_zero_pending_changes`, `/commit-tracked-tx`,
   `/rollback-tracked-tx`, `transactionID` forwarding) + wrangler-config
   normalization + replica-reset logic. Must pass against the shipped 1.6 orez.
3. **Live deploy + runtime (authoritative):**
   `bun scripts/dev/test-cf-do-deploy.ts <template> <slug> --runtime` for
   `todo`, `app`, and `flights`. Stages the template, builds, `wrangler deploy`s
   to real Cloudflare, then `validate-cf-do-runtime.ts` drives two isolated
   browser contexts to check realtime sync, reload persistence, and
   optimistic-add flicker. Requires `CLOUDFLARE_ACCOUNT_ID` +
   `CLOUDFLARE_API_TOKEN` (soot CF account `aa20b480cc813f2131bc005e2b7fd140`).
   Tear down after (omit `--keep`).
4. **Expect a compiler-gap tail.** The chat-e2e cf-do run surfaced two
   zero-1.6-specific pg→sqlite gaps (BIGSERIAL `rank`, the `track` scope bug,
   §6). soot's templates (todo/app/flights) use **different schemas**, so budget
   for new gaps the chat schema didn't exercise — e.g. other SERIAL columns,
   window functions for replica cleanup, default-expression functions. Fix them
   in `~/orez/src/pg-proxy-do-backend.ts` / `src/cf-do/worker.ts`, re-`--into`,
   redeploy. This is open-ended like the chat-e2e tail was.
   ➜ Green here = **orez-cf ready.**

### 7.5 Definition of done (both legs)

- ✅ `test:orez` full (11/11) + `:robust` (5/5) green on 1.6; orez-web bundle on
  protocol v51; `test:ultimate:quick` 28/0.
- ✅ cf-do bundle + unit guard (23/23) green; live deploy+smoke green for
  todo/app/flights; browser runtime sync green (todo).
- soot `bun check` / typecheck: run before merging to main (deploy gate).
- **Committed on `chore/upgrade-zero-1.6` (worktree, not pushed):** orez
  `bef95f8` (browser repl fix), `d2d17e2` + the §7.4 doc; soot `49d50e544`
  (zero 1.6.1 dep bumps + `build-zero-cache.ts` guard fix +
  `scripts/ensure-zero-sqlite.ts` prebuild override).
- **Remaining to ship (user permission):** publish orez (+ bedrock-sqlite) to
  npm (`bun release --patch --ci`), bump soot's `orez` pin off 0.3.9, push the
  branches / open PRs, delete `~/chat`'s temp `bunfig.toml` (post-cooldown).
  Upstream the `ensure-zero-sqlite` prebuild-install fix into `@take-out/scripts`
  so the soot-local override can be removed. (soot needs no temp bunfig — its
  committed `bunfig.toml` already sets `minimumReleaseAge = 0`.)

### 7.6 References in soot

- `docs/cloudflare-do-deploy.md` — cf-do deploy operational reference + validation cmds.
- `docs/orez-architecture.md` — the three runtimes (orez-node / orez-web / orez-cloudflare).
- `AGENTS.md` — points to both; notes the orez ≥0.3.5 transaction-barrier requirement.
- Tests: `test/orez-web-sync.test.ts` (web), `test/cloudflare-do-deploy.test.ts`
  (cf unit), `scripts/dev/test-cf-do-{bundle,deploy}.ts` +
  `scripts/dev/validate-cf-do-runtime.ts` (cf bundle/deploy/runtime).
