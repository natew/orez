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
      ➜ Passing this is **orez-node ready.**
4. **orez-web** — the browser/in-process embed path (`browser-embed.ts`, the
   wasm sqlite shim). Validate after orez-node.
5. **orez-cf** — the Cloudflare Durable Object backend (`src/cf-do/`,
   `src/worker/` overlay). **Requires coordination with `~/soot`** (soot is the
   downstream CF consumer). Validate the overlay build + a real DO deploy.

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

- `bun run test` (unit): **656/657** (see embed-integration note below).
  tsc/lint/format/`cf-patches.test.ts` green.
- `bun run test:integration`: **21/21** after the two reset fixes below.

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
   of data size (tiny stress data reproduced it too). **Not CI-gated** (CI runs
   `test:integration:native` — native-startup guard only, not this file), but
   now passes locally 3/3 with the default stress config.

#### Open follow-on (1.6)

1. **`embed-integration.test.ts` "insert triggers poke" — full-suite-only
   flake.** Passes in isolation, in the `src/worker/` group, and with the
   `src/replication/` group; only times out (poke >30s) in the full 39-file
   `bun run test`. `fileParallelism:false` reuses one vitest worker; the
   in-process change-streamer is hosted in that worker, which is bloated by
   ~file 35 (note the `MaxListeners: 11 uncaughtException listeners` warning —
   zero's `createLogContext` adds one per embed). CI was green here on 1.5
   (`13b685b`), so this is load-sensitive, likely green on a fresh CI runner.
   A real fix would isolate embed-integration in its own vitest pool.
