# Soot consumer inventory (M4a migration surface)

Status: complete. Read-only inventory of `~/soot` for the rust-sync-server
plan's M4a (Soot production migration, baseline surface). Every claim cites
`file:line` in `~/soot` unless noted.

Related: [rust-sync-server-final-plan.md](./rust-sync-server-final-plan.md)
sections "M4a", "Mutation execution", "Cutover and rollback";
[chat-query-inventory.md](./chat-query-inventory.md) §5 for the shared on-zero
transaction interface (identical `MutatorContext`).

Key framing for M4a: **Soot already runs on an HTTP-pull TypeScript reference
implementation, not zero-cache websockets.** Both planes' clients hold
`transport="http-pull"` today (`src/zero/client.tsx:279`, `:431`). So M4a
replaces the _TypeScript_ pull/push servers (`httpPull.server.ts`,
`httpPullProject.server.ts`) with the Rust engine, per namespace, and the
"current endpoint" the plan compares against is those TS handlers, not a
zero-cache server. The legacy zero-cache routes still exist in the tree
(§E) but the client no longer points at them.

---

## A. The two planes

Soot splits its Zero graph into a **control plane** (one singleton DO
namespace `soot`) and a **project plane** (one DO namespace per project,
`proj-<projectId>`). Shared constant `APP_SCHEMA = \`${ZERO_APP_ID}\_0\``=`soot_0` (`src/zero/httpPull.server.ts:22`).

### A.1 Control plane — `src/zero/httpPull.server.ts`

- Routes: `POST /zero-http/pull` (`app/zero-http/pull+api.tsx`) and
  `POST /zero-http/push` (`app/zero-http/push+api.tsx`).
- Model: **stateless full snapshot every pull** — no change log. Each response
  is `[{op:'clear'}, ...puts]` (module header `:14-20`). This is the "uniform
  project visibility, row-local predicates, proven in production" surface the
  plan says needs nothing from the query-aware layer.
- **18 synced tables** (`snapshotStatements`, `:118-266`): `user`,
  `sootAccount`, `accountMember`, `accountResourceGrant`,
  `accountGithubOrgLink`, `accountRepo`, `userState`, `project`, `workspace`,
  `subscription`, `usageState`, `usageLedger`, `projectAddon`, `planGrant`,
  `tokenUsage`, `deploySlug`, `communityListing`, `communityListingLike`.

Composition semantics:

- **Caps**: per-table `LIMIT` inside the snapshot SQL, no byte budget, no
  change-row cap (no log to bound). `usageLedger` `ORDER BY ul."createdAt"
DESC LIMIT 200` (`:216-217`); `tokenUsage` `... LIMIT 200` (`:236`).
- **Prefix LMIDs**: N/A — LMIDs read in full from the group's `clients` rows
  (`SELECT "clientID","lastMutationID" FROM "${APP_SCHEMA}".clients WHERE
"clientGroupID"=$1`, `:361-364`), folded into `lastMutationIDChanges`
  (`:388-391`). No log prefix, so no prefix-bounded LMID.
- **`visible()`**: no `visible()` abstraction; visibility is inlined per
  statement as WHERE clauses keyed on `$1 = userID`, plus the shared
  full-org-actor predicate `FULL_ORG_ACTOR_SQL` (`:112-114`):
  `(actor.role IN ('owner','admin') OR (actor."accessMode"='all_resources'
AND actor."accessLevel"='full'))`, and project rows gated by
  `projectAccessSql('project','$1')` unless `isDevOpenAccess()` (`:119-121`).
- **Skip/throw classifier**: **does not exist on the control plane** (no
  change log to classify). The only table guard is `toZeroRow`, which throws
  on a table missing from the zero schema:
  `throw new ZeroHttpError(500, \`table ${table} missing from zero schema\`)`
(`:275`).
- **Cookie/watermark**: derived, not a real change clock.
  `deriveZeroHttpPullCookie` (`src/zero/httpPullWatermark.ts:15-38`) =
  `latestSnapshotClock * 1000 + maxLastMutationID`, scanning only
  `createdAt/updatedAt/firstSeenAt/lastActiveAt/billingPeriodStart`
  (`httpPullWatermark.ts:3-9`). Its known incompleteness (a delete that moves
  neither max clock nor max lmid) is why the project-plane node path uses WAL
  instead (`httpPullProject.server.ts:539-550`). **M4a note:** the Rust engine
  replaces this derived cookie with a real watermark, so control-plane cutover
  must reset clients whose old derived cookie can't be represented (plan
  cutover step 5).

### A.2 Project plane — `src/zero/httpPullProject.server.ts`

- Routes: `POST /p-<projectId>/pull` and `/push` via
  `app/[zeroProjectBase]/pull+api.tsx` / `push+api.tsx`, dispatched by
  `projectIdFromPathname` regex `^\/p-([A-Za-z0-9_-]{1,64})\/(?:pull|push)$`
  (`:706-710`).
- **Two backends chosen by `hasProjectNamespaces()`** — a deployment
  constant, not a runtime fallback (`:47-53`, `:591-601`):
  - **CF** (`hasProjectNamespaces()===true`): `cursorPull` — cursor diffs over
    the project DO's `_orez._zero_changes` log with snapshot fallback
    (`:376-499`).
  - **node**: `sharedUpstreamSnapshotPull` — whole-project repeatable-read
    snapshot, cookie = pg WAL LSN (`:551-589`).
- **21 synced tables** (`src/zero/projectTables.ts:8-50`):
  - 16 mutating (`PROJECT_TABLE_NAMES`): `agentEvent`, `attachCommand`,
    `decision`, `deployment`, `file`, `message`, `previewBundle`,
    `previewSession`, `sessionGitBinding`, `snapshot`, `sootAgent`,
    `sootSession`, `sootTask`, `sootTaskComment`, `thread`, `worktree`.
  - 5 query-only (`PROJECT_QUERY_TABLE_NAMES`, no mutation model but synced):
    `integrationPublic`, `iosProjectScreenshot`, `iosProjectSetup`,
    `projectAddon`, `projectSecretPublic`.
  - `project`/`accountResourceGrant` physically appear in a project DO
    (provisioning mirrors them) but are **not** part of the project sync
    surface — their change rows are classified as skipped
    (`projectTables.ts:40-45`).

Composition semantics:

- **Caps**: two budgets (`:93-103`): `CHANGE_ROW_LIMIT = 10_000`,
  `CHANGE_BYTE_BUDGET = 2_000_000` (a soft backlog-pacing budget; `LENGTH()`
  counts characters not transfer bytes). Byte cap is enforced in a two-phase
  read: phase 1 reads only watermarks + image `LENGTH`s and cuts at a
  change-row boundary admitting at least one oversize row (returns the last
  included watermark as the cookie); phase 2 fetches images for the included
  prefix only (`:441-495`, cut logic `:448-459`). Node path has no cap (full
  snapshot).
- **Prefix LMIDs**: yes. Diff-pull LMIDs derive from the included log prefix's
  `${APP_SCHEMA}.clients` rows so an ack never ships in a response whose patch
  excludes its effects: `lmidChangesFromLog` (`:198-210`, used at `:492`).
  Snapshot/node path reads LMIDs up front (`lmidsStatement` + `lmidChanges`,
  `:179-193`, `:520`, `:584`).
- **`visible()`**: applied to snapshots AND diff point-reads. Two
  special-cased tables:
  - `attachCommand` — own rows only: `WHERE "projectId"=$1 AND "userId"=$2`
    (`:232-240`; point-read `:287-293`).
  - `projectAddon` — full-org-actor gate `PROJECT_ADDON_VISIBLE_SQL`
    (`:214-219`), applied in snapshot (`:241-247`) and point-read with
    positional-param rebuild (`:294-303`). An invisible/absent row resolves to
    `del` (`diffRowsPatch`, `:352-364`).
  - Comment `:59-67`: visibility predicates must be **row-local** because
    cursor diffs can't see cross-table flips; `projectAddon` is the lone
    cross-table case and its join table `accountMember` is empty in project
    DOs. **M4a note:** this is the reason Soot needs nothing from the
    query-aware layer — its project-plane predicates are row-local by design.
- **Skip/throw classifier**: exists here. `skipLogTable` (`:144-152`) skips
  `_orez.*`, `${APP_SCHEMA}.clients`, `${APP_SCHEMA}.mutations`,
  `CONTROL_LOG_TABLES` (every zero table not in the project surface,
  `:127-134`), and `LEGACY_LOG_TABLES` (dropped tables still in old logs,
  currently `public.projectMember`, `:140-142`). Anything unmapped-and-not-
  skipped **throws** (`diffRowsPatch`, `:326-333`):
  `throw new ZeroHttpError(500, \`change log row for unmapped table
  '${change.table_name}'\`)`. Rationale (`:137-139`): "a silently dropped
synced change is permanent client divergence." This is exactly the plan's
invariant 10 (explicit skip classifier, throw on unmapped tables) — the Rust
engine must reproduce `skipLogTable`'s skip set and throw arm.

Shared plumbing both planes reuse from `httpPull.server.ts`: `claimStatement`
guarded group→user claim (`:74-90`), `groupOwnersStatement` /
`assertGroupOwnership` (`:92-106`), `toZeroRow` / `toZeroValue` type coercion
(`:271-302`), `withPoolClient` / `withRepeatableRead` (`:312-343`),
`claimPushClientGroup` (`:405-425`), `withPushParams` (`:427-436`).

---

## B. Mutator registry

- Entry point: `src/zero/server.ts:13-21` builds `zeroServer` via
  `createZeroServer({ schema, models, createServerActions, queries,
mutations: mutationValidators, database: server.ZERO_UPSTREAM_DB,
defaultAllowAdminRole: 'all' })`.
- Sources: `models` = `src/data/generated/models.ts` (24 namespaces,
  auto-generated), each importing `src/data/mutations/<name>.ts`; validators =
  `src/data/generated/syncedMutations.ts` (`mutationValidators`). Client-side
  control/project partition: `src/zero/core.ts:59-88`.
- Handwritten mutator source: `src/data/mutations/*.ts` (24 files) + `helpers/`.

### B.1 Mutator names + args

Every namespace has CRUD `insert/update/upsert/delete` (auto-added by on-zero
`mutations()`). Authoritative arg shapes: `syncedMutations.ts`. Custom
(non-CRUD) mutators with validator line refs:

- `agentEvent`: `claimReviewLease` (`:16-24`).
- `attachCommand`: `claim` (`:48-50`), `complete` (`:51-88`, deeply-nested
  `result` union), `fail` (`:89-92`).
- `communityListing`: `like` (`:118-121`), `unlike` (`:122-125`),
  `recordInstall` (`:126-129`).
- `decision`: `update` (answered, `:148-154`).
- `deployment`: `approveAppStoreSubmit` (`:179-182`), `seedIosHardeningFixture`
  (`:183-187`).
- `deploySlug`: `reserve` (`:208-212`).
- `message`: `sendMainBean` (`:245-251`), `deleteBefore` (`:252-256`).
- `project`: `fork` (`:314-324`), `markSeeded`, `setHeadlessRuntimeEnabled`,
  `markHeadlessRuntimeHeartbeat`, `setFactoryModelSettings`,
  `setFactoryBehaviorSettings` (`:332-358`).
- `sootAgent`: `ensureThread`, `assignTask`, `requestChanges`, `unassign`,
  `interruptAgent`, `submitForReview`, `recoverTaskForRetry`,
  `recoverErroredAgent` (`:447-550`).
- `sootSession`: `createWithWorktree` (`:576-590`).
- `sootTask`: `approveAndMerge` (`:659-668`).
- `userState`: `update {userId,currentProjectId}` (`:764-767`).
- `workspace`: `update` layout/editorState/preferences (`:776-810`).

CRUD-only namespaces: `file`, `previewBundle`, `previewSession`,
`sessionGitBinding`, `worktree`, `snapshot`, `thread`, `subscription`,
`tokenUsage`, `user` (`subscription`/`tokenUsage` are written only by Stripe
webhooks / entitlements, never by clients — `subscription.ts:3-4`,
`tokenUsage.ts:4`).

### B.2 Table-write matrix (verified by grep of `tx.mutate.<table>.<op>`)

A mutator namespace often writes tables beyond its own — the DO-local
transaction must let a single mutator touch several tables atomically:

| Mutator file     | Tables written                                | External effect          |
| ---------------- | --------------------------------------------- | ------------------------ |
| agentEvent       | agentEvent                                    | —                        |
| attachCommand    | attachCommand                                 | —                        |
| communityListing | communityListing, communityListingLike        | —                        |
| decision         | decision                                      | —                        |
| deploySlug       | deploySlug                                    | —                        |
| deployment       | deployment, iosProjectSetup                   | `enqueueZeroAsyncAction` |
| message          | message, thread                               | —                        |
| project          | project, message, sootAgent, sootTask         | `enqueueZeroAsyncAction` |
| snapshot         | snapshot                                      | —                        |
| sootAgent        | sootAgent, message, sootTask, sootTaskComment | `enqueueZeroAsyncAction` |
| sootSession      | sootSession, workspace, worktree              | —                        |
| sootTask         | sootTask, sootAgent, sootTaskComment          | `enqueueZeroAsyncAction` |
| sootTaskComment  | sootTaskComment                               | —                        |
| thread           | thread                                        | —                        |
| userState        | userState                                     | —                        |
| workspace        | workspace                                     | —                        |

### B.3 External side effects

All external effects funnel through `enqueueZeroAsyncAction(server, action)`
in `src/data/mutations/helpers/asyncActions.ts`, which pushes onto
`server.asyncTasks` (`:75-77`) and dispatches via a global
`__soot_zero_async_action_dispatch`. **Five typed actions** (`:10-48`):
`project.seedTemplate`, `project.forkFiles`, `agent.startWork`,
`branch.mergeApprovedTask`, `deploy.triggerBuild` (spawns a docker build).
Call sites: `deployment.ts:84`, `project.ts:150,224`, `sootTask.ts:386`,
`sootAgent.ts:321`. Additional server-action effects come from
`createServerActions` (`src/data/server/createServerActions.server.ts`) and
`helpers/seedProjectFiles*.ts` (lazy import of `~/data/server/actions/
fileActions`). All run **post-commit** (invariant 17).

### B.4 The on-zero transaction interface (DO-local adapter contract for M4a)

Each mutator is `(ctx, args) => Promise<void>` with `ctx: MutatorContext`
(`node_modules/on-zero/src/types.ts:68-77`, identical to Chat's — see
chat-query-inventory.md §5.1):

```ts
MutatorContext = {
  tx: Transaction            // ZeroTransaction<Schema> (types.ts:49)
  authData: AuthData | null
  environment: 'server' | 'client'
  server?: { actions: ServerActions; asyncTasks: Array<() => Promise<void>> }
  can: Can
}
```

Required surface (observed in soot mutators):

- `tx.mutate.<table>.{insert,update,delete}(row)` — `deployment.ts:62`,
  `agentEvent.ts:37`.
- `tx.run(zql.<table>.where(...).one())` reads (`zql` from on-zero) —
  `deployment.ts:158,170,246`, `agentEvent.ts:34,57`.
- `ctx.server.asyncTasks.push(...)` post-commit effects (via
  `enqueueZeroAsyncAction`, `asyncActions.ts:75`).
- `ensureLoggedIn()` / `can` read the ambient auth scope.

**The seam M4a plugs into** is the browser/DO server shim
`src/worker/stubs/on-zero-server.ts` (`createZeroServer` there):

- `handleMutationRequest` (`:62-85`) builds mutators via `createMutators({
asyncTasks, can, createServerActions, models, authData, validateMutation,
mutationValidators })` and runs `processor.process(mutators, request)` where
  `processor = new PushProcessor(zeroNodePg(schema, externalPool))` (`:54-60`).
- It **requires an external pool** — `throw '[on-zero/server browser shim]
database strings are not supported; pass an external pool'` (`:47-52`). That
  external pool is the seam a DO-local (or Rust-backed) SQLite backend plugs
  into.
- `transaction()` (`:137-151`) reuses the in-flight `mutatorContext().tx` when
  already inside a mutation, else opens `zeroDb.transaction(...)`. Push preflight
  ordering/replay/LMID (which the plan moves into Rust) currently lives in
  `PushProcessor` (`src/worker/stubs/zero-server`).

For M4a, the Rust ordering preflight/finalization wraps this: Rust validates
ownership/replay/LMID, the TS mutator runs against a DO-local SQLite `tx`,
Rust records LMID + change markers, commit, then `asyncTasks` run.

---

## C. Auth / namespace resolution

### C.1 Session → userID — `src/auth/getAuthData.server.ts`

- `getAuthData(request)` wraps resolution in `runInControlNamespace(...)` so
  the session/user lookup hits the singleton control DO even for a
  project-scoped request (`:22-31`).
- Cookie path: reads `better-auth.session_token` / `__Secure-` / `__Host-`
  (`:78-82`), splits `token.signature`, HMAC-verifies with
  `BETTER_AUTH_SECRET` via `timingSafeEqual` (`betterAuthSignature`, `:92-102`).
- Bearer path: `Authorization: Bearer <token>` (`:42-52`).
- Both resolve via `authDataForSessionToken` — the load-bearing SQL
  (`:118-125`):
  `SELECT s."userId", u.email, u.role FROM session s JOIN "user" u ON
u.id=s."userId" WHERE s.token=$1 AND s."expiresAt" > now() LIMIT 1`.
  A failed lookup **throws** rather than returning null (documented anti-wedge
  behavior, `:44-51`, `:126-129`).
- **Normalized claims passed to the engine** (plan's "auth handoff"): `{ userId
(→ id), email, role }`. Anonymous identity: signed `soot_anon_id` cookie
  (`src/auth/anonCookie.server.ts`, IDs shaped `anon-<hex>`, `:12-14`);
  endpoints fall back to anon when no session (control `app/zero-http/
pull+api.tsx:17-18`; project `httpPullProject.server.ts:664-669`).

### C.2 userID → namespace (DO / database)

- `src/zero/withProjectNamespace.ts`: `withProjectNamespace(projectId, fn)`
  runs under `__soot_run_in_ns('proj-' + projectId, fn)` when the CF shim
  global is present, gated by `/^[A-Za-z0-9_-]{1,64}$/`; else pass-through
  (`:17-29`). Namespace name = **`proj-<projectId>`**.
- `runInControlNamespace(fn)` = `__soot_run_in_ns('soot', fn)` — `'soot'` is
  the control namespace mapped to the singleton DO (`:43-51`).
- Pool selection (`src/database/db.server.ts`): `projectDb(projectId)` returns
  `__soot_cf_project_pool(projectId)` on CF else `db.pool` (`:44-48`);
  `hasProjectNamespaces()` = `typeof __soot_cf_project_pool === 'function'`
  (`:55-60`); `db` built from `__soot_cf_do_create_pg_pool('orez-do://postgres')`
  on CF or `ZERO_UPSTREAM_DB` on node (`:14-33`).
- **Project membership gate** (session → project authorization):
  `src/project/projectStorageAccess.server.ts` —
  `resolveProjectStorageAccess(userId, projectId)` with a 5 s TTL cache +
  inflight dedup (`:20-48`), backed by `resolveProjectStorageAccessWithQuery`
  in `projectStorageAccess.shared.ts`. Both project pull and push re-check it:
  `httpPullProject.server.ts:678-679` → `403 'not a member of this project'`
  (revocation story `:671-677`). The CF namespace-routing escape this guards
  is reproduced in `test/zero-control-namespace-escape.test.ts`.

---

## D. Integration tests + Cloudflare runtime validators (what M4a re-points)

### D.1 Project-plane integration test

- `src/zero/httpPullProject.test.ts` — the primary integration suite. Drives
  the exported test seams `__testCursorPull` / `__testSharedUpstreamSnapshotPull`
  (`httpPullProject.server.ts:696-699`) against an in-memory `bun:sqlite`
  emulation of a project DO (`makeDb`, `:21-80`; stub pool with a
  between-statements race hook, `:108-142`). Covers fresh snapshot, diff
  coalescing, prefix LMIDs, unchanged/409, pruned-gap→snapshot, byte-cap cut,
  two RACE tests (append/purge between phases), `attachCommand`/`projectAddon`
  visibility, control/legacy skip + unknown-table throw, group-ownership.
  Node-mode block `:381-442` includes the WAL-cookie delete-of-non-newest-row
  test.
- Run: `bun src/env.ts -- bun test src/zero/httpPullProject.test.ts` (header
  `:14`; neutralizes `server-only` via `mock.module`, `:6`; dummy
  `ZERO_UPSTREAM_DB`, `:16`).

### D.2 Control-plane tests

- `test/zeroHttpPullWatermark.test.ts` — unit tests for
  `deriveZeroHttpPullCookie`.
- `test/zeroServerSqlConvert.test.ts` — `executePostgresQuery` SQL conversion
  in the browser zero-server stub.
- `test/zero-control-namespace-escape.test.ts` — `runInControlNamespace` /
  `withProjectNamespace` DO routing.
- Other zero tests in `test/`: `zero-async-action-route.test.ts`,
  `zeroMutationResult.test.ts`, `zeroStalePokeRecovery.test.ts`,
  `zeroClientDataError.test.ts`, `zero-push-diagnostics.test.ts`,
  `zeroProviderDisable.test.ts`, `zeroFatalLocalStore.test.ts`,
  `zeroAuthUserId.test.ts`.
- **Gap (flagged):** there is no dedicated `handleZeroHttpPull` control-plane
  _pull_ integration test parallel to `httpPullProject.test.ts` — only the
  watermark unit test and indirect references (`test/headlessFactorySupervisor
.test.ts`, `test/cloudflare-do-deploy.test.ts`). M4a's control-plane
  conformance should add one when pointing at the Rust target.

### D.3 Cloudflare runtime validators + DDL/deploy composition

- `test/cloudflare-do-deploy.test.ts` (56 KB) — main CF runtime/deploy
  validator, imports `src/deploy/cloudflareDoDeploy.ts`. Validates
  `sanitizeWorkerName`, `normalizeCloudflareDoWranglerConfig`,
  `CLOUDFLARE_DO_SHIM_SOURCE`, `CLOUDFLARE_DO_APP_SHIM_SOURCE` per-project ns
  routing (asserts a `/p-<id>/(sync|replication|mutate)/v<n>/` matcher;
  forwards `/p-...` sync/mutate to the data worker; treats non-zero `/p-`
  paths as app traffic — `:1034`, `:1154-1363`), the authoritative push
  forwarded to `/api/zero/push` (`:1363-1414`), and the zero-http shard DDL
  repair (`zeroHttpShardDDL('soot')`, `:1044-1101`).
- **DDL/deploy composition**: `src/deploy/cloudflareDoDeploy.ts` (1641 lines).
  `zeroHttpShardDDL` at `:384`; `zeroHttpShardBatchStatements` via
  `deployTimeSchemaBatchStatements` imported from `orez` at `:481-495` — i.e.
  the deploy-time per-namespace schema batch (the `_zsync_*`/`soot_0` DDL) is
  produced by orez and applied per project DO. M4a's per-namespace init writes
  the Rust engine's `_zsync_*` tables here.
- `test/cf-demo-deploy.test.ts` — demo-deploy config validator.
- Split-worker push validator: `src/zero/cloudflareDataPush.server.ts`
  (`applyDataWorkerZeroPush`); shim tests in
  `packages/orez-cf-deploy/src/shims.test.ts`.
- **Upstream conformance gate** for cursor-pull semantics is external to soot:
  orez `src/sync-server/sync-server.ts` + `src/cf-do/cursor-pull.ts`, run by
  the orez harness lanes on branch `zero-sync-server`
  (`plans/sootbean/zero/zero-http-project-plane.md:8-12`,
  `httpPullProject.server.ts:36-39`). This is the reference the Rust engine is
  replacing.

---

## E. Cutover surface (what a per-namespace flip touches, what gets deleted)

### E.1 Routes (three sync paths coexist in the tree today)

- **Legacy zero-cache transform hop (websocket path, client no longer uses):**
  `app/api/zero/pull+api.tsx` (`zeroServer.handleQueryRequest`, `:14`),
  `app/api/zero/push+api.tsx` (`handleMutationRequest`),
  `app/api/zero/async-action+api.tsx`.
- **Control-plane http-pull:** `app/zero-http/pull+api.tsx`, `push+api.tsx`.
- **Project-plane http-pull:** `app/[zeroProjectBase]/pull+api.tsx`,
  `push+api.tsx`.

### E.2 Client sync-path selection — `src/zero/client.tsx` (what a cutover flips)

Both instances are hardwired to `http-pull` today:

- Control: `controlTransport = bootstrapUserGraph ? ('http-pull') : undefined`
  (`:233`), `controlServer = \`${APP_ORIGIN}/zero-http\`` (`:215-217`), mounted
`:277-282`.
- Project: hardcoded `transport="http-pull"`, `pullIntervalMs={15_000}`,
  `cacheURL={projectId ? \`${APP_ORIGIN}/p-${projectId}\` : cacheURL}`
(`:431-433`).
- **Stale comment (flagged):** `:270` still says the project instance "keeps
  the websocket path" — contradicted by the actual `transport="http-pull"`
  prop at `:431`. Treat the code as authoritative: both planes are http-pull.
  M4a enables the plan's wake channel for the project plane here (replacing the
  15 s poll as the propagation mechanism).
- `DISABLE_ZERO` (URL param `?disableZero`, `src/constants/urlParams.ts:13`)
  points cacheURL/controlServer at a dead `http://127.0.0.1:19999`
  (`client.tsx:210-217`) — a kill switch, not a transport toggle.
- Origins: `APP_ORIGIN`, `ZERO_ORIGIN` in `src/constants/env-client.ts:12-19`.

### E.3 Env vars / deployment constants

- `ZERO_APP_ID: 'soot'` (`src/env.ts:107`) → `APP_SCHEMA='soot_0'` on both
  planes.
- `ZERO_UPSTREAM_DB` (`env.ts:133` dev / `:160` prod-blank) — node pg upstream;
  stripped on CF (`db.server.ts:9-22`).
- **`ZERO_MUTATE_URL` / `ZERO_QUERY_URL`** (`env.ts:140-141`) point the
  zero-cache child at `/api/zero/push` and `/api/zero/pull` — these wire the
  **legacy zero-cache path** and are deletion candidates once no plane uses
  zero-cache. Blank in prod (`env.ts:163-164`).
- `ZERO_CVR_DB`, `ZERO_CHANGE_DB`, `ZERO_NUM_SYNC_WORKERS`,
  `ZERO_APP_PUBLICATIONS: 'zero_soot'` (`env.ts:108`, `:134-136`, `:158-162`),
  `ZERO_MUTATE_FORWARD_COOKIES` / `ZERO_QUERY_FORWARD_COOKIES`
  (`env.ts:109-110`) — zero-cache infra, deletable once no plane uses
  zero-cache.
- `SOOT_DEV_OPEN_ACCESS` (`env.ts:121`) → `isDevOpenAccess()` drops the
  control-plane project-visibility WHERE (`httpPull.server.ts:119-121`).
- **The per-namespace backend selector is not an env var** — it is the
  deployment-constant `hasProjectNamespaces()` (presence of
  `globalThis.__soot_cf_project_pool`, `db.server.ts:55-60`) plus the CF shim
  globals `__soot_cf_do_create_pg_pool`, `__soot_cf_project_pool`,
  `__soot_run_in_ns`, `__soot_background_task` installed by
  `packages/orez-cf-deploy`. M4a's per-namespace cutover flips _which engine_
  the namespace routes to, at this seam.

### E.4 Eventually-deleted surface (M4a/M6)

Per the depth-seam plans, the `env.APP.fetch` push re-entry / legacy
`/api/zero/push` app-worker hop is being replaced by an in-data-worker lean
push processor (`plans/sootbean/zero/custom-mutator-depth-seam-2026-07-09.md:
91-105`, `:312-336`); `/api/zero/pull` stays on the app worker for now
(`:116-119`). PR #55 (`platform-depth-seam-impl-2026-07-09`) implements the
lean push server; per the handoff it is Fable-approved on code but **not yet
preview-proven or deployed** (`custom-mutator-depth-seam-handoff-2026-07-09.md:
1-29`, `:226-237`). At M4a completion the deleted surface = the legacy
zero-cache routes + env + the TS `httpPull.server.ts` / `httpPullProject
.server.ts` reference handlers the Rust engine replaces (plan M4a exit gate:
"Soot's old Zero server path is deleted").

---

## F. `plans/sootbean/zero/` index

10 files; migration-relevant:

- `zero-http-project-plane.md` — project-plane http-pull design/contract
  (status BUILT 2026-07-09); names all code files and the `/p-<id>/pull|push`
  → One-router fall-through routing fact.
- `custom-mutator-depth-seam-2026-07-09.md` — the CF subrequest-depth push
  seam (lean data-worker push processor, bundle budget <1 MB gzip,
  banned-import guard).
- `custom-mutator-depth-seam-handoff-2026-07-09.md` — handoff/status for
  PR #55 (lean push graph 69 modules / 59,546 gzip / 0 banned; agentEvent as
  canonical probe; not yet deployed).
- Background/research (not read in full): `onzero-self-heal-recovery.md`,
  `zero-first-api-route-audit.md`, `zero-first-orchestration.md`,
  `zero-first-research.md`, `zero-mutator-cleanup.md`,
  `zero-stack-dry-cleanup.md`, `d1-vs-do-sqlite-orez.md`.

---

## G. Gaps / ambiguities (not guessed)

1. No dedicated `handleZeroHttpPull` control-plane **pull** integration test
   (parallel to `httpPullProject.test.ts`); only the watermark unit test +
   indirect references. M4a control-plane conformance should add one.
2. `client.tsx:270` comment describes the project instance as websocket-based;
   the actual prop (`:431`) is `transport="http-pull"`. Code is authoritative.
3. No single env var toggles zero-cache vs http-pull; selection is client
   transport props (`client.tsx`) + deployment constant
   (`hasProjectNamespaces()`) + CF shim globals. `ZERO_MUTATE_URL` /
   `ZERO_QUERY_URL` still wire the legacy zero-cache path.
4. The control-plane cookie is _derived_ (`deriveZeroHttpPullCookie`), not a
   real change clock; cutover to the Rust real-watermark engine may not
   represent an old derived cookie safely, so control-plane clients likely
   reset on cutover (plan cutover step 5).
