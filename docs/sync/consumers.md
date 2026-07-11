# Consumers and integration guide

Two real apps run on the sync host, and they sit on opposite sides of the push
fork. Reading both is the fastest way to understand the choice you make when you
integrate a new app.

- **Chat** (start.chat, live) uses local mutators. Its mutation logic runs inside
  the Durable Object.
- **Soot** (mid-cutover) uses delegated push with upstream ingest. Its mutations
  run in the app worker and the host replicates from the data feed.

Both share the identical query-permission path: they delegate the query
transform to the app's real synced-queries endpoint. They differ only on where
writes happen.

## Chat: local mutators

Chat's integration lives in its `rust-sync/` directory, split into
`chat-config/` (the registry) and `chat-host/` (the workerd composition and
wrangler). The config is a single exported object (`rust-sync/chat-config/index.ts`):

```ts
export const chatConfig: SyncHostConfig = {
  hostVersion: 'chat-rust-sync-0.1',
  schema: chatSchema,
  queryAware: true,
  resolveQuery: (name, args, claims, env) => resolveQuery(name, args, claims, env),
  queryTransformVersion: 1,
  mutators: chatMutators, // DO-local push, no upstream
  initialize(sql) {
    initializeChatSchema(sql)
  },
  async authenticate(request, env) {
    /* resolve better-auth session */
  },
  namespace(request) {
    return new URL(request.url).pathname.split('/')[1] || null // one DO per serverId
  },
}
```

The pieces:

- **Push is DO-local.** `chatMutators` is built by reflecting over the app's
  on-zero model definitions and registering each mutator under its canonical
  wire name (for example `message|insert`), so mutations run authoritatively
  inside the DO's SQLite transaction. There is no `upstream` block and no
  `mutateUrl`.
- **`resolveQuery` delegates.** It POSTs a `transform` request to the Chat app
  worker's real `/api/zero/pull` over the `APP` service binding and returns the
  permission-transformed AST. The client's bearer token rides in claims and is
  forwarded on that subrequest.
- **`authenticate` delegates.** It reads the `Authorization: Bearer` session
  token, resolves it against the app's `/api/auth/get-session` over the `APP`
  binding, and caches the token-to-claims result briefly.
- **`namespace` is per-server.** The first path segment is the server id, so
  Chat runs one Durable Object per Chat server.
- Chat leaves `caps`, `retainChanges`, `idleTeardownMs`, `wakeCoalesceMs`, and
  `visibility` at host defaults.

The composition worker (`rust-sync/chat-host/src/worker.ts`) exports
`createSyncDurableObject(chatConfig)` as the DO class and wraps
`createSyncWorker(chatConfig)` in a CORS layer that passes WebSocket upgrades
through untouched. The deploy wrangler binds `APP` to the staging app worker and
points `SYNC_DO` at the DO class.

Chat is live. The staging deploy record (chat's
`plans/cf-orez-migration-run.md`, "Rust sync host staging deploy") documents the
host at `chat-rust-sync-host.natewienert.workers.dev` bound to the app worker,
with verification results: UI sends durable across cold reload, 10,000 of 10,000
pushes across 16 concurrent writers, the DO holding 10,008 message rows, cold
named-query pulls returning 200 at limits up to 2,000, and cross-context live
wake delivery in about 1.2 seconds.

## Soot: delegated push with ingest

Soot's integration lives in `integrations/soot-rust-sync/`. Its config is a
factory so the authenticator can differ between test and production
(`src/config.ts`):

```ts
export function createSootSyncConfig(authenticate) {
  return {
    hostVersion: 'soot-rust-sync-0.2',
    schema: sootZeroSchema,
    mutateUrl: '/api/zero/push?schema=soot_0&appID=soot', // delegated push
    mutateBinding: 'APP',
    upstream: {
      binding: 'DATA',
      namespacePath: upstreamPathForNamespace, // ns -> /soot or /proj-<id>
    },
    initialize: initializeSootSchema,
    authenticate,
    namespace: namespaceForRequest,
    queryAware: true,
    resolveQuery: (name, args, claims, env) => resolveSootQuery(name, args, claims, env),
    queryTransformVersion: 1,
    caps: { maxChangeRows: 10_000, maxChangeBytes: 2_000_000 },
    retainChanges: 4_096,
    idleTeardownMs: 5_000,
    wakeCoalesceMs: 25,
  }
}
```

The pieces:

- **Push is delegated.** The client push is forwarded to
  `/api/zero/push?schema=soot_0&appID=soot` over the `APP` binding, and the DO
  then ingests the committed change stream from the `DATA` binding's feed at
  `upstream.namespacePath`. Soot has no local mutator registry at all. Its
  projections and jobs keep writing through the app's real push endpoint, and
  clients read what ingest replicates.
- **Two service bindings.** `APP` (used by push, auth, and the query transform)
  and `DATA` (the change feed). Chat needs only `APP`.
- **`namespace` has planes.** `soot` and `zero-http` map to the control-plane
  namespace; `proj-<id>` and `p-<id>` map to a project namespace. Claims carry a
  `plane` discriminator plus project id and role.
- Soot sets `caps`, `retainChanges`, `idleTeardownMs`, and `wakeCoalesceMs`
  explicitly rather than taking defaults.

Soot is mid-cutover. The default deployable worker (`src/worker.ts`) still wires
a test authenticator gated by `SOOT_SYNC_TEST_AUTH`, so the primary artifact is
a harness. The production path exists (`src/production-worker.server.ts` plus
`src/production-auth.server.ts`, which resolves identity and project membership
over the app's `/api/zero/rust-auth`) but is referenced only by the deploy
wrangler. The real production auth is written and quarantined behind the
deploy-only entrypoint; the everyday worker ships test auth.

## How they differ

| Aspect           | Chat (live)                     | Soot (mid-cutover)                      |
| ---------------- | ------------------------------- | --------------------------------------- |
| Push             | DO-local `mutators`             | Delegated `mutateUrl` + `mutateBinding` |
| Upstream ingest  | none                            | `upstream` from the `DATA` feed         |
| Service bindings | `APP` only                      | `APP` and `DATA`                        |
| Query transform  | app `/api/zero/pull` over `APP` | app `/api/zero/pull` over `APP` (same)  |
| Auth             | app `/api/auth/get-session`     | app `/api/zero/rust-auth`               |
| Namespace        | one DO per server id            | control and project planes              |
| Tuning           | all default                     | explicit caps, retention, idle, wake    |

The reason Chat is local-mutator while Soot is delegated is timing. When Chat
deployed, the engine had no upstream-ingest mode, so a delegated push would have
split write authority. Soot's branch is the newer path where upstream ingest was
added, which is what makes delegated push safe. Both keep the identical
query-permission delegation.

## Integrate a new app

Distilling both, a new app provides four things.

### 1. A `SyncHostConfig`

Required for every app: `hostVersion`, `schema` (derived from your Zero schema),
`initialize(sql)` for your DDL, `authenticate(request, env)` returning
`{userID, ...}`, and `namespace(request)` returning the DO partition key. Carry
the raw client token into claims so `resolveQuery` can forward it.

Then choose exactly one push model:

- **Local** (like Chat): `mutators` built with `registerMutators({...})`, keyed
  by wire mutation name. No `upstream`. Right when your entire write surface can
  live in the host.
- **Delegated** (like Soot): `mutateUrl` (an absolute path on the app),
  `mutateBinding` (defaults to `upstream.binding`), and a required `upstream:
{binding, namespacePath}`. Right when writes must run in the app worker, when
  server-side effects (jobs, projections, notifications) run outside the sync
  path, or when the app already owns a real push endpoint.

For query-aware pulls (both apps use this): `queryAware: true`, a `resolveQuery`
that POSTs a transform request to your app's `/api/zero/pull` and returns the
AST, and a `queryTransformVersion` epoch you bump on permission or schema
changes.

### 2. A workerd composition entry

A `worker.ts` that exports `createSyncDurableObject(config)` as the DO class and
default-exports a fetch handler wrapping `createSyncWorker(config)` with a CORS
layer (handle preflight, echo headers, pass 101 upgrades through).

### 3. Wrangler config

- A `SYNC_DO` Durable Object binding pointing at your DO class, with a
  `[[migrations]]` `new_sqlite_classes` entry.
- A `[[services]]` `APP` binding to the app worker (used by `authenticate`,
  `resolveQuery`, and delegated `mutateUrl`).
- For delegated push, a second `[[services]]` `DATA` binding for the change
  feed, matching `upstream.binding`.
- Vars for the app origin and any admin or test flags. Import either the
  published `orez-sync-cf-host` (as Soot does) or alias the bare import to the
  orez source (as Chat does).

### 4. App-worker endpoints

The bound `APP` service must expose:

- A session or identity endpoint for `authenticate` (`/api/auth/get-session` or
  `/api/zero/rust-auth`).
- The query transform endpoint `/api/zero/pull`, which accepts a `transform`
  request and returns the permission-transformed AST.
- For delegated push only: a push endpoint (`/api/zero/push`), plus a change
  feed served at each namespace's `upstream.namespacePath` on the `DATA`
  service.
