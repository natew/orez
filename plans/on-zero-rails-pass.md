# on-zero rails pass

Goal: soot's `src/zero/` (and every future consumer's equivalent) shrinks to
client composition + server composition + visibility policy. Everything
mechanical moves into on-zero (agnostic DX layer, the "ActiveRecord") or orez
(the Zero server, owns transport/protocol mechanics). App code keeps only
policy: who sees what, who may wake what, rate limits, effect bodies.

Seam rule: on-zero is agnostic (works against zero-cache or orez) and provides
opinions/helpers/patterns. Orez mimics Zero server-side and owns everything
protocol-shaped: HTTP request handling, wake tokens, push diagnostics, naming
conventions.

Reference for what we are deleting: `~/soot/src/zero/*` (read the files; the
line counts below are what the consumer currently hand-rolls).

## on-zero features (packages/on-zero — imported in phase 1, unpushed)

1. Background mutation lifecycle (replaces soot queuedMutation.ts 215 +
   recoveryGeneration.ts 163 + mutationResult.ts 160):
   - on-zero already owns recovery (log classification, store deletion,
     scheduleReload). Own the GENERATION too: when recovery begins or the
     instance closes, in-flight and later background mutations resolve as
     typed no-op rejections (StaleGeneration), never console floods. No
     app-side fencing API — it is internal.
   - `enqueueBackgroundMutation(label, create, { coalesceKey?, settle?:
'client' | 'server', timeoutMs? })` as an on-zero export: serial queue
     per instance, coalesceKey collapses superseded same-key writes
     (streaming transcript rows), settle default 'client', error dedup.
   - Awaitable settle helpers: awaitMutationClient / awaitMutationServer with
     timeouts and typed error extraction (soot mutationResult.ts is the spec).
   - Consecutive server-ack-timeout desync detection moves INTO on-zero's
     ConnectionMonitor: N consecutive server-ack timeouts (default 2) is a
     recovery trigger like any other; a single timeout never recovers. Kills
     soot's synthetic zeroEvents error marker + string matching for it.
2. Typed events instead of string classification (replaces soot
   clientHelpers.ts): every internal classification surfaces a typed
   reasonKey on zeroEvents / scheduleReload context. Consumers switch on
   enums. The benignLogFilter prop is replaced by transport-provided
   classifications (orez transport knows its cold-boot timeout is benign) plus
   an optional app list of benign patterns as data, not code.
3. Async actions as config (replaces soot zeroAsyncActions.server.ts +
   asyncActions.ts globalThis handshake): createZeroServerBindings accepts
   `actions: { execute(action), dispatchRemote?(action) }` (naming free to
   improve). enqueueTask keeps working; a typed action envelope routes to the
   local executor or the injected remote dispatcher. No globalThis install, no
   consumer-invented secret-header protocol; the CF service-binding hop is a
   dispatcher implementation the consumer passes in.
4. Instance partition via folder structure (replaces the four hand lists +
   three assertions in soot core.ts and the hand-kept sync-surface lists in
   projectTables.ts). Convention over configuration:
   - `src/data/<instance>/queries|mutations/*` — the folder IS the partition
     declaration; a namespace lives in exactly one instance by construction.
     Optional `instance.ts` per folder for config (scope column, namespace
     derivation like `proj-<id>`).
   - No instance folders (flat `src/data/queries`) = one default instance;
     single-instance apps like takeout pay nothing. Same mechanism, not a
     second layout.
   - Generate emits per-instance groupedQueries/models plus the combined
     multi-client wiring; the module-eval partition assertions die.
   - The instance SYNC SURFACE is derived, not declared: the closure of
     tables reachable from that instance's query ASTs (related() traversal)
     covers related-only tables (soot's PROJECT_QUERY_TABLE_NAMES problem).
     Server pull endpoints and visibility partitions consume the generated
     surface.
   - Cross-instance reach (a project query related()-ing a control table)
     fails at generate time, not as an empty hydration in prod.

## orez features

5. Request-level mount seam (replaces soot zeroHttpRequest.server.ts 56 +
   most of pushHandler.server.ts 84 + route bodies):
   `createZeroHttpMount({ pathPrefix, server, authenticate(request, route) =>
claims | Response, beforePush?(request, bodyText) => Response | null })`
   gains `handleRequest(request): Promise<Response | null>`: match, parse
   JSON, run authenticate, run beforePush (rate limit hook), call
   handlePull/handlePush, map ZeroHttpRequestError/SyncExecutorRequestError to
   status responses, no-store header on pull. Also relax pathPrefix so a glued
   prefix like `/p-` is expressible (soot projectZeroHttp matcher collapses
   into this). The consumer route becomes: `mount.handleRequest(request) ??
notFound`.
6. Wake capability tokens (replaces soot wakeCapability.server.ts 103 and chat
   wakeTokenClient.ts / orezCallbackSecret.server.ts):
   - `orez/wake` exports Web Crypto based mint/verify for the versioned short-TTL
     HMAC capability the client transport's `wake.getToken` hook carries. Config:
     secret, namespace, identity, ttl. App keeps only the authorization decision.
   - `orez/wake` also exports a CF-safe timing-safe shared-secret header verifier
     for authenticated callbacks and service bindings.
   - `orez/client` exports `createWakeTokenFetcher(tokenURL, fetchInit)` for the
     standard POST-and-validate-token flow. Static or async request init supports
     cookie, Bearer, credentials, and namespace-in-body or namespace-in-path auth.
7. Structured push diagnostics (replaces soot pushDiagnostics.ts 288):
   sync-executor/zero-http push path emits a structured failure/mutation-error
   summary (request summary with arg allowlist, failure kind, per-mutation
   errors) through an optional `diagnostics` callback. Consumers log it;
   nobody re-parses push bodies.
8. Naming conventions (replaces soot appIdentity.ts): export
   `internalSchema(appId)` (currently `${appId}_0`) and publication naming so
   consumers never re-derive them.

## consumer cleanup (soot, after the above land)

- Delete queuedMutation/recoveryGeneration/mutationResult/clientHelpers/
  pushDiagnostics/wakeCapability/zeroHttpRequest + async-action plumbing;
  consume the new APIs.
- Move anonBootstrap.ts to src/features/auth (not zero code).
- core.ts partition lists -> generated.
- zeroVisibility.ts and route-level policy stay app-owned.

## constraints

- No pushes anywhere without the user's explicit permission (orez main pushes
  auto-publish canaries).
- on-zero stays agnostic: nothing in packages/on-zero may import orez.
- One path: when a helper lands, the consumer pattern it replaces is deleted
  in the same cutover, no compat shims.
