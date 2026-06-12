# zero-http spike — socketless zero for the soot control plane

status: planned 2026-06-12, ready for a dedicated agent. owner repo: ~/orez
(with on-zero work in ~/takeout and the consumer in ~/soot).

## why (measured, not speculative)

soot's CF control plane runs real zero-cache embedded in a single 128MB
`ZeroCacheDO`. two 50-user load runs against cf.sootbean.com (2026-06-12,
`~/soot/scripts/ops/cf-load-longevity.ts --users 50 --tail`, raw tails
`/tmp/cf-load-tail-cf50-*.json`) showed the singleton `exceededMemory`-reset
at ~user 39 (pre-sharding build) and ~user 15 (dual-instance build where
per-project sync is ALREADY routed to per-project DOs; control scope 697
tail events vs project 19). the wall is per-client view-syncer state for
plain control connections — structural, not query bloat. sharding shipped
and did not move it.

full context: `~/soot/plans/sootbean/zero-compatible-sync-engine.md`
(the option taxonomy + revised decision frame) and
`~/soot/plans/sootbean/deploy/cf-launch-lay-of-the-land.md` §7 (the
plain-API fallback this spike competes with).

## thesis

keep the **zero client** (zql, local store, optimistic mutations, rebase —
the proven half). drop the **socket + CVR/IVM server half** for the control
plane only. replace it with react-query-shaped HTTP:

- **pull**: stateless endpoint returning a full per-user snapshot of the
  control tables (`user`, `userState`, `project` directory, `subscription`,
  `planGrant`, `tokenUsage`, `workspace`) — KB-scale per user — straight
  from orez storage. no CVR, no view-syncer, no per-client server state.
  client-configurable triggers: refetch-after-mutation, refetch-on-focus,
  poll-while-active (token meter during chat).
- **push**: replicache has HTTP push semantics, but zero's normal client
  routes push through its socket — `#pusher` asserts a connected socket and
  sends one `["push", ...]` frame per mutation (zero.js:981). the transport
  bridges this without forking: push frames leave the fake socket as HTTP
  POSTs. mutators and their optimistic-apply semantics stay as-is.

precedent: this is replicache semantics (http pull/push + rebase) with zql
relations on top — the lineage zero descends from. soot's pre-embed
`FakeZeroCacheServer` zero-shim already proved full-table dumps work on DO;
it lacked exactly the client discipline the real zero client keeps.

## what exists to build on

- **on-zero owns the client creation seam**: soot builds its two instances
  via `createZeroClient({ models, schema, groupedQueries, instanceName })`
  + `combineZeroClients(control, project)` — see
  `~/soot/src/zero/core.ts` (`createSootZeroClients`). a
  `transport: 'http-pull'`-style mode on `createZeroClient` is the natural
  public surface; the control instance flips, the project instance is
  untouched. on-zero source: `~/takeout` (package `on-zero`).
- **zero client internals**: `@rocicorp/zero` 1.6.1,
  `node_modules/@rocicorp/zero/out/zero-client/` (+ `replicache/` — the
  rebase machinery is right there, and `zero-protocol/` for the v51
  message schemas). protocol v51.
- **orez server side**: DoBackend + query routing already serve SQL over
  HTTP inside the data worker (`ZeroSqlDO`); the snapshot pull endpoint is
  a thin authenticated read on top. the push endpoint already exists
  (on-zero server handles zero's push). soot's data-worker shim:
  `~/soot/src/deploy/cloudflareDoDeploy.ts`.
- **orez thesis fit**: orez is already "what if zero ran lighter" — shims,
  embed, DO backends. this adds a mode where the heaviest piece
  (zero-cache itself) is not needed for small namespaces.

## the spike — proof obligations

half-day to ~2 days. build the smallest thing that answers these five,
in order; stop and report the moment one fails hard.

1. **transport seam — RESOLVED BY READING (2026-06-12), validate at runtime.**
   the seam question was answered by reading 1.6.1 source; the spike's job
   is to prove the contract below works live, not to re-run the bake-off.

   **the only non-fork seam is the WebSocket global.** direct store writes
   (the old seam "a") are impossible without forking: `#rep` and
   `#pokeHandler` are hard-private class fields on `Zero`, and bypassing
   them means rebuilding LMID/rebase bookkeeping by hand. eliminated.

   **chosen path: an HTTP-backed fake WebSocket that emits real v51 poke
   messages.** verified mechanics (all refs `@rocicorp/zero@1.6.1`
   `out/zero-client/src/client/` unless noted):
   - the client reads the constructor via `mustGetBrowserGlobal("WebSocket")`
     on EVERY (re)connect (zero.js:1363). the shipped bundle tree-shakes
     `overrideBrowserGlobal` out, so the seam is a `globalThis.WebSocket`
     shim installed once at boot that intercepts only the fake control
     origin and passes every other URL to the native WebSocket (soot's
     project-plane socket must pass through untouched).
   - `server` must be `http(s)://` with at most one path component
     (server-option.js) — a sentinel like `https://zero-http.local` passes
     validation; the client appends `/sync/v51/connect` with clientID,
     clientGroupID, userID, baseCookie, lmid, wsid in the query and auth +
     initConnection (desiredQueriesPatch) in the sec-protocol header —
     everything the transport needs arrives in the constructor args.
   - socket surface actually used: `addEventListener('message'|'open'|'close')`,
     `send`, `close` (zero.js:863-865). no binary frames; JSON text only.
   - poke ingestion is the rebase path for free: `#onMessage` →
     `PokeHandler` buffers/merges → `rep.poke` → `handlePullResponseV1` →
     `maybeEndPull` replays pending mutations (zero-poke-handler.js:100,
     replicache/src/sync/pull.js). same-clientGroup `#puller` is a no-op
     (zero.js:1178) — ALL downstream data flows as pokes, never HTTP pull
     on replicache's side.

   **transport contract checklist** (each is an observable the runtime test
   must hit; miss one and the client hangs or silently drops data):
   - emit `open`, then `["connected", {wsid, timestamp}]` — this resolves
     `#connectResolver`; pushes await it forever otherwise (zero.js:983).
   - after `connected` the client sends `initConnection` or
     `changeDesiredQueries` upstream (zero.js:789-810). the transport must
     ack desired queries via a poke carrying `gotQueriesPatch`, or
     materialized views never reach `resultType: 'complete'` and
     `run(…, 'complete')` hangs.
   - answer `ping` with `pong` locally within 5s (`DEFAULT_PING_TIMEOUT_MS`,
     no HTTP round trip) or the client tears the connection down.
   - `push` frames → HTTP POST; emit `["pushResponse", …]` from the result
     so `MutationTracker` resolves `.server` promises and surfaces
     app-level mutator errors.
   - **cookie rules** (enforced by handlePullResponseV1 + mergePokes):
     `pokeStart.baseCookie` must equal the client's current cookie
     (transport tracks it: seeded from the connect URL's `baseCookie`,
     advanced on each emitted pokeEnd); mismatch = poke silently ignored,
     gap between buffered pokes = thrown. non-empty patch REQUIRES a
     strictly newer `pokeEnd.cookie`; unchanged snapshot = emit no poke at
     all, never same-cookie-plus-patch. serialize pulls so cookies chain.
   - full-snapshot pokes are `[{op:'clear'}, ...puts]` row patches +
     `lastMutationIDChanges` — idempotent by construction, which is what
     makes stateless HTTP pull safe.
   - `deleteClients` upstream may be ignored in the spike; hidden-tab
     disconnect + reconnect-on-visible comes from zero's own machinery and
     IS refetch-on-focus (fresh transport → fresh pull).

   wire-shape fixture: `~/orez/src/cf-do/worker.ts` (`applyDesiredQueries`,
   `sendSyncPoke`, `handlePush`) already speaks this exact dialect over real
   DO sockets — lift the poke-building shapes from it, but treat it as a
   fixture, not finished semantics: the stateless HTTP pull endpoint still
   needs to be real.
2. **optimistic mutation + rebase.** with a mutation in flight (pushed,
   not yet acked), a pull lands. the optimistic state must not flicker or
   be clobbered; after ack + next pull, client state equals server state.
   this is THE correctness question. cover: ack-then-pull, pull-then-ack,
   and a focused rollback test — optimistic mutation applies, server
   returns an app-level mutator error, the next poke advances LMID with no
   row patch for it, and the optimistic row disappears.
3. **relations.** one `.related()` query over the snapshot store (e.g.
   project → members shape) materializes and updates when a pull replaces
   rows.
4. **interleave under churn.** rapid mutation bursts (10+ queued) while
   pulls arrive on a timer. no lost updates, no stuck lastMutationID, no
   unbounded store growth. stretch (cheap if the fixture allows): two zero
   instances in one client group (two tabs), each on its own transport —
   no cookie fights, both converge.

5. **auth/permission parity.** the snapshot endpoint derives identity from
   the request's auth, not client-supplied params, and two different users
   pulling get disjoint per-user snapshots. in the spike this is a fixture
   schema with a user-scoped filter; full parity with soot's control
   named-query visibility rules (especially the project directory rows) is
   a named acceptance item of the follow-on integration, not the spike.

acceptance per obligation = a vitest/integration test in ~/orez (or
on-zero's suite in ~/takeout where the code lands) that fails before the
implementation and passes after — not a manual demo.

## spike scope guards

- control-plane shapes only. snapshot pulls are FULL per-user dumps —
  do NOT build cursor/diff pulls, that is explicitly step 2 / out of scope.
- no soot integration in the spike — a fixture schema mirroring the
  control tables is enough. soot wiring is the follow-on once the verdict
  is in.
- one path: the fake-socket transport is the only transport. no direct
  store-write experiments, no dual-mode toggles left behind.
- do not touch zero-cache, the embed, or the project plane.

## verdict + follow-on

- **spike passes** → write the verdict + chosen seam into
  `~/soot/plans/sootbean/zero-compatible-sync-engine.md`, then the
  integration plan: on-zero release (`bun release --into /Users/n8/soot`
  from ~/takeout for local testing, npm publish for the frozen-lockfile
  gate), soot control instance flips transport, control namespace stops
  accepting `/sync` sockets, dual-instance workarounds
  (`useControlConnectionState`, the `run(…, 'complete')` reads in
  `useAccessDeniedCheck` / `useAnonProjectRedirectOnLogin`) get deleted,
  validated by the access-denied playwright suite + a 50-user
  `cf-load-longevity` run showing zero control-DO memory events.
- **spike fails** → name the exact obligation that killed it and fall back
  to plain-API (~2k lines, scoped in
  `~/soot/plans/sootbean/deploy/cf-launch-lay-of-the-land.md` §7). a
  failed spike with a precise reason is a successful spike.

## known next ceiling (out of scope, recorded so nobody re-derives it)

the socket is not the cost — the per-client RESIDENT state its contract
requires is (hydrated IVM views + CVR per connected client, alive for the
session). stateless pull makes memory scale with in-flight requests, not
connected users. the next structural ceiling after that is the data tier:
10GB DO-SQLite per DO on the control `ZeroSqlDO` (KB-scale rows/user →
~100k+ registered users; `tokenUsage` append growth and single-DO write
serialization likely pinch first). zero-http makes that wall cheap to
handle later: a stateless pull can fan out reads across N storage shards
and compose the snapshot server-side — no change-stream fanout, no client
changes. do not build any of that now.

## step 2 (future, not this work)

cursor-diff pulls: client sends a watermark, server computes the delta
from orez's change tracking — still stateless per client. that is when
project-scale data could migrate and the zero-cache embed becomes
deletable ("do-first orez"). recorded here so the spike's seam choice
doesn't foreclose it, but build NOTHING for it now.

## working agreements

- repos: spike code in ~/orez and/or on-zero in ~/takeout — follow each
  repo's commit conventions; never push without the owner's say-so.
- ~/soot is a shared multi-agent checkout — if you read fixtures from it,
  touch nothing; its git rules (explicit-pathspec commits, no
  stash/rebase/reset) apply to any soot edits in the follow-on.
- zero source is the upstream truth — when client behavior surprises you,
  read `node_modules/@rocicorp/zero/out/` (or clone rocicorp/mono to
  ~/github) before working around it.
