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
- **push**: zero's push protocol is already HTTP POST with lastMutationID
  bookkeeping. mutators and their optimistic-apply semantics stay as-is.

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
  `node_modules/@rocicorp/zero/out/zero/` (+ `replicache/` — the rebase
  machinery is right there). protocol v51.
- **orez server side**: DoBackend + query routing already serve SQL over
  HTTP inside the data worker (`ZeroSqlDO`); the snapshot pull endpoint is
  a thin authenticated read on top. the push endpoint already exists
  (on-zero server handles zero's push). soot's data-worker shim:
  `~/soot/src/deploy/cloudflareDoDeploy.ts`.
- **orez thesis fit**: orez is already "what if zero ran lighter" — shims,
  embed, DO backends. this adds a mode where the heaviest piece
  (zero-cache itself) is not needed for small namespaces.

## the spike — proof obligations

half-day to ~2 days. build the smallest thing that answers these four,
in order; stop and report the moment one fails hard.

1. **transport seam (the gating unknown).** can the 1.6.1 zero client run
   with no socket, fed by snapshot writes, WITHOUT forking @rocicorp/zero?
   candidate seams, cheapest first:
   a. drive the client-side store directly from on-zero — bypass zero's
      ConnectionManager entirely; zql + materialized views read the local
      store, on-zero writes pulled snapshots into it, mutations go through
      zero's existing push path with on-zero tracking lastMutationID.
   b. a fake in-process WebSocket/transport object satisfying zero's
      connection contract, replaying pull snapshots as poke messages
      (v51 wire shape) — heavier, but zero's machinery does the rebase
      bookkeeping for free.
   pick by reading, not guessing — document why the loser loses.
2. **optimistic mutation + rebase.** with a mutation in flight (pushed,
   not yet acked), a pull lands. the optimistic state must not flicker or
   be clobbered; after ack + next pull, client state equals server state.
   this is THE correctness question. cover: ack-then-pull, pull-then-ack,
   push failure → rollback.
3. **relations.** one `.related()` query over the snapshot store (e.g.
   project → members shape) materializes and updates when a pull replaces
   rows.
4. **interleave under churn.** rapid mutation bursts (10+ queued) while
   pulls arrive on a timer. no lost updates, no stuck lastMutationID, no
   unbounded store growth.

acceptance per obligation = a vitest/integration test in ~/orez (or
on-zero's suite in ~/takeout where the code lands) that fails before the
implementation and passes after — not a manual demo.

## spike scope guards

- control-plane shapes only. snapshot pulls are FULL per-user dumps —
  do NOT build cursor/diff pulls, that is explicitly step 2 / out of scope.
- no soot integration in the spike — a fixture schema mirroring the
  control tables is enough. soot wiring is the follow-on once the verdict
  is in.
- one path: if seam (a) wins, no remnants of (b) survive, and vice versa.
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
