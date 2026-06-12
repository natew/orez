# brief A — HTTP-backed fake-WebSocket transport for the zero client

you are agent A of segment 1 of the zero-http spike. read, in order:

1. `plans/zero-http.md` — full context. obligation 1 contains the VERIFIED
   transport contract with exact source refs into
   `node_modules/@rocicorp/zero/out/zero-client/src/client/` — read those
   source locations yourself before writing code.
2. `plans/zero-http-spike/INTERFACE.md` — the pinned HTTP wire + fixture
   schema + file ownership. do not deviate from it.

## deliverable

`src/zero-http/transport.ts` — a URL-discriminating `globalThis.WebSocket`
shim + the HTTP-backed transport behind it, such that a STOCK
`new Zero({...})` (no fork, no patch of @rocicorp/zero) connects, completes
queries, pushes mutations, and receives full-snapshot pokes — with all HTTP
calls going through an injectable `fetch` so tests can run against canned
responses (no real server in this segment; agent B builds that in parallel).

public surface (keep it this small):

```ts
installZeroHttpTransport(opts: {
  origin: string            // e.g. 'https://zero-http.local' — intercept only this
  fetch?: typeof fetch      // injectable for tests; default globalThis.fetch
}): {
  pull(): Promise<void>     // trigger a pull now (resolves when poke emitted)
  connections: number       // live fake sockets (for pass-through assertions)
  uninstall(): void         // restore the previous globalThis.WebSocket
}
```

`src/zero-http/fixture-schema.ts` — zero client schema for the pinned
fixture tables (user/project/member, with a `project.members` relationship)

- client-side custom mutators (`project|create`, `project|rename`,
  `member|add`) that do the optimistic apply. this file is shared with later
  segments — keep it dependency-free.

`src/zero-http/transport.test.ts` — vitest, colocated like the rest of
src/. canned-fetch tests proving:

1. **connect + complete (plan obligation 1, the gate).** a real
   `new Zero({ server: origin, userID: 'u1', auth: 'token-u1', schema,
kvStore: 'mem', mutators })` reaches connected state; a materialized
   query reaches `resultType: 'complete'` after the transport acks desired
   queries via `gotQueriesPatch` and a snapshot poke lands. assert actual
   row data from the canned pull is readable via the query.
2. **push bridging.** a custom mutator call leaves the fake socket as a
   POST to `{origin}/push` with the v51 body; the canned `pushResponse` is
   emitted and the mutation's `.server` promise resolves; a follow-up pull
   is triggered automatically after the push response.
3. **cookie discipline.** unchanged pull response → NO poke emitted.
   changed response → pokeStart.baseCookie equals the tracked cookie and
   pokeEnd.cookie equals the response cookie; a second changed pull chains
   from the first's cookie. concurrent `pull()` calls coalesce/serialize —
   never two in flight.
4. **ping/pong.** after connect, wait past the 5s ping idle (use vitest
   fake timers carefully or pass a shorter `pingTimeoutMs` in ZeroOptions —
   it is a public option) and assert the connection survives because the
   transport answers pings locally.
5. **pass-through.** constructing a WebSocket to any non-`origin` URL
   yields the native implementation untouched.

## implementation notes (verified, do not re-derive)

- install the shim BEFORE constructing Zero — `getServer` returns null if
  no `WebSocket` global exists at option-validation time; node 24 provides
  a native one, your shim wraps it.
- the connect URL carries clientID, clientGroupID, userID, baseCookie
  (`''` means null), lmid, wsid as query params; auth + initConnection
  (desiredQueriesPatch) arrive in the second constructor arg
  (sec-protocol). decode it like `src/cf-do/worker.ts` decodes
  `sec-websocket-protocol` (`decodeInitConnection`) — read that file; it
  already speaks this dialect server-side and is your wire-shape fixture
  for poke construction too (`sendSyncPoke`, `applyDesiredQueries`,
  `gotQueriesPatch`).
- after emitting `open` you must emit `["connected", { wsid, timestamp }]`
  or pushes hang forever on `#connectResolver`.
- upstream messages to handle: `initConnection` / `changeDesiredQueries`
  (ack via gotQueriesPatch poke), `push` (POST + emit pushResponse +
  schedule pull), `ping` (emit `["pong", {}]` locally, no HTTP), `pull`
  (mutation-recovery pull — may answer with current cookie +
  lastMutationIDChanges, patch []), `deleteClients` (ignore).
- the fake socket needs only: `addEventListener('message'|'open'|'close')`,
  `send`, `close`, `readyState`, `url`. JSON text frames only. emit events
  as `{ data: string }` message events; zero reads `e.data`.
- pokes: `["pokeStart", { pokeID, baseCookie }]`, `["pokePart", { pokeID,
lastMutationIDChanges, rowsPatch, gotQueriesPatch? }]`, `["pokeEnd",
{ pokeID, cookie }]`. baseCookie mismatches are SILENTLY dropped by the
  client (handlePullResponseV1 returns early) — your tests must assert on
  query RESULTS, not just absence of errors.
- keep auth: forward the token from the sec-protocol header as
  `authorization: Bearer <token>` on every pull/push POST.
- KISS: one module, functions over classes where reasonable, no options
  beyond what the tests need. no polling timer in this segment (pull
  triggers: explicit `pull()`, post-push, reconnect).

## acceptance

- `npx vitest run src/zero-http/` green, plus `npx tsc --noEmit` clean for
  your files (run the repo's build/typecheck the way package.json does).
- tests FAIL before the implementation (write one, watch it fail, then
  implement — say in your report that you did this).
- conventional commit(s) with explicit pathspec, e.g.
  `feat(zero-http): http-backed fake-websocket transport (spike segment 1a)`.
  NEVER push. NEVER publish.
- final report (your last message): what passed, any contract surprises
  found while reading zero source, anything in INTERFACE.md you believe is
  wrong (do NOT silently change it), open risks for segment 2.

stay inside your file ownership (INTERFACE.md). agent B is building the
real server in parallel against the same wire — integration happens in
segment 2 after review.
