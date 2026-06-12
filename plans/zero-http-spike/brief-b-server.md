# brief B — stateless pull/push HTTP fixture server

you are agent B of segment 1 of the zero-http spike. read, in order:

1. `plans/zero-http.md` — full context; especially the thesis (stateless
   full-snapshot pull, no per-client resident state) and the cookie rules
   in obligation 1.
2. `plans/zero-http-spike/INTERFACE.md` — the pinned HTTP wire + fixture
   schema + file ownership. do not deviate from it.

## deliverable

`src/zero-http/server.ts` — an in-process `node:http` fixture server
implementing the pinned `/pull` and `/push` endpoints over an in-memory
store of the fixture tables. this is the spike's stand-in for orez
storage; the property under test is STATELESSNESS: zero per-client resident
state — the only per-client data is durable LMID bookkeeping, and every
pull computes the snapshot fresh from the store.

public surface (keep it this small):

```ts
startZeroHttpServer(opts?: {
  seed?: { user?: Row[]; project?: Row[]; member?: Row[] }
}): Promise<{
  url: string                       // http://127.0.0.1:<port>
  version(): number                 // current cookie value
  rows(table: string): Row[]        // inspect store (tests)
  close(): Promise<void>
}>
```

`src/zero-http/server.test.ts` — vitest, colocated. plain `fetch` against
the started server (no zero client in this segment; agent A builds the
transport in parallel). prove:

1. **snapshot + visibility (plan obligation 5, server side).** seed users
   u1/u2, projects owned by each, a member row putting u1 on u2's project.
   pull as `token-u1` and `token-u2`: each gets exactly the pinned
   visibility set (clear + puts, nothing of the other user's invisible
   rows). pull with a bad/missing token → 401.
2. **cookie discipline.** fresh pull returns current cookie; pulling again
   with that cookie → `{ unchanged: true, cookie: same }` and NO rowsPatch;
   after a push, pull with the old cookie returns a strictly greater
   cookie and a full clear+puts snapshot.
3. **push + LMID.** a `project|create` push (v51 body shape per
   INTERFACE.md) applies the row, returns `pushResponse` with an ok result,
   and the next pull's `lastMutationIDChanges` carries the client's new
   LMID. replaying the same mutation id → acked idempotently, NOT
   re-executed (row count unchanged), LMID unchanged.
4. **app-error semantics (feeds plan obligation 2's rollback).**
   `project|rename` on a missing id and on a project the authed user does
   not own → `pushResponse` result `{ error: 'app', details }`, LMID STILL
   advances, store unchanged, cookie still bumps so the next pull is a
   changed response whose snapshot shows no trace of the mutation.
5. **mutation ordering.** out-of-order mutation id (gap) → reject the push
   cleanly (500-style error body is fine for the spike) without corrupting
   LMID state.

## implementation notes

- read `src/cf-do/worker.ts` (`handlePush`, `sendSyncPoke`,
  `rowsPatchForTables`) as the wire-shape fixture for how zero pushes look
  in practice — but your server is HTTP + stateless, not a socket fanout;
  treat worker.ts as reference, do not import from it or modify it.
- define minimal protocol interfaces inline like worker.ts does; do not
  import `@rocicorp/zero` internals server-side.
- store: plain `Map`s per table. mutators are a small named registry
  executing against the store under the authed user — implement exactly
  the three in INTERFACE.md, no more.
- a push body carries ONE mutation per frame from the real client, but
  accept an array and process in order — segment 2 will batch.
- KISS: one module, no classes unless they pay for themselves, no config
  surface beyond `seed`. no CORS, no streaming, no cursors/diffs (full
  snapshots only — diff pulls are explicitly out of scope, see plan).

## acceptance

- `npx vitest run src/zero-http/server.test.ts` green (agent A owns the
  other test files — run only yours, the directory is shared), plus the
  repo typecheck clean for your files.
- tests FAIL before the implementation (write one, watch it fail, then
  implement — say in your report that you did this).
- conventional commit(s) with explicit pathspec, e.g.
  `feat(zero-http): stateless pull/push fixture server (spike segment 1b)`.
  NEVER push. NEVER publish.
- final report (your last message): what passed, anything in INTERFACE.md
  you believe is wrong (do NOT silently change it — report it), open risks
  for segment 2 integration.

stay inside your file ownership (INTERFACE.md). agent A is building the
client transport in parallel against the same wire — integration happens
in segment 2 after review.
