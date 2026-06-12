# zero-http spike — pinned integration interface (segment 1)

read `plans/zero-http.md` FIRST — it contains the verified transport contract
(connected handshake, ping/pong, gotQueriesPatch, cookie rules) with exact
`@rocicorp/zero@1.6.1` source refs. this file pins only the seam BETWEEN the
two segment-1 agents so their halves integrate without drift. do not change
these shapes unilaterally — if one is wrong, say so in your final report and
stop, don't invent a variant.

## HTTP wire (transport ↔ server)

### POST {origin}/pull

- headers: `authorization: Bearer <token>`, `content-type: application/json`
- body: `{ clientID: string, clientGroupID: string, cookie: number | null }`
- 200 changed:

  ```json
  {
    "cookie": 7,
    "lastMutationIDChanges": { "<clientID>": 3 },
    "rowsPatch": [
      { "op": "clear" },
      { "op": "put", "tableName": "user", "value": { "id": "u1", "name": "a" } }
    ]
  }
  ```

  - `rowsPatch` is ALWAYS `clear` followed by puts of every row visible to
    the authed user — full snapshot, idempotent by construction.
  - `lastMutationIDChanges` includes every client in the requesting
    clientGroupID that the server has seen.
  - `cookie` strictly greater than the request cookie when changed.

- 200 unchanged: `{ "cookie": <same as request cookie>, "unchanged": true }`
- 401 on missing/unknown token.

### POST {origin}/push

- headers: same auth.
- body: the v51 push frame body verbatim — exactly what the zero client put
  in `["push", body]` (see `#pusher`, zero.js:981: `{ timestamp,
clientGroupID, mutations: [...], pushVersion, requestID }`, one mutation
  per frame; custom mutations have `type: 'custom'`, `name`, `id`,
  `clientID`, `args: [argsObject]`).
- 200: `{ "pushResponse": <v51 pushResponse body> }` — the exact body the
  transport will emit as `["pushResponse", body]` so `MutationTracker`
  resolves. shape: `{ mutations: [{ id: { clientID, id }, result: {} | {
error: 'app', details?: string } }] }`.
- LMID bookkeeping per (clientGroupID, clientID). replayed mutation ids
  (id <= stored lmid) are acked idempotently without re-executing.
- an app-error mutation STILL advances the LMID and makes no row change —
  that is what drives the client-side rollback (plan obligation 2).
- every processed push (success or app error) bumps the server cookie.

## server cookie

single monotonic integer version per server instance, starts at 1, bumps on
every committed push. pull compares the client's cookie to current: equal →
unchanged path.

## fixture schema (pinned for both sides)

tables (string ids, all columns required unless noted):

- `user`: id pk, name
- `project`: id pk, ownerId, name
- `member`: id pk, projectId, userId

visibility for authed userID U: U's own `user` row; every `project` where
`ownerId = U` OR a `member` row (projectId, userId=U) exists; every `member`
row whose project is visible to U.

auth: fixture tokens map `token-<userID>` → userID (e.g. `token-u1` → `u1`).

custom mutators (names are `namespace|name` on the wire):

- `project|create` args `{ id, ownerId, name }` — inserts; app error if id
  exists or ownerId !== authed user.
- `project|rename` args `{ id, name }` — app error `'not-found'` details if
  id missing, `'forbidden'` if authed user is not owner. THE rollback-test
  mutator.
- `member|add` args `{ id, projectId, userId }` — app error if project
  missing or authed user is not the project owner.
- `member|remove` args `{ id }` (added segment 2) — app error `'not-found'`
  if member missing, `'forbidden'` if authed user is not the project owner.
  drives the visibility-revocation test: removing a member must make the
  project AND its member rows vanish from the removed user's next snapshot.

## file ownership (segment 1 — do not touch the other agent's files)

- agent A (transport): `src/zero-http/transport.ts`,
  `src/zero-http/fixture-schema.ts` (zero client schema + client mutators),
  `src/zero-http/transport.test.ts`
- agent B (server): `src/zero-http/server.ts`, `src/zero-http/server.test.ts`

shared repo rules: conventional commits; stage and commit with explicit
pathspec (`git add <files> && git commit -m "..." -- <files>`); NEVER push;
NEVER publish; no new npm deps (`@rocicorp/zero` is already installed);
define minimal protocol interfaces inline like `src/cf-do/worker.ts` does
rather than importing from `@rocicorp/zero/out/zero-protocol`.
