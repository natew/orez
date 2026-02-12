# debug: pglite deadlock during zero mutation + direct sql

## problem

test-chat integration test fails during setup. the user identified this as a
deadlock caused by running direct SQL mutations during an active zero mutation.
NOT a login failure, NOT primarily the SQLITE_CORRUPT issue.

## architecture context

- pglite is **single-session**: all TCP connections share one postgres backend
- a `Mutex` serializes all pglite access (per-message, not per-transaction)
- the mutex releases between each wire protocol message
- three independent mutexes for three pglite instances (postgres, cvr, cdb)

## the deadlock scenario (original bug, FIXED)

the `app.migrate` zero mutator (`test-chat/src/data/models/app.ts:62-66`):

```typescript
migrate: async ({ server }) => {
  server?.asyncTasks.push(async () => {
    await server?.actions.app.migrateInitialApps()
  })
}
```

note: code now uses asyncTasks correctly (heavy work runs after tx commit).

`migrateInitialApps()` (`test-chat/src/apps/migrateInitialApps.ts:33-83`):

1. opens Connection B via `getDBClient({ connectionString: ZERO_UPSTREAM_DB })`
2. runs `SELECT * FROM userPublic` (Connection B, through proxy)
3. calls `publishApp()` which uses `getDb()` (Connection C, Drizzle singleton)
4. `publishApp()` does INSERT/UPDATE on `app` table (Connection C, through proxy)

because pglite is single-session:

- Connection A has an open BEGIN (PushProcessor's transaction)
- Connection B's queries run inside Connection A's transaction
- Connection C's INSERT also runs inside Connection A's transaction
- when Connection B releases (client.release()), the pool may send cleanup
- the socket.on('close') handler runs ROLLBACK + RESET on pglite
- this ROLLBACK kills Connection A's transaction!

## key files

| file                                       | role                                                 |
| ------------------------------------------ | ---------------------------------------------------- |
| `src/pg-proxy.ts:467-473`                  | activeConns tracking (per-db connection count)       |
| `src/pg-proxy.ts:708-736`                  | socket close → conditional ROLLBACK (only last conn) |
| `src/replication/handler.ts:486-557`       | replication poll loop (mutex per poll, safe)         |
| `src/mutex.ts`                             | simple queue-based mutex, per-message serialization  |
| `test-chat/src/data/models/app.ts:62-66`   | app.migrate mutator (asyncTasks pattern)             |
| `test-chat/src/apps/migrateInitialApps.ts` | opens separate DB connections for direct SQL         |
| `test-chat/src/apps/publish.ts`            | uses getDb() Drizzle singleton for INSERT            |
| `test-chat/src/database/index.ts:11-33`    | getDb() creates pg.Pool singleton                    |
| `test-chat/app/api/zero/push+api.tsx`      | web server push endpoint                             |

## fix: activeConns tracking (implemented)

tracked active connections per database in pg-proxy.ts. only ROLLBACK on
socket close when this is the LAST connection for that database.

```typescript
const activeConns: Record<string, number> = {}

// on connect:
activeConns[dbName] = (activeConns[dbName] || 0) + 1

// on close:
activeConns[dbName] = Math.max(0, (activeConns[dbName] || 1) - 1)
const remaining = activeConns[dbName]
if (remaining === 0) {
  // safe to ROLLBACK - no other connections need this tx
  await db.exec('ROLLBACK')
  // ... reset session state
}
```

unit tests added in `src/pg-proxy.test.ts`:

- closing one connection does not rollback another's transaction ✓
- last connection closing does rollback uncommitted transaction ✓
- rapid open/close connections do not corrupt active transaction ✓
- keepalives sent during idle periods ✓

## additional fixes during investigation

### ECONNREFUSED on custom query fetches (REVERTED)

zero-cache's `fetch()` under Node 24 resolves `localhost` differently (IPv6 first).
initially changed `ZERO_MUTATE_URL` and `ZERO_QUERY_URL` to `http://127.0.0.1:`.
REVERTED: this breaks auth because better-auth's session validation checks host
against `BETTER_AUTH_URL=http://localhost:...` — host mismatch causes auth to fail
silently, so mutations run unauthenticated and `completeSignup` never sets username.
keep ZERO_MUTATE_URL/ZERO_QUERY_URL as `http://localhost:` to match BETTER_AUTH_URL.

### port patching idempotency

`--skip-clone` reuses test-chat dir from previous runs. port patching with exact
port numbers (`sed 's/8081/8082/'`) doesn't work when ports already changed.
switched to regex ranges (`8[0-9][0-9][0-9]`) for idempotent patching.

### stale source files with --skip-clone

added rsync of critical source dirs (database, data, server, apps, constants)
from ~/chat to test-chat when using --skip-clone, so schema changes are picked up.

### admin login timeout

initial zero sync involves 54+ custom queries, each requiring HTTP roundtrip.
increased timeout from 10s to 30s per attempt. now succeeds.

## current blocker: zero mutations hang

### what's happening

- `zero.mutate.app.migrate()` hangs in the browser (never resolves)
- user confirmed: "if you remove that even it hangs further down"
- suggests ALL zero mutations that go through push processor are hanging

### push flow analysis (session 3)

key discovery: zero-cache does NOT start a DB transaction before forwarding
to ZERO_MUTATE_URL. the flow is:

```
browser → zero.mutate.app.migrate()
  → zero client sends push to zero-cache via WebSocket
  → zero-cache receives push
  → zero-cache HTTP POST to ZERO_MUTATE_URL (web server)
  → web server /api/zero/push receives request
  → getZeroAuthData() — checks session via better-auth (DB access)
  → zeroServer.handleMutationRequest() — opens DB tx, runs mutation, commits
  → web server responds
  → zero-cache acknowledges to client
```

the web server handles the entire transaction, not zero-cache. so the hang
is either:

1. zero-cache can't reach web server's push endpoint
2. web server's push handler hangs (auth check or DB transaction)
3. web server's DB connection to PGlite proxy hangs

### verified not the cause

- mutex is deadlock-safe (all paths release, setImmediate between waiters)
- replication handler releases mutex between polls, no long holds
- .env.development has correct `127.0.0.1:PORT` format for ZERO_UPSTREAM_DB
- web server can serve pages (login works, pages load)

### next investigation

- add diagnostic logging to pg-proxy.ts (connection opens, query types, timing)
- add push endpoint reachability test to integration script
- check if zero-cache's HTTP POST to push endpoint even arrives
- check if the web server's auth or DB transaction hangs

## investigation log

### session 1 (from compaction summary)

- replication handler confirmed working (11 changes streamed wm 3781→3792)
- only 1 handler active (no concurrent handler race)
- watermark type is `number` throughout
- SQLITE_CORRUPT from `PRAGMA optimize` crashes zero-cache
- first login 401 then succeeds on retry

### session 2

- mapped all mutex usage across codebase
- identified socket.on('close') ROLLBACK as potential transaction killer
- identified migrateInitialApps opening separate connections inside PushProcessor tx
- added diagnostic logging: connection tracking, BEGIN/COMMIT/ROLLBACK
- confirmed the bug: conn close during active tx → ROLLBACK → zero-cache crash
- implemented activeConns fix: only ROLLBACK when last connection closes
- zero-cache now starts and stays running ✓
- fixed ECONNREFUSED: localhost → 127.0.0.1 for zero-cache custom queries
- fixed port patching: exact ports → regex ranges for --skip-clone
- fixed stale source: added rsync of schema/model dirs from ~/chat
- admin login timing out → increased timeout 10s → 30s → now succeeds ✓
- setupTamaguiServer hanging: Create Tamagui Server button calls
  zero.mutate.app.migrate() which never resolves

### session 3

- analyzed push processor: zero-cache does NOT open DB tx before forwarding
- web server handles the entire mutation transaction
- mutation code uses asyncTasks correctly (heavy work after commit)
- confirmed mutex is safe (no deadlock possible, proper release everywhere)
- confirmed replication handler safe (releases mutex between polls)
- hypothesis: push HTTP POST to web server either doesn't arrive or web server
  handler hangs (auth check or DB connection to pglite proxy)
- adding diagnostic logging to pg-proxy.ts to trace connections during push

### session 4 (current)

test run observations:

- push endpoint smoke test works! responds with 200 (schema validation error is expected)
- playwright test starts, migrations succeed
- login succeeds, replication working (watermark advancing 3706→3779)
- zero-cache successfully hydrates queries, CVR sync working
- BUT: seeing transaction nesting warnings from zero-cache:
  - `"there is already a transaction in progress"` (code 25001)
  - `"there is no transaction in progress"` (code 25P01)
- activeConns fix working correctly: connections close with `shouldRollback=false`
- test appears to be running normally so far, waiting to see if mutations hang...
