# wasm sqlite stability research

## root cause

wasm sqlite's VFS uses in-process `_malloc`'d SHM (shared memory) for WAL2
coordination. when zero-cache forks worker processes, each child gets its own
WASM instance with separate SHM. WAL2 requires shared SHM between readers and
writers, so all multi-process access to the wasm sqlite replica is broken.

this is why chat's e2e tests fail with WebSocket crashes ("connection closed
abruptly") — zero-cache's change-streamer and syncer can't coordinate on the
replica DB.

additionally, `ZERO_NUM_SYNC_WORKERS` is set as a _default_ in orez (line ~817
of index.ts) that gets overridden by consumer env vars. chat sets it to 14 via
env.ts, so zero-cache spawns 14 workers — each with its own broken WASM SHM.

## what works

- **native sqlite** (`--disable-wasm-sqlite`): uses real filesystem mmap'd SHM,
  works correctly across forked processes. test-chat forces this and passes.
- **`ZERO_NUM_SYNC_WORKERS=1` force** (added in index.ts): overrides user env
  after the spread, so wasm mode always gets 1 worker. reduces blast radius but
  doesn't fix the fundamental SHM issue since even 1 worker gets forked.

## approaches tried

### 1. SINGLE_PROCESS=1 (zero-cache's own in-process mode)

zero-cache has a `SINGLE_PROCESS` env var that makes `childWorker()` use
EventEmitter IPC instead of `fork()`. workers share one process and one WASM
instance, so SHM is shared.

**problem**: zero-cache's entry point (`server/runner/main.js`) has:

```js
if (!singleProcessMode()) exitAfter(() => runWorker(parentWorker, process.env))
```

when `SINGLE_PROCESS=1`, this skips execution entirely — the process exits
with code 0 because it thinks it's being loaded as a worker module.

**custom entry point workaround**: wrote a `.mjs` file that imports
`run-worker.js` directly, bypassing the guard:

```js
import { exitAfter } from '.../life-cycle.js'
import { runWorker } from '.../run-worker.js'
exitAfter(() => runWorker(null, process.env))
```

**result**: zero-cache starts, health check passes, WebSocket connections work,
replication handler streams data (confirmed: `found 1 changes [public.foo]`,
`streaming 4 wal messages`, `streamed ok`). BUT the view-syncer never returns
`rowsPatch` in pokePart messages. queries are acknowledged (`gotQueriesPatch`)
but no row data comes through.

**diagnosis**: SINGLE_PROCESS mode uses EventEmitter-based IPC between workers.
the view-syncer initializes a CVR (client view record) transaction that completes
with `0 clients, 0 queries, 0 desires` — meaning the client's
`desiredQueriesPatch` from the initConnection message never gets registered in
the view-syncer. the syncer receives the WebSocket connection and creates a
view-syncer instance, but the query subscription isn't processed.

this was tested with both @rocicorp/zero 0.26.2 and 1.1.1 (which fixed an "IPC
channel closure race condition during shutdown"). the issue persists in both.

native mode (forked workers, real SHM) passes 5/6 tests with both versions.

nate remembers running zero in single process mode successfully before — with
NATIVE sqlite, single process mode would work because native sqlite is a C addon
that doesn't block the event loop as severely. with WASM sqlite, the synchronous
operations block the shared event loop entirely.

### key finding: event loop starvation

in SINGLE_PROCESS wasm mode, the replica's WAL file is 0 bytes — the
change-streamer never writes replication data. the orez replication handler
confirms `streamed ok` (TCP write succeeded), but the change-streamer's TCP
socket I/O callbacks never fire because the syncer's synchronous wasm sqlite
queries block the event loop.

- forked mode: each worker has its own event loop → no starvation
- single process: shared event loop → wasm sqlite blocks ALL I/O

this makes SINGLE_PROCESS fundamentally incompatible with synchronous wasm
sqlite. the only viable fix is making forked workers share SHM so zero-cache
can keep using fork() normally.

**relevant code**: `node_modules/@rocicorp/zero/out/zero-cache/src/types/processes.js`

- `singleProcessMode()` checks `process.env.SINGLE_PROCESS`
- `childWorker()` uses `inProcChannel()` (EventEmitter pairs) when true
- `inProcChannel()` creates two connected EventEmitters with send/kill methods

### 2. fork patching (preload that overrides child_process.fork)

wrote a `--require` preload that replaces `child_process.fork` with an in-process
implementation (same as zero-cache's `inProcChannel`). this lets the top-level
entry point run normally while preventing actual forking.

**problem**: after the first fork call, `process.env.SINGLE_PROCESS` gets set
(needed so imported worker modules skip their top-level guards). subsequent
`childWorker()` calls inside the dispatcher use zero-cache's built-in
SINGLE_PROCESS path. same result as approach 1.

### 3. file-backed SHM in VFS

modified `sqlite-wasm/native/vfs.js` to use filesystem for SHM:

- `nodejsShmMap`: allocates WASM memory AND reads initial state from `.db-shm` file
- `nodejsShmBarrier`: syncs WASM memory ↔ file
- `nodejsShmLock`: tracks dirty pages via exclusive lock callbacks

**iteration 1** (write-then-read barrier): barrier writes WASM to file, then
reads back. the read-back overwrites local in-progress writes → `SQLITE_PROTOCOL`
(WAL header consistency check fails).

**iteration 2** (read-then-write barrier): barrier reads file first, then writes.
reader's stale data overwrites writer's changes in the file → same issue.

**iteration 3** (write-on-unlock, read-on-barrier): writers flush to file when
releasing exclusive SHM locks. barrier only reads (for readers to pick up
changes). got `SQLITE_IOERR` — likely from file operations interfering with
SQLite's internal state, or from the SHM file not existing when barrier tries
to read.

**key insight**: the barrier approach is fundamentally tricky because:

- the barrier function doesn't know if it's being called by a writer or reader
- SQLite's WAL header uses two copies with a barrier between them
- the writer's barrier is between writing copy 1 and copy 2 of the header
- any file I/O in the barrier can corrupt in-progress multi-step updates

**path forward**: tracking dirty state via lock callbacks (approach 3) is the
right direction but needs more careful implementation:

- only flush dirty pages on exclusive lock release (done)
- only read on barrier when NOT holding an exclusive lock (done but IOERR)
- handle missing SHM file gracefully (partially done)
- ensure file operations don't interfere with WASM heap state

## key files

- `sqlite-wasm/native/vfs.js` — VFS implementation, SHM functions (lines 190-227)
- `sqlite-wasm/native/vfs-pre.js` — VFS constants and helpers
- `sqlite-wasm/Makefile` — WASM build (emcc)
- `src/index.ts:787-880` — startZeroCache, env setup, sqlite mode handling
- `src/sqlite-mode/resolve-mode.ts` — mode auto-detection logic
- `src/sqlite-mode/shim-template.ts` — wasm shim that patches zero-sqlite3
- `node_modules/@rocicorp/zero/out/zero-cache/src/types/processes.js` — SINGLE_PROCESS, childWorker, inProcChannel
- `node_modules/@rocicorp/zero/out/zero-cache/src/server/runner/main.js` — top-level entry with singleProcessMode guard
- `node_modules/@rocicorp/zero/out/zero-cache/src/server/runner/run-worker.js` — runner that starts dispatcher
- `node_modules/@rocicorp/zero/out/zero-cache/src/server/main.js` — dispatcher that starts all workers
- `node_modules/@rocicorp/zero/out/zero-cache/src/services/life-cycle.js` — ProcessManager, runUntilKilled

## commit history

- `966ef5e` — added `SINGLE_PROCESS=1` for wasm mode (fix attempt)
- `6bd50fc` — removed it (caused zero-cache to exit immediately)
- both were in the defaults section (overridden by consumer env)

## test infrastructure

- `src/integration/integration.test.ts` — full sync pipeline test
  - native mode: 5/6 pass (concurrent inserts flaky)
  - wasm mode: 1/6 pass (only WebSocket connection, no data sync)
  - `FORCE_WASM=1` env var triggers `forceWasmSqlite` in test config
  - protocol version updated from 45 to 49 to match zero-cache 0.26.2
- `src/wasm-sqlite.test.ts` — unit tests for wasm sqlite (all pass)
- `scripts/test-chat-e2e.ts` — runs chat tests against local orez build
  - patches `--disable-wasm-sqlite` into lite:backend
  - builds native @rocicorp/zero-sqlite3 binary
  - 46/48 pass with native mode
