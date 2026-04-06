# handoff: fix wasm sqlite SHM for cross-process WAL2 coordination

date: 2026-04-06
branch: main
last commit: dd07d8b (v0.2.4)
prod: n/a (dev tool)

## problem

wasm sqlite's VFS uses in-process `_malloc`'d SHM for WAL2 coordination.
zero-cache forks worker processes (even with `ZERO_NUM_SYNC_WORKERS=1`). each
forked child gets its own WASM instance with separate SHM. WAL2 requires shared
SHM between readers (syncer) and writers (change-streamer), so replication data
never reaches the syncer's view of the replica.

**symptoms in downstream apps (~/chat)**: WebSocket "connection closed abruptly",
tests timing out on zero sync, `pokePart` messages with no `rowsPatch`.

## what's done

### root cause confirmed

- wasm VFS SHM (`sqlite-wasm/native/vfs.js:192-227`) uses `_malloc` and
  `_shmRegistry` (JS global) — not shared across forked processes
- SHM locks (`nodejsShmLock`) are no-ops — always return `SQLITE_OK`
- SHM barrier (`nodejsShmBarrier`) is a no-op
- `ZERO_NUM_SYNC_WORKERS` defaults to 1 but gets overridden by consumer env
  (chat sets it to 14). fix: force it after the `process.env` spread in
  `src/index.ts:828-841`

### approaches tried and failed

1. **SINGLE_PROCESS=1** — zero-cache's own in-process EventEmitter IPC mode.
   the entry point (`runner/main.js:6`) skips execution when
   `singleProcessMode()` is true. custom entry point bypasses this. workers
   start, connections work, `handleInitConnection` is called with correct data,
   message handler returns `stream` result. BUT the change-streamer never writes
   WAL data to the replica (WAL file = 0 bytes). **root cause: event loop
   starvation** — wasm sqlite's synchronous operations block the shared event
   loop, preventing the change-streamer's TCP socket I/O callbacks from firing.
   this makes SINGLE_PROCESS fundamentally incompatible with synchronous wasm.

2. **fork() patching via --require preload** — intercepts `child_process.fork`
   to use EventEmitter IPC. same underlying issue as SINGLE_PROCESS (event loop
   starvation).

3. **file-backed SHM in VFS** (3 iterations):
   - **write-then-read barrier**: corrupts writer's in-progress WAL header
     updates → `SQLITE_PROTOCOL`
   - **read-then-write barrier**: reader's stale data overwrites writer's
     changes in the file → same issue
   - **write-on-unlock + read-on-barrier**: closest to correct. writers flush
     dirty pages when releasing exclusive SHM lock. barrier only reads for
     readers. got `SQLITE_IOERR` — likely from missing SHM file at read time
     or file operations interfering with WASM state

## what's remaining

### 1. fix file-backed SHM in VFS (the correct approach)

**file**: `sqlite-wasm/native/vfs.js` (lines 190-227)
**rebuild**: `cd sqlite-wasm && make` (requires emscripten: `emcc` must be in PATH)
**dist symlink**: dist/ is symlinked to `node_modules/bedrock-sqlite/dist/` — rebuilds take effect immediately

the third VFS iteration was architecturally correct. the implementation needs:

#### SHM page tracking with dirty flags

```javascript
// in nodejsShmMap: store page info with dirty flag
regions[pgno] = { ptr: ptr, pgsz: pgsz, dirty: false }

// in nodejsShmLock: mark dirty on exclusive lock acquire
if ((flags & SQLITE_SHM_LOCK) && (flags & SQLITE_SHM_EXCLUSIVE)) {
  reg._hasExclusiveLock = true
}
```

#### writer flush on exclusive lock release

```javascript
// in nodejsShmLock: flush pages to file when releasing exclusive lock
if ((flags & SQLITE_SHM_UNLOCK) && (flags & SQLITE_SHM_EXCLUSIVE) && reg._hasExclusiveLock) {
  var shmPath = filePath + '-shm'
  var fd
  try { fd = fs.openSync(shmPath, fs.constants.O_RDWR | fs.constants.O_CREAT, 0o644) }
  catch (e) { reg._hasExclusiveLock = false; return SQLITE_OK }
  for (var pgno in reg) {
    if (typeof reg[pgno] !== 'object' || !reg[pgno].ptr) continue // skip metadata keys
    var r = reg[pgno]
    fs.writeSync(fd, HEAPU8.subarray(r.ptr, r.ptr + r.pgsz), 0, r.pgsz, Number(pgno) * r.pgsz)
  }
  try { fs.fsyncSync(fd) } catch (e) {}
  fs.closeSync(fd)
  reg._hasExclusiveLock = false
}
```

#### reader refresh on barrier (only when NOT holding exclusive lock)

```javascript
// in nodejsShmBarrier: read from file ONLY if not writing
if (reg._hasExclusiveLock) return // writer — don't read, you'll corrupt your own state
var shmPath = filePath + '-shm'
var fd
try { fd = fs.openSync(shmPath, fs.constants.O_RDONLY) }
catch (e) { return } // file doesn't exist yet — nothing to read
for (var pgno in reg) {
  if (typeof reg[pgno] !== 'object' || !reg[pgno].ptr) continue
  var r = reg[pgno]
  try { fs.readSync(fd, HEAPU8.subarray(r.ptr, r.ptr + r.pgsz), 0, r.pgsz, Number(pgno) * r.pgsz) }
  catch (e) {} // short read is fine — page doesn't exist in file yet
}
fs.closeSync(fd)
```

#### critical details

- the `_shmRegistry` keys include metadata (`_hasExclusiveLock`). when iterating
  pages, skip keys that aren't page objects (check `typeof reg[pgno] === 'object'
  && reg[pgno].ptr`)
- `nodejsShmUnmap` must free `regions[pgno].ptr` not `regions[pgno]` (since page
  entries are now objects not raw pointers)
- open the SHM file with `O_CREAT` on write but NOT on read — if the file
  doesn't exist when reading, silently skip (the writer hasn't flushed yet)
- use `fs.constants.O_RDONLY` for reads to avoid creating the file
- the `SQLITE_IOERR` from attempt 3 was likely from `fs.openSync(shmPath, 'r')`
  throwing when the file doesn't exist — use try/catch and return early

### 2. force ZERO_NUM_SYNC_WORKERS=1 for wasm mode

**file**: `src/index.ts` around line 841
**what**: add after the `process.env` spread in the `env` object:

```typescript
...(sqliteMode === 'wasm' ? { ZERO_NUM_SYNC_WORKERS: '1' } : {}),
```

this overrides the consumer's env (e.g. chat's `ZERO_NUM_SYNC_WORKERS=14`).
currently it's only a default (line 817) that gets overridden.

### 3. verify with integration test

**test**: `src/integration/integration.test.ts`
**run native baseline**: `bun test src/integration/integration.test.ts` (needs
native binary: `cd node_modules/@rocicorp/zero-sqlite3 && npm run install`)
**expected**: 5 pass, 1 fail (concurrent inserts is flaky)

**run wasm mode**: `FORCE_WASM=1 bun test src/integration/integration.test.ts`
**requires**: add `forceWasmSqlite: process.env.FORCE_WASM === '1'` to the
`startZeroLite` call in the test (line ~98)
**expected goal**: same as native (5 pass, 1 fail)

**note**: the test's `SYNC_PROTOCOL_VERSION` is 45 but zero-cache uses 49.
both are in the supported range (30-49) and native mode passes with 45, so
this isn't blocking. update to 49 if you want.

### 4. test in chat

**quick test**: copy updated `sqlite-wasm/dist/` to chat's
`node_modules/bedrock-sqlite/dist/` and run chat in lite mode without
`--disable-wasm-sqlite`

**full test**: run `bun scripts/test-chat-e2e.ts` — this clones chat, installs
local orez, and runs e2e tests. currently it patches `--disable-wasm-sqlite`
into `lite:backend`. to test wasm, remove that patch from
`scripts/test-chat-e2e.ts:241`

## key architecture

### how zero-cache uses sqlite

```
orez spawns zero-cache process
  └── runner (run-worker.js) — creates HTTP server, spawns dispatcher
       └── dispatcher (main.js) — routes WebSocket connections
            ├── change-streamer — receives WAL from pg-proxy, writes to SQLite replica
            ├── replicator — monitors replica, notifies syncers
            └── syncer — handles WebSocket clients, runs view-syncer queries
```

each of these workers is fork()'d by `childWorker()` in
`node_modules/@rocicorp/zero/out/zero-cache/src/types/processes.js:59-80`.
the forked workers each open the replica DB (`zero-replica.db`). WAL2 mode
uses SHM for coordination.

### how the VFS SHM works

SQLite's WAL2 coordination uses shared memory:
- `xShmMap(pgno, pgsz)` — map a SHM page (WAL index header + hash tables)
- `xShmLock(offset, n, flags)` — acquire/release byte-range locks on SHM
- `xShmBarrier()` — memory fence between writes
- `xShmUnmap()` — unmap pages

in native sqlite, SHM is mmap'd from `db-shm` file and locks use fcntl. in
our wasm VFS, SHM is `_malloc`'d WASM heap memory — not shared across
processes.

### SQLite's barrier protocol

writer:
1. acquire exclusive SHM lock
2. write WAL index header copy 1 to SHM
3. call `xShmBarrier` — ensures copy 1 is visible
4. write WAL index header copy 2 to SHM
5. release exclusive lock

reader:
1. acquire shared SHM lock
2. read WAL index header copy 2 from SHM
3. call `xShmBarrier` — ensures copy 2 is read before copy 1
4. read WAL index header copy 1 from SHM
5. if copies match → consistent read
6. release shared lock

**the barrier must NOT touch the writer's data**. if the writer calls barrier
between writing copy 1 and copy 2, and the barrier reads from the file, it
would overwrite copy 1 with stale data → `SQLITE_PROTOCOL`.

the fix: barrier reads from file ONLY when NOT holding an exclusive lock
(reader case). writers flush to file on exclusive lock RELEASE.

### key constants (from vfs-pre.js)

```javascript
SQLITE_SHM_LOCK = 2       // not defined in vfs-pre.js, comes from sqlite3.h
SQLITE_SHM_UNLOCK = 1
SQLITE_SHM_SHARED = 4
SQLITE_SHM_EXCLUSIVE = 8
```

**note**: these constants are NOT in `vfs-pre.js`. they're passed by SQLite's C
code as flags to `xShmLock`. you'll need to define them in the VFS or use the
numeric values directly:
- lock + exclusive: `flags & 2 && flags & 8`
- unlock + exclusive: `flags & 1 && flags & 8`

### files

| file | what |
|------|------|
| `sqlite-wasm/native/vfs.js` | VFS implementation — SHM functions to fix |
| `sqlite-wasm/native/vfs-pre.js` | VFS constants and helpers |
| `sqlite-wasm/Makefile` | WASM build (`make`) |
| `sqlite-wasm/dist/` | built artifacts (symlinked to node_modules/bedrock-sqlite/dist/) |
| `src/index.ts:787-880` | startZeroCache — env setup, sqlite mode, spawn |
| `src/integration/integration.test.ts` | integration test for verification |
| `plans/wasm-sqlite-stability.md` | full research notes |

## verification checklist

- [ ] VFS file-backed SHM compiles (`cd sqlite-wasm && make`, no errors)
- [ ] wasm-sqlite unit tests pass (`bun test src/wasm-sqlite.test.ts`)
- [ ] native integration test still passes (`bun test src/integration/integration.test.ts`, expect 5/6 pass)
- [ ] wasm integration test passes (`FORCE_WASM=1 bun test src/integration/integration.test.ts`, expect 5/6 pass)
- [ ] no `SQLITE_PROTOCOL` or `SQLITE_IOERR` in zero-cache output
- [ ] replica WAL file has data in wasm mode (check `.orez-integration-test-*/zero-replica.db-wal` > 0 bytes)
