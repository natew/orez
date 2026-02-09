# WASM OOM Fix — Fast Loop Iterations

## status: in progress

## context
after initial sync succeeds (1,856 rows, 42 tables), zero-cache OOMs at 8GB
during "hydrating 6 queries" / "starting poke from 00:01 to 49zls0".
no logs appear — just silent memory consumption until crash.

## iteration log

### iteration 1 — diagnostics + safety fixes
- fix `nodejsRead` to use `_safeInt(offset)` (matches `nodejsWrite`)
- add row count diagnostic to `all()` — warn at 100k, throw at 10M
- fix `SqliteError` to propagate actual sqlite error codes
- add blob size guard in `_getColumnValue`
- rebuild wasm, copy to bedrock-sqlite, test

### iteration 2 — scanStatus infinite loop
**symptom**: "RangeError: Invalid array length" in getScanstatusLoops
**cause**: zero-cache calls `stmt.scanStatus(idx, ...)` expecting undefined when idx out of bounds. missing method caused garbage return values → infinite loop → array exceeds 2^32 limit
**fix**: added `SP.scanStatus = function() { return undefined; }` to inline shim in index.ts
**note**: patchSqliteForWasm() has caching (`if (current.includes('OrigDatabase')) return`) so must delete old patches for new shim to apply
**blocker**: "Cannot find package .../lib/index.js" — native build failed, file doesn't exist
**fix2**: change `if (!existsSync(indexPath)) return` to create patch even when file missing

### iteration 2 — stale trigger cleanup
- change tracker was installing triggers on ALL public tables (including auth)
- zero-cache only knows about tables in `zero_chat` publication
- when user logs in → trigger fires on `user` → zero-cache crashes "Unknown table"
- fix: query publication tables, drop stale triggers on non-published tables

### iteration 3 — WAL visibility / SHM cross-process fix
- root cause: `_zero.replicationState` exists in replica but replica monitor can't see it
- diagnosis: zero-cache spawns workers via `child_process.fork()` — separate processes
- WASM SQLite SHM (`_shmRegistry`) is in-process JS memory, NOT shared between OS processes
- native SQLite uses `mmap` for SHM, which IS shared between processes
- fix: set `SINGLE_PROCESS=1` env var so zero-cache runs all workers in same process
- in single-process mode, all Database instances share the same `_shmRegistry`
- WAL coordination works because all connections are in the same WASM instance
