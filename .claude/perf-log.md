# orez perf optimization log

baseline: 46 passed, 5 failed, 9.4min (PGlite proxy)
target: ~5x faster (currently ~10x slower than postgres)

## what we tried

- **50ms poll interval** (was 250ms) → no wall-clock improvement, more ops
- **TCP_NODELAY on socket** → no measurable improvement
- **signalReplicationChange / LISTEN notify** → added but not measurably faster
- **fast path bypass (skip pg-gateway)** → regressed to 42/51 tests, reverted
- **connection-aware tx state tracking (25P02 fix)** → fixed migration failures ✓
  - tracks which socket owns pglite transaction, auto-ROLLBACK only for stale state
- **CHAT_REF pinning** → needed because ~/chat HEAD was actively breaking
- **single-call batching (concat Parse+Bind+Exec+Sync → one execProtocolRaw)** → ✗ broke migrations
  - pglite only processes ONE wire protocol message per execProtocolRaw call
- **per-msg batching under single mutex** → ✗ still broke with "Message code not yet implemented" code:123
  - even copying buffers, pglite returns ErrorResponse for buffered messages
  - something about processing Parse/Bind/Execute out-of-band from pg-gateway confuses pglite
- **syncToFs: false** → testing now (isolated, no batching)
- **playwright timeout patch fix** (10min→20min) → testing now
  - old patch targeted `minutes(8)`, actual was `minutes(10)`

## what works / is committed

- schema query cache (dedup information_schema queries)
- notice suppression (25001, 25P01, 55000)
- query rewrites (version, wal_level, isolation level, read only)
- noop query interception (SET TRANSACTION, SET SESSION)
- per-instance mutexes (postgres, cvr, cdb)

## current approach (testing now)

- per-message execProtocolRaw under single mutex acquisition
- syncToFs: false for all operations
- playwright timeout fixed to 20min
- buffer Parse/Bind/Describe/Execute/Close → execute each on Sync under one mutex
- strip intermediate ReadyForQuery messages

## ideas not yet tried

- pg-gateway detach() for raw mode → skip pg-gateway overhead entirely
- reduce zero-cache poll frequency further (adaptive based on load)
- profile pglite execProtocolRaw to find internal bottlenecks
- connection pooling / persistent prepared statements
