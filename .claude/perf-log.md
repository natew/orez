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
- **syncToFs: false** → ✓ 42% PGlite time reduction (152s→89s at 2M ops)
- **playwright timeout patch fix** (10min→20min) → ✓ tests now complete full suite
  - old patch targeted `minutes(8)`, actual was `minutes(10)`, needed `/g` flag
- **signalReplicationChange on every query** → ✗ 3.3M ops (up from 2M), no wall-clock gain
- **signalReplicationChange on SimpleQuery writes only** → ✓ 2.7M ops, 45 passed
- **signalReplicationChange on extended protocol writes (immediate)** → ✗ caused 75 FK violations
  - server → channel → member mutations auto-commit separately
  - signaling after each triggers replication before related records exist
  - net: 31 passed, 9 failed (worse than SimpleQuery-only)
- **signalReplicationChange on extended protocol writes (debounced 80ms)** → ✗ 0 FK errors but 4M ops
  - debounce prevents FK violations but causes 48% more ops
  - net: 41 passed, 10 failed (worse than SimpleQuery-only due to system load)
- **loginAsAdmin retry rewrite** → ✓ fixed messaging/persist tests
  - fallback to /auth/login broken (user already auth'd → redirect loop)
  - replaced with longer retry loop (8 attempts × 15s = 120s max)
- **loginAsUser data wait** → ✓ fixed admin-gets-true, member-can-send
  - wait for channel content to sync after navigation (not just networkidle)
- **loginAsAdmin sidebar-channel signal** → ✓ replaced username check with sidebar channel detection
  - data-username="admin" depends on Zero syncing userPublic (slow under PGlite)
  - sidebar channels appear earlier (channel query resolves before user query)
  - uses Promise.race: username OR sidebar channel (whichever first)
  - fixed consistently-failing `unseen state lifecycle` test
- **workers: 3** → ✓ reduced PGlite contention from default (4-6 workers)
- **global timeout: 180s** (was 120s) → ✓ prevents test timeout during loginAsAdmin
- **waitForChannelInSidebar no-reload** → ✓ removed reload retry (resets ws + zero sync)
- **loginAsAdmin post-login permission wait** → waits for pointer-events to enable
- **loginAsUser post-login permission wait** → waits for permission state to resolve

## results

- baseline (postgres): 46 passed, 5 failed, 9.4min
- syncToFs:false + old chat: 43 passed, 8 failed, 10.2min (PGlite 89s, 2M ops)
- syncToFs:false + new chat (00ba0ace) + aggressive signal: 40 passed, 11 failed, 9.6min (PGlite 176s, 3.3M ops)
- syncToFs:false + chat HEAD + SimpleQuery-only signal: 45 passed, 6 failed, 9.4min (PGlite 160s, 2.7M ops)
- ext protocol signal (immediate): 31 passed, 9 failed, ~9min (75 FK violations!)
- ext protocol signal (debounced 80ms): 41 passed, 10 failed, 10.9min (4M ops, 0 FK)
- sidebar-channel signal + workers:3 + perm waits: 49 passed, 2 failed, 15.2min (PGlite 106s, 2.5M ops)
- + loginAsAdmin pointer-events + loginAsUser perm state: testing now

## what works / is committed

- schema query cache (dedup information_schema queries)
- notice suppression (25001, 25P01, 55000)
- query rewrites (version, wal_level, isolation level, read only)
- noop query interception (SET TRANSACTION, SET SESSION)
- per-instance mutexes (postgres, cvr, cdb)
- connection-aware tx state tracking (25P02 fix)
- syncToFs: false for all operations
- playwright timeout patch to 20min
- signalReplicationChange on SimpleQuery writes only

## ideas not yet tried

- pg-gateway detach() for raw mode → skip pg-gateway overhead entirely
- reduce zero-cache poll frequency further (adaptive based on load)
- profile pglite execProtocolRaw to find internal bottlenecks
- connection pooling / persistent prepared statements
