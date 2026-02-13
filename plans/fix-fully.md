# Fix WASM and Non-WASM SQLite Modes

## Key Findings

### Session 1 - 2026-02-13

**v0.1.0 with `--disable-wasm-sqlite`**:

- ✅ Phase 1 - Onboarding dialog appeared (PASS - sync works!)
- ❌ Phase 2 - Create Tamagui server timed out (unrelated)

**latest with `--disable-wasm-sqlite`**:

- ❌ Phase 1 - WebSocket connections refused
- **Root cause**: zero-cache crashed (code 255)
- **Error**: `duplicate key value violates unique constraint "changeLog_pkey"`
- This is stale data in pglite's changeLog table from previous runs

**KEY INSIGHT**: The issue is NOT wal2 mode - it's stale data corruption causing zero-cache to crash.

## Test Commands

```bash
# Full clean (removes all data AND kills ports before)
bun lite:clean

# Full clean wasm (removes all data AND kills ports before)
bun lite:clean:wasm

# Run test (native mode)
cd ~/chat && bun src/integration/e2e/orez.ts --disable-wasm-sqlite

# Run test (wasm mode)
cd ~/chat && bun src/integration/e2e/orez.ts
```

## Next Steps

1. ✅ Verify v0.1.0 works with clean state
2. ✅ Test latest with FULLY clean state - WORKS!
3. ✅ Native mode (`--disable-wasm-sqlite`) confirmed working
4. [ ] Test wasm mode

## Session 2 - 2026-02-13 (continued)

**Root cause of earlier test failures**:

- Test script bug: was using `bun lite:clean` for native mode, which doesn't pass `--disable-wasm-sqlite`
- Fixed to use `bun lite:clean:no-wasm` for native mode
- The `replica db must be in wal2 mode (current: delete)` error was because test ran in wasm mode (which uses `delete` journal mode) instead of native mode

**latest with `--disable-wasm-sqlite` (properly cleaned)**:

- ✅ Phase 1 - Onboarding dialog appeared (sync works!)
- ✅ Phase 2 - Create Tamagui server works
- ✅ Phase 3 - Prod sync test added

**Conclusion**: Native mode works correctly on latest. No code regression - it was a test script bug.

## Session 3 - 2026-02-13 (FIXED)

**Root cause identified via bisect**:

- v0.1.3 works, v0.1.4 breaks
- v0.1.4 changed from NODE_PATH shadowing to in-place overwriting of `@rocicorp/zero-sqlite3`
- Once wasm mode runs, it destroys the original package and native mode breaks
- The backup/restore system added later didn't fully solve this

**Fix applied**:

- Reverted to NODE_PATH approach (like v0.1.3)
- Shim is written to TMPDIR, not node_modules
- NODE_PATH shadows the real package during zero-cache execution
- Non-destructive: doesn't modify node_modules at all
- Uses `wal2` journal mode (required by zero-cache)

**Test results after fix**:

- ✅ Native mode: Phase 1 (onboarding) + Phase 2 (Tamagui server) PASS
- ✅ WASM mode: Phase 1 (onboarding) + Phase 2 (Tamagui server) PASS
- ⏳ Phase 3 (prod sync) times out - separate issue, not orez related

## Append

- User request: append updates to this file; do not replace prior notes.
