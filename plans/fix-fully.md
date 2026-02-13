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
2. [ ] Test latest with FULLY clean state (rm -rf .orez before EACH test)
3. [ ] If latest still fails, compare v0.1.0 vs latest code for differences
4. [ ] Test wasm mode

## Append

- User request: append updates to this file; do not replace prior notes.
