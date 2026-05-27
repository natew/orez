# orez DO mode + chat e2e — what to know before debugging

If you landed here because chat e2e is failing against the orez Cloudflare DO
backend, read this whole file first. The same handful of pitfalls have eaten
multiple agent-sessions; the goal is for the next one to spend zero time
re-discovering them.

## 1. The architecture in one paragraph

chat in `--lite` mode launches `orez` as its database. orez exposes a Postgres
wire protocol on `VITE_PORT_POSTGRES`. zero-cache (also embedded in lite mode)
connects to orez over PG protocol and reads/writes both schema and data. orez,
in DO mode, forwards every translated SQL statement as an HTTP POST to a
wrangler-hosted `ZeroDO` worker. That worker executes against
`ctx.storage.sql` (DO SQLite). When chat e2e fails, it almost always fails at
**chat's 60-second `waitForPort(ports.zero, { timeoutMs: 60_000 })`** in
`scripts/test/e2e.ts`. That deadline measures orez+zero boot, including
hundreds of migration statements.

```
chat lite mode (one process tree)
├── bun run:dev orez --disable-wasm-sqlite ...
│     └── orez PG-protocol server (port 5632 by default; +PORT_OFFSET in tests)
│           └── DoBackend (src/pg-proxy-do-backend.ts)
│                 └── HTTP POST /exec or /batch → wrangler dev (port 8799)
│                       └── ZeroDO durable object (src/cf-do/worker.ts)
│                             └── ctx.storage.sql (DO SQLite)
├── bun migrate run  (chat's --on-db-ready callback)
└── zero-cache process — waits for orez PG, then reads schema, opens port 5048
```

## 2. The single number that matters: 60 seconds

`scripts/test/e2e.ts` in chat does:

```ts
await waitForPort(ports.postgres, { timeoutMs: 60_000 })
await waitForPort(ports.zero,     { timeoutMs: 60_000 })
await waitForPort(ports.web,      { timeoutMs: 120_000 })
```

Postgres opens almost immediately (orez TCP server). Web opens after Vite. The
`zero` port is what kills you — zero-cache only opens it after orez has
finished applying all of chat's migrations AND zero has finished reading the
resulting schema. If boot takes >60s the whole harness fails with
`task: backend failed after 1m 1s` and the test runner never even starts.

**Do not "fix" this by bumping the timeout silently.** Boot time is a real
budget that needs to fit on a developer's laptop and CI. If you need more, do
it deliberately in `scripts/test-chat-e2e.ts` (the wrapper) by post-sync
patching the file — and document why.

## 3. Where boot HTTP calls go (measured 2026-05-26)

A 15s window of chat e2e boot, captured by adding
`console.log(\`[exec] ${sql.slice(0, 80)}\`)` at the top of `handleExec` in
`src/cf-do/worker.ts`, produced ~3,120 /exec calls per 15s, split roughly:

| count | shape                                                                | source              |
|-------|----------------------------------------------------------------------|---------------------|
| 1858  | `INSERT INTO reaction(...) ON CONFLICT DO NOTHING`                   | chat seed data      |
| 665   | `UPDATE reaction SET keywords=?,category=? WHERE value=?`            | chat metadata fill  |
| 550   | `INSERT OR REPLACE INTO "_orez_pg_metadata" ...`                     | orez (was per-row)  |
| ~40   | misc DDL / catalog probes                                            | migrations          |

The first two are chat seed loops — you cannot reduce them without changing
chat. The third is **pure orez overhead** and was the obvious target.

To re-capture this distribution, add the same `console.log` temporarily, run
the harness, then:

```bash
awk -F'[exec] ' '{print $2}' /tmp/wrangler-do.log | \
  awk '{$1=""; print}' | sort | uniq -c | sort -rn | head -30
```

## 4. The three amplification bugs we fixed (commit landed 2026-05-26)

All in `src/pg-proxy-do-backend.ts`. Before/after:

### 4a. `persistDurableMetadata` was a per-row HTTP loop

It iterated `schemaMetadata` (all tables × all columns) and `publications` and
issued one `await doExecResult` per row. A migration that added five columns
to a table with already-known columns re-persisted *every* metadata row, every
time. Fix: build one multi-row `INSERT OR REPLACE INTO ... VALUES (?,?,?,?),
(?,?,?,?),...` chunked at 200 rows (SQLite ~999 param cap / 4 cols).

### 4b. `applyStatementMetadata` was called after every SQL statement

Inside a chat migration transaction, this fired N times instead of once. Fix:
when `inTransaction`, set `txMetadataDirty = true` and skip; flush in
`commitTransaction` (and the existing `rollbackTransaction` persist already
covers the rollback path).

### 4c. `snapshotTransactionChangeTables` re-ran a `sqlite_master` scan per write

For every tracked write inside a transaction, this called
`tableExistsInDo('_zero_changes')`, `tableExistsInDo('_zero_change_state')`,
and a `SELECT name FROM sqlite_master WHERE name LIKE '%zero_watermark%'`.
The change tables only need snapshotting ONCE per transaction. Chat's seed
loops do thousands of tracked writes inside one tx — this was ~4 extra HTTPs
per write, easily 7,000+ wasted HTTPs during boot. Fix: add a
`txChangeTablesSnapshotted` boolean, early-return when true, reset in
`clearTransactionState`.

### 4d. `snapshotTransactionTable` probed sqlite_master on every first-write-per-tx

For every first write to a table within a transaction,
`snapshotTransactionTable` called `tableExistsInDo` (1 /exec to sqlite_master)
before doing the snapshot CREATE. But we already have `schemaMetadata` —
populated as a side-effect of every CREATE TABLE we translate — so if a
table is in there, it exists. Fix: check `schemaMetadata.has(table)` first
and only fall back to the sqlite_master probe for tables we haven't
registered. Saves one HTTP per first-tx-write per known table.

This last optimization is what closed the mutation-race gap for the unseen
"speed bellwether" test (see §5b). Removing it will likely cause that test
to start failing again.

**Combined effect:** orez backend boot dropped from "fails at 60s" to "ready in
~13s" against the same wrangler + same chat harness on the same laptop, and
**all 51 chat e2e tests pass on first attempt** (4.2 min total runtime).

## 5. Running chat e2e against your local changes

From `~/orez`:

```bash
# 1. Build orez and start wrangler hosting the DO worker
bun run build
cd src/cf-do && bunx wrangler dev --port 8799 --local --no-show-interactive-dev-session > /tmp/wrangler-do.log 2>&1 &
cd -

# 2. Run the wrapper that syncs chat → test-chat and launches the e2e harness
PORT_OFFSET=30 bun scripts/test-chat-e2e.ts > /tmp/orez-e2e.log 2>&1 &

# 3. Tail logs
tail -f /tmp/orez-e2e.log
```

The wrapper (`scripts/test-chat-e2e.ts`) is the canonical entry point. It:

1. Mirrors `~/chat` (or wherever `CHAT_DIR` points) into `~/orez/test-chat/`.
2. Copies orez `dist/` into `test-chat/node_modules/orez/dist/`.
3. Sets `OREZ_DATA_DIR=/tmp/orez-{PORT_OFFSET}` and PORT_OFFSET-shifted ports.
4. Sets `DO_BACKEND_URL=http://127.0.0.1:8799` so orez picks the DO backend.
5. Runs `bun run test e2e --integration --lite` in `test-chat/`.

Multiple e2e runs share one wrangler instance. Reset DO state between runs by
deleting `~/orez/src/cf-do/.wrangler/state/v3/do/` (so migrations re-apply
fresh). Resetting only orez state without resetting DO state will leave
orphaned tables and miss migration-replay bugs.

## 5b. The "speed bellwether" test

`channel-unseen.test.ts` → `multiple channels track unseen independently` is
the test that fails first when the backend is too slow. It sends two
messages from a second browser context back-to-back then closes the context
without waiting for them to round-trip — so the mutation push has to clear
in the time between `sendMessageIn` returning and `ctxB.close()` returning.

A slow backend loses that race; the mutations get cancelled when the
context closes; user a never sees the unseen indicator update; the
`expectChannelUnseen(page, 'Ops', true)` assertion fails.

It is **not** flaky in a healthy config. If you see this test fail, the
backend is too slow. Fix the backend, don't "fix" the test. The optimization
in §4d (skip `tableExistsInDo` when schemaMetadata knows the table) is what
got HEAD over the line — adding HTTP round trips back to the
first-write-per-tx path will likely regress this test.

## 6. Common failure modes and what they mean

| symptom                                                            | likely cause                                                          |
|--------------------------------------------------------------------|-----------------------------------------------------------------------|
| `task: backend failed after 1m 1s`                                 | zero port didn't open within 60s — orez boot too slow                 |
| `task: backend failed` with no time                                | wrangler not running or `DO_BACKEND_URL` not set                      |
| `ECONNREFUSED 127.0.0.1:8799`                                      | wrangler dev died or never started                                    |
| `TG_OP is not defined` or trigger errors                           | chat trigger function uses Postgres `TG_OP`; orez skips these on DO   |
| "Ignoring mutation from X with ID N as it was already processed"   | chat-side mutation dedup, not orez — non-fatal                        |
| First test passes, second hangs                                    | leftover state from previous run; reset `.wrangler/state/v3/do/`      |

## 7. What never to touch

- **Chat source.** The test harness must be identical to what chat ships. If
  you need to change behaviour for a test run, do it as a post-sync patch in
  `scripts/test-chat-e2e.ts` and document why.
- **`maxFailures`, `timeout`, or `retries` in `test-chat/playwright.config.ts`.**
  Those are mirrored from chat; touching them is a cheat that masks real
  regressions and gets reverted next sync.
- **PGlite.** This whole path exists because PGlite doesn't fit in the
  Cloudflare DO 128 MB budget. Do not add a PGlite fallback for DO mode.

## 8. Future optimization opportunities

If chat boot grows past the 60s budget again, the cheapest remaining wins
look like:

- **Batch the chat seed inserts on the orez side.** chat sends ~1,858 individual
  `INSERT INTO reaction ... ON CONFLICT DO NOTHING` over the wire. If we
  detect a series of identical-shape inserts within one tx, we could
  accumulate and flush via `/batch` (one HTTP for N inserts). Risky because
  prepared statements + bind params don't line up; would need careful
  tracking.
- **Skip `tableExistsInDo` after first-time table creation.** `DoBackend`
  could memoize known-existing tables for the life of the connection and
  drop the probe.
- **Pipeline `/exec` HTTP calls.** Each call is round-tripped serially through
  wrangler. A small queue + single in-flight HTTP/2 socket would amortize
  the per-call overhead.

Do *not* attempt these speculatively. Re-measure boot HTTP distribution first
(see §3), then target the largest bucket.
