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
the backend-readiness boundary in `scripts/test/e2e.ts`. Upstream chat gives
the cold lite Postgres and Zero ports 60 seconds; orez's canonical wrapper
patches only those two waits to 120 seconds. That deadline measures orez+zero
boot, including hundreds of migration and seed statements.

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

## 2. The two budgets that matter

`scripts/test/e2e.ts` in chat does:

```ts
await waitForPort(ports.postgres, { timeoutMs: 60_000 })
await waitForPort(ports.zero, { timeoutMs: 60_000 })
await waitForPort(ports.web, { timeoutMs: 120_000 })
```

`scripts/test-chat-e2e.ts` rewrites the first two cold-lite waits to 120 seconds
inside the mirrored `test-chat` checkout. It deliberately does not change
Playwright timeouts, retries, `maxFailures`, chat source, or production
behavior. Postgres opens quickly; Zero opens only after migrations and schema
discovery complete, so it is normally the limiting readiness check.

The other limit is more important for correctness and cost: `ZeroDO` allows
150,000 billable SQLite rows per rolling five-minute window by default. A cold
Chat global setup must remain below that circuit. A longer readiness timeout
cannot make a write-amplification bug acceptable, and raising the circuit is
not a performance fix.

## 3. What cold Chat setup actually writes (measured 2026-07-13)

Chat is not seeding hundreds of thousands of application rows. The focused
migration/reaction profile was roughly 56k billable rows. The reaction loop
attempted 1,853 inserts and 1,853 metadata updates; at the observed physical
costs those accounted for 9,265 and 3,706 billable rows respectively.

The apparent 500k-to-1m workload was a rollback implementation bug. In the
failing profile:

| measurement                                                | billable rows |
| ---------------------------------------------------------- | ------------: |
| complete failing profile                                   |     1,191,374 |
| 1,244 copies of the growing `"chat_0/cdc_changeLog"` table |     1,077,552 |
| clean Playwright global setup after row journaling         |       125,402 |

The non-Postgres `zero_cdb` connection used emulated transactions, but its
parsed DML was not registered for row capture. It therefore fell back to a
full-table transaction snapshot. Repeating that while `cdc_changeLog` grew
made the cost quadratic.

The fix applies parsed-DML CDC to every database role. Application tables use
published transactional row changes. CVR/CDB and other private tables capture
the same before/after row images with `publish: false`, solely for rollback and
crash recovery. Full-table snapshots remain only for statements the compiler
cannot classify. Do not add a database-name shortcut that sends parsed
internal DML back to snapshot fallback.

### Historical request distribution (measured 2026-05-26)

A 15s window of Chat e2e boot produced about 3,120 `/exec` calls. The profile
temporarily logged each SQL prefix at the top of `handleExec` in
`src/cf-do/worker.ts` and split roughly as follows:

| count | shape                                                     | source             |
| ----- | --------------------------------------------------------- | ------------------ |
| 1853  | `INSERT INTO reaction(...) ON CONFLICT DO NOTHING`        | chat seed data     |
| 665   | `UPDATE reaction SET keywords=?,category=? WHERE value=?` | chat metadata fill |
| 550   | `INSERT OR REPLACE INTO "_orez_pg_metadata" ...`          | orez (was per-row) |
| ~40   | misc DDL / catalog probes                                 | migrations         |

The first two are chat seed loops. They can eventually be batched below the
Postgres wire boundary, but their row counts do not explain a 500k+ profile.
The third was pure orez overhead and was fixed separately.

To re-capture this distribution, temporarily log each SQL prefix from
`handleExec`, run the harness, then:

```bash
awk -F'[exec] ' '{print $2}' /tmp/wrangler-do.log | \
  awk '{$1=""; print}' | sort | uniq -c | sort -rn | head -30
```

## 4. Amplification regressions and fixes

### 4a-4d. Request amplification (landed 2026-05-26)

These are all in `src/pg-proxy-do-backend.ts`.

#### `persistDurableMetadata` was a per-row HTTP loop

It iterated `schemaMetadata` (all tables × all columns) and `publications` and
issued one `await doExecResult` per row. A migration that added five columns
to a table with already-known columns re-persisted _every_ metadata row, every
time. Fix: build one multi-row `INSERT OR REPLACE INTO ... VALUES (?,?,?,?),
(?,?,?,?),...` chunked at 200 rows (SQLite ~999 param cap / 4 cols).

#### `applyStatementMetadata` was called after every SQL statement

Inside a chat migration transaction, this fired N times instead of once. Fix:
when `inTransaction`, set `txMetadataDirty = true` and skip; flush in
`commitTransaction` (and the existing `rollbackTransaction` persist already
covers the rollback path).

#### `snapshotTransactionChangeTables` re-ran a `sqlite_master` scan per write

For every tracked write inside a transaction, this called
`tableExistsInDo('_zero_changes')`, `tableExistsInDo('_zero_change_state')`,
and a `SELECT name FROM sqlite_master WHERE name LIKE '%zero_watermark%'`.
The change tables only need snapshotting ONCE per transaction. Chat's seed
loops do thousands of tracked writes inside one tx — this was ~4 extra HTTPs
per write, easily 7,000+ wasted HTTPs during boot. Fix: add a
`txChangeTablesSnapshotted` boolean, early-return when true, reset in
`clearTransactionState`.

#### `snapshotTransactionTable` probed sqlite_master on every first-write-per-tx

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

At the time, the combined effect reduced that measured backend boot from more
than 60 seconds to about 13 seconds and all 51 then-current Chat e2e tests
passed. Treat those as historical point-in-time results, not current gates.

### 4e. Internal transaction snapshots copied a growing change log (landed 2026-07-13)

`DoBackend.trackingForStatement` used to return no tracking metadata for
non-`postgres` database names. For zero-cache's internal CDB transaction, that
sent each parsed write through `snapshotTransactionTable`, repeatedly copying
the growing `"chat_0/cdc_changeLog"` table.

The transaction path now captures row before-images for all parsed DML. The
worker's generated SQLite triggers write the row mutation and CDC staging row
in the same SQLite statement, including side effects from business triggers.
Commit publishes one complete application transaction; rollback or crash
recovery restores before-images in reverse order. Internal records are marked
`publish: false` and are removed at commit instead of entering `_zero_changes`.

The regression tests live in `src/cf-do/cdc.test.ts`,
`src/cf-do/worker-cdc.test.ts`, and `src/cf-do/tx-journal.test.ts`. Several
logical-CDC cases are adapted from Turso's CDC tests: failed multi-row
statements, transaction rollback, and primary-key updates. Business-trigger
side effects and the grouped commit/rollback cases are Orez-specific.

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
5. Extends only the mirrored cold-lite PG/Zero readiness waits to 120 seconds.
6. Runs `bun run test e2e --integration --lite` in `test-chat/`.

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

| symptom                                                          | likely cause                                                        |
| ---------------------------------------------------------------- | ------------------------------------------------------------------- |
| `task: backend failed after ~2m`                                 | Zero did not open within the wrapper's 120s cold-start budget       |
| `task: backend failed` with no time                              | wrangler not running or `DO_BACKEND_URL` not set                    |
| `ECONNREFUSED 127.0.0.1:8799`                                    | wrangler dev died or never started                                  |
| HTTP 429 `writeBudgetExceeded` during setup                      | profile billable writes; do not raise 150k before finding the loop  |
| `TG_OP is not defined` or trigger errors                         | chat trigger function uses Postgres `TG_OP`; orez skips these on DO |
| "Ignoring mutation from X with ID N as it was already processed" | chat-side mutation dedup, not orez — non-fatal                      |
| First test passes, second hangs                                  | leftover state from previous run; reset `.wrangler/state/v3/do/`    |

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

If Chat approaches either the 120-second harness budget or the 150k write
budget again, profile first. Plausible follow-ups include:

- **Batch the chat seed inserts on the orez side.** Chat sends 1,853 individual
  `INSERT INTO reaction ... ON CONFLICT DO NOTHING` over the wire. If we
  detect a series of identical-shape inserts within one tx, we could
  accumulate and flush via `/batch` (one HTTP for N inserts). Risky because
  prepared statements, result timing, and transaction errors need careful
  handling. This is a latency/write-count follow-up, not the explanation for
  the former million-row profile.
- **Pipeline `/exec` HTTP calls.** Each call is round-tripped serially through
  wrangler. A small queue + single in-flight HTTP/2 socket would amortize
  the per-call overhead.

Do _not_ attempt these speculatively. Re-measure boot HTTP distribution first
(see §3), then target the largest bucket.
