# Trade-offs and operational reality

This page is the honest account of what you take on by running the sync engine
on Durable Object SQLite instead of zero-cache on Postgres. The engine is
simpler and cheaper to operate in most respects, and it has two costs that are
easy to underestimate: write amplification billing on Cloudflare, and a
client-side reset when you change the sync host origin.

## Durable Object storage instead of an external database

The engine and its storage live in the same Durable Object. There is no separate
database process to run, connect to, scale, or back up out of band. One
namespace is one object holding its own SQLite. This is why a deployment needs
no Postgres, no connection pool, and no long-lived cache process.

The cost is that a namespace inherits Durable Object limits. Memory is the
tight one: the DO memory budget makes carrying a full Postgres-in-WASM
untenable, which is exactly why the engine is a native SQLite protocol
implementation rather than PGlite in a DO. Storage per object is bounded, and
one object is a single isolate. A namespace that outgrows a single object's
limits is a data-modeling problem you solve by partitioning into more
namespaces, not by scaling the object up.

## Single-writer semantics

One Durable Object per namespace means one writer per namespace, and writes
serialize through that object. This buys correctness for free: every pull runs
in one synchronous SQLite transaction and sees one consistent snapshot, with no
repeatable-read gymnastics of the kind a Postgres-backed server needs. Push
mutations commit atomically, rows and last-mutation-id together.

The ceiling is the flip side: a single namespace's throughput is one object's
throughput. The measured chat staging run pushed 10,000 mutations across 16
concurrent writers at roughly 48 per second through one object, which is fine
for a chat server's write rate but is not a database you point a bulk import at.
Work that fans out (many servers, many projects) scales by having many
namespaces, each its own object.

## Write amplification on Cloudflare

This is the trade-off that caused real incidents, so it gets the most detail.

Cloudflare bills Durable Object SQLite by `rowsWritten` at the physical level.
Every logical `INSERT` or `UPDATE` also writes each index it touches, so one
application row write costs `1 + N_indexes` billable rows, plus the
change-tracking rows the sync model appends for every write. The billable number
is therefore much larger than the application row count, and it grows with your
index count.

The measured figures from the 2026-07-10 and 2026-07-11 soot incident analysis:

- **About 1.3k billable rows per push.** 100 control pushes wrote 133,819
  billable rows on soot's data tier. A single small mutation is not a single
  billable row; it is a mutation plus its indexes plus change tracking, times
  the tables it touches.
- **About 127.5k billable rows for one cascading account delete.** A single
  `limit=1` deletion that cascades across foreign keys finalized at 127,555
  `ZeroSqlDO` writes. One user action at the top of a foreign-key graph can be
  five orders of magnitude more billable rows than it looks like.

Two consequences follow. First, index count and foreign-key cascade depth are
now billing decisions, not just schema decisions. Second, the counter is subtle:
`SqlStorageCursor.rowsWritten` is the billing value and it can keep increasing
while a `RETURNING` cursor is iterated. Version 0.4.53 sampled it before the
cursor was consumed and undercounted; the fix in 0.4.54 meters the monotonic
cursor delta through `toArray()`, `one()`, `next()`, iteration, and `raw()`
iteration (`packages/sync-cf-host/src/write-safeguards.ts`,
`trackBillableCursorRows`).

### The circuit breakers

Because a runaway writer bills real money and can wedge an object, the system has
defense in depth (`plans/orez-write-safeguards.md`). All three are independent,
so a failure in one does not disable the others.

1. **Data-worker write budget** (`src/cf-do/worker.ts`). The source `ZeroSqlDO`
   meters billable rows in a rolling window. Past `OREZ_DO_WRITE_BUDGET_ROWS`
   (default 150,000 per five minutes) it becomes sticky and mutating endpoints
   return HTTP 429 `writeBudgetExceeded`. Reads stay open, the trip is persisted
   so an eviction cannot quietly reopen it, and a `POST /_orez/write-budget/reopen`
   with the admin token clears it. The 150k default sits below soot's external
   200k-per-five-minute alert.
2. **Sync-host ingest breaker** (`packages/sync-cf-host/src/host.ts`). One
   breaker catches two signatures: more than `ingestBudgetRows` billable rows in
   the window, and a non-advancing upstream cursor while pages keep arriving
   (`ingestCursorStalled`), which is the signature of a partial boot replaying
   forever. It returns a structured 429 and backs off with capped exponential
   cooldown.
3. **Delegated push bounds** (`packages/sync-cf-host/src/host.ts`). Delegated
   pushes retry only transport failures, 429, and 5xx, at most
   `delegatedPushRetry.maxAttempts` times (default 3), so a failing app endpoint
   cannot become an internal hot loop.

The budgets deliberately keep the hot-path counter in the worker layer and
persist only the sticky trip state. Metering every write into a table would add
billed writes and amplify the very incident it is meant to contain.

## Cookie domain and cutovers

The cookie a client stores is the engine's change-log watermark, and a Zero
client persists its local store keyed to a server identity. When you move an app
to a different sync host origin, or start a fresh engine with a fresh watermark
domain, existing clients hold cookies from the old server that do not correspond
to the new engine's watermark. Those clients reset and re-snapshot on their next
pull.

This reset is cheap and correct. It is the same snapshot path used for a fresh
client or a below-floor cookie, and measured project snapshots are small. It is
also user-visible: a client that had local state throws it away and rebuilds from
a full snapshot once. Plan cutovers around it. Chat's staging already ran on the
rust cookie domain, so starting fresh there was free; the apex cutover inherits
the reset-on-cutover client story deliberately rather than trying to preserve
cookies across the domain change (`plans/rust-sync-upstream-ingest.md`).

## Where this engine fits, and where it does not

It fits an app that wants Zero's client experience without operating Postgres and
zero-cache: per-namespace data that fits a Durable Object, a moderate per-object
write rate, and a schema whose index and cascade cost you have looked at. It is a
poor fit for a single namespace with a very high sustained write rate, a data set
too large for one object, or a workload dominated by wide cascading deletes,
unless you have budgeted the billable-row cost of those deletes up front.
