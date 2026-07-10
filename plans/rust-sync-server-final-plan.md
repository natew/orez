# Rust SQLite Zero sync server: final unified plan

Status: ready for execution

Date: 2026-07-09

Protocol baseline: Zero 1.7.0, protocol v51

Primary consumers: `~/soot` and `~/chat`

This plan supersedes and merges four documents, now removed from the tree
and preserved in git history:

- `rust-sync-server-plan.md` (independent plan A)
- `rust-sqlite-zero-implementation-plan.md` (independent plan B)
- `rust-sqlite-zero-resolved-plan.md` (plan B author's resolution)
- `rust-sync-server-plan-comparison.md` (plan A author's resolution)

Both resolutions converged on the same architecture. Where they differed
(sequencing, the Chat compatibility branch, the push transaction API), this
document makes the final call and records why. Alternatives the source plans
considered but this plan does not adopt are recorded in the appendix so
their reasoning survives the cleanup.

## Decision

Build a SQLite-only sync engine in Rust with two hosts from the first
milestone:

1. A native host (`axum` + `rusqlite`, WAL, one file per namespace) for
   conformance, benchmarks, and Rust-only deployments.
2. The same engine compiled to WebAssembly, embedded in a thin TypeScript
   Durable Object host for Cloudflare.

The product protocol is the enhanced HTTP pull dialect. The baseline
endpoints (`POST /pull`, `POST /push`) stay byte-compatible with the vendored
transport so every existing harness lane keeps gating them unchanged. A
query-aware extension (desired queries, durable row membership) is additive
and versioned, and is required before broad Chat production use. Soot does
not need it.

Realtime propagation comes from a notification-only wake channel (see the
Protocol section): each client holds a socket to its namespace host that
carries no data and only signals "pull now" after a commit. Interval polling
alone caps cross-client propagation at the poll interval (the shipped
on-zero default is 30 seconds), which is unusable for Chat and for realtime
collaboration on Soot's project plane. The wake channel gives push-shaped
latency to both consumers without porting Zero's websocket protocol, and it
carries zero correctness weight: convergence comes entirely from the pull
protocol.

A full port of Zero's websocket, CVR, and incremental-view-maintenance
server is out of scope. Full websocket parity would mean porting the ~18k
LOC ZQL/IVM engine to serve a surface both consumers are migrating off.
Reconsider only if measurements prove the recomputation design insufficient.

### Responsibility split

The TypeScript Durable Object host owns the JavaScript platform boundary:

- transaction entry and exit (`transactionSync` for pull, async
  `ctx.storage.transaction` for push)
- request routing and namespace resolution
- authentication handoff (consumers resolve their own sessions and pass
  normalized user claims to Rust; no new token format)
- running the consumer's TypeScript mutators in-process, inside the same
  Durable Object transaction
- conversion between JavaScript values and Rust inputs and outputs

Rust owns the deterministic sync engine:

- cookies, watermarks, retention, and reset decisions
- change-log interpretation
- Zero v51 query AST validation and SQLite compilation
- durable desired-query and row-membership state
- pull patch construction
- mutation ordering, replay idempotence, and acknowledgement rules
- protocol validation and deterministic error responses

This boundary keeps the correctness-sensitive engine identical on native and
Cloudflare, solves mutators, auth, and atomicity in one move, and avoids any
interactive statement channel between processes. Both original plans
considered a remote mutator channel and both resolutions rejected it: a
Durable Object transaction cannot span an HTTP round trip to an application
worker without deadlock, re-entrancy, or a transaction split at exactly the
boundary where ordering matters most. The transaction and the mutator must be
colocated.

### Why Rust, honestly

Rust will not turn a ~180 ms network and Durable Object acknowledgement into
a sub-millisecond operation. On Cloudflare, storage, scheduling, region
placement, and wasm/JavaScript crossings dominate. The real wins are:

- a small deterministic core with strong types around protocol and storage
  invariants
- native performance and memory behavior (target sub-ms engine time locally,
  KB-scale idle namespaces, one static binary)
- one codebase for both hosts instead of the TypeScript core plus the
  7.7k-line pg-facade
- easier model and property testing
- a foundation that could later absorb an IVM engine if ever needed

## Goals

- Replace the current Zero server path in Soot and Chat with one SQLite-only
  implementation.
- Make native and Cloudflare execution first-class from the beginning.
- Preserve the behavior already proved by the TypeScript reference core.
- Prevent unauthorized rows from ever entering a client replica.
- Support the desired-query lifecycle, ordering, limits, relationships, and
  permission transformations Chat needs.
- Keep application mutators in TypeScript.
- Make restart, eviction, replay, retention, and partial-failure behavior
  deterministic and testable.
- Use real Zero clients as the compatibility proof, not Rust unit tests
  alone.
- Remove the old sync path after each consumer completes its cutover.

## Exclusions for the first production release

- PostgreSQL support
- a general database abstraction for future engines
- a Rust application framework for consumer mutators
- a full websocket, CVR, or IVM port (the notification-only wake channel is
  in scope; what is excluded is Zero's poke protocol and per-query view
  maintenance)
- a full v51 wire-type crate (implement only the message subset the HTTP
  transport consumes or emits, plus the query AST; split a protocol crate
  out only when a second Rust consumer exists)
- a new HMAC/signed-token auth scheme (consumer auth is the boundary)
- protocol compatibility with Zero versions other than the pinned release
- dual runtime selection inside a namespace after cutover

## Sources of truth

- [Rust server brief](./rust-sync-server-brief.md)
- [TypeScript reference core](../src/sync-server/sync-server.ts) and its
  [19-test delta suite](../src/sync-server/sync-server.test.ts): the
  executable specification for cursor and mutation semantics
- [Conformance harness](../harness/README.md): the product-level
  specification, because it observes real Zero 1.7 clients differentially
  against stock zero-cache
- [Vendored HTTP transport](../harness/src/vendor/httpPullTransport.ts):
  the wire truth for the baseline dialect
- [Rewrite background](./zero-server-rewrite.md),
  [harness plan](./zero-conformance-harness.md),
  [prior review](./review-zero-sync-server-2026-07-09.md)
- `~/soot/src/zero/httpPullProject.server.ts` and its tests: the production
  composition layer
- `~/chat/src/data/where/channel.ts`, `message.ts`, `data.ts`: Chat's
  permission predicates (verified to include cross-table EXISTS checks over
  `serverMembers` and `channelUserRoles`, which is what makes query-aware
  sync a security requirement for Chat)
- Chat's end-to-end test suite

## Architecture

```text
Zero 1.7 client
    |
    | enhanced HTTP pull and push transport
    v
consumer worker
    |
    | authenticate, resolve session, route namespace
    v
TypeScript Durable Object host
    |
    +-- transactionSync for pull
    |     +-- Rust wasm sync engine
    |     +-- ctx.storage.sql
    |
    +-- async storage transaction for push
    |     +-- Rust ordering preflight (ownership, replay, LMID)
    |     +-- bundled consumer TypeScript mutator
    |     +-- DO-local SQLite transaction adapter
    |     +-- Rust ordering finalization (LMID, change markers)
    |     +-- commit
    |     +-- post-commit external effects, including wake fan-out
    |
    +-- wake channel: hibernating WebSocket per client, "pull now" only

Native harness target
    +-- axum
    +-- the same Rust sync engine
    +-- rusqlite, WAL, one writer per namespace
    +-- wake channel over a plain WebSocket
```

### Repository layout

```text
crates/
  sync-core/       wire subset, validation, cursor engine, query engine later
  sync-native/     axum, rusqlite, namespace files, harness admin routes
  sync-wasm/       narrow wasm-bindgen boundary and JavaScript SQL adapter

packages/
  sync-cf-host/    TypeScript Durable Object host and consumer integration API
```

Create additional crates only when a second real consumer requires the
split. The engine uses functions and small modules, no broad adapter layer.

`sync-core` depends on a narrow synchronous database interface:

- execute SQL with positional `?` bindings only (Durable Object SqlStorage
  has no `?N`)
- return typed rows with deterministic value conversion
- no host network I/O

The host owns transaction entry and exit. The engine must not emit
transaction statements because Durable Object SQL rejects them.

### Value fidelity rules pinned by the reference core

- Patch values always come from live application rows read inside the pull
  transaction, never from logged images. This preserves float fidelity and
  collapses repeated changes safely.
- serde_json (ryu) provides shortest-round-trip float formatting matching
  JavaScript.
- SQLite-to-Zero conversion follows `toZeroValue`: booleans 0/1, JSON text
  parsed, numbers exact. Integers may exceed 2^53; serde_json::Number
  preserves i64, and the JavaScript client ceiling is documented and tested
  at the boundary. Cookies and watermarks use a representation that cannot
  silently round (M0 owns this decision).

### Durable SQLite schema

Dedicated `_zsync_*` tables. Never consume or prune `_orez._zero_changes`;
that log has other readers and retention rules.

First vertical slice:

- `_zsync_meta`: storage version, protocol version, transformation version,
  namespace epoch
- `_zsync_watermark`: durable high-water mark and retained-log floor
- `_zsync_changes`: watermark, table, operation class, touched primary key
- `_zsync_clients`: client group ownership (one durable authenticated-user
  claim), client identity, last mutation IDs

Added with the query-aware layer (designed early, created when the feature
lands):

- queries: canonical transformed AST, dependency tables, query hash
- desires: which client wants which query, at what client query version
- query-row membership: ordered, with position data needed for limits
- per-client-group row reference counts, so overlapping queries produce one
  row patch and delete only when the last reference disappears

Application-table triggers append touched primary keys to `_zsync_changes`.
Internal schema migrations are explicit and forward-only; a version mismatch
fails startup or returns a reset per a tested rule. Silent recreation is
prohibited because it can lose mutation ordering state.

## Protocol

### Baseline endpoints

`POST /pull` and `POST /push`, byte-compatible with the vendored transport:
cookie, patch, last-mutation-ID, unchanged/409/claim semantics all preserved
so the transport remains a differential oracle and existing lanes keep
gating.

### Query-aware extension (additive, versioned)

The on-zero client transport already receives `desiredQueriesPatch` and
today acknowledges it locally. The extension forwards that patch with pull
state instead:

- put a desired query by stable query identifier
- delete a desired query
- include the client's query-state version
- the server acknowledges the query-state version only after its row effects
  are durable

The consumer host resolves the query name and arguments, applies
authentication and permission transformations, and passes only the
transformed Zero v51 AST to Rust. Rust validates the AST even though it came
from trusted consumer code. The response returns row patches, mutation
acknowledgements, query acknowledgements, and a cookie for the last included
durable effect.

### Wake channel (realtime propagation)

Interval polling caps cross-client propagation at the poll interval, which
is unusable for Chat and for realtime collaboration on Soot's project
plane. The wake channel fixes latency without touching correctness:

- each connected client holds a socket to its namespace host: a hibernating
  WebSocket on the Durable Object (the hibernation API keeps idle
  namespaces evictable instead of pinned warm), a plain WebSocket on the
  native host
- the channel carries no data and no protocol state; a wake message means
  only "pull now"
- after a push commits, the host wakes the namespace's other connected
  clients as a post-commit effect; wakes coalesce per namespace, and a
  client already mid-pull needs no second wake
- the channel is advisory and carries zero correctness weight: a lost,
  delayed, or duplicated wake can never cause missed or wrong data, because
  convergence comes entirely from the pull protocol; clients keep a
  low-frequency safety poll and pull on reconnect
- no correctness lane may depend on the safety-poll interval to converge

Client-side work this implies, named because it is a real cross-repo item
like the query extension: the on-zero transport in `~/takeout` subscribes to
the wake channel and triggers a pull on wake, demoting the interval to a
safety net; the extended transport is re-vendored into the harness; and a
propagation lane measures cross-client latency differentially against stock
zero-cache's websocket push.

### Errors

One tested response per class:

- malformed or unsupported request: 400
- missing authentication: 401
- authenticated user forbidden from claiming a client group: 403
- cookie below the retained floor or incompatible epoch: 409 reset
- application mutation error: rolled back, LMID advanced in a second
  transaction, returned as an application error
- internal invariant failure or unknown change-log table: fail loudly,
  retain the previous cookie

## Query execution

Chat's read permissions are cross-table (private channels gated on
`serverMembers` and `channelUserRoles` inside one server namespace), so
uniform per-namespace visibility does not hold and the predicates are not
row-local. A broad namespace snapshot with client-side filtering would place
forbidden rows in the local database and is unacceptable. Heuristic
per-table windows do not answer permissions at all and can make a valid
query silently incomplete; they are rejected as a long-term design.

### Supported AST

Pin the compiler to the Zero 1.7.0 v51 query AST. Implement exactly the
subset Soot and Chat exercise:

- equality and comparison conditions
- conjunction, disjunction, negation
- correlated EXISTS subqueries used by permission transformations
- related rows
- ordering with a stable primary-key tie-breaker
- limits
- start cursors
- parameter binding with no SQL string interpolation

Every accepted shape gets a unit test and a real-client harness case. Every
unknown field, table, relationship, operator, or schema reference is a
deterministic rejection.

### First algorithm: recomputation and membership diff (CVR-lite)

Persist each transformed query's dependency tables. During a pull:

1. Read the included change-log prefix; collect touched tables and keys.
2. Select active queries whose dependencies intersect the touched tables or
   whose desired state changed.
3. Re-run those queries against live rows inside the transaction.
4. Diff the ordered result against durable query-row membership.
5. Update membership and row reference counts.
6. Emit row puts for newly referenced or changed rows.
7. Emit row deletes only when a reference count reaches zero.
8. Advance query acknowledgements and the cookie only after all state and
   effects are durable.

This is intentionally simple. Because it evaluates real limit and cursor
queries it also subsumes any window policy as the bounding answer. Add
incremental maintenance only after profiling shows recomputation misses a
measured production budget. Related queries need explicit dependency
tracking so a parent membership change can add or remove child rows even
when the child table was untouched.

### Transformation changes

Persist a consumer-supplied transformation version. A permission or schema
transformation change invalidates affected durable queries and recomputes
them. It must never keep an older, more permissive result set.

## Mutation execution

Consumer mutators stay in TypeScript, bundled into the Durable Object host,
running against a DO-local SQLite adapter. Existing mutators are
asynchronous even when their SQL is local, so push uses
`ctx.storage.transaction(async () => ...)`. Every SQL cursor is fully
consumed before the next `await`. External network calls, queues, and side
effects are collected and run only after a successful commit.

Normal mutation lifecycle:

1. authenticate in the consumer worker
2. route to the namespace Durable Object
3. enter the async storage transaction
4. Rust validates client-group ownership, replay, ordering, and input
5. run the registered TypeScript mutator against the DO-local adapter
6. Rust records LMID and change markers after mutation effects
7. commit
8. return acknowledgement
9. run deferred effects

Application error:

1. the effects transaction throws and rolls back
2. classify the error outside the rolled-back transaction
3. in a second transaction, Rust advances the LMID for the rejected mutation
4. return the application error result

Infrastructure or invariant error: LMID unchanged, server error returned, so
the operation retries safely. Replay of an acknowledged mutation is
idempotent.

Native hosts run built-in fixture mutators behind the same mutator trait so
harness lanes need no sidecar. Consumer end-to-end tests run on local
workerd using the production TypeScript host bundle and wasm engine, which
makes local runs production-equivalent. A standalone native
custom-mutator ABI is deferred until a real deployment outside Cloudflare
needs one.

## Correctness invariants

The implementation is incomplete until tests prove all seventeen:

1. The change log stores touched primary keys; patches read live rows.
2. A cookie may under-report durable work and may never over-report it.
3. An acknowledgement is never visible before its effects commit.
4. An LMID-only change still advances the cookie.
5. A capped response represents a complete change-log prefix; retention
   cannot delete beyond the last included prefix.
6. Caps apply at change-row boundaries before primary-key deduplication;
   the cookie denotes the last included watermark.
7. Watermarks stay monotonic through restart, eviction, and pruning
   (durable watermark = max(state, MAX(log))).
8. Application errors roll back effects and advance LMID in a second
   transaction.
9. Visibility and membership are evaluated against live state per row and
   query, never inferred from a table-wide decision.
10. Unknown change-log tables fail loudly (explicit skip classifier, throw
    on unmapped tables).
11. A client group has one durable authenticated-user owner; another user
    cannot reuse it.
12. Mutation replay is idempotent.
13. Query acknowledgements never lead durable membership and row effects.
14. Overlapping queries retain a row until its last reference disappears.
15. Permission changes cannot leave forbidden rows in the client store,
    including during reconnect, retention resets, and desired-query
    replacement.
16. Ordering has a deterministic primary-key tie-breaker.
17. External side effects run only after mutation commit.

## Milestones

Each milestone ends in a runnable, reviewable result. Lanes run on both
hosts once both exist. Ported tests run first against an incomplete engine
to observe the expected failure. After M3 the plan forks into three parallel
tracks (M4a, M4b, M4c); no consumer cutover proceeds while an earlier
correctness gate is red.

### M0: platform contract proof

The hardest platform boundary is proved before any porting.

Deliver:

- Cargo workspace; minimal core compiled natively and to wasm
- TypeScript test Durable Object host; local workerd test project
- native `rusqlite` transaction probe
- synchronous pull transaction probe (`transactionSync`)
- asynchronous push transaction probe with an `await` between SQL operations
- one representative read-then-write mutator, one multi-table mutator, one
  application-error mutator with deferred side effects (real mutator shapes,
  not toy inserts)
- JavaScript, wasm, and SQLite value round trips including integer, real,
  text, blob, null, JSON, boolean, and the i64/2^53 boundary
- Rust panic and JavaScript exception rollback tests
- Durable Object eviction and re-instantiation probe
- initial bundle-size, cold-start, CPU, and memory measurements

Exit gate:

- all transaction probes pass locally on workerd and on a deployed test
  Durable Object
- rollback after an `await` removes every SQL effect
- no external effect runs before commit
- Rust and JavaScript errors cannot advance LMID accidentally
- numeric and cookie boundaries have an explicit, tested wire representation
- the bundle keeps recorded headroom for the future query compiler

Stop rule: if atomicity cannot be preserved across the wasm boundary,
redesign the host boundary before porting anything.

### M1: port the executable cursor specification

Deliver:

- Rust implementations of pull, push ordering, watermark, retention floor
  and prune, epoch invalidation, and reset
- dedicated `_zsync_changes` triggers and metadata tables
- all 19 named reference delta tests ported, every table-driven case
  retained
- Soot's composition semantics ported from its 13-test suite: caps with
  last-included-watermark cookie, prefix LMIDs, skip/throw classifier
- randomized model tests for cookie, LMID, and capped-prefix invariants
- generated operation-trace comparison against the TypeScript core

Exit gate:

- unit, model, and differential tests pass
- randomized traces never produce an acknowledgement or cookie ahead of
  effects
- no ignored tests, unfinished semantics, or catch-all table skipping

### M2: native real-client conformance

Deliver:

- `sync-native` HTTP server: `/<ns>/pull|push` routing, one database per
  namespace, WAL, serialized writes per namespace on a blocking worker
- harness target `rust-local` (spawn hook plus base URL, modeled on the
  existing orez-local-process target)
- deterministic admin routes for the harness (`/admin/sql`, `/admin/health`,
  `/admin/status` with bootID)
- hard restart and file persistence support
- wake channel endpoint (plain WebSocket) with post-commit wake fan-out on
  push, plus the on-zero transport wake subscription in `~/takeout`
  re-vendored into the harness
- CI: cargo build/test plus rust-local smoke/shapes/sweep jobs added to
  `.github/workflows/ci.yml` (a Rust toolchain job already exists there);
  nightly on mini-16 gains rust targets

Required lanes: smoke, shapes (differential vs stock zero-cache), seeded
sweep, permissions, reconnect and persisted storage, multi-tab client
groups, hard process kill, wake propagation, storm and benchmark.

Exit gate:

- real Zero 1.7 clients converge with the stock target for the supported
  surface, no unexplained target-specific normalizers
- cookies stay monotonic through SIGKILL and restart
- wake-triggered propagation converges without waiting on the safety poll;
  native cross-client propagation p95 stays below 100 ms on the existing
  fixture
- native acknowledgement p50 is at or below 3 ms on the existing fixture

### M3: Cloudflare real-client conformance

Deliver:

- production-shaped `sync-cf-host` TypeScript package and `sync-wasm` bundle
- harness target `rust-cf` (deployed worker on the lslcf account, modeled on
  the orez-cf target including the deterministic idle-teardown eviction
  simulation and bootID)
- wake channel on Durable Object hibernating WebSockets; idle namespaces
  with open sockets stay evictable rather than pinned warm (warm-pinning is
  the same failure class as the July write-cost incident)
- pinned Rust toolchain and checked-in deployment configuration
- recorded cold-start, CPU, storage, and bundle measurements with a
  documented region and account configuration

Run every M2 lane against `rust-cf`, plus explicit transaction-rollback and
eviction scenarios.

Exit gate:

- native and Cloudflare behavior is semantically equivalent
- normal eviction produces zero 409s and monotone cookies
- sockets survive Durable Object hibernation and deliver wakes correctly
  after re-instantiation; cross-client propagation p95 stays under one
  second at storm load
- forced retention-floor loss produces the expected 409 reset
- p50 and p95 stay within 20 percent of the current TypeScript Durable
  Object baseline at equivalent load

### Fork: three parallel tracks after M3

Soot's migration needs nothing from the query-aware layer: its planes run
on exactly the reference-core semantics (uniform project visibility,
row-local predicates, caps, prefix LMIDs) and are proven in production.
Serializing Soot behind the largest component would delay the first
production consumer for no correctness gain. Chat's early branch likewise
needs only the baseline surface and produces the measurements the query
layer needs. So M4a, M4b, and M4c proceed in parallel.

### M4a: Soot production migration (baseline surface)

Deliver:

- Soot auth and namespace adapter; normalized claims passed to Rust
- Soot mutator registry bundled into the Durable Object host with the
  DO-local adapter for Soot's on-zero transaction interface
- offline cursor, snapshot, and acknowledgement comparison against the
  current endpoint on representative data
- Soot control-plane and project-plane integration tests and Cloudflare
  runtime validators pointed at the new target
- wake channel enabled for Soot clients, so the project plane gets realtime
  propagation instead of the 30-second poll
- per-namespace cutover and rollback scripts

Exit gate:

- Soot tests and deployed conformance pass; the conformance suite stays
  green using Soot's deployed composition
- canary namespaces survive restart, eviction, replay, retention, and
  application-error drills within recorded latency and error budgets
- no namespace ever has two writers
- after the observation window, remaining namespaces migrate and Soot's old
  Zero server path is deleted

### M4b: query-aware layer

Cross-repo work items, each named because each is real:

- extend the on-zero `httpPullTransport` in `~/takeout` to ship
  desired-query changes and consume server query acknowledgements (its own
  review; today it keeps `desiredQueriesPatch` client-local and synthesizes
  `gotQueriesPatch`)
- re-vendor the extended transport into the harness
- new harness lanes for the query lifecycle
- Chat's client transport flip from stock websocket to on-zero http-pull
  (lands with M5)

Deliver in the engine:

- server acknowledgement only after query effects are durable
- transformed AST validation and the SQLite compiler for the exact Soot and
  Chat query shapes, deterministic rejection of everything else
- durable query, desire, membership, ordering, and row-reference state
- dependency-driven recomputation
- transformation-version invalidation
- raw client-store inspection in the harness

Required lanes (differential vs stock zero-cache wherever stock supports the
shape): query put/delete/clear, overlapping queries sharing rows, limit
boundary shifts, related rows, parent-table permission changes, permission
expansion and contraction, reconnect before and after query
acknowledgement, lost response after commit, retention reset, forbidden-row
raw-store assertions.

Exit gate:

- both hosts pass the full query matrix
- query acknowledgement never leads its row effects
- removing one overlapping query does not delete a row retained by another
- no forbidden row appears even transiently in the client database
- every Chat query shape used by the end-to-end suite is supported

### M4c: Chat compatibility branch (measurement and early feedback)

A Chat branch pointing control and per-server clients at a local workerd
deployment of the production host bundle, using full authorized snapshots
per namespace. This is correct (every row filtered server-side; any
cross-row permission dependency change forces clear plus authorized
re-snapshot; no heuristic windows) but potentially expensive, which is
exactly what this track measures. It qualifies controlled short-term use
only and is never the broad production architecture.

Deliver:

- Chat schema and query-shape inventory
- Chat mutators inside the host transaction
- server switches using distinct namespace and local-storage identities
- raw client-store tests inspecting stored rows directly
- namespace row count, bytes, snapshot bytes, and pull latency report
  (message-heavy namespaces included), which feeds M4b's recomputation and
  narrowing decisions

Required validation: complete Chat end-to-end suite with zero new skips,
allow and deny cases per permission family, membership addition and
revocation, solo channel and secret data behavior, lost push response and
replay, fresh browser context hydration, server switching without cache
leakage, local workerd and deployed Cloudflare runs.

Exit gate: the branch is suitable for controlled short-term use, every known
scale limit is measured rather than inferred, and the branch is explicitly
not the final architecture.

### M5: Chat production migration

Requires M4b and M4c complete.

Deliver:

- permission transformations (the `where/` predicates) expressed as
  server-side transformed ASTs
- desired-query transport enabled for control and per-server clients
- the client transport flip on a branch
- production namespace migrations and canary routing
- observability for query recomputation, patch size, resets, and LMID
- rollback drill

Exit gate:

- the complete Chat suite passes locally and on Cloudflare with zero new
  skips; historical failures resolved or documented as unrelated with proof
- allow and deny raw-store tests stay green for every permission family
- representative large namespaces meet the recorded budgets
- restart, eviction, replay, and permission-change drills pass
- lost push responses do not duplicate mutations; a fresh context converges
  from an empty database; server switching cannot leak rows across
  namespaces
- old Chat sync paths are deleted after the observation window

### M6: long-term production qualification

Run before declaring the server a general replacement:

- multi-day soaks; message-heavy large namespaces
- many overlapping desired queries; query and tab churn
- repeated Durable Object eviction; process kills at every mutation and
  pull transaction boundary
- retention pressure while clients stay offline
- schema and permission transformation migrations; mixed-version deploy
  rule proved by test (if mixed versions are unsupported, deployment drains
  or resets namespaces explicitly)
- storage quota and failure injection; clock skew where application
  timestamps are used
- malformed and adversarial protocol inputs
- wasm memory growth and leak checks; recovery from partially deployed
  worker versions
- backup, restore, and rollback drills

Exit gate:

- soak and fault suites have explicit budgets, reproducible commands, and
  passing evidence
- on-call documentation can diagnose cookie, retention, mutation-ordering,
  and query-membership failures
- every retired path in soot, chat, and orez is deleted (zero-cache,
  pg-facade on the sync path, compatibility flags, dual routes, the
  temporary Chat branch, and their tests)

## Performance budgets

Record the TypeScript local and Cloudflare baselines before tuning Rust,
with identical data, client counts, polling intervals, regions, and account
settings. Budgets are the gates; speed beyond them is a stretch goal.

Gates:

- native acknowledgement p50 at or below 3 ms on the existing fixture, and
  at least parity with the TypeScript reference median
- Cloudflare p50 and p95 within 20 percent of the current Durable Object
  baseline at equivalent load
- 100-client storm convergence with no resets, missed rows, or cookie
  regressions
- cross-client propagation (wake-triggered): p95 under 100 ms native and
  under one second on Cloudflare at storm load; no lane converges via the
  safety poll
- flat wasm memory across repeated query churn, connection churn, and
  eviction cycles
- at least 40 percent bundle headroom below the applicable Cloudflare limit

Stretch: sub-millisecond native engine time.

Correctness wins over any latency target. Optimize only from profiles, in
this order: batch wasm/JavaScript crossings, cache compiled queries, index
dependency and membership tables, narrow recomputation using touched
primary keys, and add incremental maintenance only for measured-expensive
shapes.

## Observability

Every pull and push produces structured fields, never logging query
arguments, row contents, or tokens:

- namespace hash; host and engine version
- request kind and result class
- input and output cookie; retained floor and current watermark
- change rows scanned and included; queries recomputed
- row puts and deletes; LMID advances
- transaction and total latency; reset reason

Aggregate counters cover resets, application errors, invariant failures,
retention, query recompilation, and wasm boundary calls. Diagnostic
endpoints exist only in authenticated operator or harness deployments.

## Cutover and rollback

One namespace at a time:

1. verify a current backup and schema version
2. stop writes through the old implementation
3. migrate or initialize `_zsync_*` state
4. route the namespace to the new implementation
5. require clients to reset if their old cookie cannot be represented safely
6. verify mutation ordering, query state, and health counters
7. continue with the next namespace

Exactly one sync implementation and one writer own a namespace at any time.
Rollback stops the new writer before restoring the old route, as an explicit
operator action. There is no runtime fallback, automatic dual write, or
silent protocol downgrade. After each observation window, delete the retired
code, flags, routes, configuration, and tests.

No publish or release of any package without explicit approval. This
includes the on-zero transport change in `~/takeout`; local iteration uses
the dist-copy or `--into` flows.

## Remaining risks, to prove rather than debate

1. **Existing mutator compatibility.** Adapting the asynchronous on-zero
   transaction interface to DO-local synchronous SQL inside an async storage
   transaction is the main implementation risk. M0 runs representative real
   mutators.
2. **Wasm boundary overhead.** Repeated Rust-to-JavaScript SQL calls could
   erase the Cloudflare win. Measure statement count and boundary time; batch
   only where the profile shows value.
3. **Query recomputation cost.** Re-running affected queries may read too
   many rows for large Chat queries. The M4c dataset report and M4b counters
   decide where to add narrowing.
4. **JavaScript integer precision.** Cookies and watermarks need a tested
   representation that cannot silently round. M0 owns this.
5. **Native consumer hosting.** The native binary serves the engine and
   fixture mutators only; it does not run arbitrary consumer TypeScript.
   That scope boundary is explicit, and a native application ABI is designed
   only when a real deployment needs one.
6. **Wake fan-out cost.** A commit in a hot namespace wakes every connected
   client and each wake triggers a pull. Per-namespace coalescing and
   mid-pull suppression bound this, but the storm lane must measure wake
   fan-out CPU, and the Cloudflare lanes must confirm hibernation keeps
   idle namespaces cheap with sockets open.

## Definition of done

- one Rust core serves the native and Cloudflare harness targets
- the Cloudflare host runs existing TypeScript mutators inside the
  namespace Durable Object transaction
- the reference delta suite and every required harness lane pass on both
  hosts
- Soot and Chat use the new server for all production namespaces
- Chat desired queries and permission transformations are server-aware
- cross-client propagation is wake-driven on both hosts and meets the
  recorded realtime budgets
- raw client stores contain no forbidden rows
- fault, soak, migration, backup, restore, and rollback drills pass their
  documented thresholds
- deployment and incident documentation is checked in
- the old zero-cache path, PostgreSQL compatibility, dual routes, the
  temporary Chat branch, and dead tests are removed

## External references

- [Cloudflare Durable Object SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Cloudflare Durable Object state and concurrency](https://developers.cloudflare.com/durable-objects/api/state/)
- [Cloudflare Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Cloudflare Rust language support](https://developers.cloudflare.com/workers/languages/rust/)
- [Cloudflare WebSocket hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/)
- [Zero self-hosting documentation](https://zero.rocicorp.dev/docs/self-host)

## Appendix: alternatives considered in the source plans and not adopted

The four source plans were removed after this merge; git history has the
full documents. This appendix preserves the designs they explored that this
plan rejects or defers, with the reasoning, so nobody re-derives them from
scratch.

### Interactive mutator statement channel (plan A)

Plan A ran mutators in the application worker, which executed each mutator
by streaming `{sql, params}` statements back to the sync server over a
transaction-scoped channel. This kept the app-worker topology identical to
today's pg-facade shape and supported read-then-write mutators. Rejected
because a Durable Object transaction cannot span the round trip:
`transactionSync` is synchronous and cannot await the app worker, a callback
into the blocked Durable Object is re-entrant and can deadlock, splitting
statements across requests loses atomic rollback, and a network failure
inside an open transaction demands a new recovery protocol. It also would
have left the harness testing built-in mutators while consumers used a
different remote path. If a non-Cloudflare deployment ever needs remote
mutators, this design is the starting point for that separate project.

### Direct workers-rs Durable Object host (plan A)

Plan A made Rust the Durable Object itself via workers-rs 0.8.x, including
writing our own `transactionSync` extern binding (workers-rs exposes
synchronous `SqlStorage.exec` but does not bind `transactionSync`; verified
in `~/github/workers-rs/worker/src/sql.rs`). Rejected together with the
statement channel: the thin TypeScript host solves mutator colocation, auth
handoff, and transaction ownership in one move and drops the custom
binding. The workers-rs verification remains useful background if the host
boundary is ever revisited.

### Full v51 wire crate and a Rust load generator (plan A)

Plan A transcribed the entire Zero v51 message set into a standalone
`zero-wire` crate (estimated ~2 days of mechanical serde work), partly to
keep the websocket door open and partly to enable a future Rust load
generator in the zbench mold. Deferred: only the message subset the HTTP
transport consumes plus the query AST is implemented, inside `sync-core`,
and a protocol crate splits out when a second Rust consumer exists. The
load-generator idea is worth revisiting if storm lanes ever need more
client scale than real Zero clients can provide.

### HMAC signed-claims token auth (plan A)

Plan A had the application mint short-lived HMAC tokens (`{userID, exp}`,
pure-Rust crypto so it works on wasm) that the sync server verifies
directly, keeping pulls zero-round-trip for a standalone deployment.
Rejected for now: consumers already resolve better-auth sessions in their
application worker, and the host passes normalized claims to Rust. The
token design becomes relevant only if a standalone native deployment
cannot share a consumer's authentication boundary.

### Per-table windows with epoch aging for Chat (plan A)

Plan A's default Chat bounding policy was heuristic per-table windows (age
or row count, row-local predicates only) aged out via epoch snapshots, with
server-evaluated queries added only if windows measurably failed. Rejected:
windows do not answer permissions at all (Chat's predicates are
cross-table), and a fixed window can make a valid Zero query silently
incomplete. The query-aware layer subsumes windowing because real limit and
cursor queries are the bounding mechanism.

### Websocket/CVR/IVM port as its own future project (both plans)

Both plans kept the full websocket surface out of the product. Plan A
additionally sketched it as a potential standalone follow-up (a Rust
ZQL/IVM engine; zero-cache's is ~18k LOC ZQL plus 3.2k LOC zqlite). That
remains the position: reconsider only if measurements defeat the
recomputation design, and then as its own planned project rather than scope
growth here. The wake channel already covers the realtime-latency reason
one might otherwise want the websocket protocol.

### Five-crate layout (plan A)

`zero-wire`, `zsync-core`, `zsync-host`, `zsyncd`, `zsync-cf`. Collapsed to
three crates plus the TypeScript host package; the host-agnostic request
layer folds into `sync-core` until a second consumer of it exists.

### Drizzle-on-SQLite end state for consumer mutators (plan A)

Plan A noted that apps keep drizzle, with orez's existing pg-to-sqlite
translation running app-side as an interim shim and drizzle-sqlite as the
end state. This plan's DO-local adapter for the on-zero transaction
interface covers the mechanism, but the per-consumer dialect migration
(moving Soot and Chat mutators from translated Postgres SQL to native
SQLite drizzle) is consumer-repo work tracked in their own plans.

### Chat compatibility branch scale ceiling (resolved-plan debate)

Plan A's author argued per-user authorized snapshots on every permission
change are unscalable for message-heavy namespaces and left the branch out;
plan B's author kept it for early integration feedback. This plan keeps the
branch (M4c) but only because it is explicitly measurement-scoped and
short-term; if M4c's dataset report shows snapshot costs are unacceptable
even for controlled use, the branch shrinks to a measurement harness and
Chat waits for M4b, which is the outcome plan A's author predicted.
