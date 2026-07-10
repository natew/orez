# Rust SQLite Zero implementation plan

Status: superseded by [rust-sync-server-final-plan.md](./rust-sync-server-final-plan.md)

Date: 2026-07-09

Protocol baseline: Zero 1.7.0, protocol v51

Primary consumers: `~/soot` and `~/chat`

## Decision

Build a SQLite-only sync engine in Rust with two hosts from the first milestone:

1. A native host using `rusqlite` for local development, servers, tests, and benchmarks.
2. A WebAssembly build embedded in a thin TypeScript Durable Object host for Cloudflare.

Use an enhanced HTTP pull protocol as the production boundary. The server must
receive desired-query changes and maintain durable query-to-row state before it
is allowed to serve Chat. Full compatibility with Zero's websocket and CVR
protocol is outside the first product scope. It can be reconsidered only if
real consumer measurements show a feature that the HTTP protocol cannot
provide.

The TypeScript Durable Object host owns the JavaScript platform boundary:

- `ctx.storage.transactionSync`
- request routing and authentication handoff
- calls into existing TypeScript mutators
- conversion between JavaScript values and batched Rust inputs and outputs

Rust owns the sync system:

- cookies, watermarks, retention, and reset decisions
- change-log interpretation
- Zero query AST validation and SQLite compilation
- durable desired-query and row-membership state
- pull patch construction
- mutation ordering and acknowledgement rules
- protocol validation and deterministic error responses

This boundary keeps the correctness-sensitive engine identical on native and
Cloudflare while avoiding a new interactive SQL protocol between Rust and the
application's existing TypeScript mutators.

## Goals

- Replace the current Zero server path in Soot and Chat with one SQLite-only
  implementation.
- Make native and Cloudflare execution first-class from the beginning.
- Preserve the behavior already proved by the TypeScript reference core.
- Prevent unauthorized rows from entering a client replica.
- Support Zero's desired-query lifecycle, ordering, limits, relationships, and
  permission transformations needed by Chat.
- Keep existing application mutators in TypeScript.
- Make restart, eviction, replay, retention, and partial-failure behavior
  deterministic and testable.
- Use real Zero clients as the compatibility test, rather than treating Rust
  unit tests as sufficient proof.
- Remove the old sync path after each consumer completes its cutover.

## Exclusions for the first production release

- PostgreSQL support
- a general database abstraction intended for future engines
- a Rust application framework for writing consumer mutators
- a full port of Zero's websocket, CVR, and incremental-view-maintenance server
- protocol compatibility with versions other than the pinned Zero release
- dual runtime selection inside a namespace after cutover

## Sources of truth

Implementation work must begin from these checked-in artifacts:

- [Rust server brief](./rust-sync-server-brief.md)
- [TypeScript reference core](../src/sync-server/sync-server.ts)
- [Reference delta suite](../src/sync-server/sync-server.test.ts)
- [Conformance harness](../harness/README.md)
- [Rewrite background](./zero-server-rewrite.md)
- [Harness plan](./zero-conformance-harness.md)
- [Prior review](./review-zero-sync-server-2026-07-09.md)
- `~/soot/src/zero/httpPullProject.server.ts`
- `~/soot/src/zero/httpPullProject.test.ts`
- `~/chat/src/data/where/channel.ts`
- `~/chat/src/data/where/message.ts`
- `~/chat/src/data/where/data.ts`
- Chat's end-to-end test suite

The TypeScript core and its tests are the executable specification for cursor
and mutation semantics. The harness is the product-level specification because
it observes real Zero 1.7 clients.

## System shape

```text
Zero 1.7 client
    |
    | pull, push, desired-query changes
    v
consumer data worker or native HTTP host
    |
    +-- authenticate and transform named queries using consumer code
    |
    +-- execute TypeScript mutators inside the host transaction
    |
    v
Rust sync engine
    |
    +-- validate and compile Zero v51 query AST
    +-- maintain clients, desires, queries, rows, and cookies
    +-- construct patches from live SQLite rows
    |
    v
one SQLite database per namespace
    +-- application tables
    +-- _zsync_changes
    +-- _zsync_watermark and retention metadata
    +-- _zsync_clients and last mutation IDs
    +-- durable query and row-membership state
```

For Cloudflare, the consumer data worker and Durable Object remain the
deployment unit. Rust is compiled to WebAssembly and called by the Durable
Object. For native use, the same engine is called by a small Rust HTTP binary.

## Repository layout

Start with a small Cargo workspace under `crates/`:

```text
crates/
  sync-core/       protocol-neutral engine and SQLite operations
  sync-native/     native HTTP process, rusqlite adapter, admin test routes
  sync-wasm/       wasm-bindgen boundary for the TypeScript Durable Object host
```

Create another crate only after a second real consumer requires the split. In
particular, keep Zero v51 wire types in `sync-core` until their size or reuse
justifies a separate protocol crate.

The engine should use functions and small modules. Avoid a broad adapter layer.
The only host boundary needed initially is a narrow synchronous SQLite
interface that can execute statements and return typed rows.

## Deployment boundary

### Native

The native binary provides:

- one process with explicit namespace routing
- one SQLite file per namespace
- WAL mode and a single writer per namespace
- `/pull` and `/push`
- harness-only administration routes for SQL setup, health, and boot identity
- structured logs and basic latency counters
- graceful shutdown and deterministic restart behavior

`rusqlite` should run the request transaction on a blocking worker. Each
namespace gets serialized write access. Read concurrency may be added only
after the single-writer behavior and benchmark results are understood.

### Cloudflare

The Cloudflare deployment provides:

- a TypeScript worker entrypoint
- one Durable Object instance per namespace
- Durable Object SQLite as the only durable database
- a pinned `workers-rs` and Rust toolchain
- a bundled Rust WebAssembly module
- deterministic namespace routing
- the same harness administration surface in a dedicated test worker

The TypeScript Durable Object wraps each engine operation in
`transactionSync`. Rust receives a synchronous SQL callback or a compact batch
interface. Milestone 0 must prove that statement errors unwind through
WebAssembly and roll back the whole transaction.

The TypeScript host stays thin. Query semantics, patch construction, retention,
and ordering cannot be reimplemented there.

## Durable SQLite schema

Use dedicated `_zsync_*` tables. Do not consume or prune `_orez._zero_changes`
because that log has other readers and retention rules.

The initial internal schema should cover these concepts:

- metadata: schema version, protocol version, transformation version, and
  namespace epoch
- watermark: durable high-water mark and retained-log floor
- changes: monotonically ordered change rows containing table name, primary
  key, operation, and watermark
- clients: client group, client identity, claimed user, and last mutation ID
- queries: canonical transformed AST, dependency tables, and query hash
- desires: which client wants which query and at what client query version
- query rows: durable membership of rows in query results, including order
  position data needed for limits
- rows: per-client-group row reference counts so shared query results produce
  one row patch and delete only when the last reference disappears

Exact table names can be shortened during implementation. The concepts and
durability requirements cannot be removed.

Application-table triggers append touched primary keys to `_zsync_changes`.
Patch values always come from live application rows at pull time. The log must
never be treated as an authoritative row image.

Internal schema migrations are explicit and forward-only. A stored schema
version mismatch fails startup or returns a reset response according to a
tested migration rule. Silent recreation is prohibited because it can lose
mutation ordering state.

## Protocol

### Baseline endpoints

Keep the existing HTTP pull shape as the baseline:

- `POST /pull`
- `POST /push`

Preserve current cookie, patch, last mutation ID, and error behavior so the
vendored transport remains useful as a differential oracle.

### Query-aware extension

Extend the transport and request with desired-query changes:

- put a desired query by stable query identifier
- delete a desired query
- include the client's query-state version
- acknowledge the query-state version only after its row effects are durable

The application host resolves a desired query name and arguments, applies
authentication and permission transformations, and passes only the transformed
Zero v51 AST to Rust. Rust validates the AST even though it came from trusted
consumer code.

The response returns row patches, mutation acknowledgements, query
acknowledgements, and a cookie representing the last included durable effect.
The client cannot advance past effects omitted because of byte or row limits.

### Errors

Define and test one response for each class:

- malformed or unsupported request: 400
- missing authentication: 401
- authenticated user forbidden from claiming a client group: 403
- cookie below the retained floor or an incompatible epoch: 409 reset
- application mutation error: mutation is acknowledged according to the
  two-transaction rule and returned as an application error
- internal invariant or unknown change-log table: fail loudly and retain the
  previous cookie

## Query execution

Chat makes query-aware synchronization a security requirement. Its permission
rules cross channel membership, messages, data records, and related rows. A
broad namespace snapshot with client-side filtering would place forbidden rows
in the local database and is unacceptable.

### Supported AST

Pin the compiler to the Zero 1.7.0 v51 query AST. Implement the subset exercised
by Soot and Chat, then make unsupported nodes deterministic errors. The initial
compiler must support:

- equality and comparison conditions
- conjunction, disjunction, and negation
- correlated subqueries used by permission transformations
- related rows
- ordering with a stable primary-key tie-breaker
- limits
- starts and cursors
- parameter binding without SQL string interpolation

Every accepted AST shape needs a unit test and a real-client harness case.
Unknown fields, tables, relationships, operators, or schema references are
rejected.

### Correct first algorithm

For each transformed query, persist the set of tables it depends on. During a
pull:

1. Read the included change-log prefix and collect touched tables and primary
   keys.
2. Select active queries whose dependencies intersect the touched tables or
   whose desired state changed.
3. Re-run those queries against the live database inside the transaction.
4. Diff the ordered result against durable query-row membership.
5. Update membership and row reference counts.
6. Emit row puts for newly referenced or changed rows.
7. Emit row deletes only when a row's reference count reaches zero.
8. Advance query acknowledgements and the cookie only after all state and
   effects are durable.

This recomputation approach is intentionally simple. Add incremental query
maintenance only after profiling demonstrates that recomputation misses a
measured production budget.

Related queries need explicit dependency tracking so a membership change in a
parent can add or remove child rows even when the child table was untouched.

### Transformation changes

Persist a consumer-supplied transformation version. A permission or schema
transformation change invalidates affected durable queries and recomputes them.
It must never keep an older, more permissive result set.

## Required correctness invariants

The implementation is incomplete until tests prove every invariant below.

1. Change rows store touched primary keys, while patches use live row reads.
   This preserves float fidelity and collapses repeated changes safely.
2. Cookies may under-report durable work. They may never over-report it.
3. An acknowledgement is never visible before its database effects are
   committed.
4. A last-mutation-ID-only advance still moves the cookie so the acknowledgement
   cannot be lost behind an unchanged response.
5. Capped responses include a complete prefix of change rows. Retention cannot
   delete beyond the last included prefix.
6. The cap is applied at change-row boundaries before primary-key
   deduplication. The cookie denotes the last included watermark.
7. The durable watermark is monotonic across process restart, Durable Object
   eviction, and log pruning.
8. An application mutation error rolls back its effects, then advances its
   last mutation ID in a separate transaction.
9. Visibility and query membership are evaluated for each live row and query,
   rather than inferred from a table-wide decision.
10. An unknown or unmapped table in the change log fails loudly.
11. A client group has one durable authenticated-user claim. Another user
    cannot reuse it.
12. Replaying an already acknowledged mutation is idempotent and cannot repeat
    its effects.
13. Query acknowledgements are never ahead of durable query membership and row
    patches.
14. A row delete is emitted only after the last active desired query stops
    referencing the row.
15. Forbidden rows never enter the client store, including during reconnect,
    permission changes, retention resets, and desired-query replacement.
16. Stable ordering includes a deterministic primary-key tie-breaker.

## Mutation execution

Keep consumer mutators in TypeScript.

On Cloudflare, `POST /push` reaches the consumer data worker and the namespace
Durable Object. The Durable Object opens `transactionSync`, asks Rust to
validate ordering, calls the existing TypeScript mutator within that same
transaction, then asks Rust to finalize ordering metadata. Any thrown error
rolls back that transaction. The application-error acknowledgement happens in
a second transaction.

On native hosts, expose the same lifecycle through a small host callback. The
harness can provide fixture mutators in the native binary. A consumer that
needs arbitrary TypeScript mutators can run the native engine in the existing
application process through the WebAssembly boundary, or add a colocated host
only after a concrete deployment requires it.

Do not introduce an interactive SQL-over-HTTP mutation protocol in the first
release. It creates a distributed transaction that SQLite and HTTP cannot make
atomic.

## Milestones and gates

Each milestone ends in a runnable, reviewable result. Work does not proceed to
a consumer cutover while an earlier correctness gate is red.

### M0: prove the Rust and Cloudflare contract

Deliver:

- Cargo workspace and minimal engine call
- native `rusqlite` transaction probe
- WebAssembly module loaded by a test Durable Object
- synchronous SQL reads and writes through the host boundary
- `transactionSync` commit and rollback probes
- SQLite integer, real, text, blob, null, JSON, and boolean conversion probes
- statement-error and Rust-panic behavior
- bundle-size and cold-start measurements
- Durable Object eviction and re-instantiation probe

Exit gate:

- native and Cloudflare produce byte-equivalent probe results
- errors roll back every statement in the transaction
- 64-bit integer limits are documented and tested at the Zero JavaScript
  boundary
- the production bundle remains below a recorded internal budget with enough
  headroom for the query compiler

If this gate exposes a WebAssembly boundary that cannot preserve atomic
transactions, stop the Cloudflare implementation and revise the host boundary
before porting the engine.

### M1: port the reference core

Deliver:

- Rust implementations of pull, push ordering, watermark, retention, and reset
- dedicated `_zsync_changes` triggers and metadata tables
- all 19 named reference tests ported, including every table-driven case
- Soot cap, prefix ordering, and unknown-table behavior
- randomized model tests for cookies, last mutation IDs, and capped prefixes

Test method:

- first run each ported test against an incomplete engine and observe the
  expected failure
- compare Rust outputs with the TypeScript core for generated operation traces

Exit gate:

- every reference and Soot composition test passes
- randomized traces never produce an acknowledgement or cookie ahead of
  effects
- no ignored tests, unfinished semantics, or catch-all table skipping remain

### M2: native real-client conformance

Deliver:

- `sync-native` HTTP server
- harness target `rust-local`
- deterministic test administration routes
- native process restart and file persistence support

Required harness lanes:

- smoke
- shapes
- seeded sweep
- reconnect and persisted storage
- eviction or process-kill equivalent
- multi-tab client groups
- storm

Exit gate:

- real Zero 1.7 clients converge with the stock target for the supported
  surface
- cookies remain monotonic through hard process termination and restart
- no unexplained target-specific normalizers exist

### M3: Cloudflare real-client conformance

Deliver:

- `sync-wasm` production build
- thin TypeScript Durable Object host
- harness target `rust-cf`
- test-only deterministic eviction identity
- Cloudflare deployment documentation and checked-in configuration

Run every M2 lane against `rust-cf` plus explicit transaction-rollback and
Durable Object eviction scenarios.

Exit gate:

- native and Cloudflare responses are semantically equivalent
- zero 409 responses occur in the normal eviction lane
- forced retention-floor tests produce the expected 409 reset
- cold start, CPU, storage, and bundle measurements are recorded

### M4: desired queries and durable row membership

Deliver:

- desired-query transport extension
- transformed AST validation and SQLite compiler
- durable query, desire, query-row, and row-reference state
- dependency-based recomputation
- ordering, limit, relationship, and transformation-version behavior
- raw client-store inspection in the harness

Required harness lanes:

- all query shapes used by Soot and Chat
- add and remove desired query
- overlapping queries referencing the same row
- limit boundary shifts
- parent and related-row changes
- permission expansion and contraction
- transformation-version invalidation
- reconnect before and after query acknowledgement
- lost response after effects commit
- reset below the retention floor
- forbidden-row raw-store assertions

Exit gate:

- both hosts pass the full query matrix
- no forbidden row appears transiently in the client database
- query acknowledgements cannot lead row effects
- removing one overlapping query does not delete a row retained by another

### M5: Soot migration

Soot is the first production consumer because its current HTTP pull composition
already models the required operational behavior.

Deliver:

- Soot query transformation and authentication adapter
- Soot TypeScript mutators inside the Durable Object transaction boundary
- existing control-plane and project-plane tests pointed at the new target
- offline comparison of patches and acknowledgements on representative data
- per-namespace cutover procedure
- metrics and rollback procedure

Exit gate:

- Soot unit, integration, and Cloudflare runtime validators pass
- the conformance suite stays green using Soot's deployed composition
- canary namespaces complete restart, eviction, replay, and retention drills
- production latency and error rates remain within the recorded budget

After the observation window, migrate remaining namespaces and remove Soot's
old Zero server path. Rollback changes namespace routing as an explicit
operator action and never runs both writers for the same namespace.

### M6: Chat migration branch

Create a dedicated Chat branch that uses the new local SQLite target. Keep
Chat's per-server namespace model.

Deliver:

- complete Chat schema and query-shape inventory
- permission transformations for channels, messages, data, and relationships
- all Chat mutators running inside the chosen host transaction
- local native target for fast end-to-end development
- Cloudflare target for production-equivalent tests
- raw client-store security assertions
- server-switching, fresh-context, reconnect, replay, and restart cases

Required gate:

- the complete Chat end-to-end suite passes with zero new skips
- historical failures are resolved or documented as unrelated with proof
- every permission family has at least one allow and one deny raw-store test
- lost push responses do not duplicate mutations
- changing servers cannot leak cached rows across namespaces
- a new browser context converges from an empty database
- local and Cloudflare runs exercise the same Rust engine version

Passing Chat end to end makes the implementation suitable for controlled
short-term use. It does not complete long-term qualification.

### M7: long-term production qualification

Run sustained and fault-oriented validation before declaring the server a
general replacement:

- multi-hour and multi-day soak runs
- large namespaces with message-heavy distributions
- many overlapping desired queries
- query churn and tab churn
- repeated Durable Object eviction
- process kills at every mutation and pull transaction boundary
- retention pressure while clients remain offline
- schema and transformation migrations
- clock skew where application timestamps are used
- storage-full and quota behavior
- malformed and adversarial protocol inputs
- WebAssembly memory growth and leak checks
- recovery from partially deployed worker versions

Promote a protocol or storage version only after mixed-version tests prove the
upgrade rule. If mixed versions are unsupported, deployment must drain or reset
namespaces explicitly.

Exit gate:

- soak and fault suites have recorded budgets and pass thresholds
- on-call documentation can diagnose cookie, retention, mutation-ordering, and
  query-membership failures
- backup, restore, schema upgrade, and rollback drills have succeeded
- old Soot and Chat server code and configuration are deleted

## Validation matrix

| Behavior                        | Rust unit/model | Native harness | Cloudflare harness | Soot     | Chat     |
| ------------------------------- | --------------- | -------------- | ------------------ | -------- | -------- |
| cursor and retention invariants | required        | required       | required           | required | required |
| mutation replay and app errors  | required        | required       | required           | required | required |
| restart and eviction            | required        | required       | required           | required | required |
| desired-query lifecycle         | required        | required       | required           | required | required |
| ordering and limits             | required        | required       | required           | required | required |
| relationship changes            | required        | required       | required           | required | required |
| permission contraction          | required        | required       | required           | required | required |
| forbidden-row raw store         | fixture         | required       | required           | required | required |
| storm and soak                  | model           | required       | required           | canary   | required |
| namespace switching             | n/a             | fixture        | fixture            | if used  | required |

## Performance budgets

Record the existing TypeScript local and Cloudflare baselines before tuning
Rust. Use identical data, client counts, polling intervals, regions, and
Cloudflare account settings.

Initial acceptance criteria:

- Rust native engine time matches or improves the TypeScript reference median
  and does not regress p95 by more than 20 percent.
- Cloudflare acknowledgement and propagation percentiles stay within 20
  percent of the current Durable Object baseline at the same load.
- One hundred-client storm convergence has no resets, missed rows, or cookie
  regressions.
- WebAssembly memory is stable across repeated query churn and eviction cycles.
- The worker bundle keeps at least 40 percent headroom below the applicable
  Cloudflare limit.

Correctness wins over a latency target. Optimize only with profiles from the
native benchmark or Cloudflare counters. Likely optimization order:

1. batch WebAssembly and JavaScript value crossings
2. reduce repeated query compilation
3. index query dependencies and membership tables
4. narrow recomputation using touched primary keys
5. add incremental maintenance for measured expensive query shapes

## Observability and operations

Every pull and push should produce structured fields without logging query
arguments, row contents, tokens, or other sensitive values:

- namespace hash
- host and engine version
- request kind and result class
- input and output cookie
- retained floor and current watermark
- change rows scanned and included
- queries recomputed
- row puts and deletes
- last mutation ID advances
- transaction and total latency
- reset reason

Expose aggregate counters for resets, application errors, invariant failures,
retention, query recompilation, and WebAssembly boundary calls. Add a diagnostic
endpoint only in authenticated operator or harness deployments.

## Cutover and rollback

Cut over one namespace at a time:

1. verify the namespace has a current backup and schema version
2. stop writes through the old implementation
3. migrate or initialize `_zsync_*` state
4. route the namespace to the new implementation
5. require clients to reset if their old cookie cannot be represented safely
6. verify mutation ordering, query state, and health counters
7. continue with the next namespace

Exactly one sync implementation and one writer may own a namespace. A rollback
stops the new writer before restoring the old route. There is no runtime
fallback, automatic dual write, or silent protocol downgrade.

After the observation window, delete old code, flags, routes, configuration,
and tests that describe the retired path. The repository must present one
current architecture.

## Work order

The dependency order is:

```text
Cloudflare transaction proof
    -> cursor and mutation core
    -> native real-client conformance
    -> Cloudflare real-client conformance
    -> query compiler and durable membership
    -> Soot production migration
    -> Chat full end-to-end migration
    -> long-term fault and soak qualification
    -> old-path deletion
```

M0 and the native half of M1 can proceed together after the transaction API is
shaped. Consumer integration begins only after M4 because query-aware security
is part of the server contract.

## Definition of done

The project is complete when:

- one Rust engine serves native and Cloudflare hosts
- Soot and Chat use that engine for all production sync namespaces
- Zero 1.7 real-client harness lanes pass on both hosts
- Chat's complete end-to-end suite passes without new skips
- raw-store tests prove permission isolation
- fault, soak, restart, eviction, replay, retention, migration, and rollback
  suites pass documented thresholds
- the dedicated change log and durable query state have operational tooling
- the old Zero server, compatibility flags, dual routes, and dead tests are
  removed
- deployment and incident documentation is checked in

## External references

- [Cloudflare Rust language support](https://developers.cloudflare.com/workers/languages/rust/)
- [Durable Object SQLite API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Durable Object WebSocket guidance](https://developers.cloudflare.com/durable-objects/best-practices/websockets/)
- [Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [Zero self-hosting documentation](https://zero.rocicorp.dev/docs/self-host)
