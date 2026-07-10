# Rust SQLite Zero plan review and resolution

Status: superseded by [rust-sync-server-final-plan.md](./rust-sync-server-final-plan.md)

Date: 2026-07-09

Reviewed plans:

- [Independent Rust sync-server plan](./rust-sync-server-plan.md)
- [Rust SQLite Zero implementation plan](./rust-sqlite-zero-implementation-plan.md)

Supporting brief:

- [Rust sync-server brief](./rust-sync-server-brief.md)

## Bottom line

The independent plan is the better implementation kickoff. It is shorter,
closer to the executable TypeScript core, clearer about what Rust can improve,
and more disciplined about delaying a full Zero websocket and incremental view
engine.

My plan is the better production qualification plan. It treats Cloudflare as a
day-one architecture constraint, addresses Chat's query and permission shape,
defines durable state and cutover rules, and gives the Chat end-to-end suite the
right role without mistaking it for long-term proof.

Neither plan should be executed unchanged.

The independent plan's proposed interactive statement channel cannot be the
Cloudflare mutation design. A Durable Object cannot hold a transaction, call an
application worker, and then service re-entrant SQL calls from that worker as a
normal atomic flow. The synchronous transaction API also cannot await the
round trip. This creates a deadlock or a transaction split at the exact boundary
where ordering matters most.

My plan also made two mistakes. It put the query-aware engine too early in the
critical path, delaying the fast feedback available from Soot and Chat. It also
described `transactionSync` as the host for existing TypeScript mutators even
though those mutators use asynchronous interfaces.

The resolved design is:

1. Rust owns the deterministic sync engine.
2. A thin TypeScript Durable Object host owns Cloudflare transactions and runs
   consumer mutators locally in the same Durable Object.
3. Pure pull operations use `transactionSync`.
4. Push operations use `ctx.storage.transaction(async () => ...)`, with every
   SQL cursor fully consumed before an `await` and all external effects deferred
   until after commit.
5. The first vertical slice ports the proven cursor core and syncs a bounded,
   authorized table surface.
6. Soot is the first production consumer of that surface.
7. Chat gets an early local and Cloudflare compatibility branch using its
   per-server namespaces and full authorized snapshots.
8. Server-aware desired queries and durable query membership are required
   before broad Chat production use, while the early branch can qualify
   short-term controlled use.
9. Native and Cloudflare harness targets remain first-class throughout.
10. Full Zero websocket and incremental-view protocol compatibility stays out
    of scope unless measurements prove the enhanced HTTP design insufficient.

## What the independent plan does better

### It starts from the executable specification

The independent plan keeps the first implementation centered on the roughly
500-line TypeScript core and its 19 named tests. That is the correct way to
avoid designing a replacement in the abstract.

Its early order is strong:

- port the cursor and mutation rules
- expose a native target
- attach the existing real-client harness
- add the Cloudflare target
- run behavior and load lanes

That order produces useful failures quickly.

### It draws the correct product boundary around HTTP pull

The independent plan is right to reject a full websocket, CVR, ZQL, and
incremental-view port as the first product. That work is much larger than wire
protocol serialization and would reproduce machinery that Soot is already
moving away from.

The enhanced HTTP transport is sufficient to test the core architecture. A
full websocket implementation can become its own project if an actual consumer
requirement survives the HTTP design.

### It is honest about Cloudflare performance

Rust will not turn a 180 ms network and Durable Object acknowledgement into a
sub-millisecond operation. On Cloudflare, storage, scheduling, region placement,
and JavaScript to WebAssembly crossings will dominate many requests.

The strongest reasons for Rust are:

- a small deterministic core
- native performance and memory behavior
- shared semantics across native and Cloudflare
- strong types around protocol and storage invariants
- easier model and property testing
- removing the large zero-cache and PostgreSQL compatibility stack

The independent plan states this more clearly than mine did.

### It identifies useful low-level constraints

The following details should be retained:

- positional `?` bindings for Durable Object SQL
- live row reads after touched-primary-key logging
- shortest-round-trip JSON number formatting
- explicit handling of JavaScript's integer precision boundary
- monotonic durable watermark recovery from state and the log
- byte and row caps at change-row boundaries
- a loud error for unknown change-log tables
- a native `axum` and `rusqlite` target using WAL and serialized writes

### It keeps consumer mutators in TypeScript

Porting Chat and Soot's application mutators to Rust would consume the project
without improving sync correctness. Both plans agree that they stay in
TypeScript. The independent plan is especially clear about the scale of that
work and the need to preserve read-then-write behavior.

### It is concise enough to use as a kickoff

My implementation plan is comprehensive but too long for daily execution. The
independent plan makes its strategic calls visible and names concrete harness
gates. The resolved plan should preserve that clarity.

## What the independent plan gets wrong or leaves too weak

### The interactive mutator statement channel is the wrong boundary

The proposed flow is roughly:

```text
Rust server opens SQLite transaction
    -> Rust calls application worker
    -> application worker calls back with SQL statements
    -> Rust executes them in the open transaction
    -> application worker continues read-then-write logic
```

This is workable only as a custom streaming RPC on a native process. It does
not fit a Cloudflare Durable Object transaction cleanly:

- `transactionSync` must finish synchronously and cannot await the application
  worker.
- an application worker callback into the same blocked Durable Object is
  re-entrant and can deadlock or time out.
- splitting the SQL calls across ordinary requests loses atomic rollback.
- network failure creates a new recovery protocol inside an open transaction.
- the harness would exercise built-in mutators while the consumers exercise a
  different remote channel.

This is the most important flaw in the independent plan.

The transaction and the mutator must be colocated. The generated Cloudflare
worker bundle should include the consumer's mutator registry and a Durable
Object-local SQLite transaction adapter.

### Cloudflare proof comes too late

Robust Cloudflare support from day one means proving the hardest platform
boundary before porting the whole core. The independent plan waits until M2 to
exercise the Rust Durable Object host.

The first milestone must prove:

- Rust WebAssembly can call Durable Object SQL safely
- synchronous pull transactions roll back on JavaScript and Rust errors
- asynchronous push transactions include SQL before and after an `await`
- existing asynchronous mutator shapes can run against a local SQL adapter
- application errors can roll back effects and advance LMID separately
- values survive JavaScript, WebAssembly, and SQLite conversion
- the bundle retains enough size and memory headroom

### Whole-namespace windowing is too casual for Chat

Chat's permission rules include cross-table membership and role checks. A
membership change can revoke access to many channels and messages without
touching those message rows. A row-only cursor diff cannot discover that
revocation.

Whole authorized snapshots can be correct if every row is filtered and the
server clears and re-snapshots whenever a permission dependency changes. They
are a useful compatibility stage because Chat now has per-server namespaces.
Arbitrary windows are different. A fixed window can make a valid Zero query
silently incomplete and can leave relationship results inconsistent.

The combined plan therefore permits full authorized namespace snapshots for
the early Chat branch. It does not make heuristic table windows the long-term
default. Query-aware synchronization lands before broad Chat production use.

### A full v51 wire crate is unnecessary in the first release

The independent plan proposes transcribing the entire Zero v51 message set even
while deferring the websocket product. That adds drift and review surface
without helping the HTTP server.

Implement only the message shapes actually consumed or emitted by the vendored
HTTP transport, plus the desired-query AST needed by the later query-aware
milestone. Split a protocol crate out only when a second Rust consumer exists.

### A new HMAC token format duplicates consumer authentication

Soot and Chat already resolve their sessions in the application worker. The
application host should pass a normalized authenticated user and claims object
to Rust. Rust enforces the durable client-group ownership claim.

A new token issuer and verifier adds key rotation, expiry, clock, and deployment
work. It is useful only if a future standalone native deployment cannot share
the consumer's authentication boundary.

### The plan does not go far enough on durable query state and operations

It covers cursor durability well but leaves these long-term areas unresolved:

- desired-query acknowledgement ordering
- overlapping query row references
- permission transformation versioning
- raw client-store security assertions
- schema migration and mixed-version behavior
- namespace cutover and rollback
- soak, eviction, storage pressure, and partial deployment drills
- old-path deletion

Those areas from my plan should remain.

## What my plan does better

### It treats Cloudflare as an architecture constraint

My plan puts the transaction and value-conversion proof first. That is the
right response to the project's stated deployment goal. Rust native success is
insufficient if the same engine cannot be hosted safely inside a Durable
Object.

### It rejects the distributed mutation transaction

Keeping the TypeScript host inside the Durable Object makes one SQLite database
and one transaction the authority. Rust performs ordering preflight and
finalization around the application mutator. The application cannot receive an
acknowledgement before its effects.

### It gives Chat's permissions the required weight

Chat's end-to-end tests can prove that the application works. Raw client-store
assertions are still needed to prove that forbidden rows never arrived and were
merely hidden by a query or component.

The query-aware stage also solves scale and semantic completeness without
porting Zero's full incremental-view engine. Re-running affected SQLite queries
and diffing durable membership is a much smaller first algorithm.

### It separates short-term confidence from long-term qualification

Passing Chat's full end-to-end suite is a strong short-term gate. It does not
exercise multi-day retention, repeated eviction, storage quotas, schema
upgrades, malformed input, or large message-heavy namespaces.

The long-term fault matrix and explicit definition of done should remain.

### It defines safe cutover

One namespace has one sync implementation and one writer. Canary routing is an
operator action. Rollback stops the new writer before restoring the old route.
After the observation window, the old code and flags are removed.

This is clearer and safer than an indefinite consumer feature flag.

## What my plan gets wrong or overstates

### It front-loads too much query machinery

Requiring durable desired-query membership before any Chat branch would delay
the most useful integration feedback. Chat's per-server split gives us a
bounded place to test the cursor engine first.

The corrected sequence is:

1. full authorized namespace sync for the compatibility branch
2. complete Chat end-to-end and raw-store security tests
3. measure real namespace sizes and snapshot costs
4. add desired-query synchronization before broad production rollout

This produces evidence earlier without accepting windowed sync as the final
architecture.

### It chose the wrong transaction API for asynchronous mutators

`transactionSync` is correct for the pure pull engine. Existing TypeScript
mutators are asynchronous even when their actual SQL operation is local.

The push host should use `ctx.storage.transaction(async () => ...)`. Cloudflare
documents that SQL executed through `ctx.storage.sql` inside that closure is
part of the transaction. Every cursor must be fully consumed before the next
`await`. Milestone 0 must prove rollback across an await using workerd and a
deployed test Durable Object.

External network calls, queues, and side effects cannot happen inside the
transaction. Existing post-commit task machinery must collect and execute them
only after a successful commit.

### It did not choose a concrete local mutator host

A native Rust binary cannot execute arbitrary Chat TypeScript by itself. Adding
a native statement RPC immediately would recreate the independent plan's most
complex new protocol.

The resolved choice is:

- use the native Rust server for core conformance, persistence, and benchmarks
- use local workerd with the same TypeScript Durable Object and Rust WebAssembly
  bundle for Chat and Soot end-to-end tests
- defer a standalone native custom-mutator ABI until a real non-Cloudflare
  consumer requires it

This still gives us a true native engine target while making local consumer
tests production-equivalent.

### It creates too much schema before the first vertical slice

Watermark, change log, clients, and mutation ordering state belong in the first
port. Query, desire, membership, and reference-count tables belong in the
query-aware milestone. Their migrations should be designed early and created
when the feature lands.

### Its performance thresholds were too soft

The resolved plan keeps the independent plan's direct native target and adds
baseline-relative Cloudflare gates:

- native acknowledgement p50 below 3 ms on the existing fixture
- no Cloudflare p50 or p95 regression beyond 20 percent at equivalent load
- no correctness failure or reset during the 100-client storm lane
- stable WebAssembly memory across query and connection churn

## Resolved decisions

| Topic                | Independent plan                     | My plan                           | Resolution                                               |
| -------------------- | ------------------------------------ | --------------------------------- | -------------------------------------------------------- |
| Product protocol     | HTTP pull first                      | enhanced HTTP pull                | enhanced HTTP pull, websocket deferred                   |
| First implementation | direct core port                     | platform proof then core          | Cloudflare proof first, core port immediately after      |
| Rust host            | direct workers-rs Durable Object     | Rust Wasm in TypeScript host      | TypeScript Durable Object host with Rust Wasm            |
| Pull transaction     | Rust-owned transaction               | TypeScript `transactionSync`      | `transactionSync` around pure Rust and SQL pull          |
| Push transaction     | remote interactive statement channel | colocated TypeScript mutator      | async Durable Object transaction with bundled mutator    |
| Native role          | full product host                    | core and HTTP host                | conformance, benchmark, and Rust-only host initially     |
| Local consumer test  | native plus sidecar                  | unspecified callback              | local workerd using production host bundle               |
| Authentication       | new HMAC claim token                 | host handoff                      | existing consumer auth, normalized claims passed to Rust |
| Change log           | touched primary keys                 | dedicated touched-primary-key log | dedicated `_zsync_changes` from the start                |
| Initial sync surface | whole namespace or windows           | query-aware                       | bounded authorized table surface, no heuristic windows   |
| Chat early branch    | late measurement gate                | blocked on query engine           | early full authorized per-server branch                  |
| Chat production      | windows unless they fail             | desired-query membership          | desired-query membership before broad rollout            |
| Wire types           | full v51 crate                       | used subset in core               | used HTTP and AST subset, split later if needed          |
| Crates               | five crates                          | three crates                      | three Rust crates plus one thin TypeScript host package  |
| Verification         | named harness lanes                  | harness plus fault matrix         | both, staged by milestone                                |
| Cutover              | feature flag then flip               | namespace ownership               | explicit per-namespace cutover and old-path deletion     |

## Resolved architecture

```text
Zero 1.7 client
    |
    | enhanced HTTP pull and push transport
    v
consumer worker
    |
    | authenticate and route namespace
    v
TypeScript Durable Object host
    |
    +-- transactionSync for pull
    |     +-- Rust Wasm sync engine
    |     +-- ctx.storage.sql
    |
    +-- async storage transaction for push
          +-- Rust ordering preflight
          +-- bundled TypeScript mutator
          +-- DO-local SQLite transaction adapter
          +-- Rust ordering finalization
          +-- commit
          +-- post-commit external effects

Native harness target
    |
    +-- axum
    +-- the same Rust sync engine
    +-- rusqlite, WAL, one writer per namespace
```

### Rust workspace

```text
crates/
  sync-core/       wire subset, validation, cursor engine, query engine later
  sync-native/     axum, rusqlite, namespace files, harness administration
  sync-wasm/       narrow wasm-bindgen calls and JavaScript SQL adapter

packages/
  sync-cf-host/    TypeScript Durable Object host and consumer integration API
```

`sync-core` depends on a small synchronous database interface:

- execute SQL with positional bindings
- return typed rows with deterministic conversion
- run no host network I/O

The host owns transaction entry and exit. The engine must not create transaction
statements because Durable Object SQL rejects them.

### Durable state in the first vertical slice

- `_zsync_meta`: storage and protocol version, namespace epoch
- `_zsync_watermark`: durable high-water mark and retained floor
- `_zsync_changes`: watermark, table, operation class, touched primary key
- `_zsync_clients`: client group ownership and last mutation IDs

Application-table triggers append touched primary keys. Pull responses resolve
all patch values against live application rows inside the transaction.

The sync log is dedicated. It does not share retention with
`_orez._zero_changes`.

### Durable state added with desired queries

- transformed query AST and hash
- query dependency tables
- client desired-query state and acknowledgement version
- ordered query-row membership
- per-client-group row reference counts
- permission transformation version

The first query algorithm re-runs affected SQLite queries and diffs membership.
Incremental maintenance is added only after a profile proves it necessary.

### Mutator lifecycle

For a normal mutation:

1. authenticate in the consumer worker
2. route to the namespace Durable Object
3. enter `ctx.storage.transaction(async () => ...)`
4. Rust validates client-group ownership, replay, ordering, and input
5. run the registered TypeScript mutator against a DO-local SQLite adapter
6. Rust records LMID and change markers after mutation effects
7. commit
8. return acknowledgement
9. run deferred effects after commit

For an application error:

1. the effects transaction throws and rolls back
2. classify the error outside the rolled-back transaction
3. enter a second transaction
4. Rust advances the LMID for the rejected mutation
5. return the application error result

For an infrastructure or invariant error, keep the LMID unchanged and return a
server error so the operation can be retried safely.

## Resolved milestone plan

### M0: platform contract before the port

Deliver:

- minimal Rust core compiled natively and to WebAssembly
- TypeScript test Durable Object host
- local workerd test project
- native `rusqlite` probe
- sync pull transaction probe
- async push transaction probe with an `await` between SQL operations
- one representative read-then-write mutator
- one multi-table mutator
- one application-error mutator with deferred side effects
- JavaScript, WebAssembly, and SQLite value round trips
- Rust panic and JavaScript exception rollback tests
- initial bundle, cold-start, CPU, and memory measurements

Exit gate:

- all transaction probes pass locally and on a deployed test Durable Object
- no external effect runs before commit
- rollback after an await removes every SQL effect
- Rust and JavaScript errors cannot advance LMID accidentally
- numeric and cookie boundaries have an explicit wire representation

### M1: port the executable cursor specification

Deliver:

- all 19 named TypeScript reference tests ported to Rust
- every table-driven case retained
- Soot's cap, prefix LMID, skip classifier, and unknown-table tests
- dedicated change-log triggers
- snapshot, diff, reset, retention, replay, and application-error semantics
- generated trace comparison against the TypeScript reference

Exit gate:

- unit, model, and differential tests pass
- acknowledgement and cookie properties hold for randomized traces
- there are no ignored cases or generic unknown-table skips

### M2: native real-client target

Deliver:

- `sync-native` process
- `rust-local` harness target
- namespace file lifecycle
- deterministic administration routes for the harness
- hard restart and persistence support

Required lanes:

- smoke
- shapes
- seeded sweep
- permissions
- reconnect and persisted storage
- multi-tab client groups
- hard process kill
- storm and benchmark

Exit gate:

- real Zero 1.7 clients converge
- cookies stay monotonic through process death and restart
- native acknowledgement p50 is below 3 ms on the existing fixture

### M3: Cloudflare real-client target

Deliver:

- production-shaped TypeScript host
- Rust WebAssembly bundle
- `rust-cf` harness target
- deployed test Durable Object
- deterministic boot identity for eviction tests
- pinned toolchain and deployment configuration

Run every M2 behavior lane on Cloudflare. Run benchmark lanes with a documented
region and account configuration.

Exit gate:

- native and Cloudflare behavior is semantically equivalent
- normal eviction creates no reset
- forced retention loss creates the expected reset
- p50 and p95 stay within 20 percent of the current TypeScript Durable Object
  baseline at equivalent load
- the bundle and memory measurements retain explicit headroom

### M4: Soot production composition

Soot uses a bounded project sync surface and already has the strongest HTTP
pull production composition. It is the right first consumer.

Deliver:

- Soot auth and namespace adapter
- Soot mutator registry bundled into the Durable Object host
- DO-local SQLite adapter for Soot's on-zero transaction interface
- cursor and snapshot comparison against the current endpoint
- control-plane and project-plane integration tests
- Cloudflare runtime validation
- explicit namespace cutover and rollback scripts

Exit gate:

- Soot tests and deployed conformance pass
- canary namespaces survive replay, eviction, retention, and application errors
- no namespace has two writers
- old Soot path is removed after the observation window

### M5: Chat compatibility branch

Create a Chat branch that points both control and per-server clients at the new
local workerd deployment. Use full authorized sync surfaces at this stage.

Correctness rules:

- every row is filtered by the server-side permission policy
- any cross-row permission dependency forces clear plus authorized snapshot
- no heuristic age or row-count windows
- server switches use distinct namespace and local storage identities
- raw client-store tests inspect stored rows directly

Required validation:

- complete Chat end-to-end suite with zero new skips
- allow and deny cases for every permission family
- membership addition and revocation
- solo channel and secret data behavior
- lost push response and mutation replay
- fresh browser context hydration
- server switching without cache leakage
- local workerd and deployed Cloudflare runs
- namespace row count, bytes, snapshot bytes, and pull latency report

Exit gate:

- the branch is suitable for controlled short-term use
- any known scale limit is measured rather than inferred
- the branch is not treated as the final broad Chat production architecture

### M6: server-aware desired queries

Extend the current HTTP transport, which already receives
`desiredQueriesPatch`, so it forwards the patch with pull state instead of
acknowledging it locally.

Deliver:

- server acknowledgement only after query effects are durable
- transformed Zero 1.7 query AST validation
- SQLite compilation for the exact Soot and Chat query shapes
- durable desire, membership, ordering, and row-reference state
- dependency-based query recomputation
- transformation-version invalidation

Required cases:

- query put, delete, and clear
- overlapping queries sharing rows
- ordering and limit boundary shifts
- related rows
- parent-table permission changes
- permission contraction and expansion
- reconnect before and after query acknowledgement
- lost response after commit
- retention reset
- raw-store forbidden-row assertions

Exit gate:

- query acknowledgement never leads its row effects
- a row is deleted only after its last active query reference disappears
- every Chat query shape used by the end-to-end suite is supported
- unsupported AST shapes fail deterministically

### M7: Chat production cutover

Deliver:

- desired-query transport enabled for control and per-server clients
- production namespace migrations
- canary namespace routing
- observability for query recomputation, patch size, resets, and LMID
- rollback drill

Exit gate:

- the complete Chat suite passes locally and on Cloudflare
- raw-store security remains green
- representative large namespaces meet the recorded budgets
- restart, eviction, replay, and permission-change drills pass
- old Chat sync paths are deleted after the observation window

### M8: long-term qualification

Run:

- multi-day soak
- high desired-query and tab churn
- message-heavy large namespaces
- repeated eviction and process kill boundaries
- offline clients beyond retention
- schema and permission transformation upgrades
- storage quota and failure injection
- malformed and adversarial input
- WebAssembly memory growth checks
- partial deployment and rollback drills
- backup and restore

The server becomes the general replacement only after these have explicit
budgets, reproducible commands, and passing evidence.

## Correctness invariants retained from both plans

1. The change log stores touched primary keys and patches read live rows.
2. A cookie may under-report durable work and may never over-report it.
3. An acknowledgement never becomes visible before its effects commit.
4. An LMID-only change still advances the cookie.
5. A capped response represents a complete change-log prefix.
6. Caps apply before primary-key deduplication at change-row boundaries.
7. Watermarks stay monotonic through restart, eviction, and pruning.
8. Application errors roll back effects and advance LMID in a second
   transaction.
9. Visibility is evaluated against live state.
10. Unknown change-log tables fail loudly.
11. Client groups have one durable authenticated-user owner.
12. Mutation replay is idempotent.
13. Query acknowledgements never lead durable membership and row effects.
14. Overlapping queries retain a row until its last reference disappears.
15. Permission changes cannot leave forbidden rows in the client store.
16. Ordering has a deterministic primary-key tie-breaker.
17. External side effects run only after mutation commit.

## Remaining risks to prove, rather than debate

### Existing mutator compatibility

The main implementation risk is adapting the existing asynchronous on-zero
transaction interface to DO-local synchronous SQL calls inside an asynchronous
storage transaction. M0 must run representative real mutators, not toy inserts.

### WebAssembly boundary overhead

Repeated Rust to JavaScript SQL calls could erase a Cloudflare performance win.
Measure statement count and boundary time. Batch plans only where the profile
shows value.

### Query recomputation cost

Re-running affected queries is simpler and safer than building incremental view
maintenance. It may consume too many SQLite row reads for large Chat queries.
The M5 dataset report and M6 counters decide where to add dependency or
primary-key narrowing.

### JavaScript integer precision

Cloudflare SQL returns numbers through JavaScript and documents a precision
limit. Cookies and watermarks should use a tested representation that cannot
silently round. M0 owns this decision.

### Native consumer hosting

The native binary initially proves and serves the Rust engine but does not run
arbitrary TypeScript application mutators. That is an explicit scope boundary.
A native application ABI should be designed only when a real deployment needs
one.

## Definition of done

The project is complete when:

- one Rust core serves native and Cloudflare harness targets
- the Cloudflare host runs existing TypeScript mutators in the namespace
  Durable Object transaction
- the reference delta suite and every required harness lane pass
- Soot and Chat use the new server for all production namespaces
- Chat desired queries and permission transformations are server-aware
- raw client stores contain no forbidden rows
- fault, soak, migration, backup, restore, and rollback drills pass
- deployment and incident documentation is checked in
- the old zero-cache, PostgreSQL compatibility, dual routes, and temporary
  compatibility branch are removed

## References

- [Cloudflare Durable Object SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Cloudflare Durable Object state and concurrency](https://developers.cloudflare.com/durable-objects/api/state/)
- [Cloudflare Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/)
- [TypeScript reference core](../src/sync-server/sync-server.ts)
- [Reference delta suite](../src/sync-server/sync-server.test.ts)
- [Vendored HTTP transport](../harness/src/vendor/httpPullTransport.ts)
- `~/soot/src/zero/httpPullProject.server.ts`
- `~/chat/src/zero/client.tsx`
- `~/chat/src/zero/core.ts`
