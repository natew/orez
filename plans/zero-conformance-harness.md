# zero conformance + consistency harness (research + execution plan)

companion to `plans/zero-server-rewrite.md`. question asked: does a serious
zero test suite (lots of query shapes, many clients, jepsen-level consistency
checking) already exist upstream, and if not, how do we build one, validate it
against real zero first, and scale it?

**DECIDED (nate, 2026-07-09):** jepsen/elle is overkill; dropped. rust is
deferred (revisit only if the load generator hits client-count ceilings in
TS). we start from upstream's just-landed fuzzer assets (apache-2.0, portable
with attribution), add heavy query-shape lanes and load lanes, and the
harness MUST run against a pure-sqlite local target as well as cloudflare.
runners: nate's mac mini (already an agentbus peer, `mini-16`, isolated and
ownable) plus cloudflare (api key + credits available; lslcf account already
wired for orez experiments). see EXECUTION PLAN below; the jepsen and rust
sections that follow are kept as research context only.

## finding 1: upstream has half of it, and it is 3 weeks old and moving fast

surveyed `~/github/mono` at origin/main 6d84471c5 (2026-07-09). timeline:

- early 2026: a small zql fuzzer existed in `packages/zql` (query-gen.ts),
  toggled on/off in CI (#5303..#5338). this is probably why "last i heard
  there wasn't one" was true at the time.
- **2026-06-18** (#6136): a coverage-driven differential fuzzer for the
  ZQL/IVM engine landed in
  `packages/zql-integration-tests/src/chinook/fuzz/` (~20 modules: skeleton
  enumeration, pairwise coverage over query axes, scalar subqueries,
  exists-flips, swarm generation, push histories for incremental
  maintenance, shrink-to-repro with a budget, committed regression corpus,
  metamorphic wrappers, random-yield scheduling interleave). the driver
  header says it was **ported from rocicorp's internal rust project
  `rusty-ivm` (`rindle-fuzz/src/driver.rs`)**. that rust repo is not public
  (searched; only archived `repc` exists publicly in rust).
- **2026-07-06** (#6197): `ZERO_QUERY_FUZZER_EXPANSION_DESIGN.md` committed
  at repo root: a layered plan L0→L4, expanding "in concentric circles"
  from the IVM engine outward, ending at multi-client mutations, auth axes,
  reconnect/resume, and "proxies at every network boundary to test network
  fault injection". same commit landed the first L1/L2 harness
  (`chinook-zero-cache-fuzzer.pg.test.ts`): boots REAL pg logical
  replication → change-streamer → replicator → view-syncer in-process, plus
  a synthetic protocol client, and compares protocol-materialized results
  against a fresh pg oracle with explicit replication/client barriers.
- **2026-07-08** (#6204): +805 lines expanding it to randomized zero-cache →
  client query equivalence.

their core invariants (worth stealing verbatim):

```
query through zero == same query over postgres
incrementally maintained result == fresh hydrate after the same writes
```

differential testing against a pg oracle, plus "incremental == fresh".

also present upstream: `zql-integration-tests` (zql vs zqlite vs zero-pg
differential, chinook + pagila fixtures, collation/text-semantics/bigint
edges), `zql-benchmarks`, 144 test files in zero-cache, plus vitest lanes
against pg 15/16/17/18. protocol is still v51 on latest main (matches the
orez spike's pinned knowledge; chat runs 1.7.0-canary.3, soot/orez pin
1.6.1, one protocol client covers all of it).

## finding 2: the gap is exactly the half we need

what upstream's suite does NOT have (their own design doc confirms these are
future layers):

1. **multi-client concurrency**: no concurrent mutation workloads across
   many clients, no optimistic-rebase/rollback checking under interleave
   (their L4).
2. **faults**: no network fault injection, no process-kill/restart lanes,
   no reconnect/resume-under-writes (planned as proxies, not built).
3. **black-box operation**: everything is vitest driving mono internals
   in-process. none of it can run against a different implementation of the
   server. that is fatal for us: the whole point of a conformance suite for
   the rewrite ("run their test suites against ours", per the
   zero-compatible-sync-engine decision doc) requires a harness that speaks
   the WIRE protocol, not their module graph.
4. **scale/longevity measurement**: no harness that answers "how does
   memory/latency scale with N clients × M queries" (the measurement that
   found soot's 128MB wall was our own cf-load-longevity script).
5. **permissions**: fuzz lanes run ANYONE_CAN_DO_ANYTHING; auth axes are
   L4-future.

so: upstream is converging on the same idea from the inside out; we need the
outside-in half, and ours must be implementation-agnostic.

## finding 3: existing suites we already own

- **chat e2e** (`~/chat/src/integration/e2e/`, 15 playwright specs incl.
  multi-user-sync + 3 permissions suites + thread lifecycle): real-app
  conformance. orez already uses it as its acceptance suite (46/48 native,
  51/51 on the DO path at the time of the cf work; see
  `~/orez/src/cf-do/CHAT_E2E.md`). chat moved to cloudflare zero recently
  (largely agent-automated; solidity unverified) and has
  `test/chat-cf-load-longevity.test.ts`.
- **soot**: `scripts/ops/cf-load-longevity.ts` (the 50-user protocol load
  harness that measured the singleton wall), `validate-cf-do-runtime.ts`
  (multi-context browser validation + flicker MutationObserver), access-denied
  playwright suite.
- **orez**: the zero-http spike suite (26 tests pinning the v51 transport
  contract), now ported+hardened in on-zero `src/httpPull/`.

these are app-shaped and catch integration regressions, but none generate
adversarial query/mutation workloads or check consistency under faults.

## jepsen: what "jepsen-level" concretely means here

jepsen = generative concurrent workloads + fault injection (nemesis) +
history-based consistency checking (elle for transactional anomalies,
knossos for linearizability). elle is the useful part for us: black-box,
linear-time cycle detection over append/read histories, and it is consumable
WITHOUT the clojure stack via `elle-cli` (histories as edn/json).

prior art directly on point: `nurturenature/jepsen-causal-consistency`
(jepsen tests for local-first/CRDT sync systems) extends elle with
strong-session causal models and adds the key local-first insight: a
client's local view can hide replication failures, so checks need a
convergence phase ("final reads" on every client + strong convergence
assertion) in addition to per-client session checks.

zero's checkable guarantees, mapped:

| invariant | how to check |
| --- | --- |
| pokes are transactionally atomic (never observe a partial upstream tx) | tag rows with tx ids; assert clients never materialize a strict subset of one tx |
| per-client-group causal / monotonic snapshots | elle strong-session models over append histories |
| read-your-writes after mutation ack | history check: ack(m) then query must reflect m |
| exactly-once mutation application (LMID) | counter/list-append mutators; final value equals committed history |
| rollback on app-error is complete (no phantom rows) | inject failing mutators; assert optimistic state fully reverts |
| strong convergence | quiesce; every client's materialized views byte-equal the pg oracle |
| query correctness across the zql surface | differential vs pg oracle (steal upstream's axes/normalization rules) |

elle's list-append workload maps cleanly: custom mutators append integers to
a row's json array column (or an append-only table per key); reads are synced
queries over those keys; each client records an op history; export to
elle-cli. this is a real jepsen test of a sync engine, not a pastiche.

## proposal: `zbench` (working name), a rust wire-protocol harness

rust is the right call here, for the reasons nate gave (efficiency: tens of
thousands of concurrent websocket clients + history recording from one box;
safety; fast agent iteration) plus one more: rocicorp themselves prototype
the engine in rust internally, and their fuzz driver design is already
documented in TS for us to crib.

crates (one workspace):

1. **zero-protocol-client**: clean-room v51 client. websocket connect
   (initConnection, desiredQueriesPatch), poke application into an in-memory
   store (cookie ordering, gotQueriesPatch, lmid tracking), push (CRUD +
   custom mutators), reconnect/resume. references: mono
   `packages/zero-protocol/src/*` (valita schemas are readable specs), the
   orez spike's nine wire discoveries, on-zero `httpPullTransport.ts`. NOTE:
   this same crate must also speak the on-zero http-pull dialect
   (`/zero-http/pull|push`) so one harness drives both stock zero and the
   orez planes.
2. **workload**: query generator (port upstream's skeleton/axes/pairwise
   coverage design from `fuzz/` — it is well-factored and documented),
   mutation generator (insert/update/delete + elle list-append + failing
   mutators), multi-client schedules (concurrent writers, readers joining
   mid-stream, reconnect storms, query churn add/remove).
3. **oracle + checkers**: pg snapshot oracle with upstream's normalization
   rules (ordering, timestamp precision, numeric/json normalization);
   differential compare; elle history export (elle-cli as the transactional
   checker); custom checkers for the sync-specific invariants in the table
   above (convergence, poke atomicity, RYW, lmid).
4. **nemesis**: process kill/restart (zero-cache, pg, orez DO eviction via
   CF api), tcp/websocket faults via an in-harness proxy or toxiproxy
   (partition, latency, drop, slow-drip), replication-lag injection,
   client-side: tab-hide/resume semantics (transport pause).
5. **metrics**: per-op latency, poke lag (commit→client-observed), server
   rss/cpu sampling → scaling curves (clients × queries × write-rate). this
   reproduces the measurement that found the 128MB wall and gives the
   rewrite a quantified target.

deterministic-simulation (fdb/antithesis-style) is out of scope: the system
under test is node/typescript and can't be deterministically scheduled from
outside without antithesis-class tooling. black-box jepsen-style is the
right fit; keep seeds + full histories so every failure replays.

## run it against real zero FIRST (validation ladder)

1. stock zero-cache + real pg (docker compose), zero 1.6.1 and 1.7 canary
   lanes. harness must pass clean here or the harness is wrong. seed known
   bugs (kill -9 mid-tx, drop websocket mid-poke) to prove the checkers
   catch them. divergences found here are upstreamable bug reports
   (credibility + fixes flow back).
2. scale lane on stock zero: N clients × M queries × write-rate sweeps →
   published scaling curves (memory per client group, poke lag percentiles).
3. chat's CF zero deploy (validates the agent-automated migration nate is
   unsure about).
4. orez control plane (zero-http, already prod) and the embed project plane.
5. becomes the acceptance gate for rewrite phase 2/3: the same workloads +
   checkers, byte-identical, run against the new server. this IS the
   conformance suite the clean-room engine was always going to need.

## where to run it

- **OVH dedicated (primary farm)**: jepsen-style fault injection wants root,
  tc/netem, network namespaces, and freedom to kill processes; a single big
  box (32c/128GB class) runs many isolated pg+zero-cache stacks in netns
  simultaneously, 24/7, at fixed cost. rent one, maybe two (second as
  workload-generator so client load and server under test don't share a
  box when measuring scaling curves).
- **cloudflare containers (burst + CF-shaped lanes)**: GA since 2026-04-13,
  thousands of lite instances, active-cpu billing, scale-to-zero. good for
  embarrassingly parallel fuzz sweeps (each container = one self-contained
  pg+zero+harness case) and for generating load against CF-deployed orez
  from inside the CF network. weak for nemesis work (no root netem, no
  arbitrary network shaping). use for width, not for faults.
- laptop for dev loop; every lane must run with one command locally.

## sequencing (rough)

1. zero-protocol-client crate + a 10-client smoke against stock zero-cache
   (docker) with the convergence checker. smallest thing that proves the
   wire client.
2. elle list-append workload + elle-cli integration + nemesis v1
   (process kill + tcp proxy faults). first real jepsen run against stock
   zero. write up findings.
3. port the query-axes generator; differential oracle lanes; regression
   corpus format (steal upstream's replay-artifact json shape from their
   design doc so cases can be shared/compared).
4. scale lanes + metrics; OVH box provisioned; nightly runs.
5. point it at chat-CF, then orez planes; wire into rewrite phase 2 gates.

## EXECUTION PLAN (2026-07-09, the decided path)

goal: a WORKING harness fast, not a framework. TS throughout (reuse the
stock zero client as the driver; reuse upstream's generator design). home:
`~/orez/harness/` with its own package.json, never published with the orez
package; extract to its own repo later only if it earns it.

### the design constraint that shapes everything: three targets, one harness

```
SyncTarget interface
  setup(schema, seedData)          create/reset the authoritative store
  client(n, opts) -> ZeroClient[]  stock @rocicorp/zero clients, transport per target
  write(sql | mutation[])          upstream writes (sql) and client mutations
  oracle(query) -> rows            FRESH query against the authoritative store
  barrier()                        wait: writes visible to server, pokes observed
  metrics()                        rss/cpu (local), DO analytics (cf), poke lag
  teardown()
```

| target | server | store | client transport | oracle |
| --- | --- | --- | --- | --- |
| `stock-zero` | real zero-cache (docker) | postgres (testcontainers) | stock websocket | fresh pg query |
| `orez-local` | orez sync server core, plain bun/node process | **pure sqlite file** | on-zero `http-pull` | fresh sqlite query |
| `orez-cf` | same core hosted in a DO | DO sqlite | on-zero `http-pull` | sealed admin sql read on the DO |

the `orez-local` target does not exist yet and building it IS the point: a
generic, schema-driven sync server core (snapshot pull + push/LMID, later
cursor-diff) written as a plain TS module over a sqlite handle (bun:sqlite
locally, `ctx.storage.sql` on the DO). one core, two hosts. this seeds
rewrite phase 2/3 (`plans/zero-server-rewrite.md`): the generalization of
soot's `httpPull.server.ts` semantics plus the spike fixture server, backed
by real sqlite instead of in-memory maps. the harness and the rewritten
server grow up together, which is exactly the leverage we want.

### milestones (each ends runnable with one command)

**M0, baseline upstream [RAN 2026-07-09 on `work`, needs a clean re-run on
the mini]:** full zql-integration-tests matrix (pg 15/16/17/18 via
testcontainers + no-pg): 1094 passed / 7 failed / 54 skipped in 613s on a
loaded 16-core box. the 7 failures: ~5 are fuzz push-parity tests hitting
their 120s budget under machine load (rerun clean on the mini before calling
them real), plus text-semantics.pg failures on pg-15/16 worth triaging
(possible collation/ICU environment sensitivity). invocation gotchas learned:
the package's `TEST_PG_MODE` env is a root-CI concept, use
`pnpm exec vitest run --project='*17*'` to scope; the zero-cache protocol
fuzzer file runs under packages/zero-cache's vitest config, not
zql-integration-tests'. runbook in `harness/README.md`.

**M1, harness skeleton + stock-zero target [DONE 2026-07-09, modern API]:**
`~/orez/harness/` (zharness, commits d0315e0 + 96d284b). SyncTarget
interface; stock-zero target = embedded postgres (wal_level=logical, no
docker) + real zero-cache 1.6.1 spawned from node_modules + the fixture app
server (`harness/src/app-server.ts`) serving named-query transform
(ZERO_QUERY_URL) and custom-mutator execution (ZERO_MUTATE_URL); permissions
deployed by replicating zero-deploy-permissions' SQL in-process. legacy
CRUD/queries are OFF (`ZERO_ENABLE_CRUD_MUTATIONS=false`, no
enableLegacy* schema flags) per nate: the harness exercises the modern
surface the orez server must serve. smoke green: 50 clients hydrate named
queries, push 400 custom mutations (server-acked in 1.16s), receive
upstream-behind-zero's-back writes via replication, converge, oracle-compare
equal, and an ad-hoc local zql query is asserted to read the synced cache
without syncing more. wire facts pinned in harness/README.md (auth-echo
userID pinning, callable-form registry mutate, node-not-bun spawn).
still open in M1 scope: a 1.7-canary lane, chinook fixture.

**M2, orez-local pure-sqlite target [DONE 2026-07-09]:** the sync server
core landed in orez proper (`src/sync-server/sync-server.ts`, commit
db05f1c): snapshot pulls with schema-driven value conversion + per-user
visibility hook, v51 custom-mutator push with LMID bookkeeping (replay
idempotent, app-error advances LMID via savepoint rollback), soot's
group→user claim in sqlite dialect, monotonic version cookie, 409
future-cookie. host-agnostic `SyncDb` interface (bun:sqlite now,
`ctx.storage.sql` next). the harness target runs it over bun:sqlite +
node:http with on-zero's PRODUCTION httpPullTransport (checkout source;
the module is self-contained). smoke green first run; 50 clients/400
mutations in 5.5s, no postgres, no docker.

**M3, query-shape lanes [FIRST LANE DONE 2026-07-09, generator port still
open]:** shipped `harness/src/shapes.ts` (commit d21277d): a 17-query
CROSS-IMPLEMENTATION differential — identical deterministic dataset + write
script on stock-zero and orez-local, every corpus query compared at hydrate
+ post-writes + incremental==fresh, with a no-vacuous-greens gate (every
query must return data). this reframing matters: stock evaluates queries
server-side (IVM row selection), orez-local ships snapshots and evaluates
client-side; equal results IS the conformance property the rewrite must
hold. found immediately: client not(exists()) unsupported (upstream bug
3438), postgres.js jsonb double-encoding writer discipline, pg-jsonb key
order normalization policy, 1.6.1 registry raw-args invocation. still open:
port upstream's generator for randomized sweep lanes + replay artifacts +
regression corpus; grow the corpus toward chat's full census (start/cursor
pagination, deeper junction shapes).

query-shape corpus: model on ~/chat (nate 2026-07-09), the canonical large
zero app. census of its query layer (`src/data/queries/` 37 files +
`src/data/generated/syncedQueries.ts`, 125 synced queries): 127 `.related()`
calls (deepest shape `queryMessageItemRelations`: 9 relations off message,
3 levels deep, nested `one()` + per-relation where/orderBy/limit windows),
132 `.one()`, 75 exists-style permission fragments (`src/data/where/`:
auth-dependent `serverWhere` transforms composed into every query), and/or
trees (36), `IN` lists, `LIKE/ILIKE` (14), junction relationships with
filters (where upstream zql still has gaps: "order by not supported in
junction relationships yet" is a live comment there). the generator's axes
must cover these shapes, and chat's own query files can be lifted almost
verbatim as a fixture schema + named-query corpus for a "real app" lane.

harness invariant (nate 2026-07-09): zero answers reads from the CLIENT
cache, so a single client round-tripping its own writes verifies nothing.
every lane must verify on (a) clients that did NOT write the data (cross-
client sync) and (b) a fresh client that connects after the writes and
hydrates purely from the server. the smoke already enforces both; keep them
mandatory in every future lane.

**M4, load lanes:** N clients × M queries × write-rate grid, plus longevity
(hours). measures: poke/pull lag percentiles, convergence time after write
bursts, server rss/cpu over time, per-client memory. client driver runs many
stock zero clients per node process, multi-process fanout on the mini; if TS
tops out below the client counts we need, THAT is the trigger to revisit
rust for the load generator only. deliverable: scaling curves committed to
plans/, including the stock-zero baseline curve the rewrite must beat.

**M5, orez-cf target:** host the M2 core in a DO on the lslcf account
(extend the orez-cf-todo-experiment wiring), sealed admin oracle endpoint,
run smoke + backbone + a load lane from the mini against it. cf containers
(credits) come in here for sweep width: each container runs one
self-contained harness case; workers-ai-free parallelism for the
embarrassingly parallel lanes only. faults/kill-restart lanes stay local
where we have process control.

**M6, make it a gate:** nightly backbone+sweep+load on `mini-16` (agentbus
scheduled), results posted; wire `bun harness backbone --target orez-local`
into orez CI; the rewrite plan's phase 2 acceptance references these lanes
byte-for-byte.

### runners (nate 2026-07-09: dev + initial validation happen on `work`;
### the mini is purely the runner for LARGER tests)

- `work` (this machine): all development, M0 initial validation, backbone
  lanes, smokes.
- `mini-16` (mac mini, agentbus peer, online, idle/isolated): the runner
  for the big stuff only — sweep lanes, load grids, longevity runs. it has
  been idle a while; provision on first use (bun, docker/colima, checkouts).
  note: spawn cwd must be a real path on the peer, `~` did not resolve.
- `ci-64` peer exists too if a sweep needs more cores.
- cloudflare: lslcf account for the orez-cf target deploys + containers for
  sweep width (GA, active-cpu billing, thousands of lite instances).

### open questions (narrowed)

- custom mutators need a push executor per target; M2's core provides it
  for orez targets, and stock-zero lanes use zero's own push endpoint with
  a tiny fixture app server (zero-pg PushProcessor).
- permissions axes: model soot's named-query visibility shapes once M3 is
  stable; upstream punts these to their L4 so this is greenfield.
- how much generator code ports cleanly vs needs rewrite: answered in M3 by
  doing it; the design doc + apache license make either path fine.
