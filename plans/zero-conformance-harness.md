# zero conformance + consistency harness (research, 2026-07-09)

companion to `plans/zero-server-rewrite.md`. question asked: does a serious
zero test suite (lots of query shapes, many clients, jepsen-level consistency
checking) already exist upstream, and if not, how do we build one, validate it
against real zero first, and scale it?

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

## open questions

- how much of upstream's fuzz generator to port vs call out to (their
  generator is TS; porting the DESIGN is cheap, sharing regression corpora
  needs a common ast/json format, which their replay-artifact spec already
  sketches).
- custom mutators require an app server (push endpoint executes them). the
  harness needs a minimal generic push server for stock-zero lanes (zero's
  `zero-pg` PushProcessor makes this ~small) mirroring the orez fixture
  server semantics.
- permissions axes: worth designing early (upstream punts to L4); soot's
  named-query visibility rules are the real-world shape to model.
- name/home: separate repo (`~/zbench`?) vs `~/orez/harness`. leaning
  separate repo: it tests four different systems, none of which own it.
