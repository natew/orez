# Rust sync server: plan comparison and resolved combined plan

Status: superseded by [rust-sync-server-final-plan.md](./rust-sync-server-final-plan.md)

Date: 2026-07-09. Inputs: my plan (`rust-sync-server-plan.md`, "plan A") and
the other agent's plan (`rust-sqlite-zero-implementation-plan.md`, "plan B"),
written independently from the same brief. This doc is my honest comparison
and the plan I would actually execute.

## Where plan B is better (I concede these)

### 1. The Cloudflare host boundary — plan B is right, and my design was broken

Plan A made the Rust code the Durable Object itself (workers-rs) and ran app
mutators over an interactive worker↔DO statement channel. That channel cannot
work: `transactionSync` takes a synchronous closure, so a transaction can
never span an HTTP round trip to the app worker. Making it atomic anyway
means reintroducing exactly the snapshot/restore transaction emulation +
tx-journal machinery this rewrite exists to delete. Plan B's boundary — a
thin TypeScript DO host that owns `transactionSync`, routing, auth handoff,
and calls the app's TypeScript mutators in-process inside the same
transaction, with Rust compiled to wasm as the engine library — solves
mutators, auth (better-auth session resolution is TS-native), and atomicity
in one move. It also deletes plan A's need for a `transactionSync` extern
binding and a signed-token auth scheme. Adopt plan B's boundary wholesale.

### 2. Chat's security requirement — plan B read the code, plan A hand-waved

I verified `~/chat/src/data/where/channel.ts` / `message.ts`: chat has
private channels whose read permission is a cross-table EXISTS
(`serverMembers`, `channelUserRoles`, denormalized `canAdmin`) inside one
server namespace. Uniform visibility per namespace — the assumption that
makes CVR-free cursor diffs correct — does not hold for chat, and these
predicates are not row-local, so the reference core's two escape hatches
(diff everything, or per-user snapshot every pull) are respectively insecure
and unscalable for message-heavy namespaces. Plan A deferred this behind a
"measure then maybe add windows" gate; windows don't answer permissions at
all. Plan B's query-aware layer (durable desired-query state, per-query row
membership with refcounts, recomputation-on-touched-dependencies — CVR-lite
without porting the IVM engine) is the right middle path, and because it
evaluates real limit/cursor queries it also subsumes plan A's window policy
as the bounding answer. Adopt it, including the raw-client-store
forbidden-row assertions as a permanent harness lane.

### 3. Sharper risk retirement and ops rigor

Plan B's M0 (prove wasm transaction commit/rollback, panic unwinding, value
fidelity incl. i64 limits, bundle size, eviction re-instantiation, with a
stop rule if atomicity fails) is the correct first milestone. Its
observability field list, one-namespace-at-a-time cutover with single-writer
guarantee, mixed-version upgrade rule, and M7 soak/fault qualification are
all better than plan A's thinner "harness is the verification" story. Its
performance framing (record TS baselines first, hold within budgets, only
profile-driven optimization) is more honest than plan A's "beat TS local
p50" as a gate. Its 16-invariant list is a superset of plan A's; adopt it.

### 4. KISS crate layout

Three crates (`sync-core`, `sync-native`, `sync-wasm`), wire types inside
`sync-core` until reuse justifies a split. Plan A's standalone `zero-wire`
crate with the full v51 transcription "for a future load generator" is
speculative surface — cut it.

## Where plan A is better (the combined plan keeps these)

### 1. Sequencing: don't block soot on chat's requirements

Plan B gates all consumer integration on M4 (query compiler + durable
membership), because "query-aware security is part of the server contract."
That's true for chat and false for soot: soot's two planes are already
proven in production on exactly the reference-core semantics (uniform
project visibility with a handful of row-local per-table predicates, caps,
prefix lmids), and soot's migration needs none of the query-aware machinery.
Serializing soot behind the largest, riskiest component delays the first
production consumer by the entire M4 build for no correctness gain. The
combined plan forks after Cloudflare conformance: soot migrates on the
baseline surface while the query-aware layer is built in parallel for chat.
Soot can later adopt query-aware sync if it ever wants non-row-local
predicates, from a position of already running the new engine.

### 2. Name the client-side and harness work the query extension implies

Plan B says "extend the transport" in one line. That's a real cross-repo
work item plan A's framing surfaces: on-zero's `httpPullTransport`
(~/takeout) today keeps `desiredQueriesPatch` entirely client-local and
synthesizes `gotQueriesPatch` — the server never sees queries. Query-aware
sync requires: (a) extending the on-zero transport to ship desired-query
changes and consume server query acks, (b) re-vendoring it into the harness,
(c) new harness lanes for the query lifecycle, (d) chat's client transport
flip from stock websocket to on-zero http-pull. Each is named in the
combined milestones; (a) lands in ~/takeout and needs its own review.

### 3. Keep the concrete harness/CI wiring

Plan A specified the mechanics plan B leaves implicit: harness targets
`rust-local` (spawn hook, modeled on orez-local-process) and `rust-cf`
(deployed worker, modeled on orez-cf including the deterministic
idle-teardown bootID), cargo jobs added to the existing Rust-toolchain CI in
`.github/workflows/ci.yml`, nightly + soak on mini-16, differential lanes
(shapes/sweep vs stock zero-cache) staying the oracle for all query
semantics including the M4 matrix. Keep all of it.

## Points of genuine disagreement I resolved by picking

- **Native mutators for consumers**: plan B gestures at "run the engine via
  the wasm boundary inside the app process." Fine as a later option; the
  combined plan keeps the native binary + built-in fixture mutators as the
  only supported native mutator path until a real consumer needs more.
- **Performance targets**: budgets (plan B) are the gates; plan A's sub-ms
  native engine time stays as a stretch goal, not a gate.
- **Protocol naming**: "enhanced http-pull" (baseline endpoints byte-stable,
  query extension additive and versioned) so every existing lane keeps
  gating the baseline unchanged.

## The resolved combined plan

Decision: SQLite-only Rust sync engine, one engine crate behind a narrow
synchronous SQL/host interface; two hosts from the start — native Rust HTTP
binary (rusqlite, WAL, one file per namespace) and a thin TypeScript DO host
calling the engine as wasm (host owns transactionSync, routing, auth, and
runs app TS mutators in-process inside the engine-managed mutation
lifecycle). Baseline surface is today's http-pull dialect, byte-compatible
with the vendored transport; the query-aware extension is additive and
required before chat (not soot). No websocket/CVR/IVM port; reconsider only
if measurements defeat recomputation.

Crates: `crates/{sync-core, sync-native, sync-wasm}`. Durable state in
dedicated `_zsync_*` tables (never consuming `_orez._zero_changes`):
metadata/epoch, watermark+floor, changes (touched pks only), clients/LMIDs,
and — for the query layer — queries (canonical AST + dependency tables),
desires, query-row membership (with order data for limits), and per-group
row refcounts. Patch values always from live rows. Plan B's 16 invariants
are the acceptance list; plan A's reference-core details (toZeroValue
conversions, positional-`?` only, serde_json/ryu float fidelity, i64
documentation) fold in.

Milestones (each gated; lanes run on both hosts once both exist):

- **M0 — platform contract proof** (plan B's M0 verbatim): wasm-in-DO
  transaction commit/rollback/panic probes, value fidelity, bundle/cold-start
  budgets, eviction probe. Stop rule: if atomicity can't be preserved across
  the wasm boundary, redesign the host boundary before porting anything.
- **M1 — engine port**: reference core + soot hardening (caps with
  last-included-watermark cookie, prefix lmids, skip/throw classifier,
  epoch); all 19 delta tests + soot's composition cases as Rust tests,
  failing-first; randomized model tests for cookie/LMID/prefix invariants.
- **M2 — native real-client conformance**: `sync-native` binary, `rust-local`
  harness target, lanes: smoke, shapes, sweep, permissions, reconnect,
  multi-tab, process-restart, storm. CI gains cargo build/test +
  smoke/shapes/sweep on rust-local; nightly gains rust targets.
- **M3 — Cloudflare real-client conformance**: `sync-wasm` + TS DO host,
  `rust-cf` target, all M2 lanes + eviction/hibernation lane (monotone
  cookies, zero 409s), recorded cold-start/CPU/bundle measurements.
- **M4a — soot migration** (parallel with M4b): soot's endpoints compose the
  Rust engine behind a flag beside the existing path; soot validators +
  chat-e2e-suite equivalents; per-namespace cutover with single-writer rule
  and rollback as an operator action; then delete soot's old path. Baseline
  surface only — soot needs nothing from M4b.
- **M4b — query-aware layer**: desired-query transport extension in on-zero
  (~/takeout, own review) + re-vendor into harness; AST-subset validator and
  SQLite compiler (equality/comparison, and/or/not, correlated EXISTS,
  related, orderBy + pk tie-breaker, limit, start cursors, bound params,
  deterministic rejection of everything else); durable
  queries/desires/membership/refcounts; dependency-driven recomputation;
  transformation-version invalidation; new harness lanes: query lifecycle,
  overlapping queries, limit boundary shifts, parent/related changes,
  permission expansion AND contraction, reconnect around query acks,
  forbidden-row raw-store assertions — all differential vs stock zero-cache
  where stock supports the shape.
- **M5 — chat migration**: chat schema + query-shape inventory, permission
  transformations (the `where/` predicates) as server-side transformed ASTs,
  chat mutators inside the host transaction, client transport flip on a
  branch, full chat e2e suite green with zero new skips, allow+deny
  raw-store test per permission family, namespace-switching leak tests.
- **M6 — long-term qualification** (plan B's M7): multi-day soaks,
  message-heavy namespaces, query/tab churn, kills at every tx boundary,
  retention pressure with offline clients, wasm memory stability,
  mixed-version deploy rule, ops docs, then delete every retired path in
  soot, chat, and orez.

Performance: record TS baselines (local + CF, identical load) before tuning;
gates are plan B's budgets (native ≥ parity with TS median, CF within 20% of
DO baseline, flat wasm memory, ≥40% bundle headroom); optimization order:
batch wasm crossings → cache compiled queries → index membership tables →
narrow recomputation → incremental maintenance only for measured-expensive
shapes. Sub-ms native engine time is a stretch goal.

Observability, cutover, and rollback sections of plan B apply as written.
No publish/release of any package without explicit approval.
