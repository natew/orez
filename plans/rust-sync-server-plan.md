# orez-rs: a Rust zero sync server, sqlite-only, Cloudflare-ready from day 1

Status: superseded by [rust-sync-server-final-plan.md](./rust-sync-server-final-plan.md)

## Context

We are replacing the server half of rocicorp zero for ~/soot and ~/chat. The TS
reference core (`~/orez/src/sync-server/sync-server.ts`, ~500 lines + 19-test
delta suite) proved the CVR-free cursor-diff design; the conformance harness
(`~/orez/harness`) drives real zero 1.7 clients differentially against stock
zero-cache and any http-pull target; soot's production composition
(`~/soot/src/zero/httpPullProject.server.ts`) proved the real-world layer.
Nate's directive: a purpose-built Rust server, sqlite-only, with robust
Cloudflare deploy support from day 1, for maximum performance. This plan is the
implementation strategy; every milestone is gated on an existing harness lane.

## The three strategic calls (my thoughts, for review)

### 1. Scope: build the http-pull server as the product; treat the websocket/CVR surface as a separate, gated future project

The brief recommended planning the full websocket surface (b) with http-pull
(a) as milestone one. Having now studied zero-cache internals, I partially
disagree. Full websocket parity is not mostly protocol work — it requires
porting zero's ZQL/IVM engine (~18k LOC `zql` + 3.2k LOC `zqlite`) because the
server incrementally maintains per-query views. That's 80% of the effort of
scope (b) and it serves a surface both consumers are actively migrating OFF:
soot's control plane already shipped on http-pull, its project plane is
flipping now, and chat's replacement path is the same transport flip. Building
a Rust IVM engine to support a protocol we're abandoning is backwards.

So: the Rust server's v1 surface is the http-pull dialect (`POST /pull`,
`POST /push`), CVR-free, sqlite-authoritative — the thing the harness gates
today. The `zero-wire` crate transcribes the full v51 message set anyway
(it's ~2 days of mechanical serde work and also gives us a future Rust load
generator per the zbench design), so the ws door stays open. We commit to a
ws/IVM phase only if the chat measurement gate (below) proves whole-namespace
windowed sync can't carry chat.

### 2. Cloudflare from day 1: one core crate, two thin hosts

Verified: workers-rs 0.8.x exposes DO `SqlStorage` with synchronous `exec`
(`~/github/workers-rs/worker/src/sql.rs`). `transactionSync` is not bound yet —
we write that extern binding ourselves (small wasm-bindgen shim; the platform
capability is proven, the TS core already uses it in a DO). The design that
makes dual-host cheap is the same one the TS core uses: the entire engine is
generic over a 3-method `SyncDb` trait (`exec`, `all` (query), `transaction`),
synchronous, positional-`?`-only. Native binds rusqlite; CF binds
`ctx.storage.sql` via wasm. Everything above that trait is shared and
identical, so conformance runs against both hosts with the same core.

Honest performance note: on CF, every statement crosses the wasm↔JS boundary
into the same DO sqlite the TS core uses, so Rust won't dramatically beat the
TS DO numbers there (ack p50 174-184ms is mostly network + DO scheduling).
The Rust wins are: native deploys (target sub-ms engine time, ~3ms → <1ms ack
p50 local, one static binary, no bun/node), memory (KB per idle namespace),
one codebase for both hosts instead of TS-core + 7.7k-line pg-facade, and a
foundation that can later absorb the IVM engine if we ever need it.

### 3. Custom mutators stay in the app (TypeScript); the Rust server owns the transaction around them

Mutators are app code (drizzle/TS) in soot and chat; a Rust server cannot run
them, and porting 54 chat mutation files to Rust is a non-starter. Today's CF
shape already has the answer: the app worker executes mutators as SQL against
the data DO (through orez's pg-facade). We keep that topology and delete the
pg-wire translation from the sync path:

- `POST /push` lands on the Rust server. It validates, orders (LMID), and
  opens a per-mutation transaction.
- For each mutation it calls the app's mutator endpoint (colocated worker /
  local HTTP) with `{name, args, userID, mutationID}` plus a tx-scoped
  statement channel: the app executes its mutator by sending
  `{sql, params}` statements (sqlite dialect) back over that channel,
  interactively (read-then-write works). On CF this is worker↔DO traffic, the
  same cost profile as today's pg-facade statements, minus the AST rewriting.
- App throws → Rust rolls back tx1, advances LMID in tx2 (two-transaction
  app-error semantics, exactly the reference core). Replay → idempotent ack.
  Triggers feed the change log regardless of who wrote.

Apps keep drizzle; orez's existing pg→sqlite translation can run app-side as
an interim shim, with drizzle-sqlite as the end state. For the harness (whose
fixture mutators are trivial), the native binary also supports built-in
mutators behind a feature flag so lanes don't need a sidecar app server —
same code path, the built-ins just implement the same mutator-session trait.

## Architecture

```
cargo workspace at ~/orez/crates/
  zero-wire        v51 + http-pull wire types (serde), ["tag", body] tuple enums,
                   transcribed from mono packages/zero-protocol @ 1.7
  zsync-core       the engine, port of sync-server.ts + soot hardening:
                   SyncDb trait; per-table triggers (touched-pks-only change log);
                   watermark cookie; diff resolved against LIVE rows; snapshot
                   (clear+puts) as the single recovery path; LMID + markers;
                   two-tx app errors; retention floor + prune; epoch invalidate;
                   group→user claim (403); 400/401/403/409; byte/row caps with
                   last-INCLUDED-watermark cookie + cut at change-row boundary
                   BEFORE pk dedup; lmids from included log prefix; explicit
                   skip classifier, THROW on unmapped log tables
  zsync-host       host-agnostic request layer: parse/validate bodies, auth hook
                   trait, mutator-session trait, admin surface for the harness
                   (/admin/sql, /admin/health, /admin/status {bootID})
  zsyncd           native binary: axum + rusqlite (WAL), one db per namespace,
                   /<ns>/pull|push routing, config file, harness spawn-friendly
  zsync-cf         workers-rs DO: SyncDb over ctx.storage.sql, transactionSync
                   extern binding, per-namespace DO routing (mirrors
                   harness/cf/worker.ts incl. the deterministic idle-teardown
                   eviction simulation + bootID)
```

Correctness details pinned by the reference core that the port must preserve:

- floats: serde_json (ryu) is shortest-roundtrip like JS — but patch values
  come from LIVE row reads, never logged images, so fidelity is structural
- sqlite→zero value conversion (`toZeroValue`): booleans 0/1→bool, json
  text→parsed, numbers exact; integers may exceed 2^53 — serde_json::Number
  preserves i64, document the client-side JS ceiling
- positional `?` bindings only (DO SqlStorage has no ?N)
- cookies never regress across restart/eviction (durable watermark =
  max(state, MAX(log)); the eviction lane asserts this through SIGKILL and
  DO teardown)
- unchanged/409/claim semantics byte-compatible with the vendored transport
  (`harness/src/vendor/httpPullTransport.ts` is the wire truth)

Auth: hook trait with two impls — harness token convention (`token-<userID>`),
and a signed-claims token (HMAC, `{userID, exp}`, pure-Rust crypto so it works
on wasm) that the app issues; soot/chat resolve better-auth sessions app-side
and mint the short-lived token. No auth webhook from the Rust server (keeps
pulls zero-round-trip).

## Milestones (each = a harness lane green; run every lane on BOTH hosts)

M0 walking skeleton: workspace, zsync-core over rusqlite passing ~5 ported
delta tests, zsyncd serving /pull /push /admin/\*, harness target
`rust-local` (copy of orez-local-process target: spawn hook + base URL),
smoke lane green (20 clients).
M1 full port: all 19 delta-suite tests as Rust unit tests + the soot
composition semantics (caps, prefix lmids, skip/throw) with tests ported
from soot's 13-test suite; shapes lane (22 query shapes differential vs
stock zero-cache) + seeded sweep green. CI: add cargo build/test + the
rust-local smoke/shapes/sweep jobs to .github/workflows/ci.yml (a Rust
toolchain job already exists there).
M2 CF host: zsync-cf deployed as a zharness-style worker (lslcf account),
harness target `rust-cf`; smoke + shapes green against the DO; eviction
lane green (bootID changes, zero 409s, monotone cookies).
M3 behavior lanes: permissions (visible() forces snapshot), reconnect +
persisted storage (409 reset path, lost-push recovery, floor fallback),
multi-tab client groups.
M4 mutator session protocol: implement the app-executes-mutators channel;
new harness lane that runs fixture mutators through a sidecar bun app
server over the channel (this surface is new and needs its own gate);
native built-in mutators behind the same trait for existing lanes.
M5 load: storm lane (100 clients converging) + bench on both hosts; soak +
scaling run on mini-16 (nightly.sh gains rust targets). Targets: local
ack p50 < 3ms (beat TS), CF parity with TS DO numbers, flat memory.
M6 consumer integration (each behind a flag, no publish without approval):
soot — point the project-plane/control endpoints at the Rust DO beside
the existing path, run soot's validate-cf-do-runtime + chat e2e, then the
flip; chat — measure per-server namespace table sizes/bytes first (same
measurement soot ran; chat has message-heavy namespaces), decide the
bounding policy at that gate (per-table windows + epoch aging as default;
server-evaluated named queries only if windows measurably fail), then the
on-zero http-pull transport flip in chat's client.
M7 deletion follows in soot/chat per their phase-3 plans (embed, pg-facade
on the sync path). Not this repo's milestone, listed for the arc.

## Risks / open questions (flagged, with my picks)

1. Mutator channel design (M4) is the one genuinely new protocol surface —
   my pick is the interactive tx-scoped statement channel above; the
   alternative (batch statements, no interactive reads) is simpler but can't
   express read-then-write mutators, which both apps have.
2. Chat bounding: unknown until measured; the decision gate is in M6 and the
   answer may add a per-table window policy to zsync-core (row-local
   predicates only, aged out via epoch snapshots).
3. workers-rs maturity (0.8.x): we own a transactionSync binding; wasm binary
   size and JsValue conversion overhead are watchpoints, measured in M2.
4. If the ws/IVM surface is ever needed, zero-wire already carries the types;
   that becomes its own planned project (a Rust ZQL/IVM engine), not scope
   creep here.

## Verification

The harness IS the verification: every milestone above names its lane, lanes
run against both hosts, CI runs smoke/shapes/sweep on rust-local per push,
nightly runs the full set + bench on mini-16. Rust unit tests port the delta
suite so semantics are pinned twice (unit + differential). M6 additionally
runs soot's production validators before any flip. No release/publish of
anything without explicit approval.
