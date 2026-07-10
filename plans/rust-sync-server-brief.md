# brief: rust zero sync server — full rewrite context

audience: the agent planning the Rust implementation (ab-mreav3k5-26179).
author: the session that built the TS reference core + conformance harness +
soot production endpoint (2026-07-09). everything below is what you need to
write the implementation plan without re-learning what this stack already
learned. nate's directive: a full-on Rust implementation of the zero sync
server. deliverable of YOUR first pass: an implementation plan, not code.

## 1. what "zero sync server" means here, and the scope decision you own

zero (rocicorp, ~/github/mono, npm @rocicorp/zero, now **1.7 stable**) splits
into a client (ZQL/IVM query engine, local store, optimistic mutations —
battle-tested, stays THEIRS) and zero-cache, the server. the server's jobs:
replicate upstream data, feed row changes to clients, execute custom
mutators server-side (PushProcessor), track per-client lastMutationIDs, and
serve per-query incremental sync (CVR) over a websocket poke protocol.

we already replaced the server's sync half in TypeScript for the SMALLER
surface: on-zero's **http-pull transport** (a fake-WebSocket client transport
that turns sync into stateless HTTP polls). that surface is:

- `POST {base}/pull` `{clientID, clientGroupID, cookie: number|null}` →
  `{cookie, unchanged: true}` | `{cookie, lastMutationIDChanges: {clientID:
lmid}, rowsPatch: [{op:'clear'} | {op:'put', tableName, value} |
{op:'del', tableName, id}]}` — ops are zero protocol-v51 poke rowsPatch
  shapes passed through verbatim to the client.
- `POST {base}/push` = zero PushProcessor body verbatim (custom mutators) →
  `{pushResponse}`. LMID bookkeeping in `<appID>_0.clients`.
- errors: 400 malformed (cookie must be null or non-negative safe integer),
  401/403 auth/ownership (client surfaces Unauthorized and stops), 409
  future-cookie (client resets its local store — the reset path is cheap and
  correct, used deliberately).

**your first scope decision**: (a) Rust server for the http-pull surface —
smallest, the conformance harness gates it TODAY, drop-in behind any HTTP
host; (b) the real zero-cache websocket surface (`/sync/v*/connect`, pokes,
CVR, per-query sync, desiredQueriesPatch) — the true full rewrite, much
bigger (you own connection state, per-client-group view bookkeeping, and the
poke protocol), and the harness's differential lanes still gate it because
they drive real clients end to end. recommended: plan (b) as the goal with
(a) as the first shippable milestone — (a) is where all our semantics are
already pinned executable, and it forces the storage/log/watermark core
you'll need for (b) anyway. note rocicorp prototypes their engine internally
in Rust (`rusty-ivm`, private; only archived `repc` is public) — their TS
fuzzer in mono was ported FROM it (driver header in
`packages/zql-integration-tests/src/chinook/fuzz/`).

## 2. the assets you inherit (read these before planning)

- **TS reference core (executable spec)**: `~/orez/src/sync-server/
sync-server.ts` (~500 lines) + `sync-server.test.ts` (19-test delta
  suite). implements: change log of touched pks (watermark autoincrement),
  cursor-diff pulls resolved against LIVE rows, snapshot recovery
  (clear+puts) as the single recovery path, marker rows so LMID-only
  advances move the cookie, two-transaction app-error handling, retention
  pruning + floor, epoch invalidation, client-group ownership. port the
  delta suite's cases as your Rust unit tests — they encode the semantics.
- **production composition**: `~/soot/src/zero/httpPullProject.server.ts`
  (three adversarial review rounds, 13-test suite) — the real-world layer:
  per-user row visibility, byte caps + completeness rule, ordering-based
  consistency when the storage gives you no cross-statement transactions,
  log-derived lmids, legacy/control table classification, membership auth.
  `~/soot/src/zero/httpPull.server.ts` is the simpler control-plane
  (snapshot-only) shape. design doc with every decision + revision history:
  `~/soot/plans/sootbean/zero/zero-http-project-plane.md`.
- **cursor primitive**: `~/orez/src/cf-do/cursor-pull.ts` (+ tests) — the
  table-identity contract (schema-qualified log names, explicit skip
  classifier, THROW on unmapped — silent drops are permanent divergence).
- **the wire truth**: on-zero's transport, vendored at
  `~/orez/harness/src/vendor/httpPullTransport.ts` — read it to see exactly
  what a real client sends/expects, incl. cookie stringification, auth
  frames, and how rowsPatch becomes a v51 pokePart.
- **the conformance harness (your development loop)**: `~/orez/harness/` —
  drives REAL zero clients differentially against stock zero-cache and any
  http-pull target. targets are pluggable (`harness/targets/*.ts` — a Rust
  server is a new target = base URL + spawn hook). lanes: smoke, 22 query
  shapes, seeded randomized sweep, permissions/visibility, reconnect +
  persisted storage, multi-tab client groups, 100-client storm, process
  restart + DO hibernation (eviction). CI runs them on orez main. results
  to beat (TS core): local ack p50 3ms; CF (in a DO) ack p50 174-184ms,
  propagation p95 1.6s at 1s polls, 100 clients converging.
- **the original Rust harness proposal**: `~/orez/plans/
zero-conformance-harness.md` § "proposal: zbench" — a clean-room
  protocol-v51 Rust client design (websocket connect, initConnection,
  desiredQueriesPatch, poke application). nate deferred it for the harness;
  it's directly reusable as the protocol-crate skeleton for scope (b), and
  as a Rust load generator later.
- **plans**: `~/orez/plans/zero-server-rewrite.md` (phases + landing
  status), review docs from the codex worker in the worktree
  (`~/.worktrees/orez-zero-sync-server/plans/
review-zero-sync-server-2026-07-09.md`).

## 3. hard-won invariants — do not re-learn these in production

1. **never ship logged row images as patch values.** log touched PKS only;
   resolve puts against live rows at pull time. (original sin: sqlite
   json_object formats REAL at 15 significant digits — 0.1+0.2 came back
   "0.3"; floats must round-trip exactly or clients diverge silently.)
2. **cookies under-report, patches are idempotent.** the returned cookie
   must never exceed the state actually shipped. derive it ONLY from reads
   taken BEFORE the change scan; read the retention floor AFTER the scan
   (purge racing the read then forces snapshot instead of silently missing
   rows). every race becomes benign re-delivery.
3. **an ack must never precede its effects.** lastMutationIDChanges for a
   diff response must come from the included log prefix (the clients-table
   rows ride the log in watermark order), not a separate later read —
   otherwise a capped/racing pull confirms a mutation whose rows aren't in
   the patch and the client drops its optimistic layer early.
4. **LMID-only advances must move the cookie** or mutation recovery never
   settles (recovery settles ONLY via lastMutationIDChanges in a
   NON-unchanged pull). reference core: marker rows; soot prod: the shard
   clients table is itself change-tracked.
5. **retention is a prefix-purge model**: live rows form a suffix; diff is
   servable iff cookie ≥ MIN(watermark)-1 (with rows pending), else
   snapshot. under row/byte caps, return the last INCLUDED watermark —
   never a global max over a truncated read — and cut at a change-row
   boundary BEFORE pk dedup.
6. **watermarks/cookies must never regress** across process restarts,
   crashes, and (on CF) DO eviction/hibernation — durable watermark state,
   max(state, MAX(log), sequence). the eviction lane asserts monotone
   cookies through SIGKILL and hibernation; 409 is the reset escape hatch.
7. **app-error rollback is two transactions**: tx1 (whole mutation) aborts
   wholly; tx2 advances the LMID and carries the error result. replay of an
   already-applied mutation is a no-op ack; out-of-order is a 400.
8. **per-user visibility predicates must be row-local** (own columns only)
   wherever cursor diffs serve them — a cross-table predicate flips without
   the row being touched and diffs cannot see it. non-row-local tables must
   either be empty on the diff plane or force snapshot.
9. **single-writer storage deletes whole concurrency classes** — the TS
   core assumes sqlite one-writer semantics. if your Rust storage is
   in-process sqlite, keep that; if you target anything multi-writer,
   re-derive every ordering argument above.
10. **fail loud on unknown log tables** (throw, never skip silently); keep
    explicit classified skip sets for internal/legacy tables (real prod
    logs carry rows for tables DROPPED from the schema — lazily-migrated
    namespaces).

## 4. deployment-target question (shapes the whole plan)

the TS core runs (1) in-process on node/bun over any sqlite, (2) inside a
CF Durable Object over `ctx.storage.sql`. for Rust decide early:

- **native binary** (dedicated box / OVH / fly): rusqlite, easiest and
  fastest; the harness local lanes point at any HTTP base URL already.
  nate has an OVH account + an isolated mac mini (agentbus peer `mini-16`)
  for big runs.
- **cloudflare**: Rust→wasm workers exist (workers-rs) but DO SqlStorage
  bindings from wasm are awkward and orez's whole pg-facade/DoBackend moat
  is TS — a Rust CF story is a real design problem, not a checkbox. it is
  FINE for the Rust server to be native-first: the conformance harness
  treats targets uniformly.
- protocol version: pin against zero **1.7 stable** (the codex worker
  ab-mre5wzth-59453 is upgrading the harness client pin to 1.7 right now;
  1.7 passed every HTTP/local lane so far). protocol schemas live in mono's
  `packages/zero-protocol` (zod) — transcribe, don't guess.

## 5. operational context (don't collide)

- the TS landing is IN FLIGHT: orez 0.4.46 on npm carries the reference
  core + harness (orez main); soot main carries the production endpoint +
  a `?projectHttpPull=1` rollout flag, deploying to production now, then
  coexistence measurement → default flip → phase 3 deletes the
  project-namespace zero-cache embed. your work is greenfield beside this.
- the orez WORKTREE `~/.worktrees/orez-zero-sync-server` belongs to the
  codex worker (ab-mre5wzth-59453, harness owner). base your reading on
  **orez main (v0.4.46)**. coordinate via agentbus; the worker is the
  person to ask about harness targets/lanes.
- soot main + ~/soot are shared with active co-tenants; you shouldn't need
  to touch soot at all.
- history, for the record: nate proposed Rust on day one; it was deferred
  ("DECIDED" header in zero-conformance-harness.md) in favor of shipping
  the TS harness + core first — that decision is what makes YOUR rewrite
  safe to attempt now, because conformance is a command, not a hope.

## 6. suggested plan skeleton (yours to overrule)

1. read: reference core + delta suite, soot endpoint + design doc, vendored
   transport, harness targets, zbench section, mono zero-protocol 1.7.
2. decide scope (a)-first vs (b)-direct and the storage/deploy target;
   write the plan with milestones gated on harness lanes (each lane green =
   a milestone, same as the TS build did: smoke → shapes → sweep →
   permissions → reconnect → storm → eviction).
3. crate sketch (from zbench + this build): `zero-wire` (protocol types,
   transcribed from zero-protocol 1.7), `sync-core` (change log, watermark,
   diff/snapshot, lmid, retention — port the delta suite), `sync-http`
   (axum/hyper host, auth hooks), later `zero-ws` for scope (b).
4. budget a differential soak + storm run on mini-16 before calling any
   milestone done. "it should work" is not verification here — the TS build
   found real bugs in EVERY adversarial review round; assume yours will too.
