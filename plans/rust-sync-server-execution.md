# rust sync server: execution tracker

Branch: `rust-sync-server`, worktree `~/.worktrees/orez-rust-sync`.
Spec: [rust-sync-server-final-plan.md](./rust-sync-server-final-plan.md).
Coordinator: the fable session that owns this file (agentbus scope orez).

Multiple agents work this worktree concurrently. Rules:

- Own your paths (table below). Do not edit another track's files; if you
  need a change there, message the owner or the coordinator on agentbus.
- Commit with explicit pathspecs only (`git add <files> && git commit -m
"..." -- <files>`). Conventional commits. Never `git add -A`, never
  stash, never amend, never reset.
- `crates/sync-core`'s public API (esp. `db.rs` SyncDb/SqlValue/Row) is
  shared by all tracks. M1 owns it; announce any signature change on
  agentbus to the M0 and M2 owners before committing it.
- No publishing, no deploys to shared infra without the coordinator
  relaying explicit user approval. The lslcf test worker deploys for M3
  probes are allowed (harness already deploys there).

## Ownership

| Track                 | Paths                                                  | Owner       |
| --------------------- | ------------------------------------------------------ | ----------- |
| M0 platform proof     | `crates/sync-wasm`, `packages/sync-cf-host`, `probes/` | sol-m0      |
| M1 core port          | `crates/sync-core`                                     | opus-m1     |
| M2 native + harness   | `crates/sync-native`, `harness/`                       | opus-m2     |
| Workspace root, plans | `Cargo.toml`, `plans/`                                 | coordinator |

## Status

- [x] worktree + cargo workspace skeleton (coordinator)
- [x] M0: platform contract proof (workerd DO host, transactionSync +
      async-tx probes, real-shape mutators, value round trips, rollback,
      eviction, size/latency measurements)
- [x] M1: sync-core port of the reference cursor spec (19 delta tests,
      soot 13-test composition semantics, model tests, trace differential)
- [x] M2: sync-native axum host + harness `rust-local` target + admin
      routes + wake channel + CI lanes
- [x] M3: sync-cf-host production shape + `rust-cf` target + hibernating
      wake sockets (exit gate 2026-07-09: full lane matrix vs rust-cf,
      eviction zero 409s + monotone cookies, 100-client wake-only storm
      p95 810 ms < 1 s, propagation commit->seen p95 406 ms without
      safety-poll convergence, ack/prop within 20% of the TS DO baseline;
      deploy 871a13df, README 4c2a3bc)
- [~] M4a: soot migration prep (baseline surface) — auth/namespace
  adapter, DO-local mutator adapter with post-commit outbox, shared
  visibility fragments (fixed a repeated-`?` param bug in the legacy
  endpoint), workerd control/project/wake tests, cutover/rollback
  scripts (not executed). Offline executable comparison green (legacy
  cursor/snapshot/ack 17 pass, Rust workerd 8 pass). Production cutover
  remains user-gated. UPDATE 2026-07-10: soot now consumes orez's
  createSyncServerMount for routing + dispatch on soot branch
  soot-mount-consume (b50df8aa87, not pushed): mount routing verified
  byte-identical to projectIdFromPathname across 13 edge cases, one
  stable per-project SyncServer-shaped adapter delegating to the
  unchanged cursorPull/snapshot, push kept on a thin request-aware seam
  (rate limit + withPushParams need the raw Request; adapter handlePush
  fails loud), 17/17 soot tests green with ZERO expectation rewrites.
  Key cutover finding baked into that commit: soot's project plane spans
  THREE cookie domains (zero-cache embed watermark / node WAL LSN / orez
  \_zsync_changes) — a full createSyncServer replacement is the cookie-
  domain break itself and must ride the user-gated cutover, not prep.
  Branch caveat: requires an orez dist overlay (mount not on npm) until
  a release is approved.
  DEPLOYED CONFORMANCE CLOSED 2026-07-10: soot's OWN composition
  (createSyncWorker(sootConfig)) deployed as a test worker on lslcf
  (soot-rust-sync-prep, version 6579f13a, 278 KiB gzip, 29 ms start; the
  lslcf account, never soot's) and the new committed harness lane
  soot-deployed-conformance (dc69387) passed 20/20 against it:
  control-plane own-row visibility + cross-user write denial, project
  plane attachCommand/projectAddon visibility, cross-table mutators
  (snapshot + message.sendMainBean), app-error and access-deny paths,
  and immediate wake propagation with pusher excluded. The generic
  fixture drill lanes cannot target soot's composition (different auth
  header, mutator registry, and schema — curl-confirmed); host-level
  eviction/retention/replay mechanics are covered by the M6 lanes on the
  identical host code, with soot-specific semantics + wake re-proven by
  the deployed lane. One pre-existing soot-side stale unit assertion
  noted ({sql:'FALSE'} vs {sql:'0'}, both deny predicates; runtime
  verified). What remains for M4a is exactly the user-gated production
  canary/cutover.
- [x] M4b: query-aware layer (AST compiler, membership, desired queries).
      GATE CLOSED 2026-07-10 (cross-model reviewer APPROVED at e306bda).
      Engine + transport + lifecycle lanes green vs rust-local and rust-cf
      (v51 AST subset incl. junction EXISTS, nested related, ILIKE folding,
      empty IN, composite tie-breakers; recomputation narrowing measured
      12x dependency-intersection and 11x touched-pk; forbidden-row
      raw-store, overlap retention, permission contraction, reconnect
      replay, lost-response). Three adversarial review rounds
      (plans/rust-sync-review-findings-2026-07-09.md) fixed 8 original
      defects (two criticals: client raw-AST permission bypass, global
      query-hash cross-group collision), then 5 round-2 gaps, then 2
      residuals — every fix with a regression test:
  - nested per-parent bounds COMPILE via ROW_NUMBER windowing (a840443,
    all 22 query-diff shapes green vs rust-local and rust-cf, 15b8be8)
  - start-cursor keeps the full effective ordering key incl. implicit PK
    tie-breaks and ignores non-ordering full-row fields (675df77,
    1d293bb); null-aware keyset comparisons match stock makeComparator
  - collision-safe ROW_NUMBER rank alias, ASCII-case-insensitive
    (12c1225, e306bda)
  - query-schema forward migration bumps the epoch + version-checked,
    fail-loud on a future version (6235344, 3efedee)
  - unknown named query 400 (8de39d1); CI runs cargo test --workspace +
    a sync-cf-host job (d0753a8; immediately caught a latent broken
    assertion, 8c17d03)
    Coordinator also root-caused the intermittent rust-cf query-diff stall
    to volatile admin knobs in the CF host (durable \_zsync_host_control +
    workerd restart regression lane, 002def5), and a sticky before-commit
    fault that rolled back its own one-shot consume inside the aborted
    transaction (50c6860).
- [x] M4c: chat compatibility branch — chatConfig fully implemented
      (schema/permissions/resolveQuery/initialize/authenticate/namespace/
      mutators) against the real SyncHostConfig; DO-local mutator adapter
      reusing soot's M4a on-zero pattern, runtime-validated over wrangler
      dev (owner insert lands, cross-user write denied through the adapter,
      external effects fail closed), permanent regression chat-host/test/
      mutators.test.ts, permission lane 24/24 with the full mutator graph
      bundled (chat worktree 94012c0b6). Remaining resolveQuery corpus (124
      named queries) + more permission families are incremental, lane-driven.
- [x] M5/M6 gates: observability mirrored in both hosts, rollback/canary +
      one-writer scripts, fault/soak/fuzz/memory/backup lanes with budgets,
      incident runbook. QUALIFICATION CLOSED 2026-07-10: full native (7/7)
      and CF (9/9) suites re-run green on a trusted model at 011bf2d /
      deploy 3762dad8, replacing the retracted small-model passes
      (per-lane evidence in plans/rust-sync-m6-qualification.md, 54c48bd);
      coordinator independently reproduced the rollback/one-writer drill
      PASS against the same deploy. NOTE: the M5/M6 harness/host commits
      authored while the sol lane was silently downgraded to gpt-5.6-luna
      were audited; one real bug fixed (50c6860), the rest sound. The
      first CF suite pass claim was retracted when a tee-masked exit code
      was found; the rerun (with per-case fuzz error context, f08ac4c)
      is the pass of record. Production cutover for soot/chat remains
      user-gated and is NOT part of this gate.

- [x] Chat testbed integration (user-cleared, 2026-07-10): the real Chat app
      runs end-to-end against the rust chat-host — browser Zero client with the
      better-auth bearer bridge, CORS, pull/push/wake all green; sustained
      message.send load at ~346 msg/s with 0 failures; query-aware
      channelMessages history load 47 ms for 800 messages. Six integration bugs
      found and fixed on rust-sync-compat (auth timing, bearer bridge,
      websocket-close DO crash — fixed in packages/sync-cf-host on this branch,
      CORS, session-cache negative poisoning, admin-email elevation) plus a
      dev/test config collision. resolveQuery corpus COMPLETE: 123/124 named
      queries registered as per-query builders composed with composable
      permission families (exploreTable deliberately unregistered — runtime
      table name, fail-loud); 285 tests green including a ~250-case allow/deny
      oracle lane and a completeness assertion that fails on any un-transcribed
      upstream query; fixed a latent channelReadable admin-correlation bug and
      added the faithful admin read bypass. devtools PII queries
      (waitlist/privateBetaInvite) secured admin-only server-side even though
      ungated in source; dataSearch publicOnly stays open by design.
      PUSH-PATH OOM ROOT-CAUSED AND FIXED: workerd died at ~10k sustained
      pushes because on-zero's createMutators.withTimeoutGuard leaked one 60s
      setTimeout per completed mutation (never cleared after Promise.race),
      hitting workerd's 10,000 active-timer cap (V8 heap 30->102 MB, wasm
      flat). Fixed at source in takeout d942e301 (clearTimeout in finally,
      regression tests, on-zero 147 tests green — NOT published; user gates
      releases); chat's client-side sibling (src/zero/mutate.ts 90s timer)
      fixed on rust-sync-compat e1dfaa7d6; soot already cleared its timers.
      After the fix two independent 12k real-Chat runs completed 12,000/12,000
      with zero failures and flat heap. New harness push-memory-soak lane +
      JS-heap gauge in /admin/status wired into CI and the M6 qualification
      doc (d133d83) so this class regresses loudly.
      Production cutover NOT touched.

Keep this checklist current when a track lands its exit gate.

## Recorded measurements

M0 (workerd + deployed lslcf probe, 2026-07-09): wasm bundle 83.41 KiB
gzip (97.3% below the 3 MiB limit); deployed startup 1 ms; local cold DO
p50/p95 2.059/2.624 ms; async tx p50/p95 3/3 ms. Counter wire decision:
i64 internal, decimal strings at the wasm/JS boundary, JSON numbers on the
HTTP wire (byte-compat with the vendored transport).

M2 native lanes vs `rust-local` (all required lanes green, 2026-07-09):
smoke, shapes (22-query differential vs stock-zero, 0 divergences), sweep
(seed 1623147715, 12 rounds), permissions, reconnect, multi-tab, eviction
(SIGKILL, cookies monotone, zero 409s), storm (20 clients), bench,
propagation (wake-driven, no safety-poll convergence). Native ack p50 1 ms
(gate <= 3 ms), p95 3 ms; wake propagation p95 12 ms (gate < 100 ms);
differential vs stock zero-cache websocket push p95 391 ms.

Independent replication on mini-16 (10 cores / 16 GB, 2026-07-09):
smoke (20 clients, 791 ms), propagation (commit->seen p95 12 ms, no
safety-poll convergence), storm (100 clients: ack p50/p95 9/23 ms,
propagation p50/p95 70/84 ms), eviction (SIGKILL, outage 1563 ms, zero
409s, cookies monotone) — all PASS vs rust-local on a fresh checkout.

Mini-16 extended matrix at 726ac0c (2026-07-10, run headless over SSH
since mini's agentbus daemon cannot launch agents — bare PATH + a
deepseek-proxied claude): protocol fuzz 3 x 34,000 structural cases
(seeds 1/7/13), 102,000 total, every case 400, ~590 ms per seed batch;
storm 100 clients ack p50/p95/p99 9/17/17 ms, propagation 71/97/97 ms,
PASS; eviction 25/25 consecutive SIGKILL cycles PASS with outage
1562-1567 ms, converge 50-53 ms, zero 409s, cookies monotone every
cycle.

M6 qualification (SHA 011bf2d, deploy 3762dad8, 2026-07-10): NATIVE 7/7
in 43.4s — protocol fuzz 10,000 seeded structural cases all-400 in 137 ms
(seeds 1/2/3/42); eviction SIGKILL outage 1559 ms, 30 writes, converge
53 ms, zero 409s, cookies monotone; reconnect 4 phases; multi-tab;
clock-skew ±24h LMID 2; storage-faults 5 boundary points kill-durability
(pre-commit row absent after restart, post-commit survives);
backup-restore 91 rows -> 91 fresh-snapshot puts. CF 9/9 in 157.2s —
fuzz 10,000 all-400 in 68.9s; eviction boot-ID change, 20 writes, zero
409s, monotone, late converge 155 ms; reconnect; multi-tab; clock-skew
LMID 2; storage-faults 5 points error/quota; backup-restore 91/91;
wasm-memory-soak on a live instance (no restarts, cbb66bc): samples flat
at 1,572,864 bytes, growth 0/0/0 against the 65,536-byte page budget;
rollback/one-writer drill PASS with 0 invariant failures (independently
reproduced by the coordinator: phases old-only -> none -> new-only ->
none -> old-only, old watermark 4, new watermark 2, floors 0). Bundle
220.39 KiB gzip upload (92.8% headroom); local cold DO p50/p95
5.351/7.829 ms; ack p50/p95 1.797/3.127 ms; storage delta 8,192 bytes
per 50 pushes.

M3 production host (orez-rust-sync on lslcf, 2026-07-09 checkpoint):
bundle 121.58 KiB gzip; startup 1 ms; cold p50/p95 4.792/6.652 ms; ack
p50/p95 13.951/15.055 ms remote; storage 81,920 -> 90,112 bytes across 50
pushes; hibernating wake sockets + teardown probes green; deployed
integration 16/16. Full lane matrix vs `rust-cf` pending target
registration.

## CI status (branch rust-sync-server, run 29082143145 + fixes to e306bda)

Rust-specific lanes GREEN: `rust` (workspace build + cargo test
--workspace + fmt), `sync-cf-host` (config + workerd platform +
integration), `rust-local` (native host vs stock zero-cache), `compiler`,
`test` (format gate). Fixed this session: the `test` format gate (oxfmt
sweep 06ba5cd), the `rust` job missing bun for the differential ts-oracle
(32d88c9), and the harness/rust-local embedded-postgres `libicuuc.so.60`
loader failure — npm can't ship the soname symlinks, so stock-zero.ts now
recreates them (32d88c9).

BRANCH CI FULLY GREEN 2026-07-10: run 29086944710 at 9f054a9 (all jobs).
Three intermittent legacy-orez flakes were root-caused and deflaked on
this branch (all pre-existing test-timing issues, none introduced by or
related to the Rust hosts):

- `test` `tcp-replication`: the update-stream test used a single fixed
  1500 ms collection window; converted to the deadline-loop pattern the
  delete/multi-table tests already use (7e74d30, failed run 29086372162).
- `native-integration` `restore-live-stress`: after the SIGUSR1 reset the
  zero port can accept briefly and then drop during the replica resync;
  the after-reset reconnect window was widened 30s -> 120s within the
  360s describe budget (9f054a9, failed runs 29085214517 + 29086848167).
  Deeper root cause fixed in 2bfb3fc: the initial sync after a reset can
  transiently abort the change-streamer's schema-sync transaction (25P02
  cascade), and because the crash watcher ignores exits while
  resetInProgress, one bad boot left zero-cache down permanently. The
  reset path now retries the start up to 3 times (kill half-booted
  instance + clean partial replica between attempts). Green at 2bfb3fc:
  run 29088122120.
- `harness` `randomized sweep`: see below.

ROOT-CAUSED AND FIXED 2026-07-10: the intermittent `harness`
`randomized sweep` failure (`Expected string at taskID. Got true`). It is
an upstream zero-cache 1.7.0 bug: when ZERO*TASK_ID is unset the runner
generates a nanoid and re-exports it to child workers via ZERO_TASK_ID;
the shared options parser converts env vars to argv (`--task-id <value>`),
and a nanoid that starts with `-` followed by letters is consumed by
command-line-args as an option token, leaving `--task-id` valueless
(boolean true) and failing the string schema. Reproduced deterministically
with `ZERO_TASK_ID='-abcdefg'` (crash) vs `-p123456` / `*-abc`(fine);
~0.03% of boots flake, which is why the same boot passed in the shapes
step and in two later full-green runs (29083578717, 29083748638). Fix:
the harness pins`ZERO_TASK_ID=zharness-stock-<port>` in stock-zero.ts so
the nanoid path never runs. (An earlier note here claimed this failure was
also red on main; that was wrong — on main the harness job died earlier at
the embedded-postgres libicu step, so the sweep never booted zero-cache.)
The bug is worth reporting upstream to rocicorp/mono.
