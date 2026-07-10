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

| Track | Paths | Owner |
| --- | --- | --- |
| M0 platform proof | `crates/sync-wasm`, `packages/sync-cf-host`, `probes/` | sol-m0 |
| M1 core port | `crates/sync-core` | opus-m1 |
| M2 native + harness | `crates/sync-native`, `harness/` | opus-m2 |
| Workspace root, plans | `Cargo.toml`, `plans/` | coordinator |

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
      remains user-gated.
- [~] M4b: query-aware layer (AST compiler, membership, desired queries).
      Engine + transport + lifecycle lanes green vs rust-local and rust-cf
      (v51 AST subset incl. junction EXISTS, nested related, ILIKE folding,
      empty IN, composite tie-breakers; recomputation narrowing measured
      12x dependency-intersection and 11x touched-pk; forbidden-row
      raw-store, overlap retention, permission contraction, reconnect
      replay, lost-response). A cross-model adversarial review + re-verify
      (plans/rust-sync-review-findings-2026-07-09.md) fixed 8 defects
      including two criticals (client raw-AST permission bypass, global
      query-hash cross-group collision). REOPENED items before the gate is
      green: (1) nested related per-relation orderBy+limit must be
      SUPPORTED not rejected — Chat's windowed queries need it, and 5
      corpus shapes are currently skipped in query-diff; (2) forward
      migration for the _zsync_queries (clientGroupID,hash) rekey; (3) CI
      must run cargo test --workspace + a sync-cf-host test job; (4)
      unknown named query must return 400 not 500.
- [ ] M4c: chat compatibility branch (measurement)
- [ ] M5/M6 gates: see final plan

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

M3 production host (orez-rust-sync on lslcf, 2026-07-09 checkpoint):
bundle 121.58 KiB gzip; startup 1 ms; cold p50/p95 4.792/6.652 ms; ack
p50/p95 13.951/15.055 ms remote; storage 81,920 -> 90,112 bytes across 50
pushes; hibernating wake sockets + teardown probes green; deployed
integration 16/16. Full lane matrix vs `rust-cf` pending target
registration.
