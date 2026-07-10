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
- [ ] M1: sync-core port of the reference cursor spec (19 delta tests,
      soot 13-test composition semantics, model tests, trace differential)
- [ ] M2: sync-native axum host + harness `rust-local` target + admin
      routes + wake channel + CI lanes
- [ ] M3: sync-cf-host production shape + `rust-cf` target + hibernating
      wake sockets
- [ ] M4a: soot migration prep (baseline surface)
- [ ] M4b: query-aware layer (AST compiler, membership, desired queries)
- [ ] M4c: chat compatibility branch (measurement)
- [ ] M5/M6 gates: see final plan

Keep this checklist current when a track lands its exit gate.
