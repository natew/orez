This file is the repo's agent contract. `.claude/claude.md` is a symlink to it,
so Claude and Codex read the same source. Edit this file, never the symlink.

DO NOT PUBLUSH without permission!

COMMIT before you publish!

ALWAYS merge finished work to main and push (pull first). Standing
permission from nate (2026-07-16): main pushes do not need re-asking.
Publishing/releasing still ALWAYS needs explicit permission.

Worktrees go under `~/.worktrees/orez-<slug>`, never in `/tmp` or inside the
repo. Remove yours (`git worktree remove <path>`) when the task ends, or leave
it clean with every commit pushed to its branch. Uncommitted work in a worktree
at session end is lost work: commit and push, as `wip:` if unfinished. Managers
prune any worktree with no live owner, a clean tree, and a pushed HEAD without
asking; `tm drift` is the audit.

USE CONVENTIONAL COMMITS

Lint and format are SEPARATE CI gates. `bun run lint` (oxlint) passing does not
mean `bun run format:check` (oxfmt) passes, and CI runs both in the `test` job.
Run `bun run format:check` before pushing, or CI goes red on whitespace after a
green suite. This has already cost one red main push.

the ONLY way to publish is:

`bun release --patch --ci`

IF YOU JUST TESTED USE `--skip-test`

## Cloudflare reads and writes are a billing contract (owner rule, 2026-08-03)

Treat every added read and write on a Durable Object path as a cost decision
before it is a correctness decision. Amplification here has caused real
incidents and it gets expensive fast.

Cloudflare bills DO SQLite per physical row. One logical write costs
`1 + N_indexes` billable rows plus the change-tracking rows sync appends, so
the billed number is always well above the application row count. Measured:
about 1.3k billable rows for a single push, and 127.5k for one cascading
account delete. A cold start once hit 1,191,374, of which 1,077,552 were
transaction snapshots of a growing internal table.

Before adding a query, a probe, a retry, or a table to a DO path, answer:

- Is this a point lookup, or does it scan? A probe that reads every membership
  row to answer one question is the exact shape that has bitten us. Recent
  fixes: `8ab7ab6`, `be2e7e8`.
- Does an idle or caught-up operation write anything? It must write nothing.
  A retrying client multiplies any per-attempt write by its retry count.
- Does this run per-row inside a loop, or once per transaction? Pack the batch
  (`packages/sync-executor/src/packed-ledger.ts` writes a whole transaction as
  one payload).
- Does a failure path re-read or re-snapshot? Recovery code amplifies the
  incident it is meant to contain.

Pin the cost in a test, not a comment. `crates/sync-core/tests/pull_write_cost.rs`
is the pattern: meter with `total_changes()` and assert the row count directly.
Raising a circuit-breaker budget to make something pass hides amplification
instead of fixing it.

Two different counters, do not mix them up. `total_changes()` counts
trigger-written rows and is what billing means. `changes()` excludes them and
is what "did my statement match its target row" means, but it holds a stale
value through a read.

Full detail, including the circuit breakers, lives in `docs/sync/trade-offs.md`.
Read it before changing anything on a DO write path.

PACKAGE EXPORTS (owner rule, 2026-07-27): implementation may live in
sub-packages (orez-sync-executor, orez-sync-cf-host), but the CANONICAL import
path for consumers is always the parent package (`orez-lite/realtime`, not
`orez-sync-executor/realtime`). Published consumers (on-zero, apps) and docs
use the parent path only. Direct sub-package imports are allowed solely inside
this repo's own sub-packages and the harness where importing through the
parent would create a package cycle or an unbuilt-dist resolution problem.
