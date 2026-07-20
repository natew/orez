# Orez sync consolidation resume

This branch is based on `origin/main` at `6b6ae2a`. It implements stage 1 of
`/Users/n8/soot/plans/orez-sync-consolidation-2026-07-19.md`. The constraints in
that plan still apply: one cutover, no compatibility shims, no dual execution
paths, no ledger migration, and only light new tests. Zero is pinned to the
strict `>=1.7.0` boundary requested by the owner.

## Done

- Added `packages/sync-executor` and the `orez/sync-executor` export. The
  executor owns async application database transactions, registered mutation
  dispatch, CRUD SQL, `_zsync_clients` and `_zsync_changes`, client-group
  ownership, sequential LMIDs, replay acknowledgement, application-error LMID
  settlement, direct transactions, ZQL reads, and deferred effects.
- Implemented SQLite and PostgreSQL application database adapters. PostgreSQL
  claims are serialized with an advisory transaction lock and the client row is
  read `FOR UPDATE`.
- Implemented Zero's insert contract in `packages/sync-executor/src/crud.ts`:
  `ON CONFLICT (<primary key>) DO NOTHING`. The executor test proves a duplicate
  row A does not abort the later insert of row B and advances the LMID once.
- Rebound `packages/sync-cf-host` and `packages/sync-browser-host` to the shared
  executor for local mutation execution and direct application transactions.
  Both hosts now require an `authorize` hook after authentication. Delegated CF
  push remains a separate deployment mode and does not duplicate local
  execution semantics.
- Moved shared registry, transaction, claims, effect, error, CRUD metadata, and
  visibility types to `orez-sync-executor`. Removed their host-local copies.
- Added `src/zero-http/mount.ts`, its push/replay/pull round-trip test, and an
  executor-backed `src/zero-http/server.ts` fixture. The Rust differential
  oracle and all existing harness targets now use the executor-backed mount.
- Deleted the old TypeScript semantics implementation and tests:
  `src/sync-server/sync-server.ts`, `src/sync-server/sync-server.test.ts`, and
  `src/sync-server/sync-server-mount.test.ts`. Removed the root `./sync-server`
  export with no re-export shim.
- Deleted the CF-local effect and mutation-error modules and tests:
  `packages/sync-cf-host/src/post-commit.ts`, `post-commit.test.mjs`,
  `src/mutation-error.ts`, and `mutation-error.test.mjs`.
- Updated current sync documentation and the release package set for the new
  executor package. Regenerated `bun.lock` with `@rocicorp/zero` 1.7.0.
- Reconciled unpublished overlap commit `b93d35c`: the branch exposes exact
  browser snapshot deletion by storage key and preserves sibling snapshots.
  Executor CRUD deletes by the complete primary key, the mount emits the exact
  physical deletion ID, and the fixture server delegates deletes through that
  single executor path. The existing light tests now preserve a sibling row
  while deleting the target.

## In progress

There is no intentionally half-written source implementation. These files are
coherent but still need final broad validation on the faster machine:

- `packages/sync-executor/src/*.ts`: executor, adapters, transaction builder,
  CRUD, effects, and light tests compile and pass targeted tests. Run the full
  repository suite to exercise them under every host.
- `packages/sync-cf-host/src/host.ts`, `src/config.ts`, `src/types.ts`, and the
  changed CF fixtures: the executor and authorization cutover passed typecheck
  and targeted host tests, but the complete CF workerd suite still needs a
  terminal run.
- `packages/sync-browser-host/src/host.ts`, `src/types.ts`, and changed browser
  fixtures: the executor and authorization cutover passed typecheck and
  targeted unit coverage, but the complete browser suite still needs a
  terminal run.
- `src/zero-http/mount.ts`, `src/zero-http/server.ts`,
  `harness/src/executor-host.ts`, the changed harness targets, and
  `crates/sync-core/ts-oracle/run-oracle.ts`: targeted mount/server tests,
  harness tests, and the Rust differential passed. The interrupted full suite
  must be the final confirmation.
- `docs/sync/*.md`: documentation-only edits were made after the last complete
  `bun run check`; they need only formatting validation.

## Not started

- Stage 3 and every Soot source change are deliberately untouched.
- Ledger migration is deliberately not implemented because the controlling
  plan forbids it and allows connected owner-test clients to reset.
- The design document's large migration, staging, and provider-retry test
  matrix was deliberately not ported. The controlling plan requests only the
  three light tests: insert convergence, executor replay idempotency, and one
  pull/push mount round trip.
- No package release or publish was attempted. Package versions remain 0.5.30;
  the downstream branch requires the next stage-1 version `>=0.5.31`.

## Test state

- `bun run format:check` and `bun run check` passed. Logs:
  `/tmp/orez-format-check.log` and `/tmp/orez-check.log`.
- Executor tests passed: 5 tests. Log:
  `/tmp/orez-sync-executor-tests.log` (the latest direct run also passed).
- `src/zero-http/mount.test.ts` and `src/zero-http/server.test.ts` passed: 8
  tests total after the final mount refactor.
- The restored harness suite passed: 34 tests. The Rust differential passed 2
  tests with 1 intentionally ignored. Log: `/tmp/orez-differential.log`.
- Targeted CF, browser, and root CF DO tests passed before the full-suite run.
- `bun run test:all > /tmp/orez-suite.log 2>&1` reached a terminal result on the
  faster machine: 1,187 passed, four skipped, and one integration test timed
  out waiting for a replication poke while load peaked near the machine limit.
  The Node 25 native SQLite binding was rebuilt first and is loadable. Re-run
  the full suite at calm load to prove this is a load timeout.
- The requested `bun heavy -- <cmd>` wrapper does not exist as a package script
  or executable in this checkout. Heavy commands were run directly and logged.

## Precise next steps

1. Read both controlling plan files before changing code. Fetch origin and
   check whether `b93d35c` is now available before resolving deletion conflicts.
2. Install dependencies, ensure `wasm-pack` is available, run
   `make -C sqlite-wasm dist/package.json`, and confirm the native SQLite module
   loads under the active Node version. Use the faster machine's heavy-command
   wrapper if it has one.
3. Run `bun run format:check`, `bun run check`,
   `bun run test:all > /tmp/orez-suite.log 2>&1`,
   `bun run test:sync-browser-host > /tmp/orez-browser-host.log 2>&1`, and
   `bun run test:sync-cf-host > /tmp/orez-cf-host.log 2>&1`. Do not pipe any
   suite through output filters.
4. Fix only demonstrated failures at their source. Re-run the failed full
   command to a terminal result and preserve the complete logs.
5. Build or pack the unreleased executor and use it to regenerate and validate
   the downstream Takeout lockfile. Do not publish without direct owner
   permission.
