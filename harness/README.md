# zharness

zero sync-engine conformance + load harness. plan and status:
`../plans/zero-conformance-harness.md`. never published; private tooling.

one harness, multiple targets (`src/target.ts`): stock zero-cache,
orez-local pure-sqlite, and orez-cf (cloudflare DO). every lane points stock
`@rocicorp/zero` clients at a target, writes both through sync and straight
to the upstream store, requires convergence, and compares converged client
state against a fresh oracle read of the authoritative store.

## run

```sh
bun install
bun run smoke                                      # 10 clients vs real zero-cache
bun src/smoke.ts --target orez-local --clients 50  # same vs the sqlite sync-server core
bun src/smoke.ts --target orez-cf --clients 5      # same vs the deployed CF DO
bun src/shapes.ts                                  # 22-query differential: stock-zero vs orez-local
bun src/shapes.ts --against orez-cf                # same differential vs the CF DO
bun src/sweep.ts --rounds 15                       # seeded randomized differential (prints seed)
bun src/sweep.ts --seed 12345 --against orez-cf    # deterministic replay / CF host
bun src/bench.ts --target orez-local --clients 20 --writers 5 --rate 10 --duration 15
bun src/storm.ts --target orez-local --clients 100 # one-project width + round gates
bun src/storm.ts --target orez-cf --clients 100    # same against one deployed DO
bun src/eviction.ts --target local                 # SIGKILL + file-backed restart
bun src/eviction.ts --target cf                    # idle DO memory teardown + resume
bun src/permissions.ts                             # per-user visibility + add/revoke
bun src/reconnect.ts                               # persisted resume + recovery faults
bun src/multi-tab.ts                               # real shared client group + LMIDs
```

sweep divergences write replay artifacts to `regressions/` (seed + spec +
both sides); re-run with the printed `--seed` to reproduce exactly.

targets: `stock-zero` (real zero-cache + embedded postgres + fixture app
server), `orez-local` (orez `src/sync-server` core over pure bun:sqlite,
clients on on-zero's production http-pull transport), and `orez-cf` (the
SAME core hosted in a durable object over `ctx.storage.sql` — `cf/worker.ts`
deployed as `zharness-sync` on lslcf; fresh namespace per run; admin oracle
gated by the ADMIN_KEY secret, key file `~/.zharness-cf-admin-key`; deploy
with `cd cf && bunx wrangler deploy`, then wait ~1min for propagation before
probing). DO sqlite gotchas the core is written around: no raw
`BEGIN`/`SAVEPOINT` SQL (only `storage.transactionSync`), no `?N` numbered
bindings.

`permissions.ts` runs `orez-local` with its optional `visible()` policy:
owner-or-member projects, tasks/members through visible projects, and only
the authenticated user's own user row. it is intentionally separate from
the globally visible differential lanes and asserts that membership removal
clears already-cached project/task data.

`reconnect.ts` and `multi-tab.ts` use a temporary file-backed implementation
of Zero's own SQLite KV store. they exercise the actual persisted Replicache
cookie/client-group state: sequential close/reopen recovery (including lost
responses, retention/epoch fallback, host restart, and future-cookie reset)
and concurrent same-storage-key tabs with distinct client IDs and per-client LMIDs.

`storm.ts` holds one `projectById(p0)` reactive read on every client while five
writers create, toggle, and re-rank the same task set at 5 mutations/sec. every
round gates on all-client convergence and an authoritative oracle comparison;
the final state is checked again through a fresh late client. use the same
arguments at 10, 20, and 100 clients for the width comparison recorded in the
plan.

`eviction.ts` keeps clients alive across authority-memory loss. the local path
runs the sync server in a child process over a WAL-mode SQLite file, SIGKILLs it
mid-churn, and starts a new PID on the same file. the CF path disables interval
polling, idles beyond the harness DO's deterministic memory-teardown window,
proves the boot ID changed, and resumes the same clients. both require exact
all-client/oracle/late-client convergence, zero 409s, and monotone request and
response cookies. expected connection-close logs during the local outage are
stock Zero's reconnect diagnostics, not lane failures.

`stock-zero` boots embedded postgres (wal_level=logical), the fixture app
server (`src/app-server.ts`: named-query transform on /query + custom-mutator
execution on /mutate, the role soot's app worker plays in prod), and real
zero-cache from node_modules (spawned with `node`, never bun). no docker
needed. Zero is pinned at stable 1.7.0 to match orez/soot. use Node 22 or 24
for stock-zero lanes; `@rocicorp/zero-sqlite3` does not support Node 25.

modern zero surface ONLY, no legacy: queries are `defineQueries` named
queries transformed server-side via `ZERO_QUERY_URL`; writes are
`defineMutators` custom mutators via `ZERO_MUTATE_URL`;
`ZERO_ENABLE_CRUD_MUTATIONS=false` so nothing can fall back to CRUD. wire
facts pinned by this setup:

- with both URLs set and no JWT config, zero-cache forwards the client's raw
  `auth` token as a bearer header; the app server authenticates it and the
  userID it passes to `handleQueryRequest`/`handleMutateRequest` is echoed
  back and pinned server-side ("Connection userID does not match validated
  server userID" when they disagree). fixture tokens: `token-<userID>`.
- a `defineMutators` REGISTRY on the client is invoked callable-style:
  `zero.mutate(mutators.project.create(args))` (property-style
  `zero.mutate.project.create` exists only for plain def objects).
- ad-hoc zql from `createBuilder` reads the local synced cache only and
  never syncs more data; the smoke asserts this explicitly.

## upstream (mono) fuzz lanes runbook

from `~/github/mono` (pnpm install first; docker running for testcontainers):

```sh
# everything incl. pg 15/16/17/18 matrix lanes (long)
cd packages/zql-integration-tests && pnpm test
# pg-17 only
pnpm exec vitest run --project='*17*'
# the zero-cache protocol fuzzer runs under zero-cache's config:
cd ../zero-cache && pnpm exec vitest run --project='*17*' ../zql-integration-tests/src/chinook/chinook-zero-cache-fuzzer.pg.test.ts
```

fuzz push-parity tests time out (120s budget) on a loaded machine; clean
baselines belong on the isolated runner (mini), not a dev box.
