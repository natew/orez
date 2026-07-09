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
bun src/bench.ts --target orez-local --clients 20 --writers 5 --rate 10 --duration 15
```

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

`stock-zero` boots embedded postgres (wal_level=logical), the fixture app
server (`src/app-server.ts`: named-query transform on /query + custom-mutator
execution on /mutate, the role soot's app worker plays in prod), and real
zero-cache from node_modules (spawned with `node`, never bun). no docker
needed. zero pinned at 1.6.1 to match orez/soot.

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
