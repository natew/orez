# zharness

zero sync-engine conformance + load harness. plan and status:
`../plans/zero-conformance-harness.md`. never published; private tooling.

one harness, multiple targets (`src/target.ts`): stock zero-cache today,
orez-local pure-sqlite and orez-cf next. every lane points stock
`@rocicorp/zero` clients at a target, writes both through sync and straight
to the upstream store, requires convergence, and compares converged client
state against a fresh oracle read of the authoritative store.

## run

```sh
bun install
bun run smoke                       # 10 clients vs real zero-cache + embedded postgres
bun src/smoke.ts --clients 50 --projects 4
```

`stock-zero` boots embedded postgres (wal_level=logical) and spawns real
zero-cache from node_modules (spawned with `node`, never bun). no docker
needed. zero pinned at 1.6.1 to match orez/soot; the fixture schema sets
`enableLegacyQueries`/`enableLegacyMutators` (zero 1.6 gates
`zero.query.<table>` and CRUD mutators behind them; custom mutators need a
push server, which arrives with the orez-local target).

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
