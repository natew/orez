# orez/cf-do architecture - READ FIRST

## The fundamental constraint

**Cloudflare Durable Objects have a 128 MB memory budget per instance.**

PGlite + WASM Postgres + extension binaries pushes real deployments over that
budget. That is the dead end. zero-cache is still essential: it owns the sync
protocol, IVM/CVR machinery, replication handling, and client semantics.

The orez Cloudflare path is therefore:

- **yes** real `@rocicorp/zero` zero-cache
- **yes** Durable Object SQLite as the durable storage engine
- **yes** orez `DoBackend` serving Postgres-protocol semantics to zero-cache
- **no** PGlite
- **no** `pglite.wasm`, `pglite.data`, extension `.so` files, or WASM Postgres

## Production shape

```
┌────────────────────────────────────────────────────────────────┐
│ Worker                                                         │
│                                                                │
│ /sync/v* and /api/zero/* ─────► ZERO_CACHE_DO singleton        │
│ everything else ──────────────► ASSETS                         │
└────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────┐
│ ZERO_CACHE_DO Durable Object                                   │
│                                                                │
│ startZeroCacheEmbedCF() runs real zero-cache in-process        │
│   │                                                            │
│   ├─ zero-cache replica/CVR/CDB SQLite                         │
│   │    @rocicorp/zero-sqlite3 -> orez worker SQLite shim       │
│   │    backed by ctx.storage.sql                               │
│   │                                                            │
│   └─ zero-cache upstream Postgres connections                  │
│        postgres -> orez postgres browser shim                  │
│        DoBackend -> ZERO_SQL_DO /exec and /batch               │
└────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────┐
│ ZERO_SQL_DO Durable Object                                     │
│                                                                │
│ ZeroDO raw SQL endpoints                                       │
│   /exec, /batch, /changes, /notify, /__orez/*                  │
│   ctx.storage.sql                                              │
│   _orez.changes populated by SQL tracking triggers             │
└────────────────────────────────────────────────────────────────┘
```

All app traffic for a deployed project must use singleton DO IDs for both
`ZERO_CACHE_DO` and `ZERO_SQL_DO`; otherwise browser sessions will not share the
same zero-cache process and durable SQLite state.

## Important files

- `src/worker/zero-cache-embed-cf.ts` - starts real zero-cache inside a Durable
  Object and wires its storage/network dependencies to CF-safe shims.
- `src/worker/cf-patches.ts` - prepares an isolated zero-cache overlay whose
  worker graph and writer run in the Workers runtime without mutating
  `node_modules`.
- `src/pg-proxy-do-backend.ts` - translates Postgres protocol operations from
  zero-cache into DO SQL endpoint requests.
- `src/cf-do/worker.ts` - `ZeroDO`, the generic DO SQL backend. It also still
  contains a bespoke Zero sync protocol handler used for development and
  protocol experiments, but the production Soot deploy path uses real
  zero-cache through `startZeroCacheEmbedCF()`.
- `src/do-sql-tracking.ts` and `src/replication/*` - change tracking and
  logical replication support over `_orez.changes`.

## What not to do

- Do not import `@electric-sql/pglite` into the Cloudflare DO deploy path.
- Do not bundle PGlite WASM/data/extensions into the deploy template.
- Do not replace zero-cache with the bespoke handler for production sync.
- Do not add a second fallback path that silently switches between PGlite,
  bespoke sync, and zero-cache. There should be one production path:
  zero-cache -> orez Postgres protocol -> DO SQLite.

If a future change needs more Postgres behavior, implement it in
`DoBackend`/the SQL translator or the DO SQL backend. Do not put PGlite back in
the request path.

## Running the chat e2e harness against this backend

See `src/cf-do/CHAT_E2E.md`. The DO path is exercised end-to-end by chat's
`--lite` mode harness, which has a hard 60-second `waitForPort(zero)` budget
during boot. Three amplification bugs in `DoBackend` were fixed
2026-05-26 (snapshot fan-out, metadata persist per-row HTTP, metadata persist
per-statement-in-tx); boot now completes in ~13s on a developer laptop. If
boot regresses past the budget again, re-capture the /exec distribution as
described in `CHAT_E2E.md` §3 before guessing at fixes.
