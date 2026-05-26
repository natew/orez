# pg-sqlite-compiler

PostgreSQL SQL → SQLite SQL compiler. Single pass over the libpg_query AST,
emitting via pgsql-deparser with SQLite-specific overrides.

## Architecture

```
PG SQL ──► parseSync()       (libpg-query WASM, real PG parser)
              ↓
           PG AST            (RawStmt[])
              ↓
           passes[]          (one visitor per concern)
              ↓
           PG AST (mutated)
              ↓
           emit()            (pgsql-deparser + SQLite overrides)
              ↓
        SQLite SQL
```

## Passes

Each pass is a focused visitor for one PG → SQLite concern:

- `passes/datetime.ts` — NOW(), CURRENT_TIMESTAMP, EXTRACT, DATE_TRUNC, INTERVAL
- `passes/array.ts` — ARRAY[…], @>, <@, unnest, array literals
- `passes/cast.ts` — `::type`, CAST chains, PG → SQLite type names
- `passes/json.ts` — `->`, `->>`, jsonb_set/get/path
- `passes/create_table.ts` — type mappings, BIGSERIAL → INTEGER, defaults
- `passes/insert.ts` — ON CONFLICT semantics, RETURNING (mostly native)
- `passes/catalog.ts` — pg_class / pg_attribute / information_schema rewrites

## Testing

Two layers:

1. **Snapshot tests** (`test/*.test.ts`) — for each pass, fixed (input, expected
   output) pairs. Fast, deterministic, tracked in git.

2. **Oracle tests** (`test/oracle.test.ts`) — spawn pgsqlite, send query to it
   via PG wire, send same query through our compiler + bun:sqlite, compare
   result sets. Validates semantic equivalence, not text identity. Only runs
   when pgsqlite binary is available (`scripts/pgsqlite/ensure.ts`).

The pgsqlite binary itself is NOT shipped. It's a dev-time / CI oracle only.

## Why not just use pgsqlite?

pgsqlite is a Rust server (tokio + rusqlite); doesn't run in Cloudflare workerd.
We need PG → SQLite translation as a pure-TS library that compiles into a CF
Durable Object alongside zero-cache. So we reimplement the translation in TS,
using pgsqlite as our quality oracle.
