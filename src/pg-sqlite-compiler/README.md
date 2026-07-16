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

Each pass is a focused visitor for one PG → SQLite concern. They run in the
order listed by `passes/index.ts`:

- `passes/dml-cte.ts` — data-modifying CTEs
- `passes/array.ts` — `= ANY(…)` / `<> ALL(…)` → `IN (SELECT value FROM json_each(…))`
- `passes/types.ts` — `::type`, CAST chains, PG → SQLite type names, BIGSERIAL
- `passes/datetime.ts` — NOW(), CURRENT_TIMESTAMP, EXTRACT, DATE_TRUNC, INTERVAL
- `passes/string-functions.ts` — PG string builtins → SQLite equivalents
- `passes/json-functions.ts` — json_agg/json_build_object → json_group_array/json_object
- `passes/row-json.ts` — row_to_json and friends
- `passes/catalog.ts` — pg_class / pg_attribute / information_schema rewrites
- `passes/schema.ts` — schema-qualified name flattening
- `passes/unsupported.ts` — warns on constructs with no SQLite equivalent

## Array params

`col = ANY($1::text[])` compiles to `json_each($1)`, which reads a **JSON array
string** — not a PG array literal. `compile()` returns the bind slots this
applies to so the caller can encode them:

```ts
const { sql, arrayParamNumbers } = compile(`SELECT id FROM t WHERE id = ANY($1::text[])`)
// arrayParamNumbers === [1]
const bound = params.map((v, i) =>
  arrayParamNumbers.includes(i + 1) ? JSON.stringify(v) : v
)
```

Only `= ANY` and `<> ALL` translate. Other operators (`> ANY`, `<= ALL`, …)
have no order-preserving json_each form and raise an `unsupported-array-operator`
warning, so `strict: true` callers reject them instead of shipping SQL that dies
at prepare.

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
