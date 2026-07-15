# SQLite ZQL transaction query contract

This contract covers `tx.run(query)` inside an authoritative SQLite mutation.
It does not change Zero pull, push, or the public `/api/zero` routes. The Rust
sync engine continues to handle client query membership separately.

## Interface and result

`engine_compile_query` accepts all three inputs that Zero's server adapter uses:

```ts
engine_compile_query(schema, ast, format): CompiledQueryPlan
```

`format` is required. It is separate from the AST in Zero and carries facts the
AST cannot recover, including whether `.limit(1)` returns a list, whether
`.one()` returns one row, and whether each relationship is singular.

The Wasm result is a JSON-compatible recursive execution plan. The root and
each visible relationship contain:

- one SQLite `SELECT` with positional `?` parameters;
- bindings in placeholder order, each either a compiled literal or a logical
  field from the already materialized parent row;
- the logical table and column types needed to decode SQLite values;
- visible child plans keyed by their relationship alias and singularity.

A child select is scoped to one parent. A hidden two-hop junction is compiled
as one child select that joins the junction to the visible destination. The
junction is never present in the returned object.

The host executes and fully materializes each select before it recurses or
awaits. It hydrates the flat rows into the ordinary Zero server result:

- a plural root is an array, including `[]` for no matches;
- a singular root is one object or `undefined`;
- a plural relationship property is an array;
- a singular relationship property is one object or `null`, matching Zero's
  current `z2s` runtime result;
- relationship property names and row column names are always logical Zero
  names.

Rows and cursors never cross the Wasm boundary. Only the compiled plan crosses
it. The host returns fully materialized plain objects to the mutation callback.
This avoids JSON aggregate coercion changing booleans, JSON columns, timestamps,
or integers.

Each `tx.run` execution has two host-enforced limits. The defaults are 256
selects and 10,000 total materialized rows; a consumer may lower or raise them
in host configuration. The counters include the root select and every related
select at every depth. Crossing either limit throws
`transaction_query_budget_exceeded`, aborts the owning transaction, and reports
the query's registered name when present or its logical root table plus stable
plan hash otherwise. The diagnostic includes both counters and both limits. An
unbounded root with a related child therefore fails explicitly instead of
consuming the Durable Object CPU budget without a bound.

## Supported query subset

The compiler accepts the current Zero AST fields `schema`, `table`, `alias`,
`where`, `related`, `start`, `limit`, and `orderBy`.

- `where` supports `and`, `or`, and simple comparisons `=`, `!=`, `IS`,
  `IS NOT`, `<`, `>`, `<=`, `>=`, `LIKE`, `NOT LIKE`, `ILIKE`, `NOT ILIKE`,
  `IN`, and `NOT IN`.
- `LIKE` and `NOT LIKE` are case-sensitive, while `ILIKE` and `NOT ILIKE` are
  case-insensitive, matching Zero on PostgreSQL. The compiler converts each
  bound PostgreSQL LIKE pattern to an escaped SQLite GLOB pattern. It emits
  `GLOB` for LIKE and `LOWER(value) GLOB LOWER(?)` for ILIKE. This is the only
  pattern-matching path; it does not depend on SQLite's connection-wide
  `case_sensitive_like` setting.
- comparison values are resolved scalar literals. `IN` and `NOT IN` accept
  scalar literal arrays. Every value becomes a positional binding.
- correlated `EXISTS` and `NOT EXISTS` support compound equality correlations
  and may nest recursively. `flip` is accepted as a planning hint with no
  effect on SQL semantics, matching Zero's server compiler.
- visible related output supports one-hop correlations, compound keys, and
  recursive related children.
- Zero's standard hidden two-hop junction shape is supported when it contains
  exactly one visible destination relationship.
- root and related selects support `where`, `orderBy`, `limit`, and `start`.
  Ordering and limits inside a relationship apply independently to each parent.
  The table primary key is appended as a stable ordering tie-breaker at every
  level.
- root and related `.one()` shapes use `format.singular`; an AST limit of one
  alone does not change the output from an array to an object.

Every logical identifier is validated against `schema`. SQL uses
`table.serverName ?? table.name ?? logicalTableName` for a physical table and
`column.serverName ?? logicalColumnName` for a physical column. Selected
physical columns are aliased back to logical names before hydration. Correlation,
filter, ordering, cursor, and primary-key references all use the same mapping.

## Rejections

Compilation fails with a status-400 engine error before any SQL executes for:

- an unknown table, column, format relationship, operator, or AST field;
- a missing, duplicate, or conflicting visible relationship alias;
- any nonempty `related` tree that cannot be compiled completely;
- a format tree that does not exactly describe the visible related tree;
- static parameters, cross-table column references, or non-scalar filter values;
- `scalar: true` correlated conditions;
- a hidden relationship that is not Zero's exact two-hop junction shape;
- invalid correlation arity, ordering, cursor, limit, schema type, or physical
  name mapping.

There is no root-only result for an unsupported related query. A consumer sees
the compiler error and the owning application transaction aborts.

## Conformance gates

The implementation is accepted only when all of these agree:

1. Rust unit tests cover every supported operator, compound correlations,
   logical-to-physical table and column mapping, binding order, stable ordering,
   and every rejection above.
2. SQLite execution tests cover empty and populated root one/many results,
   related one/many results, nested related output, a hidden two-hop relation,
   per-parent ordering/limit/start, and logical decoding of boolean, JSON,
   number, and string values. Targeted rows named `Alice` and `alice`
   distinguish LIKE from ILIKE regardless of what the harvested corpus covers.
   Null-bearing rows prove that `= NULL` and `!= NULL` match no rows, `IS NULL`
   matches null rows, and `IS NOT NULL` matches non-null rows.
3. A Wasm/workerd test serializes the recursive plan, executes it through the
   same materializing host adapter used by mutators, and proves that a missing
   or malformed format fails instead of returning root-only rows.
4. Chat conformance uses the final seven-batch corpus at Chat commit
   `cc2d26fa24a88161231f3337c0e0cae9d43ae2d1` as the result oracle: 252
   caller/query cases across 125 named queries. Compiler fixtures are harvested
   from Chat's real query builders and normalized with their
   `asQueryInternals(...).format`, rather than manually transcribing ASTs.
   Current mutation coverage also exercises Chat's 30 direct `tx.run` calls,
   including singular related `app`, nested relations, `IN`, composite keys,
   and list queries.
5. The same harvested vectors run through Zero's official `z2s` server compiler
   and the SQLite plan. Their materialized logical results must be deeply equal.
   The differential set also contains explicit case-sensitive LIKE,
   case-insensitive ILIKE, ordinary `=`/`!=` against null-bearing columns, and
   `IS`/`IS NOT` against those same rows. Chat's on-zero tests and `/api/zero`
   push/pull envelope tests remain unchanged.

The compiler, Wasm wrapper, Cloudflare host adapter, browser host adapter, and
root `ZeroDO` executor land as one versioned Orez slice. The release graph pins
the host dependency before Orez, so an older two-argument compiler cannot be
paired silently with a newer transaction adapter.
