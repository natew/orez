# on-zero data layout and instance configuration

Design for rails-pass item 4. A table's namespace and sync membership are
declared by its data file. Multi-instance partitioning is declared once in
`on-zero.config.ts`. Single-instance applications keep a flat data root and
need no config.

## What this deletes

The generator derives three previously hand-maintained lists:

1. The drizzle-zero table allowlist. A table enters the schema when it owns a
   namespace, is reached through `related()`, is reached statically through a
   mutation transaction, or is declared as a server-only support table.
2. Per-instance query and model lists. Configured directory ownership replaces
   hand-built `controlQueries`, `projectQueries`, and matching model lists.
3. Per-instance sync surfaces. The generator computes each instance's
   namespace tables plus its complete `related()` closure.

## Namespace layout

A namespace has one of two shapes:

```text
src/data/
  post.ts
  thread/
    queries.ts
    mutations.ts
    helpers.ts
  generated/
```

Small namespaces use `<name>.ts`. Larger namespaces use
`<name>/queries.ts` plus `<name>/mutations.ts`; other files in that folder are
private helpers. The removed top-level `queries/`, `mutations/`, and `models/`
layouts are hard errors.

Membership is decided by export shape. A root file is a namespace only when the
AST pass recognizes query builders, `mutations(...)`, `serverWhere(...)`, or a
table declaration. Wiring files such as `server.ts`, `types.ts`, and
`zero-client.tsx` are ignored when they export no data shape. There is no
reserved filename list, so a real namespace named `server` works.

## Single instance

Single-instance applications put namespaces directly in the data root and do
not create a config file:

```text
src/data/
  post.ts
  comment/
    queries.ts
    mutations.ts
```

Generation emits one `default` manifest entry. Existing `createZeroClient`
usage is unchanged.

## Multiple instances

Multi-instance applications add one config file at the data root:

```ts
// src/data/on-zero.config.ts
import { defineConfig } from 'on-zero'

export default defineConfig({
  instances: {
    default: { dir: '.' },
    project: { dir: './project-data', scope: 'projectId' },
  },
})
```

Each instance key is its generated client name. `dir` is optional and defaults
to `./<key>`. Paths resolve relative to the config file, so applications own
their physical layout:

```text
src/data/
  on-zero.config.ts
  control/
    account.ts
  project-data/
    message.ts
    snapshot/
      queries.ts
      mutations.ts
```

The data root owns namespaces only when an instance explicitly declares
`dir: '.'`. This lets an existing default instance remain flat while nested
instances are configured beside it. Configured directories may also point
outside the root, and generation, caching, watching, type resolution, and
generated import paths follow them.

`instance.ts` and `defineInstance` are removed. Any remaining `instance.ts`
fails generation with migration guidance. There is one configuration path.

## defineConfig options

`defineConfig` is exported from `on-zero` and returns its input unchanged. Its
types carry detailed editor documentation. `on-zero.config.ts` is the only home
for current and future generator configuration.

Each instance supports:

- `dir`: namespace directory relative to the config file; defaults to the key.
- `scope`: column required on every synced table in that instance. It also
  emits the default row-visibility predicate in the generated manifest.
- `supportTables`: server-only tables static mutation analysis cannot discover.
  They enter schema generation and push typing, but never become query
  namespaces or synced tables. Scope validation does not apply to them.

## CLI and programmatic generation

The config is auto-discovered for existing programmatic calls:

```ts
await generate({ dir: 'src/data' })
await deriveDataMembership({ dir: 'src/data' })
```

The CLI accepts either a data directory or an explicit config path:

```sh
on-zero generate
on-zero generate ./src/data
on-zero generate ./src/data/on-zero.config.ts
```

Passing the config path makes multi-instance generation self-explanatory while
preserving `generate({ dir })` for browser-project codegen and arbitrary base
directories.

## Generated manifest

`generated/instances.ts` contains one entry per configured key:

```ts
export const instances = {
  control: { queries, models, tables, syncTables, supportTables, scope: null },
  project: {
    queries,
    models,
    tables,
    syncTables,
    supportTables,
    scope: 'projectId',
    defaultVisibility,
  },
}
```

- `tables` contains namespace-owned tables.
- `syncTables` contains `tables` plus the complete `related()` closure.
- `supportTables` contains server-only mutation dependencies.
- `createZeroClients(instances)` creates each client and the typed combined
  facade. Single-instance applications may continue using `createZeroClient`.

## Generate-time validation

- a configured directory does not exist
- two instances resolve to the same directory
- a recognized root namespace is outside every configured directory
- any removed `instance.ts` remains
- a namespace is claimed by more than one instance
- a query reaches a table owned by another instance through `related()`
- a synced table lacks its instance's scope column
- `related()` uses a dynamic name the generator cannot resolve

Tables without `where` or CRUD exports remain valid. Visibility policy stays
application-owned.

## Derivation order

The cheap layout and AST membership pass runs before zero-schema construction
and TypeScript type resolution:

1. load `on-zero.config.ts` when present
2. resolve and validate instance directories
3. discover namespace exports
4. derive related and mutation support closures
5. build the filtered drizzle-zero schema and relations
6. run typed query and mutation generation

This ordering lets the derived table membership replace the drizzle-zero
allowlist without circular type dependencies.
