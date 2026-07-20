# on-zero instance folders — data layout v2

Design for rails-pass item 4. The principle: a table's sync membership,
instance assignment, and namespace are ONE declaration — where its files sit.
Partitioning is opt-in via a single colocated marker file; apps that never
partition (most apps) see nothing new.

## What this deletes

Three hand-maintained lists that restate overlapping truth today:

1. The Zero table allowlist (`drizzle-zero.config.ts` `tables:` map in
   takeout; the `zeroSchemaInput` re-export module in soot). Derived instead:
   a table is synced iff it has a queries or mutations file or is reachable
   from any instance's queries via `related()`. A table nothing can query has
   no reason to sync, so the derivation is complete.
2. The instance partition lists (soot `core.ts`: controlQueries,
   projectQueries, controlModels, projectModels + three module-eval
   assertions). Derived from folder membership.
3. The sync-surface lists (soot `projectTables.ts`: PROJECT_TABLE_NAMES,
   PROJECT_QUERY_TABLE_NAMES, PROJECT_SYNC_TABLE_NAMES). Derived per instance
   as the `related()` closure of that instance's query ASTs — covers
   related-only tables (iosPublishStepRun etc.) nobody has to remember to
   list.

## Layout

```
src/data/
  generated/
  reaction.ts        small namespace: ONE file (where + query + mutate)
  thread/            big namespace: a folder
    queries.ts
    mutations.ts
    helpers.ts       private decomposition — generator ignores extra files
  <instance>/        a folder is an instance ONLY because of the marker:
    instance.ts        export default defineInstance({ scope: 'projectId' })
    snapshot.ts        namespaces nest identically inside instances
    message/
      queries.ts
      mutations.ts
```

MODEL-FIRST, one convention: the namespace is the organizing unit. A
namespace is a file, or a folder with `queries.ts`/`mutations.ts` when it
outgrows the file (like route.tsx vs route/index.tsx). A folder containing
`instance.ts` is an instance, not a namespace — the marker disambiguates.
The old top-level `queries/` + `mutations/` directories and the legacy
`models/` alias are REMOVED — no optional older style. Every consumer
(takeout, chat, soot, browser-project codegen) migrates in the cutover;
mechanical file moves.

Why folder mode keeps queries.ts/mutations.ts separate: query files are a
shared read surface imported by many graphs (client components, server
named-query resolution, dev queryTransform, the CF sync host's lean query
resolver) while mutation files are imported only by the two registries and
carry write-side weight + registration side effects (the split originally
arrived with Zero's named-queries migration — takeout bddfde6a, chat
357b42b53 — mirroring Zero's two registries). A single-file namespace merges
those graphs; that cost is real but borne only by the namespace choosing it.
Guidance: start single-file; go folder when the file gets big (chat's
mutations/message.ts is ~690 lines) or its mutations carry heavy imports.
Namespace folders also give big tables a home for private helper files,
which the flat layout never had.

- The ROOT is always the default/primary instance. Simple apps (takeout, most
  generated browser projects) never leave it — zero new concepts, no config.
- `instance.ts` is the explicit opt-in indicator. A folder without it is just
  organization, never an instance — nothing is inferred from a folder name
  alone.
- Soot maps naturally: control plane = root, `project/` = the partitioned
  instance. Two instances, one marker file.
- The legacy `models/` directory alias is removed in this pass; the two
  spellings above are the only layouts.

## defineInstance

```ts
// src/data/project/instance.ts
import { defineInstance } from 'on-zero'

export default defineInstance({
  // column every table in this instance must carry; also emits the default
  // row-visibility predicate for sync hosts
  scope: 'projectId',
})
```

Instance name = folder name (acceptable now that membership itself is
explicit via the marker). Keep the option surface minimal; grow it only when
a real consumer needs more.

## Permission convention (groundwork for item 9)

Each table's `serverWhere` permission is exported from that table's query
file under a canonical name (`export const where = serverWhere(...)`), where
permissions already live today. Later (item 9) the orez sync hosts compile
that same ZQL predicate to their SQL row visibility, so query permission,
mutation permission (ctx.can), and sync visibility become one declaration and
most of soot's zeroVisibility.ts dies. Not part of this item; the convention
just makes that migration a rename, not a restructure.

## Generated manifest

`generated/instances.ts`:

```ts
export const instances = {
  default: { queries, models, tables, syncTables, scope: null },
  project: { queries, models, tables, syncTables, scope: 'projectId', defaultVisibility },
}
```

- `tables` = namespaces with files; `syncTables` = tables ∪ related()
  closure. Server pull endpoints and visibility partitions consume
  `syncTables`.
- Client composition: `createZeroClients(instances)` in on-zero/multi builds
  each instance client, combines with the root as primary/outer, returns the
  combined facade + per-instance providers. Soot core.ts (~240 lines)
  becomes ~30 lines of re-exports.
- Single-instance apps get the same manifest with one entry; createZeroClient
  keeps working unchanged.

## Generate-time validation (replaces runtime assertions)

- cross-instance reach: an instance's query `related()`-ing into a table
  owned by another instance is an ERROR naming the query and both instances
  (physical partitioning makes this unservable; denormalize instead).
- scope column missing on a member table: error.
- namespace claimed by two instances (same filename under two instance
  dirs): error — the combined facade is flat, namespaces stay global.
- no opinion on missing `where`/CRUD: tables without permissions or mutations
  are legitimate (server-owned writes, public rows, related-only tables).
  Visibility policy stays app-owned.

## Implementation notes / risks

- Derivation ordering: sync membership must be derivable BEFORE type
  resolution (the zero schema that types zql is built from the allowlist).
  Membership needs only filenames + related() name strings — a cheap AST
  pass — so the pipeline is: walk dirs -> derive membership/closure -> build
  zero schema -> typed generation as today. Verify against generate.ts's
  real ordering.
- The related() closure must resolve through relations.ts to target tables,
  not just read string literals in query files.
- Watch mode + input hash already walk the whole base dir; instance dirs are
  covered.
- drizzle-zero.config.ts survives only if it carries non-allowlist options
  (column overrides); the `tables:` map is deleted.
- Consumers to migrate in the cutover: takeout (no change beyond allowlist
  deletion), soot (project/ instance folder), chat, and the contrast
  browser-project codegen (programmatic generate({ dir }) — keep arbitrary
  base dirs working).
