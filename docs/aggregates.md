# Orez Lite aggregates

Orez Lite aggregates maintain queryable `count` and `sum` columns while Zero does
not yet support aggregate queries. One declaration owns the authoritative
SQLite triggers, the backfill SQL installed with the schema migration, and the
client-side projection used during optimistic Zero mutations.

```ts
import { count, defineAggregates, sum } from 'orez-lite/aggregate'
import { schema } from './data/generated/schema'

export const aggregates = defineAggregates(schema, {
  categorySpend: {
    source: 'expense',
    target: 'categorySpend',
    mode: 'materialized',
    groupBy: {
      accountId: 'accountId',
      categoryId: 'categoryId',
    },
    columns: {
      expenseCount: count(),
      spent: sum('amount'),
    },
  },
  postCommentCount: {
    source: 'comment',
    target: 'post',
    mode: 'existing',
    groupBy: {
      postId: 'id',
    },
    columns: {
      commentCount: count(),
    },
  },
})
```

The source and target tables remain ordinary Drizzle and Zero tables. This keeps
their rows queryable and syncable through the stock Zero schema. The aggregate
declaration adds the behavior that Drizzle and Zero do not currently express.

`materialized` targets contain only the group key and aggregate columns. Orez
creates and removes those rows as groups appear and disappear. `existing`
targets are application-owned rows, such as a post or account. Orez updates the
aggregate columns without creating or deleting the parent row.

## Migration contract

Install `aggregateMigrationStatements(aggregates)` after the Drizzle statements that
create the source and target tables. The statements backfill current rows and
create the three SQLite triggers for each aggregate. They are normal migration
statements and must be committed with the schema migration.

Changing an aggregate definition requires a new migration. The generated statements
drop and recreate only Orez-owned triggers, then recompute the declared
aggregate columns from the source rows in the same transaction.

## Namespaces and generation

A project declares aggregates in `data/<namespace>/aggregates.ts`, exporting the
definitions themselves rather than a compiled set:

```ts
import { count } from 'orez-lite/aggregate'

export const aggregates = {
  postCommentCount: {
    source: 'comment',
    target: 'post',
    mode: 'existing',
    groupBy: { postId: 'id' },
    columns: { commentCount: count() },
  },
}
```

`on-zero generate` collects every namespace that has one and emits
`data/generated/aggregates.ts`, which calls `defineAggregates` once over the
union. Compiling in one place is what makes the cross-namespace checks possible:
two namespaces writing the same target column, or reusing one aggregate name,
are conflicts only the whole set can see. A name declared twice is refused by
`mergeAggregateDefinitions` rather than silently resolved by import order.

Nothing registers a namespace. Adding the file is the whole step.

## Optimistic client contract

Pass the generated set to `createZeroClient({ aggregates, ... })`. Orez wraps the
transaction seen by every generated or custom on-zero mutator. Source writes
update locally available target rows during the optimistic run. The
authoritative server run does not perform that client projection because the
SQLite trigger owns it.

The server result still wins during Zero's normal rebase. An existing-row target
that is not present in the client's local data is left alone and arrives from
the server. A materialized insert can create a new local group from the inserted
row. The authoritative aggregate replaces it during the normal rebase if the
client did not have the group's complete prior state.

## First-slice limits

- Aggregates are unfiltered `count` and numeric `sum`.
- Every group field maps to every target primary-key field.
- Group values must be non-null.
- A materialized target has no columns beyond its key and aggregate columns.
- Source and target must be different tables.
- An aggregate target cannot feed another aggregate.
- Aggregate target columns are derived. Application mutations do not write them.

These limits keep one authoritative implementation. Average is represented by a
count and sum and divided at read time. Minimum, maximum, filters, and
relationship aggregates need additional semantics before they belong here.
