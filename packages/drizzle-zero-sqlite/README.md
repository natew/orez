# drizzle-zero-sqlite

Generate a Zero schema from a Drizzle SQLite schema while keeping Drizzle as the single schema source.

```ts
import { drizzleZeroConfig } from 'drizzle-zero-sqlite'

export const zeroSchema = drizzleZeroConfig(
  { ...schema, relations },
  {
    tables: {
      user: true,
      message: true,
    },
  }
)
```

`drizzleZeroConfig()` returns a real Zero schema created with
`@rocicorp/zero`. Pass it anywhere Zero expects your schema.

The translator accepts Drizzle `sqliteTable` definitions and beta
`defineRelations` exports, including `through(...)` relations. It rejects
PostgreSQL tables so a project cannot silently generate the wrong dialect.
