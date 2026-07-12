# pg-to-sqlite

PostgreSQL SQL to SQLite SQL compiler extracted from orez's Cloudflare Durable
Object data path.

```ts
import { compile } from 'pg-to-sqlite'

const result = compile('SELECT NOW() AS t', { strict: true })
console.log(result.sql)
```

The compiler uses the real PostgreSQL parser through `pgsql-parser`, mutates the
AST with focused SQLite compatibility passes, then emits SQLite-compatible SQL.

## API

- `compile(sql, options?)`
- `compileMany(sqls, options?)`

Use `strict: true` to throw when a statement produces compiler warnings. Strict
mode rejects known PostgreSQL-only constructs that cannot execute in SQLite,
including `LATERAL`, `DISTINCT ON`, and `GREATEST`/`LEAST`.
