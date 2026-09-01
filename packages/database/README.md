# @o/database

SQLite database helpers for Orez hosts.

- `@o/database/sqlite`: `createSQLiteDatabase` builds typed Drizzle inside a
  transaction the host owns (a Durable Object session, a native SQLite
  connection). The host decides what a session is; Drizzle never emits BEGIN
  or COMMIT. `createBunSQLiteTransactionProvider` is the Bun-native provider.
- `@o/database/better-auth`: `createBetterAuthSQLiteAdapter` gives Better
  Auth its Drizzle adapter over the same host-owned transaction. Pass
  `readTransactionProvider` so `findOne`, `findMany`, and `count` run on the
  host's read lane instead of taking its writer turn.
- `@o/database/seed`: `applySeed` applies plain seed rows with
  `INSERT ... ON CONFLICT DO NOTHING`; `SeedData` types them against a Drizzle
  schema.
- `privateTable` marks a Drizzle table as excluded from Zero replication.

Moved here from `@take-out/database`, which keeps the Postgres helpers.
