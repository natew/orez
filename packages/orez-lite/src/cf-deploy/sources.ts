import { quoteSqlIdentifier } from './leaves.js'

/** SQLite schema used by the native Orez Lite mutation acknowledgement shard. */
export function zeroHttpShardDDL(appId: string): string {
  const appSchema = `${appId}_0`
  const schema = quoteSqlIdentifier(appSchema)
  return [
    `CREATE SCHEMA IF NOT EXISTS ${schema};`,
    `CREATE TABLE IF NOT EXISTS ${schema}.clients (` +
      `"clientGroupID" text NOT NULL, ` +
      `"clientID" text NOT NULL, ` +
      `"lastMutationID" bigint NOT NULL, ` +
      `PRIMARY KEY ("clientGroupID", "clientID")` +
      `);`,
    `ALTER TABLE ${schema}.clients ADD COLUMN IF NOT EXISTS "userID" text;`,
    `CREATE TABLE IF NOT EXISTS ${schema}.mutations (` +
      `"clientGroupID" text NOT NULL, ` +
      `"clientID" text NOT NULL, ` +
      `"mutationID" bigint NOT NULL, ` +
      `result json NOT NULL, ` +
      `PRIMARY KEY ("clientGroupID", "clientID", "mutationID")` +
      `);`,
  ].join('\n--> statement-breakpoint\n')
}
