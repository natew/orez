import { PGlite } from '@electric-sql/pglite'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import {
  getChangesSince,
  installChangeTracking,
} from '../../replication/change-tracker.js'
import { usePublicationsEnv } from '../../test-env.js'
import { createPostgresShim } from './postgres.js'

usePublicationsEnv(undefined)

// drive the REAL PGlite shim (createPostgresShim → executeQuery) end to end:
// the shim strips the FKs, captures the edges, and must expand a parent DELETE
// into the child statements — which fire PGlite's real change-tracking triggers.
describe('postgres shim FK cascade', () => {
  let db: PGlite
  let sql: ReturnType<typeof createPostgresShim>

  // drizzle's real form: CREATE TABLE with no FK, then ALTER TABLE ADD
  // CONSTRAINT … FOREIGN KEY (exactly what soot's migrations emit). the shim
  // strips both the inline and the ALTER FK; capture must see the ALTER form.
  const SCHEMA = [
    `CREATE TABLE "thread" ("id" text PRIMARY KEY, "title" text)`,
    `CREATE TABLE "message" ("id" text PRIMARY KEY, "threadId" text, "body" text)`,
    `CREATE TABLE "reaction" ("id" text PRIMARY KEY, "messageId" text)`,
    `CREATE TABLE "bookmark" ("id" text PRIMARY KEY, "threadId" text, "label" text)`,
    `ALTER TABLE "message" ADD CONSTRAINT "message_thread_fk" FOREIGN KEY ("threadId") REFERENCES "thread"("id") ON DELETE cascade`,
    `ALTER TABLE "reaction" ADD CONSTRAINT "reaction_message_fk" FOREIGN KEY ("messageId") REFERENCES "message"("id") ON DELETE cascade`,
    `ALTER TABLE "bookmark" ADD CONSTRAINT "bookmark_thread_fk" FOREIGN KEY ("threadId") REFERENCES "thread"("id") ON DELETE set null`,
  ]

  beforeEach(async () => {
    db = new PGlite()
    await db.waitReady
    sql = createPostgresShim(db)
    for (const ddl of SCHEMA) await sql.unsafe(ddl)
    await installChangeTracking(db)
    await sql.unsafe(`INSERT INTO "thread" VALUES ('t1', 'keep'), ('t2', 'doomed')`)
    await sql.unsafe(
      `INSERT INTO "message" VALUES ('m1', 't2', 'a'), ('m2', 't2', 'b'), ('m3', 't1', 'c')`
    )
    await sql.unsafe(
      `INSERT INTO "reaction" VALUES ('r1', 'm1'), ('r2', 'm1'), ('r3', 'm2')`
    )
    await sql.unsafe(
      `INSERT INTO "bookmark" VALUES ('b1', 't2', 'doomed'), ('b2', 't1', 'kept')`
    )
  }, 60_000)

  afterEach(async () => {
    await db.close()
  })

  async function count(table: string): Promise<number> {
    const r = await db.query<{ n: number }>(`SELECT count(*)::int AS n FROM "${table}"`)
    return r.rows[0].n
  }

  it('the shim strips FKs — so without expansion the store has no native cascade', async () => {
    // sanity: the tables really have no FK enforcement (a child insert with a
    // dangling parent succeeds), proving the cascade below is ours, not PG's.
    await sql.unsafe(`INSERT INTO "message" VALUES ('orphan', 'no-such-thread', 'x')`)
    expect(await count('message')).toBe(4)
    await sql.unsafe(`DELETE FROM "message" WHERE "id" = 'orphan'`)
  })

  it('parameterized parent DELETE cascades + every child deletion is captured', async () => {
    const watermark = await currentWatermark(db)
    await sql.unsafe(`DELETE FROM "thread" WHERE "id" = $1`, ['t2'])

    expect(await count('thread')).toBe(1)
    expect(await count('message')).toBe(1) // only m3 (on t1) survives
    expect(await count('reaction')).toBe(0) // grandchildren all gone
    expect(await count('bookmark')).toBe(2) // set-null keeps the rows
    const b1 = await db.query<{ threadId: string | null }>(
      `SELECT "threadId" FROM "bookmark" WHERE "id" = 'b1'`
    )
    expect(b1.rows[0].threadId).toBeNull()

    const changes = await getChangesSince(db, watermark)
    const tally = (table: string, op: string) =>
      changes.filter((c) => c.table_name === `public.${table}` && c.op === op).length
    expect(tally('reaction', 'DELETE')).toBe(3)
    expect(tally('message', 'DELETE')).toBe(2)
    expect(tally('thread', 'DELETE')).toBe(1)
    expect(tally('bookmark', 'UPDATE')).toBe(1)
  })

  it('non-parameterized parent DELETE cascades too', async () => {
    await sql.unsafe(`DELETE FROM "thread" WHERE "id" = 't2'`)
    expect(await count('message')).toBe(1)
    expect(await count('reaction')).toBe(0)
  })
})

async function currentWatermark(db: PGlite): Promise<number> {
  const r = await db.query<{ last_value: string; is_called: boolean }>(
    'SELECT last_value, is_called FROM _orez._zero_watermark'
  )
  const { last_value, is_called } = r.rows[0]
  return is_called ? Number(last_value) : 0
}
