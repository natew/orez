import { PGlite } from '@electric-sql/pglite'
import { loadModule, parseSync } from 'pgsql-parser'
import { beforeAll, beforeEach, afterEach, describe, expect, it } from 'vitest'

import {
  expandDelete,
  FkCascadeRegistry,
  quoteIdent,
  recordCreateTableForeignKeys,
  type TableRef,
} from './fk-cascade'
import { getChangesSince, installChangeTracking } from './replication/change-tracker'
import { usePublicationsEnv } from './test-env'

usePublicationsEnv(undefined)

// PGlite-namespace resolver: tables stay schema-qualified, so the canonical
// identifier (and registry key) is "schema"."table" with public as the default.
const resolve = (ref: TableRef): string =>
  `${quoteIdent(ref.schemaname ?? 'public')}.${quoteIdent(ref.relname)}`

function registryFromDdl(...ddls: string[]): FkCascadeRegistry {
  const registry = new FkCascadeRegistry()
  for (const ddl of ddls) {
    const stmt = parseSync(ddl).stmts[0].stmt.CreateStmt
    recordCreateTableForeignKeys(stmt, registry, resolve)
  }
  return registry
}

beforeAll(async () => {
  await loadModule()
})

describe('fk-cascade expansion (pure)', () => {
  const THREAD = `CREATE TABLE public.thread (id text PRIMARY KEY)`
  const MESSAGE = `CREATE TABLE public.message (id text PRIMARY KEY, "threadId" text REFERENCES public.thread(id) ON DELETE CASCADE)`
  const REACTION = `CREATE TABLE public.reaction (id text PRIMARY KEY, "messageId" text REFERENCES public.message(id) ON DELETE CASCADE)`
  const BOOKMARK = `CREATE TABLE public.bookmark (id text PRIMARY KEY, "threadId" text REFERENCES public.thread(id) ON DELETE SET NULL)`

  it('captures inline + table-level cascade and set-null edges', () => {
    const registry = registryFromDdl(THREAD, MESSAGE, BOOKMARK)
    const children = registry.childrenOf('"public"."thread"')
    expect(children.map((c) => c.table).sort()).toEqual([
      '"public"."bookmark"',
      '"public"."message"',
    ])
    expect(children.find((c) => c.table === '"public"."message"')?.onDelete).toBe(
      'cascade'
    )
    expect(children.find((c) => c.table === '"public"."bookmark"')?.onDelete).toBe(
      'set-null'
    )
  })

  it('ignores no-action / restrict edges (no enforcement to expand)', () => {
    const registry = registryFromDdl(
      THREAD,
      `CREATE TABLE public.audit (id text PRIMARY KEY, "threadId" text REFERENCES public.thread(id))`
    )
    expect(registry.hasEdges).toBe(false)
  })

  it('emits leaves-first, set-based statements with the parent predicate nested', () => {
    const registry = registryFromDdl(THREAD, MESSAGE, REACTION, BOOKMARK)
    const out = expandDelete('"public"."thread"', `id = 'x'`, registry)
    expect(out).toEqual([
      `DELETE FROM "public"."reaction" WHERE "messageId" IN (SELECT "id" FROM "public"."message" WHERE "threadId" IN (SELECT "id" FROM "public"."thread" WHERE id = 'x'))`,
      `DELETE FROM "public"."message" WHERE "threadId" IN (SELECT "id" FROM "public"."thread" WHERE id = 'x')`,
      `UPDATE "public"."bookmark" SET "threadId" = NULL WHERE "threadId" IN (SELECT "id" FROM "public"."thread" WHERE id = 'x')`,
    ])
  })

  it('returns nothing for a childless / fk-free schema', () => {
    expect(expandDelete('"public"."thread"', null, new FkCascadeRegistry())).toEqual([])
  })
})

// the FK-bearing schema as authored. the registry is built from THIS — the same
// DDL orez parses at its strip site before discarding the constraint.
const SCHEMA = [
  `CREATE TABLE public.thread (id text PRIMARY KEY, title text)`,
  `CREATE TABLE public.message (id text PRIMARY KEY, "threadId" text REFERENCES public.thread(id) ON DELETE CASCADE, body text)`,
  `CREATE TABLE public.reaction (id text PRIMARY KEY, "messageId" text REFERENCES public.message(id) ON DELETE CASCADE, emoji text)`,
  `CREATE TABLE public.bookmark (id text PRIMARY KEY, "threadId" text REFERENCES public.thread(id) ON DELETE SET NULL, note text)`,
]

// orez strips every FK from the DDL before it reaches the store, so nothing
// cascades natively. mirror that exactly: the store tables are created WITHOUT
// constraints — the precise environment the expansion has to work in.
function stripForeignKeys(ddl: string): string {
  const stripped = ddl.replace(
    /\s+REFERENCES\s+[^\s(]+\s*\([^)]*\)(?:\s+ON DELETE\s+(?:CASCADE|SET NULL))?/gi,
    ''
  )
  return stripped
}

// the load-bearing proof: expanded child statements must flow through the REAL
// change-tracking triggers, because that capture log is exactly what zero-cache
// streams to clients. native engine cascade would bypass these triggers; explicit
// expanded deletes must not.
describe('fk-cascade through real change tracking', () => {
  let db: PGlite

  beforeEach(async () => {
    db = new PGlite()
    await db.waitReady
    for (const ddl of SCHEMA) await db.exec(stripForeignKeys(ddl))
    await installChangeTracking(db)
    await db.exec(`
      INSERT INTO public.thread (id, title) VALUES ('t1', 'keep me'), ('t2', 'delete me');
      INSERT INTO public.message (id, "threadId", body) VALUES
        ('m1', 't2', 'a'), ('m2', 't2', 'b'), ('m3', 't1', 'untouched');
      INSERT INTO public.reaction (id, "messageId", emoji) VALUES
        ('r1', 'm1', '👍'), ('r2', 'm1', '🎉'), ('r3', 'm2', '🔥');
      INSERT INTO public.bookmark (id, "threadId", note) VALUES
        ('b1', 't2', 'on doomed thread'), ('b2', 't1', 'on kept thread');
    `)
  }, 60_000)

  afterEach(async () => {
    await db.close()
  })

  async function rowCount(table: string): Promise<number> {
    const r = await db.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM public.${table}`
    )
    return r.rows[0].n
  }

  it('NEGATIVE CONTROL: a bare parent delete orphans children and captures only the parent delete', async () => {
    const watermark = await currentWatermark(db)
    await db.exec(`DELETE FROM public.thread WHERE id = 't2'`)

    // children are orphaned — this is the bug the expansion fixes
    expect(await rowCount('message')).toBe(3)
    expect(await rowCount('reaction')).toBe(3)

    const changes = await getChangesSince(db, watermark)
    expect(changes).toHaveLength(1)
    expect(changes[0]).toMatchObject({ op: 'DELETE', table_name: 'public.thread' })
  }, 60_000)

  it('expanded delete cascades the graph AND every child deletion is captured for replication', async () => {
    const registry = registryFromDdl(...SCHEMA)
    const watermark = await currentWatermark(db)

    const statements = [
      ...expandDelete('"public"."thread"', `id = 't2'`, registry),
      `DELETE FROM "public"."thread" WHERE id = 't2'`,
    ]
    // run the whole cascade in one transaction (atomic; one commit, not N)
    await db.exec('BEGIN')
    for (const sql of statements) await db.exec(sql)
    await db.exec('COMMIT')

    // graph state matches PG ON DELETE semantics exactly
    expect(await rowCount('thread')).toBe(1) // t1 kept
    expect(await rowCount('message')).toBe(1) // only m3 (on t1) survives
    expect(await rowCount('reaction')).toBe(0) // r1,r2,r3 all hung off deleted messages
    expect(await rowCount('bookmark')).toBe(2) // set-null keeps the row…
    const orphanBookmark = await db.query<{ threadId: string | null }>(
      `SELECT "threadId" FROM public.bookmark WHERE id = 'b1'`
    )
    expect(orphanBookmark.rows[0].threadId).toBeNull() // …with the link nulled

    // and the capture log — what a zero client consumes — reflects all of it
    const changes = await getChangesSince(db, watermark)
    const byTable = (table: string, op: string) =>
      changes.filter((c) => c.table_name === table && c.op === op).length

    expect(byTable('public.reaction', 'DELETE')).toBe(3)
    expect(byTable('public.message', 'DELETE')).toBe(2)
    expect(byTable('public.thread', 'DELETE')).toBe(1)
    expect(byTable('public.bookmark', 'UPDATE')).toBe(1)
    // nothing touched on the kept thread's subtree
    expect(changes.some((c) => c.old_data?.id === 'm3' || c.old_data?.id === 'b2')).toBe(
      false
    )
  }, 60_000)
})

async function currentWatermark(db: PGlite): Promise<number> {
  const r = await db.query<{ last_value: string; is_called: boolean }>(
    'SELECT last_value, is_called FROM _orez._zero_watermark'
  )
  const { last_value, is_called } = r.rows[0]
  return is_called ? Number(last_value) : 0
}
