import Database from '@rocicorp/zero-sqlite3'
/**
 * Integration test — drives the full pass pipeline against representative
 * chat-app workloads: CREATE TABLE + INSERT + SELECT + catalog probe, all
 * round-tripping through a real @rocicorp/zero-sqlite3 instance.
 *
 * Tests the END-TO-END story: "the compiler turns PG SQL into SQLite SQL
 * that executes correctly against DO storage, including the catalog probes
 * zero-cache makes on startup."
 */
import { describe, expect, it } from 'vitest'

import { buildCatalogTables } from './catalog/seed.js'
import { compile } from './index.js'

function rewriteParams(sql: string): string {
  return sql.replace(/\$\d+/g, '?')
}

describe('full compiler pipeline against chat-app workload', () => {
  it('removes PostgreSQL RESTRICT from DROP COLUMN', () => {
    expect(compile('ALTER TABLE plugin DROP COLUMN type RESTRICT').sql).toBe(
      'ALTER TABLE plugin \n  DROP COLUMN type'
    )
  })

  it('round-trip: schema → insert → select → catalog probe', () => {
    const db = new Database(':memory:')

    // 1. CREATE TABLE with PG types — types pass should make these all
    //    SQLite-valid and the DDL should execute.
    const ddlStatements = [
      `CREATE TABLE event (
        id BIGSERIAL PRIMARY KEY,
        user_id uuid NOT NULL,
        "createdAt" timestamp with time zone DEFAULT NOW() NOT NULL,
        payload jsonb NOT NULL DEFAULT '{}',
        tags text[] NOT NULL DEFAULT '[]'
      )`,
      `CREATE TABLE message (
        id text PRIMARY KEY,
        content text NOT NULL,
        sent_at timestamp DEFAULT NOW() NOT NULL,
        read boolean NOT NULL DEFAULT false
      )`,
    ]
    for (const s of ddlStatements) {
      const { sql, warnings } = compile(s)
      expect(warnings).toEqual([])
      db.exec(sql)
    }

    // 2. INSERT + SELECT roundtrip
    const { sql: insertSql } = compile(
      'INSERT INTO message (id, content) VALUES ($1, $2)'
    )
    db.prepare(rewriteParams(insertSql)).run('m1', 'hello world')

    const { sql: selectSql } = compile(
      'SELECT id, content, sent_at FROM message WHERE id = $1'
    )
    const rows = db.prepare(rewriteParams(selectSql)).all('m1') as any[]
    expect(rows).toHaveLength(1)
    expect(rows[0].id).toBe('m1')
    expect(rows[0].content).toBe('hello world')
    expect(String(rows[0].sent_at)).toMatch(/^\d{4}-\d{2}-\d{2}/)

    // 3. Catalog seed + probe — zero-cache on startup
    buildCatalogTables(db, { publications: ['orez_zero_public'] })

    const { sql: catalogSql } = compile(
      "SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'r' ORDER BY relname"
    )
    const catalogRows = db.prepare(rewriteParams(catalogSql)).all() as {
      relname: string
    }[]
    const tableNames = catalogRows.map((r) => r.relname)
    expect(tableNames).toContain('event')
    expect(tableNames).toContain('message')

    // 4. information_schema.columns probe
    const { sql: colSql } = compile(
      'SELECT column_name, data_type FROM information_schema.columns ' +
        "WHERE table_name = 'message' ORDER BY ordinal_position"
    )
    const cols = db.prepare(rewriteParams(colSql)).all() as {
      column_name: string
      data_type: string
    }[]
    expect(cols.map((c) => c.column_name)).toEqual(['id', 'content', 'sent_at', 'read'])

    // 5. pg_publication probe
    const { sql: pubSql } = compile(
      "SELECT pubname FROM pg_catalog.pg_publication WHERE pubname = 'orez_zero_public'"
    )
    const pubs = db.prepare(rewriteParams(pubSql)).all() as { pubname: string }[]
    expect(pubs).toHaveLength(1)
    expect(pubs[0].pubname).toBe('orez_zero_public')

    db.close()
  })

  it('insert with ON CONFLICT + RETURNING', () => {
    const db = new Database(':memory:')
    const { sql: ddl } = compile(
      'CREATE TABLE counter (key text PRIMARY KEY, count integer NOT NULL DEFAULT 0)'
    )
    db.exec(ddl)

    const { sql: insertSql } = compile(
      'INSERT INTO counter (key, count) VALUES ($1, 1) ' +
        'ON CONFLICT (key) DO UPDATE SET count = counter.count + 1 ' +
        'RETURNING key, count'
    )
    const r1 = db.prepare(rewriteParams(insertSql)).all('hits') as any[]
    expect(r1).toEqual([{ key: 'hits', count: 1 }])
    const r2 = db.prepare(rewriteParams(insertSql)).all('hits') as any[]
    expect(r2).toEqual([{ key: 'hits', count: 2 }])
    db.close()
  })

  it('UPDATE with NOW() in SET', () => {
    const db = new Database(':memory:')
    const { sql: ddl } = compile(
      'CREATE TABLE event (id text PRIMARY KEY, "updatedAt" timestamp)'
    )
    db.exec(ddl)
    db.prepare('INSERT INTO event (id) VALUES (?)').run('e1')

    const { sql: updateSql } = compile(
      'UPDATE event SET "updatedAt" = NOW() WHERE id = $1'
    )
    db.prepare(rewriteParams(updateSql)).run('e1')

    const row = db
      .prepare('SELECT id, "updatedAt" FROM event WHERE id = ?')
      .get('e1') as any
    expect(row.id).toBe('e1')
    expect(String(row.updatedAt)).toMatch(/^\d{4}-\d{2}-\d{2}/)
    db.close()
  })

  it('runs Chat trim and clock timestamp predicates', () => {
    const db = new Database(':memory:')
    db.exec(`
      CREATE TABLE "user" (id text PRIMARY KEY, email text NOT NULL);
      CREATE TABLE invite (
        id text PRIMARY KEY,
        email text NOT NULL,
        accepted integer NOT NULL,
        "expiresAt" text NOT NULL
      );
      INSERT INTO "user" VALUES ('u1', ' Person@example.com ');
      INSERT INTO invite VALUES (
        'i1',
        'person@example.com',
        0,
        '2999-01-01 00:00:00'
      );
    `)

    const { sql, warnings } = compile(`
      SELECT i.id
      FROM invite i
      JOIN "user" u ON lower(trim(u.email)) = lower(trim(i.email))
      WHERE u.id = $1
        AND i.accepted = false
        AND i."expiresAt" > clock_timestamp()
    `)
    expect(warnings).toEqual([])
    expect(sql).not.toMatch(/TRIM\s*\(\s*BOTH/i)
    expect(sql).not.toMatch(/clock_timestamp/i)
    expect(db.prepare(rewriteParams(sql)).all('u1')).toEqual([{ id: 'i1' }])
    db.close()
  })

  it('round-trips a sqlite keyword table name', () => {
    const db = new Database(':memory:')
    const { sql: ddl } = compile(
      'CREATE TABLE IF NOT EXISTS "transaction" (id text PRIMARY KEY, amount integer NOT NULL)'
    )
    db.exec(ddl)

    const { sql: indexSql } = compile(
      'CREATE INDEX "transaction_amount_idx" ON "transaction" (amount)'
    )
    db.exec(indexSql)

    const { sql: insertSql } = compile(
      'INSERT INTO "transaction" (id, amount) VALUES ($1, $2)'
    )
    db.prepare(rewriteParams(insertSql)).run('tx1', 42)

    const { sql: updateSql } = compile(
      'UPDATE "transaction" SET amount = $1 WHERE id = $2'
    )
    db.prepare(rewriteParams(updateSql)).run(64, 'tx1')

    const { sql: selectSql } = compile(
      'SELECT "transaction".id, amount FROM "transaction" WHERE id = $1'
    )
    const rows = db.prepare(rewriteParams(selectSql)).all('tx1') as any[]
    expect(rows).toEqual([{ id: 'tx1', amount: 64 }])

    const { sql: deleteSql } = compile('DELETE FROM "transaction" WHERE id = $1')
    db.prepare(rewriteParams(deleteSql)).run('tx1')

    const emptyRows = db.prepare(rewriteParams(selectSql)).all('tx1') as any[]
    expect(emptyRows).toEqual([])
    db.close()
  })
})
