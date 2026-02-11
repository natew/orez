/**
 * integration test for pg_restore through a running orez instance.
 *
 * generates a pg_dump-style SQL file, starts fresh orez, restores via wire
 * protocol, then verifies data via wire queries + zero-cache websocket sync.
 */

import { writeFileSync, unlinkSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { loadModule } from 'pgsql-parser'
import postgres from 'postgres'
import { describe, test, expect, beforeAll, afterAll } from 'vitest'

import { execDumpFile } from '../cli.js'
import { startZeroLite } from '../index.js'

import type { PGlite } from '@electric-sql/pglite'

// generate a pg_dump-style SQL file with our test schema + data
function generateDump(): string {
  const lines: string[] = []

  // preamble (mimics pg_dump)
  lines.push('SET statement_timeout = 0;')
  lines.push("SET client_encoding = 'UTF8';")
  lines.push('SET standard_conforming_strings = on;')
  lines.push('')

  // tables
  lines.push(`CREATE TABLE items (
  id integer NOT NULL,
  name text NOT NULL,
  data text,
  score integer DEFAULT 0
);`)
  lines.push('')
  lines.push(
    `CREATE SEQUENCE items_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;`
  )
  lines.push(`ALTER SEQUENCE items_id_seq OWNED BY items.id;`)
  lines.push(
    `ALTER TABLE ONLY items ALTER COLUMN id SET DEFAULT nextval('items_id_seq'::regclass);`
  )
  lines.push('')

  lines.push(`CREATE TABLE tags (
  id integer NOT NULL,
  item_id integer,
  label text NOT NULL
);`)
  lines.push('')
  lines.push(
    `CREATE SEQUENCE tags_id_seq AS integer START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;`
  )
  lines.push(`ALTER SEQUENCE tags_id_seq OWNED BY tags.id;`)
  lines.push(
    `ALTER TABLE ONLY tags ALTER COLUMN id SET DEFAULT nextval('tags_id_seq'::regclass);`
  )
  lines.push('')

  // view + function
  lines.push(`CREATE VIEW item_summary AS
  SELECT i.id, i.name, count(t.id) AS tag_count
  FROM items i LEFT JOIN tags t ON t.item_id = i.id
  GROUP BY i.id, i.name;`)
  lines.push('')
  lines.push(
    `CREATE FUNCTION item_count() RETURNS integer LANGUAGE sql AS $$SELECT count(*)::integer FROM items$$;`
  )
  lines.push('')

  // COPY items data (200 rows)
  lines.push('COPY items (id, name, data, score) FROM stdin;')
  for (let i = 0; i < 200; i++) {
    const id = i + 1
    const name = i % 7 === 0 ? `O'Brien's item #${i}` : `item-${i}`
    const data = i % 11 === 0 ? '\\N' : `data-${'x'.repeat(100)}-${i}`
    const score = i * 10
    // COPY text format: tab-separated, \N for NULL, backslash escapes
    lines.push(`${id}\t${escapeCopy(name)}\t${data}\t${score}`)
  }
  lines.push('\\.')
  lines.push('')

  // COPY tags data (50 rows)
  lines.push('COPY tags (id, item_id, label) FROM stdin;')
  for (let i = 0; i < 50; i++) {
    lines.push(`${i + 1}\t${(i % 200) + 1}\ttag-${i}`)
  }
  lines.push('\\.')
  lines.push('')

  // constraints (pg_dump adds these after data)
  lines.push('ALTER TABLE ONLY items ADD CONSTRAINT items_pkey PRIMARY KEY (id);')
  lines.push('ALTER TABLE ONLY tags ADD CONSTRAINT tags_pkey PRIMARY KEY (id);')
  lines.push(
    'ALTER TABLE ONLY tags ADD CONSTRAINT tags_item_id_fkey FOREIGN KEY (item_id) REFERENCES items(id);'
  )
  lines.push('')

  // sequence values
  lines.push(`SELECT pg_catalog.setval('items_id_seq', 200, true);`)
  lines.push(`SELECT pg_catalog.setval('tags_id_seq', 50, true);`)
  lines.push('')

  return lines.join('\n')
}

// escape a value for COPY text format
function escapeCopy(val: string): string {
  return val.replace(/\\/g, '\\\\').replace(/\t/g, '\\t').replace(/\n/g, '\\n')
}

describe('restore integration', { timeout: 120_000 }, () => {
  let db: PGlite
  let pgPort: number
  let zeroPort: number
  let shutdown: () => Promise<void>
  let dataDir: string
  let dumpFile: string

  beforeAll(async () => {
    await loadModule()

    // write dump file
    dumpFile = join(tmpdir(), `orez-restore-test-${Date.now()}.sql`)
    writeFileSync(dumpFile, generateDump())
    dataDir = `.orez-restore-test-${Date.now()}`

    // start orez without zero-cache (restore doesn't need sync)
    const fresh = await startZeroLite({
      pgPort: 25000 + Math.floor(Math.random() * 1000),
      zeroPort: 26000 + Math.floor(Math.random() * 1000),
      dataDir,
      logLevel: 'warn',
      skipZeroCache: true,
    })

    db = fresh.db
    pgPort = fresh.pgPort
    zeroPort = fresh.zeroPort
    shutdown = fresh.stop

    // restore via wire protocol
    const sql = postgres({
      host: '127.0.0.1',
      port: pgPort,
      user: 'user',
      password: 'password',
      database: 'postgres',
      max: 1,
    })

    const wireDb = { exec: (query: string) => sql.unsafe(query) as Promise<unknown> }
    const result = await execDumpFile(wireDb, dumpFile)
    console.log(
      `[restore-test] restored: ${result.executed} executed, ${result.skipped} skipped`
    )
    await sql.end()
  }, 60_000)

  afterAll(async () => {
    if (shutdown) await shutdown()
    if (dataDir) {
      try {
        rmSync(dataDir, { recursive: true, force: true })
      } catch {}
    }
    if (dumpFile) {
      try {
        unlinkSync(dumpFile)
      } catch {}
    }
  })

  test('tables exist and row counts match', async () => {
    const sql = wireClient()
    try {
      const items = await sql`SELECT count(*) as cnt FROM items`
      expect(Number(items[0].cnt)).toBe(200)

      const tags = await sql`SELECT count(*) as cnt FROM tags`
      expect(Number(tags[0].cnt)).toBe(50)
    } finally {
      await sql.end()
    }
  })

  test('data integrity preserved (quotes, nulls, large values)', async () => {
    const sql = wireClient()
    try {
      const quoted =
        await sql`SELECT name FROM items WHERE name LIKE ${"O'Brien%"} LIMIT 1`
      expect(quoted[0].name).toContain("O'Brien")

      const nulls = await sql`SELECT count(*) as cnt FROM items WHERE data IS NULL`
      expect(Number(nulls[0].cnt)).toBeGreaterThan(0)

      const scores = await sql`SELECT min(score) as lo, max(score) as hi FROM items`
      expect(Number(scores[0].lo)).toBe(0)
      expect(Number(scores[0].hi)).toBe(1990)
    } finally {
      await sql.end()
    }
  })

  test('views work after restore', async () => {
    const sql = wireClient()
    try {
      const summary = await sql`SELECT * FROM item_summary ORDER BY id LIMIT 3`
      expect(summary.length).toBe(3)
      expect(summary[0]).toHaveProperty('name')
      expect(summary[0]).toHaveProperty('tag_count')
    } finally {
      await sql.end()
    }
  })

  test('functions work after restore', async () => {
    const sql = wireClient()
    try {
      const result = await sql`SELECT item_count() as cnt`
      expect(Number(result[0].cnt)).toBe(200)
    } finally {
      await sql.end()
    }
  })

  test('foreign keys intact', async () => {
    const sql = wireClient()
    try {
      const joined =
        await sql`SELECT t.label, i.name FROM tags t JOIN items i ON i.id = t.item_id LIMIT 1`
      expect(joined.length).toBe(1)

      // FK enforced — inserting with nonexistent item_id should fail
      try {
        await sql`INSERT INTO tags (item_id, label) VALUES (99999, 'bad')`
        expect.unreachable('should have thrown FK violation')
      } catch (err: any) {
        expect(err.message).toContain('foreign key')
      }
    } finally {
      await sql.end()
    }
  })

  test('new inserts via wire protocol work after restore', async () => {
    const sql = wireClient()
    try {
      await sql`INSERT INTO items (name, score) VALUES ('post-restore', 9999)`
      const result = await sql`SELECT * FROM items WHERE name = 'post-restore'`
      expect(result.length).toBe(1)
      expect(Number(result[0].score)).toBe(9999)
    } finally {
      await sql.end()
    }
  })

  // --- helpers ---

  function wireClient() {
    return postgres({
      host: '127.0.0.1',
      port: pgPort,
      user: 'user',
      password: 'password',
      database: 'postgres',
      max: 1,
    })
  }
})
