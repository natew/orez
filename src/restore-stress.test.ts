import { writeFileSync, unlinkSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { PGlite } from '@electric-sql/pglite'
import { loadModule } from 'pgsql-parser'
import postgres from 'postgres'
import { describe, it, expect, beforeAll, afterEach } from 'vitest'

import { execDumpFile } from './cli.js'
import { getConfig } from './config.js'
import { startPgProxy } from './pg-proxy.js'

import type { Server } from 'node:net'
import type { AddressInfo } from 'node:net'

// generate a pg_dump-style SQL file with COPY data
function generateDump(opts: {
  tables: number
  rowsPerTable: number
  columnsPerTable: number
}): string {
  const lines: string[] = []
  lines.push('-- pg_dump style output')
  lines.push('SET statement_timeout = 0;')
  lines.push("SET client_encoding = 'UTF8';")
  lines.push('')

  for (let t = 0; t < opts.tables; t++) {
    const tableName = `test_table_${t}`
    const cols = Array.from({ length: opts.columnsPerTable }, (_, i) => `col_${i} TEXT`)
    lines.push(`CREATE TABLE ${tableName} (id SERIAL PRIMARY KEY, ${cols.join(', ')});`)
    lines.push('')

    // COPY block
    const colNames = Array.from({ length: opts.columnsPerTable }, (_, i) => `col_${i}`)
    lines.push(`COPY ${tableName} (id, ${colNames.join(', ')}) FROM stdin;`)

    for (let r = 0; r < opts.rowsPerTable; r++) {
      const vals = Array.from({ length: opts.columnsPerTable }, (_, i) => {
        if (r % 17 === 0 && i === 0) return '\\N'
        if (r % 13 === 0 && i === 1) return `value with tab\\there`
        if (r % 11 === 0 && i === 2) return `O'Brien's "quoted" value\\nwith newline`
        return `row_${r}_col_${i}_${'x'.repeat(20)}`
      })
      lines.push(`${r + 1}\t${vals.join('\t')}`)
    }
    lines.push('\\.')
    lines.push('')
  }

  return lines.join('\n')
}

describe('restore stress', () => {
  let tmpFile: string

  beforeAll(async () => {
    await loadModule()
  })

  afterEach(() => {
    try {
      unlinkSync(tmpFile)
    } catch {}
  })

  it('direct: 5 tables x 500 rows', async () => {
    const dump = generateDump({ tables: 5, rowsPerTable: 500, columnsPerTable: 5 })
    tmpFile = join(tmpdir(), `orez-stress-${Date.now()}.sql`)
    writeFileSync(tmpFile, dump)

    const db = new PGlite({ relaxedDurability: true })
    await db.waitReady

    const { executed, skipped } = await execDumpFile(db, tmpFile)

    for (let t = 0; t < 5; t++) {
      const result = await db.query(`SELECT count(*) as cnt FROM test_table_${t}`)
      expect(Number(result.rows[0].cnt)).toBe(500)
    }

    console.log(`direct: ${executed} executed, ${skipped} skipped`)
    await db.close()
  }, 60_000)

  it('direct: 2 tables x 5000 rows (memory pressure)', async () => {
    const dump = generateDump({ tables: 2, rowsPerTable: 5000, columnsPerTable: 5 })
    tmpFile = join(tmpdir(), `orez-stress-large-${Date.now()}.sql`)
    writeFileSync(tmpFile, dump)

    const db = new PGlite({ relaxedDurability: true })
    await db.waitReady

    const { executed, skipped } = await execDumpFile(db, tmpFile)

    for (let t = 0; t < 2; t++) {
      const result = await db.query(`SELECT count(*) as cnt FROM test_table_${t}`)
      expect(Number(result.rows[0].cnt)).toBe(5000)
    }

    console.log(`large direct: ${executed} executed, ${skipped} skipped`)
    await db.close()
  }, 120_000)

  it('wire: 3 tables x 1000 rows through pg proxy', async () => {
    const dump = generateDump({ tables: 3, rowsPerTable: 1000, columnsPerTable: 5 })
    tmpFile = join(tmpdir(), `orez-stress-wire-${Date.now()}.sql`)
    writeFileSync(tmpFile, dump)

    const db = new PGlite({ relaxedDurability: true })
    await db.waitReady

    const config = { ...getConfig(), pgPort: 0 }
    const server: Server = await startPgProxy(db, config)
    const port = (server.address() as AddressInfo).port

    try {
      const sql = postgres({
        host: '127.0.0.1',
        port,
        user: 'user',
        password: 'password',
        database: 'postgres',
        max: 1,
      })

      const wireDb = { exec: (query: string) => sql.unsafe(query) as Promise<unknown> }
      const { executed, skipped } = await execDumpFile(wireDb, tmpFile)

      for (let t = 0; t < 3; t++) {
        const result =
          await sql`SELECT count(*) as cnt FROM test_table_${sql.unsafe(String(t))}`
        // use direct db to verify
      }

      // verify via direct PGlite
      for (let t = 0; t < 3; t++) {
        const result = await db.query(`SELECT count(*) as cnt FROM test_table_${t}`)
        expect(Number(result.rows[0].cnt)).toBe(1000)
      }

      console.log(`wire: ${executed} executed, ${skipped} skipped`)
      await sql.end()
    } finally {
      server.close()
      await db.close()
    }
  }, 120_000)

  it('wire: 2 tables x 3000 rows (memory pressure via proxy)', async () => {
    const dump = generateDump({ tables: 2, rowsPerTable: 3000, columnsPerTable: 8 })
    tmpFile = join(tmpdir(), `orez-stress-wire-large-${Date.now()}.sql`)
    writeFileSync(tmpFile, dump)

    const db = new PGlite({ relaxedDurability: true })
    await db.waitReady

    const config = { ...getConfig(), pgPort: 0 }
    const server: Server = await startPgProxy(db, config)
    const port = (server.address() as AddressInfo).port

    try {
      const sql = postgres({
        host: '127.0.0.1',
        port,
        user: 'user',
        password: 'password',
        database: 'postgres',
        max: 1,
      })

      const wireDb = { exec: (query: string) => sql.unsafe(query) as Promise<unknown> }
      const { executed, skipped } = await execDumpFile(wireDb, tmpFile)

      for (let t = 0; t < 2; t++) {
        const result = await db.query(`SELECT count(*) as cnt FROM test_table_${t}`)
        expect(Number(result.rows[0].cnt)).toBe(3000)
      }

      console.log(`wire large: ${executed} executed, ${skipped} skipped`)
      await sql.end()
    } finally {
      server.close()
      await db.close()
    }
  }, 180_000)
})
