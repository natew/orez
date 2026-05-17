#!/usr/bin/env bun
/**
 * Quick sanity check for orez changes.
 * Runs in ~15 seconds. Use for fast iteration.
 *
 * Usage:
 *   bun run perf/scripts/quick-check.ts
 *   bun run perf/scripts/quick-check.ts --single-db
 *   bun run perf/scripts/quick-check.ts --single-db --skip-zero
 */

import { rmSync, mkdirSync } from 'node:fs'
import { tmpdir } from 'node:os'

import postgres from 'postgres'

import { startZeroLite } from '../../src/index.js'
import { ensureTablesInPublications } from '../../src/integration/test-permissions.js'
import { installChangeTracking } from '../../src/replication/change-tracker.js'

const singleDb = process.argv.includes('--single-db')
const dir = tmpdir() + '/orez-quick-' + Date.now()
mkdirSync(dir)

const t0 = performance.now()
const orez = await startZeroLite({
  dataDir: dir,
  singleDb,
  logLevel: 'error',
  pgPort: 0,
  zeroPort: 0,
  adminPort: 0,
})
const startupMs = Math.round(performance.now() - t0)

const mem = process.memoryUsage()
const results: string[] = []

results.push(`startup: ${startupMs}ms`)
results.push(`RSS: ${Math.round(mem.rss / 1024 / 1024)}MB`)

// Setup
const db = orez.db
await db.exec('CREATE TABLE quick (id TEXT PRIMARY KEY, v TEXT, n INTEGER DEFAULT 0)')
await ensureTablesInPublications(db, ['quick'])
await installChangeTracking(db)

// Basic proxy ops
const sql = postgres({
  host: '127.0.0.1',
  port: orez.pgPort,
  database: 'postgres',
  username: 'user',
  password: 'password',
  max: 1,
  no_subscribe: true,
})

const t1 = performance.now()
await sql.unsafe('INSERT INTO quick (id, v, n) VALUES ($1, $2, $3)', ['k1', 'hello', 42])
results.push(`insert: ${Math.round(performance.now() - t1)}ms`)

const t2 = performance.now()
const r = (await sql.unsafe("SELECT * FROM quick WHERE id = 'k1'")) as any[]
results.push(`select: ${Math.round(performance.now() - t2)}ms, rows=${r.length}`)

// Check change tracking
const t3 = performance.now()
const changes = await db.query('SELECT count(*)::text as cnt FROM _orez._zero_changes')
results.push(
  `changes: ${changes.rows[0].cnt}, ct-check: ${Math.round(performance.now() - t3)}ms`
)

// Concurrent writes
const t4 = performance.now()
const inserts = []
for (let i = 0; i < 100; i++) {
  inserts.push(
    db.query('INSERT INTO quick (id, v, n) VALUES ($1, $2, $3)', [
      `conc-${i}`,
      `v${i}`,
      i,
    ])
  )
}
await Promise.all(inserts)
results.push(`100-concurrent-inserts: ${Math.round(performance.now() - t4)}ms`)

// Cleanup
await sql.end()
await new Promise((r) => setTimeout(r, 200))

const t5 = performance.now()
await Promise.race([orez.stop(), new Promise((r) => setTimeout(r, 5000))]).catch(() => {})
results.push(`stop: ${Math.round(performance.now() - t5)}ms`)

try {
  rmSync(dir, { recursive: true, force: true })
} catch {}

console.log('\n=== Quick Check ===')
for (const r of results) console.log(`  ${r}`)
console.log('===================')
