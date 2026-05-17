#!/usr/bin/env bun
/**
 * measure proxy overhead: raw PGlite vs through-proxy queries.
 *
 * creates a PGlite instance directly and also starts the full orez proxy.
 * compares latency for identical queries to measure proxy overhead.
 *
 * Usage:
 *   bun run perf/scripts/bench-proxy-overhead.ts
 *   bun run perf/scripts/bench-proxy-overhead.ts --single-db
 */

import { existsSync, mkdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'

import { PGlite } from '@electric-sql/pglite'
import postgres from 'postgres'

function log(msg: string) {
  console.log(`\x1b[1m\x1b[35m[overhead]\x1b[0m ${msg}`)
}

interface OverheadResult {
  query: string
  rawMeanMs: number
  proxyMeanMs: number
  overheadMs: number
  overheadPct: number
  rawRuns: number
  proxyRuns: number
}

async function benchRaw(
  db: PGlite,
  query: string,
  params?: any[],
  runs = 50
): Promise<number> {
  // warmup
  for (let i = 0; i < 5; i++) {
    await db.query(query, params)
  }

  const times: number[] = []
  for (let i = 0; i < runs; i++) {
    const t0 = performance.now()
    await db.query(query, params)
    times.push(performance.now() - t0)
  }

  return times.reduce((a, b) => a + b, 0) / times.length
}

async function benchProxy(
  sql: ReturnType<typeof postgres>,
  query: string,
  params?: any[],
  runs = 50
): Promise<number> {
  // warmup
  for (let i = 0; i < 5; i++) {
    await sql.unsafe(query, params as any)
  }

  const times: number[] = []
  for (let i = 0; i < runs; i++) {
    const t0 = performance.now()
    await sql.unsafe(query, params as any)
    times.push(performance.now() - t0)
  }

  return times.reduce((a, b) => a + b, 0) / times.length
}

async function main() {
  const singleDb = process.argv.includes('--single-db')
  const dataDir = resolve(tmpdir(), `orez-overhead-${Date.now()}`)

  log(`Measuring proxy overhead (${singleDb ? 'singleDb' : 'multi-instance'})`)

  // start raw PGlite
  log('Starting raw PGlite...')
  const rawPg = new PGlite()
  await rawPg.waitReady
  await rawPg.exec(`
    CREATE TABLE IF NOT EXISTS overhead_test (
      id SERIAL PRIMARY KEY,
      val TEXT,
      num INTEGER
    )
  `)
  // seed
  for (let i = 0; i < 100; i++) {
    await rawPg.query(`INSERT INTO overhead_test (val, num) VALUES ($1, $2)`, [
      `raw-${i}`,
      i,
    ])
  }
  log('Raw PGlite ready')

  // start orez proxy
  log('Starting orez proxy...')
  if (existsSync(dataDir)) rmSync(dataDir, { recursive: true, force: true })
  mkdirSync(dataDir, { recursive: true })

  const { startZeroLite } = await import('../../src/index.js')
  const orez = await startZeroLite({
    dataDir,
    singleDb,
    logLevel: 'error',
    pgPort: 0,
    zeroPort: 0,
    adminPort: 0,
    skipZeroCache: true, // no zero-cache needed for proxy overhead test
  })
  log(`Proxy ready (pg=${orez.pgPort})`)

  const sql = postgres({
    host: '127.0.0.1',
    port: orez.pgPort,
    database: 'postgres',
    username: 'user',
    password: 'password',
    max: 1,
    no_subscribe: true,
  })

  // create same test table through proxy
  await sql.unsafe(`
    CREATE TABLE IF NOT EXISTS overhead_test (
      id SERIAL PRIMARY KEY,
      val TEXT,
      num INTEGER
    )
  `)
  // seed through proxy
  for (let i = 0; i < 100; i++) {
    await sql.unsafe(`INSERT INTO overhead_test (val, num) VALUES ($1, $2)`, [
      `proxy-${i}`,
      i,
    ])
  }

  // bench queries
  const testQueries: Array<{ name: string; sql: string; params?: any[] }> = [
    { name: 'SELECT 1', sql: 'SELECT 1' },
    { name: 'SELECT count(*)', sql: 'SELECT count(*) FROM overhead_test' },
    {
      name: 'SELECT WHERE eq',
      sql: 'SELECT * FROM overhead_test WHERE num = $1 LIMIT 1',
      params: [50],
    },
    {
      name: 'SELECT range',
      sql: 'SELECT * FROM overhead_test WHERE num > $1 AND num < $2',
      params: [25, 75],
    },
    {
      name: 'INSERT',
      sql: 'INSERT INTO overhead_test (val, num) VALUES ($1, $2)',
      params: ['bench', 999],
    },
    {
      name: 'UPDATE',
      sql: 'UPDATE overhead_test SET val = $1 WHERE num = $2',
      params: ['updated', 50],
    },
    {
      name: 'DELETE',
      sql: 'DELETE FROM overhead_test WHERE num = $1',
      params: [999],
    },
  ]

  const results: OverheadResult[] = []

  for (const q of testQueries) {
    log(`Benchmarking: ${q.name}...`)
    const rawMean = await benchRaw(rawPg, q.sql, q.params)
    const proxyMean = await benchProxy(sql, q.sql, q.params)
    const overhead = proxyMean - rawMean
    const overheadPct = rawMean > 0 ? (overhead / rawMean) * 100 : 0

    results.push({
      query: q.name,
      rawMeanMs: Math.round(rawMean * 1000) / 1000,
      proxyMeanMs: Math.round(proxyMean * 1000) / 1000,
      overheadMs: Math.round(overhead * 1000) / 1000,
      overheadPct: Math.round(overheadPct * 10) / 10,
      rawRuns: 50,
      proxyRuns: 50,
    })
  }

  // print results
  console.log('\n' + '='.repeat(75))
  console.log('  PROXY OVERHEAD ANALYSIS')
  console.log('='.repeat(75))
  console.log('  Query                      raw       proxy     overhead  %')
  console.log('  ' + '-'.repeat(70))

  for (const r of results) {
    const name = r.query.padEnd(25)
    console.log(
      `  ${name} ${r.rawMeanMs.toFixed(2).padStart(7)}ms ${r.proxyMeanMs.toFixed(2).padStart(7)}ms ${r.overheadMs >= 0 ? '+' : ''}${r.overheadMs.toFixed(2).padStart(7)}ms ${r.overheadPct.toFixed(1).padStart(5)}%`
    )
  }

  console.log('='.repeat(75))

  // assessment
  const avgOverhead = results.reduce((a, r) => a + r.overheadPct, 0) / results.length
  const maxOverhead = Math.max(...results.map((r) => r.overheadPct))

  console.log(`\nAverage overhead: ${avgOverhead.toFixed(1)}%`)
  console.log(`Max overhead: ${maxOverhead.toFixed(1)}%`)

  if (avgOverhead < 20) {
    console.log('Assessment: ✅ Acceptable overhead')
  } else if (avgOverhead < 50) {
    console.log('Assessment: 🟡 Moderate overhead — room for optimization')
  } else {
    console.log('Assessment: 🔴 High overhead — needs investigation')
  }

  // save
  const reportFile = resolve(
    import.meta.dirname!,
    '..',
    'reports',
    `proxy-overhead-${Date.now()}.json`
  )
  mkdirSync(resolve(import.meta.dirname!, '..', 'reports'), { recursive: true })
  writeFileSync(reportFile, JSON.stringify(results, null, 2))
  log(`Report: ${reportFile}`)

  // cleanup
  await sql.end().catch(() => {})
  await orez.stop()
  await rawPg.close()
  try {
    rmSync(dataDir, { recursive: true, force: true })
  } catch {}

  process.exit(avgOverhead > 50 ? 1 : 0)
}

main().catch((err) => {
  console.error('Overhead bench failed:', err)
  process.exit(1)
})
