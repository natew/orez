#!/usr/bin/env bun
/**
 * orez performance micro-benchmarks.
 *
 * Measures specific hot-path operations to identify bottlenecks.
 * Runs both raw PGlite and through-proxy for comparison.
 *
 * Usage:
 *   bun run perf/scripts/bench-all.ts                    # all benchmarks
 *   bun run perf/scripts/bench-all.ts --single-db        # singleDb mode
 *   bun run perf/scripts/bench-all.ts --warmup=3 --runs=10
 *   bun run perf/scripts/bench-all.ts --output=bench.json
 */

import { existsSync, mkdirSync, rmSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { tmpdir } from 'node:os'
import postgres from 'postgres'

// ---- config ----

interface BenchConfig {
  singleDb: boolean
  warmupRuns: number
  measureRuns: number
  outputFile: string | null
  dataDir: string
}

function parseArgs(): BenchConfig {
  const args = process.argv.slice(2)
  const get = (key: string) => {
    const arg = args.find((a) => a.startsWith(`--${key}=`))
    return arg?.split('=')[1] ?? undefined
  }
  const has = (key: string) => args.includes(`--${key}`)

  return {
    singleDb: has('single-db'),
    warmupRuns: parseInt(get('warmup') || '3', 10),
    measureRuns: parseInt(get('runs') || '20', 10),
    outputFile: get('output') || null,
    dataDir: resolve(tmpdir(), `orez-bench-${Date.now()}`),
  }
}

function log(msg: string) {
  console.log(`\x1b[1m\x1b[36m[bench]\x1b[0m ${msg}`)
}

// ---- benchmark helpers ----

interface BenchResult {
  name: string
  runs: number
  totalMs: number
  minMs: number
  maxMs: number
  meanMs: number
  p50Ms: number
  p95Ms: number
  p99Ms: number
  opsPerSec: number
}

function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0
  const sorted = [...values].sort((a, b) => a - b)
  const idx = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[Math.max(0, idx)]
}

async function bench(
  name: string,
  fn: () => Promise<void>,
  config: BenchConfig
): Promise<BenchResult> {
  // warmup
  for (let i = 0; i < config.warmupRuns; i++) {
    await fn()
  }

  // measure
  const times: number[] = []
  for (let i = 0; i < config.measureRuns; i++) {
    const t0 = performance.now()
    await fn()
    const elapsed = performance.now() - t0
    times.push(elapsed)
  }

  const totalMs = times.reduce((a, b) => a + b, 0)
  return {
    name,
    runs: config.measureRuns,
    totalMs: Math.round(totalMs * 100) / 100,
    minMs: Math.round(Math.min(...times) * 100) / 100,
    maxMs: Math.round(Math.max(...times) * 100) / 100,
    meanMs: Math.round((totalMs / times.length) * 100) / 100,
    p50Ms: Math.round(percentile(times, 50) * 100) / 100,
    p95Ms: Math.round(percentile(times, 95) * 100) / 100,
    p99Ms: Math.round(percentile(times, 99) * 100) / 100,
    opsPerSec: Math.round((times.length / (totalMs / 1000)) * 100) / 100,
  }
}

// ---- benchmarks ----

async function runBenchmarks(config: BenchConfig): Promise<BenchResult[]> {
  const results: BenchResult[] = []

  // start orez
  const { startZeroLite } = await import('../../src/index.js')

  if (existsSync(config.dataDir)) rmSync(config.dataDir, { recursive: true, force: true })
  mkdirSync(config.dataDir, { recursive: true })

  const t0 = performance.now()
  const orez = await startZeroLite({
    dataDir: config.dataDir,
    singleDb: config.singleDb,
    logLevel: 'error',
    pgPort: 0,
    zeroPort: 0,
    adminPort: 0,
  })
  const startupMs = Math.round(performance.now() - t0)
  log(`orez ready in ${startupMs}ms (pg=${orez.pgPort})`)

  const sql = postgres({
    host: '127.0.0.1',
    port: orez.pgPort,
    database: 'postgres',
    username: 'user',
    password: 'password',
    max: 1,
    no_subscribe: true,
  })

  try {
    // setup
    await sql.unsafe(`
      CREATE TABLE IF NOT EXISTS bench_simple (
        id SERIAL PRIMARY KEY,
        text_val TEXT,
        int_val INTEGER,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `)

    // fill with data for SELECT benchmarks
    log('Seeding test data...')
    const batchSize = 100
    for (let batch = 0; batch < 10; batch++) {
      const values: string[] = []
      const params: any[] = []
      for (let i = 0; i < batchSize; i++) {
        params.push(`text-${batch}-${i}`, batch * batchSize + i)
        const base = params.length - 1
        values.push(`($${base}, $${base + 1})`)
      }
      await sql.unsafe(
        `INSERT INTO bench_simple (text_val, int_val) VALUES ${values.join(', ')}`,
        params
      )
    }
    log(`Seeded ${10 * batchSize} rows`)

    // --- Benchmark 1: Simple SELECT 1 ---
    log('\n--- Simple Queries ---')
    results.push(
      await bench(
        'SELECT 1',
        async () => {
          await sql.unsafe('SELECT 1')
        },
        config
      )
    )

    // --- Benchmark 2: SELECT count(*) ---
    results.push(
      await bench(
        'SELECT count(*)',
        async () => {
          await sql.unsafe('SELECT count(*) FROM bench_simple')
        },
        config
      )
    )

    // --- Benchmark 3: SELECT with WHERE + LIMIT ---
    results.push(
      await bench(
        'SELECT WHERE LIMIT 10',
        async () => {
          await sql.unsafe(
            `SELECT * FROM bench_simple WHERE int_val > $1 LIMIT 10`,
            [500]
          )
        },
        config
      )
    )

    // --- Benchmark 4: Simple INSERT ---
    log('\n--- Write Queries ---')
    let insertCounter = 10000
    results.push(
      await bench(
        'INSERT single row',
        async () => {
          const id = insertCounter++
          await sql.unsafe(
            `INSERT INTO bench_simple (text_val, int_val) VALUES ($1, $2)`,
            [`bench-insert-${id}`, id]
          )
        },
        config
      )
    )

    // --- Benchmark 5: UPDATE ---
    results.push(
      await bench(
        'UPDATE single row',
        async () => {
          await sql.unsafe(
            `UPDATE bench_simple SET text_val = $1 WHERE int_val = $2`,
            [`updated-${Date.now()}`, 500]
          )
        },
        config
      )
    )

    // --- Benchmark 6: INSERT + SELECT (transaction-like) ---
    log('\n--- Mixed Workloads ---')
    let mixedCounter = 20000
    results.push(
      await bench(
        'INSERT + SELECT',
        async () => {
          const id = mixedCounter++
          await sql.unsafe(
            `INSERT INTO bench_simple (text_val, int_val) VALUES ($1, $2)`,
            [`mixed-${id}`, id]
          )
          await sql.unsafe('SELECT count(*) FROM bench_simple')
        },
        config
      )
    )

    // --- Benchmark 7: Batch INSERT (10 rows) ---
    results.push(
      await bench(
        'INSERT batch (10 rows)',
        async () => {
          const values: string[] = []
          const params: any[] = []
          for (let i = 0; i < 10; i++) {
            const id = mixedCounter++
            params.push(`batch-${id}`, id)
            const base = params.length - 1
            values.push(`($${base}, $${base + 1})`)
          }
          await sql.unsafe(
            `INSERT INTO bench_simple (text_val, int_val) VALUES ${values.join(', ')}`,
            params
          )
        },
        config
      )
    )

    // --- Benchmark 8: Concurrent reads ---
    log('\n--- Concurrency ---')
    const concurrentSql = postgres({
      host: '127.0.0.1',
      port: orez.pgPort,
      database: 'postgres',
      username: 'user',
      password: 'password',
      max: 5,
      idle_timeout: 10,
      no_subscribe: true,
    })

    results.push(
      await bench(
        '5 concurrent SELECT 1',
        async () => {
          await Promise.all(
            Array.from({ length: 5 }, () => concurrentSql.unsafe('SELECT 1'))
          )
        },
        config
      )
    )

    results.push(
      await bench(
        '5 concurrent INSERT',
        async () => {
          const id = mixedCounter++
          await Promise.all(
            Array.from({ length: 5 }, (_, i) =>
              concurrentSql.unsafe(
                `INSERT INTO bench_simple (text_val, int_val) VALUES ($1, $2)`,
                [`concurrent-${id}-${i}`, id * 10 + i]
              )
            )
          )
        },
        config
      )
    )

    await concurrentSql.end()

  } finally {
    await sql.end().catch(() => {})
  }

  // stop orez
  await orez.stop()
  try { rmSync(config.dataDir, { recursive: true, force: true }) } catch {}

  return results
}

// ---- report ----

function printResults(results: BenchResult[]) {
  console.log('\n' + '='.repeat(80))
  console.log('  PERFORMANCE BENCHMARKS')
  console.log('='.repeat(80))
  console.log(
    '  Benchmark                          min      mean     p50      p95      p99      ops/s'
  )
  console.log('  ' + '-'.repeat(77))

  for (const r of results) {
    const name = r.name.padEnd(35)
    console.log(
      `  ${name} ${r.minMs.toFixed(1).padStart(7)}ms ${r.meanMs.toFixed(1).padStart(7)}ms ${r.p50Ms.toFixed(1).padStart(7)}ms ${r.p95Ms.toFixed(1).padStart(7)}ms ${r.p99Ms.toFixed(1).padStart(7)}ms ${r.opsPerSec.toFixed(0).padStart(7)}/s`
    )
  }

  console.log('='.repeat(80))

  // summary
  const simpleRead = results.find((r) => r.name === 'SELECT 1')
  const simpleWrite = results.find((r) => r.name === 'INSERT single row')
  const mixed = results.find((r) => r.name === 'INSERT + SELECT')
  const batch = results.find((r) => r.name === 'INSERT batch (10 rows)')
  const concurrentRead = results.find((r) => r.name === '5 concurrent SELECT 1')

  console.log('\nSummary:')
  if (simpleRead) console.log(`  Simple read:  ${simpleRead.p50Ms}ms p50, ${simpleRead.opsPerSec} ops/s`)
  if (simpleWrite) console.log(`  Simple write: ${simpleWrite.p50Ms}ms p50, ${simpleWrite.opsPerSec} ops/s`)
  if (mixed) console.log(`  Mixed r/w:    ${mixed.p50Ms}ms p50, ${mixed.opsPerSec} ops/s`)
  if (batch) console.log(`  Batch insert: ${batch.p50Ms}ms p50 (${(batch.opsPerSec * 10).toFixed(0)} rows/s)`)
  if (concurrentRead) console.log(`  Concurrent:   ${concurrentRead.p50Ms}ms p50 (5 parallel reads)`)
}

// ---- main ----

async function main() {
  const config = parseArgs()

  log(`Performance Benchmarks`)
  log(`Mode: ${config.singleDb ? 'singleDb' : 'multi-instance'}`)
  log(`Warmup: ${config.warmupRuns}, Measure: ${config.measureRuns}`)

  const results = await runBenchmarks(config)
  printResults(results)

  if (config.outputFile) {
    writeFileSync(config.outputFile, JSON.stringify(results, null, 2))
    log(`Results saved to ${config.outputFile}`)
  }
}

main().catch((err) => {
  console.error('Benchmarks failed:', err)
  process.exit(1)
})
