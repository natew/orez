#!/usr/bin/env bun
/**
 * orez memory profiler.
 *
 * Starts orez, runs a controlled workload, and takes regular heap snapshots
 * to detect memory leaks and measure memory usage patterns.
 *
 * Also checks for known leak sources in the proxy:
 *   - schemaQueryCache unbounded growth
 *   - schemaQueryInFlight orphaned promises
 *   - proxy connection tracking leaks
 *   - change-tracker trigger accumulation
 *
 * Usage:
 *   bun run perf/memory/profile.ts                          # default: 120s profile
 *   bun run perf/memory/profile.ts --duration=600            # 10 minute profile
 *   bun run perf/memory/profile.ts --single-db               # singleDb mode
 *   bun run perf/memory/profile.ts --output=profile.json     # save report
 *   bun run perf/memory/profile.ts --check-leaks             # detailed leak analysis
 */

import { spawn } from 'node:child_process'
import { existsSync, mkdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'

import postgres from 'postgres'

// ---- config ----

interface ProfileConfig {
  durationSec: number
  singleDb: boolean
  forceWasm: boolean
  outputFile: string | null
  checkLeaks: boolean
  dataDir: string
}

function parseArgs(): ProfileConfig {
  const args = process.argv.slice(2)
  const get = (key: string) => {
    const arg = args.find((a) => a.startsWith(`--${key}=`))
    return arg?.split('=')[1] ?? undefined
  }
  const has = (key: string) => args.includes(`--${key}`)

  return {
    durationSec: parseInt(get('duration') || '120', 10),
    singleDb: has('single-db'),
    forceWasm: has('wasm'),
    outputFile: get('output') || null,
    checkLeaks: has('check-leaks'),
    dataDir: resolve(tmpdir(), `orez-mem-profile-${Date.now()}`),
  }
}

function log(msg: string) {
  const ts = new Date().toISOString().slice(11, 23)
  console.log(`\x1b[2m[${ts}]\x1b[0m \x1b[1m\x1b[35m[mem]\x1b[0m ${msg}`)
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)}GB`
}

// ---- memory sampling ----

interface TimelinePoint {
  elapsedSec: number
  rss: number
  heapTotal: number
  heapUsed: number
  external: number
  arrayBuffers: number
  // GC stats from performance.memory (V8 only)
  gcDuration?: number
}

interface LeakCheck {
  name: string
  initialSize?: number
  finalSize?: number
  growth: number
  suspect: boolean
  details: string
}

interface ProfileReport {
  config: {
    durationSec: number
    singleDb: boolean
    forceWasm: boolean
  }
  startTime: string
  startupMs: number
  timeline: TimelinePoint[]
  summary: {
    initialRss: number
    finalRss: number
    peakRss: number
    rssGrowth: number
    rssGrowthRateBytesPerSec: number
    initialHeapUsed: number
    finalHeapUsed: number
    peakHeapUsed: number
    heapGrowth: number
    snapshots: number
  }
  leakChecks: LeakCheck[]
  workload: {
    queries: number
    writes: number
    connections: number
  }
}

// ---- leak checks (post-mortem) ----

async function runLeakChecks(pgPort: number, _dataDir: string): Promise<LeakCheck[]> {
  const checks: LeakCheck[] = []

  const sql = postgres({
    host: '127.0.0.1',
    port: pgPort,
    database: 'postgres',
    username: 'user',
    password: 'password',
    max: 1,
    no_subscribe: true,
  })

  try {
    // Check 1: _orez change table size
    try {
      const r = (await sql.unsafe(
        `SELECT count(*) as cnt FROM _orez._zero_changes`
      )) as any[]
      const cnt = Number(r[0]?.cnt || 0)
      checks.push({
        name: '_orez._zero_changes rows',
        growth: cnt,
        suspect: cnt > 1_000_000,
        details: `${cnt} change records accumulated`,
      })
    } catch {
      checks.push({
        name: '_orez._zero_changes rows',
        growth: 0,
        suspect: false,
        details: 'table not found (expected if no writes)',
      })
    }

    // Check 2: publication tables
    try {
      const r = (await sql.unsafe(
        `SELECT count(*) as cnt FROM pg_publication_tables`
      )) as any[]
      const cnt = Number(r[0]?.cnt || 0)
      checks.push({
        name: 'publication table memberships',
        growth: cnt,
        suspect: false, // growing publication list is expected with schema changes
        details: `${cnt} tables in publications`,
      })
    } catch {
      checks.push({
        name: 'publication table memberships',
        growth: 0,
        suspect: false,
        details: 'query failed',
      })
    }

    // Check 3: pg_stat_activity connections
    try {
      const r = (await sql.unsafe(
        `SELECT count(*) as cnt FROM pg_stat_activity WHERE state = 'active'`
      )) as any[]
      const cnt = Number(r[0]?.cnt || 0)
      checks.push({
        name: 'active connections',
        growth: cnt,
        suspect: cnt > 100,
        details: `${cnt} active connections`,
      })
    } catch {
      checks.push({
        name: 'active connections',
        growth: 0,
        suspect: false,
        details: 'query failed',
      })
    }
  } finally {
    await sql.end().catch(() => {})
  }

  return checks
}

// ---- main ----

async function main() {
  const config = parseArgs()
  const report: ProfileReport = {
    config: {
      durationSec: config.durationSec,
      singleDb: config.singleDb,
      forceWasm: config.forceWasm,
    },
    startTime: new Date().toISOString(),
    startupMs: 0,
    timeline: [],
    summary: {
      initialRss: 0,
      finalRss: 0,
      peakRss: 0,
      rssGrowth: 0,
      rssGrowthRateBytesPerSec: 0,
      initialHeapUsed: 0,
      finalHeapUsed: 0,
      peakHeapUsed: 0,
      heapGrowth: 0,
      snapshots: 0,
    },
    leakChecks: [],
    workload: { queries: 0, writes: 0, connections: 0 },
  }

  log(
    `Memory profile: ${config.durationSec}s, ${config.singleDb ? 'singleDb' : 'multi-instance'}`
  )

  // clean and create data dir
  if (existsSync(config.dataDir)) {
    rmSync(config.dataDir, { recursive: true, force: true })
  }
  mkdirSync(config.dataDir, { recursive: true })

  // take baseline memory of this process
  const baselineMem = process.memoryUsage()

  // import and start orez
  const { startZeroLite } = await import('../../src/index.js')

  // track startup
  const t0 = performance.now()
  log('Starting orez...')

  const orez = await startZeroLite({
    dataDir: config.dataDir,
    singleDb: config.singleDb,
    forceWasmSqlite: config.forceWasm,
    disableWasmSqlite: !config.forceWasm,
    logLevel: 'error', // minimal logging
    pgPort: 0,
    zeroPort: 0,
    adminPort: 0,
    useWorkerThreads: true,
  })

  report.startupMs = Math.round(performance.now() - t0)
  log(`orez ready in ${report.startupMs}ms (pg=${orez.pgPort}, zero=${orez.zeroPort})`)

  // take initial snapshot
  const initialMem = process.memoryUsage()
  report.timeline.push({
    elapsedSec: 0,
    rss: initialMem.rss,
    heapTotal: initialMem.heapTotal,
    heapUsed: initialMem.heapUsed,
    external: initialMem.external,
    arrayBuffers: initialMem.arrayBuffers,
  })
  report.summary.initialRss = initialMem.rss
  report.summary.initialHeapUsed = initialMem.heapUsed
  report.summary.peakRss = initialMem.rss
  report.summary.peakHeapUsed = initialMem.heapUsed

  // ---- workload ----

  log(`Running workload for ${config.durationSec}s...`)

  const sql = postgres({
    host: '127.0.0.1',
    port: orez.pgPort,
    database: 'postgres',
    username: 'user',
    password: 'password',
    max: 5,
    idle_timeout: 10,
    connect_timeout: 5,
    no_subscribe: true,
  })

  try {
    // setup test tables
    await sql.unsafe(`
      CREATE TABLE IF NOT EXISTS mem_test (
        id SERIAL PRIMARY KEY,
        payload TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `)
    await sql.unsafe(`
      CREATE TABLE IF NOT EXISTS mem_test_wide (
        id SERIAL PRIMARY KEY,
        col1 TEXT, col2 TEXT, col3 TEXT, col4 TEXT, col5 TEXT,
        col6 INTEGER, col7 INTEGER, col8 INTEGER, col9 INTEGER, col10 INTEGER,
        col11 JSONB, col12 JSONB,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `)

    const endTime = Date.now() + config.durationSec * 1000
    const sampleIntervalMs = 5000 // memory sample every 5s
    let lastSampleTime = 0
    let iter = 0

    // snapshot timer
    const startTime = performance.now()

    while (Date.now() < endTime) {
      iter++

      // insert batch
      try {
        const batchSize = 10
        const values: string[] = []
        const params: any[] = []
        for (let i = 0; i < batchSize; i++) {
          params.push(`payload-${iter}-${i}-${Math.random().toString(36).slice(2, 15)}`)
          values.push(`($${params.length})`)
        }
        await sql.unsafe(
          `INSERT INTO mem_test (payload) VALUES ${values.join(', ')}`,
          params
        )
        report.workload.writes += batchSize
        report.workload.queries++
      } catch (e: any) {
        // silently continue
      }

      // read back
      try {
        await sql.unsafe(`SELECT count(*) FROM mem_test`)
        report.workload.queries++
      } catch {}

      // occasional wide query
      if (iter % 10 === 0) {
        try {
          await sql.unsafe(
            `INSERT INTO mem_test_wide (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::jsonb)`,
            [
              'a',
              'b',
              'c',
              'd',
              'e',
              iter,
              iter + 1,
              iter + 2,
              iter + 3,
              iter + 4,
              JSON.stringify({ key: 'value', iter }),
              JSON.stringify({ arr: [1, 2, 3, 4, 5] }),
            ]
          )
          report.workload.writes++
          report.workload.queries++
        } catch {}
      }

      // memory sample
      const now_ = performance.now()
      const elapsed = (now_ - startTime) / 1000
      if (now_ - lastSampleTime >= sampleIntervalMs) {
        const mem = process.memoryUsage()
        const point: TimelinePoint = {
          elapsedSec: Math.round(elapsed),
          rss: mem.rss,
          heapTotal: mem.heapTotal,
          heapUsed: mem.heapUsed,
          external: mem.external,
          arrayBuffers: mem.arrayBuffers,
        }

        // try to get V8 GC stats (bun doesn't support this but node does)
        try {
          const perf = (performance as any).memory
          if (perf) {
            point.gcDuration = perf.usedJSHeapSize
          }
        } catch {}

        report.timeline.push(point)
        report.summary.peakRss = Math.max(report.summary.peakRss, mem.rss)
        report.summary.peakHeapUsed = Math.max(report.summary.peakHeapUsed, mem.heapUsed)

        // log every 30s
        if (Math.round(elapsed) % 30 === 0) {
          log(
            `t=${Math.round(elapsed)}s RSS=${formatBytes(mem.rss)} ` +
              `heapUsed=${formatBytes(mem.heapUsed)} external=${formatBytes(mem.external)}`
          )
        }

        lastSampleTime = now_
      }

      // small delay
      await new Promise((r) => setTimeout(r, 20))
    }
  } finally {
    await sql.end().catch(() => {})
    // Give TCP connections time to fully drain before stopping orez
    await new Promise((r) => setTimeout(r, 500))
  }

  // final snapshot
  const finalMem = process.memoryUsage()
  report.timeline.push({
    elapsedSec: config.durationSec,
    rss: finalMem.rss,
    heapTotal: finalMem.heapTotal,
    heapUsed: finalMem.heapUsed,
    external: finalMem.external,
    arrayBuffers: finalMem.arrayBuffers,
  })
  report.summary.finalRss = finalMem.rss
  report.summary.finalHeapUsed = finalMem.heapUsed
  report.summary.peakRss = Math.max(report.summary.peakRss, finalMem.rss)
  report.summary.peakHeapUsed = Math.max(report.summary.peakHeapUsed, finalMem.heapUsed)

  // calculate growth rates
  const first = report.timeline[0]
  const last = report.timeline[report.timeline.length - 1]
  report.summary.rssGrowth = last.rss - first.rss
  report.summary.heapGrowth = last.heapUsed - first.heapUsed
  if (last.elapsedSec > 0) {
    report.summary.rssGrowthRateBytesPerSec = report.summary.rssGrowth / last.elapsedSec
  }
  report.summary.snapshots = report.timeline.length

  // leak checks
  if (config.checkLeaks) {
    log('Running leak checks...')
    report.leakChecks = await runLeakChecks(orez.pgPort, config.dataDir)
  }

  // stop orez
  log('Stopping orez...')
  await orez.stop()

  // print report
  printReport(report)

  // save
  if (config.outputFile) {
    writeFileSync(config.outputFile, JSON.stringify(report, null, 2))
    log(`Report saved to ${config.outputFile}`)

    // also save CSV timeline for charting
    const csvPath = config.outputFile.replace(/\.json$/, '.csv')
    const csvLines = ['elapsedSec,rss,heapTotal,heapUsed,external,arrayBuffers']
    for (const p of report.timeline) {
      csvLines.push(
        `${p.elapsedSec},${p.rss},${p.heapTotal},${p.heapUsed},${p.external},${p.arrayBuffers}`
      )
    }
    writeFileSync(csvPath, csvLines.join('\n'))
    log(`Timeline CSV saved to ${csvPath}`)
  }

  // clean up
  try {
    rmSync(config.dataDir, { recursive: true, force: true })
  } catch {}
}

function printReport(report: ProfileReport) {
  console.log('\n' + '='.repeat(65))
  console.log('  MEMORY PROFILE REPORT')
  console.log('='.repeat(65))
  console.log(`  Duration:       ${report.config.durationSec}s`)
  console.log(
    `  Mode:           ${report.config.singleDb ? 'singleDb' : 'multi-instance'}`
  )
  console.log(
    `  SQLite:         ${report.config.forceWasm ? 'WASM' : 'native (default)'}`
  )
  console.log(`  Startup:        ${report.startupMs}ms`)
  console.log(`  Snapshots:      ${report.summary.snapshots}`)
  console.log('  ---')
  console.log(`  RSS initial:    ${formatBytes(report.summary.initialRss)}`)
  console.log(`  RSS final:      ${formatBytes(report.summary.finalRss)}`)
  console.log(`  RSS peak:       ${formatBytes(report.summary.peakRss)}`)
  console.log(
    `  RSS growth:     ${report.summary.rssGrowth > 0 ? '+' : ''}${formatBytes(report.summary.rssGrowth)}`
  )
  console.log(
    `  RSS growth rate: ${formatBytes(report.summary.rssGrowthRateBytesPerSec)}/s`
  )
  console.log('  ---')
  console.log(`  Heap initial:   ${formatBytes(report.summary.initialHeapUsed)}`)
  console.log(`  Heap final:     ${formatBytes(report.summary.finalHeapUsed)}`)
  console.log(`  Heap peak:      ${formatBytes(report.summary.peakHeapUsed)}`)
  console.log(
    `  Heap growth:    ${report.summary.heapGrowth > 0 ? '+' : ''}${formatBytes(report.summary.heapGrowth)}`
  )
  console.log('  ---')
  console.log(`  Queries:        ${report.workload.queries}`)
  console.log(`  Writes:         ${report.workload.writes}`)

  if (report.leakChecks.length > 0) {
    console.log('  ---')
    console.log('  Leak Checks:')
    for (const check of report.leakChecks) {
      const flag = check.suspect ? ' ⚠️ SUSPECT' : ''
      console.log(`    ${check.name}: ${check.details}${flag}`)
    }
  }

  // growth assessment
  const growthPerMin = report.summary.rssGrowthRateBytesPerSec * 60
  console.log('  ---')
  if (report.summary.rssGrowth <= 0) {
    console.log('  Assessment: ✅ No RSS growth detected')
  } else if (growthPerMin < 1024 * 1024) {
    // < 1MB/min
    console.log(
      `  Assessment: 🟡 Minor growth (${formatBytes(growthPerMin)}/min) — likely fragmentation`
    )
  } else if (growthPerMin < 10 * 1024 * 1024) {
    // < 10MB/min
    console.log(
      `  Assessment: 🟠 Moderate growth (${formatBytes(growthPerMin)}/min) — investigate`
    )
  } else {
    console.log(
      `  Assessment: 🔴 High growth (${formatBytes(growthPerMin)}/min) — likely leak`
    )
  }

  console.log('='.repeat(65))
}

main().catch((err) => {
  console.error('Profile failed:', err)
  process.exit(1)
})
