#!/usr/bin/env bun
/**
 * orez load testing harness.
 *
 * Starts a fresh orez instance with zero-cache and runs configurable load
 * scenarios against it. Measures throughput, latency, memory, and replication
 * health. Outputs structured JSON for analysis.
 *
 * Usage:
 *   bun run perf/load/harness.ts                          # default: 60s mixed workload
 *   bun run perf/load/harness.ts --scenario=basic-crud     # specific scenario
 *   bun run perf/load/harness.ts --duration=300 --concurrency=10
 *   bun run perf/load/harness.ts --single-db --wasm        # singleDb + WASM mode
 *   bun run perf/load/harness.ts --output=report.json       # write JSON report
 *   bun run perf/load/harness.ts --profile-memory           # take heap snapshots
 *
 * Scenarios (defined inline):
 *   basic-crud       — simple INSERT/SELECT/DELETE at concurrency N
 *   replication      — measure mutation → WS poke latency
 *   connection-churn — rapid connect/disconnect cycles
 *   mixed            — realistic mix of reads/writes/replication
 *   sustained        — long-running steady load with periodic health checks
 */

import { existsSync, mkdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'

import postgres from 'postgres'
import WebSocket from 'ws'

// ---- config ----

interface HarnessConfig {
  scenario: string
  durationSec: number
  concurrency: number
  singleDb: boolean
  forceWasm: boolean
  outputFile: string | null
  profileMemory: boolean
  dataDir: string
}

function parseArgs(): HarnessConfig {
  const args = process.argv.slice(2)
  const get = (key: string) => {
    const arg = args.find((a) => a.startsWith(`--${key}=`))
    return arg?.split('=')[1] ?? undefined
  }
  const has = (key: string) => args.includes(`--${key}`)

  return {
    scenario: get('scenario') || 'mixed',
    durationSec: parseInt(get('duration') || '60', 10),
    concurrency: parseInt(get('concurrency') || '10', 10),
    singleDb: has('single-db'),
    forceWasm: has('wasm'),
    outputFile: get('output') || null,
    profileMemory: has('profile-memory'),
    dataDir: resolve(tmpdir(), `orez-load-test-${Date.now()}`),
  }
}

// ---- helpers ----

function log(msg: string) {
  const ts = new Date().toISOString().slice(11, 23)
  console.log(`\x1b[2m[${ts}]\x1b[0m \x1b[1m\x1b[36m[harness]\x1b[0m ${msg}`)
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)}GB`
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms.toFixed(1)}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`
  return `${(ms / 60000).toFixed(1)}m`
}

function now(): number {
  return performance.now()
}

// ---- memory profiling ----

interface MemorySnapshot {
  timestamp: number
  elapsedSec: number
  rss: number
  heapTotal: number
  heapUsed: number
  external: number
  arrayBuffers: number
}

function takeMemorySnapshot(elapsedSec: number): MemorySnapshot {
  const mem = process.memoryUsage()
  return {
    timestamp: Date.now(),
    elapsedSec: Math.round(elapsedSec),
    rss: mem.rss,
    heapTotal: mem.heapTotal,
    heapUsed: mem.heapUsed,
    external: mem.external,
    arrayBuffers: mem.arrayBuffers,
  }
}

// ---- metrics collection ----

interface RunMetrics {
  scenario: string
  config: {
    durationSec: number
    concurrency: number
    singleDb: boolean
    forceWasm: boolean
  }
  startTime: string
  endTime: string
  durationMs: number
  queries: {
    total: number
    reads: number
    writes: number
    errors: number
  }
  latency: {
    p50: number
    p95: number
    p99: number
    max: number
    mean: number
  }
  replication: {
    mutations: number
    pokes: number
    p50LatencyMs: number
    p95LatencyMs: number
    p99LatencyMs: number
    missedPokes: number
  }
  connections: {
    peak: number
    totalOpened: number
    totalClosed: number
  }
  memory: {
    snapshots: MemorySnapshot[]
    peakRss: number
    peakHeapUsed: number
    finalRss: number
    finalHeapUsed: number
  }
  errors: string[]
}

function createMetrics(config: HarnessConfig): RunMetrics {
  return {
    scenario: config.scenario,
    config: {
      durationSec: config.durationSec,
      concurrency: config.concurrency,
      singleDb: config.singleDb,
      forceWasm: config.forceWasm,
    },
    startTime: new Date().toISOString(),
    endTime: '',
    durationMs: 0,
    queries: { total: 0, reads: 0, writes: 0, errors: 0 },
    latency: { p50: 0, p95: 0, p99: 0, max: 0, mean: 0 },
    replication: {
      mutations: 0,
      pokes: 0,
      p50LatencyMs: 0,
      p95LatencyMs: 0,
      p99LatencyMs: 0,
      missedPokes: 0,
    },
    connections: { peak: 0, totalOpened: 0, totalClosed: 0 },
    memory: {
      snapshots: [],
      peakRss: 0,
      peakHeapUsed: 0,
      finalRss: 0,
      finalHeapUsed: 0,
    },
    errors: [],
  }
}

function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0
  const sorted = [...values].sort((a, b) => a - b)
  const idx = Math.ceil((p / 100) * sorted.length) - 1
  return sorted[Math.max(0, idx)]
}

// ---- scenario: basic-crud ----

async function runBasicCrud(
  pgPort: number,
  metrics: RunMetrics,
  config: HarnessConfig
): Promise<void> {
  const latencies: number[] = []

  async function worker(id: number) {
    const sql = postgres({
      host: '127.0.0.1',
      port: pgPort,
      database: 'postgres',
      username: 'user',
      password: 'password',
      max: 1,
      idle_timeout: 10,
      connect_timeout: 5,
      no_subscribe: true,
    })

    try {
      // ensure test table
      await sql.unsafe(`
        CREATE TABLE IF NOT EXISTS load_test_items (
          id SERIAL PRIMARY KEY,
          worker_id INTEGER,
          value TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `)

      const endTime = Date.now() + config.durationSec * 1000
      let iter = 0

      while (Date.now() < endTime) {
        iter++

        // write
        const wStart = now()
        try {
          await sql.unsafe(
            `INSERT INTO load_test_items (worker_id, value) VALUES ($1, $2)`,
            [id, `worker-${id}-iter-${iter}-${Math.random().toString(36).slice(2, 10)}`]
          )
          metrics.queries.writes++
        } catch (e: any) {
          metrics.queries.errors++
          metrics.errors.push(`write error w${id}: ${e.message}`)
        }
        const wLat = now() - wStart
        latencies.push(wLat)
        metrics.queries.total++

        // read
        const rStart = now()
        try {
          const result = await sql.unsafe(
            `SELECT count(*) as cnt FROM load_test_items WHERE worker_id = $1`,
            [id]
          )
          metrics.queries.reads++
        } catch (e: any) {
          metrics.queries.errors++
          metrics.errors.push(`read error w${id}: ${e.message}`)
        }
        const rLat = now() - rStart
        latencies.push(rLat)
        metrics.queries.total++

        // small delay to not hammer too hard
        await new Promise((r) => setTimeout(r, 10))
      }
    } finally {
      await sql.end().catch(() => {})
    }
  }

  const workers = Array.from({ length: config.concurrency }, (_, i) => worker(i))
  await Promise.all(workers)

  metrics.latency = {
    p50: percentile(latencies, 50),
    p95: percentile(latencies, 95),
    p99: percentile(latencies, 99),
    max: latencies.length > 0 ? Math.max(...latencies) : 0,
    mean:
      latencies.length > 0 ? latencies.reduce((a, b) => a + b, 0) / latencies.length : 0,
  }
}

// ---- scenario: replication ----

async function runReplication(
  pgPort: number,
  zeroPort: number,
  metrics: RunMetrics,
  config: HarnessConfig
): Promise<void> {
  // connect a websocket client to zero-cache and measure mutation→poke latency
  const wsUrl = `ws://127.0.0.1:${zeroPort}/sync/v0`

  // connect to pg for writes
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
    // create test schema
    await sql.unsafe(`
      CREATE TABLE IF NOT EXISTS repl_test (
        id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        num INTEGER DEFAULT 0
      )
    `)

    // wait a bit for zero-cache to pick up the schema
    await new Promise((r) => setTimeout(r, 2000))

    // connect websocket with sync protocol
    const ws = new WebSocket(wsUrl)
    const pokeLatencies: number[] = []
    let pokeCount = 0
    let mutationCount = 0
    const pendingMutations = new Map<string, number>() // mutationId → timestamp

    const wsReady = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('WS connection timeout')), 15000)

      ws.on('open', () => {
        // send initConnection message
        const clientSchema = {
          tables: {
            repl_test: {
              columns: {
                id: { type: 'string' },
                value: { type: 'string' },
                num: { type: 'number' },
              },
              primaryKey: ['id'],
            },
          },
        }
        const initMsg = {
          initConnectionMessage: {
            desiredQueriesPatch: {
              queryID: '1',
              patch: {
                op: 'put',
                hash: 'test-hash',
                ast: {
                  schema: 'public',
                  table: 'repl_test',
                  orderBy: [['id', 'asc']],
                },
              },
            },
            clientSchema,
            protocolVersion: 49,
          },
        }
        ws.send(JSON.stringify(['initConnection', initMsg]))
      })

      ws.on('message', (data: Buffer) => {
        try {
          const msg = JSON.parse(data.toString())
          if (msg[0] === 'initConnection') {
            clearTimeout(timeout)
            resolve()
          }
          if (msg[0] === 'poke' || (Array.isArray(msg) && msg[0] === 'pokeStart')) {
            pokeCount++
            metrics.replication.pokes++

            // try to match with pending mutations
            const pokeData = Array.isArray(msg[0]) ? msg[1] : msg
            if (pokeData?.pokeStart?.baseCookie) {
              // got a poke — measure latency
              const now_ = Date.now()
              for (const [mutationId, sentAt] of pendingMutations) {
                const latency = now_ - sentAt
                pokeLatencies.push(latency)
                pendingMutations.delete(mutationId)
              }
            }
          }
        } catch {}
      })

      ws.on('error', (err) => {
        clearTimeout(timeout)
        reject(err)
      })
    })

    await wsReady
    log('WS connection established, starting replication test')

    // now do mutations and measure
    const endTime = Date.now() + config.durationSec * 1000

    while (Date.now() < endTime) {
      const mutationId = `mut-${mutationCount++}-${Math.random().toString(36).slice(2, 8)}`
      const sentAt = Date.now()

      try {
        await sql.unsafe(
          `INSERT INTO repl_test (id, value, num) VALUES ($1, $2, $3)
           ON CONFLICT (id) DO UPDATE SET value = $2, num = repl_test.num + 1`,
          [mutationId, `value-${mutationCount}`, mutationCount]
        )
        metrics.replication.mutations++
        metrics.queries.writes++
        metrics.queries.total++
        pendingMutations.set(mutationId, sentAt)
      } catch (e: any) {
        metrics.queries.errors++
        metrics.errors.push(`repl write error: ${e.message}`)
      }

      // small delay
      await new Promise((r) => setTimeout(r, 50))
    }

    // wait for remaining pokes
    await new Promise((r) => setTimeout(r, 5000))

    // count missed pokes
    metrics.replication.missedPokes = pendingMutations.size

    if (pokeLatencies.length > 0) {
      metrics.replication.p50LatencyMs = percentile(pokeLatencies, 50)
      metrics.replication.p95LatencyMs = percentile(pokeLatencies, 95)
      metrics.replication.p99LatencyMs = percentile(pokeLatencies, 99)
    }

    ws.close()
  } finally {
    await sql.end().catch(() => {})
  }
}

// ---- scenario: connection-churn ----

async function runConnectionChurn(
  pgPort: number,
  metrics: RunMetrics,
  config: HarnessConfig
): Promise<void> {
  const endTime = Date.now() + config.durationSec * 1000

  async function churner(id: number) {
    while (Date.now() < endTime) {
      const sql = postgres({
        host: '127.0.0.1',
        port: pgPort,
        database: 'postgres',
        username: 'user',
        password: 'password',
        max: 1,
        idle_timeout: 1,
        connect_timeout: 5,
        no_subscribe: true,
      })

      try {
        metrics.connections.totalOpened++
        metrics.connections.peak = Math.max(
          metrics.connections.peak,
          metrics.connections.totalOpened - metrics.connections.totalClosed
        )

        await sql.unsafe('SELECT 1')
        metrics.queries.reads++
        metrics.queries.total++

        // randomly do a write
        if (Math.random() < 0.3) {
          await sql.unsafe('SELECT 1 as x')
          metrics.queries.writes++
          metrics.queries.total++
        }
      } catch (e: any) {
        metrics.queries.errors++
        metrics.errors.push(`churn error c${id}: ${e.message}`)
      } finally {
        await sql.end().catch(() => {})
        metrics.connections.totalClosed++
      }

      // random delay
      await new Promise((r) => setTimeout(r, 50 + Math.random() * 200))
    }
  }

  const churners = Array.from({ length: config.concurrency }, (_, i) => churner(i))
  await Promise.all(churners)
}

// ---- scenario: mixed ----

async function runMixed(
  pgPort: number,
  zeroPort: number,
  metrics: RunMetrics,
  config: HarnessConfig
): Promise<void> {
  // run a subset of workers doing CRUD and a subset doing replication checks
  const crudWorkers = Math.max(1, Math.floor(config.concurrency * 0.7))
  const replConcurrency = Math.max(1, config.concurrency - crudWorkers)

  log(`Mixed: ${crudWorkers} CRUD workers + ${replConcurrency} repl workers`)

  const crudCfg = { ...config, concurrency: crudWorkers }
  const replCfg = {
    ...config,
    concurrency: replConcurrency,
    durationSec: config.durationSec,
  }

  await Promise.all([
    runBasicCrud(pgPort, metrics, crudCfg),
    runReplication(pgPort, zeroPort, metrics, replCfg),
  ])
}

// ---- scenario: sustained ----

async function runSustained(
  pgPort: number,
  zeroPort: number,
  metrics: RunMetrics,
  config: HarnessConfig
): Promise<void> {
  log(`Sustained load for ${formatDuration(config.durationSec * 1000)}`)

  const healthCheckInterval = 30_000 // every 30s
  const healthChecks: Array<{ time: string; ok: boolean; error?: string }> = []
  let lastHealthCheck = 0

  const endTime = Date.now() + config.durationSec * 1000
  const sql = postgres({
    host: '127.0.0.1',
    port: pgPort,
    database: 'postgres',
    username: 'user',
    password: 'password',
    max: 3,
    idle_timeout: 30,
    connect_timeout: 5,
    no_subscribe: true,
  })

  try {
    await sql.unsafe(`
      CREATE TABLE IF NOT EXISTS sustained_test (
        id SERIAL PRIMARY KEY,
        ts TIMESTAMPTZ DEFAULT NOW(),
        worker_id INTEGER,
        payload TEXT
      )
    `)

    async function worker(id: number) {
      while (Date.now() < endTime) {
        try {
          await sql.unsafe(
            `INSERT INTO sustained_test (worker_id, payload) VALUES ($1, $2)`,
            [id, `data-${Date.now()}-${Math.random().toString(36).slice(2)}`]
          )
          metrics.queries.writes++
          metrics.queries.total++
        } catch (e: any) {
          metrics.queries.errors++
          if (metrics.errors.length < 100) {
            metrics.errors.push(`sustained w${id}: ${e.message}`)
          }
        }

        // occasional read
        if (Math.random() < 0.3) {
          try {
            await sql.unsafe(`SELECT count(*) FROM sustained_test`)
            metrics.queries.reads++
            metrics.queries.total++
          } catch {}
        }

        await new Promise((r) => setTimeout(r, 100))
      }
    }

    // run workers + periodic health checks
    const workers = Array.from({ length: config.concurrency }, (_, i) => worker(i))

    const healthLoop = async () => {
      while (Date.now() < endTime) {
        if (Date.now() - lastHealthCheck >= healthCheckInterval) {
          try {
            await sql.unsafe('SELECT 1')
            healthChecks.push({ time: new Date().toISOString(), ok: true })
          } catch (e: any) {
            healthChecks.push({
              time: new Date().toISOString(),
              ok: false,
              error: e.message,
            })
          }
          lastHealthCheck = Date.now()
        }
        await new Promise((r) => setTimeout(r, 1000))
      }
    }

    await Promise.all([...workers, healthLoop()])
  } finally {
    await sql.end().catch(() => {})
  }
}

// ---- main ----

async function main() {
  const config = parseArgs()
  const metrics = createMetrics(config)

  log(`Scenario: ${config.scenario}`)
  log(`Duration: ${config.durationSec}s, Concurrency: ${config.concurrency}`)
  log(
    `Mode: ${config.singleDb ? 'singleDb' : 'multi-instance'}, SQLite: ${config.forceWasm ? 'WASM' : 'native (default)'}`
  )
  log(`Data dir: ${config.dataDir}`)

  // clean data dir
  if (existsSync(config.dataDir)) {
    rmSync(config.dataDir, { recursive: true, force: true })
  }
  mkdirSync(config.dataDir, { recursive: true })

  // import orez
  const { startZeroLite } = await import('../../src/index.js')

  // memory snapshots
  if (config.profileMemory) {
    metrics.memory.snapshots.push(takeMemorySnapshot(0))
  }

  // start orez
  log('Starting orez...')
  const startTime = now()

  const orez = await startZeroLite({
    dataDir: config.dataDir,
    singleDb: config.singleDb,
    forceWasmSqlite: config.forceWasm,
    disableWasmSqlite: !config.forceWasm,
    logLevel: 'warn',
    pgPort: 0, // auto
    zeroPort: 0, // auto
    adminPort: 0, // disable admin
    useWorkerThreads: true,
  })

  const startupMs = now() - startTime
  log(
    `orez ready in ${startupMs.toFixed(0)}ms (pg=${orez.pgPort}, zero=${orez.zeroPort})`
  )

  if (config.profileMemory) {
    metrics.memory.snapshots.push(takeMemorySnapshot(startupMs / 1000))
  }

  // run scenario
  const runStart = now()
  const scenarioFns: Record<string, Function> = {
    'basic-crud': () => runBasicCrud(orez.pgPort, metrics, config),
    replication: () => runReplication(orez.pgPort, orez.zeroPort, metrics, config),
    'connection-churn': () => runConnectionChurn(orez.pgPort, metrics, config),
    mixed: () => runMixed(orez.pgPort, orez.zeroPort, metrics, config),
    sustained: () => runSustained(orez.pgPort, orez.zeroPort, metrics, config),
  }

  const runner = scenarioFns[config.scenario]
  if (!runner) {
    console.error(`Unknown scenario: ${config.scenario}`)
    console.error(`Available: ${Object.keys(scenarioFns).join(', ')}`)
    process.exit(1)
  }

  // memory profiling loop (parallel to scenario)
  let memoryTimer: ReturnType<typeof setInterval> | null = null
  if (config.profileMemory) {
    const startSec = startupMs / 1000
    memoryTimer = setInterval(() => {
      const elapsed = (now() - startTime) / 1000
      const snap = takeMemorySnapshot(elapsed)

      // track peaks
      metrics.memory.peakRss = Math.max(metrics.memory.peakRss, snap.rss)
      metrics.memory.peakHeapUsed = Math.max(metrics.memory.peakHeapUsed, snap.heapUsed)

      metrics.memory.snapshots.push(snap)
    }, 5000) // every 5 seconds
  }

  await runner()

  if (memoryTimer) clearInterval(memoryTimer)

  const runMs = now() - runStart
  metrics.durationMs = Math.round(runMs)
  metrics.endTime = new Date().toISOString()

  // final memory snapshot
  const finalMem = process.memoryUsage()
  metrics.memory.finalRss = finalMem.rss
  metrics.memory.finalHeapUsed = finalMem.heapUsed
  metrics.memory.peakRss = Math.max(metrics.memory.peakRss, finalMem.rss)
  metrics.memory.peakHeapUsed = Math.max(metrics.memory.peakHeapUsed, finalMem.heapUsed)

  if (config.profileMemory) {
    metrics.memory.snapshots.push(takeMemorySnapshot((now() - startTime) / 1000))
  }

  // stop orez
  log('Stopping orez...')
  await orez.stop()
  log('Stopped.')

  // print summary
  printSummary(metrics, startupMs)

  // write output
  if (config.outputFile) {
    writeFileSync(config.outputFile, JSON.stringify(metrics, null, 2))
    log(`Report written to ${config.outputFile}`)
  }

  // clean up
  try {
    rmSync(config.dataDir, { recursive: true, force: true })
  } catch {}
}

function printSummary(metrics: RunMetrics, startupMs: number) {
  const qps =
    metrics.durationMs > 0
      ? ((metrics.queries.total / metrics.durationMs) * 1000).toFixed(1)
      : '0'

  console.log('\n' + '='.repeat(60))
  console.log('  LOAD TEST SUMMARY')
  console.log('='.repeat(60))
  console.log(`  Scenario:       ${metrics.scenario}`)
  console.log(`  Duration:       ${formatDuration(metrics.durationMs)}`)
  console.log(`  Startup time:   ${startupMs.toFixed(0)}ms`)
  console.log(`  Queries:        ${metrics.queries.total} (${qps} qps)`)
  console.log(`    Reads:        ${metrics.queries.reads}`)
  console.log(`    Writes:       ${metrics.queries.writes}`)
  console.log(`    Errors:       ${metrics.queries.errors}`)
  console.log(`  Latency:`)
  console.log(`    p50:         ${metrics.latency.p50.toFixed(1)}ms`)
  console.log(`    p95:         ${metrics.latency.p95.toFixed(1)}ms`)
  console.log(`    p99:         ${metrics.latency.p99.toFixed(1)}ms`)
  console.log(`    max:         ${metrics.latency.max.toFixed(1)}ms`)
  console.log(`    mean:        ${metrics.latency.mean.toFixed(1)}ms`)
  console.log(`  Replication:`)
  console.log(`    mutations:   ${metrics.replication.mutations}`)
  console.log(`    pokes:       ${metrics.replication.pokes}`)
  console.log(`    missed:      ${metrics.replication.missedPokes}`)
  if (metrics.replication.p50LatencyMs > 0) {
    console.log(`    p50 latency: ${metrics.replication.p50LatencyMs.toFixed(1)}ms`)
    console.log(`    p95 latency: ${metrics.replication.p95LatencyMs.toFixed(1)}ms`)
  }
  console.log(`  Connections:`)
  console.log(`    opened:      ${metrics.connections.totalOpened}`)
  console.log(`    closed:      ${metrics.connections.totalClosed}`)
  console.log(`    peak:        ${metrics.connections.peak}`)
  console.log(`  Memory (harness process):`)
  console.log(`    final RSS:   ${formatBytes(metrics.memory.finalRss)}`)
  console.log(`    peak RSS:    ${formatBytes(metrics.memory.peakRss)}`)
  console.log(`    peak heap:   ${formatBytes(metrics.memory.peakHeapUsed)}`)
  if (metrics.memory.snapshots.length > 1) {
    const first = metrics.memory.snapshots[0]
    const last = metrics.memory.snapshots[metrics.memory.snapshots.length - 1]
    const growth = last.rss - first.rss
    console.log(
      `    RSS growth:  ${growth > 0 ? '+' : ''}${formatBytes(growth)} (over ${formatDuration((last.elapsedSec - first.elapsedSec) * 1000)})`
    )
  }
  if (metrics.errors.length > 0) {
    console.log(`  Errors (${metrics.errors.length}):`)
    const unique = [...new Set(metrics.errors)].slice(0, 10)
    for (const e of unique) {
      console.log(`    - ${e.slice(0, 120)}`)
    }
    if (metrics.errors.length > 10) {
      console.log(`    ... and ${metrics.errors.length - 10} more`)
    }
  }
  console.log('='.repeat(60))
}

main().catch((err) => {
  console.error('Harness failed:', err)
  process.exit(1)
})
