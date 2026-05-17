#!/usr/bin/env bun
/**
 * orez long-term soak test.
 *
 * Runs orez under sustained load for an extended period (hours/days),
 * periodically checking health, measuring memory, and recording anomalies.
 * Designed to detect slow leaks, gradual degradation, and crash patterns.
 *
 * Usage:
 *   bun run perf/stability/soak.ts                      # default: 1 hour
 *   bun run perf/stability/soak.ts --duration=86400     # 24 hours
 *   bun run perf/stability/soak.ts --single-db          # constrained mode
 *   bun run perf/stability/soak.ts --report-interval=60  # report every 60s
 *   bun run perf/stability/soak.ts --no-cleanup          # keep data dir for forensics
 */

import { existsSync, mkdirSync, rmSync, writeFileSync, appendFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { tmpdir } from 'node:os'
import postgres from 'postgres'

// ---- config ----

interface SoakConfig {
  durationSec: number
  singleDb: boolean
  reportIntervalSec: number
  noCleanup: boolean
  dataDir: string
  reportDir: string
}

function parseArgs(): SoakConfig {
  const args = process.argv.slice(2)
  const get = (key: string) => {
    const arg = args.find((a) => a.startsWith(`--${key}=`))
    return arg?.split('=')[1] ?? undefined
  }
  const has = (key: string) => args.includes(`--${key}`)

  const ts = Date.now()
  return {
    durationSec: parseInt(get('duration') || '3600', 10), // 1 hour default
    singleDb: has('single-db'),
    reportIntervalSec: parseInt(get('report-interval') || '30', 10),
    noCleanup: has('no-cleanup'),
    dataDir: resolve(tmpdir(), `orez-soak-${ts}`),
    reportDir: resolve(import.meta.dirname!, '..', 'reports', `soak-${ts}`),
  }
}

// ---- helpers ----

function log(msg: string) {
  const ts = new Date().toISOString().slice(11, 23)
  const line = `\x1b[2m[${ts}]\x1b[0m \x1b[1m\x1b[33m[soak]\x1b[0m ${msg}`
  console.log(line)
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)}GB`
}

function formatDuration(sec: number): string {
  if (sec < 60) return `${sec}s`
  if (sec < 3600) return `${Math.floor(sec / 60)}m ${sec % 60}s`
  const h = Math.floor(sec / 3600)
  const m = Math.floor((sec % 3600) / 60)
  return `${h}h ${m}m`
}

// ---- report structures ----

interface SoakReport {
  config: { durationSec: number; singleDb: boolean }
  startTime: string
  endTime: string
  actualDurationSec: number
  cycles: HealthCycle[]
  anomalies: Anomaly[]
  crashes: CrashEvent[]
  finalStatus: 'completed' | 'crashed' | 'timeout' | 'aborted'
  summary: {
    totalQueries: number
    totalWrites: number
    totalReads: number
    totalErrors: number
    peakRss: number
    finalRss: number
    rssGrowth: number
    highestQueryLatencyMs: number
  }
}

interface HealthCycle {
  elapsedSec: number
  timestamp: string
  ok: boolean
  rss: number
  heapUsed: number
  queries: number
  writes: number
  errors: number
  avgQueryMs: number
  notes: string[]
}

interface Anomaly {
  elapsedSec: number
  timestamp: string
  type: 'memory_spike' | 'latency_spike' | 'error_burst' | 'connection_failure' | 'other'
  details: string
}

interface CrashEvent {
  elapsedSec: number
  timestamp: string
  recovered: boolean
  recoveryTimeMs: number
  details: string
}

// ---- main ----

async function main() {
  const config = parseArgs()
  const report: SoakReport = {
    config: { durationSec: config.durationSec, singleDb: config.singleDb },
    startTime: new Date().toISOString(),
    endTime: '',
    actualDurationSec: 0,
    cycles: [],
    anomalies: [],
    crashes: [],
    finalStatus: 'completed',
    summary: {
      totalQueries: 0,
      totalWrites: 0,
      totalReads: 0,
      totalErrors: 0,
      peakRss: 0,
      finalRss: 0,
      rssGrowth: 0,
      highestQueryLatencyMs: 0,
    },
  }

  // create dirs
  mkdirSync(config.reportDir, { recursive: true })
  if (existsSync(config.dataDir)) rmSync(config.dataDir, { recursive: true, force: true })
  mkdirSync(config.dataDir, { recursive: true })

  log(`Soak test starting: ${formatDuration(config.durationSec)}`)
  log(`Mode: ${config.singleDb ? 'singleDb' : 'multi-instance'}`)
  log(`Reports: ${config.reportDir}`)

  // start orez
  const { startZeroLite } = await import('../../src/index.js')

  let orez: Awaited<ReturnType<typeof startZeroLite>>
  try {
    orez = await startZeroLite({
      dataDir: config.dataDir,
      singleDb: config.singleDb,
      logLevel: 'error',
      pgPort: 0,
      zeroPort: 0,
      adminPort: 0,
    })
  } catch (e: any) {
    log(`Failed to start orez: ${e.message}`)
    report.finalStatus = 'crashed'
    report.anomalies.push({
      elapsedSec: 0,
      timestamp: new Date().toISOString(),
      type: 'other',
      details: `Startup failure: ${e.message}`,
    })
    saveReport(report, config)
    process.exit(1)
  }

  log(`orez ready (pg=${orez.pgPort}, zero=${orez.zeroPort})`)

  // connect to database
  const sql = postgres({
    host: '127.0.0.1',
    port: orez.pgPort,
    database: 'postgres',
    username: 'user',
    password: 'password',
    max: 3,
    idle_timeout: 30,
    connect_timeout: 5,
    no_subscribe: true,
  })

  try {
    // setup test tables
    await sql.unsafe(`
      CREATE TABLE IF NOT EXISTS soak_test (
        id SERIAL PRIMARY KEY,
        worker_id INTEGER,
        payload TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
      )
    `)

    // metrics counters
    let totalQueries = 0
    let totalWrites = 0
    let totalReads = 0
    let totalErrors = 0
    let peakRss = 0
    let highestLatency = 0
    const latencies: number[] = []
    const startTime = performance.now()

    // periodic report
    let lastReportTime = startTime
    const reportFile = resolve(config.reportDir, 'soak-report.json')
    const anomalyFile = resolve(config.reportDir, 'anomalies.jsonl')
    const timelineFile = resolve(config.reportDir, 'timeline.csv')
    writeFileSync(timelineFile, 'elapsedSec,rss,heapUsed,queries,writes,errors,avgLatencyMs\n')
    writeFileSync(anomalyFile, '') // clear

    const endTime = Date.now() + config.durationSec * 1000
    let iter = 0

    while (Date.now() < endTime) {
      iter++

      // do some writes
      try {
        const t0 = performance.now()
        await sql.unsafe(
          `INSERT INTO soak_test (worker_id, payload) VALUES ($1, $2)`,
          [iter % 10, `data-${iter}-${Math.random().toString(36).slice(2, 15)}`]
        )
        const lat = performance.now() - t0
        latencies.push(lat)
        highestLatency = Math.max(highestLatency, lat)
        totalWrites++
        totalQueries++
      } catch (e: any) {
        totalErrors++
        // record anomaly if burst of errors
        if (totalErrors > 0 && totalErrors % 50 === 0) {
          report.anomalies.push({
            elapsedSec: Math.round((performance.now() - startTime) / 1000),
            timestamp: new Date().toISOString(),
            type: 'error_burst',
            details: `${totalErrors} errors so far, latest: ${e.message}`,
          })
          appendFileSync(anomalyFile, JSON.stringify(report.anomalies[report.anomalies.length - 1]) + '\n')
        }
      }

      // occasional read
      if (iter % 3 === 0) {
        try {
          const t0 = performance.now()
          await sql.unsafe(`SELECT count(*) FROM soak_test`)
          const lat = performance.now() - t0
          latencies.push(lat)
          highestLatency = Math.max(highestLatency, lat)
          totalReads++
          totalQueries++
        } catch (e: any) {
          totalErrors++
        }
      }

      // health check every report interval
      const now_ = performance.now()
      const elapsed = (now_ - startTime) / 1000

      if (now_ - lastReportTime >= config.reportIntervalSec * 1000) {
        const mem = process.memoryUsage()
        peakRss = Math.max(peakRss, mem.rss)

        // check zero-cache health
        let zeroOk = false
        let notes: string[] = []
        try {
          const resp = await fetch(`http://127.0.0.1:${orez.zeroPort}/`, {
            signal: AbortSignal.timeout(5000),
          })
          zeroOk = resp.ok || resp.status === 404
        } catch (e: any) {
          notes.push(`zero-cache unreachable: ${e.message}`)
          report.anomalies.push({
            elapsedSec: Math.round(elapsed),
            timestamp: new Date().toISOString(),
            type: 'connection_failure',
            details: `zero-cache health check failed: ${e.message}`,
          })
          appendFileSync(anomalyFile, JSON.stringify(report.anomalies[report.anomalies.length - 1]) + '\n')
        }

        // check pg health
        let pgOk = false
        try {
          const r = await sql.unsafe('SELECT 1 as ok') as any[]
          pgOk = r[0]?.ok === 1
        } catch (e: any) {
          notes.push(`pg unreachable: ${e.message}`)
        }

        const avgLatency =
          latencies.length > 0
            ? latencies.reduce((a, b) => a + b, 0) / latencies.length
            : 0
        latencies.length = 0 // reset for next interval

        const cycle: HealthCycle = {
          elapsedSec: Math.round(elapsed),
          timestamp: new Date().toISOString(),
          ok: zeroOk && pgOk,
          rss: mem.rss,
          heapUsed: mem.heapUsed,
          queries: totalQueries,
          writes: totalWrites,
          errors: totalErrors,
          avgQueryMs: Math.round(avgLatency * 100) / 100,
          notes,
        }

        report.cycles.push(cycle)

        // append to timeline CSV
        appendFileSync(
          timelineFile,
          `${cycle.elapsedSec},${mem.rss},${mem.heapUsed},${totalQueries},${totalWrites},${totalErrors},${cycle.avgQueryMs}\n`
        )

        // detect memory spikes
        if (cycle.rss > peakRss * 1.3 && peakRss > 0) {
          report.anomalies.push({
            elapsedSec: Math.round(elapsed),
            timestamp: new Date().toISOString(),
            type: 'memory_spike',
            details: `RSS spiked to ${formatBytes(cycle.rss)} (peak was ${formatBytes(peakRss)})`,
          })
          appendFileSync(anomalyFile, JSON.stringify(report.anomalies[report.anomalies.length - 1]) + '\n')
        }

        // detect latency spikes
        if (cycle.avgQueryMs > 100) {
          report.anomalies.push({
            elapsedSec: Math.round(elapsed),
            timestamp: new Date().toISOString(),
            type: 'latency_spike',
            details: `avg query latency ${cycle.avgQueryMs}ms`,
          })
          appendFileSync(anomalyFile, JSON.stringify(report.anomalies[report.anomalies.length - 1]) + '\n')
        }

        // status update
        const remaining = config.durationSec - elapsed
        const status = zeroOk
          ? `✅ zc:ok`
          : `❌ zc:down`
        log(
          `t=${formatDuration(Math.round(elapsed))} (${formatDuration(Math.round(remaining))} left) ` +
            `RSS=${formatBytes(mem.rss)} queries=${totalQueries} errs=${totalErrors} ` +
            `avgLat=${cycle.avgQueryMs}ms ${status}`
        )

        lastReportTime = now_
      }

      // small delay between batches
      await new Promise((r) => setTimeout(r, 50))
    }

    // final snapshot
    const finalMem = process.memoryUsage()

    report.endTime = new Date().toISOString()
    report.actualDurationSec = Math.round((performance.now() - startTime) / 1000)
    report.summary = {
      totalQueries,
      totalWrites,
      totalReads,
      totalErrors,
      peakRss,
      finalRss: finalMem.rss,
      rssGrowth: finalMem.rss - (report.cycles[0]?.rss || finalMem.rss),
      highestQueryLatencyMs: highestLatency,
    }

    log('Soak test completed successfully')
    log(`Final: RSS=${formatBytes(finalMem.rss)}, errors=${totalErrors}`)
    log(`Anomalies: ${report.anomalies.length}, Crashes: ${report.crashes.length}`)
  } catch (e: any) {
    log(`Soak test error: ${e.message}`)
    report.finalStatus = 'aborted'
    report.endTime = new Date().toISOString()
  } finally {
    await sql.end().catch(() => {})
  }

  // stop orez
  try {
    await orez.stop()
  } catch {}

  // save final report
  saveReport(report, config)

  // clean up
  if (!config.noCleanup) {
    try {
      rmSync(config.dataDir, { recursive: true, force: true })
    } catch {}
  } else {
    log(`Data dir preserved: ${config.dataDir}`)
  }

  log(`Report saved to ${config.reportDir}`)

  // exit with non-zero if anomalies/warnings
  if (report.anomalies.length > 0) {
    log(`⚠️  ${report.anomalies.length} anomalies detected`)
    process.exit(2)
  }
}

function saveReport(report: SoakReport, config: SoakConfig) {
  const reportFile = resolve(config.reportDir, 'soak-report.json')
  writeFileSync(reportFile, JSON.stringify(report, null, 2))

  // also write a human-readable summary
  const summaryFile = resolve(config.reportDir, 'summary.md')
  const lines: string[] = [
    '# Soak Test Report',
    '',
    `- **Duration:** ${formatDuration(report.actualDurationSec)}`,
    `- **Mode:** ${report.config.singleDb ? 'singleDb' : 'multi-instance'}`,
    `- **Status:** ${report.finalStatus}`,
    `- **Start:** ${report.startTime}`,
    `- **End:** ${report.endTime}`,
    '',
    '## Summary',
    '',
    `| Metric | Value |`,
    `|--------|-------|`,
    `| Total queries | ${report.summary.totalQueries} |`,
    `| Total writes | ${report.summary.totalWrites} |`,
    `| Total reads | ${report.summary.totalReads} |`,
    `| Total errors | ${report.summary.totalErrors} |`,
    `| Peak RSS | ${formatBytes(report.summary.peakRss)} |`,
    `| Final RSS | ${formatBytes(report.summary.finalRss)} |`,
    `| RSS growth | ${formatBytes(report.summary.rssGrowth)} |`,
    `| Highest latency | ${report.summary.highestQueryLatencyMs.toFixed(1)}ms |`,
    '',
    `## Anomalies (${report.anomalies.length})`,
    '',
  ]

  for (const a of report.anomalies) {
    lines.push(`- **[${formatDuration(a.elapsedSec)}]** [${a.type}] ${a.details}`)
  }

  lines.push('', `## Crashes (${report.crashes.length})`, '')
  for (const c of report.crashes) {
    lines.push(
      `- **[${formatDuration(c.elapsedSec)}]** recovered=${c.recovered} (${c.recoveryTimeMs}ms): ${c.details}`
    )
  }

  lines.push(
    '',
    '## Health Cycles',
    '',
    '| Time | RSS | Queries | Errors | Avg Lat | OK |',
    '|------|-----|---------|--------|---------|----|'
  )
  for (const c of report.cycles) {
    lines.push(
      `| ${formatDuration(c.elapsedSec)} | ${formatBytes(c.rss)} | ${c.queries} | ${c.errors} | ${c.avgQueryMs}ms | ${c.ok ? '✅' : '❌'} |`
    )
  }

  writeFileSync(summaryFile, lines.join('\n'))
}

main().catch((err) => {
  console.error('Soak test failed:', err)
  process.exit(1)
})
