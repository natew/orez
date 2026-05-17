#!/usr/bin/env bun
/**
 * orez diagnostic script.
 *
 * Connects to a running orez instance and reports health, memory,
 * connection stats, replication state, and potential issues.
 *
 * Usage:
 *   bun run perf/scripts/diagnose.ts                    # auto-detect ports
 *   bun run perf/scripts/diagnose.ts --pg=6434 --zero=5849
 *   bun run perf/scripts/diagnose.ts --data-dir=.orez   # read from pid/admin files
 */

import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import postgres from 'postgres'

// ---- helpers ----

function log(msg: string) {
  console.log(`\x1b[1m\x1b[36m[diag]\x1b[0m ${msg}`)
}

function header(msg: string) {
  console.log(`\n\x1b[1m\x1b[33m--- ${msg} ---\x1b[0m`)
}

function ok(msg: string) {
  console.log(`  ✅ ${msg}`)
}

function warn(msg: string) {
  console.log(`  ⚠️  ${msg}`)
}

function err(msg: string) {
  console.log(`  ❌ ${msg}`)
}

function info(msg: string) {
  console.log(`  ℹ️  ${msg}`)
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)}GB`
}

// ---- auto-detect ports ----

function detectPorts(): { pgPort: number; zeroPort: number; adminPort: number } {
  const args = process.argv.slice(2)
  const get = (key: string) => {
    const arg = args.find((a) => a.startsWith(`--${key}=`))
    return arg?.split('=')[1]
  }

  let pgPort = parseInt(get('pg') || '0', 10)
  let zeroPort = parseInt(get('zero') || '0', 10)
  let adminPort = parseInt(get('admin') || '0', 10)

  // try to read from data dir
  const dataDirArg = get('data-dir')
  if (dataDirArg) {
    const dir = resolve(dataDirArg)
    try {
      const pidContent = readFileSync(resolve(dir, 'orez.pid'), 'utf8').trim()
      info(`Found PID file: ${pidContent}`)
    } catch {}

    try {
      const adminContent = readFileSync(resolve(dir, 'orez.admin'), 'utf8').trim()
      adminPort = parseInt(adminContent, 10)
      info(`Found admin port: ${adminPort}`)
    } catch {}

    try {
      const readyContent = readFileSync(resolve(dir, 'orez.ready'), 'utf8').trim()
      info(`Ready marker: ${new Date(Number(readyContent)).toISOString()}`)
    } catch {
      warn('No ready marker found — orez may not have finished starting')
    }
  }

  // try defaults
  if (!pgPort) pgPort = 6434
  if (!zeroPort) zeroPort = 5849

  return { pgPort, zeroPort, adminPort }
}

// ---- db queries ----

async function diagnosePg(pgPort: number) {
  header('PostgreSQL Proxy')

  const sql = postgres({
    host: '127.0.0.1',
    port: pgPort,
    database: 'postgres',
    username: 'user',
    password: 'password',
    max: 1,
    connect_timeout: 5,
    no_subscribe: true,
  })

  try {
    // basic connectivity
    try {
      const r = await sql.unsafe('SELECT 1 as ok') as any[]
      if (r[0]?.ok === 1) ok('Connected')
    } catch (e: any) {
      err(`Cannot connect: ${e.message}`)
      return
    }

    // version
    try {
      const r = await sql.unsafe('SELECT version()') as any[]
      info(`Version: ${r[0]?.version?.slice(0, 60) || 'unknown'}`)
    } catch {}

    // database size
    try {
      const r = await sql.unsafe(
        `SELECT pg_database_size(current_database()) as size`
      ) as any[]
      info(`Database size: ${formatBytes(Number(r[0]?.size || 0))}`)
    } catch {}

    // table count
    try {
      const r = await sql.unsafe(
        `SELECT count(*) as cnt FROM information_schema.tables WHERE table_schema = 'public'`
      ) as any[]
      info(`Public tables: ${r[0]?.cnt || 0}`)
    } catch {}

    // publication status
    try {
      const r = await sql.unsafe(
        `SELECT pubname FROM pg_publication`
      ) as any[]
      if (r.length > 0) {
        ok(`Publications: ${r.map((x: any) => x.pubname).join(', ')}`)
      } else {
        warn('No publications found')
      }
    } catch {}

    // change tracking status
    try {
      const r = await sql.unsafe(
        `SELECT count(*) as cnt FROM _orez._zero_changes`
      ) as any[]
      info(`CDC changes recorded: ${r[0]?.cnt || 0}`)
    } catch {
      warn('CDC table _orez._zero_changes not found')
    }

    // replication slots
    try {
      const r = await sql.unsafe(
        `SELECT slot_name, active, restart_lsn FROM _orez._zero_replication_slots`
      ) as any[]
      if (r.length > 0) {
        for (const slot of r) {
          ok(`Slot "${slot.slot_name}": active=${slot.active}, restart=${slot.restart_lsn}`)
        }
      } else {
        warn('No replication slots')
      }
    } catch {
      info('Replication slots table not found (normal before zero-cache connects)')
    }

    // latest change watermark
    try {
      const r = await sql.unsafe(
        `SELECT max(watermark) as wm FROM _orez._zero_changes`
      ) as any[]
      info(`Latest CDC watermark: ${r[0]?.wm || 'none'}`)
    } catch {}

    // trigger status
    try {
      const r = await sql.unsafe(
        `SELECT count(*) as cnt
         FROM information_schema.triggers
         WHERE trigger_name = '_zero_track_change_trigger'`
      ) as any[]
      if (Number(r[0]?.cnt || 0) > 0) {
        ok(`${r[0].cnt} change tracking triggers installed`)
      } else {
        warn('No change tracking triggers found')
      }
    } catch {}

    // large tables (top 5)
    try {
      const r = await sql.unsafe(
        `SELECT tablename, n_live_tup as rows
         FROM pg_stat_user_tables
         ORDER BY n_live_tup DESC
         LIMIT 5`
      ) as any[]
      if (r.length > 0) {
        info('Largest tables:')
        for (const t of r) {
          console.log(`    ${t.tablename}: ${t.rows} rows`)
        }
      }
    } catch {}

    await sql.end()
  } catch (e: any) {
    err(`PG diagnosis failed: ${e.message}`)
  }
}

async function diagnoseZero(zeroPort: number) {
  header('Zero-Cache')

  try {
    const resp = await fetch(`http://127.0.0.1:${zeroPort}/`, {
      signal: AbortSignal.timeout(5000),
    })
    if (resp.ok || resp.status === 404) {
      ok(`HTTP reachable (status ${resp.status})`)
    } else {
      warn(`HTTP returned unexpected status: ${resp.status}`)
    }
  } catch (e: any) {
    err(`Zero-cache not reachable: ${e.message}`)
    return
  }

  // try WebSocket
  try {
    const { default: WebSocket } = await import('ws')
    await new Promise<void>((resolve, reject) => {
      const ws = new WebSocket(`ws://127.0.0.1:${zeroPort}/sync/v0`)
      const timeout = setTimeout(() => {
        ws.close()
        reject(new Error('timeout'))
      }, 5000)

      ws.on('open', () => {
        clearTimeout(timeout)
        ok('WebSocket /sync/v0 reachable')
        ws.close()
        resolve()
      })

      ws.on('error', (e) => {
        clearTimeout(timeout)
        reject(e)
      })
    })
  } catch (e: any) {
    warn(`WebSocket not reachable: ${e.message}`)
  }

  // try /api/status if admin is enabled
  try {
    const resp = await fetch(`http://127.0.0.1:${zeroPort}/__orez/api/status`, {
      signal: AbortSignal.timeout(3000),
    })
    if (resp.ok) {
      const status = await resp.json()
      ok('Admin API reachable')
      info(`Uptime: ${status.uptimeMs ? Math.round(status.uptimeMs / 1000) + 's' : 'unknown'}`)
    }
  } catch {
    // admin not enabled, that's fine
  }
}

async function diagnoseProcess() {
  header('Process Info')

  const mem = process.memoryUsage()
  info(`Diagnostic script memory: RSS=${formatBytes(mem.rss)}, heap=${formatBytes(mem.heapUsed)}`)

  // check for orphaned processes
  const { execSync } = await import('node:child_process')
  try {
    const result = execSync(
      'ps aux | grep -E "(zero-cache|orez)" | grep -v grep | head -10',
      { encoding: 'utf8' }
    )
    const lines = result.trim().split('\n').filter(Boolean)
    if (lines.length > 0) {
      info(`Related processes:`)
      for (const line of lines) {
        const parts = line.trim().split(/\s+/)
        const pid = parts[1]
        const mem_ = parts[3]
        const cpu = parts[2]
        const cmd = parts.slice(10).join(' ')
        console.log(`    PID ${pid}: ${cpu}% CPU, ${mem_}% MEM — ${cmd.slice(0, 80)}`)
      }
    } else {
      info('No orez processes found (expected if running in-process)')
    }
  } catch {}
}

// ---- main ----

async function main() {
  console.log('\n' + '='.repeat(60))
  console.log('  OREZ DIAGNOSTIC REPORT')
  console.log('='.repeat(60))

  const { pgPort, zeroPort, adminPort } = detectPorts()

  info(`Target ports: pg=${pgPort}, zero=${zeroPort}, admin=${adminPort || 'disabled'}`)
  info(`Time: ${new Date().toISOString()}`)
  info(`Platform: ${process.platform} ${process.arch}, Node ${process.version}`)

  await diagnosePg(pgPort)
  await diagnoseZero(zeroPort)
  await diagnoseProcess()

  console.log('\n' + '='.repeat(60))
  console.log('  DIAGNOSIS COMPLETE')
  console.log('='.repeat(60) + '\n')
}

main().catch((err) => {
  console.error('Diagnosis failed:', err)
  process.exit(1)
})
