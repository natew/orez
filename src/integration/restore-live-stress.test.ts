/**
 * live restore stress test.
 *
 * keeps a frontend-like websocket connection active while a large restore runs,
 * then triggers the same full reset path used by pg_restore (SIGUSR1) and
 * verifies sync still works after restart.
 */

import { readFileSync, rmSync, unlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { loadModule } from 'pgsql-parser'
import postgres from 'postgres'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import WebSocket from 'ws'

import { execDumpFile } from '../cli.js'
import { startZeroLite } from '../index.js'
import { installChangeTracking } from '../replication/change-tracker.js'
import {
  ensureTablesInPublications,
  hasNonNullPermissions,
  installAllowAllPermissions,
} from './test-permissions.js'

import type { PGlite } from '@electric-sql/pglite'

const SYNC_PROTOCOL_VERSION = 45
const LIVE_CLIENT_SCHEMA = {
  tables: {
    restore_live_probe: {
      columns: {
        id: { type: 'string' },
        value: { type: 'string' },
      },
      primaryKey: ['id'],
    },
  },
}

function encodeSecProtocols(
  initConnectionMessage: unknown,
  authToken: string | undefined
): string {
  const payload = JSON.stringify({ initConnectionMessage, authToken })
  return encodeURIComponent(Buffer.from(payload, 'utf-8').toString('base64'))
}

class Queue<T> {
  private items: T[] = []
  private waiters: Array<{
    resolve: (v: T) => void
    timer?: ReturnType<typeof setTimeout>
  }> = []

  enqueue(item: T) {
    const waiter = this.waiters.shift()
    if (waiter) {
      if (waiter.timer) clearTimeout(waiter.timer)
      waiter.resolve(item)
    } else {
      this.items.push(item)
    }
  }

  dequeue(fallback?: T, timeoutMs = 10_000): Promise<T> {
    if (this.items.length > 0) {
      return Promise.resolve(this.items.shift()!)
    }
    return new Promise<T>((resolve) => {
      const waiter: { resolve: (v: T) => void; timer?: ReturnType<typeof setTimeout> } = {
        resolve,
      }
      if (fallback !== undefined) {
        waiter.timer = setTimeout(() => {
          const idx = this.waiters.indexOf(waiter)
          if (idx >= 0) this.waiters.splice(idx, 1)
          resolve(fallback)
        }, timeoutMs)
      }
      this.waiters.push(waiter)
    })
  }
}

function envInt(name: string, fallback: number): number {
  const raw = process.env[name]
  if (!raw) return fallback
  const n = Number(raw)
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback
}

function escapeCopy(val: string): string {
  return val
    .replace(/\\/g, '\\\\')
    .replace(/\t/g, '\\t')
    .replace(/\n/g, '\\n')
    .replace(/\r/g, '\\r')
}

function generateStressDump(opts: {
  tables: number
  rowsPerTable: number
  columnsPerTable: number
  payloadBytes: number
}): string {
  const lines: string[] = []
  lines.push('SET statement_timeout = 0;')
  lines.push("SET client_encoding = 'UTF8';")
  lines.push('SET standard_conforming_strings = on;')
  lines.push('')

  for (let t = 0; t < opts.tables; t++) {
    const table = `stress_restore_${t}`
    const cols = Array.from({ length: opts.columnsPerTable }, (_, i) => `c_${i} TEXT`)
    lines.push(
      `CREATE TABLE IF NOT EXISTS ${table} (id BIGINT PRIMARY KEY, ${cols.join(', ')});`
    )
    lines.push(
      `COPY ${table} (id, ${Array.from({ length: opts.columnsPerTable }, (_, i) => `c_${i}`).join(', ')}) FROM stdin;`
    )

    for (let r = 0; r < opts.rowsPerTable; r++) {
      const id = t * 1_000_000 + r + 1
      const row = Array.from({ length: opts.columnsPerTable }, (_, c) => {
        if (r % 97 === 0 && c === 0) return '\\N'
        const base = `t${t}_r${r}_c${c}_`
        return escapeCopy(base + 'x'.repeat(Math.max(1, opts.payloadBytes - base.length)))
      })
      lines.push(`${id}\t${row.join('\t')}`)
    }
    lines.push('\\.')
    lines.push('')
  }

  return lines.join('\n')
}

function connectAndSubscribe(
  port: number,
  downstream: Queue<unknown>,
  query: Record<string, unknown>
): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const initConnectionMessage: [string, Record<string, unknown>] = [
      'initConnection',
      {
        desiredQueriesPatch: [{ op: 'put', hash: 'q1', ast: query }],
        clientSchema: LIVE_CLIENT_SCHEMA,
      },
    ]
    const secProtocol = encodeSecProtocols(initConnectionMessage, undefined)
    const ws = new WebSocket(
      `ws://127.0.0.1:${port}/sync/v${SYNC_PROTOCOL_VERSION}/connect` +
        `?clientGroupID=restore-live-cg-${Date.now()}` +
        `&clientID=restore-live-client` +
        `&wsid=ws1&schemaVersion=1&baseCookie=&ts=${Date.now()}&lmid=0`,
      secProtocol
    )

    let settled = false
    const failTimer = setTimeout(() => {
      if (settled) return
      settled = true
      try {
        ws.close()
      } catch {}
      reject(new Error('websocket connected but no downstream messages'))
    }, 7000)

    ws.on('message', (data) => {
      const msg = JSON.parse(data.toString())
      downstream.enqueue(msg)
      if (!settled) {
        settled = true
        clearTimeout(failTimer)
        resolve(ws)
      }
    })
    ws.once('error', (err) => {
      if (settled) return
      settled = true
      clearTimeout(failTimer)
      reject(err)
    })
    ws.once('close', () => {
      if (settled) return
      settled = true
      clearTimeout(failTimer)
      reject(new Error('websocket closed before initial downstream message'))
    })
  })
}

async function connectAndSubscribeWithRetry(
  port: number,
  downstream: Queue<unknown>,
  query: Record<string, unknown>,
  timeoutMs = 30_000
): Promise<WebSocket> {
  const deadline = Date.now() + timeoutMs
  let lastErr: unknown
  while (Date.now() < deadline) {
    try {
      return await connectAndSubscribe(port, downstream, query)
    } catch (err) {
      lastErr = err
      await new Promise((r) => setTimeout(r, 300))
    }
  }
  throw new Error(
    `timed out connecting websocket after reset: ${lastErr instanceof Error ? lastErr.message : String(lastErr)}`
  )
}

async function drainInitialPokes(downstream: Queue<unknown>) {
  const deadline = Date.now() + 30_000
  while (Date.now() < deadline) {
    const msg = (await downstream.dequeue('timeout' as any, 3000)) as any
    if (msg === 'timeout') return
    if (Array.isArray(msg) && msg[0] === 'pokeEnd') return
  }
}

async function waitForPokeWithValue(
  downstream: Queue<unknown>,
  expectedValue: string,
  timeoutMs = 20_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  const seen: unknown[] = []
  while (Date.now() < deadline) {
    const remaining = Math.max(1000, deadline - Date.now())
    const msg = (await downstream.dequeue('timeout' as any, remaining)) as any
    if (msg === 'timeout') {
      throw new Error(
        `timed out waiting for pokePart; recent messages: ${JSON.stringify(seen.slice(-8))}`
      )
    }
    seen.push(msg)
    if (!Array.isArray(msg) || msg[0] !== 'pokePart' || !msg[1]?.rowsPatch) continue
    const rowsPatch = msg[1].rowsPatch as Array<Record<string, any>>
    if (
      rowsPatch.some(
        (patch) =>
          patch.op === 'put' &&
          patch.tableName === 'restore_live_probe' &&
          patch.value?.value === expectedValue
      )
    ) {
      return
    }
  }
  throw new Error(
    `timed out waiting for restore_live_probe value "${expectedValue}"; recent messages: ${JSON.stringify(seen.slice(-8))}`
  )
}

async function waitForZero(port: number, timeoutMs = 60_000) {
  const { Socket } = await import('node:net')
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const ok = await new Promise<boolean>((resolve) => {
      const sock = new Socket()
      const done = (value: boolean) => {
        sock.removeAllListeners()
        try {
          sock.destroy()
        } catch {}
        resolve(value)
      }
      sock.setTimeout(1000)
      sock.once('connect', () => done(true))
      sock.once('timeout', () => done(false))
      sock.once('error', () => done(false))
      sock.connect(port, '127.0.0.1')
    })
    if (ok) return
    await new Promise((r) => setTimeout(r, 500))
  }
  throw new Error(`zero-cache not ready on port ${port} after ${timeoutMs}ms`)
}

describe('live restore stress with connected frontend', { timeout: 360_000 }, () => {
  let db: PGlite
  let pgPort: number
  let zeroPort: number
  let shutdown: () => Promise<void>
  let restartZero: (() => Promise<void>) | undefined
  let resetZeroFull: (() => Promise<void>) | undefined
  let dataDir: string
  let dumpFile: string

  beforeAll(async () => {
    await loadModule()

    const tables = envInt('OREZ_STRESS_TABLES', 6)
    const rowsPerTable = envInt('OREZ_STRESS_ROWS', 1800)
    const columnsPerTable = envInt('OREZ_STRESS_COLS', 8)
    const payloadBytes = envInt('OREZ_STRESS_PAYLOAD', 96)

    dumpFile = join(tmpdir(), `orez-live-stress-${Date.now()}.sql`)
    writeFileSync(
      dumpFile,
      generateStressDump({ tables, rowsPerTable, columnsPerTable, payloadBytes })
    )

    dataDir = `.orez-live-stress-test-${Date.now()}`
    const started = await startZeroLite({
      pgPort: 29000 + Math.floor(Math.random() * 1000),
      zeroPort: 30000 + Math.floor(Math.random() * 1000),
      dataDir,
      logLevel: 'warn',
      skipZeroCache: false,
    })

    db = started.db
    pgPort = started.pgPort
    zeroPort = started.zeroPort
    shutdown = started.stop
    restartZero = started.restartZero
    resetZeroFull = started.resetZeroFull
    await waitForZero(zeroPort, 90_000)
  }, 180_000)

  afterAll(async () => {
    if (shutdown) await shutdown()
    try {
      unlinkSync(dumpFile)
    } catch {}
    if (dataDir) {
      try {
        rmSync(dataDir, { recursive: true, force: true })
      } catch {}
    }
  })

  test('frontend stays connected through restore lifecycle and syncs after reset', async () => {
    await db.exec(`
      CREATE TABLE IF NOT EXISTS restore_live_probe (
        id TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
    `)
    await ensureTablesInPublications(db, ['restore_live_probe'])
    await installAllowAllPermissions(db, ['restore_live_probe'])
    expect(await hasNonNullPermissions(db)).toBe(true)
    if (resetZeroFull) {
      await resetZeroFull()
      await waitForZero(zeroPort, 90_000)
    } else if (restartZero) {
      await restartZero()
      await waitForZero(zeroPort, 60_000)
    }
    const pubName = process.env.ZERO_APP_PUBLICATIONS?.trim()
    if (pubName) {
      const quotedPub = '"' + pubName.replace(/"/g, '""') + '"'
      await db
        .exec(`ALTER PUBLICATION ${quotedPub} ADD TABLE "public"."restore_live_probe"`)
        .catch(() => {})
      await installChangeTracking(db)
    }
    await db.query(`INSERT INTO restore_live_probe (id, value) VALUES ($1, $2)`, [
      'before-restore',
      'before',
    ])

    const downstream = new Queue<unknown>()
    let ws = await connectAndSubscribeWithRetry(zeroPort, downstream, {
      table: 'restore_live_probe',
      orderBy: [['id', 'asc']],
    })
    await drainInitialPokes(downstream)

    // restore while websocket is connected (frontend simulation)
    const sql = postgres({
      host: '127.0.0.1',
      port: pgPort,
      user: 'user',
      password: 'password',
      database: 'postgres',
      max: 1,
      onnotice: () => {},
    })
    try {
      const wireDb = { exec: (query: string) => sql.unsafe(query) as Promise<unknown> }
      await execDumpFile(wireDb, dumpFile)
    } finally {
      await sql.end({ timeout: 1 }).catch(() => {})
    }

    const pid = Number(readFileSync(join(dataDir, 'orez.pid'), 'utf-8').trim())
    expect(pid).toBeGreaterThan(0)
    process.kill(pid, 'SIGUSR1')
    await waitForZero(zeroPort, 90_000)
    if (pubName) {
      const quotedPub = '"' + pubName.replace(/"/g, '""') + '"'
      await db
        .exec(`ALTER PUBLICATION ${quotedPub} ADD TABLE "public"."restore_live_probe"`)
        .catch(() => {})
    }

    try {
      ws.close()
    } catch {}
    const downstreamAfterReset = new Queue<unknown>()
    ws = await connectAndSubscribeWithRetry(zeroPort, downstreamAfterReset, {
      table: 'restore_live_probe',
      orderBy: [['id', 'asc']],
    })
    await drainInitialPokes(downstreamAfterReset)

    // verify write is captured in change tracking after reset
    const marker = `after-${Date.now()}`
    await db.query(`INSERT INTO restore_live_probe (id, value) VALUES ($1, $2)`, [
      `post-restore-${Date.now()}`,
      marker,
    ])
    const tracked = await db.query<{ count: string }>(
      `SELECT count(*)::text as count
       FROM _orez._zero_changes
       WHERE table_name = 'public.restore_live_probe'`
    )
    if (Number(tracked.rows[0]?.count || '0') === 0) {
      throw new Error('post-reset write was not captured in _orez._zero_changes')
    }

    await waitForPokeWithValue(downstreamAfterReset, marker, 30_000)

    ws.close()
  })
})
