/**
 * orez correctness tests.
 *
 * Property-based tests that verify orez maintains data integrity under
 * various conditions. Uses the same WS protocol as the existing integration tests.
 *
 * Tests are split into two suites:
 *   - "all modes": data integrity via direct PG queries (works in WASM + native)
 *   - "native only": full replication sync via WebSocket pokes (needs native SQLite)
 *
 * Run:
 *   bun test perf/stability/correctness.test.ts
 *   bun test perf/stability/correctness.test.ts --single-db
 *   FORCE_NATIVE=1 bun test perf/stability/correctness.test.ts  # include native-only tests
 */

import { existsSync, mkdirSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'

import postgres from 'postgres'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import WebSocket from 'ws'

import { startZeroLite } from '../../src/index.js'
import {
  ensureTablesInPublications,
  installAllowAllPermissions,
} from '../../src/integration/test-permissions.js'
import { installChangeTracking } from '../../src/replication/change-tracker.js'

import type { PGlite } from '@electric-sql/pglite'

const SYNC_PROTOCOL_VERSION = 50
const useSingleDb = process.argv.includes('--single-db')
const forceNative = process.env.FORCE_NATIVE === '1'

// ---- helpers ----

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

  dequeue(fallback?: T, timeoutMs = 15000): Promise<T> {
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

function connectAndSubscribe(
  zeroPort: number,
  query: Record<string, unknown>,
  downstream: Queue<unknown>
): WebSocket {
  const cg = `corr-cg-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`
  const cid = `corr-client-${Date.now()}`
  const secProtocol = encodeSecProtocols(
    [
      'initConnection',
      {
        desiredQueriesPatch: [{ op: 'put', hash: 'q1', ast: query }],
        clientSchema: {
          tables: {
            correctness_items: {
              columns: {
                id: { type: 'string' },
                value: { type: 'string' },
                counter: { type: 'number' },
              },
              primaryKey: ['id'],
            },
          },
        },
      },
    ],
    undefined
  )
  const ws = new WebSocket(
    `ws://127.0.0.1:${zeroPort}/sync/v${SYNC_PROTOCOL_VERSION}/connect` +
      `?clientGroupID=${cg}&clientID=${cid}&wsid=ws1&schemaVersion=1&baseCookie=&ts=${Date.now()}&lmid=0`,
    secProtocol
  )

  ws.on('message', (data: Buffer) => {
    try {
      downstream.enqueue(JSON.parse(data.toString()))
    } catch {}
  })

  return ws
}

async function waitForPokePart(
  downstream: Queue<unknown>,
  timeoutMs = 15000
): Promise<Record<string, any>> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const remaining = Math.max(1000, deadline - Date.now())
    const msg = (await downstream.dequeue('timeout' as any, remaining)) as any
    if (msg === 'timeout') throw new Error('timed out waiting for pokePart')
    if (Array.isArray(msg) && msg[0] === 'pokePart' && msg[1]?.rowsPatch) {
      return msg[1]
    }
  }
  throw new Error('timed out waiting for pokePart')
}

async function drainInitialPokes(downstream: Queue<unknown>) {
  let settled = false
  const timeout = Date.now() + 30000
  while (!settled && Date.now() < timeout) {
    const msg = (await downstream.dequeue('timeout' as any, 3000)) as any
    if (msg === 'timeout') {
      settled = true
    } else if (Array.isArray(msg) && msg[0] === 'pokeEnd') {
      const next = (await downstream.dequeue('timeout' as any, 2000)) as any
      if (next === 'timeout') settled = true
    }
  }
}

function makeSql() {
  return postgres({
    host: '127.0.0.1',
    port: 0, // filled in by closure
    database: 'postgres',
    username: 'user',
    password: 'password',
    max: 1,
    no_subscribe: true,
  } as any)
}

// ---- test setup ----

const DATA_DIR = resolve(tmpdir(), `orez-correctness-${Date.now()}`)

// shared state
let pgPort: number
let zeroPort: number
let db: PGlite
let stop: () => Promise<void>
let sqlMode: string

describe('orez correctness (all modes)', () => {
  beforeAll(async () => {
    if (existsSync(DATA_DIR)) rmSync(DATA_DIR, { recursive: true, force: true })
    mkdirSync(DATA_DIR, { recursive: true })

    const orez = await startZeroLite({
      dataDir: DATA_DIR,
      singleDb: useSingleDb,
      disableWasmSqlite: forceNative,
      logLevel: 'error',
      pgPort: 0,
      zeroPort: 0,
      adminPort: 0,
    })

    pgPort = orez.pgPort
    zeroPort = orez.zeroPort
    db = orez.db
    stop = orez.stop
    sqlMode = orez.config.forceWasmSqlite
      ? 'wasm'
      : orez.config.disableWasmSqlite
        ? 'native'
        : 'auto'

    console.log(`[test] orez ready (pg=${pgPort}, zero=${zeroPort}, mode=${sqlMode})`)

    // install schema and permissions
    await db.exec(`
      CREATE TABLE IF NOT EXISTS correctness_items (
        id TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        counter INTEGER NOT NULL DEFAULT 0
      )
    `)
    await ensureTablesInPublications(db, ['correctness_items'])
    await installChangeTracking(db)
    await installAllowAllPermissions(db, ['correctness_items'])

    // wait for zero-cache
    await new Promise((r) => setTimeout(r, 3000))

    // verify zero-cache health
    try {
      const resp = await fetch(`http://127.0.0.1:${zeroPort}/`)
      console.log(`[test] zero-cache status: ${resp.status}`)
    } catch (e: any) {
      console.log(`[test] zero-cache health check: ${e.message}`)
    }
  }, 60_000)

  afterAll(async () => {
    await stop().catch(() => {})
    try {
      rmSync(DATA_DIR, { recursive: true, force: true })
    } catch {}
  })

  // ---- WS & Health (run early while zero-cache is fresh) ----

  test('zero-cache accepts websocket connections', async () => {
    const cg = `ws-cg-${Date.now()}`
    const cid = `ws-cid-${Date.now()}`
    const secProtocol = encodeSecProtocols(
      ['initConnection', { desiredQueriesPatch: [] }],
      undefined
    )
    const ws = new WebSocket(
      `ws://127.0.0.1:${zeroPort}/sync/v${SYNC_PROTOCOL_VERSION}/connect` +
        `?clientGroupID=${cg}&clientID=${cid}&wsid=ws1&schemaVersion=1&baseCookie=&ts=${Date.now()}&lmid=0`,
      secProtocol
    )

    const firstMessage = await new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('ws connect timeout')), 10000)
      ws.on('message', (data: Buffer) => {
        clearTimeout(timer)
        resolve(JSON.parse(data.toString()))
      })
      ws.on('error', (err) => {
        clearTimeout(timer)
        reject(err)
      })
    })

    expect(Array.isArray(firstMessage)).toBe(true)
    ws.close()
  }, 20_000)

  test('zero-cache health check returns ok', async () => {
    const resp = await fetch(`http://127.0.0.1:${zeroPort}/`, {
      signal: AbortSignal.timeout(5000),
    })
    expect(resp.ok || resp.status === 404).toBe(true)
  }, 10_000)

  // ---- PG Data Integrity (works in all modes) ----

  test('basic insert and select via pg proxy', async () => {
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
      const id = `basic-${Date.now()}`
      await sql.unsafe(
        `INSERT INTO correctness_items (id, value, counter) VALUES ($1, $2, $3)`,
        [id, 'hello', 42]
      )

      const r = (await sql.unsafe(`SELECT * FROM correctness_items WHERE id = $1`, [
        id,
      ])) as any[]
      expect(r.length).toBe(1)
      expect(r[0].value).toBe('hello')
      expect(r[0].counter).toBe(42)
    } finally {
      await sql.end()
    }
  }, 10_000)

  test('watermarks are strictly increasing, no duplicates', async () => {
    const id = `nodup-${Date.now()}`
    // Use direct db.query for reliable trigger testing
    await db.query(
      `INSERT INTO correctness_items (id, value, counter) VALUES ($1, $2, $3)`,
      [id, 'a', 1]
    )
    await db.query(
      `UPDATE correctness_items SET value = $1, counter = counter + 1 WHERE id = $2`,
      ['b', id]
    )
    await db.query(
      `UPDATE correctness_items SET value = $1, counter = counter + 1 WHERE id = $2`,
      ['c', id]
    )

    const changes = await db.query<{ watermark: string }>(
      `SELECT watermark::text as watermark FROM _orez._zero_changes
       WHERE table_name LIKE '%correctness_items%'
       ORDER BY watermark DESC LIMIT 10`
    )

    expect(changes.rows.length).toBeGreaterThanOrEqual(2)

    const wms = changes.rows.map((r) => Number(r.watermark)).sort((a, b) => a - b)
    for (let i = 1; i < wms.length; i++) {
      expect(wms[i]).toBeGreaterThan(wms[i - 1])
    }
    expect(new Set(wms).size).toBe(wms.length)
  }, 20_000)

  test('concurrent insertions do not lose rows', async () => {
    const CONCURRENCY = 10
    const ROWS_PER_WORKER = 100
    const BATCH = `concurrent-${Date.now()}`

    // Use direct db.query (avoid proxy extended-protocol race with concurrent connections)
    const inserts: Promise<any>[] = []
    for (let w = 0; w < CONCURRENCY; w++) {
      for (let i = 0; i < ROWS_PER_WORKER; i++) {
        inserts.push(
          db.query(
            `INSERT INTO correctness_items (id, value, counter) VALUES ($1, $2, $3)
             ON CONFLICT (id) DO NOTHING`,
            [`${BATCH}-w${w}-r${i}`, `w${w}-v${i}`, i]
          )
        )
      }
    }
    await Promise.all(inserts)
    await new Promise((r) => setTimeout(r, 500))

    const result = await db.query<{ cnt: string }>(
      `SELECT count(*)::text as cnt FROM correctness_items WHERE id LIKE $1`,
      [`${BATCH}-%`]
    )
    expect(Number(result.rows[0]?.cnt || 0)).toBe(CONCURRENCY * ROWS_PER_WORKER)
  }, 60_000)

  test('rapid connection open/close does not corrupt database', async () => {
    for (let i = 0; i < 50; i++) {
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
        await sql.unsafe('SELECT 1')
        if (i % 5 === 0) {
          await sql.unsafe(
            `INSERT INTO correctness_items (id, value, counter) VALUES ($1, $2, $3)
             ON CONFLICT (id) DO UPDATE SET counter = correctness_items.counter + 1`,
            [`churn-${i}`, `churn-${i}`, i]
          )
        }
      } finally {
        await sql.end().catch(() => {})
      }
    }

    const verify = postgres({
      host: '127.0.0.1',
      port: pgPort,
      database: 'postgres',
      username: 'user',
      password: 'password',
      max: 1,
      no_subscribe: true,
    })

    try {
      const r = (await verify.unsafe('SELECT 1 as ok')) as any[]
      expect(r[0]?.ok).toBe(1)
    } finally {
      await verify.end()
    }
  }, 30_000)

  test('large batch inserts are fully committed', async () => {
    // Build INSERT with literal values to avoid postgres driver type inference issues
    const BATCH_SIZE = 500
    const batchId = `large-${Date.now()}`
    const valueTuples: string[] = []

    for (let i = 0; i < BATCH_SIZE; i++) {
      const id = `${batchId}-${i}`
      valueTuples.push(`('${id.replace(/'/g, "''")}', 'val-${i}', ${i})`)
    }

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
      await sql.unsafe(
        `INSERT INTO correctness_items (id, value, counter) VALUES ${valueTuples.join(', ')} ON CONFLICT (id) DO NOTHING`
      )

      const r = (await sql.unsafe(
        `SELECT count(*) as cnt FROM correctness_items WHERE id LIKE $1`,
        [`${batchId}-%`]
      )) as any[]
      expect(Number(r[0]?.cnt || 0)).toBe(BATCH_SIZE)
    } finally {
      await sql.end()
    }
  }, 30_000)

  test('update to same value produces no change (no-op suppression)', async () => {
    const id = `noop-${Date.now()}`
    await db.query(
      `INSERT INTO correctness_items (id, value, counter) VALUES ($1, $2, $3)`,
      [id, 'sameval', 1]
    )

    // Count changes for this specific row before the no-op update
    const before = await db.query<{ cnt: string }>(
      `SELECT count(*)::text as cnt FROM _orez._zero_changes WHERE row_data->>'id' = $1`,
      [id]
    )
    const beforeCnt = Number(before.rows[0]?.cnt || 0)

    // update to same value — should be suppressed by trigger
    await db.query(`UPDATE correctness_items SET value = $1 WHERE id = $2`, [
      'sameval',
      id,
    ])
    await new Promise((r) => setTimeout(r, 500))

    const after = await db.query<{ cnt: string }>(
      `SELECT count(*)::text as cnt FROM _orez._zero_changes WHERE row_data->>'id' = $1`,
      [id]
    )
    const afterCnt = Number(after.rows[0]?.cnt || 0)

    // No new changes should be created for the no-op update
    expect(afterCnt).toBe(beforeCnt)
  }, 15_000)

  test('change tracking captures inserts, updates, and deletes', async () => {
    const id = `ct-${Date.now()}`
    const startWm = Number(
      (
        await db.query<{ max: string }>(
          `SELECT COALESCE(max(watermark), 0)::text as max FROM _orez._zero_changes`
        )
      ).rows[0]?.max
    )

    // insert
    await db.query(
      `INSERT INTO correctness_items (id, value, counter) VALUES ($1, $2, $3)`,
      [id, 'val1', 10]
    )
    // update
    await db.query(
      `UPDATE correctness_items SET value = $1, counter = $2 WHERE id = $3`,
      ['val2', 20, id]
    )
    // delete
    await db.query(`DELETE FROM correctness_items WHERE id = $1`, [id])

    await new Promise((r) => setTimeout(r, 500))

    const changes = await db.query<{ op: string; table_name: string }>(
      `SELECT op, table_name FROM _orez._zero_changes WHERE watermark > $1 ORDER BY watermark`,
      [startWm]
    )

    expect(changes.rows.length).toBeGreaterThanOrEqual(3)

    const ops = changes.rows.map((r) => r.op)
    expect(ops).toContain('INSERT')
    expect(ops).toContain('UPDATE')
    expect(ops).toContain('DELETE')

    // all changes should be for our table
    for (const r of changes.rows) {
      expect(r.table_name).toContain('correctness_items')
    }
  }, 20_000)
})

// ---- Native-only replication tests ----
// These test the full sync pipeline: mutation → replication → WS poke.
// Only works with native SQLite (WASM has known cross-process SHM issues).

if (forceNative) {
  describe('orez correctness (native replication)', () => {
    // reuse same ports from the shared beforeAll
    test('initial sync delivers existing rows via poke', async () => {
      // insert data before connecting
      const id = `init-${Date.now()}`
      await db.query(
        `INSERT INTO correctness_items (id, value, counter) VALUES ($1, $2, $3)`,
        [id, 'hello-init', 42]
      )

      // wait for replication
      await new Promise((r) => setTimeout(r, 3000))

      const downstream = new Queue<unknown>()
      const ws = connectAndSubscribe(
        zeroPort,
        {
          schema: 'public',
          table: 'correctness_items',
          orderBy: [['id', 'asc']],
        },
        downstream
      )

      const poke = await waitForPokePart(downstream, 30000)
      expect(poke.rowsPatch).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            tableName: 'correctness_items',
            value: expect.objectContaining({ id }),
          }),
        ])
      )

      ws.close()
    }, 45_000)

    test('live insert triggers poke', async () => {
      const downstream = new Queue<unknown>()
      const ws = connectAndSubscribe(
        zeroPort,
        {
          schema: 'public',
          table: 'correctness_items',
          orderBy: [['id', 'asc']],
        },
        downstream
      )

      // drain initial pokes
      await drainInitialPokes(downstream)

      // insert
      const id = `live-${Date.now()}`
      await db.query(
        `INSERT INTO correctness_items (id, value, counter) VALUES ($1, $2, $3)`,
        [id, 'live-val', 99]
      )

      const poke = await waitForPokePart(downstream, 30000)
      expect(poke.rowsPatch).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            tableName: 'correctness_items',
            value: expect.objectContaining({ id, value: 'live-val' }),
          }),
        ])
      )

      ws.close()
    }, 45_000)
  })
}
