/**
 * replication latency stress test.
 *
 * measures the end-to-end time from a proxy write to the zero-cache
 * websocket poke arriving at the client. this is the critical path
 * that determines whether UI re-renders overlap with user interactions.
 *
 * run: vitest run src/integration/replication-latency.test.ts
 */

import { describe, expect, test, beforeAll, afterAll } from 'vitest'
import WebSocket from 'ws'
import postgres from 'postgres'

import { startZeroLite } from '../index.js'
import { installChangeTracking } from '../replication/change-tracker.js'
import {
  ensureTablesInPublications,
  installAllowAllPermissions,
} from './test-permissions.js'

import type { PGlite } from '@electric-sql/pglite'

const SYNC_PROTOCOL_VERSION = 45

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

  dequeue(fallback?: T, timeoutMs = 10000): Promise<T> {
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

describe('replication latency', { timeout: 120000 }, () => {
  let db: PGlite
  let zeroPort: number
  let pgPort: number
  let shutdown: () => Promise<void>
  let resetZeroFull: (() => Promise<void>) | undefined
  let dataDir: string
  let sql: ReturnType<typeof postgres>

  beforeAll(async () => {
    const testPgPort = 24000 + Math.floor(Math.random() * 1000)
    const testZeroPort = testPgPort + 100

    dataDir = `.orez-latency-test-${Date.now()}`
    const result = await startZeroLite({
      pgPort: testPgPort,
      zeroPort: testZeroPort,
      dataDir,
      logLevel: 'info',
      skipZeroCache: false,
    })

    db = result.db
    zeroPort = result.zeroPort
    pgPort = result.pgPort
    shutdown = result.stop
    resetZeroFull = result.resetZeroFull

    // create test table
    await db.exec(`
      CREATE TABLE IF NOT EXISTS latency_test (
        id TEXT PRIMARY KEY,
        value TEXT,
        ts BIGINT
      );
    `)
    await ensureTablesInPublications(db, ['latency_test'])
    const pubName = process.env.ZERO_APP_PUBLICATIONS?.trim()
    if (pubName) {
      const quotedPub = '"' + pubName.replace(/"/g, '""') + '"'
      await db.exec(`ALTER PUBLICATION ${quotedPub} ADD TABLE "public"."latency_test"`).catch(() => {})
      await installChangeTracking(db)
    }
    await installAllowAllPermissions(db, ['latency_test'])
    if (resetZeroFull) await resetZeroFull()

    // wait for zero-cache ready
    await waitForZero(zeroPort, 90000)

    // connect via wire protocol (like a real app would)
    sql = postgres(`postgresql://user:password@127.0.0.1:${pgPort}/postgres`, {
      max: 1,
      idle_timeout: 0,
    })
  }, 120000)

  afterAll(async () => {
    if (sql) await sql.end()
    if (shutdown) await shutdown()
    if (dataDir) {
      const { rmSync } = await import('node:fs')
      try {
        rmSync(dataDir, { recursive: true, force: true })
      } catch {}
    }
  })

  test('measure write-to-poke latency (single inserts)', async () => {
    const downstream = new Queue<unknown>()
    const ws = connectAndSubscribe(zeroPort, downstream)
    await drainInitialPokes(downstream)

    const NUM_WRITES = 20
    const latencies: number[] = []

    for (let i = 0; i < NUM_WRITES; i++) {
      const id = `latency-${i}-${Date.now()}`
      const writeStart = performance.now()

      // write through the wire protocol proxy (like a real app)
      await sql`INSERT INTO latency_test (id, value, ts) VALUES (${id}, ${'test'}, ${Date.now()})`

      // wait for the poke containing our row
      const poke = await waitForPokeWithRow(downstream, 'latency_test', id, 10000)
      const latencyMs = performance.now() - writeStart

      expect(poke).toBeTruthy()
      latencies.push(latencyMs)
    }

    ws.close()

    // report
    latencies.sort((a, b) => a - b)
    const avg = latencies.reduce((s, v) => s + v, 0) / latencies.length
    const p50 = latencies[Math.floor(latencies.length * 0.5)]
    const p95 = latencies[Math.floor(latencies.length * 0.95)]
    const p99 = latencies[Math.floor(latencies.length * 0.99)]
    const max = latencies[latencies.length - 1]

    console.log(`\n[replication latency] ${NUM_WRITES} single inserts via wire protocol:`)
    console.log(`  avg=${avg.toFixed(1)}ms  p50=${p50.toFixed(1)}ms  p95=${p95.toFixed(1)}ms  p99=${p99.toFixed(1)}ms  max=${max.toFixed(1)}ms`)
    console.log(`  all: ${latencies.map((l) => l.toFixed(0)).join(', ')}ms`)

    // assert reasonable latency — under 200ms avg means the UI re-render
    // arrives before a user can interact with the element
    expect(avg).toBeLessThan(200)
    // no single write should take more than 500ms
    expect(max).toBeLessThan(500)
  })

  test('count poke batches per single write', async () => {
    // theory: orez causes 2+ poke batches per write because zero-cache
    // writes shard updates back through the proxy, creating a separate
    // replication batch. real postgres doesn't have this round-trip.
    const downstream = new Queue<unknown>()
    const ws = connectAndSubscribe(zeroPort, downstream)
    await drainInitialPokes(downstream)

    const id = `poke-count-${Date.now()}`
    await sql`INSERT INTO latency_test (id, value, ts) VALUES (${id}, ${'count-test'}, ${Date.now()})`

    // collect ALL messages for 2 seconds after the write
    const messages: any[] = []
    const deadline = Date.now() + 2000
    while (Date.now() < deadline) {
      const remaining = Math.max(100, deadline - Date.now())
      const msg = (await downstream.dequeue('timeout' as any, remaining)) as any
      if (msg !== 'timeout') messages.push(msg)
    }

    const pokeStarts = messages.filter((m) => Array.isArray(m) && m[0] === 'pokeStart')
    const pokeEnds = messages.filter((m) => Array.isArray(m) && m[0] === 'pokeEnd')
    const pokeParts = messages.filter((m) => Array.isArray(m) && m[0] === 'pokePart')

    console.log(`\n[poke batches] after 1 INSERT:`)
    console.log(`  pokeStart=${pokeStarts.length}  pokePart=${pokeParts.length}  pokeEnd=${pokeEnds.length}`)
    console.log(`  total messages: ${messages.length}`)
    for (const msg of messages) {
      if (Array.isArray(msg)) {
        const type = msg[0]
        if (type === 'pokePart' && msg[1]?.rowsPatch) {
          const tables = msg[1].rowsPatch.map((r: any) => `${r.op}:${r.tableName}`).join(', ')
          console.log(`    pokePart: ${tables}`)
        } else {
          console.log(`    ${type}`)
        }
      }
    }

    // ideally just 1 poke cycle per write, but we want to measure reality
    expect(pokeStarts.length).toBeGreaterThanOrEqual(1)

    ws.close()
  })

  test('count poke batches when shard tables update', async () => {
    // simulate what happens in the real app: zero-cache writes to shard
    // tables (clients.lastMutationID) after processing a mutation.
    // these shard writes go through the proxy and trigger replication.
    const downstream = new Queue<unknown>()
    const ws = connectAndSubscribe(zeroPort, downstream)
    await drainInitialPokes(downstream)

    const id = `shard-test-${Date.now()}`
    // insert via proxy (triggers replication)
    await sql`INSERT INTO latency_test (id, value, ts) VALUES (${id}, ${'shard'}, ${Date.now()})`

    // now simulate a shard write (like zero-cache updating clients table)
    // check if any shard schemas exist
    const shardSchemas = await sql`
      SELECT nspname FROM pg_namespace
      WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'public', '_orez')
        AND nspname NOT LIKE 'pg_%'
        AND nspname NOT LIKE 'zero_%'
        AND nspname NOT LIKE '_zero_%'
        AND nspname NOT LIKE '%/%'
    `

    // collect messages for 3 seconds
    const messages: any[] = []
    const deadline = Date.now() + 3000
    while (Date.now() < deadline) {
      const remaining = Math.max(100, deadline - Date.now())
      const msg = (await downstream.dequeue('timeout' as any, remaining)) as any
      if (msg !== 'timeout') messages.push(msg)
    }

    const pokeStarts = messages.filter((m) => Array.isArray(m) && m[0] === 'pokeStart')
    const pokeParts = messages.filter((m) => Array.isArray(m) && m[0] === 'pokePart')

    console.log(`\n[shard poke batches] after INSERT + shard schemas=${shardSchemas.length}:`)
    console.log(`  pokeStart=${pokeStarts.length}  pokePart=${pokeParts.length}`)
    for (const msg of messages) {
      if (Array.isArray(msg) && msg[0] === 'pokePart' && msg[1]?.rowsPatch) {
        const tables = msg[1].rowsPatch.map((r: any) => `${r.op}:${r.tableName}`).join(', ')
        console.log(`    pokePart: ${tables}`)
      }
    }

    expect(pokeStarts.length).toBeGreaterThanOrEqual(1)
    ws.close()
  })

  test('measure rapid sequential write latency', async () => {
    const downstream = new Queue<unknown>()
    const ws = connectAndSubscribe(zeroPort, downstream)
    await drainInitialPokes(downstream)

    // simulate rapid sequential writes (like a chat app sending messages)
    const NUM_WRITES = 10
    const ids: string[] = []
    const writeStart = performance.now()

    for (let i = 0; i < NUM_WRITES; i++) {
      const id = `rapid-${i}-${Date.now()}`
      ids.push(id)
      await sql`INSERT INTO latency_test (id, value, ts) VALUES (${id}, ${'rapid'}, ${Date.now()})`
    }

    const writeEnd = performance.now()

    // wait for ALL rows to arrive
    const receivedIds = new Set<string>()
    const deadline = Date.now() + 30000
    while (receivedIds.size < NUM_WRITES && Date.now() < deadline) {
      const msg = (await downstream.dequeue('timeout' as any, 5000)) as any
      if (msg === 'timeout') continue
      if (Array.isArray(msg) && msg[0] === 'pokePart' && msg[1]?.rowsPatch) {
        for (const row of msg[1].rowsPatch) {
          if (row.op === 'put' && row.tableName === 'latency_test' && row.value?.id) {
            receivedIds.add(row.value.id)
          }
        }
      }
    }

    const totalMs = performance.now() - writeStart
    const writeMs = writeEnd - writeStart
    const replicationMs = totalMs - writeMs

    console.log(`\n[replication latency] ${NUM_WRITES} rapid sequential inserts:`)
    console.log(`  write=${writeMs.toFixed(1)}ms  replication=${replicationMs.toFixed(1)}ms  total=${totalMs.toFixed(1)}ms`)
    console.log(`  received ${receivedIds.size}/${NUM_WRITES} rows`)

    expect(receivedIds.size).toBe(NUM_WRITES)
    for (const id of ids) {
      expect(receivedIds.has(id)).toBe(true)
    }
    // all 10 writes + replication should complete in under 3s
    expect(totalMs).toBeLessThan(3000)
  })

  // --- helpers ---

  function connectAndSubscribe(port: number, downstream: Queue<unknown>): WebSocket {
    const cg = `latency-cg-${Date.now()}`
    const cid = `latency-client-${Date.now()}`
    const secProtocol = encodeSecProtocols(
      [
        'initConnection',
        {
          desiredQueriesPatch: [
            {
              op: 'put',
              hash: 'q1',
              ast: {
                table: 'latency_test',
                orderBy: [['id', 'asc']],
              },
            },
          ],
          clientSchema: {
            tables: {
              latency_test: {
                columns: {
                  id: { type: 'string' },
                  value: { type: 'string' },
                  ts: { type: 'number' },
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
      `ws://localhost:${port}/sync/v${SYNC_PROTOCOL_VERSION}/connect` +
        `?clientGroupID=${cg}&clientID=${cid}&wsid=ws1&schemaVersion=1&baseCookie=&ts=${Date.now()}&lmid=0`,
      secProtocol
    )
    ws.on('message', (data) => downstream.enqueue(JSON.parse(data.toString())))
    return ws
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

  async function waitForPokeWithRow(
    downstream: Queue<unknown>,
    tableName: string,
    rowId: string,
    timeoutMs = 10000
  ): Promise<Record<string, any> | null> {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      const remaining = Math.max(500, deadline - Date.now())
      const msg = (await downstream.dequeue('timeout' as any, remaining)) as any
      if (msg === 'timeout') return null
      if (Array.isArray(msg) && msg[0] === 'pokePart' && msg[1]?.rowsPatch) {
        const match = msg[1].rowsPatch.find(
          (r: any) => r.op === 'put' && r.tableName === tableName && r.value?.id === rowId
        )
        if (match) return match
      }
    }
    return null
  }
})

async function waitForZero(port: number, timeoutMs = 60000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://localhost:${port}/`)
      if (res.ok) return
    } catch {}
    await new Promise((r) => setTimeout(r, 500))
  }
  throw new Error(`zero-cache did not become ready within ${timeoutMs}ms`)
}
