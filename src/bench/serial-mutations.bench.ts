/**
 * benchmark: serial mutations with connected client
 *
 * measures the time for N mutations to be fully replicated
 * to a connected websocket client.
 *
 * run: bun src/bench/serial-mutations.bench.ts
 */

import WebSocket from 'ws'

import { startZeroLite } from '../index.js'
import {
  ensureTablesInPublications,
  installAllowAllPermissions,
} from '../integration/test-permissions.js'
import { installChangeTracking } from '../replication/change-tracker.js'

import type { PGlite } from '@electric-sql/pglite'

const SYNC_PROTOCOL_VERSION = 49
const NUM_MUTATIONS = 100

// test schema
const CLIENT_SCHEMA = {
  tables: {
    bench_items: {
      columns: {
        id: { type: 'string' },
        value: { type: 'string' },
        num: { type: 'number' },
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

async function waitForZero(port: number, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://localhost:${port}/`)
      if (res.ok || res.status === 404) return
    } catch {}
    await new Promise((r) => setTimeout(r, 500))
  }
  throw new Error(`zero-cache not ready on port ${port} after ${timeoutMs}ms`)
}

async function runBenchmark() {
  console.log(`\n=== Serial Mutations Benchmark (${NUM_MUTATIONS} mutations) ===\n`)

  const testPgPort = 24000 + Math.floor(Math.random() * 1000)
  const testZeroPort = testPgPort + 100
  const dataDir = `.orez-bench-${Date.now()}`

  console.log(`starting orez on pg:${testPgPort} zero:${testZeroPort}`)
  const result = await startZeroLite({
    pgPort: testPgPort,
    zeroPort: testZeroPort,
    dataDir,
    logLevel: 'info',
    skipZeroCache: false,
  })

  const db = result.db
  const zeroPort = result.zeroPort

  try {
    // create test table
    await db.exec(`
      CREATE TABLE IF NOT EXISTS bench_items (
        id TEXT PRIMARY KEY,
        value TEXT,
        num INTEGER
      );
    `)

    // add table to publication and install permissions
    await ensureTablesInPublications(db, ['bench_items'])
    await installChangeTracking(db)
    await installAllowAllPermissions(db, ['bench_items'])

    if (result.resetZeroFull) {
      await result.resetZeroFull()
    } else if (result.restartZero) {
      await result.restartZero()
    }

    console.log('waiting for zero-cache...')
    await waitForZero(zeroPort, 90000)
    console.log('zero-cache ready')

    // connect websocket client
    const downstream = new Queue<unknown>()
    const cg = `bench-cg-${Date.now()}`
    const cid = `bench-client-${Date.now()}`
    const secProtocol = encodeSecProtocols(
      [
        'initConnection',
        {
          desiredQueriesPatch: [
            {
              op: 'put',
              hash: 'q1',
              ast: { table: 'bench_items', orderBy: [['id', 'asc']] },
            },
          ],
          clientSchema: CLIENT_SCHEMA,
        },
      ],
      undefined
    )

    const ws = new WebSocket(
      `ws://localhost:${zeroPort}/sync/v${SYNC_PROTOCOL_VERSION}/connect` +
        `?clientGroupID=${cg}&clientID=${cid}&wsid=ws1&schemaVersion=1&baseCookie=&ts=${Date.now()}&lmid=0`,
      secProtocol
    )

    ws.on('message', (data) => {
      downstream.enqueue(JSON.parse(data.toString()))
    })

    // wait for connection
    await new Promise<void>((resolve, reject) => {
      ws.on('open', resolve)
      ws.on('error', reject)
      setTimeout(() => reject(new Error('ws connect timeout')), 5000)
    })
    console.log('websocket connected')

    // drain initial pokes
    let settled = false
    const timeout = Date.now() + 30000
    while (!settled && Date.now() < timeout) {
      const msg = (await downstream.dequeue('timeout' as any, 3000)) as any
      if (msg === 'timeout') {
        settled = true
      } else if (Array.isArray(msg) && msg[0] === 'pokeEnd') {
        const next = (await downstream.dequeue('timeout' as any, 2000)) as any
        if (next === 'timeout') {
          settled = true
        }
      }
    }
    console.log('initial sync complete, starting benchmark...\n')

    // ========== BENCHMARK: Serial Mutations ==========
    const receivedIds = new Set<string>()
    const startTime = performance.now()

    // insert mutations serially
    for (let i = 0; i < NUM_MUTATIONS; i++) {
      const id = `bench-${i}`
      await db.query(`INSERT INTO bench_items (id, value, num) VALUES ($1, $2, $3)`, [
        id,
        `value-${i}`,
        i,
      ])
    }
    const insertEndTime = performance.now()
    console.log(`inserts completed in ${(insertEndTime - startTime).toFixed(1)}ms`)

    // wait for all mutations to be replicated
    const replicationTimeout = Date.now() + 60000
    while (receivedIds.size < NUM_MUTATIONS && Date.now() < replicationTimeout) {
      const msg = (await downstream.dequeue('timeout' as any, 1000)) as any
      if (
        msg !== 'timeout' &&
        Array.isArray(msg) &&
        msg[0] === 'pokePart' &&
        msg[1]?.rowsPatch
      ) {
        for (const row of msg[1].rowsPatch) {
          if (row.op === 'put' && row.tableName === 'bench_items' && row.value?.id) {
            receivedIds.add(row.value.id)
          }
        }
      }
    }
    const endTime = performance.now()

    ws.close()

    // results
    const totalMs = endTime - startTime
    const insertMs = insertEndTime - startTime
    const replicationMs = endTime - insertEndTime
    const perMutation = totalMs / NUM_MUTATIONS

    console.log(`\n=== Results ===`)
    console.log(`total time: ${totalMs.toFixed(1)}ms`)
    console.log(
      `insert time: ${insertMs.toFixed(1)}ms (${(insertMs / NUM_MUTATIONS).toFixed(1)}ms/op)`
    )
    console.log(`replication time: ${replicationMs.toFixed(1)}ms`)
    console.log(`per mutation (end-to-end): ${perMutation.toFixed(1)}ms`)
    console.log(`mutations received: ${receivedIds.size}/${NUM_MUTATIONS}`)
    console.log(`throughput: ${(1000 / perMutation).toFixed(1)} mutations/sec`)

    if (receivedIds.size < NUM_MUTATIONS) {
      console.log(`\nWARNING: not all mutations were replicated!`)
      const missing = []
      for (let i = 0; i < NUM_MUTATIONS; i++) {
        if (!receivedIds.has(`bench-${i}`)) missing.push(i)
      }
      console.log(
        `missing: ${missing.slice(0, 10).join(', ')}${missing.length > 10 ? '...' : ''}`
      )
    }
  } finally {
    await result.stop()
    // cleanup
    const { rmSync } = await import('node:fs')
    try {
      rmSync(dataDir, { recursive: true, force: true })
    } catch {}
  }
}

runBenchmark().catch((err) => {
  console.error('benchmark failed:', err)
  process.exit(1)
})
