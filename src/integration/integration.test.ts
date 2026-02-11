/**
 * integration test for zero-cache sync pipeline.
 *
 * validates: pglite → change tracking → replication protocol →
 * zero-cache → websocket poke messages to clients.
 *
 * uses orez's startZeroLite() with beforeZero to set up tables
 * before zero-cache starts its initial sync. deploys ANYONE_CAN
 * permissions after zero-cache creates its schema tables.
 */

import { describe, expect, test, beforeAll, afterAll } from 'vitest'
import WebSocket from 'ws'

import { startZeroLite } from '../index.js'

import type { PGlite } from '@electric-sql/pglite'

// encode initConnectionMessage + authToken for sec-websocket-protocol header
// mirrors @rocicorp/zero's encodeSecProtocols
function encodeSecProtocol(
  initConnectionMessage: unknown,
  authToken: string = ''
): string {
  const protocols = { initConnectionMessage, authToken }
  const bytes = new TextEncoder().encode(JSON.stringify(protocols))
  const s = Array.from(bytes, (byte: number) => String.fromCharCode(byte)).join('')
  return encodeURIComponent(btoa(s))
}

// zero v0.25 requires clientSchema for new client groups
const clientSchema = {
  tables: {
    foo: {
      columns: {
        id: { type: 'string' },
        value: { type: 'string' },
        num: { type: 'number' },
      },
      primaryKey: ['id'],
    },
    bar: {
      columns: {
        id: { type: 'string' },
        foo_id: { type: 'string' },
      },
      primaryKey: ['id'],
    },
  },
}

// ANYONE_CAN permissions — empty AND condition = always true
const anyoneCanPermissions = JSON.stringify({
  tables: {
    foo: {
      row: {
        select: [['allow', { type: 'and', conditions: [] }]],
        insert: [['allow', { type: 'and', conditions: [] }]],
        update: {
          preMutation: [['allow', { type: 'and', conditions: [] }]],
          postMutation: [['allow', { type: 'and', conditions: [] }]],
        },
        delete: [['allow', { type: 'and', conditions: [] }]],
      },
    },
    bar: {
      row: {
        select: [['allow', { type: 'and', conditions: [] }]],
        insert: [['allow', { type: 'and', conditions: [] }]],
        update: {
          preMutation: [['allow', { type: 'and', conditions: [] }]],
          postMutation: [['allow', { type: 'and', conditions: [] }]],
        },
        delete: [['allow', { type: 'and', conditions: [] }]],
      },
    },
  },
})

// simple async queue for collecting websocket messages
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

describe('orez integration', { timeout: 120000 }, () => {
  let db: PGlite
  let zeroPort: number
  let shutdown: () => Promise<void>
  let dataDir: string

  beforeAll(async () => {
    const testPgPort = 23000 + Math.floor(Math.random() * 1000)
    const testZeroPort = testPgPort + 1000

    dataDir = `.orez-integration-test-${Date.now()}`
    const result = await startZeroLite({
      pgPort: testPgPort,
      zeroPort: testZeroPort,
      dataDir,
      logLevel: 'info',
      skipZeroCache: false,
      beforeZero: async (pglite) => {
        await pglite.exec(`
          CREATE TABLE IF NOT EXISTS foo (
            id TEXT PRIMARY KEY,
            value TEXT,
            num INTEGER
          );
          CREATE TABLE IF NOT EXISTS bar (
            id TEXT PRIMARY KEY,
            foo_id TEXT
          );
        `)
        // insert test data before zero-cache starts so initial sync includes it
        await pglite.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
          'seed1',
          'hello',
          42,
        ])
        await pglite.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
          'seed2',
          'world',
          99,
        ])
      },
    })

    db = result.db
    zeroPort = result.zeroPort
    shutdown = result.stop

    await waitForZero(zeroPort, 90000)

    // deploy ANYONE_CAN permissions after zero-cache creates its schema tables
    await db.query(
      `INSERT INTO zero.permissions (lock, hash, permissions)
       VALUES (true, $1, $2)
       ON CONFLICT (lock) DO UPDATE SET hash = $1, permissions = $2`,
      ['integration-test', anyoneCanPermissions]
    )
    // wait for permissions to replicate to zero-cache's sqlite replica
    await new Promise((r) => setTimeout(r, 3000))
  }, 120000)

  afterAll(async () => {
    if (shutdown) await shutdown()
    if (dataDir) {
      const { rmSync } = await import('node:fs')
      try {
        rmSync(dataDir, { recursive: true, force: true })
      } catch {}
    }
  })

  test('initial sync delivers existing rows via poke', async () => {
    const downstream = new Queue<unknown>()
    const ws = connectAndSubscribe(zeroPort, downstream, {
      table: 'foo',
      orderBy: [['id', 'asc']],
    })

    const poke = await waitForPokePart(downstream, 15000)
    const ids = poke.rowsPatch
      .filter((r: any) => r.op === 'put' && r.tableName === 'foo')
      .map((r: any) => r.value.id)
      .sort()

    expect(ids).toContain('seed1')
    expect(ids).toContain('seed2')
    expect(poke.rowsPatch).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          op: 'put',
          tableName: 'foo',
          value: expect.objectContaining({ id: 'seed1', value: 'hello' }),
        }),
      ])
    )

    ws.close()
  })

  test('initial sync delivers correct values for all columns', async () => {
    const downstream = new Queue<unknown>()
    const ws = connectAndSubscribe(zeroPort, downstream, {
      table: 'foo',
      orderBy: [['id', 'asc']],
    })

    const poke = await waitForPokePart(downstream, 15000)
    const row = poke.rowsPatch.find(
      (r: any) => r.op === 'put' && r.tableName === 'foo' && r.value.id === 'seed2'
    )

    expect(row).toBeDefined()
    expect(row.value).toEqual({ id: 'seed2', value: 'world', num: 99 })

    ws.close()
  })

  // --- helpers ---

  let clientCounter = 0

  function connectAndSubscribe(
    port: number,
    downstream: Queue<unknown>,
    query: Record<string, unknown>
  ): WebSocket {
    const cid = `test-${++clientCounter}-${Date.now()}`
    const wsid = `ws-${clientCounter}-${Date.now()}`
    const initMsg = [
      'initConnection',
      {
        desiredQueriesPatch: [{ op: 'put', hash: 'q1', ast: query }],
        clientSchema,
      },
    ]
    const secProtocol = encodeSecProtocol(initMsg)

    const ws = new WebSocket(
      `ws://localhost:${port}/sync/v45/connect` +
        `?clientGroupID=${cid}&clientID=${cid}-c&wsid=${wsid}&ts=${Date.now()}&lmid=0`,
      [secProtocol]
    )

    ws.on('message', (data) => {
      downstream.enqueue(JSON.parse(data.toString()))
    })

    return ws
  }

  async function waitForPokePart(
    downstream: Queue<unknown>,
    timeoutMs = 10000
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
})

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
