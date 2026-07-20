/**
 * integration test for zero-cache embedded mode.
 *
 * validates that zero-cache can run in-process with SINGLE_PROCESS=1,
 * connected to PGlite via the TCP proxy. this is the same pipeline as
 * the full integration test but without child_process.fork().
 *
 * test flow:
 * 1. create PGlite instances (postgres, cvr, cdb)
 * 2. start TCP proxy
 * 3. start zero-cache in-process via startZeroCacheEmbed()
 * 4. connect WebSocket client and verify sync
 */

import { mkdirSync } from 'node:fs'
import { resolve } from 'node:path'

import { describe, test, expect, beforeAll, afterAll } from 'vitest'
import WebSocket from 'ws'

import { getConfig, getConnectionString } from '../config.js'
import { startPgProxy } from '../pg-proxy.js'
import { createPGliteInstances, type PGliteInstances } from '../pglite-manager.js'
import { installChangeTracking } from '../replication/change-tracker.js'
import { usePublicationsEnv } from '../test-env.js'
import { startZeroCacheEmbed, type ZeroCacheEmbed } from '../worker/zero-cache-embed.js'
import {
  ensureTablesInPublications,
  installAllowAllPermissions,
} from './test-permissions.js'

import type { PGlite } from '@electric-sql/pglite'

// pinned for the whole file (and restored after): installChangeTracking and
// the embedded zero-cache read this env, and leaving it set would leak into
// later test files in `bun test`'s shared process.
usePublicationsEnv('orez_zero_public')

const SYNC_PROTOCOL_VERSION = 51

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
    resolve: (value: T) => void
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
      const waiter = { resolve } as {
        resolve: (value: T) => void
        timer?: ReturnType<typeof setTimeout>
      }
      if (fallback !== undefined) {
        waiter.timer = setTimeout(() => {
          const index = this.waiters.indexOf(waiter)
          if (index >= 0) this.waiters.splice(index, 1)
          resolve(fallback)
        }, timeoutMs)
      }
      this.waiters.push(waiter)
    })
  }
}

describe('zero-cache embed integration', { timeout: 120000 }, () => {
  let db: PGlite
  let instances: PGliteInstances
  let pgServer: ReturnType<Awaited<ReturnType<typeof startPgProxy>>>
  let embed: ZeroCacheEmbed
  let zeroPort: number
  let pgPort: number
  let dataDir: string
  const clientGroupID = `test-cg-${Date.now()}`
  const sockets: WebSocket[] = []

  beforeAll(async () => {
    // use random ports to avoid conflicts with other tests
    pgPort = 24000 + Math.floor(Math.random() * 1000)
    zeroPort = pgPort + 100

    dataDir = `.orez-embed-test-${Date.now()}`

    const config = getConfig({
      pgPort,
      zeroPort,
      dataDir,
      logLevel: 'info',
      useWorkerThreads: false,
      singleDb: false,
    })

    mkdirSync(dataDir, { recursive: true })

    // create PGlite instances
    instances = await createPGliteInstances(config)
    db = instances.postgres

    // create test table
    await db.exec(`
        CREATE TABLE IF NOT EXISTS foo (
          id TEXT PRIMARY KEY,
          value TEXT,
          num INTEGER
        )
      `)

    // set up publications (env pinned by usePublicationsEnv above)
    const pubName = `orez_zero_public`
    await db.exec(`CREATE PUBLICATION "${pubName}"`).catch(() => {})
    await db
      .exec(`ALTER PUBLICATION "${pubName}" ADD TABLE "public"."foo"`)
      .catch(() => {})

    // install change tracking
    await installChangeTracking(db)

    // install allow-all permissions for test
    await installAllowAllPermissions(db, ['foo'])
    await ensureTablesInPublications(db, ['foo'])

    // start TCP proxy
    pgServer = await startPgProxy(instances, config)

    // start zero-cache in-process
    const upstreamDb = getConnectionString(config, 'postgres')
    const cvrDb = getConnectionString(config, 'zero_cvr')
    const changeDb = getConnectionString(config, 'zero_cdb')
    const replicaFile = resolve(dataDir, 'zero-replica.db')

    console.log(`[embed-test] starting in-process zero-cache on port ${zeroPort}`)
    console.log(`[embed-test] upstream: ${upstreamDb}`)

    embed = await startZeroCacheEmbed({
      pglite: db,
      upstreamDb,
      cvrDb,
      changeDb,
      replicaFile,
      port: zeroPort,
      publications: [pubName],
      env: {
        ZERO_LOG_LEVEL: 'info',
      },
    })

    console.log(`[embed-test] zero-cache ready on port ${embed.port}`)

    // wait for HTTP health check
    await waitForZero(zeroPort, 30000)
    console.log(`[embed-test] health check passed`)
  }, 120000)

  afterAll(async () => {
    for (const socket of sockets) socket.close()
    if (embed) await embed.stop()
    if (pgServer) pgServer.close()
    if (instances) {
      await instances.postgres.close().catch(() => {})
      await instances.cvr.close().catch(() => {})
      await instances.cdb.close().catch(() => {})
    }
    if (dataDir) {
      const { rmSync } = await import('node:fs')
      try {
        rmSync(dataDir, { recursive: true, force: true })
      } catch {}
    }
  })

  test('zero-cache is ready', () => {
    expect(embed.ready).toBe(true)
    expect(embed.port).toBe(zeroPort)
  })

  test('accepts WebSocket connections', async () => {
    const downstream = new Queue<unknown>()
    const ws = connectAndSubscribe(zeroPort, downstream, clientGroupID)
    sockets.push(ws)

    await drainInitialPokes(downstream)

    expect(ws.readyState).toBe(WebSocket.OPEN)
  })

  test('live replication: insert triggers poke', async () => {
    const downstream = new Queue<unknown>()
    const ws = connectAndSubscribe(zeroPort, downstream, clientGroupID)
    sockets.push(ws)

    await drainInitialPokes(downstream)

    await db.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
      'embed-row',
      'hello-embed',
      42,
    ])

    const poke = await waitForPokePart(downstream, 30000)
    expect(poke.rowsPatch).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          op: 'put',
          tableName: 'foo',
          value: expect.objectContaining({
            id: 'embed-row',
            value: 'hello-embed',
          }),
        }),
      ])
    )
  })

  // sootbean points zero clients at `https://host/p-<projectId>` — the zero
  // client appends its sync path after that single base component, producing
  // `/p-<id>/sync/v51/connect`. on node the prefix is inert and the server
  // must sync exactly like an unprefixed client. zero supports this natively:
  // the client's getServer() permits exactly one path component and the
  // server's WorkerDispatcher matches `(/:base)/:worker/v:version/:action`,
  // ignoring `base`. this test pins that tolerance so a zero upgrade that
  // drops it fails here instead of in downstream prefixed deployments.
  test('syncs through a p-<id> server path prefix', async () => {
    const downstream = new Queue<unknown>()
    const ws = connectAndSubscribe(zeroPort, downstream, clientGroupID, '/p-abc123')
    sockets.push(ws)

    await drainInitialPokes(downstream)

    await db.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
      'embed-row-prefixed',
      'hello-prefixed',
      43,
    ])

    const poke = await waitForPokePart(downstream, 30000)
    expect(poke.rowsPatch).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          op: 'put',
          tableName: 'foo',
          value: expect.objectContaining({
            id: 'embed-row-prefixed',
            value: 'hello-prefixed',
          }),
        }),
      ])
    )
  })
})

let nextClientID = 0

function connectAndSubscribe(
  port: number,
  downstream: Queue<unknown>,
  clientGroupID: string,
  basePath = ''
): WebSocket {
  const clientID = `test-client-${++nextClientID}`
  const secProtocol = encodeSecProtocols(
    [
      'initConnection',
      {
        desiredQueriesPatch: [
          {
            op: 'put',
            hash: 'q1',
            ast: {
              table: 'foo',
              orderBy: [['id', 'asc']],
            },
          },
        ],
        clientSchema: {
          tables: {
            foo: {
              columns: {
                id: { type: 'string' },
                value: { type: 'string' },
                num: { type: 'number' },
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
    `ws://localhost:${port}${basePath}/sync/v${SYNC_PROTOCOL_VERSION}/connect` +
      `?clientGroupID=${clientGroupID}&clientID=${clientID}&wsid=ws1&schemaVersion=1&baseCookie=&ts=${Date.now()}&lmid=0`,
    secProtocol
  )

  ws.on('message', (data) => {
    downstream.enqueue(JSON.parse(data.toString()))
  })

  return ws
}

async function drainInitialPokes(downstream: Queue<unknown>) {
  const deadline = Date.now() + 30000

  while (Date.now() < deadline) {
    const remaining = Math.max(1000, deadline - Date.now())
    const msg = (await downstream.dequeue('timeout' as any, remaining)) as any
    if (msg === 'timeout') break
    if (Array.isArray(msg) && msg[0] === 'pokeEnd') return
  }

  throw new Error('timed out waiting for initial poke')
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
