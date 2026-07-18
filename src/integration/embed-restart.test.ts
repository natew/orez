/**
 * embed restart contract: a SECOND zero-cache embed generation must boot in
 * the SAME process (module cache intact) against the durable state the first
 * generation left behind. this is the CF idle-hibernation cycle — the DO
 * tears the embed down at zero connections, the isolate (and every module
 * singleton in it) survives, and the next request cold-starts the embed.
 *
 * wiring matches embed-cvr-local-backend.test.ts (the CF data plane):
 *   - upstream `postgres`: DoBackend over the real ZeroDO on sqlite
 *   - cvr/cdb: DoBackend over createLocalSqlBackend on shared sqlite
 *   - createBrowserProxy + per-connection protocol sessions
 *   - each generation re-runs the startZeroCacheEmbedCF start sequence:
 *     resetReplicationState() + orphaned-tx recovery, fresh backends/proxy.
 *
 * regression under test (live on the CF demo worker, 2026-06-10): gen-2
 * boot stalls right after pg-client creation and never reaches ready,
 * which forced idle hibernation to be disabled for the SootBean demo.
 */

import { createHash } from 'node:crypto'
import { mkdirSync, rmSync } from 'node:fs'
import { createServer, type Server } from 'node:net'
import { resolve } from 'node:path'

// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { afterAll, beforeAll, describe, test, vi } from 'vitest'
import WebSocket from 'ws'

vi.mock('cloudflare:workers', () => ({
  DurableObject: class {
    ctx: unknown
    env: unknown
    constructor(ctx: unknown, env: unknown) {
      this.ctx = ctx
      this.env = env
    }
  },
  RpcTarget: class {},
}))

import { ZeroDO } from '../cf-do/worker.js'
import { createBrowserProxy, type BrowserProxy } from '../pg-proxy-browser.js'
import { DoBackend } from '../pg-proxy-do-backend.js'
import { resetReplicationState } from '../replication/handler.js'
import { usePublicationsEnv } from '../test-env.js'
import { createLocalSqlBackend } from '../worker/local-sql-backend.js'
import { startZeroCacheEmbed, type ZeroCacheEmbed } from '../worker/zero-cache-embed.js'

import type { PGlite } from '@electric-sql/pglite'

const SYNC_PROTOCOL_VERSION = 51
const PUB_NAME = 'orez_zero_public'
const EMBED_TX_OWNER = 'orez-embed'

usePublicationsEnv(PUB_NAME)

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

function connectAndSubscribe(
  port: number,
  clientGroupID: string,
  downstream: Queue<unknown>
): WebSocket {
  const cid = `${clientGroupID}-client`
  const secProtocol = encodeSecProtocols(
    [
      'initConnection',
      {
        desiredQueriesPatch: [
          { op: 'put', hash: 'q1', ast: { table: 'foo', orderBy: [['id', 'asc']] } },
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
    `ws://localhost:${port}/sync/v${SYNC_PROTOCOL_VERSION}/connect` +
      `?clientGroupID=${clientGroupID}&clientID=${cid}&wsid=ws1&schemaVersion=1&baseCookie=&ts=${Date.now()}&lmid=0`,
    secProtocol
  )
  ws.on('message', (data) => {
    downstream.enqueue(JSON.parse(data.toString()))
  })
  return ws
}

// multiple ids may arrive batched in a single pokePart rowsPatch, so a
// per-id wait would consume its siblings — collect until all ids are seen.
async function waitForRowPuts(
  downstream: Queue<unknown>,
  ids: string[],
  timeoutMs: number
): Promise<void> {
  const missing = new Set(ids)
  const deadline = Date.now() + timeoutMs
  while (missing.size > 0 && Date.now() < deadline) {
    const remaining = Math.max(1000, deadline - Date.now())
    const msg = (await downstream.dequeue('timeout' as any, remaining)) as any
    if (msg === 'timeout') break
    if (Array.isArray(msg) && msg[0] === 'pokePart' && msg[1]?.rowsPatch) {
      for (const row of msg[1].rowsPatch) {
        if (row.op === 'put' && row.tableName === 'foo') missing.delete(row.value?.id)
      }
    }
  }
  if (missing.size > 0) {
    throw new Error(
      `timed out waiting for puts of foo/[${[...missing]}] after ${timeoutMs}ms`
    )
  }
}

async function waitForZero(port: number, timeoutMs = 60000) {
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

function createSqliteStorage() {
  const nativeDb = new BedrockSqlite.Database(':memory:')
  const exec = (sql: string, ...params: unknown[]) => {
    const stmt = nativeDb.prepare(sql)
    const rows: Array<Record<string, unknown>> = stmt.reader
      ? stmt.all(...params)
      : (stmt.run(...params), [])
    return {
      toArray: () => rows,
      one: () => rows[0],
      columnNames: stmt.reader ? stmt.columns().map((c: any) => c.name) : [],
    }
  }
  return {
    exec,
    transactionSync<T>(fn: () => T): T {
      return nativeDb.transaction(fn)()
    },
  }
}

const SSL_REQUEST_CODE = 0x04d2162f

function startBridge(proxy: BrowserProxy): Promise<{ server: Server; port: number }> {
  const server = createServer((socket) => {
    const channel = new MessageChannel()
    proxy.handleConnection(channel.port2 as unknown as MessagePort)
    channel.port1.onmessage = (ev: MessageEvent) => {
      const data = ev.data as ArrayBuffer | Uint8Array
      const buf =
        data instanceof ArrayBuffer
          ? Buffer.from(data)
          : Buffer.from(data.buffer, data.byteOffset, data.byteLength)
      if (!socket.destroyed) socket.write(buf)
    }
    let sawFirstChunk = false
    socket.on('data', (chunk) => {
      if (!sawFirstChunk) {
        sawFirstChunk = true
        if (chunk.length === 8 && chunk.readInt32BE(4) === SSL_REQUEST_CODE) {
          socket.write(Buffer.from('N'))
          return
        }
      }
      channel.port1.postMessage(new Uint8Array(chunk).buffer)
    })
    socket.on('close', () => channel.port1.close())
    socket.on('error', () => {})
  })
  return new Promise((resolvePort, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address()
      if (!addr || typeof addr === 'string') return reject(new Error('no port'))
      resolvePort({ server, port: addr.port })
    })
  })
}

function createZeroDo() {
  const nativeDb = new BedrockSqlite.Database(':memory:')
  let initialized = Promise.resolve()
  const sql = {
    exec(query: string, ...params: unknown[]) {
      const stmt = nativeDb.prepare(query)
      const rows: Array<Record<string, unknown>> = stmt.reader
        ? stmt.all(...params)
        : (stmt.run(...params), [])
      return {
        toArray: () => rows,
        one: () => rows[0],
        columnNames: stmt.reader ? stmt.columns().map((c: any) => c.name) : [],
      }
    },
  }
  const ctx = {
    storage: {
      sql,
      get: async () => undefined,
      put: async () => undefined,
      delete: async () => false,
      transaction: async <T>(fn: () => T): Promise<T> => nativeDb.transaction(fn)(),
      transactionSync: <T>(fn: () => T): T => nativeDb.transaction(fn)(),
    },
    blockConcurrencyWhile<T>(fn: () => Promise<T>): Promise<T> {
      const result = fn()
      initialized = result.then(() => undefined)
      return result
    },
    acceptWebSocket() {},
    getWebSockets: () => [],
  }
  const zeroDo = new (ZeroDO as any)(ctx, {})
  return {
    fetch: (async (input: RequestInfo | URL, init?: RequestInit) => {
      await initialized
      return zeroDo.fetch(new Request(input as RequestInfo, init))
    }) as typeof globalThis.fetch,
  }
}

const ALLOW_ALL = { type: 'and', conditions: [] as unknown[] }
const ALLOW_ALL_POLICY = [['allow', ALLOW_ALL]]

function allowAllPermissionsJson(tables: string[]): string {
  return JSON.stringify({
    tables: Object.fromEntries(
      tables.map((table) => [
        table,
        {
          row: {
            select: ALLOW_ALL_POLICY,
            insert: ALLOW_ALL_POLICY,
            update: { preMutation: ALLOW_ALL_POLICY, postMutation: ALLOW_ALL_POLICY },
            delete: ALLOW_ALL_POLICY,
          },
        },
      ])
    ),
  })
}

// ── per-generation embed wiring (mirrors startZeroCacheEmbedCF's start) ──

interface Generation {
  embed: ZeroCacheEmbed
  zeroPort: number
  stop(): Promise<void>
}

async function startGeneration(opts: {
  remoteFetch: typeof globalThis.fetch
  localSql: ReturnType<typeof createLocalSqlBackend>
  zeroPort: number
  replicaFile: string
}): Promise<Generation> {
  // the startZeroCacheEmbedCF start sequence, in order: replication-state
  // reset, orphaned-tx recovery (local + remote), fresh backends + proxy.
  resetReplicationState()
  opts.localSql.recoverOrphanedTransactions()
  await opts.remoteFetch(
    'https://orez-do-backend.local/recover-txs?db=postgres&ns=zero',
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ owner: EMBED_TX_OWNER }),
    }
  )

  const createBackend = (dbName: string) =>
    new DoBackend('https://orez-do-backend.local', dbName, 'zero', {
      allowTransactionalDDL: true,
      fetch: dbName === 'postgres' ? opts.remoteFetch : opts.localSql.fetch,
      txOwner: EMBED_TX_OWNER,
    })

  const backends = {
    postgres: createBackend('postgres'),
    cvr: createBackend('zero_cvr'),
    cdb: createBackend('zero_cdb'),
  }
  await Promise.all([
    backends.postgres.waitReady,
    backends.cvr.waitReady,
    backends.cdb.waitReady,
  ])
  Object.assign(backends.postgres, {
    createProtocolSession: () => createBackend('postgres'),
  })
  Object.assign(backends.cvr, {
    createProtocolSession: () => createBackend('zero_cvr'),
  })
  Object.assign(backends.cdb, {
    createProtocolSession: () => createBackend('zero_cdb'),
  })

  const proxy = await createBrowserProxy(
    {
      postgres: backends.postgres as unknown as PGlite,
      cvr: backends.cvr as unknown as PGlite,
      cdb: backends.cdb as unknown as PGlite,
      postgresReplicas: [],
    } as any,
    { pgUser: 'user', pgPassword: 'password', singleDb: false, logLevel: 'info' }
  )

  const bridge = await startBridge(proxy)
  const base = `postgresql://user:password@127.0.0.1:${bridge.port}`

  const embed = await startZeroCacheEmbed({
    pglite: null as unknown as PGlite,
    upstreamDb: `${base}/postgres`,
    cvrDb: `${base}/zero_cvr`,
    changeDb: `${base}/zero_cdb`,
    replicaFile: opts.replicaFile,
    port: opts.zeroPort,
    publications: [PUB_NAME],
    env: { ZERO_LOG_LEVEL: 'info' },
    readyTimeout: 45000,
  })
  await waitForZero(opts.zeroPort, 45000)

  return {
    embed,
    zeroPort: opts.zeroPort,
    async stop() {
      await embed.stop().catch(() => {})
      bridge.server.close()
      await proxy.close()
      await Promise.all([
        backends.postgres.close().catch(() => {}),
        backends.cvr.close().catch(() => {}),
        backends.cdb.close().catch(() => {}),
      ])
    },
  }
}

describe('embed restart contract (gen-2 boot in one process)', () => {
  let remote: ReturnType<typeof createZeroDo>
  let localSql: ReturnType<typeof createLocalSqlBackend>
  let seed: DoBackend
  let dataDir: string
  let basePort: number

  beforeAll(async () => {
    basePort = 26000 + Math.floor(Math.random() * 1000)
    dataDir = resolve(`.orez-embed-restart-test-${Date.now()}`)
    mkdirSync(dataDir, { recursive: true })

    remote = createZeroDo()
    localSql = createLocalSqlBackend(createSqliteStorage())

    // seed upstream through a DoBackend session (the app worker's role)
    seed = new DoBackend('https://orez-do-backend.local', 'postgres', 'zero', {
      fetch: remote.fetch,
    })
    await seed.waitReady
    await seed.exec(`CREATE TABLE foo (id TEXT PRIMARY KEY, value TEXT, num INTEGER)`)
    await seed.exec(`CREATE PUBLICATION "${PUB_NAME}"`)
    await seed.exec(`ALTER PUBLICATION "${PUB_NAME}" ADD TABLE "public"."foo"`)
    await seed.exec(
      `CREATE TABLE IF NOT EXISTS "zero"."permissions" ("permissions" JSONB, "hash" TEXT, "lock" BOOL PRIMARY KEY DEFAULT true)`
    )
    const permissions = allowAllPermissionsJson(['foo'])
    await seed.query(
      `INSERT INTO "zero"."permissions" ("permissions", "hash", "lock") VALUES ($1, $2, true)`,
      [permissions, createHash('md5').update(permissions).digest('hex')]
    )
    await seed.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
      'row-1',
      'hello',
      1,
    ])
  }, 60000)

  afterAll(async () => {
    await seed?.close().catch(() => {})
    try {
      rmSync(dataDir, { recursive: true, force: true })
    } catch {}
  })

  test(
    'second embed generation boots and resumes sync',
    { timeout: 240000 },
    async () => {
      const replicaFile = resolve(dataDir, 'zero-replica.db')

      // ── generation 1: cold start, hydrate, live replication ──
      const gen1 = await startGeneration({
        remoteFetch: remote.fetch,
        localSql,
        zeroPort: basePort,
        replicaFile,
      })
      {
        const downstream = new Queue<unknown>()
        const ws = connectAndSubscribe(gen1.zeroPort, `cg-gen1-${Date.now()}`, downstream)
        try {
          await waitForRowPuts(downstream, ['row-1'], 45000)
          await seed.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
            'row-gen1',
            'live-gen1',
            2,
          ])
          await waitForRowPuts(downstream, ['row-gen1'], 45000)
        } finally {
          ws.close()
        }
      }
      // idle teardown: client gone, the DO stops the embed. the module cache
      // (zero-cache + orez + shims) survives — that's the contract under test.
      await gen1.stop()

      // ── generation 2: warm start in the same process ──
      const gen2 = await startGeneration({
        remoteFetch: remote.fetch,
        localSql,
        zeroPort: basePort + 1,
        replicaFile,
      })
      try {
        const downstream = new Queue<unknown>()
        const ws = connectAndSubscribe(gen2.zeroPort, `cg-gen2-${Date.now()}`, downstream)
        try {
          // existing rows re-hydrate from the durable replica
          await waitForRowPuts(downstream, ['row-1', 'row-gen1'], 45000)
          // live replication resumes in the new generation
          await seed.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
            'row-gen2',
            'live-gen2',
            3,
          ])
          await waitForRowPuts(downstream, ['row-gen2'], 45000)
        } finally {
          ws.close()
        }
      } finally {
        await gen2.stop()
      }
    }
  )

  test(
    'a replica reset does not orphan retained _zero_changes (2026-07 CF cost incident)',
    { timeout: 240000 },
    async () => {
      const replicaFile = resolve(dataDir, 'zero-replica.db')

      // ── the poisoning sequence from prod (soot-cf-orez-data-demo, Jul 3-7):
      // 1. writes land while no embed generation is up → rows accumulate in
      //    _zero_changes with no consumer to stream to.
      await seed.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
        'row-orphan-1',
        'accumulated-while-down',
        10,
      ])
      await seed.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
        'row-orphan-2',
        'accumulated-while-down',
        11,
      ])

      // 2. the replica is reset (resetReplicaIfTableSetChanged / a restore's
      //    reset_derived) — for the node-file replica that is deleting the file.
      //    the next generation must re-run a full initial sync, whose snapshot
      //    ALREADY CONTAINS the accumulated rows.
      rmSync(replicaFile, { force: true })
      rmSync(`${replicaFile}-wal`, { force: true })
      rmSync(`${replicaFile}-shm`, { force: true })

      // 3. next generation boots: fresh slot + initial sync over current state.
      const gen3 = await startGeneration({
        remoteFetch: remote.fetch,
        localSql,
        zeroPort: basePort + 2,
        replicaFile,
      })
      try {
        const downstream = new Queue<unknown>()
        const ws = connectAndSubscribe(gen3.zeroPort, `cg-gen3-${Date.now()}`, downstream)
        try {
          // snapshot hydration includes the accumulated rows
          await waitForRowPuts(downstream, ['row-orphan-1', 'row-orphan-2'], 45000)
          // live replication works for post-snapshot writes
          await seed.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
            'row-gen3',
            'live-gen3',
            12,
          ])
          await waitForRowPuts(downstream, ['row-gen3'], 45000)

          // the incident assertion: nothing may retain the pre-snapshot
          // changes forever. in prod they were re-streamed on EVERY embed
          // boot (never confirmable — the consumer's snapshot already covered
          // them), burning ~47k DO rows-written per boot, $50-65/day under
          // active traffic. once the consumer confirms any post-snapshot
          // batch, or the initial sync completes, the backlog must purge.
          const deadline = Date.now() + 30000
          let retained = -1
          while (Date.now() < deadline) {
            const res = await seed.query<{ n: string | number }>(
              `SELECT COUNT(*) AS n FROM _orez._zero_changes`
            )
            retained = Number(res.rows[0]?.n)
            if (retained === 0) break
            await new Promise((r) => setTimeout(r, 1000))
          }
          if (retained !== 0) {
            throw new Error(
              `replica reset orphaned the change log: _zero_changes still ` +
                `retains ${retained} row(s) 30s after a fresh initial sync + a ` +
                `confirmed post-snapshot batch — every future embed boot will ` +
                `re-stream this backlog forever`
            )
          }
        } finally {
          ws.close()
        }
      } finally {
        await gen3.stop()
      }
    }
  )
})
