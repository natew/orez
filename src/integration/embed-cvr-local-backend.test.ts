/**
 * CF-shaped embed repro: real zero-cache (in-process) wired exactly like
 * zero-cache-embed-cf.ts wires a Durable Object:
 *
 *   - upstream `postgres`: DoBackend over the REAL ZeroDO (src/cf-do/worker)
 *     running on sqlite — tracked SQL, _zero_changes, tx journal,
 *     /commit-tx, /recover-txs — the remote ZeroSqlDO of a CF deploy.
 *   - cvr + change DBs: DoBackend over createLocalSqlBackend on one shared
 *     sqlite storage — the embed-local backend.
 *   - createBrowserProxy in front of all three, with per-connection
 *     protocol sessions, plus boot-time orphan recovery (local + remote).
 *
 * only the transport to zero-cache differs: a TCP ↔ MessagePort bridge
 * stands in for the bundler's postgres-socket shim, because the node embed
 * dials connection strings.
 *
 * regression under test (live on the CF demo worker): the FIRST client
 * group after embed boot hydrates; a SUBSEQUENT client group connecting to
 * the same live embed hangs in the view-syncer before initial sync — the
 * websocket opens and no pokes ever arrive.
 *
 * both backend fetches are instrumented so a hang reports exactly which
 * statements were in flight (or that the session never reached a backend
 * at all, i.e. it is gated inside the proxy).
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
}))

import { ZeroDO } from '../cf-do/worker.js'
import { createBrowserProxy, type BrowserProxy } from '../pg-proxy-browser.js'
import { DoBackend } from '../pg-proxy-do-backend.js'
import { usePublicationsEnv } from '../test-env.js'
import { createLocalSqlBackend } from '../worker/local-sql-backend.js'
import { startZeroCacheEmbed, type ZeroCacheEmbed } from '../worker/zero-cache-embed.js'

import type { PGlite } from '@electric-sql/pglite'

const SYNC_PROTOCOL_VERSION = 51
const PUB_NAME = 'orez_zero_public'
const EMBED_TX_OWNER = 'orez-embed'

usePublicationsEnv(PUB_NAME)

// ── helpers shared with embed-integration.test.ts (file-local copies) ──

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

// subscribe to the keyless composite-key table (the accountMember shape:
// no PRIMARY KEY on the table, key carried by a separate <name>_pkey unique
// index — soot generateDDL emits composite primaryKey() this way for the DO).
function connectAndSubscribeMember(
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
          {
            op: 'put',
            hash: 'qm1',
            ast: { table: 'member', orderBy: [['accountId', 'asc']] },
          },
        ],
        clientSchema: {
          tables: {
            member: {
              columns: {
                accountId: { type: 'string' },
                userId: { type: 'string' },
                role: { type: 'string' },
              },
              primaryKey: ['accountId', 'userId'],
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

async function waitForMemberRole(
  downstream: Queue<unknown>,
  accountId: string,
  role: string,
  timeoutMs: number
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const remaining = Math.max(1000, deadline - Date.now())
    const msg = (await downstream.dequeue('timeout' as any, remaining)) as any
    if (msg === 'timeout') break
    if (Array.isArray(msg) && msg[0] === 'pokePart' && msg[1]?.rowsPatch) {
      for (const row of msg[1].rowsPatch) {
        if (
          row.op === 'put' &&
          row.tableName === 'member' &&
          row.value?.accountId === accountId &&
          row.value?.role === role
        ) {
          return
        }
      }
    }
  }
  throw new Error(
    `timed out waiting for member/${accountId} role=${role} after ${timeoutMs}ms`
  )
}

async function waitForRowPut(
  downstream: Queue<unknown>,
  id: string,
  timeoutMs: number
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const remaining = Math.max(1000, deadline - Date.now())
    const msg = (await downstream.dequeue('timeout' as any, remaining)) as any
    if (msg === 'timeout') break
    if (Array.isArray(msg) && msg[0] === 'pokePart' && msg[1]?.rowsPatch) {
      for (const row of msg[1].rowsPatch) {
        if (row.op === 'put' && row.tableName === 'foo' && row.value?.id === id) return
      }
    }
  }
  throw new Error(`timed out waiting for put of foo/${id} after ${timeoutMs}ms`)
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

// ── DO-sqlite storage (same shape tx-journal.test.ts uses) ──

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

// ── instrumentation: record every request a backend sends ──

interface TrackedRequest {
  id: number
  path: string
  body: string
  startedAt: number
  endedAt?: number
}

function trackFetch(label: string, target: typeof globalThis.fetch) {
  let seq = 0
  const inflight = new Map<number, TrackedRequest>()
  const recent: TrackedRequest[] = []
  const fetch: typeof globalThis.fetch = async (input, init) => {
    const id = ++seq
    const request = new Request(input as RequestInfo, init)
    const body = typeof init?.body === 'string' ? init.body : ''
    const entry: TrackedRequest = {
      id,
      path: new URL(request.url).pathname,
      body: body.slice(0, 240),
      startedAt: Date.now(),
    }
    inflight.set(id, entry)
    try {
      return await target(input as RequestInfo, init)
    } finally {
      entry.endedAt = Date.now()
      inflight.delete(id)
      recent.push(entry)
      if (recent.length > 60) recent.shift()
    }
  }
  return {
    fetch,
    diagnostics(): string {
      const stuck = [...inflight.values()].map(
        (r) => `IN-FLIGHT ${Date.now() - r.startedAt}ms ${r.path} ${r.body}`
      )
      const last = recent
        .slice(-12)
        .map((r) => `${r.endedAt! - r.startedAt}ms ${r.path} ${r.body}`)
      return [
        `[${label}] in-flight (${stuck.length}):`,
        ...stuck,
        `[${label}] last ${last.length} completed:`,
        ...last,
      ].join('\n')
    },
  }
}

// ── TCP ↔ MessagePort bridge so the node embed can dial the browser proxy ──

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
        // answer SSLRequest with 'N' (pg-gateway does this on the TCP proxy;
        // the MessagePort shim never sends one)
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

// ── the real ZeroDO on sqlite (the remote ZeroSqlDO of a CF deploy) ──

function createZeroDo() {
  const nativeDb = new BedrockSqlite.Database(':memory:')
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
      transaction: async <T>(fn: () => T): Promise<T> => nativeDb.transaction(fn)(),
      transactionSync: <T>(fn: () => T): T => nativeDb.transaction(fn)(),
    },
    acceptWebSocket() {},
    getWebSockets: () => [],
  }
  const zeroDo = new (ZeroDO as any)(ctx, {})
  return {
    fetch: ((input: RequestInfo | URL, init?: RequestInit) =>
      zeroDo.fetch(new Request(input as RequestInfo, init))) as typeof globalThis.fetch,
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

describe('zero-cache embed on the CF data plane (ZeroDO upstream, local cvr/cdb)', () => {
  let proxy: BrowserProxy
  let bridge: { server: Server; port: number }
  let embed: ZeroCacheEmbed
  let zeroPort: number
  let dataDir: string
  let local: ReturnType<typeof trackFetch>
  let remote: ReturnType<typeof trackFetch>
  let seed: DoBackend

  const diagnostics = () => `${remote.diagnostics()}\n${local.diagnostics()}`

  beforeAll(async () => {
    zeroPort = 25000 + Math.floor(Math.random() * 1000)
    dataDir = resolve(`.orez-embed-cvr-local-test-${Date.now()}`)
    mkdirSync(dataDir, { recursive: true })

    // remote ZeroSqlDO: the real ZeroDO class on sqlite
    const zeroDo = createZeroDo()
    remote = trackFetch('remote ZeroDO', zeroDo.fetch)

    // embed-local backend for cvr/cdb (one shared storage, like the DO's own)
    const localStorage = createSqliteStorage()
    const localSql = createLocalSqlBackend(localStorage)
    local = trackFetch('local backend', (input, init) => localSql.fetch(input, init))

    const createBackend = (dbName: string) =>
      new DoBackend('https://orez-do-backend.local', dbName, 'zero', {
        fetch: dbName === 'postgres' ? remote.fetch : local.fetch,
        txOwner: EMBED_TX_OWNER,
      })

    // boot-time crash recovery, exactly as startZeroCacheEmbedCF does it
    localSql.recoverOrphanedTransactions()
    await remote.fetch('https://orez-do-backend.local/recover-txs?db=postgres&ns=zero', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ owner: EMBED_TX_OWNER }),
    })

    // seed upstream through a DoBackend session (the app worker's role):
    // app table, publication, allow-all permissions
    seed = createBackend('postgres')
    await seed.waitReady
    await seed.exec(`CREATE TABLE foo (id TEXT PRIMARY KEY, value TEXT, num INTEGER)`)
    // the accountMember LEGACY shape, worst case: keyless table with NULLABLE
    // physical columns (created before NOT NULL reached the generated DDL, so
    // neither PRAGMA nor durable metadata says notNull) and the composite key
    // added later as a <table>_pkey unique index. 2026-07-10 soot prod: first
    // UPDATE through the streamer crashed change-processor #getKey.
    await seed.exec(`CREATE TABLE member ("accountId" TEXT, "userId" TEXT, role TEXT)`)
    await seed.exec(
      `CREATE UNIQUE INDEX "member_pkey" ON "member" ("accountId", "userId")`
    )
    await seed.exec(`CREATE PUBLICATION "${PUB_NAME}"`)
    await seed.exec(`ALTER PUBLICATION "${PUB_NAME}" ADD TABLE "public"."foo"`)
    await seed.exec(`ALTER PUBLICATION "${PUB_NAME}" ADD TABLE "public"."member"`)
    await seed.exec(
      `CREATE TABLE IF NOT EXISTS "zero"."permissions" ("permissions" JSONB, "hash" TEXT, "lock" BOOL PRIMARY KEY DEFAULT true)`
    )
    const permissions = allowAllPermissionsJson(['foo', 'member'])
    await seed.query(
      `INSERT INTO "zero"."permissions" ("permissions", "hash", "lock") VALUES ($1, $2, true)`,
      [permissions, createHash('md5').update(permissions).digest('hex')]
    )
    await seed.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
      'row-1',
      'hello',
      1,
    ])
    await seed.query(
      `INSERT INTO member ("accountId", "userId", role) VALUES ($1, $2, $3)`,
      ['acc-1', 'user-1', 'member']
    )

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
    // per-connection protocol sessions, as embed-cf wires them
    Object.assign(backends.postgres, {
      createProtocolSession: () => createBackend('postgres'),
    })
    Object.assign(backends.cvr, {
      createProtocolSession: () => createBackend('zero_cvr'),
    })
    Object.assign(backends.cdb, {
      createProtocolSession: () => createBackend('zero_cdb'),
    })

    proxy = await createBrowserProxy(
      {
        postgres: backends.postgres as unknown as PGlite,
        cvr: backends.cvr as unknown as PGlite,
        cdb: backends.cdb as unknown as PGlite,
        postgresReplicas: [],
      } as any,
      { pgUser: 'user', pgPassword: 'password', singleDb: false, logLevel: 'info' }
    )

    bridge = await startBridge(proxy)
    const base = `postgresql://user:password@127.0.0.1:${bridge.port}`

    embed = await startZeroCacheEmbed({
      pglite: null as unknown as PGlite, // unused by the embed; zero dials the proxy
      upstreamDb: `${base}/postgres`,
      cvrDb: `${base}/zero_cvr`,
      changeDb: `${base}/zero_cdb`,
      replicaFile: resolve(dataDir, 'zero-replica.db'),
      port: zeroPort,
      publications: [PUB_NAME],
      env: { ZERO_LOG_LEVEL: 'info' },
    })
    await waitForZero(zeroPort, 60000)
  }, 120000)

  afterAll(async () => {
    if (embed) await embed.stop().catch(() => {})
    if (bridge) bridge.server.close()
    if (proxy) proxy.close()
    try {
      rmSync(dataDir, { recursive: true, force: true })
    } catch {}
  })

  test('first client group hydrates', { timeout: 60000 }, async () => {
    const downstream = new Queue<unknown>()
    const ws = connectAndSubscribe(zeroPort, `cg-one-${Date.now()}`, downstream)
    try {
      await waitForRowPut(downstream, 'row-1', 45000)
    } catch (err) {
      throw new Error(`${(err as Error).message}\n${diagnostics()}`)
    } finally {
      ws.close()
    }
  })

  test(
    'a second client group hydrates against the live embed',
    { timeout: 90000 },
    async () => {
      // small gap, mirroring the live probe's back-to-back page sessions
      await new Promise((r) => setTimeout(r, 2000))
      const downstream = new Queue<unknown>()
      const ws = connectAndSubscribe(zeroPort, `cg-two-${Date.now()}`, downstream)
      try {
        await waitForRowPut(downstream, 'row-1', 45000)
      } catch (err) {
        throw new Error(`${(err as Error).message}\n${diagnostics()}`)
      } finally {
        ws.close()
      }
    }
  )

  test(
    'a third client group hydrates while another is still connected',
    { timeout: 90000 },
    async () => {
      const holdQueue = new Queue<unknown>()
      const holder = connectAndSubscribe(zeroPort, `cg-hold-${Date.now()}`, holdQueue)
      await waitForRowPut(holdQueue, 'row-1', 45000)

      const downstream = new Queue<unknown>()
      const ws = connectAndSubscribe(zeroPort, `cg-three-${Date.now()}`, downstream)
      try {
        await waitForRowPut(downstream, 'row-1', 45000)
      } catch (err) {
        throw new Error(`${(err as Error).message}\n${diagnostics()}`)
      } finally {
        ws.close()
        holder.close()
      }
    }
  )

  test(
    'consumed changes are confirmed and purged from _zero_changes',
    { timeout: 120000 },
    async () => {
      // a live consumer: hydrate a client group and KEEP it connected so the
      // change-streamer stays on the replication stream and can ack.
      const downstream = new Queue<unknown>()
      const ws = connectAndSubscribe(zeroPort, `cg-purge-${Date.now()}`, downstream)
      try {
        await waitForRowPut(downstream, 'row-1', 45000)

        // app-origin write → change-capture row → streamed → consumer commits
        // (proved by the poke arriving at the client)
        await seed.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
          'row-purge',
          'purge-me',
          2,
        ])
        await waitForRowPut(downstream, 'row-purge', 45000)

        // the consumer durably committed the change (the poke can only come
        // from a committed changeLog entry), so its standby status update must
        // confirm the batch and the producer must purge the retained rows —
        // real pg's WAL-retention contract. the 2026-07 CF incident: this
        // never happened, _zero_changes retained everything forever, and every
        // embed boot re-streamed the whole set (~$60/day of DO rows-written).
        const deadline = Date.now() + 30000
        let retained = -1
        while (Date.now() < deadline) {
          const res = await seed.query<{ n: string | number }>(
            `SELECT COUNT(*) AS n FROM _orez._zero_changes`
          )
          retained = Number(res.rows[0]?.n)
          if (retained === 0) return
          await new Promise((r) => setTimeout(r, 1000))
        }
        throw new Error(
          `consumer committed the change but _zero_changes still retains ` +
            `${retained} row(s) after 30s — standby-feedback confirmation is not ` +
            `reaching the producer, so retention can never trim\n${diagnostics()}`
        )
      } finally {
        ws.close()
      }
    }
  )

  test(
    'a keyless table with a composite unique index syncs updates and deletes',
    { timeout: 120000 },
    async () => {
      // hydrate a client group on the member table (replica spec must key it
      // off the member_pkey unique index — there is no PRIMARY KEY)
      const downstream = new Queue<unknown>()
      const ws = connectAndSubscribeMember(
        zeroPort,
        `cg-member-${Date.now()}`,
        downstream
      )
      try {
        await waitForMemberRole(downstream, 'acc-1', 'member', 45000)

        // the 2026-07-10 soot prod crash: the FIRST UPDATE through the
        // change-streamer for this shape threw in change-processor #getKey
        // ("Cannot replicate table without a PRIMARY KEY or UNIQUE INDEX")
        // when the replica lacked the index. a healthy replica must stream it.
        await seed.query(
          `UPDATE member SET role = $1 WHERE "accountId" = $2 AND "userId" = $3`,
          ['admin', 'acc-1', 'user-1']
        )
        await waitForMemberRole(downstream, 'acc-1', 'admin', 45000)

        // deletes exercise the same key derivation
        await seed.query(
          `INSERT INTO member ("accountId", "userId", role) VALUES ($1, $2, $3)`,
          ['acc-2', 'user-2', 'guest']
        )
        await waitForMemberRole(downstream, 'acc-2', 'guest', 45000)
        await seed.query(`DELETE FROM member WHERE "accountId" = $1 AND "userId" = $2`, [
          'acc-2',
          'user-2',
        ])
        const deadline = Date.now() + 45000
        let deleted = false
        while (Date.now() < deadline && !deleted) {
          const remaining = Math.max(1000, deadline - Date.now())
          const msg = (await downstream.dequeue('timeout' as any, remaining)) as any
          if (msg === 'timeout') break
          if (Array.isArray(msg) && msg[0] === 'pokePart' && msg[1]?.rowsPatch) {
            for (const row of msg[1].rowsPatch) {
              if (
                row.op === 'del' &&
                row.tableName === 'member' &&
                row.id?.accountId === 'acc-2'
              ) {
                deleted = true
              }
            }
          }
        }
        if (!deleted) {
          throw new Error(`timed out waiting for del of member/acc-2\n${diagnostics()}`)
        }
      } catch (err) {
        throw new Error(`${(err as Error).message}\n${diagnostics()}`)
      } finally {
        ws.close()
      }
    }
  )
})
