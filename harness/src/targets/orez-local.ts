// orez-local target: the sqlite-native sync server core (orez
// src/sync-server) hosted in-process over bun:sqlite, serving STOCK zero
// clients through on-zero's production http-pull transport. pure sqlite, no
// postgres, no zero-cache, no docker — this target IS the rewrite's server
// core under test (plans/zero-server-rewrite.md phase 2).
import { Database } from 'bun:sqlite'
import { createServer, type Server } from 'node:http'

import { Zero } from '@rocicorp/zero'

import {
  type SyncDb,
  type SyncServerConfig,
  createSyncServer,
} from '../../../src/sync-server/sync-server'
import {
  assertExpectedExactlyOncePush,
  parseExactlyOncePush,
  type ExpectedExactlyOncePush,
} from '../consistency/exactly-once-workload.js'
import { TABLES, executeMutator, seedSqlite, userIDFromAuth } from '../fixture-data.js'
import { mutators, schema } from '../fixture.js'
// the production transport source, vendored verbatim (self-contained module,
// no imports) so CI runs without the takeout checkout — provenance + refresh
// instructions in the vendor file header
import { ensureHttpPullTransport } from '../vendor/httpPullTransport.js'

import type { Rows, SyncTarget } from '../target.js'

export type PullObservation = {
  body: unknown
  response: unknown
}

export type OrezLocalTarget = SyncTarget & {
  readonly origin: string
  dropNextPushResponse(): void
  pull(): Promise<void>
  invalidate(): void
  resetCursor(): void
  restart(downForMs?: number): Promise<void>
  armExactlyOnceResponseDrop(
    expected: ExpectedExactlyOncePush,
    onStage: (stage: 'arm' | 'fire' | 'heal') => void
  ): void
}

function bunSqliteDb(db: Database): SyncDb {
  return {
    exec(sql, params = []) {
      db.query(sql).run(...(params as never[]))
    },
    all(sql, params = []) {
      return db.query(sql).all(...(params as never[])) as Record<string, unknown>[]
    },
    transaction<T>(fn: () => T): T {
      return db.transaction(fn)() as T
    },
  }
}

export async function startOrezLocal(opts?: {
  port?: number
  pullIntervalMs?: number
  retainChanges?: number
  visible?: SyncServerConfig['visible']
  onPull?: (observation: PullObservation) => void
  fetch?: typeof fetch
}): Promise<OrezLocalTarget> {
  // random per run — see stock-zero.ts port note
  const port = opts?.port ?? 59_000 + Math.floor(Math.random() * 4_000)
  const sqlite = new Database(':memory:')
  const db = bunSqliteDb(sqlite)

  seedSqlite(db)

  const sync = createSyncServer({
    db,
    tables: TABLES,
    mutate: executeMutator,
    retainChanges: opts?.retainChanges,
    visible: opts?.visible,
  })

  let dropPushResponse = false
  let exactDrop:
    | {
        expected: ExpectedExactlyOncePush
        onStage: (stage: 'arm' | 'fire' | 'heal') => void
      }
    | undefined
  const handleRequest: Parameters<typeof createServer>[0] = async (req, res) => {
    try {
      const url = new URL(req.url ?? '/', 'http://localhost')
      const chunks: Buffer[] = []
      for await (const chunk of req) chunks.push(chunk as Buffer)
      const body = JSON.parse(Buffer.concat(chunks).toString() || 'null')
      const userID = userIDFromAuth(req.headers.authorization)
      if (!userID) {
        res.statusCode = 401
        res.end(JSON.stringify({ error: 'missing auth' }))
        return
      }
      const parsedExact =
        url.pathname === '/push' && exactDrop ? parseExactlyOncePush(body) : undefined
      if (parsedExact && exactDrop) {
        assertExpectedExactlyOncePush(parsedExact, exactDrop.expected)
      }
      const response =
        url.pathname === '/pull'
          ? sync.handlePull(body, userID)
          : url.pathname === '/push'
            ? sync.handlePush(body, userID)
            : null
      if (!response) {
        res.statusCode = 404
        res.end()
        return
      }
      if (url.pathname === '/pull') opts?.onPull?.({ body, response })
      if (url.pathname === '/push' && exactDrop) {
        const drop = exactDrop
        drop.onStage('fire')
        exactDrop = undefined
        drop.onStage('heal')
        res.destroy()
        return
      }
      if (url.pathname === '/push' && dropPushResponse) {
        dropPushResponse = false
        res.destroy()
        return
      }
      res.setHeader('content-type', 'application/json')
      res.end(JSON.stringify(response))
    } catch (error) {
      const status = (error as { status?: number }).status ?? 500
      if (status === 500) console.error('[orez-local]', error)
      res.statusCode = status
      res.end(JSON.stringify({ error: String(error) }))
    }
  }

  let httpServer: Server | undefined
  const listen = async () => {
    httpServer = createServer(handleRequest)
    await new Promise<void>((resolve) => httpServer!.listen(port, '127.0.0.1', resolve))
  }
  const closeHttpServer = async () => {
    const closing = httpServer
    httpServer = undefined
    if (!closing) return
    await new Promise<void>((resolve, reject) =>
      closing.close((err) => (err ? reject(err) : resolve()))
    )
  }
  await listen()

  const origin = `http://127.0.0.1:${port}`
  // production transport: fake-WebSocket over HTTP for this origin only;
  // other origins (e.g. the stock-zero target in the same process) pass
  // through to the native WebSocket untouched
  const transport = ensureHttpPullTransport({
    origin,
    fetch: opts?.fetch,
    pullIntervalMs: opts?.pullIntervalMs ?? 250,
  })

  const clients: Zero<typeof schema, typeof mutators>[] = []
  let clientN = 0

  return {
    name: 'orez-local',
    origin,

    createClient(userID: string, storage) {
      const zero = new Zero({
        server: origin,
        userID,
        auth: `token-${userID}`,
        schema,
        mutators,
        kvStore: storage?.kvStore ?? ('mem' as const),
        onClientStateNotFound: storage?.onClientStateNotFound,
        storageKey: storage?.storageKey ?? `zharness-local-${++clientN}`,
      })
      clients.push(zero)
      return zero
    },

    async sql(query: string): Promise<Rows> {
      // the core's table triggers feed the change log, so upstream writes
      // advance the watermark on their own
      return db.all(query)
    },

    async oracle(query: string): Promise<Rows> {
      return db.all(query)
    },

    async metrics() {
      return { serverRssMb: Math.round(process.memoryUsage().rss / 1024 / 1024) }
    },

    dropNextPushResponse() {
      dropPushResponse = true
    },

    pull() {
      return transport.pull()
    },

    armExactlyOnceResponseDrop(expected, onStage) {
      if (exactDrop) throw new Error('exactly-once response drop already armed')
      exactDrop = { expected: structuredClone(expected), onStage }
      onStage('arm')
    },

    invalidate() {
      sync.invalidate()
    },

    resetCursor() {
      db.transaction(() => {
        db.exec(`DELETE FROM _zsync_changes`)
        db.exec(`UPDATE _zsync_meta SET floor = 0`)
      })
    },

    async restart(downForMs = 100) {
      await closeHttpServer()
      await new Promise((resolve) => setTimeout(resolve, downForMs))
      await listen()
    },

    async close() {
      while (clients.length) await clients.pop()?.close()
      transport.uninstall()
      await closeHttpServer()
      sqlite.close()
    },
  }
}
