// orez-local target: the sqlite-native sync server core (orez
// src/sync-server) hosted in-process over bun:sqlite, serving STOCK zero
// clients through on-zero's production http-pull transport. pure sqlite, no
// postgres, no zero-cache, no docker — this target IS the rewrite's server
// core under test (plans/zero-server-rewrite.md phase 2).
import { Database } from 'bun:sqlite'
import { createServer, type Server } from 'node:http'
import { Zero } from '@rocicorp/zero'
// the production transport source, verbatim (self-contained module, no
// imports). the npm package does not expose a transport subpath export yet;
// running the checkout source keeps the harness on the exact code soot ships.
import { ensureHttpPullTransport } from '../../../../takeout/packages/on-zero/src/httpPullTransport'
import { type SyncDb, createSyncServer } from '../../../src/sync-server/sync-server'
import { TABLES, executeMutator, seedSqlite, userIDFromAuth } from '../fixture-data.js'
import { mutators, schema } from '../fixture.js'
import type { Rows, SyncTarget } from '../target.js'

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
}): Promise<SyncTarget> {
  // random per run — see stock-zero.ts port note
  const port = opts?.port ?? 59_000 + Math.floor(Math.random() * 4_000)
  const sqlite = new Database(':memory:')
  const db = bunSqliteDb(sqlite)

  seedSqlite(db)

  const sync = createSyncServer({
    db,
    tables: TABLES,
    mutate: executeMutator,
  })

  const server: Server = createServer(async (req, res) => {
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
      res.setHeader('content-type', 'application/json')
      res.end(JSON.stringify(response))
    } catch (error) {
      const status = (error as { status?: number }).status ?? 500
      if (status === 500) console.error('[orez-local]', error)
      res.statusCode = status
      res.end(JSON.stringify({ error: String(error) }))
    }
  })
  await new Promise<void>((resolve) => server.listen(port, '127.0.0.1', resolve))

  const origin = `http://127.0.0.1:${port}`
  // production transport: fake-WebSocket over HTTP for this origin only;
  // other origins (e.g. the stock-zero target in the same process) pass
  // through to the native WebSocket untouched
  const transport = ensureHttpPullTransport({
    origin,
    pullIntervalMs: opts?.pullIntervalMs ?? 250,
  })

  const clients: Zero<typeof schema, typeof mutators>[] = []
  let clientN = 0

  return {
    name: 'orez-local',

    createClient(userID: string) {
      const zero = new Zero({
        server: origin,
        userID,
        auth: `token-${userID}`,
        schema,
        mutators,
        kvStore: 'mem' as const,
        storageKey: `zharness-local-${++clientN}`,
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

    async close() {
      while (clients.length) await clients.pop()?.close()
      transport.uninstall()
      await new Promise<void>((resolve, reject) =>
        server.close((err) => (err ? reject(err) : resolve()))
      )
      sqlite.close()
    },
  }
}
