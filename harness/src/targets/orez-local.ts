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
import {
  MutationAppError,
  type SyncDb,
  createSyncServer,
  tablesFromZeroSchema,
} from '../../../src/sync-server/sync-server'
import { DDL, SEED, jsonColumns, mutators, schema } from '../fixture.js'
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

// server-side custom mutator execution against sqlite. same names/semantics
// as the client registry in fixture.ts; plain SQL like soot's server side.
// semantics must MATCH the client impls exactly (e.g. project.delete does not
// cascade, because the client mutator doesn't).
function executeMutator(
  tx: SyncDb,
  name: string,
  args: unknown,
  _ctx: { userID: string }
) {
  switch (name) {
    case 'project.create': {
      const a = args as { id: string; ownerId: string; name: string }
      const exists = tx.all(`SELECT 1 FROM project WHERE id = ?`, [a.id])
      if (exists.length > 0) throw new MutationAppError('exists')
      tx.exec(`INSERT INTO project (id, "ownerId", name) VALUES (?, ?, ?)`, [
        a.id,
        a.ownerId,
        a.name,
      ])
      return
    }
    case 'project.rename': {
      const a = args as { id: string; name: string }
      tx.exec(`UPDATE project SET name = ? WHERE id = ?`, [a.name, a.id])
      return
    }
    case 'project.delete': {
      const a = args as { id: string }
      tx.exec(`DELETE FROM project WHERE id = ?`, [a.id])
      return
    }
    case 'member.add': {
      const a = args as { id: string; projectId: string; userId: string }
      tx.exec(`INSERT INTO member (id, "projectId", "userId") VALUES (?, ?, ?)`, [
        a.id,
        a.projectId,
        a.userId,
      ])
      return
    }
    case 'member.remove': {
      const a = args as { id: string }
      tx.exec(`DELETE FROM member WHERE id = ?`, [a.id])
      return
    }
    case 'task.create': {
      const a = args as {
        id: string
        projectId: string
        title: string
        rank: number
        done: boolean
        meta?: unknown
        dueAt?: number
      }
      tx.exec(
        `INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt")
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [
          a.id,
          a.projectId,
          a.title,
          a.rank,
          a.done ? 1 : 0,
          a.meta === undefined || a.meta === null ? null : JSON.stringify(a.meta),
          a.dueAt ?? null,
        ]
      )
      return
    }
    case 'task.toggle': {
      const a = args as { id: string }
      const existing = tx.all(`SELECT done FROM task WHERE id = ?`, [a.id])
      if (existing.length === 0) throw new MutationAppError('not-found')
      tx.exec(`UPDATE task SET done = ? WHERE id = ?`, [existing[0]!.done ? 0 : 1, a.id])
      return
    }
    default:
      throw new Error(`unknown mutator: ${name}`)
  }
}

function userIDFromAuth(header: string | undefined): string | null {
  return header?.match(/^Bearer token-(.+)$/)?.[1] ?? null
}

export async function startOrezLocal(opts?: {
  port?: number
  pullIntervalMs?: number
}): Promise<SyncTarget> {
  // random per run — see stock-zero.ts port note
  const port = opts?.port ?? 59_000 + Math.floor(Math.random() * 4_000)
  const sqlite = new Database(':memory:')
  const db = bunSqliteDb(sqlite)

  for (const stmt of DDL) db.exec(stmt)
  for (const [tableName, rows] of Object.entries(SEED)) {
    const jsonCols = jsonColumns(tableName)
    for (const row of rows) {
      const cols = Object.keys(row)
      // sqlite json storage = the JSON-ENCODED text of the value (matches
      // zero's replica model), so scalar json values round-trip too
      const values = Object.entries(row).map(([k, v]) =>
        jsonCols.has(k) && v !== null
          ? JSON.stringify(v)
          : typeof v === 'boolean'
            ? v
              ? 1
              : 0
            : v
      )
      db.exec(
        `INSERT INTO "${tableName}" (${cols.map((c) => `"${c}"`).join(', ')})
         VALUES (${cols.map(() => '?').join(', ')})`,
        values
      )
    }
  }

  const sync = createSyncServer({
    db,
    tables: tablesFromZeroSchema(schema),
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
      const rows = db.all(query)
      sync.bumpVersion()
      return rows
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
