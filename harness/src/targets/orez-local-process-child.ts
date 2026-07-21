// Child process for the kill/restart fault lane. It owns the sync server and a
// real file-backed SQLite authority; the parent keeps stock clients alive and
// SIGKILLs this process mid-churn before starting a new process on the same
// port/database file.
import { Database } from 'bun:sqlite'
import { createServer } from 'node:http'

import { createHarnessSyncServer } from '../executor-host.js'
import { seedSqlite, userIDFromAuth } from '../fixture-data.js'

import type { ZeroHttpSyncDb as SyncDb } from '../../../src/zero-http/mount.js'

const port = Number(process.env.ZHARNESS_PROCESS_PORT)
const filename = process.env.ZHARNESS_PROCESS_DB
if (!Number.isInteger(port) || port <= 0) throw new Error('invalid process target port')
if (!filename) throw new Error('missing process target database file')

const sqlite = new Database(filename, { create: true })
sqlite.exec('PRAGMA journal_mode = WAL')
sqlite.exec('PRAGMA synchronous = FULL')

const db: SyncDb = {
  exec(sql, params = []) {
    sqlite.query(sql).run(...(params as never[]))
  },
  all(sql, params = []) {
    return sqlite.query(sql).all(...(params as never[])) as Record<string, unknown>[]
  },
  transaction<T>(fn: () => T): T {
    return sqlite.transaction(fn)() as T
  },
}

const seeded = db.all(
  `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'project'`
)
if (seeded.length === 0) db.transaction(() => seedSqlite(db))

const sync = createHarnessSyncServer(db)

const json = (
  res: Parameters<Parameters<typeof createServer>[0]>[1],
  value: unknown,
  status = 200
) => {
  res.statusCode = status
  res.setHeader('content-type', 'application/json')
  res.end(JSON.stringify(value))
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', 'http://localhost')
  try {
    if (url.pathname === '/admin/health') {
      json(res, { ok: true, pid: process.pid })
      return
    }

    const chunks: Buffer[] = []
    for await (const chunk of req) chunks.push(chunk as Buffer)
    const body = JSON.parse(Buffer.concat(chunks).toString() || 'null')
    if (url.pathname === '/admin/sql') {
      json(res, { rows: db.all((body as { query: string }).query) })
      return
    }

    const userID = userIDFromAuth(req.headers.authorization)
    if (!userID) {
      json(res, { error: 'missing auth' }, 401)
      return
    }
    if (url.pathname === '/pull') {
      json(res, await sync.handlePull(body, { id: userID }))
      return
    }
    if (url.pathname === '/push') {
      json(res, await sync.handlePush(body, { id: userID }))
      return
    }
    json(res, { error: 'not found' }, 404)
  } catch (error) {
    const status = (error as { status?: number }).status ?? 500
    if (status === 500) console.error('[orez-local-process]', error)
    json(res, { error: String(error) }, status)
  }
})

server.listen(port, '127.0.0.1')

process.on('SIGTERM', () => {
  server.close(() => {
    sqlite.close()
    process.exit(0)
  })
})
