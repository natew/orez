// zharness-sync: the orez sync-server core hosted in a cloudflare durable
// object over ctx.storage.sql — the orez-cf harness target. each first path
// segment is a namespace routed to its own DO (fresh dataset per harness
// run): POST /<ns>/pull, /<ns>/push (bearer token-<userID>), and
// /<ns>/admin/sql guarded by the ADMIN_KEY secret (the harness oracle +
// upstream-write channel; a real deploy would never expose this).
//
// bundles fixture-data + orez src/sync-server only — no @rocicorp/zero in
// the worker.
import { type SyncDb, type SyncServer, createSyncServer } from '../../src/sync-server/sync-server'
import { DDL, TABLES, executeMutator, seedSqlite, userIDFromAuth } from '../src/fixture-data'

type Env = {
  SYNC_DO: DurableObjectNamespace
  ADMIN_KEY: string
}

// minimal DO typings (no workers-types dep for one file)
type SqlStorageLike = {
  exec(query: string, ...bindings: unknown[]): { toArray(): Record<string, unknown>[] }
}
type DurableObjectState = {
  storage: { sql: SqlStorageLike; transactionSync<T>(fn: () => T): T }
}
type DurableObjectNamespace = {
  idFromName(name: string): unknown
  get(id: unknown): { fetch(request: Request): Promise<Response> }
}

function doSqliteDb(state: DurableObjectState): SyncDb {
  return {
    exec(sql, params = []) {
      state.storage.sql.exec(sql, ...params)
    },
    all(sql, params = []) {
      return state.storage.sql.exec(sql, ...params).toArray()
    },
    transaction<T>(fn: () => T): T {
      return state.storage.transactionSync(fn)
    },
  }
}

export class SyncServerDO {
  #db: SyncDb
  #sync: SyncServer | null = null

  constructor(state: DurableObjectState) {
    this.#db = doSqliteDb(state)
  }

  #ensure(): SyncServer {
    if (this.#sync) return this.#sync
    const seeded = this.#db.all(
      `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'project'`
    )
    if (seeded.length === 0) {
      this.#db.transaction(() => seedSqlite(this.#db))
    }
    this.#sync = createSyncServer({ db: this.#db, tables: TABLES, mutate: executeMutator })
    return this.#sync
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    // path arrives as /<ns>/<route...>; strip the namespace segment
    const [, , ...rest] = url.pathname.split('/')
    const route = `/${rest.join('/')}`
    const sync = this.#ensure()

    const json = (value: unknown, status = 200) =>
      new Response(JSON.stringify(value), {
        status,
        headers: { 'content-type': 'application/json' },
      })

    try {
      if (route === '/admin/sql') {
        const body = (await request.json()) as { query: string; write?: boolean }
        const rows = this.#db.all(body.query)
        if (body.write) sync.bumpVersion()
        return json({ rows })
      }

      const userID = userIDFromAuth(request.headers.get('authorization'))
      if (!userID) return json({ error: 'missing auth' }, 401)

      if (route === '/pull') {
        return json(sync.handlePull((await request.json()) as never, userID))
      }
      if (route === '/push') {
        return json(sync.handlePush((await request.json()) as never, userID))
      }
      return json({ error: 'not found' }, 404)
    } catch (error) {
      const status = (error as { status?: number }).status ?? 500
      return json({ error: String(error) }, status)
    }
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    const [, ns, first] = url.pathname.split('/')
    if (!ns) return new Response('zharness-sync', { status: 200 })
    // admin route auth happens at the worker edge, before the DO
    if (first === 'admin' && request.headers.get('x-admin-key') !== env.ADMIN_KEY) {
      return new Response(JSON.stringify({ error: 'forbidden' }), { status: 403 })
    }
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(ns))
    return stub.fetch(request)
  },
}
