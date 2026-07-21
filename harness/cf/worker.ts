import { createHarnessSyncServer, type HarnessSyncServer } from '../src/executor-host.js'
import { seedSqlite, userIDFromAuth } from '../src/fixture-data'

// zharness-sync: the executor-backed zero-http mount hosted in a cloudflare
// durable object over ctx.storage.sql. each first path
// segment is a namespace routed to its own DO (fresh dataset per harness
// run): POST /<ns>/pull, /<ns>/push (bearer token-<userID>), and
// /<ns>/admin/sql guarded by the ADMIN_KEY secret (the harness oracle +
// upstream-write channel; a real deploy would never expose this).
//
import type { ZeroHttpSyncDb as SyncDb } from '../../src/zero-http/mount.js'

type Env = {
  SYNC_DO: DurableObjectNamespace
  ADMIN_KEY: string
}

// minimal DO typings (no workers-types dep for one file)
type SqlStorageLike = {
  exec(query: string, ...bindings: unknown[]): { toArray(): Record<string, unknown>[] }
}
type DurableObjectState = {
  storage: {
    sql: SqlStorageLike
    transaction<Value>(work: () => Value | Promise<Value>): Promise<Value>
    transactionSync<Value>(work: () => Value): Value
  }
}
type DurableObjectNamespace = {
  idFromName(name: string): unknown
  get(id: unknown): { fetch(request: Request): Promise<Response> }
}

// Deterministic harness analogue of a DO eviction: after this much inactivity,
// the next request discards every in-memory sync-server object and reconstructs
// it over the same durable SQL storage. A real platform eviction also reruns
// the class constructor; bootID changes in either case, giving the lane proof
// that it crossed a memory-teardown boundary instead of merely waiting.
const IDLE_TEARDOWN_MS = 5_000

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
  readonly #state: DurableObjectState
  #db: SyncDb
  #sync: HarnessSyncServer | null = null
  #bootID = crypto.randomUUID()
  #lastRequestAt = 0
  #hibernations = 0

  constructor(state: DurableObjectState) {
    this.#state = state
    this.#db = doSqliteDb(state)
  }

  #ensure(): HarnessSyncServer {
    if (this.#sync) return this.#sync
    const seeded = this.#db.all(
      `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'project'`
    )
    if (seeded.length === 0) {
      this.#db.transaction(() => seedSqlite(this.#db))
    }
    this.#sync = createHarnessSyncServer(this.#db, {
      transaction: (work) => this.#state.storage.transaction(work),
    })
    return this.#sync
  }

  async fetch(request: Request): Promise<Response> {
    const now = Date.now()
    if (this.#lastRequestAt > 0 && now - this.#lastRequestAt >= IDLE_TEARDOWN_MS) {
      this.#sync = null
      this.#bootID = crypto.randomUUID()
      this.#hibernations++
    }
    this.#lastRequestAt = now
    const url = new URL(request.url)
    // path arrives as /<ns>/<route...>; strip the namespace segment
    const [, , ...rest] = url.pathname.split('/')
    const route = `/${rest.join('/')}`
    const sync = this.#ensure()
    await sync.ready()

    const json = (value: unknown, status = 200) =>
      new Response(JSON.stringify(value), {
        status,
        headers: { 'content-type': 'application/json' },
      })

    try {
      if (route === '/admin/status') {
        return json({
          bootID: this.#bootID,
          idleTeardownMs: IDLE_TEARDOWN_MS,
          hibernations: this.#hibernations,
        })
      }
      if (route === '/admin/sql') {
        // the core's table triggers feed the change log, so admin writes
        // advance the watermark on their own
        const body = (await request.json()) as { query: string }
        return json({ rows: this.#db.all(body.query) })
      }

      const userID = userIDFromAuth(request.headers.get('authorization'))
      if (!userID) return json({ error: 'missing auth' }, 401)

      if (route === '/pull') {
        return json(
          await sync.handlePull((await request.json()) as never, { id: userID })
        )
      }
      if (route === '/push') {
        return json(
          await sync.handlePush((await request.json()) as never, { id: userID })
        )
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
