import { WorkerEntrypoint } from 'cloudflare:workers'

import { ZeroDO } from '../../../src/cf-do/worker.js'
import { createSyncDurableObject, createSyncWorker } from './index.js'

import type { SyncHostConfig, SyncHostEnv, ZeroSchemaConfig } from './index.js'

const schema = {
  tables: {
    item: {
      columns: {
        id: { type: 'string' },
        label: { type: 'string' },
        rank: { type: 'number' },
        done: { type: 'boolean' },
        meta: { type: 'json' },
      },
      primaryKey: ['id'],
    },
  },
} as const satisfies ZeroSchemaConfig

type Fetcher = { fetch(input: string | Request, init?: RequestInit): Promise<Response> }
interface Env extends SyncHostEnv {
  DATA: Fetcher
  APP: Fetcher
  UPSTREAM_DO: DurableObjectNamespace
}

const config: SyncHostConfig<Env> = {
  hostVersion: 'upstream-ingest-harness',
  schema,
  mutateUrl: '/api/zero/push',
  mutateBinding: 'APP',
  upstream: {
    binding: 'DATA',
    namespacePath: (namespace) => `/${namespace}`,
    changeLimit: 2,
    intervalMs: 1_000,
  },
  initialize(sql) {
    sql.exec(
      'CREATE TABLE IF NOT EXISTS item (id TEXT PRIMARY KEY, label TEXT NOT NULL, rank REAL NOT NULL, done INTEGER NOT NULL, meta TEXT)'
    )
  },
  authenticate(request) {
    const userID = request.headers.get('authorization')?.match(/^Bearer token-(.+)$/)?.[1]
    return userID ? { userID } : null
  },
  namespace(request) {
    return new URL(request.url).pathname.split('/')[1] || null
  },
}

export const SyncDurableObject = createSyncDurableObject(config)
export { ZeroDO }

async function upstreamFetch(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url)
  const [, namespace, ...rest] = url.pathname.split('/')
  if (!namespace) return new Response('namespace required', { status: 400 })
  const stub = env.UPSTREAM_DO.get(env.UPSTREAM_DO.idFromName(namespace))
  const exec = async (sql: string, params: unknown[] = []) => {
    const response = await stub.fetch('https://upstream.invalid/exec', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ sql, params }),
    })
    if (!response.ok) throw new Error(`upstream init failed: ${await response.text()}`)
  }
  await exec(
    'CREATE TABLE IF NOT EXISTS item (id TEXT PRIMARY KEY, label TEXT NOT NULL, rank REAL NOT NULL, done INTEGER NOT NULL, meta TEXT)'
  )
  await exec(
    'CREATE TABLE IF NOT EXISTS _zero_schema_tables (name TEXT PRIMARY KEY, schema_json TEXT NOT NULL)'
  )
  await exec(
    'INSERT OR IGNORE INTO _zero_schema_tables (name, schema_json) VALUES (?, ?)',
    ['item', JSON.stringify(schema.tables.item)]
  )
  url.pathname = `/${rest.join('/')}`
  return stub.fetch(new Request(url, request))
}

/** Self service-binding target backed by the real ZeroSqlDO. */
export class DataService extends WorkerEntrypoint<Env> {
  fetch(request: Request): Promise<Response> {
    const pathname = new URL(request.url).pathname
    if (!pathname.endsWith('/changes') && !pathname.endsWith('/snapshot')) {
      return Promise.resolve(
        new Response('DATA route rejected non-feed request', { status: 418 })
      )
    }
    return upstreamFetch(request, this.env)
  }
}

export class AppService extends WorkerEntrypoint<Env> {
  fetch(request: Request): Promise<Response> {
    if (!new URL(request.url).pathname.endsWith('/api/zero/push')) {
      return Promise.resolve(
        new Response('APP route rejected non-push request', { status: 418 })
      )
    }
    return upstreamFetch(request, this.env)
  }
}

const syncWorker = createSyncWorker(config)
export default {
  fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname.startsWith('/upstream/')) {
      url.pathname = url.pathname.slice('/upstream'.length)
      return upstreamFetch(new Request(url, request), env)
    }
    return syncWorker.fetch!(request as never, env, ctx) as Promise<Response>
  },
}
