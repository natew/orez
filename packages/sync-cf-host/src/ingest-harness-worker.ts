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

const runawayNamespaces = new Set<string>()
const numericTextNamespaces = new Set<string>()
const jsonValueNamespaces = new Set<string>()
const hydratedNamespaces = new Set<string>()
let delegatedFailuresRemaining = 0
let delegatedAttempts = 0
let delegatedPushFailedRemaining = 0

const config: SyncHostConfig<Env> = {
  hostVersion: 'upstream-ingest-harness',
  schema,
  mutateUrl: '/api/zero/push',
  mutateBinding: 'APP',
  delegatedPushRetry: {
    maxAttempts: 3,
    initialBackoffMs: 10,
    maxBackoffMs: 20,
    timeoutMs: 1_000,
  },
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
  url.pathname = `/${rest.join('/')}`
  if (url.pathname === '/_orez/write-budget') return stub.fetch(new Request(url, request))
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
  return stub.fetch(new Request(url, request))
}

/** Self service-binding target backed by the real ZeroSqlDO. */
export class DataService extends WorkerEntrypoint<Env> {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const pathname = url.pathname
    if (
      !pathname.endsWith('/changes') &&
      !pathname.endsWith('/snapshot') &&
      !pathname.endsWith('/_orez/write-budget')
    ) {
      return Promise.resolve(
        new Response('DATA route rejected non-feed request', { status: 418 })
      )
    }
    const namespace = pathname.split('/')[1] ?? ''
    if (pathname.endsWith('/changes') && jsonValueNamespaces.has(namespace)) {
      const cursor = Number(new URL(request.url).searchParams.get('watermark') ?? 0)
      const values = [
        { nested: { tags: ['a', 2, true] } },
        [1, 'two', null],
        '42',
        'true',
        'null',
        '{"looks":"encoded"}',
        42.5,
        true,
      ]
      return Promise.resolve(
        Response.json({
          watermark: values.length,
          changes: values
            .map((meta, index) => ({
              watermark: index + 1,
              tableName: 'item',
              op: 'INSERT',
              rowData: {
                id: `json-${index}`,
                label: 'json round trip',
                rank: index,
                done: false,
                meta,
              },
              oldData: null,
            }))
            .filter((change) => change.watermark > cursor)
            .slice(0, 2),
        })
      )
    }
    if (pathname.endsWith('/changes') && numericTextNamespaces.has(namespace)) {
      const watermark = Number(new URL(request.url).searchParams.get('watermark') ?? 0)
      return Promise.resolve(
        Response.json({
          watermark: 2,
          changes:
            watermark >= 2
              ? []
              : [
                  {
                    watermark: 1,
                    tableName: 'item',
                    op: 'INSERT',
                    rowData: {
                      id: 'numeric-text',
                      label: 'SQL timestamp text',
                      rank: '2026-07-11 13:34:46',
                      done: false,
                      meta: null,
                    },
                    oldData: null,
                  },
                  {
                    watermark: 2,
                    tableName: 'item',
                    op: 'INSERT',
                    rowData: {
                      id: 'numeric-native',
                      label: 'native JSON number',
                      rank: 1783776886000,
                      done: false,
                      meta: null,
                    },
                    oldData: null,
                  },
                ],
        })
      )
    }
    if (pathname.endsWith('/changes') && runawayNamespaces.has(namespace)) {
      return Promise.resolve(
        Response.json({
          watermark: 100,
          changes: [
            {
              watermark: 1,
              tableName: 'item',
              op: 'INSERT',
              rowData: {
                id: 'runaway-replay',
                label: 'replayed without cursor progress',
                rank: 1,
                done: false,
                meta: null,
              },
              oldData: null,
            },
          ],
        })
      )
    }
    const response = await upstreamFetch(request, this.env)
    if (pathname.endsWith('/changes') && response.ok) hydratedNamespaces.add(namespace)
    return response
  }
}

export class AppService extends WorkerEntrypoint<Env> {
  fetch(request: Request): Promise<Response> {
    if (!new URL(request.url).pathname.endsWith('/api/zero/push')) {
      return Promise.resolve(
        new Response('APP route rejected non-push request', { status: 418 })
      )
    }
    const namespace = new URL(request.url).pathname.split('/')[1] ?? ''
    if (!hydratedNamespaces.has(namespace)) {
      return Promise.resolve(
        Response.json({ error: 'schema provisioning has not completed' }, { status: 500 })
      )
    }
    delegatedAttempts++
    if (delegatedPushFailedRemaining > 0) {
      delegatedPushFailedRemaining--
      return Promise.resolve(
        Response.json({
          kind: 'PushFailed',
          origin: 'server',
          reason: 'database',
          mutationIDs: [{ clientID: 'writer', id: 2 }],
          message: 'synthetic mutation result persistence failure',
        })
      )
    }
    if (delegatedFailuresRemaining > 0) {
      delegatedFailuresRemaining--
      return Promise.resolve(
        Response.json({ error: 'synthetic delegated push failure' }, { status: 503 })
      )
    }
    return upstreamFetch(request, this.env)
  }
}

const syncWorker = createSyncWorker(config)
export default {
  fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname.startsWith('/json-values-control/')) {
      const namespace = url.pathname.slice('/json-values-control/'.length)
      return request
        .json()
        .catch(() => ({}))
        .then((body) => {
          if ((body as { enabled?: unknown }).enabled === true)
            jsonValueNamespaces.add(namespace)
          else jsonValueNamespaces.delete(namespace)
          return Response.json({ ok: true, namespace })
        })
    }
    if (url.pathname.startsWith('/numeric-text-control/')) {
      const namespace = url.pathname.slice('/numeric-text-control/'.length)
      return request
        .json()
        .catch(() => ({}))
        .then((body) => {
          if ((body as { enabled?: unknown }).enabled === true)
            numericTextNamespaces.add(namespace)
          else numericTextNamespaces.delete(namespace)
          return Response.json({ ok: true, namespace })
        })
    }
    if (url.pathname.startsWith('/runaway-control/')) {
      const namespace = url.pathname.slice('/runaway-control/'.length)
      return request
        .json()
        .catch(() => ({}))
        .then((body) => {
          if ((body as { enabled?: unknown }).enabled === true)
            runawayNamespaces.add(namespace)
          else runawayNamespaces.delete(namespace)
          return Response.json({
            ok: true,
            namespace,
            enabled: runawayNamespaces.has(namespace),
          })
        })
    }
    if (url.pathname === '/delegation-control') {
      if (request.method === 'GET') {
        return Promise.resolve(
          Response.json({
            delegatedFailuresRemaining,
            delegatedPushFailedRemaining,
            delegatedAttempts,
          })
        )
      }
      return request
        .json()
        .catch(() => ({}))
        .then((body) => {
          delegatedFailuresRemaining = Math.max(
            0,
            Number((body as { failures?: unknown }).failures) || 0
          )
          delegatedPushFailedRemaining = Math.max(
            0,
            Number((body as { pushFailed?: unknown }).pushFailed) || 0
          )
          delegatedAttempts = 0
          return Response.json({
            delegatedFailuresRemaining,
            delegatedPushFailedRemaining,
            delegatedAttempts,
          })
        })
    }
    if (url.pathname.startsWith('/upstream/')) {
      url.pathname = url.pathname.slice('/upstream'.length)
      return upstreamFetch(new Request(url, request), env)
    }
    return syncWorker.fetch!(request as never, env, ctx) as Promise<Response>
  },
}
