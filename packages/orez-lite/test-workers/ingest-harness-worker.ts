import { sha256 } from '@noble/hashes/sha2.js'
import { createBuilder, defineQueries, defineQuery } from '@rocicorp/zero'
import { WorkerEntrypoint } from 'cloudflare:workers'
import { defineStreamingFields } from 'orez-sync-executor/realtime'

import {
  createSyncDurableObject,
  createSyncWorker,
} from '../../sync-cf-host/src/index.js'
import { createApplicationSqlClient } from '../src/cf-do/application-sql.js'
import { createOrezDataWorker } from '../src/cf-do/lite-data-worker.js'

import type { SyncHostConfig, SyncHostEnv } from '../../sync-cf-host/src/index.js'
import type { OrezDataWorkerEnv } from '../src/cf-do/lite-data-worker.js'
import type { Schema } from '@rocicorp/zero'

const schema = {
  tables: {
    item: {
      name: 'item',
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
  relationships: {},
} as const satisfies Schema

const zql = createBuilder(schema as never) as { item: unknown }
const queries = defineQueries({
  allItems: defineQuery(() => zql.item as never),
})

type Fetcher = { fetch(input: string | Request, init?: RequestInit): Promise<Response> }
interface Env extends SyncHostEnv, OrezDataWorkerEnv {
  DATA: Fetcher
  APP: Fetcher
  UPSTREAM_DO: DurableObjectNamespace
}

const streaming = defineStreamingFields(schema, {
  item: {
    label: { maxBytes: 10_000, maxUpdatesPerSecond: 60, maxBytesPerSecond: 60_000 },
  },
})

const runawayNamespaces = new Set<string>()
const unpublishedNamespaces = new Set<string>()
const numericTextNamespaces = new Set<string>()
const jsonValueNamespaces = new Set<string>()
const hydratedNamespaces = new Set<string>()
const heldSnapshots = new Set<string>()
const holdSnapshotsAfterCursor = new Set<string>()
const activeSnapshots = new Set<string>()
const snapshotLimits = new Map<string, number[]>()
const heldDelegatedPushes = new Set<string>()
const activeDelegatedPushes = new Set<string>()
const completedDelegatedPushes = new Set<string>()
const heldChangeResponses = new Set<string>()
const activeHeldChangeResponses = new Set<string>()
const upstreamChangeRequests = new Map<string, number>()
const commitNotificationAttempts = new Map<string, number>()
const commitNotificationFailures = new Map<string, 'sync' | 'async'>()
const restoreObjects = new Map<string, string>()
let delegatedFailuresRemaining = 0
let delegatedAttempts = 0
let delegatedPushFailedRemaining = 0
let delegatedUrl = ''

const config: SyncHostConfig<Env> = {
  hostVersion: 'upstream-ingest-harness',
  schema,
  queries: queries as never,
  mutateUrl: '/api/zero/push?schema=feed_0&appID=feed',
  mutateOrigin: 'https://app.internal',
  mutateBinding: 'APP',
  delegatedPushRetry: {
    maxAttempts: 3,
    initialBackoffMs: 10,
    maxBackoffMs: 20,
    timeoutMs: 1_000,
  },
  upstream: {
    binding: 'DATA',
    namespacePath: (namespace) =>
      namespace.startsWith('root-mount-') ? '/' : `/${namespace}`,
    changeLimit: 2,
    intervalMs: 1_000,
    ingestBudgetRows: 600,
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
  authorize() {
    return true
  },
  authorizeWake(request) {
    return new URL(request.url).searchParams.get('wakeToken') === 'ingest-harness-wake'
      ? { userID: 'user-a' }
      : false
  },
  authorizeNotify(request, env) {
    return Boolean(env.ADMIN_KEY) && request.headers.get('x-admin-key') === env.ADMIN_KEY
  },
  namespace(request) {
    return new URL(request.url).pathname.split('/')[1] || null
  },
  streamingManifest: streaming.manifest,
  authorizeProduce(request) {
    return new URL(request.url).searchParams.get('adminKey') === 'ingest-harness-admin'
  },
}

const syncWorker = createSyncWorker(config)

const restoreBucket = {
  async get(key: string) {
    const value = restoreObjects.get(key)
    if (value === undefined) return null
    return {
      body: new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode(value))
          controller.close()
        },
      }),
      async json() {
        return JSON.parse(value)
      },
    }
  },
  async createMultipartUpload() {
    throw new Error('restore harness does not export backups')
  },
  async put(key: string, value: string) {
    restoreObjects.set(key, value)
  },
  async list({ prefix }: { prefix: string }) {
    return {
      objects: [...restoreObjects.keys()]
        .filter((key) => key.startsWith(prefix))
        .map((key) => ({ key })),
    }
  },
  async delete(keys: readonly string[]) {
    for (const key of keys) restoreObjects.delete(key)
  },
}

const dataWorker = createOrezDataWorker<Env>({
  name: 'ingestharness',
  schema: {
    version: 'ingest-harness-v1',
    schema,
    publicTables: [{ table: 'item', publicTable: 'public.item' }],
    migrate: async () => undefined,
  },
  backup: {
    format: 'ingest-harness-backup-v2',
    acceptedFormats: ['ingest-harness-backup-v1'],
    bucket: () => restoreBucket,
    inventory: async () => [],
    authorize(request, env) {
      return (
        Boolean(env.ADMIN_KEY) && request.headers.get('x-admin-key') === env.ADMIN_KEY
      )
    },
  },
  applicationSqlDidCommit(context) {
    commitNotificationAttempts.set(
      context.instance,
      (commitNotificationAttempts.get(context.instance) ?? 0) + 1
    )
    const failure = commitNotificationFailures.get(context.instance)
    if (failure === 'sync') throw new Error('synthetic synchronous notifier failure')
    if (failure === 'async') {
      return Promise.reject(new Error('synthetic asynchronous notifier failure'))
    }
    return syncWorker.notify(context.env, context.instance).then(async (response) => {
      if (!response.ok) {
        throw new Error(`sync notification returned ${response.status}`)
      }
      await response.body?.cancel()
    })
  },
})

export const SyncDurableObject = createSyncDurableObject(config)
export const ZeroDO = dataWorker.ZeroDO

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
    if (pathname === '/changes') {
      const cursor = Number(url.searchParams.get('watermark') ?? 0)
      return Promise.resolve(
        Response.json({
          watermark: 1,
          changes:
            cursor >= 1
              ? []
              : [
                  {
                    watermark: 1,
                    tableName: 'item',
                    op: 'INSERT',
                    rowData: {
                      id: 'root-feed-row',
                      label: 'root-mounted upstream feed',
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
    if (pathname === '/_orez/write-budget') {
      return Promise.resolve(Response.json({ enabled: true, rootMount: true }))
    }
    const namespace = pathname.split('/')[1] ?? ''
    if (pathname.endsWith('/changes')) {
      upstreamChangeRequests.set(
        namespace,
        (upstreamChangeRequests.get(namespace) ?? 0) + 1
      )
    }
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
    // a feed that drops rows for a table the replica models, reporting the drop
    // and still advancing the cursor. this is what a publication misconfiguration
    // looks like from the replica's side.
    if (pathname.endsWith('/changes') && unpublishedNamespaces.has(namespace)) {
      return Promise.resolve(
        Response.json({
          watermark: 100,
          changes: [],
          unpublishedTables: ['item'],
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
    if (pathname.endsWith('/changes') && heldChangeResponses.has(namespace)) {
      activeHeldChangeResponses.add(namespace)
      try {
        while (heldChangeResponses.has(namespace)) await scheduler.wait(10)
      } finally {
        activeHeldChangeResponses.delete(namespace)
      }
    }
    if (pathname.endsWith('/snapshot')) {
      const limit = Number(url.searchParams.get('limit'))
      if (Number.isSafeInteger(limit)) {
        const limits = snapshotLimits.get(namespace) ?? []
        limits.push(limit)
        snapshotLimits.set(namespace, limits)
      }
      const shouldHold =
        heldSnapshots.has(namespace) &&
        (!holdSnapshotsAfterCursor.has(namespace) || url.searchParams.has('cursor'))
      if (shouldHold) {
        activeSnapshots.add(namespace)
        try {
          while (
            heldSnapshots.has(namespace) &&
            (!holdSnapshotsAfterCursor.has(namespace) || url.searchParams.has('cursor'))
          ) {
            await scheduler.wait(10)
          }
        } finally {
          activeSnapshots.delete(namespace)
        }
      }
    }
    if (pathname.endsWith('/changes') && response.ok) hydratedNamespaces.add(namespace)
    return response
  }
}

export class AppService extends WorkerEntrypoint<Env> {
  async fetch(request: Request): Promise<Response> {
    delegatedUrl = request.url
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
    if (heldDelegatedPushes.has(namespace)) {
      activeDelegatedPushes.add(namespace)
      try {
        while (heldDelegatedPushes.has(namespace)) await scheduler.wait(10)
      } finally {
        activeDelegatedPushes.delete(namespace)
      }
    }
    const push = (await request.clone().json()) as {
      mutations?: Array<{ clientID?: string; id?: number; name?: string }>
    }
    const cleanupIDs = new Set(
      (push.mutations ?? [])
        .filter((mutation) => mutation.name === '_zero_cleanupResults')
        .map((mutation) => `${mutation.clientID}:${mutation.id}`)
    )
    const response = await upstreamFetch(request, this.env)
    completedDelegatedPushes.add(namespace)
    if (response.ok && cleanupIDs.size > 0) {
      const body = (await response.json()) as {
        mutations?: Array<{ id?: { clientID?: string; id?: number } }>
        pushResponse?: {
          mutations?: Array<{ id?: { clientID?: string; id?: number } }>
        }
      }
      const mutations = body.pushResponse?.mutations ?? body.mutations
      if (Array.isArray(mutations)) {
        const filtered = mutations.filter(
          (mutation) => !cleanupIDs.has(`${mutation.id?.clientID}:${mutation.id?.id}`)
        )
        if (body.pushResponse) body.pushResponse.mutations = filtered
        else body.mutations = filtered
      }
      return Response.json(body, { status: response.status })
    }
    return response
  }
}

async function runApplicationCommitProbe(request: Request, env: Env): Promise<Response> {
  const [, , namespace, action] = new URL(request.url).pathname.split('/')
  if (!namespace || !action) {
    return Response.json({ error: 'namespace and action are required' }, { status: 400 })
  }
  const body = (await request.json().catch(() => ({}))) as { id?: unknown }
  const id = typeof body.id === 'string' ? body.id : `${action}-item`
  const stub = env.UPSTREAM_DO.get(env.UPSTREAM_DO.idFromName(namespace))
  const exec = async (sql: string) => {
    const response = await stub.fetch('https://upstream.invalid/exec', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ sql }),
    })
    if (!response.ok)
      throw new Error(`application probe setup failed: ${await response.text()}`)
  }
  await exec(
    'CREATE TABLE IF NOT EXISTS item (id TEXT PRIMARY KEY, label TEXT NOT NULL, rank REAL NOT NULL, done INTEGER NOT NULL, meta TEXT)'
  )
  const client = createApplicationSqlClient(env.UPSTREAM_DO, namespace)
  commitNotificationAttempts.set(namespace, 0)
  commitNotificationFailures.delete(namespace)
  if (action === 'sync-failure') commitNotificationFailures.set(namespace, 'sync')
  if (action === 'async-failure') commitNotificationFailures.set(namespace, 'async')

  try {
    if (action === 'public' || action === 'sync-failure' || action === 'async-failure') {
      await client.exec(
        'INSERT INTO item (id, label, rank, done, meta) VALUES (?, ?, ?, ?, ?)',
        [id, action, 1, 0, null],
        { table: 'item', publicTable: 'item', kind: 'insert' }
      )
    } else if (action === 'private') {
      await exec(
        'CREATE TABLE IF NOT EXISTS private_note (id TEXT PRIMARY KEY, body TEXT)'
      )
      await client.registerTables([
        { table: 'private_note', publicTable: 'private_note', publish: false },
      ])
      await client.exec('INSERT INTO private_note (id, body) VALUES (?, ?)', [id, action])
    } else if (action === 'no-op') {
      await client.exec("UPDATE item SET label = 'missing' WHERE id = 'missing'", [], {
        table: 'item',
        publicTable: 'item',
        kind: 'update',
      })
    } else if (action === 'rollback') {
      try {
        await client.transaction(
          () => {
            throw new Error('application probe query compiler should not run')
          },
          async (tx) => {
            await tx.exec(
              'INSERT INTO item (id, label, rank, done, meta) VALUES (?, ?, ?, ?, ?)',
              [id, action, 1, 0, null],
              { table: 'item', publicTable: 'item', kind: 'insert' }
            )
            throw new Error('intentional application probe rollback')
          }
        )
      } catch (error) {
        if (!String(error).includes('intentional application probe rollback')) throw error
      }
    } else {
      return Response.json(
        { error: 'unknown application commit action' },
        { status: 404 }
      )
    }
    const rows = await client.query<{ id: string }>('SELECT id FROM item WHERE id = ?', [
      id,
    ])
    return Response.json({ ok: true, action, id, rows })
  } catch (error) {
    return Response.json({ ok: false, action, id, error: String(error) }, { status: 500 })
  }
}

export default {
  fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname.startsWith('/data-worker/')) {
      url.pathname = url.pathname.slice('/data-worker'.length)
      return dataWorker.fetch(new Request(url, request), env, ctx)
    }
    if (url.pathname.startsWith('/internal-notify/')) {
      const namespace = url.pathname.slice('/internal-notify/'.length)
      return syncWorker.notify(env, namespace)
    }
    if (url.pathname.startsWith('/upstream-change-requests/')) {
      const namespace = url.pathname.slice('/upstream-change-requests/'.length)
      if (request.method === 'POST') upstreamChangeRequests.set(namespace, 0)
      return Promise.resolve(
        Response.json({ requests: upstreamChangeRequests.get(namespace) ?? 0 })
      )
    }
    if (url.pathname.startsWith('/application-commit-status/')) {
      const namespace = url.pathname.slice('/application-commit-status/'.length)
      if (request.method === 'POST') commitNotificationAttempts.set(namespace, 0)
      return Promise.resolve(
        Response.json({
          attempts: commitNotificationAttempts.get(namespace) ?? 0,
          failure: commitNotificationFailures.get(namespace) ?? null,
        })
      )
    }
    if (url.pathname.startsWith('/application-commit/')) {
      return runApplicationCommitProbe(request, env)
    }
    if (url.pathname.startsWith('/application-total-changes/')) {
      const namespace = url.pathname.slice('/application-total-changes/'.length)
      return dataWorker
        .applicationSqlClient(env, namespace)
        .query<{ value: number }>('SELECT total_changes() AS value')
        .then((rows) => Response.json({ value: Number(rows[0]?.value ?? 0) }))
    }
    if (url.pathname.startsWith('/restore-observation/')) {
      const namespace = url.pathname.slice('/restore-observation/'.length)
      const client = dataWorker.applicationSqlClient(env, namespace)
      return Promise.all([
        client.query<{ name: string }>(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND (name IN ('item', 'itemReaction') OR name LIKE 'current_only_%') ORDER BY name"
        ),
        client.query<{ id: string; label: string }>(
          'SELECT id, label FROM item ORDER BY id'
        ),
      ]).then(([tables, rows]) => Response.json({ tables, rows }))
    }
    if (url.pathname.startsWith('/restore-fixture/')) {
      const namespace = url.pathname.slice('/restore-fixture/'.length)
      const instance = namespace.startsWith('ns:') ? namespace : `ns:${namespace}`
      const client = dataWorker.applicationSqlClient(env, instance)
      return client
        .transaction(
          () => {
            throw new Error('restore fixture does not compile query ASTs')
          },
          async (tx) => {
            await tx.exec('CREATE TABLE item (id TEXT PRIMARY KEY, label TEXT)')
            await tx.exec(
              'CREATE TABLE itemReaction (id TEXT PRIMARY KEY, itemId TEXT NOT NULL REFERENCES item(id))'
            )
            await tx.exec("INSERT INTO item VALUES ('current', 'before restore')")
            await tx.exec("INSERT INTO itemReaction VALUES ('reaction', 'current')")
            for (let index = 0; index < 41; index++) {
              await tx.exec(
                `CREATE TABLE "current_only_${String(index).padStart(3, '0')}" (id TEXT)`
              )
            }
          }
        )
        .then(() => {
          commitNotificationAttempts.set(instance, 0)
          const key = `restore-fixtures/${instance}.ndjson`
          const entries = [
            {
              kind: 'header',
              format: 'ingest-harness-backup-v2',
              integrity: 'sha256',
              ns: 'source',
              orderedTables: true,
            },
            {
              kind: 'table',
              name: 'item',
              sql: 'CREATE TABLE item (id TEXT PRIMARY KEY, label TEXT)',
              indexes: [],
            },
            {
              kind: 'rows',
              table: 'item',
              rows: [{ id: 'restored', label: 'after restore' }],
            },
          ]
          const payload = `${entries.map((entry) => JSON.stringify(entry)).join('\n')}\n`
          const digest = Array.from(sha256(new TextEncoder().encode(payload)), (byte) =>
            byte.toString(16).padStart(2, '0')
          ).join('')
          restoreObjects.set(
            key,
            `${payload}${JSON.stringify({ kind: 'footer', tables: 1, rows: 1, sha256: digest })}\n`
          )
          return Response.json({ instance, key })
        })
    }
    if (url.pathname.startsWith('/delegated-ingest-control/')) {
      const namespace = url.pathname.slice('/delegated-ingest-control/'.length)
      if (request.method === 'GET') {
        return Promise.resolve(
          Response.json({
            appHeld: heldDelegatedPushes.has(namespace),
            appActive: activeDelegatedPushes.has(namespace),
            appCompleted: completedDelegatedPushes.has(namespace),
            changesHeld: heldChangeResponses.has(namespace),
            changesActive: activeHeldChangeResponses.has(namespace),
          })
        )
      }
      return request
        .json()
        .catch(() => ({}))
        .then((body) => {
          if ((body as { appHeld?: unknown }).appHeld === true) {
            heldDelegatedPushes.add(namespace)
            completedDelegatedPushes.delete(namespace)
          } else if ((body as { appHeld?: unknown }).appHeld === false) {
            heldDelegatedPushes.delete(namespace)
          }
          if ((body as { changesHeld?: unknown }).changesHeld === true) {
            heldChangeResponses.add(namespace)
          } else if ((body as { changesHeld?: unknown }).changesHeld === false) {
            heldChangeResponses.delete(namespace)
          }
          return Response.json({ ok: true, namespace })
        })
    }
    if (url.pathname.startsWith('/snapshot-control/')) {
      const namespace = url.pathname.slice('/snapshot-control/'.length)
      if (request.method === 'GET') {
        return Promise.resolve(
          Response.json({
            active: activeSnapshots.has(namespace),
            held: heldSnapshots.has(namespace),
            afterCursor: holdSnapshotsAfterCursor.has(namespace),
            limits: snapshotLimits.get(namespace) ?? [],
          })
        )
      }
      return request
        .json()
        .catch(() => ({}))
        .then((body) => {
          if ((body as { hold?: unknown }).hold === true) {
            heldSnapshots.add(namespace)
            if ((body as { afterCursor?: unknown }).afterCursor === true) {
              holdSnapshotsAfterCursor.add(namespace)
            } else {
              holdSnapshotsAfterCursor.delete(namespace)
            }
          } else {
            heldSnapshots.delete(namespace)
            holdSnapshotsAfterCursor.delete(namespace)
          }
          if ((body as { reset?: unknown }).reset === true)
            snapshotLimits.set(namespace, [])
          return Response.json({
            active: activeSnapshots.has(namespace),
            held: heldSnapshots.has(namespace),
            afterCursor: holdSnapshotsAfterCursor.has(namespace),
            limits: snapshotLimits.get(namespace) ?? [],
          })
        })
    }
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
    if (url.pathname.startsWith('/unpublished-control/')) {
      const namespace = url.pathname.slice('/unpublished-control/'.length)
      return request
        .json()
        .catch(() => ({}))
        .then((body) => {
          if ((body as { enabled?: unknown }).enabled === true)
            unpublishedNamespaces.add(namespace)
          else unpublishedNamespaces.delete(namespace)
          return Response.json({
            ok: true,
            namespace,
            enabled: unpublishedNamespaces.has(namespace),
          })
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
            delegatedUrl,
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
          delegatedUrl = ''
          return Response.json({
            delegatedFailuresRemaining,
            delegatedPushFailedRemaining,
            delegatedAttempts,
            delegatedUrl,
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
