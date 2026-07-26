import { AsyncLocalStorage } from 'node:async_hooks'

import type {
  ApplicationSqlClient,
  ApplicationSqlClientOptions,
  ApplicationSqlDurableObjectNamespace,
} from '../cf-do/application-sql.js'

export type OrezLiteFeedTables = Readonly<Record<string, readonly string[]>>

export type OrezLiteMigrationRunner = (options?: {
  instance?: string
  schemaOnly?: boolean
  registrationOnly?: boolean
}) => Promise<unknown>

export type OrezLiteApplicationSqlClientFactory = (
  durableObjects: ApplicationSqlDurableObjectNamespace,
  namespace: string,
  options?: ApplicationSqlClientOptions
) => ApplicationSqlClient

type SqlDurableObjectStub = {
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>
}

type SqlDurableObjectNamespace = Omit<ApplicationSqlDurableObjectNamespace, 'get'> & {
  get(
    id: unknown
  ): ReturnType<ApplicationSqlDurableObjectNamespace['get']> & SqlDurableObjectStub
}

export type OrezLiteWorkerEnv = Record<string, unknown> & {
  ZERO_SQL_DO: SqlDurableObjectNamespace
  AUTH_DB?: unknown
  FILES?: unknown
}

export type OrezLiteExecutionContext = {
  waitUntil(task: Promise<unknown>): void
}

export type OrezLiteApplicationWorker<Env extends OrezLiteWorkerEnv> = {
  fetch(
    request: Request,
    env: Env,
    ctx: OrezLiteExecutionContext
  ): Response | Promise<Response | undefined> | undefined
}

export type OrezLiteWorkerRuntimeOptions<Env extends OrezLiteWorkerEnv> = {
  prefix: string
  feedTables: OrezLiteFeedTables
  createApplicationSqlClient: OrezLiteApplicationSqlClientFactory
  runMigrations: OrezLiteMigrationRunner
  loadApp: () => Promise<OrezLiteApplicationWorker<Env>>
}

export type OrezLiteWorkerRuntime<Env extends OrezLiteWorkerEnv> = {
  dataFeed(request: Request, env: Env): Promise<Response>
  fetch(request: Request, env: Env, ctx: OrezLiteExecutionContext): Promise<Response>
}

type RuntimeGlobal = typeof globalThis & Record<string, unknown>
type JsonRecord = Record<string, unknown>

function isRecord(value: unknown): value is JsonRecord {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function runtimeGlobal(): RuntimeGlobal {
  return globalThis as RuntimeGlobal
}

function stringEnv(env: Record<string, unknown>): Record<string, string> {
  return Object.fromEntries(
    Object.entries(env).filter((entry): entry is [string, string] => {
      return typeof entry[1] === 'string'
    })
  )
}

function publicRow(
  feedTables: OrezLiteFeedTables,
  tableName: string,
  row: unknown
): unknown {
  if (!isRecord(row)) return row
  const columns = feedTables[tableName]
  if (!columns) return row
  return Object.fromEntries(
    columns.filter((column) => column in row).map((column) => [column, row[column]])
  )
}

/**
 * Strip private columns from the authoritative SQLite feed before the Rust host
 * sees it. Orez Lite owns `_zsync_clients`; it advances mutation watermarks but
 * is represented in replicas by the private `syncCursor` row.
 */
export function normalizeOrezLiteFeedBody(
  feedTables: OrezLiteFeedTables,
  value: unknown
): unknown {
  if (!isRecord(value)) return value

  if (isRecord(value.tables)) {
    value.tables = Object.fromEntries(
      Object.entries(value.tables).map(([rawTableName, rows]) => {
        const tableName = rawTableName.replace(/^public\./, '')
        return [
          tableName,
          Array.isArray(rows)
            ? rows.map((row) => publicRow(feedTables, tableName, row))
            : rows,
        ]
      })
    )
  }

  if (Array.isArray(value.changes)) {
    value.changes = value.changes.map((rawChange) => {
      if (!isRecord(rawChange)) return rawChange
      const tableName = String(rawChange.tableName).replace(/^public\./, '')
      if (tableName === '_zsync_clients') {
        return {
          watermark: rawChange.watermark,
          tableName: 'syncCursor',
          op: 'INSERT',
          rowData: { id: 'zero-http', watermark: rawChange.watermark },
          oldData: null,
        }
      }
      return {
        ...rawChange,
        tableName,
        rowData: publicRow(feedTables, tableName, rawChange.rowData),
        oldData: publicRow(feedTables, tableName, rawChange.oldData),
      }
    })
  }

  return value
}

function needsSqlSchema(request: Request, pathname: string): boolean {
  if (pathname.startsWith('/api/zero/')) return true
  if (pathname.startsWith('/api/bootstrap-')) return true
  return pathname.startsWith('/api/auth/') && request.method !== 'GET'
}

/**
 * Runtime shared by a Cloudflare app worker, its authoritative SQLite Durable
 * Object, and the private feed consumed by `orez-sync-cf-host`.
 *
 * The deploy builder generates only the Cloudflare-required static imports and
 * named class exports. Request behavior stays here as normal, typechecked code.
 */
export function createOrezLiteWorkerRuntime<Env extends OrezLiteWorkerEnv>(
  options: OrezLiteWorkerRuntimeOptions<Env>
): OrezLiteWorkerRuntime<Env> {
  const requestSignals = new AsyncLocalStorage<AbortSignal>()
  const globalNames = {
    applicationSqlClient: `__${options.prefix}_cf_application_sql_client`,
    backgroundTask: `__${options.prefix}_background_task`,
    r2Bucket: `__${options.prefix}_cf_r2_bucket`,
  }
  let appSchemaReady: Promise<unknown> | undefined

  const installApplicationSqlClient = (env: Env): void => {
    if (!env.ZERO_SQL_DO) {
      throw new Error('Cloudflare SQL Durable Object binding is not initialized')
    }
    const globals = runtimeGlobal()
    globals[globalNames.applicationSqlClient] = (namespace = 'singleton') =>
      options.createApplicationSqlClient(env.ZERO_SQL_DO, String(namespace), {
        signal: requestSignals.getStore(),
      })
    if (env.FILES) globals[globalNames.r2Bucket] = env.FILES
  }

  const ensureAppSchema = (env: Env): Promise<unknown> => {
    if (appSchemaReady) return appSchemaReady
    installApplicationSqlClient(env)
    appSchemaReady = options
      .runMigrations({ instance: 'singleton', schemaOnly: true })
      .catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error)
        if (!message.includes('application SQLite schema mismatch')) {
          appSchemaReady = undefined
        }
        throw error
      })
    return appSchemaReady
  }

  const withAppProcessEnv = async <Value>(
    env: Env,
    run: () => Value | Promise<Value>
  ): Promise<Value> => {
    const processEnv = process.env
    const previous = new Map<string, string | undefined>()
    for (const key of ['ZERO_UPSTREAM_DB', 'ZERO_CVR_DB', 'ZERO_CHANGE_DB']) {
      previous.set(key, processEnv[key])
      delete processEnv[key]
    }
    for (const [key, value] of Object.entries(stringEnv(env))) {
      if (previous.has(key)) continue
      previous.set(key, processEnv[key])
      processEnv[key] = value
    }
    try {
      return await run()
    } finally {
      for (const [key, value] of previous) {
        if (value === undefined) delete processEnv[key]
        else processEnv[key] = value
      }
    }
  }

  const dataFeed = async (request: Request, env: Env): Promise<Response> => {
    return requestSignals.run(request.signal, async () => {
      installApplicationSqlClient(env)
      const url = new URL(request.url)
      const allowed =
        url.pathname === '/snapshot' ||
        url.pathname === '/changes' ||
        url.pathname === '/_orez/write-budget' ||
        url.pathname === '/_orez/write-budget/reopen'
      if (!allowed) return new Response('not found', { status: 404 })
      if (url.pathname === '/snapshot' || url.pathname === '/changes') {
        await ensureAppSchema(env)
      }

      const forward = new Request(url.toString(), request)
      forward.headers.set(`x-${options.prefix}-do-instance`, 'singleton')
      const id = env.ZERO_SQL_DO.idFromName('singleton')
      const response = await env.ZERO_SQL_DO.get(id).fetch(forward)
      if (!response.ok || url.pathname.startsWith('/_orez/')) return response

      const body = normalizeOrezLiteFeedBody(options.feedTables, await response.json())
      const headers = new Headers(response.headers)
      headers.delete('content-length')
      headers.set('content-type', 'application/json')
      return new Response(JSON.stringify(body), {
        status: response.status,
        headers,
      })
    })
  }

  const fetch = async (
    request: Request,
    env: Env,
    ctx: OrezLiteExecutionContext
  ): Promise<Response> => {
    return requestSignals.run(request.signal, async () => {
      installApplicationSqlClient(env)
      if (env.AUTH_DB) runtimeGlobal().AUTH_DB = env.AUTH_DB
      runtimeGlobal()[globalNames.backgroundTask] = (task: unknown) => {
        try {
          ctx.waitUntil(Promise.resolve(task))
        } catch {}
      }

      const url = new URL(request.url)
      if (needsSqlSchema(request, url.pathname)) {
        try {
          await ensureAppSchema(env)
        } catch (error) {
          console.error(`[${options.prefix}] application schema migration failed`, error)
          return new Response('application schema migration failed', { status: 503 })
        }
      }

      return withAppProcessEnv(env, async () => {
        const app = await options.loadApp()
        const response = await app.fetch(request, env, ctx)
        return response instanceof Response
          ? response
          : new Response('not found', { status: 404 })
      })
    })
  }

  return { dataFeed, fetch }
}
