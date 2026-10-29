// one data worker and one sync host, each built at the schema version wrangler
// --define selects. rolling-schema-test.mjs restarts this worker over the same
// persisted state with different versions to replay a real deploy order.
import {
  createBuilder,
  defineQueries,
  defineQuery,
  defineQueryWithType,
} from '@rocicorp/zero'
import { WorkerEntrypoint } from 'cloudflare:workers'

import {
  createSyncDurableObject,
  createSyncWorker,
} from '../../sync-cf-host/src/index.js'
import { createOrezDataWorker } from '../src/cf-do/lite-data-worker.js'

import type { SyncHostConfig, SyncHostEnv } from '../../sync-cf-host/src/index.js'
import type { OrezDataWorkerEnv } from '../src/cf-do/lite-data-worker.js'
import type { Schema } from '@rocicorp/zero'

declare const HOST_SCHEMA: SchemaVersion
declare const DATA_SCHEMA: SchemaVersion

type SchemaVersion = 'v1' | 'v2' | 'v3' | 'v4'

// v2 adds an optional column, the additive change that must roll in either
// order. v3 adds a required column instead, which must fail loudly.
const columnsByVersion = {
  v4: { id: { type: 'string' }, label: { type: 'string' } },
  v1: {
    id: { type: 'string' },
    label: { type: 'string' },
  },
  v2: {
    id: { type: 'string' },
    label: { type: 'string' },
    note: { type: 'string', optional: true },
  },
  v3: {
    id: { type: 'string' },
    label: { type: 'string' },
    note: { type: 'string', optional: true },
    weight: { type: 'number' },
  },
} as const

const ddlByVersion: Record<SchemaVersion, [column: string, ddl: string][]> = {
  v1: [],
  v4: [],
  v2: [['note', 'note TEXT']],
  v3: [
    ['note', 'note TEXT'],
    ['weight', 'weight INTEGER NOT NULL DEFAULT 0'],
  ],
}

const counterTable = {
  name: 'counter',
  columns: {
    id: { type: 'string' },
    value: { type: 'number' },
    owner: { type: 'string' },
  },
  primaryKey: ['id'],
} as const
const counterDDL =
  'CREATE TABLE IF NOT EXISTS counter (id TEXT PRIMARY KEY, value REAL NOT NULL, owner TEXT NOT NULL)'

function schemaFor(version: SchemaVersion) {
  return {
    tables: {
      item: { name: 'item', columns: columnsByVersion[version], primaryKey: ['id'] },
      ...(version === 'v4' ? { counter: counterTable } : {}),
    },
    relationships: {},
  } as const satisfies Schema
}

const hostSchema = schemaFor(HOST_SCHEMA)
const dataSchema = schemaFor(DATA_SCHEMA)
const zql = createBuilder(hostSchema as never) as { item: unknown }
const counterBuilder = createBuilder({
  tables: { counter: counterTable },
  relationships: {},
})
const scopedQuery = defineQueryWithType<{ userID: string }>()
const queries = defineQueries({
  allItems: defineQuery(() => zql.item as never),
  ownCounters: scopedQuery(({ ctx }) =>
    counterBuilder.counter.where('owner', ctx.userID)
  ),
})

type Fetcher = { fetch(input: string | Request, init?: RequestInit): Promise<Response> }
interface Env extends SyncHostEnv, OrezDataWorkerEnv {
  DATA: Fetcher
}

const hostConfig: SyncHostConfig<Env> = {
  hostVersion: `rolling-schema-${HOST_SCHEMA}`,
  schema: hostSchema,
  queries: queries as never,
  // the harness writes through the data worker; clients push nothing
  mutateUrl: '/push',
  mutateBinding: 'DATA',
  upstream: {
    binding: 'DATA',
    namespacePath: (namespace) => `/${namespace}`,
    intervalMs: 60_000,
  },
  initialize(sql) {
    sql.exec('CREATE TABLE IF NOT EXISTS item (id TEXT PRIMARY KEY, label TEXT NOT NULL)')
    if (HOST_SCHEMA === 'v4') sql.exec(counterDDL)
    const present = new Set(
      sql.query<{ name: string }>('PRAGMA table_info(item)').map((row) => row.name)
    )
    for (const [column, ddl] of ddlByVersion[HOST_SCHEMA]) {
      if (!present.has(column)) sql.exec(`ALTER TABLE item ADD COLUMN ${ddl}`)
    }
  },
  authenticate: (request) => ({
    userID: request.headers.get('authorization')?.replace(/^Bearer /, '') ?? 'reader',
  }),
  authorize: () => true,
  authorizeWake: () => false,
  authorizeNotify: () => true,
  namespace(request) {
    return new URL(request.url).pathname.split('/')[1] || null
  },
}

const syncWorker = createSyncWorker(hostConfig)
const publicTables = [
  { table: 'item', publicTable: 'public.item' },
  ...(DATA_SCHEMA === 'v4' ? [{ table: 'counter', publicTable: 'public.counter' }] : []),
]
const trackedTables = [
  { table: 'item', publicTable: 'public.item' },
  { table: 'counter', publicTable: 'public.counter' },
]

const dataWorker = createOrezDataWorker<Env>({
  name: 'rolling',
  schema: {
    version: `rolling-schema-${DATA_SCHEMA}`,
    schema: dataSchema,
    publicTables,
    async migrate(options) {
      const client = options?.client
      if (!client) throw new Error('rolling schema migration needs its namespace client')
      await client.transaction(
        () => {
          throw new Error('rolling schema migration compiles no queries')
        },
        async (tx) => {
          await tx.exec(
            'CREATE TABLE IF NOT EXISTS item (id TEXT PRIMARY KEY, label TEXT NOT NULL)'
          )
          await tx.exec(counterDDL)
          const present = new Set(
            (await tx.query<{ name: string }>('PRAGMA table_info(item)')).map(
              (row) => row.name
            )
          )
          for (const [column, ddl] of ddlByVersion[DATA_SCHEMA]) {
            if (!present.has(column)) await tx.exec(`ALTER TABLE item ADD COLUMN ${ddl}`)
          }
          await tx.exec(
            'CREATE TABLE IF NOT EXISTS _zero_schema_tables (name TEXT PRIMARY KEY, schema_json TEXT NOT NULL)'
          )
          await tx.exec(
            'INSERT OR REPLACE INTO _zero_schema_tables (name, schema_json) VALUES (?, ?)',
            ['item', JSON.stringify({ ...dataSchema.tables.item, physicalName: 'item' })]
          )
          await tx.exec(
            'INSERT OR REPLACE INTO _zero_schema_tables (name, schema_json) VALUES (?, ?)',
            ['counter', JSON.stringify({ ...counterTable, physicalName: 'counter' })]
          )
          await tx.registerTables(trackedTables)
        }
      )
    },
  },
  applicationSqlDidCommit({ env, instance }) {
    return syncWorker
      .notify(env, instance.slice('ns:'.length))
      .then((response) => response.body?.cancel())
  },
})

export const SyncDurableObject = createSyncDurableObject(hostConfig)
export const ZeroDO = dataWorker.ZeroDO

/** the sync host's upstream binding, served by the real data worker */
export class DataService extends WorkerEntrypoint<Env> {
  async fetch(request: Request): Promise<Response> {
    const response = await dataWorker.fetch(request, this.env, this.ctx)
    const url = new URL(request.url)
    if (
      url.pathname === '/proj-racing/snapshot' &&
      url.searchParams.get('table') === 'counter'
    ) {
      // mutate only after capturing the initial page, before the host receives
      // it. the restored-table catch-up must consume these concurrent writes.
      const body = await response.text()
      await dataWorker.applicationSqlClient(this.env, 'ns:proj-racing').transaction(
        () => {
          throw new Error('race writes compile no queries')
        },
        async (tx) => {
          const rows = await tx.query<{ value: number }>(
            "SELECT value FROM counter WHERE id = 'a'"
          )
          if (rows[0]?.value === 42) {
            await tx.exec("UPDATE counter SET value = 43 WHERE id = 'a'")
            await tx.exec("UPDATE counter SET id = 'raced' WHERE id = 'a'")
            await tx.exec("INSERT INTO counter VALUES ('temporary', 1, 'reader')")
            await tx.exec("DELETE FROM counter WHERE id = 'temporary'")
          }
        }
      )
      return new Response(body, response)
    }
    return response
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url)
    if (url.pathname === '/versions') {
      return Response.json({ host: HOST_SCHEMA, data: DATA_SCHEMA })
    }
    // POST /write/<namespace> { sql, params } runs one application write
    const write = /^\/write\/([^/]+)$/.exec(url.pathname)
    if (write && request.method === 'POST') {
      const { sql, params } = (await request.json()) as { sql: string; params: unknown[] }
      try {
        await dataWorker.applicationSqlClient(env, `ns:${write[1]}`).exec(sql, params)
        return Response.json({ ok: true })
      } catch (error) {
        return Response.json(
          { error: error instanceof Error ? error.message : String(error) },
          { status: 500 }
        )
      }
    }
    return syncWorker.fetch(request, env, ctx)
  },
}
