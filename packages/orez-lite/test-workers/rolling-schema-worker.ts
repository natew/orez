// one data worker and one sync host, each built at the schema version wrangler
// --define selects. rolling-schema-test.mjs restarts this worker over the same
// persisted state with different versions to replay a real deploy order.
import { createBuilder, defineQueries, defineQuery } from '@rocicorp/zero'
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

type SchemaVersion = 'v1' | 'v2' | 'v3'

// v2 adds an optional column, the additive change that must roll in either
// order. v3 adds a required column instead, which must fail loudly.
const columnsByVersion = {
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
  v2: [['note', 'note TEXT']],
  v3: [
    ['note', 'note TEXT'],
    ['weight', 'weight INTEGER NOT NULL DEFAULT 0'],
  ],
}

function schemaFor(version: SchemaVersion) {
  return {
    tables: {
      item: { name: 'item', columns: columnsByVersion[version], primaryKey: ['id'] },
    },
    relationships: {},
  } as const satisfies Schema
}

const hostSchema = schemaFor(HOST_SCHEMA)
const dataSchema = schemaFor(DATA_SCHEMA)
const zql = createBuilder(hostSchema as never) as { item: unknown }
const queries = defineQueries({ allItems: defineQuery(() => zql.item as never) })

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
    const present = new Set(
      sql.query<{ name: string }>('PRAGMA table_info(item)').map((row) => row.name)
    )
    for (const [column, ddl] of ddlByVersion[HOST_SCHEMA]) {
      if (!present.has(column)) sql.exec(`ALTER TABLE item ADD COLUMN ${ddl}`)
    }
  },
  authenticate: () => ({ userID: 'reader' }),
  authorize: () => true,
  authorizeWake: () => false,
  authorizeNotify: () => true,
  namespace(request) {
    return new URL(request.url).pathname.split('/')[1] || null
  },
}

const syncWorker = createSyncWorker(hostConfig)
const publicTables = [{ table: 'item', publicTable: 'public.item' }] as const

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
          await tx.registerTables(publicTables)
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
  fetch(request: Request): Promise<Response> {
    return dataWorker.fetch(request, this.env, this.ctx)
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
