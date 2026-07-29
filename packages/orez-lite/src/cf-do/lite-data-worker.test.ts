import { beforeAll, describe, expect, it, vi } from 'vitest'

vi.mock('cloudflare:workers', () => ({
  DurableObject: class {
    protected ctx: unknown
    protected env: unknown
    constructor(ctx: unknown, env: unknown) {
      this.ctx = ctx
      this.env = env
    }
  },
  RpcTarget: class {},
}))

let canonicalOrezNamespace: typeof import('./lite-data-worker.js').canonicalOrezNamespace
let createOrezDataWorker: typeof import('./lite-data-worker.js').createOrezDataWorker
let projectOrezFeedBody: typeof import('./lite-data-worker.js').projectOrezFeedBody
let resolveOrezDataRequest: typeof import('./lite-data-worker.js').resolveOrezDataRequest

beforeAll(async () => {
  const factory = await import('./lite-data-worker.js')
  canonicalOrezNamespace = factory.canonicalOrezNamespace
  createOrezDataWorker = factory.createOrezDataWorker
  projectOrezFeedBody = factory.projectOrezFeedBody
  resolveOrezDataRequest = factory.resolveOrezDataRequest
})

const descriptor = {
  version: 'schema-v7',
  schema: {
    tables: {
      widget: {
        name: 'widget',
        serverName: 'widget_record',
        columns: {
          id: { type: 'string' as const, serverName: 'widget_id' },
          title: { type: 'string' as const, serverName: 'display_title' },
        },
        primaryKey: ['id'] as const,
      },
    },
    relationships: { widget: {} },
  },
  publicTables: [{ table: 'widget_record', publicTable: 'public.widget' }],
  migrate: vi.fn(async () => undefined),
}

describe('Orez Lite namespace routing', () => {
  it('canonicalizes aliases, raw tenants, and canonical instance names', () => {
    const options = { controlPlaneNamespaces: ['control'] }
    expect(canonicalOrezNamespace('', options)).toBe('singleton')
    expect(canonicalOrezNamespace('control', options)).toBe('singleton')
    expect(canonicalOrezNamespace('proj-a', options)).toBe('ns:proj-a')
    expect(canonicalOrezNamespace('ns:proj-a', options)).toBe('ns:proj-a')
    expect(canonicalOrezNamespace('proj-a/b', options)).toBeNull()
  })

  it('resolves Rust path mounts and root header mounts identically', () => {
    const mounted = resolveOrezDataRequest(
      new Request('https://data.test/proj-a/changes?watermark=4')
    )
    expect(mounted).toMatchObject({
      instance: 'ns:proj-a',
      pathname: '/changes',
    })
    expect(mounted?.url.pathname).toBe('/changes')
    expect(mounted?.url.searchParams.get('watermark')).toBe('4')

    const rooted = resolveOrezDataRequest(
      new Request('https://data.test/changes', {
        headers: { 'x-orez-ns': 'proj-a' },
      })
    )
    expect(rooted).toMatchObject({
      instance: 'ns:proj-a',
      pathname: '/changes',
    })
  })
})

describe('Orez Lite feed projection', () => {
  it('uses physical schema names as input and emits only public Zero names', () => {
    expect(
      projectOrezFeedBody(descriptor, {
        watermark: 9,
        tables: {
          'public.widget': [
            {
              widget_id: 'w1',
              display_title: 'Public',
              private_token: 'secret',
            },
          ],
          private_table: [{ secret: true }],
        },
        changes: [
          {
            watermark: 8,
            tableName: 'public.widget',
            op: 'INSERT',
            rowData: {
              widget_id: 'w2',
              display_title: 'Visible',
              private_token: 'secret',
            },
            oldData: null,
          },
          {
            watermark: 9,
            tableName: 'private_table',
            op: 'INSERT',
            rowData: { secret: true },
            oldData: null,
          },
        ],
      })
    ).toEqual({
      watermark: 9,
      tables: {
        widget: [{ id: 'w1', title: 'Public' }],
        syncCursor: [{ id: 'zero-http', watermark: 9 }],
      },
      changes: [
        {
          watermark: 8,
          tableName: 'widget',
          op: 'INSERT',
          rowData: { id: 'w2', title: 'Visible' },
          oldData: null,
        },
      ],
    })
  })

  it('projects known internal cursor sources without schema enumeration', () => {
    const result = projectOrezFeedBody(descriptor, {
      watermark: 12,
      changes: [
        {
          watermark: 11,
          tableName: '_zsync_clients',
          op: 'UPDATE',
          rowData: { private: 'ignored' },
        },
        {
          watermark: 12,
          tableName: 'app_0.mutations',
          op: 'DELETE',
          oldData: { private: 'ignored' },
        },
      ],
    })
    expect(result).toEqual({
      watermark: 12,
      changes: [
        {
          watermark: 11,
          tableName: 'syncCursor',
          op: 'INSERT',
          rowData: { id: 'zero-http', watermark: 11 },
          oldData: null,
        },
        {
          watermark: 12,
          tableName: 'syncCursor',
          op: 'INSERT',
          rowData: { id: 'zero-http', watermark: 12 },
          oldData: null,
        },
      ],
    })
  })

  it('projects paged snapshot rows using the requested Zero table', () => {
    expect(
      projectOrezFeedBody(
        descriptor,
        {
          watermark: 4,
          rows: [
            {
              widget_id: 'w1',
              display_title: 'Page',
              private_token: 'secret',
            },
          ],
          nextCursor: null,
        },
        'widget'
      )
    ).toEqual({
      watermark: 4,
      rows: [{ id: 'w1', title: 'Page' }],
      nextCursor: null,
    })
  })

  it('fills current optional columns omitted by historical full rows', () => {
    const evolvingDescriptor = {
      ...descriptor,
      schema: {
        ...descriptor.schema,
        tables: {
          widget: {
            ...descriptor.schema.tables.widget,
            columns: {
              ...descriptor.schema.tables.widget.columns,
              location: {
                type: 'string' as const,
                optional: true,
                serverName: 'widget_location',
              },
            },
          },
        },
      },
    }

    expect(
      projectOrezFeedBody(evolvingDescriptor, {
        watermark: 10,
        changes: [
          {
            watermark: 10,
            tableName: 'public.widget',
            op: 'UPDATE',
            rowData: {
              widget_id: 'w1',
              display_title: 'Historical',
            },
            oldData: null,
          },
        ],
      })
    ).toEqual({
      watermark: 10,
      changes: [
        {
          watermark: 10,
          tableName: 'widget',
          op: 'UPDATE',
          rowData: {
            id: 'w1',
            title: 'Historical',
            location: null,
          },
          oldData: null,
        },
      ],
    })
  })
})

describe('createOrezDataWorker', () => {
  it('validates durable control-table prefixes', () => {
    expect(() =>
      createOrezDataWorker({
        name: 'testapp',
        schema: descriptor,
        tablePrefix: '_soot',
      })
    ).not.toThrow()
    expect(() =>
      createOrezDataWorker({
        name: 'testapp',
        schema: descriptor,
        tablePrefix: '_Bad-Name' as `_${string}`,
      })
    ).toThrow(/tablePrefix/)
  })

  it('returns the concrete Cloudflare class and forwards standard feed routes', async () => {
    const status = vi.fn(async () => ({
      schemaVersion: 'schema-v7',
      ready: true,
      running: false,
      attemptCount: 1,
      lastError: null,
    }))
    const fetch = vi.fn(async () =>
      Response.json({
        watermark: 1,
        changes: [
          {
            watermark: 1,
            tableName: 'widget',
            op: 'INSERT',
            rowData: { widget_id: 'w1', display_title: 'hello', secret: 'no' },
            oldData: null,
          },
        ],
      })
    )
    const stub = {
      fetch,
      orezApplicationSchemaStatus: status,
      orezRunApplicationSchema: vi.fn(),
      orezStartApplicationSchema: vi.fn(),
      orezImportBatch: vi.fn(),
    }
    const env = {
      ZERO_SQL_DO: {
        idFromName: vi.fn((name: string) => ({ toString: () => `id:${name}` })),
        get: vi.fn(() => stub),
      },
    }
    const runtime = createOrezDataWorker({
      name: 'testapp',
      schema: descriptor,
    })
    expect(runtime.ZeroSqlDO).toBe(runtime.ZeroDO)

    const response = await runtime.fetch(
      new Request('https://data.test/proj-a/changes?watermark=0'),
      env,
      { waitUntil: vi.fn() }
    )
    expect(response.status).toBe(200)
    expect(await response.json()).toEqual({
      watermark: 1,
      changes: [
        {
          watermark: 1,
          tableName: 'widget',
          op: 'INSERT',
          rowData: { id: 'w1', title: 'hello' },
          oldData: null,
        },
      ],
    })
    expect(env.ZERO_SQL_DO.idFromName).toHaveBeenCalledWith('ns:proj-a')
    expect(status).toHaveBeenCalledWith('schema-v7')
    expect(fetch.mock.calls[0]?.[0].headers.get('x-orez-do-instance')).toBe('ns:proj-a')
  })

  it('serves the synthetic syncCursor snapshot page from the change-log head', async () => {
    const fetch = vi.fn(async () => Response.json({ watermark: 17, changes: [] }))
    const stub = {
      fetch,
      orezApplicationSchemaStatus: vi.fn(async () => ({
        schemaVersion: 'schema-v7',
        ready: true,
        running: false,
        attemptCount: 1,
        lastError: null,
      })),
      orezRunApplicationSchema: vi.fn(),
      orezStartApplicationSchema: vi.fn(),
      orezImportBatch: vi.fn(),
    }
    const runtime = createOrezDataWorker({
      name: 'testapp',
      schema: descriptor,
    })
    const response = await runtime.fetch(
      new Request('https://data.test/proj-a/snapshot?table=syncCursor&limit=2000'),
      {
        ZERO_SQL_DO: {
          idFromName: (name: string) => ({ toString: () => name }),
          get: () => stub,
        },
      },
      { waitUntil: vi.fn() }
    )

    expect(await response.json()).toEqual({
      watermark: 17,
      rows: [{ id: 'zero-http', watermark: 17 }],
      nextCursor: null,
    })
    expect(new URL(fetch.mock.calls[0]?.[0].url).pathname).toBe('/changes')
  })

  it('reloads persisted table metadata after a live schema migration', async () => {
    const oldTable = {
      columns: { id: { type: 'string' as const } },
      primaryKey: ['id'] as const,
    }
    const newTable = {
      columns: {
        id: { type: 'string' as const },
        location: { type: 'string' as const },
      },
      primaryKey: ['id'] as const,
    }
    let persistedTable = oldTable
    const runtime = createOrezDataWorker({
      name: 'testapp',
      schema: {
        ...descriptor,
        version: 'schema-v8',
        migrate: async () => {
          persistedTable = newTable
        },
      },
    })
    const zero = Object.create(runtime.ZeroDO.prototype) as any
    zero.sql = {
      exec(sql: string) {
        if (sql.startsWith('CREATE TABLE IF NOT EXISTS _zero_schema_tables')) {
          return { one: () => undefined, toArray: () => [] }
        }
        if (sql.startsWith('SELECT schema_json FROM _zero_schema_tables')) {
          return {
            one: () => ({ schema_json: JSON.stringify(persistedTable) }),
            toArray: () => [{ schema_json: JSON.stringify(persistedTable) }],
          }
        }
        throw new Error(`unexpected application SQL: ${sql}`)
      },
    }
    zero.tableSchemas = new Map([['widget', oldTable]])
    zero.watermarks = { invalidateCache: () => {} }
    zero.cdc = { reload: () => {} }
    zero.orezStorage = {
      sql: {
        exec: () => ({ toArray: () => [] }),
      },
    }
    zero.applicationSqlLocalClient = () => ({})
    zero.orezRestoreInProgress = () => false
    zero.orezApplicationSchemaReady = () => false
    zero.orezBeginApplicationSchemaReconcile = () => {}
    zero.orezMarkApplicationSchemaReady = () => {}
    zero.orezSchemaRunVersion = null
    zero.orezSchemaRun = null

    expect(zero.schemaForTable('widget')).toEqual(oldTable)
    await zero.orezRunApplicationSchema('schema-v8', 'singleton', { force: true })
    expect(zero.schemaForTable('widget')).toEqual(newTable)
  })

  it('runs a forced reconcile again after the prior schema run completed', async () => {
    let migrationRuns = 0
    const runtime = createOrezDataWorker({
      name: 'testapp',
      schema: {
        ...descriptor,
        version: 'schema-v8',
        migrate: async () => {
          migrationRuns++
        },
      },
    })
    const zero = Object.create(runtime.ZeroDO.prototype) as any
    zero.orezStorage = {
      sql: {
        exec: () => ({ toArray: () => [] }),
      },
    }
    zero.applicationSqlLocalClient = () => ({})
    zero.orezRestoreInProgress = () => false
    zero.orezApplicationSchemaReady = () => false
    zero.orezBeginApplicationSchemaReconcile = () => {}
    zero.orezMarkApplicationSchemaReady = () => {}
    zero.invalidateSchemaCaches = () => {}
    zero.orezSchemaRunVersion = null
    zero.orezSchemaRun = null

    await zero.orezRunApplicationSchema('schema-v8', 'singleton', { force: true })
    await zero.orezRunApplicationSchema('schema-v8', 'singleton', { force: true })

    expect(migrationRuns).toBe(2)
  })

  it('reloads persisted table metadata after a migration that fails partway', async () => {
    const oldTable = {
      columns: { id: { type: 'string' as const } },
      primaryKey: ['id'] as const,
    }
    const newTable = {
      columns: {
        id: { type: 'string' as const },
        location: { type: 'string' as const },
      },
      primaryKey: ['id'] as const,
    }
    let persistedTable = oldTable
    const runtime = createOrezDataWorker({
      name: 'testapp',
      schema: {
        ...descriptor,
        version: 'schema-v9',
        migrate: async () => {
          // earlier statements committed through their own sessions before the
          // failing one, exactly like a partial native SQL migration
          persistedTable = newTable
          throw new Error('statement 8 violates a constraint')
        },
      },
    })
    const zero = Object.create(runtime.ZeroDO.prototype) as any
    zero.sql = {
      exec(sql: string) {
        if (sql.startsWith('CREATE TABLE IF NOT EXISTS _zero_schema_tables')) {
          return { one: () => undefined, toArray: () => [] }
        }
        if (sql.startsWith('SELECT schema_json FROM _zero_schema_tables')) {
          return {
            one: () => ({ schema_json: JSON.stringify(persistedTable) }),
            toArray: () => [{ schema_json: JSON.stringify(persistedTable) }],
          }
        }
        throw new Error(`unexpected application SQL: ${sql}`)
      },
    }
    zero.tableSchemas = new Map([['widget', oldTable]])
    zero.watermarks = { invalidateCache: () => {} }
    zero.cdc = { reload: () => {} }
    zero.orezStorage = {
      sql: {
        exec: () => ({ toArray: () => [] }),
      },
    }
    zero.applicationSqlLocalClient = () => ({})
    zero.orezRestoreInProgress = () => false
    zero.orezApplicationSchemaReady = () => false
    zero.orezBeginApplicationSchemaReconcile = () => {}
    zero.orezMarkApplicationSchemaReady = () => {}
    zero.orezSchemaRunVersion = null
    zero.orezSchemaRun = null

    await expect(
      zero.orezRunApplicationSchema('schema-v9', 'singleton', { force: true })
    ).rejects.toThrow('statement 8')
    expect(zero.schemaForTable('widget')).toEqual(newTable)
  })

  it('keeps the durable object schema version authoritative across rolling deploys', async () => {
    const migrate = vi.fn(async () => undefined)
    const runtime = createOrezDataWorker({
      name: 'testapp',
      schema: {
        ...descriptor,
        version: 'schema-owned-by-do',
        migrate,
      },
    })
    let readyVersion: string | null = null
    let attempt:
      | { version: string; attempt_count: number; last_error: string | null }
      | undefined
    const zero = Object.create(runtime.ZeroDO.prototype) as any
    zero.orezStorage = {
      sql: {
        exec(sql: string, ...params: unknown[]) {
          if (sql.startsWith('SELECT version FROM _orez_application_schema ')) {
            return {
              toArray: () => (readyVersion ? [{ version: readyVersion }] : []),
            }
          }
          if (sql.startsWith('SELECT attempt_count, last_error ')) {
            return {
              toArray: () => (attempt && attempt.version === params[0] ? [attempt] : []),
            }
          }
          if (sql.startsWith('INSERT INTO _orez_application_schema_attempt ')) {
            const version = String(params[0])
            attempt = {
              version,
              attempt_count: attempt?.version === version ? attempt.attempt_count + 1 : 1,
              last_error: attempt?.version === version ? attempt.last_error : null,
            }
            return { toArray: () => [] }
          }
          if (sql.startsWith('UPDATE _orez_application_schema_attempt ')) {
            if (attempt?.version === params.at(-1)) {
              attempt.last_error = sql.includes('SET last_error = NULL')
                ? null
                : String(params[0])
            }
            return { toArray: () => [] }
          }
          if (sql.startsWith('DELETE FROM _orez_application_schema ')) {
            readyVersion = null
            return { toArray: () => [] }
          }
          if (sql.startsWith('INSERT INTO _orez_application_schema ')) {
            readyVersion = String(params[0])
            return { toArray: () => [] }
          }
          throw new Error(`unexpected durable SQL: ${sql}`)
        },
      },
    }
    zero.applicationSqlLocalClient = () => ({})
    zero.orezRestoreInProgress = () => false
    zero.invalidateSchemaCaches = () => {}
    zero.orezSchemaRunVersion = null
    zero.orezSchemaRun = null
    zero.orezReadyVersion = null
    zero.orezWorkerVersion = 'worker-f54'

    expect(zero.orezApplicationSchemaStatus('schema-from-new-caller')).toMatchObject({
      schemaVersion: 'schema-owned-by-do',
      ready: false,
      running: false,
      lastError: expect.stringMatching(/schema version mismatch/),
    })
    expect(
      zero.orezStartApplicationSchema('schema-from-new-caller', 'singleton')
    ).toMatchObject({
      schemaVersion: 'schema-owned-by-do',
      ready: false,
      running: false,
    })
    await expect(
      zero.orezRunApplicationSchema('schema-from-new-caller', 'singleton', {
        force: true,
      })
    ).rejects.toThrow(/schema version mismatch/)
    expect(migrate).not.toHaveBeenCalled()
    expect(readyVersion).toBeNull()

    await zero.orezRunApplicationSchema('schema-owned-by-do', 'singleton', {
      force: true,
    })
    expect(migrate).toHaveBeenCalledOnce()
    expect(zero.orezApplicationSchemaStatus('schema-owned-by-do')).toMatchObject({
      schemaVersion: 'schema-owned-by-do',
      ready: true,
      running: false,
    })
    expect(readyVersion).toBe('schema-owned-by-do')
  })
})
