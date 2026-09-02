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
        {
          watermark: 9,
          tableName: 'syncCursor',
          op: 'INSERT',
          rowData: { id: 'zero-http', watermark: 9 },
          oldData: null,
        },
      ],
    })
  })

  it('skips rollback-only registrations and still rejects published tables missing from the schema', () => {
    const rollbackOnly = {
      ...descriptor,
      publicTables: [
        ...descriptor.publicTables,
        { table: 'user', publicTable: 'public.user', publish: false },
      ],
    }
    expect(
      projectOrezFeedBody(rollbackOnly, {
        watermark: 3,
        tables: { 'public.widget': [{ widget_id: 'w1', display_title: 'Kept' }] },
        changes: [],
      })
    ).toEqual({
      watermark: 3,
      tables: {
        widget: [{ id: 'w1', title: 'Kept' }],
        syncCursor: [{ id: 'zero-http', watermark: 3 }],
      },
      changes: [],
    })

    const publishedButAbsent = {
      ...descriptor,
      publicTables: [
        ...descriptor.publicTables,
        { table: 'user', publicTable: 'public.user' },
      ],
    }
    expect(() =>
      projectOrezFeedBody(publishedButAbsent, { watermark: 3, changes: [] })
    ).toThrow('absent from the Zero schema')
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

  it('routes one application push call to the owning durable object', async () => {
    const responseHeaders: [string, string][] = [['content-type', 'application/json']]
    const orezApplicationPush = vi.fn(async (input: unknown) => ({
      body: new TextEncoder().encode(JSON.stringify({ input })).buffer,
      headers: responseHeaders,
      status: 202,
      statusText: 'Accepted',
    }))
    const idFromName = vi.fn((name: string) => name)
    const runtime = createOrezDataWorker({
      name: 'testapp',
      schema: descriptor,
      routes: async (context) => {
        if (context.url.pathname !== '/proj-a/api/push') return null
        return context.executeApplicationPush({ mutation: 'widget.insert' }, 'proj-a')
      },
    })

    const response = await runtime.fetch(
      new Request('https://data.test/proj-a/api/push', {
        method: 'POST',
      }),
      {
        ZERO_SQL_DO: {
          idFromName,
          get: () => ({ orezApplicationPush }),
        },
      },
      { waitUntil: vi.fn() }
    )

    expect(idFromName).toHaveBeenCalledWith('ns:proj-a')
    expect(orezApplicationPush).toHaveBeenCalledOnce()
    expect(orezApplicationPush).toHaveBeenCalledWith({
      mutation: 'widget.insert',
    })
    expect(response.status).toBe(202)
    expect(response.statusText).toBe('Accepted')
    expect(response.headers.get('content-type')).toBe('application/json')
    expect(await response.json()).toEqual({
      input: { mutation: 'widget.insert' },
    })
  })

  it('runs application push SQL locally for the owning namespace at zero wrapper row cost', async () => {
    const localClient = { namespace: 'ns:proj-a' }
    const remoteStub = {
      applicationSqlSession: vi.fn(),
      applicationSqlQuery: vi.fn(),
    }
    const idFromName = vi.fn((name: string) => name)
    const get = vi.fn(() => remoteStub)
    const applicationPush = vi.fn(async (context) => {
      expect(context.instance).toBe('ns:proj-a')
      expect(context.input).toEqual({ mutation: 'widget.insert' })
      expect(context.applicationSql()).toBe(localClient)
      expect(context.applicationSql('proj-a', { priority: 'latency-sensitive' })).toBe(
        localClient
      )
      expect(
        context.applicationSql('proj-b', { priority: 'latency-sensitive' }).namespace
      ).toBe('ns:proj-b')
      return new Response('committed', {
        status: 201,
        headers: { 'x-push-result': 'local' },
      })
    })
    const runtime = createOrezDataWorker({
      name: 'testapp',
      schema: descriptor,
      applicationPush,
    })
    const zero = Object.create(runtime.ZeroDO.prototype) as any
    const durableSql = vi.fn(() => {
      throw new Error('push wrapper must not add durable SQL reads or writes')
    })
    zero.orezEnv = {
      ZERO_SQL_DO: { idFromName, get },
    }
    zero.orezExecutionContext = { waitUntil: vi.fn() }
    zero.orezInstance = 'ns:proj-a'
    zero.orezStorage = { sql: { exec: durableSql } }
    zero.applicationSqlLocalClient = vi.fn(() => localClient)

    const result = await zero.orezApplicationPush({ mutation: 'widget.insert' })

    expect(applicationPush).toHaveBeenCalledOnce()
    expect(zero.applicationSqlLocalClient).toHaveBeenNthCalledWith(1, 'ns:proj-a', {})
    expect(zero.applicationSqlLocalClient).toHaveBeenNthCalledWith(2, 'ns:proj-a', {
      priority: 'latency-sensitive',
    })
    expect(idFromName).toHaveBeenCalledOnce()
    expect(idFromName).toHaveBeenCalledWith('ns:proj-b')
    expect(get).toHaveBeenCalledWith('ns:proj-b')
    expect(durableSql).not.toHaveBeenCalled()
    expect(result.status).toBe(201)
    expect(result.headers).toContainEqual(['x-push-result', 'local'])
    expect(new TextDecoder().decode(result.body)).toBe('committed')
  })

  it('runs a namespace backup through one read-only application SQL session', async () => {
    const rowsFor = (sql: string) =>
      sql.includes('SELECT write_seq') ? [{ write_seq: 7 }] : []
    const session = {
      [Symbol.dispose]: vi.fn(),
      begin: vi.fn(async () => undefined),
      query: vi.fn(async (sql: string) => rowsFor(sql)),
      queryPreemptible: vi.fn(async (sql: string) => ({
        outcome: 'completed' as const,
        value: rowsFor(sql),
      })),
      queryBatch: vi.fn(async (statements: readonly { sql: string }[]) =>
        statements.map((statement) => rowsFor(statement.sql))
      ),
      queryBatchPreemptible: vi.fn(async (statements: readonly { sql: string }[]) => ({
        outcome: 'completed' as const,
        value: statements.map((statement) => rowsFor(statement.sql)),
      })),
      exec: vi.fn(),
      queryPlan: vi.fn(),
      queryPlanPreemptible: vi.fn(),
      registerTables: vi.fn(),
      commit: vi.fn(async () => undefined),
      commitPreemptible: vi.fn(async () => ({
        outcome: 'completed' as const,
        value: undefined,
      })),
      rollback: vi.fn(async () => undefined),
    }
    const applicationSqlSession = vi.fn(
      async (_sessionID: string, _options?: { readOnly?: boolean; priority?: string }) =>
        session
    )
    const applicationSqlQuery = vi.fn(
      async (sql: string) => rowsFor(sql) as Record<string, unknown>[]
    )
    const runtime = createOrezDataWorker({
      name: 'testapp',
      schema: descriptor,
      backup: {
        bucket: () => ({
          async createMultipartUpload() {
            return {
              async uploadPart(partNumber: number) {
                return { partNumber }
              },
              async complete() {},
              async abort() {},
            }
          },
          async get() {
            return null
          },
          async put() {},
          async list() {
            return { objects: [] }
          },
          async delete() {},
        }),
        async inventory() {
          return []
        },
        async authorize() {
          return true
        },
      },
    })
    const env = {
      ZERO_SQL_DO: {
        idFromName: (name: string) => name,
        get: () => ({ applicationSqlSession, applicationSqlQuery }),
      },
    }

    await runtime.backupManager!.exportNamespace(env as any, 'singleton')

    expect(applicationSqlSession).toHaveBeenCalledOnce()
    expect(applicationSqlQuery).not.toHaveBeenCalled()
    expect(session.begin).toHaveBeenCalledOnce()
    expect(session.queryPreemptible).toHaveBeenCalledTimes(2)
    expect(session.queryBatchPreemptible).toHaveBeenCalledTimes(2)
    expect(session.query).not.toHaveBeenCalled()
    expect(session.queryBatch).not.toHaveBeenCalled()
    expect(session.commitPreemptible).toHaveBeenCalledOnce()
    expect(session.commit).not.toHaveBeenCalled()
    expect(session.rollback).not.toHaveBeenCalled()

    await runtime.backupManager!.exportNamespace(env as any, 'singleton', {
      priority: 'normal',
      scanChunkBytes: 32 * 1024 * 1024,
    })

    expect(applicationSqlSession).toHaveBeenNthCalledWith(1, expect.any(String), {
      readOnly: true,
      priority: 'background',
    })
    expect(applicationSqlSession).toHaveBeenNthCalledWith(2, expect.any(String), {
      readOnly: true,
      priority: 'normal',
    })
    expect(session.queryPreemptible).toHaveBeenCalledTimes(2)
    expect(session.queryBatchPreemptible).toHaveBeenCalledTimes(2)
    expect(session.query).toHaveBeenCalledTimes(2)
    expect(session.queryBatch).toHaveBeenCalledTimes(2)
    expect(session.commitPreemptible).toHaveBeenCalledOnce()
    expect(session.commit).toHaveBeenCalledOnce()
  })

  it('schedules an application consumer only after a published commit', async () => {
    const notified = vi.fn(() => undefined)
    const runtime = createOrezDataWorker({
      name: 'testapp',
      schema: descriptor,
      applicationSqlDidCommit: notified,
    })
    const pending: Promise<unknown>[] = []
    const executionContext = {
      waitUntil(promise: Promise<unknown>) {
        pending.push(promise)
      },
    }
    const env = { ZERO_SQL_DO: {} }
    const zero = Object.create(runtime.ZeroDO.prototype) as any
    zero.orezEnv = env
    zero.orezExecutionContext = executionContext
    zero.orezInstance = 'ns:proj-a'
    zero.orezBumpBackupMarker = vi.fn()

    zero.applicationSqlDidCommit(false, true)
    expect(zero.orezBumpBackupMarker).toHaveBeenCalledOnce()
    expect(notified).not.toHaveBeenCalled()

    zero.applicationSqlDidCommit(true, true)
    expect(notified).not.toHaveBeenCalled()
    await Promise.all(pending)
    expect(zero.orezBumpBackupMarker).toHaveBeenCalledTimes(2)
    expect(notified).toHaveBeenCalledWith({
      env,
      executionContext,
      instance: 'ns:proj-a',
    })
  })

  it.each([
    [
      'synchronous',
      () => {
        throw new Error('synchronous notifier failure')
      },
    ],
    [
      'asynchronous',
      async () => {
        throw new Error('asynchronous notifier failure')
      },
    ],
  ])('contains a %s post-commit notifier failure', async (_kind, notify) => {
    const runtime = createOrezDataWorker({
      name: 'testapp',
      schema: descriptor,
      applicationSqlDidCommit: notify,
    })
    const pending: Promise<unknown>[] = []
    const zero = Object.create(runtime.ZeroDO.prototype) as any
    zero.orezEnv = { ZERO_SQL_DO: {} }
    zero.orezExecutionContext = {
      waitUntil(promise: Promise<unknown>) {
        pending.push(promise)
      },
    }
    zero.orezInstance = 'ns:proj-failure'
    zero.orezBumpBackupMarker = vi.fn()
    const reported = vi.spyOn(console, 'error').mockImplementation(() => {})

    expect(() => zero.applicationSqlDidCommit(true, true)).not.toThrow()
    await expect(Promise.all(pending)).resolves.toEqual([undefined])
    expect(reported).toHaveBeenCalledWith(
      expect.stringContaining('application_sql_commit_notification_failed')
    )
    reported.mockRestore()
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
      OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN: 'operator-token',
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

    fetch.mockResolvedValueOnce(Response.json({ bootID: 'boot-1' }))
    const statusResponse = await runtime.fetch(
      new Request('https://data.test/proj-a/_orez/status', {
        headers: { 'x-orez-admin-token': 'operator-token' },
      }),
      env,
      { waitUntil: vi.fn() }
    )
    expect(await statusResponse.json()).toEqual({ bootID: 'boot-1' })
    expect(status).toHaveBeenCalledTimes(1)
    const statusRequest = fetch.mock.calls[1]?.[0]
    expect(new URL(statusRequest.url).pathname).toBe('/_orez/status')
    expect(statusRequest.headers.get('x-orez-admin-token')).toBe('operator-token')
    expect(statusRequest.headers.get('x-orez-do-instance')).toBe('ns:proj-a')
  })

  it('rejects protected status before resolving or instantiating the durable object', async () => {
    const idFromName = vi.fn()
    const get = vi.fn()
    const setup = vi.fn()
    const routes = vi.fn()
    const runtime = createOrezDataWorker({
      name: 'testapp',
      schema: descriptor,
      setup,
      routes,
    })

    const response = await runtime.fetch(
      new Request('https://data.test/proj-cold/_orez/status'),
      {
        OREZ_DO_WRITE_BUDGET_ADMIN_TOKEN: 'operator-token',
        ZERO_SQL_DO: { idFromName, get },
      },
      { waitUntil: vi.fn() }
    )

    expect(response.status).toBe(403)
    expect(await response.json()).toEqual({
      error: 'forbidden',
      sqlBillingSinceBoot: { rowsWritten: 0 },
    })
    expect(setup).not.toHaveBeenCalled()
    expect(routes).not.toHaveBeenCalled()
    expect(idFromName).not.toHaveBeenCalled()
    expect(get).not.toHaveBeenCalled()
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
