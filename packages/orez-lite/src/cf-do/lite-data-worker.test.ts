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
})
