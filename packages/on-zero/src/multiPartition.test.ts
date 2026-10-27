import { createSchema, string, table } from '@rocicorp/zero'
import { describe, expect, test, vi } from 'vitest'

import { getInstanceForQueryFn } from './instanceRegistry'
import { createZeroClients } from './multi'

const schema = createSchema({
  tables: [table('message').columns({ id: string() }).primaryKey('id')],
})

describe('createZeroClients', () => {
  test('builds generated partitions with default as the primary client', () => {
    const instances = {
      default: {
        schema,
        queries: { account: { current: () => ({}) as never } },
        models: {},
        tables: ['account'],
        syncTables: ['account'],
        supportTables: [],
        scope: null,
        defaultVisibility: null,
      },
      project: {
        schema,
        queries: { message: { all: () => ({}) as never } },
        models: {},
        tables: ['message'],
        syncTables: ['message'],
        supportTables: [],
        scope: 'projectId',
        defaultVisibility: (value: string) => ({ column: 'projectId', value }),
      },
    } as const

    const result = createZeroClients(instances, { inner: 'default' })

    expect(Object.keys(result.clients)).toEqual(['project', 'default'])
    expect(result.clients.default.instanceName).toBe('default')
    expect(result.clients.project.instanceName).toBe('project')
    expect(result.providers.default).toBe(result.clients.default.ProvideZero)
    expect(result.providers.project).toBe(result.clients.project.ProvideZero)
    expect(result.combined.run).toBeTypeOf('function')

    const innerQuery = vi
      .spyOn(result.clients.default, 'useQuery')
      .mockReturnValue('inner' as never)
    const outerQuery = vi
      .spyOn(result.clients.project, 'useQueryDirect')
      .mockReturnValue('outer' as never)
    expect(result.combined.useQuery(instances.default.queries.account.current, {})).toBe(
      'inner'
    )
    expect(result.combined.useQuery(instances.project.queries.message.all, {})).toBe(
      'outer'
    )
    expect(innerQuery).toHaveBeenCalledOnce()
    expect(outerQuery).toHaveBeenCalledOnce()
  })

  test('requires at least one generated instance', () => {
    expect(() => createZeroClients({}, { inner: 'missing' as never })).toThrow(
      /at least one instance/
    )
  })

  test('rejects an unknown inner instance before replacing registered clients', () => {
    const entry = {
      schema,
      queries: { preserved: { current: () => ({}) as never } },
      models: {},
      tables: [],
      syncTables: [],
      supportTables: [],
      scope: null,
      defaultVisibility: null,
    }
    createZeroClients({ default: entry }, { inner: 'default' })
    const owner = getInstanceForQueryFn(entry.queries.preserved.current)
    expect(owner?.name).toBe('default')
    expect(() =>
      createZeroClients({ default: entry }, { inner: 'missing' as never })
    ).toThrow(/inner instance 'missing'/)
    expect(getInstanceForQueryFn(entry.queries.preserved.current)).toBe(owner)
  })

  test('replaces an existing partition atomically when a namespace moves', () => {
    const entry = (queries: Record<string, Record<string, () => never>>) => ({
      schema,
      queries,
      models: {},
      tables: Object.keys(queries),
      syncTables: Object.keys(queries),
      supportTables: [],
      scope: null,
      defaultVisibility: null,
    })

    createZeroClients(
      {
        default: entry({ movedNamespace: { before: () => ({}) as never } }),
        project: entry({ stableProject: { before: () => ({}) as never } }),
      },
      { inner: 'project' }
    )

    expect(() =>
      createZeroClients(
        {
          default: entry({ stableDefault: { after: () => ({}) as never } }),
          project: entry({ movedNamespace: { after: () => ({}) as never } }),
        },
        { inner: 'project' }
      )
    ).not.toThrow()
  })
})
