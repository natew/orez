import { createSchema, string, table } from '@rocicorp/zero'
import { describe, expect, test } from 'vitest'

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
        scope: null,
        defaultVisibility: null,
      },
      project: {
        schema,
        queries: { message: { all: () => ({}) as never } },
        models: {},
        tables: ['message'],
        syncTables: ['message'],
        scope: 'projectId',
        defaultVisibility: (value: string) => ({ column: 'projectId', value }),
      },
    } as const

    const result = createZeroClients(instances)

    expect(Object.keys(result.clients)).toEqual(['project', 'default'])
    expect(result.clients.default.instanceName).toBe('default')
    expect(result.clients.project.instanceName).toBe('project')
    expect(result.providers.default).toBe(result.clients.default.ProvideZero)
    expect(result.providers.project).toBe(result.clients.project.ProvideZero)
    expect(result.combined.run).toBeTypeOf('function')
  })

  test('requires at least one generated instance', () => {
    expect(() => createZeroClients({})).toThrow(/at least one instance/)
  })

  test('replaces an existing partition atomically when a namespace moves', () => {
    const entry = (queries: Record<string, Record<string, () => never>>) => ({
      schema,
      queries,
      models: {},
      tables: Object.keys(queries),
      syncTables: Object.keys(queries),
      scope: null,
      defaultVisibility: null,
    })

    createZeroClients({
      default: entry({ movedNamespace: { before: () => ({}) as never } }),
      project: entry({ stableProject: { before: () => ({}) as never } }),
    })

    expect(() =>
      createZeroClients({
        default: entry({ stableDefault: { after: () => ({}) as never } }),
        project: entry({ movedNamespace: { after: () => ({}) as never } }),
      })
    ).not.toThrow()
  })
})
