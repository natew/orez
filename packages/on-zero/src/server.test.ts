import { createSchema, defineQueries, defineQuery, string, table } from '@rocicorp/zero'
import * as v from 'valibot'
import { describe, expect, test, vi } from 'vitest'

import { getScopedAuthData } from './helpers/mutatorContext'
import {
  createSyncQueries,
  createZeroServerBindings,
  type ZeroServerExecutor,
  type ZeroServerMutatorRegistry,
  type ZeroServerTransaction,
} from './server'
import { zql } from './zql'

import type { AuthData, MutatorContext } from './types'

const project = table('project')
  .columns({
    id: string(),
    ownerId: string(),
    name: string(),
  })
  .primaryKey('id')

const schema = createSchema({
  tables: [project],
  relationships: [],
  enableLegacyQueries: true,
})

const queries = defineQueries({
  project: {
    byOwner: defineQuery(v.object({ ownerId: v.string() }), ({ args }) =>
      zql.project.where('ownerId', args.ownerId)
    ),
  },
})

type ProjectRow = { id: string; ownerId: string; name: string }

function createTestExecutor(
  mutators: ZeroServerMutatorRegistry<typeof schema>,
  rows: ProjectRow[]
) {
  const scheduledBackground: Promise<void>[] = []
  const queryClaims: unknown[] = []
  const tx = {
    mutate: {
      project: {
        async insert(value: ProjectRow) {
          rows.push(value)
        },
      },
    },
    async run() {
      return []
    },
  } as unknown as ZeroServerTransaction<typeof schema>

  const executor: ZeroServerExecutor<typeof schema> = {
    async execute(name, args, claims) {
      const barriers: Promise<void>[] = []
      const mutator = mutators[name]
      if (!mutator) throw new Error(`unknown mutator: ${name}`)
      await mutator({
        tx,
        args,
        ctx: {
          claims,
          defer(effect, options) {
            const promise = Promise.resolve().then(effect)
            if (options?.barrier) barriers.push(promise)
            else scheduledBackground.push(promise)
          },
        },
      })
      await Promise.all(barriers)
    },
    async transaction(_claims, work) {
      return work(tx)
    },
    async query(claims, work) {
      queryClaims.push(claims)
      return work(tx)
    },
  }

  return { executor, queryClaims, scheduledBackground }
}

describe('createZeroServerBindings', () => {
  test('an anonymous mutation keeps authData null so auth-required mutators reject', async () => {
    const rows: ProjectRow[] = []
    let seenAuthData: AuthData | null | undefined
    const bindings = createZeroServerBindings({
      schema,
      models: {
        project: {
          mutate: {
            create: async ({ authData, tx }: MutatorContext, value: ProjectRow) => {
              seenAuthData = authData
              if (!authData) throw new Error('not authenticated')
              await tx.mutate.project.insert(value)
            },
          },
        },
      },
      createServerActions: () => ({}),
      queries,
    })
    const { executor } = createTestExecutor(bindings.mutators, rows)
    const server = bindings.server(executor)

    await expect(
      server.mutate.project.create({
        id: 'project-1',
        ownerId: 'anon',
        name: 'sneaky',
      })
    ).rejects.toThrow('not authenticated')
    expect(seenAuthData).toBe(null)
    expect(rows).toEqual([])
  })

  test('enqueues typed actions through the configured local executor', async () => {
    type Action =
      | { type: 'project.provisionNamespace'; projectId: string }
      | { type: 'project.invalidateAccess'; projectId: string }

    const executed: Action[] = []
    let releaseBackground: () => void = () => {}
    const backgroundGate = new Promise<void>((resolve) => {
      releaseBackground = resolve
    })
    const bindings = createZeroServerBindings({
      schema,
      models: {
        project: {
          mutate: {
            create: async ({ server }: MutatorContext, value: ProjectRow) => {
              const enqueueAction = server?.enqueueAction as (
                action: Action,
                options?: { barrier?: boolean }
              ) => void
              enqueueAction(
                { type: 'project.provisionNamespace', projectId: value.id },
                { barrier: true }
              )
              enqueueAction({ type: 'project.invalidateAccess', projectId: value.id })
            },
          },
        },
      },
      createServerActions: () => ({}),
      actions: {
        async execute(action: Action) {
          if (action.type === 'project.invalidateAccess') await backgroundGate
          executed.push(action)
        },
      },
    })
    const { executor, scheduledBackground } = createTestExecutor(bindings.mutators, [])

    await executor.execute(
      'project|create',
      { id: 'project-1', ownerId: 'user-1', name: 'first' },
      { userID: 'user-1', authData: { id: 'user-1' } }
    )
    expect(executed).toEqual([
      { type: 'project.provisionNamespace', projectId: 'project-1' },
    ])
    releaseBackground()
    await Promise.all(scheduledBackground)
    expect(executed).toEqual([
      { type: 'project.provisionNamespace', projectId: 'project-1' },
      { type: 'project.invalidateAccess', projectId: 'project-1' },
    ])
  })

  test('selects remote dispatch once and never falls back to local execution', async () => {
    type Action = { type: 'durable.run'; operationId: string }
    const execute = vi.fn(async (_action: Action) => {})
    const dispatchRemote = vi.fn(async (_action: Action) => {
      throw new Error('remote unavailable')
    })
    const bindings = createZeroServerBindings({
      schema,
      models: {
        project: {
          mutate: {
            create: async ({ server }: MutatorContext) => {
              const enqueueAction = server?.enqueueAction as (
                action: Action,
                options?: { barrier?: boolean }
              ) => void
              enqueueAction(
                { type: 'durable.run', operationId: 'operation-1' },
                { barrier: true }
              )
            },
          },
        },
      },
      createServerActions: () => ({}),
      actions: { execute, dispatchRemote },
    })
    const { executor } = createTestExecutor(bindings.mutators, [])

    await expect(
      executor.execute(
        'project|create',
        { id: 'project-1', ownerId: 'user-1', name: 'first' },
        { userID: 'user-1', authData: { id: 'user-1' } }
      )
    ).rejects.toThrow('remote unavailable')
    expect(dispatchRemote).toHaveBeenCalledOnce()
    expect(execute).not.toHaveBeenCalled()
  })
})

describe('createSyncQueries', () => {
  // a sync host decides between refusing a whole pull and dropping one stale
  // query by testing `typeof entry.fn === 'function'`, so a name this registry
  // cannot serve has to read as absent rather than as an entry that throws.
  const hostQueries = createSyncQueries({ schema, queries })

  test('serves a registered query', () => {
    const entry = (hostQueries as Record<string, Record<string, { fn?: unknown }>>)
      .project.byOwner
    expect(typeof entry.fn).toBe('function')
  })

  test('reports an unregistered name as absent', () => {
    const registry = hostQueries as Record<string, Record<string, unknown>>
    expect(registry.project.byOwnerRenamed).toBeUndefined()
    expect(registry.noSuchNamespace.anything).toBeUndefined()
    expect(registry.permission.project).toBeUndefined()
  })
})
