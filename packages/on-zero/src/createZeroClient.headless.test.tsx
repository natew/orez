// @vitest-environment jsdom

import { createSchema, string, table } from '@rocicorp/zero'
import { expect, test } from 'vitest'

import { createZeroClient } from './createZeroClient'
import { onMutationError, type MutationError } from './helpers/useMutation'

import type { Query } from '@rocicorp/zero'

const userTable = table('user').columns({ id: string(), name: string() }).primaryKey('id')
const schema = createSchema({ tables: [userTable] })

const byId = (args: { id: string }) => args as unknown as Query<'user', typeof schema>

// connectHeadless exists so a host with no react tree — a worker, a durable
// object, a script — resolves the same facade a mounted app resolves. the
// behavior that matters is publication: waitForZero() is how consumers block
// until the instance is usable, and before connectHeadless it must not resolve,
// or every headless caller would race an instance that isn't there yet.
test('connectHeadless publishes the instance with no react tree', async () => {
  const client = createZeroClient({
    schema,
    models: {},
    groupedQueries: { headlessTest: { byId } },
    instanceName: 'headless-test',
  })

  // waitForZero must be pending, not resolved, before anything connects.
  // Promise.race against an already-resolved sentinel is what distinguishes
  // "pending" from "resolved with undefined" — an await alone cannot.
  const pending = Symbol('pending')
  const before = await Promise.race([
    client.waitForZero(),
    Promise.resolve(pending as unknown as never),
  ])
  expect(before).toBe(pending)

  const connection = client.connectHeadless({
    userID: 'user-headless',
    kvStore: 'mem',
    storageKey: 'headless-test',
  })

  expect(await client.waitForZero()).toBe(connection.zero)

  await connection.close()
})

test('a hot-recreated client resolves the mounted same-name instance', async () => {
  const first = createZeroClient({
    schema,
    models: {},
    groupedQueries: { headlessHotTest: { byId } },
    instanceName: 'headless-hot-test',
  })
  const connection = first.connectHeadless({
    userID: 'user-headless-hot',
    kvStore: 'mem',
    storageKey: 'headless-hot-test',
  })

  const refreshed = createZeroClient({
    schema,
    models: {},
    groupedQueries: { headlessHotTest: { byId } },
    instanceName: 'headless-hot-test',
  })

  expect(refreshed.zero.clientID).toBe(connection.zero.clientID)
  expect(await refreshed.waitForZero()).toBe(connection.zero)

  await connection.close()
  expect(() => refreshed.zero.clientID).toThrow(/not initialized/)
})

test('direct facade mutations surface client failures without explicit observation', async () => {
  const client = createZeroClient({
    schema,
    models: {
      user: {
        mutate: {
          insert: async () => {
            throw new Error('optimistic mutation failed')
          },
        },
      },
    } as never,
    groupedQueries: { headlessObservedMutationTest: { byId } },
    instanceName: 'headless-observed-mutation-test',
  })
  const connection = client.connectHeadless({
    userID: 'user-headless-observed-mutation',
    kvStore: 'mem',
    storageKey: 'headless-observed-mutation-test',
  })
  const errors: MutationError[] = []
  const dispose = onMutationError((error) => errors.push(error))

  const mutate = Reflect.get(client.zero, 'mutate')
  const user = Reflect.get(mutate, 'user')
  const insert = Reflect.get(user, 'insert')
  const result = Reflect.apply(insert, null, [{ id: 'u1', name: 'Ada' }])
  if (result === null || typeof result !== 'object') {
    throw new Error('mutation did not return client/server promises')
  }
  await Promise.all([Reflect.get(result, 'client'), Reflect.get(result, 'server')])
  await Promise.resolve()

  expect(errors).toHaveLength(1)
  expect(errors[0]).toMatchObject({
    kind: 'app',
    message: 'optimistic mutation failed',
  })

  errors.length = 0
  const missing = Reflect.get(user, 'createdAfterClientConstruction')
  expect(() => Reflect.apply(missing, null, [{}])).toThrow(
    "mutation 'user.createdAfterClientConstruction' is not registered"
  )
  expect(errors).toEqual([
    {
      scope: 'client',
      kind: 'zero',
      message:
        "[on-zero] mutation 'user.createdAfterClientConstruction' is not registered on the active Zero client.",
    },
  ])

  dispose()
  await connection.close()
})
