// @vitest-environment jsdom

import { createSchema, string, table } from '@rocicorp/zero'
import { expect, test } from 'vitest'

import { createZeroClient } from './createZeroClient'

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
