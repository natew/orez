// @vitest-environment node
//
// eager construction is a CLIENT-runtime behavior. ProvideZero renders through
// ProvideZeroServer on a server runtime, which has no hooks and creates
// nothing: a render pass on the server must never open a store or a socket,
// and every descendant still gets the inert stub plus DisabledContext='empty'.

import { renderToString } from 'react-dom/server'
import { expect, test, vi } from 'vitest'

import { useZero } from '../zero/react.js'
import { createSchema, string, table } from '../zero/zero.js'

const fakeZero = vi.hoisted(() => {
  const instances: unknown[] = []
  class FakeZero {
    constructor() {
      instances.push(this)
    }
  }
  return { FakeZero, instances }
})

vi.mock('../zero/zero.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../zero/zero.js')>()
  return { ...actual, Zero: fakeZero.FakeZero }
})

import { createZeroClient } from './createZeroClient.js'
import { IS_SERVER_RUNTIME } from './helpers/platform.js'

if (typeof window !== 'undefined') {
  throw new Error('provideZero.ssr.test.tsx must run with @vitest-environment node')
}

const todoTable = table('todo')
  .columns({ id: string(), title: string() })
  .primaryKey('id')
const schema = createSchema({ tables: [todoTable] })

const client = createZeroClient({
  schema,
  models: {},
  groupedQueries: {},
  instanceName: 'ssr-eager-test',
})

test('the server runtime is what this file exercises', () => {
  expect(IS_SERVER_RUNTIME).toBe(true)
})

test('rendering ProvideZero on the server constructs no client', () => {
  let contextClientID: unknown
  const Probe = () => {
    contextClientID = (useZero() as unknown as { clientID: string }).clientID
    return null
  }

  renderToString(
    <client.ProvideZero cacheURL="http://127.0.0.1:7788/zero" userID="ssr">
      <Probe />
    </client.ProvideZero>
  )

  expect(fakeZero.instances.length).toBe(0)
  expect(contextClientID).toBe('disabled')
  expect(() => (client.zero as unknown as { clientID: string }).clientID).toThrow(
    /Zero instance not initialized/
  )
})
