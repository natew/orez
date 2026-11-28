import { expect, test } from 'vitest'

import { NativeZero } from './client.js'
import { nativeZeroClient } from './zeroClient.js'

test("the provider's mutate returns Zero's settlement pair, never a promise", async () => {
  const native = new NativeZero({
    schema: { tables: {} },
    mutators: { message: { send: async () => {} } },
    transport: {
      pull: () => new Promise(() => {}),
      push: () => new Promise(() => {}),
    },
    engine: {
      async close() {},
      async execute(input) {
        switch (input.op) {
          case 'state':
            return {
              clientID: 'client',
              clientGroupID: 'group',
              queries: [],
              queryVersion: 0,
            }
          case 'beginMutation':
            return {
              transaction: 'tx',
              mutation: { id: 1, clientID: 'client', name: 'message.send', timestamp: 0 },
            }
          case 'publish':
            return { results: [] }
          case 'commitMutation':
            return { clientID: 'client' }
          default:
            throw new Error('unexpected engine operation ' + input.op)
        }
      },
    },
  })
  const zero = await nativeZeroClient(
    native,
    { name: 'store', schema: { tables: {} } as never, transport: undefined },
    { userID: 'user', active: false }
  )

  // an async boundary returning the result must hand it back intact
  const issue = async () => zero.mutate.message.send({})
  const result = await issue()
  expect('then' in result).toBe(false)
  await expect(result.client).resolves.toEqual({ type: 'success' })
  zero.close()
})
