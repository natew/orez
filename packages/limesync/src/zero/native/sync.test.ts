import { expect, test, vi } from 'vitest'

import { NativeZero } from './client.js'
import { createOrezTransport } from './orez.js'
import { NativeSync } from './sync.js'

import type { ClientState } from './types.js'

test('native pulls send only query changes after the replica commits them', async () => {
  const state: ClientState = {
    clientID: 'client',
    clientGroupID: 'group',
    cookie: '0',
    nextMutationID: 1,
    lastMutationID: 0,
    queryVersion: 0,
    queries: [],
    recoveryRequired: null,
    completedQueries: [],
    queryErrors: {},
  }
  const requests: unknown[] = []
  let loseResponse = false
  const transport = createOrezTransport({
    server: 'https://zero.test',
    appID: 'zero',
    schemaVersion: '1',
    async fetch(_url, init) {
      requests.push(JSON.parse(String(init?.body)).queries)
      if (loseResponse) throw new Error('response lost')
      return Response.json({ cookie: state.cookie })
    },
  })
  const zero = new NativeZero({
    schema: { tables: {} },
    mutators: {},
    engine: {
      async execute(input) {
        switch (input.op) {
          case 'setQueries':
            state.queries = Array.isArray(input.queries) ? input.queries : state.queries
            state.queryVersion++
            return state
          case 'beginPull':
            expect(input.response).toBe(JSON.stringify({ cookie: state.cookie }))
            return {
              transaction: 'pull',
              pending: [],
              changed: false,
              lastMutationID: 0,
            }
          case 'commitPull':
            return state
          default:
            throw new Error('unexpected engine operation ' + input.op)
        }
      },
      async close() {},
    },
    transport,
  })
  const first = { hash: 'first', ast: { table: 'issue' } }
  const second = { hash: 'second', ast: { table: 'comment' } }
  try {
    await zero.pull([first])
    await zero.pull([first])
    await zero.pull([first, second])
    loseResponse = true
    await expect(zero.pull([second])).rejects.toThrow('response lost')
    loseResponse = false
    await zero.pull([first, second])
    await zero.pull([first, second])
    expect(requests).toEqual([
      { version: 1, baseVersion: 0, patch: [{ op: 'clear' }, { op: 'put', ...first }] },
      undefined,
      { version: 3, baseVersion: 1, patch: [{ op: 'put', ...second }] },
      { version: 4, baseVersion: 3, patch: [{ op: 'del', hash: first.hash }] },
      {
        version: 5,
        baseVersion: 0,
        patch: [{ op: 'clear' }, { op: 'put', ...first }, { op: 'put', ...second }],
      },
      undefined,
    ])
  } finally {
    await zero.close()
  }
})

test('native allocation failures back off across wakes and reconnects', async () => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'performance'] })
  const allocationError = new Error('std::bad_alloc (op=beginPull)')
  const push = vi
    .fn()
    .mockResolvedValue(undefined)
    .mockResolvedValueOnce(undefined)
    .mockRejectedValueOnce(allocationError)
  const pull = vi.fn().mockResolvedValue(undefined).mockRejectedValueOnce(allocationError)
  const report = vi.fn()
  let wake: (() => void) | undefined
  const sync = new NativeSync(
    { push, pull },
    {
      subscribeWake(request) {
        wake = request
        return () => {
          wake = undefined
        }
      },
    },
    report
  )

  try {
    await vi.advanceTimersByTimeAsync(0)
    expect(push).toHaveBeenCalledTimes(1)
    expect(pull).toHaveBeenCalledTimes(1)
    expect(report).toHaveBeenCalledWith(allocationError)

    await vi.advanceTimersByTimeAsync(400)
    wake?.()
    sync.setOnline(false)
    sync.setOnline(true)
    sync.request()
    await vi.advanceTimersByTimeAsync(599)
    expect(push).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1)
    expect(push).toHaveBeenCalledTimes(2)
    expect(pull).toHaveBeenCalledTimes(1)

    await vi.advanceTimersByTimeAsync(400)
    sync.setActive(false)
    sync.setActive(true)
    wake?.()
    await vi.advanceTimersByTimeAsync(1599)
    expect(push).toHaveBeenCalledTimes(2)
    await vi.advanceTimersByTimeAsync(1)
    expect(sync.state.name).toBe('connected')
    expect(push).toHaveBeenCalledTimes(3)
    expect(report).toHaveBeenCalledTimes(2)

    wake?.()
    await vi.advanceTimersByTimeAsync(0)
    expect(push).toHaveBeenCalledTimes(4)
  } finally {
    sync.stop()
    vi.useRealTimers()
  }
})
