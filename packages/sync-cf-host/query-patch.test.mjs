import { describe, expect, test } from 'bun:test'

import { resolveQueryPatch } from './src/query-patch.ts'

const fail = (message) => new Error(message)

const registry = {
  good: { ok: { fn: () => ({ ast: { table: 'ok' } }) } },
  broken: {
    boom: {
      fn: () => {
        throw new Error('exploded')
      },
    },
  },
}

const put = (name, hash) => ({ op: 'put', name, hash, args: [{}] })

const resolve = (patch, tolerate) =>
  resolveQueryPatch(patch, registry, {}, 7, fail, tolerate)

describe('resolveQueryPatch unknown names', () => {
  test('refuses the whole pull by default, naming the unknown query', () => {
    expect(() => resolve([put('good.ok', 'a'), put('gone.removed', 'b')])).toThrow(
      /gone\.removed/
    )
  })

  test('tolerating skew drops only the unknown query', () => {
    const out = resolve(
      [put('gone.removed', 'a'), put('good.ok', 'b'), { op: 'del', hash: 'c' }],
      true
    )
    expect(out).toEqual([
      { op: 'put', hash: 'b', ast: { table: 'ok' }, transformVersion: 7 },
      { op: 'del', hash: 'c' },
    ])
  })

  test('a patch of only unknown queries resolves to an empty patch', () => {
    expect(resolve([put('gone.removed', 'a')], true)).toEqual([])
  })

  test('a registered query that fails to build still fails the whole patch', () => {
    // tolerating skew must not swallow a fault in the app's own query code.
    expect(() => resolve([put('good.ok', 'a'), put('broken.boom', 'b')], true)).toThrow(
      /broken\.boom.*exploded/
    )
  })
})
