import { describe, expect, test } from 'vitest'

import { encodeSqlParams, encodeSqlValue } from './sql-wire.js'

describe('SQL wire encoding', () => {
  test('encodes every supported SQLite binding without losing integer precision', () => {
    expect(
      encodeSqlParams([
        null,
        false,
        true,
        -9,
        1.25,
        9_007_199_254_740_991n,
        'hello',
        Uint8Array.of(0, 127, 255),
      ])
    ).toEqual([
      { kind: 'null' },
      { kind: 'integer', value: '0' },
      { kind: 'integer', value: '1' },
      { kind: 'integer', value: '-9' },
      { kind: 'real', value: 1.25 },
      { kind: 'integer', value: '9007199254740991' },
      { kind: 'text', value: 'hello' },
      { kind: 'blob', value: [0, 127, 255] },
    ])
  })

  test('encodes the exact signed i64 bounds', () => {
    expect(encodeSqlValue(-(1n << 63n))).toEqual({
      kind: 'integer',
      value: '-9223372036854775808',
    })
    expect(encodeSqlValue((1n << 63n) - 1n)).toEqual({
      kind: 'integer',
      value: '9223372036854775807',
    })
  })

  test.each([
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.MAX_SAFE_INTEGER + 1,
    1n << 63n,
    undefined,
    { value: 1 },
  ])('rejects an ambiguous or unsupported binding %#', (value) => {
    expect(() => encodeSqlValue(value)).toThrow(TypeError)
  })
})
