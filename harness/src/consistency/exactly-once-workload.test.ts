import { describe, expect, test } from 'bun:test'

import {
  assertExpectedExactlyOncePush,
  parseExactlyOncePush,
  validateIncrementProbeArgs,
} from './exactly-once-workload.js'

const body = {
  timestamp: 123450,
  clientGroupID: 'group-1',
  pushVersion: 1,
  requestID: 'request-1',
  mutations: [
    {
      type: 'custom',
      clientID: 'client-1',
      id: 1,
      timestamp: 123456,
      name: 'exactlyOnce.incrementProbe',
      args: [{ id: 'probe-1' }],
    },
  ],
}

describe('exactly-once workload boundary', () => {
  test('parses one canonical mutation and produces a stable digest', () => {
    const parsed = parseExactlyOncePush(body)
    expect(parsed.identity).toEqual({
      clientGroupId: 'group-1',
      clientId: 'client-1',
      mutationId: 1,
    })
    expect(parsed.args).toEqual({ id: 'probe-1' })
    expect(parsed.bodyDigest).toMatch(/^[0-9a-f]{64}$/)
    expect(
      parseExactlyOncePush({ ...body, requestID: 'different', timestamp: 999 }).bodyDigest
    ).toBe(parsed.bodyDigest)
    const changedTimestamp = {
      ...body,
      mutations: [{ ...body.mutations[0], timestamp: 123457 }],
    }
    expect(parseExactlyOncePush(changedTimestamp).bodyDigest).not.toBe(parsed.bodyDigest)
  })

  test('rejects zero, multiple, malformed, and mismatched mutations', () => {
    expect(() => parseExactlyOncePush({ ...body, mutations: [] })).toThrow(
      'exactly one mutation'
    )
    expect(() =>
      parseExactlyOncePush({ ...body, mutations: [...body.mutations, ...body.mutations] })
    ).toThrow('exactly one mutation')
    expect(() =>
      parseExactlyOncePush({
        ...body,
        mutations: [{ ...body.mutations[0], id: 0 }],
      })
    ).toThrow('does not match')
    expect(() => parseExactlyOncePush({ ...body, pushVersion: 2 })).toThrow(
      'does not match'
    )
    const { timestamp: _timestamp, ...withoutTimestamp } = body
    expect(() => parseExactlyOncePush(withoutTimestamp)).toThrow('unknown fields')
    const { requestID: _requestID, ...withoutRequestID } = body
    expect(() => parseExactlyOncePush(withoutRequestID)).toThrow('unknown fields')
    expect(() => parseExactlyOncePush({ ...body, extra: true })).toThrow('unknown fields')
    expect(() =>
      parseExactlyOncePush({
        ...body,
        mutations: [{ ...body.mutations[0], extra: true }],
      })
    ).toThrow('does not match')
    expect(() =>
      parseExactlyOncePush({
        ...body,
        mutations: [{ ...body.mutations[0], args: [{ id: 'probe-1', extra: true }] }],
      })
    ).toThrow('nonempty id')
    const parsed = parseExactlyOncePush(body)
    expect(() =>
      assertExpectedExactlyOncePush(parsed, {
        identity: { ...parsed.identity, mutationId: 2 },
        args: parsed.args,
      })
    ).toThrow('does not match the armed')
  })

  test('validates the probe id', () => {
    expect(validateIncrementProbeArgs({ id: 'probe' })).toEqual({ id: 'probe' })
    expect(() => validateIncrementProbeArgs({ id: '' })).toThrow('nonempty id')
  })
})
