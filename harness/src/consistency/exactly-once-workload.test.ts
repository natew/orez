import { describe, expect, test } from 'bun:test'

import {
  assertExpectedExactlyOncePush,
  assertRejectingIncrementResponse,
  buildRejectingIncrementPush,
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
    expect(parsed.operationDigest).toMatch(/^[0-9a-f]{64}$/)
    expect(parsed.mutationTimestamp).toBe(123456)
    expect(
      parseExactlyOncePush({ ...body, requestID: 'different', timestamp: 999 })
        .operationDigest
    ).toBe(parsed.operationDigest)
    const changedTimestamp = {
      ...body,
      mutations: [{ ...body.mutations[0], timestamp: 123457 }],
    }
    expect(parseExactlyOncePush(changedTimestamp).operationDigest).toBe(
      parsed.operationDigest
    )
    expect(parseExactlyOncePush(changedTimestamp).mutationTimestamp).toBe(123457)
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

  test('builds mutation 2 and requires its deterministic app error', () => {
    const parsed = parseExactlyOncePush(body)
    const rejection = JSON.parse(
      buildRejectingIncrementPush(JSON.stringify(body), {
        identity: parsed.identity,
        args: parsed.args,
      })
    )
    expect(rejection.mutations).toEqual([
      expect.objectContaining({
        clientID: 'client-1',
        id: 2,
        name: 'exactlyOnce.incrementThenReject',
        args: [{ id: 'probe-1' }],
      }),
    ])
    const response = {
      pushResponse: {
        mutations: [
          {
            id: { clientID: 'client-1', id: 2 },
            result: {
              error: 'app',
              message: 'intentional-reject',
              details: 'intentional-reject',
            },
          },
        ],
      },
    }
    expect(() =>
      assertRejectingIncrementResponse(response, {
        ...parsed.identity,
        mutationId: 2,
      })
    ).not.toThrow()
    response.pushResponse.mutations[0]!.result = {} as never
    expect(() =>
      assertRejectingIncrementResponse(response, {
        ...parsed.identity,
        mutationId: 2,
      })
    ).toThrow('expected one app-error response')
  })
})
