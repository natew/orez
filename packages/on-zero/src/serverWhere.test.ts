import { describe, expect, test, beforeEach } from 'vitest'

import { serverWhere } from './serverWhere'
import { getEnvironment, setEnvironment } from './state'
import { setEvaluatingPermission } from './where'

// mock expression builder that tracks what was called
function createMockEB() {
  const calls: string[] = []
  return {
    calls,
    and: () => {
      calls.push('and()')
      return { type: 'noop' }
    },
    cmp: (field: string, value: any) => {
      calls.push(`cmp(${field}, ${value})`)
      return { type: 'condition', field, value }
    },
    cmpLit: (a: any, op: string, b: any) => {
      calls.push(`cmpLit(${a}, ${op}, ${b})`)
      return { type: 'literal', a, op, b }
    },
  }
}

describe('serverWhere SSR behavior', () => {
  beforeEach(() => {
    // reset state before each test
    ;(globalThis as any)[Symbol.for('on-zero:state')] = null
  })

  test('environment stays server when already set (SSR scenario)', () => {
    // simulate SSR: server sets environment first
    setEnvironment('server')
    expect(getEnvironment()).toBe('server')

    // simulate createZeroClient being called during SSR
    // (it should NOT overwrite to 'client' when environment is already set)
    if (getEnvironment() === null) {
      setEnvironment('client')
    }

    // environment should still be 'server'
    expect(getEnvironment()).toBe('server')
  })

  test('environment defaults to client when not set by server', () => {
    // simulate pure client: no server ran first
    expect(getEnvironment()).toBe(null)

    // createZeroClient sets environment when null
    if (getEnvironment() === null) {
      setEnvironment('client')
    }

    expect(getEnvironment()).toBe('client')
  })

  test('nested serverWhere calls evaluate during permission check', () => {
    setEnvironment('client')

    // outer permission check uses serverWhere
    const outerWhere = serverWhere('post', (eb) => eb.cmp('ownerId', 'user-123'))
    // nested serverWhere inside (simulates complex permission logic)
    const nestedWhere = serverWhere('comment', (eb) => eb.cmp('postId', 'post-456'))

    const eb = createMockEB()

    // without evaluating permission flag, both return no-op
    const result1 = outerWhere(eb as any, { id: 'user-123' })
    expect(result1).toEqual({ type: 'noop' })

    const eb2 = createMockEB()
    // with evaluating permission flag, both should evaluate
    setEvaluatingPermission(true)
    try {
      const result2 = outerWhere(eb2 as any, { id: 'user-123' })
      expect(result2).toEqual({ type: 'condition', field: 'ownerId', value: 'user-123' })

      const eb3 = createMockEB()
      const result3 = nestedWhere(eb3 as any, { id: 'user-123' })
      expect(result3).toEqual({ type: 'condition', field: 'postId', value: 'post-456' })
    } finally {
      setEvaluatingPermission(false)
    }
  })
})
