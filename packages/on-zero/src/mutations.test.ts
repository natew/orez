import { describe, expect, test } from 'vitest'

import { mutations } from './mutations'
import { serverWhere } from './serverWhere'

describe('mutations registry', () => {
  test('re-registering a handler replaces it per key (HMR)', () => {
    const permissions = serverWhere('post', () => true)
    const v1 = async () => {}
    const v2 = async () => {}
    mutations('post', permissions, { custom: v1 })
    const proxy = mutations('post', permissions, { custom: v2 })
    // per-key merge must still take the newest registration for an edited
    // handler, otherwise HMR would pin the stale implementation
    expect(proxy.custom).toBe(v2)
  })
})
