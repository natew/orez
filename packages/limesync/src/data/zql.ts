import { getZQL } from './state.js'

import type { QueryBuilder } from './types.js'

export const zql = new Proxy({} as QueryBuilder, {
  get(_target, prop) {
    const b = getZQL()
    return (b as any)[prop]
  },
})
