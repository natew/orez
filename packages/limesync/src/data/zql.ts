import { getZQL } from './state.js'

import type { QueryBuilder } from './types.js'

export const query = new Proxy({} as QueryBuilder, {
  get(_target, prop) {
    const b = getZQL()
    return (b as any)[prop]
  },
})

/** @deprecated Use `query` from `limesync/data`. */
export const zql = query
