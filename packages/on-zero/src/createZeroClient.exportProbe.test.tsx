import { createSchema, string, table } from '@rocicorp/zero'
import { describe, expect, test } from 'vitest'

import { createZeroClient } from './createZeroClient'

import type { Query } from '@rocicorp/zero'

const userTable = table('user').columns({ id: string(), name: string() }).primaryKey('id')
const schema = createSchema({ tables: [userTable] })

// `zero` is exported from the app's zero-client module, so JS tooling reads
// identity properties off it while no provider has created an instance yet.
// metro's fast refresh is the case that bit us: it registers every module
// export as a react-refresh family and, on the next hot update, reads
// `.prototype` on the previous and next values. that read threw out of
// performReactRefresh, which abandoned the whole pass and silently dropped
// every pending component update.
describe('zero proxy identity probes with no instance', () => {
  const { zero } = createZeroClient({
    schema,
    models: {},
    groupedQueries: {
      probe: {
        byId: (args: { id: string }) => args as unknown as Query<'user', typeof schema>,
      },
    },
    instanceName: 'export-probe',
  })

  test('reads that can never be Zero API answer undefined instead of throwing', () => {
    expect((zero as unknown as { prototype?: unknown }).prototype).toBeUndefined()
    expect(Object.prototype.toString.call(zero)).toBe('[object Object]')
    expect(
      (zero as unknown as Record<symbol, unknown>)[Symbol.toPrimitive]
    ).toBeUndefined()
    expect((zero as unknown as Record<symbol, unknown>)[Symbol.iterator]).toBeUndefined()
  })

  test('real API access still throws loudly', () => {
    expect(() => (zero as unknown as { query: unknown }).query).toThrow(
      /Zero instance not initialized/
    )
  })
})
