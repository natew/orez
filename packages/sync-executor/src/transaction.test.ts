import { createSchema, string, table } from '@rocicorp/zero'
import { describe, expect, test, vi } from 'vitest'

import { createServerTransaction } from './transaction.js'

import type { ApplicationTransaction } from './types.js'

describe('createServerTransaction', () => {
  test('preserves a __proto__ table as an own CRUD property', async () => {
    const protoName = '__proto__' as const
    const schema = createSchema({
      tables: [table(protoName).columns({ id: string() }).primaryKey('id')],
    })
    const exec = vi.fn(async () => ({ changes: 1 }))
    const applicationTx: ApplicationTransaction = {
      exec,
      async query<Row extends Record<string, unknown>>() {
        return [] as readonly Row[]
      },
      async queryAst<Result>(): Promise<Result> {
        throw new Error('queryAst is not used by this fixture')
      },
    }

    const tx = createServerTransaction(schema, applicationTx)
    const mutate = tx.mutate as unknown as Record<
      string,
      { insert(value: { id: string }): Promise<void> }
    >

    expect(Object.getPrototypeOf(tx.mutate)).toBe(Object.prototype)
    expect(Object.hasOwn(tx.mutate, protoName)).toBe(true)
    expect(Object.getOwnPropertyDescriptor(tx.mutate, protoName)?.value).toBe(
      mutate[protoName]
    )

    await mutate[protoName].insert({ id: 'safe' })

    expect(exec).toHaveBeenCalledOnce()
    expect(exec.mock.calls[0]?.[0]).toContain('INSERT INTO "__proto__"')
  })
})
