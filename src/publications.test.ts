import { describe, expect, test } from 'vitest'

import { ensurePublicationHasTables, syncManagedPublications } from './publications.js'

import type { PGlite } from '@electric-sql/pglite'

type QueryStep = {
  match: RegExp
  rows: unknown[]
}

function fakeDb(
  querySteps: QueryStep[],
  onExec: (sql: string) => void | Promise<void>
): PGlite {
  return {
    async query(sql: string) {
      const step = querySteps.shift()
      expect(step, `unexpected query: ${sql}`).toBeDefined()
      expect(sql).toMatch(step!.match)
      return { rows: step!.rows }
    },
    async exec(sql: string) {
      await onExec(sql)
    },
  } as unknown as PGlite
}

function alreadyMemberError(table: string): Error {
  return new Error(`table "public.${table}" is already member of publication "pub"`)
}

describe('publication sync', () => {
  test('syncManagedPublications tolerates tables added after membership snapshot', async () => {
    const altered: string[] = []
    const db = fakeDb(
      [
        {
          match: /FROM pg_tables/,
          rows: [{ tablename: 'foo' }, { tablename: 'bar' }],
        },
        {
          match: /FROM pg_publication_tables/,
          rows: [],
        },
      ],
      (sql) => {
        if (!/ALTER PUBLICATION/.test(sql)) return
        if (sql.includes('"public"."foo"')) throw alreadyMemberError('foo')
        if (sql.includes('"public"."bar"')) altered.push('bar')
      }
    )

    await syncManagedPublications(db, ['pub'], true)

    expect(altered).toEqual(['bar'])
  })

  test('ensurePublicationHasTables tolerates tables added after empty-publication check', async () => {
    const altered: string[] = []
    const db = fakeDb(
      [
        {
          match: /FROM pg_publication_tables/,
          rows: [{ count: '0' }],
        },
        {
          match: /FROM pg_publication WHERE/,
          rows: [{ count: '1' }],
        },
        {
          match: /FROM pg_tables/,
          rows: [{ tablename: 'foo' }, { tablename: 'bar' }],
        },
      ],
      (sql) => {
        if (sql.includes('"public"."foo"')) throw alreadyMemberError('foo')
        if (sql.includes('"public"."bar"')) altered.push('bar')
      }
    )

    await ensurePublicationHasTables(db, ['pub'])

    expect(altered).toEqual(['bar'])
  })
})
