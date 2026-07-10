// cursor-pull delta primitive: semantics must mirror the reference core's
// delta suite (src/sync-server/sync-server.test.ts) — see the build plan in
// plans/zero-server-rewrite.md. fixtures are PRODUCTION-SHAPED (2026-07-09
// review, blocker 1): schema-qualified log identities like `public.item`,
// internal soot_0_/_orez_ rows, and unmapped tables that must throw.
import { describe, expect, test } from 'vitest'

import { type ChangeLogRow, cursorDiffPatch } from './cursor-pull'

const TABLES = { 'public.item': { clientName: 'item', primaryKey: ['id'] } }

// production internal classifier shape: bookkeeping + private app tables
const skip = (name: string) =>
  name.startsWith('_') ||
  name.startsWith('soot_0.') ||
  name.startsWith('_orez.') ||
  name === 'public.projectSecret'

function store(rows: Record<string, Record<string, unknown>>) {
  return (clientTableName: string, pk: Record<string, unknown>) =>
    clientTableName === 'item' ? rows[String(pk.id)] : undefined
}

const change = (
  op: ChangeLogRow['op'],
  rowData: Record<string, unknown> | null,
  oldData: Record<string, unknown> | null = null,
  tableName = 'public.item'
): ChangeLogRow => ({ tableName, op, rowData, oldData })

describe('cursorDiffPatch', () => {
  test('qualified production log identity maps to the client table name', () => {
    const logged = { id: 'a', v: 1 }
    const live = { id: 'a', v: 2 } // updated again after the logged change
    const patch = cursorDiffPatch({
      changes: [change('INSERT', logged)],
      tables: TABLES,
      skip,
      readRow: store({ a: live }),
    })
    expect(patch).toEqual([{ op: 'put', tableName: 'item', row: live }])
  })

  test('delete dels by the old pk', () => {
    const patch = cursorDiffPatch({
      changes: [change('DELETE', null, { id: 'a', v: 1 })],
      tables: TABLES,
      skip,
      readRow: store({}),
    })
    expect(patch).toEqual([{ op: 'del', tableName: 'item', pk: { id: 'a' } }])
  })

  test('insert then delete collapses to a del; delete then recreate to a put', () => {
    const patch = cursorDiffPatch({
      changes: [
        change('INSERT', { id: 'gone', v: 1 }),
        change('DELETE', null, { id: 'gone', v: 1 }),
        change('DELETE', null, { id: 'back', v: 1 }),
        change('INSERT', { id: 'back', v: 2 }),
      ],
      tables: TABLES,
      skip,
      readRow: store({ back: { id: 'back', v: 2 } }),
    })
    expect(patch).toContainEqual({ op: 'del', tableName: 'item', pk: { id: 'gone' } })
    expect(patch).toContainEqual({
      op: 'put',
      tableName: 'item',
      row: { id: 'back', v: 2 },
    })
    expect(patch).toHaveLength(2)
  })

  test('pk-changing update with old image dels the old pk and puts the new', () => {
    // NOTE: the main pg-proxy tracking path records old_data=null for
    // UPDATEs, so this coverage only holds where old images exist —
    // published-table pks must be immutable in production (module header)
    const patch = cursorDiffPatch({
      changes: [change('UPDATE', { id: 'new', v: 1 }, { id: 'old', v: 1 })],
      tables: TABLES,
      skip,
      readRow: store({ new: { id: 'new', v: 1 } }),
    })
    expect(patch).toContainEqual({ op: 'del', tableName: 'item', pk: { id: 'old' } })
    expect(patch).toContainEqual({
      op: 'put',
      tableName: 'item',
      row: { id: 'new', v: 1 },
    })
    expect(patch).toHaveLength(2)
  })

  test('production UPDATE without old image (old_data=null) still puts the new pk', () => {
    const patch = cursorDiffPatch({
      changes: [change('UPDATE', { id: 'a', v: 2 }, null)],
      tables: TABLES,
      skip,
      readRow: store({ a: { id: 'a', v: 2 } }),
    })
    expect(patch).toEqual([{ op: 'put', tableName: 'item', row: { id: 'a', v: 2 } }])
  })

  test('repeated updates dedup to one put', () => {
    const rows = [1, 2, 3].map((v) =>
      change('UPDATE', { id: 'a', v }, { id: 'a', v: v - 1 })
    )
    const patch = cursorDiffPatch({
      changes: rows,
      tables: TABLES,
      skip,
      readRow: store({ a: { id: 'a', v: 3 } }),
    })
    expect(patch).toEqual([{ op: 'put', tableName: 'item', row: { id: 'a', v: 3 } }])
  })

  test('classified internal tables are skipped', () => {
    const patch = cursorDiffPatch({
      changes: [
        change('INSERT', { k: 'x' }, null, '_orez._zero_watermark'),
        change('UPDATE', { id: 'c' }, { id: 'c' }, 'soot_0.clients'),
        change('INSERT', { id: 's' }, null, 'public.projectSecret'),
      ],
      tables: TABLES,
      skip,
      readRow: () => {
        throw new Error('must not read non-synced tables')
      },
    })
    expect(patch).toEqual([])
  })

  test('an unmapped, unclassified table THROWS instead of silently dropping', () => {
    // the review-reproduced failure mode: bare-vs-qualified mismatch (or a
    // newly published table missing from the spec) must fail loudly
    expect(() =>
      cursorDiffPatch({
        changes: [change('INSERT', { id: 'a' }, null, 'public.newTable')],
        tables: TABLES,
        skip,
        readRow: () => undefined,
      })
    ).toThrowError(/unmapped table 'public\.newTable'/)
    expect(() =>
      cursorDiffPatch({
        changes: [change('INSERT', { id: 'a' }, null, 'item')], // bare name, spec is qualified
        tables: TABLES,
        skip,
        readRow: () => undefined,
      })
    ).toThrowError(/unmapped table 'item'/)
  })

  test('composite primary keys key the dedup correctly', () => {
    const tables = { 'public.pair': { clientName: 'pair', primaryKey: ['a', 'b'] } }
    const patch = cursorDiffPatch({
      changes: [
        change('INSERT', { a: 1, b: 2, v: 'x' }, null, 'public.pair'),
        change('INSERT', { a: 1, b: 3, v: 'y' }, null, 'public.pair'),
        change('UPDATE', { a: 1, b: 2, v: 'z' }, { a: 1, b: 2, v: 'x' }, 'public.pair'),
      ],
      tables,
      skip,
      readRow: (_t, pk) => (pk.b === 2 ? { a: 1, b: 2, v: 'z' } : { a: 1, b: 3, v: 'y' }),
    })
    expect(patch).toHaveLength(2)
    expect(patch).toContainEqual({
      op: 'put',
      tableName: 'pair',
      row: { a: 1, b: 2, v: 'z' },
    })
    expect(patch).toContainEqual({
      op: 'put',
      tableName: 'pair',
      row: { a: 1, b: 3, v: 'y' },
    })
  })
})
