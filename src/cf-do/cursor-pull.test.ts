// cursor-pull delta primitive: semantics must mirror the reference core's
// delta suite (src/sync-server/sync-server.test.ts) — see the build plan in
// plans/zero-server-rewrite.md.
import { describe, expect, test } from 'vitest'

import { type ChangeLogRow, cursorDiffPatch } from './cursor-pull'

const TABLES = { item: { primaryKey: ['id'] } }

function store(rows: Record<string, Record<string, unknown>>) {
  return (tableName: string, pk: Record<string, unknown>) =>
    tableName === 'item' ? rows[String(pk.id)] : undefined
}

const change = (
  op: ChangeLogRow['op'],
  rowData: Record<string, unknown> | null,
  oldData: Record<string, unknown> | null = null
): ChangeLogRow => ({ tableName: 'item', op, rowData, oldData })

describe('cursorDiffPatch', () => {
  test('insert puts the live row, not the logged image', () => {
    const logged = { id: 'a', v: 1 }
    const live = { id: 'a', v: 2 } // updated again after the logged change
    const patch = cursorDiffPatch({
      changes: [change('INSERT', logged)],
      tables: TABLES,
      readRow: store({ a: live }),
    })
    expect(patch).toEqual([{ op: 'put', tableName: 'item', row: live }])
  })

  test('delete dels by the old pk', () => {
    const patch = cursorDiffPatch({
      changes: [change('DELETE', null, { id: 'a', v: 1 })],
      tables: TABLES,
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

  test('pk-changing update dels the old pk and puts the new', () => {
    const patch = cursorDiffPatch({
      changes: [change('UPDATE', { id: 'new', v: 1 }, { id: 'old', v: 1 })],
      tables: TABLES,
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

  test('repeated updates dedup to one put', () => {
    const rows = [1, 2, 3].map((v) =>
      change('UPDATE', { id: 'a', v }, { id: 'a', v: v - 1 })
    )
    const patch = cursorDiffPatch({
      changes: rows,
      tables: TABLES,
      readRow: store({ a: { id: 'a', v: 3 } }),
    })
    expect(patch).toEqual([{ op: 'put', tableName: 'item', row: { id: 'a', v: 3 } }])
  })

  test('non-synced tables are skipped', () => {
    const patch = cursorDiffPatch({
      changes: [
        {
          tableName: '_orez_pg_metadata',
          op: 'INSERT',
          rowData: { k: 'x' },
          oldData: null,
        },
        {
          tableName: 'soot_0_clients',
          op: 'UPDATE',
          rowData: { id: 'c' },
          oldData: { id: 'c' },
        },
      ],
      tables: TABLES,
      readRow: () => {
        throw new Error('must not read non-synced tables')
      },
    })
    expect(patch).toEqual([])
  })

  test('composite primary keys key the dedup correctly', () => {
    const tables = { pair: { primaryKey: ['a', 'b'] } }
    const patch = cursorDiffPatch({
      changes: [
        {
          tableName: 'pair',
          op: 'INSERT',
          rowData: { a: 1, b: 2, v: 'x' },
          oldData: null,
        },
        {
          tableName: 'pair',
          op: 'INSERT',
          rowData: { a: 1, b: 3, v: 'y' },
          oldData: null,
        },
        {
          tableName: 'pair',
          op: 'UPDATE',
          rowData: { a: 1, b: 2, v: 'z' },
          oldData: { a: 1, b: 2, v: 'x' },
        },
      ],
      tables,
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
