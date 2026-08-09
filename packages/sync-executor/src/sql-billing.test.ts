import { describe, expect, it } from 'vitest'

import { trackBillableCursorRows } from './sql-billing.js'

describe('trackBillableCursorRows', () => {
  it('records billing rows that appear only while a cursor is drained', () => {
    const rows = [{ id: 1 }, { id: 2 }, { id: 3 }]
    let index = 0
    const cursor = {
      rowsWritten: 0,
      next() {
        if (index >= rows.length) return { done: true, value: undefined }
        this.rowsWritten += 4
        return { done: false, value: rows[index++] }
      },
      toArray() {
        const out = []
        for (;;) {
          const item = this.next()
          if (item.done) return out
          out.push(item.value)
        }
      },
    }
    const deltas: number[] = []
    const tracked = trackBillableCursorRows(cursor, (delta) => deltas.push(delta))

    expect(tracked.toArray()).toEqual(rows)
    expect(deltas).toEqual([12])
  })

  it('records immediate rows and monotonic deltas from every consumption path', () => {
    const cursor = {
      rowsRead: 1,
      rowsWritten: 2,
      next() {
        this.rowsRead = 3
        this.rowsWritten = 5
        return { done: true, value: undefined }
      },
      raw() {
        let done = false
        return {
          [Symbol.iterator]() {
            return this
          },
          next: () => {
            if (done) return { done: true, value: undefined }
            done = true
            this.rowsRead = 7
            this.rowsWritten = 9
            return { done: false, value: [1] }
          },
        }
      },
    }
    const written: number[] = []
    const read: number[] = []
    const tracked = trackBillableCursorRows(
      cursor,
      (delta) => written.push(delta),
      (delta) => read.push(delta)
    )

    tracked.next()
    expect(Array.from(tracked.raw())).toEqual([[1]])
    expect(written).toEqual([2, 3, 4])
    expect(read).toEqual([1, 2, 4])
  })
})
