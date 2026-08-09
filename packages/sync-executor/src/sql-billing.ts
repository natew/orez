export type BillableCursor = {
  rowsRead?: number
  rowsWritten?: number
  next?: (...args: unknown[]) => unknown
  one?: (...args: unknown[]) => unknown
  toArray?: (...args: unknown[]) => unknown
  raw?: (...args: unknown[]) => unknown
  [Symbol.iterator]?: () => Iterator<unknown>
}

/**
 * Account a cursor's final billing rows as it is consumed.
 *
 * Cloudflare's `rowsRead` and `rowsWritten` can increase during cursor
 * iteration (notably for `... RETURNING` statements). Sampling only when
 * `sql.exec()` returns misses that work. This proxy records monotonic deltas
 * after every consumption method while preserving the cursor's native `this`
 * binding.
 */
export function trackBillableCursorRows<Cursor extends BillableCursor>(
  cursor: Cursor,
  recordWritten: (rows: number) => void,
  recordRead: (rows: number) => void = () => {}
): Cursor {
  if (!cursor || typeof cursor !== 'object') return cursor
  let accountedWritten = 0
  let accountedRead = 0

  const account = () => {
    const written = Number(cursor.rowsWritten ?? 0)
    if (Number.isSafeInteger(written) && written > accountedWritten) {
      recordWritten(written - accountedWritten)
      accountedWritten = written
    }
    const read = Number(cursor.rowsRead ?? 0)
    if (Number.isSafeInteger(read) && read > accountedRead) {
      recordRead(read - accountedRead)
      accountedRead = read
    }
  }

  const wrapIterator = (iterator: Iterator<unknown>): IterableIterator<unknown> => {
    const next = iterator.next.bind(iterator)
    return {
      next(...args: [] | [unknown]) {
        try {
          return next(...args)
        } finally {
          account()
        }
      },
      [Symbol.iterator]() {
        return this
      },
    }
  }

  let proxy: Cursor
  proxy = new Proxy(cursor, {
    get(target, property) {
      const value = Reflect.get(target, property, target)
      if (property === Symbol.iterator) return () => proxy as unknown as Iterator<unknown>
      if (
        (property === 'next' || property === 'one' || property === 'toArray') &&
        typeof value === 'function'
      ) {
        return (...args: unknown[]) => {
          try {
            return Reflect.apply(value, target, args)
          } finally {
            account()
          }
        }
      }
      if (property === 'raw' && typeof value === 'function') {
        return (...args: unknown[]) =>
          wrapIterator(Reflect.apply(value, target, args) as Iterator<unknown>)
      }
      return typeof value === 'function' ? value.bind(target) : value
    },
  })
  account()
  return proxy
}
