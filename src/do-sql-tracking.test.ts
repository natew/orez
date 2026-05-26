import { describe, expect, it } from 'vitest'

import { trackedChangeRow } from './do-sql-tracking.js'

describe('trackedChangeRow', () => {
  it('keeps only table columns and strips internal returning expressions', () => {
    expect(
      trackedChangeRow(
        {
          id: 't1',
          body: 'hello',
          __orez_returning_1: 'HELLO',
          extra: 'client-visible only',
        },
        { rowColumns: ['id', 'body'] }
      )
    ).toEqual({ id: 't1', body: 'hello' })
  })
})
