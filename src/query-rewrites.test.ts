import { describe, expect, test } from 'vitest'

import { rewritePgColumnSizeTotalBytesQuery } from './query-rewrites.js'

describe('rewritePgColumnSizeTotalBytesQuery', () => {
  test('rewrites zero totalBytes pg_column_size sums into scalar subselects', () => {
    const query =
      'SELECT (SUM(COALESCE(pg_column_size("id"), 0)) + SUM(COALESCE(pg_column_size("parts"), 0)) + SUM(COALESCE(pg_column_size("threadId"), 0))) AS "totalBytes" FROM "public"."message" '

    expect(rewritePgColumnSizeTotalBytesQuery(query)).toBe(
      'SELECT (SELECT SUM(COALESCE(pg_column_size("id"), 0)) FROM "public"."message") + (SELECT SUM(COALESCE(pg_column_size("parts"), 0)) FROM "public"."message") + (SELECT SUM(COALESCE(pg_column_size("threadId"), 0)) FROM "public"."message") AS "totalBytes"'
    )
  })

  test('preserves row filters on every scalar subselect', () => {
    const query =
      'SELECT (SUM(COALESCE(pg_column_size("id"), 0)) + SUM(COALESCE(pg_column_size("parts"), 0))) AS "totalBytes" FROM "public"."message" WHERE "projectId" = \'proj_1\' OR "role" = \'user\';'

    expect(rewritePgColumnSizeTotalBytesQuery(query)).toBe(
      'SELECT (SELECT SUM(COALESCE(pg_column_size("id"), 0)) FROM "public"."message" WHERE "projectId" = \'proj_1\' OR "role" = \'user\') + (SELECT SUM(COALESCE(pg_column_size("parts"), 0)) FROM "public"."message" WHERE "projectId" = \'proj_1\' OR "role" = \'user\') AS "totalBytes"'
    )
  })

  test('leaves non-matching SQL unchanged', () => {
    const query =
      'SELECT (SUM(COALESCE(pg_column_size("id"), 0)) + count(*)) AS "totalBytes" FROM "public"."message"'

    expect(rewritePgColumnSizeTotalBytesQuery(query)).toBe(query)
  })
})
