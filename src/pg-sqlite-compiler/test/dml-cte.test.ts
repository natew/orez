import Database from '@rocicorp/zero-sqlite3'
/**
 * dml-cte pass: the counted-delete CTE (zero-cache's changeLog purge shape)
 * compiles to a top-level WITH + DELETE … RETURNING count-marker that real
 * SQLite executes; every other data-modifying CTE warns (strict rejects).
 */
import { describe, expect, it } from 'vitest'

import { compile } from '../index.js'
import { foldCountMarkerResult } from '../passes/dml-cte.js'

// byte-for-byte the statement zero 1.6's Storer.purgeRecordsBefore emits
const ZERO_PURGE = `
  -- The backup watermark can be ahead of the durable changeLog if the
  -- storer is behind but the backup replica has consumed forwarded changes.
  WITH keep AS (
    SELECT max(watermark) AS watermark
    FROM "changeLog"
  ), purged AS (
    DELETE FROM "changeLog" WHERE watermark < '03'
      AND watermark < (SELECT watermark FROM keep)
      RETURNING watermark, pos
  ) SELECT COUNT(*) as deleted FROM purged;`

function freshChangeLog(): InstanceType<typeof Database> {
  const db = new Database(':memory:')
  db.exec(
    `CREATE TABLE "changeLog" (watermark TEXT NOT NULL, pos INT NOT NULL, PRIMARY KEY (watermark, pos))`
  )
  db.exec(
    `INSERT INTO "changeLog" (watermark, pos) VALUES ('01',0),('02',0),('03',0),('04',0)`
  )
  return db
}

describe('dml-cte pass', () => {
  it("compiles zero's changeLog purge into a SQLite-native WITH + DELETE", () => {
    const { sql, warnings } = compile(ZERO_PURGE)
    expect(warnings).toEqual([])
    expect(sql).toMatch(/^WITH\s+keep AS/i)
    expect(sql).toMatch(/DELETE FROM "changeLog"/)
    expect(sql).toMatch(/RETURNING 1 AS "?__orez_count__deleted"?/)

    const db = freshChangeLog()
    const rows = db.prepare(sql).all()
    expect(rows).toHaveLength(2) // '01' and '02' purged; keep boundary held
    const folded = foldCountMarkerResult(rows.length, Object.keys(rows[0] ?? {}))
    expect(folded).toEqual({ rows: [{ deleted: 2 }], columns: ['deleted'] })
    const left = db.prepare(`SELECT watermark FROM "changeLog" ORDER BY watermark`).all()
    expect(left.map((r: any) => r.watermark)).toEqual(['03', '04'])
    db.close()
  })

  it('folds zero-row results via the marker in the SQL text', () => {
    const { sql } = compile(ZERO_PURGE.replace(`< '03'`, `< '01'`))
    const db = freshChangeLog()
    const rows = db.prepare(sql).all()
    expect(rows).toHaveLength(0)
    const folded = foldCountMarkerResult(rows.length, sql)
    expect(folded).toEqual({ rows: [{ deleted: 0 }], columns: ['deleted'] })
    db.close()
  })

  it('handles the single-CTE shape (no leading boundary CTE)', () => {
    const { sql, warnings } = compile(
      `WITH purged AS (
         DELETE FROM "changeLog" WHERE watermark < '04' RETURNING watermark
       ) SELECT COUNT(*) AS n FROM purged`
    )
    expect(warnings).toEqual([])
    expect(sql).not.toMatch(/^WITH/i)
    const db = freshChangeLog()
    const rows = db.prepare(sql).all()
    expect(foldCountMarkerResult(rows.length, sql)).toEqual({
      rows: [{ n: 3 }],
      columns: ['n'],
    })
    db.close()
  })

  it('warns on data-modifying CTEs it cannot translate', () => {
    const { warnings } = compile(
      `WITH moved AS (
         DELETE FROM "changeLog" WHERE watermark < '03' RETURNING watermark, pos
       ) INSERT INTO "archive" SELECT * FROM moved`
    )
    expect(warnings.some((w) => w.kind === 'data-modifying-cte')).toBe(true)
  })

  it('leaves plain CTE selects untouched', () => {
    const { sql, warnings } = compile(
      `WITH latest AS (SELECT max(watermark) AS w FROM "changeLog")
       SELECT COUNT(*) AS n FROM latest`
    )
    expect(warnings).toEqual([])
    expect(sql).toMatch(/^WITH\s+latest/i)
    expect(sql).not.toMatch(/__orez_count__/)
  })
})
