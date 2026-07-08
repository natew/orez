// zero-cache purges its CDB changeLog with a data-modifying CTE (zero 1.6:
// a leading `keep` catchup-boundary CTE + a `purged AS (DELETE … RETURNING …)`
// CTE + `SELECT COUNT(*)`). SQLite rejects a DELETE inside a CTE, and before
// deleteReturningCountCTE learned the multi-CTE shape every purge tick 500'd
// on the CF DO backend — the changeLog grew forever and the retained change
// set re-streamed on every embed boot (the 2026-07 rows-written burn, ~$118
// over two days). this runs zero's EXACT purge SQL end-to-end against a real
// SQLite through the DoBackend.

// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { DoBackend } from './pg-proxy-do-backend.js'
import { createLocalSqlBackend } from './worker/local-sql-backend.js'

// byte-for-byte the statement zero 1.6's Storer.purgeRecordsBefore emits
// (storer.js), with #cdc("changeLog") rendered the way zero's sql tag does.
const ZERO_PURGE_SQL = (watermark: string) => `
        -- The backup watermark can be ahead of the durable changeLog if the
        -- storer is behind but the backup replica has consumed forwarded
        -- changes. Preserve the latest durable changeLog transaction as the
        -- catchup boundary instead of assuming the backup watermark exists.
        -- The storer inserts each changeLog transaction atomically, so any
        -- durable row for a watermark implies the full transaction is durable.
        WITH keep AS (
          SELECT max(watermark) AS watermark
          FROM "cdb_0/cdc"."changeLog"
        ), purged AS (
          DELETE FROM "cdb_0/cdc"."changeLog" WHERE watermark < '${watermark}'
            AND watermark < (SELECT watermark FROM keep)
            RETURNING watermark, pos
        ) SELECT COUNT(*) as deleted FROM purged;`

describe('zero cdb changeLog purge on the DO SQLite backend', () => {
  let storage: { close: () => void }
  let backend: DoBackend

  beforeEach(async () => {
    const nativeDb = new BedrockSqlite.Database(':memory:')
    const sqlite = {
      exec: (sql: string, ...params: unknown[]) => {
        const stmt = nativeDb.prepare(sql)
        const rows: Array<Record<string, unknown>> = stmt.reader
          ? stmt.all(...params)
          : (stmt.run(...params), [])
        return {
          toArray: () => rows,
          one: () => rows[0],
          columnNames: stmt.reader ? stmt.columns().map((c: any) => c.name) : [],
        }
      },
      close: () => nativeDb.close(),
      transactionSync<T>(fn: () => T): T {
        return nativeDb.transaction(fn)()
      },
    }
    storage = sqlite
    const localSql = createLocalSqlBackend(sqlite)
    backend = new DoBackend('https://orez-do-backend.local', 'zero_cdb', 'cdb-purge', {
      fetch: localSql.fetch,
    })
    await backend.waitReady
  })

  afterEach(() => {
    storage.close()
  })

  const ddl = `CREATE TABLE "cdb_0/cdc"."changeLog" (watermark text NOT NULL, pos int NOT NULL, change text, PRIMARY KEY (watermark, pos))`
  const seed = `INSERT INTO "cdb_0/cdc"."changeLog" (watermark, pos, change) VALUES
    ('01', 0, 'a'), ('02', 0, 'b'), ('03', 0, 'c'), ('04', 0, 'd')`

  it("executes zero's exact purge CTE, deletes below the boundary, and returns the count", async () => {
    await backend.exec(ddl)
    await backend.exec(seed)
    const result = await backend.query<{ deleted: unknown }>(ZERO_PURGE_SQL('03'))
    expect(Number(result.rows[0]?.deleted)).toBe(2)
    const remaining = await backend.query<{ watermark: string }>(
      `SELECT watermark FROM "cdb_0/cdc"."changeLog" ORDER BY watermark`
    )
    expect(remaining.rows.map((r) => r.watermark)).toEqual(['03', '04'])
  })

  it('preserves the keep boundary: never deletes past max(watermark)', async () => {
    await backend.exec(ddl)
    await backend.exec(seed)
    // purge target beyond the newest row — the keep CTE must hold '04'
    const result = await backend.query<{ deleted: unknown }>(ZERO_PURGE_SQL('99'))
    expect(Number(result.rows[0]?.deleted)).toBe(3)
    const remaining = await backend.query<{ watermark: string }>(
      `SELECT watermark FROM "cdb_0/cdc"."changeLog"`
    )
    expect(remaining.rows.map((r) => r.watermark)).toEqual(['04'])
  })

  it('purges nothing on a single-row log (keep boundary is the only row)', async () => {
    await backend.exec(ddl)
    await backend.exec(
      `INSERT INTO "cdb_0/cdc"."changeLog" (watermark, pos, change) VALUES ('09', 0, 'z')`
    )
    const result = await backend.query<{ deleted: unknown }>(ZERO_PURGE_SQL('99'))
    expect(Number(result.rows[0]?.deleted)).toBe(0)
  })
})
