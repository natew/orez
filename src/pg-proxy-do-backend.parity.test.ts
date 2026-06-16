import { PGlite } from '@electric-sql/pglite'
// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { afterEach, beforeEach, describe, expect, test } from 'vitest'

import { DoBackend } from './pg-proxy-do-backend.js'
import { createLocalSqlBackend } from './worker/local-sql-backend.js'

function createSqliteStorage() {
  const nativeDb = new BedrockSqlite.Database(':memory:')
  const exec = (sql: string, ...params: unknown[]) => {
    const stmt = nativeDb.prepare(sql)
    const rows: Array<Record<string, unknown>> = stmt.reader
      ? stmt.all(...params)
      : (stmt.run(...params), [])
    return {
      toArray: () => rows,
      one: () => rows[0],
      columnNames: stmt.reader ? stmt.columns().map((c: any) => c.name) : [],
    }
  }
  return {
    exec,
    close: () => nativeDb.close(),
    transactionSync<T>(fn: () => T): T {
      return nativeDb.transaction(fn)()
    },
  }
}

function expectSameRows(actual: unknown[], expected: unknown[]): void {
  expect(JSON.parse(JSON.stringify(actual))).toEqual(JSON.parse(JSON.stringify(expected)))
}

describe('DoBackend PG/SQLite parity corpus', () => {
  let pg: PGlite
  let storage: ReturnType<typeof createSqliteStorage>
  let backend: DoBackend

  beforeEach(async () => {
    pg = new PGlite()
    await pg.waitReady
    storage = createSqliteStorage()
    const localSql = createLocalSqlBackend(storage)
    backend = new DoBackend('https://orez-do-backend.local', 'postgres', 'parity', {
      fetch: localSql.fetch,
    })
    await backend.waitReady
  })

  afterEach(async () => {
    await pg.close()
    storage.close()
  })

  test('raw query rows match PG value types for bool, jsonb, and timestamptz', async () => {
    const ddl = `
      CREATE TABLE job (
        id text PRIMARY KEY,
        config jsonb NOT NULL,
        "createdAt" timestamptz NOT NULL,
        enabled boolean NOT NULL
      )
    `
    const insert = `
      INSERT INTO job VALUES (
        'j1',
        '{"enabled":true,"limit":3}'::jsonb,
        TIMESTAMPTZ '2026-06-16T00:00:00.000Z',
        true
      )
    `
    const query = `
      SELECT
        EXISTS(SELECT 1 FROM job) AS "hasRows",
        config,
        "createdAt",
        enabled
      FROM job
    `

    await pg.query(ddl)
    await backend.exec(ddl)
    await pg.query(insert)
    await backend.exec(insert)

    const pgRows = (await pg.query(query)).rows
    const doRows = (await backend.query(query)).rows

    expectSameRows(doRows, pgRows)
    expect(typeof doRows[0].hasRows).toBe('boolean')
    expect(doRows[0].config).toEqual({ enabled: true, limit: 3 })
    expect(doRows[0].createdAt).toBeInstanceOf(Date)
  })

  test('schema-qualified upserts match PG after sqlite table flattening', async () => {
    await pg.query('CREATE SCHEMA chat_0')
    await pg.query(
      'CREATE TABLE chat_0.replicas ("slot" text PRIMARY KEY, "version" text NOT NULL)'
    )
    await backend.exec(
      'CREATE TABLE chat_0.replicas ("slot" text PRIMARY KEY, "version" text NOT NULL)'
    )

    const upsert = `
      INSERT INTO chat_0.replicas ("slot", "version")
      VALUES ($1, $2)
      ON CONFLICT ("slot") DO UPDATE SET "version" = excluded."version"
      WHERE replicas."version" IS DISTINCT FROM excluded."version"
    `

    for (const version of ['v1', 'v2']) {
      await pg.query(upsert, ['slot_1', version])
      await backend.query(upsert, ['slot_1', version])
    }

    const pgRows = (
      await pg.query('SELECT "slot", "version" FROM chat_0.replicas ORDER BY "slot"')
    ).rows
    const doRows = (
      await backend.query('SELECT "slot", "version" FROM chat_0.replicas ORDER BY "slot"')
    ).rows

    expectSameRows(doRows, pgRows)
  })

  test('composite ON CONFLICT targets backed by real constraints match PG', async () => {
    const ddl = `
      CREATE TABLE team_member (
        "teamId" text NOT NULL,
        "userId" text NOT NULL,
        role text NOT NULL,
        PRIMARY KEY ("teamId", "userId")
      )
    `
    const upsert = `
      INSERT INTO team_member ("teamId", "userId", role)
      VALUES ($1, $2, $3)
      ON CONFLICT ("teamId", "userId") DO UPDATE SET role = excluded.role
    `

    await pg.query(ddl)
    await backend.exec(ddl)
    for (const role of ['member', 'owner']) {
      await pg.query(upsert, ['t1', 'u1', role])
      await backend.query(upsert, ['t1', 'u1', role])
    }

    const query =
      'SELECT "teamId", "userId", role FROM team_member ORDER BY "teamId", "userId"'
    expectSameRows((await backend.query(query)).rows, (await pg.query(query)).rows)
  })

  test('FOR UPDATE clauses preserve row results while sqlite strips the lock syntax', async () => {
    const ddl = 'CREATE TABLE lock_probe (id text PRIMARY KEY, rank integer NOT NULL)'
    const insert = "INSERT INTO lock_probe VALUES ('a', 1), ('b', 2)"
    const query = 'SELECT id, rank FROM lock_probe ORDER BY rank FOR UPDATE'

    await pg.query(ddl)
    await backend.exec(ddl)
    await pg.query(insert)
    await backend.exec(insert)

    await pg.query('BEGIN')
    await backend.query('BEGIN')
    try {
      const pgRows = (await pg.query(query)).rows
      const doRows = (await backend.query(query)).rows
      expectSameRows(doRows, pgRows)
    } finally {
      await pg.query('ROLLBACK')
      await backend.query('ROLLBACK')
    }
  })
})
