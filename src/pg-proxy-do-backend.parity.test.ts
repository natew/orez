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

  // `col = ANY($1::text[])` is how a driver writes a variable-length IN list
  // against PG. It reaches sqlite as json_each(), which only works if the bind
  // slot is re-encoded as JSON — so cover both wire shapes a driver can send
  // (JS array, and the '{a,c}' array literal the text protocol delivers), at 0,
  // 1 and N values. The empty case is the one that silently returns everything
  // if the rewrite degrades to a scalar comparison.
  const ANY_SELECT = `SELECT id FROM deck WHERE id = ANY($1::text[]) ORDER BY id`

  async function seedDeck(): Promise<void> {
    const ddl = `CREATE TABLE deck (id text PRIMARY KEY, label text NOT NULL)`
    const seed = `INSERT INTO deck VALUES ('a','one'), ('b','two'), ('c','three')`
    await pg.query(ddl)
    await backend.exec(ddl)
    await pg.query(seed)
    await backend.exec(seed)
  }

  for (const [shape, values] of [
    ['js array', [['a', 'c'], ['b'], []]],
    ['array literal', ['{a,c}', '{b}', '{}']],
  ] as Array<[string, unknown[]]>) {
    test(`= ANY($1::text[]) matches PG for 0/1/N values (${shape})`, async () => {
      await seedDeck()
      for (const value of values) {
        expectSameRows(
          (await backend.query(ANY_SELECT, [value])).rows,
          (await pg.query(ANY_SELECT, [value])).rows
        )
      }
    })
  }

  test('DELETE ... WHERE NOT (id = ANY($1::text[])) matches PG', async () => {
    await seedDeck()
    const del = `DELETE FROM deck WHERE NOT (id = ANY($1::text[]))`
    await pg.query(del, [['a', 'b']])
    await backend.query(del, [['a', 'b']])
    const read = 'SELECT id FROM deck ORDER BY id'
    expectSameRows((await backend.query(read)).rows, (await pg.query(read)).rows)
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

  // the DO/local-sql store has no FK enforcement (the pipeline strips every FK),
  // so a bare DELETE would orphan children. the fk-cascade wiring restores ON
  // DELETE CASCADE/SET NULL by expansion — assert it matches PG, which cascades
  // natively here (raw PGlite keeps its FKs). exercises both the parameterized
  // (query) and non-parameterized (exec) execute paths, plus a grandchild chain.
  const CASCADE_SCHEMA = [
    `CREATE TABLE thread (id text PRIMARY KEY, title text)`,
    `CREATE TABLE message (id text PRIMARY KEY, "threadId" text REFERENCES thread(id) ON DELETE CASCADE, body text)`,
    `CREATE TABLE reaction (id text PRIMARY KEY, "messageId" text REFERENCES message(id) ON DELETE CASCADE, emoji text)`,
    `CREATE TABLE bookmark (id text PRIMARY KEY, "threadId" text REFERENCES thread(id) ON DELETE SET NULL, note text)`,
  ]
  const CASCADE_SEED = [
    `INSERT INTO thread VALUES ('t1', 'keep'), ('t2', 'doomed')`,
    `INSERT INTO message VALUES ('m1', 't2', 'a'), ('m2', 't2', 'b'), ('m3', 't1', 'c')`,
    `INSERT INTO reaction VALUES ('r1', 'm1', 'x'), ('r2', 'm1', 'y'), ('r3', 'm2', 'z')`,
    `INSERT INTO bookmark VALUES ('b1', 't2', 'doomed'), ('b2', 't1', 'kept')`,
  ]
  const CASCADE_READS = [
    'SELECT id FROM thread ORDER BY id',
    'SELECT id, "threadId" FROM message ORDER BY id',
    'SELECT id FROM reaction ORDER BY id',
    'SELECT id, "threadId" FROM bookmark ORDER BY id',
  ]

  async function seedCascade(): Promise<void> {
    for (const ddl of CASCADE_SCHEMA) {
      await pg.query(ddl)
      await backend.exec(ddl)
    }
    for (const sql of CASCADE_SEED) {
      await pg.query(sql)
      await backend.exec(sql)
    }
  }

  async function expectCascadeParity(): Promise<void> {
    for (const q of CASCADE_READS) {
      expectSameRows((await backend.query(q)).rows, (await pg.query(q)).rows)
    }
  }

  test('ON DELETE CASCADE/SET NULL parity (parameterized delete) matches PG native cascade', async () => {
    await seedCascade()
    await pg.query('DELETE FROM thread WHERE id = $1', ['t2'])
    await backend.query('DELETE FROM thread WHERE id = $1', ['t2'])
    await expectCascadeParity()
    // grandchildren (reaction → message → thread) are gone, m3/b2 (on t1) survive,
    // and b1's link is nulled rather than the row deleted.
    expect((await backend.query('SELECT id FROM reaction')).rows).toHaveLength(0)
    expect((await backend.query('SELECT id FROM message ORDER BY id')).rows).toEqual([
      { id: 'm3' },
    ])
    expect(
      (await backend.query(`SELECT "threadId" FROM bookmark WHERE id = 'b1'`)).rows[0]
        .threadId
    ).toBeNull()
  })

  test('ON DELETE CASCADE/SET NULL parity (non-parameterized delete) matches PG native cascade', async () => {
    await seedCascade()
    await pg.query(`DELETE FROM thread WHERE id = 't2'`)
    await backend.exec(`DELETE FROM thread WHERE id = 't2'`)
    await expectCascadeParity()
  })

  // NOTE: cascade-child CHANGE CAPTURE (so deletions replicate to clients) is
  // not assertable here — the local-sql test backend rejects tracked writes by
  // design (tracking "belongs to the shared upstream db / ZeroSqlDO"). it IS
  // validated two ways: fk-cascade.test.ts proves expanded deletes flow through
  // real change-tracking (PGlite triggers), and the mock-http DO test
  // ("cascade DELETE sends change-tracked child statements …") asserts each
  // cascade child reaches the DO as a RETURNING-tracked write, identical to a
  // normal delete.

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
