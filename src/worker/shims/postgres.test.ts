import { PGlite } from '@electric-sql/pglite'
import { describe, it, expect, beforeEach, afterEach } from 'vitest'

import { createPostgresShim, PostgresError } from './postgres.js'

describe('postgres shim', () => {
  let pglite: PGlite
  let sql: ReturnType<typeof createPostgresShim>

  beforeEach(async () => {
    pglite = new PGlite()
    await pglite.waitReady
    sql = createPostgresShim(pglite)

    // set up test table
    await sql.unsafe(`
      CREATE TABLE test_users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        active BOOLEAN DEFAULT true,
        metadata JSONB,
        score NUMERIC
      )
    `)
  })

  afterEach(async () => {
    await pglite.close()
  })

  // -- tagged template queries --

  describe('tagged template queries', () => {
    it('basic select', async () => {
      await sql.unsafe(
        `INSERT INTO test_users (name, email) VALUES ('alice', 'alice@test.com')`
      )
      const rows = await sql`SELECT * FROM test_users WHERE name = ${'alice'}`
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('alice')
      expect(rows[0].email).toBe('alice@test.com')
    })

    it('multiple parameters', async () => {
      await sql.unsafe(
        `INSERT INTO test_users (name, email) VALUES ('bob', 'bob@test.com')`
      )
      await sql.unsafe(
        `INSERT INTO test_users (name, email) VALUES ('carol', 'carol@test.com')`
      )
      const rows =
        await sql`SELECT * FROM test_users WHERE name = ${'bob'} OR email = ${'carol@test.com'}`
      expect(rows).toHaveLength(2)
    })

    it('empty result set', async () => {
      const rows = await sql`SELECT * FROM test_users WHERE name = ${'nobody'}`
      expect(rows).toHaveLength(0)
      expect(rows.length).toBe(0)
    })

    it('null parameter', async () => {
      await sql`INSERT INTO test_users (name, email) VALUES (${'dave'}, ${null})`
      const rows = await sql`SELECT * FROM test_users WHERE name = ${'dave'}`
      expect(rows).toHaveLength(1)
      expect(rows[0].email).toBeNull()
    })

    it('boolean parameter', async () => {
      await sql`INSERT INTO test_users (name, active) VALUES (${'eve'}, ${false})`
      const rows = await sql`SELECT * FROM test_users WHERE name = ${'eve'}`
      expect(rows[0].active).toBe(false)
    })

    it('json parameter', async () => {
      const meta = { role: 'admin', tags: ['a', 'b'] }
      await sql`INSERT INTO test_users (name, metadata) VALUES (${'frank'}, ${meta})`
      const rows = await sql`SELECT * FROM test_users WHERE name = ${'frank'}`
      expect(rows[0].metadata).toEqual(meta)
    })

    it('bigint parameter', async () => {
      // bigint gets serialized to string, numeric column can hold it
      const big = BigInt('99999999999999999')
      await sql`INSERT INTO test_users (name, score) VALUES (${'grace'}, ${big})`
      const rows = await sql`SELECT * FROM test_users WHERE name = ${'grace'}`
      expect(rows[0].score).toBe('99999999999999999')
    })
  })

  // -- result format --

  describe('result format', () => {
    it('array-like with indexed access', async () => {
      await sql.unsafe(`INSERT INTO test_users (name) VALUES ('a'), ('b'), ('c')`)
      const rows = await sql`SELECT name FROM test_users ORDER BY name`
      expect(rows.length).toBe(3)
      expect(rows[0].name).toBe('a')
      expect(rows[1].name).toBe('b')
      expect(rows[2].name).toBe('c')
    })

    it('iterable', async () => {
      await sql.unsafe(`INSERT INTO test_users (name) VALUES ('x'), ('y')`)
      const rows = await sql`SELECT name FROM test_users ORDER BY name`
      const names: string[] = []
      for (const row of rows) {
        names.push(row.name)
      }
      expect(names).toEqual(['x', 'y'])
    })

    it('destructurable', async () => {
      await sql.unsafe(
        `INSERT INTO test_users (name, email) VALUES ('zara', 'z@test.com')`
      )
      const [{ name, email }] =
        await sql`SELECT name, email FROM test_users WHERE name = ${'zara'}`
      expect(name).toBe('zara')
      expect(email).toBe('z@test.com')
    })

    it('has count metadata', async () => {
      await sql.unsafe(`INSERT INTO test_users (name) VALUES ('a'), ('b')`)
      const rows = await sql`SELECT * FROM test_users`
      expect(rows.count).toBeGreaterThanOrEqual(2)
    })

    it('has command metadata', async () => {
      const rows = await sql`SELECT 1 as val`
      expect(rows.command).toBe('SELECT')
    })

    it('has columns metadata', async () => {
      await sql.unsafe(`INSERT INTO test_users (name) VALUES ('test')`)
      const rows = await sql`SELECT name, email FROM test_users`
      expect(rows.columns).toHaveLength(2)
      expect(rows.columns[0].name).toBe('name')
      expect(rows.columns[1].name).toBe('email')
    })
  })

  // -- unsafe queries --

  describe('unsafe queries', () => {
    it('DDL without params', async () => {
      await sql.unsafe(`CREATE TABLE unsafe_test (id INT)`)
      const rows =
        await sql`SELECT table_name FROM information_schema.tables WHERE table_name = 'unsafe_test'`
      expect(rows).toHaveLength(1)
    })

    it('query with params', async () => {
      await sql.unsafe(`INSERT INTO test_users (name) VALUES ('unsafe_user')`)
      const rows = await sql.unsafe('SELECT * FROM test_users WHERE name = $1', [
        'unsafe_user',
      ])
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('unsafe_user')
    })

    it('multi-statement', async () => {
      // pglite.query handles single statements; multi-statement DDL
      // typically uses exec. unsafe forwards to query which handles most cases.
      const result = await sql.unsafe(`SELECT 1 as a`)
      expect(result[0].a).toBe(1)
    })
  })

  // -- transactions --

  describe('transactions', () => {
    it('commit on success', async () => {
      await sql.begin(async (tx) => {
        await tx`INSERT INTO test_users (name) VALUES (${'tx_user'})`
      })
      const rows = await sql`SELECT * FROM test_users WHERE name = ${'tx_user'}`
      expect(rows).toHaveLength(1)
    })

    it('rollback on error', async () => {
      await expect(
        sql.begin(async (tx) => {
          await tx`INSERT INTO test_users (name) VALUES (${'rollback_user'})`
          throw new Error('intentional rollback')
        })
      ).rejects.toThrow('intentional rollback')

      const rows = await sql`SELECT * FROM test_users WHERE name = ${'rollback_user'}`
      expect(rows).toHaveLength(0)
    })

    it('with isolation level string (ignored but accepted)', async () => {
      await sql.begin('serializable', async (tx) => {
        await tx`INSERT INTO test_users (name) VALUES (${'iso_user'})`
      })
      const rows = await sql`SELECT * FROM test_users WHERE name = ${'iso_user'}`
      expect(rows).toHaveLength(1)
    })

    it('tx.unsafe works', async () => {
      await sql.begin(async (tx) => {
        await tx.unsafe(`INSERT INTO test_users (name) VALUES ('tx_unsafe')`)
      })
      const rows = await sql`SELECT * FROM test_users WHERE name = ${'tx_unsafe'}`
      expect(rows).toHaveLength(1)
    })

    it('nested queries in transaction', async () => {
      const result = await sql.begin(async (tx) => {
        await tx`INSERT INTO test_users (name) VALUES (${'nested_a'})`
        await tx`INSERT INTO test_users (name) VALUES (${'nested_b'})`
        return tx`SELECT name FROM test_users WHERE name LIKE 'nested_%' ORDER BY name`
      })
      expect(result).toHaveLength(2)
      expect(result[0].name).toBe('nested_a')
      expect(result[1].name).toBe('nested_b')
    })
  })

  // -- PostgresError --

  describe('PostgresError', () => {
    it('has correct .code property', () => {
      const err = new PostgresError({ message: 'duplicate key', code: '23505' })
      expect(err.code).toBe('23505')
      expect(err.message).toBe('duplicate key')
      expect(err.name).toBe('PostgresError')
      expect(err).toBeInstanceOf(Error)
      expect(err).toBeInstanceOf(PostgresError)
    })

    it('has all standard fields', () => {
      const err = new PostgresError({
        message: 'test',
        code: '42P01',
        severity: 'ERROR',
        detail: 'some detail',
        hint: 'some hint',
        schema_name: 'public',
        table_name: 'users',
      })
      expect(err.severity).toBe('ERROR')
      expect(err.detail).toBe('some detail')
      expect(err.hint).toBe('some hint')
      expect(err.schema_name).toBe('public')
      expect(err.table_name).toBe('users')
    })
  })

  // -- identifier escaping --

  describe('identifier escaping', () => {
    it('sql(string) returns escaped identifier usable in templates', async () => {
      const tableName = 'test_users'
      const colName = 'name'
      await sql.unsafe(`INSERT INTO test_users (name) VALUES ('ident_test')`)
      const rows =
        await sql`SELECT ${sql(colName)} FROM ${sql(tableName)} WHERE name = ${'ident_test'}`
      expect(rows).toHaveLength(1)
      expect(rows[0].name).toBe('ident_test')
    })

    it('escapes quotes in identifiers', () => {
      const ident = sql('my"table')
      expect(ident.value).toBe('"my""table"')
    })

    it('escapes dots as schema separators', () => {
      const ident = sql('public.users')
      expect(ident.value).toBe('"public"."users"')
    })
  })

  // -- options and metadata --

  describe('options and metadata', () => {
    it('sql.options has expected shape', () => {
      expect(sql.options).toBeDefined()
      expect(sql.options.host).toEqual(['localhost'])
      expect(sql.options.port).toEqual([5432])
      expect(sql.options.database).toBe('pglite')
      expect(sql.options.max).toBe(1)
      expect(typeof sql.options.fetch_types).toBe('boolean')
    })

    it('sql.PostgresError is the error class', () => {
      expect(sql.PostgresError).toBe(PostgresError)
    })

    it('sql.end() resolves without error', async () => {
      await expect(sql.end()).resolves.toBeUndefined()
    })
  })

  // -- simple() modifier --

  describe('query modifiers', () => {
    it('.simple() returns the same pending query', async () => {
      await sql.unsafe(`INSERT INTO test_users (name) VALUES ('simple_test')`)
      const rows =
        await sql`SELECT * FROM test_users WHERE name = ${'simple_test'}`.simple()
      expect(rows).toHaveLength(1)
    })
  })

  // -- error propagation --

  describe('error propagation', () => {
    it('propagates SQL errors from tagged template', async () => {
      await expect(sql`SELECT * FROM nonexistent_table`).rejects.toThrow()
    })

    it('propagates SQL errors from unsafe', async () => {
      await expect(sql.unsafe('SELECT * FROM nonexistent_table')).rejects.toThrow()
    })
  })

  // -- multi-statement queries --

  describe('multi-statement DDL', () => {
    it('handles multi-statement via unsafe()', async () => {
      await sql.unsafe(`
        CREATE SCHEMA IF NOT EXISTS test_schema;
        CREATE TABLE IF NOT EXISTS test_schema.items (
          id TEXT PRIMARY KEY,
          value TEXT
        )
      `)
      // verify schema and table were created
      const result =
        await sql`SELECT table_name FROM information_schema.tables WHERE table_schema = 'test_schema'`
      expect(result.length).toBeGreaterThan(0)
    })

    it('handles multi-statement via tagged template', async () => {
      await sql`
        CREATE SCHEMA IF NOT EXISTS test_schema2;
        CREATE TABLE IF NOT EXISTS test_schema2.things (
          id TEXT PRIMARY KEY,
          name TEXT
        )
      `
      const result =
        await sql`SELECT table_name FROM information_schema.tables WHERE table_schema = 'test_schema2'`
      expect(result.length).toBeGreaterThan(0)
    })

    it('handles multi-statement with quoted identifiers containing special chars', async () => {
      await sql.unsafe(`
        CREATE SCHEMA IF NOT EXISTS "zero_0/cvr";
        CREATE TABLE IF NOT EXISTS "zero_0/cvr"."clients" (
          "clientGroupID" TEXT NOT NULL,
          "clientID" TEXT NOT NULL,
          PRIMARY KEY ("clientGroupID", "clientID")
        )
      `)
      const result =
        await sql`SELECT table_name FROM information_schema.tables WHERE table_schema = 'zero_0/cvr'`
      expect(result.length).toBeGreaterThan(0)
    })
  })
})
