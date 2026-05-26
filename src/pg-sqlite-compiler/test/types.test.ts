import Database from '@rocicorp/zero-sqlite3'
/**
 * Snapshot tests for the types pass. Each case asserts that the emitted
 * SQLite SQL has the expected normalized type name AND that the result
 * actually executes against a fresh @rocicorp/zero-sqlite3 in-memory db.
 */
import { describe, expect, it } from 'vitest'

import { compile } from '../index.js'

function compilesAndRuns(pgSql: string): { sql: string } {
  const { sql, warnings } = compile(pgSql)
  expect(warnings).toEqual([])
  const db = new Database(':memory:')
  db.exec(sql)
  db.close()
  return { sql }
}

describe('types pass', () => {
  it('BIGSERIAL → INTEGER (rowid alias on PRIMARY KEY)', () => {
    const { sql } = compilesAndRuns('CREATE TABLE t (id BIGSERIAL PRIMARY KEY)')
    expect(sql).toMatch(/INTEGER PRIMARY KEY/i)
    expect(sql).not.toMatch(/bigserial/i)
  })

  it('jsonb → TEXT', () => {
    const { sql } = compilesAndRuns(
      'CREATE TABLE t (id text PRIMARY KEY, p jsonb NOT NULL)'
    )
    expect(sql).toMatch(/p TEXT NOT NULL/i)
    expect(sql).not.toMatch(/jsonb/i)
  })

  it('text[] → TEXT (arrays as JSON text)', () => {
    const { sql } = compilesAndRuns(
      'CREATE TABLE t (id text PRIMARY KEY, tags text[] NOT NULL)'
    )
    expect(sql).toMatch(/tags TEXT NOT NULL/i)
    expect(sql).not.toMatch(/\[/)
  })

  it('timestamp with time zone → TEXT', () => {
    const { sql } = compilesAndRuns(
      'CREATE TABLE t (id text PRIMARY KEY, ts timestamp with time zone NOT NULL)'
    )
    expect(sql).toMatch(/ts TEXT NOT NULL/i)
    expect(sql).not.toMatch(/timestamp/i)
    expect(sql).not.toMatch(/with time zone/i)
  })

  it('varchar(N) → TEXT (drop length typmod)', () => {
    const { sql } = compilesAndRuns(
      'CREATE TABLE t (id text PRIMARY KEY, val varchar(64))'
    )
    expect(sql).toMatch(/val TEXT/i)
    expect(sql).not.toMatch(/varchar/i)
    expect(sql).not.toMatch(/\(64\)/)
  })

  it('boolean → INTEGER', () => {
    const { sql } = compilesAndRuns(
      'CREATE TABLE t (id text PRIMARY KEY, enabled boolean NOT NULL DEFAULT false)'
    )
    expect(sql).toMatch(/enabled INTEGER NOT NULL/i)
    expect(sql).not.toMatch(/boolean/i)
  })

  it('uuid → TEXT', () => {
    const { sql } = compilesAndRuns('CREATE TABLE t (id uuid PRIMARY KEY)')
    expect(sql).toMatch(/id TEXT PRIMARY KEY/i)
    expect(sql).not.toMatch(/uuid/i)
  })

  it('bytea → BLOB', () => {
    const { sql } = compilesAndRuns('CREATE TABLE t (id text PRIMARY KEY, body bytea)')
    expect(sql).toMatch(/body BLOB/i)
    expect(sql).not.toMatch(/bytea/i)
  })

  it('numeric(10,2) → NUMERIC (drops precision)', () => {
    const { sql } = compilesAndRuns(
      'CREATE TABLE t (id text PRIMARY KEY, amount numeric(10,2))'
    )
    expect(sql).toMatch(/amount NUMERIC/i)
    expect(sql).not.toMatch(/\(10/)
  })

  it('composite: all common chat-app types in one table', () => {
    const { sql } = compilesAndRuns(
      'CREATE TABLE event (' +
        'id BIGSERIAL PRIMARY KEY, ' +
        'user_id uuid NOT NULL, ' +
        '"createdAt" timestamp with time zone DEFAULT NOW() NOT NULL, ' +
        "payload jsonb NOT NULL DEFAULT '{}', " +
        "tags text[] NOT NULL DEFAULT '{}', " +
        'amount numeric(10,2), ' +
        'enabled boolean NOT NULL DEFAULT true' +
        ')'
    )
    expect(sql).toMatch(/INTEGER PRIMARY KEY/i)
    expect(sql).toMatch(/user_id TEXT NOT NULL/i)
    expect(sql).toMatch(/"createdAt" TEXT DEFAULT CURRENT_TIMESTAMP NOT NULL/i)
    expect(sql).toMatch(/payload TEXT NOT NULL/i)
    expect(sql).toMatch(/tags TEXT NOT NULL/i)
    expect(sql).toMatch(/amount NUMERIC/i)
    expect(sql).toMatch(/enabled INTEGER NOT NULL/i)
  })
})
