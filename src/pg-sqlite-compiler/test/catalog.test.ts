import Database from '@rocicorp/zero-sqlite3'
/**
 * Catalog pass tests.
 *
 * Validates two things in tandem:
 *   1. The pass rewrites catalog refs to _orez_catalog__* (snapshot-style).
 *   2. The seed creates matching tables that the rewritten query can
 *      actually execute against (semantic equivalence with a real catalog).
 */
import { describe, expect, it } from 'vitest'

import { buildCatalogTables } from '../catalog/seed.js'
import { compile } from '../index.js'

function rewriteParams(sql: string): string {
  return sql.replace(/\$\d+/g, '?')
}

function freshDb(): { db: any; setup: (s: string[]) => void } {
  const db = new Database(':memory:')
  return {
    db,
    setup: (statements: string[]) => {
      for (const s of statements) {
        const { sql } = compile(s)
        db.exec(sql)
      }
    },
  }
}

describe('catalog pass — rewrite', () => {
  it('pg_catalog.pg_class → _orez_catalog__pg_class', () => {
    const { sql } = compile('SELECT relname FROM pg_catalog.pg_class WHERE relkind = $1')
    expect(sql).toMatch(/_orez_catalog__pg_class/)
    expect(sql).not.toMatch(/pg_catalog\./)
  })

  it('bare pg_class (no schema) is NOT rewritten — would hijack user tables', () => {
    // Apps may legitimately have a table called `pg_user`/`pg_views`/etc.
    // Catalog clients always qualify (`pg_catalog.pg_class`), so the bare
    // form is treated as a user table.
    const { sql } = compile('SELECT relname FROM pg_class WHERE relkind = $1')
    expect(sql).not.toMatch(/_orez_catalog/)
    expect(sql).toMatch(/FROM pg_class/i)
  })

  it('information_schema.columns → _orez_catalog__information_schema_columns', () => {
    const { sql } = compile(
      "SELECT column_name FROM information_schema.columns WHERE table_name = 't'"
    )
    expect(sql).toMatch(/_orez_catalog__information_schema_columns/)
    expect(sql).not.toMatch(/information_schema\./)
  })

  it('user table refs (e.g. message) are NOT rewritten', () => {
    const { sql } = compile('SELECT id FROM message WHERE id = $1')
    expect(sql).not.toMatch(/_orez_catalog/)
    expect(sql).toMatch(/FROM message/i)
  })

  it('information_schema.tables → flat name', () => {
    const { sql } = compile(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    )
    expect(sql).toMatch(/_orez_catalog__information_schema_tables/)
  })

  it('schema-qualified pg_publication → flat name', () => {
    const { sql } = compile(
      'SELECT pubname FROM pg_catalog.pg_publication WHERE pubname IN ($1)'
    )
    expect(sql).toMatch(/_orez_catalog__pg_publication/)
  })

  it('INSERT INTO pg_user (bare) is NOT rewritten — user-owned table', () => {
    // Regression guard: earlier iterations rewrote bare PG catalog names,
    // which silently redirected user DML at synthetic catalog tables.
    const { sql } = compile('INSERT INTO pg_user (id) VALUES ($1)')
    expect(sql).not.toMatch(/_orez_catalog/)
  })
})

describe('catalog seed + pass roundtrip — executable', () => {
  it('pg_class returns the user-table rows after seeding', () => {
    const { db, setup } = freshDb()
    setup([
      'CREATE TABLE message (id text PRIMARY KEY, content text)',
      'CREATE TABLE event (id text PRIMARY KEY, ts timestamp)',
    ])
    buildCatalogTables(db)
    const { sql } = compile(`SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'r'`)
    const rows = db.prepare(rewriteParams(sql)).all() as { relname: string }[]
    const names = rows.map((r) => r.relname).sort()
    expect(names).toContain('message')
    expect(names).toContain('event')
    db.close()
  })

  it('pg_attribute returns column info per table', () => {
    const { db, setup } = freshDb()
    setup(['CREATE TABLE message (id text PRIMARY KEY, content text NOT NULL)'])
    buildCatalogTables(db)
    const { sql } = compile(`
      SELECT a.attname, a.attnotnull
      FROM pg_catalog.pg_attribute a
      JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
      WHERE c.relname = 'message'
      ORDER BY a.attnum
    `)
    const rows = db.prepare(rewriteParams(sql)).all() as {
      attname: string
      attnotnull: number
    }[]
    expect(rows.map((r) => r.attname)).toEqual(['id', 'content'])
    // `content text NOT NULL` is reported NOT NULL; SQLite's PRAGMA table_info
    // reports `id text PRIMARY KEY` as NOT NULL only when explicitly written
    // that way (historical SQLite quirk), so we only check `content` here.
    expect(rows[1].attnotnull).toBe(1)
    db.close()
  })

  it('information_schema.columns returns column metadata', () => {
    const { db, setup } = freshDb()
    setup(['CREATE TABLE event (id text PRIMARY KEY, "createdAt" timestamp)'])
    buildCatalogTables(db)
    const { sql } = compile(
      "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'event' ORDER BY ordinal_position"
    )
    const rows = db.prepare(rewriteParams(sql)).all() as {
      column_name: string
      data_type: string
    }[]
    expect(rows.map((r) => r.column_name)).toEqual(['id', 'createdAt'])
    db.close()
  })

  it('pg_namespace contains public + pg_catalog', () => {
    const { db, setup } = freshDb()
    setup([])
    buildCatalogTables(db)
    const { sql } = compile('SELECT nspname FROM pg_catalog.pg_namespace ORDER BY oid')
    const rows = db.prepare(rewriteParams(sql)).all() as { nspname: string }[]
    expect(rows.map((r) => r.nspname)).toEqual([
      'pg_catalog',
      'information_schema',
      'public',
    ])
    db.close()
  })

  it('pg_publication is populated when ZERO_APP_PUBLICATIONS-style names supplied', () => {
    const { db, setup } = freshDb()
    setup(['CREATE TABLE message (id text PRIMARY KEY)'])
    buildCatalogTables(db, { publications: ['orez_zero_public'] })
    const { sql } = compile('SELECT pubname FROM pg_catalog.pg_publication')
    const rows = db.prepare(rewriteParams(sql)).all() as { pubname: string }[]
    expect(rows.map((r) => r.pubname)).toEqual(['orez_zero_public'])

    const { sql: sql2 } = compile(
      "SELECT pubname, tablename FROM pg_catalog.pg_publication_tables WHERE pubname = 'orez_zero_public'"
    )
    const ptRows = db.prepare(rewriteParams(sql2)).all() as {
      pubname: string
      tablename: string
    }[]
    expect(ptRows.length).toBeGreaterThanOrEqual(1)
    expect(ptRows[0].tablename).toBe('message')
    db.close()
  })
})
