import Database from '@rocicorp/zero-sqlite3'
import { describe, expect, it } from 'vitest'

import { CompileError, compile } from '../index.js'

const UNSUPPORTED_SQL = [
  ['greatest', 'SELECT GREATEST(a, b) FROM event'],
  ['least', 'SELECT LEAST(a, b) FROM event'],
  [
    'distinct on',
    'SELECT DISTINCT ON (project_id) * FROM deployment ORDER BY project_id, created_at DESC',
  ],
  [
    'lateral subquery',
    `SELECT project.id, latest.id
     FROM project
     LEFT JOIN LATERAL (
       SELECT id FROM deployment
       WHERE deployment.project_id = project.id
       ORDER BY created_at DESC LIMIT 1
     ) latest ON true`,
  ],
  [
    'lateral function',
    'SELECT value FROM event CROSS JOIN LATERAL generate_series(1, 2) AS value',
  ],
] as const

describe('unsupported PostgreSQL syntax', () => {
  it.each(UNSUPPORTED_SQL)('warns for %s', (_name, sql) => {
    const { warnings } = compile(sql)
    expect(warnings).toHaveLength(1)
    expect(warnings[0].message).toMatch(/not supported by pg-to-sqlite/)
  })

  it.each(UNSUPPORTED_SQL)('rejects %s in strict mode', (_name, sql) => {
    expect(() => compile(sql, { strict: true })).toThrow(CompileError)
  })

  it('rejects SQL that the Zero SQLite binding cannot prepare', () => {
    const db = new Database(':memory:')
    const rejectedAtRuntime = [
      'SELECT GREATEST(1, 2)',
      'SELECT LEAST(1, 2)',
      'SELECT DISTINCT ON (id) id FROM (SELECT 1 AS id)',
      `SELECT * FROM (SELECT 1 AS id) base
       LEFT JOIN LATERAL (SELECT base.id) nested ON true`,
      `SELECT * FROM (SELECT 1 AS id) base
       CROSS JOIN LATERAL json_each('[1]')`,
    ]

    try {
      for (const sql of rejectedAtRuntime) {
        const emitted = compile(sql).sql
        expect(() => db.prepare(emitted)).toThrow()
        expect(() => compile(sql, { strict: true })).toThrow(CompileError)
      }
    } finally {
      db.close()
    }
  })

  it('executes the corresponding portable forms with the Zero SQLite binding', () => {
    const db = new Database(':memory:')
    db.exec(`
      CREATE TABLE deployment (project_id text NOT NULL);
      CREATE TABLE event (a integer NOT NULL, b integer NOT NULL);
      INSERT INTO deployment VALUES ('p2'), ('p1'), ('p1');
      INSERT INTO event VALUES (4, 2), (1, 3);
    `)
    const compatible = [
      'SELECT DISTINCT project_id FROM deployment ORDER BY project_id',
      'SELECT base.id FROM (SELECT 1 AS id) base LEFT JOIN (SELECT 1) nested ON true',
      'SELECT min(a) AS min_a, max(b) AS max_b FROM event',
    ]

    try {
      for (const sql of compatible) {
        const result = compile(sql, { strict: true })
        expect(result.warnings).toEqual([])
        expect(() => db.prepare(result.sql).all()).not.toThrow()
      }
    } finally {
      db.close()
    }
  })
})
