import Database from '@rocicorp/zero-sqlite3'
import { describe, expect, it } from 'vitest'

import { compile } from '../index.js'

/**
 * `col = ANY($1::text[])` is the portable way to write a variable-length IN
 * list against PG: one bind slot, any number of values. Drivers emit it
 * constantly, so passing it through unchanged fails every consumer on real
 * SQLite with "no such function: ANY" — the same failure mode json_agg had.
 *
 * These run the compiled SQL against a real SQLite to prove the rewrite
 * executes, not merely that the text changed shape.
 */

function seeded() {
  const db = new Database(':memory:')
  db.exec(`CREATE TABLE deck (id text PRIMARY KEY, label text NOT NULL)`)
  db.exec(`INSERT INTO deck VALUES ('a','one'),('b','two'),('c','three')`)
  return db
}

/** bind $N -> ?, JSON-encoding whichever params the compiler flagged as arrays. */
function run(db: any, pgSql: string, params: unknown[]) {
  const result = compile(pgSql)
  const arrayParams = new Set(result.arrayParamNumbers)
  const bound = params.map((value, index) =>
    arrayParams.has(index + 1) ? JSON.stringify(value) : value
  )
  return db.prepare(result.sql.replace(/\$(\d+)/g, '?')).all(...bound)
}

describe('array pass: ANY/ALL over an array operand', () => {
  it('= ANY($1::text[]) matches N, 1, and 0 values', () => {
    const db = seeded()
    const sql = `SELECT id FROM deck WHERE id = ANY($1::text[]) ORDER BY id`

    expect(run(db, sql, [['a', 'c']])).toEqual([{ id: 'a' }, { id: 'c' }])
    expect(run(db, sql, [['b']])).toEqual([{ id: 'b' }])
    expect(run(db, sql, [[]])).toEqual([])
    db.close()
  })

  it('flags the array bind slot so callers encode it as JSON', () => {
    expect(
      compile(`SELECT 1 FROM deck WHERE id = ANY($1::text[])`).arrayParamNumbers
    ).toEqual([1])
    expect(
      compile(`SELECT 1 FROM deck WHERE id = ANY($2::uuid[])`).arrayParamNumbers
    ).toEqual([2])
    expect(compile(`SELECT 1 FROM deck WHERE id = 'a'`).arrayParamNumbers).toEqual([])
  })

  it('<> ALL($1::text[]) is the negation of = ANY', () => {
    const db = seeded()
    const sql = `SELECT id FROM deck WHERE id <> ALL($1::text[]) ORDER BY id`

    expect(run(db, sql, [['a', 'c']])).toEqual([{ id: 'b' }])
    expect(run(db, sql, [[]])).toEqual([{ id: 'a' }, { id: 'b' }, { id: 'c' }])
    db.close()
  })

  it('= ANY(ARRAY[...]) with inline literals needs no bind slot', () => {
    const db = seeded()
    const rows = run(
      db,
      `SELECT id FROM deck WHERE id = ANY(ARRAY['a','b']) ORDER BY id`,
      []
    )
    expect(rows).toEqual([{ id: 'a' }, { id: 'b' }])
    db.close()
  })

  it('rewrites ANY in DML, not just SELECT', () => {
    const db = seeded()
    const del = compile(`DELETE FROM deck WHERE id = ANY($1::text[])`)
    db.prepare(del.sql.replace(/\$(\d+)/g, '?')).run(JSON.stringify(['a', 'b']))
    expect(db.prepare('SELECT id FROM deck ORDER BY id').all()).toEqual([{ id: 'c' }])
    db.close()
  })

  it('leaves no ANY/ALL keyword for SQLite to choke on', () => {
    const { sql } = compile(`SELECT 1 FROM deck WHERE id = ANY($1::text[])`)
    expect(sql).not.toMatch(/\bANY\s*\(/i)
    expect(sql).toMatch(/json_each/i)
  })
})
