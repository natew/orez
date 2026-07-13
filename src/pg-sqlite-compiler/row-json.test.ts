import Database from '@rocicorp/zero-sqlite3'
/**
 * row_to_json rewrite executes the COMPILED SQLite against a real
 * @rocicorp/zero-sqlite3 instance, not just text comparison. The rewrite must
 * only ever reference columns that genuinely exist in the subquery output; it
 * must never invent a name a `SELECT *` or an unnamed expression would not
 * actually produce.
 */
import { describe, expect, it } from 'vitest'

import { CompileError, compile } from './index.js'

import type { SchemaInfo } from './index.js'

function schemaWith(tables: Record<string, string[]>): SchemaInfo {
  return {
    getColumnType: () => undefined,
    getEnum: () => undefined,
    isEnumValue: () => false,
    getTableColumns: (_schema, table) => tables[table],
  }
}

/** compile `pgSql`, run it against `setup`-seeded sqlite, return parsed json. */
function runRowJson(
  pgSql: string,
  setup: string,
  opts?: Parameters<typeof compile>[1]
): { result: any; warnings: ReturnType<typeof compile>['warnings']; sql: string } {
  const { sql, warnings } = compile(pgSql, opts)
  const db = new Database(':memory:')
  try {
    db.exec(setup)
    const row = db.prepare(sql).get() as { zql_result: string }
    return { result: JSON.parse(row.zql_result), warnings, sql }
  } finally {
    db.close()
  }
}

describe('row_to_json → json_object executes correctly', () => {
  it('rewrites the Zero-style aliased subquery and returns real values', () => {
    const { result, warnings, sql } = runRowJson(
      `SELECT row_to_json(zql_root) AS zql_result
       FROM (
         SELECT s.id AS id, s."userId" AS "userId"
         FROM "sootSession" AS s
       ) AS zql_root`,
      `CREATE TABLE "sootSession" (id TEXT PRIMARY KEY, "userId" TEXT);
       INSERT INTO "sootSession" (id, "userId") VALUES ('s1', 'u1');`
    )
    expect(warnings).toEqual([])
    expect(sql).not.toMatch(/row_to_json/i)
    expect(result).toEqual({ id: 's1', userId: 'u1' })
  })

  it('aliases an unnamed expression instead of referencing an invented name', () => {
    const { result, warnings, sql } = runRowJson(
      `SELECT row_to_json(zql_root) AS zql_result
       FROM (
         SELECT amount + 1, id
         FROM ledger
       ) AS zql_root`,
      `CREATE TABLE ledger (id TEXT, amount INTEGER);
       INSERT INTO ledger (id, amount) VALUES ('l1', 41);`
    )
    expect(warnings).toEqual([])
    // the subquery must gain a real alias for the unnamed expression so the
    // outer reference resolves at runtime
    expect(sql).toMatch(/AS\s+_orez_col_1/i)
    expect(result).toEqual({ _orez_col_1: 42, id: 'l1' })
  })

  it('executes the original no-FROM unnamed-expression reproduction', () => {
    const { result, warnings } = runRowJson(
      `SELECT row_to_json(z) AS zql_result FROM (SELECT 1 + 1) AS z`,
      ''
    )
    expect(warnings).toEqual([])
    expect(result).toEqual({ _orez_col_1: 2 })
  })

  it('generates an alias that does not collide with a real output name', () => {
    const { result, warnings, sql } = runRowJson(
      `SELECT row_to_json(z) AS zql_result
       FROM (SELECT 1 + 1, 3 AS _orez_col_1) AS z`,
      ''
    )
    expect(warnings).toEqual([])
    expect(sql).toMatch(/AS\s+_orez_col_2/i)
    expect(result).toEqual({ _orez_col_1: 3, _orez_col_2: 2 })
  })

  it('uses the real column name for an unaliased column reference', () => {
    const { result, warnings } = runRowJson(
      `SELECT row_to_json(zql_root) AS zql_result
       FROM (SELECT t.id, t.label FROM widget AS t) AS zql_root`,
      `CREATE TABLE widget (id TEXT, label TEXT);
       INSERT INTO widget (id, label) VALUES ('w1', 'hello');`
    )
    expect(warnings).toEqual([])
    expect(result).toEqual({ id: 'w1', label: 'hello' })
  })

  it('expands a bare SELECT * from the authoritative schema shape', () => {
    const { result, warnings, sql } = runRowJson(
      `SELECT row_to_json(zql_root) AS zql_result
       FROM (SELECT * FROM widget) AS zql_root`,
      `CREATE TABLE widget (id TEXT, label TEXT);
       INSERT INTO widget (id, label) VALUES ('w1', 'hello');`,
      { schema: schemaWith({ widget: ['id', 'label'] }) }
    )
    expect(warnings).toEqual([])
    expect(sql).not.toMatch(/row_to_json/i)
    expect(sql).toContain(`zql_root.id`)
    expect(sql).toContain(`zql_root.label`)
    expect(result).toEqual({ id: 'w1', label: 'hello' })
  })

  it('expands a qualified t.* from the authoritative schema shape', () => {
    const { result, warnings } = runRowJson(
      `SELECT row_to_json(zql_root) AS zql_result
       FROM (SELECT w.* FROM widget AS w) AS zql_root`,
      `CREATE TABLE widget (id TEXT, label TEXT);
       INSERT INTO widget (id, label) VALUES ('w1', 'hello');`,
      { schema: schemaWith({ widget: ['id', 'label'] }) }
    )
    expect(warnings).toEqual([])
    expect(result).toEqual({ id: 'w1', label: 'hello' })
  })

  it('expands a qualified t.* even when other FROM sources are present', () => {
    // qualified `w.*` names exactly one table, so it stays resolvable regardless
    // of what else sits in the FROM clause.
    const { result, warnings } = runRowJson(
      `SELECT row_to_json(zql_root) AS zql_result
       FROM (
         SELECT w.* FROM widget AS w, (SELECT 7 AS extra) AS q
       ) AS zql_root`,
      `CREATE TABLE widget (id TEXT, label TEXT);
       INSERT INTO widget (id, label) VALUES ('w1', 'hello');`,
      { schema: schemaWith({ widget: ['id', 'label'] }) }
    )
    expect(warnings).toEqual([])
    expect(result).toEqual({ id: 'w1', label: 'hello' })
  })
})

describe('row_to_json declines rather than inventing names', () => {
  it('warns and leaves row_to_json intact for SELECT * without schema info', () => {
    const { sql, warnings } = compile(
      `SELECT row_to_json(zql_root) AS zql_result
       FROM (SELECT * FROM widget) AS zql_root`
    )
    expect(warnings.some((w) => w.kind === 'row-json-unsupported')).toBe(true)
    // declined: no invented _orez_col_N references, row_to_json is left as-is
    expect(sql).toMatch(/row_to_json/i)
    expect(sql).not.toMatch(/_orez_col_/i)
  })

  it('declines a bare SELECT * when the FROM clause has more than one source', () => {
    // widget=[id] resolves, but the extra sub-select `q` also contributes a
    // column. A single-table expansion would silently drop q.extra, so a bare
    // `*` over a multi-source FROM must decline instead of guessing.
    const { sql, warnings } = compile(
      `SELECT row_to_json(z) AS zql_result
       FROM (SELECT * FROM widget, (SELECT 7 AS extra) q) AS z`,
      { schema: schemaWith({ widget: ['id'] }) }
    )
    expect(warnings.some((w) => w.kind === 'row-json-unsupported')).toBe(true)
    expect(sql).toMatch(/row_to_json/i)
    expect(sql).not.toMatch(/json_object/i)
  })

  it('throws in strict mode when a SELECT * cannot be resolved', () => {
    expect(() =>
      compile(
        `SELECT row_to_json(zql_root) AS zql_result
         FROM (SELECT * FROM widget) AS zql_root`,
        { strict: true }
      )
    ).toThrow(CompileError)
  })

  const duplicateShapeCases: Array<[string, string]> = [
    [
      'two columns resolving to the same name',
      `SELECT row_to_json(z) AS zql_result FROM (SELECT a.id, b.id FROM a, b) AS z`,
    ],
    [
      'duplicate explicit aliases',
      `SELECT row_to_json(z) AS zql_result FROM (SELECT a AS dup, b AS dup FROM widget) AS z`,
    ],
    [
      'names differing only in case',
      `SELECT row_to_json(z) AS zql_result FROM (SELECT id, "ID" FROM widget) AS z`,
    ],
  ]

  for (const [label, pgSql] of duplicateShapeCases) {
    it(`declines when output names are not unique: ${label}`, () => {
      const { sql, warnings } = compile(pgSql)
      expect(warnings.some((w) => w.kind === 'row-json-unsupported')).toBe(true)
      expect(sql).toMatch(/row_to_json/i)
      expect(sql).not.toMatch(/json_object/i)
    })

    it(`throws in strict mode for non-unique output names: ${label}`, () => {
      expect(() => compile(pgSql, { strict: true })).toThrow(CompileError)
    })
  }

  it('declines a star-plus-explicit collision using the authoritative shape', () => {
    // `*` expands to [id, label]; the explicit `id` collides with the star's id.
    const { sql, warnings } = compile(
      `SELECT row_to_json(z) AS zql_result FROM (SELECT *, id FROM widget) AS z`,
      { schema: schemaWith({ widget: ['id', 'label'] }) }
    )
    expect(warnings.some((w) => w.kind === 'row-json-unsupported')).toBe(true)
    expect(sql).not.toMatch(/json_object/i)
  })

  it('demonstrates the runtime data loss that the decline prevents', () => {
    const db = new Database(':memory:')
    try {
      db.exec(`CREATE TABLE a (id TEXT); INSERT INTO a (id) VALUES ('a-id');
               CREATE TABLE b (id TEXT); INSERT INTO b (id) VALUES ('b-id');`)
      // the naive rewrite the compiler must NOT emit: both refs hit the first id
      const naive = `SELECT json_object('id', z.id, 'id', z.id) AS zql_result
                     FROM (SELECT a.id, b.id FROM a, b) AS z`
      const lossy = JSON.parse(
        (db.prepare(naive).get() as { zql_result: string }).zql_result
      )
      // b.id is silently gone: only one 'id' survives, and it is a's
      expect(lossy).toEqual({ id: 'a-id' })
    } finally {
      db.close()
    }

    // the compiler declines instead of emitting that lossy shape
    const { sql, warnings } = compile(
      `SELECT row_to_json(z) AS zql_result FROM (SELECT a.id, b.id FROM a, b) AS z`
    )
    expect(warnings.some((w) => w.kind === 'row-json-unsupported')).toBe(true)
    expect(sql).not.toMatch(/json_object/i)
  })
})
