import Database from '@rocicorp/zero-sqlite3'
/**
 * json-functions pass: PG JSON aggregates/constructors rename to their SQLite
 * equivalents (same argument shapes). The z2s relationship-hydration shape —
 * COALESCE(json_agg(json_build_object(...)), '[]') — must execute on real
 * SQLite; ORDER BY/FILTER inside an aggregate warns instead of mistranslating.
 */
import { describe, expect, it } from 'vitest'

import { compile } from '../index.js'

// the shape zero's z2s compiler emits for hydrating a related collection
const Z2S_HYDRATION = `
  SELECT COALESCE(json_agg(json_build_object('id', "zql_root"."id", 'name', "zql_root"."name")), '[]') AS rows
  FROM "project" AS "zql_root" WHERE "zql_root"."ownerId" = 'u1'`

function freshProjects(): InstanceType<typeof Database> {
  const db = new Database(':memory:')
  db.exec(`CREATE TABLE "project" (id TEXT PRIMARY KEY, name TEXT, "ownerId" TEXT)`)
  db.exec(
    `INSERT INTO "project" (id, name, "ownerId") VALUES ('p1','a','u1'),('p2','b','u1'),('p3','c','u2')`
  )
  return db
}

describe('json-functions pass', () => {
  it("compiles z2s's json_agg(json_build_object(...)) hydration to executable SQLite", () => {
    const { sql, warnings } = compile(Z2S_HYDRATION)
    expect(warnings).toEqual([])
    // the deparser quotes function names it doesn't know as pg built-ins;
    // SQLite accepts quoted function names, so match either form.
    expect(sql).toMatch(/json_group_array\("?json_object"?\(/)
    expect(sql).not.toMatch(/json_agg|json_build_object/)

    const db = freshProjects()
    const row = db.prepare(sql).get() as { rows: string }
    expect(JSON.parse(row.rows)).toEqual([
      { id: 'p1', name: 'a' },
      { id: 'p2', name: 'b' },
    ])
  })

  it('maps the jsonb variants and json_object_agg', () => {
    const { sql, warnings } = compile(
      `SELECT jsonb_agg(x.v) AS a, jsonb_object_agg(x.k, x.v) AS o FROM (SELECT 'k' AS k, 1 AS v) AS x`
    )
    expect(warnings).toEqual([])
    expect(sql).toContain('json_group_array(')
    expect(sql).toContain('json_group_object(')

    const db = new Database(':memory:')
    const row = db.prepare(sql).get() as { a: string; o: string }
    expect(JSON.parse(row.a)).toEqual([1])
    expect(JSON.parse(row.o)).toEqual({ k: 1 })
  })

  it('warns instead of mistranslating an aggregate with ORDER BY', () => {
    const { warnings } = compile(
      `SELECT json_agg(v ORDER BY v) FROM (SELECT 1 AS v) AS x`
    )
    expect(warnings.map((w) => w.kind)).toContain('unsupported-function')
  })
})
