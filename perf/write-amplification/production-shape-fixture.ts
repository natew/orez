export const PRODUCTION_SHAPE_TABLE_COUNTS: ReadonlyArray<readonly [string, number]> = [
  ['reaction', 1_853],
  ['server_log', 793],
  ['job', 669],
  ['data', 454],
  ['message', 150],
  ['channel', 104],
  ['server_member', 100],
  ['user_role', 88],
  ...Array.from(
    { length: 43 },
    (_, index) =>
      [`fixture_${String(index + 1).padStart(2, '0')}`, index < 22 ? 11 : 10] as const
  ),
]

export const PRODUCTION_SHAPE_FIXTURE = {
  tables: PRODUCTION_SHAPE_TABLE_COUNTS.length,
  rows: PRODUCTION_SHAPE_TABLE_COUNTS.reduce((sum, [, count]) => sum + count, 0),
  indexes:
    PRODUCTION_SHAPE_TABLE_COUNTS.length + PRODUCTION_SHAPE_TABLE_COUNTS.length * 2 + 23,
}

if (
  JSON.stringify(PRODUCTION_SHAPE_FIXTURE) !==
  JSON.stringify({ tables: 51, rows: 4_663, indexes: 176 })
) {
  throw new Error(
    `production-shape fixture changed: ${JSON.stringify(PRODUCTION_SHAPE_FIXTURE)}`
  )
}

export function productionShapeDDL(): string {
  const statements: string[] = []
  for (const [index, [table]] of PRODUCTION_SHAPE_TABLE_COUNTS.entries()) {
    statements.push(`CREATE TABLE IF NOT EXISTS ${table} (
      id TEXT PRIMARY KEY,
      group_id TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      kind TEXT NOT NULL
    )`)
    statements.push(`CREATE INDEX IF NOT EXISTS ${table}_group_id ON ${table}(group_id)`)
    statements.push(`CREATE INDEX IF NOT EXISTS ${table}_kind ON ${table}(kind)`)
    if (index < 23) {
      statements.push(
        `CREATE INDEX IF NOT EXISTS ${table}_created_at ON ${table}(created_at)`
      )
    }
  }
  return statements.join(';\n')
}

export function productionShapeZeroSchemaSource(): string {
  const tables = Object.fromEntries(
    PRODUCTION_SHAPE_TABLE_COUNTS.map(([table]) => [
      table,
      {
        name: table,
        columns: {
          id: { type: 'string', optional: false, customType: null },
          group_id: { type: 'string', optional: false, customType: null },
          created_at: { type: 'number', optional: false, customType: null },
          kind: { type: 'string', optional: false, customType: null },
        },
        primaryKey: ['id'],
      },
    ])
  )
  return `export const schema = ${JSON.stringify({ tables, relationships: {} })}\n`
}
