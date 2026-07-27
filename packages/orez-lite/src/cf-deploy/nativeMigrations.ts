import { existsSync, readFileSync, readdirSync } from 'node:fs'
import { join } from 'node:path'

export type NativeSqlMigrationStatement = {
  id: string
  sql: string
  declaredColumns?: Array<{ definition: string; name: string }>
  rebuildColumns?: string[]
  rebuildTarget?: string
  skipIfColumnExists?: { column: string; table: string }
  skipIfColumnMissing?: { column: string; table: string }
  skipIfTableMissing?: string
  supersedes?: string[]
}

type ParsedNativeSqlMigrationStatement = Omit<
  NativeSqlMigrationStatement,
  'id' | 'rebuildColumns' | 'rebuildTarget' | 'supersedes' | 'skipIfTableMissing'
>

// the column names declared by a rebuild block's `__new_<t>` CREATE. this is the
// only honest evidence the block landed: the ledger records the block statement
// by statement, but a rolled-back block leaves the ORIGINAL <t> behind, so the
// table NAME still resolves while the rebuilt shape is gone.
function rebuildColumnNames(createSql: string): string[] {
  return createTableColumnEntries(createSql).map((entry) => entry.name)
}

// every column ENTRY a CREATE TABLE declares, with its full definition text.
// the phantom-ledger reconcile uses these to converge a live table that
// predates its ledgered CREATE: the old table resolves by NAME, so the CREATE
// never re-runs, and columns that only ever existed inside its definition are
// otherwise unreachable (prod 2026-07-26, 217 wedged namespaces).
function createTableColumnEntries(
  createSql: string
): Array<{ definition: string; name: string }> {
  const open = createSql.indexOf('(')
  if (open < 0) return []
  const entries: string[] = []
  let entry = ''
  let depth = 0
  // quote tracking is not optional here. a column DEFAULT can legally contain a
  // comma or a paren — `platforms text DEFAULT '{"ios":true,"android":false}'`
  // is in the checked-in baseline — and a depth-only split cuts through the
  // literal and reads its tail ("android") as a column name. one phantom column
  // makes blockLanded permanently false for a table that is exactly current,
  // which drops and rebuilds a live table on every single migration run.
  const body = createSql.slice(open + 1)
  for (let index = 0; index < body.length; index++) {
    const character = body[index]
    if (
      character === "'" ||
      character === '"' ||
      character === '`' ||
      character === '['
    ) {
      const closing = character === '[' ? ']' : character
      entry += character
      index++
      while (index < body.length) {
        if (body[index] === closing) {
          // '' / "" / `` inside a literal is an escaped quote, not the end of it
          if (body[index + 1] === closing && closing !== ']') {
            entry += body[index] + body[index + 1]
            index += 2
            continue
          }
          entry += body[index]
          break
        }
        entry += body[index]
        index++
      }
      continue
    }
    if (character === '(') depth++
    else if (character === ')') {
      if (depth === 0) break
      depth--
    }
    if (character === ',' && depth === 0) {
      entries.push(entry)
      entry = ''
      continue
    }
    entry += character
  }
  entries.push(entry)
  return entries
    .map((value) => value.trim())
    .filter(
      (value) =>
        value.length > 0 &&
        !/^(?:CONSTRAINT|FOREIGN KEY|PRIMARY KEY|UNIQUE|CHECK)\b/i.test(value)
    )
    .flatMap((definition) => {
      const name = /^[`"]?(\w+)/.exec(definition)?.[1]
      return name ? [{ definition, name }] : []
    })
}

export function readNativeSqlMigrationStatements(
  migrationsDir: string,
  parseStatement: (sql: string) => ParsedNativeSqlMigrationStatement
): NativeSqlMigrationStatement[] {
  const migrationPaths: string[] = []
  const visit = (dir: string) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name)
      if (entry.isDirectory()) visit(path)
      else if (entry.isFile() && entry.name === 'migration.sql') migrationPaths.push(path)
    }
  }
  visit(migrationsDir)
  const statements = migrationPaths
    .sort((a, b) => a.localeCompare(b))
    .flatMap((path) => {
      const migrationId = path.slice(migrationsDir.length + 1)
      // drizzle rebuilds a table by creating `__new_<table>`, copying rows into
      // it, dropping the original, and renaming the copy over it. the runner
      // ledgers those statements individually, and the rename destroys the very
      // table the create made — so a run that stops after the rename records the
      // create as applied with nothing left to rename, and every later run dies
      // on `no such table: __new_<table>`. carrying the temp table onto the rest
      // of the block makes it skip itself once it has already completed.
      let recreateTempTable: string | null = null
      return readFileSync(path, 'utf-8')
        .split('--> statement-breakpoint')
        .map((statement) => statement.trim())
        .filter(Boolean)
        .map((sql, index) => {
          const statement = parseStatement(sql)
          const rewritten = statement.sql
            .replace(
              /^CREATE TABLE\s+(?!IF NOT EXISTS\s+)/i,
              'CREATE TABLE IF NOT EXISTS '
            )
            .replace(
              /^CREATE UNIQUE INDEX\s+(?!IF NOT EXISTS\s+)/i,
              'CREATE UNIQUE INDEX IF NOT EXISTS '
            )
            .replace(
              /^CREATE INDEX\s+(?!IF NOT EXISTS\s+)/i,
              'CREATE INDEX IF NOT EXISTS '
            )
          const skipIfTableMissing = recreateTempTable
          const created = /^CREATE TABLE IF NOT EXISTS\s+[`"]?(__new_\w+)[`"]?/i.exec(
            rewritten
          )
          if (created) recreateTempTable = created[1]
          // every statement from the CREATE through the RENAME belongs to the
          // block, the rename included. the reconciler resurrects them as one
          // unit, so it needs membership on the DROP too.
          const blockTempTable = recreateTempTable
          if (
            recreateTempTable &&
            !created &&
            new RegExp(
              `^ALTER TABLE\\s+[\`"]?${recreateTempTable}[\`"]?\\s+RENAME TO`,
              'i'
            ).test(rewritten)
          ) {
            recreateTempTable = null
          }
          const plainCreate =
            !created && /^CREATE TABLE IF NOT EXISTS\s+[`"]?\w+/i.test(rewritten)
          return {
            id: `${migrationId}:${index}`,
            ...statement,
            ...(skipIfTableMissing ? { skipIfTableMissing } : null),
            ...(blockTempTable
              ? { rebuildTarget: blockTempTable.slice('__new_'.length) }
              : null),
            ...(created ? { rebuildColumns: rebuildColumnNames(rewritten) } : null),
            ...(plainCreate
              ? { declaredColumns: createTableColumnEntries(rewritten) }
              : null),
            sql: rewritten,
          }
        })
    })
  const supersessionsPath = join(migrationsDir, 'supersessions.json')
  if (!existsSync(supersessionsPath)) return statements
  const configuredSupersessions: unknown = JSON.parse(
    readFileSync(supersessionsPath, 'utf8')
  )
  if (
    !configuredSupersessions ||
    typeof configuredSupersessions !== 'object' ||
    Array.isArray(configuredSupersessions)
  ) {
    throw new Error('migration supersessions must be an object')
  }
  const statementIndexes = new Map(
    statements.map((statement, index) => [statement.id, index])
  )
  const supersessions: Record<string, string[]> = {}
  const successors = new Map<string, string>()
  for (const [successor, configuredSuperseded] of Object.entries(
    configuredSupersessions
  )) {
    const successorIndex = statementIndexes.get(successor)
    if (successorIndex === undefined) {
      throw new Error(`migration supersession successor does not exist: ${successor}`)
    }
    if (
      !Array.isArray(configuredSuperseded) ||
      configuredSuperseded.length === 0 ||
      configuredSuperseded.some((id) => typeof id !== 'string')
    ) {
      throw new Error(`migration supersession must name statements: ${successor}`)
    }
    const superseded: string[] = configuredSuperseded
    for (const id of superseded) {
      const supersededIndex = statementIndexes.get(id)
      if (supersededIndex === undefined) {
        throw new Error(`superseded migration statement does not exist: ${id}`)
      }
      if (supersededIndex >= successorIndex) {
        throw new Error(`migration statement must supersede an earlier statement: ${id}`)
      }
      const existingSuccessor = successors.get(id)
      if (existingSuccessor) {
        throw new Error(
          `migration statement has multiple successors: ${id} (${existingSuccessor}, ${successor})`
        )
      }
      successors.set(id, successor)
    }
    supersessions[successor] = superseded
  }
  return statements.map((statement) => {
    const supersedes = supersessions[statement.id]
    return supersedes ? { ...statement, supersedes } : statement
  })
}
