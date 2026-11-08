import {
  jsonStoragePredicate,
  numberStoragePredicate,
  wireStoragePredicate,
} from 'orez-sync-executor/sqlite'

import type { DurableSqlStorage } from './watermark.js'
import type { ZeroSchemaConfig } from 'orez-sync-executor'

const prefix = '_orez_number_guard_'
const quote = (value: string) => `"${value.replace(/"/g, '""')}"`
const literal = (value: string) => `'${value.replace(/'/g, "''")}'`

function validStorage(value: string, type: string): string {
  const wire = wireStoragePredicate(value)
  if (type === 'number') return `(${wire}) AND (${numberStoragePredicate(value)})`
  if (type === 'json') return `(${wire}) AND (${jsonStoragePredicate(value)})`
  return wire
}

/** Guards are durable SQLite triggers on the authoritative store, not its replicas. */
export class NumberStorageGuards {
  constructor(
    private readonly sql: DurableSqlStorage,
    private readonly schema: ZeroSchemaConfig
  ) {}

  // a table's columns change only with its CREATE text, which SQLite rewrites
  // on every ALTER. reconcile runs around each schema change and reads every
  // guarded table, so a fresh object booting through its migrations asked for
  // thousands of column lists that had not changed.
  private readonly shapes = new Map<
    string,
    { definition: string; columns: Set<string> }
  >()

  private tables(): Map<string, string> {
    return new Map(
      this.sql
        .exec("SELECT name, sql FROM sqlite_schema WHERE type = 'table'")
        .toArray()
        .map((row) => [String(row.name).toLowerCase(), String(row.sql ?? '')])
    )
  }

  private shape(table: string, tables: Map<string, string>): Set<string> {
    const definition = tables.get(table.toLowerCase())
    if (definition === undefined) return new Set()
    const cached = this.shapes.get(table)
    if (cached?.definition === definition) return cached.columns
    const columns = new Set(
      this.sql
        .exec(`PRAGMA table_xinfo(${quote(table)})`)
        .toArray()
        .map((row) => String(row.name))
    )
    this.shapes.set(table, { definition, columns })
    return columns
  }

  // workerd caps compound select terms well below stock SQLite, so an OR of
  // EXISTS reads, chunked to stay under its expression depth limit
  private anyRows(tables: readonly string[]): boolean {
    for (let start = 0; start < tables.length; start += 20) {
      const probe = tables
        .slice(start, start + 20)
        .map((table) => `EXISTS (SELECT 1 FROM ${quote(table)})`)
        .join(' OR ')
      if (this.sql.exec(`SELECT ${probe} AS present`).toArray()[0]?.present) return true
    }
    return false
  }

  invalidCells(): Map<string, number> {
    const cells = new Map<string, number>()
    // this runs around every schema change, so the common answer, that no
    // guarded table holds a row (a fresh object booting through its
    // migrations), costs a few reads instead of two per table.
    const tables = this.tables()
    const guarded = Object.entries(this.schema.tables).filter(([name, table]) =>
      tables.has((table.serverName ?? table.name ?? name).toLowerCase())
    )
    if (
      guarded.length === 0 ||
      !this.anyRows(
        guarded.map(([name, table]) => table.serverName ?? table.name ?? name)
      )
    ) {
      return cells
    }
    for (const [name, table] of guarded) {
      const physical = table.serverName ?? table.name ?? name
      const current = this.shape(physical, tables)
      if (current.size === 0) continue
      // an empty table holds no cell to be invalid, and a fresh object boots
      // through every migration with all of them empty
      if (
        this.sql.exec(`SELECT 1 FROM ${quote(physical)} LIMIT 1`).toArray().length === 0
      )
        continue
      const keys = table.primaryKey
        .map((key) => table.columns[key]?.serverName ?? key)
        .filter((key) => current.has(key))
      for (const [name, definition] of Object.entries(table.columns)) {
        const column = definition.serverName ?? name
        if (!current.has(column)) continue
        const field = `_orez_guard_row.${quote(column)}`
        const valid = validStorage(field, definition.type)
        const identity = [...keys, column]
          .flatMap((value) => {
            const field = quote(value)
            return [`typeof(${field})`, `hex(CAST(${field} AS BLOB))`]
          })
          .join(', ')
        const rows = this.sql
          .exec(
            `SELECT json_array(${identity}) AS identity, count(*) AS count FROM ${quote(physical)} AS _orez_guard_row WHERE NOT (${valid}) GROUP BY identity`
          )
          .toArray()
        for (const row of rows) {
          cells.set(JSON.stringify([physical, column, row.identity]), Number(row.count))
        }
      }
    }
    return cells
  }

  reconcile(before?: Map<string, number>): void {
    if (before) {
      for (const [cell, count] of this.invalidCells()) {
        if (count > (before.get(cell) ?? 0)) {
          const [table, column] = JSON.parse(cell) as [string, string, string]
          throw new Error(`invalid number storage in ${table}.${column}`)
        }
      }
    }
    const expected = new Set<string>()
    const ddl = new Map<string, string>()
    const tables = this.tables()
    for (const [name, table] of Object.entries(this.schema.tables)) {
      const physical = table.serverName ?? table.name ?? name
      const current = this.shape(physical, tables)
      if (current.size === 0) continue
      const columns = Object.entries(table.columns)
        .map(([column, definition]) => ({
          name: definition.serverName ?? column,
          type: definition.type,
        }))
        .filter((column) => current.has(column.name))
      if (columns.length === 0) continue
      const keys = table.primaryKey.map((key) => table.columns[key]?.serverName ?? key)
      const keyChanged =
        keys
          .filter((key) => current.has(key))
          .map((key) => `OLD.${quote(key)} IS NOT NEW.${quote(key)}`)
          .join(' OR ') || '0'
      const signature = [
        physical,
        ...columns.flatMap((column) => [column.name, column.type]),
        ...keys,
      ]
        .map((value) =>
          [...new TextEncoder().encode(value)]
            .map((byte) => byte.toString(16).padStart(2, '0'))
            .join('')
        )
        .join('_')
      const insertName = `${prefix}${signature}_v4_insert`
      const updateName = `${prefix}${signature}_v4_update`
      expected.add(insertName)
      expected.add(updateName)
      const insert: string[] = []
      const update: string[] = []
      for (const column of columns) {
        const field = quote(column.name)
        const predicate = validStorage(`NEW.${field}`, column.type)
        const message = literal(`invalid number storage in ${physical}.${column.name}`)
        insert.push(
          `SELECT CASE WHEN NOT (${predicate}) THEN RAISE(ABORT, ${message}) END;`
        )
        update.push(
          `SELECT CASE WHEN (OLD.${field} IS NOT NEW.${field} OR (${keyChanged})) AND NOT (${predicate}) THEN RAISE(ABORT, ${message}) END;`
        )
      }
      ddl.set(
        insertName,
        `CREATE TRIGGER IF NOT EXISTS ${quote(insertName)} AFTER INSERT ON ${quote(physical)} BEGIN ${insert.join('')} END`
      )
      ddl.set(
        updateName,
        `CREATE TRIGGER IF NOT EXISTS ${quote(updateName)} AFTER UPDATE ON ${quote(physical)} BEGIN ${update.join('')} END`
      )
    }
    for (const row of this.sql
      .exec(
        "SELECT name FROM sqlite_schema WHERE type = 'trigger' AND name GLOB '_orez_number_guard_*'"
      )
      .toArray()) {
      const name = String(row.name)
      if (!expected.has(name)) this.sql.exec(`DROP TRIGGER ${quote(name)}`).toArray()
      else ddl.delete(name)
    }
    for (const statement of ddl.values()) this.sql.exec(statement).toArray()
  }

  suspend(): void {
    for (const row of this.sql
      .exec(
        "SELECT name FROM sqlite_schema WHERE type = 'trigger' AND name GLOB '_orez_number_guard_*'"
      )
      .toArray()) {
      this.sql.exec(`DROP TRIGGER ${quote(String(row.name))}`).toArray()
    }
  }
}
