import {
  journalValueSqlBinding,
  parseJournalRecord,
  restoreTriggers,
  suspendTriggers,
  tableIdentity,
  type JournalRecord,
  type TableIdentity,
} from './cdc.js'
import { schemaRestoreOwnsTable } from './tx-journal.js'

import type { DurableSqlStorage } from './watermark.js'

export const PENDING_CHANGES_TABLE = '_zero_pending_changes'

type Op = 'INSERT' | 'UPDATE' | 'DELETE'

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

export function ensurePendingChangesTable(sql: DurableSqlStorage): void {
  sql.exec(
    `CREATE TABLE IF NOT EXISTS ${PENDING_CHANGES_TABLE} (` +
      'id INTEGER PRIMARY KEY AUTOINCREMENT, ' +
      'transaction_id TEXT NOT NULL, ' +
      'physical_table_name TEXT, ' +
      'table_name TEXT NOT NULL, ' +
      'publish INTEGER NOT NULL DEFAULT 1, ' +
      "op TEXT NOT NULL CHECK (op IN ('INSERT', 'UPDATE', 'DELETE')), " +
      'row_data TEXT, ' +
      'old_data TEXT, ' +
      'row_journal TEXT, ' +
      'old_journal TEXT, ' +
      'new_rowid TEXT, ' +
      'old_rowid TEXT, ' +
      'undoable INTEGER NOT NULL DEFAULT 0, ' +
      'created_at INTEGER NOT NULL DEFAULT (unixepoch()))'
  )
  const columns = sql
    .exec(`PRAGMA table_info(${PENDING_CHANGES_TABLE})`)
    .toArray()
    .map((column) => String(column.name ?? ''))
  if (!columns.includes('physical_table_name')) {
    sql.exec(`ALTER TABLE ${PENDING_CHANGES_TABLE} ADD COLUMN physical_table_name TEXT`)
  }
  if (!columns.includes('publish')) {
    sql.exec(
      `ALTER TABLE ${PENDING_CHANGES_TABLE} ADD COLUMN publish INTEGER NOT NULL DEFAULT 1`
    )
  }
  for (const column of ['row_journal', 'old_journal', 'new_rowid', 'old_rowid']) {
    if (columns.includes(column)) continue
    sql.exec(`ALTER TABLE ${PENDING_CHANGES_TABLE} ADD COLUMN ${column} TEXT`)
  }
  if (!columns.includes('undoable')) {
    sql.exec(
      `ALTER TABLE ${PENDING_CHANGES_TABLE} ADD COLUMN undoable INTEGER NOT NULL DEFAULT 0`
    )
    // Rows written before the typed journal claimed row-level undo but only
    // carry the lossy Zero wire image, where a blob is indistinguishable from
    // text and an int64 is already rounded. Restoring from that would corrupt
    // the table, so mark them undoable and let rollback fail loudly on them
    // rather than silently revive a wrong value.
    sql.exec(`UPDATE ${PENDING_CHANGES_TABLE} SET undoable = 1`)
  }
}

export function appendPendingChange(
  sql: DurableSqlStorage,
  change: {
    transactionID: string
    physicalTableName?: string
    tableName: string
    publish?: boolean
    op: Op
    rowData: Record<string, unknown> | null
    oldData: Record<string, unknown> | null
    /**
     * Present only for CDC-captured writes. Their absence means the row is
     * published to the changefeed but rolled back by a table snapshot, so undo
     * must skip it.
     */
    rowJournal?: JournalRecord | null
    oldJournal?: JournalRecord | null
    newRowid?: string | null
    oldRowid?: string | null
    undoable: boolean
  }
): void {
  sql.exec(
    `INSERT INTO ${PENDING_CHANGES_TABLE} ` +
      '(transaction_id, physical_table_name, table_name, publish, op, row_data, old_data, ' +
      'row_journal, old_journal, new_rowid, old_rowid, undoable) ' +
      'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
    change.transactionID,
    change.physicalTableName || null,
    change.tableName,
    change.publish === false ? 0 : 1,
    change.op,
    change.rowData ? JSON.stringify(change.rowData) : null,
    change.oldData ? JSON.stringify(change.oldData) : null,
    change.rowJournal ? JSON.stringify(change.rowJournal) : null,
    change.oldJournal ? JSON.stringify(change.oldJournal) : null,
    change.newRowid ?? null,
    change.oldRowid ?? null,
    change.undoable ? 1 : 0
  )
}

/**
 * The WHERE clause that selects exactly the row this change touched.
 *
 * The captured rowid is the strongest identity: it survives primary-key updates
 * and tells two otherwise identical rows of a keyless table apart. WITHOUT ROWID
 * tables have no rowid but always declare a primary key, so they match on that.
 */
function identityWhere(
  identity: TableIdentity,
  journal: JournalRecord | null,
  rowid: string | null,
  table: string
): { sql: string; params: unknown[] } {
  if (identity.rowidAlias && rowid !== null) {
    return {
      sql: `${identity.rowidAlias} = CAST(? AS INTEGER)`,
      params: [rowid],
    }
  }
  if (identity.keyColumns.length === 0) {
    throw new Error(
      `cdc undo: ${table} has no stable row identity (no rowid, no primary key)`
    )
  }
  const clauses: string[] = []
  const params: unknown[] = []
  for (const column of identity.keyColumns) {
    const encoded = journal?.[column]
    if (encoded === undefined) {
      throw new Error(`cdc undo: ${table} before-image is missing key column ${column}`)
    }
    const binding = journalValueSqlBinding(encoded)
    clauses.push(`${quoteIdent(column)} IS ${binding.expr}`)
    params.push(...binding.params)
  }
  return { sql: clauses.join(' AND '), params }
}

/**
 * Run one undo statement and assert it moved exactly one row. A rollback that
 * silently touches zero rows (already gone) or several (a keyless duplicate
 * matched by value) has corrupted the table, so fail the storage transaction
 * instead of leaving the damage behind.
 */
function execExactlyOneRow(
  sql: DurableSqlStorage,
  statement: string,
  params: unknown[],
  context: string
): void {
  const affected = sql.exec(`${statement} RETURNING 1 AS ok`, ...params).toArray().length
  if (affected !== 1) {
    throw new Error(`cdc undo: ${context} matched ${affected} rows, expected exactly 1`)
  }
}

function requireCompleteImage(
  journal: JournalRecord | null,
  identity: TableIdentity,
  table: string,
  label: string
): asserts journal is JournalRecord {
  if (!journal) throw new Error(`cdc undo: ${table} has no ${label}`)
  const missing = identity.columns.filter((column) => journal[column] === undefined)
  if (missing.length > 0) {
    throw new Error(
      `cdc undo: ${table} ${label} is missing column(s): ${missing.join(', ')}`
    )
  }
}

/**
 * Restore row before-images in reverse statement order. Must run atomically.
 *
 * Every trigger on the touched tables is detached for the duration, business
 * triggers included: a restore replays the inverse DML, and letting the table's
 * own triggers fire on it would write side effects the original transaction
 * never made.
 */
export function rollbackPendingChanges(
  sql: DurableSqlStorage,
  transactionID: string
): number {
  ensurePendingChangesTable(sql)
  const rows = sql
    .exec(
      `SELECT physical_table_name, op, row_journal, old_journal, new_rowid, old_rowid
       FROM ${PENDING_CHANGES_TABLE}
       WHERE transaction_id = ? AND physical_table_name IS NOT NULL AND undoable != 0
       ORDER BY id DESC`,
      transactionID
    )
    .toArray()
  const tables = rows.map((row) => String(row.physical_table_name ?? '')).filter(Boolean)
  const suspended = suspendTriggers(sql, tables)
  const identities = new Map<string, TableIdentity | null>()
  const schemaOwned = new Map<string, boolean>()
  try {
    for (const row of rows) {
      const table = String(row.physical_table_name)
      const op = String(row.op) as Op
      const next = parseJournalRecord(row.row_journal)
      const old = parseJournalRecord(row.old_journal)
      const newRowid = row.new_rowid == null ? null : String(row.new_rowid)
      const oldRowid = row.old_rowid == null ? null : String(row.old_rowid)

      // a table created, dropped, or rebuilt by in-tx transactional DDL needs
      // no row undo: the schema rollback (restoreSchemaSnapshot) rebuilds it
      // wholesale from its pre-tx snapshot, and undoing rows against a
      // dropped or reshaped table throws (which used to wedge recovery — and
      // the namespace — permanently).
      if (!schemaOwned.has(table)) {
        schemaOwned.set(table, schemaRestoreOwnsTable(sql, transactionID, table))
      }
      if (schemaOwned.get(table)) continue
      if (!identities.has(table)) identities.set(table, tableIdentity(sql, table))
      const identity = identities.get(table)
      if (!identity) {
        throw new Error(
          `cdc undo: ${table} has no stable row identity, so its rows cannot be restored`
        )
      }
      // Every undoable row is written with its before-image in the same SQLite
      // statement as the write itself. A missing one means the row predates the
      // typed journal, and its lossy wire image would restore the wrong value.
      // Fail the whole rollback rather than let a write survive it.
      if ((op === 'INSERT' && !next && newRowid === null) || (op !== 'INSERT' && !old)) {
        throw new Error(
          `cdc undo: ${op} on ${table} has no usable before-image, so the transaction ` +
            `cannot be rolled back (pending row predates the typed journal)`
        )
      }
      if (op === 'INSERT' || op === 'UPDATE') {
        requireCompleteImage(next, identity, table, 'new image')
      }
      if (op === 'DELETE' || op === 'UPDATE') {
        requireCompleteImage(old, identity, table, 'old image')
      }

      if (op === 'INSERT') {
        const where = identityWhere(identity, next, newRowid, table)
        execExactlyOneRow(
          sql,
          `DELETE FROM ${quoteIdent(table)} WHERE ${where.sql}`,
          where.params,
          `INSERT undo on ${table}`
        )
        continue
      }

      if (op === 'DELETE') {
        const oldImage = old!
        const columns: string[] = []
        const values: string[] = []
        const params: unknown[] = []
        for (const column of identity.writableColumns) {
          const encoded = oldImage[column]
          const binding = journalValueSqlBinding(encoded)
          columns.push(quoteIdent(column))
          values.push(binding.expr)
          params.push(...binding.params)
        }
        // Restore the original rowid too, so any earlier change in this
        // transaction that identified a row by rowid still resolves. When the
        // table has an INTEGER PRIMARY KEY that column already is the rowid.
        if (identity.rowidAlias && !identity.rowidColumn && oldRowid !== null) {
          columns.push(identity.rowidAlias)
          values.push('CAST(? AS INTEGER)')
          params.push(oldRowid)
        }
        if (columns.length === 0) {
          throw new Error(`cdc undo: DELETE on ${table} has no writable columns`)
        }
        execExactlyOneRow(
          sql,
          `INSERT INTO ${quoteIdent(table)} (${columns.join(', ')}) VALUES (${values.join(', ')})`,
          params,
          `DELETE undo on ${table}`
        )
        continue
      }

      const assignments: string[] = []
      const params: unknown[] = []
      const oldImage = old!
      for (const column of identity.writableColumns) {
        const encoded = oldImage[column]
        const binding = journalValueSqlBinding(encoded)
        assignments.push(`${quoteIdent(column)} = ${binding.expr}`)
        params.push(...binding.params)
      }
      if (assignments.length === 0) {
        throw new Error(`cdc undo: UPDATE on ${table} has no writable columns`)
      }
      const where = identityWhere(identity, next, newRowid, table)
      params.push(...where.params)
      execExactlyOneRow(
        sql,
        `UPDATE ${quoteIdent(table)} SET ${assignments.join(', ')} WHERE ${where.sql}`,
        params,
        `UPDATE undo on ${table}`
      )
    }
  } finally {
    restoreTriggers(sql, suspended)
  }
  return rows.length
}

export function deletePendingChanges(
  sql: DurableSqlStorage,
  transactionID: string
): number {
  ensurePendingChangesTable(sql)
  return sql
    .exec(
      `DELETE FROM ${PENDING_CHANGES_TABLE} WHERE transaction_id = ? RETURNING 1 AS deleted`,
      transactionID
    )
    .toArray().length
}
