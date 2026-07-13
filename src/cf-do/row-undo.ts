import type { TransactionalCdc } from './cdc.js'
import type { DurableSqlStorage } from './watermark.js'

export const PENDING_CHANGES_TABLE = '_zero_pending_changes'

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
      'created_at INTEGER NOT NULL DEFAULT (unixepoch()))'
  )
  const columns = sql.exec(`PRAGMA table_info(${PENDING_CHANGES_TABLE})`).toArray()
  if (!columns.some((column) => String(column.name) === 'physical_table_name')) {
    sql.exec(`ALTER TABLE ${PENDING_CHANGES_TABLE} ADD COLUMN physical_table_name TEXT`)
  }
  if (!columns.some((column) => String(column.name) === 'publish')) {
    sql.exec(
      `ALTER TABLE ${PENDING_CHANGES_TABLE} ADD COLUMN publish INTEGER NOT NULL DEFAULT 1`
    )
  }
}

export function appendPendingChange(
  sql: DurableSqlStorage,
  change: {
    transactionID: string
    physicalTableName?: string
    tableName: string
    publish?: boolean
    op: 'INSERT' | 'UPDATE' | 'DELETE'
    rowData: Record<string, unknown> | null
    oldData: Record<string, unknown> | null
  }
): void {
  sql.exec(
    `INSERT INTO ${PENDING_CHANGES_TABLE} ` +
      '(transaction_id, physical_table_name, table_name, publish, op, row_data, old_data) ' +
      'VALUES (?, ?, ?, ?, ?, ?, ?)',
    change.transactionID,
    change.physicalTableName || null,
    change.tableName,
    change.publish === false ? 0 : 1,
    change.op,
    change.rowData ? JSON.stringify(change.rowData) : null,
    change.oldData ? JSON.stringify(change.oldData) : null
  )
}

/** Restore row before-images in reverse statement order. Must run atomically. */
export function rollbackPendingChanges(
  sql: DurableSqlStorage,
  cdc: TransactionalCdc,
  transactionID: string
): number {
  ensurePendingChangesTable(sql)
  const rows = sql
    .exec(
      `SELECT physical_table_name, op, row_data, old_data
       FROM ${PENDING_CHANGES_TABLE}
       WHERE transaction_id = ? AND physical_table_name IS NOT NULL
       ORDER BY id DESC`,
      transactionID
    )
    .toArray()
  const tables = rows.map((row) => String(row.physical_table_name ?? '')).filter(Boolean)
  const suspended = cdc.suspendTables(tables)
  try {
    for (const row of rows) {
      const table = String(row.physical_table_name)
      const op = String(row.op)
      const next = row.row_data ? JSON.parse(String(row.row_data)) : null
      const old = row.old_data ? JSON.parse(String(row.old_data)) : null
      if (op === 'INSERT' && next) {
        const columns = Object.keys(next)
        if (columns.length === 0) continue
        sql.exec(
          `DELETE FROM ${quoteIdent(table)} WHERE ${columns
            .map((column) => `${quoteIdent(column)} IS ?`)
            .join(' AND ')}`,
          ...columns.map((column) => next[column])
        )
      } else if (op === 'DELETE' && old) {
        const columns = Object.keys(old)
        if (columns.length === 0) continue
        sql.exec(
          `INSERT OR REPLACE INTO ${quoteIdent(table)} (${columns
            .map(quoteIdent)
            .join(', ')}) VALUES (${columns.map(() => '?').join(', ')})`,
          ...columns.map((column) => old[column])
        )
      } else if (op === 'UPDATE' && old && next) {
        const oldColumns = Object.keys(old)
        const nextColumns = Object.keys(next)
        if (oldColumns.length === 0 || nextColumns.length === 0) continue
        sql.exec(
          `UPDATE ${quoteIdent(table)} SET ${oldColumns
            .map((column) => `${quoteIdent(column)} = ?`)
            .join(', ')} WHERE ${nextColumns
            .map((column) => `${quoteIdent(column)} IS ?`)
            .join(' AND ')}`,
          ...oldColumns.map((column) => old[column]),
          ...nextColumns.map((column) => next[column])
        )
      }
    }
  } finally {
    cdc.resumeTables(suspended)
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
