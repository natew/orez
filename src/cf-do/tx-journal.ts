/**
 * durable transaction journal for the DO SQL backend.
 *
 * the Durable Object refuses raw SQL BEGIN/COMMIT, so DoBackend emulates pg
 * transactions by applying writes eagerly and snapshotting each table on its
 * first in-tx write (`_orez_tx_<txID>_*` tables). before this journal, the
 * snapshot bookkeeping lived only in the client's memory: a DO eviction or
 * deploy upgrade-kill mid-transaction persisted the partial writes forever
 * (the 2026-06 poisoned cdc changeLog incident — zero's catchup replays
 * begin→data→begin and the replicator wedges permanently).
 *
 * the journal makes the snapshot bookkeeping durable and the commit point
 * atomic:
 *
 *   - every snapshot is recorded in `_orez_tx_manifest` in the same atomic
 *     /batch that creates the snapshot table.
 *   - COMMIT = one atomic storage transaction that drops the snapshots and
 *     deletes the manifest rows (plus promoting pending tracked changes in
 *     ZeroDO). a tx is committed if and only if its manifest rows are gone.
 *   - ROLLBACK = one atomic storage transaction that restores every table
 *     from its snapshot (reverse order, triggers detached during restore).
 *   - RECOVERY (`recoverTxJournal`) rolls back every manifest tx for an
 *     owner whose process generation is known dead (e.g. the zero-cache
 *     embed at boot, before it opens any pg session), so a partial tx is
 *     invisible on the next boot.
 *
 * all functions are synchronous over a minimal sql-exec interface so the
 * same core runs inside ZeroDO's ctx.storage.transaction() and inside the
 * embed-local backend's transactionSync().
 */

import type { DurableSqlStorage } from './watermark.js'

export const TX_MANIFEST_TABLE = '_orez_tx_manifest'

export const TX_MANIFEST_DDL =
  `CREATE TABLE IF NOT EXISTS "${TX_MANIFEST_TABLE}" (` +
  'seq INTEGER PRIMARY KEY AUTOINCREMENT, ' +
  'tx_id TEXT NOT NULL, ' +
  "owner TEXT NOT NULL DEFAULT 'default', " +
  'original TEXT NOT NULL, ' +
  'snapshot TEXT)'

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

interface ManifestRow {
  seq: number
  txId: string
  original: string
  snapshot: string | null
}

function manifestTableExists(sql: DurableSqlStorage): boolean {
  return (
    sql
      .exec(
        "SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = ?",
        TX_MANIFEST_TABLE
      )
      .toArray().length > 0
  )
}

function manifestRows(sql: DurableSqlStorage, txID: string): ManifestRow[] {
  return sql
    .exec(
      `SELECT seq, tx_id, original, snapshot FROM "${TX_MANIFEST_TABLE}" WHERE tx_id = ? ORDER BY seq`,
      txID
    )
    .toArray()
    .map((row) => ({
      seq: Number(row.seq),
      txId: String(row.tx_id),
      original: String(row.original),
      snapshot:
        row.snapshot === null || row.snapshot === undefined ? null : String(row.snapshot),
    }))
}

function dropTable(sql: DurableSqlStorage, table: string): void {
  sql.exec(`DROP TABLE IF EXISTS ${quoteIdent(table)}`)
}

/**
 * commit a journaled transaction: drop its snapshot tables and delete its
 * manifest rows. the data writes were applied eagerly during the tx, so once
 * the manifest rows are gone the tx is durably committed. must run inside an
 * atomic storage transaction.
 */
export function commitTxJournal(sql: DurableSqlStorage, txID: string): void {
  if (!manifestTableExists(sql)) return
  for (const row of manifestRows(sql, txID)) {
    if (row.snapshot) dropTable(sql, row.snapshot)
  }
  sql.exec(`DELETE FROM "${TX_MANIFEST_TABLE}" WHERE tx_id = ?`, txID)
}

/**
 * roll back a journaled transaction: restore every snapshotted table to its
 * pre-tx contents (reverse snapshot order), drop tables that did not exist at
 * first write (null snapshot), then clean up. triggers on restored tables are
 * detached during the restore and re-created after, so restore DML doesn't
 * re-fire change tracking. must run inside an atomic storage transaction.
 */
export function rollbackTxJournal(sql: DurableSqlStorage, txID: string): void {
  if (!manifestTableExists(sql)) return
  const rows = manifestRows(sql, txID).reverse()
  if (rows.length === 0) return

  const restoredTables = rows.filter((row) => row.snapshot).map((row) => row.original)
  const triggers = triggerDefinitionsForTables(sql, restoredTables)
  for (const trigger of triggers) {
    sql.exec(`DROP TRIGGER IF EXISTS ${quoteIdent(trigger.name)}`)
  }
  for (const row of rows) {
    const quotedTable = quoteIdent(row.original)
    if (!row.snapshot) {
      sql.exec(`DROP TABLE IF EXISTS ${quotedTable}`)
      continue
    }
    const quotedSnapshot = quoteIdent(row.snapshot)
    sql.exec(`DELETE FROM ${quotedTable}`)
    sql.exec(`INSERT OR REPLACE INTO ${quotedTable} SELECT * FROM ${quotedSnapshot}`)
    sql.exec(`DROP TABLE IF EXISTS ${quotedSnapshot}`)
  }
  for (const trigger of triggers) sql.exec(trigger.sql)
  sql.exec(`DELETE FROM "${TX_MANIFEST_TABLE}" WHERE tx_id = ?`, txID)
}

function triggerDefinitionsForTables(
  sql: DurableSqlStorage,
  tables: string[]
): { name: string; sql: string }[] {
  const uniqueTables = [...new Set(tables)].filter(Boolean)
  if (uniqueTables.length === 0) return []
  const placeholders = uniqueTables.map(() => '?').join(', ')
  return sql
    .exec(
      `SELECT name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name IN (${placeholders}) ORDER BY name`,
      ...uniqueTables
    )
    .toArray()
    .map((row) => ({ name: String(row.name ?? ''), sql: String(row.sql ?? '') }))
    .filter((trigger) => trigger.name && trigger.sql)
}

/**
 * roll back every journaled transaction belonging to `owner` (or every
 * transaction when owner is undefined), and sweep `_orez_tx_*` snapshot
 * tables no manifest row references (leftovers from pre-journal code).
 *
 * callers must guarantee none of the matched transactions can still be live
 * — i.e. the owning process generation is dead (embed boot before opening pg
 * sessions, or a store whose only client lives in the same isolate).
 *
 * returns the recovered transaction ids so callers can clean up associated
 * state (e.g. pending tracked changes).
 */
export function recoverTxJournal(sql: DurableSqlStorage, owner?: string): string[] {
  const recovered: string[] = []
  if (manifestTableExists(sql)) {
    const txRows =
      owner === undefined
        ? sql.exec(`SELECT DISTINCT tx_id FROM "${TX_MANIFEST_TABLE}"`).toArray()
        : sql
            .exec(
              `SELECT DISTINCT tx_id FROM "${TX_MANIFEST_TABLE}" WHERE owner = ?`,
              owner
            )
            .toArray()
    for (const row of txRows) {
      const txID = String(row.tx_id)
      rollbackTxJournal(sql, txID)
      recovered.push(txID)
    }
  }

  // sweep snapshot tables nothing references: pre-journal leftovers from a
  // killed tx. live transactions (any owner) are protected by their manifest
  // rows, which are created atomically with the snapshot table.
  const referenced = new Set(
    manifestTableExists(sql)
      ? sql
          .exec(`SELECT snapshot FROM "${TX_MANIFEST_TABLE}" WHERE snapshot IS NOT NULL`)
          .toArray()
          .map((row) => String(row.snapshot))
      : []
  )
  const orphanTables = sql
    .exec(
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE '\\_orez\\_tx\\_%' ESCAPE '\\' AND name != ?",
      TX_MANIFEST_TABLE
    )
    .toArray()
    .map((row) => String(row.name))
    .filter((name) => !referenced.has(name))
  for (const name of orphanTables) dropTable(sql, name)

  return recovered
}
