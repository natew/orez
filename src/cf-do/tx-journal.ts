/**
 * durable transaction journal for the DO SQL backend.
 *
 * the Durable Object refuses raw SQL BEGIN/COMMIT, so DoBackend emulates pg
 * transactions by applying writes eagerly. Parsed DML is rolled back from the
 * CDC row before-images captured by the same SQLite statement; unknown writes
 * fall back to a first-write table snapshot (`_orez_tx_<txID>_*`). Before this
 * journal, the rollback bookkeeping lived only in the client's memory: a DO eviction or
 * deploy upgrade-kill mid-transaction persisted the partial writes forever
 * (the 2026-06 poisoned cdc changeLog incident — zero's catchup replays
 * begin→data→begin and the replicator wedges permanently).
 *
 * the journal makes the snapshot bookkeeping durable and the commit point
 * atomic:
 *
 *   - every row-journal marker or fallback snapshot is recorded in
 *     `_orez_tx_manifest` atomically with its rollback state.
 *   - COMMIT = one atomic storage transaction that drops rollback state and
 *     deletes the manifest rows (plus promoting pending tracked changes in
 *     ZeroDO). a tx is committed if and only if its manifest rows are gone.
 *   - ROLLBACK = one atomic storage transaction that restores row before-images
 *     plus any fallback table snapshots (in reverse order).
 *   - RECOVERY (`recoverTxJournal`) rolls back every manifest tx for an
 *     owner whose process generation is known dead (e.g. the zero-cache
 *     embed at boot, before it opens any pg session), so a partial tx is
 *     invisible on the next boot.
 *
 * all functions are synchronous over a minimal sql-exec interface so the
 * same core runs inside ZeroDO's ctx.storage.transaction() and inside the
 * embed-local backend's transactionSync().
 */

import { restoreTriggers, suspendTriggers, writableColumns } from './cdc.js'

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

/** Parent snapshots must restore before children so FK cascades cannot erase a restored child. */
function parentFirst(sql: DurableSqlStorage, rows: ManifestRow[]): ManifestRow[] {
  const byTable = new Map(rows.map((row) => [row.original, row]))
  const edges = new Map(rows.map((row) => [row.original, new Set<string>()]))
  const incoming = new Map(rows.map((row) => [row.original, 0]))
  for (const child of rows) {
    const parents = sql
      .exec(`PRAGMA foreign_key_list(${quoteIdent(child.original)})`)
      .toArray()
      .map((row) => String(row.table ?? ''))
    for (const parent of new Set(parents)) {
      if (!byTable.has(parent) || parent === child.original) continue
      const children = edges.get(parent)!
      if (children.has(child.original)) continue
      children.add(child.original)
      incoming.set(child.original, (incoming.get(child.original) ?? 0) + 1)
    }
  }

  const ready = rows.filter((row) => incoming.get(row.original) === 0)
  const ordered: ManifestRow[] = []
  while (ready.length > 0) {
    const row = ready.shift()!
    ordered.push(row)
    for (const child of edges.get(row.original) ?? []) {
      const count = (incoming.get(child) ?? 0) - 1
      incoming.set(child, count)
      if (count === 0) ready.push(byTable.get(child)!)
    }
  }
  // Cyclic foreign keys are checked at the surrounding transaction boundary;
  // retain deterministic manifest order for the strongly connected remainder.
  for (const row of rows) if (!ordered.includes(row)) ordered.push(row)
  return ordered
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
 * pre-tx contents (parents before children), drop tables that did not exist at
 * first write (null snapshot), then clean up. triggers on restored tables are
 * detached during the restore and re-created after, so restore DML doesn't
 * re-fire change tracking. must run inside an atomic storage transaction.
 */
export function rollbackTxJournal(sql: DurableSqlStorage, txID: string): void {
  if (!manifestTableExists(sql)) return
  const rows = manifestRows(sql, txID).reverse()
  if (rows.length === 0) return

  const restoredTables = rows.filter((row) => row.snapshot).map((row) => row.original)
  const triggers = suspendTriggers(sql, restoredTables)
  // Defer constraint checks across the whole atomic restore. Parent tables are
  // restored before children so ON DELETE/UPDATE cascades cannot erase child
  // rows that were already copied back from their snapshot.
  sql.exec('PRAGMA defer_foreign_keys = ON')
  const snapshotRows = parentFirst(
    sql,
    rows.filter((row) => row.snapshot !== null && row.snapshot !== '')
  )
  const otherRows = rows.filter((row) => row.snapshot === null || row.snapshot === '')
  for (const row of [...snapshotRows, ...otherRows]) {
    const quotedTable = quoteIdent(row.original)
    if (row.snapshot === null) {
      sql.exec(`DROP TABLE IF EXISTS ${quotedTable}`)
      continue
    }
    // Empty-string snapshots are row-journaled tables. Their before-images
    // are restored by the owner's CDC undo callback rather than a table copy.
    if (row.snapshot === '') continue
    const quotedSnapshot = quoteIdent(row.snapshot)
    sql.exec(`DELETE FROM ${quotedTable}`)
    // `SELECT *` also selects generated columns, which SQLite refuses to let
    // any INSERT name, so restore the writable columns explicitly.
    const columns = writableColumns(sql, row.original)
    const columnList = columns.map(quoteIdent).join(', ')
    sql.exec(
      `INSERT OR REPLACE INTO ${quotedTable} (${columnList}) SELECT ${columnList} FROM ${quotedSnapshot}`
    )
    sql.exec(`DROP TABLE IF EXISTS ${quotedSnapshot}`)
  }
  restoreTriggers(sql, triggers)
  sql.exec(`DELETE FROM "${TX_MANIFEST_TABLE}" WHERE tx_id = ?`, txID)
}

/**
 * Point a transaction's journal entry for `table` at a real table snapshot.
 *
 * DoBackend marks a parsed write row-journaled (an empty snapshot) before the
 * DO has said whether it can capture that table. When CDC declines it, because
 * the table has no stable row identity, that empty marker promises a row-level
 * rollback nothing is able to perform. The owner therefore copies the table and
 * rewrites the marker in the same storage transaction as the write, which puts
 * the transaction back on the snapshot path recovery already knows how to
 * replay. Safe to call repeatedly: only the first write to a table in a
 * transaction takes the copy.
 */
export function upgradeToTableSnapshot(
  sql: DurableSqlStorage,
  txID: string,
  table: string
): void {
  sql.exec(TX_MANIFEST_DDL)
  const existing = sql
    .exec(
      `SELECT seq, snapshot FROM "${TX_MANIFEST_TABLE}" WHERE tx_id = ? AND original = ? ORDER BY seq`,
      txID,
      table
    )
    .toArray()
  // A null snapshot means the table did not exist at first write, so rollback
  // drops it and no copy is needed. A non-empty one is already a real snapshot.
  const snapshotted = existing.some(
    (row) => row.snapshot === null || String(row.snapshot ?? '') !== ''
  )
  if (snapshotted) return

  // Name the snapshot after the manifest row's seq, which is an AUTOINCREMENT
  // primary key and therefore unique. Deriving the name from the table instead
  // would have to sanitize it into an identifier, and that is not injective:
  // `a-b` and `a_b` would collide on one snapshot table, so rolling the first
  // one back would restore the other's rows and then drop the table the second
  // still needs.
  const seq =
    existing.length > 0
      ? Number(existing[0].seq)
      : Number(
          sql
            .exec(
              `INSERT INTO "${TX_MANIFEST_TABLE}" (tx_id, owner, original, snapshot) VALUES (?, 'default', ?, '') RETURNING seq`,
              txID,
              table
            )
            .toArray()[0]?.seq
        )
  const snapshot = `_orez_tx_undo_${seq}`
  sql.exec(`DROP TABLE IF EXISTS ${quoteIdent(snapshot)}`)
  sql.exec(`CREATE TABLE ${quoteIdent(snapshot)} AS SELECT * FROM ${quoteIdent(table)}`)
  sql.exec(`UPDATE "${TX_MANIFEST_TABLE}" SET snapshot = ? WHERE seq = ?`, snapshot, seq)
}

/**
 * Snapshot every user table before a statement whose source table has a
 * business trigger or is the parent of a cascading/SET foreign-key action.
 * Those effects can reach unregistered tables, and SQLite's trigger staging
 * order is not causal DML order. This conservative fallback is paid only by
 * side-effecting writes and makes table snapshots authoritative for them.
 */
export function snapshotSideEffectWriteTables(
  sql: DurableSqlStorage,
  txID: string,
  sourceTable: string
): boolean {
  const hasBusinessTrigger =
    sql
      .exec(
        "SELECT 1 AS ok FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ? AND name NOT LIKE '_orez_cdc_%' LIMIT 1",
        sourceTable
      )
      .toArray().length > 0
  const tables = sql
    .exec(
      "SELECT name FROM sqlite_master WHERE type = 'table' " +
        "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_orez_%' " +
        "AND name NOT LIKE '_zero_%' ORDER BY name"
    )
    .toArray()
    .map((row) => String(row.name ?? ''))
    .filter(Boolean)
  const hasReferentialAction = tables.some((child) =>
    sql
      .exec(`PRAGMA foreign_key_list(${quoteIdent(child)})`)
      .toArray()
      .some((row) => {
        if (String(row.table ?? '') !== sourceTable) return false
        return [row.on_update, row.on_delete].some((action) => {
          const normalized = String(action ?? 'NO ACTION').toUpperCase()
          return normalized !== 'NO ACTION' && normalized !== 'RESTRICT'
        })
      })
  )
  if (!hasBusinessTrigger && !hasReferentialAction) return false

  for (const table of tables) upgradeToTableSnapshot(sql, txID, table)
  return true
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
export function recoverTxJournal(
  sql: DurableSqlStorage,
  owner?: string,
  beforeRollback?: (txID: string) => void
): string[] {
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
      beforeRollback?.(txID)
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
