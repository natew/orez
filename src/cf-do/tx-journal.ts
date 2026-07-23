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
export const TX_SCHEMA_TABLE = '_orez_tx_schema'

export const TX_MANIFEST_DDL =
  `CREATE TABLE IF NOT EXISTS "${TX_MANIFEST_TABLE}" (` +
  'seq INTEGER PRIMARY KEY AUTOINCREMENT, ' +
  'tx_id TEXT NOT NULL, ' +
  "owner TEXT NOT NULL DEFAULT 'default', " +
  'original TEXT NOT NULL, ' +
  'snapshot TEXT)'

export const TX_SCHEMA_DDL =
  `CREATE TABLE IF NOT EXISTS "${TX_SCHEMA_TABLE}" (` +
  'seq INTEGER PRIMARY KEY AUTOINCREMENT, ' +
  'tx_id TEXT NOT NULL, ' +
  "owner TEXT NOT NULL DEFAULT 'default', " +
  'type TEXT NOT NULL, ' +
  'name TEXT NOT NULL, ' +
  'tbl_name TEXT NOT NULL, ' +
  'sql TEXT)'

const INTERNAL_TABLES = new Set([
  TX_MANIFEST_TABLE,
  TX_SCHEMA_TABLE,
  '_orez_cdc_tables',
  '_orez_cdc_buffer',
  '_zero_pending_changes',
  '_zero_changes',
  '_zero_change_state',
  '_zero_schema_tables',
])

function isInternalObject(name: string): boolean {
  return (
    INTERNAL_TABLES.has(name) ||
    name.startsWith('_orez_tx_') ||
    name.startsWith('sqlite_')
  )
}

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

function tableExists(sql: DurableSqlStorage, name: string): boolean {
  return (
    sql
      .exec("SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = ?", name)
      .toArray().length > 0
  )
}

function schemaTableExists(sql: DurableSqlStorage): boolean {
  return (
    sql
      .exec(
        "SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = ?",
        TX_SCHEMA_TABLE
      )
      .toArray().length > 0
  )
}

interface SchemaRow {
  seq: number
  type: string
  name: string
  table: string
  sql: string | null
}

function schemaRows(sql: DurableSqlStorage, txID: string): SchemaRow[] {
  if (!schemaTableExists(sql)) return []
  return sql
    .exec(
      `SELECT seq, type, name, tbl_name, sql FROM "${TX_SCHEMA_TABLE}" WHERE tx_id = ? ORDER BY seq`,
      txID
    )
    .toArray()
    .map((row) => ({
      seq: Number(row.seq),
      type: String(row.type),
      name: String(row.name),
      table: String(row.tbl_name),
      sql: row.sql === null || row.sql === undefined ? null : String(row.sql),
    }))
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
 * Capture the application schema before the first DDL statement and the data
 * of each table that a later DDL statement can destructively change. SQLite
 * can transact DDL, but the DO protocol spans requests, so rollback needs an
 * explicit durable image. The caller must wrap this in one storage transaction.
 */
export function snapshotTxSchema(
  sql: DurableSqlStorage,
  txID: string,
  owner = 'default',
  affectedTables: string[] = []
): void {
  sql.exec(TX_SCHEMA_DDL)
  const exists = sql
    .exec(
      `SELECT 1 AS ok FROM "${TX_SCHEMA_TABLE}" WHERE tx_id = ? AND type = 'marker' LIMIT 1`,
      txID
    )
    .toArray()
  if (exists.length === 0) {
    const objects = sql
      .exec(
        'SELECT type, name, tbl_name, sql FROM sqlite_master ' +
          "WHERE type IN ('table', 'index', 'trigger', 'view') AND sql IS NOT NULL " +
          'ORDER BY rowid'
      )
      .toArray()
      .map((row) => ({
        type: String(row.type ?? ''),
        name: String(row.name ?? ''),
        table: String(row.tbl_name ?? ''),
        sql: String(row.sql ?? ''),
      }))
      .filter((row) => row.name && !isInternalObject(row.name))

    // A marker makes even an empty pre-transaction schema recoverable after a
    // kill between CREATE TABLE and the client COMMIT/ROLLBACK.
    sql.exec(
      `INSERT INTO "${TX_SCHEMA_TABLE}" (tx_id, owner, type, name, tbl_name, sql) VALUES (?, ?, 'marker', '', '', NULL)`,
      txID,
      owner
    )
    for (const object of objects) {
      sql.exec(
        `INSERT INTO "${TX_SCHEMA_TABLE}" (tx_id, owner, type, name, tbl_name, sql) VALUES (?, ?, ?, ?, ?, ?)`,
        txID,
        owner,
        object.type,
        object.name,
        object.table,
        object.sql
      )
    }
  }

  for (const table of new Set(affectedTables)) {
    if (!table || isInternalObject(table)) continue
    const tableExists = sql
      .exec(
        "SELECT 1 AS ok FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        table
      )
      .toArray()
    if (tableExists.length > 0) upgradeToTableSnapshot(sql, txID, table, owner)
  }
}

/** persist one cheap recovery marker only when a transaction first mutates data. */
export function beginTxJournal(
  sql: DurableSqlStorage,
  txID: string,
  owner = 'default'
): void {
  sql.exec(TX_SCHEMA_DDL)
  sql.exec(
    `INSERT INTO "${TX_SCHEMA_TABLE}" (tx_id, owner, type, name, tbl_name, sql) ` +
      `SELECT ?, ?, 'active', '', '', NULL WHERE NOT EXISTS (` +
      `SELECT 1 FROM "${TX_SCHEMA_TABLE}" WHERE tx_id = ? AND type = 'active')`,
    txID,
    owner,
    txID
  )
}

function restoreSchemaSnapshot(
  sql: DurableSqlStorage,
  txID: string,
  schema: SchemaRow[],
  manifest: ManifestRow[]
): void {
  const current = sql
    .exec(
      'SELECT type, name, tbl_name, sql FROM sqlite_master ' +
        "WHERE type IN ('table', 'index', 'trigger', 'view') AND sql IS NOT NULL " +
        'ORDER BY rowid DESC'
    )
    .toArray()
    .map((row) => ({
      type: String(row.type ?? ''),
      name: String(row.name ?? ''),
      table: String(row.tbl_name ?? ''),
      sql: String(row.sql ?? ''),
    }))
    .filter((row) => row.name && !isInternalObject(row.name))

  const original = schema.filter((row) => row.type !== 'marker' && row.sql)
  const key = (row: { type: string; name: string }) => `${row.type}\0${row.name}`
  const originalByKey = new Map(original.map((row) => [key(row), row]))
  const currentByKey = new Map(current.map((row) => [key(row), row]))
  const originalTables = original.filter((row) => row.type === 'table')
  const currentTables = current.filter((row) => row.type === 'table')
  const changedOriginalTables = new Set(
    originalTables
      .filter((row) => currentByKey.get(key(row))?.sql !== row.sql)
      .map((row) => row.name)
  )
  const createdTables = new Set(
    currentTables.filter((row) => !originalByKey.has(key(row))).map((row) => row.name)
  )
  const tablesToDrop = new Set([...changedOriginalTables, ...createdTables])
  const snapshotByTable = new Map(
    manifest
      .filter((row) => row.snapshot)
      .map((row) => [row.original, row.snapshot!] as const)
  )
  for (const table of changedOriginalTables) {
    if (!snapshotByTable.has(table)) {
      throw new Error(`transactional DDL rollback is missing table snapshot: ${table}`)
    }
  }

  sql.exec('PRAGMA defer_foreign_keys = ON')
  const changedCurrentObject = (object: (typeof current)[number]) => {
    const before = originalByKey.get(key(object))
    return !before || before.sql !== object.sql || tablesToDrop.has(object.table)
  }
  // Views can depend on tables. Indexes and triggers disappear with their
  // table, while changed standalone objects are explicitly removed.
  for (const object of current.filter(
    (object) => object.type === 'view' && changedCurrentObject(object)
  )) {
    sql.exec(`DROP VIEW IF EXISTS ${quoteIdent(object.name)}`)
  }
  for (const object of current.filter(
    (object) =>
      object.type === 'trigger' &&
      !tablesToDrop.has(object.table) &&
      changedCurrentObject(object)
  )) {
    sql.exec(`DROP TRIGGER IF EXISTS ${quoteIdent(object.name)}`)
  }
  for (const object of current.filter(
    (object) =>
      object.type === 'index' &&
      !tablesToDrop.has(object.table) &&
      changedCurrentObject(object)
  )) {
    sql.exec(`DROP INDEX IF EXISTS ${quoteIdent(object.name)}`)
  }
  for (const table of tablesToDrop) sql.exec(`DROP TABLE IF EXISTS ${quoteIdent(table)}`)

  const tables = originalTables.filter((row) => changedOriginalTables.has(row.name))
  for (const row of tables) sql.exec(row.sql!)

  for (const row of tables) {
    const snapshot = snapshotByTable.get(row.name)!
    const hasRows =
      sql.exec(`SELECT 1 AS ok FROM ${quoteIdent(snapshot)} LIMIT 1`).toArray().length > 0
    if (!hasRows) continue
    const columns = writableColumns(sql, row.name)
    if (columns.length > 0) {
      const columnList = columns.map(quoteIdent).join(', ')
      sql.exec(
        `INSERT INTO ${quoteIdent(row.name)} (${columnList}) SELECT ${columnList} FROM ${quoteIdent(snapshot)}`
      )
    }
  }

  // Restore secondary objects only after all table rows are back. This keeps
  // business triggers from firing while the snapshot data is inserted.
  for (const type of ['index', 'trigger', 'view']) {
    for (const row of original.filter((item) => item.type === type)) {
      const after = currentByKey.get(key(row))
      if (!after || after.sql !== row.sql || changedOriginalTables.has(row.table)) {
        sql.exec(row.sql!)
      }
    }
  }

  // Consume only the snapshots used for schema restoration. Any remaining
  // manifest entries belong to ordinary DML in the same transaction and must
  // still run through the row/table rollback path below.
  if (manifestTableExists(sql)) {
    for (const table of changedOriginalTables) {
      const snapshot = snapshotByTable.get(table)
      if (snapshot) dropTable(sql, snapshot)
      sql.exec(
        `DELETE FROM "${TX_MANIFEST_TABLE}" WHERE tx_id = ? AND original = ?`,
        txID,
        table
      )
    }
  }
  sql.exec(`DELETE FROM "${TX_SCHEMA_TABLE}" WHERE tx_id = ?`, txID)
  rollbackTxJournal(sql, txID)
}

/**
 * commit a journaled transaction: drop its snapshot tables and delete its
 * manifest rows. the data writes were applied eagerly during the tx, so once
 * the manifest rows are gone the tx is durably committed. must run inside an
 * atomic storage transaction.
 */
export function commitTxJournal(sql: DurableSqlStorage, txID: string): void {
  if (manifestTableExists(sql)) {
    for (const row of manifestRows(sql, txID)) {
      if (row.snapshot) dropTable(sql, row.snapshot)
    }
    sql.exec(`DELETE FROM "${TX_MANIFEST_TABLE}" WHERE tx_id = ?`, txID)
  }
  if (schemaTableExists(sql)) {
    sql.exec(`DELETE FROM "${TX_SCHEMA_TABLE}" WHERE tx_id = ?`, txID)
  }
}

/**
 * roll back a journaled transaction: restore every snapshotted table to its
 * pre-tx contents (parents before children), drop tables that did not exist at
 * first write (null snapshot), then clean up. triggers on restored tables are
 * detached during the restore and re-created after, so restore DML doesn't
 * re-fire change tracking. must run inside an atomic storage transaction.
 */
export function rollbackTxJournal(sql: DurableSqlStorage, txID: string): void {
  const schema = schemaRows(sql, txID)
  const rows = manifestTableExists(sql) ? manifestRows(sql, txID).reverse() : []
  if (schema.some((row) => row.type === 'marker')) {
    restoreSchemaSnapshot(sql, txID, schema, rows)
    return
  }
  if (rows.length === 0) {
    if (schema.length > 0) {
      sql.exec(`DELETE FROM "${TX_SCHEMA_TABLE}" WHERE tx_id = ?`, txID)
    }
    return
  }

  // A snapshotted table can be gone by the time its row rollback runs: a schema
  // restore in the same recovery drops created temp tables, and an earlier
  // partial recovery may already have dropped one. Its rows cannot and need not
  // be restored into a table that no longer exists — that restore is a no-op —
  // but its snapshot table and manifest row must still be cleaned up below.
  // Without this guard the DELETE/INSERT throws "no such table" and wedges the
  // durable object on every wake (prod token-usage rebuild, 2026-07-22).
  const allSnapshotRows = rows.filter(
    (row) => row.snapshot !== null && row.snapshot !== ''
  )
  const restorableRows = allSnapshotRows.filter((row) => tableExists(sql, row.original))
  const restoredTables = restorableRows.map((row) => row.original)
  const triggers = suspendTriggers(sql, restoredTables)
  // Defer constraint checks across the whole atomic restore. Delete every
  // snapshotted table before inserting any snapshot rows: interleaving those
  // phases lets a later DELETE in a cyclic cascade erase an earlier restore.
  sql.exec('PRAGMA defer_foreign_keys = ON')
  const snapshotRows = parentFirst(sql, restorableRows)
  for (const row of snapshotRows) {
    sql.exec(`DELETE FROM ${quoteIdent(row.original)}`)
  }
  for (const row of snapshotRows) {
    const quotedTable = quoteIdent(row.original)
    const quotedSnapshot = quoteIdent(row.snapshot!)
    // `SELECT *` also selects generated columns, which SQLite refuses to let
    // any INSERT name, so restore the writable columns explicitly.
    const columns = writableColumns(sql, row.original)
    const columnList = columns.map(quoteIdent).join(', ')
    sql.exec(
      `INSERT OR REPLACE INTO ${quotedTable} (${columnList}) SELECT ${columnList} FROM ${quotedSnapshot}`
    )
  }
  // Drop every snapshot table, including those whose original was gone: the
  // snapshot is dead weight once the tx is being rolled back either way.
  for (const row of allSnapshotRows) {
    sql.exec(`DROP TABLE IF EXISTS ${quoteIdent(row.snapshot!)}`)
  }
  for (const row of rows) {
    if (row.snapshot === null) {
      sql.exec(`DROP TABLE IF EXISTS ${quoteIdent(row.original)}`)
    }
    // Empty-string snapshots are row-journaled tables. Their before-images
    // were restored by the owner's CDC undo callback before this function.
  }
  restoreTriggers(sql, triggers)
  sql.exec(`DELETE FROM "${TX_MANIFEST_TABLE}" WHERE tx_id = ?`, txID)
  if (schema.length > 0) {
    sql.exec(`DELETE FROM "${TX_SCHEMA_TABLE}" WHERE tx_id = ?`, txID)
  }
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
  table: string,
  owner = 'default'
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
              `INSERT INTO "${TX_MANIFEST_TABLE}" (tx_id, owner, original, snapshot) VALUES (?, ?, ?, '') RETURNING seq`,
              txID,
              owner,
              table
            )
            .toArray()[0]?.seq
        )
  const snapshot = `_orez_tx_undo_${seq}`
  sql.exec(`DROP TABLE IF EXISTS ${quoteIdent(snapshot)}`)
  sql.exec(`CREATE TABLE ${quoteIdent(snapshot)} AS SELECT * FROM ${quoteIdent(table)}`)
  sql.exec(`UPDATE "${TX_MANIFEST_TABLE}" SET snapshot = ? WHERE seq = ?`, snapshot, seq)
}

const TRIGGER_WRITE =
  /\b(?:INSERT(?:\s+OR\s+\w+)?\s+INTO|REPLACE(?:\s+OR\s+\w+)?\s+INTO|UPDATE(?:\s+OR\s+\w+)?|DELETE\s+FROM)\s+(?:"((?:[^"]|"")+)"|`((?:[^`]|``)+)`|\[([^\]]+)\]|([A-Za-z_][\w$]*))(?:\s*\.\s*(?:"((?:[^"]|"")+)"|`((?:[^`]|``)+)`|\[([^\]]+)\]|([A-Za-z_][\w$]*)))?/i

function triggerWriteTargets(sql: string | null): string[] | null {
  if (!sql) return null
  const begin = sql.search(/\bBEGIN\b/i)
  const end = sql.search(/\bEND\s*$/i)
  if (begin < 0 || end < begin) return null
  const targets: string[] = []
  for (const statement of sql.slice(begin + 5, end).split(';')) {
    if (!/\b(?:INSERT|REPLACE|UPDATE|DELETE)\b/i.test(statement)) continue
    const match = TRIGGER_WRITE.exec(statement)
    if (!match) return null
    const target =
      match[5] ??
      match[6] ??
      match[7] ??
      match[8] ??
      match[1] ??
      match[2] ??
      match[3] ??
      match[4]
    if (!target) return null
    targets.push(target.replaceAll('""', '"').replaceAll('``', '`'))
  }
  return targets
}

// sqlite's built-in identifier comparison folds ASCII case only.
function sqliteNoCase(identifier: string): string {
  return identifier.replace(/[A-Z]/g, (character) => character.toLowerCase())
}

/**
 * snapshot the source table and the transitive targets of its business
 * triggers and cascading/SET foreign keys. trigger SQL and FK metadata name
 * every table SQLite can mutate implicitly, so copying unrelated published
 * tables only multiplies billable writes during zero-cache startup. if a
 * reachable trigger cannot be understood, retain the conservative all-table
 * fallback.
 */
export function snapshotSideEffectWriteTables(
  sql: DurableSqlStorage,
  txID: string,
  sourceTable: string
): boolean {
  const relations = sql
    .exec(
      "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') " +
        "AND name NOT GLOB 'sqlite_*' AND name NOT GLOB '_cf_*' ORDER BY name"
    )
    .toArray()
    .map((row) => ({ name: String(row.name ?? ''), type: String(row.type ?? '') }))
    .filter((relation) => relation.name)
  const knownRelations = new Set(relations.map((relation) => sqliteNoCase(relation.name)))
  const snapshotTables = new Map(
    relations
      .filter(
        (relation) =>
          relation.type === 'table' &&
          !relation.name.startsWith('_orez_') &&
          !relation.name.startsWith('_zero_')
      )
      .map((relation) => [sqliteNoCase(relation.name), relation.name])
  )
  const edges = new Map<string, Set<string>>()
  const unsafeTriggerSources = new Set<string>()
  const addEdge = (from: string, to: string) => {
    const fromKey = sqliteNoCase(from)
    const targets = edges.get(fromKey) ?? new Set<string>()
    targets.add(sqliteNoCase(to))
    edges.set(fromKey, targets)
  }

  const triggers = sql
    .exec(
      "SELECT tbl_name, sql FROM sqlite_master WHERE type = 'trigger' " +
        "AND name NOT GLOB '_orez_cdc_*'"
    )
    .toArray()
  for (const row of triggers) {
    const from = String(row.tbl_name ?? '')
    const targets = triggerWriteTargets(
      row.sql === null || row.sql === undefined ? null : String(row.sql)
    )
    if (!targets) unsafeTriggerSources.add(sqliteNoCase(from))
    else {
      for (const target of targets) {
        if (!knownRelations.has(sqliteNoCase(target))) {
          unsafeTriggerSources.add(sqliteNoCase(from))
        } else {
          addEdge(from, target)
        }
      }
    }
  }

  for (const child of relations) {
    if (child.type !== 'table') continue
    for (const row of sql
      .exec(`PRAGMA foreign_key_list(${quoteIdent(child.name)})`)
      .toArray()) {
      const parent = String(row.table ?? '')
      const hasAction = [row.on_update, row.on_delete].some((action) => {
        const normalized = String(action ?? 'NO ACTION').toUpperCase()
        return normalized !== 'NO ACTION' && normalized !== 'RESTRICT'
      })
      if (parent && hasAction) addEdge(parent, child.name)
    }
  }

  const reachable = new Set<string>()
  const pending = [sqliteNoCase(sourceTable)]
  let hasSideEffect = false
  let mustSnapshotAll = false
  while (pending.length > 0) {
    const table = pending.pop()!
    if (reachable.has(table)) continue
    reachable.add(table)
    if (unsafeTriggerSources.has(table)) mustSnapshotAll = true
    for (const target of edges.get(table) ?? []) {
      hasSideEffect = true
      pending.push(target)
    }
  }
  if (!hasSideEffect && !mustSnapshotAll) return false

  const selected = mustSnapshotAll
    ? [...snapshotTables.values()]
    : [...reachable]
        .map((table) => snapshotTables.get(table))
        .filter((table): table is string => table !== undefined)
  for (const table of selected.sort()) upgradeToTableSnapshot(sql, txID, table)
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
  const transactionIDs = new Set<string>()
  for (const table of [TX_MANIFEST_TABLE, TX_SCHEMA_TABLE]) {
    const exists =
      table === TX_MANIFEST_TABLE ? manifestTableExists(sql) : schemaTableExists(sql)
    if (!exists) continue
    const txRows =
      owner === undefined
        ? sql.exec(`SELECT DISTINCT tx_id FROM ${quoteIdent(table)}`).toArray()
        : sql
            .exec(
              `SELECT DISTINCT tx_id FROM ${quoteIdent(table)} WHERE owner = ?`,
              owner
            )
            .toArray()
    for (const row of txRows) transactionIDs.add(String(row.tx_id))
  }
  for (const txID of transactionIDs) {
    beforeRollback?.(txID)
    rollbackTxJournal(sql, txID)
    recovered.push(txID)
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
    .filter((name) => name !== TX_SCHEMA_TABLE && !referenced.has(name))
  for (const name of orphanTables) dropTable(sql, name)

  return recovered
}
