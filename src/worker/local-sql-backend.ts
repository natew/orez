/**
 * embed-local SQL backend: serves the DoBackend HTTP protocol (/exec, /batch,
 * /commit-tx, /rollback-tx) directly against the Durable Object's OWN SQLite
 * storage, with no cross-DO hop.
 *
 * zero-cache's CVR and change DBs are private state of the view-syncer /
 * change-streamer running inside ZeroCacheDO — nothing else reads them. wiring
 * their pg sessions to the remote ZeroSqlDO turned every CVR statement into a
 * cross-DO HTTP round-trip (~270 POST /exec per IDE hydration measured live);
 * keeping them local makes those statements synchronous storage calls while
 * the DoBackend pg semantics stay byte-identical (same SQL translation, same
 * tx journal, same endpoints).
 *
 * the upstream `postgres` db must NOT use this backend: it is shared with the
 * app worker and lives in ZeroSqlDO.
 */

import { TransactionalCdc } from '../cf-do/cdc.js'
import {
  appendPendingChange,
  deletePendingChanges,
  ensurePendingChangesTable,
  rollbackPendingChanges,
} from '../cf-do/row-undo.js'
import {
  commitTxJournal,
  recoverTxJournal,
  rollbackTxJournal,
  snapshotSideEffectWriteTables,
  snapshotTxSchema,
  upgradeToTableSnapshot,
} from '../cf-do/tx-journal.js'

import type { DurableSqlStorage } from '../cf-do/watermark.js'

export interface LocalDoSqlite {
  exec(
    sql: string,
    ...params: unknown[]
  ): {
    toArray(): Array<Record<string, unknown>>
    columnNames?: string[]
  }
  transactionSync<T>(fn: () => T): T
}

export interface LocalSqlBackend {
  /** drop-in for DoBackend's `fetch` option. */
  fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>
  /**
   * roll back every journaled transaction left by a dead process generation.
   * safe whenever the caller is the storage's only client and no transaction
   * of its own is open (e.g. embed boot, before any pg session exists).
   */
  recoverOrphanedTransactions(): string[]
}

interface SqlStatement {
  sql: string
  params?: unknown[]
  transactionID?: string
  track?: {
    physicalTableName?: string
    tableName: string
    operation: 'INSERT' | 'UPDATE' | 'DELETE'
    rowColumns?: string[]
    returnRows?: boolean
    transactionID?: string
    publish?: boolean
  }
}

export function createLocalSqlBackend(storage: unknown): LocalSqlBackend {
  const sqlite = storage as Partial<LocalDoSqlite> | null | undefined
  if (!sqlite || typeof sqlite.exec !== 'function') {
    throw new Error('local sql backend: storage must expose exec()')
  }
  if (typeof sqlite.transactionSync !== 'function') {
    throw new Error('local sql backend: storage must expose transactionSync()')
  }
  const sql = sqlite as LocalDoSqlite
  const journalSql: DurableSqlStorage = {
    exec(query: string, ...params: unknown[]) {
      // DO cursors are one-shot; materialize so toArray()/one() both work.
      const rows = sql.exec(query, ...params).toArray()
      return {
        toArray: () => rows,
        one: () => rows[0],
      }
    },
  }
  const cdc = new TransactionalCdc(journalSql)
  let pendingChangesReady = false

  function ensurePendingChanges() {
    if (pendingChangesReady) return
    ensurePendingChangesTable(journalSql)
    pendingChangesReady = true
  }

  /**
   * Run work in a storage transaction, re-deriving the in-memory schema caches
   * from SQLite if it aborts. transactionSync() rolls the SQLite side back on
   * throw, but the caches are plain fields that would keep asserting state
   * SQLite no longer has: a CDC table stays "registered and verified" with no
   * trigger left on disk, and every later write to it goes silently uncaptured.
   */
  function atomically<T>(work: () => T): T {
    try {
      return sql.transactionSync(work)
    } catch (error) {
      pendingChangesReady = false
      // Reload is intentionally last: corrupt CDC metadata must throw rather
      // than suppressing cache invalidation and silently disabling capture.
      cdc.reload()
      throw error
    }
  }

  function executeStatement(statement: SqlStatement) {
    const track = statement.track
    const transactionID = statement.transactionID || track?.transactionID
    if (track && !track.physicalTableName) {
      throw new Error('local sql backend: change tracking requires a physical table name')
    }
    let snapshotsOwnStatement = false
    let captures = false
    if (track) {
      if (!transactionID) {
        throw new Error('local sql backend: rollback tracking requires a transaction id')
      }
      captures = cdc.ensureTable({
        physicalTableName: track.physicalTableName!,
        tableName: track.tableName,
        publish: false,
        ...(track.rowColumns?.length ? { columns: track.rowColumns } : null),
      })
      // DoBackend already marked this table row-journaled, betting the DO could
      // capture before/after images for it. When it cannot, that marker promises
      // a rollback nothing can perform, so take the table snapshot the journal
      // would otherwise have taken, before the DML changes the table.
      if (!captures) {
        upgradeToTableSnapshot(journalSql, transactionID, track.physicalTableName!)
      }
      snapshotsOwnStatement = snapshotSideEffectWriteTables(
        journalSql,
        transactionID,
        track.physicalTableName!
      )
    }
    const suspended = cdc.beginSchemaChange(statement.sql)
    let cursor: ReturnType<LocalDoSqlite['exec']>
    try {
      cursor = sql.exec(statement.sql, ...(statement.params ?? []))
    } finally {
      cdc.finishSchemaChange(suspended)
    }
    const rows = cursor.toArray().map((row) => ({ ...row }))
    const columns = Array.isArray(cursor.columnNames)
      ? cursor.columnNames
      : rows.length > 0
        ? Object.keys(rows[0])
        : []
    // Rollback-only triggers remain installed after a transaction ends. Drain
    // them on every statement so an untracked autocommit write cannot sit in
    // the buffer and get attached to (then undone by) the next transaction.
    const captured = cdc.active ? cdc.drain() : []
    if (track && captured.length > 0) {
      ensurePendingChanges()
      for (const change of captured) {
        appendPendingChange(journalSql, {
          transactionID: transactionID!,
          physicalTableName: change.physicalTableName,
          tableName: change.tableName,
          publish: false,
          op: change.op,
          rowData: change.rowData,
          oldData: change.oldData,
          rowJournal: change.rowJournal,
          oldJournal: change.oldJournal,
          newRowid: change.newRowid,
          oldRowid: change.oldRowid,
          undoable: !snapshotsOwnStatement,
        })
      }
    }
    return track
      ? {
          rows: track.returnRows ? rows : [],
          columns: track.returnRows ? columns : [],
          affectedRows: rows.length,
          capturedChanges: 0,
        }
      : { rows, columns }
  }

  async function handle(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const body = (await request.json().catch(() => ({}))) as {
      sql?: string
      params?: unknown[]
      track?: SqlStatement['track']
      statements?: Array<string | SqlStatement>
      transactionID?: unknown
      owner?: unknown
      affectedTables?: unknown
    }
    switch (url.pathname) {
      case '/exec': {
        const statement = {
          sql: String(body.sql ?? ''),
          params: Array.isArray(body.params) ? body.params : [],
          track: body.track,
          transactionID: String(body.transactionID || '') || undefined,
        }
        const result = body.track
          ? atomically(() => executeStatement(statement))
          : executeStatement(statement)
        return Response.json(result)
      }
      case '/batch': {
        const statements = Array.isArray(body.statements) ? body.statements : []
        const results = atomically(() =>
          statements
            .map((statement) =>
              typeof statement === 'string' ? { sql: statement } : statement
            )
            .filter((statement) => statement?.sql?.trim())
            .map((statement) => executeStatement(statement))
        )
        return Response.json({ results })
      }
      case '/snapshot-tx-schema': {
        const transactionID = String(body.transactionID || '')
        if (!transactionID) throw new Error('missing transactionID')
        atomically(() =>
          snapshotTxSchema(
            journalSql,
            transactionID,
            String(body.owner || 'default'),
            Array.isArray(body.affectedTables) ? body.affectedTables.map(String) : []
          )
        )
        return Response.json({ ok: true })
      }
      case '/commit-tx': {
        const transactionID = String(body.transactionID || '')
        if (!transactionID) throw new Error('missing transactionID')
        atomically(() => {
          deletePendingChanges(journalSql, transactionID)
          commitTxJournal(journalSql, transactionID)
        })
        return Response.json({ ok: true })
      }
      case '/rollback-tx': {
        const transactionID = String(body.transactionID || '')
        if (!transactionID) throw new Error('missing transactionID')
        atomically(() => {
          rollbackPendingChanges(journalSql, transactionID)
          rollbackTxJournal(journalSql, transactionID)
          deletePendingChanges(journalSql, transactionID)
        })
        cdc.reload()
        return Response.json({ ok: true })
      }
      default:
        return Response.json(
          { error: `local sql backend: unknown path ${url.pathname}` },
          { status: 404 }
        )
    }
  }

  return {
    async fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
      const request = new Request(input, init)
      try {
        return await handle(request)
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err)
        return Response.json({ error: message }, { status: 500 })
      }
    },
    recoverOrphanedTransactions(): string[] {
      const recovered = atomically(() =>
        recoverTxJournal(journalSql, undefined, (transactionID) => {
          rollbackPendingChanges(journalSql, transactionID)
          deletePendingChanges(journalSql, transactionID)
        })
      )
      cdc.reload()
      return recovered
    },
  }
}
