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

import {
  commitTxJournal,
  recoverTxJournal,
  rollbackTxJournal,
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
  track?: unknown
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

  function executeStatement(statement: SqlStatement) {
    if (statement.track) {
      // change tracking belongs to the shared upstream db (ZeroSqlDO); the
      // local cvr/cdb stores are never replicated.
      throw new Error('local sql backend: change tracking is not supported')
    }
    const cursor = sql.exec(statement.sql, ...(statement.params ?? []))
    const rows = cursor.toArray().map((row) => ({ ...row }))
    const columns = Array.isArray(cursor.columnNames)
      ? cursor.columnNames
      : rows.length > 0
        ? Object.keys(rows[0])
        : []
    return { rows, columns }
  }

  async function handle(request: Request): Promise<Response> {
    const url = new URL(request.url)
    const body = (await request.json().catch(() => ({}))) as {
      sql?: string
      params?: unknown[]
      track?: unknown
      statements?: Array<string | SqlStatement>
      transactionID?: unknown
    }
    switch (url.pathname) {
      case '/exec': {
        const result = executeStatement({
          sql: String(body.sql ?? ''),
          params: Array.isArray(body.params) ? body.params : [],
          track: body.track,
        })
        return Response.json(result)
      }
      case '/batch': {
        const statements = Array.isArray(body.statements) ? body.statements : []
        const results = sql.transactionSync(() =>
          statements
            .map((statement) =>
              typeof statement === 'string' ? { sql: statement } : statement
            )
            .filter((statement) => statement?.sql?.trim())
            .map((statement) => executeStatement(statement))
        )
        return Response.json({ results })
      }
      case '/commit-tx': {
        const transactionID = String(body.transactionID || '')
        if (!transactionID) throw new Error('missing transactionID')
        sql.transactionSync(() => commitTxJournal(journalSql, transactionID))
        return Response.json({ ok: true })
      }
      case '/rollback-tx': {
        const transactionID = String(body.transactionID || '')
        if (!transactionID) throw new Error('missing transactionID')
        sql.transactionSync(() => rollbackTxJournal(journalSql, transactionID))
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
      return sql.transactionSync(() => recoverTxJournal(journalSql))
    },
  }
}
