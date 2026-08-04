/**
 * A delegated push canceled mid-transaction (the sync host's per-attempt
 * timeout aborts the app worker invocation) never reaches commit or rollback;
 * only the RPC stub disposal arrives at the durable object. Every write the
 * abandoned session made must be undone by that disposal — including the
 * packed ledger's captureMode toggle, which the executor writes through plain
 * SQL with no metadata. When that write survived, every later push failed
 * preparePackedLedger with "packed ledger has uncommitted capture state" and
 * the namespace never recovered (production example apps, 2026-08-03).
 *
 * Runs against real SQLite: the defect is trigger-capture coverage, and a
 * mocked SQL layer would assert the bug back into existence.
 */

// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { describe, expect, it, vi } from 'vitest'

import { RollingRowWriteBudget } from '../do-sql-tracking.js'
import { TransactionalCdc } from './cdc.js'
import { DurableWatermarkState } from './watermark.js'

vi.mock('cloudflare:workers', () => ({
  DurableObject: class {
    ctx: unknown
    constructor(ctx: unknown) {
      this.ctx = ctx
    }
  },
  RpcTarget: class {},
}))

const BetterSqlite3 = BedrockSqlite.Database

function createSqliteStorage() {
  const nativeDb = new BetterSqlite3(':memory:')
  const exec = (sql: string, ...params: unknown[]) => {
    const stmt = nativeDb.prepare(sql)
    let rows: Array<Record<string, unknown>> = []
    let rowsWritten = 0
    if (stmt.reader) {
      rows = stmt.all(...params)
    } else {
      rowsWritten = Number(stmt.run(...params).changes)
    }
    return {
      toArray: () => rows,
      one: () => rows[0],
      columnNames: stmt.reader ? stmt.columns().map((column: any) => column.name) : [],
      rowsWritten,
    }
  }
  return { nativeDb, sql: { exec } }
}

async function createWorkerCore() {
  const { ZeroDO } = await import('./worker.js')
  const storage = createSqliteStorage()
  const zero = Object.create(ZeroDO.prototype) as any
  zero.sql = storage.sql
  zero.cdc = new TransactionalCdc(storage.sql)
  zero.watermarks = new DurableWatermarkState(storage.sql)
  zero.writeBudget = new RollingRowWriteBudget({
    budgetRows: 300_000,
    windowMs: 300_000,
    now: () => 1,
  })
  zero.tableSchemas = new Map()
  zero.schemaTables = new Set<string>()
  zero.pendingChangesSchemaReady = false
  zero.applicationSqlWriter = null
  zero.applicationSqlReaders = new Set()
  zero.applicationSqlQueue = []
  const runTransaction = <T>(work: () => T): T => {
    storage.nativeDb.exec('BEGIN')
    try {
      const result = work()
      storage.nativeDb.exec('COMMIT')
      return result
    } catch (error) {
      storage.nativeDb.exec('ROLLBACK')
      throw error
    }
  }
  zero.ctx = {
    storage: {
      transaction: async <T>(work: () => T) => runTransaction(work),
      transactionSync: runTransaction,
    },
  }
  return { ...storage, zero }
}

/** The exact shape initializePackedLedger creates, with its baseline segment. */
function createPackedLedger(sql: { exec: (s: string, ...p: unknown[]) => any }) {
  sql.exec(`CREATE TABLE "_zsync_log_segments" (
    "startVersion" INTEGER PRIMARY KEY,
    "endVersion" INTEGER NOT NULL,
    "payload" TEXT NOT NULL,
    "pending" TEXT NOT NULL,
    "captureMode" INTEGER NOT NULL CHECK ("captureMode" IN (0, 1))
  ) WITHOUT ROWID`)
  sql.exec(
    `INSERT INTO "_zsync_log_segments"
       ("startVersion", "endVersion", "payload", "pending", "captureMode")
     VALUES (1, 0, ?, '[]', 0)`,
    JSON.stringify({ format: 1, lmids: {}, transactions: [] })
  )
}

function activeSegment(sql: { exec: (s: string, ...p: unknown[]) => any }) {
  return sql
    .exec(
      `SELECT "captureMode", "pending" FROM "_zsync_log_segments"
       ORDER BY "startVersion" DESC LIMIT 1`
    )
    .one() as { captureMode: number; pending: string }
}

describe('abandoned application session and the packed ledger', () => {
  it('restores captureMode when the session is disposed without commit', async () => {
    const core = await createWorkerCore()
    core.sql.exec('CREATE TABLE todo (id TEXT PRIMARY KEY, title TEXT)')
    createPackedLedger(core.sql)

    const session = await core.zero.applicationSqlSession('push-1')
    await session.begin()
    await session.registerTables([{ table: 'todo', publicTable: 'todo' }])
    // the sequence a delegated push runs: setPackedCaptureMode toggles the
    // ledger through plain sql with no metadata, then the application write.
    await session.exec(
      `UPDATE "_zsync_log_segments" SET "captureMode" = 1
       WHERE "startVersion" = (SELECT MAX("startVersion") FROM "_zsync_log_segments")`
    )
    await session.exec(`INSERT INTO todo (id, title) VALUES ('t1', 'seeded')`)

    // the wedge condition must actually exist before the abandonment, or this
    // test is vacuous by fixture.
    expect(Number(activeSegment(core.sql).captureMode)).toBe(1)

    // a canceled invocation never runs commit() or rollback(); workerd only
    // disposes the RPC stub.
    session[Symbol.dispose]()

    const segment = activeSegment(core.sql)
    expect(Number(segment.captureMode)).toBe(0)
    expect(segment.pending).toBe('[]')
    expect(Number(core.sql.exec('SELECT COUNT(*) AS c FROM todo').one().c)).toBe(0)
  })

  it('keeps a committed session intact', async () => {
    const core = await createWorkerCore()
    core.sql.exec('CREATE TABLE todo (id TEXT PRIMARY KEY, title TEXT)')
    createPackedLedger(core.sql)

    const session = await core.zero.applicationSqlSession('push-2')
    await session.begin()
    await session.registerTables([{ table: 'todo', publicTable: 'todo' }])
    await session.exec(
      `UPDATE "_zsync_log_segments" SET "captureMode" = 1
       WHERE "startVersion" = (SELECT MAX("startVersion") FROM "_zsync_log_segments")`
    )
    await session.exec(`INSERT INTO todo (id, title) VALUES ('t1', 'seeded')`)
    // commitPackedLedger ends every successful push with the mode cleared.
    await session.exec(
      `UPDATE "_zsync_log_segments" SET "captureMode" = 0
       WHERE "startVersion" = (SELECT MAX("startVersion") FROM "_zsync_log_segments")`
    )
    await session.commit()

    expect(Number(activeSegment(core.sql).captureMode)).toBe(0)
    expect(Number(core.sql.exec('SELECT COUNT(*) AS c FROM todo').one().c)).toBe(1)
  })
})
