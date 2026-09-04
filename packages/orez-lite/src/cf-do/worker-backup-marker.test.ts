/**
 * The backup marker is the fence a namespace export runs behind: the export
 * reads it once and requires every chunk of the scan to observe the same value,
 * abandoning the whole dump when it moves. It is also what the scheduled sweep
 * skips a namespace on.
 *
 * Both readings mean "the data changed". The marker used to be bumped for any
 * statement that COULD write, which is a different question and the one the
 * transaction journal asks before a statement runs. Production's control plane
 * was taking a marker bump roughly every five seconds from statements that
 * changed nothing, so its export tore on every attempt and it went 22 hours with
 * no backup while its contents were sitting still (2026-09-04: three consecutive
 * scans torn at markers 767255, 767256 and 767257, one increment apiece, against
 * sampled commits reporting rowsChanged=0).
 *
 * Runs against real SQLite. `changes` is the whole point here, and a mocked SQL
 * layer would be free to report whatever makes the test pass.
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
  zero.requestsSinceBoot = {
    fetch: 0,
    applicationSqlSessions: 0,
    applicationSqlReadSessions: 0,
    applicationSqlWriteSessions: 0,
    sqlStatements: 0,
  }
  zero.writeGrantWaitMs = { record() {} }
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
  // the marker only ever moves through this hook, so recording its second
  // argument records exactly what the fence would have been told.
  const changedData: boolean[] = []
  zero.applicationSqlDidCommit = (_published: boolean, changed: boolean) => {
    changedData.push(changed)
  }
  return { ...storage, zero, changedData }
}

async function runSession(
  core: Awaited<ReturnType<typeof createWorkerCore>>,
  id: string,
  work: (session: any) => Promise<void>
) {
  const session = await core.zero.applicationSqlSession(id)
  await session.begin()
  await work(session)
  await session.commit()
}

describe('backup marker', () => {
  it('does not move for a row mutation that matched nothing', async () => {
    const core = await createWorkerCore()
    core.sql.exec('CREATE TABLE todo (id TEXT PRIMARY KEY, title TEXT)')
    core.sql.exec(`INSERT INTO todo VALUES ('t1', 'first')`)

    await runSession(core, 'no-op-update', async (session) => {
      const result = await session.exec(
        `UPDATE todo SET title = 'x' WHERE id = 'absent'`
      )
      // the premise: SQLite really did change nothing. without this the
      // assertion below could pass for the wrong reason.
      expect(result.changes).toBe(0)
    })

    expect(core.changedData).toEqual([false])
  })

  it('moves for a row mutation that changed a row', async () => {
    const core = await createWorkerCore()
    core.sql.exec('CREATE TABLE todo (id TEXT PRIMARY KEY, title TEXT)')
    core.sql.exec(`INSERT INTO todo VALUES ('t1', 'first')`)

    await runSession(core, 'real-update', async (session) => {
      const result = await session.exec(`UPDATE todo SET title = 'x' WHERE id = 't1'`)
      expect(result.changes).toBe(1)
    })

    expect(core.changedData).toEqual([true])
  })

  it('moves for a schema change even though no row moved', async () => {
    const core = await createWorkerCore()

    await runSession(core, 'ddl', async (session) => {
      await session.exec('CREATE TABLE note (id TEXT PRIMARY KEY)')
    })

    // the dump carries every CREATE statement, so a namespace that grew a table
    // is a different database and the sweep must not skip it.
    expect(core.changedData).toEqual([true])
  })

  it('moves when any one statement in the session changed something', async () => {
    const core = await createWorkerCore()
    core.sql.exec('CREATE TABLE todo (id TEXT PRIMARY KEY, title TEXT)')
    core.sql.exec(`INSERT INTO todo VALUES ('t1', 'first')`)

    await runSession(core, 'mixed', async (session) => {
      await session.exec(`UPDATE todo SET title = 'x' WHERE id = 'absent'`)
      await session.exec(`UPDATE todo SET title = 'y' WHERE id = 't1'`)
      await session.exec(`DELETE FROM todo WHERE id = 'absent'`)
    })

    expect(core.changedData).toEqual([true])
  })

  it('does not move for a session that only read', async () => {
    const core = await createWorkerCore()
    core.sql.exec('CREATE TABLE todo (id TEXT PRIMARY KEY, title TEXT)')

    await runSession(core, 'read-only-work', async (session) => {
      await session.query('SELECT * FROM todo')
    })

    expect(core.changedData).toEqual([false])
  })
})
