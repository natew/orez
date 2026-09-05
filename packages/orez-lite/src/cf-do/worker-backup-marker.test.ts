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
      const result = await session.exec(`UPDATE todo SET title = 'x' WHERE id = 'absent'`)
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

describe('physical backup snapshots', () => {
  it('copies the rolled-back state and recovers after failed cleanup admission', async () => {
    const core = await createWorkerCore()
    core.nativeDb.exec(
      "CREATE TABLE item (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO item VALUES (1, 'before')"
    )
    const setup = await core.zero.applicationSqlSession('register-item')
    await setup.begin()
    await setup.registerTables([{ table: 'item', publicTable: 'item' }])
    await setup.commit()
    const writer = await core.zero.applicationSqlSession('rollback-writer')
    await writer.begin()
    await writer.exec("UPDATE item SET value = 'dirty' WHERE id = 1")
    const pending = core.zero.backupSnapshot({
      markerTable: 'missing_marker',
      excludedTables: [],
    })
    // admission has to queue before rollback: early copying would retain dirty.
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(core.zero.applicationSqlQueue).toHaveLength(1)
    await writer.rollback()
    const first = await pending
    expect(
      core.nativeDb.prepare(`SELECT c1 AS value FROM "_orez_bk_${first.id}_item"`).get()
    ).toEqual({ value: 'before' })
    const admission = vi
      .spyOn(core.zero, 'withLocalApplicationSqlSession')
      .mockRejectedValueOnce(new Error('turn timeout'))
    await expect(core.zero.backupSnapshotDrop(first.id)).rejects.toThrow('turn timeout')
    admission.mockRestore()
    const second = await core.zero.backupSnapshot({
      markerTable: 'missing_marker',
      excludedTables: [],
    })
    expect(second.id).not.toBe(first.id)
    expect(() =>
      core.nativeDb.prepare(`SELECT * FROM "_orez_bk_${first.id}_item"`)
    ).toThrow('no such table')
    await core.zero.backupSnapshotDrop(first.id)
    expect(
      core.nativeDb.prepare(`SELECT c1 AS value FROM "_orez_bk_${second.id}_item"`).get()
    ).toEqual({ value: 'before' })
    await core.zero.backupSnapshotDrop(second.id)
    core.nativeDb.close()
  })

  it('waits for writer commit, preserves copied rows, and does not advance the marker', async () => {
    const core = await createWorkerCore()
    core.nativeDb.exec(
      "CREATE TABLE item (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO item VALUES (1, 'before'); CREATE TABLE marker (id INTEGER, write_seq INTEGER); INSERT INTO marker VALUES (1, 7)"
    )
    const writer = await core.zero.applicationSqlSession('open-writer')
    await writer.begin()
    await writer.exec("UPDATE item SET value = 'committed' WHERE id = 1")
    expect(core.nativeDb.prepare('SELECT value FROM item').get()).toEqual({
      value: 'committed',
    })
    let finished = false
    const pending = core.zero
      .backupSnapshot({
        markerTable: 'marker',
        excludedTables: ['marker'],
      })
      .then((snapshot: unknown) => {
        finished = true
        return snapshot
      })
    await Promise.resolve()
    expect(finished).toBe(false)
    await writer.commit()
    const commitsBefore = core.changedData.filter(Boolean).length
    const snapshot: any = await pending
    expect(snapshot).toMatchObject({
      marker: 7,
      tables: expect.arrayContaining(['item']),
    })
    expect(
      core.nativeDb
        .prepare(`SELECT c1 AS value FROM "_orez_bk_${snapshot.id}_item"`)
        .get()
    ).toEqual({
      value: 'committed',
    })
    expect(
      core.nativeDb
        .prepare(
          "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name GLOB '_orez_bk_*_item'"
        )
        .all()
    ).toEqual([])
    await expect(
      core.zero.backupSnapshot({
        markerTable: 'marker',
        excludedTables: [],
      })
    ).rejects.toThrow('already active')
    await core.zero.backupSnapshotDrop('wrong-export')
    expect(
      core.nativeDb
        .prepare(`SELECT c1 AS value FROM "_orez_bk_${snapshot.id}_item"`)
        .get()
    ).toEqual({
      value: 'committed',
    })
    await core.zero.backupSnapshotDrop(snapshot.id)
    expect(core.changedData.filter(Boolean)).toHaveLength(commitsBefore)
    expect(
      core.nativeDb
        .prepare("SELECT name FROM sqlite_master WHERE name GLOB '_orez_bk_*'")
        .all()
    ).toEqual([])
    core.nativeDb.close()
  })
})
