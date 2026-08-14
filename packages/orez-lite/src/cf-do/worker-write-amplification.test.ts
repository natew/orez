import { createSchema, number, string, table } from '@rocicorp/zero'
// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { describe, expect, it, vi } from 'vitest'

import { count, defineAggregates, aggregateMigrationStatements } from '../aggregate.js'
import { RollingRowWriteBudget } from '../do-sql-tracking.js'
import { TransactionalCdc } from './cdc.js'
import { beginTxJournal, commitTxJournal, TX_MANIFEST_DDL } from './tx-journal.js'
import { DurableWatermarkState } from './watermark.js'

vi.mock('cloudflare:workers', () => ({
  DurableObject: class {
    constructor(ctx: unknown) {
      this.ctx = ctx
    }
  },
  RpcTarget: class {},
}))

const BetterSqlite3 = BedrockSqlite.Database

/**
 * Cloudflare bills every row a statement writes, including the ones its
 * triggers write, and that is the number this file is about. SQLite's
 * `total_changes()` counts trigger writes too, so it is the right meter -- with
 * one hole: `CREATE TABLE ... AS SELECT` writes its rows without touching the
 * counter, and that statement IS the table-snapshot cost. Count those off the
 * table it just created or the meter reads zero for the only thing worth
 * measuring.
 */
function createSqliteStorage() {
  const nativeDb = new BetterSqlite3(':memory:')
  const written: { sql: string; rows: number }[] = []
  let counting = false
  const totalChanges = () =>
    Number(nativeDb.prepare('SELECT total_changes() AS c').get().c)
  const exec = (sql: string, ...params: unknown[]) => {
    const before = totalChanges()
    const stmt = nativeDb.prepare(sql)
    let rows: Array<Record<string, unknown>> = []
    let rowsWritten = 0
    if (stmt.reader) {
      rows = stmt.all(...params)
    } else {
      rowsWritten = Number(stmt.run(...params).changes)
    }
    let delta = totalChanges() - before
    const created = /^\s*CREATE TABLE\s+"([^"]+)"\s+AS SELECT/i.exec(sql)
    if (created) {
      delta += Number(
        nativeDb.prepare(`SELECT COUNT(*) AS c FROM "${created[1]}"`).get().c
      )
    }
    if (counting && delta > 0) written.push({ sql, rows: delta })
    return {
      toArray: () => rows,
      one: () => rows[0],
      columnNames: stmt.reader ? stmt.columns().map((column: any) => column.name) : [],
      rowsWritten,
    }
  }
  return {
    nativeDb,
    sql: { exec },
    written,
    start: () => {
      counting = true
      written.length = 0
    },
    stop: () => {
      counting = false
    },
  }
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
  zero.applicationSqlDidCommit = () => {}
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

/**
 * soot's control namespace as measured on 2026-07-27: which tables exist, which
 * ones the CDC has registered, how many rows the unregistered ones hold, and
 * the `_zsync_changes` journal triggers over the synced tables. The row counts are the
 * point -- an unregistered table only costs what it holds, so a shape with
 * empty tables cannot show the amplification at all.
 */
function buildControlShape(sql: { exec: (s: string, ...p: unknown[]) => any }) {
  sql.exec('CREATE TABLE user (id TEXT PRIMARY KEY, name TEXT)')
  sql.exec(
    'CREATE TABLE project (id TEXT PRIMARY KEY, ownerId TEXT REFERENCES user(id) ON DELETE CASCADE, name TEXT)'
  )
  sql.exec('CREATE TABLE file (id TEXT PRIMARY KEY, projectId TEXT, body TEXT)')
  sql.exec(
    'CREATE TABLE projectMember (id TEXT PRIMARY KEY, projectId TEXT REFERENCES project(id) ON DELETE CASCADE)'
  )
  sql.exec(
    'CREATE TABLE sootsimRun (id TEXT PRIMARY KEY, projectId TEXT REFERENCES project(id) ON DELETE CASCADE)'
  )
  sql.exec('CREATE TABLE __soot_cf_migrations (id INTEGER PRIMARY KEY, name TEXT)')
  sql.exec(
    'CREATE TABLE _zsync_clients ("clientGroupID" TEXT, "clientID" TEXT, "lastMutationID" INTEGER, "userID" TEXT)'
  )
  sql.exec(
    'CREATE TABLE _zsync_changes (watermark INTEGER PRIMARY KEY AUTOINCREMENT, "tableName" TEXT, "op" TEXT, "pk" TEXT)'
  )
  sql.exec(
    'CREATE TABLE _zsync_meta (lock INTEGER PRIMARY KEY, floor INTEGER, initialized INTEGER)'
  )

  const fill = (count: number, insert: (index: number) => string) => {
    for (let index = 0; index < count; index++) sql.exec(insert(index))
  }
  fill(689, (i) => `INSERT INTO __soot_cf_migrations (name) VALUES ('m${i}')`)
  fill(612, (i) => `INSERT INTO _zsync_clients VALUES ('g', 'c${i}', ${i}, 'u')`)
  fill(77, (i) => `INSERT INTO sootsimRun VALUES ('r${i}', NULL)`)
  fill(33, (i) => `INSERT INTO projectMember VALUES ('pm${i}', NULL)`)
  fill(
    4096,
    (i) =>
      `INSERT INTO _zsync_changes ("tableName","op","pk") VALUES ('file','row','${i}')`
  )
  sql.exec('INSERT INTO _zsync_meta VALUES (1, 0, 1)')
  sql.exec("INSERT INTO user VALUES ('u1', 'nate')")
  sql.exec("INSERT INTO project VALUES ('p1', 'u1', 'contrast')")
  sql.exec("INSERT INTO file VALUES ('f1', 'p1', 'hello')")

  for (const table of ['user', 'project', 'file']) {
    const columns = table === 'file' ? ['id', 'projectId', 'body'] : ['id', 'name']
    const rowObject = (ref: 'NEW' | 'OLD') =>
      `json_object(${columns.map((column) => `'${column}', ${ref}."${column}"`).join(', ')})`
    sql.exec(`CREATE TRIGGER "_zsync_tr_${table}_i"
          AFTER INSERT ON "${table}" BEGIN
          INSERT INTO _zsync_changes ("tableName", "op", "pk")
          VALUES ('${table}', 'row', json_object('before', NULL, 'after', ${rowObject('NEW')}));
        END`)
    sql.exec(`CREATE TRIGGER "_zsync_tr_${table}_u"
          AFTER UPDATE ON "${table}" BEGIN
          INSERT INTO _zsync_changes ("tableName", "op", "pk")
          VALUES ('${table}', 'row', json_object('before', ${rowObject('OLD')}, 'after', ${rowObject('NEW')}));
          INSERT INTO _zsync_changes ("tableName", "op", "pk")
          VALUES ('_zsync_meta', 'marker', NULL);
        END`)
    sql.exec(`CREATE TRIGGER "_zsync_tr_${table}_d"
          AFTER DELETE ON "${table}" BEGIN
          INSERT INTO _zsync_changes ("tableName", "op", "pk")
          VALUES ('${table}', 'row', json_object('before', ${rowObject('OLD')}, 'after', NULL));
        END`)
  }
}

async function controlNamespace() {
  const core = await createWorkerCore()
  buildControlShape(core.sql)
  // the DO constructor runs this against every production namespace, so the
  // steady state every other test measures is the post-cleanup shape. the
  // pre-cleanup amplification keeps its own control test below.
  core.zero.dropZeroHttpJournalResidue()
  core.zero.cdc.syncTables([
    { physicalTableName: 'user', tableName: 'user' },
    { physicalTableName: 'project', tableName: 'project' },
    { physicalTableName: 'file', tableName: 'file' },
  ])
  core.zero.cdc.drain()
  core.sql.exec(TX_MANIFEST_DDL)

  /** Bill one synced statement's full lifecycle: journal, DML, capture, commit. */
  const syncedWrite = (
    label: string,
    sql: string,
    track: Record<string, unknown>,
    params: unknown[] = []
  ): { billed: number; snapshots: string[] } => {
    core.start()
    beginTxJournal(core.sql, label, 'application')
    core.zero.executeSQL(sql, params, track, label)
    const snapshots = core.sql
      .exec('SELECT original FROM _orez_tx_manifest WHERE tx_id = ?', label)
      .toArray()
      .map((row: any) => String(row.original))
      .sort()
    core.zero.commitPendingTrackedChanges(label)
    commitTxJournal(core.sql, label)
    core.stop()
    return {
      billed: core.written.reduce((sum, write) => sum + write.rows, 0),
      snapshots,
    }
  }

  const fileUpdate = {
    physicalTableName: 'file',
    tableName: 'file',
    operation: 'UPDATE' as const,
    rowColumns: ['id', 'projectId', 'body'],
  }
  return { ...core, syncedWrite, fileUpdate }
}

describe('billable write amplification on a synced namespace', () => {
  it('keeps generated aggregate triggers on the bounded captured-row path', async () => {
    const ns = await controlNamespace()
    ns.sql.exec('CREATE TABLE rollupSource (id TEXT PRIMARY KEY, groupId TEXT NOT NULL)')
    ns.sql.exec(
      'CREATE TABLE rollupTarget (groupId TEXT PRIMARY KEY, itemCount INTEGER NOT NULL)'
    )
    const source = table('rollupSource')
      .columns({ id: string(), groupId: string() })
      .primaryKey('id')
    const target = table('rollupTarget')
      .columns({ groupId: string(), itemCount: number() })
      .primaryKey('groupId')
    const schema = createSchema({ tables: [source, target] })
    const aggregates = defineAggregates(schema, {
      itemCount: {
        source: 'rollupSource',
        target: 'rollupTarget',
        mode: 'materialized',
        groupBy: { groupId: 'groupId' },
        columns: { itemCount: count() },
      },
    })
    ns.zero.cdc.syncTables([
      { physicalTableName: 'user', tableName: 'user' },
      { physicalTableName: 'project', tableName: 'project' },
      { physicalTableName: 'file', tableName: 'file' },
      {
        physicalTableName: 'rollupSource',
        tableName: 'rollupSource',
      },
      {
        physicalTableName: 'rollupTarget',
        tableName: 'rollupTarget',
      },
    ])
    for (const statement of aggregateMigrationStatements(aggregates)) {
      ns.sql.exec(statement)
    }
    ns.zero.cdc.drain()

    const write = ns.syncedWrite(
      'tx-aggregate',
      "INSERT INTO rollupSource VALUES ('r1', 'g1')",
      {
        physicalTableName: 'rollupSource',
        tableName: 'rollupSource',
        operation: 'INSERT',
        rowColumns: ['id', 'groupId'],
      }
    )

    expect(write.snapshots).toEqual([])
    expect(write.billed).toBeLessThan(50)
  })

  it('bills a synced write for itself, not for the tables it never touched', async () => {
    const ns = await controlNamespace()

    const update = ns.syncedWrite(
      'tx-update',
      "UPDATE file SET body = 'changed' WHERE id = 'f1'",
      ns.fileUpdate
    )
    const insert = ns.syncedWrite(
      'tx-insert',
      "INSERT INTO file VALUES ('f2', 'p1', 'new')",
      {
        ...ns.fileUpdate,
        operation: 'INSERT',
      }
    )

    expect(update.snapshots).toEqual([])
    expect(insert.snapshots).toEqual([])
    // The CDC pipeline costs a fixed handful of rows per captured change. The
    // bound is what matters: a namespace-sized copy lands three orders of
    // magnitude above it, which is what the control below demonstrates.
    expect(update.billed).toBeLessThan(40)
    expect(insert.billed).toBeLessThan(40)

    // CONTROL. On a namespace still carrying the zero-http residue the boot
    // cleanup removes, the journal triggers make `_zsync_changes` an uncovered
    // side-effect target and every synced write pays a journal-sized copy. If
    // this arm does not blow up, the bounds above are not testing anything.
    const residual = await createWorkerCore()
    buildControlShape(residual.sql)
    residual.zero.cdc.syncTables([
      { physicalTableName: 'user', tableName: 'user' },
      { physicalTableName: 'project', tableName: 'project' },
      { physicalTableName: 'file', tableName: 'file' },
    ])
    residual.zero.cdc.drain()
    residual.sql.exec(TX_MANIFEST_DDL)
    residual.start()
    beginTxJournal(residual.sql, 'tx-control', 'application')
    residual.zero.executeSQL(
      "UPDATE file SET body = 'control' WHERE id = 'f1'",
      [],
      ns.fileUpdate,
      'tx-control'
    )
    const controlSnapshots = residual.sql
      .exec("SELECT original FROM _orez_tx_manifest WHERE tx_id = 'tx-control'")
      .toArray()
      .map((row: any) => String(row.original))
      .sort()
    residual.zero.commitPendingTrackedChanges('tx-control')
    commitTxJournal(residual.sql, 'tx-control')
    residual.stop()
    expect(controlSnapshots).toContain('_zsync_changes')
    expect(
      residual.written.reduce((sum, write) => sum + write.rows, 0)
    ).toBeGreaterThan(4_000)
  })

  it('boot cleanup drops the zero-http journal residue exactly once', async () => {
    const core = await createWorkerCore()
    buildControlShape(core.sql)
    // production namespaces also carry the journal's old rollback-capture
    // registration row; the cleanup must remove it with the table.
    core.zero.cdc.ensureTable({
      physicalTableName: '_zsync_changes',
      tableName: '_zsync_changes',
      publish: false,
    })
    const highBefore = Number(
      core.sql.exec('SELECT MAX(watermark) AS high FROM _zsync_changes').one().high
    )
    expect(highBefore).toBeGreaterThanOrEqual(4_096)

    core.zero.dropZeroHttpJournalResidue()

    const objects = core.sql
      .exec("SELECT name, type FROM sqlite_master WHERE name LIKE '%zsync%'")
      .toArray()
      .map((row: any) => `${row.type}:${row.name}`)
      .sort()
    // the journal, its marker table, and every trigger writing it are gone;
    // the live client table and the preserved watermark remain.
    expect(objects).toEqual(['table:_zsync_clients', 'table:_zsync_watermark'])
    expect(
      Number(core.sql.exec('SELECT high FROM _zsync_watermark WHERE lock = 1').one().high)
    ).toBe(highBefore)
    expect(
      core.sql
        .exec("SELECT 1 AS ok FROM _orez_cdc_tables WHERE physical_table = '_zsync_changes'")
        .toArray()
    ).toEqual([])

    // a second boot finds nothing and writes nothing.
    core.start()
    core.zero.dropZeroHttpJournalResidue()
    core.stop()
    expect(core.written).toEqual([])

    // a cleaned namespace with an already-higher durable watermark keeps it.
    core.sql.exec('UPDATE _zsync_watermark SET high = high + 50 WHERE lock = 1')
    core.sql.exec(
      'CREATE TABLE _zsync_changes (watermark INTEGER PRIMARY KEY AUTOINCREMENT, "tableName" TEXT, "op" TEXT, "pk" TEXT)'
    )
    core.zero.dropZeroHttpJournalResidue()
    expect(
      Number(core.sql.exec('SELECT high FROM _zsync_watermark WHERE lock = 1').one().high)
    ).toBe(highBefore + 50)
  })

  it('copies every uncovered table when a trigger names a target it cannot resolve', async () => {
    const ns = await controlNamespace()
    // sqlite_stat1 is a real table the classifier's relation scan excludes, so
    // this trigger is unresolvable without being invalid SQL -- the same state a
    // business trigger whose body the parser cannot read produces.
    ns.sql.exec('ANALYZE')
    ns.sql.exec(`CREATE TRIGGER "unreadable_business_trigger"
          AFTER UPDATE ON "file" BEGIN
          INSERT INTO "sqlite_stat1" VALUES ('x', 'y', 'z');
        END`)

    const write = ns.syncedWrite(
      'tx-all',
      "UPDATE file SET body = 'all' WHERE id = 'f1'",
      ns.fileUpdate
    )

    // Every table the CDC does not already cover, and nothing it does.
    // `_zsync_watermark` is the one-row table the boot cleanup preserved the
    // retired journal's high watermark into.
    expect(write.snapshots).toEqual([
      '__soot_cf_migrations',
      '_zsync_clients',
      '_zsync_watermark',
      'projectMember',
      'sootsimRun',
    ])
    // One unreadable trigger costs a namespace-sized copy on every write. This
    // is the standing risk in this area: the fallback is unbounded, and its cost
    // scales with the rows in tables the statement provably never wrote.
    expect(write.billed).toBeGreaterThan(1_000)
  })

  it('counts logical rows as rows changed, not rows returned', async () => {
    const ns = await controlNamespace()
    // exactly the statement shape packages/sync-executor/src/crud.ts emits: no
    // RETURNING clause, so the rows the cursor yields are not the rows changed.
    // Reading `rows.length` here reported zero for every write production makes,
    // and the billable/logical ratio it feeds divided by that zero.
    ns.syncedWrite(
      'tx-logical',
      'UPDATE "file" SET "body" = ? WHERE "id" = ?',
      ns.fileUpdate,
      ['counted', 'f1']
    )

    expect(ns.zero.writeBudget.status().logicalRows).toBe(1)
  })
})
