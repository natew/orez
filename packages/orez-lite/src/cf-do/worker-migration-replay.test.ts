import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'

// @ts-expect-error - CJS module
import BedrockSqlite from 'bedrock-sqlite'
import { describe, expect, it, vi } from 'vitest'

import { defineCloudflareConfig } from '../cf-deploy/config.js'
import { buildMigrationModuleSource } from '../cf-deploy/migration.js'
import { RollingRowWriteBudget } from '../do-sql-tracking.js'
import { TransactionalCdc } from './cdc.js'
import {
  commitTxJournal,
  rollbackTxJournal,
  TX_MANIFEST_DDL,
  TX_MANIFEST_TABLE,
  TX_SCHEMA_DDL,
  TX_SCHEMA_TABLE,
} from './tx-journal.js'
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

// same billing meter as worker-write-amplification.test.ts: total_changes()
// counts trigger writes, and CREATE TABLE ... AS SELECT is counted off the
// table it created.
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

async function importJavascriptModule(source: string): Promise<Record<string, any>> {
  const directory = mkdtempSync(join(tmpdir(), 'orez-migration-replay-'))
  const file = join(directory, 'migration.mjs')
  writeFileSync(file, source)
  try {
    return await import(/* @vite-ignore */ pathToFileURL(file).href)
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
}

const TABLES = 60

function schemaModuleUrl(): string {
  const tables: Record<string, unknown> = {}
  for (let index = 0; index < TABLES; index++) {
    tables[`t${index}`] = {
      name: `t${index}`,
      columns: { id: { type: 'string' }, a: { type: 'string' }, b: { type: 'number' } },
      primaryKey: ['id'],
    }
  }
  const source = `export const schema = ${JSON.stringify({ tables, relationships: {} })}`
  return `data:text/javascript;base64,${Buffer.from(source).toString('base64')}`
}

/**
 * A production-shaped namespace mid-life: TABLES app tables with one index
 * each, all CDC-registered (three triggers per table), a few holding rows,
 * every historical migration file already in the ledger, and schema metadata
 * already published by the previous worker version.
 */
function buildStaleNamespace(core: Awaited<ReturnType<typeof createWorkerCore>>) {
  for (let index = 0; index < TABLES; index++) {
    core.sql.exec(`CREATE TABLE "t${index}" (id TEXT PRIMARY KEY, a TEXT, b INTEGER)`)
    core.sql.exec(`CREATE INDEX "t${index}_a" ON "t${index}" (a)`)
  }
  for (let row = 0; row < 530; row++) {
    core.sql.exec(`INSERT INTO t0 VALUES ('r${row}', 'a${row}', ${row})`)
  }
  for (let table = 1; table <= 5; table++) {
    for (let row = 0; row < 100; row++) {
      core.sql.exec(`INSERT INTO t${table} VALUES ('r${row}', 'a${row}', ${row})`)
    }
  }
  // registered exactly as the module's registerTables would register them, so
  // an unchanged table costs the replay nothing.
  core.zero.cdc.syncTables(
    Array.from({ length: TABLES }, (_, index) => ({
      physicalTableName: `t${index}`,
      tableName: `public.t${index}`,
    }))
  )
  core.zero.cdc.drain()
  core.sql.exec(TX_MANIFEST_DDL)

  core.sql.exec(
    'CREATE TABLE __contrast_cf_migrations (id TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)'
  )
  core.sql.exec(
    'CREATE TABLE _zero_schema_tables (name TEXT PRIMARY KEY, schema_json TEXT NOT NULL)'
  )
  // the previous worker version already published metadata for every table,
  // and a schema-version change leaves most tables' json identical, so the
  // replay should not rewrite these rows.
  for (let index = 0; index < TABLES; index++) {
    core.sql.exec(
      'INSERT INTO _zero_schema_tables (name, schema_json) VALUES (?, ?)',
      `t${index}`,
      JSON.stringify({
        columns: { id: { type: 'string' }, a: { type: 'string' }, b: { type: 'number' } },
        primaryKey: ['id'],
        physicalName: `t${index}`,
      })
    )
  }
}

/** the statement set: ten applied historical files plus two pending ones. */
function nativeSqlStatements() {
  const statements: Array<{ id: string; sql: string }> = []
  for (let index = 0; index < TABLES; index++) {
    const file = `000${Math.floor(index / 6)}_history/migration.sql`
    statements.push({
      id: `${file}:${(index % 6) * 2}`,
      sql: `CREATE TABLE "t${index}" (id TEXT PRIMARY KEY, a TEXT, b INTEGER)`,
    })
    statements.push({
      id: `${file}:${(index % 6) * 2 + 1}`,
      sql: `CREATE INDEX "t${index}_a" ON "t${index}" (a)`,
    })
  }
  statements.push(
    {
      id: '0010_widget/migration.sql:0',
      sql: 'CREATE TABLE "t60" (id TEXT PRIMARY KEY, a TEXT)',
    },
    { id: '0010_widget/migration.sql:1', sql: 'CREATE INDEX "t60_a" ON "t60" (a)' },
    { id: '0011_flag/migration.sql:0', sql: 'ALTER TABLE "t3" ADD COLUMN c TEXT' },
    {
      id: '0011_flag/migration.sql:1',
      sql: "UPDATE t3 SET c = 'seed' WHERE id = 'r0'",
    }
  )
  return statements
}

function seedLedger(
  core: Awaited<ReturnType<typeof createWorkerCore>>,
  statements: Array<{ id: string; sql: string }>
) {
  for (const statement of statements) {
    if (statement.id.startsWith('0010_') || statement.id.startsWith('0011_')) continue
    core.sql.exec(
      'INSERT INTO __contrast_cf_migrations (id, applied_at) VALUES (?, ?)',
      `${statement.id}:seeded`,
      1
    )
  }
}

/** mirror of applicationSqlLocalClient + the session commit path over the harness. */
function migrationClient(core: Awaited<ReturnType<typeof createWorkerCore>>) {
  let transactionCount = 0
  return {
    namespace: 'ns:replay',
    query: async (sql: string, params: readonly unknown[] = []) =>
      core.zero.executeSQL(sql, [...params]).rows,
    exec: async (sql: string, params: readonly unknown[] = []) => ({
      changes: core.zero.executeSQL(sql, [...params]).changes,
    }),
    registerTables: async (tables: Array<{ table: string; publicTable: string }>) =>
      core.zero.registerApplicationSqlTables(
        tables.map((entry) => ({ table: entry.table, publicTable: entry.publicTable }))
      ),
    transaction: async (
      _compile: unknown,
      work: (tx: Record<string, unknown>) => Promise<unknown>
    ) => {
      const sessionID = `replay-${++transactionCount}`
      let mutated = false
      await work({
        exec: async (sql: string, params: readonly unknown[] = []) => {
          if (core.zero.prepareApplicationSqlMutation(sessionID, sql)) mutated = true
          return {
            changes: core.zero.executeSQL(sql, [...params], undefined, sessionID).changes,
          }
        },
        execMany: async (
          statements: ReadonlyArray<{ sql: string; params?: readonly unknown[] }>
        ) =>
          statements.map(({ sql, params = [] }) => {
            if (core.zero.prepareApplicationSqlMutation(sessionID, sql)) mutated = true
            return {
              changes: core.zero.executeSQL(sql, [...params], undefined, sessionID).changes,
            }
          }),
        query: async (sql: string, params: readonly unknown[] = []) =>
          core.zero.executeSQL(sql, [...params], undefined, sessionID).rows,
        registerTables: async (tables: Array<{ table: string; publicTable: string }>) =>
          core.zero.registerApplicationSqlTables(
            tables.map((entry) => ({
              table: entry.table,
              publicTable: entry.publicTable,
            }))
          ),
      })
      if (mutated) {
        core.zero.commitPendingTrackedChanges(sessionID)
        commitTxJournal(core.sql, sessionID)
      }
    },
  }
}

function decompose(written: Array<{ sql: string; rows: number }>) {
  const buckets = new Map<string, number>()
  const bucketOf = (sql: string): string => {
    if (sql.includes('_orez_tx_schema')) return 'schema snapshot (journal)'
    if (sql.includes('_orez_tx_manifest')) return 'row-undo manifest (journal)'
    if (/CREATE TABLE\s+"_orez_tx_/i.test(sql) || /DROP TABLE\s+.*_orez_tx_/i.test(sql))
      return 'table snapshot (journal)'
    if (sql.includes('__contrast_cf_migrations')) return 'migration ledger'
    if (sql.includes('_zero_schema_tables')) return 'schema metadata publish'
    if (sql.includes('_orez_cdc')) return 'cdc bookkeeping'
    if (sql.includes('_zero_pending') || sql.includes('_zero_changes'))
      return 'changefeed'
    if (/^\s*(CREATE|DROP)\s+TRIGGER/i.test(sql)) return 'cdc triggers'
    return 'application ddl/dml'
  }
  for (const write of written) {
    const bucket = bucketOf(write.sql)
    buckets.set(bucket, (buckets.get(bucket) ?? 0) + write.rows)
  }
  return buckets
}

describe('stale namespace migration replay cost', () => {
  it('replays every sibling after a guarded scratch-column repair rolls back', async () => {
    const core = await createWorkerCore()
    core.sql.exec('CREATE TABLE widget (id TEXT PRIMARY KEY, createdAt timestamp)')
    core.sql.exec("INSERT INTO widget VALUES ('w1', '2026-08-21')")
    core.sql.exec(TX_MANIFEST_DDL)
    core.sql.exec(
      'CREATE TABLE __contrast_cf_migrations (id TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)'
    )
    const statements = [
      {
        id: '0001_type_repair/migration.sql:0',
        sql: 'ALTER TABLE widget ADD COLUMN createdAt_integer INTEGER',
        migrateIfColumnType: {
          table: 'widget',
          column: 'createdAt',
          declaredType: 'timestamp',
        },
      },
      {
        id: '0001_type_repair/migration.sql:1',
        sql: 'UPDATE widget SET createdAt_integer = 7',
        skipIfColumnMissing: { table: 'widget', column: 'createdAt_integer' },
      },
      {
        id: '0001_type_repair/migration.sql:2',
        sql: 'CREATE INDEX IF NOT EXISTS widget_createdAt_integer_idx ON widget (createdAt_integer)',
        skipIfColumnMissing: { table: 'widget', column: 'createdAt_integer' },
      },
    ]
    for (const statement of statements) {
      core.sql.exec(
        'INSERT INTO __contrast_cf_migrations (id, applied_at) VALUES (?, ?)',
        `${statement.id}:seeded`,
        1
      )
    }
    const schemaSource = `
      export const schema = {
        tables: {
          widget: {
            name: 'widget',
            columns: {
              id: { type: 'string' },
              createdAt: { type: 'string' },
              createdAt_integer: { type: 'number' },
            },
            primaryKey: ['id'],
          },
        },
        relationships: {},
      }
    `
    const schemaUrl = `data:text/javascript;base64,${Buffer.from(schemaSource).toString('base64')}`
    const migrationModule = await importJavascriptModule(
      buildMigrationModuleSource(defineCloudflareConfig('contrast'), {
        mode: 'native',
        schemaVersion: 'type-repair-v2',
        schemaImportSpecifier: schemaUrl,
        nativeSqlStatements: statements,
        expectedTables: [
          {
            name: 'widget',
            columns: [
              { name: 'id', notNull: true, primaryKeyOrder: 1, sqlType: 'text' },
              {
                name: 'createdAt',
                notNull: false,
                primaryKeyOrder: 0,
                sqlType: 'timestamp',
              },
              {
                name: 'createdAt_integer',
                notNull: false,
                primaryKeyOrder: 0,
                sqlType: 'integer',
              },
            ],
          },
        ],
      })
    )

    await migrationModule.orezAppSchema.migrate({
      client: migrationClient(core),
      instance: 'ns:type-repair',
    })

    expect(core.sql.exec('SELECT createdAt_integer FROM widget').one()).toEqual({
      createdAt_integer: 7,
    })
    expect(
      core.sql
        .exec(
          "SELECT name FROM sqlite_master WHERE name = 'widget_createdAt_integer_idx'"
        )
        .toArray()
    ).toHaveLength(1)
  })

  it('replays a ledgered index repair when the live DDL still has the old predicate', async () => {
    const core = await createWorkerCore()
    core.sql.exec('CREATE TABLE widget (slug TEXT PRIMARY KEY, status TEXT NOT NULL)')
    core.sql.exec('CREATE UNIQUE INDEX widget_active_slug_idx ON widget (slug)')
    core.sql.exec(TX_MANIFEST_DDL)
    core.sql.exec(
      'CREATE TABLE __contrast_cf_migrations (id TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)'
    )
    const statements = [
      {
        id: '0001_repair/migration.sql:0',
        sql: 'DROP INDEX IF EXISTS widget_active_slug_idx',
      },
      {
        id: '0001_repair/migration.sql:1',
        sql: `CREATE UNIQUE INDEX IF NOT EXISTS widget_active_slug_idx ON widget (slug) WHERE status <> 'canceled'`,
      },
    ]
    for (const statement of statements) {
      core.sql.exec(
        'INSERT INTO __contrast_cf_migrations (id, applied_at) VALUES (?, ?)',
        `${statement.id}:seeded`,
        1
      )
    }
    const schemaSource = `
      export const schema = {
        tables: {
          widget: {
            name: 'widget',
            columns: { slug: { type: 'string' }, status: { type: 'string' } },
            primaryKey: ['slug'],
          },
        },
        relationships: {},
      }
    `
    const schemaUrl = `data:text/javascript;base64,${Buffer.from(schemaSource).toString('base64')}`
    const migrationModule = await importJavascriptModule(
      buildMigrationModuleSource(defineCloudflareConfig('contrast'), {
        mode: 'native',
        schemaVersion: 'index-repair-v2',
        schemaImportSpecifier: schemaUrl,
        nativeSqlStatements: statements,
        expectedTables: [
          {
            name: 'widget',
            columns: [
              {
                name: 'slug',
                notNull: true,
                primaryKeyOrder: 1,
                sqlType: 'text',
              },
              {
                name: 'status',
                notNull: true,
                primaryKeyOrder: 0,
                sqlType: 'text',
              },
            ],
          },
        ],
        expectedIndexes: [
          {
            columns: ['slug'],
            name: 'widget_active_slug_idx',
            predicate: "status <> 'canceled'",
            table: 'widget',
            unique: true,
          },
        ],
      })
    )

    core.start()
    await migrationModule.orezAppSchema.migrate({
      client: migrationClient(core),
      instance: 'ns:index-repair',
    })
    core.stop()

    expect(
      core.sql
        .exec("SELECT sql FROM sqlite_master WHERE name = 'widget_active_slug_idx'")
        .one()!.sql
    ).toContain("WHERE status <> 'canceled'")
    expect(
      core.sql.exec('SELECT id FROM __contrast_cf_migrations ORDER BY id').toArray()
    ).toHaveLength(2)
    expect(core.written.reduce((rows, write) => rows + write.rows, 0)).toBe(11)

    core.start()
    await migrationModule.orezAppSchema.migrate({
      client: migrationClient(core),
      instance: 'ns:index-repair',
    })
    core.stop()
    expect(core.written.reduce((rows, write) => rows + write.rows, 0)).toBe(0)
  })

  it('measures where the billable rows of one replay go', async () => {
    const core = await createWorkerCore()
    buildStaleNamespace(core)
    const statements = nativeSqlStatements()
    seedLedger(core, statements)

    const migrationModule = await importJavascriptModule(
      buildMigrationModuleSource(defineCloudflareConfig('contrast'), {
        mode: 'native',
        schemaVersion: 'replay-v2',
        schemaImportSpecifier: schemaModuleUrl(),
        nativeSqlStatements: statements,
      })
    )

    core.start()
    await migrationModule.orezAppSchema.migrate({
      client: migrationClient(core),
      instance: 'ns:replay',
    })
    core.stop()

    const buckets = decompose(core.written)
    const total = [...buckets.values()].reduce((sum, rows) => sum + rows, 0)
    const lines = [...buckets.entries()]
      .sort((left, right) => right[1] - left[1])
      .map(([bucket, rows]) => `${String(rows).padStart(7)}  ${bucket}`)
    console.log(`replay billable rows: ${total}\n${lines.join('\n')}`)

    // the per-object schema snapshot made this bookkeeping 86% of a replay
    // (1,456 rows on this exact shape). the JSON encoding writes a handful of
    // rows per DDL transaction: two transactions here, each one insert plus
    // its delete at commit.
    expect(buckets.get('schema snapshot (journal)') ?? 0).toBeLessThanOrEqual(8)
    // metadata publish rewrites only rows whose schema json changed, and this
    // replay changes none of them.
    expect(buckets.get('schema metadata publish') ?? 0).toBe(0)
    // an unchanged table's cdc registration costs the replay nothing; only the
    // ALTERed table re-registers.
    expect(buckets.get('cdc bookkeeping') ?? 0).toBeLessThanOrEqual(8)
    // the whole-table snapshot for the ALTERed 100-row table is the one
    // remaining size-proportional cost, and it is the rollback correctness
    // cost, not bookkeeping.
    expect(buckets.get('table snapshot (journal)')).toBe(100)
  })

  // a transaction journaled by the previous worker version (one schema row
  // per sqlite_master object) can be killed by the deploy that ships the JSON
  // encoding, and the new code recovers it on the next boot. losing that read
  // path wedges the namespace exactly the way the journal exists to prevent.
  it('recovers a legacy per-object schema snapshot written before the deploy', async () => {
    const core = await createWorkerCore()
    core.sql.exec('CREATE TABLE widget (id TEXT PRIMARY KEY, label TEXT)')
    core.sql.exec("INSERT INTO widget VALUES ('w1', 'before')")
    core.sql.exec(TX_MANIFEST_DDL)
    core.sql.exec(TX_SCHEMA_DDL)

    // the legacy snapshot rows exactly as the previous version wrote them
    const originalWidgetSql = String(
      core.sql
        .exec("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'widget'")
        .one()!.sql
    )
    core.sql.exec(
      `INSERT INTO "${TX_SCHEMA_TABLE}" (tx_id, owner, type, name, tbl_name, sql) VALUES ('legacy-tx', 'application', 'marker', '', '', NULL)`
    )
    core.sql.exec(
      `INSERT INTO "${TX_SCHEMA_TABLE}" (tx_id, owner, type, name, tbl_name, sql) VALUES ('legacy-tx', 'application', 'table', 'widget', 'widget', ?)`,
      originalWidgetSql
    )

    // a schema-owned table restores wholesale from its pre-transaction snapshot
    core.sql.exec('CREATE TABLE "_orez_tx_legacy" AS SELECT * FROM widget')
    core.sql.exec(
      `INSERT INTO "${TX_MANIFEST_TABLE}" (tx_id, owner, original, snapshot) VALUES ('legacy-tx', 'application', 'widget', '_orez_tx_legacy')`
    )
    // the mid-transaction DDL the kill left behind
    core.sql.exec('ALTER TABLE widget ADD COLUMN extra TEXT')
    core.sql.exec("UPDATE widget SET label = 'after'")
    core.sql.exec('CREATE TABLE scratch (id TEXT PRIMARY KEY)')

    rollbackTxJournal(core.sql, 'legacy-tx')

    const restoredSql = String(
      core.sql
        .exec("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'widget'")
        .one()!.sql
    )
    expect(restoredSql).toBe(originalWidgetSql)
    expect(
      core.sql.exec("SELECT 1 AS ok FROM sqlite_master WHERE name = 'scratch'").toArray()
    ).toHaveLength(0)
    expect(core.sql.exec('SELECT label FROM widget').one()!.label).toBe('before')
    expect(
      core.sql.exec(`SELECT 1 AS ok FROM "${TX_SCHEMA_TABLE}"`).toArray()
    ).toHaveLength(0)
  })
})
