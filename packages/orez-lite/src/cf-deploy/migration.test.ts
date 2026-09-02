import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'

import { describe, expect, it, vi } from 'vitest'

import { defineCloudflareConfig } from './config.js'
import { buildMigrationModuleSource } from './migration.js'

function javascriptModuleUrl(source: string): string {
  return `data:text/javascript;base64,${Buffer.from(source).toString('base64')}`
}

async function importJavascriptModule(source: string): Promise<Record<string, any>> {
  const directory = mkdtempSync(join(tmpdir(), 'orez-migration-module-'))
  const file = join(directory, 'migration.mjs')
  writeFileSync(file, source)
  try {
    return await import(/* @vite-ignore */ pathToFileURL(file).href)
  } finally {
    rmSync(directory, { recursive: true, force: true })
  }
}

function migrationLedgerRows(
  sql: string,
  params: readonly unknown[],
  ledger: ReadonlySet<string>
): Array<{ baseId: string }> | null {
  if (!sql.startsWith('WITH expected(baseId, lowerId, upperId)')) return null
  const rows: Array<{ baseId: string }> = []
  for (let index = 0; index < params.length; index += 3) {
    const baseId = String(params[index])
    if ([...ledger].some((id) => id === baseId || id.startsWith(`${baseId}:`))) {
      rows.push({ baseId })
    }
  }
  return rows
}

describe('buildMigrationModuleSource', () => {
  it('exports a native descriptor backed by the imported Zero schema', async () => {
    const schemaModuleUrl = javascriptModuleUrl(`
      export const schema = {
        tables: {
          widget: {
            name: 'widget',
            columns: {
              id: { type: 'string' },
              generatedAtRuntime: { type: 'number', optional: true },
            },
            primaryKey: ['id'],
          },
        },
        relationships: { widget: {} },
        enableLegacyMutators: true,
      }
    `)
    const schemaModule = await import(/* @vite-ignore */ schemaModuleUrl)
    const configuredPublicTables = [{ table: 'widget', publicTable: 'tenant.widget' }]
    const migrationModule = await importJavascriptModule(
      buildMigrationModuleSource(defineCloudflareConfig('contrast'), {
        mode: 'native',
        schemaVersion: 'schema-v7',
        schemaImportSpecifier: schemaModuleUrl,
        nativeSqlStatements: [],
        publicTables: configuredPublicTables,
      })
    )

    expect(migrationModule.orezAppSchema).toEqual({
      version: 'schema-v7',
      schema: schemaModule.schema,
      publicTables: configuredPublicTables,
      migrate: migrationModule.runCloudflareMigrations,
    })
    expect(migrationModule.orezAppSchema.schema).toBe(schemaModule.schema)
    expect(migrationModule.orezAppSchema.migrate).toBe(
      migrationModule.runContrastCloudflareMigrations
    )

    const registerTables = vi.fn()
    await expect(
      migrationModule.orezAppSchema.migrate({
        registrationOnly: true,
        client: { registerTables },
      })
    ).resolves.toEqual({ tables: ['tenant.widget'] })
    expect(registerTables).toHaveBeenCalledWith(configuredPublicTables)
  })

  it('passes a rollback-only capture exemption through to table registration', async () => {
    const schemaModuleUrl = javascriptModuleUrl(`
      export const schema = { tables: {}, relationships: {} }
    `)
    const configuredPublicTables = [
      { table: 'widget', publicTable: 'public.widget' },
      { table: 'usageLedger', publicTable: 'public.usageLedger', publish: false },
    ]
    const migrationModule = await importJavascriptModule(
      buildMigrationModuleSource(defineCloudflareConfig('contrast'), {
        mode: 'native',
        schemaVersion: 'schema-v8',
        schemaImportSpecifier: schemaModuleUrl,
        nativeSqlStatements: [],
        publicTables: configuredPublicTables,
      })
    )

    const registerTables = vi.fn()
    await expect(
      migrationModule.orezAppSchema.migrate({
        registrationOnly: true,
        client: { registerTables },
      })
    ).resolves.toEqual({ tables: ['public.widget', 'public.usageLedger'] })
    expect(registerTables).toHaveBeenCalledWith(configuredPublicTables)
  })

  it('exports a coherent no-op descriptor', async () => {
    const migrationModule = await importJavascriptModule(
      buildMigrationModuleSource(defineCloudflareConfig('contrast'), {
        mode: 'noop',
        schemaVersion: 'schema-empty',
      })
    )

    expect(migrationModule.orezAppSchema).toEqual({
      version: 'schema-empty',
      schema: { tables: {}, relationships: {} },
      publicTables: [],
      migrate: migrationModule.runCloudflareMigrations,
    })
    expect(migrationModule.orezAppSchema.migrate).toBe(
      migrationModule.runContrastCloudflareMigrations
    )
    await expect(migrationModule.orezAppSchema.migrate()).resolves.toBeUndefined()
  })

  it('commits migration files separately so later DDL can snapshot a table', async () => {
    const schemaModuleUrl = javascriptModuleUrl(`
      export const schema = { tables: {}, relationships: {} }
    `)
    const migrationModule = await importJavascriptModule(
      buildMigrationModuleSource(defineCloudflareConfig('contrast'), {
        mode: 'native',
        schemaVersion: 'schema-backlog',
        schemaImportSpecifier: schemaModuleUrl,
        nativeSqlStatements: [
          {
            id: '0001_rows/migration.sql:0',
            sql: 'UPDATE project SET name = name',
          },
          {
            id: '0002_schema/migration.sql:0',
            sql: 'ALTER TABLE project ADD iconSha text',
          },
        ],
      })
    )
    const ledger = new Set<string>()
    const statementTransactions: number[] = []
    let transaction = 0
    let rowWriteTransaction = -1
    const tx = {
      async query(sql: string, params: readonly unknown[] = []) {
        const applied = migrationLedgerRows(sql, params, ledger)
        if (applied) return applied
        if (sql.includes('FROM sqlite_master m JOIN pragma_table_info')) return []
        if (sql.includes("SELECT name FROM sqlite_master WHERE type = 'table'")) return []
        if (sql.startsWith('SELECT name, schema_json FROM _zero_schema_tables')) return []
        throw new Error(`unexpected query: ${sql}`)
      },
      async exec(sql: string, params: unknown[] = []) {
        if (
          sql.startsWith('CREATE TABLE IF NOT EXISTS "__contrast_cf_migrations"') ||
          sql.startsWith('CREATE TABLE IF NOT EXISTS _zero_schema_tables')
        ) {
          return
        }
        if (sql === 'UPDATE project SET name = name') {
          rowWriteTransaction = transaction
          statementTransactions.push(transaction)
          return
        }
        if (sql === 'ALTER TABLE project ADD iconSha text') {
          if (rowWriteTransaction === transaction) {
            throw new Error(
              'cannot snapshot project after row undo in the same transaction'
            )
          }
          statementTransactions.push(transaction)
          return
        }
        if (sql.startsWith('INSERT INTO "__contrast_cf_migrations"')) {
          ledger.add(String(params[0]))
          return
        }
        throw new Error(`unexpected exec: ${sql}`)
      },
      async execMany(
        statements: ReadonlyArray<{ sql: string; params?: readonly unknown[] }>
      ) {
        for (const statement of statements) await this.exec(statement.sql, [...(statement.params ?? [])])
        return statements.map(() => ({ changes: 0 }))
      },
      async registerTables() {},
    }
    const client = {
      async transaction(_compile: unknown, run: (tx: typeof tx) => Promise<void>) {
        transaction++
        await run(tx)
      },
    }

    await expect(migrationModule.orezAppSchema.migrate({ client })).resolves.toEqual({
      tables: [],
    })
    expect(statementTransactions).toHaveLength(2)
    expect(statementTransactions[0]).not.toBe(statementTransactions[1])
    expect(transaction).toBe(2)
    expect(ledger.size).toBe(2)
  })

  it('bounds ledger reads and application sessions to the pending migration files', async () => {
    const schemaModuleUrl = javascriptModuleUrl(`
      export const schema = { tables: {}, relationships: {} }
    `)
    const migrationModule = await importJavascriptModule(
      buildMigrationModuleSource(defineCloudflareConfig('contrast'), {
        mode: 'native',
        schemaVersion: 'schema-cost',
        schemaImportSpecifier: schemaModuleUrl,
        nativeSqlStatements: [
          {
            id: '0001_retired/migration.sql:0',
            sql: 'DROP TABLE IF EXISTS retired',
          },
          {
            id: '0002_alpha/migration.sql:0',
            sql: 'CREATE TABLE alpha (id TEXT PRIMARY KEY)',
          },
          {
            id: '0003_beta/migration.sql:0',
            sql: 'CREATE TABLE beta (id TEXT PRIMARY KEY)',
          },
        ],
      })
    )
    const ledger = new Set<string>([
      '0001_retired/migration.sql:0:previous-hash',
      ...Array.from(
        { length: 1_000 },
        (_, index) => `historical-${String(index).padStart(4, '0')}:hash`
      ),
    ])
    const tables = new Set<string>()
    let ledgerQueries = 0
    let ledgerRowsRead = 0
    let sessions = 0
    let postCommitCallbacks = 0
    const tx = {
      async query(sql: string, params: readonly unknown[] = []) {
        const applied = migrationLedgerRows(sql, params, ledger)
        if (applied) {
          ledgerQueries++
          ledgerRowsRead += applied.length
          return applied
        }
        if (sql.includes("FROM sqlite_master WHERE type IN ('table', 'index')")) {
          return [...tables].map((name) => ({ name, type: 'table' }))
        }
        if (sql.includes('FROM sqlite_master m JOIN pragma_table_info')) return []
        if (sql.startsWith('SELECT name, schema_json FROM _zero_schema_tables')) return []
        throw new Error(`unexpected query: ${sql}`)
      },
      async exec(sql: string, params: readonly unknown[] = []) {
        if (
          sql.startsWith('CREATE TABLE IF NOT EXISTS "__contrast_cf_migrations"') ||
          sql.startsWith('CREATE TABLE IF NOT EXISTS _zero_schema_tables') ||
          sql === 'DROP TABLE IF EXISTS retired'
        ) {
          return
        }
        const created = /^CREATE TABLE (alpha|beta)/.exec(sql)?.[1]
        if (created) {
          tables.add(created)
          return
        }
        if (sql.startsWith('INSERT INTO "__contrast_cf_migrations"')) {
          ledger.add(String(params[0]))
          return
        }
        throw new Error(`unexpected exec: ${sql}`)
      },
      async execMany(
        statements: ReadonlyArray<{ sql: string; params?: readonly unknown[] }>
      ) {
        for (const statement of statements) await this.exec(statement.sql, [...(statement.params ?? [])])
        return statements.map(() => ({ changes: 0 }))
      },
      async registerTables() {},
    }
    const client = {
      async transaction(_compile: unknown, run: (tx: typeof tx) => Promise<void>) {
        sessions++
        await run(tx)
        postCommitCallbacks++
      },
    }

    await migrationModule.orezAppSchema.migrate({ client })

    expect(ledgerQueries).toBe(1)
    expect(ledgerRowsRead).toBe(1)
    expect(sessions).toBe(2)
    expect(postCommitCallbacks).toBe(2)
    expect(tables).toEqual(new Set(['alpha', 'beta']))
    expect(ledger.size).toBe(1_003)
  })

  it('accepts equivalent SQLite type affinities and still rejects incompatible types', async () => {
    const schemaModuleUrl = javascriptModuleUrl(`
      export const schema = { tables: {}, relationships: {} }
    `)
    const migrationModule = await importJavascriptModule(
      buildMigrationModuleSource(defineCloudflareConfig('contrast'), {
        mode: 'native',
        schemaVersion: 'schema-affinity',
        schemaImportSpecifier: schemaModuleUrl,
        nativeSqlStatements: [],
        expectedTables: [
          {
            name: 'account',
            columns: [
              {
                name: 'id',
                notNull: true,
                primaryKeyOrder: 1,
                sqlType: 'text',
              },
            ],
          },
        ],
      })
    )
    const columnType = { current: 'varchar(255)' }
    const tx = {
      async query(sql: string) {
        if (sql.startsWith('WITH expected(baseId, lowerId, upperId)')) return []
        if (sql.includes('FROM sqlite_master m JOIN pragma_table_info')) {
          return [
            {
              tableName: 'account',
              columnName: 'id',
              columnType: columnType.current,
              columnNotNull: 1,
              columnPk: 1,
            },
          ]
        }
        if (sql.includes("SELECT name FROM sqlite_master WHERE type = 'table'")) {
          return [{ name: 'account' }]
        }
        if (sql.startsWith('SELECT name, schema_json FROM _zero_schema_tables')) return []
        throw new Error(`unexpected query: ${sql}`)
      },
      async exec(sql: string) {
        if (
          sql.startsWith('CREATE TABLE IF NOT EXISTS "__contrast_cf_migrations"') ||
          sql.startsWith('CREATE TABLE IF NOT EXISTS _zero_schema_tables')
        ) {
          return
        }
        throw new Error(`unexpected exec: ${sql}`)
      },
      async execMany(
        statements: ReadonlyArray<{ sql: string; params?: readonly unknown[] }>
      ) {
        for (const statement of statements) await this.exec(statement.sql)
        return statements.map(() => ({ changes: 0 }))
      },
      async registerTables() {},
    }
    const client = {
      async transaction(_compile: unknown, run: (tx: typeof tx) => Promise<void>) {
        await run(tx)
      },
    }

    await expect(migrationModule.orezAppSchema.migrate({ client })).resolves.toEqual({
      tables: [],
    })

    columnType.current = 'blob'
    await expect(migrationModule.orezAppSchema.migrate({ client })).rejects.toThrow(
      'expected text, found blob'
    )
  })

  it('validates a unique index from its live DDL and pragma shape', async () => {
    const schemaModuleUrl = javascriptModuleUrl(`
      export const schema = { tables: {}, relationships: {} }
    `)
    const migrationModule = await importJavascriptModule(
      buildMigrationModuleSource(defineCloudflareConfig('contrast'), {
        mode: 'native',
        schemaVersion: 'schema-index-predicate',
        schemaImportSpecifier: schemaModuleUrl,
        nativeSqlStatements: [],
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
            predicate: "widget.status <> 'canceled'",
            table: 'widget',
            unique: true,
          },
        ],
      })
    )
    const liveIndexRows = {
      current: [
        {
          columnName: 'slug',
          columnOrder: 0,
          indexName: 'widget_active_slug_idx',
          indexSql: 'CREATE UNIQUE INDEX widget_active_slug_idx ON widget (slug)',
          indexUnique: 1,
          tableName: 'widget',
        },
      ],
    }
    const tx = {
      async query(sql: string) {
        if (sql.startsWith('WITH expected(baseId, lowerId, upperId)')) return []
        if (sql.includes('FROM sqlite_master m JOIN pragma_table_info')) {
          return [
            {
              tableName: 'widget',
              columnName: 'slug',
              columnType: 'text',
              columnNotNull: 1,
              columnPk: 1,
            },
            {
              tableName: 'widget',
              columnName: 'status',
              columnType: 'text',
              columnNotNull: 1,
              columnPk: 0,
            },
          ]
        }
        if (sql.includes('FROM sqlite_master m JOIN pragma_index_list')) {
          return liveIndexRows.current
        }
        if (sql.startsWith('SELECT name, schema_json FROM _zero_schema_tables')) return []
        throw new Error(`unexpected query: ${sql}`)
      },
      async exec(sql: string) {
        if (
          sql.startsWith('CREATE TABLE IF NOT EXISTS "__contrast_cf_migrations"') ||
          sql.startsWith('CREATE TABLE IF NOT EXISTS _zero_schema_tables')
        ) {
          return
        }
        throw new Error(`unexpected exec: ${sql}`)
      },
      async execMany(
        statements: ReadonlyArray<{ sql: string; params?: readonly unknown[] }>
      ) {
        for (const statement of statements) await this.exec(statement.sql)
        return statements.map(() => ({ changes: 0 }))
      },
      async registerTables() {},
    }
    const client = {
      async transaction(_compile: unknown, run: (tx: typeof tx) => Promise<void>) {
        await run(tx)
      },
    }

    await expect(migrationModule.orezAppSchema.migrate({ client })).rejects.toThrow(
      `index widget_active_slug_idx expected predicate widget.status <> 'canceled', found (none)`
    )

    liveIndexRows.current = []
    await expect(migrationModule.orezAppSchema.migrate({ client })).rejects.toThrow(
      'missing unique index widget_active_slug_idx'
    )

    liveIndexRows.current = [
      {
        columnName: 'slug',
        columnOrder: 0,
        indexName: 'widget_active_slug_idx',
        indexSql: `CREATE INDEX widget_active_slug_idx ON widget (slug) WHERE status <> 'canceled'`,
        indexUnique: 0,
        tableName: 'widget',
      },
    ]
    await expect(migrationModule.orezAppSchema.migrate({ client })).rejects.toThrow(
      'index widget_active_slug_idx expected unique, found non-unique'
    )

    liveIndexRows.current[0]!.indexUnique = 1
    liveIndexRows.current[0]!.tableName = 'otherWidget'
    await expect(migrationModule.orezAppSchema.migrate({ client })).rejects.toThrow(
      'index widget_active_slug_idx expected table widget, found otherWidget'
    )

    liveIndexRows.current[0]!.tableName = 'widget'
    liveIndexRows.current[0]!.columnName = 'status'
    await expect(migrationModule.orezAppSchema.migrate({ client })).rejects.toThrow(
      'index widget_active_slug_idx expected columns (slug), found (status)'
    )

    liveIndexRows.current[0]!.columnName = 'slug'
    liveIndexRows.current[0]!.indexSql = `CREATE UNIQUE INDEX widget_active_slug_idx ON widget (slug) WHERE "widget"."status" <> 'canceled'`
    await expect(migrationModule.orezAppSchema.migrate({ client })).resolves.toEqual({
      tables: [],
    })
  })

  it('leaves ledgered conditional statements unchanged when their predicate skips', async () => {
    const schemaModuleUrl = javascriptModuleUrl(`
      export const schema = { tables: {}, relationships: {} }
    `)
    const migrationModule = await importJavascriptModule(
      buildMigrationModuleSource(defineCloudflareConfig('contrast'), {
        mode: 'native',
        schemaVersion: 'schema-conditional',
        schemaImportSpecifier: schemaModuleUrl,
        nativeSqlStatements: [
          {
            id: '0001_conditional/migration.sql:0',
            sql: 'CREATE INDEX IF NOT EXISTS customer_legacy_idx ON customer (legacy)',
            skipIfColumnMissing: { table: 'customer', column: 'legacy' },
          },
        ],
      })
    )
    const ledger = new Set<string>()
    const ledgerWrites: string[] = []
    const tx = {
      async query(sql: string, params: readonly unknown[] = []) {
        const applied = migrationLedgerRows(sql, params, ledger)
        if (applied) return applied
        if (sql.includes("FROM sqlite_master WHERE type IN ('table', 'index')")) {
          return [{ name: 'customer', type: 'table' }]
        }
        if (sql.includes('FROM sqlite_master m JOIN pragma_table_info')) {
          return [
            {
              tableName: 'customer',
              columnName: 'id',
              columnType: 'text',
              columnNotNull: 1,
              columnPk: 1,
            },
          ]
        }
        if (sql.startsWith('PRAGMA table_info("customer")')) {
          return [{ name: 'id' }]
        }
        if (sql.startsWith('SELECT name, schema_json FROM _zero_schema_tables')) return []
        throw new Error(`unexpected query: ${sql}`)
      },
      async exec(sql: string, params: unknown[] = []) {
        if (
          sql.startsWith('CREATE TABLE IF NOT EXISTS "__contrast_cf_migrations"') ||
          sql.startsWith('CREATE TABLE IF NOT EXISTS _zero_schema_tables')
        ) {
          return
        }
        if (sql.startsWith('INSERT INTO "__contrast_cf_migrations"')) {
          const id = String(params[0])
          ledger.add(id)
          ledgerWrites.push(`insert:${id}`)
          return
        }
        if (sql.startsWith('DELETE FROM "__contrast_cf_migrations"')) {
          const id = String(params[0])
          ledger.delete(id)
          ledgerWrites.push(`delete:${id}`)
          return
        }
        throw new Error(`unexpected exec: ${sql}`)
      },
      async execMany(
        statements: ReadonlyArray<{ sql: string; params?: readonly unknown[] }>
      ) {
        for (const statement of statements) await this.exec(statement.sql, [...(statement.params ?? [])])
        return statements.map(() => ({ changes: 0 }))
      },
      async registerTables() {},
    }
    const client = {
      async transaction(_compile: unknown, run: (tx: typeof tx) => Promise<void>) {
        await run(tx)
      },
    }

    await migrationModule.orezAppSchema.migrate({ client })
    expect(ledger.size).toBe(1)
    ledgerWrites.length = 0

    await migrationModule.orezAppSchema.migrate({ client })
    expect(ledgerWrites).toEqual([])
    expect(ledger.size).toBe(1)
  })

  it('runs a guarded migration statement only for the matching column type', async () => {
    const schemaModuleUrl = javascriptModuleUrl(`
      export const schema = { tables: {}, relationships: {} }
    `)
    const runWithGuard = async (
      columnType: string,
      migrateIfColumnType: Record<string, unknown>
    ) => {
      const migrationModule = await importJavascriptModule(
        buildMigrationModuleSource(defineCloudflareConfig('contrast'), {
          mode: 'native',
          schemaVersion: 'schema-column-type-guard',
          schemaImportSpecifier: schemaModuleUrl,
          nativeSqlStatements: [
            {
              id: '0001_guard/migration.sql:0',
              sql: 'CREATE TABLE repaired (id TEXT PRIMARY KEY)',
              migrateIfColumnType,
            },
          ],
        })
      )
      const ledger = new Set<string>()
      const executed: string[] = []
      const tx = {
        async query(sql: string, params: readonly unknown[] = []) {
          const applied = migrationLedgerRows(sql, params, ledger)
          if (applied) return applied
          if (sql.includes('FROM sqlite_master m JOIN pragma_table_info')) return []
          if (sql.startsWith('SELECT type FROM pragma_table_info')) {
            return [{ type: columnType }]
          }
          if (sql.startsWith('SELECT name, schema_json FROM _zero_schema_tables'))
            return []
          throw new Error(`unexpected query: ${sql}`)
        },
        async exec(sql: string, params: readonly unknown[] = []) {
          if (
            sql.startsWith('CREATE TABLE IF NOT EXISTS "__contrast_cf_migrations"') ||
            sql.startsWith('CREATE TABLE IF NOT EXISTS _zero_schema_tables')
          ) {
            return
          }
          if (sql === 'CREATE TABLE repaired (id TEXT PRIMARY KEY)') {
            executed.push(sql)
            return
          }
          if (sql.startsWith('INSERT INTO "__contrast_cf_migrations"')) {
            ledger.add(String(params[0]))
            return
          }
          throw new Error(`unexpected exec: ${sql}`)
        },
        async execMany(
          statements: ReadonlyArray<{ sql: string; params?: readonly unknown[] }>
        ) {
          for (const statement of statements) await this.exec(statement.sql, [...(statement.params ?? [])])
          return statements.map(() => ({ changes: 0 }))
        },
        async registerTables() {},
      }
      const client = {
        async transaction(_compile: unknown, run: (tx: typeof tx) => Promise<void>) {
          await run(tx)
        },
      }

      await migrationModule.orezAppSchema.migrate({ client })
      return { executed, ledger }
    }

    await expect(
      runWithGuard('timestamp', {
        table: 'widget',
        column: 'createdAt',
        declaredType: ' TIMESTAMP ',
      })
    ).resolves.toMatchObject({
      executed: ['CREATE TABLE repaired (id TEXT PRIMARY KEY)'],
    })
    await expect(
      runWithGuard('timestamp', {
        table: 'widget',
        column: 'createdAt',
        affinity: 'integer',
      })
    ).resolves.toMatchObject({ executed: [] })
    await expect(
      runWithGuard('decimal(10, 2)', {
        table: 'widget',
        column: 'createdAt',
        affinity: 'numeric',
      })
    ).resolves.toMatchObject({
      executed: ['CREATE TABLE repaired (id TEXT PRIMARY KEY)'],
    })
  })

  it('does not partially replay a completed staging-table lifecycle', async () => {
    const schemaModuleUrl = javascriptModuleUrl(`
      export const schema = { tables: {}, relationships: {} }
    `)
    const migrationModule = await importJavascriptModule(
      buildMigrationModuleSource(defineCloudflareConfig('contrast'), {
        mode: 'native',
        schemaVersion: 'schema-staging',
        schemaImportSpecifier: schemaModuleUrl,
        nativeSqlStatements: [
          {
            id: '0001_staging/migration.sql:0',
            sql: 'CREATE TABLE IF NOT EXISTS "__accountCandidates" ("accountId" text)',
          },
          {
            id: '0001_staging/migration.sql:1',
            sql: 'INSERT OR IGNORE INTO "__accountCandidates" VALUES ("account-1")',
          },
          {
            id: '0001_staging/migration.sql:2',
            sql: 'CREATE TABLE IF NOT EXISTS "__accountGuard" ("ok" integer)',
          },
          {
            id: '0001_staging/migration.sql:3',
            sql: 'INSERT INTO "__accountGuard" SELECT 1 FROM "__accountCandidates"',
          },
          {
            id: '0001_staging/migration.sql:4',
            sql: 'DROP TABLE "__accountGuard"',
          },
          {
            id: '0001_staging/migration.sql:5',
            sql: 'DROP TABLE "__accountCandidates"',
          },
        ],
      })
    )
    const tables = new Set<string>()
    const tableRows = new Map<string, number>()
    const ledger = new Set<string>()
    const writes: string[] = []
    const tx = {
      async query(sql: string, params: readonly unknown[] = []) {
        const applied = migrationLedgerRows(sql, params, ledger)
        if (applied) return applied
        if (sql.includes("FROM sqlite_master WHERE type IN ('table', 'index')")) {
          return [...tables].map((name) => ({ name, type: 'table' }))
        }
        if (sql.includes('FROM sqlite_master m JOIN pragma_table_info')) {
          return [...tables].map((tableName) => ({
            tableName,
            columnName: 'id',
            columnType: 'text',
            columnNotNull: 0,
            columnPk: 0,
          }))
        }
        if (sql.startsWith('SELECT name, schema_json FROM _zero_schema_tables')) return []
        throw new Error(`unexpected query: ${sql}`)
      },
      async exec(sql: string, params: unknown[] = []) {
        if (
          sql.startsWith('CREATE TABLE IF NOT EXISTS "__contrast_cf_migrations"') ||
          sql.startsWith('CREATE TABLE IF NOT EXISTS _zero_schema_tables')
        ) {
          return
        }
        if (sql.startsWith('INSERT INTO "__contrast_cf_migrations"')) {
          const id = String(params[0])
          ledger.add(id)
          writes.push(`ledger-insert:${id}`)
          return
        }
        if (sql.startsWith('DELETE FROM "__contrast_cf_migrations"')) {
          const id = String(params[0])
          ledger.delete(id)
          writes.push(`ledger-delete:${id}`)
          return
        }
        if (sql.startsWith('CREATE TABLE IF NOT EXISTS "__accountCandidates"')) {
          tables.add('__accountCandidates')
          tableRows.set('__accountCandidates', 0)
          writes.push('create:candidates')
          return
        }
        if (sql.startsWith('INSERT OR IGNORE INTO "__accountCandidates"')) {
          tableRows.set('__accountCandidates', 1)
          writes.push('insert:candidates')
          return
        }
        if (sql.startsWith('CREATE TABLE IF NOT EXISTS "__accountGuard"')) {
          tables.add('__accountGuard')
          writes.push('create:guard')
          return
        }
        if (sql.startsWith('INSERT INTO "__accountGuard"')) {
          if (tableRows.get('__accountCandidates') !== 1) {
            throw new Error('ownership guard failed: candidates were not populated')
          }
          writes.push('insert:guard')
          return
        }
        const dropped = /^DROP TABLE "([^"]+)"/.exec(sql)?.[1]
        if (dropped) {
          tables.delete(dropped)
          tableRows.delete(dropped)
          writes.push(`drop:${dropped}`)
          return
        }
        throw new Error(`unexpected exec: ${sql}`)
      },
      async execMany(
        statements: ReadonlyArray<{ sql: string; params?: readonly unknown[] }>
      ) {
        for (const statement of statements) await this.exec(statement.sql, [...(statement.params ?? [])])
        return statements.map(() => ({ changes: 0 }))
      },
      async registerTables() {},
    }
    const client = {
      async transaction(_compile: unknown, run: (tx: typeof tx) => Promise<void>) {
        await run(tx)
      },
    }

    await migrationModule.orezAppSchema.migrate({ client })
    expect(tables.size).toBe(0)
    expect(ledger.size).toBe(6)
    writes.length = 0

    await expect(migrationModule.orezAppSchema.migrate({ client })).resolves.toEqual({
      tables: [],
    })
    expect(writes).toEqual([])
    expect(tables.size).toBe(0)
    expect(ledger.size).toBe(6)
  })
})
