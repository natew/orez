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
        if (sql.startsWith('SELECT id FROM "__contrast_cf_migrations"')) return []
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
      async query(sql: string) {
        if (sql.startsWith('SELECT id FROM "__contrast_cf_migrations"')) {
          return [...ledger].map((id) => ({ id }))
        }
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
      async query(sql: string) {
        if (sql.startsWith('SELECT id FROM "__contrast_cf_migrations"')) {
          return [...ledger].map((id) => ({ id }))
        }
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
