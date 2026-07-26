import { describe, expect, it, vi } from 'vitest'

import { defineCloudflareConfig } from './config.js'
import { buildMigrationModuleSource } from './migration.js'

function javascriptModuleUrl(source: string): string {
  return `data:text/javascript;base64,${Buffer.from(source).toString('base64')}`
}

async function importJavascriptModule(source: string): Promise<Record<string, any>> {
  return import(/* @vite-ignore */ javascriptModuleUrl(source))
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
})
