import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'

import { transform } from 'esbuild'
import { describe, expect, it, vi } from 'vitest'

import { cfDeployConfig } from './config.js'
import { EMBED_READY_TIMEOUT_MS, EMBED_WARM_TIMEOUT_MS } from './leaves.js'
import { buildMigrationModuleSource } from './migration.js'
import { readNativeSqlMigrationStatements } from './nativeMigrations.js'
import {
  buildAppShimSource,
  buildDataShimSource,
  buildRustSyncUserShimSource,
  buildUserShimSource,
} from './shims.js'

const builders = {
  data: buildDataShimSource,
  user: buildUserShimSource,
  app: buildAppShimSource,
}

function generatedNeedsSqlSchema(source: string) {
  const match = source.match(
    /function needsSqlSchema\(request, pathname\) \{[\s\S]*?\n\}/
  )
  if (!match) throw new Error('generated SQL schema gate is missing')
  // eslint-disable-next-line typescript-eslint/no-implied-eval -- executes the generated worker predicate to test its route/method contract
  const candidate: unknown = new Function(`return ${match[0]}`)()
  if (typeof candidate !== 'function') {
    throw new Error('generated SQL schema gate is not callable')
  }
  return (request: Request, pathname: string): boolean =>
    Boolean(candidate(request, pathname))
}

describe('cf shim builders', () => {
  it('generates syntactically valid data-tier worker modules', async () => {
    for (const build of [buildDataShimSource, buildUserShimSource]) {
      await expect(
        transform(build(cfDeployConfig('contrast')), {
          loader: 'js',
          format: 'esm',
        })
      ).resolves.toBeDefined()
    }
  })

  it('registers deployed tables for a preexisting-row snapshot', async () => {
    const migrationSource = buildMigrationModuleSource(cfDeployConfig('contrast'), {
      mode: 'full',
      schemaVersion: 'schema-v1',
      schemaImportSpecifier: './schema.js',
      migrationFiles: [],
      initSql: '',
      initSqlBatchStatements: [],
      zeroHttpShardSql: '',
      zeroHttpShardBatchStatements: [],
    })
    let statements: Array<{ sql: string; params?: unknown[] }> = []
    const instance = 'preexisting'
    const globals = globalThis as typeof globalThis & {
      __contrast_cf_do_sql_fetch_by_instance?: Record<string, typeof fetch>
      __contrast_test_migration_pool?: unknown
      __contrast_test_migration_schema?: unknown
    }
    globals.__contrast_cf_do_sql_fetch_by_instance = {
      [instance]: (async (_input: string | URL | Request, init?: RequestInit) => {
        statements = JSON.parse(String(init?.body)).statements
        return Response.json({ ok: true })
      }) as unknown as typeof fetch,
    }
    const Pool = class {
      async connect() {
        return { release() {} }
      }
    }
    const schema = {
      tables: {
        item: {
          name: 'item',
          columns: { id: { type: 'string' }, label: { type: 'string' } },
          primaryKey: ['id'],
        },
      },
    }
    const moduleDirectory = await mkdtemp(join(tmpdir(), 'contrast-cf-migration-test-'))
    try {
      globals.__contrast_test_migration_pool = Pool
      globals.__contrast_test_migration_schema = schema
      const executable = migrationSource
        .replace(
          "import { Pool } from 'pg'",
          'const Pool = globalThis.__contrast_test_migration_pool'
        )
        .replace(
          'import { schema } from "./schema.js"',
          'const schema = globalThis.__contrast_test_migration_schema'
        )
        .replaceAll('export const ', 'const ')
      const modulePath = join(moduleDirectory, 'migration.mjs')
      await writeFile(modulePath, executable)
      const module = await import(pathToFileURL(modulePath).href)
      await module.runContrastCloudflareMigrations({ schemaOnly: true, instance })
    } finally {
      delete globals.__contrast_cf_do_sql_fetch_by_instance
      delete globals.__contrast_test_migration_pool
      delete globals.__contrast_test_migration_schema
      await rm(moduleDirectory, { recursive: true, force: true })
    }
    const registration = statements.find((statement) =>
      statement.sql.startsWith('INSERT OR REPLACE INTO _zero_schema_tables')
    )
    expect(registration?.params?.[0]).toBe('item')
    expect(JSON.parse(String(registration?.params?.[1]))).toEqual({
      columns: schema.tables.item.columns,
      primaryKey: ['id'],
    })
  })

  it('does not replay an immutable statement id after its SQL changes', async () => {
    const previousSql = 'CREATE TABLE IF NOT EXISTS item (id TEXT PRIMARY KEY)'
    const currentSql = 'CREATE TABLE IF NOT EXISTS item (id TEXT PRIMARY KEY, label TEXT)'
    const hashSql = (sql: string) => {
      let hash = 2166136261
      for (let index = 0; index < sql.length; index++) {
        hash ^= sql.charCodeAt(index)
        hash = Math.imul(hash, 16777619)
      }
      return (hash >>> 0).toString(36)
    }
    const applied = new Set([`migration:0:${hashSql(previousSql)}`])
    const executed: string[] = []
    const globals = globalThis as typeof globalThis & {
      __contrast_cf_application_sql_client?: () => unknown
      __contrast_test_immutable_migration_schema?: unknown
    }
    globals.__contrast_test_immutable_migration_schema = { tables: {} }
    globals.__contrast_cf_application_sql_client = () => ({
      async transaction(
        _compile: unknown,
        work: (tx: {
          exec(sql: string, params?: unknown[]): Promise<void>
          query(sql: string): Promise<Array<Record<string, unknown>>>
          registerTables(): Promise<void>
        }) => Promise<void>
      ) {
        await work({
          async exec(sql, params) {
            executed.push(sql)
            if (sql.startsWith('INSERT INTO "__contrast_cf_migrations"')) {
              const id = String(params?.[0])
              if (applied.has(id)) throw new Error('duplicate migration ledger id')
              applied.add(id)
            }
          },
          async query(sql) {
            if (sql.startsWith('SELECT id FROM "__contrast_cf_migrations"')) {
              return [...applied].map((id) => ({ id }))
            }
            if (sql.startsWith('SELECT name, type FROM sqlite_master')) {
              return [{ name: 'item', type: 'table' }]
            }
            return []
          },
          async registerTables() {},
        })
      },
      async registerTables() {},
    })
    const migrationSource = buildMigrationModuleSource(cfDeployConfig('contrast'), {
      mode: 'native',
      schemaVersion: 'immutable-statement-id',
      schemaImportSpecifier: './schema.js',
      nativeSqlStatements: [{ id: 'migration:0', sql: currentSql }],
    })
    const moduleDirectory = await mkdtemp(
      join(tmpdir(), 'contrast-cf-immutable-migration-')
    )
    try {
      const executable = migrationSource
        .replace(
          'import { schema } from "./schema.js"',
          'const schema = globalThis.__contrast_test_immutable_migration_schema'
        )
        .replaceAll('export const ', 'const ')
      const modulePath = join(moduleDirectory, 'migration.mjs')
      await writeFile(modulePath, executable)
      const module = await import(pathToFileURL(modulePath).href)
      await module.runContrastCloudflareMigrations({ instance: 'singleton' })
      await module.runContrastCloudflareMigrations({ instance: 'singleton' })
    } finally {
      delete globals.__contrast_cf_application_sql_client
      delete globals.__contrast_test_immutable_migration_schema
      await rm(moduleDirectory, { recursive: true, force: true })
    }
    expect(executed).not.toContain(currentSql)
  })

  it('uses the latest rebuild when an older partial rebuild has an invalid constraint', async () => {
    const oldCreateSql =
      'CREATE TABLE IF NOT EXISTS __new_tokenUsage (id TEXT PRIMARY KEY, userId TEXT NOT NULL, FOREIGN KEY (userId) REFERENCES user(id))'
    const newCreateSql =
      'CREATE TABLE IF NOT EXISTS __new_tokenUsage (id TEXT PRIMARY KEY, userId TEXT NOT NULL)'
    const oldInsertSql =
      'INSERT INTO __new_tokenUsage (id, userId) SELECT id, userId FROM tokenUsage'
    const newInsertSql =
      'INSERT INTO __new_tokenUsage (id, userId) SELECT id, userId FROM tokenUsage /* current */'
    const dropSql = 'DROP TABLE tokenUsage'
    const renameSql = 'ALTER TABLE __new_tokenUsage RENAME TO tokenUsage'
    const hashSql = (sql: string) => {
      let hash = 2166136261
      for (let index = 0; index < sql.length; index++) {
        hash ^= sql.charCodeAt(index)
        hash = Math.imul(hash, 16777619)
      }
      return (hash >>> 0).toString(36)
    }
    const applied = new Set(['old:0', 'old:1', `old:1:${hashSql(oldCreateSql)}`])
    const executed: string[] = []
    const tokenUsageRows = 1823
    const orphanAgentRows = 86
    let temporaryTableExists = false
    let temporaryTableHasUserForeignKey = false
    let temporaryRows = 0
    let temporaryOrphanAgentRows = 0
    let finalRows = tokenUsageRows
    let finalOrphanAgentRows = 0
    const globals = globalThis as typeof globalThis & {
      __contrast_cf_application_sql_client?: () => unknown
      __contrast_test_superseded_migration_schema?: unknown
    }
    globals.__contrast_test_superseded_migration_schema = { tables: {} }
    globals.__contrast_cf_application_sql_client = () => ({
      async transaction(
        _compile: unknown,
        work: (tx: {
          exec(sql: string, params?: unknown[]): Promise<void>
          query(sql: string): Promise<Array<Record<string, unknown>>>
          registerTables(): Promise<void>
        }) => Promise<void>
      ) {
        await work({
          async exec(sql, params) {
            executed.push(sql)
            if (sql === oldCreateSql || sql === newCreateSql) {
              temporaryTableExists = true
              temporaryTableHasUserForeignKey = sql === oldCreateSql
              temporaryRows = 0
              temporaryOrphanAgentRows = 0
            }
            if (sql === oldInsertSql && !temporaryTableExists) {
              throw new Error('no such table: __new_tokenUsage')
            }
            if (sql === oldInsertSql && temporaryTableHasUserForeignKey) {
              throw new Error('FOREIGN KEY constraint failed')
            }
            if (sql === newInsertSql) {
              temporaryRows = tokenUsageRows
              temporaryOrphanAgentRows = orphanAgentRows
            }
            if (sql === renameSql) {
              temporaryTableExists = false
              finalRows = temporaryRows
              finalOrphanAgentRows = temporaryOrphanAgentRows
            }
            if (sql.startsWith('INSERT INTO "__contrast_cf_migrations"')) {
              const id = String(params?.[0])
              if (applied.has(id)) throw new Error('duplicate migration ledger id')
              applied.add(id)
            }
          },
          async query(sql) {
            if (sql.startsWith('SELECT id FROM "__contrast_cf_migrations"')) {
              return [...applied].map((id) => ({ id }))
            }
            if (sql === 'PRAGMA table_info("__new_tokenUsage")') {
              return temporaryTableExists ? [{ name: 'id' }, { name: 'userId' }] : []
            }
            if (sql.startsWith('SELECT name, type FROM sqlite_master')) {
              return [
                { name: 'tokenUsage', type: 'table' },
                ...(temporaryTableExists
                  ? [{ name: '__new_tokenUsage', type: 'table' }]
                  : []),
              ]
            }
            if (sql.startsWith('SELECT m.name AS tableName')) {
              return [
                { tableName: 'tokenUsage', columnName: 'id' },
                { tableName: 'tokenUsage', columnName: 'userId' },
                ...(temporaryTableExists
                  ? [
                      { tableName: '__new_tokenUsage', columnName: 'id' },
                      { tableName: '__new_tokenUsage', columnName: 'userId' },
                    ]
                  : []),
              ]
            }
            return []
          },
          async registerTables() {},
        })
      },
      async registerTables() {},
    })
    const migrationSource = buildMigrationModuleSource(cfDeployConfig('contrast'), {
      mode: 'native',
      schemaVersion: 'superseded-rebuild',
      schemaImportSpecifier: './schema.js',
      nativeSqlStatements: [
        { id: 'old:0', sql: 'PRAGMA foreign_keys=OFF' },
        {
          id: 'old:1',
          sql: oldCreateSql,
          rebuildTarget: 'tokenUsage',
          rebuildColumns: ['id', 'userId'],
        },
        { id: 'old:2', sql: oldInsertSql, rebuildTarget: 'tokenUsage' },
        { id: 'old:3', sql: dropSql, rebuildTarget: 'tokenUsage' },
        { id: 'old:4', sql: renameSql, rebuildTarget: 'tokenUsage' },
        { id: 'old:5', sql: 'PRAGMA foreign_keys=ON' },
        { id: 'new:0', sql: 'PRAGMA foreign_keys=OFF' },
        {
          id: 'new:1',
          sql: newCreateSql,
          supersedes: ['old:1', 'old:2', 'old:3', 'old:4'],
          rebuildTarget: 'tokenUsage',
          rebuildColumns: ['id', 'userId'],
        },
        { id: 'new:2', sql: newInsertSql, rebuildTarget: 'tokenUsage' },
        { id: 'new:3', sql: dropSql, rebuildTarget: 'tokenUsage' },
        { id: 'new:4', sql: renameSql, rebuildTarget: 'tokenUsage' },
        { id: 'new:5', sql: 'PRAGMA foreign_keys=ON' },
      ],
    })
    const moduleDirectory = await mkdtemp(
      join(tmpdir(), 'contrast-cf-superseded-migration-')
    )
    try {
      const executable = migrationSource
        .replace(
          'import { schema } from "./schema.js"',
          'const schema = globalThis.__contrast_test_superseded_migration_schema'
        )
        .replaceAll('export const ', 'const ')
      const modulePath = join(moduleDirectory, 'migration.mjs')
      await writeFile(modulePath, executable)
      const module = await import(pathToFileURL(modulePath).href)
      await module.runContrastCloudflareMigrations({ instance: 'singleton' })
      await module.runContrastCloudflareMigrations({ instance: 'singleton' })
    } finally {
      delete globals.__contrast_cf_application_sql_client
      delete globals.__contrast_test_superseded_migration_schema
      await rm(moduleDirectory, { recursive: true, force: true })
    }
    expect(executed).not.toContain(oldCreateSql)
    expect(executed).not.toContain(oldInsertSql)
    expect(executed.filter((sql) => sql === newCreateSql)).toHaveLength(1)
    expect(executed.filter((sql) => sql === newInsertSql)).toHaveLength(1)
    expect(finalRows).toBe(tokenUsageRows)
    expect(finalOrphanAgentRows).toBe(orphanAgentRows)
  })

  it('uses the successor directly on a fresh database when an older rebuild is superseded', async () => {
    const oldCreateSql =
      'CREATE TABLE IF NOT EXISTS __new_tokenUsage (id TEXT PRIMARY KEY, userId TEXT NOT NULL)'
    const oldInsertSql =
      "INSERT INTO __new_tokenUsage (id, userId) SELECT id, 'legacy' FROM tokenUsage"
    const newCreateSql =
      'CREATE TABLE IF NOT EXISTS __new_tokenUsage (id TEXT PRIMARY KEY, userId TEXT NOT NULL, accountId TEXT)'
    const newInsertSql =
      'INSERT INTO __new_tokenUsage (id, userId, accountId) SELECT id, userId, NULL FROM tokenUsage'
    const dropSql = 'DROP TABLE tokenUsage'
    const renameSql = 'ALTER TABLE __new_tokenUsage RENAME TO tokenUsage'
    const applied = new Set<string>()
    const executed: string[] = []
    let finalHasUserId = true
    let finalTableExists = true
    let finalColumns = ['id', 'userId']
    let temporaryTableExists = false
    let temporaryHasUserId = false
    let temporaryColumns: string[] = []
    const globals = globalThis as typeof globalThis & {
      __contrast_cf_application_sql_client?: () => unknown
      __contrast_test_fresh_rebuild_schema?: unknown
    }
    globals.__contrast_test_fresh_rebuild_schema = { tables: {} }
    globals.__contrast_cf_application_sql_client = () => ({
      async transaction(
        _compile: unknown,
        work: (tx: {
          exec(sql: string, params?: unknown[]): Promise<void>
          query(sql: string): Promise<Array<Record<string, unknown>>>
          registerTables(): Promise<void>
        }) => Promise<void>
      ) {
        await work({
          async exec(sql, params) {
            executed.push(sql)
            if (sql === oldCreateSql || sql === newCreateSql) {
              temporaryTableExists = true
              temporaryHasUserId = true
              temporaryColumns =
                sql === newCreateSql ? ['id', 'userId', 'accountId'] : ['id', 'userId']
            }
            if (sql === newInsertSql && !finalHasUserId) {
              throw new Error('no such column: userId')
            }
            if (sql === dropSql) {
              finalHasUserId = false
              finalTableExists = false
              finalColumns = []
            }
            if (sql === renameSql) {
              temporaryTableExists = false
              finalHasUserId = temporaryHasUserId
              finalTableExists = true
              finalColumns = temporaryColumns
            }
            if (sql.startsWith('INSERT INTO "__contrast_cf_migrations"')) {
              applied.add(String(params?.[0]))
            }
          },
          async query(sql) {
            if (sql.startsWith('SELECT id FROM "__contrast_cf_migrations"')) {
              return [...applied].map((id) => ({ id }))
            }
            if (sql === 'PRAGMA table_info("__new_tokenUsage")') {
              return temporaryTableExists ? [{ name: 'id' }] : []
            }
            if (sql.startsWith('SELECT name, type FROM sqlite_master')) {
              return [
                ...(finalTableExists ? [{ name: 'tokenUsage', type: 'table' }] : []),
                ...(temporaryTableExists
                  ? [{ name: '__new_tokenUsage', type: 'table' }]
                  : []),
              ]
            }
            if (sql.startsWith('SELECT m.name AS tableName')) {
              return [
                ...(finalTableExists
                  ? finalColumns.map((name) => ({
                      tableName: 'tokenUsage',
                      columnName: name,
                    }))
                  : []),
                ...(temporaryTableExists
                  ? temporaryColumns.map((name) => ({
                      tableName: '__new_tokenUsage',
                      columnName: name,
                    }))
                  : []),
              ]
            }
            return []
          },
          async registerTables() {},
        })
      },
      async registerTables() {},
    })
    const migrationSource = buildMigrationModuleSource(cfDeployConfig('contrast'), {
      mode: 'native',
      schemaVersion: 'fresh-rebuilds',
      schemaImportSpecifier: './schema.js',
      nativeSqlStatements: [
        { id: 'old:0', sql: 'PRAGMA foreign_keys=OFF' },
        {
          id: 'old:1',
          sql: oldCreateSql,
          rebuildTarget: 'tokenUsage',
          rebuildColumns: ['id', 'userId'],
        },
        { id: 'old:2', sql: oldInsertSql, rebuildTarget: 'tokenUsage' },
        { id: 'old:3', sql: dropSql, rebuildTarget: 'tokenUsage' },
        { id: 'old:4', sql: renameSql, rebuildTarget: 'tokenUsage' },
        { id: 'old:5', sql: 'PRAGMA foreign_keys=ON' },
        { id: 'new:0', sql: 'PRAGMA foreign_keys=OFF' },
        {
          id: 'new:1',
          sql: newCreateSql,
          supersedes: ['old:1', 'old:2', 'old:3', 'old:4'],
          rebuildTarget: 'tokenUsage',
          rebuildColumns: ['id', 'userId', 'accountId'],
        },
        { id: 'new:2', sql: newInsertSql, rebuildTarget: 'tokenUsage' },
        { id: 'new:3', sql: dropSql, rebuildTarget: 'tokenUsage' },
        { id: 'new:4', sql: renameSql, rebuildTarget: 'tokenUsage' },
        { id: 'new:5', sql: 'PRAGMA foreign_keys=ON' },
      ],
    })
    const moduleDirectory = await mkdtemp(join(tmpdir(), 'contrast-cf-fresh-rebuilds-'))
    try {
      const executable = migrationSource
        .replace(
          'import { schema } from "./schema.js"',
          'const schema = globalThis.__contrast_test_fresh_rebuild_schema'
        )
        .replaceAll('export const ', 'const ')
      const modulePath = join(moduleDirectory, 'migration.mjs')
      await writeFile(modulePath, executable)
      const module = await import(pathToFileURL(modulePath).href)
      await module.runContrastCloudflareMigrations({ instance: 'singleton' })
      await module.runContrastCloudflareMigrations({ instance: 'singleton' })
    } finally {
      delete globals.__contrast_cf_application_sql_client
      delete globals.__contrast_test_fresh_rebuild_schema
      await rm(moduleDirectory, { recursive: true, force: true })
    }
    expect(executed).not.toContain(oldCreateSql)
    expect(executed).not.toContain(oldInsertSql)
    expect(executed.filter((sql) => sql === newCreateSql)).toHaveLength(1)
    expect(executed.filter((sql) => sql === newInsertSql)).toHaveLength(1)
    expect(finalHasUserId).toBe(true)
  })

  it('does not replay a completed rebuild after its temporary table was renamed', async () => {
    const rebuildSql = [
      'PRAGMA foreign_keys=OFF',
      'CREATE TABLE IF NOT EXISTS __new_tokenUsage (id TEXT PRIMARY KEY, userId TEXT NOT NULL, FOREIGN KEY (userId) REFERENCES user(id))',
      'INSERT INTO __new_tokenUsage (id, userId) SELECT id, userId FROM tokenUsage',
      'DROP TABLE tokenUsage',
      'ALTER TABLE __new_tokenUsage RENAME TO tokenUsage',
      'PRAGMA foreign_keys=ON',
    ]
    const nextSql = 'CREATE TABLE IF NOT EXISTS nextMigration (id TEXT PRIMARY KEY)'
    const hashSql = (sql: string) => {
      let hash = 2166136261
      for (let index = 0; index < sql.length; index++) {
        hash ^= sql.charCodeAt(index)
        hash = Math.imul(hash, 16777619)
      }
      return (hash >>> 0).toString(36)
    }
    const applied = new Set(
      rebuildSql.map((sql, index) => `rebuild:${index}:${hashSql(sql)}`)
    )
    const executed: string[] = []
    let nextTableExists = false
    const globals = globalThis as typeof globalThis & {
      __contrast_cf_application_sql_client?: () => unknown
      __contrast_test_completed_migration_schema?: unknown
    }
    globals.__contrast_test_completed_migration_schema = { tables: {} }
    globals.__contrast_cf_application_sql_client = () => ({
      async transaction(
        _compile: unknown,
        work: (tx: {
          exec(sql: string, params?: unknown[]): Promise<void>
          query(sql: string): Promise<Array<Record<string, unknown>>>
          registerTables(): Promise<void>
        }) => Promise<void>
      ) {
        await work({
          async exec(sql, params) {
            executed.push(sql)
            if (sql === rebuildSql[2]) throw new Error('FOREIGN KEY constraint failed')
            if (sql === nextSql) nextTableExists = true
            if (sql.startsWith('INSERT INTO "__contrast_cf_migrations"')) {
              applied.add(String(params?.[0]))
            }
          },
          async query(sql) {
            if (sql.startsWith('SELECT id FROM "__contrast_cf_migrations"')) {
              return [...applied].map((id) => ({ id }))
            }
            if (sql === 'PRAGMA table_info("__new_tokenUsage")') return []
            if (sql === 'PRAGMA table_info("nextMigration")') {
              return nextTableExists ? [{ name: 'id' }] : []
            }
            // the rebuild already completed: tokenUsage carries the rebuilt
            // shape and the temporary table is long gone.
            if (sql.startsWith('SELECT name, type FROM sqlite_master')) {
              return [
                { name: 'tokenUsage', type: 'table' },
                ...(nextTableExists ? [{ name: 'nextMigration', type: 'table' }] : []),
              ]
            }
            if (sql.startsWith('SELECT m.name AS tableName')) {
              return [
                { tableName: 'tokenUsage', columnName: 'id' },
                { tableName: 'tokenUsage', columnName: 'userId' },
                ...(nextTableExists
                  ? [{ tableName: 'nextMigration', columnName: 'id' }]
                  : []),
              ]
            }
            return []
          },
          async registerTables() {},
        })
      },
      async registerTables() {},
    })
    const migrationSource = buildMigrationModuleSource(cfDeployConfig('contrast'), {
      mode: 'native',
      schemaVersion: 'completed-rebuild',
      schemaImportSpecifier: './schema.js',
      nativeSqlStatements: [
        ...rebuildSql.map((sql, index) => ({
          id: `rebuild:${index}`,
          sql,
          ...(index >= 1 && index <= 4 ? { rebuildTarget: 'tokenUsage' } : null),
          ...(index === 1 ? { rebuildColumns: ['id', 'userId'] } : null),
        })),
        { id: 'next:0', sql: nextSql },
      ],
    })
    const moduleDirectory = await mkdtemp(
      join(tmpdir(), 'contrast-cf-completed-migration-')
    )
    try {
      const executable = migrationSource
        .replace(
          'import { schema } from "./schema.js"',
          'const schema = globalThis.__contrast_test_completed_migration_schema'
        )
        .replaceAll('export const ', 'const ')
      const modulePath = join(moduleDirectory, 'migration.mjs')
      await writeFile(modulePath, executable)
      const module = await import(pathToFileURL(modulePath).href)
      await module.runContrastCloudflareMigrations({ instance: 'singleton' })
      await module.runContrastCloudflareMigrations({ instance: 'singleton' })
    } finally {
      delete globals.__contrast_cf_application_sql_client
      delete globals.__contrast_test_completed_migration_schema
      await rm(moduleDirectory, { recursive: true, force: true })
    }
    expect(executed.filter((sql) => rebuildSql.includes(sql))).toEqual([])
    expect(executed.filter((sql) => sql === nextSql)).toHaveLength(1)
    expect(nextTableExists).toBe(true)
  })
  it.each([true, false])(
    'converges a drifted CREATE TABLE whose columns only exist in its definition (ledgered=%s)',
    async (ledgerTheCreate) => {
      // prod 2026-07-26: 217 namespaces held an old-shape communityListing whose
      // ledgered CREATE was trusted by table NAME, so the columns that only ever
      // existed inside the CREATE definition were never added and every later
      // CREATE INDEX on them failed the whole reconcile, forever, at ~2k billed
      // rows per retry. the phantom pass must converge an existing table to its
      // ledgered CREATE's declared column set.
      const { DatabaseSync } = await import('node:sqlite')
      const migrationsDirectory = await mkdtemp(
        join(tmpdir(), 'contrast-cf-drift-migrations-')
      )
      const moduleDirectory = await mkdtemp(join(tmpdir(), 'contrast-cf-drift-module-'))
      const globals = globalThis as typeof globalThis & {
        __contrast_cf_application_sql_client?: () => unknown
        __contrast_test_drift_migration_schema?: unknown
      }
      try {
        await mkdir(join(migrationsDirectory, '0001_old'))
        await writeFile(
          join(migrationsDirectory, '0001_old', 'migration.sql'),
          'CREATE TABLE `communityListing` (\n' +
            '\t`id` text PRIMARY KEY,\n' +
            '\t`userId` text,\n' +
            '\t`submittedAt` integer,\n' +
            "\t`visibility` text DEFAULT 'public' NOT NULL,\n" +
            '\t`accountId` text NOT NULL,\n' +
            '\tCONSTRAINT `fk_communityListing_userId` FOREIGN KEY (`userId`) REFERENCES `user`(`id`) ON DELETE SET NULL\n' +
            ');--> statement-breakpoint\n' +
            'CREATE INDEX `communityListing_userId_submittedAt_idx` ON `communityListing` (`userId`,`submittedAt`);\n'
        )
        const nativeSqlStatements = readNativeSqlMigrationStatements(
          migrationsDirectory,
          (sql) => ({ sql })
        )
        const migrationSource = buildMigrationModuleSource(cfDeployConfig('contrast'), {
          mode: 'native',
          schemaVersion: 'schema-drift',
          schemaImportSpecifier: './schema.js',
          nativeSqlStatements,
          publicTables: [],
          expectedTables: [
            {
              name: 'communityListing',
              columns: [
                { name: 'id', notNull: true, primaryKeyOrder: 1, sqlType: 'text' },
                { name: 'userId', notNull: false, primaryKeyOrder: 0, sqlType: 'text' },
                {
                  name: 'submittedAt',
                  notNull: false,
                  primaryKeyOrder: 0,
                  sqlType: 'integer',
                },
                {
                  name: 'visibility',
                  notNull: true,
                  primaryKeyOrder: 0,
                  sqlType: 'text',
                },
              ],
            },
          ],
        })
        const db = new DatabaseSync(':memory:')
        // the drifted reality: the table exists in a pre-CREATE-definition shape.
        db.exec('CREATE TABLE communityListing (id text PRIMARY KEY, "userId" text)')
        db.exec(
          'CREATE TABLE "__contrast_cf_migrations" (id TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)'
        )
        // the CREATE is deliberately NOT ledgered. that is the state prod retries
        // in: the failure compensation deletes the ledger rows a failing run
        // inserted, so every retry starts with this CREATE unrecorded. a
        // convergence that only inspects ledgered statements sees nothing to do
        // here, which is exactly how the first attempt at this fix passed its
        // ledgered-only test and still healed none of the 217 wedged namespaces.
        if (ledgerTheCreate) {
          db.prepare(
            'INSERT INTO "__contrast_cf_migrations" (id, applied_at) VALUES (?, ?)'
          ).run('0001_old/migration.sql:0', 1)
        }
        const executed: string[] = []
        globals.__contrast_test_drift_migration_schema = { tables: {} }
        globals.__contrast_cf_application_sql_client = () => ({
          async transaction(
            _compile: unknown,
            work: (tx: {
              exec(sql: string, params?: unknown[]): Promise<void>
              query(sql: string): Promise<Array<Record<string, unknown>>>
              registerTables(): Promise<void>
            }) => Promise<void>
          ) {
            await work({
              async exec(sql, params) {
                executed.push(sql)
                if (params && params.length > 0) {
                  db.prepare(sql).run(...(params as (string | number)[]))
                } else {
                  db.exec(sql)
                }
              },
              async query(sql) {
                return db.prepare(sql).all() as Array<Record<string, unknown>>
              },
              async registerTables() {},
            })
          },
        })
        const executable = migrationSource
          .replace(
            'import { schema } from "./schema.js"',
            'const schema = globalThis.__contrast_test_drift_migration_schema'
          )
          .replaceAll('export const ', 'const ')
        const modulePath = join(moduleDirectory, 'migration.mjs')
        await writeFile(modulePath, executable)
        const module = await import(pathToFileURL(modulePath).href)
        await module.runContrastCloudflareMigrations({
          schemaOnly: true,
          instance: 'drifted',
        })
        const columns = db
          .prepare('PRAGMA table_info("communityListing")')
          .all() as Array<{
          name: string
          type: string
          notnull: number
        }>
        const byName = new Map(
          columns.map((column) => [
            column.name,
            { notnull: column.notnull, type: column.type.toLowerCase() },
          ])
        )
        expect(byName.get('submittedAt')).toEqual({ type: 'integer', notnull: 0 })
        expect(byName.get('visibility')).toEqual({ type: 'text', notnull: 1 })
        // `accountId` is NOT NULL with no default. converging it would mean
        // inventing a value for every existing row, so it is left alone and
        // reported by the shape assert instead of silently backfilled.
        expect(byName.has('accountId')).toBe(false)
        const indexes = db
          .prepare('PRAGMA index_list("communityListing")')
          .all() as Array<{
          name: string
        }>
        expect(indexes.map((index) => index.name)).toContain(
          'communityListing_userId_submittedAt_idx'
        )
        // a second run must find nothing left to converge.
        const alreadyExecuted = executed.length
        await module.runContrastCloudflareMigrations({
          schemaOnly: true,
          instance: 'drifted',
        })
        expect(
          executed.slice(alreadyExecuted).filter((sql) => sql.startsWith('ALTER TABLE'))
        ).toEqual([])
      } finally {
        delete globals.__contrast_cf_application_sql_client
        delete globals.__contrast_test_drift_migration_schema
        await rm(migrationsDirectory, { recursive: true, force: true })
        await rm(moduleDirectory, { recursive: true, force: true })
      }
    }
  )
})
