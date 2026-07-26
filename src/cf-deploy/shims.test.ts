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
  it('builds the Rust-host app shim with the private native SQLite client', async () => {
    const src = buildRustSyncUserShimSource(cfDeployConfig('contrast'), {
      feedTables: {
        post: ['id', 'title'],
      },
    })

    expect(src).toContain('class ZeroSqlDO extends OrezZeroSqlDO')
    expect(src).toContain('export class OrezDataFeed extends WorkerEntrypoint')
    expect(src).toContain('globalThis.__contrast_cf_application_sql_client')
    expect(src).not.toContain('DoBackend')
    expect(src).not.toContain('__contrast_pg')
    expect(src).not.toContain('__contrast_cf_do_create_pg_pool')
    expect(src).not.toContain('makeRemotePgPool')
    expect(src).not.toContain('pgSession')
    expect(src).not.toContain('OREZ_SYNC_ORIGIN')
    expect(src).toContain("pathname.startsWith('/api/zero/')")
    expect(src).toContain('const SYNC_FEED_TABLES = {"post":["id","title"]}')
    expect(src).toContain("tableName: 'syncCursor'")
    expect(src).toContain('installZeroSqlWriteCircuitBreaker')
    expect(src).toContain(
      "await runContrastCloudflareMigrations({ instance: 'singleton', schemaOnly: true })"
    )
    expect(src).not.toContain('__contrast_migrate')
    expect(src).not.toContain('parsePublications')
    expect(src).toContain("url.pathname === '/snapshot' || url.pathname === '/changes'")
    expect(src).not.toContain('ZeroCacheDO')
    expect(src).not.toContain('startZeroCacheEmbedCF')
    expect(src).not.toContain('zero-cache-embed')
    expect(src).not.toContain('libpg-query')
    await expect(transform(src, { loader: 'js', format: 'esm' })).resolves.toBeDefined()
  })

  it('substitutes the sentinel for the consumer prefix (contrast)', () => {
    const cfg = cfDeployConfig('contrast')
    for (const [name, build] of Object.entries(builders)) {
      const src = build(cfg)
      // sentinel fully substituted, contrast tokens present (this is contrast's worker)
      expect(src, `${name}: leftover sentinel`).not.toMatch(/nspfx/i)
      expect(src, `${name}: missing __contrast_ global`).toContain('__contrast_')
    }
  })

  it('produces a different prefix for a different consumer (chat)', () => {
    const cfg = cfDeployConfig('chat')
    const src = buildDataShimSource(cfg)
    expect(src).toContain('x-chat-ns')
    expect(src).toContain('__chat_cf_do_namespace')
    expect(src).toContain('_chat_write_circuit')
    expect(src).toContain('runChatCloudflareMigrations')
    // no contrast tokens leak into a chat deploy
    expect(src).not.toMatch(/\bsoot\b/)
    expect(src).not.toContain('__contrast_')
    expect(src).not.toContain('x-contrast-')
  })

  it('adds a host header to minute-cron app service requests', () => {
    const src = buildDataShimSource(
      cfDeployConfig('chat', {
        minuteCronAppForwards: [{ path: '/api/jobs/drain', secretEnvVar: 'CRON_SECRET' }],
      })
    )

    expect(src).toContain(
      "new Request('https://app/api/jobs/drain', { method: 'POST', headers: { host: 'app', 'x-cron-secret': env.CRON_SECRET || '' } })"
    )
  })

  it('serializes each pg batch as one DoBackend operation', () => {
    for (const build of [buildDataShimSource, buildUserShimSource]) {
      const src = build(cfDeployConfig('contrast'))
      expect(src).toContain('await this.contrastPgBackend.queryBatch(')
      expect(src).toContain('sql: stmt.text')
      expect(src).toContain('params: stmt.values || []')
      expect(src).not.toContain(
        'await this.contrastPgBackend.query(stmt.text, stmt.values || [])'
      )
      expect(src).not.toContain("await this.contrastPgBackend.query('ROLLBACK', [])")
      expect(src).toContain("'batch failed: '")
      expect(src).not.toContain("'batch failed at statement '")
    }
  })

  it('passes the durable object namespace to each Orez embed', () => {
    for (const build of [buildDataShimSource, buildUserShimSource]) {
      const src = build(cfDeployConfig('chat'))
      expect(src).toContain('const instance = await this.loadInstanceName()')
      expect(src).toContain('instanceId: instance,')
      expect(src).toContain(
        `log: (event) =>
            console.log('[chat] orez embed ' + JSON.stringify(event)),`
      )
    }
  })

  it('persists the boot request before scheduling its alarm', () => {
    for (const build of [buildDataShimSource, buildUserShimSource]) {
      const src = build(cfDeployConfig('chat'))
      const ensureReadyStart = src.indexOf('  ensureReady() {')
      const ensureReadyEnd = src.indexOf('\n  async bootEmbed()', ensureReadyStart)
      const ensureReady = src.slice(ensureReadyStart, ensureReadyEnd)
      const instance = src.indexOf("storage.put('__chat_instance_name'")
      const pending = src.indexOf("storage.put('__chat_boot_pending', true)")
      const alarm = src.indexOf('storage.setAlarm(Date.now())')
      expect(instance).toBeGreaterThan(-1)
      expect(pending).toBeGreaterThan(instance)
      expect(alarm).toBeGreaterThan(pending)
      expect(ensureReady).toContain('await Promise.all([')
      expect(ensureReady).not.toContain('await this.ctx.storage.put(')
      expect(src).toContain("await this.ctx.storage.get('__chat_boot_pending')")
      expect(src.match(/storage.delete\('__chat_boot_pending'\)/g)).toHaveLength(2)
    }
  })

  it('scopes terminal boot failures to the deploy version', () => {
    for (const build of [buildDataShimSource, buildUserShimSource]) {
      const src = build(cfDeployConfig('chat'))
      expect(src).toContain("'__chat_boot_failures_version'")
      expect(src).toContain("(this.env.CF_VERSION && this.env.CF_VERSION.id) || ''")
      expect(src).toContain(
        "if (currentVersion === '' || failedVersion === currentVersion) {"
      )
      expect(src).toContain("{ status: 'boot-failed', failures, reason }")
      expect(src).toContain(
        "await this.ctx.storage.delete('__chat_boot_failures_version')"
      )
      expect(src).toContain("await this.ctx.storage.delete('__chat_boot_backoff_until')")
      const alarmStart = src.indexOf('  async alarm() {')
      const alarm = src.slice(alarmStart, src.indexOf('\n  }\n}', alarmStart))
      expect(alarm).toContain(
        "await this.ctx.storage.delete('__chat_boot_failure_reason')"
      )
      expect(alarm).toContain(
        "await this.ctx.storage.delete('__chat_boot_failures_version')"
      )
    }
  })

  it('loads the Orez backend inside a request instead of worker module startup', () => {
    for (const build of [buildDataShimSource, buildUserShimSource]) {
      const src = build(cfDeployConfig('contrast'))
      expect(src).toContain("import('contrast-do-backend')")
      expect(src).toContain('new (await getDoBackend())(')
      expect(src).not.toContain("import { DoBackend } from 'contrast-do-backend'")
    }
  })

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

  it('lets session reads reach auth while migration-dependent requests stay gated', async () => {
    for (const build of [buildUserShimSource, buildAppShimSource]) {
      const source = build(cfDeployConfig('chat'))
      expect(source).toContain('needsSqlSchema(request, url.pathname)')
      expect(source).not.toContain('needsSqlSchema(url.pathname)')
      const needsSqlSchema = generatedNeedsSqlSchema(source)
      let migrationAttempts = 0
      let handlerCalls = 0
      const dispatch = async (request: Request) => {
        const pathname = new URL(request.url).pathname
        if (needsSqlSchema(request, pathname)) {
          migrationAttempts += 1
          throw new Error('migration unavailable')
        }
        handlerCalls += 1
        return new Response('auth handler result', {
          status: 207,
          headers: { 'x-request-id': request.headers.get('x-request-id') ?? '' },
        })
      }

      for (const pathname of ['/api/auth/get-session?fresh=1', '/api/auth/me']) {
        const sessionResponse = await dispatch(
          new Request(`https://chat.example${pathname}`, {
            headers: { 'x-request-id': 'req-session-read' },
          })
        )
        expect(sessionResponse.status).toBe(207)
        expect(await sessionResponse.text()).toBe('auth handler result')
        expect(sessionResponse.headers.get('x-request-id')).toBe('req-session-read')
      }
      expect(migrationAttempts).toBe(0)
      expect(handlerCalls).toBe(2)

      for (const [method, pathname] of [
        ['POST', '/api/auth/sign-in/email'],
        ['GET', '/api/auth/callback/github'],
        ['POST', '/api/bootstrap-anon'],
        ['POST', '/api/dev-login'],
        ['POST', '/api/test-login'],
        ['POST', '/api/zero/push'],
      ]) {
        await expect(
          dispatch(new Request(`https://chat.example${pathname}`, { method }))
        ).rejects.toThrow('migration unavailable')
      }
      expect(migrationAttempts).toBe(6)
      expect(handlerCalls).toBe(2)
    }
  })

  it('leaves orez-internal tokens untouched regardless of prefix', () => {
    for (const prefix of ['contrast', 'chat']) {
      const src = buildDataShimSource(cfDeployConfig(prefix))
      // orez's own internal hosts/globals are shared, never prefixed
      expect(src).toContain('orez-data.local')
      expect(src).toContain('__orez_signal_replication')
    }
  })

  it('content-hash gates schema metadata while checking publications on boot', () => {
    expect(EMBED_WARM_TIMEOUT_MS).toBe(EMBED_READY_TIMEOUT_MS + 300_000)
    for (const build of [buildDataShimSource, buildUserShimSource]) {
      const src = build(cfDeployConfig('contrast'))
      expect(src).toContain(`readyTimeout: ${EMBED_READY_TIMEOUT_MS}`)
      expect(src).not.toContain('readyTimeout: 120000,')
      expect(src).toContain('const migrationResult = await this.migrateWithPublication()')
      expect(src).toContain('await this.migrateOnly()')
      expect(src).toContain('publicationOnly: true')
      expect(src).toContain('return migrationResult')
      expect(src).not.toContain("console.log('[contrast] boot step: migrateOnly done')")
      const migrateOnly = src.slice(
        src.indexOf('migrateOnly() {'),
        src.indexOf('migrateWithPublication() {')
      )
      expect(migrateOnly).not.toContain('return migrationResult')
      const publicationStart = src.indexOf('migrateWithPublication() {')
      const migrateWithPublication = src.slice(
        publicationStart,
        src.indexOf('async fetch(request) {', publicationStart)
      )
      expect(migrateWithPublication).toContain('return migrationResult')
    }

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
    expect(migrationSource).toContain('publicationOnly = false')
    expect(migrationSource).toContain(
      'CREATE TABLE IF NOT EXISTS _zero_schema_tables (name TEXT PRIMARY KEY, schema_json TEXT NOT NULL)'
    )
    expect(migrationSource).toContain(
      'INSERT OR REPLACE INTO _zero_schema_tables (name, schema_json) VALUES (?, ?)'
    )
    expect(migrationSource).toContain(
      'if (!publicationOnly) await applyInitSqlDDL(client, instance)'
    )
  })

  it('waits on an active deploy boot before reporting its terminal failure', async () => {
    const source = buildDataShimSource(cfDeployConfig('contrast'))
    const alarmStart = source.indexOf('  async alarm() {')
    const alarmEnd = source.indexOf('\n  }\n}', alarmStart)
    const alarm = source.slice(alarmStart, alarmEnd)
    expect(alarm).toContain('this.bootAlarmRunning = true')
    expect(alarm).toContain('} finally {')
    expect(alarm).toContain('this.bootAlarmRunning = false')
    expect(alarm.indexOf("storage.put('__contrast_boot_failures'")).toBeLessThan(
      alarm.indexOf('this.ready = undefined')
    )
    const start = source.indexOf("    if (pathname === '/keepalive') {")
    const end = source.indexOf('    // never park requests on a boot in flight', start)
    expect(start).toBeGreaterThan(-1)
    expect(end).toBeGreaterThan(start)
    const keepaliveBlock = source.slice(start, end)
    // eslint-disable-next-line typescript-eslint/no-implied-eval -- executes the generated worker branch to verify its deploy-only boot contract
    const candidate: unknown = new Function(
      `return async function(request) {
        const pathname = '/keepalive'
        ${keepaliveBlock}
      }`
    )()
    if (typeof candidate !== 'function') {
      throw new Error('generated keepalive branch is not callable')
    }
    const stored = new Map<string, unknown>([
      ['__contrast_boot_pending', false],
      ['__contrast_boot_failures', 0],
      ['__contrast_boot_failures_version', 'build-current'],
    ])
    const readBootState = vi.fn(async (key: string) => stored.get(key))
    const readAlarm = vi.fn(async () => null)
    const setAlarm = vi.fn(async () => {})
    let ready: Promise<void> | undefined
    const ensureReady = vi.fn(() => {
      ready ||= Promise.resolve()
    })
    const state = {
      zeroCache: undefined,
      get ready() {
        return ready
      },
      env: { CF_VERSION: { id: 'build-current' } },
      ctx: {
        storage: {
          get: readBootState,
          getAlarm: readAlarm,
          setAlarm,
          delete: vi.fn(),
        },
      },
      ensureReady,
      lastActiveAt: 0,
    }

    const first = await candidate.call(
      state,
      new Request('https://example.test/keepalive?deploy=1')
    )
    expect(first).toBeInstanceOf(Response)
    if (!(first instanceof Response)) throw new Error('first probe returned no response')
    expect(first.status).toBe(202)
    expect(await first.text()).toBe('booting')
    expect(ensureReady).toHaveBeenCalledTimes(1)

    const active = await candidate.call(
      state,
      new Request('https://example.test/keepalive?deploy=1')
    )
    expect(active).toBeInstanceOf(Response)
    if (!(active instanceof Response)) {
      throw new Error('active probe returned no response')
    }
    expect(active.status).toBe(202)
    expect(await active.text()).toBe('booting')
    expect(ensureReady).toHaveBeenCalledTimes(2)

    ready = undefined
    stored.set('__contrast_boot_pending', true)
    Object.assign(state, { bootAlarmRunning: true })
    readBootState.mockClear()
    readAlarm.mockClear()
    setAlarm.mockClear()
    ensureReady.mockClear()
    const resumed = await candidate.call(
      state,
      new Request('https://example.test/keepalive?deploy=1')
    )
    expect(resumed).toBeInstanceOf(Response)
    if (!(resumed instanceof Response)) {
      throw new Error('resumed probe returned no response')
    }
    expect(resumed.status).toBe(202)
    expect(await resumed.text()).toBe('booting')
    expect(readBootState).not.toHaveBeenCalled()
    expect(readAlarm).not.toHaveBeenCalled()
    expect(setAlarm).not.toHaveBeenCalled()
    expect(ensureReady).not.toHaveBeenCalled()

    Object.assign(state, { bootAlarmRunning: false })
    const reset = await candidate.call(
      state,
      new Request('https://example.test/keepalive?deploy=1')
    )
    expect(reset).toBeInstanceOf(Response)
    if (!(reset instanceof Response)) {
      throw new Error('reset probe returned no response')
    }
    expect(reset.status).toBe(202)
    expect(await reset.text()).toBe('booting')
    expect(readBootState).toHaveBeenCalledWith('__contrast_boot_pending')
    expect(readAlarm).toHaveBeenCalledTimes(1)
    expect(setAlarm).toHaveBeenCalledTimes(1)
    expect(ensureReady).not.toHaveBeenCalled()

    stored.set('__contrast_boot_pending', false)
    stored.set('__contrast_boot_failures', 2)
    stored.set('__contrast_boot_failure_reason', 'replica rank mismatch')
    const terminal = await candidate.call(
      state,
      new Request('https://example.test/keepalive?deploy=1')
    )
    expect(terminal).toBeInstanceOf(Response)
    if (!(terminal instanceof Response)) {
      throw new Error('terminal probe returned no response')
    }
    expect(terminal.status).toBe(409)
    expect(await terminal.json()).toEqual({
      status: 'boot-failed',
      failures: 2,
      reason: 'replica rank mismatch',
    })
    expect(ensureReady).not.toHaveBeenCalled()
  })

  it('clears a prior build terminal boot failure and retries on a new deploy', async () => {
    const source = buildDataShimSource(cfDeployConfig('contrast'))
    const start = source.indexOf("    if (pathname === '/keepalive') {")
    const end = source.indexOf('    // never park requests on a boot in flight', start)
    const keepaliveBlock = source.slice(start, end)
    // eslint-disable-next-line typescript-eslint/no-implied-eval -- executes the generated worker branch to verify version-gated boot recovery
    const candidate: unknown = new Function(
      `return async function(request) {
        const pathname = '/keepalive'
        ${keepaliveBlock}
      }`
    )()
    if (typeof candidate !== 'function') {
      throw new Error('generated keepalive branch is not callable')
    }
    const deleted: string[] = []
    const readStorage = vi.fn(async (key: string) => {
      if (key === '__contrast_boot_pending') return false
      if (key === '__contrast_boot_failures') return 4
      if (key === '__contrast_boot_failures_version') return 'old-build'
      if (key === '__contrast_boot_failure_reason') return 'stale failure'
      return undefined
    })
    let ready: Promise<void> | undefined
    const ensureReady = vi.fn(() => {
      ready ||= Promise.resolve()
    })
    const state = {
      zeroCache: undefined,
      bootAlarmRunning: false,
      get ready() {
        return ready
      },
      env: { CF_VERSION: { id: 'new-build' } },
      ctx: {
        storage: {
          get: readStorage,
          getAlarm: vi.fn(async () => null),
          setAlarm: vi.fn(),
          delete: vi.fn(async (key: string) => {
            deleted.push(key)
          }),
        },
      },
      ensureReady,
      lastActiveAt: 0,
    }

    const response = await candidate.call(
      state,
      new Request('https://example.test/keepalive?deploy=1')
    )
    expect(response).toBeInstanceOf(Response)
    if (!(response instanceof Response)) throw new Error('probe returned no response')
    expect(response.status).toBe(202)
    expect(await response.text()).toBe('booting')
    expect(ensureReady).toHaveBeenCalledTimes(1)
    expect(deleted).toContain('__contrast_boot_failures')
    expect(deleted).toContain('__contrast_boot_failure_reason')
    expect(deleted).toContain('__contrast_boot_failures_version')
    expect(deleted).toContain('__contrast_boot_backoff_until')
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

  it('applies native SQLite schema statements and registers public CDC tables', async () => {
    const migrationSource = buildMigrationModuleSource(cfDeployConfig('contrast'), {
      mode: 'native',
      schemaVersion: 'schema-v1',
      schemaImportSpecifier: './schema.js',
      nativeSqlStatements: [
        'CREATE TABLE IF NOT EXISTS item (id TEXT NOT NULL, label TEXT NOT NULL, count INTEGER NOT NULL)',
        'CREATE UNIQUE INDEX IF NOT EXISTS item_id_label_idx ON item (id, label)',
      ],
      publicTables: [{ table: 'item', publicTable: 'public.item' }],
      expectedTables: [
        {
          name: 'item',
          columns: [
            {
              name: 'id',
              notNull: true,
              primaryKeyOrder: 1,
              sqlType: 'text',
            },
            {
              name: 'label',
              notNull: true,
              primaryKeyOrder: 2,
              sqlType: 'text',
            },
            {
              name: 'count',
              notNull: true,
              primaryKeyOrder: 0,
              sqlType: 'integer',
            },
          ],
        },
      ],
    })
    const statements: Array<{ sql: string; params?: unknown[] }> = []
    const appliedMigrations = new Set<string>()
    const registrations: Array<{ table: string; publicTable: string }[]> = []
    const migrationEvents: string[] = []
    let itemLabelType = 'TEXT'
    let itemCountHasNull = false
    let itemCompositeIndex = true
    const globals = globalThis as typeof globalThis & {
      __contrast_cf_application_sql_client?: (instance: string) => unknown
      __contrast_test_native_migration_schema?: unknown
    }
    globals.__contrast_test_native_migration_schema = {
      tables: {
        item: {
          name: 'item',
          columns: { id: { type: 'string' }, label: { type: 'string' } },
          primaryKey: ['id', 'label'],
        },
      },
    }
    globals.__contrast_cf_application_sql_client = (instance) => {
      expect(instance).toBe('preexisting')
      return {
        async transaction(
          _compile: unknown,
          work: (tx: {
            exec(sql: string, params?: unknown[]): Promise<void>
            query(sql: string): Promise<Array<Record<string, unknown>>>
            registerTables(
              tables: Array<{ table: string; publicTable: string }>
            ): Promise<void>
          }) => Promise<void>
        ) {
          await work({
            async exec(sql, params) {
              statements.push({ sql, params })
              migrationEvents.push(sql)
              if (sql.startsWith('INSERT INTO "__contrast_cf_migrations"')) {
                appliedMigrations.add(String(params?.[0]))
              }
            },
            async query(sql) {
              if (sql.startsWith('SELECT id FROM "__contrast_cf_migrations"')) {
                return [...appliedMigrations].map((id) => ({ id }))
              }
              // the phantom-ledger reconcile reads live schema before trusting
              // the ledger. a fake that reports an empty database makes it
              // resurrect every applied statement.
              if (sql.startsWith('SELECT name, type FROM sqlite_master')) {
                return [
                  { name: 'item', type: 'table' },
                  ...(itemCompositeIndex
                    ? [{ name: 'item_id_label_idx', type: 'index' }]
                    : []),
                ]
              }
              if (sql.startsWith('SELECT m.name AS tableName')) {
                return [
                  {
                    tableName: 'item',
                    columnName: 'legacyId',
                    columnNotNull: 0,
                    columnPk: 1,
                    columnType: 'TEXT',
                  },
                  {
                    tableName: 'item',
                    columnName: 'id',
                    columnNotNull: 1,
                    columnPk: 0,
                    columnType: 'TEXT',
                  },
                  {
                    tableName: 'item',
                    columnName: 'label',
                    columnNotNull: 1,
                    columnPk: 0,
                    columnType: itemLabelType,
                  },
                  {
                    tableName: 'item',
                    columnName: 'count',
                    columnNotNull: 0,
                    columnPk: 0,
                    columnType: 'TEXT',
                  },
                ]
              }
              if (sql === 'PRAGMA index_list("item")') {
                return itemCompositeIndex
                  ? [{ name: 'item_id_label_idx', partial: 0, unique: 1 }]
                  : []
              }
              if (sql === 'PRAGMA index_info("item_id_label_idx")') {
                return [
                  { name: 'id', seqno: 0 },
                  { name: 'label', seqno: 1 },
                ]
              }
              if (sql === 'SELECT 1 FROM "item" WHERE "count" IS NULL LIMIT 1') {
                return itemCountHasNull ? [{ 1: 1 }] : []
              }
              return []
            },
            async registerTables(tables) {
              registrations.push(tables)
              migrationEvents.push('registerTables')
            },
          })
        },
        async registerTables(tables: Array<{ table: string; publicTable: string }>) {
          registrations.push(tables)
        },
      }
    }
    const moduleDirectory = await mkdtemp(
      join(tmpdir(), 'contrast-cf-native-migration-test-')
    )
    try {
      const executable = migrationSource
        .replace(
          'import { schema } from "./schema.js"',
          'const schema = globalThis.__contrast_test_native_migration_schema'
        )
        .replaceAll('export const ', 'const ')
      const modulePath = join(moduleDirectory, 'migration.mjs')
      await writeFile(modulePath, executable)
      const module = await import(pathToFileURL(modulePath).href)
      await module.runContrastCloudflareMigrations({
        schemaOnly: true,
        instance: 'preexisting',
      })
      const publicTables = [{ table: 'item', publicTable: 'public.item' }]
      expect(registrations).toEqual([publicTables, publicTables])
      await module.runContrastCloudflareMigrations({
        registrationOnly: true,
        instance: 'preexisting',
      })
      expect(registrations).toEqual([publicTables, publicTables, publicTables])
      expect(migrationEvents.indexOf('registerTables')).toBeLessThan(
        migrationEvents.indexOf(
          'CREATE TABLE IF NOT EXISTS item (id TEXT NOT NULL, label TEXT NOT NULL, count INTEGER NOT NULL)'
        )
      )
      await module.runContrastCloudflareMigrations({
        schemaOnly: true,
        instance: 'preexisting',
      })
      itemCompositeIndex = false
      await expect(
        module.runContrastCloudflareMigrations({
          schemaOnly: true,
          instance: 'preexisting',
        })
      ).rejects.toThrow(
        'application SQLite schema mismatch for item.id: expected primary-key position 1, found 0'
      )
      itemCompositeIndex = true
      itemCountHasNull = true
      await expect(
        module.runContrastCloudflareMigrations({
          schemaOnly: true,
          instance: 'preexisting',
        })
      ).rejects.toThrow(
        'application SQLite schema mismatch for item.count: expected NOT NULL, found nullable'
      )
      itemCountHasNull = false
      itemLabelType = 'INTEGER'
      await expect(
        module.runContrastCloudflareMigrations({
          schemaOnly: true,
          instance: 'preexisting',
        })
      ).rejects.toThrow(
        'application SQLite schema mismatch for item.label: expected text, found integer'
      )
    } finally {
      delete globals.__contrast_cf_application_sql_client
      delete globals.__contrast_test_native_migration_schema
      await rm(moduleDirectory, { recursive: true, force: true })
    }
    expect(
      statements.filter(
        (statement) =>
          statement.sql ===
          'CREATE TABLE IF NOT EXISTS item (id TEXT NOT NULL, label TEXT NOT NULL, count INTEGER NOT NULL)'
      )
    ).toHaveLength(1)
    expect(statements.map((statement) => statement.sql)).toContain(
      'CREATE TABLE IF NOT EXISTS _zero_schema_tables (name TEXT PRIMARY KEY, schema_json TEXT NOT NULL)'
    )
    expect(migrationSource).not.toContain("from 'pg'")
    expect(migrationSource).not.toContain('__contrast_cf_do_sql_fetch_by_instance')
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

  it('converges a ledgered CREATE TABLE whose live table is missing declared columns', async () => {
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
              { name: 'accountId', notNull: true, primaryKeyOrder: 0, sqlType: 'text' },
            ],
          },
        ],
      })
      const db = new DatabaseSync(':memory:')
      // the drifted reality: the table exists in a pre-CREATE-definition shape,
      // and the ledger says the CREATE already ran.
      db.exec('CREATE TABLE communityListing (id text PRIMARY KEY, "userId" text)')
      db.exec(
        'CREATE TABLE "__contrast_cf_migrations" (id TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)'
      )
      db.prepare(
        'INSERT INTO "__contrast_cf_migrations" (id, applied_at) VALUES (?, ?)'
      ).run('0001_old/migration.sql:0', 1)
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
      const columns = db.prepare('PRAGMA table_info("communityListing")').all() as Array<{
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
      expect(byName.get('accountId')).toEqual({ type: 'text', notnull: 1 })
      const indexes = db.prepare('PRAGMA index_list("communityListing")').all() as Array<{
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
  })

  it('routes configured zero pushes inside the data worker without app re-entry', () => {
    const src = buildDataShimSource(
      cfDeployConfig('contrast', {
        dataWorkerZeroPush: {
          module: '../../src/zero/cloudflareDataPush.server.ts',
          exportName: 'handleCloudflareDataZeroPush',
        },
      })
    )

    expect(src).toContain(
      'const mod = await import("../../src/zero/cloudflareDataPush.server.ts")'
    )
    expect(src).toContain('const handler = mod["handleCloudflareDataZeroPush"]')
    expect(src).toContain("url.pathname === '/api/zero/push'")
    expect(src).toContain('return handler({ request, env, ctx, instanceName })')
    expect(src).toContain(
      'return contrastZeroApiFetch(this.env, this.ctx, tagged, instance)'
    )
    expect(src).toContain(
      'return env.APP.fetch(appWorkerRequestForInternalZeroApi(request, env))'
    )
    expect(src).not.toContain(
      'return contrastZeroApiFetch(this.env, this.ctx, appWorkerRequestForInternalZeroApi(tagged, this.env))'
    )
  })

  it('the stored templates contain no literal contrast (neutralized)', async () => {
    // guard against a regeneration that forgets to neutralize.
    const mod = await import('./shims.js')
    const sourceText = mod.buildDataShimSource(cfDeployConfig('zz'))
    // with a nonsense prefix, no real contrast token can appear
    expect(sourceText).not.toMatch(/contrast/i)
  })

  it('app shim authorizes native zero websockets by their sec-protocol bearer', () => {
    const src = buildAppShimSource(cfDeployConfig('contrast'))
    // native (expo) clients have no cookie on the zero websocket; the session
    // bearer rides zero's sec-websocket-protocol encoding. extract the shim's
    // decoder and run it against the real zero wire format to pin it.
    const fnMatch = src.match(/function bearerFromSecProtocol\(request\) \{[\s\S]*?\n\}/)
    expect(fnMatch, 'bearer extraction missing from app shim').toBeTruthy()
    // eslint-disable-next-line typescript-eslint/no-implied-eval -- evaluates the decoder extracted from the generated shim source to pin it against the real zero wire format
    const bearerFromSecProtocol = new Function(`return ${fnMatch?.[0]}`)()
    const token = 'sess_abc.123-XYZ'
    // mirror zero-protocol's encodeSecProtocols exactly
    const bytes = new TextEncoder().encode(
      JSON.stringify({
        initConnectionMessage: ['initConnection', {}],
        authToken: token,
      })
    )
    const encoded = encodeURIComponent(
      btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(''))
    )
    expect(
      bearerFromSecProtocol({
        headers: new Headers({ 'sec-websocket-protocol': encoded }),
      })
    ).toBe(token)
    expect(bearerFromSecProtocol({ headers: new Headers() })).toBe('')
    expect(
      bearerFromSecProtocol({
        headers: new Headers({ 'sec-websocket-protocol': 'not-zero-encoding' }),
      })
    ).toBe('')
    // and the authorize subrequest forwards it as a bearer getAuthData accepts
    expect(src).toContain(
      "{ authorization: 'Bearer ' + bearer, host: authorizeUrl.host }"
    )
  })

  it('consumes orez ns-router instead of inlining the validation regex', () => {
    const cfg = cfDeployConfig('contrast')
    const ns_regex = /\/\^\(proj\|test\)-\[a-zA-Z0-9_-\]\{1,64\}\$\//
    // data + user tiers delegate routing to the lifted orez primitive
    for (const build of [buildDataShimSource, buildUserShimSource]) {
      const src = build(cfg)
      expect(src).toContain(
        `import { doInstanceNameForRequest as orezDoInstanceNameForRequest } from 'orez/worker/cf-do-shim'`
      )
      expect(src).toContain('orezDoInstanceNameForRequest(request, url, {')
      expect(src).toContain(`nsHeader: 'x-contrast-ns'`)
      expect(src).toContain(`controlPlaneNamespaces: ['contrast']`)
      // the inline ns-shape regex is fully lifted out
      expect(src, 'inline ns regex should be gone').not.toMatch(ns_regex)
    }
    // app tier validates the stamp via the shared isValidNamespace
    const app = buildAppShimSource(cfg)
    expect(app).toContain(`import { isValidNamespace } from 'orez/worker/cf-do-shim'`)
    expect(app).toContain('isValidNamespace(ns)')
    expect(app, 'inline ns regex should be gone').not.toMatch(ns_regex)
  })
})
