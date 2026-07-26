import { EMBED_READY_TIMEOUT_MS } from './leaves.js'

// Cloudflare/Orez worker-source shim TEMPLATES — the source of truth for the
// deployed Durable Object worker entries (data-tier, single-worker user, and
// split-app). extracted verbatim from contrast's historical inline shims and then
// neutralized: the per-deploy token (originally 'contrast') is stored as the sentinel
// 'nspfx'/'Nspfx', and buildXShimSource() substitutes it for CfDeployConfig.prefix
// at deploy time. orez's own internal tokens (orez-data.local,
// __orez_signal_replication, _orez_pg_metadata, …) are shared and left untouched.
//
// the templates are stored JSON-escaped because the worker source contains regex
// backslashes that a template literal would corrupt; they are now the source of
// truth for the worker runtime (a more readable raw-file storage is a tracked
// follow-up). consumers (contrast, chat's vendored copy) import the build* functions.
import type { CfDeployConfig } from './config.js'

const SENTINEL_LOWER = 'nspfx'
const SENTINEL_PASCAL = 'Nspfx'

function applyPrefix(template: string, cfg: CfDeployConfig): string {
  return template
    .split(SENTINEL_PASCAL)
    .join(cfg.prefixPascal)
    .split(SENTINEL_LOWER)
    .join(cfg.prefix)
}

function applySQLiteOnlyDataTransport(source: string, splitDataWorker: boolean): string {
  const poolFactory =
    "  globalThis.__nspfx_cf_do_create_pg_pool = (connectionString = '') =>\n" +
    '    new NspfxCloudflarePgPool({ connectionString })\n'
  const applicationClient =
    "  globalThis.__nspfx_cf_application_sql_client = (namespace = 'singleton') =>\n" +
    '    createApplicationSqlClient(env.ZERO_SQL_DO, namespace)\n'
  if (!source.includes(poolFactory)) {
    throw new Error('orez SQLite data shim pool factory shape changed')
  }
  let next = source
    .replace("import { Pool as NspfxCloudflarePgPool } from 'pg'\n", '')
    .replace(/let doBackendClassPromise\nfunction getDoBackend\(\) \{[\s\S]*?\n\}\n/, '')
    .replace(poolFactory, splitDataWorker ? '' : applicationClient)
    .replace('// pg-over-DO query endpoint.', '// database-over-DO query endpoint.')

  const routeStart = next.indexOf("    if (url.pathname === '/__nspfx_pg') {")
  const routeEnd = next.indexOf('    return super.fetch(request)', routeStart)
  if (routeStart < 0 || routeEnd < 0) {
    throw new Error('orez SQLite data shim transport shape changed')
  }
  next = next.slice(0, routeStart) + next.slice(routeEnd)
  next = next
    .replaceAll('/__nspfx_pg', '/exec')
    .replaceAll(
      "JSON.stringify({ text: 'SELECT id FROM project', values: [] })",
      "JSON.stringify({ sql: 'SELECT id FROM project', params: [] })"
    )
    .replaceAll(
      "JSON.stringify({ text: 'SELECT 1', values: [] })",
      "JSON.stringify({ sql: 'SELECT 1', params: [] })"
    )

  const legacyRouteStart = next.indexOf("    if (url.pathname === '/__nspfx_query') {")
  if (!splitDataWorker) {
    if (legacyRouteStart >= 0) {
      throw new Error('orez SQLite user shim unexpectedly has a split transport')
    }
    return next
  }
  const legacyRouteEnd = next.indexOf('    // schema-warmup', legacyRouteStart)
  if (legacyRouteStart < 0 || legacyRouteEnd < 0) {
    throw new Error('orez SQLite data shim legacy transport shape changed')
  }
  return (
    next.slice(0, legacyRouteStart) +
    "    if (url.pathname === '/__nspfx_query') {\n" +
    "      return new Response('legacy SQL transport retired', { status: 410 })\n" +
    '    }\n' +
    next.slice(legacyRouteEnd)
  )
}

function applySQLiteOnlyAppTransport(source: string, splitAppWorker: boolean): string {
  let next: string
  if (source.includes("import { ZeroDO as OrezZeroSqlDO } from 'orez/cf-do'\n")) {
    next = source.replace(
      "import { ZeroDO as OrezZeroSqlDO } from 'orez/cf-do'\n",
      "import { ZeroDO as OrezZeroSqlDO, createApplicationSqlClient } from 'orez/cf-do'\n"
    )
  } else if (
    source.includes("import { isValidNamespace } from 'orez/worker/cf-do-shim'\n")
  ) {
    next = source.replace(
      "import { isValidNamespace } from 'orez/worker/cf-do-shim'\n",
      "import { createApplicationSqlClient } from 'orez/cf-do'\nimport { isValidNamespace } from 'orez/worker/cf-do-shim'\n"
    )
  } else {
    throw new Error('orez SQLite app shim import shape changed')
  }

  const globalsBefore =
    "  globalThis.__nspfx_cf_do_namespace = env.ZERO_APP_ID || 'zero'\n" +
    '  globalThis.__nspfx_cf_do_create_pg_pool = () => makeRemotePgPool(env)\n'
  const globalsAfter =
    "  globalThis.__nspfx_cf_do_namespace = env.ZERO_APP_ID || 'zero'\n" +
    "  globalThis.__nspfx_cf_application_sql_client = (namespace = 'singleton') =>\n" +
    '    createApplicationSqlClient(env.ZERO_SQL_DO, namespace)\n'
  if (!splitAppWorker) {
    if (
      !next.includes('__nspfx_cf_application_sql_client') ||
      next.includes('makeRemotePgPool')
    ) {
      throw new Error('orez SQLite user shim application client shape changed')
    }
    return next
  }
  if (!next.includes(globalsBefore)) {
    throw new Error('orez SQLite app shim globals shape changed')
  }
  next = next
    .replace(globalsBefore, globalsAfter)
    .replace(/  \/\/ per-project pool:[\s\S]*?\n  if \(env\.FILES\)/, '  if (env.FILES)')

  const poolStart = next.indexOf('function makeRemotePgPool(')
  const poolEnd = next.indexOf(
    '// poke the data tier to run the schema migration',
    poolStart
  )
  if (poolStart < 0 || poolEnd < 0) {
    throw new Error('orez SQLite app shim remote pool shape changed')
  }
  return next.slice(0, poolStart) + next.slice(poolEnd)
}

// Schema DDL and its _orez_pg_metadata rows are content-hash gated, while
// publication membership is checked on every embed generation. Keep those as
// separate phases: the generated schema batch contains unconditional metadata
// upserts, so running the old combined path on every reconnect rewrote the full
// schema even when nothing changed.
function applyMigrationLifecycle(source: string): string {
  const bootBefore =
    '        await this.migrateOnly()\n' +
    "        console.log('[nspfx] boot step: migrateOnly done')\n" +
    '        const migrationResult = await runNspfxCloudflareMigrations({\n' +
    '          publications,\n' +
    '          instance,\n' +
    '        })'
  const bootAfter = '        const migrationResult = await this.migrateWithPublication()'
  const publicationStartBefore =
    '    this.publicationReady = (async () => {\n' +
    '      const instance = await this.loadInstanceName()\n' +
    '      installSqlBackendGlobals(this.env, instance)\n' +
    '      await runNspfxCloudflareMigrations({\n' +
    '        publications: parsePublications(this.env.ZERO_APP_PUBLICATIONS),\n' +
    '        instance,\n' +
    '      })'
  const publicationStartAfter =
    '    this.publicationReady = (async () => {\n' +
    '      await this.migrateOnly()\n' +
    '      const instance = await this.loadInstanceName()\n' +
    '      installSqlBackendGlobals(this.env, instance)\n' +
    '      const migrationResult = await runNspfxCloudflareMigrations({\n' +
    '        publications: parsePublications(this.env.ZERO_APP_PUBLICATIONS),\n' +
    '        publicationOnly: true,\n' +
    '        instance,\n' +
    '      })'
  const publicationEndBefore =
    "      await this.ctx.storage.put('__nspfx_schema_version', SCHEMA_VERSION)\n" +
    '    })()'
  const publicationEndAfter =
    "      await this.ctx.storage.put('__nspfx_schema_version', SCHEMA_VERSION)\n" +
    '      return migrationResult\n' +
    '    })()'
  const publicationCommentBefore =
    '// schema DDL + the zero publication, for project-namespace provisioning. the\n' +
    '  // publication must exist in durable metadata (_orez_pg_metadata) before the\n' +
    "  // first app write to this namespace, or that write's cached DoBackend\n" +
    '  // (ZeroSqlDO.nspfxPgBackend, loaded once) sees empty publications and skips\n' +
    '  // change-capture — the write lands in the table but emits no _zero_changes\n' +
    '  // row, so no poke ever reaches the client (the empty-fileTree blocker). runs\n' +
    '  // the FULL (non-schemaOnly) migration: applyInitSqlDDL is idempotent and\n' +
    '  // ensurePublication is CREATE-if-absent, so it converges with the identical\n' +
    "  // call bootEmbed makes on the first /sync. cached per DO via migrateOnly's\n" +
    '  // SCHEMA_VERSION guard for the DDL; the publication step is idempotent.'
  const publicationCommentAfter =
    '// schema DDL + the zero publication, for project-namespace provisioning. the\n' +
    '  // publication must exist in durable metadata (_orez_pg_metadata) before the\n' +
    "  // first app write to this namespace, or that write's cached DoBackend\n" +
    '  // (ZeroSqlDO.nspfxPgBackend, loaded once) sees empty publications and skips\n' +
    '  // change-capture — the write lands in the table but emits no _zero_changes\n' +
    '  // row, so no poke ever reaches the client (the empty-fileTree blocker).\n' +
    '  // migrateOnly content-hash gates the schema DDL and metadata batch; this\n' +
    '  // method then checks publication membership without replaying that batch.'

  if (
    !source.includes(bootBefore) ||
    !source.includes(publicationStartBefore) ||
    !source.includes(publicationEndBefore) ||
    !source.includes(publicationCommentBefore)
  ) {
    throw new Error('orez shim migration lifecycle shape changed')
  }
  const next = source
    .replace(bootBefore, bootAfter)
    .replace(publicationStartBefore, publicationStartAfter)
    .replace(publicationCommentBefore, publicationCommentAfter)
  const publicationStart = next.indexOf(publicationStartAfter)
  const publicationEnd = next.indexOf(
    publicationEndBefore,
    publicationStart + publicationStartAfter.length
  )
  if (publicationStart < 0 || publicationEnd < 0) {
    throw new Error('orez shim publication migration lifecycle shape changed')
  }
  return (
    next.slice(0, publicationEnd) +
    publicationEndAfter +
    next.slice(publicationEnd + publicationEndBefore.length)
  )
}

function applySqlSchemaGateContract(source: string): string {
  const predicateBefore =
    'function needsSqlSchema(pathname) {\n' +
    '  return (\n' +
    "    pathname.startsWith('/api/auth/') ||\n" +
    "    pathname.startsWith('/api/bootstrap-') ||\n" +
    "    pathname.startsWith('/api/zero/')\n" +
    '  )\n' +
    '}'
  const predicateAfter =
    'function needsSqlSchema(request, pathname) {\n' +
    "  if (request.method === 'GET' && (pathname === '/api/auth/get-session' || pathname === '/api/auth/me')) return false\n" +
    '  return (\n' +
    "    pathname.startsWith('/api/auth/') ||\n" +
    "    pathname.startsWith('/api/bootstrap-') ||\n" +
    "    pathname === '/api/dev-login' ||\n" +
    "    pathname === '/api/test-login' ||\n" +
    "    pathname.startsWith('/api/zero/')\n" +
    '  )\n' +
    '}'
  const callsiteBefore = 'needsSqlSchema(url.pathname)'
  const callsiteAfter = 'needsSqlSchema(request, url.pathname)'
  if (!source.includes(predicateBefore) || !source.includes(callsiteBefore)) {
    throw new Error('orez shim SQL schema gate shape changed')
  }
  return source
    .replace(predicateBefore, predicateAfter)
    .replace(callsiteBefore, callsiteAfter)
}

function applyRequestScopedDoBackend(source: string): string {
  const importBefore = "import { DoBackend } from 'nspfx-do-backend'\n"
  const importAfter = `let doBackendClassPromise
function getDoBackend() {
  doBackendClassPromise ||= import('nspfx-do-backend').then((module) => module.DoBackend)
  return doBackendClassPromise
}
`
  const constructorBefore = `this.nspfxPgBackend ||= new DoBackend(
        'https://orez-do-backend.local',
        'postgres',
        this.env.ZERO_APP_ID || 'zero',
        { fetch: (input, init) => super.fetch(new Request(input, init)) },
      )`
  const constructorAfter = `// assign the promise before the import yields so a cold concurrent burst
      // cannot construct several backends with independent operation mutexes.
      this.nspfxPgBackendPromise ||= Promise.resolve().then(
        async () =>
          new (await getDoBackend())(
            'https://orez-do-backend.local',
            'postgres',
            this.env.ZERO_APP_ID || 'zero',
            { fetch: (input, init) => super.fetch(new Request(input, init)) },
          ),
      )
      this.nspfxPgBackend ||= await this.nspfxPgBackendPromise`
  if (!source.includes(importBefore) || !source.includes(constructorBefore)) {
    throw new Error('orez shim DoBackend initialization shape changed')
  }
  return source
    .replace(importBefore, importAfter)
    .replace(constructorBefore, constructorAfter)
}

function applyDataWorkerZeroPush(source: string, cfg: CfDeployConfig): string {
  const push = cfg.dataWorkerZeroPush
  if (!push) return source

  const zeroApiFetchName = `${cfg.prefix}ZeroApiFetch`
  const callsite =
    `return ${zeroApiFetchName}(\n` +
    '              this.env,\n' +
    '              this.ctx,\n' +
    '              appWorkerRequestForInternalZeroApi(tagged, this.env),\n' +
    '            )'
  const nextCallsite = `return ${zeroApiFetchName}(this.env, this.ctx, tagged, instance)`
  if (!source.includes(callsite)) {
    throw new Error('orez data shim shape changed; zero push apiFetch callsite not found')
  }

  const envNeedle = 'function appWorkerRequestForInternalZeroApi(request, env) {'
  const dataEnvHelper =
    'function withDataProcessEnv(env, run) {\n' +
    '  globalThis.process ||= {}\n' +
    '  globalThis.process.env ||= {}\n' +
    '  const processEnv = globalThis.process.env\n' +
    '  const previous = new Map()\n' +
    '  for (const [key, value] of Object.entries(env)) {\n' +
    "    if (typeof value !== 'string') continue\n" +
    '    const hadPrevious = Object.prototype.hasOwnProperty.call(processEnv, key)\n' +
    '    previous.set(key, hadPrevious ? processEnv[key] : undefined)\n' +
    '    processEnv[key] = value\n' +
    '  }\n' +
    '  const restore = () => {\n' +
    '    for (const [key, value] of previous) {\n' +
    '      if (value === undefined) delete processEnv[key]\n' +
    '      else processEnv[key] = value\n' +
    '    }\n' +
    '  }\n' +
    '  try {\n' +
    '    const result = run()\n' +
    "    if (result && typeof result.then === 'function') return result.finally(restore)\n" +
    '    restore()\n' +
    '    return result\n' +
    '  } catch (err) {\n' +
    '    restore()\n' +
    '    throw err\n' +
    '  }\n' +
    '}\n\n'
  if (!source.includes(envNeedle)) {
    throw new Error('orez data shim shape changed; app request rewrite helper not found')
  }

  const oldFetch =
    `function ${zeroApiFetchName}(env, ctx, request) {\n` +
    '  return env.APP.fetch(request)\n' +
    '}'
  const newFetch =
    `async function ${zeroApiFetchName}(env, ctx, request, instanceName) {\n` +
    '  const url = new URL(request.url)\n' +
    "  if (url.hostname === 'orez-zero-api.local' && url.pathname === '/api/zero/push') {\n" +
    '    return withDataProcessEnv(env, async () => {\n' +
    '      // the push handler resolves SQL through the by-instance DO fetch\n' +
    "      // registry: its own instance for mutator transactions, and 'singleton'\n" +
    '      // for control-plane lookups (auth session/user). refresh both per\n' +
    '      // request — a DO stub is only valid for the request whose env made it.\n' +
    "      installSqlBackendGlobals(env, 'singleton')\n" +
    "      if (instanceName && instanceName !== 'singleton') {\n" +
    '        installSqlBackendGlobals(env, instanceName)\n' +
    '      }\n' +
    `      const mod = await import(${JSON.stringify(push.module)})\n` +
    `      const handler = mod[${JSON.stringify(push.exportName)}]\n` +
    "      if (typeof handler !== 'function') {\n" +
    `        throw new Error('data worker zero push handler export missing: ${push.exportName}')\n` +
    '      }\n' +
    '      return handler({ request, env, ctx, instanceName })\n' +
    '    })\n' +
    '  }\n' +
    '  return env.APP.fetch(appWorkerRequestForInternalZeroApi(request, env))\n' +
    '}'
  if (!source.includes(oldFetch)) {
    throw new Error('orez data shim shape changed; zero api fetch helper not found')
  }

  return source
    .replace(callsite, nextCallsite)
    .replace(envNeedle, dataEnvHelper + envNeedle)
    .replace(oldFetch, newFetch)
}

function serializeShimPgBatches(source: string): string {
  const previous = `        const results = []
        try {
          for (const stmt of body.batch) {
            const result = await this.nspfxPgBackend.query(stmt.text, stmt.values || [])
            results.push({ rows: result.rows || [], rowCount: result.rowCount })
          }
        } catch (err) {`
  const serialized = `        const results = []
        try {
          const batchResults = await this.nspfxPgBackend.queryBatch(
            body.batch.map((stmt) => ({
              sql: stmt.text,
              params: stmt.values || [],
            })),
          )
          results.push(
            ...batchResults.map((result) => ({
              rows: result.rows || [],
              rowCount: result.rowCount,
            })),
          )
        } catch (err) {`
  const previousCleanup = `        } catch (err) {
          // never leave the batch's transaction open on the shared backend —
          // a later request would silently join it.
          try {
            await this.nspfxPgBackend.query('ROLLBACK', [])
          } catch {}
          return Response.json(`
  const serializedCleanup = `        } catch (err) {
          return Response.json(`
  const previousError = `                'batch failed at statement ' +
                results.length +
                ': ' +
                String((err && err.message) || err),`
  const serializedError = `                'batch failed: ' +
                String((err && err.message) || err),`
  if (!source.includes(previous)) {
    throw new Error('orez data shim shape changed; pg batch loop not found')
  }
  if (!source.includes(previousCleanup)) {
    throw new Error('orez data shim shape changed; pg batch cleanup not found')
  }
  if (!source.includes(previousError)) {
    throw new Error('orez data shim shape changed; pg batch error not found')
  }
  return source
    .replace(previous, serialized)
    .replace(previousCleanup, serializedCleanup)
    .replace(previousError, serializedError)
}

function allowLargeReplicaResync(source: string): string {
  const previous = '          readyTimeout: 120000,'
  const extended = `          // a full snapshot can exceed ten minutes on a large DO. timing it
          // out turns one resync into repeated partial-replica repair writes.
          readyTimeout: ${EMBED_READY_TIMEOUT_MS},`
  if (!source.includes(previous)) {
    throw new Error('orez data shim shape changed; embed ready timeout not found')
  }
  return source.replace(previous, extended)
}

function passEmbedInstanceId(source: string): string {
  const previous = `        this.zeroCache = await startZeroCacheEmbedCF({
          doSqlite:`
  const withInstance = `        this.zeroCache = await startZeroCacheEmbedCF({
          instanceId: instance,
          log: (event) =>
            console.log('[nspfx] orez embed ' + JSON.stringify(event)),
          doSqlite:`
  if (!source.includes(previous)) {
    throw new Error('orez data shim shape changed; embed instance id call not found')
  }
  return source.replace(previous, withInstance)
}

function persistEmbedBootRequest(source: string): string {
  const scheduleBefore = `    this.ctx.storage.setAlarm(Date.now())`
  const scheduleAfter = `    this.ctx.waitUntil(
      (async () => {
        // issue these without an intervening await so durable object storage
        // commits the boot marker and its alarm in one implicit transaction.
        await Promise.all([
          this.ctx.storage.put(
            '__nspfx_instance_name',
            this.__nspfxInstanceName || 'singleton',
          ),
          this.ctx.storage.put('__nspfx_boot_pending', true),
          this.ctx.storage.setAlarm(Date.now()),
        ])
      })(),
    )`
  const alarmBefore = `    if (this.bootDeferred) {`
  const alarmAfter = `    if (
      this.bootDeferred ||
      (await this.ctx.storage.get('__nspfx_boot_pending'))
    ) {`
  const deferredBefore = `      const deferred = this.bootDeferred
      this.bootDeferred = undefined`
  const deferredAfter = `      if (!this.bootDeferred) {
        let resolveReady, rejectReady
        this.ready = new Promise((resolve, reject) => {
          resolveReady = resolve
          rejectReady = reject
        })
        this.bootDeferred = { resolve: resolveReady, reject: rejectReady }
        this.ready.catch(() => {})
      }
      const deferred = this.bootDeferred
      this.bootDeferred = undefined`
  const successBefore = `        deferred.resolve()
        await this.ctx.storage.delete('__nspfx_boot_failures')`
  const successAfter = `        deferred.resolve()
        await this.ctx.storage.delete('__nspfx_boot_pending')
        await this.ctx.storage.delete('__nspfx_boot_failures')`
  const failureBefore = `        this.ready = undefined
        console.log('[nspfx] zero-cache embed boot: failed: ' + (err && err.message))`
  const failureAfter = `        await this.ctx.storage.delete('__nspfx_boot_pending')
        this.ready = undefined
        console.log('[nspfx] zero-cache embed boot: failed: ' + (err && err.message))`
  const alarmHandlerBefore = `  async alarm() {`
  const alarmHandlerAfter = `  async alarm() {
    this.bootAlarmRunning = true
    try {`
  const alarmHandlerEndBefore = `    if (this.zeroCache) {
      // alarms are for lifecycle only. replication is write-driven via
      // ZeroSqlDO.nudgeReplication and orez's own poll loop; waking it here
      // creates a second replication driver that can replay retained batches
      // forever when feedback does not converge.
      await this.ctx.storage.setAlarm(Date.now() + idleCheckMs(this.env))
    }
  }
}`
  const alarmHandlerEndAfter = `    if (this.zeroCache) {
      // alarms are for lifecycle only. replication is write-driven via
      // ZeroSqlDO.nudgeReplication and orez's own poll loop; waking it here
      // creates a second replication driver that can replay retained batches
      // forever when feedback does not converge.
      await this.ctx.storage.setAlarm(Date.now() + idleCheckMs(this.env))
    }
    } finally {
      this.bootAlarmRunning = false
    }
  }
}`
  if (
    !source.includes(scheduleBefore) ||
    !source.includes(alarmBefore) ||
    !source.includes(deferredBefore) ||
    !source.includes(successBefore) ||
    !source.includes(failureBefore) ||
    !source.includes(alarmHandlerBefore) ||
    !source.includes(alarmHandlerEndBefore)
  ) {
    throw new Error('orez data shim shape changed; durable embed boot request not found')
  }
  return source
    .replace(scheduleBefore, scheduleAfter)
    .replace(alarmBefore, alarmAfter)
    .replace(deferredBefore, deferredAfter)
    .replace(successBefore, successAfter)
    .replace(failureBefore, failureAfter)
    .replace(alarmHandlerBefore, alarmHandlerAfter)
    .replace(alarmHandlerEndBefore, alarmHandlerEndAfter)
}

function addDeployTerminalWarmProbe(source: string): string {
  const previous = `    if (pathname === '/keepalive') {
      this.ensureReady()
      this.lastActiveAt = Date.now()
      return this.zeroCache
        ? new Response('ready')
        : new Response('booting', { status: 202 })
    }`
  const deployAware = `    if (pathname === '/keepalive') {
      // only deploy probes treat a completed boot failure as terminal. normal
      // client reconnects keep ensureReady's retry/backoff behavior unchanged.
      const deployProbe =
        new URL(request.url).searchParams.get('deploy') === '1'
      if (!this.zeroCache && deployProbe) {
        // an active alarm already owns boot state and storage. return before
        // touching storage so a deploy probe cannot join or reset that turn.
        if (this.bootAlarmRunning) {
          this.lastActiveAt = Date.now()
          return new Response('booting', { status: 202 })
        }
        const bootPending = await this.ctx.storage.get('__nspfx_boot_pending')
        if (bootPending) {
          // heal a boot marker left by an older worker or a storage reset that
          // has no alarm capable of consuming it.
          if ((await this.ctx.storage.getAlarm()) === null) {
            await this.ctx.storage.setAlarm(Date.now())
          }
          this.lastActiveAt = Date.now()
          return new Response('booting', { status: 202 })
        }
        if (!this.ready) {
          const failures =
            (await this.ctx.storage.get('__nspfx_boot_failures')) || 0
          if (failures > 0) {
            // terminal failures belong to the worker version that recorded
            // them. a repaired deploy clears an older build's state and gets
            // one fresh boot, while probes of the same failed build remain
            // terminal and cannot restart initial sync.
            const failedVersion = await this.ctx.storage.get(
              '__nspfx_boot_failures_version',
            )
            const currentVersion =
              (this.env.CF_VERSION && this.env.CF_VERSION.id) || ''
            if (currentVersion === '' || failedVersion === currentVersion) {
              const reason =
                (await this.ctx.storage.get('__nspfx_boot_failure_reason')) ||
                'unknown error'
              return Response.json(
                { status: 'boot-failed', failures, reason },
                { status: 409 },
              )
            }
            await this.ctx.storage.delete('__nspfx_boot_failures')
            await this.ctx.storage.delete('__nspfx_boot_failure_reason')
            await this.ctx.storage.delete('__nspfx_boot_failures_version')
            await this.ctx.storage.delete('__nspfx_boot_backoff_until')
          }
        }
      }
      this.ensureReady()
      this.lastActiveAt = Date.now()
      return this.zeroCache
        ? new Response('ready')
        : new Response('booting', { status: 202 })
    }`
  if (!source.includes(previous)) {
    throw new Error('orez data shim shape changed; keepalive probe not found')
  }
  return source.replace(previous, deployAware)
}

function persistBootFailureBeforeRetry(source: string): string {
  const commentBefore = `        // clear the cache so the next request retries a fresh boot; reject
        // the waiters. do NOT rethrow — the runtime would retry the alarm
        // with no deferred left to settle.`
  const commentAfter = `        // persist the failure before clearing ready so a deploy probe cannot
        // interleave here and schedule another boot without seeing terminal
        // state. do NOT rethrow — the runtime would retry the alarm with no
        // deferred left to settle.`
  const clearBeforePersist = `        this.ready = undefined
        const failures =`
  const persistFailures = `        await this.ctx.storage.put('__nspfx_boot_failures', failures)`
  const clearFailures = `        await this.ctx.storage.delete('__nspfx_boot_failures')`
  const failureLog = `        console.log('[nspfx] zero-cache embed boot: failed: ' + (err && err.message))`
  if (
    !source.includes(commentBefore) ||
    !source.includes(clearBeforePersist) ||
    !source.includes(persistFailures) ||
    !source.includes(clearFailures) ||
    !source.includes(failureLog)
  ) {
    throw new Error('orez data shim shape changed; boot failure handling not found')
  }
  return source
    .replace(commentBefore, commentAfter)
    .replace(clearBeforePersist, '        const failures =')
    .replace(
      persistFailures,
      `${persistFailures}
        await this.ctx.storage.put(
          '__nspfx_boot_failure_reason',
          String((err && err.message) || err || 'unknown error').slice(0, 1000),
        )
        await this.ctx.storage.put(
          '__nspfx_boot_failures_version',
          (this.env.CF_VERSION && this.env.CF_VERSION.id) || '',
        )`
    )
    .replace(
      clearFailures,
      `${clearFailures}
        await this.ctx.storage.delete('__nspfx_boot_failure_reason')
        await this.ctx.storage.delete('__nspfx_boot_failures_version')`
    )
    .replace(
      failureLog,
      `        this.ready = undefined
${failureLog}`
    )
}

const DATA_SHIM_TEMPLATE =
  "import { DurableObject, waitUntil as cfWaitUntil } from 'cloudflare:workers'\nimport { Pool as NspfxCloudflarePgPool } from 'pg'\nimport { DoBackend } from 'nspfx-do-backend'\nimport { ZeroDO as OrezZeroSqlDO } from 'orez/cf-do'\nimport { doInstanceNameForRequest as orezDoInstanceNameForRequest } from 'orez/worker/cf-do-shim'\nimport { installZeroSqlWriteCircuitBreaker } from 'orez/worker/zero-sql-write-circuit'\nimport {\n  clearChangeStreamerStateIfReplicaUninitialized as orezClearChangeStreamerStateIfReplicaUninitialized,\n  dropReplicaTables as orezDropReplicaTables,\n  healNullReplicaRank as orezHealNullReplicaRank,\n  repairPartialReplicaInit as orezRepairPartialReplicaInit,\n  resetReplicaIfChangeLogPoisoned as orezResetReplicaIfChangeLogPoisoned,\n  resetReplicaIfTableSetChanged as orezResetReplicaIfTableSetChanged,\n} from 'orez/worker/zero-cache-replica-repair'\nimport {\n  doSqliteStorage as orezDoSqliteStorage,\n  installDoForbiddenSqliteGuard,\n} from 'orez/worker/zero-cache-do-sqlite'\nimport {\n  shouldHibernateIdleZeroCache,\n  ZERO_CACHE_IDLE_CHECK_MS,\n  ZERO_CACHE_IDLE_GRACE_MS,\n} from 'orez/worker/zero-cache-do-idle'\nimport { runNspfxCloudflareMigrations, SCHEMA_VERSION } from './orez-migrations.js'\n\n// the zero-cache embed (~16 MiB) is still lazy — only ZeroCacheDO.ensureReady\n// (/sync) needs it; the app-write gate (migrateOnly) does NOT, so the SQL-backend\n// and migration paths keep full headroom.\nlet _startZeroCacheEmbedCF\nasync function getStartZeroCacheEmbedCF() {\n  if (!_startZeroCacheEmbedCF) {\n    _startZeroCacheEmbedCF = (await import('orez/worker/zero-cache-embed-cf'))\n      .startZeroCacheEmbedCF\n  }\n  return _startZeroCacheEmbedCF\n}\n\n// idle-hibernation timing, env-overridable per-deploy (set both low to validate\n// teardown quickly on a throwaway deploy). defaults from orez. THIS is the cost\n// win the separation unlocks: the lean data-tier DO has no resident app modules\n// keeping it hot, so once no sync client is connected it can tear the embed down\n// and evict, stopping GB-s accrual.\nfunction idleCheckMs(env) {\n  const v = Number(env.ZERO_CACHE_IDLE_CHECK_MS)\n  return Number.isFinite(v) && v > 0 ? v : ZERO_CACHE_IDLE_CHECK_MS\n}\nfunction idleGraceMs(env) {\n  const v = Number(env.ZERO_CACHE_IDLE_GRACE_MS)\n  return Number.isFinite(v) && v > 0 ? v : ZERO_CACHE_IDLE_GRACE_MS\n}\n\n// pg-over-DO query endpoint. the DoBackend (libpg parse + SQLite translate +\n// transaction emulation) must live INSIDE the DO: it holds cross-request state\n// (an init promise, BEGIN/COMMIT spanning several /__nspfx_query calls from one\n// bootstrap), and a stateless worker handler that awaits another request's\n// in-flight I/O hits workerd's \"hanging promise\" cancellation — the\n// intermittent /__nspfx_query hang that stalled fs reads. inside a DO instance,\n// cross-request state and pending I/O are legal.\nclass ZeroSqlDO extends OrezZeroSqlDO {\n  constructor(ctx, env) {\n    super(ctx, env)\n    installZeroSqlWriteCircuitBreaker(ctx.storage.sql, {\n      table: '_nspfx_write_circuit',\n      logPrefix: '[nspfx]',\n      // tightened after the 2026-07-08 runaway: the burn ran ~1M rows/min for\n      // ~8h — HALF the old 2M/min soft default, so the breaker never tripped.\n      // legit peaks are ~20-30k rows/min (embed boots, restores), so 200k/min\n      // sustained (3min) or a single 1M/min window is unambiguously a write\n      // loop. sticky once tripped; operate via /__nspfx_circuit (status/clear).\n      rowsPerWindow: 200_000,\n      hardRowsPerWindow: 1_000_000,\n    })\n  }\n\n  async fetch(request) {\n    // every inbound call (worker router or sibling DO) stamps the instance\n    // name; remember it so the replication nudge targets THIS namespace's\n    // ZeroCacheDO. memory-only: there is no request-less turn in this DO.\n    this.__nspfxInstanceName =\n      request.headers.get('x-nspfx-do-instance') || this.__nspfxInstanceName\n    const url = new URL(request.url)\n    if (url.pathname === '/__nspfx_pg') {\n      const body = await request.json()\n      this.nspfxPgBackend ||= new DoBackend(\n        'https://orez-do-backend.local',\n        'postgres',\n        this.env.ZERO_APP_ID || 'zero',\n        { fetch: (input, init) => super.fetch(new Request(input, init)) },\n      )\n      const isWrite = (text) => /^\\s*(insert|update|delete|with)/i.test(text || '')\n      // batch: run statements in order through the same backend in ONE\n      // request. every /__nspfx_pg call is an app-worker -> data-worker -> DO\n      // hop, so multi-statement flows (provisioning's BEGIN..COMMIT graph)\n      // send one batch instead of paying the hop per statement.\n      if (Array.isArray(body.batch)) {\n        const results = []\n        try {\n          for (const stmt of body.batch) {\n            const result = await this.nspfxPgBackend.query(stmt.text, stmt.values || [])\n            results.push({ rows: result.rows || [], rowCount: result.rowCount })\n          }\n        } catch (err) {\n          // never leave the batch's transaction open on the shared backend —\n          // a later request would silently join it.\n          try {\n            await this.nspfxPgBackend.query('ROLLBACK', [])\n          } catch {}\n          return Response.json(\n            {\n              error:\n                'batch failed at statement ' +\n                results.length +\n                ': ' +\n                String((err && err.message) || err),\n            },\n            { status: 500 },\n          )\n        }\n        if (body.batch.some((stmt) => isWrite(stmt.text))) {\n          this.bumpBackupMarker()\n          this.nudgeReplication()\n        }\n        return Response.json({ results })\n      }\n      try {\n        const result = await this.nspfxPgBackend.query(body.text, body.values || [])\n        // app-origin write: nudge the embed's replication loop. orez's poll\n        // loop parks in a setTimeout that workerd never fires once the DO has\n        // no active request context, so without an explicit nudge a write sits\n        // in _zero_changes until the next embed cold boot (frozen replica, no\n        // pokes). DO execution is event-driven; so is this.\n        if (isWrite(body.text)) {\n          this.bumpBackupMarker()\n          this.nudgeReplication()\n        }\n        return Response.json({ rows: result.rows || [], rowCount: result.rowCount })\n      } catch (err) {\n        return Response.json({ error: String((err && err.message) || err) }, { status: 500 })\n      }\n    }\n    return super.fetch(request)\n  }\n\n  // monotonic change marker for scheduled backups: every app-origin write\n  // request bumps write_seq, so the cron can ask \"anything new since the last\n  // export?\" with one cheap read instead of re-exporting cold namespaces\n  // forever. zero-cache's own bookkeeping writes (/exec from the embed) do\n  // NOT pass through here and deliberately don't bump — replication pruning\n  // is not user data.\n  bumpBackupMarker() {\n    const sql = this.ctx.storage.sql\n    if (!this.__nspfxBackupMetaReady) {\n      sql.exec(\n        'CREATE TABLE IF NOT EXISTS _nspfx_backup_meta (id INTEGER PRIMARY KEY CHECK (id = 1), write_seq INTEGER NOT NULL DEFAULT 0)',\n      )\n      this.__nspfxBackupMetaReady = true\n    }\n    sql.exec(\n      'INSERT INTO _nspfx_backup_meta (id, write_seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET write_seq = write_seq + 1',\n    )\n  }\n\n  // fire-and-forget, throttled to one per second: tells the SAME namespace's\n  // ZeroCacheDO to wake its replication poll loop and give it a slice of\n  // execution.\n  nudgeReplication() {\n    const now = Date.now()\n    if (this.lastReplNudge && now - this.lastReplNudge < 1000) {\n      // within the throttle window — do NOT drop this write's nudge. dropping\n      // stranded its _zero_changes until the embed's slow idle poll: a second\n      // write <1s after the first never woke the embed, so its poke waited ~5s\n      // under load and ~30s when idle (the bursty-write replication-latency\n      // bug). coalesce instead — schedule ONE trailing nudge at the window edge\n      // so the whole burst drains+pokes within ~1s. the embed's poll loop drains\n      // every pending _zero_changes per nudge, so one trailing nudge covers N\n      // accumulated writes; pendingReplNudge keeps it to a single timer.\n      if (!this.pendingReplNudge) {\n        this.pendingReplNudge = true\n        const delay = Math.max(0, 1000 - (now - this.lastReplNudge))\n        this.ctx.waitUntil(\n          new Promise((r) => setTimeout(r, delay)).then(() => {\n            this.pendingReplNudge = false\n            this.sendReplNudge()\n          }),\n        )\n      }\n      return\n    }\n    this.sendReplNudge()\n  }\n\n  sendReplNudge() {\n    this.lastReplNudge = Date.now()\n    const instance = this.__nspfxInstanceName || 'singleton'\n    const id = this.env.ZERO_CACHE_DO.idFromName(instance)\n    this.ctx.waitUntil(\n      this.env.ZERO_CACHE_DO.get(id)\n        .fetch('https://orez-data.local/__nspfx_repl_nudge', {\n          method: 'POST',\n          headers: { 'x-nspfx-do-instance': instance },\n        })\n        .catch(() => {}),\n    )\n  }\n}\n\nexport { ZeroSqlDO, ZeroSqlDO as ZeroDO }\n\nconst INTERNAL_ZERO_MUTATE_URL = 'https://orez-zero-api.local/api/zero/push'\nconst INTERNAL_ZERO_QUERY_URL = 'https://orez-zero-api.local/api/zero/pull'\n\nfunction stringEnv(env) {\n  const out = {}\n  for (const [key, value] of Object.entries(env)) {\n    if (typeof value === 'string') out[key] = value\n  }\n  return out\n}\n\nfunction parsePublications(value) {\n  return String(value || '')\n    .split(',')\n    .map((part) => part.trim())\n    .filter(Boolean)\n}\n\nfunction normalizeZeroCachePathname(pathname) {\n  const normalized = pathname.startsWith('/api/zero/')\n    ? pathname.slice('/api/zero'.length)\n    : pathname\n  if (normalized.startsWith('/sync/v') && normalized.endsWith('/connect/')) {\n    return normalized.slice(0, -1)\n  }\n  return normalized\n}\n\n// per-project ns client routing: the app shim forwards /p-<projectId>/ zero\n// traffic with its path UNMODIFIED — zero v51's dispatcher pattern\n// (/:base)/:worker/v:version/:action natively tolerates the one base\n// component — so the data tier only RECOGNIZES the prefixed shape to route\n// it to ZeroCacheDO; it never strips it. the ns decision itself arrives via\n// the x-nspfx-ns header the app shim stamps (see doInstanceNameForRequest).\nconst PROJECT_PREFIXED_ZERO_PATH = new RegExp(\n  '^/p-[A-Za-z0-9_-]{1,64}/(sync|replication|mutate)/v[0-9]+/',\n)\n\nfunction isZeroCachePath(pathname) {\n  if (PROJECT_PREFIXED_ZERO_PATH.test(pathname)) return true\n  const normalized = normalizeZeroCachePathname(pathname)\n  if (normalized.startsWith('/sync/v') && normalized.endsWith('/connect')) return true\n  if (normalized.startsWith('/replication/v')) return true\n  if (normalized === '/statz' || normalized === '/heapz' || normalized === '/keepalive') return true\n  return false\n}\n\nfunction zeroCacheRequestForUrl(request, url) {\n  const pathname = normalizeZeroCachePathname(url.pathname)\n  if (pathname === url.pathname) return request\n  const normalizedUrl = new URL(request.url)\n  normalizedUrl.pathname = pathname\n  return new Request(normalizedUrl.toString(), request)\n}\n\n// fetch into the GIVEN namespace instance's ZeroSqlDO. the instance name is\n// threaded from the worker router (x-nspfx-do-instance header) through every\n// DO so a per-namespace ZeroCacheDO embed talks to its OWN ZeroSqlDO, never\n// the control plane's 'singleton'.\nfunction sqlDoFetch(env, instance) {\n  return (input, init) => {\n    const request = new Request(input, init)\n    request.headers.set('x-nspfx-do-instance', instance)\n    const id = env.ZERO_SQL_DO.idFromName(instance)\n    return env.ZERO_SQL_DO.get(id).fetch(request)\n  }\n}\n\n// CF Durable Object SQLite forbids storage-engine config the DO manages itself —\n// PRAGMA journal_mode / synchronous / page_size / mmap_size, VACUUM, wal_checkpoint,\n// ATTACH. zero-cache's embedded replica issues these during setup and gets\n// \"not authorized: SQLITE_AUTH\", which aborts the whole /sync replication. the DO\n// already provides WAL-grade durability + a single managed page store, so these\n// are no-ops here: filter them out before exec instead of letting them throw.\nfunction installSqlBackendGlobals(env, instance) {\n  // keyed by DO instance, NOT a single mutable global: per-namespace DO\n  // instances of one class can share an isolate, and a lone global swapped\n  // per turn would cross-route one namespace's migration writes into\n  // another's storage. consumers resolve their entry at call time (a DO stub\n  // is bound to the request whose env produced it — see backendFor).\n  const byInstance = (globalThis.__nspfx_cf_do_sql_fetch_by_instance ||= {})\n  byInstance[instance] = sqlDoFetch(env, instance)\n  globalThis.__nspfx_cf_do_namespace = env.ZERO_APP_ID || 'zero'\n  globalThis.__nspfx_cf_do_create_pg_pool = (connectionString = '') =>\n    new NspfxCloudflarePgPool({ connectionString })\n  // hoist the R2 file bucket so src/deploy/s3.ts (which never sees env) routes\n  // file storage through the native binding instead of S3-over-HTTP. without it\n  // the app worker's s3Put hangs on an empty endpoint for 30s+ and OOMs.\n  if (env.FILES) globalThis.__nspfx_cf_r2_bucket = env.FILES\n}\n\n// app-worker WRITE paths that hit the SQL DO via db.pool (better-auth incl.\n// /api/bootstrap-*, the zero mutator push). these need the platform schema to\n// exist in ZeroSqlDO first; reads/assets/marketing skip the gate so the static\n// shell stays edge-fast.\nfunction needsSqlSchema(pathname) {\n  return (\n    pathname.startsWith('/api/auth/') ||\n    pathname.startsWith('/api/bootstrap-') ||\n    pathname.startsWith('/api/zero/')\n  )\n}\n\n// the platform schema (init.sql DDL — 42 tables + their indexes) is applied to\n// ZeroSqlDO by runNspfxCloudflareMigrations. ZeroCacheDO.ensureReady runs it, but\n// only on a /sync connection; an app-worker write (bootstrap-anon provisioning\n// user/project/workspace/...) can land first and 500 on a missing table. so gate\n// the first write per isolate on the idempotent migration. it issues ~118\n// single-statement /exec calls to the SQL DO the first time (a few seconds),\n// then the cached resolved promise is ~free. the DDL is IF-NOT-EXISTS so a\n// /sync-triggered run and this one converge.\n// poke ZeroCacheDO to run the schema migration in ITS context (where the DO SQL\n// stub is valid). cached per isolate so only the first write per cold isolate\n// pays the round-trip.\nlet appSchemaReady\nfunction ensureSchemaViaZeroCacheDO(env) {\n  if (appSchemaReady) return appSchemaReady\n  appSchemaReady = (async () => {\n    const id = env.ZERO_CACHE_DO.idFromName('singleton')\n    const res = await env.ZERO_CACHE_DO.get(id).fetch(\n      new Request('https://orez-zero-cache.local/__nspfx_migrate'),\n    )\n    if (!res.ok) throw new Error('schema migration failed: ' + res.status)\n    await res.text().catch(() => {})\n  })().catch((err) => {\n    appSchemaReady = undefined // let the next write retry rather than wedge\n    throw err\n  })\n  return appSchemaReady\n}\n\nfunction withAppProcessEnv(env, run) {\n  globalThis.process ||= {}\n  globalThis.process.env ||= {}\n  const processEnv = globalThis.process.env\n  const previous = new Map()\n  for (const key of ['ZERO_UPSTREAM_DB', 'ZERO_CVR_DB', 'ZERO_CHANGE_DB']) {\n    const hadPrevious = Object.prototype.hasOwnProperty.call(processEnv, key)\n    previous.set(key, hadPrevious ? processEnv[key] : undefined)\n    delete processEnv[key]\n  }\n  for (const [key, value] of Object.entries(env)) {\n    if (typeof value !== 'string') continue\n    if (previous.has(key)) continue\n    const hadPrevious = Object.prototype.hasOwnProperty.call(processEnv, key)\n    previous.set(key, hadPrevious ? processEnv[key] : undefined)\n    processEnv[key] = value\n  }\n  const restore = () => {\n    for (const [key, value] of previous) {\n      if (value === undefined) delete processEnv[key]\n      else processEnv[key] = value\n    }\n  }\n  try {\n    const result = run()\n    if (result && typeof result.then === 'function') {\n      return result.finally(restore)\n    }\n    restore()\n    return result\n  } catch (err) {\n    restore()\n    throw err\n  }\n}\n\nfunction appWorkerRequestForInternalZeroApi(request, env) {\n  const url = new URL(request.url)\n  if (url.hostname !== 'orez-zero-api.local') return request\n  const origin =\n    env.BETTER_AUTH_URL ||\n    (env.VITE_PROTOCOL && env.VITE_WEB_HOSTNAME\n      ? env.VITE_PROTOCOL + '://' + env.VITE_WEB_HOSTNAME\n      : undefined)\n  if (!origin) return request\n  const appUrl = new URL(url.pathname + url.search, origin)\n  const headers = new Headers(request.headers)\n  headers.set('host', appUrl.host)\n  return new Request(appUrl.toString(), {\n    method: request.method,\n    headers,\n    body: request.body,\n    redirect: request.redirect,\n  })\n}\n\n// adapt the SQL-DO backend fetch to the (sql, params) -> {rows,error} exec shape\n// the orez replica-repair guards expect.\nfunction nspfxBackendExec(env, instance) {\n  const backendFetch = sqlDoFetch(env, instance)\n  return (sql, params) =>\n    backendFetch('https://orez-do.local/exec', {\n      method: 'POST',\n      headers: { 'content-type': 'application/json' },\n      body: JSON.stringify(params ? { sql, params } : { sql }),\n    }).then((r) => r.json())\n}\n\nexport class ZeroCacheDO extends DurableObject {\n  constructor(ctx, env) {\n    super(ctx, env)\n    this.ctx = ctx\n    this.env = env\n    this.zeroCache = undefined\n    this.ready = undefined\n    this.migrated = undefined\n    this.lastActiveAt = Date.now()\n    // skip DO-forbidden storage-engine statements (VACUUM/ATTACH/checkpoint\n    // PRAGMAs) the embed issues but the DO rejects with SQLITE_AUTH.\n    installDoForbiddenSqliteGuard(ctx.storage.sql)\n  }\n\n  // which namespace instance this DO is. stamped on every routed request by\n  // the worker; persisted because the alarm-carried boot runs with NO request\n  // context (a post-eviction alarm must still know its namespace).\n  async loadInstanceName() {\n    if (!this.__nspfxInstanceName) {\n      this.__nspfxInstanceName =\n        (await this.ctx.storage.get('__nspfx_instance_name')) || 'singleton'\n    }\n    return this.__nspfxInstanceName\n  }\n\n  captureInstanceName(request) {\n    const name = request.headers.get('x-nspfx-do-instance')\n    if (name && name !== this.__nspfxInstanceName) {\n      this.__nspfxInstanceName = name\n      this.ctx.storage.put('__nspfx_instance_name', name)\n    }\n  }\n\n  ensureReady() {\n    if (this.ready) return this.ready\n    // the boot must NOT run in the requesting client's context: zero's client\n    // aborts its connect after ~10s (and page reloads abort sooner), and a\n    // canceled request kills its in-flight async work — observed live as a\n    // post-hibernation reconnect leaving this.ready cached on a promise that\n    // never settles, wedging every later connect until DO eviction. park a\n    // deferred and carry the boot in an immediate alarm: alarm handlers run\n    // in the DO's own context, survive client cancellation, and get a long\n    // wall budget (a full replica resync can exceed blockConcurrencyWhile's\n    // 30s cap, so that gate is not an option).\n    let resolveReady, rejectReady\n    this.ready = new Promise((resolve, reject) => {\n      resolveReady = resolve\n      rejectReady = reject\n    })\n    this.bootDeferred = { resolve: resolveReady, reject: rejectReady }\n    // requests no longer park on this promise (they shed 503 while booting),\n    // so a failed boot's rejection needs a handler or it surfaces as an\n    // unhandled rejection. callers who do await still see the rejection.\n    this.ready.catch(() => {})\n    this.ctx.storage.setAlarm(Date.now())\n    return this.ready\n  }\n\n  async bootEmbed() {\n        // one line per embed cold boot: ties tail output to the DO instance +\n        // shim build actually serving (deploys don't always reset live DOs).\n        console.log('[nspfx] zero-cache embed boot: starting')\n        const bootStart = Date.now()\n        if (this.env.AUTH_DB) globalThis.AUTH_DB = this.env.AUTH_DB\n        const appId = this.env.ZERO_APP_ID || 'zero'\n        const publications = parsePublications(this.env.ZERO_APP_PUBLICATIONS)\n        const instance = await this.loadInstanceName()\n        installSqlBackendGlobals(this.env, instance)\n        // ensure the schema tables exist (cheap, schemaOnly — shared with the\n        // app-write gate via the persisted version guard), THEN set up the\n        // publication for replication. ensurePublication's per-table schema\n        // registration is the parse-heavy step; it runs here on the /sync path\n        // (not the app-write gate) because only replication needs it.\n        await this.migrateOnly()\n        console.log('[nspfx] boot step: migrateOnly done')\n        const migrationResult = await runNspfxCloudflareMigrations({\n          publications,\n          instance,\n        })\n        console.log('[nspfx] boot step: migrations done')\n        await this.resetReplicaIfTableSetChanged(migrationResult && migrationResult.tables)\n        console.log('[nspfx] boot step: replica tag checked')\n        // heal a replica left half-initialized by a prior interrupted embed boot\n        // (DO no-op transactions can't roll back a killed initial-sync). without\n        // this the embed re-runs setup and dies on a duplicate CREATE.\n        this.repairPartialReplicaInit()\n        // a partial transaction persisted in the cdc changeLog (a DO kill mid\n        // storer-write; the DO sqlite shim can't roll back across turns) makes\n        // every catchup replay begin→data→begin, which stops the replicator\n        // permanently: the replica freezes while /sync keeps serving stale\n        // hydrations. detect it and wipe the replica so the guard below clears\n        // cdc and the boot re-runs initial sync from upstream (no data gaps —\n        // the partial tx's upstream rows were already purged on stream).\n        await this.resetReplicaIfChangeLogPoisoned(appId)\n        console.log('[nspfx] boot step: changelog checked')\n        // a NULL replicas.rank (row predates the DO serial emulation; BIGSERIAL\n        // makes NULL impossible on real pg) crash-loops zero 1.6's\n        // change-streamer at getReplicaAtVersion, and every restart re-streams\n        // the retained change set (2026-07 rows-written burn). backfill before\n        // the embed boots.\n        await this.healNullReplicaRank(appId)\n        // the change-streamer's subscription state lives in zero_cdb (ZeroSqlDO)\n        // and SURVIVES a replica wipe (reset/repair above, or an OOM eviction).\n        // a wiped replica + surviving subscription state makes zero-cache skip\n        // initial sync (\"already synced\") and serve an EMPTY replica that only\n        // ever receives catchup changes. when the replica has no init marker,\n        // clear the cdc state so the embed re-runs initial sync from scratch.\n        await this.clearChangeStreamerStateIfReplicaUninitialized(appId)\n        console.log('[nspfx] boot step: cdc checked, starting embed')\n        const envStrings = stringEnv(this.env)\n        delete envStrings.ZERO_UPSTREAM_DB\n        delete envStrings.ZERO_CVR_DB\n        delete envStrings.ZERO_CHANGE_DB\n        const backendFetch = sqlDoFetch(this.env, instance)\n        const zeroEnv = {\n          ...envStrings,\n          NODE_ENV: 'development',\n          ZERO_APP_ID: appId,\n          ZERO_MUTATE_URL: INTERNAL_ZERO_MUTATE_URL,\n          ZERO_QUERY_URL: INTERNAL_ZERO_QUERY_URL,\n          ZERO_MUTATE_FORWARD_COOKIES: this.env.ZERO_MUTATE_FORWARD_COOKIES || 'true',\n          ZERO_QUERY_FORWARD_COOKIES: this.env.ZERO_QUERY_FORWARD_COOKIES || 'true',\n          ZERO_NUM_SYNC_WORKERS: this.env.ZERO_NUM_SYNC_WORKERS || '1',\n          ZERO_SHADOW_SYNC_ENABLED: 'false',\n          ...(publications.length\n            ? { ZERO_APP_PUBLICATIONS: publications.join(',') }\n            : {}),\n        }\n        const startZeroCacheEmbedCF = await getStartZeroCacheEmbedCF()\n        this.zeroCache = await startZeroCacheEmbedCF({\n          doSqlite: orezDoSqliteStorage(this.ctx),\n          backendFetch,\n          backendNamespace: appId,\n          appId,\n          publications: publications.length ? publications : undefined,\n          env: zeroEnv,\n          apiFetch: (request) => {\n            // entry-defined seam (hoisted function declaration appended by\n            // each worker entry): the split data tier reaches the app worker\n            // over the APP service binding; the single-worker user deploy\n            // invokes the in-process One app. without the seam the user\n            // worker crashed on this.env.APP being undefined on every push.\n            //\n            // carry THIS embed's namespace to the app worker, or the mutator\n            // replay writes land in 'singleton' while the optimistic client\n            // copy is in proj-<id> (row appears then vanishes). instance is\n            // 'ns:proj-<id>' | 'singleton'; the data tier's ns channel value is\n            // the 'proj-<id>' suffix. the app shim reads this header to scope\n            // its zeroServer pool (see __nspfx_run_in_ns).\n            const ns = instance.startsWith('ns:') ? instance.slice(3) : ''\n            const tagged = new Request(request)\n            if (ns) tagged.headers.set('x-nspfx-ns', ns)\n            return nspfxZeroApiFetch(\n              this.env,\n              this.ctx,\n              appWorkerRequestForInternalZeroApi(tagged, this.env),\n            )\n          },\n          readyTimeout: 120000,\n        })\n        console.log(\n          '[nspfx] zero-cache embed boot: ready in ' + (Date.now() - bootStart) + 'ms',\n        )\n  }\n\n  // zero-cache snapshots the publication's tables into its replica (DO SQLite,\n  // ZERO_REPLICA_FILE=':do-sqlite:') ONCE during initial sync and never picks up\n  // a table OR COLUMN added to the publication afterward — ALTER only feeds the\n  // change stream, not the existing snapshot. so a redeploy that evolves the\n  // schema leaves the persisted replica stuck on the old shape and every client\n  // fails SchemaVersionNotSupported (2026-06-10: file.title/description columns\n  // — table set unchanged, so a tables-only tag never reset). key the tag on\n  // SCHEMA_VERSION (hash of the full deploy-time DDL batch — any table/column/\n  // type change) plus the table set, and wipe the replica on change so\n  // zero-cache re-runs initial sync over the full publication. the replica is\n  // derived data — upstream rows live in the SQL DO and are untouched.\n  async resetReplicaIfTableSetChanged(tables) {\n    await orezResetReplicaIfTableSetChanged(this.ctx.storage.sql, this.ctx.storage, {\n      schemaVersion: SCHEMA_VERSION,\n      tables,\n      tagKey: '__nspfx_replica_schema_tag',\n    })\n  }\n\n  // repair a PARTIALLY-INITIALIZED replica left by an interrupted embed boot.\n  // zero-cache's runSchemaMigrations wraps initial-sync (createReplicationStateTables\n  // + the versionHistory row write) in one BEGIN EXCLUSIVE/COMMIT, expecting it to\n  // be atomic. but on a CF DO the sqlite shim makes BEGIN/COMMIT/ROLLBACK NO-OPS\n  // (the DO auto-commits per I/O turn), and the setup migration is async (it awaits\n  // initialSync, which yields across turns). so if the boot is killed mid-migration\n  // — the 120s ready-timeout, a DO eviction, an OOM — the _zero.* tables auto-commit\n  // but the closing versionHistory INSERT never runs. next boot: getVersionHistory\n  // reads an empty table => dataVersion 0 => it re-runs the setup migration =>\n  // CREATE TABLE \"_zero.replicationConfig\" => \"already exists\" SQLITE_ERROR, and\n  // /sync never reaches ready (editor stuck on \"loading files\"). detect that exact\n  // inconsistency (replica data tables present but no versionHistory row) and wipe\n  // the _zero.* replica so the embed re-runs initial sync cleanly. the replica is\n  // derived data — upstream rows live in ZeroSqlDO and are untouched.\n  repairPartialReplicaInit() {\n    orezRepairPartialReplicaInit(this.ctx.storage.sql, { logPrefix: '[nspfx]' })\n  }\n\n  // see the call site in ensureReady: a changeLog transaction group without a\n  // commit entry is an interrupted storer write (zero stores each replicated tx\n  // inside one pg transaction; real pg rolls a crashed tx back, but the DO\n  // sqlite shim auto-commits per turn, so a kill persists the partial group).\n  // catchup replays it as begin→data→begin and the replicator dies on\n  // \"Already in a transaction\" on every boot. wiping the replica here makes the\n  // uninitialized-replica guard clear cdc state, forcing a clean initial sync.\n  async resetReplicaIfChangeLogPoisoned(appId) {\n    await orezResetReplicaIfChangeLogPoisoned(\n      this.ctx.storage.sql,\n      nspfxBackendExec(this.env, await this.loadInstanceName()),\n      { appId, logPrefix: '[nspfx]' },\n    )\n  }\n\n  // see the ensureReady call site: backfill NULL replicas.rank rows upstream\n  // so zero's replicaSchema bigint parse can't kill the change-streamer.\n  async healNullReplicaRank(appId) {\n    await orezHealNullReplicaRank(\n      nspfxBackendExec(this.env, await this.loadInstanceName()),\n      { appId, logPrefix: '[nspfx]' },\n    )\n  }\n\n  // see the call site in ensureReady: a replica without its init marker must\n  // not reuse the cdc subscription state, or initial sync never re-runs.\n  async clearChangeStreamerStateIfReplicaUninitialized(appId) {\n    await orezClearChangeStreamerStateIfReplicaUninitialized(\n      this.ctx.storage.sql,\n      nspfxBackendExec(this.env, await this.loadInstanceName()),\n      { appId, logPrefix: '[nspfx]' },\n    )\n  }\n\n  // run ONLY the platform-schema migration (init.sql DDL), cached, without\n  // booting the full zero-cache embed. an app-worker write needs the schema to\n  // exist but shouldn't pay the embed cold-start; this runs inside the DO's own\n  // context where this.env.ZERO_SQL_DO is a valid stub (the app-worker entry\n  // can't await a DO subrequest before its handler without wedging workerd).\n  // SCHEMA-ONLY warmup for the app-write gate (bootstrap/get-session): apply the\n  // table DDL so writes don't 500 on a missing table, but DEFER the publication\n  // setup. ensurePublication makes the pg-proxy register every published table's\n  // schema (~260 libpg parses) and THAT burst OOMs the 128 MiB isolate during the\n  // gate. the publication is only needed for /sync replication, so it runs later\n  // in ensureReady. cached per DO via the persisted SCHEMA_VERSION.\n  migrateOnly() {\n    if (this.migrated) return this.migrated\n    this.migrated = (async () => {\n      const appliedVersion = await this.ctx.storage.get('__nspfx_schema_version')\n      if (appliedVersion === SCHEMA_VERSION) return\n      // persist FIRST (the DDL /batch is idempotent IF-NOT-EXISTS, so re-applying\n      // on a retry is a no-op) so a memory-edge reset right after the batch can't\n      // wedge the DO in a re-migrate loop: once the batch has run, the schema is\n      // there; mark it applied before any further work can OOM-reset the isolate.\n      const instance = await this.loadInstanceName()\n      installSqlBackendGlobals(this.env, instance)\n      await runNspfxCloudflareMigrations({\n        publications: parsePublications(this.env.ZERO_APP_PUBLICATIONS),\n        schemaOnly: true,\n        instance,\n      })\n      await this.ctx.storage.put('__nspfx_schema_version', SCHEMA_VERSION)\n    })().catch((err) => {\n      this.migrated = undefined\n      throw err\n    })\n    return this.migrated\n  }\n\n  // schema DDL + the zero publication, for project-namespace provisioning. the\n  // publication must exist in durable metadata (_orez_pg_metadata) before the\n  // first app write to this namespace, or that write's cached DoBackend\n  // (ZeroSqlDO.nspfxPgBackend, loaded once) sees empty publications and skips\n  // change-capture — the write lands in the table but emits no _zero_changes\n  // row, so no poke ever reaches the client (the empty-fileTree blocker). runs\n  // the FULL (non-schemaOnly) migration: applyInitSqlDDL is idempotent and\n  // ensurePublication is CREATE-if-absent, so it converges with the identical\n  // call bootEmbed makes on the first /sync. cached per DO via migrateOnly's\n  // SCHEMA_VERSION guard for the DDL; the publication step is idempotent.\n  migrateWithPublication() {\n    if (this.publicationReady) return this.publicationReady\n    this.publicationReady = (async () => {\n      const instance = await this.loadInstanceName()\n      installSqlBackendGlobals(this.env, instance)\n      await runNspfxCloudflareMigrations({\n        publications: parsePublications(this.env.ZERO_APP_PUBLICATIONS),\n        instance,\n      })\n      // marks the schema applied; a later migrateOnly() reads this and returns\n      // early without redoing the DDL batch (same SCHEMA_VERSION storage guard).\n      await this.ctx.storage.put('__nspfx_schema_version', SCHEMA_VERSION)\n    })().catch((err) => {\n      this.publicationReady = undefined\n      throw err\n    })\n    return this.publicationReady\n  }\n\n  async fetch(request) {\n    if (this.env.AUTH_DB) globalThis.AUTH_DB = this.env.AUTH_DB\n    this.captureInstanceName(request)\n    installSqlBackendGlobals(this.env, await this.loadInstanceName())\n    const pathname = new URL(request.url).pathname\n    // replication nudge from ZeroSqlDO after an app write: wake the parked\n    // poll loop and hold this request context open briefly so the drain pass\n    // (query _zero_changes → stream → apply → poke) actually gets execution —\n    // workerd never fires the loop's idle timer without a live context. never\n    // boots the embed (a later cold boot's catchup drains the backlog), and\n    // deliberately does NOT bump lastActiveAt (a nudge is not client activity\n    // and must not block idle teardown).\n    if (pathname === '/__nspfx_repl_nudge') {\n      // a nudge during embed boot (fresh-user provisioning races the first\n      // /sync cold start) must WAIT for the boot, not drop: the boot request's\n      // context dies right after ensureReady, stranding in-flight replication\n      // work — this held nudge is the context that carries it through.\n      if (!this.zeroCache && this.ready) await this.ready.catch(() => {})\n      if (!this.zeroCache) return new Response(null, { status: 204 })\n      const signal = globalThis.__orez_signal_replication\n      if (typeof signal === 'function') {\n        signal()\n        // the full drain (stream -> changeLog -> replicator apply -> CVR ->\n        // poke) needs a longer slice than the stream alone; 1500ms stored the\n        // changes but pokes never made it out.\n        await new Promise((resolve) => setTimeout(resolve, 5000))\n      }\n      return new Response('ok')\n    }\n    // post-restore derived-state wipe (called by the data worker's\n    // /__nspfx_import): the replica + embed-local CVR/change-db in THIS DO now\n    // describe pre-restore data. stop the embed and drop every non-internal\n    // table plus the replica schema tag so the next /sync cold-boots a full\n    // initial sync over the restored upstream rows. same drop scope as\n    // resetReplicaIfTableSetChanged — all of it is derived data.\n    if (pathname === '/__nspfx_reset_derived') {\n      await this.ctx.blockConcurrencyWhile(async () => {\n        if (this.zeroCache) {\n          const stopping = this.zeroCache\n          this.zeroCache = undefined\n          this.ready = undefined\n          await stopping.stop()\n        }\n        orezDropReplicaTables(this.ctx.storage.sql)\n        await this.ctx.storage.delete('__nspfx_replica_schema_tag')\n      })\n      console.log('[nspfx] derived state reset after restore')\n      return new Response('ok')\n    }\n    this.lastActiveAt = Date.now()\n    // cheap schema-only warmup: run the migration without the embed cold-start.\n    // ?publication=1 (project-namespace provisioning) ALSO creates the zero\n    // publication now, instead of deferring it to the first /sync boot. the\n    // change-capture that turns an app write into a _zero_changes row is gated\n    // on the writing DoBackend's publication membership (orez\n    // trackingForStatement); that backend (ZeroSqlDO.nspfxPgBackend) loads its\n    // publications ONCE at construction and never reloads, so the seed write\n    // that follows provisioning must find the publication already persisted in\n    // _orez_pg_metadata — otherwise every project-ns write persists but emits\n    // no change/poke and the client never sees it (the empty-fileTree blocker).\n    // the app-write schema gate keeps the schema-only fast path (no publication)\n    // because its ~260-parse burst OOMs the 128 MiB isolate mid-serve; a\n    // provision is a one-shot off the serving path, so it can afford it.\n    if (pathname === '/__nspfx_migrate') {\n      if (new URL(request.url).searchParams.get('publication') === '1') {\n        await this.migrateWithPublication()\n      } else {\n        await this.migrateOnly()\n      }\n      return new Response('ok')\n    }\n    // readiness probe: kick the alarm-carried boot, then report whether the\n    // embed has FINISHED booting WITHOUT blocking on it. this.zeroCache flips\n    // truthy only once initial-sync completes and the view-syncer can hydrate;\n    // a raw /sync websocket opens (101) long before that, so 101 is NOT a\n    // readiness signal — a client connecting mid-boot gets baseCookie=null and\n    // times out after 10s. 200 here means the very next /sync will hydrate.\n    // the deploy polls this so neither the runtime validation nor the first\n    // real visitor races a half-booted embed. cheap, non-blocking, idempotent\n    // (ensureReady returns the in-flight boot promise on repeat calls).\n    if (pathname === '/keepalive') {\n      this.ensureReady()\n      this.lastActiveAt = Date.now()\n      return this.zeroCache\n        ? new Response('ready')\n        : new Response('booting', { status: 202 })\n    }\n    // never park requests on a boot in flight: a wedged client reconnecting\n    // against a slow cold boot stacks unbounded pending upgrades inside the\n    // isolate (each held for the full 60-120s boot), which helped push\n    // fresh-namespace initial sync over the 128 MiB isolate limit (2026-07-09\n    // OOM-reset loop). kick the boot and shed with a retryable 503 until\n    // ready. zero clients reconnect with backoff, and /keepalive above has\n    // always had this non-parking shape.\n    this.ensureReady()\n    if (!this.zeroCache) {\n      return new Response('zero-cache booting', {\n        status: 503,\n        headers: { 'retry-after': '2' },\n      })\n    }\n    return this.zeroCache.handleRequest(request, { waitUntil: cfWaitUntil })\n  }\n\n  // periodic idle check: when no sync client is connected past the grace window,\n  // tear the embed down so the DO evicts and stops accruing GB-s; the next\n  // request cold-starts it from DO SQLite.\n  async alarm() {\n    // a parked boot takes priority (see ensureReady): run it here in the\n    // DO's own context so client cancellation can't kill it mid-flight.\n    if (this.bootDeferred) {\n      // consecutive-failure backoff: a boot that keeps dying (the OOM-reset\n      // loop) must not retry at client-reconnect rate, because every cycle\n      // re-drops the replica and churns slots/DDL/metadata upstream (the\n      // 2026-07-09 rows-written burn). persisted so an isolate reset (the\n      // OOM case) cannot clear it.\n      const notBefore =\n        (await this.ctx.storage.get('__nspfx_boot_backoff_until')) || 0\n      if (Date.now() < notBefore) {\n        await this.ctx.storage.setAlarm(notBefore)\n        return\n      }\n      const deferred = this.bootDeferred\n      this.bootDeferred = undefined\n      try {\n        await this.bootEmbed()\n        deferred.resolve()\n        await this.ctx.storage.delete('__nspfx_boot_failures')\n        await this.ctx.storage.delete('__nspfx_boot_backoff_until')\n      } catch (err) {\n        // clear the cache so the next request retries a fresh boot; reject\n        // the waiters. do NOT rethrow — the runtime would retry the alarm\n        // with no deferred left to settle.\n        this.ready = undefined\n        const failures =\n          ((await this.ctx.storage.get('__nspfx_boot_failures')) || 0) + 1\n        await this.ctx.storage.put('__nspfx_boot_failures', failures)\n        if (failures >= 2) {\n          const delayMs = Math.min(15000 * 2 ** (failures - 2), 300000)\n          await this.ctx.storage.put(\n            '__nspfx_boot_backoff_until',\n            Date.now() + delayMs\n          )\n          console.log(\n            '[nspfx] zero-cache embed boot: backing off ' +\n              delayMs +\n              'ms after ' +\n              failures +\n              ' consecutive failures'\n          )\n        }\n        console.log('[nspfx] zero-cache embed boot: failed: ' + (err && err.message))\n        deferred.reject(err)\n      }\n      // fall through: re-arm below via the regular cadence logic.\n    }\n    if (!this.zeroCache) return\n    const idle = shouldHibernateIdleZeroCache({\n      connectionCount: this.zeroCache.connectionCount,\n      idleMs: Date.now() - (this.lastActiveAt || 0),\n      graceMs: idleGraceMs(this.env),\n    })\n    if (idle) {\n      // tear down under a concurrency gate so no request boots a second embed\n      // mid-stop (the embed mutates shared globals).\n      console.log('[nspfx] zero-cache idle teardown: starting')\n      await this.ctx.blockConcurrencyWhile(async () => {\n        if (!this.zeroCache || this.zeroCache.connectionCount > 0) return\n        const stopping = this.zeroCache\n        this.zeroCache = undefined\n        this.ready = undefined\n        await stopping.stop()\n      })\n      // the next connect reboots in place via ensureReady's alarm-carried\n      // boot. this requires orez >= the generation-safe proxy fix (instance-\n      // scoped schema caches + no leaked pipeline mutexes) — earlier builds\n      // wedged every second-generation embed start in the same isolate.\n      console.log('[nspfx] zero-cache idle teardown: stopped cleanly')\n    }\n    // re-arm only while the embed is still up; a torn-down DO leaves no alarm\n    // pending, which is what lets it evict.\n    if (this.zeroCache) {\n      // alarms are for lifecycle only. replication is write-driven via\n      // ZeroSqlDO.nudgeReplication and orez's own poll loop; waking it here\n      // creates a second replication driver that can replay retained batches\n      // forever when feedback does not converge.\n      await this.ctx.storage.setAlarm(Date.now() + idleCheckMs(this.env))\n    }\n  }\n}\n\n// the DATA-TIER worker's entry. it only ever receives internal calls from the app\n// worker over the service binding (env.OREZ_DATA.fetch): the SQL DO backend\n// (/__nspfx_sql -> ZeroSqlDO /exec|/batch), the schema-migration poke\n// (/__nspfx_migrate), and the zero-cache /sync* traffic. it routes each to its\n// in-process DOs. NO One app here.\n// per-project sharding seam: an explicit x-nspfx-ns header (or ?ns= param)\n// routes to that namespace's OWN DO pair; absent means the control-plane\n// namespace, which keeps its historical instance name 'singleton' so\n// existing storage stays addressed. names are validated to the proj-/test-\n// shape so a stray header can't mint unbounded DO instances.\nfunction doInstanceNameForRequest(request, url) {\n  return orezDoInstanceNameForRequest(request, url, {\n    nsHeader: 'x-nspfx-ns',\n    controlPlaneNamespaces: ['nspfx'],\n  })\n}\n\n// ---- namespace backup/restore (streaming, R2 multipart) ----\n//\n// format: NDJSON, one JSON object per line —\n//   { kind: 'header', format: 'nspfx-backup-v2', ns, exportedAt, marker }\n//   { kind: 'table', name, sql, indexes: [createIndexSql, ...] }\n//   { kind: 'rows', table, rows: [{col: value, ...}, ...] }   (repeated)\n//   { kind: 'footer', tables, rows }\n// the footer is the completeness proof: R2 multipart uploads are invisible\n// until complete(), so a crashed export never leaves a partial object, and\n// restore additionally refuses a dump without a matching footer row count.\n// memory bound: one row page (adaptive, ~BACKUP_CHUNK_TARGET_BYTES) plus at\n// most one part buffer (BACKUP_PART_BYTES) is resident at a time — never the\n// whole dataset, which is what OOMed-by-design the v1 whole-dump-JSON path.\nconst BACKUP_PART_BYTES = 8 * 1024 * 1024\nconst BACKUP_CHUNK_TARGET_BYTES = 2 * 1024 * 1024\nconst BACKUP_KEEP = 10\n// the control plane is the blast-radius namespace — the 2026-07-08 recovery\n// restored prod from its most recent dump, and drill/deploy exports churn its\n// window faster than the 6h cron alone. keep a deeper history there\n// (~a week at the combined cadence); project namespaces stay at 10.\nconst BACKUP_KEEP_SINGLETON = 30\nconst BACKUP_RUN_BUDGET_MS = 10 * 60 * 1000\n\nfunction qid(name) {\n  return String(name).replaceAll('\"', '\"\"')\n}\n\nfunction backupPrefix(instanceName) {\n  return 'backups/' + instanceName.replace(':', '/') + '/'\n}\n\n// replication/change-capture bookkeeping never travels through a backup:\n// restoring it re-seeds a retained change-log backlog the fresh consumer can\n// never confirm, and every embed boot then re-streams the whole set forever\n// (the 2026-07 DO rows-written burn). orez recreates these empty on boot, and\n// the post-restore derived reset re-snapshots the replica, so a restored\n// namespace needs none of this state.\nconst REPLICATION_BOOKKEEPING_TABLES = [\n  '_zero_changes',\n  '_zero_pending_changes',\n  '_zero_change_state',\n  '_orez___zero_watermark',\n  '_orez___zero_streamed_batches',\n  '_orez__zero_replication_slots',\n]\nfunction isReplicationBookkeepingTable(name) {\n  return REPLICATION_BOOKKEEPING_TABLES.includes(String(name))\n}\n\n// raw sqlite read/write against ONE namespace's ZeroSqlDO. /exec and /batch\n// are orez's internal DO endpoints — they never appear on the public router,\n// only built here worker-side, so this is not a public SQL surface.\nasync function sqlDoExec(env, instanceName, sql, params) {\n  const id = env.ZERO_SQL_DO.idFromName(instanceName)\n  const res = await env.ZERO_SQL_DO.get(id).fetch(\n    new Request('https://orez-data.local/exec', {\n      method: 'POST',\n      headers: {\n        'content-type': 'application/json',\n        'x-nspfx-do-instance': instanceName,\n      },\n      body: JSON.stringify({ sql, params: params || [] }),\n    }),\n  )\n  const body = await res.json()\n  if (!res.ok || body.error) {\n    throw new Error('backup sql failed: ' + (body.error || res.status))\n  }\n  return body.rows || []\n}\n\nasync function sqlDoBatch(env, instanceName, statements) {\n  const id = env.ZERO_SQL_DO.idFromName(instanceName)\n  const res = await env.ZERO_SQL_DO.get(id).fetch(\n    new Request('https://orez-data.local/batch', {\n      method: 'POST',\n      headers: {\n        'content-type': 'application/json',\n        'x-nspfx-do-instance': instanceName,\n      },\n      body: JSON.stringify({ statements }),\n    }),\n  )\n  if (!res.ok) {\n    const body = await res.json().catch(() => ({}))\n    throw new Error('restore batch failed: ' + (body.error || res.status))\n  }\n  await res.json().catch(() => {})\n}\n\nasync function readBackupMarker(env, instanceName) {\n  try {\n    const rows = await sqlDoExec(\n      env,\n      instanceName,\n      'SELECT write_seq FROM _nspfx_backup_meta WHERE id = 1',\n      [],\n    )\n    return Number(rows[0] && rows[0].write_seq) || 0\n  } catch (err) {\n    if (/no such table/i.test(String((err && err.message) || err))) return 0\n    throw err\n  }\n}\n\nasync function exportNamespace(env, instanceName) {\n  const exportedAt = new Date().toISOString()\n  // read the marker BEFORE the table scan: writes landing mid-export keep the\n  // marker ahead of latest.json, so the next cron re-exports them.\n  const marker = await readBackupMarker(env, instanceName)\n  const master = await sqlDoExec(\n    env,\n    instanceName,\n    \"SELECT name, sql, type, tbl_name FROM sqlite_master WHERE type IN ('table', 'index') AND sql IS NOT NULL ORDER BY name\",\n    [],\n  )\n  // sqlite_/_cf_ are engine/platform internals (DROP on _cf_* is SQLITE_AUTH\n  // denied on restore); _orez_tx_* are mid-transaction scratch tables; the\n  // write circuit is local protection state, not user data.\n  const skip = (name) =>\n    String(name).startsWith('sqlite_') ||\n    String(name).startsWith('_cf_') ||\n    String(name).startsWith('_orez_tx_') ||\n    String(name) === '_nspfx_write_circuit' ||\n    isReplicationBookkeepingTable(name)\n  const tables = master.filter((row) => row.type === 'table' && !skip(row.name))\n  const indexes = master.filter(\n    (row) => row.type === 'index' && !skip(row.name) && !skip(row.tbl_name),\n  )\n  const key = backupPrefix(instanceName) + Date.now() + '.ndjson'\n  const upload = await env.FILES.createMultipartUpload(key)\n  const uploadedParts = []\n  const encoder = new TextEncoder()\n  let chunks = []\n  let bufferedBytes = 0\n  let totalBytes = 0\n  // R2 requires every part except the last to be the SAME size: cut exact\n  // BACKUP_PART_BYTES slices off the buffered stream.\n  const flushParts = async (final) => {\n    if (!final && bufferedBytes < BACKUP_PART_BYTES) return\n    let merged = new Uint8Array(bufferedBytes)\n    let offset = 0\n    for (const chunk of chunks) {\n      merged.set(chunk, offset)\n      offset += chunk.byteLength\n    }\n    while (merged.byteLength >= BACKUP_PART_BYTES) {\n      uploadedParts.push(\n        await upload.uploadPart(uploadedParts.length + 1, merged.slice(0, BACKUP_PART_BYTES)),\n      )\n      merged = merged.slice(BACKUP_PART_BYTES)\n    }\n    if (final && (merged.byteLength > 0 || uploadedParts.length === 0)) {\n      uploadedParts.push(await upload.uploadPart(uploadedParts.length + 1, merged))\n      merged = new Uint8Array(0)\n    }\n    chunks = merged.byteLength ? [merged] : []\n    bufferedBytes = merged.byteLength\n  }\n  const writeLine = async (value) => {\n    const bytes = encoder.encode(JSON.stringify(value) + '\\n')\n    chunks.push(bytes)\n    bufferedBytes += bytes.byteLength\n    totalBytes += bytes.byteLength\n    await flushParts(false)\n    return bytes.byteLength\n  }\n  let rowTotal = 0\n  try {\n    await writeLine({\n      kind: 'header',\n      format: 'nspfx-backup-v2',\n      ns: instanceName,\n      exportedAt,\n      marker,\n    })\n    for (const table of tables) {\n      await writeLine({\n        kind: 'table',\n        name: table.name,\n        sql: table.sql,\n        indexes: indexes\n          .filter((index) => index.tbl_name === table.name)\n          .map((index) => index.sql),\n      })\n      // keyset-paginate by rowid (orez never creates WITHOUT ROWID tables) so\n      // a page is the only table data resident in either the DO or here.\n      let cursor = 0\n      let limit = 200\n      while (true) {\n        const usedLimit = limit\n        const rows = await sqlDoExec(\n          env,\n          instanceName,\n          'SELECT rowid AS __nspfx_rid, * FROM \"' +\n            qid(table.name) +\n            '\" WHERE rowid > ? ORDER BY rowid LIMIT ?',\n          [cursor, usedLimit],\n        )\n        if (!rows.length) break\n        cursor = rows[rows.length - 1].__nspfx_rid\n        for (const row of rows) delete row.__nspfx_rid\n        const lineBytes = await writeLine({ kind: 'rows', table: table.name, rows })\n        rowTotal += rows.length\n        // adapt the page size toward the byte target so wide rows (agent\n        // transcripts) never pull an unbounded page into memory.\n        const perRow = Math.max(1, Math.ceil(lineBytes / rows.length))\n        limit = Math.max(\n          20,\n          Math.min(1000, Math.floor(BACKUP_CHUNK_TARGET_BYTES / perRow)),\n        )\n        if (rows.length < usedLimit) break\n      }\n    }\n    await writeLine({ kind: 'footer', tables: tables.length, rows: rowTotal })\n    await flushParts(true)\n    await upload.complete(uploadedParts)\n  } catch (err) {\n    try {\n      await upload.abort()\n    } catch {}\n    throw err\n  }\n  const summary = {\n    ns: instanceName,\n    key,\n    marker,\n    exportedAt,\n    tables: tables.length,\n    rows: rowTotal,\n    bytes: totalBytes,\n    parts: uploadedParts.length,\n  }\n  // never flip the latest pointer onto an EMPTY dump over a non-empty one: a\n  // freshly wiped/recreated namespace exports 0 rows (marker 0), and pointing\n  // latest.json at that would send disaster recovery to an empty dump while\n  // good history still sits next to it (post-wipe hazard, 2026-07-08). the\n  // empty dump object itself still lands (and prunes) normally.\n  let keepPreviousLatest = false\n  if (rowTotal === 0) {\n    try {\n      const previous = await env.FILES.get(backupPrefix(instanceName) + 'latest.json')\n      if (previous) {\n        const previousSummary = await previous.json()\n        keepPreviousLatest = Number(previousSummary.rows) > 0\n      }\n    } catch {}\n  }\n  if (!keepPreviousLatest) {\n    await env.FILES.put(backupPrefix(instanceName) + 'latest.json', JSON.stringify(summary))\n  }\n  return summary\n}\n\nasync function* ndjsonLines(stream) {\n  const decoder = new TextDecoder()\n  const reader = stream.getReader()\n  let carry = ''\n  while (true) {\n    const { done, value } = await reader.read()\n    if (done) break\n    carry += decoder.decode(value, { stream: true })\n    let index\n    while ((index = carry.indexOf('\\n')) !== -1) {\n      const line = carry.slice(0, index)\n      carry = carry.slice(index + 1)\n      if (line) yield line\n    }\n  }\n  carry += decoder.decode()\n  if (carry.trim()) yield carry\n}\n\n// overwrite-restore a dump into a namespace: DROP + recreate every dumped\n// table, stream-insert the rows, verify counts, then wipe the namespace's\n// derived state (replica + CVR in its ZeroCacheDO) so the next /sync runs a\n// full initial sync over the restored rows.\nasync function importNamespace(env, instanceName, key) {\n  const object = await env.FILES.get(key)\n  if (!object || !object.body) throw new Error('backup object not found: ' + key)\n  // truncate (not DROP, live change-capture triggers reference these) any\n  // replication bookkeeping the target namespace already holds: a restore\n  // must never leave a stale change-log backlog behind the derived reset.\n  for (const name of REPLICATION_BOOKKEEPING_TABLES) {\n    try {\n      await sqlDoExec(env, instanceName, 'DELETE FROM \"' + qid(name) + '\"', [])\n    } catch {\n      // table absent (fresh namespace), nothing to truncate\n    }\n  }\n  let header = null\n  let footer = null\n  let rowTotal = 0\n  let skippedRows = 0\n  const tableNames = []\n  // rows are buffered and inserted after the whole stream arrives: the export\n  // streams tables in storage order, so a child table's rows can precede the\n  // parent rows they reference, and each insert batch commits its own\n  // transaction under enforced foreign keys. tables are created as they\n  // stream, then rows land in FK-dependency order (the graph is acyclic; a\n  // cycle would fall back to stream order and fail loudly, which no current\n  // schema has).\n  const bufferedRows = new Map()\n  for await (const line of ndjsonLines(object.body)) {\n    const entry = JSON.parse(line)\n    if (entry.kind === 'header') {\n      if (entry.format !== 'nspfx-backup-v2') {\n        throw new Error('unsupported backup format: ' + entry.format)\n      }\n      header = entry\n    } else if (entry.kind === 'table') {\n      if (isReplicationBookkeepingTable(entry.name)) continue\n      tableNames.push(entry.name)\n      await sqlDoBatch(env, instanceName, [\n        { sql: 'DROP TABLE IF EXISTS \"' + qid(entry.name) + '\"' },\n        { sql: entry.sql },\n        ...(entry.indexes || []).map((sql) => ({ sql })),\n      ])\n    } else if (entry.kind === 'rows') {\n      if (isReplicationBookkeepingTable(entry.table)) {\n        skippedRows += entry.rows.length\n        continue\n      }\n      const buffered = bufferedRows.get(entry.table) || []\n      buffered.push(...entry.rows)\n      bufferedRows.set(entry.table, buffered)\n    } else if (entry.kind === 'footer') {\n      footer = entry\n    }\n  }\n  if (!header || !footer) {\n    throw new Error('backup is truncated or not a nspfx-backup-v2 dump')\n  }\n  const dependencies = new Map()\n  for (const name of tableNames) {\n    const fks = await sqlDoExec(\n      env,\n      instanceName,\n      'PRAGMA foreign_key_list(\"' + qid(name) + '\")',\n      [],\n    )\n    dependencies.set(\n      name,\n      fks\n        .map((fk) => String(fk.table))\n        .filter((table) => table !== name && tableNames.includes(table)),\n    )\n  }\n  const ordered = []\n  const done = new Set()\n  const visiting = new Set()\n  const visit = (name) => {\n    if (done.has(name) || visiting.has(name)) return\n    visiting.add(name)\n    for (const dep of dependencies.get(name) || []) visit(dep)\n    visiting.delete(name)\n    done.add(name)\n    ordered.push(name)\n  }\n  for (const name of tableNames) visit(name)\n  for (const name of ordered) {\n    const rows = bufferedRows.get(name) || []\n    for (let offset = 0; offset < rows.length; offset += 400) {\n      const chunk = rows.slice(offset, offset + 400)\n      const statements = chunk.map((row) => {\n        const columns = Object.keys(row)\n        return {\n          sql:\n            'INSERT INTO \"' +\n            qid(name) +\n            '\" (' +\n            columns.map((column) => '\"' + qid(column) + '\"').join(', ') +\n            ') VALUES (' +\n            columns.map(() => '?').join(', ') +\n            ')',\n          params: columns.map((column) => row[column]),\n        }\n      })\n      await sqlDoBatch(env, instanceName, statements)\n    }\n    rowTotal += rows.length\n  }\n  if (footer.rows !== rowTotal + skippedRows) {\n    throw new Error(\n      'row count mismatch: footer says ' +\n        footer.rows +\n        ', imported ' +\n        rowTotal +\n        ' + skipped bookkeeping ' +\n        skippedRows,\n    )\n  }\n  // independent verification: count every restored table in the target DO.\n  const counts = {}\n  for (const name of tableNames) {\n    const rows = await sqlDoExec(\n      env,\n      instanceName,\n      'SELECT COUNT(*) AS n FROM \"' + qid(name) + '\"',\n      [],\n    )\n    counts[name] = Number(rows[0] && rows[0].n) || 0\n  }\n  const cacheId = env.ZERO_CACHE_DO.idFromName(instanceName)\n  const reset = await env.ZERO_CACHE_DO.get(cacheId).fetch(\n    'https://orez-data.local/__nspfx_reset_derived',\n    { method: 'POST', headers: { 'x-nspfx-do-instance': instanceName } },\n  )\n  if (!reset.ok) throw new Error('derived-state reset failed: ' + reset.status)\n  return {\n    ok: true,\n    ns: instanceName,\n    key,\n    sourceNs: header.ns,\n    tables: tableNames.length,\n    rows: rowTotal,\n    counts,\n  }\n}\n\nasync function pruneBackups(env, instanceName) {\n  const prefix = backupPrefix(instanceName)\n  const listed = await env.FILES.list({ prefix })\n  const dumps = (listed.objects || [])\n    .filter((object) => /\\/\\d+\\.(ndjson|json)$/.test(object.key))\n    .sort((a, b) => (a.key < b.key ? -1 : 1))\n  const keep = instanceName === 'singleton' ? BACKUP_KEEP_SINGLETON : BACKUP_KEEP\n  const excess = dumps.slice(0, Math.max(0, dumps.length - keep))\n  if (excess.length) await env.FILES.delete(excess.map((object) => object.key))\n}\n\n// the namespace inventory for scheduled backups: the control plane plus one\n// ns per project row in the singleton. on a deployed user app (no project\n// table) this degrades to just the singleton.\nasync function listBackupNamespaces(env) {\n  const names = ['singleton']\n  try {\n    const id = env.ZERO_SQL_DO.idFromName('singleton')\n    const res = await env.ZERO_SQL_DO.get(id).fetch(\n      new Request('https://orez-data.local/__nspfx_pg', {\n        method: 'POST',\n        headers: {\n          'content-type': 'application/json',\n          'x-nspfx-do-instance': 'singleton',\n        },\n        body: JSON.stringify({ text: 'SELECT id FROM project', values: [] }),\n      }),\n    )\n    const body = await res.json()\n    if (!res.ok || body.error) throw new Error(String(body.error || res.status))\n    for (const row of body.rows || []) {\n      if (row && row.id) names.push('ns:proj-' + row.id)\n    }\n  } catch (err) {\n    console.log(\n      '[nspfx] backup: project enumeration unavailable: ' +\n        String((err && err.message) || err),\n    )\n  }\n  return names\n}\n\n// 1-minute warm ping: a single cheap SELECT 1 against the singleton ZeroSqlDO\n// (the control-plane SQL DO that every db.pool query — auth/me, sign-in/social's\n// verification write, all bootstrap reads — routes through). a DO that served a\n// request inside the last ~minute stays resident, so this keeps the lean SQL DO\n// off the cold-start path for real users. ONLY the singleton: per-project\n// ns:proj-<id> DOs hibernating between deploys/sessions is fine and warming them\n// all would be a fan-out wake of every project (cost + the thing the backup cron\n// deliberately avoids). never touches ZeroCacheDO — its embed is meant to\n// hibernate when no /sync client is connected.\nasync function warmDataTier(env) {\n  try {\n    const id = env.ZERO_SQL_DO.idFromName('singleton')\n    const res = await env.ZERO_SQL_DO.get(id).fetch(\n      new Request('https://orez-data.local/__nspfx_pg', {\n        method: 'POST',\n        headers: {\n          'content-type': 'application/json',\n          'x-nspfx-do-instance': 'singleton',\n        },\n        body: JSON.stringify({ text: 'SELECT 1', values: [] }),\n      }),\n    )\n    await res.text().catch(() => {})\n  } catch (err) {\n    console.log('[nspfx] warm ping failed: ' + String((err && err.message) || err))\n  }\n}\n\n// cron entry: iterate namespaces SEQUENTIALLY (waking one lean ZeroSqlDO at a\n// time — never the zero-cache embed, and never a fan-out wake of every\n// hibernated DO), skip namespaces whose write marker matches their last\n// export, and stop at the wall budget — the shuffle keeps a truncated run\n// from starving the same tail every time.\nasync function runScheduledBackups(env) {\n  const started = Date.now()\n  const namespaces = await listBackupNamespaces(env)\n  for (let i = namespaces.length - 1; i > 0; i--) {\n    const j = Math.floor(Math.random() * (i + 1))\n    ;[namespaces[i], namespaces[j]] = [namespaces[j], namespaces[i]]\n  }\n  let exported = 0\n  let skipped = 0\n  let failed = 0\n  for (const ns of namespaces) {\n    if (Date.now() - started > BACKUP_RUN_BUDGET_MS) {\n      console.log('[nspfx] backup run: wall budget reached, deferring the rest')\n      break\n    }\n    try {\n      const marker = await readBackupMarker(env, ns)\n      const latest = await env.FILES.get(backupPrefix(ns) + 'latest.json')\n      if (latest) {\n        const previous = await latest.json()\n        if (previous.marker === marker) {\n          skipped++\n          continue\n        }\n      }\n      const summary = await exportNamespace(env, ns)\n      await pruneBackups(env, ns)\n      exported++\n      console.log(\n        '[nspfx] backup: ' + ns + ' -> ' + summary.key + ' (' + summary.rows + ' rows, ' + summary.bytes + ' bytes)',\n      )\n    } catch (err) {\n      failed++\n      console.log(\n        '[nspfx] backup failed for ' + ns + ': ' + String((err && err.message) || err),\n      )\n    }\n  }\n  console.log(\n    '[nspfx] backup run: exported ' + exported + ' skipped ' + skipped + ' failed ' + failed + ' in ' + (Date.now() - started) + 'ms',\n  )\n}\n\n// data tier -> app worker over the service binding (env.APP). the app worker\n// serves the zero push/pull endpoints (/api/zero/*).\nfunction nspfxZeroApiFetch(env, ctx, request) {\n  return env.APP.fetch(request)\n}\n\nexport default {\n  async fetch(request, env) {\n    if (env.AUTH_DB) globalThis.AUTH_DB = env.AUTH_DB\n    const url = new URL(request.url)\n    const instanceName = doInstanceNameForRequest(request, url)\n    if (!instanceName) return new Response('orez-data: invalid ns', { status: 400 })\n    // db.pool query path. the app worker posts RAW pg SQL { text, values };\n    // forward into ZeroSqlDO, where the DoBackend runs with legal cross-request\n    // state (see the ZeroSqlDO subclass above) — still in the lean data tier,\n    // never in the heavy app worker.\n    if (url.pathname === '/__nspfx_query') {\n      const id = env.ZERO_SQL_DO.idFromName(instanceName)\n      const doUrl = new URL(request.url)\n      doUrl.pathname = '/__nspfx_pg'\n      const forward = new Request(doUrl.toString(), request)\n      forward.headers.set('x-nspfx-do-instance', instanceName)\n      return env.ZERO_SQL_DO.get(id).fetch(forward)\n    }\n    // schema-warmup + /sync go to ZeroCacheDO. always re-wrap so the DO learns\n    // its own instance name (it must address the SAME namespace's ZeroSqlDO).\n    if (\n      url.pathname === '/__nspfx_migrate' ||\n      isZeroCachePath(url.pathname)\n    ) {\n      const id = env.ZERO_CACHE_DO.idFromName(instanceName)\n      const forward = new Request(zeroCacheRequestForUrl(request, url))\n      forward.headers.set('x-nspfx-do-instance', instanceName)\n      return env.ZERO_CACHE_DO.get(id).fetch(forward)\n    }\n    // operational per-namespace backup: stream every table in this namespace's\n    // ZeroSqlDO (the AUTHORITATIVE rows — the replica is derived and re-syncs)\n    // to one R2 NDJSON object under backups/ via multipart upload. memory is\n    // bounded to one row page + one part buffer (see exportNamespace).\n    if (url.pathname === '/__nspfx_export') {\n      try {\n        const summary = await exportNamespace(env, instanceName)\n        await pruneBackups(env, instanceName)\n        return Response.json({ ok: true, ...summary })\n      } catch (err) {\n        return new Response('export failed: ' + String((err && err.message) || err), {\n          status: 500,\n        })\n      }\n    }\n    // restore drill / disaster recovery: stream a backups/ dump into this\n    // namespace, replacing the dumped tables wholesale. destructive by design,\n    // so it demands an explicit ?confirm=<ns> echo.\n    if (url.pathname === '/__nspfx_import' && request.method === 'POST') {\n      const expected =\n        request.headers.get('x-nspfx-ns') || url.searchParams.get('ns') || 'singleton'\n      if (url.searchParams.get('confirm') !== expected) {\n        return new Response('import requires ?confirm=' + expected, { status: 400 })\n      }\n      let key = ''\n      try {\n        key = String((await request.json()).key || '')\n      } catch {}\n      if (!key || !key.startsWith('backups/')) {\n        return new Response('import requires a backups/ R2 key', { status: 400 })\n      }\n      try {\n        return Response.json(await importNamespace(env, instanceName, key))\n      } catch (err) {\n        return new Response('import failed: ' + String((err && err.message) || err), {\n          status: 500,\n        })\n      }\n    }\n    // runaway-write circuit breaker operator surface: GET reads this\n    // namespace's breaker state; POST {action:'trip'} force-trips it (the\n    // external cost monitor's non-destructive stop — writes refuse, the DO and\n    // its data survive); POST {action:'clear'} re-opens it after RCA. the\n    // breaker's own write gate exempts statements naming the circuit table, so\n    // clear works on a tripped namespace. reachable only through the app\n    // worker's admin-token-gated proxy (this worker has no public route).\n    if (url.pathname === '/__nspfx_circuit') {\n      const table = '_nspfx_write_circuit'\n      try {\n        if (request.method === 'POST') {\n          let action = ''\n          try {\n            action = String((await request.json()).action || '')\n          } catch {}\n          if (action !== 'trip' && action !== 'clear') {\n            return new Response('circuit requires {action: trip|clear}', { status: 400 })\n          }\n          const trippedAt = action === 'trip' ? Date.now() : 0\n          await sqlDoExec(env, instanceName, 'CREATE TABLE IF NOT EXISTS ' + table + ' (id INTEGER PRIMARY KEY CHECK (id = 1), window_start INTEGER NOT NULL DEFAULT 0, rows_in_window INTEGER NOT NULL DEFAULT 0, first_over_at INTEGER NOT NULL DEFAULT 0, tripped_at INTEGER NOT NULL DEFAULT 0, last_statement TEXT)', [])\n          await sqlDoExec(env, instanceName, 'INSERT INTO ' + table + ' (id, window_start, rows_in_window, first_over_at, tripped_at, last_statement) VALUES (1, 0, 0, 0, ?, ?) ON CONFLICT (id) DO UPDATE SET tripped_at = excluded.tripped_at, first_over_at = 0, rows_in_window = 0, last_statement = excluded.last_statement', [trippedAt, 'operator ' + action])\n        }\n        const rows = await sqlDoExec(env, instanceName, 'SELECT window_start, rows_in_window, first_over_at, tripped_at, last_statement FROM ' + table + ' WHERE id = 1', [])\n        const state = rows[0] || {}\n        return Response.json({ ok: true, ns: instanceName, tripped: Number(state.tripped_at || 0) > 0, ...state })\n      } catch (err) {\n        if (/no such table/i.test(String((err && err.message) || err))) {\n          return Response.json({ ok: true, ns: instanceName, tripped: false })\n        }\n        return new Response('circuit failed: ' + String((err && err.message) || err), { status: 500 })\n      }\n    }\n    return new Response('orez-data: not found', { status: 404 })\n  },\n\n  // cron triggers (see triggers.crons in the data wrangler config):\n  //   - the 6-hourly cron runs the namespace backup sweep.\n  //   - the 1-minute cron warms the singleton ZeroSqlDO so the auth + app\n  //     db.pool hot path (every /api/auth/*, /api/zero/push|pull mutator replay,\n  //     bootstrap query) never cold-starts the lean SQL DO on a real user\n  //     request. ZeroSqlDO has no self-renewing alarm — only ZeroCacheDO does,\n  //     and that one is meant to hibernate (its 16 MiB embed is GB-s expensive\n  //     and only /sync needs it). the SQL DO is the cheap-to-keep-resident tier\n  //     that ALL request traffic funnels through, so a steady warm ping is the\n  //     right knob: cold ZeroSqlDO was the measured 1.5-3.6s /api/auth/me and\n  //     ~10s sign-in/social the user reported.\n  async scheduled(event, env, ctx) {\n    if (event.cron === '* * * * *') {\n      ctx.waitUntil(warmDataTier(env))\n__CRON_FORWARDS__      return\n    }\n    ctx.waitUntil(runScheduledBackups(env))\n  },\n}\n"

const USER_SHIM_TEMPLATE =
  "import { DurableObject, waitUntil as cfWaitUntil } from 'cloudflare:workers'\nimport { Pool as NspfxCloudflarePgPool } from 'pg'\nimport { DoBackend } from 'nspfx-do-backend'\nimport { ZeroDO as OrezZeroSqlDO } from 'orez/cf-do'\nimport { doInstanceNameForRequest as orezDoInstanceNameForRequest } from 'orez/worker/cf-do-shim'\nimport { installZeroSqlWriteCircuitBreaker } from 'orez/worker/zero-sql-write-circuit'\nimport {\n  clearChangeStreamerStateIfReplicaUninitialized as orezClearChangeStreamerStateIfReplicaUninitialized,\n  dropReplicaTables as orezDropReplicaTables,\n  repairPartialReplicaInit as orezRepairPartialReplicaInit,\n  resetReplicaIfChangeLogPoisoned as orezResetReplicaIfChangeLogPoisoned,\n  resetReplicaIfTableSetChanged as orezResetReplicaIfTableSetChanged,\n} from 'orez/worker/zero-cache-replica-repair'\nimport {\n  doSqliteStorage as orezDoSqliteStorage,\n  installDoForbiddenSqliteGuard,\n} from 'orez/worker/zero-cache-do-sqlite'\nimport {\n  shouldHibernateIdleZeroCache,\n  ZERO_CACHE_IDLE_CHECK_MS,\n  ZERO_CACHE_IDLE_GRACE_MS,\n} from 'orez/worker/zero-cache-do-idle'\nimport { runNspfxCloudflareMigrations, SCHEMA_VERSION } from './orez-migrations.js'\n\n// the zero-cache embed (~16 MiB) is still lazy — only ZeroCacheDO.ensureReady\n// (/sync) needs it; the app-write gate (migrateOnly) does NOT, so the SQL-backend\n// and migration paths keep full headroom.\nlet _startZeroCacheEmbedCF\nasync function getStartZeroCacheEmbedCF() {\n  if (!_startZeroCacheEmbedCF) {\n    _startZeroCacheEmbedCF = (await import('orez/worker/zero-cache-embed-cf'))\n      .startZeroCacheEmbedCF\n  }\n  return _startZeroCacheEmbedCF\n}\n\n// idle-hibernation timing, env-overridable per-deploy (set both low to validate\n// teardown quickly on a throwaway deploy). defaults from orez. THIS is the cost\n// win the separation unlocks: the lean data-tier DO has no resident app modules\n// keeping it hot, so once no sync client is connected it can tear the embed down\n// and evict, stopping GB-s accrual.\nfunction idleCheckMs(env) {\n  const v = Number(env.ZERO_CACHE_IDLE_CHECK_MS)\n  return Number.isFinite(v) && v > 0 ? v : ZERO_CACHE_IDLE_CHECK_MS\n}\nfunction idleGraceMs(env) {\n  const v = Number(env.ZERO_CACHE_IDLE_GRACE_MS)\n  return Number.isFinite(v) && v > 0 ? v : ZERO_CACHE_IDLE_GRACE_MS\n}\n\n// pg-over-DO query endpoint. the DoBackend (libpg parse + SQLite translate +\n// transaction emulation) must live INSIDE the DO: it holds cross-request state\n// (an init promise, BEGIN/COMMIT spanning several /__nspfx_query calls from one\n// bootstrap), and a stateless worker handler that awaits another request's\n// in-flight I/O hits workerd's \"hanging promise\" cancellation — the\n// intermittent /__nspfx_query hang that stalled fs reads. inside a DO instance,\n// cross-request state and pending I/O are legal.\nclass ZeroSqlDO extends OrezZeroSqlDO {\n  constructor(ctx, env) {\n    super(ctx, env)\n    installZeroSqlWriteCircuitBreaker(ctx.storage.sql, {\n      table: '_nspfx_write_circuit',\n      logPrefix: '[nspfx]',\n      // tightened after the 2026-07-08 runaway: the burn ran ~1M rows/min for\n      // ~8h — HALF the old 2M/min soft default, so the breaker never tripped.\n      // legit peaks are ~20-30k rows/min (embed boots, restores), so 200k/min\n      // sustained (3min) or a single 1M/min window is unambiguously a write\n      // loop. sticky once tripped; operate via /__nspfx_circuit (status/clear).\n      rowsPerWindow: 200_000,\n      hardRowsPerWindow: 1_000_000,\n    })\n  }\n\n  async fetch(request) {\n    // every inbound call (worker router or sibling DO) stamps the instance\n    // name; remember it so the replication nudge targets THIS namespace's\n    // ZeroCacheDO. memory-only: there is no request-less turn in this DO.\n    this.__nspfxInstanceName =\n      request.headers.get('x-nspfx-do-instance') || this.__nspfxInstanceName\n    const url = new URL(request.url)\n    if (url.pathname === '/__nspfx_pg') {\n      const body = await request.json()\n      this.nspfxPgBackend ||= new DoBackend(\n        'https://orez-do-backend.local',\n        'postgres',\n        this.env.ZERO_APP_ID || 'zero',\n        { fetch: (input, init) => super.fetch(new Request(input, init)) },\n      )\n      const isWrite = (text) => /^\\s*(insert|update|delete|with)/i.test(text || '')\n      // batch: run statements in order through the same backend in ONE\n      // request. every /__nspfx_pg call is an app-worker -> data-worker -> DO\n      // hop, so multi-statement flows (provisioning's BEGIN..COMMIT graph)\n      // send one batch instead of paying the hop per statement.\n      if (Array.isArray(body.batch)) {\n        const results = []\n        try {\n          for (const stmt of body.batch) {\n            const result = await this.nspfxPgBackend.query(stmt.text, stmt.values || [])\n            results.push({ rows: result.rows || [], rowCount: result.rowCount })\n          }\n        } catch (err) {\n          // never leave the batch's transaction open on the shared backend —\n          // a later request would silently join it.\n          try {\n            await this.nspfxPgBackend.query('ROLLBACK', [])\n          } catch {}\n          return Response.json(\n            {\n              error:\n                'batch failed at statement ' +\n                results.length +\n                ': ' +\n                String((err && err.message) || err),\n            },\n            { status: 500 },\n          )\n        }\n        if (body.batch.some((stmt) => isWrite(stmt.text))) {\n          this.bumpBackupMarker()\n          this.nudgeReplication()\n        }\n        return Response.json({ results })\n      }\n      try {\n        const result = await this.nspfxPgBackend.query(body.text, body.values || [])\n        // app-origin write: nudge the embed's replication loop. orez's poll\n        // loop parks in a setTimeout that workerd never fires once the DO has\n        // no active request context, so without an explicit nudge a write sits\n        // in _zero_changes until the next embed cold boot (frozen replica, no\n        // pokes). DO execution is event-driven; so is this.\n        if (isWrite(body.text)) {\n          this.bumpBackupMarker()\n          this.nudgeReplication()\n        }\n        return Response.json({ rows: result.rows || [], rowCount: result.rowCount })\n      } catch (err) {\n        return Response.json({ error: String((err && err.message) || err) }, { status: 500 })\n      }\n    }\n    return super.fetch(request)\n  }\n\n  // monotonic change marker for scheduled backups: every app-origin write\n  // request bumps write_seq, so the cron can ask \"anything new since the last\n  // export?\" with one cheap read instead of re-exporting cold namespaces\n  // forever. zero-cache's own bookkeeping writes (/exec from the embed) do\n  // NOT pass through here and deliberately don't bump — replication pruning\n  // is not user data.\n  bumpBackupMarker() {\n    const sql = this.ctx.storage.sql\n    if (!this.__nspfxBackupMetaReady) {\n      sql.exec(\n        'CREATE TABLE IF NOT EXISTS _nspfx_backup_meta (id INTEGER PRIMARY KEY CHECK (id = 1), write_seq INTEGER NOT NULL DEFAULT 0)',\n      )\n      this.__nspfxBackupMetaReady = true\n    }\n    sql.exec(\n      'INSERT INTO _nspfx_backup_meta (id, write_seq) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET write_seq = write_seq + 1',\n    )\n  }\n\n  // fire-and-forget, throttled to one per second: tells the SAME namespace's\n  // ZeroCacheDO to wake its replication poll loop and give it a slice of\n  // execution.\n  nudgeReplication() {\n    const now = Date.now()\n    if (this.lastReplNudge && now - this.lastReplNudge < 1000) {\n      // within the throttle window — do NOT drop this write's nudge. dropping\n      // stranded its _zero_changes until the embed's slow idle poll: a second\n      // write <1s after the first never woke the embed, so its poke waited ~5s\n      // under load and ~30s when idle (the bursty-write replication-latency\n      // bug). coalesce instead — schedule ONE trailing nudge at the window edge\n      // so the whole burst drains+pokes within ~1s. the embed's poll loop drains\n      // every pending _zero_changes per nudge, so one trailing nudge covers N\n      // accumulated writes; pendingReplNudge keeps it to a single timer.\n      if (!this.pendingReplNudge) {\n        this.pendingReplNudge = true\n        const delay = Math.max(0, 1000 - (now - this.lastReplNudge))\n        this.ctx.waitUntil(\n          new Promise((r) => setTimeout(r, delay)).then(() => {\n            this.pendingReplNudge = false\n            this.sendReplNudge()\n          }),\n        )\n      }\n      return\n    }\n    this.sendReplNudge()\n  }\n\n  sendReplNudge() {\n    this.lastReplNudge = Date.now()\n    const instance = this.__nspfxInstanceName || 'singleton'\n    const id = this.env.ZERO_CACHE_DO.idFromName(instance)\n    this.ctx.waitUntil(\n      this.env.ZERO_CACHE_DO.get(id)\n        .fetch('https://orez-data.local/__nspfx_repl_nudge', {\n          method: 'POST',\n          headers: { 'x-nspfx-do-instance': instance },\n        })\n        .catch(() => {}),\n    )\n  }\n}\n\nexport { ZeroSqlDO, ZeroSqlDO as ZeroDO }\n\nconst INTERNAL_ZERO_MUTATE_URL = 'https://orez-zero-api.local/api/zero/push'\nconst INTERNAL_ZERO_QUERY_URL = 'https://orez-zero-api.local/api/zero/pull'\n\nfunction stringEnv(env) {\n  const out = {}\n  for (const [key, value] of Object.entries(env)) {\n    if (typeof value === 'string') out[key] = value\n  }\n  return out\n}\n\nfunction parsePublications(value) {\n  return String(value || '')\n    .split(',')\n    .map((part) => part.trim())\n    .filter(Boolean)\n}\n\nfunction normalizeZeroCachePathname(pathname) {\n  const normalized = pathname.startsWith('/api/zero/')\n    ? pathname.slice('/api/zero'.length)\n    : pathname\n  if (normalized.startsWith('/sync/v') && normalized.endsWith('/connect/')) {\n    return normalized.slice(0, -1)\n  }\n  return normalized\n}\n\n// per-project ns client routing: the app shim forwards /p-<projectId>/ zero\n// traffic with its path UNMODIFIED — zero v51's dispatcher pattern\n// (/:base)/:worker/v:version/:action natively tolerates the one base\n// component — so the data tier only RECOGNIZES the prefixed shape to route\n// it to ZeroCacheDO; it never strips it. the ns decision itself arrives via\n// the x-nspfx-ns header the app shim stamps (see doInstanceNameForRequest).\nconst PROJECT_PREFIXED_ZERO_PATH = new RegExp(\n  '^/p-[A-Za-z0-9_-]{1,64}/(sync|replication|mutate)/v[0-9]+/',\n)\n\nfunction isZeroCachePath(pathname) {\n  if (PROJECT_PREFIXED_ZERO_PATH.test(pathname)) return true\n  const normalized = normalizeZeroCachePathname(pathname)\n  if (normalized.startsWith('/sync/v') && normalized.endsWith('/connect')) return true\n  if (normalized.startsWith('/replication/v')) return true\n  if (normalized === '/statz' || normalized === '/heapz' || normalized === '/keepalive') return true\n  return false\n}\n\nfunction zeroCacheRequestForUrl(request, url) {\n  const pathname = normalizeZeroCachePathname(url.pathname)\n  if (pathname === url.pathname) return request\n  const normalizedUrl = new URL(request.url)\n  normalizedUrl.pathname = pathname\n  return new Request(normalizedUrl.toString(), request)\n}\n\n// fetch into the GIVEN namespace instance's ZeroSqlDO. the instance name is\n// threaded from the worker router (x-nspfx-do-instance header) through every\n// DO so a per-namespace ZeroCacheDO embed talks to its OWN ZeroSqlDO, never\n// the control plane's 'singleton'.\nfunction sqlDoFetch(env, instance) {\n  return (input, init) => {\n    const request = new Request(input, init)\n    request.headers.set('x-nspfx-do-instance', instance)\n    const id = env.ZERO_SQL_DO.idFromName(instance)\n    return env.ZERO_SQL_DO.get(id).fetch(request)\n  }\n}\n\n// CF Durable Object SQLite forbids storage-engine config the DO manages itself —\n// PRAGMA journal_mode / synchronous / page_size / mmap_size, VACUUM, wal_checkpoint,\n// ATTACH. zero-cache's embedded replica issues these during setup and gets\n// \"not authorized: SQLITE_AUTH\", which aborts the whole /sync replication. the DO\n// already provides WAL-grade durability + a single managed page store, so these\n// are no-ops here: filter them out before exec instead of letting them throw.\nfunction installSqlBackendGlobals(env, instance) {\n  // keyed by DO instance, NOT a single mutable global: per-namespace DO\n  // instances of one class can share an isolate, and a lone global swapped\n  // per turn would cross-route one namespace's migration writes into\n  // another's storage. consumers resolve their entry at call time (a DO stub\n  // is bound to the request whose env produced it — see backendFor).\n  const byInstance = (globalThis.__nspfx_cf_do_sql_fetch_by_instance ||= {})\n  byInstance[instance] = sqlDoFetch(env, instance)\n  globalThis.__nspfx_cf_do_namespace = env.ZERO_APP_ID || 'zero'\n  globalThis.__nspfx_cf_do_create_pg_pool = (connectionString = '') =>\n    new NspfxCloudflarePgPool({ connectionString })\n  // hoist the R2 file bucket so src/deploy/s3.ts (which never sees env) routes\n  // file storage through the native binding instead of S3-over-HTTP. without it\n  // the app worker's s3Put hangs on an empty endpoint for 30s+ and OOMs.\n  if (env.FILES) globalThis.__nspfx_cf_r2_bucket = env.FILES\n}\n\n// app-worker WRITE paths that hit the SQL DO via db.pool (better-auth incl.\n// /api/bootstrap-*, the zero mutator push). these need the platform schema to\n// exist in ZeroSqlDO first; reads/assets/marketing skip the gate so the static\n// shell stays edge-fast.\nfunction needsSqlSchema(pathname) {\n  return (\n    pathname.startsWith('/api/auth/') ||\n    pathname.startsWith('/api/bootstrap-') ||\n    pathname.startsWith('/api/zero/')\n  )\n}\n\n// the platform schema (init.sql DDL — 42 tables + their indexes) is applied to\n// ZeroSqlDO by runNspfxCloudflareMigrations. ZeroCacheDO.ensureReady runs it, but\n// only on a /sync connection; an app-worker write (bootstrap-anon provisioning\n// user/project/workspace/...) can land first and 500 on a missing table. so gate\n// the first write per isolate on the idempotent migration. it issues ~118\n// single-statement /exec calls to the SQL DO the first time (a few seconds),\n// then the cached resolved promise is ~free. the DDL is IF-NOT-EXISTS so a\n// /sync-triggered run and this one converge.\n// poke ZeroCacheDO to run the schema migration in ITS context (where the DO SQL\n// stub is valid). cached per isolate so only the first write per cold isolate\n// pays the round-trip.\nlet appSchemaReady\nfunction ensureSchemaViaZeroCacheDO(env) {\n  if (appSchemaReady) return appSchemaReady\n  appSchemaReady = (async () => {\n    const id = env.ZERO_CACHE_DO.idFromName('singleton')\n    const res = await env.ZERO_CACHE_DO.get(id).fetch(\n      new Request('https://orez-zero-cache.local/__nspfx_migrate'),\n    )\n    if (!res.ok) throw new Error('schema migration failed: ' + res.status)\n    await res.text().catch(() => {})\n  })().catch((err) => {\n    appSchemaReady = undefined // let the next write retry rather than wedge\n    throw err\n  })\n  return appSchemaReady\n}\n\nfunction withAppProcessEnv(env, run) {\n  globalThis.process ||= {}\n  globalThis.process.env ||= {}\n  const processEnv = globalThis.process.env\n  const previous = new Map()\n  for (const key of ['ZERO_UPSTREAM_DB', 'ZERO_CVR_DB', 'ZERO_CHANGE_DB']) {\n    const hadPrevious = Object.prototype.hasOwnProperty.call(processEnv, key)\n    previous.set(key, hadPrevious ? processEnv[key] : undefined)\n    delete processEnv[key]\n  }\n  for (const [key, value] of Object.entries(env)) {\n    if (typeof value !== 'string') continue\n    if (previous.has(key)) continue\n    const hadPrevious = Object.prototype.hasOwnProperty.call(processEnv, key)\n    previous.set(key, hadPrevious ? processEnv[key] : undefined)\n    processEnv[key] = value\n  }\n  const restore = () => {\n    for (const [key, value] of previous) {\n      if (value === undefined) delete processEnv[key]\n      else processEnv[key] = value\n    }\n  }\n  try {\n    const result = run()\n    if (result && typeof result.then === 'function') {\n      return result.finally(restore)\n    }\n    restore()\n    return result\n  } catch (err) {\n    restore()\n    throw err\n  }\n}\n\nfunction appWorkerRequestForInternalZeroApi(request, env) {\n  const url = new URL(request.url)\n  if (url.hostname !== 'orez-zero-api.local') return request\n  const origin =\n    env.BETTER_AUTH_URL ||\n    (env.VITE_PROTOCOL && env.VITE_WEB_HOSTNAME\n      ? env.VITE_PROTOCOL + '://' + env.VITE_WEB_HOSTNAME\n      : undefined)\n  if (!origin) return request\n  const appUrl = new URL(url.pathname + url.search, origin)\n  const headers = new Headers(request.headers)\n  headers.set('host', appUrl.host)\n  return new Request(appUrl.toString(), {\n    method: request.method,\n    headers,\n    body: request.body,\n    redirect: request.redirect,\n  })\n}\n\n// adapt the SQL-DO backend fetch to the (sql, params) -> {rows,error} exec shape\n// the orez replica-repair guards expect.\nfunction nspfxBackendExec(env, instance) {\n  const backendFetch = sqlDoFetch(env, instance)\n  return (sql, params) =>\n    backendFetch('https://orez-do.local/exec', {\n      method: 'POST',\n      headers: { 'content-type': 'application/json' },\n      body: JSON.stringify(params ? { sql, params } : { sql }),\n    }).then((r) => r.json())\n}\n\nexport class ZeroCacheDO extends DurableObject {\n  constructor(ctx, env) {\n    super(ctx, env)\n    this.ctx = ctx\n    this.env = env\n    this.zeroCache = undefined\n    this.ready = undefined\n    this.migrated = undefined\n    this.lastActiveAt = Date.now()\n    // skip DO-forbidden storage-engine statements (VACUUM/ATTACH/checkpoint\n    // PRAGMAs) the embed issues but the DO rejects with SQLITE_AUTH.\n    installDoForbiddenSqliteGuard(ctx.storage.sql)\n  }\n\n  // which namespace instance this DO is. stamped on every routed request by\n  // the worker; persisted because the alarm-carried boot runs with NO request\n  // context (a post-eviction alarm must still know its namespace).\n  async loadInstanceName() {\n    if (!this.__nspfxInstanceName) {\n      this.__nspfxInstanceName =\n        (await this.ctx.storage.get('__nspfx_instance_name')) || 'singleton'\n    }\n    return this.__nspfxInstanceName\n  }\n\n  captureInstanceName(request) {\n    const name = request.headers.get('x-nspfx-do-instance')\n    if (name && name !== this.__nspfxInstanceName) {\n      this.__nspfxInstanceName = name\n      this.ctx.storage.put('__nspfx_instance_name', name)\n    }\n  }\n\n  ensureReady() {\n    if (this.ready) return this.ready\n    // the boot must NOT run in the requesting client's context: zero's client\n    // aborts its connect after ~10s (and page reloads abort sooner), and a\n    // canceled request kills its in-flight async work — observed live as a\n    // post-hibernation reconnect leaving this.ready cached on a promise that\n    // never settles, wedging every later connect until DO eviction. park a\n    // deferred and carry the boot in an immediate alarm: alarm handlers run\n    // in the DO's own context, survive client cancellation, and get a long\n    // wall budget (a full replica resync can exceed blockConcurrencyWhile's\n    // 30s cap, so that gate is not an option).\n    let resolveReady, rejectReady\n    this.ready = new Promise((resolve, reject) => {\n      resolveReady = resolve\n      rejectReady = reject\n    })\n    this.bootDeferred = { resolve: resolveReady, reject: rejectReady }\n    // requests no longer park on this promise (they shed 503 while booting),\n    // so a failed boot's rejection needs a handler or it surfaces as an\n    // unhandled rejection. callers who do await still see the rejection.\n    this.ready.catch(() => {})\n    this.ctx.storage.setAlarm(Date.now())\n    return this.ready\n  }\n\n  async bootEmbed() {\n        // one line per embed cold boot: ties tail output to the DO instance +\n        // shim build actually serving (deploys don't always reset live DOs).\n        console.log('[nspfx] zero-cache embed boot: starting')\n        const bootStart = Date.now()\n        if (this.env.AUTH_DB) globalThis.AUTH_DB = this.env.AUTH_DB\n        const appId = this.env.ZERO_APP_ID || 'zero'\n        const publications = parsePublications(this.env.ZERO_APP_PUBLICATIONS)\n        const instance = await this.loadInstanceName()\n        installSqlBackendGlobals(this.env, instance)\n        // ensure the schema tables exist (cheap, schemaOnly — shared with the\n        // app-write gate via the persisted version guard), THEN set up the\n        // publication for replication. ensurePublication's per-table schema\n        // registration is the parse-heavy step; it runs here on the /sync path\n        // (not the app-write gate) because only replication needs it.\n        await this.migrateOnly()\n        console.log('[nspfx] boot step: migrateOnly done')\n        const migrationResult = await runNspfxCloudflareMigrations({\n          publications,\n          instance,\n        })\n        console.log('[nspfx] boot step: migrations done')\n        await this.resetReplicaIfTableSetChanged(migrationResult && migrationResult.tables)\n        console.log('[nspfx] boot step: replica tag checked')\n        // heal a replica left half-initialized by a prior interrupted embed boot\n        // (DO no-op transactions can't roll back a killed initial-sync). without\n        // this the embed re-runs setup and dies on a duplicate CREATE.\n        this.repairPartialReplicaInit()\n        // a partial transaction persisted in the cdc changeLog (a DO kill mid\n        // storer-write; the DO sqlite shim can't roll back across turns) makes\n        // every catchup replay begin→data→begin, which stops the replicator\n        // permanently: the replica freezes while /sync keeps serving stale\n        // hydrations. detect it and wipe the replica so the guard below clears\n        // cdc and the boot re-runs initial sync from upstream (no data gaps —\n        // the partial tx's upstream rows were already purged on stream).\n        await this.resetReplicaIfChangeLogPoisoned(appId)\n        console.log('[nspfx] boot step: changelog checked')\n        // the change-streamer's subscription state lives in zero_cdb (ZeroSqlDO)\n        // and SURVIVES a replica wipe (reset/repair above, or an OOM eviction).\n        // a wiped replica + surviving subscription state makes zero-cache skip\n        // initial sync (\"already synced\") and serve an EMPTY replica that only\n        // ever receives catchup changes. when the replica has no init marker,\n        // clear the cdc state so the embed re-runs initial sync from scratch.\n        await this.clearChangeStreamerStateIfReplicaUninitialized(appId)\n        console.log('[nspfx] boot step: cdc checked, starting embed')\n        const envStrings = stringEnv(this.env)\n        delete envStrings.ZERO_UPSTREAM_DB\n        delete envStrings.ZERO_CVR_DB\n        delete envStrings.ZERO_CHANGE_DB\n        const backendFetch = sqlDoFetch(this.env, instance)\n        const zeroEnv = {\n          ...envStrings,\n          NODE_ENV: 'development',\n          ZERO_APP_ID: appId,\n          ZERO_MUTATE_URL: INTERNAL_ZERO_MUTATE_URL,\n          ZERO_QUERY_URL: INTERNAL_ZERO_QUERY_URL,\n          ZERO_MUTATE_FORWARD_COOKIES: this.env.ZERO_MUTATE_FORWARD_COOKIES || 'true',\n          ZERO_QUERY_FORWARD_COOKIES: this.env.ZERO_QUERY_FORWARD_COOKIES || 'true',\n          ZERO_NUM_SYNC_WORKERS: this.env.ZERO_NUM_SYNC_WORKERS || '1',\n          ZERO_SHADOW_SYNC_ENABLED: 'false',\n          ...(publications.length\n            ? { ZERO_APP_PUBLICATIONS: publications.join(',') }\n            : {}),\n        }\n        const startZeroCacheEmbedCF = await getStartZeroCacheEmbedCF()\n        this.zeroCache = await startZeroCacheEmbedCF({\n          doSqlite: orezDoSqliteStorage(this.ctx),\n          backendFetch,\n          backendNamespace: appId,\n          appId,\n          publications: publications.length ? publications : undefined,\n          env: zeroEnv,\n          apiFetch: (request) => {\n            // entry-defined seam (hoisted function declaration appended by\n            // each worker entry): the split data tier reaches the app worker\n            // over the APP service binding; the single-worker user deploy\n            // invokes the in-process One app. without the seam the user\n            // worker crashed on this.env.APP being undefined on every push.\n            //\n            // carry THIS embed's namespace to the app worker, or the mutator\n            // replay writes land in 'singleton' while the optimistic client\n            // copy is in proj-<id> (row appears then vanishes). instance is\n            // 'ns:proj-<id>' | 'singleton'; the data tier's ns channel value is\n            // the 'proj-<id>' suffix. the app shim reads this header to scope\n            // its zeroServer pool (see __nspfx_run_in_ns).\n            const ns = instance.startsWith('ns:') ? instance.slice(3) : ''\n            const tagged = new Request(request)\n            if (ns) tagged.headers.set('x-nspfx-ns', ns)\n            return nspfxZeroApiFetch(\n              this.env,\n              this.ctx,\n              appWorkerRequestForInternalZeroApi(tagged, this.env),\n            )\n          },\n          readyTimeout: 120000,\n        })\n        console.log(\n          '[nspfx] zero-cache embed boot: ready in ' + (Date.now() - bootStart) + 'ms',\n        )\n  }\n\n  // zero-cache snapshots the publication's tables into its replica (DO SQLite,\n  // ZERO_REPLICA_FILE=':do-sqlite:') ONCE during initial sync and never picks up\n  // a table OR COLUMN added to the publication afterward — ALTER only feeds the\n  // change stream, not the existing snapshot. so a redeploy that evolves the\n  // schema leaves the persisted replica stuck on the old shape and every client\n  // fails SchemaVersionNotSupported (2026-06-10: file.title/description columns\n  // — table set unchanged, so a tables-only tag never reset). key the tag on\n  // SCHEMA_VERSION (hash of the full deploy-time DDL batch — any table/column/\n  // type change) plus the table set, and wipe the replica on change so\n  // zero-cache re-runs initial sync over the full publication. the replica is\n  // derived data — upstream rows live in the SQL DO and are untouched.\n  async resetReplicaIfTableSetChanged(tables) {\n    await orezResetReplicaIfTableSetChanged(this.ctx.storage.sql, this.ctx.storage, {\n      schemaVersion: SCHEMA_VERSION,\n      tables,\n      tagKey: '__nspfx_replica_schema_tag',\n    })\n  }\n\n  // repair a PARTIALLY-INITIALIZED replica left by an interrupted embed boot.\n  // zero-cache's runSchemaMigrations wraps initial-sync (createReplicationStateTables\n  // + the versionHistory row write) in one BEGIN EXCLUSIVE/COMMIT, expecting it to\n  // be atomic. but on a CF DO the sqlite shim makes BEGIN/COMMIT/ROLLBACK NO-OPS\n  // (the DO auto-commits per I/O turn), and the setup migration is async (it awaits\n  // initialSync, which yields across turns). so if the boot is killed mid-migration\n  // — the 120s ready-timeout, a DO eviction, an OOM — the _zero.* tables auto-commit\n  // but the closing versionHistory INSERT never runs. next boot: getVersionHistory\n  // reads an empty table => dataVersion 0 => it re-runs the setup migration =>\n  // CREATE TABLE \"_zero.replicationConfig\" => \"already exists\" SQLITE_ERROR, and\n  // /sync never reaches ready (editor stuck on \"loading files\"). detect that exact\n  // inconsistency (replica data tables present but no versionHistory row) and wipe\n  // the _zero.* replica so the embed re-runs initial sync cleanly. the replica is\n  // derived data — upstream rows live in ZeroSqlDO and are untouched.\n  repairPartialReplicaInit() {\n    orezRepairPartialReplicaInit(this.ctx.storage.sql, { logPrefix: '[nspfx]' })\n  }\n\n  // see the call site in ensureReady: a changeLog transaction group without a\n  // commit entry is an interrupted storer write (zero stores each replicated tx\n  // inside one pg transaction; real pg rolls a crashed tx back, but the DO\n  // sqlite shim auto-commits per turn, so a kill persists the partial group).\n  // catchup replays it as begin→data→begin and the replicator dies on\n  // \"Already in a transaction\" on every boot. wiping the replica here makes the\n  // uninitialized-replica guard clear cdc state, forcing a clean initial sync.\n  async resetReplicaIfChangeLogPoisoned(appId) {\n    await orezResetReplicaIfChangeLogPoisoned(\n      this.ctx.storage.sql,\n      nspfxBackendExec(this.env, await this.loadInstanceName()),\n      { appId, logPrefix: '[nspfx]' },\n    )\n  }\n\n  // see the call site in ensureReady: a replica without its init marker must\n  // not reuse the cdc subscription state, or initial sync never re-runs.\n  async clearChangeStreamerStateIfReplicaUninitialized(appId) {\n    await orezClearChangeStreamerStateIfReplicaUninitialized(\n      this.ctx.storage.sql,\n      nspfxBackendExec(this.env, await this.loadInstanceName()),\n      { appId, logPrefix: '[nspfx]' },\n    )\n  }\n\n  // run ONLY the platform-schema migration (init.sql DDL), cached, without\n  // booting the full zero-cache embed. an app-worker write needs the schema to\n  // exist but shouldn't pay the embed cold-start; this runs inside the DO's own\n  // context where this.env.ZERO_SQL_DO is a valid stub (the app-worker entry\n  // can't await a DO subrequest before its handler without wedging workerd).\n  // SCHEMA-ONLY warmup for the app-write gate (bootstrap/get-session): apply the\n  // table DDL so writes don't 500 on a missing table, but DEFER the publication\n  // setup. ensurePublication makes the pg-proxy register every published table's\n  // schema (~260 libpg parses) and THAT burst OOMs the 128 MiB isolate during the\n  // gate. the publication is only needed for /sync replication, so it runs later\n  // in ensureReady. cached per DO via the persisted SCHEMA_VERSION.\n  migrateOnly() {\n    if (this.migrated) return this.migrated\n    this.migrated = (async () => {\n      const appliedVersion = await this.ctx.storage.get('__nspfx_schema_version')\n      if (appliedVersion === SCHEMA_VERSION) return\n      // persist FIRST (the DDL /batch is idempotent IF-NOT-EXISTS, so re-applying\n      // on a retry is a no-op) so a memory-edge reset right after the batch can't\n      // wedge the DO in a re-migrate loop: once the batch has run, the schema is\n      // there; mark it applied before any further work can OOM-reset the isolate.\n      const instance = await this.loadInstanceName()\n      installSqlBackendGlobals(this.env, instance)\n      await runNspfxCloudflareMigrations({\n        publications: parsePublications(this.env.ZERO_APP_PUBLICATIONS),\n        schemaOnly: true,\n        instance,\n      })\n      await this.ctx.storage.put('__nspfx_schema_version', SCHEMA_VERSION)\n    })().catch((err) => {\n      this.migrated = undefined\n      throw err\n    })\n    return this.migrated\n  }\n\n  // schema DDL + the zero publication, for project-namespace provisioning. the\n  // publication must exist in durable metadata (_orez_pg_metadata) before the\n  // first app write to this namespace, or that write's cached DoBackend\n  // (ZeroSqlDO.nspfxPgBackend, loaded once) sees empty publications and skips\n  // change-capture — the write lands in the table but emits no _zero_changes\n  // row, so no poke ever reaches the client (the empty-fileTree blocker). runs\n  // the FULL (non-schemaOnly) migration: applyInitSqlDDL is idempotent and\n  // ensurePublication is CREATE-if-absent, so it converges with the identical\n  // call bootEmbed makes on the first /sync. cached per DO via migrateOnly's\n  // SCHEMA_VERSION guard for the DDL; the publication step is idempotent.\n  migrateWithPublication() {\n    if (this.publicationReady) return this.publicationReady\n    this.publicationReady = (async () => {\n      const instance = await this.loadInstanceName()\n      installSqlBackendGlobals(this.env, instance)\n      await runNspfxCloudflareMigrations({\n        publications: parsePublications(this.env.ZERO_APP_PUBLICATIONS),\n        instance,\n      })\n      // marks the schema applied; a later migrateOnly() reads this and returns\n      // early without redoing the DDL batch (same SCHEMA_VERSION storage guard).\n      await this.ctx.storage.put('__nspfx_schema_version', SCHEMA_VERSION)\n    })().catch((err) => {\n      this.publicationReady = undefined\n      throw err\n    })\n    return this.publicationReady\n  }\n\n  async fetch(request) {\n    if (this.env.AUTH_DB) globalThis.AUTH_DB = this.env.AUTH_DB\n    this.captureInstanceName(request)\n    installSqlBackendGlobals(this.env, await this.loadInstanceName())\n    const pathname = new URL(request.url).pathname\n    // replication nudge from ZeroSqlDO after an app write: wake the parked\n    // poll loop and hold this request context open briefly so the drain pass\n    // (query _zero_changes → stream → apply → poke) actually gets execution —\n    // workerd never fires the loop's idle timer without a live context. never\n    // boots the embed (a later cold boot's catchup drains the backlog), and\n    // deliberately does NOT bump lastActiveAt (a nudge is not client activity\n    // and must not block idle teardown).\n    if (pathname === '/__nspfx_repl_nudge') {\n      // a nudge during embed boot (fresh-user provisioning races the first\n      // /sync cold start) must WAIT for the boot, not drop: the boot request's\n      // context dies right after ensureReady, stranding in-flight replication\n      // work — this held nudge is the context that carries it through.\n      if (!this.zeroCache && this.ready) await this.ready.catch(() => {})\n      if (!this.zeroCache) return new Response(null, { status: 204 })\n      const signal = globalThis.__orez_signal_replication\n      if (typeof signal === 'function') {\n        signal()\n        // the full drain (stream -> changeLog -> replicator apply -> CVR ->\n        // poke) needs a longer slice than the stream alone; 1500ms stored the\n        // changes but pokes never made it out.\n        await new Promise((resolve) => setTimeout(resolve, 5000))\n      }\n      return new Response('ok')\n    }\n    // post-restore derived-state wipe (called by the data worker's\n    // /__nspfx_import): the replica + embed-local CVR/change-db in THIS DO now\n    // describe pre-restore data. stop the embed and drop every non-internal\n    // table plus the replica schema tag so the next /sync cold-boots a full\n    // initial sync over the restored upstream rows. same drop scope as\n    // resetReplicaIfTableSetChanged — all of it is derived data.\n    if (pathname === '/__nspfx_reset_derived') {\n      await this.ctx.blockConcurrencyWhile(async () => {\n        if (this.zeroCache) {\n          const stopping = this.zeroCache\n          this.zeroCache = undefined\n          this.ready = undefined\n          await stopping.stop()\n        }\n        orezDropReplicaTables(this.ctx.storage.sql)\n        await this.ctx.storage.delete('__nspfx_replica_schema_tag')\n      })\n      console.log('[nspfx] derived state reset after restore')\n      return new Response('ok')\n    }\n    this.lastActiveAt = Date.now()\n    // cheap schema-only warmup: run the migration without the embed cold-start.\n    // ?publication=1 (project-namespace provisioning) ALSO creates the zero\n    // publication now, instead of deferring it to the first /sync boot. the\n    // change-capture that turns an app write into a _zero_changes row is gated\n    // on the writing DoBackend's publication membership (orez\n    // trackingForStatement); that backend (ZeroSqlDO.nspfxPgBackend) loads its\n    // publications ONCE at construction and never reloads, so the seed write\n    // that follows provisioning must find the publication already persisted in\n    // _orez_pg_metadata — otherwise every project-ns write persists but emits\n    // no change/poke and the client never sees it (the empty-fileTree blocker).\n    // the app-write schema gate keeps the schema-only fast path (no publication)\n    // because its ~260-parse burst OOMs the 128 MiB isolate mid-serve; a\n    // provision is a one-shot off the serving path, so it can afford it.\n    if (pathname === '/__nspfx_migrate') {\n      if (new URL(request.url).searchParams.get('publication') === '1') {\n        await this.migrateWithPublication()\n      } else {\n        await this.migrateOnly()\n      }\n      return new Response('ok')\n    }\n    // readiness probe: kick the alarm-carried boot, then report whether the\n    // embed has FINISHED booting WITHOUT blocking on it. this.zeroCache flips\n    // truthy only once initial-sync completes and the view-syncer can hydrate;\n    // a raw /sync websocket opens (101) long before that, so 101 is NOT a\n    // readiness signal — a client connecting mid-boot gets baseCookie=null and\n    // times out after 10s. 200 here means the very next /sync will hydrate.\n    // the deploy polls this so neither the runtime validation nor the first\n    // real visitor races a half-booted embed. cheap, non-blocking, idempotent\n    // (ensureReady returns the in-flight boot promise on repeat calls).\n    if (pathname === '/keepalive') {\n      this.ensureReady()\n      this.lastActiveAt = Date.now()\n      return this.zeroCache\n        ? new Response('ready')\n        : new Response('booting', { status: 202 })\n    }\n    // never park requests on a boot in flight: a wedged client reconnecting\n    // against a slow cold boot stacks unbounded pending upgrades inside the\n    // isolate (each held for the full 60-120s boot), which helped push\n    // fresh-namespace initial sync over the 128 MiB isolate limit (2026-07-09\n    // OOM-reset loop). kick the boot and shed with a retryable 503 until\n    // ready. zero clients reconnect with backoff, and /keepalive above has\n    // always had this non-parking shape.\n    this.ensureReady()\n    if (!this.zeroCache) {\n      return new Response('zero-cache booting', {\n        status: 503,\n        headers: { 'retry-after': '2' },\n      })\n    }\n    return this.zeroCache.handleRequest(request, { waitUntil: cfWaitUntil })\n  }\n\n  // periodic idle check: when no sync client is connected past the grace window,\n  // tear the embed down so the DO evicts and stops accruing GB-s; the next\n  // request cold-starts it from DO SQLite.\n  async alarm() {\n    // a parked boot takes priority (see ensureReady): run it here in the\n    // DO's own context so client cancellation can't kill it mid-flight.\n    if (this.bootDeferred) {\n      // consecutive-failure backoff: a boot that keeps dying (the OOM-reset\n      // loop) must not retry at client-reconnect rate, because every cycle\n      // re-drops the replica and churns slots/DDL/metadata upstream (the\n      // 2026-07-09 rows-written burn). persisted so an isolate reset (the\n      // OOM case) cannot clear it.\n      const notBefore =\n        (await this.ctx.storage.get('__nspfx_boot_backoff_until')) || 0\n      if (Date.now() < notBefore) {\n        await this.ctx.storage.setAlarm(notBefore)\n        return\n      }\n      const deferred = this.bootDeferred\n      this.bootDeferred = undefined\n      try {\n        await this.bootEmbed()\n        deferred.resolve()\n        await this.ctx.storage.delete('__nspfx_boot_failures')\n        await this.ctx.storage.delete('__nspfx_boot_backoff_until')\n      } catch (err) {\n        // clear the cache so the next request retries a fresh boot; reject\n        // the waiters. do NOT rethrow — the runtime would retry the alarm\n        // with no deferred left to settle.\n        this.ready = undefined\n        const failures =\n          ((await this.ctx.storage.get('__nspfx_boot_failures')) || 0) + 1\n        await this.ctx.storage.put('__nspfx_boot_failures', failures)\n        if (failures >= 2) {\n          const delayMs = Math.min(15000 * 2 ** (failures - 2), 300000)\n          await this.ctx.storage.put(\n            '__nspfx_boot_backoff_until',\n            Date.now() + delayMs\n          )\n          console.log(\n            '[nspfx] zero-cache embed boot: backing off ' +\n              delayMs +\n              'ms after ' +\n              failures +\n              ' consecutive failures'\n          )\n        }\n        console.log('[nspfx] zero-cache embed boot: failed: ' + (err && err.message))\n        deferred.reject(err)\n      }\n      // fall through: re-arm below via the regular cadence logic.\n    }\n    if (!this.zeroCache) return\n    const idle = shouldHibernateIdleZeroCache({\n      connectionCount: this.zeroCache.connectionCount,\n      idleMs: Date.now() - (this.lastActiveAt || 0),\n      graceMs: idleGraceMs(this.env),\n    })\n    if (idle) {\n      // tear down under a concurrency gate so no request boots a second embed\n      // mid-stop (the embed mutates shared globals).\n      console.log('[nspfx] zero-cache idle teardown: starting')\n      await this.ctx.blockConcurrencyWhile(async () => {\n        if (!this.zeroCache || this.zeroCache.connectionCount > 0) return\n        const stopping = this.zeroCache\n        this.zeroCache = undefined\n        this.ready = undefined\n        await stopping.stop()\n      })\n      // the next connect reboots in place via ensureReady's alarm-carried\n      // boot. this requires orez >= the generation-safe proxy fix (instance-\n      // scoped schema caches + no leaked pipeline mutexes) — earlier builds\n      // wedged every second-generation embed start in the same isolate.\n      console.log('[nspfx] zero-cache idle teardown: stopped cleanly')\n    }\n    // re-arm only while the embed is still up; a torn-down DO leaves no alarm\n    // pending, which is what lets it evict.\n    if (this.zeroCache) {\n      // alarms are for lifecycle only. replication is write-driven via\n      // ZeroSqlDO.nudgeReplication and orez's own poll loop; waking it here\n      // creates a second replication driver that can replay retained batches\n      // forever when feedback does not converge.\n      await this.ctx.storage.setAlarm(Date.now() + idleCheckMs(this.env))\n    }\n  }\n}\n\n// the DATA-TIER worker's entry. it only ever receives internal calls from the app\n// worker over the service binding (env.OREZ_DATA.fetch): the SQL DO backend\n// (/__nspfx_sql -> ZeroSqlDO /exec|/batch), the schema-migration poke\n// (/__nspfx_migrate), and the zero-cache /sync* traffic. it routes each to its\n// in-process DOs. NO One app here.\n// per-project sharding seam: an explicit x-nspfx-ns header (or ?ns= param)\n// routes to that namespace's OWN DO pair; absent means the control-plane\n// namespace, which keeps its historical instance name 'singleton' so\n// existing storage stays addressed. names are validated to the proj-/test-\n// shape so a stray header can't mint unbounded DO instances.\nfunction doInstanceNameForRequest(request, url) {\n  return orezDoInstanceNameForRequest(request, url, {\n    nsHeader: 'x-nspfx-ns',\n    controlPlaneNamespaces: ['nspfx'],\n  })\n}\n\n// ---- namespace backup/restore (streaming, R2 multipart) ----\n//\n// format: NDJSON, one JSON object per line —\n//   { kind: 'header', format: 'nspfx-backup-v2', ns, exportedAt, marker }\n//   { kind: 'table', name, sql, indexes: [createIndexSql, ...] }\n//   { kind: 'rows', table, rows: [{col: value, ...}, ...] }   (repeated)\n//   { kind: 'footer', tables, rows }\n// the footer is the completeness proof: R2 multipart uploads are invisible\n// until complete(), so a crashed export never leaves a partial object, and\n// restore additionally refuses a dump without a matching footer row count.\n// memory bound: one row page (adaptive, ~BACKUP_CHUNK_TARGET_BYTES) plus at\n// most one part buffer (BACKUP_PART_BYTES) is resident at a time — never the\n// whole dataset, which is what OOMed-by-design the v1 whole-dump-JSON path.\nconst BACKUP_PART_BYTES = 8 * 1024 * 1024\nconst BACKUP_CHUNK_TARGET_BYTES = 2 * 1024 * 1024\nconst BACKUP_KEEP = 10\n// the control plane is the blast-radius namespace — the 2026-07-08 recovery\n// restored prod from its most recent dump, and drill/deploy exports churn its\n// window faster than the 6h cron alone. keep a deeper history there\n// (~a week at the combined cadence); project namespaces stay at 10.\nconst BACKUP_KEEP_SINGLETON = 30\nconst BACKUP_RUN_BUDGET_MS = 10 * 60 * 1000\n\nfunction qid(name) {\n  return String(name).replaceAll('\"', '\"\"')\n}\n\nfunction backupPrefix(instanceName) {\n  return 'backups/' + instanceName.replace(':', '/') + '/'\n}\n\n// replication/change-capture bookkeeping never travels through a backup:\n// restoring it re-seeds a retained change-log backlog the fresh consumer can\n// never confirm, and every embed boot then re-streams the whole set forever\n// (the 2026-07 DO rows-written burn). orez recreates these empty on boot, and\n// the post-restore derived reset re-snapshots the replica, so a restored\n// namespace needs none of this state.\nconst REPLICATION_BOOKKEEPING_TABLES = [\n  '_zero_changes',\n  '_zero_pending_changes',\n  '_zero_change_state',\n  '_orez___zero_watermark',\n  '_orez___zero_streamed_batches',\n  '_orez__zero_replication_slots',\n]\nfunction isReplicationBookkeepingTable(name) {\n  return REPLICATION_BOOKKEEPING_TABLES.includes(String(name))\n}\n\n// raw sqlite read/write against ONE namespace's ZeroSqlDO. /exec and /batch\n// are orez's internal DO endpoints — they never appear on the public router,\n// only built here worker-side, so this is not a public SQL surface.\nasync function sqlDoExec(env, instanceName, sql, params) {\n  const id = env.ZERO_SQL_DO.idFromName(instanceName)\n  const res = await env.ZERO_SQL_DO.get(id).fetch(\n    new Request('https://orez-data.local/exec', {\n      method: 'POST',\n      headers: {\n        'content-type': 'application/json',\n        'x-nspfx-do-instance': instanceName,\n      },\n      body: JSON.stringify({ sql, params: params || [] }),\n    }),\n  )\n  const body = await res.json()\n  if (!res.ok || body.error) {\n    throw new Error('backup sql failed: ' + (body.error || res.status))\n  }\n  return body.rows || []\n}\n\nasync function sqlDoBatch(env, instanceName, statements) {\n  const id = env.ZERO_SQL_DO.idFromName(instanceName)\n  const res = await env.ZERO_SQL_DO.get(id).fetch(\n    new Request('https://orez-data.local/batch', {\n      method: 'POST',\n      headers: {\n        'content-type': 'application/json',\n        'x-nspfx-do-instance': instanceName,\n      },\n      body: JSON.stringify({ statements }),\n    }),\n  )\n  if (!res.ok) {\n    const body = await res.json().catch(() => ({}))\n    throw new Error('restore batch failed: ' + (body.error || res.status))\n  }\n  await res.json().catch(() => {})\n}\n\nasync function readBackupMarker(env, instanceName) {\n  try {\n    const rows = await sqlDoExec(\n      env,\n      instanceName,\n      'SELECT write_seq FROM _nspfx_backup_meta WHERE id = 1',\n      [],\n    )\n    return Number(rows[0] && rows[0].write_seq) || 0\n  } catch (err) {\n    if (/no such table/i.test(String((err && err.message) || err))) return 0\n    throw err\n  }\n}\n\nasync function exportNamespace(env, instanceName) {\n  const exportedAt = new Date().toISOString()\n  // read the marker BEFORE the table scan: writes landing mid-export keep the\n  // marker ahead of latest.json, so the next cron re-exports them.\n  const marker = await readBackupMarker(env, instanceName)\n  const master = await sqlDoExec(\n    env,\n    instanceName,\n    \"SELECT name, sql, type, tbl_name FROM sqlite_master WHERE type IN ('table', 'index') AND sql IS NOT NULL ORDER BY name\",\n    [],\n  )\n  // sqlite_/_cf_ are engine/platform internals (DROP on _cf_* is SQLITE_AUTH\n  // denied on restore); _orez_tx_* are mid-transaction scratch tables; the\n  // write circuit is local protection state, not user data.\n  const skip = (name) =>\n    String(name).startsWith('sqlite_') ||\n    String(name).startsWith('_cf_') ||\n    String(name).startsWith('_orez_tx_') ||\n    String(name) === '_nspfx_write_circuit' ||\n    isReplicationBookkeepingTable(name)\n  const tables = master.filter((row) => row.type === 'table' && !skip(row.name))\n  const indexes = master.filter(\n    (row) => row.type === 'index' && !skip(row.name) && !skip(row.tbl_name),\n  )\n  const key = backupPrefix(instanceName) + Date.now() + '.ndjson'\n  const upload = await env.FILES.createMultipartUpload(key)\n  const uploadedParts = []\n  const encoder = new TextEncoder()\n  let chunks = []\n  let bufferedBytes = 0\n  let totalBytes = 0\n  // R2 requires every part except the last to be the SAME size: cut exact\n  // BACKUP_PART_BYTES slices off the buffered stream.\n  const flushParts = async (final) => {\n    if (!final && bufferedBytes < BACKUP_PART_BYTES) return\n    let merged = new Uint8Array(bufferedBytes)\n    let offset = 0\n    for (const chunk of chunks) {\n      merged.set(chunk, offset)\n      offset += chunk.byteLength\n    }\n    while (merged.byteLength >= BACKUP_PART_BYTES) {\n      uploadedParts.push(\n        await upload.uploadPart(uploadedParts.length + 1, merged.slice(0, BACKUP_PART_BYTES)),\n      )\n      merged = merged.slice(BACKUP_PART_BYTES)\n    }\n    if (final && (merged.byteLength > 0 || uploadedParts.length === 0)) {\n      uploadedParts.push(await upload.uploadPart(uploadedParts.length + 1, merged))\n      merged = new Uint8Array(0)\n    }\n    chunks = merged.byteLength ? [merged] : []\n    bufferedBytes = merged.byteLength\n  }\n  const writeLine = async (value) => {\n    const bytes = encoder.encode(JSON.stringify(value) + '\\n')\n    chunks.push(bytes)\n    bufferedBytes += bytes.byteLength\n    totalBytes += bytes.byteLength\n    await flushParts(false)\n    return bytes.byteLength\n  }\n  let rowTotal = 0\n  try {\n    await writeLine({\n      kind: 'header',\n      format: 'nspfx-backup-v2',\n      ns: instanceName,\n      exportedAt,\n      marker,\n    })\n    for (const table of tables) {\n      await writeLine({\n        kind: 'table',\n        name: table.name,\n        sql: table.sql,\n        indexes: indexes\n          .filter((index) => index.tbl_name === table.name)\n          .map((index) => index.sql),\n      })\n      // keyset-paginate by rowid (orez never creates WITHOUT ROWID tables) so\n      // a page is the only table data resident in either the DO or here.\n      let cursor = 0\n      let limit = 200\n      while (true) {\n        const usedLimit = limit\n        const rows = await sqlDoExec(\n          env,\n          instanceName,\n          'SELECT rowid AS __nspfx_rid, * FROM \"' +\n            qid(table.name) +\n            '\" WHERE rowid > ? ORDER BY rowid LIMIT ?',\n          [cursor, usedLimit],\n        )\n        if (!rows.length) break\n        cursor = rows[rows.length - 1].__nspfx_rid\n        for (const row of rows) delete row.__nspfx_rid\n        const lineBytes = await writeLine({ kind: 'rows', table: table.name, rows })\n        rowTotal += rows.length\n        // adapt the page size toward the byte target so wide rows (agent\n        // transcripts) never pull an unbounded page into memory.\n        const perRow = Math.max(1, Math.ceil(lineBytes / rows.length))\n        limit = Math.max(\n          20,\n          Math.min(1000, Math.floor(BACKUP_CHUNK_TARGET_BYTES / perRow)),\n        )\n        if (rows.length < usedLimit) break\n      }\n    }\n    await writeLine({ kind: 'footer', tables: tables.length, rows: rowTotal })\n    await flushParts(true)\n    await upload.complete(uploadedParts)\n  } catch (err) {\n    try {\n      await upload.abort()\n    } catch {}\n    throw err\n  }\n  const summary = {\n    ns: instanceName,\n    key,\n    marker,\n    exportedAt,\n    tables: tables.length,\n    rows: rowTotal,\n    bytes: totalBytes,\n    parts: uploadedParts.length,\n  }\n  // never flip the latest pointer onto an EMPTY dump over a non-empty one: a\n  // freshly wiped/recreated namespace exports 0 rows (marker 0), and pointing\n  // latest.json at that would send disaster recovery to an empty dump while\n  // good history still sits next to it (post-wipe hazard, 2026-07-08). the\n  // empty dump object itself still lands (and prunes) normally.\n  let keepPreviousLatest = false\n  if (rowTotal === 0) {\n    try {\n      const previous = await env.FILES.get(backupPrefix(instanceName) + 'latest.json')\n      if (previous) {\n        const previousSummary = await previous.json()\n        keepPreviousLatest = Number(previousSummary.rows) > 0\n      }\n    } catch {}\n  }\n  if (!keepPreviousLatest) {\n    await env.FILES.put(backupPrefix(instanceName) + 'latest.json', JSON.stringify(summary))\n  }\n  return summary\n}\n\nasync function* ndjsonLines(stream) {\n  const decoder = new TextDecoder()\n  const reader = stream.getReader()\n  let carry = ''\n  while (true) {\n    const { done, value } = await reader.read()\n    if (done) break\n    carry += decoder.decode(value, { stream: true })\n    let index\n    while ((index = carry.indexOf('\\n')) !== -1) {\n      const line = carry.slice(0, index)\n      carry = carry.slice(index + 1)\n      if (line) yield line\n    }\n  }\n  carry += decoder.decode()\n  if (carry.trim()) yield carry\n}\n\n// overwrite-restore a dump into a namespace: DROP + recreate every dumped\n// table, stream-insert the rows, verify counts, then wipe the namespace's\n// derived state (replica + CVR in its ZeroCacheDO) so the next /sync runs a\n// full initial sync over the restored rows.\nasync function importNamespace(env, instanceName, key) {\n  const object = await env.FILES.get(key)\n  if (!object || !object.body) throw new Error('backup object not found: ' + key)\n  // truncate (not DROP — live change-capture triggers reference these) any\n  // replication bookkeeping the target namespace already holds: a restore\n  // must never leave a stale change-log backlog behind the derived reset.\n  for (const name of REPLICATION_BOOKKEEPING_TABLES) {\n    try {\n      await sqlDoExec(env, instanceName, 'DELETE FROM \"' + qid(name) + '\"', [])\n    } catch {\n      // table absent (fresh namespace) — nothing to truncate\n    }\n  }\n  let header = null\n  let footer = null\n  let rowTotal = 0\n  let skippedRows = 0\n  const tableNames = []\n  for await (const line of ndjsonLines(object.body)) {\n    const entry = JSON.parse(line)\n    if (entry.kind === 'header') {\n      if (entry.format !== 'nspfx-backup-v2') {\n        throw new Error('unsupported backup format: ' + entry.format)\n      }\n      header = entry\n    } else if (entry.kind === 'table') {\n      if (isReplicationBookkeepingTable(entry.name)) continue\n      tableNames.push(entry.name)\n      await sqlDoBatch(env, instanceName, [\n        { sql: 'DROP TABLE IF EXISTS \"' + qid(entry.name) + '\"' },\n        { sql: entry.sql },\n        ...(entry.indexes || []).map((sql) => ({ sql })),\n      ])\n    } else if (entry.kind === 'rows') {\n      if (isReplicationBookkeepingTable(entry.table)) {\n        skippedRows += entry.rows.length\n        continue\n      }\n      const statements = entry.rows.map((row) => {\n        const columns = Object.keys(row)\n        return {\n          sql:\n            'INSERT INTO \"' +\n            qid(entry.table) +\n            '\" (' +\n            columns.map((column) => '\"' + qid(column) + '\"').join(', ') +\n            ') VALUES (' +\n            columns.map(() => '?').join(', ') +\n            ')',\n          params: columns.map((column) => row[column]),\n        }\n      })\n      await sqlDoBatch(env, instanceName, statements)\n      rowTotal += entry.rows.length\n    } else if (entry.kind === 'footer') {\n      footer = entry\n    }\n  }\n  if (!header || !footer) {\n    throw new Error('backup is truncated or not a nspfx-backup-v2 dump')\n  }\n  if (footer.rows !== rowTotal + skippedRows) {\n    throw new Error(\n      'row count mismatch: footer says ' +\n        footer.rows +\n        ', imported ' +\n        rowTotal +\n        ' + skipped bookkeeping ' +\n        skippedRows,\n    )\n  }\n  // independent verification: count every restored table in the target DO.\n  const counts = {}\n  for (const name of tableNames) {\n    const rows = await sqlDoExec(\n      env,\n      instanceName,\n      'SELECT COUNT(*) AS n FROM \"' + qid(name) + '\"',\n      [],\n    )\n    counts[name] = Number(rows[0] && rows[0].n) || 0\n  }\n  const cacheId = env.ZERO_CACHE_DO.idFromName(instanceName)\n  const reset = await env.ZERO_CACHE_DO.get(cacheId).fetch(\n    'https://orez-data.local/__nspfx_reset_derived',\n    { method: 'POST', headers: { 'x-nspfx-do-instance': instanceName } },\n  )\n  if (!reset.ok) throw new Error('derived-state reset failed: ' + reset.status)\n  return {\n    ok: true,\n    ns: instanceName,\n    key,\n    sourceNs: header.ns,\n    tables: tableNames.length,\n    rows: rowTotal,\n    counts,\n  }\n}\n\nasync function pruneBackups(env, instanceName) {\n  const prefix = backupPrefix(instanceName)\n  const listed = await env.FILES.list({ prefix })\n  const dumps = (listed.objects || [])\n    .filter((object) => /\\/\\d+\\.(ndjson|json)$/.test(object.key))\n    .sort((a, b) => (a.key < b.key ? -1 : 1))\n  const keep = instanceName === 'singleton' ? BACKUP_KEEP_SINGLETON : BACKUP_KEEP\n  const excess = dumps.slice(0, Math.max(0, dumps.length - keep))\n  if (excess.length) await env.FILES.delete(excess.map((object) => object.key))\n}\n\n// the namespace inventory for scheduled backups: the control plane plus one\n// ns per project row in the singleton. on a deployed user app (no project\n// table) this degrades to just the singleton.\nasync function listBackupNamespaces(env) {\n  const names = ['singleton']\n  try {\n    const id = env.ZERO_SQL_DO.idFromName('singleton')\n    const res = await env.ZERO_SQL_DO.get(id).fetch(\n      new Request('https://orez-data.local/__nspfx_pg', {\n        method: 'POST',\n        headers: {\n          'content-type': 'application/json',\n          'x-nspfx-do-instance': 'singleton',\n        },\n        body: JSON.stringify({ text: 'SELECT id FROM project', values: [] }),\n      }),\n    )\n    const body = await res.json()\n    if (!res.ok || body.error) throw new Error(String(body.error || res.status))\n    for (const row of body.rows || []) {\n      if (row && row.id) names.push('ns:proj-' + row.id)\n    }\n  } catch (err) {\n    console.log(\n      '[nspfx] backup: project enumeration unavailable: ' +\n        String((err && err.message) || err),\n    )\n  }\n  return names\n}\n\n// 1-minute warm ping: a single cheap SELECT 1 against the singleton ZeroSqlDO\n// (the control-plane SQL DO that every db.pool query — auth/me, sign-in/social's\n// verification write, all bootstrap reads — routes through). a DO that served a\n// request inside the last ~minute stays resident, so this keeps the lean SQL DO\n// off the cold-start path for real users. ONLY the singleton: per-project\n// ns:proj-<id> DOs hibernating between deploys/sessions is fine and warming them\n// all would be a fan-out wake of every project (cost + the thing the backup cron\n// deliberately avoids). never touches ZeroCacheDO — its embed is meant to\n// hibernate when no /sync client is connected.\nasync function warmDataTier(env) {\n  try {\n    const id = env.ZERO_SQL_DO.idFromName('singleton')\n    const res = await env.ZERO_SQL_DO.get(id).fetch(\n      new Request('https://orez-data.local/__nspfx_pg', {\n        method: 'POST',\n        headers: {\n          'content-type': 'application/json',\n          'x-nspfx-do-instance': 'singleton',\n        },\n        body: JSON.stringify({ text: 'SELECT 1', values: [] }),\n      }),\n    )\n    await res.text().catch(() => {})\n  } catch (err) {\n    console.log('[nspfx] warm ping failed: ' + String((err && err.message) || err))\n  }\n}\n\n// cron entry: iterate namespaces SEQUENTIALLY (waking one lean ZeroSqlDO at a\n// time — never the zero-cache embed, and never a fan-out wake of every\n// hibernated DO), skip namespaces whose write marker matches their last\n// export, and stop at the wall budget — the shuffle keeps a truncated run\n// from starving the same tail every time.\nasync function runScheduledBackups(env) {\n  const started = Date.now()\n  const namespaces = await listBackupNamespaces(env)\n  for (let i = namespaces.length - 1; i > 0; i--) {\n    const j = Math.floor(Math.random() * (i + 1))\n    ;[namespaces[i], namespaces[j]] = [namespaces[j], namespaces[i]]\n  }\n  let exported = 0\n  let skipped = 0\n  let failed = 0\n  for (const ns of namespaces) {\n    if (Date.now() - started > BACKUP_RUN_BUDGET_MS) {\n      console.log('[nspfx] backup run: wall budget reached, deferring the rest')\n      break\n    }\n    try {\n      const marker = await readBackupMarker(env, ns)\n      const latest = await env.FILES.get(backupPrefix(ns) + 'latest.json')\n      if (latest) {\n        const previous = await latest.json()\n        if (previous.marker === marker) {\n          skipped++\n          continue\n        }\n      }\n      const summary = await exportNamespace(env, ns)\n      await pruneBackups(env, ns)\n      exported++\n      console.log(\n        '[nspfx] backup: ' + ns + ' -> ' + summary.key + ' (' + summary.rows + ' rows, ' + summary.bytes + ' bytes)',\n      )\n    } catch (err) {\n      failed++\n      console.log(\n        '[nspfx] backup failed for ' + ns + ': ' + String((err && err.message) || err),\n      )\n    }\n  }\n  console.log(\n    '[nspfx] backup run: exported ' + exported + ' skipped ' + skipped + ' failed ' + failed + ' in ' + (Date.now() - started) + 'ms',\n  )\n}\n\n// the One app graph is lazy so the DO isolates (schema migration / SQL\n// backend) never evaluate it — only the app-request path pays for it, once.\nlet _oneWorker\nasync function getOneWorker() {\n  if (!_oneWorker) _oneWorker = (await import('./one-app.js')).default\n  return _oneWorker\n}\n\n// zero push/pull from the embed routes into the in-process One app (this\n// worker has no APP service binding — see the apiFetch seam in bootEmbed).\nasync function nspfxZeroApiFetch(env, ctx, request) {\n  if (env.AUTH_DB) globalThis.AUTH_DB = env.AUTH_DB\n  const oneWorker = await getOneWorker()\n  return withAppProcessEnv(env, () => oneWorker.fetch(request, env, ctx))\n}\n\n// the orez backend's internal SQL surface must never be publicly reachable —\n// 404 these before the app fallthrough.\nfunction isPublicSqlBackendPath(pathname) {\n  return (\n    pathname === '/exec' ||\n    pathname === '/batch' ||\n    pathname === '/changes' ||\n    pathname === '/notify'\n  )\n}\n\nexport default {\n  async fetch(request, env, ctx) {\n    if (env.AUTH_DB) globalThis.AUTH_DB = env.AUTH_DB\n    if (env.ZERO_SQL_DO) installSqlBackendGlobals(env, 'singleton')\n    const url = new URL(request.url)\n    if (env.ZERO_CACHE_DO && isZeroCachePath(url.pathname)) {\n      const id = env.ZERO_CACHE_DO.idFromName('singleton')\n      const forward = new Request(zeroCacheRequestForUrl(request, url))\n      forward.headers.set('x-nspfx-do-instance', 'singleton')\n      return env.ZERO_CACHE_DO.get(id).fetch(forward)\n    }\n    if (isPublicSqlBackendPath(url.pathname)) {\n      return new Response('not found', { status: 404 })\n    }\n    // before an app WRITE to the SQL DO (better-auth, bootstrap, mutator),\n    // make sure the platform schema exists. driven through ZeroCacheDO's own\n    // context — a DO subrequest awaited before the app handler from this bare\n    // worker entry wedges workerd.\n    if (env.ZERO_CACHE_DO && needsSqlSchema(url.pathname)) {\n      await ensureSchemaViaZeroCacheDO(env)\n    }\n    const oneWorker = await getOneWorker()\n    // never surface a non-Response to workerd (error 1101) when a request\n    // falls through every One route — resolve to 404 like the app shim does.\n    const response = await withAppProcessEnv(env, () =>\n      oneWorker.fetch(request, env, ctx),\n    )\n    return response instanceof Response\n      ? response\n      : new Response('not found', { status: 404 })\n  },\n}\n"

const APP_SHIM_TEMPLATE =
  "import oneWorker from './one-app.js'\nimport { AsyncLocalStorage } from 'node:async_hooks'\nimport { isValidNamespace } from 'orez/worker/cf-do-shim'\n\nconst INTERNAL_ZERO_MUTATE_URL = 'https://orez-zero-api.local/api/zero/push'\nconst INTERNAL_ZERO_QUERY_URL = 'https://orez-zero-api.local/api/zero/pull'\n\n// per-request project-namespace scope. the authoritative zero push replay and\n// detached server-effect writes resolve their ns here at /__nspfx_query time, so\n// the ONE module-singleton zeroServer pool routes project-table writes to\n// ns=proj-<id> instead of the control-plane 'singleton'. AsyncLocalStorage\n// (nodejs_compat) isolates concurrent requests; absent scope => singleton.\n// withProjectNamespace (src/zero/withProjectNamespace.ts) reaches __nspfx_run_in_ns\n// for detached async-effect writes whose request scope is already unwound.\nconst __nspfxNsStore = new AsyncLocalStorage()\nglobalThis.__nspfx_run_in_ns = (ns, fn) => (ns ? __nspfxNsStore.run(ns, fn) : fn())\nglobalThis.__nspfx_current_ns = () => __nspfxNsStore.getStore()\n\nfunction parsePublications(value) {\n  return String(value || '').split(',').map((p) => p.trim()).filter(Boolean)\n}\nfunction normalizeZeroCachePathname(pathname) {\n  const normalized = pathname.startsWith('/api/zero/')\n    ? pathname.slice('/api/zero'.length)\n    : pathname\n  if (normalized.startsWith('/sync/v') && normalized.endsWith('/connect/')) {\n    return normalized.slice(0, -1)\n  }\n  return normalized\n}\nfunction isZeroCachePath(pathname) {\n  const n = normalizeZeroCachePathname(pathname)\n  if (n.startsWith('/sync/v') && n.endsWith('/connect')) return true\n  if (n.startsWith('/replication/v')) return true\n  if (n === '/statz' || n === '/heapz' || n === '/keepalive') return true\n  return false\n}\nfunction needsSqlSchema(pathname) {\n  return (\n    pathname.startsWith('/api/auth/') ||\n    pathname.startsWith('/api/bootstrap-') ||\n    pathname.startsWith('/api/zero/')\n  )\n}\nfunction zeroCacheRequestForUrl(request, url) {\n  const pathname = normalizeZeroCachePathname(url.pathname)\n  if (pathname === url.pathname) return request\n  const u = new URL(request.url)\n  u.pathname = pathname\n  return new Request(u.toString(), request)\n}\n\n// per-project sharding: the app worker OWNS the namespace decision on the\n// public surface. an explicit signal (?ns= query or nspfx-ns cookie) must be\n// shape-valid AND authorized; the x-nspfx-ns header the shim stamps is the only\n// ns channel the data tier sees from here — an inbound x-nspfx-ns never\n// forwards. until the directory split gives real clients project identity,\n// every non-control-plane ns requires the e2e admin token (probe/agent\n// surface only), so a public client cannot mint DO namespaces.\nfunction requestedNamespace(request, url) {\n  const fromQuery = url.searchParams.get('ns')\n  if (fromQuery) return fromQuery\n  const cookies = request.headers.get('cookie') || ''\n  for (const part of cookies.split(';')) {\n    const eq = part.indexOf('=')\n    if (eq === -1) continue\n    if (part.slice(0, eq).trim() !== 'nspfx-ns') continue\n    return decodeURIComponent(part.slice(eq + 1).trim())\n  }\n  return ''\n}\n\nfunction adminTokenValid(request, env) {\n  const provided = request.headers.get('x-nspfx-admin-token') || ''\n  const expected = typeof env.E2E_ADMIN_TOKEN === 'string' ? env.E2E_ADMIN_TOKEN : ''\n  if (!provided || !expected) return false\n  const enc = new TextEncoder()\n  const a = enc.encode(provided)\n  const b = enc.encode(expected)\n  if (a.byteLength !== b.byteLength) return false\n  return crypto.subtle.timingSafeEqual(a, b)\n}\n\n// per-project ns CLIENT routing: a project zero instance connects with\n// server = https://host/p-<projectId>, so its traffic arrives as\n// /p-<id>/<worker>/v<n>/<action>. zero v51's dispatcher pattern\n// (/:base)/:worker/v:version/:action natively tolerates the one base\n// component, so the path forwards UNMODIFIED — the prefix is only READ here\n// to derive ns = proj-<id>. the id charset matches the data tier's ns shape\n// validation so a stamped header can never be rejected downstream.\nconst PROJECT_ZERO_PATH = new RegExp(\n  '^/p-([A-Za-z0-9_-]{1,64})/(sync|replication|mutate)/v[0-9]+/',\n)\n\nfunction projectIdFromZeroPath(pathname) {\n  const match = PROJECT_ZERO_PATH.exec(pathname)\n  return match ? match[1] : null\n}\n\n// native (expo) zero clients carry no cookie on the websocket: the session\n// bearer rides zero's Sec-WebSocket-Protocol encoding —\n// encodeURIComponent(btoa(JSON({ initConnectionMessage, authToken }))). the\n// browser client sends the same header, but its same-origin cookie is the\n// canonical credential there, so the cookie is TRIED first when both are\n// present — and a denied cookie falls through to the bearer (authorizeProjectNs).\nfunction bearerFromSecProtocol(request) {\n  const header = request.headers.get('sec-websocket-protocol') || ''\n  if (!header) return ''\n  try {\n    const decoded = JSON.parse(atob(decodeURIComponent(header)))\n    return typeof decoded.authToken === 'string' ? decoded.authToken : ''\n  } catch {\n    return ''\n  }\n}\n\n// authorize \"this credential's session user is a member of this project\"\n// through the app's own internal endpoint (one indexed read server-side). the\n// credential is the request cookie (browser) or the session bearer from the\n// websocket sec-protocol (native) — getAuthData accepts both. verdicts are\n// cached in-memory keyed by sha-256(projectId | credential) so a websocket\n// reconnect storm collapses to one endpoint call per minute per session.\nconst NS_AUTHORIZE_TTL_MS = 60000\nconst nsAuthorizeVerdicts = new Map()\n\nasync function authorizeProjectNs(request, env, ctx, projectId) {\n  const cookie = request.headers.get('cookie') || ''\n  const bearer = bearerFromSecProtocol(request)\n  if (!cookie && !bearer) return false\n  // ios attaches the shared cookie jar to app websockets, so a NATIVE request\n  // arrives with BOTH a (possibly stale) jar cookie and the valid session\n  // bearer in the sec-protocol. try the cookie first (canonical for browsers),\n  // but a denied cookie must fall through to the bearer — a dead jar cookie\n  // shadowing a valid bearer 403'd every mobile project sync (2026-07-08).\n  if (cookie && (await authorizeNsCredential(request, env, ctx, projectId, cookie, ''))) {\n    return true\n  }\n  if (bearer && (await authorizeNsCredential(request, env, ctx, projectId, '', bearer))) {\n    return true\n  }\n  return false\n}\n\nasync function authorizeNsCredential(request, env, ctx, projectId, cookie, bearer) {\n  const digest = await crypto.subtle.digest(\n    'SHA-256',\n    new TextEncoder().encode(projectId + '|' + (cookie || 'swp:' + bearer)),\n  )\n  const key = Array.from(new Uint8Array(digest), (byte) =>\n    byte.toString(16).padStart(2, '0'),\n  ).join('')\n  const now = Date.now()\n  const cached = nsAuthorizeVerdicts.get(key)\n  if (cached && now < cached.expires) return cached.allowed\n  // session + membership read through the app's normal data path; make sure\n  // the platform schema exists before the endpoint queries it. a transient\n  // infra failure (schema poke / endpoint blip) must be a RETRYABLE deny, never\n  // a cached negative or a 1101 throw — otherwise one blip locks a real member\n  // out for the full TTL.\n  try {\n    await ensureSchemaViaDataTier(env)\n  } catch {\n    return false\n  }\n  const authorizeUrl = new URL(request.url)\n  authorizeUrl.pathname = '/api/project/authorize-ns'\n  authorizeUrl.search = '?projectId=' + encodeURIComponent(projectId)\n  // one's getURLfromRequestURL does new URL(request.url, host ? http://host : \"\")\n  // — and on workerd/V8 new URL(absolute, \"\") THROWS \"Invalid URL string\" when\n  // the base is empty (bun tolerates it, which is why this only bit on CF). a\n  // synthesized subrequest must therefore carry a host header so one derives a\n  // valid base. forward the inbound host (this is same-origin, /api on our own\n  // worker) so the subrequest resolves exactly like a top-level one would.\n  let response\n  try {\n    response = await withAppProcessEnv(env, () =>\n      oneWorker.fetch(\n        new Request(authorizeUrl.toString(), {\n          headers: cookie\n            ? { cookie: cookie, host: authorizeUrl.host }\n            : { authorization: 'Bearer ' + bearer, host: authorizeUrl.host },\n        }),\n        env,\n        ctx,\n      ),\n    )\n  } catch {\n    return false\n  }\n  const allowed = response instanceof Response && response.status === 200\n  // cache ONLY positive verdicts: a deny may be a transient infra error or a\n  // not-yet-propagated membership write; re-check those next reconnect rather\n  // than pinning the user out for the TTL.\n  if (allowed) {\n    if (nsAuthorizeVerdicts.size > 1024) nsAuthorizeVerdicts.clear()\n    nsAuthorizeVerdicts.set(key, { allowed: true, expires: now + NS_AUTHORIZE_TTL_MS })\n  }\n  return allowed\n}\n\nfunction withAppProcessEnv(env, run) {\n  globalThis.process ||= {}\n  globalThis.process.env ||= {}\n  const processEnv = globalThis.process.env\n  const previous = new Map()\n  for (const key of ['ZERO_UPSTREAM_DB', 'ZERO_CVR_DB', 'ZERO_CHANGE_DB']) {\n    const had = Object.prototype.hasOwnProperty.call(processEnv, key)\n    previous.set(key, had ? processEnv[key] : undefined)\n    delete processEnv[key]\n  }\n  for (const [key, value] of Object.entries(env)) {\n    if (typeof value !== 'string') continue\n    if (previous.has(key)) continue\n    const had = Object.prototype.hasOwnProperty.call(processEnv, key)\n    previous.set(key, had ? processEnv[key] : undefined)\n    processEnv[key] = value\n  }\n  const restore = () => {\n    for (const [key, value] of previous) {\n      if (value === undefined) delete processEnv[key]\n      else processEnv[key] = value\n    }\n  }\n  try {\n    const result = run()\n    if (result && typeof result.then === 'function') return result.finally(restore)\n    restore()\n    return result\n  } catch (err) {\n    restore()\n    throw err\n  }\n}\n\n// db.pool in the app worker is a THIN client: each query forwards RAW pg SQL\n// { text, values } to orez-data /__nspfx_query over the service binding, where the\n// DoBackend (libpg parse + SQLite translate + exec) runs in the LEAN isolate. the\n// app worker does NO pg parsing — that kept the heavy app isolate GC-thrashing /\n// OOMing. (also keeps pg/pgsql-parser/libpg-query OUT of the app worker bundle.)\nfunction installAppSqlBackendGlobals(env) {\n  globalThis.__nspfx_cf_do_namespace = env.ZERO_APP_ID || 'zero'\n  globalThis.__nspfx_cf_do_create_pg_pool = () => makeRemotePgPool(env)\n  // per-project pool: project-scoped tables (file, snapshot, …) live in the\n  // project's OWN DO namespace, which the project zero instance reads from.\n  // server writes to those tables MUST stamp ns=proj-<id> or they land in the\n  // singleton namespace and the client never sees them (the empty-file-tree\n  // bug). projectId is validated to the data tier's accepted shape; an invalid\n  // id falls back to the singleton pool (a stray id can't mint a DO namespace).\n  globalThis.__nspfx_cf_project_pool = (projectId) =>\n    typeof projectId === 'string' && /^[A-Za-z0-9_-]{1,64}$/.test(projectId)\n      ? makeRemotePgPool(env, 'proj-' + projectId)\n      : makeRemotePgPool(env)\n  if (env.FILES) globalThis.__nspfx_cf_r2_bucket = env.FILES\n  // hoist the LanguageService worker binding so check_types\n  // (src/project/projectTypecheckPlatform.ts) routes to the warm nspfx-cf-ls\n  // DO instead of trying to spawn the bun+tsgo child.\n  if (env.LS) globalThis.__nspfx_cf_ls_service = env.LS\n  // hoist the data-tier binding so project create can provision its own\n  // per-project DO namespace (src/project/projectNamespacePlatform.ts).\n  if (env.OREZ_DATA) globalThis.__nspfx_cf_data_service = env.OREZ_DATA\n  // hoist the cf-build service binding so triggerBuild ships user-app deploy\n  // builds to the nspfx-cf-build-demo CF Container instead of importing the\n  // pruned buildUserApp chunk (src/deploy/buildRunnerPlatform.ts). CF-native,\n  // the cf-ls pattern; auth is the shared admin token. (the Hetzner box URL\n  // transport was removed when the box was decommissioned.)\n  if (env.BUILD_RUNNER && env.E2E_ADMIN_TOKEN) {\n    globalThis.__nspfx_build_runner_service = {\n      service: env.BUILD_RUNNER,\n      token: env.E2E_ADMIN_TOKEN,\n    }\n  }\n  // hoist the cf-factory service binding so the headless control plane\n  // (app/api/factory/headless+api.ts) starts a per-project cf-factory CF\n  // Container instead of spawning a local child process\n  // (src/features/factory/factoryRuntimePlatform.ts). absent → the local spawn path.\n  if (env.FACTORY_RUNNER && env.E2E_ADMIN_TOKEN) {\n    globalThis.__nspfx_factory_runtime_service = {\n      service: env.FACTORY_RUNNER,\n      token: env.E2E_ADMIN_TOKEN,\n    }\n  }\n}\n\n// minimal pg.Pool/Client shim round-tripping queries to orez-data /__nspfx_query.\n// ns (optional) stamps x-nspfx-ns so project-scoped tables (file, snapshot, …)\n// land in the project's OWN DO namespace (ns=proj-<id>) — the one the project\n// zero instance reads from. absent → the control-plane 'singleton' namespace.\nfunction makeRemotePgPool(env, ns) {\n  // an explicit ns arg (e.g. __nspfx_cf_project_pool / projectDb) always wins;\n  // otherwise resolve the request-scoped ns from the ALS at call time so the\n  // no-arg zeroServer pool routes the authoritative mutator replay's writes to\n  // the project namespace the inbound push carried. absent both => singleton.\n  const headersFor = () => {\n    const effective =\n      ns ||\n      (typeof globalThis.__nspfx_current_ns === 'function'\n        ? globalThis.__nspfx_current_ns()\n        : undefined)\n    return effective\n      ? { 'content-type': 'application/json', 'x-nspfx-ns': effective }\n      : { 'content-type': 'application/json' }\n  }\n  const runQuery = async (query, params) => {\n    const text = typeof query === 'string' ? query : query && query.text\n    const values =\n      params !== undefined ? params : query && (query.values || query.params)\n    const res = await env.OREZ_DATA.fetch(\n      new Request('https://orez-data.local/__nspfx_query', {\n        method: 'POST',\n        headers: headersFor(),\n        body: JSON.stringify({ text, values: values || [] }),\n      }),\n    )\n    const body = await res.json()\n    if (!res.ok) throw new Error(body && body.error ? body.error : 'query failed')\n    const rowMode = query && typeof query === 'object' ? query.rowMode : undefined\n    const outRows = body.rows || []\n    if (rowMode === 'array' && outRows.length > 0) {\n      // drizzle-orm's typed SELECT path requests rowMode:'array' then indexes each\n      // row POSITIONALLY (mapResultRow -> row[0], row[1], ...). the data tier\n      // returns column-keyed objects, so project them into arrays in column order,\n      // exactly as node-postgres would. without this drizzle reads row[0] on an\n      // object -> undefined -> the first typed decoder (timestamp/json) throws.\n      const keys = Object.keys(outRows[0])\n      return {\n        rows: outRows.map((r) => keys.map((k) => r[k])),\n        rowCount: body.rowCount,\n        fields: keys.map((name) => ({ name })),\n      }\n    }\n    return { rows: outRows, rowCount: body.rowCount }\n  }\n  // ordered multi-statement execution in one round trip (see the DO's\n  // /__nspfx_pg batch branch). app code reaches this through db.server's\n  // sqlBatch seam.\n  const runBatch = async (statements) => {\n    const res = await env.OREZ_DATA.fetch(\n      new Request('https://orez-data.local/__nspfx_query', {\n        method: 'POST',\n        headers: headersFor(),\n        body: JSON.stringify({\n          batch: statements.map((s) => ({ text: s.text, values: s.values || [] })),\n        }),\n      }),\n    )\n    const body = await res.json()\n    if (!res.ok) throw new Error(body && body.error ? body.error : 'batch failed')\n    return body.results || []\n  }\n  const client = {\n    query: (q, p, cb) => {\n      const promise = runQuery(q, typeof p === 'function' ? undefined : p)\n      const callback = typeof p === 'function' ? p : cb\n      if (typeof callback === 'function') {\n        promise.then((r) => callback(null, r), callback)\n        return undefined\n      }\n      return promise\n    },\n    batch: runBatch,\n    release: () => {},\n    on: () => client,\n    end: async () => {},\n  }\n  return {\n    connect: (cb) => {\n      if (typeof cb === 'function') {\n        cb(null, client, () => {})\n        return undefined\n      }\n      return Promise.resolve(client)\n    },\n    query: client.query,\n    batch: runBatch,\n    on: () => client,\n    end: async () => {},\n  }\n}\n\n// poke the data tier to run the schema migration before the first app write.\nlet appSchemaReady\nfunction ensureSchemaViaDataTier(env) {\n  if (appSchemaReady) return appSchemaReady\n  appSchemaReady = (async () => {\n    const res = await env.OREZ_DATA.fetch(\n      new Request('https://orez-data.local/__nspfx_migrate'),\n    )\n    if (!res.ok) throw new Error('schema migration failed: ' + res.status)\n    await res.text().catch(() => {})\n  })().catch((err) => {\n    appSchemaReady = undefined\n    throw err\n  })\n  return appSchemaReady\n}\n\nexport default {\n  async fetch(request, env, ctx) {\n    if (env.AUTH_DB) globalThis.AUTH_DB = env.AUTH_DB\n    installAppSqlBackendGlobals(env)\n    // bridge the worker's waitUntil so app code (the zero push handler) can keep\n    // mutator asyncTasks — e.g. seedTemplateProject — alive past the response.\n    // workerd kills un-awaited promises on response; without this a created\n    // project's template files never seed.\n    globalThis.__nspfx_background_task = (p) => {\n      try {\n        ctx.waitUntil(Promise.resolve(p))\n      } catch {}\n    }\n    const url = new URL(request.url)\n    // operational per-namespace backup + restore + write-circuit-breaker\n    // control: always admin-token gated,\n    // same ns rules as sync. GET /__nspfx_export[?ns=proj-…] streams that\n    // namespace's tables to R2 backups/; POST /__nspfx_import?confirm=<ns>\n    // {key} overwrite-restores a dump into the namespace.\n    if (\n      url.pathname === '/__nspfx_export' ||\n      url.pathname === '/__nspfx_import' ||\n      url.pathname === '/__nspfx_circuit'\n    ) {\n      if (!adminTokenValid(request, env)) {\n        return new Response('forbidden', { status: 403 })\n      }\n      const ns = requestedNamespace(request, url)\n      const forwardUrl = new URL('https://orez-data.local' + url.pathname)\n      forwardUrl.search = url.search\n      const forward = new Request(forwardUrl.toString(), request)\n      forward.headers.delete('x-nspfx-ns')\n      if (ns && ns !== 'nspfx') {\n        if (!isValidNamespace(ns)) {\n          return new Response('invalid ns', { status: 400 })\n        }\n        forward.headers.set('x-nspfx-ns', ns)\n      }\n      return env.OREZ_DATA.fetch(forward)\n    }\n    // per-project zero traffic: /p-<projectId>/{sync|replication|mutate}/v*.\n    // authorize via the e2e admin token (probe surface) or the request\n    // credential's project membership (cookie, or the native websocket\n    // bearer), then forward the path unmodified with\n    // x-nspfx-ns stamped. inbound x-nspfx-ns never forwards — the stamp below\n    // overwrites it, and a denied request never reaches the data tier.\n    const zeroProjectId = projectIdFromZeroPath(url.pathname)\n    if (zeroProjectId) {\n      if (\n        !adminTokenValid(request, env) &&\n        !(await authorizeProjectNs(request, env, ctx, zeroProjectId))\n      ) {\n        return new Response('ns forbidden', { status: 403 })\n      }\n      const forward = new Request(request)\n      forward.headers.set('x-nspfx-ns', 'proj-' + zeroProjectId)\n      return env.OREZ_DATA.fetch(forward)\n    }\n    // /sync + zero-cache traffic -> the data tier's ZeroCacheDO, stamped with\n    // the shim's namespace decision (see requestedNamespace above).\n    if (isZeroCachePath(url.pathname)) {\n      const ns = requestedNamespace(request, url)\n      const forward = new Request(zeroCacheRequestForUrl(request, url))\n      forward.headers.delete('x-nspfx-ns')\n      if (ns && ns !== 'nspfx') {\n        if (!isValidNamespace(ns)) {\n          return new Response('invalid ns', { status: 400 })\n        }\n        if (!adminTokenValid(request, env)) {\n          return new Response('ns forbidden', { status: 403 })\n        }\n        forward.headers.set('x-nspfx-ns', ns)\n      }\n      return env.OREZ_DATA.fetch(forward)\n    }\n    // before an app WRITE to the SQL DO (better-auth, bootstrap, mutator), make\n    // sure the platform schema exists in the data tier.\n    if (needsSqlSchema(url.pathname)) {\n      await ensureSchemaViaDataTier(env)\n    }\n    // the authoritative zero push from a project embed carries x-nspfx-ns; scope\n    // it so the singleton zeroServer pool routes the mutator replay's writes to\n    // ns=proj-<id> (matching the optimistic client copy). control-plane traffic\n    // has no header => no scope => 'singleton', unchanged. only the internal\n    // embed reaches /api/zero/push with x-nspfx-ns; validate the shape so a stray\n    // header can't mint a namespace.\n    const inboundNs = request.headers.get('x-nspfx-ns')\n    const scopedNs =\n      inboundNs && /^(proj|test)-[A-Za-z0-9_-]{1,64}$/.test(inboundNs)\n        ? inboundNs\n        : undefined\n    return globalThis.__nspfx_run_in_ns(scopedNs, () =>\n      withAppProcessEnv(env, async () => {\n        // a request that falls through every route must never surface a\n        // non-Response to workerd (error 1101 took previews down when a stale\n        // build's asset manifest was missing a deps-web file). resolve to 404.\n        const response = await oneWorker.fetch(request, env, ctx)\n        return response instanceof Response\n          ? response\n          : new Response('not found', { status: 404 })\n      }),\n    )\n  },\n}\n"

export type ShimBuildOptions = {
  database?: 'postgres' | 'sqlite'
}

/** data-tier worker entry (control-plane split deploy): sql/migrate/sync/backup only. */
export function buildDataShimSource(
  cfg: CfDeployConfig,
  options: ShimBuildOptions = {}
): string {
  // consumer-declared minute-cron fetches forwarded to the app worker over the APP
  // service binding (workerd has no long-running process, so a job/flow runner in the
  // app worker needs a periodic tick). emitted into the data shim's minute-cron branch
  // via the __CRON_FORWARDS__ sentinel; empty (byte-identical) when none configured.
  const forwards = (cfg.minuteCronAppForwards ?? [])
    .map(
      (f) =>
        `      ctx.waitUntil(env.APP.fetch(new Request('https://app${f.path}', { method: 'POST', headers: { host: 'app', 'x-cron-secret': env.${f.secretEnvVar} || '' } })))\n`
    )
    .join('')
  const transformed = applyRequestScopedDoBackend(
    persistEmbedBootRequest(
      addDeployTerminalWarmProbe(
        persistBootFailureBeforeRetry(
          applyMigrationLifecycle(
            allowLargeReplicaResync(
              passEmbedInstanceId(serializeShimPgBatches(DATA_SHIM_TEMPLATE))
            )
          )
        )
      )
    )
  )
  const source = applyPrefix(
    options.database === 'sqlite'
      ? applySQLiteOnlyDataTransport(transformed, true)
      : transformed,
    cfg
  ).replace('__CRON_FORWARDS__', forwards)
  return applyDataWorkerZeroPush(source, cfg)
}

/** single-worker user-app entry: both DO classes + the One app in one worker. */
export function buildUserShimSource(
  cfg: CfDeployConfig,
  options: ShimBuildOptions = {}
): string {
  const transformed = applySqlSchemaGateContract(
    applyRequestScopedDoBackend(
      persistEmbedBootRequest(
        addDeployTerminalWarmProbe(
          persistBootFailureBeforeRetry(
            applyMigrationLifecycle(
              allowLargeReplicaResync(
                passEmbedInstanceId(serializeShimPgBatches(USER_SHIM_TEMPLATE))
              )
            )
          )
        )
      )
    )
  )
  return applyPrefix(
    options.database === 'sqlite'
      ? applySQLiteOnlyAppTransport(
          applySQLiteOnlyDataTransport(transformed, false),
          false
        )
      : transformed,
    cfg
  )
}

/** app-tier worker entry (control-plane split deploy): namespace router over the data binding. */
export function buildAppShimSource(
  cfg: CfDeployConfig,
  options: ShimBuildOptions = {}
): string {
  const transformed = applySqlSchemaGateContract(APP_SHIM_TEMPLATE)
  return applyPrefix(
    options.database === 'sqlite'
      ? applySQLiteOnlyAppTransport(transformed, true)
      : transformed,
    cfg
  )
}

export type RustSyncUserShimOptions = {
  feedTables: Readonly<Record<string, readonly string[]>>
}

const RUST_SYNC_USER_SHIM_TEMPLATE = `
import { WorkerEntrypoint } from 'cloudflare:workers'
import { AsyncLocalStorage } from 'node:async_hooks'
import { ZeroDO as OrezZeroSqlDO, createApplicationSqlClient } from 'orez/cf-do'
import { installZeroSqlWriteCircuitBreaker } from 'orez/worker/zero-sql-write-circuit'
import {
  runNspfxCloudflareMigrations,
} from './orez-migrations.js'

const SYNC_FEED_TABLES = __SYNC_FEED_TABLES__
const applicationSqlRequestSignals = new AsyncLocalStorage()

function stringEnv(env) {
  const out = {}
  for (const [key, value] of Object.entries(env)) {
    if (typeof value === 'string') out[key] = value
  }
  return out
}

function sqlDoFetch(env, instance) {
  return (input, init) => {
    const request = new Request(input, init)
    request.headers.set('x-nspfx-do-instance', instance)
    const id = env.ZERO_SQL_DO.idFromName(instance)
    return env.ZERO_SQL_DO.get(id).fetch(request)
  }
}

function installApplicationSqlClient(env) {
  if (!env.ZERO_SQL_DO) {
    throw new Error('Cloudflare SQL Durable Object binding is not initialized')
  }
  // This is intentionally an internal factory, not a fetch endpoint. Soot's
  // SQLite database owner chooses the authoritative namespace at each call,
  // and the Orez client binds the corresponding Durable Object once.
  globalThis.__nspfx_cf_application_sql_client = (namespace = 'singleton') =>
    createApplicationSqlClient(env.ZERO_SQL_DO, namespace, {
      signal: applicationSqlRequestSignals.getStore(),
    })
  if (env.FILES) globalThis.__nspfx_cf_r2_bucket = env.FILES
}

function withAppProcessEnv(env, run) {
  globalThis.process ||= {}
  globalThis.process.env ||= {}
  const processEnv = globalThis.process.env
  const previous = new Map()
  for (const key of ['ZERO_UPSTREAM_DB', 'ZERO_CVR_DB', 'ZERO_CHANGE_DB']) {
    const had = Object.prototype.hasOwnProperty.call(processEnv, key)
    previous.set(key, had ? processEnv[key] : undefined)
    delete processEnv[key]
  }
  for (const [key, value] of Object.entries(stringEnv(env))) {
    if (previous.has(key)) continue
    const had = Object.prototype.hasOwnProperty.call(processEnv, key)
    previous.set(key, had ? processEnv[key] : undefined)
    processEnv[key] = value
  }
  const restore = () => {
    for (const [key, value] of previous) {
      if (value === undefined) delete processEnv[key]
      else processEnv[key] = value
    }
  }
  try {
    const result = run()
    if (result && typeof result.then === 'function') return result.finally(restore)
    restore()
    return result
  } catch (error) {
    restore()
    throw error
  }
}

export class ZeroSqlDO extends OrezZeroSqlDO {
  constructor(ctx, env) {
    super(ctx, env)
    installZeroSqlWriteCircuitBreaker(ctx.storage.sql, {
      table: '_nspfx_write_circuit',
      logPrefix: '[nspfx]',
      rowsPerWindow: 200_000,
      hardRowsPerWindow: 1_000_000,
    })
  }

}

export { ZeroSqlDO as ZeroDO }

let appSchemaReady
function ensureAppSchema(env) {
  if (appSchemaReady) return appSchemaReady
  appSchemaReady = (async () => {
    await runNspfxCloudflareMigrations({ instance: 'singleton', schemaOnly: true })
  })().catch((error) => {
    if (!String(error && error.message).includes('application SQLite schema mismatch')) {
      appSchemaReady = undefined
    }
    throw error
  })
  return appSchemaReady
}

function needsSqlSchema(request, pathname) {
  if (pathname.startsWith('/api/zero/')) return true
  if (pathname.startsWith('/api/bootstrap-')) return true
  return pathname.startsWith('/api/auth/') && request.method !== 'GET'
}

function publicRow(tableName, row) {
  if (!row || typeof row !== 'object') return row
  const columns = SYNC_FEED_TABLES[tableName]
  if (!columns) return row
  return Object.fromEntries(
    columns.filter((column) => column in row).map((column) => [column, row[column]]),
  )
}

function normalizeFeedBody(body) {
  if (body && body.tables && typeof body.tables === 'object') {
    body.tables = Object.fromEntries(
      Object.entries(body.tables).map(([rawTableName, rows]) => {
        const tableName = rawTableName.replace(/^public\\./, '')
        return [
          tableName,
          Array.isArray(rows) ? rows.map((row) => publicRow(tableName, row)) : rows,
        ]
      }),
    )
  }
  if (body && Array.isArray(body.changes)) {
    body.changes = body.changes.map((change) => {
      const tableName = String(change.tableName).replace(/^public\\./, '')
      if (/^[^.]+_0\\.(clients|mutations)$/.test(tableName)) {
        return {
          watermark: change.watermark,
          tableName: 'syncCursor',
          op: 'INSERT',
          rowData: { id: 'zero-http', watermark: change.watermark },
          oldData: null,
        }
      }
      return {
        ...change,
        tableName,
        rowData: publicRow(tableName, change.rowData),
        oldData: publicRow(tableName, change.oldData),
      }
    })
  }
  return body
}

async function dataFeed(request, env) {
  installApplicationSqlClient(env)
  const url = new URL(request.url)
  const allowed =
    url.pathname === '/snapshot' ||
    url.pathname === '/changes' ||
    url.pathname === '/_orez/write-budget' ||
    url.pathname === '/_orez/write-budget/reopen'
  if (!allowed) return new Response('not found', { status: 404 })
  if (url.pathname === '/snapshot' || url.pathname === '/changes') {
    await ensureAppSchema(env)
  }
  const forward = new Request(url.toString(), request)
  forward.headers.set('x-nspfx-do-instance', 'singleton')
  const response = await sqlDoFetch(env, 'singleton')(forward)
  if (!response.ok || url.pathname.startsWith('/_orez/')) return response
  const body = normalizeFeedBody(await response.json())
  const headers = new Headers(response.headers)
  headers.delete('content-length')
  headers.set('content-type', 'application/json')
  return new Response(JSON.stringify(body), { status: response.status, headers })
}

export class OrezDataFeed extends WorkerEntrypoint {
  fetch(request) {
    return applicationSqlRequestSignals.run(request.signal, () =>
      dataFeed(request, this.env),
    )
  }
}

let oneWorkerPromise
function getOneWorker() {
  oneWorkerPromise ||= import('./one-app.js').then((module) => module.default)
  return oneWorkerPromise
}

export default {
  async fetch(request, env, ctx) {
    return applicationSqlRequestSignals.run(request.signal, async () => {
      installApplicationSqlClient(env)
      if (env.AUTH_DB) globalThis.AUTH_DB = env.AUTH_DB
      globalThis.__nspfx_background_task = (task) => {
        try {
          ctx.waitUntil(Promise.resolve(task))
        } catch {}
      }
      const url = new URL(request.url)
      if (needsSqlSchema(request, url.pathname)) {
        try {
          await ensureAppSchema(env)
        } catch (error) {
          console.error('[nspfx] application schema migration failed', error)
          return new Response('application schema migration failed', { status: 503 })
        }
      }
      return withAppProcessEnv(env, async () => {
        const oneWorker = await getOneWorker()
        const response = await oneWorker.fetch(request, env, ctx)
        return response instanceof Response ? response : new Response('not found', { status: 404 })
      })
    })
  },
}
`

/** single-worker app entry with SQL storage and a private Rust-host data feed. */
export function buildRustSyncUserShimSource(
  cfg: CfDeployConfig,
  options: RustSyncUserShimOptions
): string {
  return applyPrefix(
    RUST_SYNC_USER_SHIM_TEMPLATE.replace(
      '__SYNC_FEED_TABLES__',
      JSON.stringify(options.feedTables)
    ),
    cfg
  )
}
