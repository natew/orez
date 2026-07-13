/**
 * orez: pglite-powered zero-sync development backend.
 *
 * starts a pglite instance, tcp proxy, and zero-cache process.
 * replaces docker-based postgresql and zero-cache with a single
 * `bun run` command.
 */

import { spawn, spawnSync, type ChildProcess } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import {
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  unlinkSync,
  writeFileSync,
} from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'

import {
  createHttpLogStore,
  startHttpProxy,
  type HttpLogStore,
} from './admin/http-proxy.js'
import { createLogStore, type LogStore } from './admin/log-store.js'
import {
  isChildProcessRunning,
  isPidRunning,
  killProcessTree,
  terminateChildProcessTree,
  terminateProcessTree,
} from './child-process.js'
import { getConfig, getConnectionString } from './config.js'
import { log, port, setLogLevel, setLogStore } from './log.js'
import { DoBackend } from './pg-proxy-do-backend.js'
import { PgStartupBarrier, startPgProxy } from './pg-proxy.js'
import {
  createPGliteInstances,
  createPGliteWorkerInstances,
  createSinglePGliteInstance,
  createSinglePGliteWorkerInstance,
  createPGliteWorker,
  runMigrations,
  startPeriodicCheckpoint,
  startPeriodicVacuum,
  vacuumPGliteChurnTables,
} from './pglite-manager.js'
import { findPort, findPortBlock } from './port.js'
import { orezTitle } from './process-title.js'
import { ensurePublicationHasTables, syncManagedPublications } from './publications.js'
import {
  classifyZeroCrashRecovery,
  classifyZeroStartupRecovery,
  hasRecoverableZeroStateSignature,
  hasZeroReplicaMonitorWarmupSignature,
  getZeroReplicaStartupResetReason,
  isReplicationBankrupt,
  recoverZeroState,
  zeroInconsistencyResetMode,
  type ZeroStartupRetryState,
} from './recovery.js'
import { installChangeTracking } from './replication/change-tracker.js'
import {
  getReplicationHealth,
  markReplicationProgress,
  resetReplicationState,
} from './replication/handler.js'
import {
  applySqliteMode,
  cleanupShim,
  formatNativeBootstrapInstructions,
  hasMissingNativeBinarySignature,
  inspectNativeSqliteBinary,
  resolveSqliteMode,
  resolveSqliteModeConfig,
  type SqliteMode,
  type SqliteModeConfig,
} from './sqlite-mode/index.js'
import { enableZeroChangeLogCleanupRetry } from './zero-changelog-cleanup-patch.js'
import { enableZeroReplicaCheckpoint } from './zero-checkpoint-patch.js'
import { probeZeroCacheHttp } from './zero-health.js'
import { disableZeroLitestreamRestore } from './zero-litestream-patch.js'

import type { Hook, HookContext, ZeroLiteConfig } from './config.js'
import type { PGlite } from '@electric-sql/pglite'

type ZeroChildProcess = ChildProcess & {
  __orezPreloadPath?: string
  __orezTail?: string[]
}

function ensureDoBackendNamespace(dataDir: string): string {
  const marker = resolve(dataDir, 'do-backend-namespace')
  if (existsSync(marker)) {
    const existing = readFileSync(marker, 'utf8').trim()
    if (existing) return existing
  }
  const next = randomUUID()
  writeFileSync(marker, `${next}\n`)
  return next
}

function resolveNodeBinary(): string {
  const explicitNode = process.env.OREZ_NODE
  if (explicitNode && existsSync(explicitNode)) {
    return explicitNode
  }

  // bad agentic code, should make this configurable
  const miseResult = spawnSync('mise', ['which', 'node'], {
    encoding: 'utf8',
    env: process.env,
  })
  const miseCandidate = miseResult.stdout?.trim()
  if (miseResult.status === 0 && miseCandidate && existsSync(miseCandidate)) {
    return miseCandidate
  }

  if (process.execPath.endsWith('/node')) {
    return process.execPath
  }

  const inheritedNode = process.env.NODE
  if (inheritedNode && existsSync(inheritedNode)) {
    return inheritedNode
  }

  const whichResult = spawnSync('which', ['node'], {
    encoding: 'utf8',
    env: process.env,
  })
  const candidate = whichResult.stdout?.trim()
  if (whichResult.status === 0 && candidate && existsSync(candidate)) {
    return candidate
  }

  throw new Error(
    'could not resolve a node binary for zero-cache; set OREZ_NODE or ensure node is in PATH'
  )
}

export { defineConfig, getConfig, getConnectionString } from './config.js'
export type { Hook, HookContext, LogLevel, OrezConfig, ZeroLiteConfig } from './config.js'
export { deployTimeSchemaBatchStatements } from './pg-proxy-do-backend.js'
export { installChangeTracking } from './replication/change-tracker.js'

// helper to run a hook (string command or callback function)
async function runHook(
  hook: Hook | undefined,
  name: string,
  env: Record<string, string>,
  context: HookContext
): Promise<void> {
  if (!hook) return

  if (typeof hook === 'function') {
    log.debug.orez(`running ${name} callback`)
    // function callbacks run behind the startup barrier just like shell hooks;
    // the context hands them a privileged (barrier-bypassing) connection so
    // they can provision while ordinary clients stay held.
    await hook(context)
    log.orez(`${name} done`)
    return
  }

  // string command
  log.debug.orez(`running ${name}: ${hook}`)
  await new Promise<void>((resolve, reject) => {
    const child = spawn(hook, {
      shell: true,
      stdio: 'inherit',
      env: { ...process.env, ...env },
    })
    child.on('exit', (code) => {
      if (code === 0) {
        log.orez(`${name} done`)
        resolve()
      } else {
        reject(new Error(`${name} exited with code ${code}`))
      }
    })
    child.on('error', reject)
  })
}

/**
 * Build the context passed to a programmatic lifecycle callback. `resolve`
 * returns a connection string per database. Pass the barrier-tagged resolver at
 * startup so the callback's connections bypass the barrier, or the plain
 * resolver for post-startup hooks where no barrier is active.
 */
function buildHookContext(
  config: ZeroLiteConfig,
  resolve: (dbName: string) => string,
  applicationName?: string
): HookContext {
  return {
    upstreamConnectionString: resolve('postgres'),
    cvrConnectionString: resolve('zero_cvr'),
    cdbConnectionString: resolve('zero_cdb'),
    applicationName,
    pgPort: config.pgPort,
  }
}

function getManagedPublicationConfig(): { names: string[]; managedByOrez: boolean } {
  const existing = process.env.ZERO_APP_PUBLICATIONS?.trim()
  if (existing) {
    const names = existing
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
    return { names, managedByOrez: false }
  }

  const appId = (process.env.ZERO_APP_ID || 'zero').trim() || 'zero'
  const fallback = `orez_${appId}_public`
  process.env.ZERO_APP_PUBLICATIONS = fallback
  return { names: [fallback], managedByOrez: true }
}

function getReplicaDir(config: ZeroLiteConfig): string {
  return config.ephemeralDir ?? config.dataDir
}

function zeroReplicaPath(config: ZeroLiteConfig): string {
  return resolve(getReplicaDir(config), 'zero-replica.db')
}

function pgliteDataDirFor(config: ZeroLiteConfig, name: string): string {
  return config.ephemeral ? 'memory://' : resolve(config.dataDir, `pgdata-${name}`)
}

// resolvePackage moved to sqlite-mode/resolve-mode.ts
import { resolvePackage } from './sqlite-mode/resolve-mode.js'

export async function startZeroLite(overrides: Partial<ZeroLiteConfig> = {}) {
  const config = getConfig(overrides)
  setLogLevel(config.logLevel)

  if (config.ephemeral && !config.doBackendUrl) {
    config.pgliteOptions = { ...config.pgliteOptions, dataDir: 'memory://' }
    config.ephemeralDir = resolve(
      tmpdir(),
      `orez-ephemeral-${process.pid}-${randomUUID()}`
    )
    mkdirSync(config.ephemeralDir, { recursive: true })
  }

  // find available ports
  const pgPort = await findPort(config.pgPort)
  const zeroPort = config.skipZeroCache
    ? config.zeroPort
    : await findPortBlock(config.zeroPort, 2, { host: '::' })
  const adminPort = config.adminPort > 0 ? await findPort(config.adminPort) : 0
  if (pgPort !== config.pgPort)
    log.debug.orez(`port ${config.pgPort} in use, using ${pgPort}`)
  if (!config.skipZeroCache && zeroPort !== config.zeroPort)
    log.debug.orez(`port ${config.zeroPort} in use, using ${zeroPort}`)
  if (adminPort > 0 && adminPort !== config.adminPort)
    log.debug.orez(`port ${config.adminPort} in use, using ${adminPort}`)
  config.pgPort = pgPort
  config.zeroPort = zeroPort
  config.adminPort = adminPort

  // create log store for admin dashboard
  const logStore: LogStore | undefined =
    adminPort > 0
      ? createLogStore(config.dataDir, !config.disableDiskLogs, config.maxLogFileSize)
      : undefined

  // wire up logStore so all log.* calls flow to admin dashboard
  setLogStore(logStore)

  // create http log store for HTTP tab
  const httpLog: HttpLogStore | undefined =
    adminPort > 0 ? createHttpLogStore() : undefined

  log.debug.orez(`data dir: ${resolve(config.dataDir)}`)
  if (config.ephemeralDir) {
    log.debug.orez(`ephemeral cache dir: ${config.ephemeralDir}`)
  }

  // resolve sqlite mode config early (used for shim application and cleanup)
  // auto-detects native if available, falls back to wasm
  let sqliteMode = resolveSqliteMode(config.disableWasmSqlite, config.forceWasmSqlite)
  let sqliteModeConfig = resolveSqliteModeConfig(
    config.disableWasmSqlite,
    config.forceWasmSqlite
  )
  if (sqliteMode === 'wasm' && !sqliteModeConfig) {
    log.orez(
      'warning: wasm sqlite requested but dependencies are missing, falling back to native'
    )
    sqliteMode = 'native'
    config.disableWasmSqlite = true
    sqliteModeConfig = resolveSqliteModeConfig(true, false)
  }

  mkdirSync(config.dataDir, { recursive: true })

  // write pid file for IPC (pg_restore uses this to signal restart).
  // before overwriting, check for orphaned zero-cache processes from a
  // previous orez run that didn't shut down cleanly (e.g. SIGKILL'd before
  // the in-process watchdog could notice). sweep anything still holding
  // the zero port so the new run can bind.
  const pidFile = resolve(config.dataDir, 'orez.pid')
  let priorPid = 0
  try {
    priorPid = Number(readFileSync(pidFile, 'utf8').trim())
  } catch {}
  if (priorPid > 0 && priorPid !== process.pid) {
    if (isPidRunning(priorPid)) {
      log.orez(
        `stopping prior orez pid ${priorPid} for data dir ${resolve(config.dataDir)}`
      )
      const stopped = await terminateProcessTree(priorPid, {
        gracefulSignal: 'SIGTERM',
        forceSignal: 'SIGKILL',
        graceMs: 5000,
        forceGraceMs: 1000,
      })
      if (!stopped) {
        throw new Error(
          `could not stop prior orez pid ${priorPid} for data dir ${resolve(
            config.dataDir
          )}`
        )
      }
    } else if (!config.skipZeroCache && process.platform !== 'win32') {
      try {
        const result = spawnSync('lsof', ['-ti', `:${config.zeroPort}`], {
          encoding: 'utf8',
        })
        const orphans = (result.stdout || '')
          .split(/\s+/)
          .map((v) => Number(v.trim()))
          .filter((v) => Number.isInteger(v) && v > 0 && v !== process.pid)
        for (const pid of orphans) {
          log.orez(
            `killing orphan pid ${pid} holding zero port ${config.zeroPort} from previous orez run`
          )
          try {
            killProcessTree(pid, 'SIGKILL')
          } catch {}
        }
      } catch {}
    }
  }
  writeFileSync(pidFile, String(process.pid))

  // write admin port file so pg_restore can find it
  const adminFile = resolve(config.dataDir, 'orez.admin')
  if (adminPort > 0) {
    writeFileSync(adminFile, String(adminPort))
  }

  // remove any stale ready marker from a previous run so external waiters
  // (e.g. CI scripts) don't see a stale "ready" before this run finishes
  // initializing. the marker is (re-)written after on-db-ready completes.
  const readyFile = resolve(config.dataDir, 'orez.ready')
  try {
    unlinkSync(readyFile)
  } catch {}

  // start pglite instance(s).
  // single-db mode uses one instance for all databases (lighter for constrained envs).
  // otherwise, separate instances for postgres, zero_cvr, zero_cdb with optional
  // worker threads for non-blocking WASM execution.

  // ── DO backend path (replaces PGlite) ──────────────────────────────
  let instances: any,
    db: any,
    stopCheckpoint: any,
    stopVacuum: any = () => {}
  let migrationsApplied = 0
  let isDoBackend = false
  let nativePg: import('./native-postgres.js').NativePostgres | undefined
  let pgliteVacuumMs = 0
  let delayedVacuumTimer: ReturnType<typeof setTimeout> | undefined

  const queuePgliteVacuum = (delayMs = 15_000) => {
    if (isDoBackend || pgliteVacuumMs <= 0) return
    if (delayedVacuumTimer) clearTimeout(delayedVacuumTimer)
    delayedVacuumTimer = setTimeout(() => {
      delayedVacuumTimer = undefined
      void vacuumPGliteChurnTables(instances)
    }, delayMs)
    delayedVacuumTimer.unref?.()
  }

  if (config.doBackendUrl) {
    isDoBackend = true
    log.orez(`using DO backend: ${config.doBackendUrl}`)
    const backendUrl = config.doBackendUrl.replace(/\/+$/, '')
    const doNamespace = ensureDoBackendNamespace(config.dataDir)
    const doInstances = {
      postgres: new DoBackend(backendUrl, 'postgres', doNamespace),
      cvr: new DoBackend(backendUrl, 'zero_cvr', doNamespace),
      cdb: new DoBackend(backendUrl, 'zero_cdb', doNamespace),
      postgresReplicas: [],
    }
    await Promise.all([
      doInstances.postgres.waitReady,
      doInstances.cvr.waitReady,
      doInstances.cdb.waitReady,
    ])
    instances = doInstances
    db = doInstances.postgres
    stopCheckpoint = () => {}
  } else if (config.backend === 'postgres') {
    // ── native postgres backend (real postgres, real logical replication) ──
    const { startNativePostgres } = await import('./native-postgres.js')
    nativePg = await startNativePostgres(config)
    instances = nativePg.instances
    db = instances.postgres
    stopCheckpoint = () => {}
    log.pg(`using native postgres backend`)

    if (config.zeroPublications && !process.env.ZERO_APP_PUBLICATIONS) {
      process.env.ZERO_APP_PUBLICATIONS = config.zeroPublications
    }

    migrationsApplied = await runMigrations(db, config)
  } else {
    // ── PGlite backend (default) ────────────────────────────────────────────
    instances = config.singleDb
      ? config.useWorkerThreads
        ? await createSinglePGliteWorkerInstance(config)
        : await createSinglePGliteInstance(config)
      : config.useWorkerThreads
        ? await createPGliteWorkerInstances(config)
        : await createPGliteInstances(config)
    db = instances.postgres

    // periodic WAL checkpoint
    stopCheckpoint =
      config.checkpointIntervalMs > 0
        ? startPeriodicCheckpoint(instances, config.checkpointIntervalMs)
        : () => {}

    // config-based publications
    if (config.zeroPublications && !process.env.ZERO_APP_PUBLICATIONS) {
      process.env.ZERO_APP_PUBLICATIONS = config.zeroPublications
    }

    // run migrations & change tracking
    migrationsApplied = await runMigrations(db, config)
    log.debug.orez('installing change tracking')
    await installChangeTracking(db)

    // periodic VACUUM of the change-tracking churn tables — PGlite has no
    // effective autovacuum, so without this the change buffers bloat until the
    // change-streamer scan times out and Zero stops sending live updates. Start it
    // only after Orez's own tracking tables exist; zero-cache's CDB changeLog is
    // vacuumed once more after zero-cache has started.
    pgliteVacuumMs = Number(process.env.OREZ_VACUUM_MS ?? 60 * 1000)
    if (pgliteVacuumMs > 0) {
      await vacuumPGliteChurnTables(instances)
      stopVacuum = startPeriodicVacuum(instances, pgliteVacuumMs)
    }
  }

  // shared: publications config
  if (config.zeroPublications && !process.env.ZERO_APP_PUBLICATIONS) {
    process.env.ZERO_APP_PUBLICATIONS = config.zeroPublications
  }
  const managedPub = getManagedPublicationConfig()
  if (managedPub.managedByOrez) {
    log.debug.orez(`using managed publication: ${managedPub.names.join(', ')}`)
  }

  // sync publications. for DO backend this goes through the TCP proxy, which
  // rewrites the catalog queries and forwards CREATE PUBLICATION / ALTER PUBLICATION
  // as no-ops or DO-native equivalents (PGlite still owns the real path).
  await syncManagedPublications(db, managedPub.names, managedPub.managedByOrez)

  // on-db-ready hooks connect back through the proxy. Hold every other client at
  // PG startup until that hook has provisioned the schema; otherwise an early
  // application SELECT can monopolize the shared DB mutex while the DO retries
  // "no such table", preventing the migration itself from running. This applies
  // to BOTH forms of the hook: shell commands receive the tagged connection via
  // env, and function callbacks receive it via HookContext (see buildHookContext
  // below), so ordinary programmatic clients can't race either one.
  const pgStartupBarrier =
    !nativePg && config.onDbReady != null
      ? new PgStartupBarrier(`orez-on-db-ready-${randomUUID()}`)
      : undefined

  // start tcp proxy (routes connections to correct instance by database name).
  // the native backend needs none: real postgres listens on pgPort itself.
  const pgServer = nativePg
    ? null
    : await startPgProxy(instances, config, pgStartupBarrier)

  const hookConnectionString = (dbName: string): string => {
    const connectionString = getConnectionString(config, dbName)
    if (!pgStartupBarrier) return connectionString
    const url = new URL(connectionString)
    url.searchParams.set('application_name', pgStartupBarrier.applicationName)
    return url.toString()
  }

  try {
    if (migrationsApplied > 0)
      log.orez(
        `${migrationsApplied} migration${migrationsApplied === 1 ? '' : 's'} applied`
      )

    // seed data if needed
    await seedIfNeeded(db, config)

    // run on-db-ready hook (e.g. migrations) before zero-cache starts
    if (config.onDbReady) {
      const upstreamUrl = hookConnectionString('postgres')
      const cvrUrl = hookConnectionString('zero_cvr')
      const cdbUrl = hookConnectionString('zero_cdb')
      await runHook(
        config.onDbReady,
        'on-db-ready',
        {
          ZERO_UPSTREAM_DB: upstreamUrl,
          ZERO_CVR_DB: cvrUrl,
          ZERO_CHANGE_DB: cdbUrl,
          DATABASE_URL: upstreamUrl,
          OREZ_PG_PORT: String(config.pgPort),
          // libpq-compatible clients use this as application_name. Keep it in
          // addition to the URL parameter because some runtimes (notably Bun's
          // node-postgres compatibility path) can discard URI-only startup
          // parameters while crossing a shell hook.
          ...(pgStartupBarrier ? { PGAPPNAME: pgStartupBarrier.applicationName } : {}),
        },
        buildHookContext(config, hookConnectionString, pgStartupBarrier?.applicationName)
      )

      // re-sync publication membership
      await syncManagedPublications(db, managedPub.names, managedPub.managedByOrez)
      await ensurePublicationHasTables(db, managedPub.names)
      if (!nativePg) {
        log.debug.orez('re-installing change tracking after on-db-ready')
        await installChangeTracking(db)
      }
      if (!isDoBackend && pgliteVacuumMs > 0) {
        await vacuumPGliteChurnTables(instances)
      }
    }

    if (isDoBackend) {
      await installChangeTracking(db)
    }
    pgStartupBarrier?.release()
  } catch (error) {
    // Release waiting sockets with the initialization error instead of leaving
    // them parked until the process exits.
    pgStartupBarrier?.fail(error)
    throw error
  }

  // write the ready marker so external orchestrators (e.g. CI scripts that
  // currently `wait:ports 6434`) can wait for orez to be fully initialized.
  // The pg port is bound earlier so on-db-ready can connect. Shell hooks use a
  // tagged connection that bypasses pgStartupBarrier; ordinary clients are
  // released only after the initialization above completes.
  writeFileSync(readyFile, String(Date.now()))

  // create read replicas after the primary is fully initialized
  // (migrations, seed, change tracking, publications all set up)
  if (!nativePg && config.readReplicas > 0 && config.useWorkerThreads) {
    const { createReadReplicas } = await import('./pglite-manager.js')
    instances.postgresReplicas = await createReadReplicas(db, config.readReplicas, config)
  }

  // clean up stale lock files from previous crash. if lock files were present,
  // the previous shutdown was unclean and the replica file may not be safe to
  // reuse, but wiping CVR/CDB here evicts every persisted client with
  // ClientNotFound. rebuild only the replica; true CDC/CVR corruption is handled
  // by the crash classifiers below.
  const hadStaleLocks = cleanupStaleLockFiles(config)
  if (hadStaleLocks) {
    log.debug.orez('unclean shutdown detected, rebuilding replica (CVR preserved)')
    cleanupStaleReplica(config)
    resetReplicationState()
  }
  if (!config.skipZeroCache) {
    const replicaResetReason = getZeroReplicaStartupResetReason(getReplicaDir(config))
    if (replicaResetReason) {
      log.orez(
        `detected invalid zero replica (${replicaResetReason}), rebuilding replica (CVR preserved)`
      )
      cleanupStaleReplica(config)
      resetReplicationState()
    }
  }

  // when admin is enabled, zero-cache runs on internal port with http proxy in front
  let zeroInternalPort = config.zeroPort
  let httpProxyServer: import('node:net').Server | null = null
  if (httpLog && !config.skipZeroCache) {
    zeroInternalPort = await findPort(config.zeroPort + 1000)
    log.debug.orez(`http proxy: public ${config.zeroPort} → internal ${zeroInternalPort}`)
  }

  // start zero-cache
  let zeroCacheProcess: ChildProcess | null = null
  let zeroEnv: Record<string, string> = {}
  let zeroStopExpected = false
  let requestZeroStateRecovery:
    | ((details: string, source: 'startup' | 'live-log') => void)
    | undefined
  if (!config.skipZeroCache) {
    // use internal port when http proxy is enabled
    const zeroConfig = httpLog ? { ...config, zeroPort: zeroInternalPort } : config

    // helper to start zero-cache and wait for it (including stability check)
    const tryStartZeroCache = async () => {
      // A previous orez generation can leave zero-cache postgres backends alive
      // even though no zero-cache process tree remains. Sweep before the first
      // launch too; otherwise the first change-streamer can spend its entire
      // schema-migration attempt blocked behind stale zero-* sessions and hit
      // Postgres lock_timeout before the retry path gets a chance to clean up.
      if (nativePg) await nativePg.terminateZeroBackends()

      const result = await startZeroCache(
        zeroConfig,
        logStore,
        sqliteMode,
        sqliteModeConfig,
        (details) => requestZeroStateRecovery?.(details, 'live-log')
      )
      zeroCacheProcess = result.process
      zeroEnv = result.env
      await waitForZeroCache(zeroConfig, zeroCacheProcess, 60000, sqliteMode)

      // stability check: wait a bit to catch early crashes (e.g. change-streamer)
      // zero-cache can pass health check but crash shortly after when workers start
      await new Promise((r) => setTimeout(r, 2000))
      if (zeroCacheProcess.exitCode !== null) {
        const tail = (zeroCacheProcess as ZeroChildProcess).__orezTail
        const details = tail?.length ? tail.slice(-20).join('\n') : ''
        throw new Error(`zero-cache crashed during startup stability check\n${details}`)
      }
      if (!isDoBackend && pgliteVacuumMs > 0) {
        await vacuumPGliteChurnTables(instances)
        queuePgliteVacuum()
      }
    }

    // zero-cache can die during startup for a few distinct reasons; the policy
    // for each lives in classifyZeroStartupRecovery (pure + unit-tested) and
    // this loop just executes the chosen action. the common case is a transient
    // crash: the change-streamer worker dies mid initial-sync (a dropped proxy
    // connection or a query timeout under load), exits 255, and cascades
    // zero-cache to a graceful "exited with code 0". the on-disk state is NOT
    // corrupt, so a plain relaunch re-runs the sync and almost always succeeds —
    // exactly what a user means by "it works if I just restart it once", and
    // the same call the post-startup crash watcher already makes
    // (classifyZeroCrashRecovery → 'restart'). this used to be fatal only
    // because the initial start wasn't wired into that restart path.
    const startupRetry: ZeroStartupRetryState = {
      plainRestarts: 0,
      maxRestarts: 3,
      didRecoverState: false,
      didCacheReset: false,
      canWasmFallback: sqliteMode === 'native' && !config.disableWasmSqlite,
      didWasmFallback: false,
      nativeBinaryMissing: false,
    }
    for (;;) {
      try {
        await tryStartZeroCache()
        break
      } catch (err: any) {
        const errMsg = err?.message || String(err)
        const firstLine = errMsg.split('\n')[0]

        // make sure the crashed (or, on a health-check timeout, still-hung)
        // zero-cache tree is fully gone before relaunching so the next process
        // can't collide on the replica file or zeroPort. no-op when the tree
        // already exited (the usual crash-cascade case).
        if (isChildProcessRunning(zeroCacheProcess)) {
          await terminateChildProcessTree(zeroCacheProcess, {
            gracefulSignal: 'SIGKILL',
            forceSignal: 'SIGKILL',
            graceMs: 1000,
            forceGraceMs: 1000,
          }).catch(() => {})
        }
        // even when the tree already exited (the usual crash cascade), its
        // postgres backends can linger and hold the replication slot / open
        // txns that make the next initial sync's CREATE_REPLICATION_SLOT stall
        // and hit its lock_timeout (55P03). sweep them before relaunching so a
        // transient crash doesn't turn into a self-perpetuating restart loop.
        if (nativePg) await nativePg.terminateZeroBackends().catch(() => {})

        startupRetry.nativeBinaryMissing = hasMissingNativeBinarySignature(errMsg)
        const { action, reason } = classifyZeroStartupRecovery(errMsg, startupRetry)

        // give up on deterministic failures (corruption that survived a reset,
        // a missing native binary with no wasm fallback). surface the original
        // error so `bun dev` exits fast instead of churning through retries.
        if (action === 'give-up') throw err

        if (action === 'recover-state') {
          startupRetry.didRecoverState = true
          log.orez(`zero-cache startup failed (${reason}) — resetting zero state...`)
          await recoverZeroState({ config, instances, zeroCacheProcess, nativePg })
          continue
        }

        if (action === 'wasm-fallback') {
          startupRetry.didWasmFallback = true
          startupRetry.canWasmFallback = false
          log.orez('native sqlite failed to load, falling back to wasm...')
          sqliteMode = 'wasm'
          sqliteModeConfig = resolveSqliteModeConfig(false, true) // force wasm
          continue
        }

        if (action === 'cache-reset') {
          startupRetry.didCacheReset = true
          log.orez(
            'zero-cache still crashing after restarts — rebuilding replica and retrying once more (CVR preserved)...'
          )
          cleanupStaleReplica(config)
          resetReplicationState()
          continue
        }

        // action === 'restart': transient / unexpected crash, plain relaunch.
        startupRetry.plainRestarts++
        log.orez(
          `zero-cache crashed during startup (restart ${startupRetry.plainRestarts}/${startupRetry.maxRestarts}): ${firstLine}`
        )
      }
    }

    // surface that a retry was needed (and worked) so a flaky startup is
    // visible in logs rather than silently swallowed.
    if (
      startupRetry.plainRestarts > 0 ||
      startupRetry.didRecoverState ||
      startupRetry.didCacheReset ||
      startupRetry.didWasmFallback
    ) {
      log.orez('zero-cache started after recovery')
    }

    // start http proxy in front of zero-cache when admin is enabled
    // also exposes read-only /__orez/api/logs and /__orez/api/status
    if (httpLog) {
      httpProxyServer = await startHttpProxy({
        listenPort: config.zeroPort,
        targetPort: zeroInternalPort,
        httpLog,
        logStore,
        config,
        startTime: Date.now(),
      })
      log.debug.orez(`http proxy listening on ${config.zeroPort}`)
    }

    log.zero(`ready ${port(config.zeroPort, 'magenta')} (sqlite: ${sqliteMode})`)
  } else {
    log.orez('skip zero-cache')
  }

  // run on-healthy hook after all services are ready. the startup barrier is
  // long released by now, so the callback gets plain (untagged) connections.
  if (config.onHealthy) {
    await runHook(
      config.onHealthy,
      'on-healthy',
      {
        OREZ_PG_PORT: String(config.pgPort),
        OREZ_ZERO_PORT: String(config.zeroPort),
      },
      buildHookContext(config, (dbName) => getConnectionString(config, dbName))
    )
  }

  const killZeroCache = async () => {
    const child = zeroCacheProcess
    if (isChildProcessRunning(child)) {
      zeroStopExpected = true
      try {
        const exited = await terminateChildProcessTree(child, {
          gracefulSignal: 'SIGTERM',
          forceSignal: 'SIGKILL',
          graceMs: 5000,
          forceGraceMs: 1000,
        })
        if (!exited) {
          log.debug.orez(`zero-cache pid ${child.pid} did not exit after SIGKILL`)
        }
      } finally {
        zeroStopExpected = false
      }
    }
    // the process tree is gone now, but on the native backend its postgres
    // backends can linger (a walsender waiting out wal_sender_timeout, a backend
    // blocked mid slot-snapshot) and hold the replication slot / open txns that
    // stall the next zero-cache's initial sync. always sweep them — the crash
    // path arrives here with the child already exited but the orphans still up.
    if (nativePg) await nativePg.terminateZeroBackends()
  }

  // explicit process restart for admin use. unexpected crashes use full state
  // reset below because zero-cache's CVR/change/replica state is coupled.
  const restartZeroCache = async () => {
    await killZeroCache()
    // use internal port when http proxy is enabled
    const zeroConfig = httpLog ? { ...config, zeroPort: zeroInternalPort } : config
    const result = await startZeroCache(
      zeroConfig,
      logStore,
      sqliteMode,
      sqliteModeConfig,
      (details) => requestZeroStateRecovery?.(details, 'live-log')
    )
    zeroCacheProcess = result.process
    zeroEnv = result.env
    await waitForZeroCache(zeroConfig, zeroCacheProcess, 60000, sqliteMode)
  }

  // unified reset function for zero state
  // modes:
  //   'cache-only' - deletes replica file only (fast, for minor sync issues)
  //   'full' - deletes CVR/CDB + replica and recreates instances (for schema changes)
  let shuttingDown = false
  let resetInProgress = false
  let liveLogRecoveryQueued = false
  let zeroHttpHealthRecovering = false
  // when we last took the gentle (CVR-preserving) 'cache-only' path for a
  // recoverable inconsistency. a repeat within the window means cache-only didn't
  // resolve it, so escalate to a full reset (see chooseInconsistencyResetMode).
  let lastCacheOnlyResetAt = 0
  const ZERO_INCONSISTENCY_ESCALATE_WINDOW_MS = 5 * 60_000
  const resetFile = resolve(config.dataDir, 'orez.resetting')
  const resetZeroState = async (mode: 'cache-only' | 'full'): Promise<void> => {
    if (resetInProgress) {
      log.orez('reset already in progress, skipping')
      return
    }
    resetInProgress = true
    // write marker file so pg_restore can wait for reset to complete
    writeFileSync(resetFile, String(Date.now()))

    try {
      log.orez(`resetting zero state (${mode})...`)

      // stop zero-cache first
      log.orez('stopping zero-cache...')
      await killZeroCache()
      log.orez('zero-cache stopped')

      // pglite is one shared session: a consumer killed mid-transaction leaves
      // it in the aborted (25P02) state, where every later statement — the
      // TRUNCATEs below, or the fresh zero-cache's own schema migration — fails
      // with "current transaction is aborted". clear it before recovery work.
      if (!nativePg) {
        await db.exec('ROLLBACK').catch(() => {})
      }

      if (mode === 'full' && nativePg) {
        await nativePg.resetZeroDatabases()
      } else if (mode === 'full') {
        // give connections time to drain before closing instances
        await new Promise((r) => setTimeout(r, 500))

        // close CVR/CDB instances
        log.orez('closing CVR/CDB...')
        await instances.cvr.close().catch((e: any) => {
          log.debug.orez(`cvr close error (expected): ${e?.message || e}`)
        })
        await instances.cdb.close().catch((e: any) => {
          log.debug.orez(`cdb close error (expected): ${e?.message || e}`)
        })
        log.orez('CVR/CDB closed')

        // delete CVR/CDB data directories
        log.orez('deleting CVR/CDB data...')
        for (const dir of ['pgdata-cvr', 'pgdata-cdb']) {
          try {
            rmSync(resolve(config.dataDir, dir), { recursive: true, force: true })
          } catch {}
        }

        // recreate CVR/CDB instances
        log.orez('recreating CVR/CDB...')
        const cvrDataDir = pgliteDataDirFor(config, 'cvr')
        const cdbDataDir = pgliteDataDirFor(config, 'cdb')
        if (config.useWorkerThreads) {
          const cvrProxy = createPGliteWorker(cvrDataDir, 'cvr')
          const cdbProxy = createPGliteWorker(cdbDataDir, 'cdb')
          await Promise.all([cvrProxy.waitReady, cdbProxy.waitReady])
          instances.cvr = cvrProxy as unknown as PGlite
          instances.cdb = cdbProxy as unknown as PGlite
        } else {
          const { PGlite: PGliteCtor } = await import('@electric-sql/pglite')
          if (!config.ephemeral) {
            mkdirSync(cvrDataDir, { recursive: true })
            mkdirSync(cdbDataDir, { recursive: true })
          }
          instances.cvr = new PGliteCtor({
            dataDir: cvrDataDir,
            relaxedDurability: true,
          })
          instances.cdb = new PGliteCtor({
            dataDir: cdbDataDir,
            relaxedDurability: true,
          })
          await instances.cvr.waitReady
          await instances.cdb.waitReady
        }
        log.orez('CVR/CDB recreated')

        // give the proxy fresh per-instance state for the recreated CVR/CDB.
        // a client connection (e.g. a connected frontend's syncer) can be
        // mid-query on the old instance when it's closed here, hanging while it
        // holds that instance's proxy mutex; without this the next zero-cache's
        // CVR/CDB connections would block on the stuck mutex and never become
        // ready (the SIGUSR1 reset would then fail with "exited with code 0").
        pgServer?.resetDbState('zero_cvr')
        pgServer?.resetDbState('zero_cdb')

        // remove stale zero shard schemas from upstream; these can outlive CVR/CDB
        // and cause dispatcher errors after full reset.
        const shardSchemas = await db.query(
          `SELECT DISTINCT schemaname
           FROM pg_tables
           WHERE tablename IN ('clients', 'replicas', 'mutations')
             AND schemaname NOT IN (
               'pg_catalog',
               'information_schema',
               'pg_toast',
               'public',
               '_orez'
             )
             AND schemaname NOT LIKE 'pg_%'`
        )
        for (const { schemaname } of shardSchemas.rows) {
          const quoted = '"' + schemaname.replace(/"/g, '""') + '"'
          await db.exec(`DROP SCHEMA IF EXISTS ${quoted} CASCADE`)
        }
        if (shardSchemas.rows.length > 0) {
          log.orez(`dropped ${shardSchemas.rows.length} stale shard schema(s)`)
        }

        // clear upstream replication tracking so zero-cache starts from a
        // clean change stream baseline after full reset.
        await db.exec(`TRUNCATE _orez._zero_changes`).catch(() => {})
        await db.exec(`TRUNCATE _orez._zero_replication_slots`).catch(() => {})
        await db
          .exec(`ALTER SEQUENCE _orez._zero_watermark RESTART WITH 1`)
          .catch(() => {})
        log.orez('cleared upstream replication tracking state')
      }

      // clear cached schema info so the handler re-introspects on reconnect
      resetReplicationState()

      // always clean up replica file
      cleanupStaleReplica(config)
      log.orez('replica cleaned up')

      // re-run on-db-ready hook after full reset (re-runs migrations, syncs publication)
      if (mode === 'full' && config.onDbReady) {
        log.orez('re-running on-db-ready...')
        const upstreamUrl = getConnectionString(config, 'postgres')
        const cvrUrl = getConnectionString(config, 'zero_cvr')
        const cdbUrl = getConnectionString(config, 'zero_cdb')
        await runHook(
          config.onDbReady,
          'on-db-ready',
          {
            ZERO_UPSTREAM_DB: upstreamUrl,
            ZERO_CVR_DB: cvrUrl,
            ZERO_CHANGE_DB: cdbUrl,
            DATABASE_URL: upstreamUrl,
            OREZ_PG_PORT: String(config.pgPort),
          },
          buildHookContext(config, (dbName) => getConnectionString(config, dbName))
        )
      }

      // always re-install change tracking after a full reset so public table
      // triggers reflect any schema changes introduced by restore.
      await syncManagedPublications(db, managedPub.names, managedPub.managedByOrez)
      if (!nativePg) {
        log.debug.orez('re-installing change tracking after full reset')
        await installChangeTracking(db)
      }

      // restart zero-cache. the initial sync after a reset can hit a
      // transient failure (e.g. a statement aborting the change-streamer's
      // schema-sync transaction while post-reset state settles); the crash
      // watcher deliberately ignores exits while resetInProgress, so without
      // a retry here one bad boot leaves zero-cache down permanently.
      const zeroConfig = httpLog ? { ...config, zeroPort: zeroInternalPort } : config
      const RESET_START_ATTEMPTS = 3
      for (let attempt = 1; ; attempt++) {
        log.orez(
          `starting zero-cache...${attempt > 1 ? ` (attempt ${attempt}/${RESET_START_ATTEMPTS})` : ''}`
        )
        const result = await startZeroCache(
          zeroConfig,
          logStore,
          sqliteMode,
          sqliteModeConfig,
          (details) => requestZeroStateRecovery?.(details, 'live-log')
        )
        zeroCacheProcess = result.process
        zeroEnv = result.env
        try {
          await waitForZeroCache(zeroConfig, zeroCacheProcess, 60000, sqliteMode)
          break
        } catch (err: any) {
          if (attempt >= RESET_START_ATTEMPTS) throw err
          log.orez(
            `zero-cache failed to come up after reset (${err?.message || err}), retrying`
          )
          // kill a still-running half-booted instance (waitForZeroCache can
          // time out with the process alive) and sweep lingering native
          // backends that would stall the next initial sync
          await killZeroCache()
          // a failed boot can leave a partially initial-synced replica behind
          cleanupStaleReplica(config)
        }
      }
      // a completed reset is durable progress — the change log was cut, so the
      // bankruptcy monitor must not re-fire on the pre-reset stall window.
      markReplicationProgress()
      log.orez(`zero state reset complete (${mode})`)
      log.zero(`ready ${port(config.zeroPort, 'magenta')}`)
    } catch (err: any) {
      log.orez(`reset failed: ${err?.message || err}`)
      throw err
    } finally {
      resetInProgress = false
      // remove marker file so pg_restore knows we're done
      try {
        unlinkSync(resetFile)
      } catch {}
    }
  }

  // pick the reset mode for a recoverable inconsistency and remember when we last
  // used the gentle (CVR-preserving) cache-only path, so a repeat within the
  // window escalates to a full reset instead of looping on cache-only.
  const chooseInconsistencyResetMode = (details: string): 'cache-only' | 'full' => {
    const cacheResetExhausted =
      Date.now() - lastCacheOnlyResetAt < ZERO_INCONSISTENCY_ESCALATE_WINDOW_MS
    const mode = zeroInconsistencyResetMode(details, { cacheResetExhausted })
    if (mode === 'cache-only') lastCacheOnlyResetAt = Date.now()
    return mode
  }

  requestZeroStateRecovery = (details, source) => {
    if (shuttingDown || resetInProgress || liveLogRecoveryQueued) return
    liveLogRecoveryQueued = true
    const mode = chooseInconsistencyResetMode(details)
    log.orez(
      `zero-cache state inconsistency detected from ${source}, ` +
        (mode === 'cache-only'
          ? 'rebuilding replica (CVR preserved)'
          : 'resetting zero state (full)')
    )
    queueMicrotask(() => {
      resetZeroState(mode)
        .then(() => {
          log.orez('zero-cache live state recovery successful')
        })
        .catch((err) => {
          log.orez(`zero-cache live state recovery failed: ${err?.message || err}`)
        })
        .finally(() => {
          liveLogRecoveryQueued = false
        })
    })
  }

  // handle SIGUSR1 to reset zero state (sent by pg_restore after restore completes)
  if (!config.skipZeroCache) {
    process.on('SIGUSR1', () => {
      log.orez('received SIGUSR1 - full reset')
      resetZeroState('full').catch((err) => {
        log.orez(`SIGUSR1 reset failed: ${err?.message || err}`)
      })
    })

    // handle SIGUSR2 to quiesce zero-cache (sent by pg_restore before restore starts)
    process.on('SIGUSR2', () => {
      log.orez('received SIGUSR2 - stopping zero-cache for restore')
      killZeroCache().catch((err) => {
        log.orez(`SIGUSR2 stop failed: ${err?.message || err}`)
      })
    })
  }

  // auto-recover when zero-cache exits unexpectedly. only a crash that actually
  // corrupts or desyncs local zero state (replica / CVR DB / change DB) warrants
  // a `full` reset, which deletes the CVR DB and so severs every connected
  // client: the recreated CVR starts at the empty "00" version, the client's
  // persisted baseCookie is now ahead of it, and the view-syncer rejects the
  // reconnect with `ClientNotFound` (checkClientAndCVRVersions). a transient
  // crash — a change-streamer query timeout, an OOM kill, a stray unhandled
  // rejection — leaves the on-disk state valid, so a plain process restart
  // recovers and live clients reconnect against their existing baseCookie
  // with no error. wiping the CVR for those is the more damaging action.
  const ZERO_CRASH_WINDOW_MS = 5 * 60_000
  const ZERO_CRASH_RESET_BUDGET = 5
  let zeroCrashTimes: number[] = []
  const installCrashWatcher = () => {
    const watched = zeroCacheProcess
    if (!watched || config.skipZeroCache) return
    watched.on('exit', (code) => {
      // only react to the exit of the process we're *currently* managing. a
      // reset/restart may have already swapped in a replacement, and the old
      // process's 'exit' event can be delivered after that swap — zero 1.6's
      // graceful drain (slower while a frontend is connected) widens this
      // window. acting on a stale exit here would kill/restart the freshly
      // spawned process mid-startup (manifests as "reset failed: zero-cache
      // exited with code 0"). zeroStopExpected is cleared as soon as the kill
      // call returns, i.e. before this late event fires, so it can't be relied
      // on alone.
      if (watched !== zeroCacheProcess) return
      if (
        shuttingDown ||
        resetInProgress ||
        zeroStopExpected ||
        zeroHttpHealthRecovering ||
        code === null
      )
        return
      const tail = (zeroCacheProcess as ZeroChildProcess)?.__orezTail
      const details = tail?.length ? tail.join('\n') : ''

      // bounded by a sliding-window budget so a genuinely broken instance
      // cannot reset/restart-loop forever.
      zeroCrashTimes = zeroCrashTimes.filter((t) => Date.now() - t < ZERO_CRASH_WINDOW_MS)
      zeroCrashTimes.push(Date.now())

      if (zeroCrashTimes.length > ZERO_CRASH_RESET_BUDGET) {
        log.orez('zero-cache kept crashing after repeated recoveries — giving up')
        return
      }

      // a full reset is only correct for crashes that leave local zero state
      // inconsistent/corrupt. everything else (transient faults AND plain
      // unexpected exits) gets a restart that preserves the CVR, so connected
      // clients are not force-evicted with ClientNotFound.
      const { action, reason } = classifyZeroCrashRecovery(details)

      if (action === 'full-reset') {
        // a replica-vs-CVR desync (the common case) rebuilds only the replica and
        // keeps the CVR, so connected clients are not evicted; only true CDC
        // corruption — or a desync that a prior cache-only didn't fix — escalates
        // to the client-evicting full reset. See zeroInconsistencyResetMode.
        const mode = chooseInconsistencyResetMode(details)
        log.orez(
          `zero-cache ${reason} (code ${code}) — ` +
            (mode === 'cache-only'
              ? 'rebuilding replica (CVR preserved)'
              : 'resetting zero state (full)') +
            ` (${zeroCrashTimes.length}/${ZERO_CRASH_RESET_BUDGET})`
        )
        resetZeroState(mode)
          .then(() => {
            log.orez(`zero-cache ${mode} recovery successful`)
            installCrashWatcher()
          })
          .catch((err) => {
            log.orez(`zero-cache ${mode} recovery failed: ${err?.message || err}`)
          })
        return
      }

      log.orez(
        `zero-cache ${reason} (code ${code}) — restarting zero-cache ` +
          `(${zeroCrashTimes.length}/${ZERO_CRASH_RESET_BUDGET}, CVR preserved)`
      )
      restartZeroCache()
        .then(() => {
          log.orez('zero-cache restart recovery successful')
          installCrashWatcher()
        })
        .catch((err) => {
          log.orez(
            `zero-cache restart recovery failed (${err?.message || err}) — rebuilding replica (CVR preserved)`
          )
          resetZeroState('cache-only')
            .then(() => {
              log.orez('zero-cache replica rebuild recovery successful')
              installCrashWatcher()
            })
            .catch((resetErr) => {
              log.orez(
                `zero-cache replica rebuild recovery failed: ${resetErr?.message || resetErr}`
              )
            })
        })
    })
  }
  installCrashWatcher()

  const ZERO_HTTP_HEALTH_INTERVAL_MS = 5000
  const ZERO_HTTP_HEALTH_TIMEOUT_MS = 1000
  const ZERO_HTTP_HEALTH_FAILURES_BEFORE_RECOVERY = 3
  const ZERO_HTTP_HEALTH_WINDOW_MS = 5 * 60_000
  const ZERO_HTTP_HEALTH_RECOVERY_BUDGET = 5
  let zeroHttpHealthFailures = 0
  let zeroHttpHealthProbeInFlight = false
  let zeroHttpHealthRecoveryTimes: number[] = []
  let zeroHttpHealthTimer: ReturnType<typeof setInterval> | undefined

  const runZeroHttpHealthProbe = async () => {
    if (
      config.skipZeroCache ||
      zeroHttpHealthProbeInFlight ||
      zeroHttpHealthRecovering ||
      shuttingDown ||
      resetInProgress ||
      zeroStopExpected ||
      liveLogRecoveryQueued
    )
      return

    const watched = zeroCacheProcess
    if (!isChildProcessRunning(watched)) {
      zeroHttpHealthFailures = 0
      return
    }

    zeroHttpHealthProbeInFlight = true
    try {
      const result = await probeZeroCacheHttp(
        zeroInternalPort,
        ZERO_HTTP_HEALTH_TIMEOUT_MS
      )
      if (watched !== zeroCacheProcess) return
      if (shuttingDown || resetInProgress || zeroStopExpected || liveLogRecoveryQueued)
        return

      if (result.ok) {
        zeroHttpHealthFailures = 0
        return
      }

      zeroHttpHealthFailures++
      log.debug.orez(
        `zero-cache HTTP health probe failed ` +
          `(${zeroHttpHealthFailures}/${ZERO_HTTP_HEALTH_FAILURES_BEFORE_RECOVERY}): ` +
          result.reason
      )
      if (zeroHttpHealthFailures < ZERO_HTTP_HEALTH_FAILURES_BEFORE_RECOVERY) {
        return
      }
      zeroHttpHealthFailures = 0

      const now = Date.now()
      zeroHttpHealthRecoveryTimes = zeroHttpHealthRecoveryTimes.filter(
        (t) => now - t < ZERO_HTTP_HEALTH_WINDOW_MS
      )
      zeroHttpHealthRecoveryTimes.push(now)
      if (zeroHttpHealthRecoveryTimes.length > ZERO_HTTP_HEALTH_RECOVERY_BUDGET) {
        log.orez(
          'zero-cache HTTP liveness kept failing after repeated recoveries — giving up'
        )
        return
      }

      zeroHttpHealthRecovering = true
      const attempt = zeroHttpHealthRecoveryTimes.length
      log.orez(
        `zero-cache HTTP liveness failed (${result.reason}) — restarting zero-cache ` +
          `(${attempt}/${ZERO_HTTP_HEALTH_RECOVERY_BUDGET}, CVR preserved)`
      )
      try {
        await restartZeroCache()
        log.orez('zero-cache HTTP liveness recovery successful')
        installCrashWatcher()
      } catch (err: any) {
        log.orez(
          `zero-cache HTTP liveness restart failed (${err?.message || err}) — rebuilding replica (CVR preserved)`
        )
        try {
          await resetZeroState('cache-only')
          log.orez('zero-cache HTTP liveness replica rebuild recovery successful')
          installCrashWatcher()
        } catch (resetErr: any) {
          log.orez(
            `zero-cache HTTP liveness replica rebuild recovery failed: ${
              resetErr?.message || resetErr
            }`
          )
        }
      } finally {
        zeroHttpHealthRecovering = false
      }
    } finally {
      zeroHttpHealthProbeInFlight = false
    }
  }

  if (!config.skipZeroCache) {
    zeroHttpHealthTimer = setInterval(() => {
      void runZeroHttpHealthProbe()
    }, ZERO_HTTP_HEALTH_INTERVAL_MS)
    zeroHttpHealthTimer.unref?.()
  }

  // ── replication bankruptcy monitor ─────────────────────────────────────────
  // past a point, a change-log backlog can NEVER be drained by replay: the
  // catch-up work gets slower as the log grows, the consumer dies before
  // confirming, nothing is purged, and the next attempt faces a bigger log
  // (2026-07-03: 118k rows, hours of restart-looping). detect that state from
  // in-memory gauges only — no db access, so a wedged mutex can't blind the
  // monitor — and take the one converging action: a full reset, which
  // truncates the log and re-snapshots the base tables in seconds.
  const bankruptcyStallMs = (() => {
    const raw = process.env.OREZ_REPLICATION_BANKRUPTCY_STALL_MS
    if (!raw) return 5 * 60_000
    const parsed = Number(raw)
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : 5 * 60_000
  })()
  let bankruptcyBaselineAt = Date.now()
  let bankruptcyTimer: ReturnType<typeof setInterval> | undefined
  // native backend uses zero-cache's real change source — orez's replication
  // gauges never move there, and there is no replay doom loop to detect.
  if (!config.skipZeroCache && bankruptcyStallMs > 0 && !nativePg) {
    bankruptcyTimer = setInterval(() => {
      if (shuttingDown || resetInProgress || zeroStopExpected || liveLogRecoveryQueued)
        return
      const verdict = isReplicationBankrupt(
        getReplicationHealth(),
        Date.now(),
        bankruptcyStallMs,
        bankruptcyBaselineAt
      )
      if (!verdict.bankrupt) return
      bankruptcyBaselineAt = Date.now()
      log.orez(
        `replication pipeline bankrupt: ${verdict.reason} — ` +
          `resetting zero state (full) to re-snapshot instead of replaying`
      )
      resetZeroState('full').catch((err) => {
        log.orez(`bankruptcy reset failed: ${err?.message || err}`)
      })
    }, 30_000)
    bankruptcyTimer.unref?.()
  }

  const stop = async () => {
    log.debug.orez('shutting down')
    shuttingDown = true
    if (zeroHttpHealthTimer) clearInterval(zeroHttpHealthTimer)
    if (bankruptcyTimer) clearInterval(bankruptcyTimer)
    if (delayedVacuumTimer) clearTimeout(delayedVacuumTimer)
    stopCheckpoint()
    stopVacuum()
    httpProxyServer?.close()
    await killZeroCache()
    pgServer?.close()
    await Promise.all([
      instances.postgres.close(),
      instances.cvr.close(),
      instances.cdb.close(),
    ])
    if (nativePg) {
      await nativePg.stop()
    }
    try {
      unlinkSync(pidFile)
    } catch {}
    try {
      unlinkSync(adminFile)
    } catch {}
    try {
      unlinkSync(readyFile)
    } catch {}
    if (config.ephemeralDir) {
      try {
        rmSync(config.ephemeralDir, { recursive: true, force: true })
      } catch {}
    }
    log.debug.orez('stopped')
  }

  return {
    config,
    stop,
    db,
    instances,
    pgPort: config.pgPort,
    zeroPort: config.zeroPort,
    logStore,
    httpLog,
    zeroEnv,
    restartZero: config.skipZeroCache ? undefined : restartZeroCache,
    // stop zero-cache without restart (for pg_restore to safely modify schema)
    stopZero: config.skipZeroCache ? undefined : killZeroCache,
    // cache-only reset: just replica file (fast, for minor sync issues)
    resetZero: config.skipZeroCache ? undefined : () => resetZeroState('cache-only'),
    // full reset: CVR/CDB + replica (for schema changes, used by pg_restore via SIGUSR1)
    resetZeroFull: config.skipZeroCache ? undefined : () => resetZeroState('full'),
  }
}

/** clean lock files only — keeps replica intact for fast incremental sync on restart.
 *  returns true if any stale lock files were found (indicates unclean shutdown). */
function cleanupStaleLockFiles(config: ZeroLiteConfig): boolean {
  const replicaPath = zeroReplicaPath(config)
  let found = false
  for (const suffix of ['-wal', '-shm', '-wal2']) {
    const file = replicaPath + suffix
    try {
      if (existsSync(file)) {
        unlinkSync(file)
        log.debug.orez(`cleaned up stale ${suffix} file`)
        found = true
      }
    } catch {}
  }
  return found
}

/** delete replica + all lock/wal files — forces zero-cache to do a full resync */
function cleanupStaleReplica(config: ZeroLiteConfig): void {
  const replicaPath = zeroReplicaPath(config)
  for (const suffix of ['', '-wal', '-shm', '-wal2']) {
    const file = replicaPath + suffix
    try {
      if (existsSync(file)) {
        unlinkSync(file)
        if (suffix) log.debug.orez(`cleaned up stale ${suffix} file`)
        else log.debug.orez('cleaned up stale replica (will re-sync)')
      }
    } catch {}
  }
}

function cleanupZeroCachePreload(preloadPath: string | undefined): void {
  if (!preloadPath) return
  try {
    unlinkSync(preloadPath)
  } catch {}
}

function writeZeroCachePreload(parentPid: number, zeroTitle: string): string {
  const preloadDir = resolve(tmpdir(), 'orez-zero-cache-preload')
  mkdirSync(preloadDir, { recursive: true })
  const preloadPath = resolve(preloadDir, `parent-${parentPid}.cjs`)
  writeFileSync(
    preloadPath,
    `const fs = require('node:fs');\n` +
      `process.title = ${JSON.stringify(zeroTitle)};\n` +
      `const __orezPid = ${parentPid};\n` +
      `const __orezExit = () => {\n` +
      `  try { fs.unlinkSync(__filename); } catch {}\n` +
      `  process.exit(0);\n` +
      `};\n` +
      `setInterval(() => {\n` +
      `  try { process.kill(__orezPid, 0); } catch { __orezExit(); }\n` +
      `}, 1000).unref();\n`
  )
  return preloadPath
}

async function seedIfNeeded(db: PGlite, config: ZeroLiteConfig): Promise<void> {
  // check if we already have data
  try {
    const result = await db.query<{ count: string }>(
      'SELECT count(*) as count FROM public."user"'
    )
    if (Number(result.rows[0].count) > 0) {
      return
    }
  } catch {
    // table might not exist yet
  }

  log.debug.orez('seeding demo data')
  const seedFile = resolve(config.seedFile)
  if (!existsSync(seedFile)) {
    log.debug.orez('no seed file found, skipping')
    return
  }

  const sql = readFileSync(seedFile, 'utf-8')
  const statements = sql
    .split('--> statement-breakpoint')
    .map((s) => s.trim())
    .filter(Boolean)

  for (const stmt of statements) {
    await db.exec(stmt)
  }
  log.orez('seeded')
}

async function startZeroCache(
  config: ZeroLiteConfig,
  logStore?: LogStore,
  sqliteMode: SqliteMode = resolveSqliteMode(config.disableWasmSqlite),
  sqliteModeConfig?: SqliteModeConfig | null,
  onRecoverableZeroState?: (details: string) => void
): Promise<{ process: ChildProcess; env: Record<string, string> }> {
  // resolve @rocicorp/zero entry for finding zero-cache modules
  const zeroEntry = resolvePackage('@rocicorp/zero')

  if (!zeroEntry) {
    throw new Error('zero-cache not found. install @rocicorp/zero')
  }

  // orez owns the replica on disk and has no litestream backup; stop zero 1.5's
  // change-streamer from erroring + resyncing on every restart (see the patch).
  disableZeroLitestreamRestore()

  // zero-cache's changeLog cleanup can strand pending cleanup watermarks when
  // it runs before any subscriber is connected. Keep the cleanup timer alive so
  // local/ephemeral Orez instances do not accumulate a huge CDB catchup log.
  enableZeroChangeLogCleanupRetry()

  // litestream also checkpointed the replica WAL in stock zero-cache; with it gone,
  // nothing reclaims the wal2 (PASSIVE autocheckpoint can't pass the view-syncer's
  // held readers), so it grows unbounded and reads slow down. Inject a periodic
  // TRUNCATE checkpoint into the write worker's writable replica connection. This is
  // the NATIVE path; the sqlite shim (shim-template.ts) covers wasm mode.
  enableZeroReplicaCheckpoint()

  if (sqliteMode === 'native') {
    log.debug.orez('wasm sqlite disabled, using native @rocicorp/zero-sqlite3')
  }

  const upstreamUrl = getConnectionString(config, 'postgres')
  const cvrUrl = getConnectionString(config, 'zero_cvr')
  const cdbUrl = getConnectionString(config, 'zero_cdb')
  const replicaFile = zeroReplicaPath(config)

  // defaults that can be overridden by user env
  // when admin is enabled and user hasn't set ZERO_LOG_LEVEL, use 'info'
  // to avoid flooding stdout with debug logs (each line triggers log processing).
  // debug was too expensive — tens of thousands of lines per minute.
  const zeroLogLevel =
    config.adminPort > 0 && !process.env.ZERO_LOG_LEVEL ? 'info' : config.logLevel
  const defaults: Record<string, string> = {
    NODE_ENV: 'development',
    ZERO_LOG_LEVEL: zeroLogLevel,
    ZERO_NUM_SYNC_WORKERS: '1',
    // raise the change-streamer's change-log statement timeout well above its 20s
    // default (zero-config change.statementTimeoutMs). orez runs on single-threaded
    // WASM PGlite: under write load a change-log query can momentarily exceed 20s,
    // which aborts the change-streamer (exit 13) and cascades — restart → the
    // replica returns behind the preserved CVR (RowsVersionBehindError) → reset.
    // Letting the slow query finish instead of crashing avoids the whole cascade.
    // User-overridable via the real env var below.
    ZERO_CHANGE_STATEMENT_TIMEOUT_MS: '90000',
    // disable query planner — it relies on scanStatus which causes infinite
    // loops with wasm sqlite and has caused freezes with native too.
    // planner is an optimization, not required for correctness.
    ZERO_ENABLE_QUERY_PLANNER: 'false',
    // disable otel metrics export — zero-cache has built-in OTEL that tries
    // to export even without a collector, causing periodic Bad Request errors.
    // user can override by setting OTEL_SDK_DISABLED=false in their env.
    OTEL_SDK_DISABLED: 'true',
  }

  const env: Record<string, string> = {
    ...defaults,
    ...(process.env as Record<string, string>),
    // orez is a development tool — always run zero-cache in development mode
    // to avoid production requirements like --admin-password
    NODE_ENV: 'development',
    ZERO_UPSTREAM_DB: upstreamUrl,
    ZERO_CVR_DB: cvrUrl,
    ZERO_CHANGE_DB: cdbUrl,
    ZERO_REPLICA_FILE: replicaFile,
    ZERO_PORT: String(config.zeroPort),
    ...(config.zeroMutateUrl ? { ZERO_MUTATE_URL: config.zeroMutateUrl } : {}),
    ...(config.zeroQueryUrl ? { ZERO_QUERY_URL: config.zeroQueryUrl } : {}),
    // wasm sqlite SHM is file-backed but not as robust as native mmap —
    // force single sync worker to avoid multi-process SHM contention
    ...(sqliteMode === 'wasm' ? { ZERO_NUM_SYNC_WORKERS: '1' } : {}),
  }

  // high worker counts multiply the blast radius of any sync-worker bug
  // (e.g. orphaned workers busy-looping on EOF'd sibling pipes). dev rarely
  // benefits from more than a couple; warn so it's obvious where the CPU
  // went.
  const workerCount = Number(env.ZERO_NUM_SYNC_WORKERS)
  if (Number.isFinite(workerCount) && workerCount > 4) {
    log.orez(
      `warning: ZERO_NUM_SYNC_WORKERS=${workerCount} is high for development — each worker consumes CPU/memory and amplifies any sync-loop bug. consider 2.`
    )
  }

  const zeroCacheBin = resolve(zeroEntry, '..', 'cli.js')
  if (!existsSync(zeroCacheBin)) {
    throw new Error('zero-cache cli.js not found. install @rocicorp/zero')
  }

  // apply sqlite mode shim (wasm: patches lib/index.js, native: restores original)
  if (sqliteModeConfig) {
    const shimResult = applySqliteMode(sqliteModeConfig)
    if (!shimResult.success) {
      log.orez(`warning: sqlite shim failed: ${shimResult.error}`)
    }
  }

  // preload script to label the zero-cache child process AND self-destruct
  // if the orez parent dies. macOS has no PR_SET_PDEATHSIG, so on a hard
  // parent kill (SIGKILL) or a crash that skips the `stop()` path, zero-cache
  // workers get reparented to init and can busy-loop on EOF'd sibling pipes
  // at 100% CPU indefinitely. every forked zero-cache worker inherits
  // NODE_OPTIONS, so the --require below runs in each one; they independently
  // poll the captured orez pid and exit when it disappears.
  const zeroTitle = orezTitle('orez [zero]')
  const preloadPath = writeZeroCachePreload(process.pid, zeroTitle)

  const nodeOptions = [
    sqliteMode === 'wasm' ? '--max-old-space-size=16384' : '',
    `--require ${preloadPath}`,
    process.env.NODE_OPTIONS || '',
  ]
    .filter(Boolean)
    .join(' ')
  if (nodeOptions.trim()) env.NODE_OPTIONS = nodeOptions.trim()

  const nodeBinary = resolveNodeBinary()
  const child = spawn(nodeBinary, [zeroCacheBin], {
    env,
    // stdin piped (not 'ignore') so zero-cache's pipe fd to orez closes with
    // EOF on parent death — belt-and-suspenders alongside the ppid watchdog
    // in the --require preload above.
    stdio: ['pipe', 'pipe', 'pipe'],
  }) as ZeroChildProcess
  child.__orezPreloadPath = preloadPath
  child.__orezTail = []

  const pushTail = (line: string) => {
    const tail = child.__orezTail!
    tail.push(line)
    if (tail.length > 80) tail.splice(0, tail.length - 80)
    if (hasRecoverableZeroStateSignature(line)) {
      onRecoverableZeroState?.(line)
    }
  }

  // known transient errors during zero-cache startup — demote to debug
  const STARTUP_NOISE = [
    '_zero.tableMetadata',
    'Unable to create full ReplicationStatusEvent',
    'replication slot',
    'does not exist',
    'error dropping',
    'EPIPE',
    'socket has been ended by the other party',
    'ideal db ping time',
    'average ping to',
    // node.js warnings from stale replica timestamps causing negative setTimeout
    'TimeoutNegativeWarning',
    'does not allow a negative number',
    // otel metrics export noise when no collector is configured
    'PeriodicExportingMetricReader',
    'OTLPExporterError',
  ]
  const isStartupNoise = (line: string): boolean =>
    STARTUP_NOISE.some((pattern) => line.includes(pattern))

  // detect log level from zero-cache output
  const detectLevel = (line: string, fallback: string): string => {
    if (hasZeroReplicaMonitorWarmupSignature(line)) return 'debug'
    if (isStartupNoise(line)) return 'debug'
    const lower = line.toLowerCase()
    if (
      lower.includes('"level":"error"') ||
      lower.includes(' error ') ||
      lower.includes('error:')
    )
      return 'error'
    if (
      lower.includes('"level":"warn"') ||
      lower.includes(' warn ') ||
      lower.includes('warning:')
    )
      return 'warn'
    if (lower.includes('"level":"debug"') || lower.includes(' debug ')) return 'debug'
    return fallback
  }

  child.stdout?.on('data', (data: Buffer) => {
    const lines = data.toString().trim().split('\n')
    for (const line of lines) {
      pushTail(`stdout: ${line}`)
      const level = detectLevel(line, 'info')
      if (level === 'warn' || level === 'error') log.zero(line)
      else log.debug.zero(line)
      logStore?.push('zero', level, line)
    }
  })

  child.stderr?.on('data', (data: Buffer) => {
    const lines = data.toString().trim().split('\n')
    for (const line of lines) {
      pushTail(`stderr: ${line}`)
      const level = detectLevel(line, 'error')
      if (level === 'warn' || level === 'error') log.zero(line)
      else log.debug.zero(line)
      logStore?.push('zero', level, line)
    }
  })

  child.on('exit', (code, signal) => {
    cleanupZeroCachePreload(child.__orezPreloadPath)
    pushTail(code === null ? `exit: signal ${signal}` : `exit: code ${code}`)
    if (code !== 0 && code !== null) {
      log.zero(`exited with code ${code}`)
      logStore?.push('zero', 'error', `exited with code ${code}`)
    }
  })

  return { process: child, env }
}

async function waitForZeroCache(
  config: ZeroLiteConfig,
  zeroProcess?: ChildProcess | null,
  timeoutMs = 60000,
  sqliteMode: SqliteMode = resolveSqliteMode(config.disableWasmSqlite)
): Promise<void> {
  const start = Date.now()

  const checkProcessAlive = () => {
    if (zeroProcess && zeroProcess.exitCode !== null) {
      const tail = (zeroProcess as ZeroChildProcess).__orezTail
      const details = tail?.length ? `\n${tail.slice(-20).join('\n')}` : ''
      throw new Error(
        `zero-cache exited with code ${zeroProcess.exitCode}${details}${nativeStartupDiagnostics(details, sqliteMode)}`
      )
    }
  }

  // phase 1: wait for HTTP health check
  while (Date.now() - start < timeoutMs) {
    checkProcessAlive()
    const result = await probeZeroCacheHttp(config.zeroPort, 1000)
    if (result.ok) break
    await new Promise((r) => setTimeout(r, 500))
  }

  if (Date.now() - start < timeoutMs) {
    log.debug.orez('zero-cache HTTP health check passed')
    return
  }

  const tail = (zeroProcess as ZeroChildProcess | null | undefined)?.__orezTail
  const details = tail?.length ? `\n${tail.slice(-20).join('\n')}` : ''
  throw new Error(
    `zero-cache health check timed out after ${timeoutMs}ms${details}${nativeStartupDiagnostics(details, sqliteMode)}`
  )
}

function nativeStartupDiagnostics(details: string, sqliteMode: SqliteMode): string {
  if (sqliteMode !== 'native') return ''
  if (!details) return ''
  if (!hasMissingNativeBinarySignature(details)) return ''

  const check = inspectNativeSqliteBinary()
  const instructions = formatNativeBootstrapInstructions(check)
  return `\n\nnative sqlite startup diagnostics:\n${instructions}`
}
