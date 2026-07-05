/**
 * native postgres backend: real postgres via the optional `embedded-postgres`
 * package (per-platform server binaries shipped as npm optional deps).
 *
 * zero-cache connects to it directly and uses real logical replication, so
 * none of the pglite emulation applies here: no pg-wire proxy, no mutex, no
 * CDC trigger log, no fake replication slots, no vacuum/checkpoint timers.
 */
import { existsSync, mkdirSync, readFileSync, symlinkSync } from 'node:fs'
import { createRequire } from 'node:module'
import { dirname, relative, resolve } from 'node:path'

import { isPidRunning, terminateProcessTree } from './child-process.js'
import { log } from './log.js'

import type { ZeroLiteConfig } from './config.js'

/** the exec/query surface index.ts uses on PGlite instances */
export interface NativePgDb {
  exec(sql: string): Promise<Array<{ affectedRows?: number }>>
  query<T>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>
  waitReady: Promise<void>
  close(): Promise<void>
}

export interface NativePgInstances {
  postgres: NativePgDb
  cvr: NativePgDb
  cdb: NativePgDb
  postgresReplicas: never[]
}

export interface NativePostgres {
  instances: NativePgInstances
  /** stop the postgres server process (close the instances first) */
  stop(): Promise<void>
  /**
   * full zero reset for the native backend: drop replication slots, drop
   * stale shard schemas from the upstream db, and drop + recreate the
   * zero_cvr/zero_cdb databases. the upstream data is untouched.
   */
  resetZeroDatabases(): Promise<void>
  /**
   * terminate orphaned zero-cache postgres backends left behind by a previous
   * (crashed or killed) zero-cache generation. returns the number terminated.
   */
  terminateZeroBackends(): Promise<number>
}

const ZERO_DATABASES = ['zero_cvr', 'zero_cdb']

// wal_level=logical is required by zero-cache's change source.
// synchronous_commit=off matches the durability posture of the pglite
// backend (relaxedDurability): fast writes, no corruption risk, at most the
// last moments of commits lost on a hard crash.
const SERVER_FLAGS = [
  '-c',
  'wal_level=logical',
  '-c',
  'listen_addresses=127.0.0.1',
  '-c',
  'synchronous_commit=off',
  '-c',
  'unix_socket_directories=',
]

/**
 * resolve a transitive dependency of embedded-postgres (the platform binary
 * package, `pg`) from within its module context — they may not be hoisted
 * to the consumer's node_modules (bun isolated installs).
 */
function requireFromEmbeddedPostgres(name: string): any {
  const require = createRequire(import.meta.url)
  return createRequire(require.resolve('embedded-postgres'))(name)
}

// node-postgres, not postgres.js: it sends parameters untyped, so a JSON
// string bound to a jsonb column is parsed server-side into an object —
// the same semantics PGlite gives today. postgres.js types string params as
// text, which makes jsonb store a string scalar (breaks zero permissions).
function makeDb(config: ZeroLiteConfig, database: string): NativePgDb {
  const { Pool } = requireFromEmbeddedPostgres('pg')
  const pool = new Pool({
    host: '127.0.0.1',
    port: config.pgPort,
    user: config.pgUser,
    password: config.pgPassword,
    database,
    max: 4,
  })
  // idle clients killed by e.g. DROP DATABASE ... WITH (FORCE) — the pool
  // replaces them on next use
  pool.on('error', (err: Error) => {
    log.debug.pg(`pool error (${database}): ${err.message}`)
  })
  return {
    waitReady: Promise.resolve(),
    async exec(text: string) {
      // no params → simple protocol: multi-statement blocks (migrations,
      // seed files) and statements that refuse transaction blocks
      // (CREATE/DROP DATABASE) both work
      const result = await pool.query(text)
      const last = Array.isArray(result) ? result[result.length - 1] : result
      return [{ affectedRows: last?.rowCount ?? 0 }]
    },
    async query<T>(text: string, params?: unknown[]) {
      const result = await pool.query(text, params as never[])
      return { rows: result.rows as T[] }
    },
    async close() {
      await pool.end()
    },
  }
}

const PLATFORM_PACKAGES: Record<string, string> = {
  'darwin-arm64': '@embedded-postgres/darwin-arm64',
  'darwin-x64': '@embedded-postgres/darwin-x64',
  'linux-arm64': '@embedded-postgres/linux-arm64',
  'linux-arm': '@embedded-postgres/linux-arm',
  'linux-ia32': '@embedded-postgres/linux-ia32',
  'linux-ppc64': '@embedded-postgres/linux-ppc64',
  'linux-x64': '@embedded-postgres/linux-x64',
  'win32-x64': '@embedded-postgres/windows-x64',
}

/**
 * the @embedded-postgres platform packages ship their .dylib/.so version
 * symlinks as a pg-symlinks.json manifest hydrated by a postinstall script —
 * which bun blocks unless the consumer lists the package in
 * trustedDependencies. recreate any missing links here so the backend works
 * out of the box.
 */
async function hydrateBinarySymlinks(): Promise<void> {
  try {
    const pkgName = PLATFORM_PACKAGES[`${process.platform}-${process.arch}`]
    if (!pkgName) return
    const require = createRequire(import.meta.url)
    const entry = createRequire(require.resolve('embedded-postgres')).resolve(pkgName)
    const root = resolve(dirname(entry), '..')
    const manifest = resolve(root, 'native', 'pg-symlinks.json')
    if (!existsSync(manifest)) return
    const links: Array<{ source: string; target: string }> = JSON.parse(
      readFileSync(manifest, 'utf8')
    )
    for (const { source, target } of links) {
      const targetPath = resolve(root, target)
      if (existsSync(targetPath)) continue
      try {
        symlinkSync(relative(dirname(targetPath), resolve(root, source)), targetPath)
      } catch {
        // already exists (broken-link race) or unsupported fs — postgres will
        // surface a loader error if a required lib is actually missing
      }
    }
  } catch {}
}

/**
 * a postgres server SIGKILL'd along with a previous orez run keeps running as
 * an orphan, holding the data dir and port. postmaster.pid names it.
 */
async function sweepOrphanPostmaster(dataPath: string): Promise<void> {
  const pidFile = resolve(dataPath, 'postmaster.pid')
  if (!existsSync(pidFile)) return
  let pid = 0
  try {
    pid = Number.parseInt(readFileSync(pidFile, 'utf8').split('\n')[0]!, 10)
  } catch {
    return
  }
  if (!Number.isInteger(pid) || pid <= 0 || !isPidRunning(pid)) return
  log.pg(`stopping orphan postgres pid ${pid} from previous run`)
  await terminateProcessTree(pid, {
    gracefulSignal: 'SIGINT', // postgres fast shutdown
    forceSignal: 'SIGKILL',
    graceMs: 5000,
    forceGraceMs: 1000,
  })
}

export async function startNativePostgres(
  config: ZeroLiteConfig
): Promise<NativePostgres> {
  let EmbeddedPostgres: typeof import('embedded-postgres').default
  try {
    // createRequire, not `await import` — a static dynamic import lets consumer
    // bundlers (vite/rolldown, esbuild) follow the specifier into
    // embedded-postgres/dist/binary.js, whose literal
    // `import('@embedded-postgres/<platform>')` calls for all eight platforms
    // then fail to resolve the seven not installed on the current host, breaking
    // the consumer's build. this backend is node-only and never bundled (only
    // this backend PROCESS loads it, when backend === 'postgres'); createRequire
    // keeps it opaque to bundlers, matching how `pg` and the symlink hydration
    // above already load it. embedded-postgres is ESM with no top-level await, so
    // require() of it is fine on the supported Node versions.
    const require = createRequire(import.meta.url)
    const mod = require('embedded-postgres')
    EmbeddedPostgres = mod.default ?? mod
  } catch {
    throw new Error(
      `backend 'postgres' requires the optional dependency "embedded-postgres".\n` +
        `install it in your project: bun add -D embedded-postgres`
    )
  }

  await hydrateBinarySymlinks()

  const dataPath = resolve(config.ephemeralDir ?? config.dataDir, 'pgdata-native')
  mkdirSync(dataPath, { recursive: true })
  await sweepOrphanPostmaster(dataPath)

  // ring buffer of server output for start-failure diagnostics
  const recentLogs: string[] = []
  const pushLog = (message: string) => {
    for (const line of message.split('\n')) {
      const trimmed = line.trim()
      if (!trimmed) continue
      recentLogs.push(trimmed)
      if (recentLogs.length > 20) recentLogs.shift()
      log.debug.pg(trimmed)
    }
  }

  const server = new EmbeddedPostgres({
    databaseDir: dataPath,
    port: config.pgPort,
    user: config.pgUser,
    password: config.pgPassword,
    authMethod: 'password',
    persistent: true,
    postgresFlags: SERVER_FLAGS,
    onLog: pushLog,
    onError: (messageOrError: unknown) => pushLog(String(messageOrError)),
  })

  if (!existsSync(resolve(dataPath, 'PG_VERSION'))) {
    log.pg(`initializing native postgres cluster at ${dataPath}`)
    await server.initialise()
  }

  try {
    await server.start()
  } catch (err) {
    throw new Error(
      `native postgres failed to start (port ${config.pgPort}, data ${dataPath})` +
        (err ? `: ${err}` : '') +
        (recentLogs.length ? `\n${recentLogs.join('\n')}` : '')
    )
  }
  log.debug.pg(`native postgres ready on 127.0.0.1:${config.pgPort}`)

  const upstream = makeDb(config, 'postgres')

  // zero_cvr / zero_cdb as real databases in the same cluster
  const existing = await upstream.query<{ datname: string }>(
    `SELECT datname FROM pg_database WHERE datname = ANY($1)`,
    [ZERO_DATABASES]
  )
  const existingSet = new Set(existing.rows.map((r) => r.datname))
  for (const name of ZERO_DATABASES) {
    if (!existingSet.has(name)) {
      await upstream.exec(`CREATE DATABASE "${name}"`)
      log.debug.pg(`created database ${name}`)
    }
  }

  const instances: NativePgInstances = {
    postgres: upstream,
    cvr: makeDb(config, 'zero_cvr'),
    cdb: makeDb(config, 'zero_cdb'),
    postgresReplicas: [],
  }

  // when zero-cache is killed or crashes, its postgres backends don't always
  // exit with it: a logical walsender parked in WalSenderWaitForWal only notices
  // the dropped client after wal_sender_timeout (~60s), and a backend blocked
  // building a replication-slot snapshot never reads its socket to see the
  // disconnect at all. those orphans keep the replication slot ACTIVE and hold
  // open transactions, so the NEXT zero-cache's initial sync stalls in
  // CREATE_REPLICATION_SLOT and is canceled by its own `SET lock_timeout`
  // (PostgresError 55P03), crashing the change-streamer and feeding orez's
  // restart loop. sweep them so every (re)start begins from a clean slate. zero
  // prefixes every connection's application_name with `zero-` (see @rocicorp/zero
  // pgClient); orez's own node-pg pools set none, so this filter never touches
  // them. pg_stat_activity is cluster-wide, so one query covers postgres +
  // zero_cvr + zero_cdb.
  const ORPHAN_FILTER = `pid <> pg_backend_pid() AND application_name LIKE 'zero-%'`
  const terminateZeroBackends = async (): Promise<number> => {
    const listOrphans = async () =>
      (
        await upstream.query<{ pid: number }>(
          `SELECT pid FROM pg_stat_activity WHERE ${ORPHAN_FILTER}`
        )
      ).rows
    try {
      const orphans = await listOrphans()
      if (orphans.length === 0) return 0
      await upstream.exec(
        `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE ${ORPHAN_FILTER}`
      )
      // pg_terminate_backend only signals; the backend exits and releases its
      // slot/locks a moment later. wait for the orphans to actually clear so the
      // next zero-cache doesn't race a still-active walsender (up to ~2s).
      for (let i = 0; i < 20; i++) {
        await new Promise((r) => setTimeout(r, 100))
        if ((await listOrphans()).length === 0) break
      }
      log.orez(`terminated ${orphans.length} orphaned zero-cache backend(s)`)
      return orphans.length
    } catch (err) {
      log.debug.pg(`terminateZeroBackends failed: ${err}`)
      return 0
    }
  }

  return {
    instances,
    terminateZeroBackends,

    async stop() {
      await server.stop()
    },

    async resetZeroDatabases() {
      // zero-cache is stopped by the caller; sweep any orphaned zero backends
      // (walsenders, initial-sync connections holding the slot-management
      // advisory lock or an open txn) so the drops below aren't blocked and the
      // next run starts clean. this is called on the recovery path too, which
      // bypasses killZeroCache, so the sweep must live here as well.
      await terminateZeroBackends()
      const slots = await upstream.query<{ slot_name: string }>(
        `SELECT slot_name FROM pg_replication_slots`
      )
      for (const { slot_name } of slots.rows) {
        await upstream
          .query(`SELECT pg_drop_replication_slot($1)`, [slot_name])
          .catch((err) => {
            log.debug.pg(`slot drop failed for ${slot_name}: ${err}`)
          })
      }

      // remove stale zero shard schemas from the upstream db
      const shardSchemas = await upstream.query<{ schemaname: string }>(
        `SELECT DISTINCT schemaname FROM pg_tables
         WHERE tablename IN ('clients', 'replicas', 'mutations')
           AND schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', 'public')
           AND schemaname NOT LIKE 'pg_%'`
      )
      for (const { schemaname } of shardSchemas.rows) {
        const quoted = '"' + schemaname.replace(/"/g, '""') + '"'
        await upstream.exec(`DROP SCHEMA IF EXISTS ${quoted} CASCADE`)
      }
      if (shardSchemas.rows.length > 0) {
        log.orez(`dropped ${shardSchemas.rows.length} stale shard schema(s)`)
      }

      // WITH (FORCE) terminates any remaining connections (including our own
      // idle pool connections, which reconnect on next use)
      for (const name of ZERO_DATABASES) {
        await upstream.exec(`DROP DATABASE IF EXISTS "${name}" WITH (FORCE)`)
        await upstream.exec(`CREATE DATABASE "${name}"`)
      }
      log.orez('recreated zero_cvr/zero_cdb databases')
    },
  }
}
