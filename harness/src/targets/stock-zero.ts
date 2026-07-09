// stock-zero target: real embedded postgres (wal_level=logical) + real
// zero-cache spawned from node_modules. this is the reference implementation
// every other target's behavior is compared against.
import { type ChildProcess, spawn } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { Zero } from '@rocicorp/zero'
import postgres from 'postgres'
import { startAppServer } from '../app-server.js'
import { DDL, SEED, jsonColumns, mutators, permissions, schema } from '../fixture.js'
import type { Rows, SyncTarget } from '../target.js'

const require = createRequire(import.meta.url)

// crib of orez src/native-postgres.ts SERVER_FLAGS: logical replication for
// zero's change source, loopback only, fast commits for a throwaway store
const PG_FLAGS = [
  '-c',
  'wal_level=logical',
  '-c',
  'listen_addresses=127.0.0.1',
  '-c',
  'synchronous_commit=off',
  '-c',
  'unix_socket_directories=',
]

// zero-cache's global-tables bootstrap (deploy-permissions does exactly this
// before writing the permissions row; replicated here so we skip the CLI's
// typescript loader). appID default is 'zero'.
function globalSetupSql(app: string) {
  return `
  CREATE SCHEMA IF NOT EXISTS ${app};
  CREATE TABLE IF NOT EXISTS ${app}.permissions (
    "permissions" JSONB,
    "hash"        TEXT,
    "lock" BOOL PRIMARY KEY DEFAULT true CHECK (lock)
  );
  CREATE OR REPLACE FUNCTION ${app}.set_permissions_hash()
  RETURNS TRIGGER AS $$
  BEGIN
      NEW.hash = md5(NEW.permissions::text);
      RETURN NEW;
  END;
  $$ LANGUAGE plpgsql;
  CREATE OR REPLACE TRIGGER on_set_permissions
    BEFORE INSERT OR UPDATE ON ${app}.permissions
    FOR EACH ROW
    EXECUTE FUNCTION ${app}.set_permissions_hash();
  INSERT INTO ${app}.permissions (permissions) VALUES (NULL) ON CONFLICT DO NOTHING;
`
}

async function waitForHttp(url: string, timeoutMs: number) {
  const start = Date.now()
  let lastError: unknown
  while (Date.now() - start < timeoutMs) {
    try {
      await fetch(url)
      return
    } catch (error) {
      lastError = error
      await new Promise((r) => setTimeout(r, 200))
    }
  }
  throw new Error(`timed out waiting for ${url}: ${lastError}`)
}

export async function startStockZero(opts?: {
  pgPort?: number
  zeroPort?: number
  appPort?: number
  logLevel?: string
}): Promise<SyncTarget> {
  // random port block per run: a crashed/timed-out previous run can leave an
  // orphaned zero-cache holding fixed ports (EADDRINUSE crash-loop). NOTE:
  // zero-cache binds ZERO_PORT+1 (change-streamer) and +2 internally — the
  // app server must stay clear of that range.
  const base = 27_000 + Math.floor(Math.random() * 2_000) * 16
  const pgPort = opts?.pgPort ?? base
  const zeroPort = opts?.zeroPort ?? base + 4
  const appPort = opts?.appPort ?? base + 12
  const dataDir = mkdtempSync(join(tmpdir(), 'zharness-stock-'))

  // embedded-postgres default export is the class
  const { default: EmbeddedPostgres } = await import('embedded-postgres')
  const pg = new EmbeddedPostgres({
    databaseDir: join(dataDir, 'pg'),
    user: 'postgres',
    password: 'password',
    port: pgPort,
    persistent: false,
    postgresFlags: PG_FLAGS,
  })
  await pg.initialise()
  await pg.start()
  await pg.createDatabase('zharness')

  const dbUrl = `postgres://postgres:password@127.0.0.1:${pgPort}/zharness`
  const sql = postgres(dbUrl, { max: 4, onnotice: () => {} })

  for (const stmt of DDL) await sql.unsafe(stmt)
  for (const [tableName, rows] of Object.entries(SEED)) {
    const jsonCols = jsonColumns(tableName)
    for (const row of rows) {
      // schema-driven json encoding: sql.json makes pg store the intended
      // json VALUE. a plain string param into jsonb double-encodes (stores a
      // json string) — the shapes lane caught exactly that.
      const insert = Object.fromEntries(
        Object.entries(row).map(([k, v]) => [
          k,
          jsonCols.has(k) && v !== null ? sql.json(v as never) : v,
        ])
      )
      await sql`INSERT INTO ${sql(tableName)} ${sql(insert)}`
    }
  }

  // deploy ANYONE_CAN_DO_ANYTHING permissions the way zero-deploy-permissions does
  await sql.unsafe(globalSetupSql('zero'))
  const perms = await permissions
  await sql`UPDATE zero.permissions SET permissions = ${sql.json(perms as never)}`

  // the fixture app server: named-query transform + custom-mutator execution
  const app = await startAppServer({ dbUrl, port: appPort })

  // spawn real zero-cache from node_modules
  const zeroEntry = require.resolve('@rocicorp/zero')
  const cli = resolve(zeroEntry, '../../..', 'zero/src/cli.js')
  const replicaFile = join(dataDir, 'replica.db')
  // 'node' from PATH, NOT process.execPath: under bun that is the bun binary,
  // and zero-cache (multi-process node server) does not boot under bun
  const child: ChildProcess = spawn('node', [cli], {
    env: {
      ...process.env,
      NODE_ENV: 'development',
      ZERO_UPSTREAM_DB: dbUrl,
      ZERO_CVR_DB: dbUrl,
      ZERO_CHANGE_DB: dbUrl,
      ZERO_REPLICA_FILE: replicaFile,
      ZERO_PORT: String(zeroPort),
      ZERO_LOG_LEVEL: opts?.logLevel ?? 'warn',
      ZERO_NUM_SYNC_WORKERS: '1',
      OTEL_SDK_DISABLED: 'true',
      // modern surface only: custom mutators + named queries through the app
      // server; setting both URLs also enables them without JWT config. CRUD
      // is disabled so nothing can silently fall back to the legacy path.
      ZERO_MUTATE_URL: `${app.url}/mutate`,
      ZERO_QUERY_URL: `${app.url}/query`,
      ZERO_ENABLE_CRUD_MUTATIONS: 'false',
    },
    stdio: ['ignore', 'inherit', 'inherit'],
  })
  const childExit = new Promise<number | null>((res) => child.on('exit', res))
  childExit.then((code) => {
    if (code !== null && code !== 0) console.error(`[stock-zero] zero-cache exited ${code}`)
  })

  await waitForHttp(`http://127.0.0.1:${zeroPort}/`, 60_000)

  const clients: Zero<typeof schema>[] = []
  let clientN = 0

  return {
    name: 'stock-zero',

    createClient(userID: string) {
      const zero = new Zero({
        server: `http://127.0.0.1:${zeroPort}`,
        userID,
        auth: `token-${userID}`,
        schema,
        mutators,
        kvStore: 'mem' as const,
        storageKey: `zharness-${++clientN}`,
      })
      clients.push(zero)
      return zero
    },

    async sql(query: string): Promise<Rows> {
      return (await sql.unsafe(query)) as unknown as Rows
    },

    async oracle(query: string): Promise<Rows> {
      return (await sql.unsafe(query)) as unknown as Rows
    },

    async metrics() {
      return {}
    },

    async close() {
      while (clients.length) await clients.pop()?.close()
      child.kill('SIGTERM')
      await Promise.race([childExit, new Promise((r) => setTimeout(r, 5_000))])
      if (child.exitCode === null) child.kill('SIGKILL')
      await app.close()
      await sql.end({ timeout: 2 })
      await pg.stop()
      rmSync(dataDir, { recursive: true, force: true })
    },
  }
}
