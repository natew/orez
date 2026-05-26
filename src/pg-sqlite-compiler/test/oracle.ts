/**
 * pgsqlite-backed oracle for compiler quality.
 *
 * Spawns a real pgsqlite server, sends PG SQL via the PG wire protocol,
 * captures the result set. Compares against running the same query through
 * our compiler + bun:sqlite. Equivalence at the result-set level is what we
 * actually care about — pgsqlite is the oracle, not the spec.
 *
 * The pgsqlite binary path comes from `vendor/pgsqlite/.resolved-path`,
 * populated by `scripts/pgsqlite/ensure.ts`. If empty, oracle tests should
 * be marked `it.skip` so the suite still runs without pgsqlite installed.
 */
import { spawn, type ChildProcess } from 'node:child_process'
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { createConnection } from 'node:net'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'

import Database from '@rocicorp/zero-sqlite3'

const VENDOR_PATH_FILE = resolve(
  import.meta.dirname,
  '..',
  '..',
  '..',
  'vendor',
  'pgsqlite',
  '.resolved-path'
)

export function pgsqliteBinPath(): string | null {
  if (!existsSync(VENDOR_PATH_FILE)) return null
  const path = readFileSync(VENDOR_PATH_FILE, 'utf-8').trim()
  return path && existsSync(path) ? path : null
}

export const ORACLE_AVAILABLE = pgsqliteBinPath() !== null

export interface OracleServer {
  port: number
  dbPath: string
  stop(): Promise<void>
}

/**
 * Start a pgsqlite server on an ephemeral port with an ephemeral database.
 * Caller is responsible for calling stop() in a `finally` / `afterAll` hook.
 */
export async function startPgsqliteServer(): Promise<OracleServer> {
  const bin = pgsqliteBinPath()
  if (!bin) throw new Error('pgsqlite binary not available')

  const port = 5500 + Math.floor(Math.random() * 1000)
  const tmpDir = mkdtempSync(resolve(tmpdir(), 'orez-oracle-'))
  const dbPath = resolve(tmpDir, 'pg.db')

  const proc: ChildProcess = spawn(
    bin,
    ['--port', String(port), '--database', dbPath, '--log-level', 'error'],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  )

  // wait for "ready" by trying to connect on the port
  const start = Date.now()
  while (Date.now() - start < 5_000) {
    const ready = await new Promise<boolean>((resolveProbe) => {
      const sock = createConnection({ host: '127.0.0.1', port }, () => {
        sock.end()
        resolveProbe(true)
      })
      sock.once('error', () => resolveProbe(false))
    })
    if (ready) break
    await new Promise((r) => setTimeout(r, 50))
  }

  return {
    port,
    dbPath,
    async stop() {
      try {
        proc.kill('SIGTERM')
      } catch {}
      try {
        rmSync(tmpDir, { recursive: true, force: true })
      } catch {}
    },
  }
}

/**
 * Helper: open a postgres.js client to a running pgsqlite server.
 * Requires `postgres` to be available (we already ship it via @rocicorp/zero).
 */
export async function connectOracle(server: OracleServer): Promise<{
  exec: (sql: string, params?: any[]) => Promise<any[]>
  end: () => Promise<void>
}> {
  const { default: postgres } = await import('postgres')
  const sql = postgres({
    host: '127.0.0.1',
    port: server.port,
    user: 'oracle',
    password: '',
    database: 'main',
    ssl: false,
    max: 1,
    idle_timeout: 5,
    connect_timeout: 5,
    fetch_types: false,
    prepare: false,
  })
  return {
    exec: async (s: string, params: any[] = []) => {
      const rows = await sql.unsafe(s, params as any[])
      return rows as any[]
    },
    end: () => sql.end({ timeout: 2 }).then(() => undefined),
  }
}

/**
 * Run the same query against pgsqlite (oracle) and our compiler+SQLite (under
 * test). Return both result sets so the test can diff them however it wants.
 *
 * Uses freshly-spawned servers/dbs for each call — slow but isolated. For
 * batches use the lower-level helpers above.
 */
export async function runOracleAndCompiler(
  setupSql: string[],
  pgSql: string,
  params: any[] = []
): Promise<{
  oracle: any[]
  ours: any[]
}> {
  const server = await startPgsqliteServer()
  try {
    const conn = await connectOracle(server)
    try {
      for (const s of setupSql) await conn.exec(s)
      const oracle = await conn.exec(pgSql, params)

      // ours: setup + query against fresh in-memory sqlite (after compile())
      const { compile } = await import('../index.js')
      const db = new Database(':memory:')
      for (const s of setupSql) {
        const { sql: translated } = compile(s)
        db.exec(translated)
      }
      const { sql: translatedQuery } = compile(pgSql)
      // postgres.js uses $1 params; sqlite uses ?
      const sqliteSql = translatedQuery.replace(/\$(\d+)/g, '?')
      const stmt = db.prepare(sqliteSql)
      const ours = (
        params.length > 0 ? stmt.all(...(params as any[])) : stmt.all()
      ) as any[]
      db.close()

      return { oracle, ours }
    } finally {
      await conn.end()
    }
  } finally {
    await server.stop()
  }
}
