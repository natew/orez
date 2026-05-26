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
import { createConnection, createServer } from 'node:net'
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
 * Pick a free TCP port by binding ephemeral and reading what we got.
 * Then close immediately and hand the port to pgsqlite. There's a tiny TOCTOU
 * window where another process could grab it before pgsqlite binds, but
 * vitest's parallel test files all go through this helper, so the only races
 * are against unrelated processes on the host — vanishingly unlikely in CI.
 */
async function getFreePort(): Promise<number> {
  return new Promise((resolveFn, reject) => {
    const server = createServer()
    server.unref()
    server.on('error', reject)
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address()
      if (!addr || typeof addr === 'string') {
        server.close()
        reject(new Error('failed to get free port'))
        return
      }
      const port = addr.port
      server.close(() => resolveFn(port))
    })
  })
}

async function probePort(port: number, timeoutMs: number): Promise<boolean> {
  return new Promise((resolveFn) => {
    const sock = createConnection({ host: '127.0.0.1', port })
    const cleanup = (val: boolean) => {
      try {
        sock.destroy()
      } catch {}
      resolveFn(val)
    }
    const timer = setTimeout(() => cleanup(false), timeoutMs)
    sock.once('connect', () => {
      clearTimeout(timer)
      cleanup(true)
    })
    sock.once('error', () => {
      clearTimeout(timer)
      cleanup(false)
    })
  })
}

/**
 * Start a pgsqlite server on an OS-assigned ephemeral port with an ephemeral
 * database. Throws on probe timeout. Caller is responsible for calling
 * `stop()` in a `finally` / `afterAll` hook — `stop()` awaits child exit
 * and cleans up the tempdir.
 */
export async function startPgsqliteServer(): Promise<OracleServer> {
  const bin = pgsqliteBinPath()
  if (!bin) throw new Error('pgsqlite binary not available')

  const port = await getFreePort()
  const tmpDir = mkdtempSync(resolve(tmpdir(), 'orez-oracle-'))
  const dbPath = resolve(tmpDir, 'pg.db')

  const proc: ChildProcess = spawn(
    bin,
    ['--port', String(port), '--database', dbPath, '--log-level', 'error'],
    { stdio: ['ignore', 'pipe', 'pipe'] }
  )

  const exited: Promise<void> = new Promise((res) => proc.once('exit', () => res()))

  // wait for "ready" — poll until the server accepts a connection or we time out
  let ready = false
  const start = Date.now()
  while (Date.now() - start < 10_000) {
    if (await probePort(port, 250)) {
      ready = true
      break
    }
    if (proc.exitCode !== null) {
      throw new Error(`pgsqlite exited before becoming ready (code=${proc.exitCode})`)
    }
    await new Promise((r) => setTimeout(r, 50))
  }
  if (!ready) {
    try {
      proc.kill('SIGKILL')
    } catch {}
    try {
      rmSync(tmpDir, { recursive: true, force: true })
    } catch {}
    throw new Error(`pgsqlite did not become ready on port ${port} within 10s`)
  }

  return {
    port,
    dbPath,
    async stop() {
      try {
        proc.kill('SIGTERM')
      } catch {}
      // wait up to 2s for graceful exit, then SIGKILL
      const killTimer = setTimeout(() => {
        try {
          proc.kill('SIGKILL')
        } catch {}
      }, 2_000)
      try {
        await exited
      } finally {
        clearTimeout(killTimer)
      }
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
