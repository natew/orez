/**
 * integration tests for the admin data explorer endpoints.
 *
 * spins up pglite instances + admin server directly (no zero-cache)
 * and exercises the /api/db/* and /api/sqlite/* endpoints.
 */
import { mkdirSync, rmSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'

import { PGlite } from '@electric-sql/pglite'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'

import { startAdminServer } from './server.js'

import type { ZeroLiteConfig } from '../config.js'
import type { LogStore } from './log-store.js'
import type { Server } from 'node:http'

const TEST_PORT = 16400 + Math.floor(Math.random() * 500)
const DATA_DIR = `.orez-admin-data-test-${Date.now()}`

function stubLogStore(): LogStore {
  const entries: any[] = []
  return {
    push() {},
    query() {
      return { entries, cursor: 0 }
    },
    getAll() {
      return entries
    },
    clear() {
      entries.length = 0
    },
  }
}

function stubConfig(): ZeroLiteConfig {
  return {
    dataDir: resolve(DATA_DIR),
    pgPort: 0,
    zeroPort: 0,
    adminPort: TEST_PORT,
    pgUser: 'test',
    pgPassword: 'test',
    migrationsDir: '',
    seedFile: '',
    skipZeroCache: true,
    disableWasmSqlite: false,
    forceWasmSqlite: false,
    useWorkerThreads: false,
    singleDb: false,
    readReplicas: 0,
    logLevel: 'info',
    pgliteOptions: {},
    checkpointIntervalMs: 0,
    maxLogFileSize: 0,
    disableDiskLogs: true,
  }
}

describe('admin data explorer', { timeout: 60_000 }, () => {
  let server: Server
  let postgres: PGlite
  let cvr: PGlite
  let cdb: PGlite
  const base = `http://127.0.0.1:${TEST_PORT}`

  beforeAll(async () => {
    mkdirSync(DATA_DIR, { recursive: true })

    postgres = new PGlite()
    cvr = new PGlite()
    cdb = new PGlite()
    await Promise.all([postgres.waitReady, cvr.waitReady, cdb.waitReady])

    // create test tables
    await postgres.exec(`
      CREATE TABLE public.users (
        id serial PRIMARY KEY,
        name text NOT NULL,
        email text,
        active boolean DEFAULT true
      );
      INSERT INTO public.users (name, email) VALUES
        ('alice', 'alice@test.com'),
        ('bob', 'bob@test.com'),
        ('charlie', 'charlie@test.com');
    `)

    await postgres.exec(`
      CREATE TABLE public.posts (
        id serial PRIMARY KEY,
        user_id int REFERENCES users(id),
        title text NOT NULL,
        body text
      );
      INSERT INTO public.posts (user_id, title, body) VALUES
        (1, 'hello world', 'first post'),
        (1, 'second post', 'more content'),
        (2, 'bob writes', NULL);
    `)

    server = await startAdminServer({
      port: TEST_PORT,
      logStore: stubLogStore(),
      config: stubConfig(),
      zeroEnv: {},
      startTime: Date.now(),
      db: { postgres, cvr, cdb, postgresReplicas: [] } as any,
    })
  })

  afterAll(async () => {
    server?.close()
    await Promise.all([postgres?.close(), cvr?.close(), cdb?.close()])
    rmSync(DATA_DIR, { recursive: true, force: true })
  })

  // --- html ---

  test('GET / serves html', async () => {
    const res = await fetch(`${base}/`)
    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toContain('text/html')
    const html = await res.text()
    expect(html).toContain('oreZ admin')
    expect(html).toContain('data-db="sqlite"')
  })

  test('GET /data serves html', async () => {
    const res = await fetch(`${base}/data`)
    expect(res.status).toBe(200)
    const html = await res.text()
    expect(html).toContain('sql-editor')
  })

  // --- /api/db/tables ---

  test('lists postgres tables', async () => {
    const res = await fetch(`${base}/api/db/tables?db=postgres`)
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.tables).toBeDefined()
    const names = data.tables.map((t: any) => t.table_name)
    expect(names).toContain('users')
    expect(names).toContain('posts')
  })

  test('lists cvr tables (empty)', async () => {
    const res = await fetch(`${base}/api/db/tables?db=cvr`)
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.tables).toBeDefined()
    expect(data.tables.length).toBe(0)
  })

  test('rejects unknown db name', async () => {
    const res = await fetch(`${base}/api/db/tables?db=nope`)
    expect(res.status).toBe(400)
    const data = await res.json()
    expect(data.error).toContain('unknown db')
  })

  // --- /api/db/table-data ---

  test('browses table data', async () => {
    const res = await fetch(`${base}/api/db/table-data?db=postgres&table=users`)
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.columns).toBeDefined()
    expect(data.columns.length).toBeGreaterThanOrEqual(4)
    expect(data.rows.length).toBe(3)
    expect(data.total).toBe(3)
    // check column metadata
    const colNames = data.columns.map((c: any) => c.name)
    expect(colNames).toContain('id')
    expect(colNames).toContain('name')
    expect(colNames).toContain('email')
  })

  test('table-data supports search', async () => {
    const res = await fetch(
      `${base}/api/db/table-data?db=postgres&table=users&search=alice`
    )
    const data = await res.json()
    expect(data.rows.length).toBe(1)
    expect(data.rows[0].name).toBe('alice')
    expect(data.total).toBe(1)
  })

  test('table-data supports pagination', async () => {
    const res = await fetch(
      `${base}/api/db/table-data?db=postgres&table=users&limit=2&offset=0`
    )
    const data = await res.json()
    expect(data.rows.length).toBe(2)
    expect(data.total).toBe(3)

    const page2 = await fetch(
      `${base}/api/db/table-data?db=postgres&table=users&limit=2&offset=2`
    )
    const data2 = await page2.json()
    expect(data2.rows.length).toBe(1)
  })

  test('table-data with schema-qualified name', async () => {
    const res = await fetch(`${base}/api/db/table-data?db=postgres&table=public.posts`)
    const data = await res.json()
    expect(data.rows.length).toBe(3)
    // check NULL values come through
    const bobPost = data.rows.find((r: any) => r.title === 'bob writes')
    expect(bobPost).toBeDefined()
    expect(bobPost.body).toBeNull()
  })

  test('table-data missing table param', async () => {
    const res = await fetch(`${base}/api/db/table-data?db=postgres`)
    expect(res.status).toBe(400)
    const data = await res.json()
    expect(data.error).toContain('missing table')
  })

  // --- /api/db/query ---

  test('runs arbitrary SQL', async () => {
    const res = await fetch(`${base}/api/db/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        db: 'postgres',
        sql: 'SELECT name, email FROM users ORDER BY name',
      }),
    })
    expect(res.status).toBe(200)
    const data = await res.json()
    expect(data.fields).toEqual(['name', 'email'])
    expect(data.rowCount).toBe(3)
    expect(data.rows[0].name).toBe('alice')
    expect(data.durationMs).toBeGreaterThanOrEqual(0)
  })

  test('returns error for bad SQL', async () => {
    const res = await fetch(`${base}/api/db/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ db: 'postgres', sql: 'SELECT * FROM nonexistent' }),
    })
    expect(res.status).toBe(400)
    const data = await res.json()
    expect(data.error).toBeTruthy()
  })

  test('query with joins', async () => {
    const res = await fetch(`${base}/api/db/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        db: 'postgres',
        sql: `SELECT u.name, p.title FROM users u JOIN posts p ON p.user_id = u.id ORDER BY p.id`,
      }),
    })
    const data = await res.json()
    expect(data.rowCount).toBe(3)
    expect(data.fields).toEqual(['name', 'title'])
  })

  test('query missing sql', async () => {
    const res = await fetch(`${base}/api/db/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ db: 'postgres' }),
    })
    expect(res.status).toBe(400)
    const data = await res.json()
    expect(data.error).toContain('missing sql')
  })

  // --- sqlite ---

  test('sqlite tables returns 404 when no replica', async () => {
    const res = await fetch(`${base}/api/sqlite/tables`)
    expect(res.status).toBe(404)
    const data = await res.json()
    expect(data.error).toContain('not found')
  })

  test('sqlite endpoints work when replica file exists', async () => {
    // create a fake zero-replica.db using bedrock-sqlite
    // @ts-expect-error - CJS module
    const bedrock: any = await import('bedrock-sqlite')
    const Ctor = bedrock.Database || bedrock.default?.Database || bedrock.default
    const replicaPath = resolve(DATA_DIR, 'zero-replica.db')
    const setupDb = new Ctor(replicaPath)
    setupDb.exec(`
      CREATE TABLE widgets (
        id INTEGER PRIMARY KEY,
        label TEXT NOT NULL,
        count INTEGER
      );
      INSERT INTO widgets (label, count) VALUES
        ('alpha', 1),
        ('beta', 2),
        ('gamma', 3);
    `)
    setupDb.close()

    // list tables
    const tablesRes = await fetch(`${base}/api/sqlite/tables`)
    expect(tablesRes.status).toBe(200)
    const tables = await tablesRes.json()
    expect(tables.tables.some((t: any) => t.name === 'widgets')).toBe(true)

    // browse table data
    const browseRes = await fetch(`${base}/api/sqlite/table-data?table=widgets`)
    expect(browseRes.status).toBe(200)
    const browse = await browseRes.json()
    expect(browse.rows.length).toBe(3)
    expect(browse.total).toBe(3)

    // search
    const searchRes = await fetch(
      `${base}/api/sqlite/table-data?table=widgets&search=beta`
    )
    const search = await searchRes.json()
    expect(search.rows.length).toBe(1)
    expect(search.rows[0].label).toBe('beta')

    // raw query
    const queryRes = await fetch(`${base}/api/sqlite/query`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'SELECT count(*) as c FROM widgets' }),
    })
    expect(queryRes.status).toBe(200)
    const q = await queryRes.json()
    expect(q.rows[0].c).toBe(3)
    expect(q.fields).toContain('c')
  })

  // --- CORS ---

  test('OPTIONS returns CORS headers', async () => {
    const res = await fetch(`${base}/api/db/tables`, { method: 'OPTIONS' })
    expect(res.status).toBe(200)
    expect(res.headers.get('access-control-allow-origin')).toBe('*')
  })
})
