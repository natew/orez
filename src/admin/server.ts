import { existsSync } from 'node:fs'
import {
  createServer,
  type Server,
  type IncomingMessage,
  type ServerResponse,
} from 'node:http'
import { resolve } from 'node:path'

import { log } from '../log.js'
import { getAdminHtml } from './ui.js'

import type { ZeroLiteConfig } from '../config.js'
import type { HttpLogStore } from './http-proxy.js'
import type { LogStore } from './log-store.js'
import type { PGlite } from '@electric-sql/pglite'

export interface AdminActions {
  restartZero?: () => Promise<void>
  stopZero?: () => Promise<void>
  resetZero?: () => Promise<void>
  resetZeroFull?: () => Promise<void>
}

export interface AdminDbInstances {
  postgres: PGlite
  cvr: PGlite
  cdb: PGlite
}

export interface AdminServerOpts {
  port: number
  logStore: LogStore
  config: ZeroLiteConfig
  zeroEnv: Record<string, string>
  actions?: AdminActions
  startTime: number
  httpLog?: HttpLogStore
  db?: AdminDbInstances
}

const CORS_HEADERS: Record<string, string> = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': '*',
}

const JSON_HEADERS: Record<string, string> = {
  ...CORS_HEADERS,
  'Content-Type': 'application/json',
}

function json(res: ServerResponse, data: unknown, status = 200) {
  res.writeHead(status, JSON_HEADERS)
  res.end(JSON.stringify(data))
}

const UI_PATHS = new Set([
  '/',
  '/all',
  '/data',
  '/zero',
  '/pglite',
  '/proxy',
  '/orez',
  '/s3',
  '/http',
  '/env',
])

export function startAdminServer(opts: AdminServerOpts): Promise<Server> {
  const { logStore, config, zeroEnv, actions, startTime } = opts
  const html = getAdminHtml()

  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    if (req.method === 'OPTIONS') {
      res.writeHead(200, CORS_HEADERS)
      res.end()
      return
    }

    const url = new URL(req.url || '/', 'http://localhost:' + opts.port)

    try {
      if (req.method === 'GET' && UI_PATHS.has(url.pathname)) {
        res.writeHead(200, { ...CORS_HEADERS, 'Content-Type': 'text/html' })
        res.end(html)
        return
      }

      if (req.method === 'GET' && url.pathname === '/api/logs') {
        const source = url.searchParams.get('source') || undefined
        const level = url.searchParams.get('level') || undefined
        const sinceStr = url.searchParams.get('since')
        const limitStr = url.searchParams.get('limit')
        const since = sinceStr ? Number(sinceStr) : undefined
        const limit = limitStr ? Number(limitStr) : undefined
        json(res, logStore.query({ source, level, since, limit }))
        return
      }

      if (req.method === 'GET' && url.pathname === '/api/env') {
        const filtered = Object.entries(zeroEnv)
          .filter(
            ([k]) => k.startsWith('ZERO_') || k === 'NODE_ENV' || k === 'NODE_OPTIONS'
          )
          .sort(([a], [b]) => a.localeCompare(b))
        json(res, { env: Object.fromEntries(filtered) })
        return
      }

      if (req.method === 'GET' && url.pathname === '/api/status') {
        json(res, {
          pgPort: config.pgPort,
          zeroPort: config.zeroPort,
          adminPort: opts.port,
          uptime: Math.floor((Date.now() - startTime) / 1000),
          logLevel: config.logLevel,
          skipZeroCache: config.skipZeroCache,
          sqliteMode: config.disableWasmSqlite ? 'native' : 'wasm',
        })
        return
      }

      if (req.method === 'POST' && url.pathname === '/api/actions/restart-zero') {
        if (!actions?.restartZero) {
          json(res, { ok: false, message: 'zero-cache not running' }, 400)
          return
        }
        log.orez('admin: restarting zero-cache')
        await actions.restartZero()
        json(res, { ok: true, message: 'zero-cache restarted' })
        return
      }

      if (req.method === 'POST' && url.pathname === '/api/actions/stop-zero') {
        if (!actions?.stopZero) {
          json(res, { ok: false, message: 'zero-cache not running' }, 400)
          return
        }
        log.orez('admin: stopping zero-cache for restore')
        await actions.stopZero()
        json(res, { ok: true, message: 'zero-cache stopped' })
        return
      }

      if (req.method === 'POST' && url.pathname === '/api/actions/reset-zero') {
        if (!actions?.resetZero) {
          json(res, { ok: false, message: 'zero-cache not running' }, 400)
          return
        }
        log.orez('admin: resetting zero-cache (cache-only)')
        await actions.resetZero()
        json(res, { ok: true, message: 'zero-cache reset (cache-only) and restarted' })
        return
      }

      if (req.method === 'POST' && url.pathname === '/api/actions/reset-zero-full') {
        if (!actions?.resetZeroFull) {
          json(res, { ok: false, message: 'zero-cache not running' }, 400)
          return
        }
        log.orez('admin: resetting zero-cache (full)')
        await actions.resetZeroFull()
        json(res, { ok: true, message: 'zero-cache reset (full) and restarted' })
        return
      }

      if (req.method === 'POST' && url.pathname === '/api/actions/clear-logs') {
        logStore.clear()
        json(res, { ok: true, message: 'logs cleared' })
        return
      }

      if (req.method === 'GET' && url.pathname === '/api/http-log') {
        const sinceStr = url.searchParams.get('since')
        const path = url.searchParams.get('path') || undefined
        const since = sinceStr ? Number(sinceStr) : undefined
        json(res, opts.httpLog?.query({ since, path }) || { entries: [], cursor: 0 })
        return
      }

      if (req.method === 'POST' && url.pathname === '/api/actions/clear-http') {
        opts.httpLog?.clear()
        json(res, { ok: true, message: 'http log cleared' })
        return
      }

      // db explorer endpoints
      if (opts.db && req.method === 'GET' && url.pathname === '/api/db/tables') {
        const dbName = url.searchParams.get('db') || 'postgres'
        const instance = getDbInstance(opts.db, dbName)
        if (!instance) {
          json(res, { error: 'unknown db: ' + dbName }, 400)
          return
        }
        try {
          const result = await instance.query(
            `SELECT table_schema, table_name, pg_total_relation_size(quote_ident(table_schema) || '.' || quote_ident(table_name)) as size_bytes
             FROM information_schema.tables
             WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
             ORDER BY table_schema, table_name`
          )
          json(res, { tables: result.rows })
        } catch (err: any) {
          json(res, { error: err?.message ?? 'query failed' }, 500)
        }
        return
      }

      if (opts.db && req.method === 'GET' && url.pathname === '/api/db/table-data') {
        const dbName = url.searchParams.get('db') || 'postgres'
        const table = url.searchParams.get('table')
        if (!table) {
          json(res, { error: 'missing table param' }, 400)
          return
        }
        const instance = getDbInstance(opts.db, dbName)
        if (!instance) {
          json(res, { error: 'unknown db: ' + dbName }, 400)
          return
        }
        const search = url.searchParams.get('search') || ''
        const offset = Number(url.searchParams.get('offset') || '0')
        const limit = Number(url.searchParams.get('limit') || '100')
        try {
          // get columns first
          const colResult = await instance.query(
            `SELECT column_name, data_type FROM information_schema.columns
             WHERE table_schema || '.' || table_name = $1 OR table_name = $1
             ORDER BY ordinal_position`,
            [table]
          )
          const columns = colResult.rows.map((r: any) => ({
            name: r.column_name,
            type: r.data_type,
          }))
          // build query with optional search
          let sql = `SELECT * FROM ${quoteIdentPg(table)}`
          const params: any[] = []
          if (search) {
            // search across all text-castable columns
            const conds = columns.map(
              (_: any, i: number) =>
                `${quoteIdentPg(columns[i].name)}::text ILIKE $${params.length + 1}`
            )
            if (conds.length > 0) {
              params.push('%' + search + '%')
              sql += ' WHERE ' + conds.join(' OR ')
            }
          }
          // get total count
          const countResult = await instance.query(
            `SELECT count(*)::int as total FROM (${sql}) _c`,
            params
          )
          const total = (countResult.rows[0] as any)?.total ?? 0
          sql += ` LIMIT ${limit} OFFSET ${offset}`
          const result = await instance.query(sql, params)
          json(res, {
            columns,
            rows: result.rows,
            total,
            offset,
            limit,
          })
        } catch (err: any) {
          json(res, { error: err?.message ?? 'query failed' }, 500)
        }
        return
      }

      if (opts.db && req.method === 'POST' && url.pathname === '/api/db/query') {
        const body = await readBody(req)
        let parsed: { db?: string; sql?: string }
        try {
          parsed = JSON.parse(body)
        } catch {
          json(res, { error: 'invalid json body' }, 400)
          return
        }
        const dbName = parsed.db || 'postgres'
        const sql = parsed.sql
        if (!sql) {
          json(res, { error: 'missing sql' }, 400)
          return
        }
        const instance = getDbInstance(opts.db, dbName)
        if (!instance) {
          json(res, { error: 'unknown db: ' + dbName }, 400)
          return
        }
        try {
          const start = performance.now()
          const result = await instance.query(sql)
          const durationMs = Math.round((performance.now() - start) * 100) / 100
          json(res, {
            fields: (result.fields || []).map((f: any) => f.name),
            rows: result.rows,
            rowCount: result.rows.length,
            durationMs,
          })
        } catch (err: any) {
          json(res, { error: err?.message ?? 'query failed' }, 400)
        }
        return
      }

      // sqlite replica endpoints
      if (req.method === 'GET' && url.pathname === '/api/sqlite/tables') {
        const sqliteDb = await openSqliteReplica(opts.config.dataDir)
        if (!sqliteDb) {
          json(res, { error: 'sqlite replica not found' }, 404)
          return
        }
        try {
          const tables = sqliteDb
            .prepare(
              `SELECT name, (SELECT count(*) FROM pragma_table_info(m.name)) as col_count
               FROM sqlite_master m WHERE type='table' AND name NOT LIKE 'sqlite_%'
               ORDER BY name`
            )
            .all()
          json(res, { tables })
        } catch (err: any) {
          json(res, { error: err?.message ?? 'query failed' }, 500)
        } finally {
          sqliteDb.close()
        }
        return
      }

      if (req.method === 'GET' && url.pathname === '/api/sqlite/table-data') {
        const table = url.searchParams.get('table')
        if (!table) {
          json(res, { error: 'missing table param' }, 400)
          return
        }
        const sqliteDb = await openSqliteReplica(opts.config.dataDir)
        if (!sqliteDb) {
          json(res, { error: 'sqlite replica not found' }, 404)
          return
        }
        const search = url.searchParams.get('search') || ''
        const offset = Number(url.searchParams.get('offset') || '0')
        const limit = Number(url.searchParams.get('limit') || '100')
        try {
          const columns = sqliteDb
            .prepare(`SELECT name, type FROM pragma_table_info(?)`)
            .all(table)
          const quotedTable = '"' + table.replace(/"/g, '""') + '"'
          let sql = `SELECT * FROM ${quotedTable}`
          const params: any[] = []
          if (search) {
            const conds = columns.map(
              (c: any) => `"${c.name.replace(/"/g, '""')}" LIKE ?`
            )
            if (conds.length > 0) {
              params.push(...conds.map(() => '%' + search + '%'))
              sql += ' WHERE ' + conds.join(' OR ')
            }
          }
          const countRow = sqliteDb
            .prepare(`SELECT count(*) as total FROM (${sql})`)
            .get(...params)
          const total = (countRow as any)?.total ?? 0
          sql += ` LIMIT ? OFFSET ?`
          params.push(limit, offset)
          const stmt = sqliteDb.prepare(sql)
          const rows = stmt.all(...params)
          json(res, { columns, rows, total, offset, limit })
        } catch (err: any) {
          json(res, { error: err?.message ?? 'query failed' }, 500)
        } finally {
          sqliteDb.close()
        }
        return
      }

      if (req.method === 'POST' && url.pathname === '/api/sqlite/query') {
        const body = await readBody(req)
        let parsed: { sql?: string }
        try {
          parsed = JSON.parse(body)
        } catch {
          json(res, { error: 'invalid json body' }, 400)
          return
        }
        const sql = parsed.sql
        if (!sql) {
          json(res, { error: 'missing sql' }, 400)
          return
        }
        const sqliteDb = await openSqliteReplica(opts.config.dataDir)
        if (!sqliteDb) {
          json(res, { error: 'sqlite replica not found' }, 404)
          return
        }
        try {
          const start = performance.now()
          const stmt = sqliteDb.prepare(sql)
          const fields = stmt.columns().map((c: any) => c.name)
          const rows = stmt.all()
          const durationMs = Math.round((performance.now() - start) * 100) / 100
          json(res, { fields, rows, rowCount: rows.length, durationMs })
        } catch (err: any) {
          json(res, { error: err?.message ?? 'query failed' }, 400)
        } finally {
          sqliteDb.close()
        }
        return
      }

      res.writeHead(404, CORS_HEADERS)
      res.end('not found')
    } catch (err: any) {
      json(res, { error: err?.message ?? 'internal error' }, 500)
    }
  })

  return new Promise((resolve, reject) => {
    server.listen(opts.port, '127.0.0.1', () => {
      resolve(server)
    })
    server.on('error', reject)
  })
}

function getDbInstance(db: AdminDbInstances, name: string): PGlite | null {
  if (name === 'postgres' || name === 'main') return db.postgres
  if (name === 'cvr') return db.cvr
  if (name === 'cdb') return db.cdb
  return null
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    req.on('data', (c) => chunks.push(c))
    req.on('end', () => resolve(Buffer.concat(chunks).toString()))
    req.on('error', reject)
  })
}

function quoteIdentPg(name: string): string {
  if (name.includes('.')) {
    return name
      .split('.')
      .map((p) => '"' + p.replace(/"/g, '""') + '"')
      .join('.')
  }
  if (/^[a-z_][a-z0-9_]*$/.test(name)) return name
  return '"' + name.replace(/"/g, '""') + '"'
}

let cachedDatabaseCtor: any | null = null

async function openSqliteReplica(dataDir: string): Promise<any | null> {
  const replicaPath = resolve(dataDir, 'zero-replica.db')
  if (!existsSync(replicaPath)) return null
  try {
    if (!cachedDatabaseCtor) {
      const mod: any = await import('bedrock-sqlite')
      cachedDatabaseCtor = mod.Database || mod.default?.Database || mod.default || mod
    }
    return new cachedDatabaseCtor(replicaPath, { readonly: true })
  } catch (err: any) {
    log.debug.orez('admin: sqlite replica open failed: ' + (err?.message ?? err))
    return null
  }
}
