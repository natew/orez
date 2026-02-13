import {
  createServer,
  type Server,
  type IncomingMessage,
  type ServerResponse,
} from 'node:http'

import { log } from '../log.js'
import { getAdminHtml } from './ui.js'

import type { ZeroLiteConfig } from '../config.js'
import type { HttpLogStore } from './http-proxy.js'
import type { LogStore } from './log-store.js'

export interface AdminActions {
  restartZero?: () => Promise<void>
  resetZero?: () => Promise<void>
  resetZeroFull?: () => Promise<void>
}

export interface AdminServerOpts {
  port: number
  logStore: LogStore
  config: ZeroLiteConfig
  zeroEnv: Record<string, string>
  actions?: AdminActions
  startTime: number
  httpLog?: HttpLogStore
}

function corsHeaders(): Record<string, string> {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': '*',
  }
}

function json(res: ServerResponse, data: unknown, status = 200) {
  const headers = { ...corsHeaders(), 'Content-Type': 'application/json' }
  res.writeHead(status, headers)
  res.end(JSON.stringify(data))
}

export function startAdminServer(opts: AdminServerOpts): Promise<Server> {
  const { logStore, config, zeroEnv, actions, startTime } = opts
  const html = getAdminHtml()

  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    const headers = corsHeaders()

    if (req.method === 'OPTIONS') {
      res.writeHead(200, headers)
      res.end()
      return
    }

    const url = new URL(req.url || '/', 'http://localhost:' + opts.port)

    try {
      if (req.method === 'GET' && url.pathname === '/') {
        res.writeHead(200, { ...headers, 'Content-Type': 'text/html' })
        res.end(html)
        return
      }

      if (req.method === 'GET' && url.pathname === '/api/logs') {
        const source = url.searchParams.get('source') || undefined
        const level = url.searchParams.get('level') || undefined
        const sinceStr = url.searchParams.get('since')
        const since = sinceStr ? Number(sinceStr) : undefined
        json(res, logStore.query({ source, level, since }))
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

      res.writeHead(404, headers)
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
