/**
 * postgres browser shim — real postgres package over MessagePort.
 *
 * replaces the old postgres.ts shim. uses the REAL postgres npm package
 * with a custom socket factory that speaks wire protocol over MessagePort
 * to pg-proxy-browser.
 *
 * every connection URL contains its CF embed runtime identity.
 */

// import the REAL postgres package — the bundler aliases 'postgres-real' to the
// actual postgres npm package, avoiding circular resolution since 'postgres'
// is aliased to this file.
// @ts-expect-error — resolved by bundler alias
import postgres from 'postgres-real'

import {
  logCFInstance,
  routeCFPostgresHost,
  routeCFPostgresURL,
} from '../cf-instance-runtime.js'
import { createSocketFactory } from './postgres-socket.js'

function browserPostgres(urlOrOptions?: any, options?: any) {
  let runtime
  let opts: any
  if (typeof urlOrOptions === 'string') {
    runtime = routeCFPostgresURL(urlOrOptions)
    opts = { ...(options || {}) }
    const parsed = new URL(urlOrOptions.replace('pglite://', 'http://'))
    opts.database = parsed.pathname.replace(/^\//, '') || 'postgres'
    opts.host = parsed.hostname
  } else if (urlOrOptions && typeof urlOrOptions === 'object') {
    opts = { ...urlOrOptions }
    const host = Array.isArray(opts.host) ? opts.host[0] : opts.host
    if (typeof host !== 'string') {
      throw new Error('postgres-browser: an instance-routed postgres host is required')
    }
    runtime = routeCFPostgresHost(host)
  } else {
    throw new Error('postgres-browser: an instance-routed postgres URL is required')
  }

  opts.socket = createSocketFactory((port) => runtime.proxyConnect(port))

  opts.port = 0

  opts.ssl = false
  opts.password = runtime.pgPassword
  opts.username = runtime.pgUser
  // disable auto-subscribe
  if (opts.no_subscribe === undefined) opts.no_subscribe = true
  // default pool size 2 — many concurrent connections can overwhelm the
  // MessagePort proxy. an EXPLICIT max passes through: zero-cache's initial-sync
  // copy pool sizes itself one-connection-per-table to work around the
  // unresponsive-connection-after-COPY stream bug (same workaround zero ships
  // for win32 node), and silently clamping it re-introduces the hang.
  if (!opts.max) opts.max = 2

  logCFInstance(runtime, {
    component: 'postgres-browser',
    database: opts.database,
    event: 'client-create',
    replication: !!opts.connection?.replication,
  })
  const client = postgres(opts)
  return client
}

Object.assign(browserPostgres, postgres)
export default browserPostgres
