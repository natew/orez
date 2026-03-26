/**
 * postgres browser shim — real postgres package over MessagePort.
 *
 * replaces the old postgres.ts shim. uses the REAL postgres npm package
 * with a custom socket factory that speaks wire protocol over MessagePort
 * to pg-proxy-browser.
 *
 * setup: set globalThis.__orez_proxy_connect before importing.
 */

// import the REAL postgres package — the bundler aliases 'postgres-real' to the
// actual postgres npm package, avoiding circular resolution since 'postgres'
// is aliased to this file.
// @ts-expect-error — resolved by bundler alias
import postgres from 'postgres-real'
import { createSocketFactory } from './postgres-socket.js'

const getProxyConnect = (): ((port: MessagePort) => void) => {
  const fn = (globalThis as any).__orez_proxy_connect
  if (!fn) throw new Error('__orez_proxy_connect not set')
  return fn
}

function browserPostgres(urlOrOptions?: any, options?: any) {
  const opts: any = typeof urlOrOptions === 'string'
    ? { ...(options || {}), host: urlOrOptions }
    : { ...(urlOrOptions || {}) }

  opts.socket = createSocketFactory(getProxyConnect())

  if (typeof urlOrOptions === 'string') {
    try {
      const parsed = new URL(urlOrOptions.replace('pglite://', 'http://'))
      opts.database = parsed.pathname.replace(/^\//, '') || 'postgres'
      opts.host = '127.0.0.1'
      opts.port = 0
    } catch {}
  }

  opts.ssl = false
  opts.password = (globalThis as any).__orez_proxy_password || ''
  opts.username = (globalThis as any).__orez_proxy_user || 'user'

  const client = postgres(opts)
  // log connection errors for debugging
  if (opts.connection?.replication) {
    console.debug('[postgres-browser] created replication client', opts.database)
  }
  return client
}

Object.assign(browserPostgres, postgres)
export default browserPostgres
