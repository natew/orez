/**
 * Per-Durable-Object runtime routing for the Cloudflare zero-cache embed.
 *
 * zero-cache normally relies on process isolation. Workerd can host several
 * Durable Object instances in one isolate, so every shim boundary must carry
 * a stable instance identifier instead. The identifiers are encoded into the
 * values zero already passes around: sqlite paths, postgres hosts, HTTP API
 * hosts, and Fastify ports.
 */

export interface CFInstanceRuntimeInput {
  apiFetch?: typeof fetch
  doSqlite: unknown
  env: Readonly<Record<string, string>>
  instanceId: string
  log?: (event: Record<string, unknown>) => void
  pgPassword: string
  pgUser: string
}

export interface CFInstanceRuntime {
  readonly apiFetch?: typeof fetch
  readonly apiOriginByHost: Map<string, string>
  readonly basePort: number
  readonly doSqlite: unknown
  readonly encodedId: string
  env: Readonly<Record<string, string>>
  readonly fastifyByPort: Map<number, unknown>
  readonly fastifyInstances: Set<unknown>
  readonly instanceId: string
  readonly log?: (event: Record<string, unknown>) => void
  readonly pgPassword: string
  readonly pgUser: string
  readonly sqliteHandles: Set<{ close(): unknown }>
  proxyConnect(port: MessagePort): void
}

const SQLITE_PREFIX = '/__orez_cf_instance__/'
const POSTGRES_SUFFIX = '.orez-pg.local'
const API_SUFFIX = '.orez-zero-api.local'
const FIRST_PORT = 20_000
const PORT_STRIDE = 4
const LAST_PORT = 65_000

const runtimesById = new Map<string, CFInstanceRuntime>()
const runtimesByEncodedId = new Map<string, CFInstanceRuntime>()
const runtimesByPort = new Map<number, CFInstanceRuntime>()
const apiRoutesByHost = new Map<string, { origin: string; runtime: CFInstanceRuntime }>()
const runtimeStops = new WeakMap<CFInstanceRuntime, () => Promise<void>>()

function requireInstanceId(instanceId: string | undefined): string {
  if (typeof instanceId !== 'string') {
    throw new Error('zero-cache CF embed: instanceId is required')
  }
  const normalized = instanceId.trim()
  if (!normalized) throw new Error('zero-cache CF embed: instanceId is required')
  return normalized
}

function encodeInstanceId(instanceId: string): string {
  return [...new TextEncoder().encode(instanceId)]
    .map((byte) => byte.toString(16).padStart(2, '0'))
    .join('')
}

function allocateBasePort(): number {
  for (let port = FIRST_PORT; port <= LAST_PORT; port += PORT_STRIDE) {
    let available = true
    for (let offset = 0; offset < PORT_STRIDE; offset++) {
      if (runtimesByPort.has(port + offset)) {
        available = false
        break
      }
    }
    if (available) return port
  }
  throw new Error('zero-cache CF embed: no instance routing ports are available')
}

export function registerCFInstanceRuntime(
  input: CFInstanceRuntimeInput
): CFInstanceRuntime {
  const instanceId = requireInstanceId(input.instanceId)
  if (runtimesById.has(instanceId)) {
    throw new Error(
      `zero-cache CF embed: instance ${JSON.stringify(instanceId)} is active or still tearing down`
    )
  }

  const encodedId = encodeInstanceId(instanceId)
  if (runtimesByEncodedId.has(encodedId)) {
    throw new Error('zero-cache CF embed: encoded instanceId collision')
  }
  const basePort = allocateBasePort()
  let proxyConnect: ((port: MessagePort) => void) | undefined
  const runtime: CFInstanceRuntime = {
    apiFetch: input.apiFetch,
    apiOriginByHost: new Map(),
    basePort,
    doSqlite: input.doSqlite,
    encodedId,
    env: Object.freeze({ ...input.env }),
    fastifyByPort: new Map(),
    fastifyInstances: new Set(),
    instanceId,
    log: input.log,
    pgPassword: input.pgPassword,
    pgUser: input.pgUser,
    sqliteHandles: new Set(),
    proxyConnect(port) {
      if (!proxyConnect) {
        port.close()
        throw new Error(
          `zero-cache CF embed: postgres proxy is not ready for instance ${JSON.stringify(instanceId)}`
        )
      }
      proxyConnect(port)
    },
  }
  Object.defineProperty(runtime, '__setProxyConnect', {
    value(connect: (port: MessagePort) => void) {
      proxyConnect = connect
    },
  })
  runtimesById.set(instanceId, runtime)
  runtimesByEncodedId.set(encodedId, runtime)
  for (let offset = 0; offset < PORT_STRIDE; offset++) {
    runtimesByPort.set(basePort + offset, runtime)
  }
  logCFInstance(runtime, { event: 'runtime-register', basePort })
  return runtime
}

export function setCFInstanceProxy(
  runtime: CFInstanceRuntime,
  connect: (port: MessagePort) => void
): void {
  const current = runtimesById.get(runtime.instanceId)
  if (current !== runtime) throw new Error('zero-cache CF embed: runtime is not active')
  ;(
    runtime as CFInstanceRuntime & {
      __setProxyConnect(connect: (port: MessagePort) => void): void
    }
  ).__setProxyConnect(connect)
  logCFInstance(runtime, { event: 'postgres-proxy-ready' })
}

export function setCFInstanceEnv(
  runtime: CFInstanceRuntime,
  env: Readonly<Record<string, string>>
): void {
  if (runtimesById.get(runtime.instanceId) !== runtime) {
    throw new Error('zero-cache CF embed: runtime is not active')
  }
  runtime.env = Object.freeze({ ...env })
}

export function setCFInstanceRuntimeStop(
  runtime: CFInstanceRuntime,
  stop: () => Promise<void>
): void {
  if (runtimesById.get(runtime.instanceId) !== runtime) {
    throw new Error('zero-cache CF embed: runtime is not active')
  }
  runtimeStops.set(runtime, stop)
}

export async function stopCFInstanceRuntimeForReplacement(
  instanceId: string
): Promise<void> {
  const normalized = requireInstanceId(instanceId)
  const runtime = runtimesById.get(normalized)
  if (!runtime) return
  const stop = runtimeStops.get(runtime)
  if (!stop) {
    throw new Error(
      `zero-cache CF embed: instance ${JSON.stringify(normalized)} is active or still tearing down`
    )
  }
  let stopError: unknown
  try {
    await stop()
  } catch (error) {
    stopError = error
  }
  if (runtimesById.get(normalized) === runtime) {
    const activeError = new Error(
      `zero-cache CF embed: instance ${JSON.stringify(normalized)} is active or still tearing down`
    )
    if (stopError) {
      throw new AggregateError([activeError, stopError], activeError.message, {
        cause: stopError,
      })
    }
    throw activeError
  }
  if (stopError) {
    logCFInstance(runtime, {
      error: stopError,
      event: 'runtime-replacement-cleanup-failed',
    })
  }
}

export function releaseCFInstanceRuntime(runtime: CFInstanceRuntime): void {
  if (runtimesById.get(runtime.instanceId) !== runtime) return
  logCFInstance(runtime, { event: 'runtime-release' })
  runtimesById.delete(runtime.instanceId)
  runtimesByEncodedId.delete(runtime.encodedId)
  for (let offset = 0; offset < PORT_STRIDE; offset++) {
    if (runtimesByPort.get(runtime.basePort + offset) === runtime) {
      runtimesByPort.delete(runtime.basePort + offset)
    }
  }
  runtime.fastifyByPort.clear()
  runtime.fastifyInstances.clear()
  runtimeStops.delete(runtime)
  for (const host of runtime.apiOriginByHost.keys()) apiRoutesByHost.delete(host)
  runtime.apiOriginByHost.clear()
}

export function logCFInstance(
  runtime: CFInstanceRuntime,
  event: Record<string, unknown>
): void {
  try {
    runtime.log?.({ ...event, instanceId: runtime.instanceId })
  } catch {
    // diagnostics cannot break routing or teardown
  }
}

export function requireCFInstanceRuntime(instanceId: string): CFInstanceRuntime {
  const runtime = runtimesById.get(requireInstanceId(instanceId))
  if (!runtime) {
    throw new Error(
      `zero-cache CF embed: no active runtime for instance ${JSON.stringify(instanceId)}`
    )
  }
  return runtime
}

export function sqlitePathForCFInstance(instanceId: string): string {
  return `${sqliteDirectoryForCFInstance(instanceId)}/replica.db`
}

export function sqliteDirectoryForCFInstance(instanceId: string): string {
  const runtime = requireCFInstanceRuntime(instanceId)
  return `${SQLITE_PREFIX}${runtime.encodedId}`
}

export type CFSqliteRole = 'default' | 'replica-writer'

export function routeCFSqlitePath(path: string): {
  role: CFSqliteRole
  runtime: CFInstanceRuntime
} {
  const url = new URL(path, 'https://orez-sqlite.local')
  if (!url.pathname.startsWith(SQLITE_PREFIX)) {
    throw new Error(`sqlite shim: unroutable zero-cache path ${JSON.stringify(path)}`)
  }
  const encodedId = url.pathname.slice(SQLITE_PREFIX.length).split('/')[0]
  const runtime = runtimesByEncodedId.get(encodedId)
  if (!runtime) {
    throw new Error(`sqlite shim: no active runtime for path ${JSON.stringify(path)}`)
  }
  const role = url.searchParams.get('orezRole')
  if (role !== null && role !== 'replica-writer') {
    throw new Error(`sqlite shim: invalid orezRole ${JSON.stringify(role)}`)
  }
  return { role: role ?? 'default', runtime }
}

export function postgresURLForCFInstance(
  instanceId: string,
  dbName: string,
  pgUser: string
): string {
  const runtime = requireCFInstanceRuntime(instanceId)
  return `postgres://${encodeURIComponent(pgUser)}:ignored@${runtime.encodedId}${POSTGRES_SUFFIX}/${dbName}`
}

export function routeCFPostgresURL(url: string): CFInstanceRuntime {
  const parsed = new URL(url.replace(/^pglite:/, 'postgres:'))
  return routeCFPostgresHost(parsed.hostname)
}

export function routeCFPostgresHost(host: string): CFInstanceRuntime {
  const hostname = host.toLowerCase()
  if (!hostname.endsWith(POSTGRES_SUFFIX)) {
    throw new Error(`postgres-browser: unroutable host ${JSON.stringify(hostname)}`)
  }
  const encodedId = hostname.slice(0, -POSTGRES_SUFFIX.length)
  const runtime = runtimesByEncodedId.get(encodedId)
  if (!runtime) {
    throw new Error(
      `postgres-browser: no active runtime for host ${JSON.stringify(hostname)}`
    )
  }
  return runtime
}

export function apiURLForCFInstance(instanceId: string, input: string): string {
  const runtime = requireCFInstanceRuntime(instanceId)
  const url = new URL(input)
  let routedHost = [...runtime.apiOriginByHost].find(
    ([, origin]) => origin === url.origin
  )?.[0]
  if (!routedHost) {
    routedHost = `${runtime.encodedId}-${runtime.apiOriginByHost.size}${API_SUFFIX}`
    runtime.apiOriginByHost.set(routedHost, url.origin)
    apiRoutesByHost.set(routedHost, { origin: url.origin, runtime })
  }
  url.host = routedHost
  return url.toString()
}

export async function fetchCFInstanceAPI(
  input: RequestInfo | URL,
  init?: RequestInit
): Promise<Response> {
  const request = new Request(input, init)
  const hostname = new URL(request.url).hostname
  const route = apiRoutesByHost.get(hostname)
  if (!hostname.endsWith(API_SUFFIX) || !route) {
    throw new Error(`zero-cache CF API: unroutable host ${JSON.stringify(hostname)}`)
  }
  const { origin, runtime } = route
  if (!runtime?.apiFetch) {
    throw new Error(
      `zero-cache CF API: no fetch handler for host ${JSON.stringify(hostname)}`
    )
  }
  const target = new URL(request.url)
  const originalOrigin = new URL(origin)
  target.protocol = originalOrigin.protocol
  target.host = originalOrigin.host
  const forwarded = new Request(target, request)
  logCFInstance(runtime, {
    event: 'api-fetch',
    routedURL: request.url,
    url: forwarded.url,
  })
  return runtime.apiFetch(forwarded)
}

export function registerCFInstanceFastify(port: number, instance: unknown): void {
  const runtime = runtimesByPort.get(port)
  if (!runtime) throw new Error(`fastify shim: unroutable listen port ${port}`)
  const existing = runtime.fastifyByPort.get(port)
  if (existing && existing !== instance) {
    throw new Error(`fastify shim: port ${port} is already registered`)
  }
  runtime.fastifyByPort.set(port, instance)
  runtime.fastifyInstances.add(instance)
  logCFInstance(runtime, { event: 'fastify-register', port })
}

export function unregisterCFInstanceFastify(instance: unknown): void {
  for (const runtime of runtimesById.values()) {
    if (!runtime.fastifyInstances.delete(instance)) continue
    for (const [port, candidate] of runtime.fastifyByPort) {
      if (candidate === instance) runtime.fastifyByPort.delete(port)
    }
    logCFInstance(runtime, { event: 'fastify-unregister' })
    return
  }
}

export function routeCFInstanceFastifyURL(url: string): {
  instance: unknown
  runtime: CFInstanceRuntime
} {
  const parsed = new URL(url, 'http://localhost')
  const port = Number(parsed.port)
  const runtime = runtimesByPort.get(port)
  if (!runtime) throw new Error(`ws shim: no Fastify runtime for port ${port}`)
  const instance = runtime.fastifyByPort.get(port)
  if (!instance) throw new Error(`ws shim: no Fastify runtime for port ${port}`)
  return { instance, runtime }
}

export function dispatcherFastifyForCFInstance(instanceId: string): unknown {
  const runtime = requireCFInstanceRuntime(instanceId)
  const instance = runtime.fastifyByPort.get(runtime.basePort)
  if (!instance) {
    throw new Error(
      `zero-cache CF embed: dispatcher Fastify is not ready for instance ${JSON.stringify(instanceId)}`
    )
  }
  return instance
}
