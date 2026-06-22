/**
 * stub for Node.js built-in modules not used in SINGLE_PROCESS mode.
 *
 * zero-cache imports modules like node:fs, node:net, node:child_process
 * at the top level, but only uses them in multi-process mode. in
 * SINGLE_PROCESS mode (browser or CF Workers), these imports are dead
 * code. this stub provides empty exports so the bundler can resolve them.
 *
 * usage with bundler alias:
 *   alias: { 'node:fs': 'orez/worker/shims/node-stub' }
 */

// stub for node:http (Server class used in instanceof check)
export class Server {}

type Listener = (...args: unknown[]) => void

// stub for node:events
export class EventEmitter {
  #listeners = new Map<string | symbol, Listener[]>()

  on(event: string | symbol, listener: Listener) {
    const listeners = this.#listeners.get(event) ?? []
    listeners.push(listener)
    this.#listeners.set(event, listeners)
    return this
  }

  once(event: string | symbol, listener: Listener) {
    const wrapped: Listener = (...args) => {
      this.off(event, wrapped)
      listener(...args)
    }
    return this.on(event, wrapped)
  }

  off(event: string | symbol, listener: Listener) {
    const listeners = this.#listeners.get(event)
    if (!listeners) return this
    this.#listeners.set(
      event,
      listeners.filter((candidate) => candidate !== listener)
    )
    return this
  }

  removeListener(event: string | symbol, listener: Listener) {
    return this.off(event, listener)
  }

  emit(event: string | symbol, ...args: unknown[]) {
    for (const listener of this.#listeners.get(event) ?? []) listener(...args)
    return true
  }

  removeAllListeners(event?: string | symbol) {
    if (event === undefined) this.#listeners.clear()
    else this.#listeners.delete(event)
    return this
  }
}

// stub for node:buffer
export const Buffer = globalThis.Buffer

// stub for node:process
const globalProcess = ((globalThis as any).process ??= {})
globalProcess.env ??= {}
globalProcess.pid ??= 1
globalProcess.argv ??= []
globalProcess.version ??= 'v22.0.0'
globalProcess.versions ??= { node: '22.0.0' }
globalProcess.kill ??= () => true
export const env = globalProcess.env
export const pid = globalProcess.pid
export const argv = globalProcess.argv
export const version = globalProcess.version
export const versions = globalProcess.versions
export function kill() {
  return true
}
export function cwd() {
  return '/'
}

// stub for node:fs
export function existsSync() {
  return false
}
export function readFileSync() {
  return ''
}
export function writeFileSync() {}
export function createReadStream() {
  throw new Error('createReadStream() not available in browser')
}
export function rmSync() {}
export function mkdirSync() {}
export function statSync() {
  return { size: 0, isFile: () => false, isDirectory: () => false }
}

// stub for node:fs/promises
export function writeFile() {
  return Promise.resolve()
}
export function readFile() {
  return Promise.resolve('')
}
export function stat() {
  return Promise.resolve({ size: 0, isFile: () => false, isDirectory: () => false })
}
export function mkdir() {
  return Promise.resolve()
}
export function mkdtemp(prefix = '') {
  return Promise.resolve(`${prefix}stub`)
}
export function rm() {
  return Promise.resolve()
}
export function access() {
  return Promise.reject(new Error('not available'))
}

// stub for node:child_process
export function fork() {
  throw new Error('fork() not available in browser')
}
export function spawn() {
  throw new Error('spawn() not available in browser')
}

// stub for node:net
export function createServer() {
  return new Server()
}
export function createConnection() {
  throw new Error('createConnection() not available in browser')
}
export function isIP() {
  return 0
}

// stub for node:http/node:https
export class Agent {
  constructor(_options?: unknown) {}
  addRequest() {}
  destroy() {}
}

// stub for node:os
export function hostname() {
  return 'browser'
}
export function platform() {
  return 'browser'
}
export function tmpdir() {
  return '/tmp'
}
export function homedir() {
  return '/tmp'
}
export function availableParallelism() {
  return 1
}
export function loadavg() {
  return [0, 0, 0]
}
export function uptime() {
  return 0
}
export function totalmem() {
  return 128 * 1024 * 1024
}
export function freemem() {
  return 64 * 1024 * 1024
}
export function cpus() {
  return [{ model: 'worker', speed: 0 }]
}
export function networkInterfaces() {
  return {
    lo: [
      {
        address: '127.0.0.1',
        family: 'IPv4',
        internal: true,
      },
    ],
  }
}

// stub for node:path
export const sep = '/'
export function normalizePath(path: string): string {
  const absolute = path.startsWith('/')
  const parts: string[] = []
  for (const part of path.split('/')) {
    if (!part || part === '.') continue
    if (part === '..') parts.pop()
    else parts.push(part)
  }
  const normalized = parts.join('/')
  return `${absolute ? '/' : ''}${normalized}` || (absolute ? '/' : '.')
}
export const normalize = normalizePath
export function join(...parts: string[]) {
  return normalizePath(parts.filter(Boolean).join('/'))
}
export function resolve(...parts: string[]) {
  const joined = parts.filter(Boolean).join('/')
  return normalizePath(joined.startsWith('/') ? joined : `/${joined}`)
}
export function basename(path: string) {
  const normalized = normalizePath(path)
  return normalized === '/' ? '' : normalized.split('/').pop() || ''
}
export function dirname(path: string) {
  const normalized = normalizePath(path)
  if (normalized === '/') return '/'
  const parts = normalized.split('/')
  parts.pop()
  return parts.join('/') || (normalized.startsWith('/') ? '/' : '.')
}
export function relative(_from: string, to: string) {
  return normalizePath(to)
}
export function extname(path: string) {
  const base = basename(path)
  const dot = base.lastIndexOf('.')
  return dot > 0 ? base.slice(dot) : ''
}

// stub for node:crypto
export function timingSafeEqual(a: unknown, b: unknown) {
  return a === b
}
export function randomBytes(n: number) {
  const arr = new Uint8Array(n)
  crypto.getRandomValues(arr)
  return arr
}
// undici subresource-integrity calls crypto.getHashes() at module-load time
// to decide which SRI algorithms it'll accept (sha256/sha384/sha512). without
// it the worker throws and zero-cache never registers. WebCrypto supports
// all three, so advertise them; SRI itself isn't exercised in our paths.
export function getHashes() {
  return ['sha1', 'sha256', 'sha384', 'sha512']
}
// undici/web modules destructure these from node:crypto at module load.
// real createHash returns a stream-like hasher; we return a no-op shape
// that satisfies the `update().digest()` chain without throwing — actual
// hashing in our paths goes through globalThis.crypto.subtle.
type NoopHasher = {
  update(data?: unknown): NoopHasher
  digest(enc?: string): string
}
export function createHash(_algorithm: string): NoopHasher {
  const noop: NoopHasher = {
    update(_data?: unknown) {
      return noop
    },
    digest(_enc?: string) {
      return ''
    },
  }
  return noop
}
export function createHmac(_algorithm: string, _key: unknown): NoopHasher {
  return createHash('hmac')
}

// stub for node:url
export function fileURLToPath(url: string) {
  return url.replace('file://', '')
}
export function pathToFileURL(path: string) {
  return new URL(`file://${path.startsWith('/') ? path : `/${path}`}`)
}
export function format(value: unknown) {
  return String(value)
}

// stub for node:inspector/promises
export class Session {
  connect() {}
  post() {
    return Promise.resolve()
  }
  disconnect() {}
}

// stub for node:v8
export function getHeapStatistics() {
  const heapSizeLimit = 128 * 1024 * 1024
  return {
    total_heap_size: 64 * 1024 * 1024,
    total_heap_size_executable: 0,
    total_physical_size: 64 * 1024 * 1024,
    total_available_size: 64 * 1024 * 1024,
    used_heap_size: 32 * 1024 * 1024,
    heap_size_limit: heapSizeLimit,
    malloced_memory: 0,
    peak_malloced_memory: 0,
    does_zap_garbage: 0,
    number_of_native_contexts: 1,
    number_of_detached_contexts: 0,
    total_global_handles_size: 0,
    used_global_handles_size: 0,
    external_memory: 0,
  }
}

// stub for node:zlib
export function gzip(_data: unknown, cb: (err: null, result: Uint8Array) => void) {
  cb(null, new Uint8Array(0))
}

// stub for node:util
export function promisify(fn: unknown) {
  return fn
}
export function inspect(obj: unknown) {
  return String(obj)
}
export function stripVTControlCharacters(str: string) {
  return str
}
export function styleText(_format: unknown, text: string) {
  return text
}
export function inherits(ctor: any, superCtor: any) {
  ctor.prototype = Object.create(superCtor.prototype)
  ctor.prototype.constructor = ctor
}
export function deprecate(fn: any) {
  return fn
}
// undici / fetch / websocket diagnostics call util.debuglog('undici') etc. at
// module load time to capture a debug emit hook. without this the worker
// throws "util.debuglog is not a function" before zero-cache can register —
// the IDE then never marks the runtime ready and the preview iframes never
// mount. real Node uses NODE_DEBUG to gate the returned fn; in the worker
// we always no-op (returned fn also exposes the matching `.enabled` flag for
// callers that check it).
const noopDebuglogFn: ((...args: unknown[]) => void) & { enabled: boolean } =
  Object.assign(() => {}, { enabled: false })
export function debuglog(_section?: string, _cb?: (fn: typeof noopDebuglogFn) => void) {
  return noopDebuglogFn
}
export const debug = debuglog
export const types = {
  isProxy: () => false,
  isRegExp: (v: unknown) => v instanceof RegExp,
}

// stub for node:assert
export function strict() {}
export function ok() {}
export function equal() {}
export function deepEqual() {}
export function strictEqual() {}
export function deepStrictEqual() {}

// stub for node:module
export const builtinModules = [
  'assert',
  'async_hooks',
  'buffer',
  'child_process',
  'crypto',
  'diagnostics_channel',
  'dns',
  'events',
  'fs',
  'http',
  'http2',
  'https',
  'module',
  'net',
  'os',
  'path',
  'perf_hooks',
  'process',
  'querystring',
  'readline',
  'stream',
  'stream/promises',
  'timers',
  'tls',
  'tty',
  'url',
  'util',
  'util/types',
  'v8',
  'worker_threads',
  'zlib',
]
const builtinModuleSet = new Set(builtinModules)
export function isBuiltin(name: string) {
  const normalized = name.startsWith('node:') ? name.slice(5) : name
  return builtinModuleSet.has(normalized)
}
export function createRequire() {
  return () => ({})
}

// stub for node:child_process (named exports)
export function execSync() {
  return ''
}

// stub for node:perf_hooks
export const performance = globalThis.performance
export const constants = {
  NODE_PERFORMANCE_GC_MAJOR: 4,
  NODE_PERFORMANCE_GC_MINOR: 1,
  NODE_PERFORMANCE_GC_INCREMENTAL: 8,
  NODE_PERFORMANCE_GC_WEAKCB: 16,
}
export class PerformanceObserver {
  constructor(_cb: any) {}
  observe() {}
  disconnect() {}
}

// stub for node:worker_threads
export class Worker {
  constructor() {
    throw new Error('Worker not available in browser')
  }
}
export const isMainThread = true
export const parentPort = null

// stub for node:async_hooks
// undici/api-request destructures AsyncResource and `class RequestHandler
// extends AsyncResource` at module load — without it the worker throws
// "Class extends value undefined" before zero-cache registers. real
// AsyncResource binds an emit + async store; in the worker we don't have a
// hook tree, so the no-op constructor + minimal surface is enough to keep
// the extends chain valid.
export class AsyncResource {
  constructor(_type?: string, _opts?: unknown) {}
  runInAsyncScope<R>(
    fn: (...args: unknown[]) => R,
    thisArg?: unknown,
    ...args: unknown[]
  ): R {
    return fn.apply(thisArg as object, args)
  }
  emitDestroy() {
    return this
  }
  asyncId() {
    return 0
  }
  triggerAsyncId() {
    return 0
  }
  bind<T extends (...args: unknown[]) => unknown>(fn: T): T {
    return fn
  }
  static bind<T extends (...args: unknown[]) => unknown>(fn: T): T {
    return fn
  }
}

export class AsyncLocalStorage<T = unknown> {
  run<R>(store: T, callback: (...args: unknown[]) => R, ...args: unknown[]): R {
    void store
    return callback(...args)
  }
  getStore(): T | undefined {
    return undefined
  }
  enterWith(_store: T) {}
  disable() {}
}
export function createHook() {
  return {
    enable() {
      return this
    },
    disable() {
      return this
    },
  }
}
export function executionAsyncId() {
  return 0
}
export function triggerAsyncId() {
  return 0
}

// stub for node:diagnostics_channel
export function channel() {
  return {
    name: 'orez-stub',
    hasSubscribers: false,
    publish() {},
    subscribe() {},
    unsubscribe() {},
  }
}
export function hasSubscribers() {
  return false
}
export function subscribe() {}
export function unsubscribe() {}
export function tracingChannel() {
  return channel()
}

// stub for node:dns
export function lookup(_hostname: string, cb?: (err: null, address: string) => void) {
  if (cb) cb(null, '127.0.0.1')
  return Promise.resolve({ address: '127.0.0.1', family: 4 })
}
export function resolve4() {
  return Promise.resolve(['127.0.0.1'])
}
export function resolve6() {
  return Promise.resolve(['::1'])
}

// stub for node:querystring
export function stringify(value: Record<string, unknown>) {
  return new URLSearchParams(
    Object.entries(value).map(([key, val]) => [key, String(val)])
  ).toString()
}
export function parse(value: string) {
  return Object.fromEntries(new URLSearchParams(value))
}
export const escape = encodeURIComponent
export const unescape = decodeURIComponent

// stub for node:stream/promises
export function pipeline(..._args: unknown[]) {
  return Promise.resolve()
}

// stub for node:fs (promises sub-export)
export const promises = {
  readFile: () => Promise.resolve(''),
  writeFile: () => Promise.resolve(),
  stat: () => Promise.resolve({ size: 0 }),
  mkdir,
  mkdtemp,
  rm,
  access,
}

// stub for node:crypto (additional)
export function randomUUID() {
  return crypto.randomUUID()
}

// stub for node:os (additional)
export function arch() {
  return 'wasm'
}
export function release() {
  return '0.0.0'
}

// default export for modules that use default import or CJS require()
export default {
  randomUUID,
  randomBytes,
  timingSafeEqual,
  getHashes,
  createHash,
  createHmac,
  hostname,
  platform,
  tmpdir,
  homedir,
  availableParallelism,
  loadavg,
  uptime,
  totalmem,
  freemem,
  cpus,
  arch,
  release,
  networkInterfaces,
  sep,
  normalize,
  join,
  resolve,
  basename,
  dirname,
  relative,
  extname,
  existsSync,
  readFileSync,
  writeFileSync,
  createReadStream,
  rmSync,
  mkdirSync,
  statSync,
  writeFile,
  readFile,
  mkdir,
  mkdtemp,
  rm,
  access,
  promises,
  fork,
  spawn,
  execSync,
  createServer,
  createConnection,
  isIP,
  Agent,
  fileURLToPath,
  pathToFileURL,
  format,
  promisify,
  inspect,
  stripVTControlCharacters,
  styleText,
  inherits,
  deprecate,
  debuglog,
  debug,
  types,
  performance,
  constants,
  PerformanceObserver,
  getHeapStatistics,
  gzip,
  strict,
  ok,
  equal,
  deepEqual,
  strictEqual,
  deepStrictEqual,
  builtinModules,
  isBuiltin,
  createRequire,
  pipeline,
  Server,
  EventEmitter,
  Buffer,
  env,
  pid,
  argv,
  version,
  versions,
  kill,
  cwd,
  Session,
  Worker,
  isMainThread,
  parentPort,
  AsyncResource,
  AsyncLocalStorage,
  createHook,
  executionAsyncId,
  triggerAsyncId,
  channel,
  hasSubscribers,
  subscribe,
  unsubscribe,
  tracingChannel,
  lookup,
  resolve4,
  resolve6,
  stringify,
  parse,
  escape,
  unescape,
}
