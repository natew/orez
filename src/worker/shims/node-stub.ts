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

// stub for node:fs
export function existsSync() {
  return false
}
export function readFileSync() {
  return ''
}
export function writeFileSync() {}
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
export function availableParallelism() {
  return 1
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

// stub for node:url
export function fileURLToPath(url: string) {
  return url.replace('file://', '')
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
  return { total_heap_size: 0, used_heap_size: 0 }
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
export function inherits(ctor: any, superCtor: any) {
  ctor.prototype = Object.create(superCtor.prototype)
  ctor.prototype.constructor = ctor
}
export function deprecate(fn: any) {
  return fn
}
export const types = {
  isProxy: () => false,
  isRegExp: (v: unknown) => v instanceof RegExp,
}

// stub for node:assert
export function strict() {}
export function ok() {}

// stub for node:module
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

// stub for node:stream/promises
export function pipeline(..._args: unknown[]) {
  return Promise.resolve()
}

// stub for node:fs (promises sub-export)
export const promises = {
  readFile: () => Promise.resolve(''),
  writeFile: () => Promise.resolve(),
  stat: () => Promise.resolve({ size: 0 }),
  mkdir: () => Promise.resolve(),
  rm: () => Promise.resolve(),
  access: () => Promise.reject(new Error('not available')),
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
  hostname,
  platform,
  tmpdir,
  availableParallelism,
  arch,
  release,
  existsSync,
  readFileSync,
  writeFileSync,
  rmSync,
  mkdirSync,
  statSync,
  writeFile,
  readFile,
  promises,
  fork,
  spawn,
  execSync,
  createServer,
  createConnection,
  fileURLToPath,
  promisify,
  inspect,
  stripVTControlCharacters,
  inherits,
  deprecate,
  types,
  performance,
  constants,
  PerformanceObserver,
  getHeapStatistics,
  gzip,
  strict,
  ok,
  createRequire,
  pipeline,
  Server,
  Session,
}
