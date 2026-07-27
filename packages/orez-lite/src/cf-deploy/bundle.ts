import {
  copyFileSync,
  existsSync,
  readFileSync,
  readdirSync,
  realpathSync,
  rmSync,
  writeFileSync,
} from 'fs'
import { createRequire } from 'module'
import { basename, dirname, join } from 'path'

import { build, type Plugin } from 'esbuild'

import { NODE_EXTERNALS, isOrezLiteImporter, resolveAlias } from './leaves.js'
import { pruneUnreachableWorkerModules, shimWorkerCreateRequire } from './prune.js'

const WORKER_DEFINE = {
  'process.env.NODE_ENV': '"development"',
  'process.env.SINGLE_PROCESS': '"1"',
  'process.versions.node': '"20.0.0"',
}

import type { CloudflareConfig } from './config.js'
import type { WorkerChunkPruneSignatures } from './prune.js'

export interface BundleCloudflareWorkerOptions {
  workerDir: string
  entryPoint: string
  outfile: string
  writeMigrationModule: (workerDir: string) => Promise<string>
  chunkPruneSignatures?: WorkerChunkPruneSignatures
  define?: Readonly<Record<string, string>>
}

// react's build-context marker packages, resolved for the SERVER context these
// workers are. 'server-only' ships a throwing default entry (its guard targets
// client bundles); under this bundle's [workerd, worker, import] conditions it
// resolves to that throw and kills any importing module at init — in prod this
// took out the data-worker push handler for every custom mutator (2026-07-09).
// the DO/data worker is a server context, so the marker must be a no-op here.
// unlike the generic aliases below, these apply to EVERY importer (consumer
// code, not just orez internals). scripts/check/zero-push-graph.ts asserts this
// map covers any marker package the push graph imports.
export const SERVER_CONTEXT_STUBS: Record<string, string> = {
  'server-only': 'export {}\n',
}

export function orezLiteAliasPlugin(
  cfg: CloudflareConfig,
  aliases: Record<string, string>,
  resolveDir: string,
  nodeModulesPath: string,
  wasmOutDir = resolveDir
): Plugin {
  const entries = Object.entries(aliases)
  const utilVirtualModule =
    'export function promisify(fn) { return fn }\nexport function inspect(value) { try { return JSON.stringify(value) } catch { return String(value) } }\nexport function format(...args) { return args.map((arg) => typeof arg === "string" ? arg : inspect(arg)).join(" ") }\nexport function stripVTControlCharacters(value) { return value }\nexport function styleText(_format, text) { return text }\nexport function inherits(ctor, superCtor) { ctor.prototype = Object.create(superCtor.prototype); ctor.prototype.constructor = ctor }\nexport function deprecate(fn) { return fn }\nexport const types = { isDate: (value) => value instanceof Date, isRegExp: (value) => value instanceof RegExp, isTypedArray: (value) => ArrayBuffer.isView(value) && !(value instanceof DataView), isProxy: () => false }\nexport default { promisify, inspect, format, stripVTControlCharacters, styleText, inherits, deprecate, types }\n'
  const eventsVirtualModule =
    'export class EventEmitter { constructor() { this._events = new Map() } on(event, listener) { const list = this._events.get(event) || []; list.push(listener); this._events.set(event, list); return this } once(event, listener) { const wrapped = (...args) => { this.off(event, wrapped); listener(...args) }; return this.on(event, wrapped) } off(event, listener) { const list = this._events.get(event); if (list) this._events.set(event, list.filter((item) => item !== listener)); return this } removeListener(event, listener) { return this.off(event, listener) } removeAllListeners(event) { if (event === undefined) this._events.clear(); else this._events.delete(event); return this } emit(event, ...args) { for (const listener of this._events.get(event) || []) listener(...args); return true } }\nexport default EventEmitter\n'
  const fsVirtualModule =
    'const missing = () => new Error("filesystem is not available in Cloudflare Workers")\nexport function existsSync() { return false }\nexport function readFileSync() { return "" }\nexport function writeFileSync() {}\nexport function mkdirSync() {}\nexport function rmSync() {}\nexport function statSync() { return { size: 0, mode: 0, isFile: () => false, isDirectory: () => false } }\nexport function stat(_path, callback) { const error = missing(); if (callback) { queueMicrotask(() => callback(error)); return } return Promise.reject(error) }\nexport function createReadStream() { return { on() { return this }, once() { return this }, pipe(target) { return target }, destroy() {} } }\nexport function readFile() { return Promise.resolve("") }\nexport function writeFile() { return Promise.resolve() }\nexport function mkdir() { return Promise.resolve() }\nexport function mkdtemp(prefix = "") { return Promise.resolve(`${prefix}stub`) }\nexport function rm() { return Promise.resolve() }\nexport function access() { return Promise.reject(missing()) }\nexport const promises = { readFile, writeFile, mkdir, mkdtemp, rm, stat: () => Promise.reject(missing()), access }\nexport default { existsSync, readFileSync, writeFileSync, mkdirSync, rmSync, statSync, stat, createReadStream, readFile, writeFile, mkdir, mkdtemp, rm, access, promises }\n'
  const pathVirtualModule =
    'export const sep = "/"\nfunction normalizePath(path) { const absolute = String(path).startsWith("/"); const parts = []; for (const part of String(path).split("/")) { if (!part || part === ".") continue; if (part === "..") parts.pop(); else parts.push(part) } const normalized = parts.join("/"); return `${absolute ? "/" : ""}${normalized}` || (absolute ? "/" : ".") }\nexport const normalize = normalizePath\nexport function join(...parts) { return normalizePath(parts.filter(Boolean).join("/")) }\nexport function resolve(...parts) { const joined = parts.filter(Boolean).join("/"); return normalizePath(joined.startsWith("/") ? joined : `/${joined}`) }\nexport function basename(path) { const normalized = normalizePath(path); return normalized === "/" ? "" : normalized.split("/").pop() || "" }\nexport function dirname(path) { const normalized = normalizePath(path); if (normalized === "/") return "/"; const parts = normalized.split("/"); parts.pop(); return parts.join("/") || (normalized.startsWith("/") ? "/" : ".") }\nexport function relative(_from, to) { return normalizePath(to) }\nexport function extname(path) { const base = basename(path); const dot = base.lastIndexOf("."); return dot > 0 ? base.slice(dot) : "" }\nexport default { sep, normalize, join, resolve, basename, dirname, relative, extname }\n'
  const bufferVirtualModule =
    'export const Buffer = globalThis.Buffer\nexport default { Buffer }\n'
  const virtualModules: Record<string, string> = {
    buffer: bufferVirtualModule,
    'node:buffer': bufferVirtualModule,
    util: utilVirtualModule,
    'node:util': utilVirtualModule,
    events: eventsVirtualModule,
    'node:events': eventsVirtualModule,
    fs: fsVirtualModule,
    'node:fs': fsVirtualModule,
    'fs/promises': fsVirtualModule,
    'node:fs/promises': fsVirtualModule,
    path: pathVirtualModule,
    'node:path': pathVirtualModule,
    '@dotenvx/dotenvx':
      'const empty = { parsed: {}, error: undefined }\nexport function config() { return empty }\nexport function configDotenv() { return empty }\nexport function parse() { return {} }\nexport function populate(target, source = {}) { if (target && source && typeof target === "object") Object.assign(target, source); return target }\nexport function decrypt() { return "" }\nexport default { config, configDotenv, parse, populate, decrypt }\n',
    '@fastify/websocket': 'export default function websocket() {}\n',
    '@opentelemetry/api':
      'const noopSpan = { setAttribute() { return this }, setAttributes() { return this }, addEvent() { return this }, recordException() { return this }, setStatus() { return this }, end() {}, spanContext() { return {} } }\nexport const ROOT_CONTEXT = {}\nexport const SpanStatusCode = { OK: 1, ERROR: 2, UNSET: 0 }\nexport const DiagLogLevel = { NONE: 0, ERROR: 30, WARN: 50, INFO: 60, DEBUG: 70, VERBOSE: 80, ALL: 9999 }\nexport const context = { active() { return ROOT_CONTEXT }, with(_ctx, fn, thisArg, ...args) { return fn.apply(thisArg, args) }, bind(_ctx, target) { return target } }\nexport const propagation = { inject() {}, extract(_ctx) { return _ctx || ROOT_CONTEXT } }\nexport const trace = { getTracer() { return { startSpan() { return noopSpan }, startActiveSpan(_name, a, b, c) { const fn = typeof a === "function" ? a : typeof b === "function" ? b : c; return fn(noopSpan) } } }, setSpan(ctx) { return ctx }, getSpan() { return noopSpan } }\nexport const metrics = { getMeter() { return { createCounter() { return { add() {} } }, createUpDownCounter() { return { add() {} } }, createHistogram() { return { record() {} } }, createGauge() { return { record() {} } }, createObservableGauge() { return { addCallback() {} } }, createObservableCounter() { return { addCallback() {} } } } } }\nexport const diag = { setLogger() {}, debug() {}, info() {}, warn() {}, error() {}, verbose() {} }\n',
    '@opentelemetry/api-logs':
      'export const SeverityNumber = { TRACE: 1, DEBUG: 5, INFO: 9, WARN: 13, ERROR: 17, FATAL: 21 }\nexport const logs = { getLogger() { return { emit() {} } } }\n',
    '@opentelemetry/auto-instrumentations-node':
      'export function getNodeAutoInstrumentations() { return [] }\n',
    '@opentelemetry/exporter-metrics-otlp-http':
      'export class OTLPMetricExporter { constructor() {} }\n',
    '@opentelemetry/resources':
      'export function resourceFromAttributes(attributes) { return { attributes } }\n',
    '@opentelemetry/sdk-metrics':
      'export class MeterProvider { constructor() {} getMeter() { return { createObservableGauge() { return { addCallback() {} } }, createObservableCounter() { return { addCallback() {} } } } } async shutdown() {} }\nexport class PeriodicExportingMetricReader { constructor() {} }\n',
    '@opentelemetry/sdk-node':
      'export class NodeSDK { constructor() {} start() {} async shutdown() {} }\n',
    '@opentelemetry/semantic-conventions':
      "export const ATTR_SERVICE_VERSION = 'service.version'\n",
    'chalk-template':
      'export function template(strings, ...values) { return String.raw({ raw: strings }, ...values) }\nexport default template\n',
    'command-line-usage': 'export default function commandLineUsage() { return "" }\n',
    'is-in-subnet':
      'export function isIPv6(value) { return String(value).includes(":") }\nexport function isPrivate() { return false }\nexport function isReserved() { return false }\n',
    ...SERVER_CONTEXT_STUBS,
  }
  return {
    name: 'orez-lite-aliases',
    setup(buildApi) {
      // consumer-declared .wasm modules (cfg.compiledWasmModules) → attach each as a
      // CompiledWasm worker module: resolve the specifier (honoring its package
      // exports), copy the file next to the worker output, and rewrite the import to
      // a relative external specifier. wrangler's CompiledWasm rule then turns it
      // into a WebAssembly.Module at runtime (workerd forbids compiling wasm from
      // bytes). app-neutral — every specifier comes from the consumer's config, not
      // baked in here. each onResolve only fires when that specifier is imported.
      const wasmModules = cfg.compiledWasmModules ?? []
      if (wasmModules.length > 0) {
        const requireFrom = createRequire(join(nodeModulesPath, '..', '_orez-cf.cjs'))
        for (const specifier of wasmModules) {
          const filter = new RegExp(
            `^${specifier.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`
          )
          buildApi.onResolve({ filter }, () => {
            const real = requireFrom.resolve(specifier)
            const name = basename(real)
            copyFileSync(real, join(wasmOutDir, name))
            return { path: `./${name}`, external: true }
          })
        }
      }
      for (const specifier of Object.keys(virtualModules)) {
        const filter = new RegExp(`^${specifier.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`)
        buildApi.onResolve({ filter }, (args) => {
          // server-context stubs apply regardless of importer: consumer app
          // code (e.g. an auth module's `import 'server-only'`) must hit the
          // no-op, not the throwing client-guard entry
          if (specifier in SERVER_CONTEXT_STUBS) {
            return { path: specifier, namespace: 'orez-lite-virtual' }
          }
          if (args.importer && !isOrezLiteImporter(args.importer)) return undefined
          return { path: specifier, namespace: 'orez-lite-virtual' }
        })
      }
      buildApi.onLoad({ filter: /.*/, namespace: 'orez-lite-virtual' }, (args) => ({
        contents: virtualModules[args.path],
        loader: 'js',
        resolveDir,
      }))

      for (const [specifier, target] of entries) {
        const filter = new RegExp(`^${specifier.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`)
        buildApi.onResolve({ filter }, async (args) => {
          const isVirtualBuildModule = specifier.startsWith('orez:')
          if (
            !isVirtualBuildModule &&
            args.importer &&
            !isOrezLiteImporter(args.importer)
          ) {
            return undefined
          }
          return (
            (await resolveAlias(buildApi, target, resolveDir, args.kind)) ?? {
              path: target,
            }
          )
        })
      }
    },
  }
}

/** bundle a One app with SQL storage but without the zero-cache runtime. */
export async function bundleCloudflareLiteAppWorker(
  cfg: CloudflareConfig,
  options: BundleCloudflareWorkerOptions
) {
  const { workerDir } = options
  const nodeModulesPath = realpathSync(
    [
      join(workerDir, 'node_modules'),
      join(workerDir, '..', '..', 'node_modules'),
      join(process.cwd(), 'node_modules'),
    ].find((candidate) => existsSync(candidate)) || join(process.cwd(), 'node_modules')
  )
  return bundleCloudflareSplitAppWorker(cfg, options, nodeModulesPath, {
    'orez:application-worker': join(workerDir, 'one-app.js'),
    'orez:cloudflare-migrations': join(workerDir, 'orez-migrations.js'),
  })
}

async function bundleCloudflareSplitAppWorker(
  cfg: CloudflareConfig,
  options: BundleCloudflareWorkerOptions,
  nodeModulesPath: string,
  aliases: Record<string, string>
) {
  const { workerDir, entryPoint, outfile, writeMigrationModule } = options
  await writeMigrationModule(workerDir)
  // Expose One's worker output under a stable name so the entrypoint can dynamically
  // import it (getOneWorker -> './one-app.js'). esbuild code-splitting then emits
  // it as its own lazily-evaluated chunk, so the DO isolates (schema migration /
  // SQL backend) never evaluate the One app graph — the fix for the 128 MiB DO
  // OOM. without code-splitting, a single-outfile bundle inlines + eagerly
  // evaluates the dynamic import, which is what OOMed the DO.
  const oneAppPath = join(workerDir, 'one-app.js')
  copyFileSync(join(workerDir, 'index.js'), oneAppPath)
  // Workers node compatibility provides streams. leaving these external avoids
  // pulling readable-stream's browser dependency graph into the DO bundle.
  for (const key of [
    'node:stream',
    'stream',
    'node:stream/promises',
    'stream/promises',
    'readable-stream',
  ]) {
    delete aliases[key]
  }
  // split output into a directory so dynamic imports become real lazy chunks.
  const outDir = join(workerDir, '.do-bundle')
  if (existsSync(outDir)) rmSync(outDir, { recursive: true, force: true })
  await build({
    entryPoints: [entryPoint],
    outdir: outDir,
    entryNames: 'index',
    bundle: true,
    splitting: true,
    format: 'esm',
    platform: 'neutral',
    target: 'es2022',
    conditions: ['workerd', 'worker', 'import'],
    mainFields: ['browser', 'module', 'main'],
    external: ['cloudflare:*', './assets/*', ...NODE_EXTERNALS],
    define: {
      ...WORKER_DEFINE,
      ...options.define,
      // shim the CJS module globals, which are undefined in this workerd ESM
      // bundle (platform: 'neutral' does not inject them). a bundled dep that
      // references __filename/__dirname — common in RN/node libraries pulled
      // into the SSR worker — otherwise throws "__filename is not defined" at
      // worker startup, failing deploy validation.
      __filename: JSON.stringify('index.js'),
      __dirname: JSON.stringify('/'),
    },
    plugins: [orezLiteAliasPlugin(cfg, aliases, workerDir, nodeModulesPath, workerDir)],
    logLevel: 'silent',
  })
  const oneAppChunks = readdirSync(outDir).filter((name) =>
    /^one-app-[A-Za-z0-9_-]+\.js$/.test(name)
  )
  if (oneAppChunks.length > 1) {
    throw new Error(
      `expected at most one lazy one-app chunk, found ${oneAppChunks.length}: ${oneAppChunks.join(', ')}`
    )
  }
  const oneAppChunk = oneAppChunks[0]
  if (oneAppChunk) {
    // Dynamic Orez Lite entrypoints emit one lazy app chunk. One's external asset
    // modules import the entry's helpers back from ../index.js, so retarget
    // those references to that chunk. A static entrypoint emits no chunk and
    // needs no rewrite.
    const assetsDir = join(workerDir, 'assets')
    if (existsSync(assetsDir)) {
      for (const name of readdirSync(assetsDir)) {
        if (!name.endsWith('.js') && !name.endsWith('.mjs')) continue
        const file = join(assetsDir, name)
        const source = readFileSync(file, 'utf8')
        const rewritten = source.replace(
          /(\b(?:from|import)\s*\(?\s*)(['"])\.\.\/index\.js\2/g,
          `$1$2../${oneAppChunk}$2`
        )
        if (rewritten !== source) writeFileSync(file, rewritten)
      }
    }
  }
  rmSync(oneAppPath, { force: true })
  // The caller-owned entrypoint plus the migration + schema-version modules it
  // statically imports are esbuild INPUTS: esbuild inlined their code into
  // index.js, so the root source files are orphans. leave them and wrangler's
  // general `*-*.js` module rule (they all contain a dash) attaches dead
  // duplicates into every DO isolate — resident weight against the 128 MiB DO
  // budget the code-splitting above exists to protect. rm them like one-app.js.
  rmSync(join(workerDir, 'orez-migrations.js'), { force: true })
  rmSync(join(workerDir, 'orez-schema-version.js'), { force: true })
  // collect the split output: index.js (entry) + chunk-*.js (lazy graphs) ->
  // place them where wrangler attaches modules. the entry goes to `outfile`; the
  // chunks go next to it (the rules glob `assets/**` doesn't cover them, so they
  // sit beside index.js in dist/worker and the `./chunk-*.js` relative imports
  // resolve).
  for (const name of readdirSync(outDir)) {
    const src = join(outDir, name)
    if (name === 'index.js') {
      copyFileSync(src, outfile)
    } else {
      copyFileSync(src, join(workerDir, name))
    }
  }
  rmSync(outDir, { recursive: true, force: true })

  // prune asset modules UNREACHABLE from the worker entry's import graph.
  // wrangler's `assets/**/*.js` rule attaches every chunk in assets/ as a worker
  // module, and workerd holds ALL attached modules resident in EVERY isolate.
  // hundreds of those are orphans (client-only / dead route chunks the server
  // never imports). that resident-module weight — not the migration — is what
  // OOMs the 128 MiB DO isolate. attach only what the entry actually reaches.
  pruneUnreachableWorkerModules(workerDir, basename(outfile))

  // neutralize the CJS-interop banner that throws at module eval in workerd.
  shimWorkerCreateRequire(workerDir)
}

/** Bundle a data worker that owns the authoritative SQLite Durable Object. */
export async function bundleCloudflareLiteDataWorker(
  cfg: CloudflareConfig,
  options: BundleCloudflareWorkerOptions
) {
  const { workerDir, entryPoint, outfile, writeMigrationModule } = options
  const nodeModulesPath = realpathSync(
    [
      join(workerDir, 'node_modules'),
      join(workerDir, '..', '..', 'node_modules'),
      join(process.cwd(), 'node_modules'),
    ].find((candidate) => existsSync(candidate)) || join(process.cwd(), 'node_modules')
  )
  await writeMigrationModule(workerDir)
  await build({
    entryPoints: [entryPoint],
    outfile,
    bundle: true,
    format: 'esm',
    platform: 'neutral',
    target: 'es2022',
    conditions: ['workerd', 'worker', 'import'],
    mainFields: ['browser', 'module', 'main'],
    external: ['cloudflare:*', ...NODE_EXTERNALS],
    define: {
      ...WORKER_DEFINE,
      ...options.define,
      __filename: JSON.stringify('index.js'),
      __dirname: JSON.stringify('/'),
    },
    plugins: [
      orezLiteAliasPlugin(
        cfg,
        {
          'orez:cloudflare-migrations': join(workerDir, 'orez-migrations.js'),
        },
        workerDir,
        nodeModulesPath,
        dirname(outfile)
      ),
    ],
    logLevel: 'silent',
  })
}
