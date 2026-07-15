import { copyFileSync, mkdirSync, realpathSync, rmSync } from 'node:fs'
import { dirname, resolve } from 'node:path'

import { build, type Plugin } from 'esbuild'

import {
  getBrowserAliases,
  getBrowserDefine,
} from '../../src/worker/browser-build-config.js'
import { prepareZeroCacheForCF } from '../../src/worker/cf-patches.js'

const root = resolve(import.meta.dir, '../..')
const generatedDir = resolve(import.meta.dir, '.generated')
const nodeModulesPath = realpathSync(resolve(root, 'node_modules'))
rmSync(generatedDir, { recursive: true, force: true })
mkdirSync(generatedDir, { recursive: true })

const originalLog = console.log
console.log = console.error
let zeroOverlay: ReturnType<typeof prepareZeroCacheForCF>
try {
  zeroOverlay = prepareZeroCacheForCF({
    nodeModulesPath,
    outDir: resolve(generatedDir, 'zero-cache-cf'),
  })
} finally {
  console.log = originalLog
}
const aliases = getBrowserAliases(zeroOverlay)
for (const specifier of [
  'node:stream',
  'stream',
  'node:stream/promises',
  'stream/promises',
]) {
  delete aliases[specifier]
}
for (const [specifier, target] of Object.entries(aliases)) {
  if (target.startsWith('orez/')) {
    aliases[specifier] = resolve(root, 'src', `${target.slice('orez/'.length)}.ts`)
  }
}
delete aliases['node:events']
delete aliases.events

const wasmSource = resolve(
  zeroOverlay.outDir,
  'node_modules/libpg-query/wasm/libpg-query.wasm'
)
const wasmTarget = resolve(generatedDir, 'libpg-query.wasm')
const wasmPlugin: Plugin = {
  name: 'compiled-wasm',
  setup(buildApi) {
    buildApi.onResolve({ filter: /^libpg-query\/wasm\/libpg-query\.wasm$/ }, () => {
      mkdirSync(dirname(wasmTarget), { recursive: true })
      copyFileSync(wasmSource, wasmTarget)
      return { path: './libpg-query.wasm', external: true }
    })
  },
}

const virtualModules: Record<string, string> = {
  events: `
    export class EventEmitter {
      constructor() { this._events = new Map() }
      on(event, listener) { const list = this._events.get(event) || []; list.push(listener); this._events.set(event, list); return this }
      once(event, listener) { const wrapped = (...args) => { this.off(event, wrapped); listener(...args) }; return this.on(event, wrapped) }
      off(event, listener) { const list = this._events.get(event); if (list) this._events.set(event, list.filter((item) => item !== listener)); return this }
      removeListener(event, listener) { return this.off(event, listener) }
      removeAllListeners(event) { if (event === undefined) this._events.clear(); else this._events.delete(event); return this }
      emit(event, ...args) { for (const listener of this._events.get(event) || []) listener(...args); return true }
    }
    export default EventEmitter
  `,
  'node:events': `
    export class EventEmitter {
      constructor() { this._events = new Map() }
      on(event, listener) { const list = this._events.get(event) || []; list.push(listener); this._events.set(event, list); return this }
      once(event, listener) { const wrapped = (...args) => { this.off(event, wrapped); listener(...args) }; return this.on(event, wrapped) }
      off(event, listener) { const list = this._events.get(event); if (list) this._events.set(event, list.filter((item) => item !== listener)); return this }
      removeListener(event, listener) { return this.off(event, listener) }
      removeAllListeners(event) { if (event === undefined) this._events.clear(); else this._events.delete(event); return this }
      emit(event, ...args) { for (const listener of this._events.get(event) || []) listener(...args); return true }
    }
    export default EventEmitter
  `,
  '@fastify/websocket': 'export default function websocket() {}',
  '@opentelemetry/api': `
    const noopSpan = { setAttribute() { return this }, setAttributes() { return this }, addEvent() { return this }, recordException() { return this }, setStatus() { return this }, end() {}, spanContext() { return {} } }
    export const ROOT_CONTEXT = {}
    export const SpanStatusCode = { OK: 1, ERROR: 2, UNSET: 0 }
    export const DiagLogLevel = { NONE: 0, ERROR: 30, WARN: 50, INFO: 60, DEBUG: 70, VERBOSE: 80, ALL: 9999 }
    export const context = { active() { return ROOT_CONTEXT }, with(_ctx, fn, thisArg, ...args) { return fn.apply(thisArg, args) }, bind(_ctx, target) { return target } }
    export const propagation = { inject() {}, extract(ctx) { return ctx || ROOT_CONTEXT } }
    export const trace = { getTracer() { return { startSpan() { return noopSpan }, startActiveSpan(_name, a, b, c) { const fn = typeof a === 'function' ? a : typeof b === 'function' ? b : c; return fn(noopSpan) } } }, setSpan(ctx) { return ctx }, getSpan() { return noopSpan } }
    export const metrics = { getMeter() { return { createCounter() { return { add() {} } }, createUpDownCounter() { return { add() {} } }, createHistogram() { return { record() {} } }, createGauge() { return { record() {} } }, createObservableGauge() { return { addCallback() {} } }, createObservableCounter() { return { addCallback() {} } } } } }
    export const diag = { setLogger() {}, debug() {}, info() {}, warn() {}, error() {}, verbose() {} }
  `,
  '@opentelemetry/api-logs': `
    export const SeverityNumber = { TRACE: 1, DEBUG: 5, INFO: 9, WARN: 13, ERROR: 17, FATAL: 21 }
    export const logs = { getLogger() { return { emit() {} } } }
  `,
  '@opentelemetry/auto-instrumentations-node':
    'export function getNodeAutoInstrumentations() { return [] }',
  '@opentelemetry/exporter-metrics-otlp-http':
    'export class OTLPMetricExporter { constructor() {} }',
  '@opentelemetry/resources':
    'export function resourceFromAttributes(attributes) { return { attributes } }',
  '@opentelemetry/sdk-metrics': `
    export class MeterProvider { constructor() {} getMeter() { return { createObservableGauge() { return { addCallback() {} } }, createObservableCounter() { return { addCallback() {} } } } } async shutdown() {} }
    export class PeriodicExportingMetricReader { constructor() {} }
  `,
  '@opentelemetry/sdk-node':
    'export class NodeSDK { constructor() {} start() {} async shutdown() {} }',
  '@opentelemetry/semantic-conventions':
    "export const ATTR_SERVICE_VERSION = 'service.version'",
}
const virtualPlugin: Plugin = {
  name: 'workerd-virtual-modules',
  setup(buildApi) {
    for (const specifier of Object.keys(virtualModules)) {
      const filter = new RegExp(`^${specifier.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`)
      buildApi.onResolve({ filter }, () => ({
        path: specifier,
        namespace: 'workerd-virtual',
      }))
    }
    buildApi.onLoad({ filter: /.*/, namespace: 'workerd-virtual' }, (args) => ({
      contents: virtualModules[args.path],
      loader: 'js',
    }))
  },
}

await build({
  entryPoints: [resolve(import.meta.dir, 'production-shape-worker.ts')],
  outfile: resolve(generatedDir, 'production-shape-worker.js'),
  bundle: true,
  format: 'esm',
  platform: 'browser',
  target: 'es2022',
  conditions: ['workerd', 'worker', 'browser', 'import'],
  mainFields: ['browser', 'module', 'main'],
  alias: aliases,
  external: [
    'cloudflare:*',
    'stream',
    'stream/promises',
    'node:stream',
    'node:stream/promises',
  ],
  define: getBrowserDefine(),
  plugins: [virtualPlugin, wasmPlugin],
  logLevel: 'warning',
})
