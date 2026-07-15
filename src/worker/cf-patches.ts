/**
 * zero-cache CF Workers overlay.
 *
 * copies @rocicorp/zero's compiled `out/` tree into a generated overlay and
 * applies CF Worker patches there. the installed package in node_modules is
 * never modified.
 *
 * eleven patches:
 * 1. worker-urls.js — replace file:// URLs with zero-worker:// identifiers
 * 2. server worker entrypoints — disable CLI auto-start blocks
 * 3. worker state — localize log contexts and disable process-global telemetry
 * 4. processes.js — replace dynamic import() with static worker module lookup
 * 5. process lifecycle — stop and join a cancelled in-process worker tree
 * 6. write-worker-client.js — run zero-cache's replica writer in-process
 * 7. custom/fetch.js — route mutate/query fetches by logical DO identity
 * 8. initial-sync.js — cap DO batch parameter counts
 * 9. litestream commands.js — retain the durable replica on restart
 * 10. change-streamer-service.js — keep cleanup alive after no-subscriber ticks
 * 11. pgsql-parser — load libpg-query as a precompiled Worker wasm module
 *
 * usage in a worker build script:
 *
 *   import { prepareZeroCacheForCF } from 'orez/worker/cf-patches'
 *   import { getBrowserAliases } from 'orez/worker/browser-build-config'
 *
 *   const zero = prepareZeroCacheForCF({ nodeModulesPath: './node_modules' })
 *   const alias = getBrowserAliases(zero)
 *
 * idempotent: safe to run multiple times. the overlay directory is recreated.
 */

import {
  cpSync,
  existsSync,
  lstatSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs'
import { dirname, resolve } from 'node:path'

import { applyChangeLogCleanupRetryPatch } from '../zero-changelog-cleanup-patch.js'
import { applyLitestreamRestoreGuard } from '../zero-litestream-patch.js'

const ZERO_CACHE_WORKERS = [
  'main',
  'change-streamer',
  'reaper',
  'replicator',
  'syncer',
] as const
const CF_ZERO_VERSION = '1.7.0'

// the stable prefix of every zero-cache worker's import-time auto-start guard.
// flipping the condition to `false` neutralizes the call without depending on
// the (version-volatile) runWorker(...) arguments that follow it.
const WORKER_AUTOSTART_PREFIX = 'if (!singleProcessMode()) exitAfter('
const WORKER_AUTOSTART_DISABLED = 'if (false) /* orez-disable-autostart */ exitAfter('

export interface ZeroCacheCFPrepareOptions {
  nodeModulesPath: string
  outDir?: string
}

export interface ZeroCacheCFPrepareResult {
  nodeModulesPath: string
  outDir: string
  zeroOutDir: string
  zeroCacheSrcDir: string
  aliases: Record<string, string>
}

export function prepareZeroCacheForCF(
  input: string | ZeroCacheCFPrepareOptions
): ZeroCacheCFPrepareResult {
  const options = typeof input === 'string' ? { nodeModulesPath: input } : input
  const nodeModulesPath = resolve(options.nodeModulesPath)
  const outDir = resolve(
    options.outDir ?? resolve(nodeModulesPath, '..', '.orez', 'zero-cache-cf')
  )
  const sourceZeroPackage = resolve(nodeModulesPath, '@rocicorp', 'zero')
  const sourceZeroOut = resolve(nodeModulesPath, '@rocicorp', 'zero', 'out')
  const sourceZeroPackageJson = resolve(sourceZeroPackage, 'package.json')
  const zeroOutDir = resolve(outDir, '@rocicorp', 'zero', 'out')
  const zcBase = resolve(zeroOutDir, 'zero-cache', 'src')

  if (!existsSync(sourceZeroOut)) {
    throw new Error(
      `@rocicorp/zero compiled out/ directory not found at ${sourceZeroOut}`
    )
  }
  const installedZeroVersion = JSON.parse(
    readFileSync(sourceZeroPackageJson, 'utf8')
  ).version
  if (installedZeroVersion !== CF_ZERO_VERSION) {
    throw new Error(
      `orez CF overlay supports @rocicorp/zero ${CF_ZERO_VERSION}, found ${String(installedZeroVersion)}`
    )
  }

  rmSync(outDir, { recursive: true, force: true })
  mkdirSync(dirname(zeroOutDir), { recursive: true })
  cpSync(sourceZeroOut, zeroOutDir, { recursive: true, dereference: true })
  if (existsSync(sourceZeroPackageJson)) {
    cpSync(sourceZeroPackageJson, resolve(outDir, '@rocicorp', 'zero', 'package.json'))
  }
  const overlayNodeModules = resolve(outDir, 'node_modules')
  linkPackageDependencies(
    dirname(dirname(realpathSync(sourceZeroPackage))),
    overlayNodeModules,
    {
      '@rocicorp/zero': resolve(outDir, '@rocicorp', 'zero'),
    }
  )
  linkPackageDependencies(
    resolve(nodeModulesPath, '.bun', 'node_modules'),
    overlayNodeModules,
    {}
  )

  patchWorkerUrls(zcBase)
  patchWorkerEntrypoints(zcBase)
  patchInstanceIsolation(zcBase)
  patchProcesses(zcBase)
  patchStartupShutdown(zcBase)
  patchWriteWorkerClient(zcBase)
  patchCustomFetch(zcBase)
  patchInitialSyncBatchParams(zcBase)
  patchLitestreamRestore(zcBase)
  patchChangeLogCleanupRetry(zcBase)
  const parserAliases = patchPgsqlParserWasm(nodeModulesPath, outDir)
  const packageAliases = getPackageAliases(nodeModulesPath)

  return {
    nodeModulesPath,
    outDir,
    zeroOutDir,
    zeroCacheSrcDir: zcBase,
    aliases: {
      '@rocicorp/zero/out/zero-cache/src/server/runner/run-worker.js': resolve(
        zeroOutDir,
        'zero-cache',
        'src',
        'server',
        'runner',
        'run-worker.js'
      ),
      '@rocicorp/zero/out/zero-cache/src/types/processes.js': resolve(
        zeroOutDir,
        'zero-cache',
        'src',
        'types',
        'processes.js'
      ),
      ...packageAliases,
      ...parserAliases,
    },
  }
}

function patchWorkerUrls(zcBase: string): void {
  const workerUrlsPath = resolve(zcBase, 'server', 'worker-urls.js')
  if (!existsSync(workerUrlsPath)) {
    throw new Error(`orez CF overlay: worker-urls.js missing at ${workerUrlsPath}`)
  }

  const content = readFileSync(workerUrlsPath, 'utf-8')

  // skip only when the verified Zero 1.7 worker graph is already present.
  if (content.includes('zero-worker://') && content.includes('SHADOW_SYNCER_URL')) {
    return
  }

  writeFileSync(
    workerUrlsPath,
    `// patched by orez for CF Workers (replaces file:// URLs with identifiers)
const u = (n) => new URL("zero-worker://" + n);
export const MAIN_URL = u("main");
export const CHANGE_STREAMER_URL = u("change-streamer");
export const REAPER_URL = u("reaper");
export const REPLICATOR_URL = u("replicator");
export const SHADOW_SYNCER_URL = u("shadow-syncer");
export const SYNCER_URL = u("syncer");
// write-worker is spawned via 'new Worker()' (node:worker_threads), not via
// childWorker() — it uses its own URL → worker resolution path. we still expose
// it here so write-worker-client.js can import it without throwing.
export const WRITE_WORKER_URL = u("write-worker");
`
  )
  console.log('[orez] patched zero-cache worker-urls.js')
}

function patchWorkerEntrypoints(zcBase: string): void {
  for (const entrypoint of ZERO_CACHE_WORKERS) {
    const entrypointPath = resolve(zcBase, 'server', `${entrypoint}.js`)
    if (!existsSync(entrypointPath)) {
      throw new Error(`orez CF overlay: worker entrypoint missing at ${entrypointPath}`)
    }

    const code = readFileSync(entrypointPath, 'utf-8')
    if (code.includes('orez-disable-autostart')) {
      continue
    }

    // every entrypoint auto-starts itself at import time via
    //   if (!singleProcessMode()) exitAfter(lc, () => runWorker(...))
    // the runWorker(...) call shape varies per worker and across zero versions
    // (1.6 added the `lc` arg to exitAfter, kept `...process.argv.slice(2)` on
    // some workers, and wrapped change-streamer's runWorker in a `.catch()` that
    // publishes startup errors). matching the whole call is brittle, so we only
    // touch the stable guard prefix: flipping the condition to `false` turns the
    // entire statement into dead code regardless of its arguments. CF embeds run
    // runWorker explicitly via childWorker, so nothing is lost.
    if (!code.includes(WORKER_AUTOSTART_PREFIX)) {
      throw new Error(`orez CF overlay: auto-start guard missing in ${entrypoint}.js`)
    }

    const next = code.split(WORKER_AUTOSTART_PREFIX).join(WORKER_AUTOSTART_DISABLED)
    writeFileSync(entrypointPath, next)
    console.log(`[orez] patched zero-cache ${entrypoint}.js (disabled auto-start)`)
  }
}

function patchInstanceIsolation(zcBase: string): void {
  for (const entrypoint of ZERO_CACHE_WORKERS) {
    const entrypointPath = resolve(zcBase, 'server', `${entrypoint}.js`)
    let code = readFileSync(entrypointPath, 'utf8')
    if (code.includes('orez-instance-local-log-context')) continue
    const declaration = 'var lc = new LogContext("info", {}, consoleLogSink);'
    const assignment = '\tlc = createLogContext('
    if (code.split(declaration).length - 1 !== 1) {
      throw new Error(
        `orez CF overlay: expected one log context declaration in ${entrypoint}.js`
      )
    }
    if (code.split(assignment).length - 1 !== 1) {
      throw new Error(
        `orez CF overlay: expected one log context assignment in ${entrypoint}.js`
      )
    }
    code = code
      .replace(
        declaration,
        '/* orez-instance-local-log-context */\nconst lc = new LogContext("info", {}, consoleLogSink);'
      )
      .replace(assignment, '\tconst lc = createLogContext(')
    writeFileSync(entrypointPath, code)
  }

  const eventsPath = resolve(zcBase, 'observability', 'events.js')
  const otelPath = resolve(zcBase, 'server', 'otel-start.js')
  const anonymousPath = resolve(zcBase, 'server', 'anonymous-otel-start.js')
  const dotenvPath = resolve(zcBase, '..', '..', 'shared', 'src', 'dotenv.js')
  for (const path of [eventsPath, otelPath, anonymousPath, dotenvPath]) {
    if (!existsSync(path)) {
      throw new Error(`orez CF overlay: telemetry module missing at ${path}`)
    }
  }
  writeFileSync(
    eventsPath,
    `// orez-instance-isolated-events: CF embeds do not share process event sinks.\nfunction initEventSink() {}\nasync function publishCriticalEvent(lc, event) {\n  lc.info?.(\`ZeroEvent: \${event.type}\`, event);\n}\nfunction makeErrorDetails(value) {\n  const error = value instanceof Error ? value : new Error(String(value));\n  return { name: error.name, message: error.message, stack: error.stack };\n}\nexport { initEventSink, makeErrorDetails, publishCriticalEvent };\n`
  )
  writeFileSync(
    otelPath,
    `// orez-instance-isolated-otel: process-global OTel is disabled in CF embeds.\nfunction startOtelAuto() {}\nexport { startOtelAuto };\n`
  )
  writeFileSync(
    anonymousPath,
    `// orez-instance-isolated-anonymous-telemetry: process singleton disabled in CF embeds.\nconst noop = () => {};\nexport { noop as recordConnectionAttempted, noop as recordConnectionSuccess, noop as recordMutation, noop as recordQuery, noop as recordRowsSynced, noop as setActiveClientGroupsGetter, noop as setActiveUsersGetter, noop as startAnonymousTelemetry };\n`
  )
  writeFileSync(dotenvPath, '// orez-cf-dotenv-disabled: env is passed explicitly.\n')
}

function patchProcesses(zcBase: string): void {
  const processesPath = resolve(zcBase, 'types', 'processes.js')
  if (!existsSync(processesPath)) {
    throw new Error(`orez CF overlay: processes.js missing at ${processesPath}`)
  }

  let code = readFileSync(processesPath, 'utf-8')
  const proxyGetAnchor = 'return Reflect.get(target, prop, receiver);'
  const proxyGetReplacement =
    'const value = Reflect.get(target, prop, target); ' +
    'if (typeof value !== "function") return value; ' +
    'return (...args) => { const result = value.apply(target, args); ' +
    'return result === target ? receiver : result; };'
  const patchProcessWrapperProxy = (source: string): string => {
    const anchorCount = source.split(proxyGetAnchor).length - 1
    if (anchorCount === 1) return source.replace(proxyGetAnchor, proxyGetReplacement)
    if (anchorCount === 0 && source.includes(proxyGetReplacement)) return source
    throw new Error('orez CF overlay: process wrapper proxy anchor missing')
  }

  if (code.includes('__zc_workers')) {
    for (const marker of [
      'waitForOrezZeroWorkersStopped',
      '__orez_latched_signal',
      '__orez_track_worker',
    ]) {
      if (!code.includes(marker)) {
        throw new Error(`orez CF overlay: existing processes patch missing ${marker}`)
      }
    }
    const patched = patchProcessWrapperProxy(code)
    if (patched !== code) writeFileSync(processesPath, patched)
    return
  }

  // add static imports of all zero-cache worker modules at the top.
  // these are relative to processes.js location in @rocicorp/zero.
  // NOTE: mutator.js and write-worker.js don't export a default `runWorker`
  // (mutator runs via auto-run guard, write-worker spawns via node:worker_threads
  // not via childWorker()), so they're not in the lookup table.
  const workerImports = `\
// patched by orez for CF Workers (static imports replace dynamic import())
import { default as __zc_main } from "../server/main.js";
import { default as __zc_change_streamer } from "../server/change-streamer.js";
import { default as __zc_reaper } from "../server/reaper.js";
import { default as __zc_replicator } from "../server/replicator.js";
import { default as __zc_syncer } from "../server/syncer.js";
const __zc_workers = {
  "main": __zc_main,
  "change-streamer": __zc_change_streamer,
  "reaper": __zc_reaper,
  "replicator": __zc_replicator,
  "syncer": __zc_syncer,
};
const __orez_shutdown_signals = new Set(["SIGINT", "SIGTERM", "SIGQUIT", "SIGABRT"]);
const __orez_latched_signal = Symbol("orez-latched-signal");
const __orez_worker_executions = new Map();
function __orez_track_worker(task, execution) {
  let active = __orez_worker_executions.get(task);
  if (!active) __orez_worker_executions.set(task, active = new Set());
  active.add(execution);
  void execution.finally(() => {
    active.delete(execution);
    if (active.size === 0) __orez_worker_executions.delete(task);
  }).catch(() => {});
  return execution;
}
async function waitForOrezZeroWorkersStopped(task) {
  for (;;) {
    const active = __orez_worker_executions.get(task);
    if (!active?.size) return;
    await Promise.allSettled([...active]);
  }
}
`

  // replace the dynamic import in childWorker with a synchronous lookup.
  // original: import(moduleUrl.href).then(async ({ default: runWorker }) => ...
  // patched:  lookup __zc_workers by name, then continue as before
  const dynamicImportPattern =
    'import(moduleUrl.href).then(async ({ default: runWorker })'
  const staticLookup =
    '((async () => { ' +
    'const _name = moduleUrl.hostname || moduleUrl.pathname.split("/").pop()?.replace(".js",""); ' +
    'const _debug = env.OREZ_DEBUG_WIRE === "1"; ' +
    'const _task = env.ZERO_TASK_ID || _name; ' +
    'if (_debug) { ' +
    'console.debug("[orez-zc-worker] start", _task, _name, args); ' +
    'child.on("message", (msg) => console.debug("[orez-zc-worker] message", _task, _name, msg)); ' +
    'child.on("error", (err) => console.error("[orez-zc-worker] error", _task, _name, err)); ' +
    'child.on("close", (code, signal) => console.debug("[orez-zc-worker] close", _task, _name, code, signal)); ' +
    '} ' +
    'const runWorkerImpl = __zc_workers[_name]; ' +
    'if (!runWorkerImpl) throw new Error("orez: unknown zero-cache worker: " + _name + " (available: " + Object.keys(__zc_workers).join(", ") + ")"); ' +
    'const runWorker = (...runArgs) => __orez_track_worker(_task, Promise.resolve().then(() => runWorkerImpl(...runArgs))); ' +
    'return { default: runWorker, name: _name }; ' +
    '})()).then(async ({ default: runWorker, name })'

  if (!code.includes(dynamicImportPattern)) {
    throw new Error('orez CF overlay: dynamic import anchor missing in processes.js')
  }

  code = patchProcessWrapperProxy(code.replace(dynamicImportPattern, staticLookup))

  const signalKill =
    'const kill = (dest) => (signal = "SIGTERM") => dest.emit(signal, signal);'
  const latchedSignalKill = `const kill = (dest) => (signal = "SIGTERM") => {
  dest[__orez_latched_signal] = signal;
  dest.emit(signal, signal);
};`
  if (!code.includes(signalKill)) {
    throw new Error('orez CF overlay: in-process signal anchor missing')
  }
  code = code.replace(signalKill, latchedSignalKill)

  const wrapSwitch = 'switch (prop) {'
  const wrapSwitchReplacement = `switch (prop) {
      case "on":
      case "once": return (type, handler) => {
        target[prop](type, handler);
        if (__orez_shutdown_signals.has(type) && target[__orez_latched_signal] === type) {
          queueMicrotask(() => {
            if (prop === "once") target.off(type, handler);
            handler(type);
          });
        }
        return receiver;
      };`
  if (code.split(wrapSwitch).length - 1 !== 1) {
    throw new Error('orez CF overlay: process wrapper switch anchor missing')
  }
  code = code.replace(wrapSwitch, wrapSwitchReplacement)

  const exportAnchor = 'export { childWorker, parentWorker, singleProcessMode };'
  if (!code.includes(exportAnchor)) {
    throw new Error('orez CF overlay: process export anchor missing')
  }
  code = code.replace(
    exportAnchor,
    'export { childWorker, parentWorker, singleProcessMode, waitForOrezZeroWorkersStopped };'
  )
  code = workerImports + code
  writeFileSync(processesPath, code)
  console.log('[orez] patched zero-cache processes.js (static worker imports)')
}

function patchStartupShutdown(zcBase: string): void {
  const lifecyclePath = resolve(zcBase, 'services', 'life-cycle.js')
  if (!existsSync(lifecyclePath)) {
    throw new Error(`orez CF overlay: life-cycle.js missing at ${lifecyclePath}`)
  }

  const marker = 'OrezZeroStartupStoppedError'
  let code = readFileSync(lifecyclePath, 'utf-8')
  if (code.includes(marker)) {
    if (!code.includes('if (stopPromise) await stopPromise')) {
      throw new Error('orez CF overlay: existing lifecycle patch does not join shutdown')
    }
    patchMainStartupShutdown(zcBase)
    return
  }

  const previous = `\tasync allWorkersReady() {
\t\tawait Promise.all(this.#ready);
\t}`
  const patched = `\tasync allWorkersReady() {
\t\tawait Promise.race([
\t\t\tPromise.all(this.#ready),
\t\t\tthis.#runningState.stopped().then(() => {
\t\t\t\tconst error = new Error("zero-cache startup stopped before workers became ready");
\t\t\t\terror.name = "${marker}";
\t\t\t\tthrow error;
\t\t\t}),
\t\t]);
\t}`
  if (!code.includes(previous)) {
    throw new Error('orez CF overlay: allWorkersReady lifecycle anchor missing')
  }

  code = code.replace(previous, patched)

  const runUntilKilledStart = `async function runUntilKilled(lc, parent, ...services) {
\tif (services.length === 0) return;
\tfor (const signal of [...GRACEFUL_SHUTDOWN, ...FORCEFUL_SHUTDOWN]) parent.once(signal, () => {
\t\tconst GRACEFUL_SIGNALS = GRACEFUL_SHUTDOWN;
\t\tservices.forEach(async (svc) => {
\t\t\tif (GRACEFUL_SIGNALS.includes(signal) && svc.drain) {
\t\t\t\tlc.info?.(\`draining \${svc.constructor.name} \${svc.id} (\${signal})\`);
\t\t\t\tawait svc.drain();
\t\t\t}
\t\t\tlc.info?.(\`stopping \${svc.constructor.name} \${svc.id} (\${signal})\`);
\t\t\tawait svc.stop();
\t\t});
\t});`
  const joinedRunUntilKilledStart = `async function runUntilKilled(lc, parent, ...services) {
\tif (services.length === 0) return;
\tlet stopPromise;
\tfor (const signal of [...GRACEFUL_SHUTDOWN, ...FORCEFUL_SHUTDOWN]) parent.once(signal, () => {
\t\tconst GRACEFUL_SIGNALS = GRACEFUL_SHUTDOWN;
\t\tstopPromise ??= Promise.all(services.map(async (svc) => {
\t\t\tif (GRACEFUL_SIGNALS.includes(signal) && svc.drain) {
\t\t\t\tlc.info?.(\`draining \${svc.constructor.name} \${svc.id} (\${signal})\`);
\t\t\t\tawait svc.drain();
\t\t\t}
\t\t\tlc.info?.(\`stopping \${svc.constructor.name} \${svc.id} (\${signal})\`);
\t\t\tawait svc.stop();
\t\t}));
\t});`
  if (!code.includes(runUntilKilledStart)) {
    throw new Error('orez CF overlay: runUntilKilled shutdown anchor missing')
  }
  code = code.replace(runUntilKilledStart, joinedRunUntilKilledStart)

  const serviceStopped =
    '\t\tconst svc = await Promise.race(services.map((svc) => svc.run().then(() => svc)));\n\t\tlc.info?.(`${svc.constructor.name} (${svc.id}) stopped`);'
  const serviceCleanupJoined =
    '\t\tconst svc = await Promise.race(services.map((svc) => svc.run().then(() => svc)));\n\t\tif (stopPromise) await stopPromise;\n\t\tlc.info?.(`${svc.constructor.name} (${svc.id}) stopped`);'
  if (!code.includes(serviceStopped)) {
    throw new Error('orez CF overlay: runUntilKilled completion anchor missing')
  }
  code = code.replace(serviceStopped, serviceCleanupJoined)
  writeFileSync(lifecyclePath, code)

  patchMainStartupShutdown(zcBase)
  console.log('[orez] patched zero-cache startup shutdown')
}

function patchMainStartupShutdown(zcBase: string): void {
  const mainPath = resolve(zcBase, 'server', 'main.js')
  let code = readFileSync(mainPath, 'utf-8')
  const marker = '__orezAwaitStartup'
  if (code.includes(marker)) return

  const manager = '\tconst processes = new ProcessManager(lc, parent);'
  const managerWithBarrier = `${manager}
\tconst ${marker} = (promise) => Promise.race([promise, processes.done().then(() => {
\t\tconst error = new Error("zero-cache startup stopped before workers became ready");
\t\terror.name = "OrezZeroStartupStoppedError";
\t\tthrow error;
\t})]);`
  if (!code.includes(manager)) {
    throw new Error('orez CF overlay: main process manager anchor missing')
  }
  code = code.replace(manager, managerWithBarrier)

  const startupAwaits = [
    [
      'await restoreReplica(lc, config, null)',
      `${marker}(restoreReplica(lc, config, null))`,
    ],
    ['await changeStreamerReady', `${marker}(changeStreamerReady)`],
    ['await backupReady', `${marker}(backupReady)`],
    ['await reaperReady', `${marker}(reaperReady)`],
    ['await shadowReady', `${marker}(shadowReady)`],
    ['await replicaReady', `${marker}(replicaReady)`],
  ] as const
  for (const [previousAwait, nextAwait] of startupAwaits) {
    const count = code.split(previousAwait).length - 1
    if (count !== 1) {
      throw new Error(
        `orez CF overlay: expected one main startup await anchor for ${previousAwait}`
      )
    }
    code = code.replace(previousAwait, `await ${nextAwait}`)
  }
  writeFileSync(mainPath, code)
}

function patchWriteWorkerClient(zcBase: string): void {
  const clientPath = resolve(zcBase, 'services', 'replicator', 'write-worker-client.js')
  if (!existsSync(clientPath)) {
    throw new Error(`orez CF overlay: write-worker-client.js missing at ${clientPath}`)
  }

  let code = readFileSync(clientPath, 'utf-8')
  if (
    code.includes('orez-inline-write-worker') &&
    code.includes('orezRole=replica-writer')
  ) {
    return
  }

  if (
    !code.includes('orez-inline-write-worker') &&
    !code.includes('import { Worker } from "node:worker_threads";')
  ) {
    throw new Error(
      'orez CF overlay: node:worker_threads anchor missing in write-worker-client.js'
    )
  }

  writeFileSync(
    clientPath,
    `// patched by orez for CF Workers (orez-inline-write-worker)
import { must } from "../../../../shared/src/must.js";
import { Database } from "../../../../zqlite/src/db.js";
import { createLogContext } from "../../server/logging.js";
import { StatementRunner } from "../../db/statements.js";
import { getSubscriptionState } from "./schema/replication-state.js";
import { ChangeProcessor } from "./change-processor.js";

function applyPragmas(db, pragmas) {
  db.pragma(\`busy_timeout = \${pragmas.busyTimeout}\`);
  db.pragma(\`analysis_limit = \${pragmas.analysisLimit}\`);
  if (pragmas.walAutocheckpoint !== void 0) {
    db.pragma(\`wal_autocheckpoint = \${pragmas.walAutocheckpoint}\`);
  }
}

// re-exported to keep this module's surface identical to upstream
// write-worker-client.js (write-worker.js imports it). orez runs the writer
// inline so the thread boundary that uses it is gone, but a complete export
// surface keeps the bundler from choking if it pulls write-worker.js in.
function serializeError(err) {
  if (!(err instanceof Error)) return {
    name: "Error",
    message: String(err),
    details: err && typeof err === "object" ? { ...err } : void 0
  };
  const details = Object.fromEntries(Object.getOwnPropertyNames(err).filter((key) => ![
    "name",
    "message",
    "stack",
    "cause"
  ].includes(key)).map((key) => [key, err[key]]));
  const cause = err.cause instanceof Error ? serializeError(err.cause) : err.cause === void 0 ? void 0 : String(err.cause);
  return {
    name: err.name,
    message: err.message,
    stack: err.stack,
    cause,
    details: Object.keys(details).length ? details : void 0
  };
}

function createAPI(onWriteError) {
  let db;
  let runner;
  let processor;
  let mode;
  let lc;

  function createProcessor() {
    processor = new ChangeProcessor(must(runner), must(mode), (_lc, err) => {
      onWriteError(err instanceof Error ? err : new Error(String(err)));
    });
  }

  return {
    init(dbPath, cpMode, pragmas, logConfig) {
      lc = createLogContext({ log: logConfig }, "write-worker");
      const separator = dbPath.includes("?") ? "&" : "?";
      db = new Database(lc, dbPath + separator + "orezRole=replica-writer");
      applyPragmas(db, pragmas);
      runner = new StatementRunner(db);
      mode = cpMode;
      createProcessor();
    },
    getSubscriptionState() {
      return getSubscriptionState(must(runner));
    },
    processMessage(downstream) {
      return must(processor).processMessage(must(lc), downstream);
    },
    abort() {
      must(processor).abort(must(lc));
      createProcessor();
    },
    stop() {
      db?.close();
      db = void 0;
      runner = void 0;
      processor = void 0;
    },
  };
}

class ThreadWriteWorkerClient {
  #api;
  #errorHandler = () => {};
  #writeError = null;

  constructor() {
    this.#api = createAPI((err) => {
      this.#writeError = err;
      this.#errorHandler(err);
    });
  }

  async #call(method, args) {
    if (this.#writeError) throw this.#writeError;
    try {
      const result = this.#api[method](...args);
      if (this.#writeError) throw this.#writeError;
      return result;
    } catch (err) {
      throw err instanceof Error ? err : new Error(String(err));
    }
  }

  init(dbPath, mode, pragmas, logConfig) {
    return this.#call("init", [dbPath, mode, pragmas, logConfig]);
  }

  getSubscriptionState() {
    return this.#call("getSubscriptionState", []);
  }

  processMessage(downstream) {
    return this.#call("processMessage", [downstream]);
  }

  abort() {
    if (!this.#writeError) {
      try {
        this.#api.abort();
      } catch {
      }
    }
  }

  async stop() {
    await this.#call("stop", []);
  }

  onError(handler) {
    this.#errorHandler = handler;
  }
}

export { ThreadWriteWorkerClient, applyPragmas, serializeError };
`
  )
  console.log('[orez] patched zero-cache write-worker-client.js (inline writer)')
}

function patchCustomFetch(zcBase: string): void {
  const customFetchPath = resolve(zcBase, 'custom', 'fetch.js')
  if (!existsSync(customFetchPath)) {
    throw new Error(`orez CF overlay: custom/fetch.js missing at ${customFetchPath}`)
  }
  let code = readFileSync(customFetchPath, 'utf8')
  if (code.includes('__orezFetchCFInstanceAPI')) {
    if (
      code.includes('fetchCFInstanceAPI as __orezFetchCFInstanceAPI') &&
      code.includes('await __orezFetchCFInstanceAPI(finalUrl, {')
    ) {
      return
    }
    throw new Error('orez CF overlay: custom fetch patch is incomplete')
  }

  const anchor = 'const response = await fetch(finalUrl, {'
  const count = code.split(anchor).length - 1
  if (count !== 1) {
    throw new Error(`orez CF overlay: expected one custom fetch anchor, found ${count}`)
  }
  code =
    'import { fetchCFInstanceAPI as __orezFetchCFInstanceAPI } from "orez/worker/cf-instance-runtime";\n' +
    code.replace(anchor, 'const response = await __orezFetchCFInstanceAPI(finalUrl, {')
  writeFileSync(customFetchPath, code)
  console.log('[orez] patched zero-cache custom fetch (instance-routed API host)')
}

// zero-cache's initial sync prepares a fixed 50-row multi-row INSERT per
// copied table (insertSql + `,${valuesSql}`.repeat(49)). better-sqlite3
// accepts thousands of bound parameters, but Cloudflare DO SQLite caps a
// statement at 100 — the prepare alone throws "too many SQL variables" for
// any table wider than 2 columns, killing initial sync before the first row.
// derive the batch row count from the column count under a 96-param budget:
// identical inserts, smaller batches.
function patchInitialSyncBatchParams(zcBase: string): void {
  const initialSyncPath = resolve(
    zcBase,
    'services',
    'change-source',
    'pg',
    'initial-sync.js'
  )
  if (!existsSync(initialSyncPath)) {
    throw new Error(`orez CF overlay: initial-sync.js missing at ${initialSyncPath}`)
  }
  let code = readFileSync(initialSyncPath, 'utf-8')
  if (code.includes('orezRowsPerBatch')) return
  // each pattern appears once in copyBinary and once in copyText
  const replacements: Array<[string, string]> = [
    [
      'const insertBatchStmt = to.prepare(insertSql + `,${valuesSql}`.repeat(49));',
      'const orezRowsPerBatch = Math.max(1, Math.floor(96 / Math.max(1, columnNames.length)));\n\tconst insertBatchStmt = to.prepare(insertSql + `,${valuesSql}`.repeat(orezRowsPerBatch - 1));',
    ],
    [
      'const valuesPerBatch = valuesPerRow * 50;',
      'const valuesPerBatch = valuesPerRow * orezRowsPerBatch;',
    ],
    [
      'for (; pendingRows > 50; pendingRows -= 50) insertBatchStmt.run(pendingValues.slice(l, l += valuesPerBatch));',
      'for (; pendingRows > orezRowsPerBatch; pendingRows -= orezRowsPerBatch) insertBatchStmt.run(pendingValues.slice(l, l += valuesPerBatch));',
    ],
  ]
  for (const [from, to] of replacements) {
    const count = code.split(from).length - 1
    if (count !== 2) {
      throw new Error(
        `orez CF overlay: expected 2 initial-sync batch anchors, found ${count}`
      )
    }
    code = code.replaceAll(from, to)
  }
  writeFileSync(initialSyncPath, code)
  console.log(
    '[orez] patched zero-cache initial-sync.js (DO bound-parameter cap batches)'
  )
}

// the dedicated change-streamer calls restoreReplica() on every start; with no
// litestream backup configured it throws ("Missing --litestream-executable")
// and "recovers" by wiping + fully resyncing the replica — on workerd that
// discards the durable DO-sqlite replica on EVERY embed cold boot, turning each
// idle-teardown wake into a full resync. the node path patches this in-place
// via disableZeroLitestreamRestore(); the CF overlay needs the same guard here.
function patchLitestreamRestore(zcBase: string): void {
  const commandsPath = resolve(zcBase, 'services', 'litestream', 'commands.js')
  if (!existsSync(commandsPath)) {
    throw new Error(`orez CF overlay: litestream commands.js missing at ${commandsPath}`)
  }
  applyLitestreamRestoreGuard(commandsPath)
  console.log('[orez] patched zero-cache litestream commands.js (no-op restore)')
}

function patchChangeLogCleanupRetry(zcBase: string): void {
  const servicePath = resolve(
    zcBase,
    'services',
    'change-streamer',
    'change-streamer-service.js'
  )
  if (!existsSync(servicePath)) {
    throw new Error(`orez CF overlay: change-streamer service missing at ${servicePath}`)
  }
  applyChangeLogCleanupRetryPatch(servicePath)
  console.log('[orez] patched zero-cache changeLog cleanup retry')
}

function patchPgsqlParserWasm(
  nodeModulesPath: string,
  outDir: string
): Record<string, string> {
  const sourcePackagePath = findPackageRoot(nodeModulesPath, 'libpg-query')
  if (!sourcePackagePath) {
    throw new Error('orez CF overlay: libpg-query package is missing')
  }
  const targetPackagePath = resolve(outDir, 'node_modules', 'libpg-query')
  const parserIndexPath = resolve(targetPackagePath, 'wasm', 'index.js')
  const wasmPath = resolve(targetPackagePath, 'wasm', 'libpg-query.wasm')

  rmSync(targetPackagePath, { recursive: true, force: true })
  mkdirSync(dirname(targetPackagePath), { recursive: true })
  cpSync(sourcePackagePath, targetPackagePath, { recursive: true, dereference: true })

  if (!existsSync(parserIndexPath) || !existsSync(wasmPath)) {
    throw new Error('orez CF overlay: libpg-query wasm files are missing')
  }

  let code = readFileSync(parserIndexPath, 'utf-8')
  if (
    code.includes('orez-libpg-query-wasm-binary') &&
    code.includes('__orezLibPgQueryInit')
  ) {
    return {
      'libpg-query': targetPackagePath,
      'libpg-query/wasm': parserIndexPath,
      'libpg-query/wasm/index.js': parserIndexPath,
    }
  }
  if (code.includes('orez-libpg-query-wasm-binary')) {
    throw new Error(
      'orez CF overlay: libpg-query wasm loader has an unsupported older patch'
    )
  }

  const pattern = 'const initPromise = PgQueryModule().then((module) => {'
  if (!code.includes(pattern)) {
    throw new Error(
      'orez CF overlay: PgQueryModule init anchor missing in libpg-query wasm index'
    )
  }

  // workerd forbids compiling wasm from bytes at runtime (WebAssembly.instantiate
  // on raw bytes throws "Wasm code generation disallowed by embedder"), so we do
  // NOT embed the parser bytes and hand them to Emscripten. instead import the
  // parser wasm as a module — the CF bundler attaches it as a CompiledWasm worker
  // module (it lives next to this overlay at wasm/libpg-query.wasm) — and give
  // Emscripten a ready instance through its standard instantiateWasm hook.
  const replacement = `\
// orez-libpg-query-wasm-binary: precompiled parser wasm module for CF Workers.
import __orezLibPgQueryWasmModule from 'libpg-query/wasm/libpg-query.wasm';
try {
    const g = globalThis;
    if (g.self && !g.self.location) g.self.location = { href: 'https://orez.local/libpg-query.js' };
    if (!g.location) g.location = { href: 'https://orez.local/libpg-query.js' };
}
catch {
}
const __orezLibPgQueryPreviousProcessType = globalThis.process?.type;
if (globalThis.process && !globalThis.process.type) globalThis.process.type = 'renderer';
const __orezLibPgQueryInit = PgQueryModule({
    instantiateWasm(imports, receiveInstance) {
        const instance = new WebAssembly.Instance(__orezLibPgQueryWasmModule, imports);
        receiveInstance(instance, __orezLibPgQueryWasmModule);
        return instance.exports;
    },
});
if (globalThis.process && __orezLibPgQueryPreviousProcessType === undefined) {
    delete globalThis.process.type;
}
else if (globalThis.process) {
    globalThis.process.type = __orezLibPgQueryPreviousProcessType;
}
const initPromise = __orezLibPgQueryInit.then((module) => {`

  code = code.replace(pattern, replacement)
  writeFileSync(parserIndexPath, code)
  console.log('[orez] patched libpg-query wasm loader (precompiled wasm module)')
  return {
    'libpg-query': targetPackagePath,
    'libpg-query/wasm': parserIndexPath,
    'libpg-query/wasm/index.js': parserIndexPath,
  }
}

function findPackageRoot(nodeModulesPath: string, packageName: string): string | null {
  const candidates = [
    resolve(nodeModulesPath, packageName),
    resolve(nodeModulesPath, '.bun', 'node_modules', packageName),
    ...findBunPackageCandidates(nodeModulesPath, packageName),
  ]

  for (const candidate of candidates) {
    if (existsSync(resolve(candidate, 'package.json'))) {
      return candidate
    }
  }

  return null
}

function findBunPackageCandidates(
  nodeModulesPath: string,
  packageName: string
): string[] {
  const bunDir = resolve(nodeModulesPath, '.bun')
  if (!existsSync(bunDir)) return []

  const packageKey = packageName.replace('/', '+')
  return readdirSync(bunDir)
    .filter((entry) => entry === packageKey || entry.startsWith(`${packageKey}@`))
    .map((entry) => resolve(bunDir, entry, 'node_modules', packageName))
}

function getPackageAliases(nodeModulesPath: string): Record<string, string> {
  const aliases: Record<string, string> = {}
  const packageAliases: Record<string, string> = {
    'postgres-real': 'postgres',
    'readable-stream': 'readable-stream',
    '@pgsql/types': '@pgsql/types',
    '@opentelemetry/semantic-conventions': '@opentelemetry/semantic-conventions',
  }

  for (const [alias, packageName] of Object.entries(packageAliases)) {
    const packageRoot = findPackageRoot(nodeModulesPath, packageName)
    if (packageRoot) aliases[alias] = packageRoot
  }

  return aliases
}

function linkPackageDependencies(
  sourceRoot: string,
  targetRoot: string,
  overrides: Record<string, string>
): void {
  if (!existsSync(sourceRoot)) return
  mkdirSync(targetRoot, { recursive: true })

  for (const entry of readdirSync(sourceRoot)) {
    if (entry.startsWith('.')) continue
    const sourceEntry = resolve(sourceRoot, entry)
    const sourceEntryStat = lstatSync(sourceEntry)
    if (!sourceEntryStat.isDirectory() && !sourceEntryStat.isSymbolicLink()) {
      continue
    }

    if (entry.startsWith('@')) {
      const targetScope = resolve(targetRoot, entry)
      mkdirSync(targetScope, { recursive: true })
      for (const scopedEntry of readdirSync(sourceEntry)) {
        const packageName = `${entry}/${scopedEntry}`
        const sourcePackage = resolve(sourceEntry, scopedEntry)
        const targetPackage = resolve(targetScope, scopedEntry)
        linkPackage(
          overrides[packageName] ?? sourcePackage,
          targetPackage,
          Boolean(overrides[packageName])
        )
      }
      continue
    }

    linkPackage(overrides[entry] ?? sourceEntry, resolve(targetRoot, entry), false)
  }

  for (const [packageName, target] of Object.entries(overrides)) {
    const targetPackage = resolve(targetRoot, ...packageName.split('/'))
    linkPackage(target, targetPackage, true)
  }
}

function linkPackage(source: string, target: string, replace: boolean): void {
  if (replace) {
    rmSync(target, { recursive: true, force: true })
  } else if (existsSync(target)) {
    return
  }
  mkdirSync(dirname(target), { recursive: true })
  symlinkSync(source, target, 'dir')
}
