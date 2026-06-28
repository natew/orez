/**
 * zero-cache CF Workers overlay.
 *
 * copies @rocicorp/zero's compiled `out/` tree into a generated overlay and
 * applies CF Worker patches there. the installed package in node_modules is
 * never modified.
 *
 * seven patches:
 * 1. worker-urls.js — replace file:// URLs with zero-worker:// identifiers
 * 2. server worker entrypoints — disable CLI auto-start blocks
 * 3. processes.js — replace dynamic import() with static worker module lookup
 * 4. write-worker-client.js — run zero-cache's replica writer in-process
 * 5. initial-sync.js — cap DO batch parameter counts
 * 6. change-streamer-service.js — keep cleanup alive after no-subscriber ticks
 * 7. pgsql-parser — embed libpg-query wasm bytes for Workers
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
  patchProcesses(zcBase)
  patchWriteWorkerClient(zcBase)
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
      ...packageAliases,
      ...parserAliases,
    },
  }
}

function patchWorkerUrls(zcBase: string): void {
  const workerUrlsPath = resolve(zcBase, 'server', 'worker-urls.js')
  if (!existsSync(workerUrlsPath)) {
    console.warn('[orez] worker-urls.js not found at', workerUrlsPath)
    return
  }

  const content = readFileSync(workerUrlsPath, 'utf-8')

  // skip only when the current Zero 1.5 worker graph is already present.
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
      console.warn('[orez] zero-cache worker entrypoint not found at', entrypointPath)
      continue
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
      console.warn(
        `[orez] could not find auto-start guard in ${entrypoint}.js. ` +
          'zero-cache version may have changed — check compatibility.'
      )
      continue
    }

    const next = code.split(WORKER_AUTOSTART_PREFIX).join(WORKER_AUTOSTART_DISABLED)
    writeFileSync(entrypointPath, next)
    console.log(`[orez] patched zero-cache ${entrypoint}.js (disabled auto-start)`)
  }
}

function patchProcesses(zcBase: string): void {
  const processesPath = resolve(zcBase, 'types', 'processes.js')
  if (!existsSync(processesPath)) {
    console.warn('[orez] processes.js not found at', processesPath)
    return
  }

  let code = readFileSync(processesPath, 'utf-8')

  if (code.includes('__zc_workers')) {
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
`

  // replace the dynamic import in childWorker with a synchronous lookup.
  // original: import(moduleUrl.href).then(async ({ default: runWorker }) => ...
  // patched:  lookup __zc_workers by name, then continue as before
  const dynamicImportPattern =
    'import(moduleUrl.href).then(async ({ default: runWorker })'
  const staticLookup =
    '((async () => { ' +
    'const _name = moduleUrl.hostname || moduleUrl.pathname.split("/").pop()?.replace(".js",""); ' +
    'const _debug = process.env.OREZ_DEBUG_WIRE === "1" || globalThis.__OREZ_DEBUG_WIRE__ === true; ' +
    'if (_debug) { ' +
    'console.debug("[orez-zc-worker] start", _name, args); ' +
    'child.on("message", (msg) => console.debug("[orez-zc-worker] message", _name, msg)); ' +
    'child.on("error", (err) => console.error("[orez-zc-worker] error", _name, err)); ' +
    'child.on("close", (code, signal) => console.debug("[orez-zc-worker] close", _name, code, signal)); ' +
    '} ' +
    'const runWorker = __zc_workers[_name]; ' +
    'if (!runWorker) throw new Error("orez: unknown zero-cache worker: " + _name + " (available: " + Object.keys(__zc_workers).join(", ") + ")"); ' +
    'return { default: runWorker, name: _name }; ' +
    '})()).then(async ({ default: runWorker, name })'

  if (!code.includes(dynamicImportPattern)) {
    console.warn(
      '[orez] could not find dynamic import pattern in processes.js. ' +
        'zero-cache version may have changed — check compatibility.'
    )
    return
  }

  code = workerImports + code.replace(dynamicImportPattern, staticLookup)
  writeFileSync(processesPath, code)
  console.log('[orez] patched zero-cache processes.js (static worker imports)')
}

function patchWriteWorkerClient(zcBase: string): void {
  const clientPath = resolve(zcBase, 'services', 'replicator', 'write-worker-client.js')
  if (!existsSync(clientPath)) {
    console.warn('[orez] write-worker-client.js not found at', clientPath)
    return
  }

  let code = readFileSync(clientPath, 'utf-8')
  if (
    code.includes('orez-inline-write-worker') &&
    code.includes('__orez_zero_sqlite_role')
  ) {
    return
  }

  if (
    !code.includes('orez-inline-write-worker') &&
    !code.includes('import { Worker } from "node:worker_threads";')
  ) {
    console.warn(
      '[orez] could not find node:worker_threads import in write-worker-client.js. ' +
        'zero-cache version may have changed — check compatibility.'
    )
    return
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
      const previousRole = globalThis.__orez_zero_sqlite_role;
      globalThis.__orez_zero_sqlite_role = "replica-writer";
      try {
        db = new Database(lc, dbPath);
      } finally {
        if (previousRole === void 0) {
          delete globalThis.__orez_zero_sqlite_role;
        } else {
          globalThis.__orez_zero_sqlite_role = previousRole;
        }
      }
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
    console.warn('[orez] initial-sync.js not found at', initialSyncPath)
    return
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
      console.warn(
        `[orez] expected 2 occurrences of initial-sync batch pattern, found ${count} — ` +
          'zero-cache version may have changed; skipping the DO bound-parameter batch patch. ' +
          'initial sync of tables with rows WILL fail on Cloudflare DOs until this is fixed.'
      )
      return
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
    console.warn('[orez] litestream commands.js not found at', commandsPath)
    return
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
    console.warn('[orez] change-streamer-service.js not found at', servicePath)
    return
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
    return {}
  }
  const targetPackagePath = resolve(outDir, 'node_modules', 'libpg-query')
  const parserIndexPath = resolve(targetPackagePath, 'wasm', 'index.js')
  const wasmPath = resolve(targetPackagePath, 'wasm', 'libpg-query.wasm')

  rmSync(targetPackagePath, { recursive: true, force: true })
  mkdirSync(dirname(targetPackagePath), { recursive: true })
  cpSync(sourcePackagePath, targetPackagePath, { recursive: true, dereference: true })

  if (!existsSync(parserIndexPath) || !existsSync(wasmPath)) {
    return {}
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
    console.warn(
      '[orez] libpg-query wasm loader already patched with an older shape. ' +
        'Regenerate the overlay from an unpatched libpg-query install.'
    )
    return {}
  }

  const pattern = 'const initPromise = PgQueryModule().then((module) => {'
  if (!code.includes(pattern)) {
    console.warn(
      '[orez] could not find PgQueryModule init in libpg-query wasm index. ' +
        'pgsql-parser version may have changed — check compatibility.'
    )
    return {}
  }

  const wasmBase64 = readFileSync(wasmPath).toString('base64')
  const replacement = `\
// orez-libpg-query-wasm-binary: embed parser wasm for CF Workers.
const __orezLibPgQueryWasmBase64 = '${wasmBase64}';
function __orezLibPgQueryWasmBinary() {
    const decode = globalThis.atob
        ? globalThis.atob(__orezLibPgQueryWasmBase64)
        : Buffer.from(__orezLibPgQueryWasmBase64, 'base64').toString('binary');
    const bytes = new Uint8Array(decode.length);
    for (let i = 0; i < decode.length; i++) bytes[i] = decode.charCodeAt(i);
    return bytes;
}
try {
    const g = globalThis;
    if (g.self && !g.self.location) g.self.location = { href: 'https://orez.local/libpg-query.js' };
    if (!g.location) g.location = { href: 'https://orez.local/libpg-query.js' };
}
catch {
}
const __orezLibPgQueryPreviousProcessType = globalThis.process?.type;
if (globalThis.process && !globalThis.process.type) globalThis.process.type = 'renderer';
const __orezLibPgQueryInit = PgQueryModule({ wasmBinary: __orezLibPgQueryWasmBinary() });
if (globalThis.process && __orezLibPgQueryPreviousProcessType === undefined) {
    delete globalThis.process.type;
}
else if (globalThis.process) {
    globalThis.process.type = __orezLibPgQueryPreviousProcessType;
}
const initPromise = __orezLibPgQueryInit.then((module) => {`

  code = code.replace(pattern, replacement)
  writeFileSync(parserIndexPath, code)
  console.log('[orez] patched libpg-query wasm loader (embedded wasm bytes)')
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
