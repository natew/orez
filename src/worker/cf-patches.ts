/**
 * zero-cache CF Workers patches.
 *
 * applies patches to @rocicorp/zero's internal files so zero-cache
 * can run in SINGLE_PROCESS mode on CF Workers where dynamic import()
 * doesn't work.
 *
 * five patches:
 * 1. worker-urls.js — replace file:// URLs with zero-worker:// identifiers
 * 2. server worker entrypoints — disable CLI auto-start blocks
 * 3. processes.js — replace dynamic import() with static worker module lookup
 * 4. write-worker-client.js — run zero-cache's replica writer in-process
 * 5. pgsql-parser — embed libpg-query wasm bytes for Workers
 *
 * usage in a post-build script:
 *
 *   import { patchZeroCacheForCF } from 'orez/worker/cf-patches'
 *   patchZeroCacheForCF('./node_modules')
 *
 * idempotent: safe to run multiple times.
 */

import { readFileSync, writeFileSync, existsSync } from 'node:fs'
import { resolve } from 'node:path'

export function patchZeroCacheForCF(nodeModulesPath: string): void {
  const zcBase = resolve(nodeModulesPath, '@rocicorp', 'zero', 'out', 'zero-cache', 'src')

  patchWorkerUrls(zcBase)
  patchWorkerEntrypoints(zcBase)
  patchProcesses(zcBase)
  patchWriteWorkerClient(zcBase)
  patchPgsqlParserWasm(nodeModulesPath)
}

function patchWorkerUrls(zcBase: string): void {
  const workerUrlsPath = resolve(zcBase, 'server', 'worker-urls.js')
  if (!existsSync(workerUrlsPath)) {
    console.warn('[orez] worker-urls.js not found at', workerUrlsPath)
    return
  }

  const content = readFileSync(workerUrlsPath, 'utf-8')

  // skip if already patched
  if (content.includes('zero-worker://')) {
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
  const entrypoints = ['main', 'change-streamer', 'reaper', 'replicator', 'syncer']

  for (const entrypoint of entrypoints) {
    const entrypointPath = resolve(zcBase, 'server', `${entrypoint}.js`)
    if (!existsSync(entrypointPath)) {
      console.warn('[orez] zero-cache worker entrypoint not found at', entrypointPath)
      continue
    }

    let code = readFileSync(entrypointPath, 'utf-8')
    if (code.includes('orez-disable-autostart')) {
      continue
    }

    const next = code.replace(
      /if \(!singleProcessMode\(\)\) exitAfter\(\(\) => runWorker\(must\(parentWorker\), process\.env(?:, \.\.\.process\.argv\.slice\(2\))?\)\);/g,
      '// orez-disable-autostart: childWorker invokes runWorker explicitly in CF embeds.'
    )

    if (next === code) {
      console.warn(
        `[orez] could not find auto-start guard in ${entrypoint}.js. ` +
          'zero-cache version may have changed — check compatibility.'
      )
      continue
    }

    code = next
    writeFileSync(entrypointPath, code)
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

  // skip if already patched
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
    'if (process.env.OREZ_DEBUG_WIRE === "1" || globalThis.__OREZ_DEBUG_WIRE__ === true) console.debug("[orez-zc-worker] start", _name, args); ' +
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
      lc = createLogContext({ log: logConfig }, { worker: "write-worker" });
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

export { ThreadWriteWorkerClient, applyPragmas };
`
  )
  console.log('[orez] patched zero-cache write-worker-client.js (inline writer)')
}

function patchPgsqlParserWasm(nodeModulesPath: string): void {
  const parserIndexPath = resolve(nodeModulesPath, 'libpg-query', 'wasm', 'index.js')
  const wasmPath = resolve(nodeModulesPath, 'libpg-query', 'wasm', 'libpg-query.wasm')

  if (!existsSync(parserIndexPath) || !existsSync(wasmPath)) {
    console.warn('[orez] libpg-query wasm files not found under', nodeModulesPath)
    return
  }

  let code = readFileSync(parserIndexPath, 'utf-8')
  if (
    code.includes('orez-libpg-query-wasm-binary') &&
    code.includes('__orezLibPgQueryInit')
  ) {
    return
  }
  if (code.includes('orez-libpg-query-wasm-binary')) {
    console.warn(
      '[orez] libpg-query wasm loader already patched with an older shape. ' +
        'Reinstall libpg-query or restore node_modules before re-patching.'
    )
    return
  }

  const pattern = 'const initPromise = PgQueryModule().then((module) => {'
  if (!code.includes(pattern)) {
    console.warn(
      '[orez] could not find PgQueryModule init in libpg-query wasm index. ' +
        'pgsql-parser version may have changed — check compatibility.'
    )
    return
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
}
