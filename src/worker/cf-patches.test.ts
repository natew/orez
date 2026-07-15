import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

import { afterEach, describe, expect, it } from 'vitest'

import { prepareZeroCacheForCF } from './cf-patches.js'

const AUTO_START =
  'if (!singleProcessMode()) exitAfter(() => runWorker(must(parentWorker), process.env, ...process.argv.slice(2)));'

const ENTRYPOINTS = ['main', 'change-streamer', 'reaper', 'replicator', 'syncer']

let tmpDirs: string[] = []

afterEach(() => {
  for (const dir of tmpDirs) {
    rmSync(dir, { recursive: true, force: true })
  }
  tmpDirs = []
})

describe('prepareZeroCacheForCF', () => {
  it('patches a Zero 1.7 overlay for Cloudflare Workers without mutating node_modules', () => {
    const nodeModules = makeFakeNodeModules()

    const result = prepareZeroCacheForCF({ nodeModulesPath: nodeModules })

    const sourceBase = zeroCacheBase(nodeModules)
    const overlayBase = result.zeroCacheSrcDir
    const workerUrls = readText(overlayBase, 'server/worker-urls.js')
    expect(workerUrls).toContain('export const SHADOW_SYNCER_URL = u("shadow-syncer");')
    expect(workerUrls).toContain('export const WRITE_WORKER_URL = u("write-worker");')
    expect(readText(sourceBase, 'server/worker-urls.js')).toContain(
      'export const SHADOW_SYNCER_URL = resolve("./shadow-syncer.ts");'
    )
    expect(readText(sourceBase, 'server/worker-urls.js')).not.toContain('zero-worker://')

    const processes = readText(overlayBase, 'types/processes.js')
    expect(processes).not.toContain('shadow-syncer')
    expect(processes).toContain('const runWorkerImpl = __zc_workers[_name];')
    expect(processes).toContain('env.ZERO_TASK_ID')
    expect(processes).toContain('abandonOrezZeroWorkers')
    expect(processes).toContain('waitForOrezZeroWorkersStopped')
    expect(processes).toContain('__orez_worker_executions.get(task) === active')
    expect(processes).toContain('__orez_latched_signal')
    expect(processes).toContain('result === target ? receiver : result')
    expect(processes).not.toContain('__OREZ_DEBUG_WIRE__')
    expect(readText(sourceBase, 'types/processes.js')).toContain('import(moduleUrl.href)')

    const lifecycle = readText(overlayBase, 'services/life-cycle.js')
    expect(lifecycle).toContain('OrezZeroStartupStoppedError')
    expect(lifecycle).toContain('this.#runningState.stopped()')
    expect(lifecycle).toContain('if (stopPromise) await stopPromise')
    expect(readText(sourceBase, 'services/life-cycle.js')).not.toContain(
      'OrezZeroStartupStoppedError'
    )
    expect(readText(overlayBase, 'server/main.js')).toContain('__orezAwaitStartup')

    for (const entrypoint of ENTRYPOINTS) {
      const worker = readText(overlayBase, `server/${entrypoint}.js`)
      expect(worker).toContain('orez-instance-local-log-context')
      expect(worker).toContain('const lc = createLogContext(')
      expect(worker).not.toContain('\tlc = createLogContext(')
    }
    expect(readText(overlayBase, 'observability/events.js')).toContain(
      'orez-instance-isolated-events'
    )
    expect(readText(overlayBase, 'server/otel-start.js')).toContain(
      'orez-instance-isolated-otel'
    )
    expect(readText(overlayBase, 'server/anonymous-otel-start.js')).toContain(
      'orez-instance-isolated-anonymous-telemetry'
    )
    expect(
      readFileSync(resolve(overlayBase, '..', '..', 'shared', 'src', 'dotenv.js'), 'utf8')
    ).toContain('orez-cf-dotenv-disabled')

    const customFetch = readText(overlayBase, 'custom/fetch.js')
    expect(customFetch).toContain('fetchCFInstanceAPI as __orezFetchCFInstanceAPI')
    expect(customFetch).toContain('await __orezFetchCFInstanceAPI(finalUrl, {')
    expect(readText(sourceBase, 'custom/fetch.js')).toContain(
      'const response = await fetch(finalUrl, {'
    )

    const writeWorkerClient = readText(
      overlayBase,
      'services/replicator/write-worker-client.js'
    )
    expect(writeWorkerClient).toContain('orez-inline-write-worker')
    expect(writeWorkerClient).toContain('orezRole=replica-writer')
    expect(writeWorkerClient).not.toContain('__orez_zero_sqlite_role')
    expect(writeWorkerClient).toContain('class ThreadWriteWorkerClient')
    expect(readText(sourceBase, 'services/replicator/write-worker-client.js')).toContain(
      'import { Worker } from "node:worker_threads";'
    )

    const initialSync = readText(overlayBase, 'services/change-source/pg/initial-sync.js')
    expect(initialSync).not.toContain('.repeat(49)')
    expect(initialSync).not.toContain('valuesPerRow * 50')
    expect(initialSync).not.toContain('pendingRows > 50')
    expect(initialSync).toContain(
      'var MAX_BUFFERED_ROWS = 256; /* orez-cf-storage-burst-cap */'
    )
    expect(initialSync.match(/orezRowsPerBatch - 1/g)).toHaveLength(2)
    expect(initialSync.match(/pendingRows > orezRowsPerBatch/g)).toHaveLength(2)
    expect(readText(sourceBase, 'services/change-source/pg/initial-sync.js')).toContain(
      '.repeat(49)'
    )
    expect(readText(sourceBase, 'services/change-source/pg/initial-sync.js')).toContain(
      'var MAX_BUFFERED_ROWS = 1e4;'
    )

    const changeStreamerService = readText(
      overlayBase,
      'services/change-streamer/change-streamer-service.js'
    )
    expect(changeStreamerService).toContain(
      'orez: retry changeLog cleanup when subscribers are absent'
    )
    expect(
      readText(sourceBase, 'services/change-streamer/change-streamer-service.js')
    ).not.toContain('orez: retry changeLog cleanup when subscribers are absent')

    const sourceParser = readFileSync(
      resolve(nodeModules, 'libpg-query', 'wasm', 'index.js'),
      'utf-8'
    )
    expect(sourceParser).not.toContain('__orezLibPgQueryInit')

    const parser = readFileSync(
      resolve(result.outDir, 'node_modules', 'libpg-query', 'wasm', 'index.js'),
      'utf-8'
    )
    expect(parser).toContain('__orezLibPgQueryInit')
    expect(parser).toContain('orez-libpg-query-wasm-binary')
    // workerd forbids runtime wasm compilation: the overlay must import the parser
    // wasm as a (CompiledWasm) module and use Emscripten's instantiateWasm hook,
    // never hand raw bytes to PgQueryModule for runtime compilation.
    expect(parser).toContain(
      "import __orezLibPgQueryWasmModule from 'libpg-query/wasm/libpg-query.wasm';"
    )
    expect(parser).toContain('instantiateWasm(imports, receiveInstance)')
    expect(parser).toContain('new WebAssembly.Instance(__orezLibPgQueryWasmModule')
    expect(parser).not.toContain('wasmBinary')
    expect(parser).not.toContain('__orezLibPgQueryWasmBase64')
    expect(result.aliases['libpg-query/wasm/index.js']).toBe(
      resolve(result.outDir, 'node_modules', 'libpg-query', 'wasm', 'index.js')
    )
    expect(
      result.aliases['@rocicorp/zero/out/zero-cache/src/server/runner/run-worker.js']
    ).toBe(resolve(overlayBase, 'server', 'runner', 'run-worker.js'))
    expect(result.aliases['@rocicorp/zero/out/zero-cache/src/types/processes.js']).toBe(
      resolve(overlayBase, 'types', 'processes.js')
    )
  })

  it('rejects an unverified Zero version before generating an overlay', () => {
    const nodeModules = makeFakeNodeModules()
    writeFileSync(
      resolve(nodeModules, '@rocicorp', 'zero', 'package.json'),
      JSON.stringify({ name: '@rocicorp/zero', version: '1.7.1' })
    )

    expect(() => prepareZeroCacheForCF({ nodeModulesPath: nodeModules })).toThrow(
      'supports @rocicorp/zero 1.7.0, found 1.7.1'
    )
  })

  it('upgrades an older patched worker-urls file to include shadow-syncer', () => {
    const nodeModules = makeFakeNodeModules()
    const zcBase = zeroCacheBase(nodeModules)
    writeText(
      zcBase,
      'server/worker-urls.js',
      `const u = (n) => new URL("zero-worker://" + n);
export const MAIN_URL = u("main");
export const CHANGE_STREAMER_URL = u("change-streamer");
export const REAPER_URL = u("reaper");
export const REPLICATOR_URL = u("replicator");
export const SYNCER_URL = u("syncer");
export const WRITE_WORKER_URL = u("write-worker");
`
    )

    const result = prepareZeroCacheForCF({ nodeModulesPath: nodeModules })

    expect(readText(result.zeroCacheSrcDir, 'server/worker-urls.js')).toContain(
      'export const SHADOW_SYNCER_URL = u("shadow-syncer");'
    )
    expect(readText(zcBase, 'server/worker-urls.js')).not.toContain('SHADOW_SYNCER_URL')
  })

  it('finds libpg-query in Bun transitive dependency layout', () => {
    const nodeModules = makeFakeNodeModules({ libPgQueryLayout: 'bun' })

    const result = prepareZeroCacheForCF({ nodeModulesPath: nodeModules })

    const parser = readFileSync(
      resolve(result.outDir, 'node_modules', 'libpg-query', 'wasm', 'index.js'),
      'utf-8'
    )
    expect(parser).toContain('__orezLibPgQueryInit')
    expect(result.aliases['libpg-query']).toBe(
      resolve(result.outDir, 'node_modules', 'libpg-query')
    )
  })

  it('does not bundle the optional shadow-syncer worker', () => {
    const nodeModules = makeFakeNodeModules()

    const result = prepareZeroCacheForCF({ nodeModulesPath: nodeModules })

    const processes = readText(result.zeroCacheSrcDir, 'types/processes.js')
    expect(processes).not.toContain('__zc_shadow_syncer')
    expect(processes).not.toContain('"shadow-syncer"')
    expect(processes).toContain('result === target ? receiver : result')
    expect(processes).not.toContain('Reflect.get(target, prop, receiver)')
  })

  it('rejects an incomplete existing process overlay', () => {
    const nodeModules = makeFakeNodeModules()
    writeText(
      zeroCacheBase(nodeModules),
      'types/processes.js',
      'const __zc_workers = {};\nfunction wrap(target) { return target; }\n'
    )

    expect(() => prepareZeroCacheForCF({ nodeModulesPath: nodeModules })).toThrow(
      'existing processes patch missing abandonOrezZeroWorkers'
    )
  })
})

function makeFakeNodeModules(
  options: { libPgQueryLayout?: 'direct' | 'bun' } = {}
): string {
  const root = mkdtempSync(join(tmpdir(), 'orez-cf-patches-'))
  tmpDirs.push(root)
  const nodeModules = resolve(root, 'node_modules')
  const zcBase = zeroCacheBase(nodeModules)
  const libPgQueryRoot =
    options.libPgQueryLayout === 'bun'
      ? resolve(nodeModules, '.bun', 'libpg-query@17.9.11', 'node_modules', 'libpg-query')
      : resolve(nodeModules, 'libpg-query')

  mkdirSync(resolve(zcBase, 'server'), { recursive: true })
  mkdirSync(resolve(zcBase, 'custom'), { recursive: true })
  mkdirSync(resolve(zcBase, 'observability'), { recursive: true })
  mkdirSync(resolve(zcBase, 'types'), { recursive: true })
  mkdirSync(resolve(zcBase, 'services', 'change-streamer'), { recursive: true })
  mkdirSync(resolve(zcBase, 'services', 'litestream'), { recursive: true })
  mkdirSync(resolve(zcBase, 'services', 'replicator'), { recursive: true })
  mkdirSync(resolve(zcBase, '..', '..', 'shared', 'src'), { recursive: true })
  mkdirSync(resolve(libPgQueryRoot, 'wasm'), { recursive: true })

  writeFileSync(
    resolve(nodeModules, '@rocicorp', 'zero', 'package.json'),
    JSON.stringify({ name: '@rocicorp/zero', version: '1.7.0' })
  )

  writeText(
    zcBase,
    'custom/fetch.js',
    `export async function customFetch(finalUrl) {
  const response = await fetch(finalUrl, {
    method: "POST",
  });
  return response;
}
`
  )
  writeFileSync(
    resolve(zcBase, '..', '..', 'shared', 'src', 'dotenv.js'),
    'import { config } from "@dotenvx/dotenvx"; config();\n'
  )

  writeText(
    zcBase,
    'server/worker-urls.js',
    `const tsRe = /\\.ts$/;
function resolve(path) {
  const { url } = import.meta;
  if (url.endsWith(".js")) path = path.replace(tsRe, ".js");
  return new URL(path, url);
}
export const CHANGE_STREAMER_URL = resolve("./change-streamer.ts");
export const MAIN_URL = resolve("./main.ts");
export const REAPER_URL = resolve("./reaper.ts");
export const REPLICATOR_URL = resolve("./replicator.ts");
export const SHADOW_SYNCER_URL = resolve("./shadow-syncer.ts");
export const SYNCER_URL = resolve("./syncer.ts");
export const WRITE_WORKER_URL = resolve("./write-worker.ts");
`
  )

  for (const entrypoint of ENTRYPOINTS) {
    writeText(
      zcBase,
      `server/${entrypoint}.js`,
      `import { parentWorker, singleProcessMode } from "../types/processes.js";
import { exitAfter } from "../services/life-cycle.js";
var lc = new LogContext("info", {}, consoleLogSink);
function runWorker() {
\tlc = createLogContext(config, "${entrypoint}");
}
${AUTO_START}
export { runWorker as default };
`
    )
  }

  writeText(
    zcBase,
    'server/main.js',
    `import { parentWorker, singleProcessMode } from "../types/processes.js";
import { exitAfter } from "../services/life-cycle.js";
var lc = new LogContext("info", {}, consoleLogSink);
async function runWorker(parent) {
\tlc = createLogContext(config, "main");
\tconst processes = new ProcessManager(lc, parent);
\tif (litestream.executable) await restoreReplica(lc, config, null);
\tawait changeStreamerReady;
\tawait backupReady;
\tawait reaperReady;
\tawait shadowReady;
\tawait replicaReady;
\tawait processes.allWorkersReady();
}
${AUTO_START}
export { runWorker as default };
`
  )

  writeText(zcBase, 'observability/events.js', 'export function initEventSink() {}\n')
  writeText(zcBase, 'server/otel-start.js', 'export function startOtelAuto() {}\n')
  writeText(
    zcBase,
    'server/anonymous-otel-start.js',
    'export function startAnonymousTelemetry() {}\n'
  )

  writeText(
    zcBase,
    'types/processes.js',
    `function childWorker(moduleUrl, env, ...args) {
  const [parent, child] = inProcChannel();
  import(moduleUrl.href).then(async ({ default: runWorker }) => {
    await runWorker();
  });
  return child;
}
function wrap(target) {
  return new Proxy(target, { get(target, prop, receiver) {
    switch (prop) {
      case "onMessageType": return () => receiver;
    }
    return Reflect.get(target, prop, receiver);
  }});
}
function inProcChannel() {
  const worker1 = new EventEmitter();
  const worker2 = new EventEmitter();
  const kill = (dest) => (signal = "SIGTERM") => dest.emit(signal, signal);
  return [wrap(Object.assign(worker1, { kill: kill(worker2) })), wrap(Object.assign(worker2, { kill: kill(worker1) }))];
}
export { childWorker, parentWorker, singleProcessMode };
`
  )

  writeText(
    zcBase,
    'services/change-streamer/change-streamer-service.js',
    `
async function purgeOldChanges() {
\t\tconst current = [...this.#forwarder.getAcks()];
\t\tif (current.length === 0) {
\t\t\tthis.#lc.warn?.("No subscribers to confirm cleanup");
\t\t\treturn;
\t\t}
\t\ttry {
\t\t\tthis.#lc.info?.("Purging changes");
\t\t} finally {
\t\t\tif (this.#initialWatermarks.size) this.#state.setTimeout(() => this.#purgeOldChanges(), CLEANUP_DELAY_MS);
\t\t}
}
`
  )

  writeText(
    zcBase,
    'services/litestream/commands.js',
    `class BackupNotFoundException extends Error {}
async function restoreReplica(lc, config, replicaConstraints) {
  return [lc, config, replicaConstraints];
}
export { BackupNotFoundException, restoreReplica };
`
  )

  writeText(
    zcBase,
    'services/life-cycle.js',
    `const GRACEFUL_SHUTDOWN = ["SIGTERM", "SIGINT"];
const FORCEFUL_SHUTDOWN = ["SIGQUIT", "SIGABRT"];
class ProcessManager {
\t#ready = [];
\t#runningState;
\tdone() { return this.#runningState.stopped(); }
\tasync allWorkersReady() {
\t\tawait Promise.all(this.#ready);
\t}
}
async function runUntilKilled(lc, parent, ...services) {
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
\t});
\ttry {
\t\tconst svc = await Promise.race(services.map((svc) => svc.run().then(() => svc)));
\t\tlc.info?.(\`\${svc.constructor.name} (\${svc.id}) stopped\`);
\t} catch (e) {
\t\tthrow e;
\t}
}
export { ProcessManager, runUntilKilled };
`
  )

  writeText(
    zcBase,
    'services/replicator/write-worker-client.js',
    `import { Worker } from "node:worker_threads";
export class ThreadWriteWorkerClient {}
`
  )

  mkdirSync(resolve(zcBase, 'services', 'change-source', 'pg'), { recursive: true })
  const copyVariant = `	const insertStmt = to.prepare(insertSql);
	const insertBatchStmt = to.prepare(insertSql + \`,\${valuesSql}\`.repeat(49));
	const valuesPerRow = columnSpecs.length;
	const valuesPerBatch = valuesPerRow * 50;
	function flush() {
		let l = 0;
		for (; pendingRows > 50; pendingRows -= 50) insertBatchStmt.run(pendingValues.slice(l, l += valuesPerBatch));
		for (; pendingRows > 0; pendingRows--) insertStmt.run(pendingValues.slice(l, l += valuesPerRow));
	}
`
  writeText(
    zcBase,
    'services/change-source/pg/initial-sync.js',
    `var MAX_BUFFERED_ROWS = 1e4;
async function copyBinary() {
${copyVariant}}
async function copyText() {
${copyVariant}}
`
  )

  writeFileSync(
    resolve(libPgQueryRoot, 'package.json'),
    '{"name":"libpg-query","version":"0.0.0"}'
  )
  writeFileSync(
    resolve(libPgQueryRoot, 'wasm', 'index.js'),
    `const initPromise = PgQueryModule().then((module) => {
  return module;
});
`
  )
  writeFileSync(resolve(libPgQueryRoot, 'wasm', 'libpg-query.wasm'), 'wasm')

  return nodeModules
}

function zeroCacheBase(nodeModules: string): string {
  return resolve(nodeModules, '@rocicorp', 'zero', 'out', 'zero-cache', 'src')
}

function readText(base: string, path: string): string {
  return readFileSync(resolve(base, path), 'utf-8')
}

function writeText(base: string, path: string, text: string): void {
  writeFileSync(resolve(base, path), text)
}
