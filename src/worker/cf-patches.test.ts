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
  it('patches a Zero 1.5 overlay for Cloudflare Workers without mutating node_modules', () => {
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
    expect(processes).toContain('const runWorker = __zc_workers[_name];')
    expect(readText(sourceBase, 'types/processes.js')).toContain('import(moduleUrl.href)')

    const writeWorkerClient = readText(
      overlayBase,
      'services/replicator/write-worker-client.js'
    )
    expect(writeWorkerClient).toContain('orez-inline-write-worker')
    expect(writeWorkerClient).toContain('class ThreadWriteWorkerClient')
    expect(readText(sourceBase, 'services/replicator/write-worker-client.js')).toContain(
      'import { Worker } from "node:worker_threads";'
    )

    const initialSync = readText(overlayBase, 'services/change-source/pg/initial-sync.js')
    expect(initialSync).not.toContain('.repeat(49)')
    expect(initialSync).not.toContain('valuesPerRow * 50')
    expect(initialSync).not.toContain('pendingRows > 50')
    expect(initialSync.match(/orezRowsPerBatch - 1/g)).toHaveLength(2)
    expect(initialSync.match(/pendingRows > orezRowsPerBatch/g)).toHaveLength(2)
    expect(readText(sourceBase, 'services/change-source/pg/initial-sync.js')).toContain(
      '.repeat(49)'
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
    expect(result.aliases['libpg-query/wasm/index.js']).toBe(
      resolve(result.outDir, 'node_modules', 'libpg-query', 'wasm', 'index.js')
    )
    expect(
      result.aliases['@rocicorp/zero/out/zero-cache/src/server/runner/run-worker.js']
    ).toBe(resolve(overlayBase, 'server', 'runner', 'run-worker.js'))
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
    const zcBase = zeroCacheBase(nodeModules)
    writeText(
      zcBase,
      'types/processes.js',
      `import { default as __zc_main } from "../server/main.js";
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
function childWorker(moduleUrl, env, ...args) {
  ((async () => {
    const _name = moduleUrl.hostname || moduleUrl.pathname.split("/").pop()?.replace(".js","");
    const runWorker = __zc_workers[_name];
    return { default: runWorker, name: _name };
  })()).then(async ({ default: runWorker, name }) => runWorker);
}
`
    )

    const result = prepareZeroCacheForCF({ nodeModulesPath: nodeModules })

    const processes = readText(result.zeroCacheSrcDir, 'types/processes.js')
    expect(processes).not.toContain('__zc_shadow_syncer')
    expect(processes).not.toContain('"shadow-syncer"')
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
  mkdirSync(resolve(zcBase, 'types'), { recursive: true })
  mkdirSync(resolve(zcBase, 'services', 'change-streamer'), { recursive: true })
  mkdirSync(resolve(zcBase, 'services', 'replicator'), { recursive: true })
  mkdirSync(resolve(libPgQueryRoot, 'wasm'), { recursive: true })

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
function runWorker() {}
${AUTO_START}
export { runWorker as default };
`
    )
  }

  writeText(
    zcBase,
    'types/processes.js',
    `function childWorker(moduleUrl, env, ...args) {
  import(moduleUrl.href).then(async ({ default: runWorker }) => {
    await runWorker();
  });
}
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
    `async function copyBinary() {
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
