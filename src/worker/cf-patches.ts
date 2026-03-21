/**
 * zero-cache CF Workers patches.
 *
 * applies patches to @rocicorp/zero's internal files so zero-cache
 * can run in SINGLE_PROCESS mode on CF Workers where dynamic import()
 * doesn't work.
 *
 * two patches:
 * 1. worker-urls.js — replace file:// URLs with zero-worker:// identifiers
 * 2. processes.js — replace dynamic import() with static worker module lookup
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
  const zcBase = resolve(
    nodeModulesPath,
    '@rocicorp',
    'zero',
    'out',
    'zero-cache',
    'src'
  )

  patchWorkerUrls(zcBase)
  patchProcesses(zcBase)
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
`
  )
  console.log('[orez] patched zero-cache worker-urls.js')
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
  const dynamicImportPattern = 'import(moduleUrl.href).then(async ({ default: runWorker })'
  const staticLookup =
    '((async () => { ' +
    'const _name = moduleUrl.hostname || moduleUrl.pathname.split("/").pop()?.replace(".js",""); ' +
    'const runWorker = __zc_workers[_name]; ' +
    'if (!runWorker) throw new Error("orez: unknown zero-cache worker: " + _name + " (available: " + Object.keys(__zc_workers).join(", ") + ")"); ' +
    'return { default: runWorker }; ' +
    '})()).then(async ({ default: runWorker })'

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
