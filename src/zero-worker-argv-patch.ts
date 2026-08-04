/**
 * keep host process arguments out of embedded zero-cache workers.
 *
 * zero-cache normally appends the parent CLI arguments before forking each
 * worker. in single-process mode those arguments belong to the embedding host
 * (for example Vitest), but nested workers parse them as zero-cache options.
 * keep the inheritance for real child processes and pass only explicit worker
 * arguments to in-process workers.
 */

import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'

import { resolvePackage } from './sqlite-mode/package-resolve.js'

const OREZ_MARKER = '/* orez: host argv belongs only to forked workers */'
const MAIN_OREZ_MARKER = '/* orez: embedded main ignores host argv */'

const CHILD_WORKER_ANCHOR = `function childWorker(moduleUrl, env, ...args) {
\targs = workerArgs(args);
\tif (singleProcessMode()) {`

const CHILD_WORKER_REPLACEMENT = `function childWorker(moduleUrl, env, ...args) {
\t${OREZ_MARKER}
\tif (singleProcessMode()) {`

const FORK_ANCHOR = `\treturn forkChildWorkerWithArgs(moduleUrl, env, args);
}`

const FORK_REPLACEMENT = `\treturn forkChildWorkerWithArgs(moduleUrl, env, workerArgs(args));
}`

const MAIN_CONFIG_ANCHOR = '\tconst config = getNormalizedZeroConfig({ env });'

const MAIN_CONFIG_REPLACEMENT =
  `\t${MAIN_OREZ_MARKER}\n` +
  '\tconst config = getNormalizedZeroConfig(\n' +
  '\t\tsingleProcessMode() ? { env, argv: [] } : { env }\n' +
  '\t);'

function findZeroWorkerFiles(): { mainPath: string; processesPath: string } | null {
  const zeroEntry = resolvePackage('@rocicorp/zero')
  if (!zeroEntry) return null

  let dir = dirname(zeroEntry)
  while (dir && !existsSync(resolve(dir, 'package.json'))) {
    const parent = resolve(dir, '..')
    if (parent === dir) break
    dir = parent
  }

  const processesPath = resolve(dir, 'out', 'zero-cache', 'src', 'types', 'processes.js')
  const mainPath = resolve(dir, 'out', 'zero-cache', 'src', 'server', 'main.js')
  return existsSync(processesPath) && existsSync(mainPath)
    ? { mainPath, processesPath }
    : null
}

export function applyZeroWorkerArgvPatch(processesPath: string): void {
  const content = readFileSync(processesPath, 'utf-8')
  if (content.includes(OREZ_MARKER)) return

  if (
    !content.includes(CHILD_WORKER_ANCHOR) ||
    content.split(FORK_ANCHOR).length - 1 !== 1
  ) {
    throw new Error(
      `orez: could not isolate embedded zero-cache worker arguments in ${processesPath}. ` +
        `@rocicorp/zero may have changed; update zero-worker-argv-patch.ts.`
    )
  }

  writeFileSync(
    processesPath,
    content
      .replace(CHILD_WORKER_ANCHOR, CHILD_WORKER_REPLACEMENT)
      .replace(FORK_ANCHOR, FORK_REPLACEMENT)
  )
}

export function applyZeroMainArgvPatch(mainPath: string): void {
  const content = readFileSync(mainPath, 'utf-8')
  if (content.includes(MAIN_OREZ_MARKER)) return

  if (content.split(MAIN_CONFIG_ANCHOR).length - 1 !== 1) {
    throw new Error(
      `orez: could not isolate embedded zero-cache main arguments in ${mainPath}. ` +
        `@rocicorp/zero may have changed; update zero-worker-argv-patch.ts.`
    )
  }

  writeFileSync(mainPath, content.replace(MAIN_CONFIG_ANCHOR, MAIN_CONFIG_REPLACEMENT))
}

export function isolateEmbeddedZeroWorkerArgs(): void {
  const files = findZeroWorkerFiles()
  if (!files) return
  applyZeroWorkerArgvPatch(files.processesPath)
  applyZeroMainArgvPatch(files.mainPath)
}
