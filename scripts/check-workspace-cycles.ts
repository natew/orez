// A workspace package may not depend on something that depends on it.
//
// orderReleasePackages already refuses a cycle, but only when a release runs,
// and by then the damage is done: on 2026-07-27 sync-cf-host was given an
// import of orez-lite/realtime while orez-lite already depended on
// sync-cf-host, which broke CI and failed a canary release. Worse, it passed
// locally, because a previously built orez-lite/dist was still on disk and
// resolved the import that a fresh checkout cannot.
//
// So this runs in `bun run check`, reading manifests only. It never consults
// node_modules or dist, which is exactly why it sees what a clean checkout sees.
//
//   bun scripts/check-workspace-cycles.ts

import { readFileSync, readdirSync, existsSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { orderReleasePackages } from './release-package-order.js'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')

// the same globs bun installs from, so this cannot scan less than the workspace
const globs =
  (
    JSON.parse(readFileSync(join(root, 'package.json'), 'utf8')) as {
      workspaces?: string[]
    }
  ).workspaces ?? []

const memberDirs = globs.flatMap((glob) => {
  if (!glob.includes('*')) return [join(root, glob)]
  const parent = join(root, dirname(glob))
  if (!existsSync(parent)) return []
  const prefix = glob.slice(glob.lastIndexOf('/') + 1).replace('*', '')
  return readdirSync(parent, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && entry.name.startsWith(prefix))
    .map((entry) => join(parent, entry.name))
})

const packages = memberDirs
  .map((dir) => join(dir, 'package.json'))
  .filter((path) => existsSync(path))
  .map((path) => ({
    pkg: JSON.parse(readFileSync(path, 'utf8')) as {
      name: string
      dependencies?: Record<string, string>
      optionalDependencies?: Record<string, string>
    },
  }))

try {
  orderReleasePackages(packages)
} catch (error) {
  console.error(`[check:cycles] ${(error as Error).message}`)
  console.error(`    A package imported something that imports it back. A fresh checkout`)
  console.error(`    cannot build that, and the release refuses it. Move the shared code`)
  console.error(`    down to a package both sides already depend on.`)
  process.exit(1)
}

console.info(`[check:cycles] ${packages.length} workspace packages, no cycles`)
