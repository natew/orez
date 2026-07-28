// A workspace package may not depend on something that depends on it.
//
// orderReleasePackages already refuses a cycle, but only when a release runs,
// and by then the damage is done: on 2026-07-27 sync-cf-host was given an
// import of orez-lite/realtime while orez-lite already depended on
// sync-cf-host, which broke CI and failed a canary release. Worse, it passed
// locally, because a previously built orez-lite/dist was still on disk and
// resolved the import that a fresh checkout cannot. That import was never
// declared in sync-cf-host's manifest, so a manifest-only pass finds no cycle
// on the exact commit that broke: the edges here come from the source imports
// as well as declared dependencies.
//
// So this runs in `bun run check`, reading manifests and source files only. It
// never consults node_modules or dist, which is exactly why it sees what a
// clean checkout sees.
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

const manifests = memberDirs
  .map((dir) => ({ dir, path: join(dir, 'package.json') }))
  .filter(({ path }) => existsSync(path))
  .map(({ dir, path }) => ({
    dir,
    pkg: JSON.parse(readFileSync(path, 'utf8')) as {
      name: string
      dependencies?: Record<string, string>
      optionalDependencies?: Record<string, string>
    },
  }))

const workspaceNames = new Set(manifests.map(({ pkg }) => pkg.name))

const SOURCE_FILE = /\.(ts|tsx|mts|cts|js|jsx|mjs|cjs)$/
const SKIP_DIRS = new Set(['node_modules', 'dist', 'build', 'types', '.turbo'])
// static and dynamic imports, re-exports, and require calls
const IMPORT_SPECIFIER = /(?:from\s+|import\s*\(\s*|require\s*\(\s*)['"]([^'"]+)['"]/g

function sourceFiles(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    if (entry.isDirectory()) {
      return SKIP_DIRS.has(entry.name) ? [] : sourceFiles(join(dir, entry.name))
    }
    // test files may import a downstream package to exercise it without
    // creating a build-order edge, so they cannot vote here
    if (!SOURCE_FILE.test(entry.name) || /\.test\./.test(entry.name)) return []
    return [join(dir, entry.name)]
  })
}

function importedWorkspacePackages(dir: string): Set<string> {
  const imported = new Set<string>()
  for (const file of sourceFiles(dir)) {
    const source = readFileSync(file, 'utf8')
    for (const match of source.matchAll(IMPORT_SPECIFIER)) {
      const specifier = match[1]!
      if (specifier.startsWith('.') || specifier.startsWith('node:')) continue
      const name = specifier.startsWith('@')
        ? specifier.split('/').slice(0, 2).join('/')
        : specifier.split('/')[0]!
      if (workspaceNames.has(name)) imported.add(name)
    }
  }
  return imported
}

const packages = manifests.map(({ dir, pkg }) => ({
  pkg: {
    ...pkg,
    dependencies: {
      ...Object.fromEntries(
        [...importedWorkspacePackages(dir)]
          .filter((name) => name !== pkg.name)
          .map((name) => [name, 'workspace:* (source import)'])
      ),
      ...pkg.dependencies,
    },
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
