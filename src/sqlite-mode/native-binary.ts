import { existsSync, readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { dirname, resolve } from 'node:path'

import { resolvePackage } from './package-resolve.js'

const NATIVE_BINARY_RELATIVE_PATHS = ['build/Release/better_sqlite3.node']

function resolvedPrebuildPath(packageRoot: string): string {
  if (!packageRoot) return ''
  try {
    const requireFromPackage = createRequire(resolve(packageRoot, 'package.json'))
    const loader = requireFromPackage('node-gyp-build') as {
      path?(root: string): unknown
    }
    const filePath = loader.path?.(packageRoot)
    return typeof filePath === 'string' ? filePath : ''
  } catch {
    return ''
  }
}

export interface NativeBinaryCheckResult {
  packageEntryPath: string
  packageRoot: string
  expectedPaths: string[]
  existingPaths: string[]
  found: boolean
}

function findPackageRoot(entryPath: string): string {
  if (!entryPath) return ''

  let dir = dirname(entryPath)
  for (let i = 0; i < 12; i++) {
    const pkgJson = resolve(dir, 'package.json')
    if (existsSync(pkgJson)) {
      try {
        const pkg = JSON.parse(readFileSync(pkgJson, 'utf-8'))
        if (pkg?.name === '@rocicorp/zero-sqlite3') {
          return dir
        }
      } catch {
        // ignore malformed package.json and continue searching upward
      }
    }

    const parent = dirname(dir)
    if (parent === dir) break
    dir = parent
  }

  return ''
}

export function inspectNativeSqliteBinary(): NativeBinaryCheckResult {
  const packageEntryPath = resolvePackage('@rocicorp/zero-sqlite3')
  const packageRoot = findPackageRoot(packageEntryPath)
  const expectedPaths = packageRoot
    ? [
        ...NATIVE_BINARY_RELATIVE_PATHS.map((relativePath) =>
          resolve(packageRoot, relativePath)
        ),
        resolvedPrebuildPath(packageRoot),
      ].filter((filePath, index, paths) => filePath && paths.indexOf(filePath) === index)
    : []
  const existingPaths = expectedPaths.filter((filePath) => existsSync(filePath))

  return {
    packageEntryPath,
    packageRoot,
    expectedPaths,
    existingPaths,
    found: existingPaths.length > 0,
  }
}

export function hasMissingNativeBinarySignature(message: string): boolean {
  const text = message.toLowerCase()
  return (
    text.includes('better_sqlite3.node') ||
    (text.includes('could not locate the bindings file') &&
      text.includes('zero-sqlite3')) ||
    (text.includes('err_dlopen_failed') && text.includes('better_sqlite3')) ||
    (text.includes('no native build was found') && text.includes('sqlite'))
  )
}

export function formatNativeBootstrapInstructions(
  result: NativeBinaryCheckResult
): string {
  const expectedList =
    result.expectedPaths.length > 0
      ? result.expectedPaths.map((filePath) => `  - ${filePath}`).join('\n')
      : '  - <unable to resolve @rocicorp/zero-sqlite3 package root>'

  return [
    'native sqlite binary is missing.',
    `resolved package entry: ${result.packageEntryPath || '<not resolved>'}`,
    'expected native binary path(s):',
    expectedList,
    'fix:',
    '  bun i @rocicorp/zero-sqlite3',
    '  bun run native:bootstrap',
    'manual emergency fallback (not automated): copy a known-good better_sqlite3.node into build/Release and re-run native:bootstrap.',
  ].join('\n')
}
