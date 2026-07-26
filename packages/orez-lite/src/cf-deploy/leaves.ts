import { existsSync, readFileSync } from 'fs'
import { join, relative } from 'path'

import type { OnResolveArgs, PluginBuild } from 'esbuild'

export function isOrezLiteImporter(importer: string): boolean {
  const normalized = importer.replaceAll('\\', '/')
  return (
    normalized.includes('/node_modules/orez-lite/') ||
    normalized.includes('/node_modules/orez-sync-cf-host/') ||
    normalized.includes('/node_modules/orez-sync-executor/') ||
    normalized.includes('/packages/orez-lite/') ||
    normalized.includes('/packages/sync-cf-host/') ||
    normalized.includes('/packages/sync-executor/')
  )
}

export async function resolveAlias(
  buildApi: PluginBuild,
  path: string,
  resolveDir: string,
  kind: OnResolveArgs['kind']
) {
  const target = packageEntryForDirectory(path)
  const resolved = await buildApi.resolve(target, { resolveDir, kind })
  if (resolved.errors.length) return undefined
  return resolved
}

export function packageEntryForDirectory(path: string): string {
  const packagePath = join(path, 'package.json')
  if (!existsSync(packagePath)) return path
  const pkg = JSON.parse(readFileSync(packagePath, 'utf-8')) as {
    module?: string
    main?: string
  }
  return join(path, pkg.module || pkg.main || 'index.js')
}

export function importSpecifier(fromDir: string, filePath: string): string {
  const specifier = relative(fromDir, filePath).replaceAll('\\', '/')
  return specifier.startsWith('.') ? specifier : `./${specifier}`
}

export function quoteSqlIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

export const NODE_EXTERNALS = [
  'node:*',
  'async_hooks',
  'buffer',
  'child_process',
  'crypto',
  'diagnostics_channel',
  'dns',
  'dns/promises',
  'events',
  'fs',
  'fs/promises',
  'http',
  'http2',
  'https',
  'inspector/promises',
  'module',
  'net',
  'os',
  'path',
  'perf_hooks',
  'process',
  'querystring',
  'stream',
  'stream/promises',
  'tls',
  'tty',
  'url',
  'util',
  'v8',
  'worker_threads',
  'zlib',
]
