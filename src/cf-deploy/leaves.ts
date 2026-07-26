import { existsSync, readFileSync } from 'fs'
import { join, relative } from 'path'

import type { OnResolveArgs, PluginBuild } from 'esbuild'

export const WORKERS_DEV_URL_PATTERN =
  /https?:\/\/[a-z0-9-]+\.[a-z0-9-]+\.workers\.dev\b/i
export const HEALTH_POLL_INTERVAL_MS = 2_000
export const HEALTH_POLL_MAX_ATTEMPTS = 60
// after the worker is reachable, wait for the zero-cache embed to finish its
// cold-boot initial-sync (per-statement libpg parses over the full publication)
// so neither the runtime validation nor the first visitor races a half-booted
// embed whose view-syncer can't hydrate yet. /keepalive returns 200 once ready,
// 202 while booting. generous ceiling because a large publication's initial
// sync can run minutes; the outer deploy budget also needs time for DO
// preparation before orez starts its own readiness clock.
export const EMBED_READY_TIMEOUT_MS = 600_000
export const EMBED_WARM_TIMEOUT_MS = EMBED_READY_TIMEOUT_MS + 300_000
export const EMBED_WARM_INTERVAL_MS = 3_000
export const EMBED_WARM_REQUEST_TIMEOUT_MS = 8_000
export const INTERNAL_ZERO_MUTATE_URL = 'https://orez-zero-api.local/api/zero/push'
export const INTERNAL_ZERO_QUERY_URL = 'https://orez-zero-api.local/api/zero/pull'

export function isOrezAliasImporter(importer: string): boolean {
  const normalized = importer.replaceAll('\\', '/')
  return (
    normalized.includes('/node_modules/orez/') ||
    normalized.includes('/.orez/zero-cache-cf/') ||
    normalized.includes('/node_modules/pgsql-parser/') ||
    normalized.includes('/node_modules/basic-auth/') ||
    normalized.includes('/node_modules/safe-buffer/') ||
    normalized.includes('/node_modules/cloudevents/') ||
    normalized.includes('/node_modules/@databases/') ||
    normalized.includes('/node_modules/pg/') ||
    normalized.includes('/node_modules/pg-cloudflare/') ||
    normalized.includes('/node_modules/pg-connection-string/') ||
    normalized.includes('/node_modules/pg-pool/') ||
    normalized.includes('/node_modules/pgpass/') ||
    normalized.includes('/node_modules/split2/')
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
