/**
 * browser build configuration for zero-cache embed.
 *
 * provides the bundler alias map and polyfill configuration needed
 * to bundle zero-cache for browser Web Workers.
 *
 * usage with esbuild:
 *
 *   import { getBrowserAliases, getBrowserDefine } from 'orez/worker/browser-build-config'
 *
 *   await esbuild.build({
 *     entryPoints: ['./my-worker.ts'],
 *     bundle: true,
 *     format: 'esm',
 *     alias: getBrowserAliases(),
 *     define: getBrowserDefine(),
 *   })
 *
 * usage with vite:
 *
 *   import { getBrowserAliases } from 'orez/worker/browser-build-config'
 *
 *   export default defineConfig({
 *     resolve: { alias: getBrowserAliases() },
 *     worker: { format: 'es' },
 *   })
 */

/**
 * bundler aliases that swap zero-cache's Node.js dependencies
 * for browser-compatible shims.
 *
 * the consumer must have orez installed for postgres/sqlite/fastify/ws/node
 * shims. no transitive polyfill packages are required.
 */
export interface BrowserAliasOptions {
  zeroCacheSrcDir?: string
  zeroOutDir?: string
  aliases?: Record<string, string>
}

export function getBrowserAliases(
  options: BrowserAliasOptions = {}
): Record<string, string> {
  const aliases: Record<string, string> = {
    // -- orez shims for zero-cache dependencies --
    // postgres-browser uses the real postgres package with MessagePort transport
    // to pg-proxy-browser, matching orez-node's wire protocol architecture.
    // falls back to old PGlite-wrapping shim if postgres-browser isn't available.
    postgres: 'orez/worker/shims/postgres-browser',
    '@rocicorp/zero-sqlite3': 'orez/worker/shims/sqlite',
    fastify: 'orez/worker/shims/fastify',
    ws: 'orez/worker/shims/ws',
    oxfmt: 'orez/worker/shims/oxfmt',

    // -- Node.js built-in polyfills --
    // these are needed because zero-cache imports node: modules.
    // the bundler replaces them with browser-compatible packages.
    'node:events': 'orez/worker/shims/node-stub',
    events: 'orez/worker/shims/node-stub',
    'node:buffer': 'orez/worker/shims/node-stub',
    buffer: 'orez/worker/shims/node-stub',
    'node:process': 'orez/worker/shims/node-stub',
    process: 'orez/worker/shims/node-stub',
    'process/browser': 'orez/worker/shims/node-stub',
    'node:crypto': 'orez/worker/shims/node-stub',
    crypto: 'orez/worker/shims/node-stub',
    'crypto-browserify': 'orez/worker/shims/node-stub',
    'node:stream': 'orez/worker/shims/stream-browser',
    stream: 'orez/worker/shims/stream-browser',
    'node:stream/promises': 'orez/worker/shims/node-stub',
    'stream/promises': 'orez/worker/shims/node-stub',
    'node:path': 'orez/worker/shims/node-stub',
    path: 'orez/worker/shims/node-stub',
    'node:os': 'orez/worker/shims/node-stub',
    os: 'orez/worker/shims/node-stub',

    // -- stubs for Node.js modules that zero-cache imports but doesn't --
    // -- use in SINGLE_PROCESS mode --
    'node:http': 'orez/worker/shims/node-stub',
    http: 'orez/worker/shims/node-stub',
    'node:https': 'orez/worker/shims/node-stub',
    https: 'orez/worker/shims/node-stub',
    'node:http2': 'orez/worker/shims/node-stub',
    http2: 'orez/worker/shims/node-stub',
    'node:net': 'orez/worker/shims/node-stub',
    net: 'orez/worker/shims/node-stub',
    'node:tls': 'orez/worker/shims/node-stub',
    tls: 'orez/worker/shims/node-stub',
    'node:child_process': 'orez/worker/shims/node-stub',
    child_process: 'orez/worker/shims/node-stub',
    'node:fs': 'orez/worker/shims/node-stub',
    fs: 'orez/worker/shims/node-stub',
    'node:fs/promises': 'orez/worker/shims/node-stub',
    'fs/promises': 'orez/worker/shims/node-stub',
    'node:url': 'orez/worker/shims/node-stub',
    url: 'orez/worker/shims/node-stub',
    'node:util': 'orez/worker/shims/node-stub',
    util: 'orez/worker/shims/node-stub',
    'node:assert': 'orez/worker/shims/node-stub',
    assert: 'orez/worker/shims/node-stub',
    'node:async_hooks': 'orez/worker/shims/node-stub',
    async_hooks: 'orez/worker/shims/node-stub',
    'node:diagnostics_channel': 'orez/worker/shims/node-stub',
    diagnostics_channel: 'orez/worker/shims/node-stub',
    'node:dns': 'orez/worker/shims/node-stub',
    dns: 'orez/worker/shims/node-stub',
    'node:dns/promises': 'orez/worker/shims/node-stub',
    'dns/promises': 'orez/worker/shims/node-stub',
    'node:querystring': 'orez/worker/shims/node-stub',
    querystring: 'orez/worker/shims/node-stub',
    'node:inspector/promises': 'orez/worker/shims/node-stub',
    'inspector/promises': 'orez/worker/shims/node-stub',
    'node:v8': 'orez/worker/shims/node-stub',
    v8: 'orez/worker/shims/node-stub',
    'node:zlib': 'orez/worker/shims/node-stub',
    zlib: 'orez/worker/shims/node-stub',
    'node:module': 'orez/worker/shims/node-stub',
    module: 'orez/worker/shims/node-stub',
    'node:perf_hooks': 'orez/worker/shims/node-stub',
    perf_hooks: 'orez/worker/shims/node-stub',
    'node:worker_threads': 'orez/worker/shims/node-stub',
    worker_threads: 'orez/worker/shims/node-stub',
    'node:tty': 'orez/worker/shims/node-stub',
    tty: 'orez/worker/shims/node-stub',
  }
  const zeroCacheSrcDir =
    options.zeroCacheSrcDir ||
    (options.zeroOutDir ? `${options.zeroOutDir}/zero-cache/src` : undefined)
  if (zeroCacheSrcDir) {
    aliases['@rocicorp/zero/out/zero-cache/src/server/runner/run-worker.js'] =
      `${zeroCacheSrcDir}/server/runner/run-worker.js`
  }
  Object.assign(aliases, options.aliases)
  return aliases
}

/**
 * esbuild define map for browser builds.
 * replaces Node.js globals with browser equivalents.
 */
export function getBrowserDefine(): Record<string, string> {
  return {
    'process.env.NODE_ENV': '"development"',
    'process.env.SINGLE_PROCESS': '"1"',
    'process.versions.node': '"20.0.0"',
  }
}

/**
 * combined config for esbuild builds.
 * merges aliases, define, and common settings.
 */
export function getBrowserBuildConfig() {
  return {
    alias: getBrowserAliases(),
    define: getBrowserDefine(),
    // recommended esbuild settings for browser worker bundles
    format: 'esm' as const,
    platform: 'browser' as const,
    target: 'es2022',
    bundle: true,
  }
}
