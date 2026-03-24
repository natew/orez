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
 * the consumer must have these packages installed:
 * - orez (provides postgres/sqlite/fastify/ws shims)
 * - events (EventEmitter polyfill)
 * - buffer (Buffer polyfill)
 * - process (process polyfill)
 *
 * optional (only needed if zero-cache code uses them):
 * - crypto-browserify, stream-browserify, path-browserify, os-browserify
 */
export function getBrowserAliases(): Record<string, string> {
  return {
    // -- orez shims for zero-cache dependencies --
    postgres: 'orez/worker/shims/postgres',
    '@rocicorp/zero-sqlite3': 'orez/worker/shims/sqlite',
    fastify: 'orez/worker/shims/fastify',
    ws: 'orez/worker/shims/ws',

    // -- Node.js built-in polyfills --
    // these are needed because zero-cache imports node: modules.
    // the bundler replaces them with browser-compatible packages.
    'node:events': 'events',
    'node:buffer': 'buffer',
    'node:process': 'process/browser',
    'node:crypto': 'orez/worker/shims/node-stub',
    'crypto-browserify': 'orez/worker/shims/node-stub',
    'node:stream': 'stream-browserify',
    'node:path': 'path-browserify',
    'node:os': 'os-browserify/browser',

    // -- stubs for Node.js modules that zero-cache imports but doesn't --
    // -- use in SINGLE_PROCESS mode --
    'node:http': 'orez/worker/shims/node-stub',
    'node:net': 'orez/worker/shims/node-stub',
    'node:tls': 'orez/worker/shims/node-stub',
    'node:child_process': 'orez/worker/shims/node-stub',
    'node:fs': 'orez/worker/shims/node-stub',
    'node:fs/promises': 'orez/worker/shims/node-stub',
    'node:url': 'orez/worker/shims/node-stub',
    'node:util': 'orez/worker/shims/node-stub',
    'node:assert': 'orez/worker/shims/node-stub',
    'node:inspector/promises': 'orez/worker/shims/node-stub',
    'node:v8': 'orez/worker/shims/node-stub',
    'node:zlib': 'orez/worker/shims/node-stub',
  }
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
