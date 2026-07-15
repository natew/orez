import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
  statSync,
} from 'node:fs'
import { dirname, extname, join, resolve } from 'node:path'

import { build, type OnResolveArgs, type Plugin } from 'esbuild'

import { getBrowserAliases, getBrowserDefine } from '../../worker/browser-build-config.js'
import { prepareZeroCacheForCF } from '../../worker/cf-patches.js'

const fixtureDir = import.meta.dirname
const rootDir = resolve(fixtureDir, '../../..')
const outDir = join(fixtureDir, 'dist')
const overlay = prepareZeroCacheForCF({
  nodeModulesPath: join(rootDir, 'node_modules'),
  outDir: join(fixtureDir, '.orez/zero-cache-cf'),
})
const aliases = getBrowserAliases(overlay)
for (const streamModule of [
  'node:stream',
  'stream',
  'node:stream/promises',
  'stream/promises',
  'readable-stream',
]) {
  delete aliases[streamModule]
}

function packageEntry(path: string): string {
  if (!existsSync(path) || !statSync(path).isDirectory()) return path
  const packageJson = join(path, 'package.json')
  if (!existsSync(packageJson)) return join(path, 'index.js')
  const pkg = JSON.parse(readFileSync(packageJson, 'utf8')) as {
    browser?: string | Record<string, string | false>
    module?: string
    main?: string
  }
  const entry =
    (typeof pkg.browser === 'string' ? pkg.browser : undefined) ||
    pkg.module ||
    pkg.main ||
    'index.js'
  return join(path, entry)
}

function orezSource(specifier: string): string | undefined {
  if (!specifier.startsWith('orez/')) return undefined
  const relative = specifier.slice('orez/'.length)
  const source = resolve(rootDir, 'src', relative)
  for (const candidate of [source, `${source}.ts`, `${source}.js`]) {
    if (existsSync(candidate)) return candidate
  }
  return undefined
}

function aliasTarget(specifier: string, target: string): string {
  const fromOrez = orezSource(target)
  if (fromOrez) return fromOrez
  if (existsSync(target)) return packageEntry(target)
  return target
}

const aliasEntries = Object.entries(aliases).sort(
  ([left], [right]) => right.length - left.length
)

function fixtureAliases(): Plugin {
  return {
    name: 'orez-two-do-fixture-aliases',
    setup(api) {
      api.onResolve({ filter: /zero-cache-run-worker\.js$/ }, (args) => {
        if (!args.importer.endsWith('/worker/zero-cache-embed-cf.ts')) return undefined
        return { path: join(fixtureDir, 'probe-run-worker.ts') }
      })

      api.onResolve({ filter: /^libpg-query\/wasm\/libpg-query\.wasm$/ }, () => {
        const source = resolve(
          overlay.outDir,
          'node_modules/libpg-query/wasm/libpg-query.wasm'
        )
        const target = join(outDir, 'libpg-query.wasm')
        mkdirSync(dirname(target), { recursive: true })
        copyFileSync(source, target)
        return { path: './libpg-query.wasm', external: true }
      })

      api.onResolve({ filter: /.*/ }, (args: OnResolveArgs) => {
        for (const [specifier, target] of aliasEntries) {
          if (args.path !== specifier && !args.path.startsWith(`${specifier}/`)) continue
          const suffix = args.path.slice(specifier.length)
          const resolved = aliasTarget(specifier, target)
          const path =
            suffix && existsSync(resolved) && statSync(resolved).isDirectory()
              ? join(resolved, suffix)
              : resolved
          return { path: extname(path) ? path : packageEntry(path) }
        }
        return undefined
      })
    },
  }
}

rmSync(outDir, { recursive: true, force: true })
mkdirSync(outDir, { recursive: true })

await build({
  entryPoints: [join(fixtureDir, 'worker.ts')],
  outfile: join(outDir, 'worker.js'),
  bundle: true,
  conditions: ['workerd', 'worker', 'import'],
  define: {
    ...getBrowserDefine(),
    __dirname: JSON.stringify('/'),
    __filename: JSON.stringify('worker.js'),
  },
  external: [
    'cloudflare:*',
    'node:stream',
    'stream',
    'node:stream/promises',
    'stream/promises',
  ],
  format: 'esm',
  logLevel: 'info',
  mainFields: ['browser', 'module', 'main'],
  platform: 'neutral',
  plugins: [fixtureAliases()],
  target: 'es2022',
})
