import { createHash } from 'node:crypto'
import {
  copyFileSync,
  cpSync,
  existsSync,
  mkdirSync,
  realpathSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { homedir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'

import { build, type Plugin } from 'esbuild'

import { deployTimeSchemaBatchStatements } from '../../src/pg-proxy-do-backend.js'
import {
  getBrowserAliases,
  getBrowserDefine,
} from '../../src/worker/browser-build-config.js'
import { prepareZeroCacheForCF } from '../../src/worker/cf-patches.js'
import { instrumentChatDataWorker } from './chat-wrapper-source.js'
import {
  productionShapeDDL,
  productionShapeZeroSchemaSource,
} from './production-shape-fixture.js'

const root = resolve(import.meta.dir, '../..')
const generatedDir = resolve(import.meta.dir, '.generated-chat-wrapper')
rmSync(generatedDir, { recursive: true, force: true })
mkdirSync(generatedDir, { recursive: true })

function chatRoot(): string {
  const configured = process.env.OREZ_CHAT_PROFILE_REPO
  const candidate = configured ? resolve(configured) : resolve(homedir(), 'chat')
  if (!existsSync(join(candidate, 'src/deploy/cloudflareDoDeploy.ts'))) {
    throw new Error(
      `Chat checkout missing at ${candidate}; set OREZ_CHAT_PROFILE_REPO to the checkout under test`
    )
  }
  return candidate
}

const chat = chatRoot()
const chatImport = (path: string) => pathToFileURL(join(chat, path)).href
const chatDeploy = await import(chatImport('src/deploy/cloudflareDoDeploy.ts'))
const chatBundle = await import(chatImport('packages/orez-cf-deploy/src/bundle.ts'))
const chatMigration = await import(chatImport('packages/orez-cf-deploy/src/migration.ts'))
const chatLeaves = await import(chatImport('packages/orez-cf-deploy/src/leaves.ts'))
const chatSources = await import(chatImport('packages/orez-cf-deploy/src/sources.ts'))

const rawShim = String(chatDeploy.CLOUDFLARE_DO_SHIM_SOURCE)
const instrumented = instrumentChatDataWorker(rawShim)
const shimPath = join(generatedDir, 'chat-wrapper-worker.js')
writeFileSync(shimPath, instrumented.source)

const fixtureSchemaPath = join(generatedDir, 'fixture-schema.js')
writeFileSync(fixtureSchemaPath, productionShapeZeroSchemaSource())

const ddl = productionShapeDDL()
const initSqlBatchStatements = await deployTimeSchemaBatchStatements(ddl)
const zeroHttpShardSql = chatSources.zeroHttpShardDDL('chat')
const zeroHttpShardBatchStatements =
  await deployTimeSchemaBatchStatements(zeroHttpShardSql)
const schemaVersion = createHash('sha256')
  .update(JSON.stringify([initSqlBatchStatements, zeroHttpShardBatchStatements]))
  .update('\0orez-production-shape-chat-wrapper-v1')
  .digest('hex')
  .slice(0, 16)
const cfg = {
  prefix: 'chat',
  prefixPascal: 'Chat',
}
writeFileSync(
  join(generatedDir, 'orez-migrations.js'),
  chatMigration.buildMigrationModuleSource(cfg, {
    mode: 'full',
    schemaVersion,
    schemaImportSpecifier: './fixture-schema.js',
    migrationFiles: [],
    initSql: ddl,
    initSqlBatchStatements,
    zeroHttpShardSql,
    zeroHttpShardBatchStatements,
  })
)

const nodeModulesPath = realpathSync(resolve(root, 'node_modules'))
const selfPackagePath = join(nodeModulesPath, 'orez')
rmSync(selfPackagePath, { recursive: true, force: true })
mkdirSync(selfPackagePath, { recursive: true })
copyFileSync(join(root, 'package.json'), join(selfPackagePath, 'package.json'))
cpSync(join(root, 'dist'), join(selfPackagePath, 'dist'), { recursive: true })

const originalLog = console.log
console.log = console.error
let zeroOverlay: ReturnType<typeof prepareZeroCacheForCF>
try {
  zeroOverlay = prepareZeroCacheForCF({
    nodeModulesPath,
    outDir: resolve(generatedDir, '.orez/zero-cache-cf'),
  })
} finally {
  console.log = originalLog
}

const wasmSource = resolve(
  zeroOverlay.outDir,
  'node_modules/libpg-query/wasm/libpg-query.wasm'
)
const wasmTarget = resolve(generatedDir, 'libpg-query.wasm')
const wasmPlugin: Plugin = {
  name: 'profile-compiled-wasm',
  setup(buildApi) {
    buildApi.onResolve({ filter: /^libpg-query\/wasm\/libpg-query\.wasm$/ }, () => {
      mkdirSync(dirname(wasmTarget), { recursive: true })
      copyFileSync(wasmSource, wasmTarget)
      return { path: './libpg-query.wasm', external: true }
    })
  },
}
const localOrezPlugin: Plugin = {
  name: 'profile-local-orez',
  setup(buildApi) {
    buildApi.onResolve({ filter: /^orez\// }, (args) => {
      const subpath = args.path.slice('orez/'.length)
      const relative = subpath === 'cf-do' ? 'cf-do/worker.js' : `${subpath}.js`
      return { path: join(selfPackagePath, 'dist', relative) }
    })
  },
}
const aliases = getBrowserAliases(zeroOverlay)
for (const key of [
  'node:stream',
  'stream',
  'node:stream/promises',
  'stream/promises',
  'readable-stream',
]) {
  delete aliases[key]
}

await build({
  entryPoints: [shimPath],
  outfile: resolve(generatedDir, 'chat-wrapper-bundle.js'),
  bundle: true,
  format: 'esm',
  platform: 'neutral',
  target: 'es2022',
  conditions: ['workerd', 'worker', 'import'],
  mainFields: ['browser', 'module', 'main'],
  external: ['cloudflare:*', ...chatLeaves.NODE_EXTERNALS],
  define: {
    ...getBrowserDefine(),
    __filename: JSON.stringify('chat-wrapper-bundle.js'),
    __dirname: JSON.stringify('/'),
  },
  plugins: [
    wasmPlugin,
    localOrezPlugin,
    chatBundle.orezCfAliasPlugin(
      cfg,
      aliases,
      generatedDir,
      nodeModulesPath,
      generatedDir
    ),
  ],
  logLevel: 'warning',
})

writeFileSync(
  join(generatedDir, 'build-meta.json'),
  JSON.stringify(
    {
      chatRoot: chat,
      sourceHash: instrumented.sourceHash,
      schemaVersion,
      builtAt: new Date().toISOString(),
    },
    null,
    2
  )
)
