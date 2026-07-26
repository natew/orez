import { existsSync, readFileSync, writeFileSync } from 'fs'
import { dirname, join, relative } from 'path'

import { quoteSqlIdentifier } from './leaves.js'

import type { CfDeployConfig } from './config.js'

const SENTINEL_LOWER = 'nspfx'
const SENTINEL_PASCAL = 'Nspfx'

function applyPrefix(template: string, cfg: CfDeployConfig): string {
  return template
    .split(SENTINEL_PASCAL)
    .join(cfg.prefixPascal)
    .split(SENTINEL_LOWER)
    .join(cfg.prefix)
}

export function zeroHttpShardDDL(appId: string): string {
  const appSchema = `${appId}_0`
  const schema = quoteSqlIdentifier(appSchema)
  return [
    `CREATE SCHEMA IF NOT EXISTS ${schema};`,
    `CREATE TABLE IF NOT EXISTS ${schema}.clients (` +
      `"clientGroupID" text NOT NULL, ` +
      `"clientID" text NOT NULL, ` +
      `"lastMutationID" bigint NOT NULL, ` +
      `PRIMARY KEY ("clientGroupID", "clientID")` +
      `);`,
    `ALTER TABLE ${schema}.clients ADD COLUMN IF NOT EXISTS "userID" text;`,
    `CREATE TABLE IF NOT EXISTS ${schema}.mutations (` +
      `"clientGroupID" text NOT NULL, ` +
      `"clientID" text NOT NULL, ` +
      `"mutationID" bigint NOT NULL, ` +
      `result json NOT NULL, ` +
      `PRIMARY KEY ("clientGroupID", "clientID", "mutationID")` +
      `);`,
  ].join('\n--> statement-breakpoint\n')
}

export function cloudflarePgVirtualModule(
  cfg: CfDeployConfig,
  doBackendPath: string
): string {
  return applyPrefix(
    `
const backends = new Map()
let doBackendClassPromise

function getDoBackend() {
  doBackendClassPromise ||= import(${JSON.stringify(doBackendPath)}).then(
    (module) => module.DoBackend,
  )
  return doBackendClassPromise
}

function parseConnectionString(connectionString) {
  try {
    const url = new URL(connectionString || 'orez-do://postgres')
    return {
      dbName: url.hostname || url.pathname.replace(/^\\/+/, '') || 'postgres',
      namespace:
        url.searchParams.get('ns') ||
        globalThis.__nspfx_cf_do_namespace ||
        'zero',
      instance: url.searchParams.get('instance') || 'singleton',
    }
  } catch {
    return {
      dbName: 'postgres',
      namespace: globalThis.__nspfx_cf_do_namespace || 'zero',
      instance: 'singleton',
    }
  }
}

async function backendFor(connectionString) {
  const { dbName, namespace, instance } = parseConnectionString(connectionString)
  const key = instance + '\\0' + namespace + '\\0' + dbName
  const existing = backends.get(key)
  if (existing) return existing

  const DoBackend = await getDoBackend()
  const concurrent = backends.get(key)
  if (concurrent) return concurrent

  // ALWAYS resolve the DO fetch from the per-instance registry at CALL time,
  // never capture it. a DO stub (env.ZERO_SQL_DO.get(id)) is bound to the
  // request whose env produced it; reusing a captured stub in a later request
  // makes workerd hang the subrequest ("Promise will never complete").
  // installSqlBackendGlobals refreshes the registry entry every request, and
  // keying by instance keeps co-located per-namespace DOs in one isolate from
  // cross-routing each other's SQL.
  const fetch = (input, init) => {
    const current = (globalThis.__nspfx_cf_do_sql_fetch_by_instance || {})[instance]
    if (typeof current !== 'function') {
      throw new Error('Cloudflare SQL Durable Object binding is not initialized')
    }
    return current(input, init)
  }

  const backend = new DoBackend('https://orez-do-backend.local', dbName, namespace, {
    fetch,
  })
  backends.set(key, backend)
  return backend
}

function normalizeQueryArgs(query, params, callback) {
  if (typeof params === 'function') {
    callback = params
    params = undefined
  }
  if (query && typeof query === 'object') {
    return {
      text: query.text || '',
      values: query.values || query.params || [],
      rowMode: query.rowMode,
      callback,
    }
  }
  return { text: String(query || ''), values: params || [], rowMode: undefined, callback }
}

class DoClient {
  constructor(pool) {
    this.pool = pool
  }

  on() {
    return this
  }

  release() {}

  query(query, params, callback) {
    return this.pool.query(query, params, callback)
  }
}

export class Pool {
  constructor(config = {}) {
    this.connectionString =
      typeof config === 'string' ? config : config.connectionString || ''
  }

  on() {
    return this
  }

  async connect(callback) {
    const promise = Promise.resolve(new DoClient(this))
    if (typeof callback === 'function') {
      promise.then((client) => callback(null, client), callback)
    }
    return promise
  }

  query(query, params, callback) {
    const normalized = normalizeQueryArgs(query, params, callback)
    let promise = backendFor(this.connectionString).then((backend) =>
      backend.query(normalized.text, normalized.values),
    )
    // honor pg's rowMode:'array' contract. drizzle-orm's node-postgres typed
    // SELECT path passes { rowMode: 'array' } and then indexes each row
    // POSITIONALLY via mapResultRow(fields, row) -> row[0], row[1], ...
    // the DO backend returns rows as column-keyed OBJECTS, so without this a
    // real result row { id, createdAt, ... } read as row[0] yields undefined,
    // and the first typed decoder (timestamp/json/numeric) throws on undefined
    // (e.g. "Cannot read properties of undefined (reading 'toISOString')").
    // project objects into arrays in column order, exactly as node-postgres does.
    if (normalized.rowMode === 'array') {
      promise = promise.then((result) => {
        if (!result || !Array.isArray(result.rows) || result.rows.length === 0) {
          return result
        }
        const keys = Object.keys(result.rows[0])
        return {
          ...result,
          rows: result.rows.map((row) => keys.map((key) => row[key])),
          fields: result.fields || keys.map((name) => ({ name })),
        }
      })
    }
    if (typeof normalized.callback === 'function') {
      promise.then((result) => normalized.callback(null, result), normalized.callback)
    }
    return promise
  }

  async end() {}
}

export class Client extends Pool {}
export const types = { setTypeParser() {} }
export const defaults = {}
export const native = null
export default { Pool, Client, types, defaults, native }
`,
    cfg
  )
}

export function cloudflareZeroServerPoolFile(cfg: CfDeployConfig): string {
  return `${cfg.prefix}-cloudflare-zero-server-pool.ts`
}

export function cloudflareZeroServerPoolSource(cfg: CfDeployConfig): string {
  return applyPrefix(
    `export function createNspfxCloudflareZeroServerPool(connectionString = '') {
  const createPool = globalThis.__nspfx_cf_do_create_pg_pool
  if (typeof createPool !== 'function') {
    throw new Error('Cloudflare SQL Durable Object pool factory is not initialized')
  }
  return createPool(connectionString)
}
`,
    cfg
  )
}

export function patchZeroServerForCloudflareDo(
  cfg: CfDeployConfig,
  source: string,
  zeroServerPath: string
): string {
  const createPoolName = `create${cfg.prefixPascal}CloudflareZeroServerPool`
  if (source.includes(createPoolName)) return source
  // native sqlite servers already receive their owning transaction provider.
  // their app graph must never be rewritten back through the legacy pg pool.
  // all spellings are tenant source we only detect, never emit. templates use
  // createSQLiteApplicationDatabase directly; the platform app owns the same
  // database behind createApplicationSqlDatabase.
  if (/\bcreateSQLiteApplicationDatabase\s*\(/.test(source)) return source
  if (/\bcreateApplicationSqlDatabase\s*\(/.test(source)) return source
  if (/\bcreateZeroSQLiteServer\s*\(/.test(source)) return source
  // the platform server can own the same application database one module
  // below this entrypoint. getZeroPushRuntime constructs its executor through
  // createApplicationSqlDatabase, so rewriting this binding would introduce a
  // second database path instead of adapting the one it already owns.
  if (/\bgetZeroPushRuntime\s*\(/.test(source)) return source
  // seam-based sources already route through the installed pool factory at
  // runtime (zeroServerDatabase reads the same global the DO shim installs),
  // so they need no rewrite here; the seam module itself is hardened for the
  // DO build by patchZeroServerDatabaseForCloudflareDo.
  if (/\.\.\.\s*zeroServerDatabase\s*\(/.test(source)) return source
  const databaseConfigPattern = /database\s*:\s*server\.ZERO_UPSTREAM_DB\s*,?/
  if (!databaseConfigPattern.test(source)) {
    throw new Error(
      `Could not patch ${relative(process.cwd(), zeroServerPath)} for Cloudflare DO: expected an application SQL runtime or createZeroServer({ database: server.ZERO_UPSTREAM_DB })`
    )
  }
  return `import { ${createPoolName} } from './${cloudflareZeroServerPoolFile(cfg).replace(/\.ts$/, '')}'\n${source}`.replace(
    databaseConfigPattern,
    `pool: ${createPoolName}(server.ZERO_UPSTREAM_DB),`
  )
}

export function prepareCloudflareDoBuildDirectory(
  cfg: CfDeployConfig,
  buildDir: string
): void {
  const viteConfigPath = join(buildDir, 'vite.config.ts')
  if (!existsSync(viteConfigPath)) return

  const zeroServerPaths = [
    join(buildDir, 'data', 'zero-server.ts'),
    join(buildDir, 'src', 'zero', 'server.ts'),
  ].filter((path) => existsSync(path))

  for (const zeroServerPath of zeroServerPaths) {
    const zeroServerDir = dirname(zeroServerPath)
    const originalZeroServerSource = readFileSync(zeroServerPath, 'utf-8')
    const patchedZeroServerSource = patchZeroServerForCloudflareDo(
      cfg,
      originalZeroServerSource,
      zeroServerPath
    )
    const createPoolName = `create${cfg.prefixPascal}CloudflareZeroServerPool`
    if (patchedZeroServerSource.includes(createPoolName)) {
      writeFileSync(
        join(zeroServerDir, cloudflareZeroServerPoolFile(cfg)),
        cloudflareZeroServerPoolSource(cfg)
      )
    }
    if (patchedZeroServerSource !== originalZeroServerSource) {
      writeFileSync(zeroServerPath, patchedZeroServerSource)
    }
  }

  const seamPath = join(buildDir, 'src', 'zero', 'zeroServerDatabase.ts')
  if (existsSync(seamPath)) {
    writeFileSync(
      seamPath,
      patchZeroServerDatabaseForCloudflareDo(readFileSync(seamPath, 'utf-8'), seamPath)
    )
  }
}

// inside the worker bundle the seam's `database:` fallback would let on-zero
// silently build its own pg pool when the shim's factory is missing, instead
// of failing fast the way the generated pool helper always has. harden the
// fallback into a hard error for DO builds. (the sqlite-pool side-effect
// import is stripped separately by the app's build-mutated-files pass, which
// owns that concern for every file.)
export function patchZeroServerDatabaseForCloudflareDo(
  source: string,
  seamPath: string
): string {
  if (source.includes('pool factory is not initialized')) return source
  const fallbackPattern =
    /if\s*\(typeof factory !== 'function'\)\s*return\s*\{\s*database:\s*connectionString\s*\}/
  if (!fallbackPattern.test(source)) {
    throw new Error(
      `Could not patch ${relative(process.cwd(), seamPath)} for Cloudflare DO: expected the database fallback`
    )
  }
  return source.replace(
    fallbackPattern,
    `if (typeof factory !== 'function') {\n    throw new Error('Cloudflare SQL Durable Object pool factory is not initialized')\n  }`
  )
}
