// orez/cf-deploy — Cloudflare wrangler-config normalization for the split
// app/data-worker Durable Object architecture. app-neutral: the DO binding/class
// names and the compat date are orez-architecture constants shared by every
// consumer (contrast, chat, …), NOT prefix-derived tokens, so they live here as
// fixed consts rather than on CfDeployConfig.

export type WranglerConfig = Record<string, unknown>
type WranglerBinding = Record<string, unknown>
type WranglerMigration = Record<string, unknown>

const ZERO_CACHE_DO_BINDING = 'ZERO_CACHE_DO'
const ZERO_SQL_DO_BINDING = 'ZERO_SQL_DO'
const ZERO_CACHE_DO_CLASS = 'ZeroCacheDO'
const ZERO_SQL_DO_CLASS = 'ZeroSqlDO'
const LEGACY_ZERO_DO_CLASS = 'ZeroDO'
const LEGACY_ZERO_DO_BINDING = 'ZERO_DO'
const LEGACY_ZERO_CACHE_MIGRATION_TAG = 'v2-zero-cache-do'
// the orez CF-DO embed (startZeroCacheEmbedCF + the SQL DO backend) needs a
// recent workerd to register the durable-object actors correctly; the older
// compat date `one build` writes (`2024-12-05`) drops into workerd's
// actor-detection fallback and the ZERO_*_DO bindings never resolve. pin to the
// date proven to bind cleanly under the installed runtime.
export const CLOUDFLARE_DO_COMPAT_DATE = '2026-03-29'

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined
}

function bindingName(binding: WranglerBinding): string {
  return typeof binding.name === 'string' ? binding.name : ''
}

function migrationNewSqliteClasses(migration: WranglerMigration): string[] {
  return Array.isArray(migration.new_sqlite_classes)
    ? migration.new_sqlite_classes.filter(
        (name): name is string => typeof name === 'string'
      )
    : []
}

function migrationDeletedClasses(migration: WranglerMigration): string[] {
  return Array.isArray(migration.deleted_classes)
    ? migration.deleted_classes.filter((name): name is string => typeof name === 'string')
    : []
}

function uniqueMigrationTag(migrations: WranglerMigration[], preferred: string): string {
  const tags = new Set(
    migrations
      .map((migration) => migration.tag)
      .filter((tag): tag is string => typeof tag === 'string')
  )
  if (!tags.has(preferred)) return preferred
  let index = 2
  while (tags.has(`${preferred}-${index}`)) index++
  return `${preferred}-${index}`
}

export function normalizeCloudflareDoWranglerConfig(
  config: WranglerConfig
): WranglerConfig {
  // keep the generated worker config in one's deploy shape before adding DOs.
  // attach EVERY root-level esbuild split chunk with one general glob. root
  // chunks are hash-suffixed: chunk-*.js (shared graphs, statically imported)
  // plus the DYNAMIC-import chunks esbuild names after their source module
  // (one-app-*.js, zero-cache-embed-cf-*.js, pg-proxy-do-backend-*.js, and any
  // future dependency split). `*-*.js` matches all of them and skips the
  // dash-free index.js `main` entry. per-package name enumeration is the trap
  // that caused the 2026-07-15 Pennywise wedge: orez 0.5.11 split out
  // pg-proxy-do-backend-*, no glob attached it, and — because dynamic
  // specifiers are NOT validated at upload — the worker deployed clean then
  // 500'd every embed boot with `No such module "pg-proxy-do-backend-….js"`.
  // a general glob cannot regress when a dependency adds another chunk.
  config.rules = [
    {
      type: 'ESModule',
      globs: ['assets/**/*.js', 'assets/**/*.mjs', '*-*.js'],
    },
  ]
  // expose the per-deploy version to the worker + its DOs so ZeroCacheDO can
  // scope its persisted boot-failure count to the build that recorded it. a
  // fixed redeploy (new CF_VERSION) then gets a clean boot attempt instead of
  // inheriting a stale terminal count it could never clear. the control-plane
  // app/data configs already bind this; user-app configs did not.
  config.version_metadata = { binding: 'CF_VERSION' }
  config.compatibility_date = CLOUDFLARE_DO_COMPAT_DATE

  // One sets `run_worker_first: true`, which routes EVERY request — including the
  // prerendered SSG shell and hashed /assets/* — through the (heavy, zero-cache-
  // embedding) worker. that makes even an edge-cached asset pay the worker's
  // cold-start, so the shell loads ~10x slower than a plain One deploy. only the
  // dynamic backend paths actually need the worker; the ~1.2k prerendered HTML
  // files + hashed assets are static and must serve straight from the Asset
  // Worker (edge cache). scope worker-first to the API + zero-sync paths so the
  // static shell loads at edge speed.
  const assets = asRecord(config.assets) ?? {}
  assets.run_worker_first = ['/api/*', '/sync', '/sync/*']
  config.assets = assets

  delete config.artifacts
  delete config.flagship
  delete config.vpc_networks

  const durableObjects = asRecord(config.durable_objects) ?? {}
  const existingBindings = Array.isArray(durableObjects.bindings)
    ? durableObjects.bindings.filter((binding): binding is WranglerBinding =>
        Boolean(asRecord(binding))
      )
    : []
  const managedBindings = new Set([
    LEGACY_ZERO_DO_BINDING,
    ZERO_CACHE_DO_BINDING,
    ZERO_SQL_DO_BINDING,
  ])
  durableObjects.bindings = [
    ...existingBindings.filter((binding) => !managedBindings.has(bindingName(binding))),
    { name: ZERO_CACHE_DO_BINDING, class_name: ZERO_CACHE_DO_CLASS },
    { name: ZERO_SQL_DO_BINDING, class_name: ZERO_SQL_DO_CLASS },
  ]
  config.durable_objects = durableObjects

  const migrations = Array.isArray(config.migrations)
    ? config.migrations.filter((migration): migration is WranglerMigration =>
        Boolean(asRecord(migration))
      )
    : []
  const existingClasses = new Set(migrations.flatMap(migrationNewSqliteClasses))
  const hasLegacyZeroDo = existingClasses.has(LEGACY_ZERO_DO_CLASS)
  const missingClasses = [ZERO_CACHE_DO_CLASS, ZERO_SQL_DO_CLASS].filter(
    (className) => !existingClasses.has(className)
  )

  if (!hasLegacyZeroDo) {
    const preferredTag = migrations.length === 0 ? 'v1' : LEGACY_ZERO_CACHE_MIGRATION_TAG
    config.migrations =
      missingClasses.length > 0
        ? [
            ...migrations,
            {
              tag: uniqueMigrationTag(migrations, preferredTag),
              new_sqlite_classes: missingClasses,
            },
          ]
        : migrations
    return config
  }

  if (missingClasses.length > 0) {
    config.migrations = [
      ...migrations,
      {
        tag: uniqueMigrationTag(migrations, LEGACY_ZERO_CACHE_MIGRATION_TAG),
        new_sqlite_classes: missingClasses,
      },
    ]
  } else {
    config.migrations = migrations
  }
  return config
}

/** keep the authoritative SQL DO while removing the embedded sync-cache class. */
export function normalizeCloudflareRustSyncWranglerConfig(
  config: WranglerConfig
): WranglerConfig {
  normalizeCloudflareDoWranglerConfig(config)

  const compatibilityFlags = Array.isArray(config.compatibility_flags)
    ? config.compatibility_flags.filter(
        (flag): flag is string => typeof flag === 'string'
      )
    : []
  config.compatibility_flags = [
    ...new Set([...compatibilityFlags, 'enable_request_signal']),
  ]

  const durableObjects = asRecord(config.durable_objects) ?? {}
  const bindings = Array.isArray(durableObjects.bindings)
    ? durableObjects.bindings.filter((binding): binding is WranglerBinding =>
        Boolean(asRecord(binding))
      )
    : []
  durableObjects.bindings = bindings.filter(
    (binding) => bindingName(binding) !== ZERO_CACHE_DO_BINDING
  )
  config.durable_objects = durableObjects

  const assets = asRecord(config.assets) ?? {}
  assets.run_worker_first = ['/api/*', '/zero-http', '/zero-http/*']
  config.assets = assets

  const migrations = Array.isArray(config.migrations)
    ? config.migrations.filter((migration): migration is WranglerMigration =>
        Boolean(asRecord(migration))
      )
    : []
  const deletedClasses = new Set(migrations.flatMap(migrationDeletedClasses))
  if (!deletedClasses.has(ZERO_CACHE_DO_CLASS)) {
    migrations.push({
      tag: uniqueMigrationTag(migrations, 'v3-rust-sync-host'),
      deleted_classes: [ZERO_CACHE_DO_CLASS],
    })
  }
  config.migrations = migrations
  return config
}
