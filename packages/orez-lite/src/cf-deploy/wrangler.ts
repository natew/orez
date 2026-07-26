export type WranglerConfig = Record<string, unknown>
type WranglerBinding = Record<string, unknown>
type WranglerMigration = Record<string, unknown>

export const CLOUDFLARE_COMPATIBILITY_DATE = '2026-03-29'

function record(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === 'string')
    : []
}

/**
 * Add the current Orez Lite Durable Object contract to a Wrangler config.
 *
 * The caller owns routes, assets, services, and every app-specific binding.
 */
export function configureCloudflareWorker(config: WranglerConfig): WranglerConfig {
  config.compatibility_date = CLOUDFLARE_COMPATIBILITY_DATE
  config.compatibility_flags = [
    ...new Set([...stringArray(config.compatibility_flags), 'enable_request_signal']),
  ]

  const durableObjects = record(config.durable_objects) ?? {}
  const bindings = Array.isArray(durableObjects.bindings)
    ? durableObjects.bindings.filter((binding): binding is WranglerBinding =>
        Boolean(record(binding))
      )
    : []
  if (
    !bindings.some(
      (binding) => binding.name === 'ZERO_SQL_DO' && binding.class_name === 'ZeroSqlDO'
    )
  ) {
    bindings.push({ name: 'ZERO_SQL_DO', class_name: 'ZeroSqlDO' })
  }
  durableObjects.bindings = bindings
  config.durable_objects = durableObjects

  const migrations = Array.isArray(config.migrations)
    ? config.migrations.filter((migration): migration is WranglerMigration =>
        Boolean(record(migration))
      )
    : []
  const hasZeroSqlMigration = migrations.some((migration) =>
    stringArray(migration.new_sqlite_classes).includes('ZeroSqlDO')
  )
  if (!hasZeroSqlMigration) {
    const tags = new Set(
      migrations
        .map((migration) => migration.tag)
        .filter((tag): tag is string => typeof tag === 'string')
    )
    let tag = 'orez-lite-v1'
    let suffix = 2
    while (tags.has(tag)) tag = `orez-lite-v${suffix++}`
    migrations.push({ tag, new_sqlite_classes: ['ZeroSqlDO'] })
  }
  config.migrations = migrations
  return config
}
