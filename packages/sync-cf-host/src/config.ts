import type { PullCaps, SyncHostConfig, SyncHostEnv } from './types.js'

export function validatePullCaps(caps: PullCaps): PullCaps {
  if (!Number.isSafeInteger(caps.maxChangeRows) || caps.maxChangeRows < 1) {
    throw new TypeError('caps.maxChangeRows must be a positive safe integer')
  }
  return caps
}

export function validateSyncHostConfig<Env extends SyncHostEnv>(
  config: SyncHostConfig<Env>
): SyncHostConfig<Env> {
  const hasMutators = config.mutators !== undefined
  const hasDelegate = config.mutateUrl !== undefined
  if (hasMutators === hasDelegate) {
    throw new TypeError('sync host config requires exactly one of mutators or mutateUrl')
  }
  if (hasDelegate && !config.upstream) {
    throw new TypeError('sync host config mutateUrl requires upstream')
  }
  if (config.mutateBinding !== undefined && !hasDelegate) {
    throw new TypeError('sync host config mutateBinding requires mutateUrl')
  }
  if (config.mutateBinding !== undefined && !config.mutateBinding) {
    throw new TypeError('sync host config mutateBinding must not be empty')
  }
  if (hasMutators && config.upstream) {
    throw new TypeError(
      'sync host config cannot combine local mutators with upstream ingest'
    )
  }
  if (config.mutateUrl && !config.mutateUrl.startsWith('/')) {
    throw new TypeError('mutateUrl must be an absolute path')
  }
  if (config.upstream) {
    if (!config.upstream.binding) throw new TypeError('upstream.binding is required')
    const limit = config.upstream.changeLimit ?? 1_000
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 10_000) {
      throw new TypeError('upstream.changeLimit must be a safe integer in 1..10000')
    }
    const interval = config.upstream.intervalMs ?? 15_000
    if (!Number.isSafeInteger(interval) || interval < 1_000) {
      throw new TypeError('upstream.intervalMs must be a safe integer >= 1000')
    }
  }
  return config
}
