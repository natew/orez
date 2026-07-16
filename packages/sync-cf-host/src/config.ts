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
  if (typeof config.authorizeWake !== 'function') {
    throw new TypeError('sync host config authorizeWake is required')
  }
  if (typeof config.authorizeNotify !== 'function') {
    throw new TypeError('sync host config authorizeNotify is required')
  }
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
  if (config.mutateOrigin !== undefined && !hasDelegate) {
    throw new TypeError('sync host config mutateOrigin requires mutateUrl')
  }
  if (config.delegatedPushRetry !== undefined && !hasDelegate) {
    throw new TypeError('sync host config delegatedPushRetry requires mutateUrl')
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
  if (config.mutateOrigin !== undefined) {
    let origin: URL
    try {
      origin = new URL(config.mutateOrigin)
    } catch {
      throw new TypeError('mutateOrigin must be an absolute http(s) origin')
    }
    if (
      (origin.protocol !== 'http:' && origin.protocol !== 'https:') ||
      origin.origin !== config.mutateOrigin
    ) {
      throw new TypeError('mutateOrigin must be an absolute http(s) origin')
    }
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
    for (const [name, value] of Object.entries({
      ingestBudgetRows: config.upstream.ingestBudgetRows ?? 150_000,
      ingestBudgetWindowMs: config.upstream.ingestBudgetWindowMs ?? 300_000,
      ingestBackoffMs: config.upstream.ingestBackoffMs ?? 1_000,
      ingestMaxBackoffMs: config.upstream.ingestMaxBackoffMs ?? 60_000,
    })) {
      if (!Number.isSafeInteger(value) || value < 1)
        throw new TypeError(`upstream.${name} must be a positive safe integer`)
    }
  }
  if (config.delegatedPushRetry) {
    for (const [name, value] of Object.entries({
      maxAttempts: config.delegatedPushRetry.maxAttempts ?? 3,
      initialBackoffMs: config.delegatedPushRetry.initialBackoffMs ?? 100,
      maxBackoffMs: config.delegatedPushRetry.maxBackoffMs ?? 1_000,
      timeoutMs: config.delegatedPushRetry.timeoutMs ?? 5_000,
    })) {
      if (!Number.isSafeInteger(value) || value < 1)
        throw new TypeError(`delegatedPushRetry.${name} must be a positive safe integer`)
    }
  }
  if (config.transactionQueryBudget) {
    for (const [name, value] of Object.entries(config.transactionQueryBudget)) {
      if (!Number.isSafeInteger(value) || Number(value) < 1) {
        throw new TypeError(
          `transactionQueryBudget.${name} must be a positive safe integer`
        )
      }
    }
  }
  return config
}
