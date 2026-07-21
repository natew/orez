export * from './createPermissions'
export * from './queryRegistry'
export * from './helpers/batchQuery'
export * from './helpers/createMutators'
export * from './helpers/ensureLoggedIn'
export * from './helpers/mutatorContext'
export * from './helpers/useMutation'
export { ensureAuth, getAuth } from './helpers/getAuth'
export { setAuthData, setEnvironment } from './state'

export {
  createZeroClient,
  type CreateZeroClientOptions,
  type GroupedQueries,
  type PermissionStrategy,
  type WaitForZeroOptions,
  type ZeroProviderTransport,
} from './createZeroClient'
export * from './createUseQuery'
export * from './resolveQuery'
export * from './run'
export { setRunner, type ZeroRunner } from './zeroRunner'
export * from './mutations'
export * from './where'
export * from './serverWhere'
export * from './zql'
export { defineConfig, type DataConfig, type DataInstanceConfig } from './config'
export { defineQuery, defineQueries } from '@rocicorp/zero'

export type DataInstanceOptions<Scope extends string = string> = {
  /**
   * Scoping column. Required in a nested instance directory; omit it in the
   * root `data/instance.ts`, which only configures the default instance.
   */
  scope?: Scope
  /**
   * Tables this instance writes that `on-zero generate` cannot reach by static
   * analysis — writes arriving through server actions or seeding helpers, and
   * writes to a table owned by another instance. Declared names are unioned
   * into the generated `supportTables`, which keeps the sync change log
   * mappable without syncing those rows to clients.
   */
  supportTables?: readonly string[]
}

export function defineInstance<const Scope extends string>(
  options: DataInstanceOptions<Scope>
): DataInstanceOptions<Scope> {
  return options
}

// drizzle-zero re-exports moved to 'on-zero/drizzle' to avoid pulling
// drizzle-zero + drizzle-orm/_relations into the main bundle
// (breaks vite dep optimization under vxrn-web conditions)

export type * from './types'

export {
  clearZeroClientData,
  type ClearZeroClientDataOptions,
} from './helpers/clearZeroClientData'
export {
  showZeroClientErrorOnce,
  resetShownZeroClientError,
  type ShowZeroClientErrorOptions,
  type ZeroClientErrorInfo,
} from './helpers/showZeroClientError'
export {
  classifyZeroRecoveryLog,
  makeZeroRecovery,
  composeRecoveryLogSink,
  isRecoverableZeroStalePokeMessage,
  type ZeroRecoveryLogClassification,
  type ZeroRecoveryDeps,
  type ScheduleReloadContext,
  type RecoveryGuardStorage,
  type ZeroLogPattern,
} from './helpers/recoverZeroClient'

export {
  isStaleGenerationError,
  mutationErrorMessage,
  MutationResultError,
  MutationTimeoutError,
  StaleGenerationError,
  type BackgroundMutationOptions,
  type MutationLike,
  type MutationPhase,
} from './helpers/mutationLifecycle'
