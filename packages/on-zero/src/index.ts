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
