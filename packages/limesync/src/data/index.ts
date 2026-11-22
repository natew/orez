export * from './createPermissions.js'
export * from './queryRegistry.js'
export * from './helpers/batchQuery.js'
export * from './helpers/createMutators.js'
export * from './helpers/ensureLoggedIn.js'
export * from './helpers/mutatorContext.js'
export * from './helpers/useMutation.js'
export { ensureAuth, getAuth } from './helpers/getAuth.js'
export { setAuthData, setEnvironment } from './state.js'

export {
  createZeroClient,
  type CreateZeroClientOptions,
  type GroupedQueries,
  type PermissionStrategy,
  type WaitForZeroOptions,
  type ZeroProviderGeneration,
  type ZeroProviderTransport,
} from './createZeroClient.js'
export * from './createUseQuery.js'
export * from './resolveQuery.js'
export * from './run.js'
export { setRunner, type ZeroRunner } from './zeroRunner.js'
export * from './mutations.js'
export * from './where.js'
export * from './serverWhere.js'
export * from './zql.js'
export { defineConfig, type DataConfig, type DataInstanceConfig } from './config.js'
export { defineQuery, defineQueries } from '../zero/zero.js'

export type * from './types.js'

export {
  clearZeroClientData,
  type ClearZeroClientDataOptions,
} from './helpers/clearZeroClientData.js'
export {
  showZeroClientErrorOnce,
  resetShownZeroClientError,
  type ShowZeroClientErrorOptions,
  type ZeroClientErrorInfo,
} from './helpers/showZeroClientError.js'
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
} from './helpers/recoverZeroClient.js'

export {
  isStaleGenerationError,
  mutationErrorMessage,
  MutationResultError,
  MutationTimeoutError,
  StaleGenerationError,
  type BackgroundMutationDrainResult,
  type BackgroundMutationOptions,
  type CombinedMutationLifecycle,
  type MutationLifecycle,
  type MutationLike,
  type MutationPhase,
} from './helpers/mutationLifecycle.js'
