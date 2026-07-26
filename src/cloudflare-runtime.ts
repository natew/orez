/**
 * Runtime-only Orez Lite APIs for modules evaluated by workerd.
 *
 * Deploy tooling lives at `orez/cloudflare` so Node-based build scripts never
 * evaluate `cloudflare:workers`.
 */
export { createApplicationSqlClient, ZeroDO } from './cf-do/worker.js'
export type {
  ApplicationSqlClient,
  ApplicationSqlClientOptions,
  ApplicationSqlDurableObjectNamespace,
  ApplicationSqlExecResult,
  ApplicationSqlQueryCompiler,
  ApplicationSqlRpc,
  ApplicationSqlSessionRpc,
  ApplicationSqlTable,
  ApplicationSqlTransaction,
  ApplicationSqlTransactionWork,
} from './cf-do/application-sql.js'
export {
  doInstanceName,
  doInstanceNameForRequest,
  isValidNamespace,
} from './worker/cf-do-shim.js'
export type { NamespaceRoutingOptions } from './worker/cf-do-shim.js'
export { installZeroSqlWriteCircuitBreaker } from './worker/zero-sql-write-circuit.js'
export type {
  DurableSqlCursor,
  DurableSqlStorage,
  WriteCircuitOptions,
} from './worker/zero-sql-write-circuit.js'
export { createNamespaceBackupManager } from './cf-do/namespace-backup.js'
export type {
  NamespaceBackupBucket,
  NamespaceBackupManager,
  NamespaceBackupObject,
  NamespaceBackupOptions,
  NamespaceBackupStatement,
  NamespaceBackupSummary,
  NamespaceRestoreSummary,
} from './cf-do/namespace-backup.js'
