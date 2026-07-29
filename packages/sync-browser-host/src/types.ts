import type { AnyQueryRegistry, Schema } from '@rocicorp/zero'
import type { TransactionQueryBudget } from 'orez-sync-cf-host/transaction-query'
import type {
  AuthData,
  ExecResult,
  MutatorRegistry,
  SqlStatementMetadata,
  SyncExecutor,
} from 'orez-sync-executor'

export interface SyncSql {
  exec(
    sql: string,
    params?: readonly unknown[],
    metadata?: SqlStatementMetadata
  ): ExecResult
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Row[]
}

export type BrowserSyncHostAssets = {
  sqliteWasmUrl?: string | URL
  syncWasmUrl?: string | URL
}

export type BrowserSyncHostConfig<
  S extends Schema = Schema,
  A extends AuthData = AuthData,
> = {
  storageKey: string
  assets?: BrowserSyncHostAssets
  schema: S
  initialize(sql: SyncSql): void
  authenticate(request: Request): A | null | Promise<A | null>
  authorize(
    request: Request,
    authData: A | null,
    namespace: string
  ): boolean | Promise<boolean>
  mutators: MutatorRegistry<S>
  /**
   * The app's ordinary Zero query registry (the `defineQueries` result the
   * client is built with). The host resolves every desired named query
   * in-process against it, with the authenticated authData as query context.
   */
  queries: AnyQueryRegistry
  queryTransformVersion?: number | ((authData: A | null) => number)
  retainChanges?: number
  transactionQueryBudget?: Partial<TransactionQueryBudget>
  onDataChanged?: () => void
}

export interface BrowserSyncHost<S extends Schema = Schema> {
  readonly executor: SyncExecutor<S>
  handlePull(request: Request): Promise<Response>
  handlePush(request: Request): Promise<Response>
  fetch(request: Request): Promise<Response>
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<Row[]>
  exec(
    sql: string,
    params?: readonly unknown[],
    metadata?: SqlStatementMetadata
  ): Promise<ExecResult>
  subscribe(listener: () => void): () => void
  close(): Promise<void>
}

export interface BrowserSyncHostPortClient {
  fetch(input: string | URL | Request, init?: RequestInit): Promise<Response>
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<Row[]>
  exec(
    sql: string,
    params?: readonly unknown[],
    metadata?: SqlStatementMetadata
  ): Promise<ExecResult>
  subscribe(listener: () => void): () => void
  close(): void
}
