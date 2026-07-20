import type { Schema } from '@rocicorp/zero'
import type { TransactionQueryBudget } from 'orez-sync-cf-host/transaction-query'
import type {
  ExecResult,
  MutatorRegistry,
  NormalizedClaims,
  QueryResolver,
  SqlStatementMetadata,
  SyncExecutor,
  VisibilityConfig,
} from 'orez-sync-executor'

export {
  visibility,
  type VisibilityExpression,
  type VisibilityFilter,
  type VisibilityOperand,
  type VisibilityValue,
} from 'orez-sync-cf-host/visibility'
export type { VisibilityConfig } from 'orez-sync-executor'

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

export type PullCaps = {
  maxChangeRows: number
  maxChangeBytes: number
}

export type BrowserSyncHostAssets = {
  sqliteWasmUrl?: string | URL
  syncWasmUrl?: string | URL
}

export type BrowserSyncHostConfig<S extends Schema = Schema> = {
  storageKey: string
  assets?: BrowserSyncHostAssets
  schema: S
  initialize(sql: SyncSql): void
  authenticate(
    request: Request
  ): NormalizedClaims | null | Promise<NormalizedClaims | null>
  authorize(
    request: Request,
    claims: NormalizedClaims,
    namespace: string
  ): boolean | Promise<boolean>
  mutators: MutatorRegistry<S>
  visibility?: VisibilityConfig
  queryAware?: boolean | ((claims: NormalizedClaims) => boolean)
  resolveQuery?: QueryResolver
  queryTransformVersion?: number | ((claims: NormalizedClaims) => number)
  retainChanges?: number
  caps?: Partial<PullCaps>
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
