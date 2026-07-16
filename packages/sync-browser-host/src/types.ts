import type {
  TransactionQueryBudget,
  TransactionQueryFormat,
} from 'orez-sync-cf-host/transaction-query'

export type JsonPrimitive = string | number | boolean | null
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue }

export type NormalizedClaims = {
  userID: string
  [claim: string]: JsonValue
}

export type ZeroSchemaConfig = {
  readonly tables: Readonly<
    Record<
      string,
      {
        readonly name?: string
        readonly serverName?: string
        readonly columns: Readonly<
          Record<string, { readonly type: string; readonly serverName?: string }>
        >
        readonly primaryKey: readonly string[]
      }
    >
  >
}

export interface SyncSql {
  exec(sql: string, params?: readonly unknown[]): void
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Row[]
}

export interface MutatorSql {
  exec(sql: string, params?: readonly unknown[]): Promise<void>
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<Row[]>
  queryAst<Result = unknown>(
    ast: JsonValue,
    format: TransactionQueryFormat,
    queryName?: string
  ): Promise<Result>
}

export type DeferredEffect = () => void | Promise<void>

export type MutatorContext = {
  claims: NormalizedClaims
  clientID: string
  mutationID: string
  defer(effect: DeferredEffect): void
}

export type RegisteredMutator = (
  tx: MutatorSql,
  args: JsonValue,
  context: MutatorContext
) => void | Promise<void>

export type MutatorRegistry = Readonly<Record<string, RegisteredMutator>>

export function registerMutators<
  const Registry extends Record<string, RegisteredMutator>,
>(registry: Registry): Readonly<Registry> {
  return Object.freeze({ ...registry })
}

export {
  MutationApplicationError,
  isMutationApplicationError,
} from 'orez-sync-cf-host/mutation-error'

export type VisibilityFilter = {
  sql: string
  params?: readonly JsonPrimitive[]
}

export type VisibilityConfig = {
  rowLocal: boolean | ((claims: NormalizedClaims) => boolean)
  filter(table: string, claims: NormalizedClaims): VisibilityFilter | undefined
}

export type QueryResolver = (
  name: string,
  args: readonly JsonValue[],
  claims: NormalizedClaims
) => JsonValue | Promise<JsonValue>

export type PullCaps = {
  maxChangeRows: number
  maxChangeBytes: number
}

export type BrowserSyncHostAssets = {
  sqliteWasmUrl?: string | URL
  syncWasmUrl?: string | URL
}

export type BrowserSyncHostConfig = {
  storageKey: string
  assets?: BrowserSyncHostAssets
  schema: ZeroSchemaConfig
  initialize(sql: SyncSql): void
  authenticate(
    request: Request
  ): NormalizedClaims | null | Promise<NormalizedClaims | null>
  mutators: MutatorRegistry
  visibility?: VisibilityConfig
  queryAware?: boolean | ((claims: NormalizedClaims) => boolean)
  resolveQuery?: QueryResolver
  queryTransformVersion?: number | ((claims: NormalizedClaims) => number)
  retainChanges?: number
  caps?: Partial<PullCaps>
  transactionQueryBudget?: Partial<TransactionQueryBudget>
  onDataChanged?: () => void
}

export interface BrowserSyncHost {
  handlePull(request: Request): Promise<Response>
  handlePush(request: Request): Promise<Response>
  fetch(request: Request): Promise<Response>
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<Row[]>
  exec(sql: string, params?: readonly unknown[]): Promise<void>
  subscribe(listener: () => void): () => void
  close(): Promise<void>
}

export interface BrowserSyncHostPortClient {
  fetch(input: string | URL | Request, init?: RequestInit): Promise<Response>
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<Row[]>
  exec(sql: string, params?: readonly unknown[]): Promise<void>
  subscribe(listener: () => void): () => void
  close(): void
}
