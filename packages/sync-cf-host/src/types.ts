export type JsonPrimitive = string | number | boolean | null
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue }

export type NormalizedClaims = {
  /** Stable consumer user id used for client-group ownership. */
  userID: string
  [claim: string]: JsonValue
}

export type ZeroSchemaConfig = {
  readonly tables: Readonly<
    Record<
      string,
      {
        readonly columns: Readonly<Record<string, { readonly type: string }>>
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
  /** Execute a validated Zero AST inside the current application transaction. */
  queryAst<Row extends Record<string, unknown> = Record<string, unknown>>(
    ast: JsonValue
  ): Promise<Row[]>
}

export type DeferredEffect = () => void | Promise<void>

export type MutatorContext = {
  claims: NormalizedClaims
  /** Current Zero mutation identity, when invoked by the production host. */
  clientID?: string
  mutationID?: string | number
  defer(effect: DeferredEffect): void
}

export type RegisteredMutator = (
  tx: MutatorSql,
  args: JsonValue,
  context: MutatorContext
) => void | Promise<void>

export type MutatorRegistry = Readonly<Record<string, RegisteredMutator>>

/** Preserve mutator names while making the host-facing registry immutable. */
export function registerMutators<
  const Registry extends Record<string, RegisteredMutator>,
>(registry: Registry): Readonly<Registry> {
  return Object.freeze({ ...registry })
}

export class MutationApplicationError extends Error {
  constructor(
    readonly details: string,
    message = details
  ) {
    super(message)
    this.name = 'MutationApplicationError'
  }
}

export type VisibilityFilter = {
  /** SQL WHERE fragment only, without the WHERE keyword. */
  sql: string
  params?: readonly JsonPrimitive[]
}

export type VisibilityConfig = {
  /** True only when every predicate depends on the selected row alone. */
  rowLocal: boolean | ((claims: NormalizedClaims) => boolean)
  filter(table: string, claims: NormalizedClaims): VisibilityFilter | undefined
}

export type QueryResolver = (
  name: string,
  args: readonly JsonValue[],
  claims: NormalizedClaims,
  env: SyncHostEnv
) => JsonValue | Promise<JsonValue>

export type PullCaps = {
  maxChangeRows: number
  maxChangeBytes: number
}

export interface SyncHostEnv {
  SYNC_DO: DurableObjectNamespace
  ADMIN_KEY?: string
}

export type ServiceBinding = {
  fetch(input: string | Request, init?: RequestInit): Promise<Response>
}

export type UpstreamConfig = {
  /** Env key for the service binding that owns the app write endpoint and feed. */
  binding: string
  /** Path to this namespace on the bound service (for example `/data/<id>`). */
  namespacePath: string | ((namespace: string) => string)
  /** Feed page size; the cursor loop continues until the reported head is reached. */
  changeLimit?: number
  /** Active wake-socket alarm safety net. Defaults to 15 seconds. */
  intervalMs?: number
  /** Billable SQLite rows written by ingest per rolling window. Defaults to 150,000. */
  ingestBudgetRows?: number
  /** Rolling ingest budget window. Defaults to five minutes. */
  ingestBudgetWindowMs?: number
  /** Initial breaker cooldown. Defaults to one second. */
  ingestBackoffMs?: number
  /** Maximum breaker cooldown. Defaults to one minute. */
  ingestMaxBackoffMs?: number
}

export type DelegatedPushRetryConfig = {
  /** Total attempts including the first request. Defaults to 3. */
  maxAttempts?: number
  /** Initial exponential delay. Defaults to 100ms. */
  initialBackoffMs?: number
  /** Delay cap. Defaults to 1,000ms. */
  maxBackoffMs?: number
  /** Per-attempt service-binding timeout. Defaults to 5,000ms. */
  timeoutMs?: number
}

export type SyncHostConfig<Env extends SyncHostEnv = SyncHostEnv> = {
  hostVersion: string
  schema: ZeroSchemaConfig
  mutators?: MutatorRegistry
  /** Relative app push endpoint on the upstream service binding. */
  mutateUrl?: string
  /** Env binding for delegated pushes; defaults to upstream.binding. */
  mutateBinding?: string
  delegatedPushRetry?: DelegatedPushRetryConfig
  /** Required for delegated push; forbidden with local mutators (no dual apply). */
  upstream?: UpstreamConfig
  /** Application DDL and optional seed, called before sync-core schema init. */
  initialize(sql: SyncSql): void
  authenticate(
    request: Request,
    env: Env
  ): NormalizedClaims | null | Promise<NormalizedClaims | null>
  /** Resolve the first path component or another consumer-defined namespace. */
  namespace(request: Request): string | null
  visibility?: VisibilityConfig
  /** Enable desired-query pulls for this namespace and resolve named queries
   * into validated Zero ASTs before they reach sync-core. */
  queryAware?: boolean | ((claims: NormalizedClaims) => boolean)
  resolveQuery?: QueryResolver
  /** Server-owned invalidation epoch for permission/schema transforms. */
  queryTransformVersion?: number | ((claims: NormalizedClaims) => number)
  /** Enable consumer visibility from the first request. Defaults to false for harnesses. */
  visibilityEnabled?: boolean
  retainChanges?: number
  caps?: Partial<PullCaps>
  idleTeardownMs?: number
  wakeCoalesceMs?: number
  authorizeAdmin?: (request: Request, env: Env) => boolean | Promise<boolean>
}
