import type { TransactionQueryBudget } from './transaction-query.js'
import type { AnyQueryRegistry, Schema } from '@rocicorp/zero'
import type {
  ExecResult,
  MutatorRegistry,
  NormalizedClaims,
  SqlStatementMetadata,
} from 'orez-sync-executor'
import type { StreamingManifest } from 'orez-sync-executor/realtime'

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
  /**
   * Per-attempt service-binding timeout. Defaults to 30,000ms. Delegate wall
   * time under write-lane contention is queueing rather than compute, and the
   * abort cancels the app invocation mid-transaction, so a short timeout
   * cancels real pushes on any nontrivial seed.
   */
  timeoutMs?: number
}

export type SyncHostConfig<
  Env extends SyncHostEnv = SyncHostEnv,
  S extends Schema = Schema,
> = {
  hostVersion: string
  schema: S
  mutators?: MutatorRegistry<S>
  /**
   * absolute app push path on the delegated mutation service. a successful
   * response must be causally visible through the configured upstream data
   * feed before the app returns, because the host ingests effects before it
   * records the mutation's lmid.
   */
  mutateUrl?: string
  /** Absolute origin used for delegated push requests through the service binding. */
  mutateOrigin?: string
  /**
   * env binding for delegated pushes; defaults to upstream.binding. the bound
   * service and upstream feed must satisfy the mutateUrl causality contract.
   */
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
  /** Authorize authenticated application access before selecting a namespace DO. */
  authorize(
    request: Request,
    claims: NormalizedClaims,
    namespace: string,
    env: Env
  ): boolean | Promise<boolean>
  /** Authorize the advisory wake socket before selecting a namespace DO.
   * Browser clients should present a short-lived, namespace-scoped capability
   * in the query string because WebSocket cannot set request headers.
   *
   * Return `{ userID }` instead of `true` to also identify the socket, which is
   * what a namespace serving `streamingManifest` must do: field subscriptions
   * ride this socket and are authorized against that userID. Returning bare
   * `true` there is refused rather than quietly downgraded to a wake-only
   * socket, because the failure would otherwise look like streaming that just
   * never arrives. The capability is the only credential available here, so the
   * userID belongs inside it. */
  authorizeWake(
    request: Request,
    env: Env
  ): boolean | { userID: string } | Promise<boolean | { userID: string }>
  /** Authorize upstream service notifications before selecting a namespace DO. */
  authorizeNotify(request: Request, env: Env): boolean | Promise<boolean>
  /** Resolve the first path component or another consumer-defined namespace. */
  namespace(request: Request): string | null
  /**
   * The app's ordinary Zero query registry (the `defineQueries` result the
   * client is built with). The host resolves every desired named query
   * in-process against it — argument validation and context scoping run in
   * the query definitions themselves, with the authenticated claims as the
   * query context. There is no app endpoint to call and nothing else to wire.
   */
  queries: AnyQueryRegistry
  /**
   * Streaming fields for this namespace: which columns may carry a live,
   * uncommitted value, and their publish mode and rate bounds.
   *
   * Supplied as a live object rather than data because a field's `validate` is
   * a function; there is nothing to serialize into a deploy bundle, and the
   * application's own module is where it belongs. Absent means the namespace
   * serves no field subscriptions and rejects producer frames.
   *
   * See docs/streaming-fields.md.
   */
  streamingManifest?: StreamingManifest
  /**
   * Authorize a producer socket, which may publish a value into any streaming
   * field for any row. That is a much stronger capability than the wake socket
   * or an ordinary client's, so it has no default: leaving this unset means the
   * namespace accepts no producers, and every `/realtime/produce` upgrade is
   * refused. Producers are server-side callers (an AI generation worker, a job
   * runner), so a service binding or a shared secret is the usual check.
   */
  authorizeProduce?: (request: Request, env: Env) => boolean | Promise<boolean>
  /** Server-owned invalidation epoch for permission/schema transforms. */
  queryTransformVersion?: number | ((claims: NormalizedClaims) => number)
  retainChanges?: number
  idleTeardownMs?: number
  wakeCoalesceMs?: number
  /** per-query guard for recursive transaction query materialization. */
  transactionQueryBudget?: Partial<TransactionQueryBudget>
  authorizeAdmin?: (request: Request, env: Env) => boolean | Promise<boolean>
}
