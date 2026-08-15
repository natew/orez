import type {
  HumanReadable,
  Query,
  Schema,
  ServerTransaction as ZeroServerTransaction,
} from '@rocicorp/zero'

export type JsonPrimitive = string | number | boolean | null
export type JsonValue =
  | JsonPrimitive
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue }

export type NormalizedClaims = {
  readonly userID: string
  readonly [claim: string]: JsonValue
}

export type AuthData = {
  readonly id: string
}

export type ZeroSchemaConfig = {
  readonly schemaID?: string
  readonly tables: Readonly<
    Record<
      string,
      {
        readonly name?: string
        readonly serverName?: string
        readonly columns: Readonly<
          Record<
            string,
            {
              readonly type: string
              readonly serverName?: string
              readonly optional?: boolean
              readonly encrypted?: true
            }
          >
        >
        readonly primaryKey: readonly string[]
      }
    >
  >
}

export type TransactionQueryFormat = {
  readonly relationships: Readonly<Record<string, TransactionQueryFormat>>
  readonly singular: boolean
}

type SqlStatementMetadataBase = {
  readonly table: string
  readonly publicTable: string
  readonly kind: 'delete' | 'insert' | 'update' | 'upsert'
}

export type SqlStatementMetadata = SqlStatementMetadataBase &
  (
    | {
        /** omitted metadata keeps arbitrary SQL on the transparent trigger lane. */
        readonly capture?: 'triggers'
        readonly primaryKeys?: never
      }
    | {
        /** generated `tx.mutate` helpers select the cheaper lane with exact keys. */
        readonly capture: 'exact'
        readonly primaryKeys: readonly {
          readonly before?: Readonly<Record<string, JsonPrimitive>>
          readonly after?: Readonly<Record<string, JsonPrimitive>>
        }[]
      }
  )

export type ExecResult = { readonly changes: number }

export interface ApplicationTransaction {
  exec(
    sql: string,
    params?: readonly unknown[],
    metadata?: SqlStatementMetadata
  ): Promise<ExecResult>

  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<readonly Row[]>

  queryAst<Result = unknown>(
    ast: JsonValue,
    format: TransactionQueryFormat,
    queryName?: string
  ): Promise<Result>
}

export interface ApplicationDatabase {
  transaction<Value>(
    work: (tx: ApplicationTransaction) => Value | Promise<Value>
  ): Promise<Value>

  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[]
  ): Promise<readonly Row[]>
}

export type DeferredEffect = () => void | Promise<void>

export type DeferredEffectOptions = {
  readonly barrier?: boolean
}

export type EffectScheduler = {
  runBackground(promise: Promise<void>): void | Promise<void>
  report(error: unknown): void
}

export type MutationContext = {
  readonly claims: NormalizedClaims
  defer(effect: DeferredEffect, options?: DeferredEffectOptions): void
} & (
  | {
      readonly source: 'zero-push'
      readonly clientGroupID: string
      readonly clientID: string
      readonly mutationID: number
    }
  | {
      readonly source: 'direct'
    }
)

export type ServerTransaction<S extends Schema> = ZeroServerTransaction<
  S,
  ApplicationTransaction
>

export type RegisteredMutator<
  S extends Schema = Schema,
  Args extends JsonValue = JsonValue,
> = (input: {
  readonly tx: ServerTransaction<S>
  readonly args: Args
  readonly ctx: MutationContext
}) => void | Promise<void>

export type MutatorRegistry<S extends Schema = Schema> = Readonly<
  Record<string, RegisteredMutator<S>>
>

export type CreateSyncExecutorOptions<S extends Schema> = {
  readonly database: ApplicationDatabase
  readonly schema: S
  readonly mutators: MutatorRegistry<S>
  readonly effects: EffectScheduler
}

export type PushResult = {
  readonly pushResponse:
    | {
        readonly mutations: readonly {
          readonly id: { readonly clientID: string; readonly id: number }
          readonly result:
            | Record<string, never>
            | { readonly error: 'alreadyProcessed'; readonly details: string }
            | {
                readonly error: 'app'
                readonly message: string
                readonly details?: JsonValue
              }
        }[]
      }
    | {
        readonly error: 'unsupportedPushVersion'
        readonly mutationIDs: readonly {
          readonly clientID: string
          readonly id: number
        }[]
      }
}

export interface SyncExecutor<S extends Schema> {
  readonly schema: S

  push(body: unknown, claims: NormalizedClaims): Promise<PushResult>

  execute(
    name: keyof MutatorRegistry<S> & string,
    args: JsonValue,
    claims: NormalizedClaims
  ): Promise<void>

  transaction<Value>(
    claims: NormalizedClaims,
    work: (tx: ServerTransaction<S>) => Value | Promise<Value>
  ): Promise<Value>

  query<Result>(
    claims: NormalizedClaims,
    work: (tx: ServerTransaction<S>) => Result | Promise<Result>
  ): Promise<Result>
}

export type TransactionQuery = Query<string, Schema, unknown>
export type TransactionQueryResult = HumanReadable<unknown>
