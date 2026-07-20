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

export type SqlStatementMetadata = {
  readonly table: string
  readonly publicTable: string
  readonly kind: 'delete' | 'insert' | 'update' | 'upsert'
}

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
  readonly dialect: 'sqlite' | 'postgresql'
  readonly internalSchema?: string

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
                readonly details: JsonValue
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

export type VisibilityValue = JsonPrimitive

export type VisibilityOperand =
  | {
      readonly type: 'column'
      readonly table: string
      readonly column: string
      readonly qualifier?: string
    }
  | { readonly type: 'value'; readonly value: VisibilityValue }

export type VisibilityExpression =
  | {
      readonly type: 'comparison'
      readonly operator: '=' | '!=' | '<' | '>' | '<=' | '>=' | 'IS' | 'IS NOT'
      readonly left: VisibilityOperand
      readonly right: VisibilityOperand
    }
  | {
      readonly type: 'and' | 'or'
      readonly conditions: readonly VisibilityExpression[]
    }
  | {
      readonly type: 'exists'
      readonly table: string
      readonly qualifier?: string
      readonly where: VisibilityExpression
    }

export type VisibilityFilter =
  | {
      readonly kind: 'expression'
      readonly expression: VisibilityExpression
      readonly sql?: never
      readonly params?: never
    }
  | {
      readonly kind: 'raw'
      readonly sql: string
      readonly params?: readonly VisibilityValue[]
      readonly expression?: never
    }

export type VisibilityConfig = {
  readonly rowLocal: boolean | ((claims: NormalizedClaims) => boolean)
  filter(table: string, claims: NormalizedClaims): VisibilityFilter | undefined
}

export type QueryResolver = (
  name: string,
  args: readonly JsonValue[],
  claims: NormalizedClaims
) => JsonValue | Promise<JsonValue>

export type TransactionQuery = Query<string, Schema, unknown>
export type TransactionQueryResult = HumanReadable<unknown>
