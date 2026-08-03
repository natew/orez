import { MutationWriteSetError } from './errors.js'

import type {
  ApplicationTransaction,
  JsonPrimitive,
  SqlStatementMetadata,
  ZeroSchemaConfig,
} from './types.js'

type CapturedPrimaryKey = {
  readonly table: string
  readonly key: Readonly<Record<string, JsonPrimitive>>
}

function isJsonPrimitive(value: unknown): value is JsonPrimitive {
  return (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'boolean' ||
    (typeof value === 'number' && Number.isFinite(value))
  )
}

function canonicalPrimaryKey(
  schema: ZeroSchemaConfig,
  tableName: string,
  value: unknown,
  source: 'executor metadata' | 'trigger'
): CapturedPrimaryKey {
  if (!Object.hasOwn(schema.tables, tableName)) {
    throw new MutationWriteSetError(`${source} names unknown table ${tableName}`)
  }
  const table = schema.tables[tableName]!
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new MutationWriteSetError(
      `${source} primary key for ${tableName} is not an object`
    )
  }
  const input = value as Record<string, unknown>
  const inputColumns = Object.keys(input).sort()
  const primaryKeyColumns = [...table.primaryKey].sort()
  if (
    inputColumns.length !== primaryKeyColumns.length ||
    inputColumns.some((column, index) => column !== primaryKeyColumns[index])
  ) {
    throw new MutationWriteSetError(
      `${source} primary key for ${tableName} must contain exactly ${table.primaryKey.join(', ')}`
    )
  }

  const key: Record<string, JsonPrimitive> = {}
  for (const column of table.primaryKey) {
    let field = input[column]
    if (source === 'trigger' && table.columns[column]?.type === 'boolean') {
      if (field === 0) field = false
      if (field === 1) field = true
    }
    if (!isJsonPrimitive(field)) {
      throw new MutationWriteSetError(
        `${source} primary key ${tableName}.${column} is not a JSON primitive`
      )
    }
    key[column] = field
  }
  return { table: tableName, key }
}

function encodedPrimaryKey(primaryKey: CapturedPrimaryKey): string {
  return JSON.stringify([primaryKey.table, primaryKey.key])
}

function validateMetadata(
  schema: ZeroSchemaConfig,
  metadata: Extract<SqlStatementMetadata, { readonly capture: 'exact' }>
): CapturedPrimaryKey[] {
  if (!Object.hasOwn(schema.tables, metadata.publicTable)) {
    throw new MutationWriteSetError(
      `executor metadata names unknown table ${metadata.publicTable}`
    )
  }
  const table = schema.tables[metadata.publicTable]!
  const physicalTable = table.serverName ?? table.name ?? metadata.publicTable
  if (metadata.table !== physicalTable) {
    throw new MutationWriteSetError(
      `executor metadata table ${metadata.table} does not match ${physicalTable}`
    )
  }

  const captured: CapturedPrimaryKey[] = []
  for (const [index, row] of metadata.primaryKeys.entries()) {
    if (!row.before && !row.after) {
      throw new MutationWriteSetError(
        `executor metadata primaryKeys[${index}] has neither before nor after`
      )
    }
    if (row.before) {
      captured.push(
        canonicalPrimaryKey(schema, metadata.publicTable, row.before, 'executor metadata')
      )
    }
    if (row.after) {
      captured.push(
        canonicalPrimaryKey(schema, metadata.publicTable, row.after, 'executor metadata')
      )
    }
  }
  return captured
}

export function beginWriteSetCapture(
  schema: ZeroSchemaConfig,
  applicationTx: ApplicationTransaction,
  dialect: 'sqlite' | 'postgresql'
): {
  readonly transaction: ApplicationTransaction
  validate(): Promise<void>
} {
  if (dialect !== 'sqlite') {
    return {
      transaction: applicationTx,
      validate: async () => {},
    }
  }

  let active = true
  let operation = Promise.resolve<unknown>(undefined)
  let fatalFailure: unknown

  const assertActive = () => {
    if (!active) {
      throw new MutationWriteSetError('application transaction is no longer active')
    }
  }
  const serialize = <Value>(work: () => Promise<Value>): Promise<Value> => {
    assertActive()
    const result = operation.then(work)
    operation = result.then(
      () => undefined,
      () => undefined
    )
    return result
  }

  const transaction: ApplicationTransaction = {
    exec(sql, params, metadata) {
      return serialize(async () => {
        // arbitrary sql keeps the product's trigger-backed "just use the db"
        // contract. generated helpers opt each modeled statement into shadowing.
        if (metadata?.capture !== 'exact') {
          return applicationTx.exec(sql, params, metadata)
        }
        try {
          const statementKeys = validateMetadata(schema, metadata)
          const checkpoint =
            (
              await applicationTx.query<{ watermark: number | bigint | string }>(
                'SELECT COALESCE(MAX("watermark"), 0) AS "watermark" FROM "_zsync_changes"'
              )
            )[0]?.watermark ?? 0
          const result = await applicationTx.exec(sql, params, metadata)
          const rows = await applicationTx.query<{
            tableName: string
            pk: string | null
          }>(
            `SELECT "tableName" AS "tableName", "pk" AS "pk"
             FROM "_zsync_changes"
             WHERE "watermark" > ? AND "op" = 'row'`,
            [checkpoint]
          )
          const triggered = new Map<string, CapturedPrimaryKey>()
          for (const row of rows) {
            if (typeof row.tableName !== 'string' || typeof row.pk !== 'string') {
              throw new MutationWriteSetError(
                'trigger emitted an invalid touched-key row'
              )
            }
            let parsed: unknown
            try {
              parsed = JSON.parse(row.pk)
            } catch {
              throw new MutationWriteSetError(
                `trigger emitted invalid primary-key JSON for ${row.tableName}`
              )
            }
            const primaryKey = canonicalPrimaryKey(
              schema,
              row.tableName,
              parsed,
              'trigger'
            )
            triggered.set(encodedPrimaryKey(primaryKey), primaryKey)
          }

          const executorKeys = [
            ...new Set((result.changes > 0 ? statementKeys : []).map(encodedPrimaryKey)),
          ].sort()
          const triggerKeys = [...triggered.keys()].sort()
          if (
            executorKeys.length !== triggerKeys.length ||
            executorKeys.some((key, index) => key !== triggerKeys[index])
          ) {
            throw new MutationWriteSetError(
              `executor write set ${JSON.stringify(executorKeys)} does not match trigger write set ${JSON.stringify(triggerKeys)}`
            )
          }
          return result
        } catch (error) {
          if (
            error &&
            typeof error === 'object' &&
            'name' in error &&
            error.name === 'MutationWriteSetError'
          ) {
            fatalFailure ??= error
          }
          throw error
        }
      })
    },
    query(sql, params) {
      return serialize(() => applicationTx.query(sql, params))
    },
    queryAst(ast, format, queryName) {
      return serialize(() => applicationTx.queryAst(ast, format, queryName))
    },
  }

  return {
    transaction,
    async validate(): Promise<void> {
      assertActive()
      try {
        await operation
        if (fatalFailure !== undefined) throw fatalFailure
      } finally {
        active = false
      }
    },
  }
}
