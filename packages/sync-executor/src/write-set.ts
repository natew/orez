import { MutationWriteSetError } from './errors.js'
import {
  commitPackedLedger,
  preparePackedLedger,
  setPackedCaptureMode,
} from './packed-ledger.js'

import type { PackedLedgerIdentity, PackedLedgerKey } from './packed-ledger.js'
import type {
  ApplicationTransaction,
  JsonPrimitive,
  SqlStatementMetadata,
  ZeroSchemaConfig,
} from './types.js'

type CapturedPrimaryKey = PackedLedgerKey

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
  source: 'database result' | 'executor metadata'
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
    if (source === 'database result' && table.columns[column]?.type === 'boolean') {
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

function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`
}

function expectedResultKeys(
  schema: ZeroSchemaConfig,
  metadata: Extract<SqlStatementMetadata, { readonly capture: 'exact' }>
): CapturedPrimaryKey[] {
  const side = metadata.kind === 'delete' ? 'before' : 'after'
  return metadata.primaryKeys.flatMap((row) => {
    const value = row[side]
    return value
      ? [canonicalPrimaryKey(schema, metadata.publicTable, value, 'executor metadata')]
      : []
  })
}

function validateMetadata(
  schema: ZeroSchemaConfig,
  metadata: Extract<SqlStatementMetadata, { readonly capture: 'exact' }>
): void {
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

  for (const [index, row] of metadata.primaryKeys.entries()) {
    if (!row.before && !row.after) {
      throw new MutationWriteSetError(
        `executor metadata primaryKeys[${index}] has neither before nor after`
      )
    }
    const before = row.before
      ? canonicalPrimaryKey(schema, metadata.publicTable, row.before, 'executor metadata')
      : undefined
    const after = row.after
      ? canonicalPrimaryKey(schema, metadata.publicTable, row.after, 'executor metadata')
      : undefined
    if (before && after && encodedPrimaryKey(before) !== encodedPrimaryKey(after)) {
      throw new MutationWriteSetError(
        'primary-key-changing SQL must use transparent trigger capture'
      )
    }
  }
}

export async function beginWriteSetCapture(
  schema: ZeroSchemaConfig,
  applicationTx: ApplicationTransaction,
  dialect: 'sqlite' | 'postgresql'
): Promise<{
  readonly transaction: ApplicationTransaction
  commit(identity?: PackedLedgerIdentity): Promise<void>
}> {
  if (dialect !== 'sqlite') {
    return {
      transaction: applicationTx,
      commit: async () => {},
    }
  }
  const captureStart = await preparePackedLedger(applicationTx)

  let active = true
  let operation = Promise.resolve<unknown>(undefined)
  let fatalFailure: unknown
  let exactMode = false
  let rawWrite = false
  const exactKeys = new Map<string, CapturedPrimaryKey>()

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
  const setMode = async (exact: boolean) => {
    if (exactMode === exact) return
    await setPackedCaptureMode(applicationTx, exact)
    exactMode = exact
  }

  const transaction: ApplicationTransaction = {
    exec(sql, params, metadata) {
      return serialize(async () => {
        // arbitrary sql keeps the product's trigger-backed "just use the db"
        // contract. generated helpers opt each modeled run into the packed lane.
        if (metadata?.capture !== 'exact') {
          await setMode(false)
          const result = await applicationTx.exec(sql, params, metadata)
          rawWrite ||= result.changes > 0
          return result
        }
        try {
          validateMetadata(schema, metadata)
          const table = schema.tables[metadata.publicTable]!
          if (/\bRETURNING\b/i.test(sql)) {
            throw new MutationWriteSetError(
              'exact write SQL must leave RETURNING to the executor'
            )
          }
          const returning = table.primaryKey
            .map((column) => {
              const physical = table.columns[column]?.serverName ?? column
              return `${quoteIdentifier(physical)} AS ${quoteIdentifier(column)}`
            })
            .join(', ')
          const statement = sql.trim().replace(/;$/, '')
          await setMode(true)
          const returned = await applicationTx.query<Record<string, unknown>>(
            `${statement} RETURNING ${returning}`,
            params
          )
          const actualResultKeys = returned.map((row) =>
            canonicalPrimaryKey(schema, metadata.publicTable, row, 'database result')
          )
          const expected =
            returned.length === 0
              ? []
              : [
                  ...new Set(expectedResultKeys(schema, metadata).map(encodedPrimaryKey)),
                ].sort()
          const actual = [...new Set(actualResultKeys.map(encodedPrimaryKey))].sort()
          if (
            expected.length !== actual.length ||
            expected.some((key, index) => key !== actual[index])
          ) {
            throw new MutationWriteSetError(
              `executor write set ${JSON.stringify(expected)} does not match returned write set ${JSON.stringify(actual)}`
            )
          }
          if (returned.length > 0) {
            for (const key of actualResultKeys) {
              exactKeys.set(encodedPrimaryKey(key), key)
            }
          }
          return { changes: returned.length }
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
    query<Row extends Record<string, unknown> = Record<string, unknown>>(
      sql: string,
      params?: readonly unknown[]
    ) {
      return serialize(async () => {
        await setMode(false)
        const rows = await applicationTx.query<Row>(sql, params)
        rawWrite ||= /\b(?:INSERT|UPDATE|DELETE|REPLACE)\b/i.test(sql)
        return rows
      })
    },
    queryAst(ast, format, queryName) {
      return serialize(async () => {
        await setMode(false)
        return applicationTx.queryAst(ast, format, queryName)
      })
    },
  }

  return {
    transaction,
    async commit(identity?: PackedLedgerIdentity): Promise<void> {
      assertActive()
      try {
        await operation
        if (fatalFailure !== undefined) throw fatalFailure
        await commitPackedLedger(
          applicationTx,
          [...exactKeys.values()],
          identity,
          rawWrite ? captureStart : undefined
        )
      } finally {
        active = false
      }
    },
  }
}
