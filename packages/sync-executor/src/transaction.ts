import { createBuilder } from '@rocicorp/zero'
import { asQueryInternals } from '@rocicorp/zero/bindings'

import { executeCrud } from './crud.js'

import type {
  ApplicationTransaction,
  JsonValue,
  ServerTransaction,
  TransactionQueryFormat,
} from './types.js'
import type { HumanReadable, Query, Schema } from '@rocicorp/zero'

export function createServerTransaction<S extends Schema>(
  schema: S,
  applicationTx: ApplicationTransaction,
  dialect: 'sqlite' | 'postgresql',
  clientID = '',
  mutationID = 0
): ServerTransaction<S> {
  const mutate: Record<string, Record<string, (value: unknown) => Promise<void>>> = {}
  for (const tableName of Object.keys(schema.tables)) {
    mutate[tableName] = {
      insert: (value) =>
        executeCrud(applicationTx, schema, dialect, tableName, 'insert', value),
      upsert: (value) =>
        executeCrud(applicationTx, schema, dialect, tableName, 'upsert', value),
      update: (value) =>
        executeCrud(applicationTx, schema, dialect, tableName, 'update', value),
      delete: (value) =>
        executeCrud(applicationTx, schema, dialect, tableName, 'delete', value),
    }
  }

  const dbTransaction = {
    wrappedTransaction: applicationTx,
    async query(sql: string, args: unknown[]) {
      return applicationTx.query(sql, args)
    },
    async runQuery<Result>(
      ast: JsonValue,
      format: TransactionQueryFormat
    ): Promise<Result> {
      return applicationTx.queryAst<Result>(ast, format)
    },
  }

  return {
    location: 'server',
    reason: 'authoritative',
    clientID,
    mutationID,
    mutate,
    query: createBuilder(schema),
    dbTransaction,
    async run<TTable extends keyof S['tables'] & string, Result>(
      query: Query<TTable, S, Result>
    ): Promise<HumanReadable<Result>> {
      const internals = asQueryInternals(query)
      return applicationTx.queryAst<HumanReadable<Result>>(
        internals.ast as JsonValue,
        internals.format
      )
    },
  } as unknown as ServerTransaction<S>
}
