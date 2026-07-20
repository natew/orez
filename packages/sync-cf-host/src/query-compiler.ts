import { engine_compile_query } from './wasm.js'

import type { CompiledTransactionQueryPlan } from './transaction-query.js'
import type { TransactionQueryFormat, ZeroSchemaConfig } from 'orez-sync-executor'

export type TransactionQueryCompiler = (
  ast: unknown,
  format: TransactionQueryFormat
) => CompiledTransactionQueryPlan

export function createQueryCompiler(schema: ZeroSchemaConfig): TransactionQueryCompiler {
  return (ast, format) => engine_compile_query(schema, ast, format)
}
