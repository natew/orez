import { engine_compile_query } from './wasm.js'

import type {
  CompiledTransactionQueryPlan,
  TransactionQueryFormat,
} from './transaction-query.js'
import type { ZeroSchemaConfig } from './types.js'

export type TransactionQueryCompiler = (
  ast: unknown,
  format: TransactionQueryFormat
) => CompiledTransactionQueryPlan

export function createQueryCompiler(schema: ZeroSchemaConfig): TransactionQueryCompiler {
  return (ast, format) => engine_compile_query(schema, ast, format)
}
