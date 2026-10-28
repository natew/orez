import jsonPredicate from './zero-json-predicate.json' with { type: 'json' }
import predicate from './zero-number-predicate.json' with { type: 'json' }
import wirePredicate from './zero-wire-predicate.json' with { type: 'json' }

// shared with sync-core: the placeholder is an already-quoted SQL expression.
export function numberStoragePredicate(value: string): string {
  return predicate.join('\n').replaceAll('__OREZ_NUMBER_VALUE__', value)
}

export function wireStoragePredicate(value: string): string {
  return wirePredicate.join('\n').replaceAll('__OREZ_NUMBER_VALUE__', value)
}

export function jsonStoragePredicate(value: string): string {
  return jsonPredicate.join('\n').replaceAll('__OREZ_NUMBER_VALUE__', value)
}

const identifier = '(?:"(?:[^"]|"")+"|`[^`]+`|\\[[^\\]]+\\]|[a-z_][a-z0-9_]*)'
const indexDdl = /^\s*(?:CREATE\s+(?:UNIQUE\s+)?INDEX|DROP\s+INDEX)\s/i
const createTableDdl = new RegExp(
  `^\\s*CREATE\\s+(?:TEMP(?:ORARY)?\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?${identifier}\\s*\\(`,
  'i'
)

// A schema change that cannot alter a stored cell: an index changes neither
// values nor column shape, and a CREATE TABLE with a column list starts empty.
// Both skip the per-column scan of every guarded table, and an index also skips
// the guard reconcile, which depends only on column shape.
export function cellPreservingDdl(sql: string): 'index' | 'create-table' | undefined {
  const statement = sql.replace(/;\s*$/, '')
  if (statement.includes(';')) return undefined
  if (indexDdl.test(statement)) return 'index'
  if (createTableDdl.test(statement)) return 'create-table'
  return undefined
}

// A single CREATE IF NOT EXISTS statement cannot alter an existing table.
export function createTableIfAbsentTarget(sql: string): string | undefined {
  if (sql.includes(';')) return undefined
  const match =
    /^\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(?:"((?:[^"]|"")+)"|([a-z_][a-z0-9_]*))\s*\(/i.exec(
      sql
    )
  return match?.[1]?.replaceAll('""', '"') ?? match?.[2]
}
