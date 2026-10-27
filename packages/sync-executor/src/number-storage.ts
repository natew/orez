import predicate from './zero-number-predicate.json' with { type: 'json' }
import wirePredicate from './zero-wire-predicate.json' with { type: 'json' }
import jsonPredicate from './zero-json-predicate.json' with { type: 'json' }

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

// A single CREATE IF NOT EXISTS statement cannot alter an existing table.
export function createTableIfAbsentTarget(sql: string): string | undefined {
  if (sql.includes(';')) return undefined
  const match =
    /^\s*CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(?:"((?:[^"]|"")+)"|([a-z_][a-z0-9_]*))\s*\(/i.exec(
      sql
    )
  return match?.[1]?.replaceAll('""', '"') ?? match?.[2]
}
