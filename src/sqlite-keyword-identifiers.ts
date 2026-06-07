const SQLITE_RESERVED_KEYWORDS = new Set([
  'abort',
  'action',
  'add',
  'after',
  'all',
  'alter',
  'always',
  'analyze',
  'and',
  'as',
  'asc',
  'attach',
  'autoincrement',
  'before',
  'begin',
  'between',
  'by',
  'cascade',
  'case',
  'cast',
  'check',
  'collate',
  'column',
  'commit',
  'conflict',
  'constraint',
  'create',
  'cross',
  'current',
  'current_date',
  'current_time',
  'current_timestamp',
  'database',
  'default',
  'deferrable',
  'deferred',
  'delete',
  'desc',
  'detach',
  'distinct',
  'do',
  'drop',
  'each',
  'else',
  'end',
  'escape',
  'except',
  'exclude',
  'exclusive',
  'exists',
  'explain',
  'fail',
  'filter',
  'first',
  'following',
  'for',
  'foreign',
  'from',
  'full',
  'generated',
  'glob',
  'group',
  'groups',
  'having',
  'if',
  'ignore',
  'immediate',
  'in',
  'index',
  'indexed',
  'initially',
  'inner',
  'insert',
  'instead',
  'intersect',
  'into',
  'is',
  'isnull',
  'join',
  'key',
  'last',
  'left',
  'like',
  'limit',
  'match',
  'materialized',
  'natural',
  'no',
  'not',
  'nothing',
  'notnull',
  'null',
  'nulls',
  'of',
  'offset',
  'on',
  'or',
  'order',
  'others',
  'outer',
  'over',
  'partition',
  'plan',
  'pragma',
  'preceding',
  'primary',
  'query',
  'raise',
  'range',
  'recursive',
  'references',
  'regexp',
  'reindex',
  'release',
  'rename',
  'replace',
  'restrict',
  'returning',
  'right',
  'rollback',
  'row',
  'rows',
  'savepoint',
  'select',
  'set',
  'table',
  'temp',
  'temporary',
  'then',
  'ties',
  'to',
  'transaction',
  'trigger',
  'unbounded',
  'union',
  'unique',
  'update',
  'using',
  'vacuum',
  'values',
  'view',
  'virtual',
  'when',
  'where',
  'window',
  'with',
  'without',
])

const IDENTIFIER_KEYS = new Set([
  'aliasname',
  'catalogname',
  'colname',
  'conname',
  'dbname',
  'fdwname',
  'idxname',
  'indexname',
  'name',
  'newname',
  'plname',
  'portalname',
  'provider',
  'relname',
  'rolename',
  'rulename',
  'schemaname',
  'servername',
  'subname',
  'tableSpaceName',
  'trigname',
])

const MARKER_PREFIX = '__orez_sqlite_quote_marker_'
const MARKER_SUFFIX = '__X'

function quoteIdentifier(value: string): string {
  return `"${value.replace(/"/g, '""')}"`
}

function needsSQLiteKeywordQuote(value: string): boolean {
  return SQLITE_RESERVED_KEYWORDS.has(value.toLowerCase())
}

export function markSQLiteKeywordIdentifiers(root: unknown): Map<string, string> {
  const markers = new Map<string, string>()
  const quotedByMarker = new Map<string, string>()
  let markerIndex = 0

  const markerFor = (value: string) => {
    const existing = markers.get(value)
    if (existing) return existing
    const marker = `${MARKER_PREFIX}${markerIndex++}${MARKER_SUFFIX}`
    markers.set(value, marker)
    quotedByMarker.set(marker, quoteIdentifier(value))
    return marker
  }

  const visit = (node: unknown, inColumnRef = false) => {
    if (!node || typeof node !== 'object') return
    if (Array.isArray(node)) {
      for (const item of node) visit(item, inColumnRef)
      return
    }

    const record = node as Record<string, unknown>
    const childColumnRef = inColumnRef || typeof record.ColumnRef === 'object'

    for (const [key, value] of Object.entries(record)) {
      if (
        IDENTIFIER_KEYS.has(key) &&
        typeof value === 'string' &&
        needsSQLiteKeywordQuote(value)
      ) {
        record[key] = markerFor(value)
        continue
      }

      if (
        childColumnRef &&
        key === 'sval' &&
        typeof value === 'string' &&
        needsSQLiteKeywordQuote(value)
      ) {
        record[key] = markerFor(value)
        continue
      }

      visit(value, childColumnRef)
    }
  }

  visit(root)
  return quotedByMarker
}

export function restoreSQLiteKeywordIdentifierMarkers(
  sql: string,
  quotedByMarker: Map<string, string>
): string {
  let next = sql
  for (const [marker, quotedIdentifier] of quotedByMarker) {
    next = next
      .replaceAll(quoteIdentifier(marker), quotedIdentifier)
      .replaceAll(marker, quotedIdentifier)
  }
  return next
}
