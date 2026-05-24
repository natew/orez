const TOTAL_BYTES_ALIAS_RE = /^AS\s+"totalBytes"\s+/i
const TOTAL_BYTES_TERM_RE =
  /^SUM\s*\(\s*COALESCE\s*\(\s*pg_column_size\s*\(\s*((?:"(?:[^"]|"")+")|(?:[a-z_][a-z0-9_$]*))\s*\)\s*,\s*0\s*\)\s*\)$/i

function findMatchingParen(sql: string, openIndex: number): number {
  let depth = 0
  let inSingleQuote = false
  let inDoubleQuote = false

  for (let i = openIndex; i < sql.length; i++) {
    const ch = sql[i]
    const next = sql[i + 1]

    if (inSingleQuote) {
      if (ch === "'" && next === "'") {
        i++
      } else if (ch === "'") {
        inSingleQuote = false
      }
      continue
    }

    if (inDoubleQuote) {
      if (ch === '"' && next === '"') {
        i++
      } else if (ch === '"') {
        inDoubleQuote = false
      }
      continue
    }

    if (ch === "'") {
      inSingleQuote = true
      continue
    }
    if (ch === '"') {
      inDoubleQuote = true
      continue
    }
    if (ch === '(') {
      depth++
      continue
    }
    if (ch === ')') {
      depth--
      if (depth === 0) return i
    }
  }

  return -1
}

function splitTopLevelAddends(expr: string): string[] | null {
  const terms: string[] = []
  let depth = 0
  let start = 0
  let inSingleQuote = false
  let inDoubleQuote = false

  for (let i = 0; i < expr.length; i++) {
    const ch = expr[i]
    const next = expr[i + 1]

    if (inSingleQuote) {
      if (ch === "'" && next === "'") {
        i++
      } else if (ch === "'") {
        inSingleQuote = false
      }
      continue
    }

    if (inDoubleQuote) {
      if (ch === '"' && next === '"') {
        i++
      } else if (ch === '"') {
        inDoubleQuote = false
      }
      continue
    }

    if (ch === "'") {
      inSingleQuote = true
      continue
    }
    if (ch === '"') {
      inDoubleQuote = true
      continue
    }
    if (ch === '(') {
      depth++
      continue
    }
    if (ch === ')') {
      depth--
      if (depth < 0) return null
      continue
    }
    if (ch === '+' && depth === 0) {
      terms.push(expr.slice(start, i).trim())
      start = i + 1
    }
  }

  if (depth !== 0 || inSingleQuote || inDoubleQuote) return null

  terms.push(expr.slice(start).trim())
  return terms
}

function stripTrailingSemicolon(sql: string): string {
  const trimmedEnd = sql.trimEnd()
  return trimmedEnd.endsWith(';') ? trimmedEnd.slice(0, -1) : sql
}

export function rewritePgColumnSizeTotalBytesQuery(query: string): string {
  const leadingWhitespace = query.match(/^\s*/)?.[0] ?? ''
  const trimmedStart = query.slice(leadingWhitespace.length)
  if (!/^SELECT\b/i.test(trimmedStart)) return query

  const afterSelect = trimmedStart.slice('SELECT'.length).trimStart()
  if (!afterSelect.startsWith('(')) return query

  const openIndex = trimmedStart.indexOf(afterSelect)
  const closeIndex = findMatchingParen(trimmedStart, openIndex)
  if (closeIndex < 0) return query

  const expression = trimmedStart.slice(openIndex + 1, closeIndex)
  const afterExpression = trimmedStart.slice(closeIndex + 1).trimStart()
  const aliasMatch = afterExpression.match(TOTAL_BYTES_ALIAS_RE)
  if (!aliasMatch) return query

  const fromClause = stripTrailingSemicolon(
    afterExpression.slice(aliasMatch[0].length).trim()
  )
  if (!/^FROM\b/i.test(fromClause)) return query

  const terms = splitTopLevelAddends(expression)
  if (!terms || terms.length === 0) return query

  const columns: string[] = []
  for (const term of terms) {
    const match = term.match(TOTAL_BYTES_TERM_RE)
    if (!match) return query
    columns.push(match[1])
  }

  const rewrittenTerms = columns.map(
    (column) => `(SELECT SUM(COALESCE(pg_column_size(${column}), 0)) ${fromClause})`
  )
  return `${leadingWhitespace}SELECT ${rewrittenTerms.join(' + ')} AS "totalBytes"`
}
