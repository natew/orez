#!/usr/bin/env bun
/**
 * Harvest representative test cases from the upstream pgsqlite repo into
 * vendored JSON fixtures we can drive our compiler against.
 *
 * Source: $PGSQLITE_REPO (default ~/github/pgsqlite). We read the rust
 * integration test files under tests/, extract SQL strings from
 * `client.query(...)`, `client.execute(...)`, `client.simple_query(...)`,
 * etc., and bucket them by concern (datetime, array, cast, json, catalog,
 * insert, create-table, returning, …).
 *
 * The result is `src/pg-sqlite-compiler/fixtures/pgsqlite/<bucket>.json`
 * with shape:
 *   { source: "pgsqlite tests/...", cases: [{ name, sql, tags? }] }
 *
 * These fixtures are static — re-run this script when bumping the pgsqlite
 * pin. The script is intentionally simple: it does NOT execute the queries,
 * just harvests strings. Compiler tests decide what to do with each.
 */
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { basename, resolve } from 'node:path'

const PGSQLITE_REPO =
  process.env.PGSQLITE_REPO ?? resolve(homedir(), 'github', 'pgsqlite')
const OUT_DIR = resolve(
  import.meta.dirname,
  '..',
  '..',
  'src',
  'pg-sqlite-compiler',
  'fixtures',
  'pgsqlite'
)

interface Case {
  name: string
  sql: string
  source: string
}

// classify a test file path → bucket (matches our passes/ structure).
function bucketFor(filename: string): string {
  const base = basename(filename, '.rs').toLowerCase()
  if (
    base.startsWith('datetime') ||
    base.includes('interval') ||
    base.includes('extract')
  )
    return 'datetime'
  if (base.startsWith('array')) return 'array'
  if (base.startsWith('cast') || base.startsWith('numeric_cast')) return 'cast'
  if (base.startsWith('json') || base.includes('jsonb')) return 'json'
  if (
    base.startsWith('catalog') ||
    base.startsWith('information_schema') ||
    base.startsWith('pg_')
  )
    return 'catalog'
  if (base.startsWith('enum')) return 'enum'
  if (base.startsWith('create_') || base.startsWith('create-')) return 'create-table'
  if (
    base.startsWith('insert') ||
    base.includes('returning') ||
    base.includes('conflict')
  )
    return 'insert'
  if (base.startsWith('batch_')) return 'insert'
  if (base.startsWith('arithmetic')) return 'arithmetic'
  if (base.startsWith('decimal') || base.startsWith('numeric_format')) return 'numeric'
  return 'misc'
}

// extract SQL literals from a Rust test file. Looks for:
//   client.query("...", ...)
//   client.execute("...", ...)
//   client.simple_query("...")
//   client.query_one("...", ...)
//   client.batch_execute("...")
//   r#"..."#  raw strings
// Heuristic but adequate for harvesting.
function extractSqlFromRust(content: string, filename: string): Case[] {
  const cases: Case[] = []
  let counter = 0

  // pattern 1: ordinary double-quoted "..."
  const re1 =
    /\bclient\s*\.\s*(?:query|query_one|query_opt|execute|simple_query|batch_execute|prepare)\s*\(\s*"((?:[^"\\]|\\.)*)"/g
  let m: RegExpExecArray | null
  while ((m = re1.exec(content)) !== null) {
    const sql = m[1]
      .replace(/\\"/g, '"')
      .replace(/\\n/g, '\n')
      .replace(/\\\\/g, '\\')
      .trim()
    if (sql.length < 4) continue
    if (/^[A-Z_]+$/.test(sql)) continue // skip single tokens
    counter++
    cases.push({
      name: `${basename(filename, '.rs')}-${counter}`,
      sql,
      source: filename,
    })
  }

  // pattern 2: rust raw strings r#"..."# (often multi-line SQL)
  const re2 =
    /\bclient\s*\.\s*(?:query|query_one|query_opt|execute|simple_query|batch_execute|prepare)\s*\(\s*r#"([^]*?)"#/g
  while ((m = re2.exec(content)) !== null) {
    const sql = m[1].trim()
    if (sql.length < 4) continue
    counter++
    cases.push({
      name: `${basename(filename, '.rs')}-${counter}`,
      sql,
      source: filename,
    })
  }

  // pattern 3: `setup_test_table(&server, "name", "CREATE ...")` style helpers
  const re3 = /setup_test_table\s*\([^,]+,\s*"[^"]+"\s*,\s*"((?:[^"\\]|\\.)*)"/g
  while ((m = re3.exec(content)) !== null) {
    const sql = m[1].replace(/\\"/g, '"').trim()
    if (sql.length < 4) continue
    counter++
    cases.push({
      name: `${basename(filename, '.rs')}-${counter}`,
      sql,
      source: filename,
    })
  }

  return cases
}

function main() {
  if (!existsSync(PGSQLITE_REPO)) {
    console.error(
      `pgsqlite repo not found at ${PGSQLITE_REPO}. Set PGSQLITE_REPO or run scripts/pgsqlite/ensure.ts first.`
    )
    process.exit(1)
  }

  const testsDir = resolve(PGSQLITE_REPO, 'tests')
  if (!existsSync(testsDir)) {
    console.error(`no tests/ dir under ${PGSQLITE_REPO}`)
    process.exit(1)
  }

  // collect by bucket
  const buckets: Record<string, Case[]> = {}
  for (const entry of readdirSync(testsDir)) {
    if (!entry.endsWith('.rs')) continue
    if (entry.startsWith('benchmark_')) continue
    const filepath = resolve(testsDir, entry)
    const content = readFileSync(filepath, 'utf-8')
    const bucket = bucketFor(entry)
    const cases = extractSqlFromRust(content, entry)
    if (cases.length === 0) continue
    if (!buckets[bucket]) buckets[bucket] = []
    buckets[bucket].push(...cases)
  }

  // dedupe per bucket by sql text
  for (const bucket in buckets) {
    const seen = new Set<string>()
    buckets[bucket] = buckets[bucket].filter((c) => {
      const k = c.sql.replace(/\s+/g, ' ').trim()
      if (seen.has(k)) return false
      seen.add(k)
      return true
    })
  }

  // write fixtures
  mkdirSync(OUT_DIR, { recursive: true })
  let totalCases = 0
  for (const [bucket, cases] of Object.entries(buckets)) {
    const outPath = resolve(OUT_DIR, `${bucket}.json`)
    writeFileSync(
      outPath,
      JSON.stringify(
        {
          source: 'erans/pgsqlite tests/',
          bucket,
          count: cases.length,
          cases,
        },
        null,
        2
      ) + '\n'
    )
    totalCases += cases.length
    console.log(
      `  ${bucket.padEnd(14)} ${String(cases.length).padStart(5)} cases → ${outPath}`
    )
  }
  console.log(
    `\nharvested ${totalCases} cases across ${Object.keys(buckets).length} buckets`
  )
}

main()
