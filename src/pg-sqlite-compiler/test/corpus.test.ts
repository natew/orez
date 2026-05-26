/**
 * Corpus tests — run every vendored pgsqlite fixture through the compiler.
 *
 * This test is intentionally lenient: it asserts the compiler doesn't throw
 * for *most* fixtures. Specific buckets have their own oracle tests
 * (datetime.oracle.test.ts, etc.) that check translation correctness.
 *
 * The numbers here are tracked as a regression bar: as we add passes, the
 * "didn't-throw" count should rise. We log a summary table at the end so a
 * human reviewing CI output can see the trendline.
 */
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'

import { describe, expect, it } from 'vitest'

import { compile } from '../index.js'

const FIXTURES_DIR = resolve(import.meta.dirname, '..', 'fixtures', 'pgsqlite')

interface BucketFixture {
  source: string
  bucket: string
  count: number
  cases: { name: string; sql: string; source: string }[]
}

function loadBucket(bucket: string): BucketFixture {
  return JSON.parse(readFileSync(resolve(FIXTURES_DIR, `${bucket}.json`), 'utf-8'))
}

const BUCKETS = [
  'datetime',
  'array',
  'cast',
  'json',
  'catalog',
  'enum',
  'create-table',
  'insert',
  'arithmetic',
  'misc',
]

interface BucketResult {
  bucket: string
  total: number
  parseOk: number
  compileOk: number
  // examples of failures (first 5) — useful when diagnosing regressions
  failures: { name: string; sql: string; error: string }[]
}

const results: BucketResult[] = []

describe('pgsqlite corpus survival', () => {
  for (const bucket of BUCKETS) {
    describe(bucket, () => {
      let fixture: BucketFixture
      try {
        fixture = loadBucket(bucket)
      } catch {
        it.skip(`fixture missing for ${bucket}`, () => {})
        return
      }

      it(`compiles ${fixture.cases.length} cases without throwing`, () => {
        const result: BucketResult = {
          bucket,
          total: fixture.cases.length,
          parseOk: 0,
          compileOk: 0,
          failures: [],
        }
        for (const c of fixture.cases) {
          try {
            const { sql, warnings } = compile(c.sql)
            if (sql) {
              result.compileOk++
              result.parseOk++
            } else if (warnings.length === 0) {
              result.compileOk++
              result.parseOk++
            }
          } catch (err: any) {
            if (result.failures.length < 5) {
              result.failures.push({
                name: c.name,
                sql: c.sql.slice(0, 200),
                error: (err.message || String(err)).slice(0, 200),
              })
            }
          }
        }
        results.push(result)
        // be lenient at v1: require at least 50% non-throwing
        const pctOk = (result.compileOk / Math.max(1, result.total)) * 100
        expect(pctOk).toBeGreaterThanOrEqual(50)
      })
    })
  }
})

// emit a summary table after all tests run
import { afterAll } from 'vitest'
afterAll(() => {
  if (results.length === 0) return
  console.log('\n========== CORPUS COMPILER SURVIVAL ==========')
  console.log('bucket         total   ok    pct  example-failures')
  console.log('--------------------------------------------------')
  let totalAll = 0
  let okAll = 0
  for (const r of results.sort((a, b) => a.bucket.localeCompare(b.bucket))) {
    const pct = ((r.compileOk / Math.max(1, r.total)) * 100).toFixed(1).padStart(5)
    const ex = r.failures[0] ? `${r.failures[0].error.slice(0, 50)}` : ''
    console.log(
      `${r.bucket.padEnd(14)} ${String(r.total).padStart(5)} ${String(r.compileOk).padStart(5)} ${pct}%  ${ex}`
    )
    totalAll += r.total
    okAll += r.compileOk
  }
  console.log('--------------------------------------------------')
  console.log(
    `${'TOTAL'.padEnd(14)} ${String(totalAll).padStart(5)} ${String(okAll).padStart(5)} ${((okAll / Math.max(1, totalAll)) * 100).toFixed(1).padStart(5)}%`
  )
  console.log('==============================================')
})
