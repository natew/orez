/**
 * Smoke test for the compiler scaffold.
 *
 * Validates the pipeline (parse → passes → emit) works end-to-end on a few
 * representative cases. Real coverage comes from the snapshot + oracle tests.
 */
import { describe, expect, it } from 'vitest'

import { CompileError, compile } from './index.js'

describe('pg-sqlite-compiler smoke', () => {
  it('passes through trivial select', () => {
    const { sql, warnings } = compile('SELECT 1 AS ok')
    expect(warnings).toEqual([])
    expect(sql).toMatch(/SELECT\s+1\s+AS\s+ok/i)
  })

  it('rewrites NOW() to CURRENT_TIMESTAMP', () => {
    const { sql } = compile('SELECT NOW() AS t')
    expect(sql).toMatch(/CURRENT_TIMESTAMP/i)
    expect(sql).not.toMatch(/NOW\s*\(/i)
  })

  it('rewrites NOW() inside CREATE TABLE DEFAULT', () => {
    const { sql } = compile(
      'CREATE TABLE event (id text PRIMARY KEY, "createdAt" timestamp DEFAULT NOW() NOT NULL)'
    )
    expect(sql).toMatch(/CURRENT_TIMESTAMP/i)
    expect(sql).not.toMatch(/NOW\s*\(/i)
  })

  it('passes CURRENT_TIMESTAMP keyword through unchanged', () => {
    // CURRENT_TIMESTAMP is a SQL bareword, not a function call — should already
    // round-trip cleanly without our pass touching it.
    const { sql } = compile('SELECT CURRENT_TIMESTAMP AS t')
    expect(sql).toMatch(/CURRENT_TIMESTAMP/i)
  })

  it('rewrites pg_catalog.now() to CURRENT_TIMESTAMP', () => {
    const { sql } = compile('SELECT pg_catalog.now() AS t')
    expect(sql).toMatch(/CURRENT_TIMESTAMP/i)
    expect(sql).not.toMatch(/pg_catalog/i)
  })

  it('rewrites NOW() in an UPDATE SET clause', () => {
    const { sql } = compile('UPDATE event SET "updatedAt" = NOW() WHERE id = $1')
    expect(sql).toMatch(/CURRENT_TIMESTAMP/i)
    expect(sql).not.toMatch(/now\s*\(/i)
  })

  it('rewrites CURRENT_DATE used as DEFAULT', () => {
    const { sql } = compile(
      'CREATE TABLE log (id text PRIMARY KEY, "day" date DEFAULT CURRENT_DATE NOT NULL)'
    )
    expect(sql).toMatch(/CURRENT_DATE/i)
  })

  it('preserves multi-statement input', () => {
    const { sql } = compile('SELECT 1; SELECT 2')
    expect(sql).toMatch(/SELECT\s+1/i)
    expect(sql).toMatch(/SELECT\s+2/i)
  })

  it('preserves quoted identifiers', () => {
    const { sql } = compile('SELECT id, "createdAt" FROM public.message')
    expect(sql).toMatch(/"createdAt"/)
    expect(sql).toMatch(/public\.message/i)
  })

  it('quotes sqlite keyword relation identifiers', () => {
    const cases = [
      [
        'CREATE TABLE IF NOT EXISTS "transaction" (id text PRIMARY KEY)',
        /CREATE TABLE IF NOT EXISTS "transaction"/i,
      ],
      ['SELECT id FROM "transaction" WHERE id = $1', /FROM "transaction"/i],
      ['INSERT INTO "transaction" (id) VALUES ($1)', /INSERT INTO "transaction"/i],
      ['UPDATE "transaction" SET id = $1', /UPDATE "transaction"/i],
      ['DELETE FROM "transaction" WHERE id = $1', /DELETE FROM "transaction"/i],
      ['CREATE INDEX "transaction_id_idx" ON "transaction" (id)', /ON "transaction"/i],
    ] as const

    for (const [input, expected] of cases) {
      const { sql } = compile(input)
      expect(sql).toMatch(expected)
    }
  })

  it('throws on warnings in strict mode', () => {
    expect(() =>
      compile('SELECT 1', {
        strict: true,
        passes: [
          {
            name: 'test-warning-pass',
            run() {
              throw new Error('forced warning')
            },
          },
        ],
      })
    ).toThrow(CompileError)
  })
})
