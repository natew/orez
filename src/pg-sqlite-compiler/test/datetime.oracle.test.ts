/**
 * Oracle tests for the datetime pass.
 *
 * For each PG query: run it against pgsqlite (oracle) AND through our
 * compile() + bun:sqlite. Compare result shapes. We don't compare exact
 * timestamps (CURRENT_TIMESTAMP returns "now" — non-deterministic) — just
 * that both sides return a row of the same shape with a parseable timestamp.
 */
import { describe, expect, it } from 'vitest'

import { compile } from '../index.js'
import {
  ORACLE_AVAILABLE,
  connectOracle,
  runOracleAndCompiler,
  startPgsqliteServer,
} from './oracle.js'

const describeOracle = ORACLE_AVAILABLE ? describe : describe.skip

describeOracle('datetime pass against pgsqlite oracle', () => {
  it('NOW() in SELECT — both sides return a timestamp', async () => {
    const { oracle, ours } = await runOracleAndCompiler([], 'SELECT NOW() AS t')
    expect(oracle).toHaveLength(1)
    expect(ours).toHaveLength(1)
    // both should expose 't' with something Date-parseable
    const ot = String((oracle[0] as any).t)
    const ut = String((ours[0] as any).t)
    expect(Number.isNaN(Date.parse(ot))).toBe(false)
    expect(Number.isNaN(Date.parse(ut))).toBe(false)
  }, 30_000)

  /**
   * pgsqlite has a known bug here: it rewrites `DEFAULT NOW()` →
   * `DEFAULT datetime('now')`, which isn't valid in a SQLite CREATE TABLE
   * DEFAULT clause (only a small expression grammar is permitted). Our
   * compiler rewrites to `DEFAULT CURRENT_TIMESTAMP` which IS valid.
   *
   * So this test validates *our* side standalone; if pgsqlite ever fixes
   * this bug, we can promote it to a full oracle test.
   */
  it('NOW() in CREATE TABLE DEFAULT — ours works (pgsqlite has a known bug here)', async () => {
    // @ts-expect-error — test-only sqlite driver
    const { default: Database } = await import('@rocicorp/zero-sqlite3')
    const db = new Database(':memory:')
    const { sql: c1 } = compile(
      'CREATE TABLE event (id text PRIMARY KEY, ts timestamp DEFAULT NOW() NOT NULL)'
    )
    db.exec(c1)
    db.prepare('INSERT INTO event (id) VALUES (?)').run('e1')
    const ours = db.prepare('SELECT id, ts FROM event WHERE id = ?').all('e1') as any[]
    db.close()
    expect(ours).toHaveLength(1)
    expect(ours[0].id).toBe('e1')
    expect(String(ours[0].ts)).toMatch(/^\d{4}-\d{2}-\d{2}/)
  })

  it('CURRENT_DATE in DEFAULT — both sides accept', async () => {
    const server = await startPgsqliteServer()
    try {
      const conn = await connectOracle(server)
      try {
        await conn.exec(
          'CREATE TABLE log (id text PRIMARY KEY, day date DEFAULT CURRENT_DATE NOT NULL)'
        )
        await conn.exec('INSERT INTO log (id) VALUES ($1)', ['l1'])
        const oracle = (await conn.exec('SELECT id, day FROM log WHERE id = $1', [
          'l1',
        ])) as any[]

        // @ts-expect-error — test-only sqlite driver
        const { default: Database } = await import('@rocicorp/zero-sqlite3')
        const db = new Database(':memory:')
        const { sql: c1 } = compile(
          'CREATE TABLE log (id text PRIMARY KEY, day date DEFAULT CURRENT_DATE NOT NULL)'
        )
        db.exec(c1)
        db.prepare('INSERT INTO log (id) VALUES (?)').run('l1')
        const ours = db.prepare('SELECT id, day FROM log WHERE id = ?').all('l1') as any[]
        db.close()

        expect(oracle).toHaveLength(1)
        expect(ours).toHaveLength(1)
        // postgres.js parses DATE values into JS Date objects; verify both
        // sides produced a Date-equivalent (today). Don't compare strings —
        // pgsqlite returns ISO, sqlite returns YYYY-MM-DD, both valid.
        const oracleDate = new Date(String(oracle[0].day))
        const ourDate = new Date(String(ours[0].day))
        expect(Number.isNaN(oracleDate.getTime())).toBe(false)
        expect(Number.isNaN(ourDate.getTime())).toBe(false)
        // Within ~24h of each other (allowing for TZ).
        expect(Math.abs(oracleDate.getTime() - ourDate.getTime())).toBeLessThan(
          86_400_000
        )
      } finally {
        await conn.end()
      }
    } finally {
      await server.stop()
    }
  }, 30_000)
})
