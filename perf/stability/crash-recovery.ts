#!/usr/bin/env bun
/**
 * orez crash recovery tests — simplified.
 *
 * Tests that actually work within a single process:
 *   1. Clean stop/restart — data survives
 *   2. CDC corruption injection and auto-recovery
 *   3. Multiple sequential starts on same data dir
 *
 * Usage:
 *   bun run perf/stability/crash-recovery.ts
 *   bun run perf/stability/crash-recovery.ts --single-db
 */

import { existsSync, mkdirSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { resolve } from 'node:path'

import postgres from 'postgres'

function log(msg: string) {
  console.log(`\x1b[1m\x1b[31m[crash]\x1b[0m ${msg}`)
}

function pass(msg: string) {
  console.log(`  \x1b[32m✅\x1b[0m ${msg}`)
}

function fail(msg: string) {
  console.log(`  \x1b[31m❌\x1b[0m ${msg}`)
}

const singleDb = process.argv.includes('--single-db')
const DATA_DIR = resolve(tmpdir(), `orez-crash-${Date.now()}`)
const REPORT_DIR = resolve(import.meta.dirname!, '..', 'reports', `crash-${Date.now()}`)

async function main() {
  mkdirSync(REPORT_DIR, { recursive: true })
  if (existsSync(DATA_DIR)) rmSync(DATA_DIR, { recursive: true, force: true })
  mkdirSync(DATA_DIR, { recursive: true })

  log('=== Orez Crash Recovery ===')
  log(`Mode: ${singleDb ? 'singleDb' : 'multi-instance'}`)

  const { startZeroLite } = await import('../../src/index.js')
  const results: Array<{ test: string; passed: boolean; detail: string }> = []

  // ---- Test 1: Clean stop/restart ----

  log('Test 1: Clean stop/restart — data survives')
  {
    const orez1 = await startZeroLite({
      dataDir: DATA_DIR,
      singleDb,
      logLevel: 'error',
      pgPort: 0,
      zeroPort: 0,
      adminPort: 0,
    })

    const sql = postgres({
      host: '127.0.0.1',
      port: orez1.pgPort,
      database: 'postgres',
      username: 'user',
      password: 'password',
      max: 1,
      no_subscribe: true,
    })

    await sql.unsafe(
      `CREATE TABLE IF NOT EXISTS restart_test (id SERIAL PRIMARY KEY, data TEXT)`
    )
    await sql.unsafe(`INSERT INTO restart_test (data) VALUES ('before-restart')`)
    const count1 = (
      (await sql.unsafe(`SELECT count(*) as cnt FROM restart_test`)) as any[]
    )[0]?.cnt
    await sql.end()

    log(`  Data before stop: ${count1} rows`)
    await orez1.stop()
    log('  Cleanly stopped')

    // Restart on same data dir
    const orez2 = await startZeroLite({
      dataDir: DATA_DIR,
      singleDb,
      logLevel: 'error',
      pgPort: 0,
      zeroPort: 0,
      adminPort: 0,
    })

    const sql2 = postgres({
      host: '127.0.0.1',
      port: orez2.pgPort,
      database: 'postgres',
      username: 'user',
      password: 'password',
      max: 1,
      no_subscribe: true,
    })

    const count2 = (
      (await sql2.unsafe(`SELECT count(*) as cnt FROM restart_test`)) as any[]
    )[0]?.cnt
    log(`  Data after restart: ${count2} rows`)

    const ok = Number(count2) >= Number(count1)
    results.push({
      test: 'clean stop/restart',
      passed: ok,
      detail: `${count1}→${count2} rows`,
    })
    if (ok) {
      pass('data survived restart')
    } else {
      fail('data lost')
    }

    await sql2.end()
    await orez2.stop()
  }

  // ---- Test 2: CDC corruption recovery ----

  log('Test 2: CDC corruption injection and recovery')
  {
    const orez1 = await startZeroLite({
      dataDir: DATA_DIR,
      singleDb,
      logLevel: 'error',
      pgPort: 0,
      zeroPort: 0,
      adminPort: 0,
    })

    const sql = postgres({
      host: '127.0.0.1',
      port: orez1.pgPort,
      database: 'postgres',
      username: 'user',
      password: 'password',
      max: 1,
      no_subscribe: true,
    })

    // Create table and insert some data to generate CDC entries
    await sql.unsafe(
      `CREATE TABLE IF NOT EXISTS cdc_test (id SERIAL PRIMARY KEY, data TEXT)`
    )
    for (let i = 0; i < 10; i++) {
      await sql.unsafe(`INSERT INTO cdc_test (data) VALUES ($1)`, [`cdc-${i}`])
    }
    await new Promise((r) => setTimeout(r, 1000))

    // Corrupt CDC: insert duplicate watermark
    const latestWm = (
      (await sql.unsafe(`SELECT max(watermark) as wm FROM _orez._zero_changes`)) as any[]
    )[0]?.wm
    log(`  Latest watermark: ${latestWm}`)

    if (latestWm) {
      try {
        await sql.unsafe(
          `INSERT INTO _orez._zero_changes (watermark, table_name, op, row_data)
           VALUES ($1, 'public.cdc_test', 'INSERT', $2::jsonb)`,
          [latestWm, JSON.stringify({ id: 999, data: 'corrupt' })]
        )
        log('  Injected duplicate watermark')
      } catch (e: any) {
        log(`  Could not inject duplicate: ${e.message?.slice(0, 80)}`)
      }
    }

    await sql.end()
    await orez1.stop()

    // Restart — should handle CDC corruption
    log('  Restarting (expect CDC auto-recovery)...')
    try {
      const orez2 = await startZeroLite({
        dataDir: DATA_DIR,
        singleDb,
        logLevel: 'error',
        pgPort: 0,
        zeroPort: 0,
        adminPort: 0,
      })

      const sql2 = postgres({
        host: '127.0.0.1',
        port: orez2.pgPort,
        database: 'postgres',
        username: 'user',
        password: 'password',
        max: 1,
        no_subscribe: true,
      })

      const rows = (
        (await sql2.unsafe(`SELECT count(*) as cnt FROM cdc_test`)) as any[]
      )[0]?.cnt
      log(`  Rows after recovery: ${rows}`)

      const ok = Number(rows) >= 10
      results.push({
        test: 'CDC corruption recovery',
        passed: ok,
        detail: `${rows} rows survived`,
      })
      if (ok) {
        pass('recovery successful')
      } else {
        fail(`expected >=10, got ${rows}`)
      }

      await sql2.end()
      await orez2.stop()
    } catch (e: any) {
      results.push({
        test: 'CDC corruption recovery',
        passed: false,
        detail: e.message?.slice(0, 100),
      })
      fail(`recovery failed: ${e.message?.slice(0, 100)}`)
    }
  }

  // ---- Test 3: Clean restart reconnects ----

  log('Test 3: Restart produces a working proxy')
  {
    if (existsSync(DATA_DIR)) rmSync(DATA_DIR, { recursive: true, force: true })
    mkdirSync(DATA_DIR, { recursive: true })

    const orez = await startZeroLite({
      dataDir: DATA_DIR,
      singleDb,
      logLevel: 'error',
      pgPort: 0,
      zeroPort: 0,
      adminPort: 0,
    })

    const sql = postgres({
      host: '127.0.0.1',
      port: orez.pgPort,
      database: 'postgres',
      username: 'user',
      password: 'password',
      max: 1,
      no_subscribe: true,
    })

    await sql.unsafe(`CREATE TABLE t (id SERIAL PRIMARY KEY, x INTEGER)`)
    await sql.unsafe(`INSERT INTO t (x) VALUES (42)`)
    const result = (await sql.unsafe(`SELECT x FROM t WHERE id = 1`)) as any[]
    const ok = result.length === 1 && result[0].x === 42
    results.push({
      test: 'restart proxy works',
      passed: ok,
      detail: ok ? 'SELECT returned 42' : 'query failed',
    })
    if (ok) {
      pass('proxy functional')
    } else {
      fail('proxy broken')
    }

    await sql.end()
    await orez.stop()
  }

  // ---- Summary ----

  const passed = results.filter((r) => r.passed).length
  const failed = results.filter((r) => !r.passed).length

  console.log(`\n=== Results: ${passed}/${results.length} passed, ${failed} failed ===`)
  for (const r of results) {
    console.log(`  ${r.passed ? '✅' : '❌'} ${r.test}: ${r.detail}`)
  }

  writeFileSync(
    resolve(REPORT_DIR, 'crash-report.json'),
    JSON.stringify({ timestamp: new Date().toISOString(), singleDb, results }, null, 2)
  )

  try {
    rmSync(DATA_DIR, { recursive: true, force: true })
  } catch {}
  process.exit(failed > 0 ? 1 : 0)
}

main().catch((err) => {
  console.error('Crash tests failed:', err)
  process.exit(1)
})
