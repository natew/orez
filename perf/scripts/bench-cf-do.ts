#!/usr/bin/env bun
/**
 * cf-do backend perf + conformance harness.
 *
 * Drives the orez Cloudflare DO SQL backend (the `ZeroDO` worker in
 * src/cf-do/worker.ts) over its HTTP endpoints — /exec, /batch, /changes — the
 * exact surface DoBackend hammers during chat e2e boot. Measures throughput +
 * latency distribution per scenario and asserts conformance (roundtrip, batch
 * atomicity, change capture, monotonic watermark). Use it to baseline the DO
 * SQL path and to verify that an optimization actually helped without breaking
 * correctness.
 *
 * The DO worker must already be running (same as chat e2e, CHAT_E2E.md §5):
 *
 *   cd src/cf-do && bunx wrangler dev --port 8799 --local \
 *     --no-show-interactive-dev-session
 *
 * Then:
 *   bun run perf/scripts/bench-cf-do.ts                 # default load
 *   CF_DO_URL=http://127.0.0.1:8799 CONC=8 N=2000 \
 *     bun run perf/scripts/bench-cf-do.ts
 *
 * env:
 *   CF_DO_URL  backend base url (default http://127.0.0.1:8799)
 *   CONC       concurrency (default 4 — keep modest, this is a shared machine)
 *   N          ops per throughput scenario (default 1000)
 */

import { mkdirSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'

const BASE = process.env.CF_DO_URL || 'http://127.0.0.1:8799'
const CONC = Math.max(1, Number(process.env.CONC) || 4)
const N = Math.max(1, Number(process.env.N) || 1000)
const TABLE = 'bench_item'

function log(msg: string) {
  console.log(`\x1b[1m\x1b[36m[cf-do]\x1b[0m ${msg}`)
}

class ConformanceError extends Error {}
function assert(cond: unknown, message: string): asserts cond {
  if (!cond) throw new ConformanceError(message)
}

// ── HTTP backend client ────────────────────────────────────────────────────

interface ExecResult {
  rows: Record<string, unknown>[]
  columns: string[]
  affectedRows?: number
  error?: string
}
interface SqlTrack {
  tableName: string
  operation: 'INSERT' | 'UPDATE' | 'DELETE'
  returnRows?: boolean
}

async function exec(sql: string, params: unknown[] = [], track?: SqlTrack): Promise<ExecResult> {
  const res = await fetch(`${BASE}/exec`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ sql, params, track }),
  })
  const body = (await res.json()) as ExecResult
  if (!res.ok) throw new Error(`exec ${res.status}: ${body?.error ?? 'unknown'}`)
  return body
}

async function execRaw(
  sql: string,
  params: unknown[] = [],
  track?: SqlTrack
): Promise<{ status: number; body: ExecResult }> {
  const res = await fetch(`${BASE}/exec`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ sql, params, track }),
  })
  return { status: res.status, body: (await res.json()) as ExecResult }
}

async function batch(
  statements: Array<{ sql: string; params?: unknown[]; track?: SqlTrack }>
): Promise<{ status: number; body: { results?: ExecResult[]; error?: string } }> {
  const res = await fetch(`${BASE}/batch`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ statements }),
  })
  return { status: res.status, body: (await res.json()) as any }
}

async function changesSince(
  watermark: number
): Promise<{ watermark: number; changes: Array<{ watermark: number; tableName: string; op: string }> }> {
  const res = await fetch(`${BASE}/changes?watermark=${watermark}&limit=100000`)
  const body = (await res.json()) as any
  if (!res.ok) throw new Error(`changes ${res.status}: ${body?.error ?? 'unknown'}`)
  return body
}

// ── load runner + stats ─────────────────────────────────────────────────────

async function runPool(
  total: number,
  concurrency: number,
  task: (i: number) => Promise<void>
): Promise<{ latencies: number[]; wallMs: number }> {
  const latencies = new Array<number>(total)
  let next = 0
  const t0 = performance.now()
  async function worker() {
    for (;;) {
      const i = next++
      if (i >= total) break
      const s = performance.now()
      await task(i)
      latencies[i] = performance.now() - s
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, total) }, worker))
  return { latencies, wallMs: performance.now() - t0 }
}

function pct(sorted: number[], p: number): number {
  if (!sorted.length) return 0
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)
  return sorted[Math.max(0, idx)]
}

interface ScenarioResult {
  name: string
  ops: number
  concurrency: number
  opsPerSec: number
  meanMs: number
  p50Ms: number
  p95Ms: number
  p99Ms: number
  maxMs: number
}

function summarize(name: string, ops: number, wallMs: number, latencies: number[]): ScenarioResult {
  const sorted = [...latencies].sort((a, b) => a - b)
  const mean = sorted.reduce((a, b) => a + b, 0) / (sorted.length || 1)
  const round = (n: number) => Math.round(n * 100) / 100
  return {
    name,
    ops,
    concurrency: CONC,
    opsPerSec: Math.round((ops / wallMs) * 1000),
    meanMs: round(mean),
    p50Ms: round(pct(sorted, 50)),
    p95Ms: round(pct(sorted, 95)),
    p99Ms: round(pct(sorted, 99)),
    maxMs: round(sorted[sorted.length - 1] ?? 0),
  }
}

// ── scenarios ───────────────────────────────────────────────────────────────

async function setup() {
  log(`resetting ${TABLE} (fresh baseline)`)
  await exec(`DROP TABLE IF EXISTS ${TABLE}`)
  await exec(`CREATE TABLE ${TABLE} (id TEXT PRIMARY KEY, val TEXT, num INTEGER)`)
}

const trackInsert: SqlTrack = { tableName: TABLE, operation: 'INSERT', returnRows: true }

async function scenarioInsert(): Promise<ScenarioResult> {
  const { latencies, wallMs } = await runPool(N, CONC, async (i) => {
    await exec(
      `INSERT INTO ${TABLE} (id, val, num) VALUES (?, ?, ?) RETURNING *`,
      [`item-${i}`, `val-${i}`, i],
      trackInsert
    )
  })
  return summarize('exec INSERT (tracked)', N, wallMs, latencies)
}

async function scenarioSelect(): Promise<ScenarioResult> {
  const { latencies, wallMs } = await runPool(N, CONC, async (i) => {
    await exec(`SELECT * FROM ${TABLE} WHERE id = ?`, [`item-${i % N}`])
  })
  return summarize('exec SELECT (point)', N, wallMs, latencies)
}

async function scenarioBatch(): Promise<ScenarioResult> {
  // M batches of K tracked inserts each — the win we want to quantify vs the
  // per-row /exec amplification chat boot suffers.
  const K = 20
  const M = Math.max(1, Math.floor(N / K))
  const { latencies, wallMs } = await runPool(M, CONC, async (b) => {
    const statements = Array.from({ length: K }, (_, k) => ({
      sql: `INSERT INTO ${TABLE} (id, val, num) VALUES (?, ?, ?) RETURNING *`,
      params: [`batch-${b}-${k}`, `v`, b * K + k],
      track: trackInsert,
    }))
    const { status, body } = await batch(statements)
    if (status !== 200) throw new Error(`batch failed: ${body.error}`)
  })
  const r = summarize(`batch x${K} INSERT`, M, wallMs, latencies)
  // report effective per-statement throughput alongside per-batch
  r.opsPerSec = Math.round(((M * K) / wallMs) * 1000)
  r.name = `batch x${K} INSERT (per-stmt ops/s)`
  return r
}

// ── conformance ─────────────────────────────────────────────────────────────

async function conformance() {
  log('conformance checks...')

  // 1. roundtrip: count matches the inserts done in the insert scenario.
  const count = await exec(`SELECT count(*) AS c FROM ${TABLE} WHERE id LIKE 'item-%'`)
  assert(Number(count.rows[0]?.c) === N, `roundtrip: expected ${N} rows, got ${count.rows[0]?.c}`)

  // 2. change capture + monotonic watermark for the insert scenario's writes.
  const base = await changesSince(0)
  const itemChanges = base.changes.filter(
    (c) => c.tableName === TABLE && c.op === 'INSERT'
  )
  assert(itemChanges.length >= N, `change capture: expected >=${N} INSERT changes, got ${itemChanges.length}`)
  let prev = -1
  for (const c of base.changes) {
    assert(c.watermark > prev, `watermark not strictly increasing at ${c.watermark} (prev ${prev})`)
    prev = c.watermark
  }

  // 3. batch atomicity: a batch with a bad statement must roll back wholesale.
  const before = await exec(`SELECT count(*) AS c FROM ${TABLE}`)
  const beforeCount = Number(before.rows[0]?.c)
  const bad = await batch([
    {
      sql: `INSERT INTO ${TABLE} (id, val, num) VALUES (?, ?, ?)`,
      params: ['atomic-ok', 'x', 1],
    },
    { sql: `INSERT INTO ${TABLE} (id, nope) VALUES (?, ?)`, params: ['atomic-bad', 'y'] },
  ])
  assert(bad.status !== 200, `batch atomicity: bad batch should fail, got ${bad.status}`)
  const after = await exec(`SELECT count(*) AS c FROM ${TABLE}`)
  assert(
    Number(after.rows[0]?.c) === beforeCount,
    `batch atomicity: rollback failed — count ${after.rows[0]?.c} != ${beforeCount}`
  )
  const leaked = await exec(`SELECT id FROM ${TABLE} WHERE id = ?`, ['atomic-ok'])
  assert(leaked.rows.length === 0, `batch atomicity: 'atomic-ok' leaked despite rollback`)

  // 4. delete emits a DELETE change.
  const wmBeforeDelete = (await changesSince(0)).watermark
  await exec(`INSERT INTO ${TABLE} (id, val, num) VALUES (?, ?, ?) RETURNING *`, ['del-me', 'z', 0], trackInsert)
  await exec(`DELETE FROM ${TABLE} WHERE id = ? RETURNING *`, ['del-me'], {
    tableName: TABLE,
    operation: 'DELETE',
    returnRows: true,
  })
  const afterDelete = await changesSince(wmBeforeDelete)
  assert(
    afterDelete.changes.some((c) => c.tableName === TABLE && c.op === 'DELETE'),
    `delete tracking: no DELETE change captured`
  )
  const gone = await exec(`SELECT id FROM ${TABLE} WHERE id = ?`, ['del-me'])
  assert(gone.rows.length === 0, `delete: row still present`)

  log('conformance: ✅ all checks passed')
}

// ── main ────────────────────────────────────────────────────────────────────

async function main() {
  log(`target ${BASE}  concurrency=${CONC}  ops=${N}`)
  // fail fast if the worker isn't up
  try {
    await exec('SELECT 1 AS ok')
  } catch (err) {
    log(`backend not reachable at ${BASE} — start wrangler first (see header).`)
    throw err
  }

  await setup()

  const results: ScenarioResult[] = []
  results.push(await scenarioInsert())
  results.push(await scenarioSelect())
  results.push(await scenarioBatch())

  await conformance()

  // table
  console.log('\n' + '='.repeat(86))
  console.log('  CF-DO SQL BACKEND PERF')
  console.log('='.repeat(86))
  console.log(
    '  scenario'.padEnd(34) +
      'ops/s'.padStart(10) +
      'mean'.padStart(9) +
      'p50'.padStart(9) +
      'p95'.padStart(9) +
      'p99'.padStart(9) +
      'max'.padStart(9)
  )
  console.log('  ' + '-'.repeat(82))
  for (const r of results) {
    console.log(
      '  ' +
        r.name.padEnd(32) +
        String(r.opsPerSec).padStart(10) +
        `${r.meanMs}`.padStart(8) +
        `${r.p50Ms}`.padStart(9) +
        `${r.p95Ms}`.padStart(9) +
        `${r.p99Ms}`.padStart(9) +
        `${r.maxMs}`.padStart(9)
    )
  }
  console.log('='.repeat(86))

  const reportsDir = resolve(import.meta.dirname!, '..', 'reports')
  mkdirSync(reportsDir, { recursive: true })
  const reportFile = resolve(reportsDir, `cf-do-${Date.now()}.json`)
  writeFileSync(
    reportFile,
    JSON.stringify({ base: BASE, concurrency: CONC, ops: N, results }, null, 2)
  )
  log(`report: ${reportFile}`)
}

main().catch((err) => {
  if (err instanceof ConformanceError) {
    console.error(`\n\x1b[31m[cf-do] CONFORMANCE FAILED:\x1b[0m ${err.message}`)
  } else {
    console.error('\n[cf-do] bench failed:', err?.message ?? err)
  }
  process.exit(1)
})
