import { PGlite } from '@electric-sql/pglite'

import {
  installChangeTracking,
  getChangesSince,
  purgeConsumedChanges,
} from '../../src/replication/change-tracker.ts'
const BODY = 'x'.repeat(1024)
function pct(a: number[], p: number) {
  const s = [...a].sort((x, y) => x - y)
  return s[Math.min(s.length - 1, Math.floor(p * s.length))]
}
function stat(n: string, a: number[]) {
  const m = a.reduce((x, y) => x + y, 0) / a.length
  console.log(
    `${n.padEnd(44)} mean=${m.toFixed(3)}ms p50=${pct(a, 0.5).toFixed(3)} p95=${pct(a, 0.95).toFixed(3)} n=${a.length}`
  )
  return m
}
async function freshDb() {
  const db = new PGlite({ dataDir: 'memory://', relaxedDurability: true })
  await db.waitReady
  await db.exec('CREATE EXTENSION IF NOT EXISTS plpgsql')
  await db.exec(
    `CREATE TABLE message(id text primary key, session text, role text, body text, updated_at bigint)`
  )
  await installChangeTracking(db)
  return db
}

console.log('=== BENCH 2: replay path scaling ===')
for (const size of [1000, 10000, 100000]) {
  const db = await freshDb()
  const fillStart = performance.now()
  // triggers fire per-row on this set-based insert too
  await db.exec(`INSERT INTO message (id,session,role,body,updated_at)
    SELECT 'm'||g, 'sess1','assistant','${BODY}', ${Date.now()} FROM generate_series(1,${size}) g`)
  const cnt = await db.query<{ c: string }>(`SELECT count(*) c FROM _orez._zero_changes`)
  console.log(
    `\nsize=${size} filled ${cnt.rows[0].c} change rows in ${(performance.now() - fillStart).toFixed(0)}ms`
  )
  const plan = await db.query<{ 'QUERY PLAN': string }>(
    `EXPLAIN SELECT watermark FROM _orez._zero_changes WHERE watermark > $1 ORDER BY watermark LIMIT 1000`,
    [0]
  )
  console.log(
    '  plan(wm>0 limit1000):',
    plan.rows.map((r) => r['QUERY PLAN']).join(' | ')
  )
  {
    const t: number[] = []
    for (let i = 0; i < 30; i++) {
      const s = performance.now()
      await getChangesSince(db, 0, 1000)
      t.push(performance.now() - s)
    }
    stat(`  getChangesSince(0,1000)`, t)
  }
  {
    const near = size - 1000
    const t: number[] = []
    for (let i = 0; i < 30; i++) {
      const s = performance.now()
      await getChangesSince(db, near, 1000)
      t.push(performance.now() - s)
    }
    stat(`  getChangesSince(tail,1000)`, t)
  }
  const planC = await db.query<{ 'QUERY PLAN': string }>(
    `EXPLAIN SELECT count(*) FROM _orez._zero_changes`
  )
  console.log('  plan(count):', planC.rows.map((r) => r['QUERY PLAN']).join(' | '))
  {
    const t: number[] = []
    for (let i = 0; i < 10; i++) {
      const s = performance.now()
      await db.query(`SELECT count(*) FROM _orez._zero_changes`)
      t.push(performance.now() - s)
    }
    stat(`  count(*)`, t)
  }
  const hi = Number(
    (await db.query<{ m: string }>(`SELECT max(watermark) m FROM _orez._zero_changes`))
      .rows[0].m
  )
  const ps = performance.now()
  const purged = await purgeConsumedChanges(db, hi)
  console.log(`  purge ${purged} rows in ${(performance.now() - ps).toFixed(0)}ms`)
  await db.close()
}
console.log('\nDONE')
