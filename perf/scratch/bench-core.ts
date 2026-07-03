import { PGlite } from '@electric-sql/pglite'
import {
  installChangeTracking,
  getChangesSince,
  purgeConsumedChanges,
} from '../../src/replication/change-tracker.ts'

// realistic-ish agentbus message body (~1KB)
const BODY = 'x'.repeat(1024)
function pct(arr: number[], p: number) {
  const s = [...arr].sort((a, b) => a - b)
  return s[Math.min(s.length - 1, Math.floor(p * s.length))]
}
function stat(name: string, arr: number[]) {
  const mean = arr.reduce((a, b) => a + b, 0) / arr.length
  console.log(
    `${name.padEnd(42)} mean=${mean.toFixed(3)}ms p50=${pct(arr,0.5).toFixed(3)} p95=${pct(arr,0.95).toFixed(3)} p99=${pct(arr,0.99).toFixed(3)} n=${arr.length}`
  )
  return mean
}

async function freshDb(withTriggers: boolean, opts: any = {}) {
  const db = new PGlite({ dataDir: 'memory://', relaxedDurability: true, ...opts })
  await db.waitReady
  await db.exec('CREATE EXTENSION IF NOT EXISTS plpgsql')
  await db.exec(`CREATE TABLE message (
    id text primary key, session text, role text, body text, updated_at bigint
  )`)
  if (withTriggers) await installChangeTracking(db)
  return db
}

// ---------- BENCH 1: trigger cost (differential) ----------
console.log('\n=== BENCH 1: CDC trigger overhead (direct PGlite, memory://) ===')
for (const withTriggers of [false, true]) {
  const db = await freshDb(withTriggers)
  const tag = withTriggers ? 'WITH triggers ' : 'NO triggers   '
  // INSERT
  {
    const t: number[] = []
    for (let i = 0; i < 2000; i++) {
      const s = performance.now()
      await db.query(`INSERT INTO message (id,session,role,body,updated_at) VALUES ($1,$2,$3,$4,$5)`,
        [`m${i}`, 'sess1', 'assistant', BODY, Date.now()])
      t.push(performance.now() - s)
    }
    stat(`${tag}INSERT`, t.slice(100)) // drop warmup
  }
  // UPSERT (ON CONFLICT DO UPDATE) — the real agentbus write shape
  {
    const t: number[] = []
    for (let i = 0; i < 2000; i++) {
      const s = performance.now()
      await db.query(
        `INSERT INTO message (id,session,role,body,updated_at) VALUES ($1,$2,$3,$4,$5)
         ON CONFLICT (id) DO UPDATE SET body=EXCLUDED.body, updated_at=EXCLUDED.updated_at`,
        [`m${i}`, 'sess1', 'assistant', BODY + i, Date.now()])
      t.push(performance.now() - s)
    }
    stat(`${tag}UPSERT (real change)`, t.slice(100))
  }
  // no-op UPSERT (same body) — trigger's to_jsonb compare should skip the change row
  {
    const t: number[] = []
    for (let i = 0; i < 2000; i++) {
      const s = performance.now()
      await db.query(
        `INSERT INTO message (id,session,role,body,updated_at) VALUES ($1,$2,$3,$4,$5)
         ON CONFLICT (id) DO UPDATE SET body=EXCLUDED.body`,
        [`m${i}`, 'sess1', 'assistant', BODY + i, Date.now()])
      t.push(performance.now() - s)
    }
    stat(`${tag}UPSERT (no-op body)`, t.slice(100))
  }
  // SELECT point
  {
    const t: number[] = []
    for (let i = 0; i < 2000; i++) {
      const s = performance.now()
      await db.query(`SELECT * FROM message WHERE id=$1`, [`m${i % 1000}`])
      t.push(performance.now() - s)
    }
    stat(`${tag}SELECT point`, t.slice(100))
  }
  await db.close()
}

// ---------- BENCH 2: replay path scaling ----------
console.log('\n=== BENCH 2: getChangesSince LIMIT 1000 at table sizes ===')
for (const size of [1000, 10000, 100000]) {
  const db = await freshDb(true)
  // fill _zero_changes by doing that many inserts (triggers append)
  for (let i = 0; i < size; i++) {
    await db.query(`INSERT INTO message (id,session,role,body,updated_at) VALUES ($1,$2,$3,$4,$5)`,
      [`m${i}`, 'sess1', 'assistant', BODY, Date.now()])
  }
  const cnt = await db.query<{c:string}>(`SELECT count(*) c FROM _orez._zero_changes`)
  // EXPLAIN the getChangesSince query
  const plan = await db.query<{'QUERY PLAN':string}>(
    `EXPLAIN SELECT watermark, table_name, op, row_data, old_data FROM _orez._zero_changes WHERE watermark > $1 ORDER BY watermark LIMIT 1000`, [0])
  console.log(`  size=${size} changes-rows=${cnt.rows[0].c}`)
  console.log(`    plan(wm>0 limit1000): ${plan.rows.map(r=>r['QUERY PLAN']).join(' | ')}`)
  // getChangesSince from watermark 0 (worst realistic: consumer far behind)
  {
    const t: number[] = []
    for (let i = 0; i < 30; i++) {
      const s = performance.now()
      await getChangesSince(db, 0, 1000)
      t.push(performance.now() - s)
    }
    stat(`  getChangesSince(0,1000) @size=${size}`, t)
  }
  // getChangesSince near the tail (consumer caught up — the steady-state case)
  {
    const near = size - 1000
    const t: number[] = []
    for (let i = 0; i < 30; i++) {
      const s = performance.now()
      await getChangesSince(db, near, 1000)
      t.push(performance.now() - s)
    }
    stat(`  getChangesSince(tail,1000) @size=${size}`, t)
  }
  // count(*) — the "hang on 118k rows" pathology
  {
    const planC = await db.query<{'QUERY PLAN':string}>(`EXPLAIN SELECT count(*) FROM _orez._zero_changes`)
    console.log(`    plan(count): ${planC.rows.map(r=>r['QUERY PLAN']).join(' | ')}`)
    const t: number[] = []
    for (let i = 0; i < 10; i++) {
      const s = performance.now()
      await db.query(`SELECT count(*) FROM _orez._zero_changes`)
      t.push(performance.now() - s)
    }
    stat(`  count(*) @size=${size}`, t)
  }
  // purge cost (delete all)
  {
    const hi = Number((await db.query<{m:string}>(`SELECT max(watermark) m FROM _orez._zero_changes`)).rows[0].m)
    const s = performance.now()
    const purged = await purgeConsumedChanges(db, hi)
    console.log(`  purgeConsumedChanges(${hi}) purged=${purged} in ${(performance.now()-s).toFixed(1)}ms @size=${size}`)
  }
  await db.close()
}
console.log('\nDONE')
