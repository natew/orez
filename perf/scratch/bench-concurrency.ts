import { existsSync, mkdirSync, rmSync } from 'node:fs'
import { resolve } from 'node:path'

import postgres from 'postgres'

import {
  getChangesSince,
  purgeConsumedChanges,
} from '../../src/replication/change-tracker.ts'

const BODY = 'x'.repeat(1024)
function pct(a: number[], p: number) {
  const s = [...a].sort((x, y) => x - y)
  return s.length ? s[Math.min(s.length - 1, Math.floor(p * s.length))] : 0
}
function stat(n: string, a: number[]) {
  if (!a.length) {
    console.log(`${n}: no samples`)
    return
  }
  const m = a.reduce((x, y) => x + y, 0) / a.length
  console.log(
    `${n.padEnd(34)} mean=${m.toFixed(2)}ms p50=${pct(a, 0.5).toFixed(2)} p95=${pct(a, 0.95).toFixed(2)} p99=${pct(a, 0.99).toFixed(2)} max=${Math.max(...a).toFixed(1)} n=${a.length}`
  )
}

const dataDir = resolve('/tmp/orez-bench', `conc-${Date.now()}`)
if (existsSync(dataDir)) rmSync(dataDir, { recursive: true, force: true })
mkdirSync(dataDir, { recursive: true })
const { startZeroLite } = await import('../../src/index.js')
const orez = await startZeroLite({
  dataDir,
  singleDb: true,
  logLevel: 'error',
  pgPort: 0,
  zeroPort: 0,
  adminPort: 0,
  skipZeroCache: true,
})
console.log(`proxy up pg=${orez.pgPort} (singleDb)`)
const sql = postgres({
  host: '127.0.0.1',
  port: orez.pgPort,
  database: 'postgres',
  username: 'user',
  password: 'password',
  max: 5,
  no_subscribe: true,
})
await sql.unsafe(
  `CREATE TABLE message(id text primary key, session text, role text, body text, updated_at bigint)`
)
// trigger installation happens via replication connect normally; here install directly through proxy so writes are tracked
// (mirror change-tracking install)
import('../../src/replication/change-tracker.ts').then(async (m) => {})

// We reach into the underlying instance for the replication-side reads/purges to
// emulate the change-streamer sharing the mutex. Use a 2nd proxy connection instead
// (same mutex path). getChangesSince/purge run as plain SQL over a pg connection.
const replConn = postgres({
  host: '127.0.0.1',
  port: orez.pgPort,
  database: 'postgres',
  username: 'user',
  password: 'password',
  max: 1,
  no_subscribe: true,
})
async function changesSince(wm: number) {
  return replConn.unsafe(
    `SELECT watermark,table_name,op,row_data,old_data FROM _orez._zero_changes WHERE watermark > $1 ORDER BY watermark LIMIT 1000`,
    [wm] as any
  )
}

// install triggers by invoking the proxy's replication path indirectly:
// simplest — run installChangeTracking through a direct SQL wrapper over the proxy conn
{
  const { installChangeTracking } =
    await import('../../src/replication/change-tracker.ts')
  const wrap = {
    exec: async (s: string) => {
      await replConn.unsafe(s)
      return [{}]
    },
    query: async (s: string, p?: unknown[]) => ({
      rows: await replConn.unsafe(s, (p as any) || []),
    }),
  }
  await installChangeTracking(wrap as any)
}
console.log('triggers installed via proxy')

const DURATION_MS = 8000
const NWRITERS = 3
let idc = 0
const writeLat: number[] = []
const readLat: number[] = []
const changeLat: number[] = []
let stop = false

// writers: upsert full-body messages (agentbus shape). 3 concurrent.
async function writer(w: number) {
  while (!stop) {
    const id = `m${w}-${idc++ % 500}` // recycle ids to force real UPDATEs (change rows w/ old+new)
    const s = performance.now()
    await sql.unsafe(
      `INSERT INTO message(id,session,role,body,updated_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET body=EXCLUDED.body,updated_at=EXCLUDED.updated_at`,
      [id, 'sess' + w, 'assistant', BODY + idc, Date.now()] as any
    )
    writeLat.push(performance.now() - s)
  }
}
// reader: point selects (client queries)
async function reader() {
  while (!stop) {
    const s = performance.now()
    await sql.unsafe(`SELECT * FROM message WHERE id=$1`, ['m0-' + (idc % 500)] as any)
    readLat.push(performance.now() - s)
    await new Promise((r) => setTimeout(r, 2))
  }
}
// change-streamer emulation: getChangesSince loop + purge
async function streamer() {
  let wm = 0
  while (!stop) {
    const s = performance.now()
    const rows: any = await changesSince(wm)
    changeLat.push(performance.now() - s)
    if (rows.length) {
      wm = Number(rows[rows.length - 1].watermark)
    }
    if (wm > 0 && idc % 50 === 0) {
      await replConn
        .unsafe(`DELETE FROM _orez._zero_changes WHERE watermark <= $1`, [
          wm - 2000,
        ] as any)
        .catch(() => {})
    }
    await new Promise((r) => setTimeout(r, 5))
  }
}

console.log(
  `\n=== BENCH 3: real concurrent workload (${NWRITERS} writers + 1 reader + 1 streamer, ${DURATION_MS}ms) ===`
)
const tasks = [reader(), streamer()]
for (let w = 0; w < NWRITERS; w++) tasks.push(writer(w))
await new Promise((r) => setTimeout(r, DURATION_MS))
stop = true
await Promise.all(tasks)
console.log(
  `total writes=${writeLat.length} (${(writeLat.length / (DURATION_MS / 1000)).toFixed(0)}/s) reads=${readLat.length} changeQueries=${changeLat.length}`
)
stat('WRITE upsert (through proxy)', writeLat)
stat('READ point select', readLat)
stat('getChangesSince(1000)', changeLat)

// now measure SOLO write latency (no contention) for comparison
stop = false
writeLat.length = 0
const solo = writer(9)
await new Promise((r) => setTimeout(r, 3000))
stop = true
await solo
console.log('\n--- solo writer (no contention) ---')
stat('WRITE upsert solo', writeLat)

await sql.end()
await replConn.end()
await orez.stop?.()
console.log('DONE')
process.exit(0)
