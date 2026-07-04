import { existsSync, mkdirSync, rmSync } from 'node:fs'
import { resolve } from 'node:path'

import postgres from 'postgres'
const BODY = 'x'.repeat(1024)
function pct(a: number[], p: number) {
  const s = [...a].sort((x, y) => x - y)
  return s.length ? s[Math.min(s.length - 1, Math.floor(p * s.length))] : 0
}
function stat(n: string, a: number[]) {
  if (!a.length) {
    console.log(`${n}: none`)
    return
  }
  const m = a.reduce((x, y) => x + y, 0) / a.length
  console.log(
    `  ${n.padEnd(30)} mean=${m.toFixed(2)} p50=${pct(a, 0.5).toFixed(2)} p95=${pct(a, 0.95).toFixed(2)} p99=${pct(a, 0.99).toFixed(2)} n=${a.length}`
  )
}
const dataDir = resolve('/tmp/orez-bench', `decomp-${Date.now()}`)
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
const sql = postgres({
  host: '127.0.0.1',
  port: orez.pgPort,
  database: 'postgres',
  username: 'user',
  password: 'password',
  max: 6,
  no_subscribe: true,
})
const rc = postgres({
  host: '127.0.0.1',
  port: orez.pgPort,
  database: 'postgres',
  username: 'user',
  password: 'password',
  max: 1,
  no_subscribe: true,
})
await sql.unsafe(
  `CREATE TABLE message(id text primary key, session text, role text, body text, updated_at bigint)`
)
const { installChangeTracking } = await import('../../src/replication/change-tracker.ts')
await installChangeTracking({
  exec: async (s: string) => {
    await rc.unsafe(s)
    return [{}]
  },
  query: async (s: string, p?: unknown[]) => ({
    rows: await rc.unsafe(s, (p as any) || []),
  }),
} as any)
let idc = 0
async function runScenario(
  name: string,
  { writers, reader, streamer }: { writers: number; reader: boolean; streamer: boolean },
  ms = 5000
) {
  const wl: number[] = []
  let stop = false
  const tasks: Promise<void>[] = []
  for (let w = 0; w < writers; w++)
    tasks.push(
      (async () => {
        while (!stop) {
          const id = `m${w}-${idc++ % 500}`
          const s = performance.now()
          await sql.unsafe(
            `INSERT INTO message(id,session,role,body,updated_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET body=EXCLUDED.body,updated_at=EXCLUDED.updated_at`,
            [id, 's' + w, 'assistant', BODY + idc, Date.now()] as any
          )
          wl.push(performance.now() - s)
        }
      })()
    )
  if (reader)
    tasks.push(
      (async () => {
        while (!stop) {
          await sql.unsafe(`SELECT * FROM message WHERE id=$1`, [
            'm0-' + (idc % 500),
          ] as any)
          await new Promise((r) => setTimeout(r, 2))
        }
      })()
    )
  if (streamer)
    tasks.push(
      (async () => {
        let wm = 0
        while (!stop) {
          const rows: any = await rc.unsafe(
            `SELECT watermark,table_name,op,row_data,old_data FROM _orez._zero_changes WHERE watermark>$1 ORDER BY watermark LIMIT 1000`,
            [wm] as any
          )
          if (rows.length) wm = Number(rows[rows.length - 1].watermark)
          if (wm > 2000)
            await rc
              .unsafe(`DELETE FROM _orez._zero_changes WHERE watermark<=$1`, [
                wm - 2000,
              ] as any)
              .catch(() => {})
          await new Promise((r) => setTimeout(r, 5))
        }
      })()
    )
  await new Promise((r) => setTimeout(r, ms))
  stop = true
  await Promise.all(tasks)
  console.log(
    `\n[${name}] writes=${wl.length} (${(wl.length / (ms / 1000)).toFixed(0)}/s)`
  )
  stat('write latency', wl)
}
console.log(
  '=== BENCH 4: contention decomposition (write latency as contenders added) ==='
)
await runScenario('1 writer only', { writers: 1, reader: false, streamer: false })
await runScenario('3 writers', { writers: 3, reader: false, streamer: false })
await runScenario('3 writers + reader', { writers: 3, reader: true, streamer: false })
await runScenario('3 writers + streamer', { writers: 3, reader: false, streamer: true })
await runScenario('3 writers + reader + streamer', {
  writers: 3,
  reader: true,
  streamer: true,
})
await sql.end()
await rc.end()
await orez.stop?.()
console.log('\nDONE')
process.exit(0)
