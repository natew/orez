import { rmSync } from 'node:fs'
import { resolve } from 'node:path'

import { DoBackend } from '../../src/pg-proxy-do-backend.js'

const TABLE_COUNTS: Array<[string, number]> = [
  ['reaction', 1_853],
  ['server_log', 793],
  ['job', 669],
  ['data', 454],
  ['message', 150],
  ['channel', 104],
  ['server_member', 100],
  ['user_role', 88],
  ...Array.from(
    { length: 43 },
    (_, index) =>
      [`fixture_${String(index + 1).padStart(2, '0')}`, index < 22 ? 11 : 10] as [
        string,
        number,
      ]
  ),
]

const fixture = {
  tables: TABLE_COUNTS.length,
  rows: TABLE_COUNTS.reduce((sum, [, count]) => sum + count, 0),
  indexes: TABLE_COUNTS.length + TABLE_COUNTS.length * 2 + 23,
}

if (
  JSON.stringify(fixture) !== JSON.stringify({ tables: 51, rows: 4_663, indexes: 176 })
) {
  throw new Error(`fixture shape changed: ${JSON.stringify(fixture)}`)
}

function literal(value: string): string {
  return `'${value.replaceAll("'", "''")}'`
}

async function seed(baseURL: string, namespace: string) {
  const backend = new DoBackend(`${baseURL}/source`, 'postgres', namespace)
  await backend.waitReady
  const ddl: string[] = []
  for (const [index, [table]] of TABLE_COUNTS.entries()) {
    ddl.push(`CREATE TABLE ${table} (
      id TEXT PRIMARY KEY,
      group_id TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      kind TEXT NOT NULL
    )`)
    ddl.push(`CREATE INDEX ${table}_group_id ON ${table}(group_id)`)
    ddl.push(`CREATE INDEX ${table}_kind ON ${table}(kind)`)
    if (index < 23) ddl.push(`CREATE INDEX ${table}_created_at ON ${table}(created_at)`)
  }
  await backend.exec(ddl.join(';\n'))

  for (const [table, count] of TABLE_COUNTS) {
    for (let offset = 0; offset < count; offset += 100) {
      const values = Array.from({ length: Math.min(100, count - offset) }, (_, i) => {
        const row = offset + i
        return `(${literal(`${table}-${row}`)}, ${literal(`group-${row % 17}`)}, ${literal(`2026-01-${String((row % 28) + 1).padStart(2, '0')} 00:00:00+00`)}, ${literal(`kind-${row % 7}`)})`
      })
      await backend.exec(
        `INSERT INTO ${table} (id, group_id, created_at, kind) VALUES ${values.join(',')}`
      )
    }
  }
  await backend.exec(
    `CREATE PUBLICATION profile_publication FOR TABLE ${TABLE_COUNTS.map(([table]) => table).join(', ')}`
  )

  const tables = await backend.query<{ count: number }>(
    `SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'table' AND name IN (${TABLE_COUNTS.map(([table]) => literal(table)).join(',')})`
  )
  const indexes = await backend.query<{ count: number }>(
    `SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'index' AND tbl_name IN (${TABLE_COUNTS.map(([table]) => literal(table)).join(',')})`
  )
  let rowCount = 0
  for (const [table] of TABLE_COUNTS) {
    const result = await backend.query<{ count: number }>(
      `SELECT COUNT(*) AS count FROM ${table}`
    )
    rowCount += Number(result.rows[0]?.count)
  }
  const actual = {
    tables: Number(tables.rows[0]?.count),
    rows: rowCount,
    indexes: Number(indexes.rows[0]?.count),
  }
  if (JSON.stringify(actual) !== JSON.stringify(fixture)) {
    throw new Error(`seed verification failed: ${JSON.stringify(actual)}`)
  }
  await backend.close()
}

async function json<T>(
  url: string,
  init?: RequestInit
): Promise<{ response: Response; body: T }> {
  const response = await fetch(url, init)
  const body = (await response.json()) as T
  return { response, body }
}

const port = 9_900 + Math.floor(Math.random() * 80)
const persistencePath = `/tmp/orez-production-shape-${crypto.randomUUID()}`
const baseURL = `http://127.0.0.1:${port}`
const wrangler = Bun.spawn(
  [
    'bunx',
    'wrangler',
    'dev',
    '--config',
    'wrangler.production-shape.toml',
    '--local',
    '--persist-to',
    persistencePath,
    '--port',
    String(port),
  ],
  { cwd: import.meta.dir, stdout: 'ignore', stderr: 'inherit' }
)

try {
  for (let attempt = 0; ; attempt++) {
    try {
      if ((await fetch(`${baseURL}/health`)).ok) break
    } catch {}
    if (attempt >= 300) throw new Error('workerd did not become ready')
    await Bun.sleep(100)
  }

  await seed(baseURL, 'clean-source')
  const clean = await json<{ ok: boolean; ready: boolean; durationMs: number }>(
    `${baseURL}/cache/boot?cache=clean-cache&source=clean-source&phase=clean&readyTimeout=1200000`,
    { method: 'POST' }
  )
  if (!clean.response.ok || !clean.body.ready) {
    throw new Error(`clean initial sync failed: ${JSON.stringify(clean.body)}`)
  }
  const cleanReport = await json<Record<string, unknown>>(
    `${baseURL}/cache/report?cache=clean-cache`
  )
  await fetch(`${baseURL}/cache/stop?cache=clean-cache`, { method: 'POST' })

  await seed(baseURL, 'retry-source')
  const forcedTimeoutMs = Math.max(250, Math.floor(clean.body.durationMs * 0.2))
  const timedOut = await json<{ ok: boolean; durationMs: number; error: string }>(
    `${baseURL}/cache/boot?cache=retry-cache&source=retry-source&phase=timedOut&readyTimeout=${forcedTimeoutMs}`,
    { method: 'POST' }
  )
  if (
    timedOut.response.status !== 504 ||
    !/timed out waiting for ready/.test(timedOut.body.error)
  ) {
    throw new Error(`forced timeout did not time out: ${JSON.stringify(timedOut.body)}`)
  }
  const retry = await json<{ ok: boolean; ready: boolean; durationMs: number }>(
    `${baseURL}/cache/boot?cache=retry-cache&source=retry-source&phase=retry&readyTimeout=1200000`,
    { method: 'POST' }
  )
  if (!retry.response.ok || !retry.body.ready) {
    throw new Error(`retry failed: ${JSON.stringify(retry.body)}`)
  }
  const retryReport = await json<Record<string, unknown>>(
    `${baseURL}/cache/report?cache=retry-cache`
  )
  await fetch(`${baseURL}/cache/stop?cache=retry-cache`, { method: 'POST' })

  console.log(
    JSON.stringify(
      {
        measuredAt: new Date().toISOString(),
        fixture,
        clean: { boot: clean.body, measurements: cleanReport.body },
        forcedTimeout: {
          readyTimeoutMs: forcedTimeoutMs,
          attempt: timedOut.body,
          retry: retry.body,
          measurements: retryReport.body,
        },
      },
      null,
      2
    )
  )
} finally {
  wrangler.kill()
  await wrangler.exited
  rmSync(persistencePath, { recursive: true, force: true })
}
