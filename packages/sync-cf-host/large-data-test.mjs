// The discriminating large-data proof for single-mode sync: a namespace holding
// tens of megabytes serves a scoped named query with work and bytes
// proportional to the QUERY RESULT, never the projection. Under the deleted
// full-projection mode this exact pull shipped the whole namespace (a real
// 43 MB initial pull failed with HTTP 500 "Invalid array buffer length");
// under query membership it must stay a few kilobytes and fast.
import assert from 'node:assert/strict'

import { findPort } from '../../src/port.ts'

const adminKey = process.env.M3_ADMIN_KEY ?? 'local-admin'
const port = await findPort(0)
const server = Bun.spawn(
  [
    'bunx',
    'wrangler',
    'dev',
    '--config',
    'wrangler.toml',
    '--local',
    '--var',
    `ADMIN_KEY:${adminKey}`,
    '--port',
    String(port),
  ],
  {
    cwd: new URL('.', import.meta.url).pathname,
    stdout: 'inherit',
    stderr: 'inherit',
  }
)
const baseURL = `http://127.0.0.1:${port}`
for (let attempt = 0; ; attempt++) {
  try {
    if ((await fetch(baseURL)).ok) break
  } catch {}
  if (attempt >= 150) throw new Error('workerd did not become ready')
  await new Promise((resolve) => setTimeout(resolve, 100))
}

let assertions = 0
const equal = (actual, expected, message) => {
  assert.deepStrictEqual(actual, expected, message)
  assertions++
}
const namespace = `large-${crypto.randomUUID()}`
const origin = `${baseURL}/${namespace}`

const admin = async (body) => {
  const response = await fetch(`${origin}/admin/sql`, {
    method: 'POST',
    headers: { 'x-admin-key': adminKey, 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  assert.equal(response.ok, true, `admin/sql: ${response.status}`)
  return response.json()
}

try {
  // seed ~24 MB of task rows in bulk: 12k rows, ~2 KB of random hex each.
  // batched so no single Durable Object transaction carries the whole load.
  for (let batch = 0; batch < 12; batch++) {
    await admin({
      query: `WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 1000)
        INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt")
        SELECT 'bulk-${batch}-' || n, 'p0', 'bulk', n, 0, hex(randomblob(1024)), NULL FROM seq`,
    })
  }
  const [{ bytes, rows }] = (
    await admin({
      query: 'SELECT COUNT(*) AS rows, SUM(LENGTH(meta)) AS bytes FROM task',
    })
  ).rows
  assert.ok(Number(bytes) > 20_000_000, `namespace holds ${bytes} bytes of task meta`)
  assertions++

  // the scoped named query: myProjects resolves server-side against the
  // authenticated claims (ownerId = userID). expected rows from the oracle.
  const expected = (
    await admin({ query: `SELECT id FROM project WHERE "ownerId" = 'u1' ORDER BY id` })
  ).rows.map((row) => row.id)
  assert.ok(expected.length > 0, 'fixture seeds projects owned by u1')
  assertions++

  const started = performance.now()
  const response = await fetch(`${origin}/pull`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-u1',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientID: 'large-client',
      clientGroupID: 'large-group',
      cookie: null,
      queries: {
        version: 1,
        patch: [{ op: 'put', hash: 'q-mine', name: 'myProjects', args: [] }],
      },
    }),
  })
  const elapsedMs = performance.now() - started
  equal(response.status, 200, 'scoped pull over a large namespace succeeds')
  const text = await response.text()
  const body = JSON.parse(text)

  // bytes proportional to the query result, not the namespace: the response
  // must be under 64 KB while the namespace holds > 20 MB.
  assert.ok(
    text.length < 65_536,
    `scoped pull response is query-sized (${text.length} bytes)`
  )
  assertions++
  assert.ok(elapsedMs < 5_000, `scoped pull finished in ${Math.round(elapsedMs)}ms`)
  assertions++

  equal(
    body.gotQueries,
    { version: 1, patch: [{ op: 'put', hash: 'q-mine' }] },
    'scoped query is acked'
  )
  const puts = body.rowsPatch.filter((op) => op.op === 'put')
  equal(
    puts.map((op) => op.value.id).sort(),
    expected,
    'pull carries exactly the rows the scoped query selects'
  )
  equal(
    puts.every((op) => op.tableName === 'project'),
    true,
    'no bulk task row rides the scoped pull'
  )
  equal(body.rowsPatch[0], { op: 'clear' }, 'fresh client starts from clear')

  console.log(
    `large-data test passed (${assertions} assertions): ` +
      `${rows} rows / ${bytes} bytes in namespace, scoped pull ${text.length} bytes in ${Math.round(elapsedMs)}ms`
  )
} finally {
  server.kill()
}
