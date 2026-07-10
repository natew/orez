// regression: the admin-set namespace knobs (query-aware, visibility,
// retention, writer) must survive a REAL instance restart. pre-fix they were
// instance fields, so an eviction between `/admin/query-aware {enabled:true}`
// and the client's pulls silently reverted the namespace to baseline mode and
// every query-aware pull deadlocked on {cookie, unchanged:true} while the
// client kept re-sending its desired-query patch (the intermittent rust-cf
// query-diff timeout of 2026-07-09). workerd is killed and restarted on the
// same persist dir: in-memory DO state is lost, durable storage survives —
// exactly what a CF eviction does.
import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

const adminKey = 'local-admin'
const port = 9_600 + Math.floor(Math.random() * 300)
const persist = mkdtempSync(join(tmpdir(), 'sync-cf-restart-'))
const baseURL = `http://127.0.0.1:${port}`
const namespace = `restart-${crypto.randomUUID()}`
const origin = `${baseURL}/${namespace}`

function startWorkerd() {
  return Bun.spawn(
    [
      'bunx',
      'wrangler',
      'dev',
      '--config',
      'wrangler.toml',
      '--local',
      '--persist-to',
      persist,
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
}

async function waitReady() {
  for (let attempt = 0; ; attempt++) {
    try {
      if ((await fetch(baseURL)).ok) return
    } catch {}
    if (attempt >= 300) throw new Error('workerd did not become ready')
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

const admin = async (path, body) => {
  const response = await fetch(`${origin}${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: {
      'x-admin-key': adminKey,
      ...(body === undefined ? {} : { 'content-type': 'application/json' }),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  assert.equal(response.status, 200, `admin ${path}`)
  return response.json()
}

const queryPull = () =>
  fetch(`${origin}/pull`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-user-a',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientID: 'restart-client',
      clientGroupID: 'restart-group',
      cookie: null,
      queries: {
        version: 1,
        patch: [
          {
            op: 'put',
            hash: 'tasks-p1-p4',
            name: 'tasksInProjects',
            args: [{ projectIds: ['p1', 'p4'] }],
          },
        ],
      },
    }),
  }).then(async (response) => ({ status: response.status, body: await response.json() }))

let server = startWorkerd()
try {
  await waitReady()

  await admin('/admin/query-aware', { enabled: true })
  await admin('/admin/retention', { retainChanges: 512 })

  // sanity: query-aware works before the restart
  const before = await queryPull()
  assert.equal(before.status, 200, 'pre-restart pull status')
  assert.deepStrictEqual(
    before.body.gotQueries,
    { version: 1, patch: [{ op: 'put', hash: 'tasks-p1-p4' }] },
    'pre-restart pull acknowledges the named query'
  )

  // the real eviction: kill workerd, restart on the same durable storage
  server.kill()
  await server.exited
  server = startWorkerd()
  await waitReady()

  const after = await queryPull()
  assert.equal(after.status, 200, 'post-restart pull status')
  assert.notEqual(
    after.body.unchanged,
    true,
    'post-restart pull must not fall back to baseline unchanged'
  )
  assert.deepStrictEqual(
    after.body.gotQueries,
    { version: 1, patch: [{ op: 'put', hash: 'tasks-p1-p4' }] },
    'query-aware override survives an instance restart'
  )

  console.log('restart-test: PASS (admin knobs survive a real workerd restart)')
} finally {
  server.kill()
  await server.exited.catch(() => {})
  rmSync(persist, { recursive: true, force: true })
}
