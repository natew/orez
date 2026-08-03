// regression: the admin-set retention override must survive a REAL instance
// restart. workerd is killed and restarted on the same persist dir, so
// in-memory DO state is lost while durable storage survives, exactly as it
// does across a CF eviction.
import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { findPort } from '../../src/port.ts'

const adminKey = 'local-admin'
const port = await findPort(0)
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

const post = (path, body) =>
  fetch(`${origin}${path}`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-user-a',
      'content-type': 'application/json',
    },
    body: JSON.stringify(body),
  }).then(async (response) => ({ status: response.status, body: await response.json() }))

const retentionPull = () =>
  post('/pull', {
    clientID: 'restart-client',
    clientGroupID: 'restart-group',
    cookie: null,
  })

const push = (id) =>
  post('/push', {
    clientGroupID: 'restart-writer-group',
    pushVersion: 1,
    mutations: [
      {
        type: 'custom',
        clientID: 'restart-writer',
        id,
        name: 'project.create',
        args: [{ id: `restart-project-${id}`, ownerId: 'user-a', name: `project ${id}` }],
      },
    ],
  })

const storedRetention = () =>
  admin('/admin/sql', {
    query: "SELECT value FROM _zsync_host_control WHERE key = 'retainChanges'",
  })

let server = startWorkerd()
try {
  await waitReady()

  const initialBilling = await admin('/admin/sql-billing')
  assert.equal(
    initialBilling.rowsWritten > 0,
    true,
    'fresh schema initialization proves the billing meter can observe DDL writes'
  )

  await admin('/admin/retention', { retainChanges: 1 })
  assert.deepStrictEqual(
    await storedRetention(),
    { rows: [{ value: '1' }] },
    'retention override is stored before restart'
  )

  // the real eviction: kill workerd, restart on the same durable storage
  server.kill()
  await server.exited
  server = startWorkerd()
  await waitReady()

  const restartBilling = await admin('/admin/sql-billing')
  assert.equal(
    restartBilling.rowsWritten,
    0,
    'reopening an initialized object rewrites no schema or trigger rows'
  )
  const triggerRows = await admin('/admin/sql', {
    query:
      "SELECT sql FROM sqlite_schema WHERE type = 'trigger' AND name LIKE '_zsync_tr_%_v2' ORDER BY name LIMIT 1",
  })
  assert.equal(triggerRows.rows.length, 1, 'the restart probe found a packed trigger')
  const beforeTriggerReplay = await admin('/admin/sql-billing')
  await admin('/admin/sql', {
    query: triggerRows.rows[0].sql.replace(
      /^CREATE TRIGGER /,
      'CREATE TRIGGER IF NOT EXISTS '
    ),
  })
  const afterTriggerReplay = await admin('/admin/sql-billing')
  assert.equal(
    afterTriggerReplay.rowsWritten - beforeTriggerReplay.rowsWritten,
    0,
    'replaying CREATE TRIGGER IF NOT EXISTS writes no SQLite rows'
  )

  assert.deepStrictEqual(
    await storedRetention(),
    { rows: [{ value: '1' }] },
    'retention override remains stored after restart'
  )
  for (let id = 1; id <= 3; id++) {
    const response = await push(id)
    assert.equal(response.status, 200, `post-restart push ${id}`)
  }

  const pull = await retentionPull()
  assert.equal(pull.status, 200, 'post-restart pull status')
  assert.equal(
    pull.body.rowsPatch.some((entry) => entry.op === 'put'),
    false,
    'a client without desired queries receives no rows'
  )
  const status = await admin('/admin/status')
  assert.deepStrictEqual(
    { floor: status.engine.floor, watermark: status.engine.watermark },
    { floor: '2', watermark: '3' },
    'post-restart pull prunes to the persisted one-change retention window'
  )

  console.log('restart-test: PASS (retention override survives a real workerd restart)')
} finally {
  server.kill()
  await server.exited.catch(() => {})
  rmSync(persist, { recursive: true, force: true })
}
