import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { findPort } from '../../src/port.ts'

const port = await findPort(0)
const persist = mkdtempSync(join(tmpdir(), 'sync-cf-init-guard-'))
const base = `http://127.0.0.1:${port}`
const namespace = `init-guard-${crypto.randomUUID()}`
let origin = `${base}/${namespace}`
function start() {
  return Bun.spawn(
    [
      'bunx',
      'wrangler',
      'dev',
      '--config',
      'test/init-guard-wrangler.toml',
      '--local',
      '--persist-to',
      persist,
      '--var',
      'ADMIN_KEY:local-admin',
      '--port',
      String(port),
    ],
    { cwd: new URL('.', import.meta.url).pathname, stdout: 'inherit', stderr: 'inherit' }
  )
}
async function ready() {
  for (let attempt = 0; ; attempt++) {
    try {
      if ((await fetch(base)).ok) return
    } catch {}
    if (attempt >= 300) throw new Error('workerd did not become ready')
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}
async function admin(path, body) {
  const response = await fetch(`${origin}${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: {
      'x-admin-key': 'local-admin',
      ...(body === undefined ? {} : { 'content-type': 'application/json' }),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  assert.equal(response.status, 200, `admin ${path}: ${await response.clone().text()}`)
  return response.json()
}
let server = start()
try {
  await ready()
  await admin('/admin/status')
  await admin('/admin/sql', {
    query: `WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<2000)
    INSERT INTO project (id, "ownerId", name) SELECT 'bulk-' || x, 'user-a', 'valid' FROM n`,
  })
  await admin('/admin/sql', {
    query: "DELETE FROM _zsync_host_control WHERE key = 'initFingerprint'",
  })
  for (const mode of ['poison', 'repair']) {
    origin = `${base}/${namespace}-${mode}`
    await admin('/admin/status')
    await admin('/admin/sql', { query: `INSERT INTO init_guard_test VALUES ('${mode}')` })
    await admin('/admin/sql', {
      query: "DELETE FROM _zsync_host_control WHERE key = 'initFingerprint'",
    })
  }
  server.kill()
  await server.exited
  server = start()
  await ready()
  origin = `${base}/${namespace}`
  const billing = await admin('/admin/sql-billing')
  const status = await admin('/admin/status')
  assert.equal(status.initSkipped, false)
  assert.ok(
    billing.rowsRead < 5000,
    `schema pass must not scan stored rows: ${billing.rowsRead} reads`
  )
  const rows = await admin('/admin/sql', {
    query: "SELECT count(*) AS count FROM project WHERE id LIKE 'bulk-%'",
  })
  assert.equal(rows.rows[0].count, 2000)
  origin = `${base}/${namespace}-poison`
  const rejected = await fetch(`${origin}/admin/status`, {
    headers: { 'x-admin-key': 'local-admin' },
  })
  assert.equal(rejected.status, 500, 'initializer cannot introduce invalid stored values')
  assert.match(await rejected.text(), /invalid number storage in task\.rank/)
  origin = `${base}/${namespace}-repair`
  const repaired = await admin('/admin/sql', {
    query: "SELECT rank FROM task WHERE id = 'bad'",
  })
  assert.equal(repaired.rows[0].rank, 1, 'a self-repairing initializer remains valid')
  console.log(`init-guard-test: PASS (${billing.rowsRead} reads for 2000 retained rows)`)
} finally {
  server.kill()
  await server.exited.catch(() => {})
  rmSync(persist, { recursive: true, force: true })
}
