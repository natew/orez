// replays both deploy orders of an additive column change against a real data
// worker and sync host, restarting one wrangler worker over persisted state
// with each side's schema version, the way a deploy swaps one side's code.
// every request in every window must succeed and the replica must converge.
// a required column added on either side must still fail every pull.
import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { findPort } from '../../src/port.ts'

const cwd = new URL('.', import.meta.url).pathname
const namespace = 'proj-rolling'

async function deployed(persistTo, versions, work) {
  const port = await findPort(0)
  const server = Bun.spawn(
    [
      'bunx',
      'wrangler',
      'dev',
      '--config',
      'wrangler.rolling-schema.toml',
      '--local',
      '--persist-to',
      persistTo,
      '--port',
      String(port),
      '--define',
      `HOST_SCHEMA:"${versions.host}"`,
      '--define',
      `DATA_SCHEMA:"${versions.data}"`,
    ],
    { cwd, stdout: 'ignore', stderr: 'inherit' }
  )
  const base = `http://127.0.0.1:${port}`
  try {
    for (let attempt = 0; ; attempt++) {
      try {
        const response = await fetch(`${base}/versions`)
        if (response.ok) {
          assert.deepEqual(await response.json(), versions)
          break
        }
      } catch {}
      if (server.exitCode !== null) throw new Error('rolling schema harness exited')
      if (attempt >= 300) throw new Error('rolling schema harness did not become ready')
      await Bun.sleep(100)
    }
    return await work(base)
  } finally {
    server.kill()
    await server.exited
  }
}

let clientGroup = 0

async function write(base, sql, params = []) {
  const response = await fetch(`${base}/write/${namespace}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ sql, params }),
  })
  const body = await response.json()
  assert.equal(response.status, 200, `write failed: ${JSON.stringify(body)}`)
}

// a fresh client group receives every row the replica holds
async function pull(base) {
  const response = await fetch(`${base}/${namespace}/pull`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      clientID: 'reader',
      clientGroupID: `group-${++clientGroup}`,
      cookie: null,
      queries: {
        version: 1,
        patch: [{ op: 'put', hash: 'q-all-items', name: 'allItems', args: [] }],
      },
    }),
  })
  const body = await response.json()
  if (response.status !== 200) return { status: response.status, body }
  const rows = Object.fromEntries(
    body.rowsPatch
      .filter((entry) => entry.op === 'put' && entry.tableName === 'item')
      .map((entry) => [entry.value.id, entry.value])
  )
  return { status: 200, rows }
}

async function pullOk(base) {
  const result = await pull(base)
  assert.equal(result.status, 200, `pull failed: ${JSON.stringify(result.body)}`)
  return result.rows
}

// a rollout window: writes interleaved with pulls, every one must succeed
async function window(base, writes) {
  for (const [sql, params] of writes) {
    await write(base, sql, params)
    await pullOk(base)
  }
  return pullOk(base)
}

const insertV1 = (id) => ['INSERT INTO item (id, label) VALUES (?, ?)', [id, id]]
const insertV2 = (id, note) => [
  'INSERT INTO item (id, label, note) VALUES (?, ?, ?)',
  [id, id, note],
]
const setNote = (id, note) => ['UPDATE item SET note = ? WHERE id = ?', [note, id]]

const roots = []
const persist = () => {
  const root = mkdtempSync(join(tmpdir(), 'orez-rolling-schema-'))
  roots.push(root)
  return root
}

try {
  // data first: the feed adds the column while the host still runs v1
  {
    const state = persist()
    await deployed(state, { host: 'v1', data: 'v1' }, (base) =>
      window(base, [insertV1('a1')])
    )
    const behind = await deployed(state, { host: 'v1', data: 'v2' }, (base) =>
      window(base, [insertV2('a2', 'written ahead'), setNote('a1', 'updated ahead')])
    )
    assert.deepEqual(Object.keys(behind).sort(), ['a1', 'a2'])
    assert.equal('note' in behind.a2, false)
    const converged = await deployed(state, { host: 'v2', data: 'v2' }, (base) =>
      window(base, [insertV2('a3', 'written after')])
    )
    // the window's values were dropped by the v1 host; the v2 host's one
    // snapshot recovers them
    assert.equal(converged.a1.note, 'updated ahead')
    assert.equal(converged.a2.note, 'written ahead')
    assert.equal(converged.a3.note, 'written after')
    console.log('data first: every request succeeded and the replica converged')

    // a required column is not additive in either direction
    await deployed(state, { host: 'v3', data: 'v2' }, async (base) => {
      await write(base, ...insertV2('a4', 'host requires weight'))
      const result = await pull(base)
      assert.equal(result.status, 409, JSON.stringify(result.body))
      assert.match(result.body.error, /missing required column weight/)
    })
    await deployed(state, { host: 'v2', data: 'v3' }, async (base) => {
      await write(base, 'INSERT INTO item (id, label, weight) VALUES (?, ?, ?)', [
        'a5',
        'a5',
        7,
      ])
      const result = await pull(base)
      assert.equal(result.status, 409, JSON.stringify(result.body))
      assert.match(result.body.error, /unknown column item\.weight/)
    })
    console.log('required column: every pull failed with a schema refresh')
  }

  // sync first: the host models the column before the feed has it
  {
    const state = persist()
    await deployed(state, { host: 'v1', data: 'v1' }, (base) =>
      window(base, [insertV1('b1')])
    )
    const ahead = await deployed(state, { host: 'v2', data: 'v1' }, (base) =>
      window(base, [insertV1('b2')])
    )
    assert.deepEqual(Object.keys(ahead).sort(), ['b1', 'b2'])
    assert.equal(ahead.b2.note, null)
    const converged = await deployed(state, { host: 'v2', data: 'v2' }, (base) =>
      window(base, [insertV2('b3', 'written after'), setNote('b1', 'updated after')])
    )
    assert.equal(converged.b1.note, 'updated after')
    assert.equal(converged.b2.note, null)
    assert.equal(converged.b3.note, 'written after')
    console.log('sync first: every request succeeded and the replica converged')
  }
} finally {
  for (const root of roots) rmSync(root, { recursive: true, force: true })
}
