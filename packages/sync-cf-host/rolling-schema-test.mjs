// replays both deploy orders of an additive column change against a real data
// worker and sync host, restarting one wrangler worker over persisted state
// with each side's schema version, the way a deploy swaps one side's code.
// every request in every window must succeed and the replica must converge.
// a required column added on either side must still fail every pull.
import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import {
  Zero,
  createBuilder,
  createSchema,
  defineQueries,
  defineQuery,
  number,
  string,
  table,
} from '@rocicorp/zero'

import { findPort } from '../../src/port.ts'
import { installHttpPullTransport } from '../orez-lite/src/client/transport.ts'

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

async function write(base, sql, params = [], project = namespace) {
  const response = await fetch(`${base}/write/${project}`, {
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

  // restore a previously private table after the replica consumed its last
  // mutation. its initial rows must appear without a later source mutation,
  // while an existing item subscriber keeps its cookie and complete rows.
  {
    const state = persist()
    let ordinaryCookie = null
    const ordinaryPull = async (base) => {
      const body = {
        clientID: 'ordinary',
        clientGroupID: 'ordinary',
        cookie: ordinaryCookie,
      }
      if (ordinaryCookie === null)
        body.queries = {
          version: 1,
          patch: [{ op: 'put', hash: 'ordinary', name: 'allItems', args: [] }],
        }
      const response = await fetch(base + '/' + namespace + '/pull', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      })
      const result = await response.json()
      assert.equal(response.status, 200, JSON.stringify(result))
      ordinaryCookie = result.cookie
      return result
    }
    await deployed(state, { host: 'v1', data: 'v1' }, async (base) => {
      await write(base, ...insertV1('existing'))
      await write(base, 'INSERT INTO counter VALUES (?, ?, ?)', ['a', 42, 'reader'])
      for (let value = 1; value <= 8; value++)
        await write(base, 'UPDATE item SET label = ? WHERE id = ?', [
          String(value),
          'existing',
        ])
      const original = await ordinaryPull(base)
      assert.equal(original.rowsPatch.filter((p) => p.op === 'put').length, 1)
      assert.equal(original.rowsPatch.find((p) => p.op === 'put').value.label, '8')
    })
    await deployed(state, { host: 'v4', data: 'v1' }, async (base) => {
      const existing = await ordinaryPull(base)
      assert.equal((existing.rowsPatch ?? []).length, 0)
      const response = await fetch(base + '/' + namespace + '/pull', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          clientID: 'ahead',
          clientGroupID: 'ahead',
          cookie: null,
          queries: {
            version: 1,
            patch: [{ op: 'put', hash: 'ahead', name: 'ownCounters', args: [] }],
          },
        }),
      })
      const result = await response.json()
      assert.equal(response.status, 200, JSON.stringify(result))
      assert.equal(result.rowsPatch.filter((p) => p.op === 'put').length, 0)
    })
    const counter = table('counter')
      .columns({ id: string(), value: number(), owner: string() })
      .primaryKey('id')
    const clientSchema = createSchema({ tables: [counter] })
    const builder = createBuilder(clientSchema)
    const clientQueries = defineQueries({
      ownCounters: defineQuery(() => builder.counter.where('owner', 'reader')),
    })
    let lastRows = []
    await deployed(state, { host: 'v4', data: 'v4' }, async (base) => {
      const origin = base + '/' + namespace
      const transport = installHttpPullTransport({ origin })
      const clients = [0, 1].map(
        (i) =>
          new Zero({
            server: origin,
            userID: 'reader',
            auth: 'reader',
            schema: clientSchema,
            kvStore: 'mem',
            storageKey: 'table-bootstrap-' + i + '-' + crypto.randomUUID(),
          })
      )
      const views = clients.map((client) =>
        client.materialize(clientQueries.ownCounters())
      )
      const observed = views.map(() => [])
      const ready = views.map(
        (view, i) =>
          new Promise((resolve, reject) => {
            const timer = setTimeout(
              () => reject(new Error('table bootstrap initial client timed out')),
              10000
            )
            view.addListener((data, type) => {
              observed[i] = data
              if (type === 'complete') {
                clearTimeout(timer)
                resolve(data)
              }
            })
          })
      )
      try {
        const initial = await Promise.all(ready)
        assert.deepEqual(
          initial.map((rows) => rows.map((row) => [row.id, row.value])),
          [[['a', 42]], [['a', 42]]]
        )
        const existing = await ordinaryPull(base)
        assert.ok(!existing.rowsPatch?.some((p) => p.op === 'clear'))
        assert.equal((existing.rowsPatch ?? []).length, 0)
        const converge = async (expected) => {
          const complete = views.map(
            (view) =>
              new Promise((resolve, reject) => {
                const timer = setTimeout(
                  () =>
                    reject(
                      new Error('clients did not converge to ' + JSON.stringify(expected))
                    ),
                  10000
                )
                const remove = view.addListener((data) => {
                  if (
                    JSON.stringify(data.map((row) => [row.id, row.value])) ===
                    JSON.stringify(expected)
                  ) {
                    clearTimeout(timer)
                    queueMicrotask(() => remove())
                    resolve()
                  }
                })
              })
          )
          await transport.pull()
          await Promise.all(complete)
          assert.deepEqual(observed[0], observed[1])
        }
        await write(base, 'UPDATE counter SET value = ? WHERE id = ?', [43, 'a'])
        await converge([['a', 43]])
        await write(base, 'UPDATE counter SET id = ? WHERE id = ?', ['renamed', 'a'])
        await converge([['renamed', 43]])
        await write(base, 'INSERT INTO counter VALUES (?, ?, ?)', [
          'temporary',
          1,
          'reader',
        ])
        await write(base, 'DELETE FROM counter WHERE id = ?', ['temporary'])
        await converge([['renamed', 43]])
        await write(base, 'UPDATE counter SET owner = ? WHERE id = ?', [
          'other',
          'renamed',
        ])
        await converge([])
        await write(base, 'UPDATE counter SET owner = ? WHERE id = ?', [
          'reader',
          'renamed',
        ])
        await converge([['renamed', 43]])
        await write(base, 'DELETE FROM counter WHERE id = ?', ['renamed'])
        await converge([])
        lastRows = observed[0]
      } finally {
        for (const view of views) view.destroy()
        await Promise.all(clients.map((client) => client.close()))
        transport.uninstall()
      }
    })
    await deployed(state, { host: 'v4', data: 'v4' }, async (base) => {
      const reconnected = await ordinaryPull(base)
      assert.ok(!reconnected.rowsPatch?.some((p) => p.op === 'clear'))
      assert.equal((reconnected.rowsPatch ?? []).length, 0)
      assert.deepEqual(lastRows, [])
      const response = await fetch(base + '/' + namespace + '/pull', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          clientID: 'final',
          clientGroupID: 'final',
          cookie: null,
          queries: {
            version: 1,
            patch: [{ op: 'put', hash: 'final', name: 'ownCounters', args: [] }],
          },
        }),
      })
      const result = await response.json()
      assert.equal(response.status, 200, JSON.stringify(result))
      assert.equal(result.rowsPatch.filter((p) => p.op === 'put').length, 0)
    })
    console.log(
      'restored table: two Zero clients received initial rows, updates, rename, coalesced create/delete, final delete and owner revocation; ordinary subscriber survived deploy and reconnect'
    )
  }

  {
    const state = persist()
    const racePull = async (base, group, cookie, query) => {
      const body = { clientID: group, clientGroupID: group, cookie }
      if (query)
        body.queries = {
          version: 1,
          patch: [{ op: 'put', hash: group, name: query, args: [] }],
        }
      const response = await fetch(base + '/proj-racing/pull', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      })
      const wire = await response.text()
      const result = JSON.parse(wire)
      assert.equal(response.status, 200, JSON.stringify(result))
      console.log(
        'bootstrap HTTP response',
        group,
        'rows',
        (result.rowsPatch ?? []).filter((p) => p.op === 'put').length,
        'bytes',
        Buffer.byteLength(wire)
      )
      return result
    }
    let cookie
    await deployed(state, { host: 'v1', data: 'v1' }, async (base) => {
      await write(
        base,
        'INSERT INTO item VALUES (?, ?)',
        ['ordinary', 'preserved'],
        'proj-racing'
      )
      await write(
        base,
        'INSERT INTO counter VALUES (?, ?, ?)',
        ['a', 42, 'reader'],
        'proj-racing'
      )
      cookie = (await racePull(base, 'ordinary-race', null, 'allItems')).cookie
    })
    await deployed(state, { host: 'v4', data: 'v4' }, async (base) => {
      const initial = await Promise.all(
        ['race-a', 'race-b'].map((group) => racePull(base, group, null, 'ownCounters'))
      )
      for (const result of initial) {
        const puts = result.rowsPatch.filter((p) => p.op === 'put')
        assert.deepEqual(
          puts.map((p) => [p.value.id, p.value.value]),
          [['raced', 43]]
        )
      }
      const existing = await racePull(base, 'ordinary-race', cookie)
      assert.equal((existing.rowsPatch ?? []).length, 0)
      const resumed = await racePull(base, 'race-a', initial[0].cookie)
      assert.equal((resumed.rowsPatch ?? []).length, 0)
    })
    console.log(
      'restored table race: captured snapshot preceded concurrent update, rename, create/delete; both observers converged and existing subscriber received no rows'
    )
  }
} finally {
  for (const root of roots) rmSync(root, { recursive: true, force: true })
}
