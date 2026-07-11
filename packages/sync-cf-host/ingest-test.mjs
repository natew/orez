import assert from 'node:assert/strict'

const port = 9_000 + Math.floor(Math.random() * 500)
const server = Bun.spawn(
  [
    'bunx',
    'wrangler',
    'dev',
    '--config',
    'wrangler.ingest.toml',
    '--local',
    '--port',
    String(port),
  ],
  { cwd: new URL('.', import.meta.url).pathname, stdout: 'inherit', stderr: 'inherit' }
)
const base = `http://127.0.0.1:${port}`

try {
  for (let attempt = 0; ; attempt++) {
    try {
      if ((await fetch(base)).ok) break
    } catch {}
    if (attempt >= 200) throw new Error('ingest harness did not become ready')
    await Bun.sleep(100)
  }

  const namespace = `ingest-${crypto.randomUUID()}`
  const origin = `${base}/${namespace}`
  const upstream = `${base}/upstream/${namespace}`
  const post = async (path, body) => {
    const response = await fetch(`${origin}${path}`, {
      method: 'POST',
      headers: {
        authorization: 'Bearer token-user-a',
        'content-type': 'application/json',
      },
      body: JSON.stringify(body),
    })
    const text = await response.text()
    let parsed
    try {
      parsed = JSON.parse(text)
    } catch {
      throw new Error(`${path} returned ${response.status}: ${text}`)
    }
    return { status: response.status, body: parsed }
  }

  // Force a retention gap before the engine has a cursor. The next pull must
  // recover via the ZeroDO /snapshot endpoint, not an unavailable change row.
  const upstreamPush = await fetch(`${upstream}/api/zero/push`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      clientGroupID: 'upstream',
      mutations: [
        {
          type: 'custom',
          clientID: 'seed',
          id: 1,
          name: 'item.insert',
          args: [
            { id: 'snapshot-1', label: 'snapshot', rank: 1, done: false, meta: null },
          ],
        },
        {
          type: 'custom',
          clientID: 'seed',
          id: 2,
          name: 'item.insert',
          args: [
            { id: 'snapshot-2', label: 'snapshot', rank: 2, done: true, meta: null },
          ],
        },
      ],
    }),
  })
  assert.equal(upstreamPush.status, 200)
  const prune = await fetch(`${upstream}/exec`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ sql: 'DELETE FROM _zero_changes WHERE watermark = 1' }),
  })
  assert.equal(prune.status, 200)

  const initial = await post('/pull', {
    clientID: 'reader',
    clientGroupID: 'group',
    cookie: null,
  })
  assert.equal(initial.status, 200)
  assert.equal(
    initial.body.rowsPatch.filter(
      (entry) => entry.op === 'put' && entry.tableName === 'item'
    ).length,
    2
  )

  const pushBody = {
    clientGroupID: 'group',
    pushVersion: 1,
    mutations: [
      {
        type: 'custom',
        clientID: 'writer',
        id: 1,
        name: 'item.insert',
        args: [
          {
            id: 'up-1',
            label: 'delegated',
            rank: 7.5,
            done: false,
            meta: { lane: true },
          },
        ],
      },
    ],
  }
  await fetch(`${base}/delegation-control`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ failures: 2 }),
  })
  const pushed = await post('/push', pushBody)
  assert.equal(pushed.status, 200)
  assert.deepEqual(pushed.body.pushResponse.mutations, [
    { id: { clientID: 'writer', id: 1 }, result: {} },
  ])
  const retriedDelegation = await fetch(`${base}/delegation-control`).then((response) =>
    response.json()
  )
  assert.equal(retriedDelegation.delegatedAttempts, 3)

  // A persistently failing endpoint receives exactly maxAttempts requests;
  // the host returns the terminal response instead of spinning a hot loop.
  await fetch(`${base}/delegation-control`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ failures: 10 }),
  })
  const exhaustedDelegation = await fetch(`${origin}/push`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-user-a',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      ...pushBody,
      mutations: [{ ...pushBody.mutations[0], id: 2 }],
    }),
  })
  assert.equal(exhaustedDelegation.status, 503)
  const boundedDelegation = await fetch(`${base}/delegation-control`).then((response) =>
    response.json()
  )
  assert.equal(boundedDelegation.delegatedAttempts, 3)
  await fetch(`${base}/delegation-control`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ failures: 0 }),
  })

  const pulled = await post('/pull', {
    clientID: 'reader',
    clientGroupID: 'group',
    cookie: initial.body.cookie,
  })
  assert.equal(pulled.status, 200)
  assert.equal(pulled.body.lastMutationIDChanges.writer, 1)
  const put = pulled.body.rowsPatch.find(
    (entry) =>
      entry.op === 'put' && entry.tableName === 'item' && entry.value.id === 'up-1'
  )
  assert.deepEqual(put.value, {
    id: 'up-1',
    label: 'delegated',
    rank: 7.5,
    done: false,
    meta: { lane: true },
  })

  // A feed that keeps returning changes while the engine cursor no longer
  // advances must trip the ingest breaker instead of hot-looping. Once the
  // bad feed is removed and an admin reopens the circuit, normal pulls recover.
  const runawayNamespace = `runaway-${crypto.randomUUID()}`
  const runawayOrigin = `${base}/${runawayNamespace}`
  const runawayUpstream = `${base}/upstream/${runawayNamespace}`
  const seededRunaway = await fetch(`${runawayUpstream}/api/zero/push`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      clientGroupID: 'upstream',
      mutations: [
        {
          type: 'custom',
          clientID: 'seed',
          id: 1,
          name: 'item.insert',
          args: [
            {
              id: 'runaway-replay',
              label: 'replayed without cursor progress',
              rank: 1,
              done: false,
              meta: null,
            },
          ],
        },
      ],
    }),
  })
  assert.equal(seededRunaway.status, 200)
  await fetch(`${base}/runaway-control/${runawayNamespace}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ enabled: true }),
  })
  const runawayPull = await fetch(`${runawayOrigin}/pull`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-user-a',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientID: 'runaway-reader',
      clientGroupID: 'runaway-group',
      cookie: null,
    }),
  })
  assert.equal(runawayPull.status, 429)
  assert.equal((await runawayPull.json()).error, 'ingestCursorStalled')

  await fetch(`${base}/runaway-control/${runawayNamespace}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ enabled: false }),
  })
  const reopened = await fetch(`${runawayOrigin}/admin/ingest-breaker`, {
    method: 'POST',
    headers: { 'x-admin-key': 'ingest-harness-admin' },
  })
  assert.equal(reopened.status, 200)
  assert.equal((await reopened.json()).tripped, false)
  const recoveredPull = await fetch(`${runawayOrigin}/pull`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-user-a',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientID: 'runaway-reader',
      clientGroupID: 'runaway-group',
      cookie: null,
    }),
  })
  assert.equal(recoveredPull.status, 200)
  console.log('upstream ingest delegated-push harness: PASS')
} finally {
  server.kill()
  await server.exited
}
