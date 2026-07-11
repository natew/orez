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

  // A push can be the first request for a brand-new namespace. The host must
  // force the DATA /changes provisioning barrier before delegating the
  // mutation to APP; otherwise APP observes a half-provisioned namespace and
  // returns the production's silent 500.
  const firstPushNamespace = `first-push-${crypto.randomUUID()}`
  const firstPushResponse = await fetch(`${base}/${firstPushNamespace}/push`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-user-a',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientGroupID: 'first-push-group',
      pushVersion: 1,
      mutations: [
        {
          type: 'custom',
          clientID: 'first-push-writer',
          id: 1,
          name: 'item.insert',
          args: [
            {
              id: 'first-push-row',
              label: 'first request is a push',
              rank: 1,
              done: false,
              meta: null,
            },
          ],
        },
      ],
    }),
  })
  assert.equal(firstPushResponse.status, 200)

  // The data DO itself must order a query that arrives just before the deploy
  // shim's migration instead of returning `no such table` immediately.
  const lateTableNamespace = `late-table-${crypto.randomUUID()}`
  const lateTableUpstream = `${base}/upstream/${lateTableNamespace}`
  const pendingRead = fetch(`${lateTableUpstream}/exec`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ sql: 'SELECT 1 FROM late_file LIMIT 1' }),
  })
  await Bun.sleep(100)
  const migration = await fetch(`${lateTableUpstream}/exec`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ sql: 'CREATE TABLE late_file (id TEXT PRIMARY KEY)' }),
  })
  assert.equal(migration.status, 200)
  assert.equal((await pendingRead).status, 200)

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
  const upstreamBudgetStatus = await fetch(`${origin}/admin/upstream-write-budget`, {
    headers: { 'x-admin-key': 'ingest-harness-admin' },
  })
  assert.equal(upstreamBudgetStatus.status, 200)
  const upstreamBudget = await upstreamBudgetStatus.json()
  assert.equal(upstreamBudget.enabled, true)
  assert.equal(upstreamBudget.budget, 150_000)
  assert.equal(upstreamBudget.windowRows, upstreamBudget.billableRows)
  assert.ok(Number.isSafeInteger(upstreamBudget.billableRows))
  assert.ok(Number.isSafeInteger(upstreamBudget.logicalRows))
  assert.ok(upstreamBudget.billableRows > upstreamBudget.logicalRows)

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

  // Production timestamp columns are Zero `number`s, but SQLite returns their
  // SQL timestamp representation as TEXT. Prove the real ingest -> incremental
  // pull path converts it to epoch milliseconds on the wire.
  const numericTextNamespace = `numeric-text-${crypto.randomUUID()}`
  const numericTextOrigin = `${base}/${numericTextNamespace}`
  const numericPost = async (body) => {
    const response = await fetch(`${numericTextOrigin}/pull`, {
      method: 'POST',
      headers: {
        authorization: 'Bearer token-user-a',
        'content-type': 'application/json',
      },
      body: JSON.stringify(body),
    })
    return { status: response.status, body: await response.json() }
  }
  const numericInitial = await numericPost({
    clientID: 'numeric-reader',
    clientGroupID: 'numeric-group',
    cookie: null,
  })
  assert.equal(numericInitial.status, 200)
  await fetch(`${base}/numeric-text-control/${numericTextNamespace}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ enabled: true }),
  })
  const numericIncremental = await numericPost({
    clientID: 'numeric-reader',
    clientGroupID: 'numeric-group',
    cookie: numericInitial.body.cookie,
  })
  assert.equal(numericIncremental.status, 200)
  const numericPut = numericIncremental.body.rowsPatch.find(
    (entry) => entry.op === 'put' && entry.value?.id === 'numeric-text'
  )
  assert.equal(numericPut.value.rank, 1783776886000)
  assert.equal(typeof numericPut.value.rank, 'number')
  const numericNativePut = numericIncremental.body.rowsPatch.find(
    (entry) => entry.op === 'put' && entry.value?.id === 'numeric-native'
  )
  assert.equal(numericNativePut.value.rank, 1783776886000)
  assert.equal(typeof numericNativePut.value.rank, 'number')

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
