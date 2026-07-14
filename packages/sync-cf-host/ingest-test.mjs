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

  // An upstream namespace must not become a permanent polling timer after its
  // client leaves. Pulls ingest synchronously and therefore do not need an
  // alarm. A wake socket arms the safety poll; after that socket closes, the
  // next alarm observes zero consumers and expires without rescheduling.
  const idleNamespace = `idle-alarm-${crypto.randomUUID()}`
  const idleOrigin = `${base}/${idleNamespace}`
  const idlePull = await fetch(`${idleOrigin}/pull`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-user-a',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientID: 'idle-reader',
      clientGroupID: 'idle-group',
      cookie: null,
    }),
  })
  assert.equal(idlePull.status, 200)
  const idleStatus = async () => {
    const response = await fetch(`${idleOrigin}/admin/status`, {
      headers: { 'x-admin-key': 'ingest-harness-admin' },
    })
    assert.equal(response.status, 200)
    return response.json()
  }
  assert.equal((await idleStatus()).upstreamAlarmAt, null)

  const wakeSocket = new WebSocket(
    `${idleOrigin.replace('http://', 'ws://')}/wake?clientID=idle-reader`
  )
  await new Promise((resolve, reject) => {
    wakeSocket.addEventListener('open', resolve, { once: true })
    wakeSocket.addEventListener('error', reject, { once: true })
  })
  await Bun.sleep(50)
  const activeWake = await idleStatus()
  assert.equal(activeWake.connectedWakeSockets, 1)
  assert.equal(typeof activeWake.upstreamAlarmAt, 'number')
  wakeSocket.close()
  await new Promise((resolve) =>
    wakeSocket.addEventListener('close', resolve, { once: true })
  )
  await Bun.sleep(1_250)
  const stoppedWake = await idleStatus()
  assert.equal(stoppedWake.connectedWakeSockets, 0)
  assert.equal(stoppedWake.upstreamAlarmAt, null)

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

  // First-push provisioning and delegated endpoint construction must remain
  // namespace-local under contention. Boot several namespaces concurrently,
  // push a unique row through APP into each DATA object, then prove no engine
  // observes a peer namespace's row. This is the host-side boundary for the
  // production failure class where an app permission read reached singleton.
  const isolatedNamespaces = Array.from(
    { length: 8 },
    (_, index) => `isolated-${index}-${crypto.randomUUID()}`
  )
  const isolatedPushes = await Promise.all(
    isolatedNamespaces.map((isolatedNamespace, index) =>
      fetch(`${base}/${isolatedNamespace}/push`, {
        method: 'POST',
        headers: {
          authorization: 'Bearer token-user-a',
          'content-type': 'application/json',
        },
        body: JSON.stringify({
          clientGroupID: `isolated-group-${index}`,
          pushVersion: 1,
          mutations: [
            {
              type: 'custom',
              clientID: `isolated-writer-${index}`,
              id: 1,
              name: 'item.insert',
              args: [
                {
                  id: `isolated-row-${index}`,
                  label: `namespace ${index}`,
                  rank: index,
                  done: false,
                  meta: { isolatedNamespace },
                },
              ],
            },
          ],
        }),
      })
    )
  )
  assert.deepEqual(
    isolatedPushes.map((response) => response.status),
    isolatedNamespaces.map(() => 200)
  )
  const isolatedPulls = await Promise.all(
    isolatedNamespaces.map((isolatedNamespace, index) =>
      fetch(`${base}/${isolatedNamespace}/pull`, {
        method: 'POST',
        headers: {
          authorization: 'Bearer token-user-a',
          'content-type': 'application/json',
        },
        body: JSON.stringify({
          clientID: `isolated-reader-${index}`,
          clientGroupID: `isolated-reader-group-${index}`,
          cookie: null,
        }),
      }).then(async (response) => ({
        status: response.status,
        body: await response.json(),
      }))
    )
  )
  for (const [index, response] of isolatedPulls.entries()) {
    assert.equal(response.status, 200)
    const rowIDs = response.body.rowsPatch
      .filter((entry) => entry.op === 'put' && entry.tableName === 'item')
      .map((entry) => entry.value.id)
    assert.deepEqual(rowIDs, [`isolated-row-${index}`])
  }

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

  // A structured application-level PushFailed is a valid delegated response,
  // not a malformed success. It intentionally has no per-mutation results:
  // preserve it for the Zero client and leave the host LMID unchanged.
  await fetch(`${base}/delegation-control`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ pushFailed: 1 }),
  })
  const structuredFailure = await post('/push', {
    ...pushBody,
    mutations: [{ ...pushBody.mutations[0], id: 2 }],
  })
  assert.equal(structuredFailure.status, 200)
  assert.deepEqual(structuredFailure.body.pushResponse, {
    kind: 'PushFailed',
    origin: 'server',
    reason: 'database',
    mutationIDs: [{ clientID: 'writer', id: 2 }],
    message: 'synthetic mutation result persistence failure',
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

  // an operator re-snapshot repairs a corrupt derived application row from
  // the authoritative DATA snapshot even when the normal change cursor is
  // already caught up. internal client/LMID state must survive the rebuild.
  const corruptDerived = await fetch(`${origin}/admin/sql`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-admin-key': 'ingest-harness-admin',
    },
    body: JSON.stringify({
      query: "UPDATE item SET label = 'corrupt-derived' WHERE id = 'up-1'",
    }),
  })
  assert.equal(corruptDerived.status, 200)
  const beforeResnapshot = await fetch(`${origin}/admin/status`, {
    headers: { 'x-admin-key': 'ingest-harness-admin' },
  }).then((response) => response.json())
  const holdSnapshot = await fetch(`${base}/snapshot-control/${namespace}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ hold: true }),
  })
  assert.equal(holdSnapshot.status, 200)
  const resnapshotStartedAt = performance.now()
  const resnapshotPending = fetch(`${origin}/admin/resnapshot`, {
    method: 'POST',
    headers: {
      'x-admin-key': 'ingest-harness-admin',
    },
    signal: AbortSignal.timeout(5_000),
  })
  for (let attempt = 0; ; attempt++) {
    const snapshotState = await fetch(`${base}/snapshot-control/${namespace}`).then(
      (response) => response.json()
    )
    if (snapshotState.active === true) break
    if (attempt >= 100) throw new Error('operator snapshot fetch did not start')
    await Bun.sleep(10)
  }
  const concurrentUpstreamPush = await fetch(`${upstream}/api/zero/push`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      clientGroupID: 'upstream',
      mutations: [
        {
          type: 'custom',
          clientID: 'seed',
          id: 3,
          name: 'item.insert',
          args: [
            {
              id: 'during-resnapshot',
              label: 'arrived while snapshot was held',
              rank: 3,
              done: false,
              meta: null,
            },
          ],
        },
      ],
    }),
  })
  assert.equal(concurrentUpstreamPush.status, 200)
  const concurrentPullPending = fetch(`${origin}/pull`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-user-a',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientID: 'reader',
      clientGroupID: 'group',
      cookie: pulled.body.cookie,
    }),
    signal: AbortSignal.timeout(5_000),
  })
  const releaseSnapshot = await fetch(`${base}/snapshot-control/${namespace}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ hold: false }),
  })
  assert.equal(releaseSnapshot.status, 200)
  const [resnapshot, concurrentPull] = await Promise.all([
    resnapshotPending,
    concurrentPullPending,
  ])
  assert.ok(performance.now() - resnapshotStartedAt < 5_000)
  assert.equal(resnapshot.status, 200)
  const resnapshotBody = await resnapshot.json()
  assert.equal(resnapshotBody.ok, true)
  assert.equal(
    resnapshotBody.beforeUpstreamWatermark,
    beforeResnapshot.engine.upstreamWatermark
  )
  assert.equal(
    Number(resnapshotBody.afterUpstreamWatermark) >
      Number(beforeResnapshot.engine.upstreamWatermark),
    true
  )
  assert.ok(resnapshotBody.applied >= 3)
  assert.equal(concurrentPull.status, 200)
  const concurrentPullBody = await concurrentPull.json()
  assert.ok(
    concurrentPullBody.rowsPatch.some(
      (entry) => entry.op === 'put' && entry.value?.id === 'during-resnapshot'
    )
  )
  const repairedDerived = await fetch(`${origin}/admin/sql`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-admin-key': 'ingest-harness-admin',
    },
    body: JSON.stringify({
      query:
        "SELECT id, label, (SELECT lastMutationID FROM _zsync_clients WHERE clientGroupID = 'group' AND clientID = 'writer') AS lastMutationID FROM item WHERE id IN ('up-1', 'during-resnapshot') ORDER BY id",
    }),
  }).then((response) => response.json())
  assert.deepEqual(repairedDerived.rows, [
    {
      id: 'during-resnapshot',
      label: 'arrived while snapshot was held',
      lastMutationID: 1,
    },
    { id: 'up-1', label: 'delegated', lastMutationID: 1 },
  ])

  // A production project DATA namespace contains private/control tables such
  // as `user` alongside its public sync surface. The host schema deliberately
  // excludes those tables. Resnapshot must request only its declared table
  // surface instead of letting an unrelated DATA table abort the atomic repair.
  const scopedSnapshotNamespace = `scoped-snapshot-${crypto.randomUUID()}`
  const scopedSnapshotOrigin = `${base}/${scopedSnapshotNamespace}`
  const scopedInitial = await fetch(`${scopedSnapshotOrigin}/pull`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-user-a',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientID: 'scoped-reader',
      clientGroupID: 'scoped-group',
      cookie: null,
    }),
  })
  assert.equal(scopedInitial.status, 200)
  const enablePrivateSnapshot = await fetch(
    `${base}/private-snapshot-control/${scopedSnapshotNamespace}`,
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ enabled: true }),
    }
  )
  assert.equal(enablePrivateSnapshot.status, 200)
  const seedScopedSnapshot = await fetch(
    `${base}/upstream/${scopedSnapshotNamespace}/exec`,
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        sql: 'INSERT INTO item (id, label, rank, done, meta) VALUES (?, ?, ?, ?, ?)',
        params: [
          'timestamp-repair',
          'authoritative snapshot row',
          '2026-07-11 13:34:46+00',
          0,
          null,
        ],
      }),
    }
  )
  assert.equal(seedScopedSnapshot.status, 200)
  const scopedResnapshotResponse = await fetch(
    `${scopedSnapshotOrigin}/admin/resnapshot`,
    {
      method: 'POST',
      headers: { 'x-admin-key': 'ingest-harness-admin' },
    }
  )
  const scopedResnapshot = await scopedResnapshotResponse.json()
  assert.equal(
    scopedResnapshotResponse.status,
    200,
    `project-scoped resnapshot failed: ${JSON.stringify(scopedResnapshot)}`
  )
  assert.equal(scopedResnapshot.ok, true)
  const scopedRows = await fetch(`${scopedSnapshotOrigin}/admin/sql`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-admin-key': 'ingest-harness-admin',
    },
    body: JSON.stringify({
      query:
        "SELECT id, rank, EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'user') AS privateTableExists FROM item WHERE id = 'timestamp-repair'",
    }),
  }).then((response) => response.json())
  assert.deepEqual(scopedRows.rows, [
    {
      id: 'timestamp-repair',
      rank: '2026-07-11 13:34:46+00',
      privateTableExists: 0,
    },
  ])
  const scopedPull = await fetch(`${scopedSnapshotOrigin}/pull`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-user-a',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientID: 'scoped-repair-reader',
      clientGroupID: 'scoped-repair-group',
      cookie: null,
    }),
  }).then(async (response) => ({ status: response.status, body: await response.json() }))
  assert.equal(scopedPull.status, 200)
  const repairedTimestamp = scopedPull.body.rowsPatch.find(
    (entry) => entry.op === 'put' && entry.value?.id === 'timestamp-repair'
  )
  assert.equal(repairedTimestamp.value.rank, 1783776886000)

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

  // zero json columns carry JSON values rather than JSON-encoded strings on
  // the wire. prove every JSON kind, including strings that look like encoded
  // numbers/booleans/null/objects, survives Rust ingest, SQLite persistence,
  // and rowsPatch hydration without changing type.
  const jsonNamespace = `json-values-${crypto.randomUUID()}`
  const jsonOrigin = `${base}/${jsonNamespace}`
  const jsonPost = async (body) => {
    const response = await fetch(`${jsonOrigin}/pull`, {
      method: 'POST',
      headers: {
        authorization: 'Bearer token-user-a',
        'content-type': 'application/json',
      },
      body: JSON.stringify(body),
    })
    return { status: response.status, body: await response.json() }
  }
  const jsonInitial = await jsonPost({
    clientID: 'json-reader',
    clientGroupID: 'json-group',
    cookie: null,
  })
  assert.equal(jsonInitial.status, 200)
  await fetch(`${base}/json-values-control/${jsonNamespace}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ enabled: true }),
  })
  const jsonIncremental = await jsonPost({
    clientID: 'json-reader',
    clientGroupID: 'json-group',
    cookie: jsonInitial.body.cookie,
  })
  assert.equal(jsonIncremental.status, 200)
  const expectedJson = [
    { nested: { tags: ['a', 2, true] } },
    [1, 'two', null],
    '42',
    'true',
    'null',
    '{"looks":"encoded"}',
    42.5,
    true,
  ]
  const actualJson = jsonIncremental.body.rowsPatch
    .filter((entry) => entry.op === 'put' && entry.tableName === 'item')
    .sort((a, b) => a.value.id.localeCompare(b.value.id))
    .map((entry) => entry.value.meta)
  assert.deepEqual(actualJson, expectedJson)

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
