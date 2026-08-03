import assert from 'node:assert/strict'

import { findPort } from '../../src/port.ts'

const externalURL = process.env.M3_BASE_URL?.replace(/\/$/, '')
const adminKey = process.env.M3_ADMIN_KEY ?? 'local-admin'
const port = await findPort(0)
const server = externalURL
  ? undefined
  : Bun.spawn(
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
const baseURL = externalURL ?? `http://127.0.0.1:${port}`

if (server) {
  for (let attempt = 0; ; attempt++) {
    try {
      if ((await fetch(baseURL)).ok) break
    } catch {}
    if (attempt >= 150) throw new Error('production workerd did not become ready')
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

let assertions = 0
const equal = (actual, expected, message) => {
  assert.deepStrictEqual(actual, expected, message)
  assertions++
}
const namespace = `production-${crypto.randomUUID()}`
const origin = `${baseURL}/${namespace}`

const post = async (path, body, userID = 'user-a') => {
  const response = await fetch(`${origin}${path}`, {
    method: 'POST',
    headers: {
      authorization: `Bearer token-${userID}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify(body),
  })
  return {
    status: response.status,
    headers: response.headers,
    body: await response.json(),
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
  assert.equal(
    response.ok,
    true,
    `${path}: ${response.status} ${await response.clone().text()}`
  )
  return response.json()
}

const mutation = (clientID, id, name, args) => ({
  clientGroupID: `group-${clientID}`,
  pushVersion: 1,
  mutations: [{ type: 'custom', clientID, id, name, args: [args] }],
})

async function mintWakeToken(userID) {
  const response = await fetch(`${origin}/auth/wake-token`, {
    method: 'POST',
    headers: { authorization: `Bearer token-${userID}` },
  })
  equal(response.status, 200, `wake token mint status for ${userID}`)
  return (await response.json()).token
}

async function openWake(clientID, userID) {
  const wakeToken = await mintWakeToken(userID)
  const url =
    `${origin.replace('http:', 'ws:')}/wake?clientID=${encodeURIComponent(clientID)}` +
    `&wakeToken=${encodeURIComponent(wakeToken)}`
  const socket = new WebSocket(url)
  const messages = []
  socket.addEventListener('message', (event) => messages.push(String(event.data)))
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`wake socket ${clientID} did not open`)),
      5_000
    )
    socket.addEventListener('open', () => {
      clearTimeout(timer)
      resolve({ socket, messages })
    })
    socket.addEventListener('error', reject)
  })
}

async function eventually(check, timeoutMs, label) {
  const started = Date.now()
  let lastError
  while (Date.now() - started < timeoutMs) {
    try {
      check()
      return
    } catch (error) {
      lastError = error
      await new Promise((resolve) => setTimeout(resolve, 10))
    }
  }
  throw new Error(`${label}: ${lastError}`)
}

try {
  const unauthenticatedMint = await fetch(`${origin}/auth/wake-token`, {
    method: 'POST',
  })
  equal(unauthenticatedMint.status, 401, 'wake token mint requires authentication')

  const unauthenticatedWake = await fetch(`${origin}/wake?clientID=attacker`)
  equal(unauthenticatedWake.status, 401, 'wake rejects missing capability')

  const unauthenticatedNotify = await fetch(`${origin}/notify`, { method: 'POST' })
  equal(unauthenticatedNotify.status, 403, 'notify rejects missing service capability')

  const authorizedNotify = await fetch(`${origin}/notify`, {
    method: 'POST',
    headers: { 'x-admin-key': adminKey },
  })
  equal(authorizedNotify.status, 200, 'admin capability permits notify')
  equal(
    await authorizedNotify.json(),
    { ok: true, applied: 0 },
    'authorized notify response'
  )

  const rawBegin = await fetch(`${origin}/admin/sql`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-admin-key': adminKey },
    body: JSON.stringify({ query: 'BEGIN' }),
  })
  equal(rawBegin.status, 400, 'one-shot admin SQL rejects raw BEGIN')
  equal(
    await rawBegin.json(),
    { error: 'transaction SQL is host-owned and forbidden' },
    'raw BEGIN rejection is explicit'
  )

  const boundAdminValues = await admin('/admin/sql', {
    query:
      'SELECT ? AS integerValue, ? AS realValue, ? AS textValue, ? AS nullValue, hex(?) AS blobValue',
    params: [
      { kind: 'integer', value: '42' },
      { kind: 'real', value: 1.5 },
      { kind: 'text', value: 'bound' },
      { kind: 'null' },
      { kind: 'blob', value: [0, 255] },
    ],
  })
  equal(
    boundAdminValues.rows,
    [
      {
        integerValue: 42,
        realValue: 1.5,
        textValue: 'bound',
        nullValue: null,
        blobValue: '00FF',
      },
    ],
    'admin SQL binds the typed parameter envelope'
  )

  const ambiguousAdminParams = await fetch(`${origin}/admin/sql`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-admin-key': adminKey },
    body: JSON.stringify({ query: 'SELECT ?', params: [1] }),
  })
  equal(ambiguousAdminParams.status, 400, 'admin SQL rejects ambiguous raw params')
  assert.match(
    (await ambiguousAdminParams.json()).error,
    /^invalid params:/,
    'ambiguous param rejection is diagnostic'
  )
  assertions++

  const firstPull = await post('/pull', {
    clientID: 'client-a',
    clientGroupID: 'group-client-a',
    cookie: null,
    queries: {
      version: 1,
      patch: [
        {
          op: 'put',
          hash: 'all-projects',
          name: 'allProjects',
          args: [],
        },
        {
          op: 'put',
          hash: 'all-tasks',
          name: 'tasksInProjects',
          args: [
            {
              projectIds: [
                'p0',
                'p1',
                'p2',
                'p3',
                'p4',
                'p5',
                'p6',
                'p7',
                'p8',
                'p9',
                'p10',
                'p11',
              ],
            },
          ],
        },
      ],
    },
  })
  equal(firstPull.status, 200, 'initial pull status')
  equal(firstPull.body.cookie, 0, 'initial cookie')
  equal(
    firstPull.body.gotQueries,
    {
      version: 1,
      patch: [
        { op: 'put', hash: 'all-projects' },
        { op: 'put', hash: 'all-tasks' },
      ],
    },
    'initial pull acknowledges every named query'
  )
  assert.ok(
    firstPull.body.rowsPatch.length > 60,
    'initial named queries include fixture rows'
  )
  assertions++

  const queryPull = await post('/pull', {
    clientID: 'query-client',
    clientGroupID: 'query-group',
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
  })
  equal(queryPull.status, 200, 'scoped query pull status')
  equal(
    queryPull.body.gotQueries,
    {
      version: 1,
      patch: [{ op: 'put', hash: 'tasks-p1-p4' }],
    },
    'server resolves and acknowledges named query'
  )
  const queryTaskPuts = queryPull.body.rowsPatch.filter(
    (entry) => entry.op === 'put' && entry.tableName === 'task'
  )
  assert.ok(queryTaskPuts.length > 0, 'scoped query pull includes members')
  assert.ok(
    queryTaskPuts.every((entry) => ['p1', 'p4'].includes(entry.value.projectId)),
    'scoped query pull excludes non-members'
  )
  const queryStatus = await admin('/admin/status')
  assert.ok(
    queryStatus.counters.queryRecompilations >= 1,
    'query recompilation counter increments for named query puts'
  )
  assert.ok(
    Number.isSafeInteger(queryStatus.wasmMemoryBytes) && queryStatus.wasmMemoryBytes > 0,
    'authenticated status reports wasm linear-memory bytes'
  )
  assert.ok(
    queryStatus.heapUsedBytes === null || Number.isSafeInteger(queryStatus.heapUsedBytes),
    'authenticated status reports js heap bytes when workerd exposes them'
  )
  assertions += 4
  const storedTransform = await admin('/admin/sql', {
    query:
      "SELECT transformVersion FROM _zsync_queries WHERE clientGroupID = 'query-group' AND hash = 'tasks-p1-p4'",
  })
  equal(
    storedTransform.rows,
    [{ transformVersion: 1 }],
    'query transform version is server-authored'
  )

  const queryFollowup = await post('/pull', {
    clientID: 'query-client',
    clientGroupID: 'query-group',
    cookie: queryPull.body.cookie,
  })
  equal(queryFollowup.status, 200, 'query pull without query patch status')
  equal(
    queryFollowup.body.unchanged,
    true,
    'registered query persists without queries field'
  )

  equal(
    (
      await post(
        '/push',
        mutation('claims-project-a', 1, 'project.create', {
          id: 'claims-project-a',
          ownerId: 'user-a',
          name: 'user a project',
        }),
        'user-a'
      )
    ).status,
    200,
    'user a scoped-query fixture push'
  )
  equal(
    (
      await post(
        '/push',
        mutation('claims-project-b', 1, 'project.create', {
          id: 'claims-project-b',
          ownerId: 'user-b',
          name: 'user b project',
        }),
        'user-b'
      )
    ).status,
    200,
    'user b scoped-query fixture push'
  )
  const userAProjects = await post(
    '/pull',
    {
      clientID: 'claims-query-a',
      clientGroupID: 'claims-query-group-a',
      cookie: null,
      queries: {
        version: 1,
        patch: [{ op: 'put', hash: 'my-projects-a', name: 'myProjects', args: [] }],
      },
    },
    'user-a'
  )
  const userBProjects = await post(
    '/pull',
    {
      clientID: 'claims-query-b',
      clientGroupID: 'claims-query-group-b',
      cookie: null,
      queries: {
        version: 1,
        patch: [{ op: 'put', hash: 'my-projects-b', name: 'myProjects', args: [] }],
      },
    },
    'user-b'
  )
  equal(userAProjects.status, 200, 'user a scoped-query pull status')
  equal(userBProjects.status, 200, 'user b scoped-query pull status')
  equal(
    userAProjects.body.gotQueries,
    { version: 1, patch: [{ op: 'put', hash: 'my-projects-a' }] },
    'user a scoped query is acknowledged'
  )
  equal(
    userBProjects.body.gotQueries,
    { version: 1, patch: [{ op: 'put', hash: 'my-projects-b' }] },
    'user b scoped query is acknowledged'
  )
  equal(
    userAProjects.body.rowsPatch
      .filter((entry) => entry.op === 'put' && entry.tableName === 'project')
      .map((entry) => [entry.value.id, entry.value.ownerId]),
    [['claims-project-a', 'user-a']],
    'user a receives only projects selected by claims'
  )
  equal(
    userBProjects.body.rowsPatch
      .filter((entry) => entry.op === 'put' && entry.tableName === 'project')
      .map((entry) => [entry.value.id, entry.value.ownerId]),
    [['claims-project-b', 'user-b']],
    'user b receives only projects selected by claims'
  )

  // An unknown query fails its own pull and names itself, so one bad query in a
  // batch is still attributable.
  const unknownQueryPull = await post('/pull', {
    clientID: 'unknown-query-client',
    clientGroupID: 'unknown-query-group',
    cookie: null,
    queries: {
      version: 1,
      patch: [
        {
          op: 'put',
          hash: 'known',
          name: 'tasksInProjects',
          args: [{ projectIds: ['p1'] }],
        },
        { op: 'put', hash: 'unknown', name: 'noSuchQuery', args: [{}] },
      ],
    },
  })
  equal(unknownQueryPull.status, 400, 'unknown query in a batch is a request error')
  assert.match(
    unknownQueryPull.body.error,
    /noSuchQuery/,
    'batch error names the query that failed'
  )
  assertions++

  const injected = await post('/pull', {
    clientID: 'attacker',
    clientGroupID: 'attacker-group',
    cookie: null,
    queries: {
      version: 1,
      patch: [
        {
          op: 'put',
          hash: 'client-controlled',
          ast: { table: 'user' },
        },
      ],
    },
  })
  equal(injected.status, 400, 'client-authored raw AST is rejected')
  assert.match(injected.body.error, /server-resolved named query/)
  assertions++

  const malformedQuery = await post('/pull', {
    clientID: 'malformed-query',
    clientGroupID: 'malformed-group',
    cookie: null,
    queries: {
      version: -1,
      patch: [
        {
          op: 'put',
          hash: 'bad-version',
          name: 'tasksDone',
          args: [],
        },
      ],
    },
  })
  equal(malformedQuery.status, 400, 'query EngineError status reaches HTTP')

  for (const route of ['/pull', '/push']) {
    for (const body of ['{', 'null', '[]']) {
      const malformed = await fetch(`${origin}${route}`, {
        method: 'POST',
        headers: {
          authorization: 'Bearer token-user-a',
          'content-type': 'application/json',
        },
        body,
      })
      equal(malformed.status, 400, `${route} malformed/non-object JSON is a bad request`)
      await malformed.arrayBuffer()
    }
  }

  let writerStatus = await admin('/admin/writer')
  equal(writerStatus.writerEnabled, true, 'writer starts enabled')
  writerStatus = await admin('/admin/writer', { enabled: false })
  equal(writerStatus.writerEnabled, false, 'operator can stop writer')
  let disabledPush = await post(
    '/push',
    mutation('writer-probe', 1, 'project.create', {
      id: 'writer-disabled-row',
      ownerId: 'user-a',
      name: 'must not commit',
    })
  )
  equal(disabledPush.status, 503, 'disabled writer rejects pushes')
  let writerRows = await admin('/admin/sql', {
    query: "SELECT COUNT(*) AS n FROM project WHERE id = 'writer-disabled-row'",
  })
  equal(writerRows.rows, [{ n: 0 }], 'disabled writer commits no application row')
  writerStatus = await admin('/admin/writer', { enabled: true })
  equal(writerStatus.writerEnabled, true, 'operator can restore writer')

  await admin('/admin/fault', { point: 'push_before_mutation', kind: 'error' })
  let faultResponse = await post(
    '/push',
    mutation('fault-before-client', 1, 'project.create', {
      id: 'fault-before-row',
      ownerId: 'user-a',
      name: 'must not commit',
    })
  )
  equal(faultResponse.status, 500, 'pre-mutation fault returns infra error')
  let faultRows = await admin('/admin/sql', {
    query:
      "SELECT (SELECT COUNT(*) FROM project WHERE id = 'fault-before-row') AS projectCount, " +
      "(SELECT COUNT(*) FROM _zsync_clients WHERE clientID = 'fault-before-client') AS clientCount",
  })
  equal(
    faultRows.rows,
    [{ projectCount: 0, clientCount: 0 }],
    'pre-mutation fault changes no application or LMID state'
  )

  await admin('/admin/fault', {
    point: 'push_after_write_before_commit',
    kind: 'quota',
  })
  faultResponse = await post(
    '/push',
    mutation('fault-before-commit-client', 1, 'project.create', {
      id: 'fault-before-commit-row',
      ownerId: 'user-a',
      name: 'must roll back',
    })
  )
  equal(faultResponse.status, 507, 'pre-commit quota fault returns quota error')
  faultRows = await admin('/admin/sql', {
    query:
      "SELECT (SELECT COUNT(*) FROM project WHERE id = 'fault-before-commit-row') AS projectCount, " +
      "(SELECT COUNT(*) FROM _zsync_clients WHERE clientID = 'fault-before-commit-client') AS clientCount",
  })
  equal(
    faultRows.rows,
    [{ projectCount: 0, clientCount: 0 }],
    'pre-commit quota fault rolls back application row and LMID'
  )
  // the fault must be one-shot even though it aborts the transaction: the
  // consume must not roll back with the abort (regression: every retry
  // re-fired the fault until an operator cleared it)
  faultResponse = await post(
    '/push',
    mutation('fault-before-commit-client', 1, 'project.create', {
      id: 'fault-before-commit-row',
      ownerId: 'user-a',
      name: 'must roll back',
    })
  )
  equal(faultResponse.status, 200, 'pre-commit fault is consumed on first fire')

  const afterCommitMutation = mutation('fault-after-commit-client', 1, 'project.create', {
    id: 'fault-after-commit-row',
    ownerId: 'user-a',
    name: 'committed before response fault',
  })
  await admin('/admin/fault', {
    point: 'push_after_commit_before_response',
    kind: 'error',
  })
  faultResponse = await post('/push', afterCommitMutation)
  equal(faultResponse.status, 500, 'post-commit response fault returns infra error')
  faultRows = await admin('/admin/sql', {
    query:
      "SELECT (SELECT COUNT(*) FROM project WHERE id = 'fault-after-commit-row') AS projectCount, " +
      "(SELECT CAST(lastMutationID AS TEXT) FROM _zsync_clients WHERE clientID = 'fault-after-commit-client') AS lmid",
  })
  equal(
    faultRows.rows,
    [{ projectCount: 1, lmid: '1' }],
    'post-commit response fault preserves durable row and LMID'
  )
  faultResponse = await post('/push', afterCommitMutation)
  equal(faultResponse.status, 200, 'post-commit retry is handled as a replay')

  await admin('/admin/fault', { point: 'pull_during_tx', kind: 'error' })
  faultResponse = await post('/pull', {
    clientID: 'fault-pull-during',
    clientGroupID: 'fault-pull-group-during',
    cookie: null,
  })
  equal(faultResponse.status, 500, 'in-pull transaction fault returns infra error')
  faultRows = await admin('/admin/sql', {
    query:
      "SELECT COUNT(*) AS n FROM _zsync_clients WHERE clientID = 'fault-pull-during'",
  })
  equal(faultRows.rows, [{ n: 0 }], 'in-pull fault rolls back client claim')

  await admin('/admin/fault', { point: 'pull_after_commit', kind: 'quota' })
  faultResponse = await post('/pull', {
    clientID: 'fault-pull-after',
    clientGroupID: 'fault-pull-group-after',
    cookie: null,
  })
  equal(faultResponse.status, 507, 'post-pull commit fault returns quota error')
  faultRows = await admin('/admin/sql', {
    query: "SELECT COUNT(*) AS n FROM _zsync_clients WHERE clientID = 'fault-pull-after'",
  })
  equal(faultRows.rows, [{ n: 1 }], 'post-pull fault preserves committed client claim')

  let response = await post(
    '/push',
    mutation('client-a', 1, 'project.create', {
      id: 'prod-created',
      ownerId: 'user-a',
      name: 'production host',
    })
  )
  equal(response.status, 200, 'push status')
  equal(response.body.pushResponse.mutations[0].result, {}, 'push result')

  response = await post(
    '/push',
    mutation('capture-exact', 1, 'test.exactWriteSet', { id: 'capture-exact' })
  )
  equal(response.status, 200, 'exact raw SQL write set commits')

  response = await post(
    '/push',
    mutation('capture-raw', 1, 'test.rawWrite', { id: 'capture-raw' })
  )
  equal(response.status, 200, 'metadata-less raw SQL stays on the trigger lane')

  response = await post(
    '/push',
    mutation('capture-mixed', 1, 'test.mixedCapture', { id: 'capture-mixed' })
  )
  equal(response.status, 200, 'helper and raw trigger writes share one transaction')

  response = await post(
    '/push',
    mutation('capture-wrong', 1, 'test.wrongWriteSet', { id: 'capture-wrong' })
  )
  equal(response.status, 500, 'wrong raw SQL write set fails trigger shadowing')

  response = await post(
    '/push',
    mutation('capture-query', 1, 'test.queryWrite', { id: 'capture-query' })
  )
  equal(response.status, 200, 'write issued through query stays on the trigger lane')

  let rows = await admin('/admin/sql', {
    query: "SELECT id FROM project WHERE id LIKE 'capture-%' ORDER BY id;",
  })
  equal(
    rows.rows,
    [
      { id: 'capture-exact' },
      { id: 'capture-mixed-helper' },
      { id: 'capture-mixed-raw' },
      { id: 'capture-query' },
      { id: 'capture-raw' },
    ],
    'helper and raw trigger writes commit while wrong helper keys roll back'
  )
  rows = await admin('/admin/sql', {
    query:
      "SELECT clientID FROM _zsync_clients WHERE clientID LIKE 'capture-%' ORDER BY clientID;",
  })
  equal(
    rows.rows,
    [
      { clientID: 'capture-exact' },
      { clientID: 'capture-mixed' },
      { clientID: 'capture-query' },
      { clientID: 'capture-raw' },
    ],
    'only the wrong helper-key probe leaves its mutation id unconsumed'
  )

  response = await post(
    '/push',
    mutation('client-a', 2, 'test.effectSuccess', {
      id: 'effect-success',
    })
  )
  equal(response.status, 200, 'deferred-effect push status')

  response = await post(
    '/push',
    mutation('client-a', 3, 'test.effectRollback', { id: 'effect-rollback' })
  )
  equal(response.status, 200, 'application error is a push response')
  equal(response.body.pushResponse.mutations[0].result.error, 'app', 'app error result')
  rows = await admin('/admin/sql', {
    query:
      "SELECT (SELECT COUNT(*) FROM project WHERE id = 'effect-rollback') AS projectCount, " +
      "(SELECT CAST(lastMutationID AS TEXT) FROM _zsync_clients WHERE clientID = 'client-a') AS lmid",
  })
  equal(
    rows.rows,
    [{ projectCount: 0, lmid: '3' }],
    'awaited app error rolls back rows/effect and advances LMID in tx2'
  )

  // a retryable rejection is the opposite of the app error above: refusing has
  // to be free, because the thing that refuses is usually a budget, and paying
  // a ledger write per refusal is how a spend guard keeps spending after it
  // closes. the mutation id also has to survive, or refusing silently drops it.
  const changesBeforeRetry = await admin('/admin/sql', {
    query: 'SELECT COUNT(*) AS n FROM _zsync_changes',
  })
  response = await post(
    '/push',
    mutation('client-a', 4, 'test.retryLater', { id: 'retry-later' })
  )
  equal(response.status, 429, 'retryable rejection is refused, not acknowledged')
  equal(response.headers.get('retry-after'), '300', 'refusal says when to return')
  equal(
    response.body.details,
    { error: 'harnessBudgetExceeded' },
    'refusal keeps the reason the caller paces on'
  )
  rows = await admin('/admin/sql', {
    query:
      "SELECT (SELECT COUNT(*) FROM project WHERE id = 'retry-later') AS projectCount, " +
      '(SELECT COUNT(*) FROM _zsync_changes) AS changeCount, ' +
      "(SELECT CAST(lastMutationID AS TEXT) FROM _zsync_clients WHERE clientID = 'client-a') AS lmid",
  })
  equal(
    rows.rows,
    [
      {
        projectCount: 0,
        changeCount: changesBeforeRetry.rows[0].n,
        lmid: '3',
      },
    ],
    'retryable rejection writes no row, no change, and no ledger advance'
  )

  response = await post(
    '/push',
    mutation('client-a', 4, 'project.create', {
      id: 'retry-later-applied',
      ownerId: 'user-a',
      name: 'retried',
    })
  )
  equal(response.status, 200, 'the refused mutation id is still available')
  equal(response.body.pushResponse.mutations[0].result, {}, 'the retry applies')

  const receiver = await openWake('wake-receiver', 'user-a')
  const pusher = await openWake('wake-pusher', 'user-b')
  response = await post(
    '/push',
    mutation('wake-pusher', 1, 'project.create', {
      id: 'wake-created',
      ownerId: 'user-b',
      name: 'wake',
    }),
    'user-b'
  )
  equal(response.status, 200, 'wake push status')
  await eventually(
    () => equal(receiver.messages[0], 'wake', 'receiver got wake'),
    1_000,
    'wake'
  )
  await new Promise((resolve) => setTimeout(resolve, 50))
  equal(pusher.messages, [], 'pusher excluded from wake')

  const beforeIdle = await admin('/admin/status')
  await new Promise((resolve) => setTimeout(resolve, beforeIdle.idleTeardownMs + 100))
  const afterIdle = await admin('/admin/status')
  assert.notEqual(afterIdle.bootID, beforeIdle.bootID, 'idle teardown changes boot ID')
  assertions++
  equal(afterIdle.connectedWakeSockets, 2, 'hibernating sockets survive teardown model')

  await post(
    '/push',
    mutation('wake-pusher', 2, 'project.create', {
      id: 'wake-after-idle',
      ownerId: 'user-b',
      name: 'wake after idle',
    }),
    'user-b'
  )
  await eventually(
    () => equal(receiver.messages[1], 'wake', 'wake delivered after re-instantiation'),
    1_000,
    'post-idle wake'
  )
  receiver.socket.close()
  pusher.socket.close()

  console.log(
    `M3 production ${externalURL ? 'deployed' : 'workerd'} integration passed (${assertions} assertions)`
  )
} finally {
  if (server) {
    server.kill()
    await server.exited
  }
}
