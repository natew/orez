import assert from 'node:assert/strict'

const port = 9_000 + Math.floor(Math.random() * 500)
const server = Bun.spawn(
  [
    'bunx',
    'wrangler',
    'dev',
    '--config',
    'wrangler.toml',
    '--local',
    '--var',
    'ADMIN_KEY:local-admin',
    '--port',
    String(port),
  ],
  {
    cwd: new URL('.', import.meta.url).pathname,
    stdout: 'inherit',
    stderr: 'inherit',
  },
)
const baseURL = `http://127.0.0.1:${port}`

for (let attempt = 0; ; attempt++) {
  try {
    if ((await fetch(baseURL)).ok) break
  } catch {}
  if (attempt >= 150) throw new Error('production workerd did not become ready')
  await new Promise((resolve) => setTimeout(resolve, 100))
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
  return { status: response.status, body: await response.json() }
}

const admin = async (path, body) => {
  const response = await fetch(`${origin}${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: {
      'x-admin-key': 'local-admin',
      ...(body === undefined ? {} : { 'content-type': 'application/json' }),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  assert.equal(response.ok, true, `${path}: ${response.status} ${await response.clone().text()}`)
  return response.json()
}

const mutation = (clientID, id, name, args) => ({
  clientGroupID: `group-${clientID}`,
  pushVersion: 1,
  mutations: [{ type: 'custom', clientID, id, name, args: [args] }],
})

function openWake(clientID, userID) {
  const url = `${origin.replace('http:', 'ws:')}/wake?clientID=${encodeURIComponent(clientID)}`
  const socket = new WebSocket(url, {
    headers: { authorization: `Bearer token-${userID}` },
  })
  const messages = []
  socket.addEventListener('message', (event) => messages.push(String(event.data)))
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`wake socket ${clientID} did not open`)), 5_000)
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
  const firstPull = await post('/pull', {
    clientID: 'client-a',
    clientGroupID: 'group-client-a',
    cookie: null,
  })
  equal(firstPull.status, 200, 'initial pull status')
  equal(firstPull.body.cookie, 0, 'initial cookie')
  assert.ok(firstPull.body.rowsPatch.length > 60, 'initial snapshot includes fixture rows')
  assertions++

  let response = await post(
    '/push',
    mutation('client-a', 1, 'project.create', {
      id: 'prod-created',
      ownerId: 'user-a',
      name: 'production host',
    }),
  )
  equal(response.status, 200, 'push status')
  equal(response.body.pushResponse.mutations[0].result, {}, 'push result')

  response = await post(
    '/push',
    mutation('client-a', 2, 'test.effectSuccess', {
      id: 'effect-success',
      clientID: 'client-a',
      mutationID: 2,
    }),
  )
  equal(response.status, 200, 'deferred-effect push status')
  let rows = await admin('/admin/sql', {
    query: "SELECT observedCommitted FROM _harness_effects WHERE id = 'effect-success'",
  })
  equal(rows.rows, [{ observedCommitted: 1 }], 'effect ran only after LMID commit')

  response = await post(
    '/push',
    mutation('client-a', 3, 'test.effectRollback', { id: 'effect-rollback' }),
  )
  equal(response.status, 200, 'application error is a push response')
  equal(response.body.pushResponse.mutations[0].result.error, 'app', 'app error result')
  rows = await admin('/admin/sql', {
    query:
      "SELECT (SELECT COUNT(*) FROM project WHERE id = 'effect-rollback') AS projectCount, " +
      "(SELECT COUNT(*) FROM _harness_effects WHERE id = 'effect-rollback') AS effectCount, " +
      "(SELECT CAST(lastMutationID AS TEXT) FROM _zsync_clients WHERE clientID = 'client-a') AS lmid",
  })
  equal(
    rows.rows,
    [{ projectCount: 0, effectCount: 0, lmid: '3' }],
    'awaited app error rolls back rows/effect and advances LMID in tx2',
  )

  const receiver = await openWake('wake-receiver', 'user-a')
  const pusher = await openWake('wake-pusher', 'user-b')
  response = await post(
    '/push',
    mutation('wake-pusher', 1, 'project.create', {
      id: 'wake-created',
      ownerId: 'user-b',
      name: 'wake',
    }),
    'user-b',
  )
  equal(response.status, 200, 'wake push status')
  await eventually(() => equal(receiver.messages[0], 'wake', 'receiver got wake'), 1_000, 'wake')
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
    'user-b',
  )
  await eventually(
    () => equal(receiver.messages[1], 'wake', 'wake delivered after re-instantiation'),
    1_000,
    'post-idle wake',
  )
  receiver.socket.close()
  pusher.socket.close()

  console.log(`M3 production workerd integration passed (${assertions} assertions)`)
} finally {
  server.kill()
  await server.exited
}
