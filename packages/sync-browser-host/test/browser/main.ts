import { createBrowserSyncHostPortClient } from 'orez/sync-browser-host'

import type { BrowserHostTestFaultPoint } from '../../src/host.js'
import type { BrowserSyncHostPortClient } from '../../src/types.js'

type WorkerControlMessage =
  | { type: 'ready' }
  | { type: 'boot-error'; message: string; stack?: string }
  | { type: 'fault-reached'; point: BrowserHostTestFaultPoint }
  | { type: 'effect-complete'; id: string }
  | { type: 'connected'; id: string }
  | { type: 'application-transaction-effect'; id: string }
  | { type: 'application-transaction-complete'; id: string; rows: unknown[] }
  | { type: 'application-transaction-error'; id: string; message: string }
  | { type: 'application-transaction-rollback-effect'; id: string }
  | { type: 'application-transaction-rollback-complete'; id: string; message: string }
  | { type: 'application-transaction-rollback-error'; id: string; message: string }

type Connection = {
  worker: Worker
  client: BrowserSyncHostPortClient
  attachClient(): Promise<BrowserSyncHostPortClient>
  waitForFault(point: BrowserHostTestFaultPoint): Promise<void>
  waitForEffect(id: string): Promise<void>
  runApplicationTransaction(): Promise<{ rows: unknown[]; effectBeforeResolve: boolean }>
  runRolledBackApplicationTransaction(): Promise<{
    message: string
    effectRan: boolean
  }>
  terminate(): void
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

function equal(actual: unknown, expected: unknown, message: string): void {
  const canonical = (value: unknown): unknown => {
    if (Array.isArray(value)) return value.map(canonical)
    if (value && typeof value === 'object' && !(value instanceof Uint8Array)) {
      return Object.fromEntries(
        Object.entries(value)
          .sort(([left], [right]) => left.localeCompare(right))
          .map(([key, entry]) => [key, canonical(entry)])
      )
    }
    return value
  }
  if (JSON.stringify(canonical(actual)) !== JSON.stringify(canonical(expected))) {
    throw new Error(
      `${message}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`
    )
  }
}

async function openConnection(
  storageKey: string,
  faultPoint?: BrowserHostTestFaultPoint,
  checkpointFailure = false
): Promise<Connection> {
  const worker = new Worker('/worker.js', { type: 'module' })
  const channel = new MessageChannel()
  const messages: WorkerControlMessage[] = []
  const waiters = new Set<() => void>()
  worker.addEventListener('message', (event: MessageEvent<WorkerControlMessage>) => {
    messages.push(event.data)
    for (const wake of waiters) wake()
  })
  const waitFor = async (
    predicate: (message: WorkerControlMessage) => boolean
  ): Promise<WorkerControlMessage> => {
    for (;;) {
      const match = messages.find(predicate)
      if (match) return match
      await new Promise<void>((resolve) => {
        const wake = () => {
          waiters.delete(wake)
          resolve()
        }
        waiters.add(wake)
      })
    }
  }
  worker.postMessage(
    {
      type: 'start',
      storageKey,
      faultPoint,
      checkpointFailure,
      port: channel.port1,
    },
    [channel.port1]
  )
  const boot = await waitFor(
    (message) => message.type === 'ready' || message.type === 'boot-error'
  )
  if (boot.type === 'boot-error') {
    worker.terminate()
    throw new Error(boot.message)
  }
  const client = createBrowserSyncHostPortClient(channel.port2)
  return {
    worker,
    client,
    async attachClient() {
      const additional = new MessageChannel()
      const id = crypto.randomUUID()
      worker.postMessage({ type: 'connect', id, port: additional.port1 }, [
        additional.port1,
      ])
      await waitFor((message) => message.type === 'connected' && message.id === id)
      return createBrowserSyncHostPortClient(additional.port2)
    },
    async waitForFault(point) {
      await waitFor(
        (message) => message.type === 'fault-reached' && message.point === point
      )
    },
    async waitForEffect(id) {
      await waitFor((message) => message.type === 'effect-complete' && message.id === id)
    },
    async runApplicationTransaction() {
      const id = crypto.randomUUID()
      worker.postMessage({ type: 'application-transaction', id })
      const complete = await waitFor(
        (message) =>
          (message.type === 'application-transaction-complete' ||
            message.type === 'application-transaction-error') &&
          message.id === id
      )
      if (complete.type !== 'application-transaction-complete') {
        throw new Error(
          complete.type === 'application-transaction-error'
            ? complete.message
            : 'unexpected application transaction response'
        )
      }
      return {
        rows: complete.rows,
        effectBeforeResolve: messages.some(
          (message) =>
            message.type === 'application-transaction-effect' && message.id === id
        ),
      }
    },
    async runRolledBackApplicationTransaction() {
      const id = crypto.randomUUID()
      worker.postMessage({ type: 'application-transaction-rollback', id })
      const complete = await waitFor(
        (message) =>
          (message.type === 'application-transaction-rollback-complete' ||
            message.type === 'application-transaction-rollback-error') &&
          message.id === id
      )
      if (complete.type !== 'application-transaction-rollback-complete') {
        throw new Error(
          complete.type === 'application-transaction-rollback-error'
            ? complete.message
            : 'unexpected application transaction rollback response'
        )
      }
      return {
        message: complete.message,
        effectRan: messages.some(
          (message) =>
            message.type === 'application-transaction-rollback-effect' &&
            message.id === id
        ),
      }
    },
    terminate() {
      worker.terminate()
      client.close()
    },
  }
}

function mutation(
  clientID: string,
  id: number,
  name: string,
  args: Record<string, unknown>
): Record<string, unknown> {
  return {
    clientGroupID: `group-${clientID}`,
    pushVersion: 1,
    mutations: [{ type: 'custom', clientID, id, name, args: [args] }],
  }
}

async function post(
  client: BrowserSyncHostPortClient,
  path: '/pull' | '/push',
  body: unknown,
  authenticated = true,
  queryAware = false
): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await client.fetch(`http://preview.invalid${path}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(authenticated ? { authorization: 'Bearer preview-token' } : {}),
      ...(queryAware ? { 'x-query-aware': '1' } : {}),
    },
    body: JSON.stringify(body),
  })
  return {
    status: response.status,
    body: (await response.json()) as Record<string, unknown>,
  }
}

function isReplay(response: Record<string, unknown>): boolean {
  return JSON.stringify(response).includes('alreadyProcessed')
}

async function runFaultCase(point: BrowserHostTestFaultPoint) {
  const storageKey = `fault:${point}:${crypto.randomUUID()}`
  const clientID = `client-${point}`
  const rowID = `row-${point}`
  const body = mutation(clientID, 1, 'todo.create', {
    id: rowID,
    title: point,
  })
  const connection = await openConnection(storageKey, point)
  const fault = connection.waitForFault(point)
  void post(connection.client, '/push', body).catch(() => undefined)
  await fault
  connection.terminate()

  const restarted = await openConnection(storageKey)
  const rows = await restarted.client.query<{ count: number }>(
    'SELECT COUNT(*) AS count FROM todo WHERE id = ?',
    [rowID]
  )
  const durable =
    point === 'after_idb_commit_before_response' || point === 'during_response_delivery'
  equal(rows, [{ count: durable ? 1 : 0 }], `${point} restored row state`)
  const replay = await post(restarted.client, '/push', body)
  equal(replay.status, 200, `${point} replay status`)
  equal(isReplay(replay.body), durable, `${point} replay classification`)
  const afterReplay = await restarted.client.query<{ count: number }>(
    'SELECT COUNT(*) AS count FROM todo WHERE id = ?',
    [rowID]
  )
  equal(afterReplay, [{ count: 1 }], `${point} applies application effect once`)
  restarted.terminate()
  return { point, durable, replay: isReplay(replay.body) }
}

async function runCheckpointFailureCase() {
  const storageKey = `checkpoint-failure:${crypto.randomUUID()}`
  const body = mutation('checkpoint-client', 1, 'todo.create', {
    id: 'checkpoint-row',
    title: 'must restore old snapshot',
  })
  const connection = await openConnection(storageKey, undefined, true)
  const failed = await post(connection.client, '/push', body)
  equal(failed.status, 500, 'checkpoint failure rejects push')
  assert(
    String(failed.body.error).includes('host terminated'),
    'checkpoint failure returns a fatal durability error'
  )
  let rejectedAfterFailure = false
  try {
    await connection.client.query('SELECT 1')
  } catch (error) {
    rejectedAfterFailure = String(error).includes('checkpoint failed')
  }
  assert(rejectedAfterFailure, 'host rejects operations after checkpoint failure')
  connection.terminate()

  const restarted = await openConnection(storageKey)
  const rows = await restarted.client.query<{ count: number }>(
    'SELECT COUNT(*) AS count FROM todo WHERE id = ?',
    ['checkpoint-row']
  )
  equal(rows, [{ count: 0 }], 'restart restores the last durable snapshot')
  const replay = await post(restarted.client, '/push', body)
  equal(isReplay(replay.body), false, 'failed checkpoint leaves mutation replayable')
  restarted.terminate()
  return { fatal: true, restoredOldSnapshot: true }
}

async function runBrowserHostSpike() {
  const storageKey = `browser-host:${crypto.randomUUID()}`
  let connection = await openConnection(storageKey)
  let wakes = 0
  connection.client.subscribe(() => wakes++)
  const secondClient = await connection.attachClient()
  let secondWakes = 0
  secondClient.subscribe(() => secondWakes++)

  const unauthorized = await post(
    connection.client,
    '/pull',
    { clientID: 'unauthorized', clientGroupID: 'unauthorized', cookie: null },
    false
  )
  equal(unauthorized.status, 401, 'pull authentication')

  const initial = await post(connection.client, '/pull', {
    clientID: 'client-main',
    clientGroupID: 'group-client-main',
    cookie: null,
  })
  equal(initial.status, 200, 'initial pull status')
  assert('cookie' in initial.body, 'initial pull returns a cookie')

  await connection.client.exec(
    'CREATE TABLE IF NOT EXISTS blob_probe (id TEXT PRIMARY KEY, payload BLOB NOT NULL)'
  )
  await connection.client.exec('INSERT INTO blob_probe (id, payload) VALUES (?, ?)', [
    'blob',
    Uint8Array.from([0, 127, 128, 255]),
  ])
  const blobRows = await connection.client.query<{ payload: Uint8Array }>(
    'SELECT payload FROM blob_probe WHERE id = ?',
    ['blob']
  )
  equal(
    Array.from(blobRows[0]?.payload ?? []),
    [0, 127, 128, 255],
    'Bedrock browser BLOB values cross the worker port'
  )
  equal(wakes, 2, 'direct SQL wakes the first attached client')
  equal(secondWakes, 2, 'direct SQL wakes the second attached client')
  wakes = 0
  secondWakes = 0

  const rolledBackApplicationTransaction =
    await connection.runRolledBackApplicationTransaction()
  equal(
    rolledBackApplicationTransaction.message,
    'rollback requested',
    'application transaction returns the callback error'
  )
  equal(
    rolledBackApplicationTransaction.effectRan,
    false,
    'application transaction drops deferred effects on rollback'
  )
  equal(
    await connection.client.query('SELECT id FROM todo WHERE id = ?', [
      'application-transaction-rollback',
    ]),
    [],
    'application transaction rolls back row writes'
  )
  equal(wakes, 0, 'rolled-back application transaction does not wake clients')
  equal(secondWakes, 0, 'rolled-back application transaction does not wake peer clients')

  const applicationTransaction = await connection.runApplicationTransaction()
  equal(
    applicationTransaction.rows,
    [{ id: 'application-transaction', title: 'trusted', done: false }],
    'application transaction materializes queryAst rows'
  )
  equal(
    applicationTransaction.effectBeforeResolve,
    true,
    'application transaction runs deferred effects before resolving'
  )
  equal(wakes, 1, 'application transaction wakes the first attached client')
  equal(secondWakes, 1, 'application transaction wakes the second attached client')
  wakes = 0
  secondWakes = 0

  const createBody = mutation('client-main', 1, 'todo.create', {
    id: 'persistent',
    title: 'first',
  })
  const created = await post(connection.client, '/push', createBody)
  equal(created.status, 200, 'create push status')
  equal(isReplay(created.body), false, 'first create is not a replay')
  equal(wakes, 1, 'durable push sends one advisory wake')
  equal(secondWakes, 1, 'durable push wakes every attached client')

  const querySeed = await post(
    connection.client,
    '/push',
    mutation('query-seed', 1, 'todo.create', {
      id: 'query-done',
      title: 'query member',
      done: true,
    })
  )
  equal(querySeed.status, 200, 'query seed status')
  const queryPull = await post(
    connection.client,
    '/pull',
    {
      clientID: 'query-client',
      clientGroupID: 'query-group',
      cookie: null,
      queries: {
        version: 1,
        patch: [{ op: 'put', hash: 'done', name: 'todosDone', args: [] }],
      },
    },
    true,
    true
  )
  equal(queryPull.status, 200, 'query-aware pull status')
  equal(
    queryPull.body.gotQueries,
    { version: 1, patch: [{ op: 'put', hash: 'done' }] },
    'named query is resolved and acknowledged'
  )
  const queryRows = (queryPull.body.rowsPatch as Array<Record<string, unknown>>)
    .filter((entry) => entry.op === 'put' && entry.tableName === 'todo')
    .map((entry) => (entry.value as { id: string }).id)
  equal(queryRows, ['query-done'], 'query-aware pull includes only matching rows')

  const tag = await post(
    connection.client,
    '/push',
    mutation('query-read', 1, 'todo.addTag', {
      id: 'tag-1',
      todoId: 'query-done',
      label: 'important',
    })
  )
  equal(tag.status, 200, 'transaction query tag seed status')
  const copied = await post(
    connection.client,
    '/push',
    mutation('query-read', 2, 'todo.copyFromQuery', {
      sourceId: 'query-done',
      targetId: 'query-copy',
    })
  )
  equal(copied.status, 200, 'transaction query mutation status')
  equal(
    await connection.client.query('SELECT title, done FROM todo WHERE id = ?', [
      'query-copy',
    ]),
    [{ title: 'query member:important', done: 1 }],
    'browser mutator hydrates a related transaction query'
  )

  const inserted = await connection.client.query(
    'SELECT id, title, done FROM todo WHERE id = ?',
    ['persistent']
  )
  equal(
    inserted,
    [{ id: 'persistent', title: 'first', done: 0 }],
    'create commits application row'
  )

  const incremental = await post(connection.client, '/pull', {
    clientID: 'client-main',
    clientGroupID: 'group-client-main',
    cookie: initial.body.cookie,
  })
  equal(incremental.status, 200, 'incremental pull status')
  const insertedPatch = incremental.body.rowsPatch as Array<Record<string, unknown>>
  assert(
    insertedPatch.some(
      (entry) =>
        entry.op === 'put' &&
        entry.tableName === 'todo' &&
        (entry.value as { id?: unknown })?.id === 'persistent'
    ),
    'incremental pull includes inserted todo'
  )

  const replay = await post(connection.client, '/push', createBody)
  equal(replay.status, 200, 'replay status')
  equal(isReplay(replay.body), true, 'duplicate mutation is classified as replay')
  const oneRow = await connection.client.query(
    'SELECT COUNT(*) AS count FROM todo WHERE id = ?',
    ['persistent']
  )
  equal(oneRow, [{ count: 1 }], 'replay does not duplicate application effect')

  const deferredEffect = connection.waitForEffect('deferred')
  const deferred = await post(
    connection.client,
    '/push',
    mutation('effect-client', 1, 'todo.createDeferred', {
      id: 'deferred',
      title: 'post-commit',
    })
  )
  equal(deferred.status, 200, 'deferred-effect mutation status')
  await deferredEffect
  const deferredRows = await connection.client.query(
    'SELECT title FROM todo WHERE id = ?',
    ['deferred']
  )
  equal(
    deferredRows,
    [{ title: 'post-commit' }],
    'deferred effect runs after durable application commit'
  )

  const renamed = await post(
    connection.client,
    '/push',
    mutation('client-main', 2, 'todo.rename', {
      id: 'persistent',
      title: 'renamed',
    })
  )
  equal(renamed.status, 200, 'rename status')
  const renamePull = await post(connection.client, '/pull', {
    clientID: 'client-main',
    clientGroupID: 'group-client-main',
    cookie: incremental.body.cookie,
  })
  assert(
    (renamePull.body.rowsPatch as Array<Record<string, unknown>>).some(
      (entry) =>
        entry.op === 'put' && (entry.value as { title?: unknown })?.title === 'renamed'
    ),
    'incremental pull includes update'
  )

  const temporary = await post(
    connection.client,
    '/push',
    mutation('client-main', 3, 'todo.create', { id: 'temporary', title: 'delete me' })
  )
  equal(temporary.status, 200, 'temporary create status')
  const beforeDelete = await post(connection.client, '/pull', {
    clientID: 'client-main',
    clientGroupID: 'group-client-main',
    cookie: renamePull.body.cookie,
  })
  const deleted = await post(
    connection.client,
    '/push',
    mutation('client-main', 4, 'todo.delete', { id: 'temporary' })
  )
  equal(deleted.status, 200, 'delete status')
  const deletePull = await post(connection.client, '/pull', {
    clientID: 'client-main',
    clientGroupID: 'group-client-main',
    cookie: beforeDelete.body.cookie,
  })
  assert(
    (deletePull.body.rowsPatch as Array<Record<string, unknown>>).some(
      (entry) => entry.op === 'del' && entry.tableName === 'todo'
    ),
    'incremental pull includes delete'
  )

  secondClient.close()
  connection.terminate()
  connection = await openConnection(storageKey)
  const restoredOnce = await connection.client.query(
    'SELECT id, title FROM todo WHERE id = ?',
    ['persistent']
  )
  equal(
    restoredOnce,
    [{ id: 'persistent', title: 'renamed' }],
    'first worker restart restores committed database'
  )
  connection.terminate()
  connection = await openConnection(storageKey)
  const restoredTwice = await connection.client.query(
    'SELECT id, title FROM todo WHERE id = ?',
    ['persistent']
  )
  equal(
    restoredTwice,
    [{ id: 'persistent', title: 'renamed' }],
    'second worker restart restores the same database'
  )
  connection.terminate()

  const faultPoints: BrowserHostTestFaultPoint[] = [
    'before_mutation',
    'after_app_write_before_sqlite_commit',
    'after_sqlite_commit_before_idb_commit',
    'after_idb_commit_before_response',
    'during_response_delivery',
  ]
  const faults = []
  for (const point of faultPoints) faults.push(await runFaultCase(point))
  const checkpointFailure = await runCheckpointFailureCase()

  const result = {
    initialCookie: initial.body.cookie,
    wakes,
    restored: restoredTwice,
    faults,
    checkpointFailure,
  }
  const output = document.querySelector('#result')
  if (output) output.textContent = JSON.stringify(result, null, 2)
  return result
}

;(
  globalThis as unknown as { runBrowserHostSpike: typeof runBrowserHostSpike }
).runBrowserHostSpike = runBrowserHostSpike
