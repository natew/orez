import assert from 'node:assert/strict'

import { findPort } from '../../src/port.ts'

const externalURL = process.env.M0_BASE_URL?.replace(/\/$/, '')
const port = await findPort(0)
const server = externalURL
  ? undefined
  : Bun.spawn(
      [
        'bunx',
        'wrangler',
        'dev',
        '--config',
        'wrangler.platform.toml',
        '--local',
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
      const response = await fetch(baseURL)
      if (response.ok) break
    } catch {}
    if (attempt >= 150) throw new Error('wrangler workerd did not become ready')
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

let assertions = 0
const check = (actual, expected, message) => {
  assert.deepStrictEqual(actual, expected, message)
  assertions++
}
const ns = (label) => `${label}-${crypto.randomUUID()}`
const call = async (namespace, route, body, signal) => {
  const response = await fetch(`${baseURL}/${namespace}${route}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal,
  })
  return { status: response.status, body: await response.json() }
}
const waitForStage = async (namespace, stage) => {
  for (let attempt = 0; attempt < 200; attempt++) {
    const status = await call(`_application-cancellation/${namespace}`, '/status')
    if (status.body.stages.includes(stage)) return
    await new Promise((resolve) => setTimeout(resolve, 10))
  }
  throw new Error(`application cancellation stage did not start: ${stage}`)
}
const callWithTimeout = async (namespace, route, timeoutMs = 3_000) => {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)
  try {
    return await call(namespace, route, undefined, controller.signal)
  } finally {
    clearTimeout(timer)
  }
}

try {
  const transactions = ns('transactions')
  let result = await call(transactions, '/pull')
  check(result.status, 200, 'pull status')
  check(result.body.transaction, 'transactionSync', 'pull transaction type')
  check(result.body.snapshot, { lmid: '0', balance: 100 }, 'pull snapshot')

  const patternRecovery = ns('pattern-recovery')
  result = await call(patternRecovery, '/pattern-complexity')
  check(result.status, 200, 'long LIKE recovery probe status')
  check(
    result.body.existingClient.ok,
    true,
    'incremental pull accepts the persisted long prefix query'
  )
  check(
    result.body.reloadedClient.ok,
    true,
    'a reloaded client recovers in the same client group'
  )
  check(result.body.freshGroup.ok, true, 'fresh client group remains healthy')

  result = await call(transactions, '/push/read-then-write', { mutationID: 'm1' })
  check(result.status, 200, 'read-then-write status')
  check(result.body.awaitedInsideTransaction, true, 'async tx crossed await')
  check(result.body.state.lmid, '1', 'read-then-write LMID')
  check(result.body.state.balance, 110, 'read-then-write balance')
  check(result.body.state.ledgerCount, 1, 'read-then-write ledger')
  check(result.body.state.sideEffectCount, 1, 'post-commit effect count')
  check(
    result.body.state.sideEffects[0].observedCommitted,
    true,
    'effect observed commit'
  )

  result = await call(transactions, '/push/multi-table', { mutationID: 'm2' })
  check(result.status, 200, 'multi-table status')
  check(result.body.state.lmid, '2', 'multi-table LMID')
  check(result.body.state.balance, 105, 'multi-table balance')
  check(result.body.state.ledgerCount, 2, 'multi-table ledger')
  check(result.body.state.outboxCount, 1, 'multi-table outbox')
  check(result.body.state.sideEffectCount, 2, 'multi-table post-commit effect')

  result = await call(transactions, '/transaction-query')
  check(result.status, 200, 'transaction query status')
  check(result.body.result.balance, 105, 'transaction query singular root')
  check(result.body.result.entries.length, 2, 'transaction query related rows')
  check(result.body.result.entries[0].note, 'read-then-write', 'related row order')
  check(result.body.execResult, { changes: 1 }, 'sync host reports changed rows')
  check(result.body.plan.root.relationships.length, 1, 'recursive plan crosses wasm')
  check(result.body.malformedFormatStatus, 400, 'malformed format is a 400')

  result = await call(transactions, '/application-transaction-query')
  check(result.status, 200, 'application transaction query status')
  check(result.body.result.balance, 105, 'application transaction singular root')
  check(result.body.result.entries.length, 2, 'application transaction related rows')

  const applicationRpc = ns('application-rpc')
  result = await call(`_application-rpc/${applicationRpc}`, '/commit')
  check(result.status, 200, 'application RPC transaction status')
  check(
    result.body.before,
    [{ balance: 100 }],
    'application RPC query before transaction'
  )
  check(
    result.body.result.account.balance,
    100,
    'application RPC queryAst serializes to the Durable Object'
  )
  check(
    result.body.result.account.entries.length,
    0,
    'application RPC queryAst materializes relations'
  )
  check(
    result.body.after,
    [{ balance: 111 }],
    'application RPC exec commits through session'
  )
  check(
    result.body.result.execResult,
    { changes: 1 },
    'application RPC reports changed rows'
  )

  result = await call(`_application-rpc/${applicationRpc}`, '/rollback')
  check(result.status, 409, 'application RPC rollback status')
  check(
    result.body.before,
    [{ balance: 111 }],
    'application RPC rollback starts after commit'
  )
  check(
    result.body.after,
    [{ balance: 111 }],
    'application RPC rollback discards SQL write'
  )

  const applicationOverlap = ns('application-overlap')
  result = await call(`_application-rpc/${applicationOverlap}`, '/overlap')
  check(result.status, 200, 'overlapping application RPC status')
  check(
    result.body.waitedForFirst,
    true,
    'overlapping transaction and snapshot wait for the active application session'
  )
  check(result.body.firstResult.ok, false, 'first overlapping transaction rolls back')
  check(result.body.snapshotStatus, 200, 'snapshot completes after rollback')
  assert.ok(
    result.body.snapshotBalance === 100 || result.body.snapshotBalance === 105,
    `snapshot observed intermediate balance ${result.body.snapshotBalance}`
  )
  assertions++
  check(result.body.secondResult.ok, true, 'second overlapping transaction commits')
  check(
    result.body.after,
    [{ balance: 105 }],
    'overlapping transaction result is durable'
  )

  // Mixed application-SQL load: arrival-order admission, read concurrency, and
  // cancellation, measured through the real client against a real Durable
  // Object. Every assertion below is false against competitive `begin()`
  // polling: admission order was decided by whose 25 ms timer fired first, and
  // there was no read lane at all.
  const admissionFifo = ns('application-admission-fifo')
  result = await call(
    `_application-admission/${admissionFifo}`,
    '/mixed?writers=8&readers=0&holdMs=60&staggerMs=20'
  )
  check(result.status, 200, 'admission fifo status')
  check(result.body.admitted, 8, 'every write session was admitted')
  check(result.body.inversions, 0, 'write sessions are admitted in arrival order')
  check(result.body.order, result.body.launched, 'admission order equals arrival order')
  check(result.body.maxConcurrentWrites, 1, 'write sessions stay mutually exclusive')
  check(result.body.balance, [{ balance: 108 }], 'every admitted write committed once')
  check(
    result.body.residue,
    { writer: false, readers: 0, queued: 0 },
    'the admission queue is empty after the load'
  )

  const admissionMixed = ns('application-admission-mixed')
  result = await call(
    `_application-admission/${admissionMixed}`,
    '/mixed?writers=4&readers=8&cancels=2&holdMs=60&staggerMs=20'
  )
  check(result.status, 200, 'mixed admission status')
  check(result.body.inversions, 0, 'mixed load is admitted in arrival order')
  check(result.body.canceled, 2, 'queued cancellations abandon their turn')
  check(result.body.maxConcurrentWrites, 1, 'a write session excludes every other')
  check(result.body.readerSawWriter, false, 'no read session runs beside a write session')
  check(result.body.writerSawReader, false, 'no write session runs beside a read session')
  check(result.body.balance, [{ balance: 104 }], 'canceled writes left nothing behind')
  check(
    result.body.residue,
    { writer: false, readers: 0, queued: 0 },
    'cancellation leaves no waiter in the admission queue'
  )
  assert.ok(
    result.body.maxConcurrentReads > 1,
    `read sessions serialized (maxConcurrentReads ${result.body.maxConcurrentReads})`
  )
  assertions++
  // The same load with the read lane disabled is the negative control: these
  // reads are identical SQL, so a concurrency number above one here would mean
  // the counter is measuring something other than admission.
  const admissionControl = ns('application-admission-control')
  const control = await call(
    `_application-admission/${admissionControl}`,
    '/mixed?writers=4&readers=8&cancels=0&holdMs=60&staggerMs=20&readLane=0'
  )
  check(control.status, 200, 'admission negative control status')
  check(control.body.maxConcurrentReads, 1, 'write-lane reads serialize one at a time')
  assert.ok(
    control.body.waitMs.readP95 > result.body.waitMs.readP95,
    `read lane did not reduce read admission wait (read lane p95 ${result.body.waitMs.readP95} ms, write lane p95 ${control.body.waitMs.readP95} ms)`
  )
  assertions++
  console.log(
    `application admission: read lane p50 ${result.body.waitMs.p50} ms / p95 ${result.body.waitMs.p95} ms / max ${result.body.waitMs.max} ms, read p95 ${result.body.waitMs.readP95} ms, concurrent reads ${result.body.maxConcurrentReads}; write-lane control read p95 ${control.body.waitMs.readP95} ms, max ${control.body.waitMs.max} ms`
  )

  const canceledQueued = ns('application-canceled-queued')
  const held = call(`_application-cancellation/${canceledQueued}`, '/hold')
  await waitForStage(canceledQueued, 'hold-active')
  const queued = call(`_application-cancellation/${canceledQueued}`, '/queued')
  await waitForStage(canceledQueued, 'queued')
  check((await queued).body.ok, false, 'queued application RPC observes cancellation')
  await call(`_application-cancellation/${canceledQueued}`, '/release')
  const heldResult = await held
  check(heldResult.body.ok, false, 'held owner rolls back after release')
  result = await callWithTimeout(`_application-cancellation/${canceledQueued}`, '/verify')
  check(
    result.body.direct,
    [{ balance: 105 }],
    'canceling a queued transaction leaves no owner behind'
  )
  check(result.body.snapshotStatus, 200, 'snapshot follows canceled queued transaction')
  check(
    result.body.snapshot.tables.accounts[0].balance,
    105,
    'snapshot excludes the canceled queued write'
  )

  const canceledActive = ns('application-canceled-active')
  const active = call(`_application-cancellation/${canceledActive}`, '/active')
  await waitForStage(canceledActive, 'active-active')
  check((await active).body.ok, false, 'active application RPC observes cancellation')
  const activeStatus = await call(
    `_application-cancellation/${canceledActive}`,
    '/status'
  )
  check(activeStatus.body.activeSession, false, 'active session is disposed after abort')
  result = await callWithTimeout(`_application-cancellation/${canceledActive}`, '/verify')
  check(
    result.body.direct,
    [{ balance: 105 }],
    'canceling an active transaction rolls back its write and releases ownership'
  )
  check(result.body.snapshotStatus, 200, 'snapshot follows canceled active transaction')
  check(
    result.body.snapshot.tables.accounts[0].balance,
    105,
    'snapshot excludes the canceled active write'
  )

  result = await call(transactions, '/application-transaction-query-budget')
  check(result.status, 409, 'application transaction query budget status')
  check(
    result.body.code,
    'transaction_query_budget_exceeded',
    'application transaction budget code'
  )
  check(
    result.body.query,
    'budgetedApplicationTransactionQuery',
    'application transaction budget query name'
  )
  check(result.body.selects, 2, 'application transaction budget select count')

  const beforeAppError = await call(transactions, '/status')
  check(beforeAppError.status, 200, 'application error baseline status')
  result = await call(transactions, '/push/application-error', { mutationID: 'm3' })
  check(result.status, 409, 'application error status')
  check(result.body.effectsDeferredButNotRun, 1, 'failed mutator deferred an effect')
  check(
    result.body.state,
    beforeAppError.body.state,
    'application error rolls back and runs no effect'
  )

  const jsError = ns('js-error')
  result = await call(jsError, '/js-exception')
  check(result.status, 409, 'JS exception status')
  check(
    result.body.after,
    result.body.before,
    'awaited JS exception rolls back every effect and LMID'
  )
  check(result.body.after.lmid, '0', 'JS exception cannot advance LMID')

  const rustPanic = ns('rust-panic')
  result = await call(rustPanic, '/rust-panic')
  check(result.status, 409, 'Rust panic status')
  check(
    result.body.after,
    result.body.before,
    'Rust panic rolls back every effect and LMID'
  )
  check(result.body.after.lmid, '0', 'Rust panic cannot advance LMID')

  const values = ns('values')
  const valueInput = {
    integer: '-42',
    real: 0.1 + 0.2,
    text: 'wasm ↔ JS ↔ SQLite',
    blob: [0, 1, 127, 128, 255],
    null: null,
    json: { nested: ['value', 3], ok: true },
    boolean: true,
    boundary: '9007199254740993',
  }
  result = await call(values, '/values', valueInput)
  check(result.status, 200, 'value probe status')
  check(result.body, valueInput, 'all values round-trip exactly')
  check(String(result.body.boundary), '9007199254740993', '2^53 + 1 stays exact decimal')
  check(result.body.real, 0.30000000000000004, 'float keeps shortest-round-trip fidelity')

  // canonical primary-key encoding, cross-build half. sync-native compiles
  // serde_json with preserve_order and this wasm build does not, so the same
  // composite key used to encode differently in each. membership keys, delete
  // ids, and realtime topics all key off this string. the native half of these
  // vectors runs in sync_core::value's tests.
  const pkVectors = ns('canonical-pk')
  const { vectors } = await Bun.file(
    new URL('../../harness/fixtures/canonical-pk-vectors.json', import.meta.url)
  ).json()
  let pkDisagreements = 0
  for (const vector of vectors) {
    result = await call(pkVectors, '/canonical-pk', {
      primaryKey: vector.primaryKey,
      pk: vector.pk,
    })
    check(result.status, 200, `canonical pk status: ${vector.name}`)
    check(result.body.encoded, vector.expected, `canonical pk vector: ${vector.name}`)
    if (JSON.stringify(vector.pk) !== vector.expected) pkDisagreements++
  }
  // negative control: at least one vector must differ from a plain
  // JSON.stringify of the input object, otherwise these vectors are not
  // exercising key ordering at all and would pass under any encoder.
  assert.ok(pkDisagreements > 0, 'canonical pk vectors do not exercise key ordering')
  assertions++

  const guard = ns('guard')
  result = await call(guard, '/adapter-guard')
  check(result.status, 200, 'adapter guard status')
  check(result.body.errors.length, 2, 'adapter rejects tx SQL and ?N parameters')
  check(result.body.sqlFailure.operation, 'query', 'adapter SQL failure operation')
  check(
    result.body.sqlFailure.sql,
    "SELECT 1 AS matched WHERE 'x' GLOB ?",
    'adapter SQL failure statement'
  )
  check(
    result.body.sqlFailure.params[0].value,
    'record\\_prefix\\_that\\_is\\_longer\\_than\\_the\\_durable\\_object\\_glob\\_limit\\_%',
    'adapter SQL failure pattern'
  )

  const eviction = ns('eviction')
  await call(eviction, '/push/read-then-write', { mutationID: 'persist-before-eviction' })
  const beforeEviction = await call(eviction, '/status')
  await new Promise((resolve) => setTimeout(resolve, 325))
  const afterEviction = await call(eviction, '/status')
  assert.notStrictEqual(afterEviction.body.bootID, beforeEviction.body.bootID)
  assertions++
  check(
    afterEviction.body.reinstantiations,
    beforeEviction.body.reinstantiations + 1,
    'one additional idle teardown was observed'
  )
  check(afterEviction.body.state.lmid, '1', 'LMID persists across re-instantiation')
  check(
    afterEviction.body.state.balance,
    110,
    'application data persists across re-instantiation'
  )
  check(
    afterEviction.body.state.mutationCount,
    1,
    'mutation record persists across re-instantiation'
  )

  console.log(
    `M0 ${externalURL ? 'deployed' : 'local workerd'} probe passed (${assertions} assertions)`
  )
} finally {
  if (server) {
    server.kill()
    await server.exited
  }
}
