import assert from 'node:assert/strict'

const externalURL = process.env.M0_BASE_URL?.replace(/\/$/, '')
const port = 9_000 + Math.floor(Math.random() * 500)
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
const call = async (namespace, route, body) => {
  const response = await fetch(`${baseURL}/${namespace}${route}`, {
    method: body === undefined ? 'GET' : 'POST',
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  return { status: response.status, body: await response.json() }
}

try {
  const transactions = ns('transactions')
  let result = await call(transactions, '/pull')
  check(result.status, 200, 'pull status')
  check(result.body.transaction, 'transactionSync', 'pull transaction type')
  check(result.body.snapshot, { lmid: '0', balance: 100 }, 'pull snapshot')

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
  const beforeAppError = result.body.state

  result = await call(transactions, '/transaction-query')
  check(result.status, 200, 'transaction query status')
  check(result.body.result.balance, 105, 'transaction query singular root')
  check(result.body.result.entries.length, 2, 'transaction query related rows')
  check(result.body.result.entries[0].note, 'read-then-write', 'related row order')
  check(result.body.plan.root.relationships.length, 1, 'recursive plan crosses wasm')
  check(result.body.malformedFormatStatus, 400, 'malformed format is a 400')

  result = await call(transactions, '/application-transaction-query')
  check(result.status, 200, 'application transaction query status')
  check(result.body.result.balance, 105, 'application transaction singular root')
  check(result.body.result.entries.length, 2, 'application transaction related rows')

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

  result = await call(transactions, '/push/application-error', { mutationID: 'm3' })
  check(result.status, 409, 'application error status')
  check(result.body.effectsDeferredButNotRun, 1, 'failed mutator deferred an effect')
  check(
    result.body.state,
    beforeAppError,
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

  const guard = ns('guard')
  result = await call(guard, '/adapter-guard')
  check(result.status, 200, 'adapter guard status')
  check(result.body.errors.length, 2, 'adapter rejects tx SQL and ?N parameters')

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
