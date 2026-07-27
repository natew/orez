import { createQueryCompiler } from 'orez-sync-cf-host/query-compiler'

import {
  SqlStorageDirect,
  SqlStorageMutatorTransaction,
  SqlStorageSyncDb,
} from '../../sync-cf-host/src/sql-storage-adapter.js'
import {
  canonical_pk_probe,
  init_probe_schema,
  pull_snapshot,
  push_finalize,
  push_preflight,
  rust_panic_after_writes,
  value_round_trip,
} from '../../sync-cf-host/src/wasm-platform.js'
import { createApplicationSqlClient, ZeroDO } from '../src/cf-do/worker.js'

import type { TransactionQueryFormat, ZeroSchemaConfig } from 'orez-sync-executor'

interface Env {
  PROBE_DO: DurableObjectNamespace<ProbeDurableObject>
}

type DeferredEffect = { mutationID: string; kind: string }
type MutatorName = 'read-then-write' | 'multi-table' | 'application-error'

const transactionQuerySchema = {
  tables: {
    account: {
      name: 'account',
      serverName: 'accounts',
      columns: {
        id: { type: 'string' },
        balance: { type: 'number' },
      },
      primaryKey: ['id'],
    },
    entry: {
      name: 'entry',
      serverName: 'ledger',
      columns: {
        id: { type: 'number' },
        accountId: { type: 'string', serverName: 'account_id' },
        amount: { type: 'number' },
        note: { type: 'string' },
      },
      primaryKey: ['id'],
    },
  },
} as const satisfies ZeroSchemaConfig

const transactionQueryAst = {
  table: 'account',
  where: {
    type: 'simple',
    op: '=',
    left: { type: 'column', name: 'id' },
    right: { type: 'literal', value: 'primary' },
  },
  related: [
    {
      correlation: { parentField: ['id'], childField: ['accountId'] },
      subquery: { table: 'entry', alias: 'entries', orderBy: [['id', 'asc']] },
    },
  ],
}

const transactionQueryFormat = {
  singular: true,
  relationships: { entries: { singular: false, relationships: {} } },
} as const satisfies TransactionQueryFormat

const compileTransactionQuery = createQueryCompiler(transactionQuerySchema)

// Deterministic local analogue of normal DO idle eviction, matching the
// harness/cf probe: discard all in-memory state after an idle gap while SQL
// storage remains intact. This keeps the test sub-second instead of waiting
// for workerd/platform eviction heuristics.
const IDLE_TEARDOWN_MS = 250

const json = (value: unknown, status = 200) =>
  Response.json(value, {
    status,
    headers: { 'cache-control': 'no-store' },
  })

async function runApplicationRpcProbe(
  env: Env,
  namespace: string,
  action: 'commit' | 'rollback'
): Promise<Response> {
  const client = createApplicationSqlClient(env.PROBE_DO, namespace)
  const before = await client.query<{ balance: number }>(
    "SELECT balance FROM accounts WHERE id = 'primary'"
  )
  try {
    const result = await client.transaction(
      compileTransactionQuery,
      async (tx) => {
        const account = await tx.queryAst<{ balance: number; entries: unknown[] }>(
          transactionQueryAst,
          transactionQueryFormat,
          `applicationRpc${action}`
        )
        const execResult = await tx.exec(
          "UPDATE accounts SET balance = balance + ? WHERE id = 'primary'",
          [11],
          {
            table: 'accounts',
            publicTable: 'public.account',
            kind: 'update',
          }
        )
        if (action === 'rollback') throw new Error('intentional application RPC rollback')
        return { account, execResult }
      },
      { maxSelects: 8, maxRows: 20 }
    )
    const after = await client.query<{ balance: number }>(
      "SELECT balance FROM accounts WHERE id = 'primary'"
    )
    return json({ ok: true, before, after, result })
  } catch (error) {
    const after = await client.query<{ balance: number }>(
      "SELECT balance FROM accounts WHERE id = 'primary'"
    )
    return json({ ok: false, error: String(error), before, after }, 409)
  }
}

async function runApplicationOverlapProbe(
  env: Env,
  namespace: string
): Promise<Response> {
  const firstClient = createApplicationSqlClient(env.PROBE_DO, namespace)
  const secondClient = createApplicationSqlClient(env.PROBE_DO, namespace)
  const target = env.PROBE_DO.get(env.PROBE_DO.idFromName(namespace))
  await firstClient.exec(
    'CREATE TABLE IF NOT EXISTS _zero_schema_tables (name TEXT PRIMARY KEY, schema_json TEXT NOT NULL)'
  )
  await firstClient.exec(
    'INSERT OR REPLACE INTO _zero_schema_tables (name, schema_json) VALUES (?, ?)',
    [
      'accounts',
      JSON.stringify({
        columns: transactionQuerySchema.tables.account.columns,
        primaryKey: transactionQuerySchema.tables.account.primaryKey,
      }),
    ]
  )

  let releaseFirst = () => {}
  const firstCanFinish = new Promise<void>((resolve) => {
    releaseFirst = resolve
  })
  let markFirstWrite = () => {}
  const firstWriteFinished = new Promise<void>((resolve) => {
    markFirstWrite = resolve
  })
  const first = firstClient
    .transaction(compileTransactionQuery, async (tx) => {
      await tx.exec(
        "UPDATE accounts SET balance = balance + ? WHERE id = 'primary'",
        [100],
        {
          table: 'accounts',
          publicTable: 'public.account',
          kind: 'update',
        }
      )
      markFirstWrite()
      await firstCanFinish
      throw new Error('intentional overlapping application rollback')
    })
    .then(
      () => ({ ok: true }),
      (error) => ({ ok: false, error: String(error) })
    )
  await firstWriteFinished

  let snapshotSettled = false
  const snapshot = target
    .fetch(new Request('https://orez-probe.local/snapshot'))
    .then(async (response) => {
      snapshotSettled = true
      return { status: response.status, body: await response.json() }
    })
  let secondSettled = false
  const second = secondClient
    .transaction(compileTransactionQuery, async (tx) => {
      await tx.exec(
        "UPDATE accounts SET balance = balance + ? WHERE id = 'primary'",
        [5],
        {
          table: 'accounts',
          publicTable: 'public.account',
          kind: 'update',
        }
      )
    })
    .then(
      () => {
        secondSettled = true
        return { ok: true }
      },
      (error) => {
        secondSettled = true
        return { ok: false, error: String(error) }
      }
    )

  await new Promise((resolve) => setTimeout(resolve, 40))
  const waitedForFirst = !snapshotSettled && !secondSettled
  releaseFirst()
  const [firstResult, snapshotResult, secondResult] = await Promise.all([
    first,
    snapshot,
    second,
  ])
  const after = await firstClient.query<{ balance: number }>(
    "SELECT balance FROM accounts WHERE id = 'primary'"
  )
  let snapshotBalance: unknown
  if (snapshotResult.body && typeof snapshotResult.body === 'object') {
    const tables = Reflect.get(snapshotResult.body, 'tables')
    if (tables && typeof tables === 'object') {
      const accounts = Reflect.get(tables, 'accounts')
      if (Array.isArray(accounts) && accounts[0] && typeof accounts[0] === 'object') {
        snapshotBalance = Reflect.get(accounts[0], 'balance')
      }
    }
  }

  return json({
    waitedForFirst,
    firstResult,
    snapshotStatus: snapshotResult.status,
    snapshotBalance,
    secondResult,
    after,
  })
}

type AdmissionRecord = {
  label: string
  lane: 'read' | 'write'
  waitMs: number
  ok: boolean
}

/**
 * Drive a mixed application-SQL load through the real client and report how the
 * Durable Object admitted it.
 *
 * `order` is recorded when each session's transaction body starts, which is the
 * instant it owns its turn, so comparing it to the launch order measures
 * admission fairness directly rather than inferring it from latency. The holder
 * counters bracket admission to settlement, so `maxConcurrent` is the real
 * number of sessions inside the database at once.
 */
async function runApplicationAdmissionProbe(
  env: Env,
  namespace: string,
  url: URL
): Promise<Response> {
  const count = (name: string, fallback: number) => {
    const value = Number(url.searchParams.get(name))
    return Number.isFinite(value) && value >= 0 ? value : fallback
  }
  const writers = count('writers', 6)
  const readers = count('readers', 0)
  const cancels = count('cancels', 0)
  const holdMs = count('holdMs', 60)
  const staggerMs = count('staggerMs', 20)
  const readLane = url.searchParams.get('readLane') !== '0'

  const client = createApplicationSqlClient(env.PROBE_DO, namespace)
  const order: string[] = []
  const launched: string[] = []
  const records: AdmissionRecord[] = []
  const open = { read: 0, write: 0 }
  const maxConcurrent = { read: 0, write: 0 }
  let readerSawWriter = false
  let writerSawReader = false

  const session = (label: string, lane: 'read' | 'write', signal?: AbortSignal) => {
    const startedAt = Date.now()
    launched.push(label)
    const scoped = signal
      ? createApplicationSqlClient(env.PROBE_DO, namespace, { signal })
      : client
    const work = async (tx: {
      query: (sql: string) => Promise<unknown>
      exec: (sql: string, params: unknown[], metadata: unknown) => Promise<unknown>
    }) => {
      order.push(label)
      records.push({ label, lane, waitMs: Date.now() - startedAt, ok: true })
      // the holder window closes when the body ends, not when the client's
      // commit response lands: the object releases the turn inside commit, so
      // the next session is legitimately admitted while this one's commit reply
      // is still in flight.
      open[lane]++
      maxConcurrent[lane] = Math.max(maxConcurrent[lane], open[lane])
      if (lane === 'read' && open.write > 0) readerSawWriter = true
      if (lane === 'write' && open.read > 0) writerSawReader = true
      try {
        await tx.query("SELECT balance FROM accounts WHERE id = 'primary'")
        if (lane === 'write') {
          await tx.exec(
            "UPDATE accounts SET balance = balance + 1 WHERE id = 'primary'",
            [],
            { table: 'accounts', publicTable: 'public.account', kind: 'update' }
          )
        }
        await new Promise((resolve) => setTimeout(resolve, holdMs))
      } finally {
        open[lane]--
      }
    }
    const pending =
      lane === 'read' && readLane
        ? scoped.readTransaction(compileTransactionQuery, work)
        : scoped.transaction(compileTransactionQuery, work)
    return pending.then(
      () => true,
      () => {
        records.push({ label, lane, waitMs: Date.now() - startedAt, ok: false })
        return false
      }
    )
  }

  const pending: Promise<boolean>[] = []
  const total = writers + readers + cancels
  let writerIndex = 0
  let readerIndex = 0
  let cancelIndex = 0
  for (let step = 0; step < total; step++) {
    // interleave the lanes so arrival order is not lane order
    const wantRead = readerIndex < readers && (step % 2 === 1 || writerIndex >= writers)
    const wantCancel =
      !wantRead && cancelIndex < cancels && step % 3 === 2 && writerIndex >= 1
    if (wantCancel) {
      const controller = new AbortController()
      pending.push(session(`cancel-${cancelIndex++}`, 'write', controller.signal))
      setTimeout(() => controller.abort(), Math.max(1, Math.floor(staggerMs / 2)))
    } else if (wantRead) {
      pending.push(session(`read-${readerIndex++}`, 'read'))
    } else if (writerIndex < writers) {
      pending.push(session(`write-${writerIndex++}`, 'write'))
    } else if (readerIndex < readers) {
      pending.push(session(`read-${readerIndex++}`, 'read'))
    } else {
      const controller = new AbortController()
      pending.push(session(`cancel-${cancelIndex++}`, 'write', controller.signal))
      setTimeout(() => controller.abort(), Math.max(1, Math.floor(staggerMs / 2)))
    }
    if (staggerMs > 0) await new Promise((resolve) => setTimeout(resolve, staggerMs))
  }
  await Promise.all(pending)

  // an inversion is a session that was admitted before one that arrived earlier
  const arrivalRank = new Map(launched.map((label, index) => [label, index]))
  const admittedRanks = order.map((label) => arrivalRank.get(label) ?? -1)
  let inversions = 0
  for (let i = 0; i < admittedRanks.length; i++) {
    for (let j = i + 1; j < admittedRanks.length; j++) {
      if (admittedRanks[j] < admittedRanks[i]) inversions++
    }
  }
  const admittedWaits = records
    .filter((record) => record.ok)
    .map((record) => record.waitMs)
    .sort((left, right) => left - right)
  const percentile = (fraction: number) =>
    admittedWaits.length === 0
      ? 0
      : admittedWaits[
          Math.min(admittedWaits.length - 1, Math.floor(fraction * admittedWaits.length))
        ]
  const readWaits = records
    .filter((record) => record.ok && record.lane === 'read')
    .map((record) => record.waitMs)
    .sort((left, right) => left - right)

  const balance = await client.query<{ balance: number }>(
    "SELECT balance FROM accounts WHERE id = 'primary'"
  )
  const target = env.PROBE_DO.get(env.PROBE_DO.idFromName(namespace))
  return json({
    residue: await target.applicationAdmissionResidue(),
    launched,
    order,
    inversions,
    admitted: order.length,
    canceled: records.filter((record) => !record.ok).length,
    maxConcurrentReads: maxConcurrent.read,
    maxConcurrentWrites: maxConcurrent.write,
    readerSawWriter,
    writerSawReader,
    waitMs: {
      p50: percentile(0.5),
      p95: percentile(0.95),
      max: admittedWaits[admittedWaits.length - 1] ?? 0,
      readP95:
        readWaits.length === 0
          ? 0
          : readWaits[
              Math.min(readWaits.length - 1, Math.floor(0.95 * readWaits.length))
            ],
    },
    balance,
  })
}

async function runApplicationCancellationProbe(
  env: Env,
  namespace: string,
  action: 'hold' | 'queued' | 'active' | 'status' | 'release' | 'verify'
): Promise<Response> {
  const target = env.PROBE_DO.get(env.PROBE_DO.idFromName(namespace))
  if (action === 'status') return json(await target.applicationCancellationStatus())
  if (action === 'release') {
    await target.applicationCancellationRelease()
    return json({ ok: true })
  }
  const cancellationController =
    action === 'queued' || action === 'active' ? new AbortController() : undefined
  const client = createApplicationSqlClient(env.PROBE_DO, namespace, {
    signal: cancellationController?.signal,
  })
  if (action === 'verify') {
    await client.exec(
      'CREATE TABLE IF NOT EXISTS _zero_schema_tables (name TEXT PRIMARY KEY, schema_json TEXT NOT NULL)'
    )
    await client.exec(
      'INSERT OR REPLACE INTO _zero_schema_tables (name, schema_json) VALUES (?, ?)',
      [
        'accounts',
        JSON.stringify({
          columns: transactionQuerySchema.tables.account.columns,
          primaryKey: transactionQuerySchema.tables.account.primaryKey,
        }),
      ]
    )
    await client.transaction(compileTransactionQuery, (tx) =>
      tx.exec("UPDATE accounts SET balance = balance + 5 WHERE id = 'primary'", [], {
        table: 'accounts',
        publicTable: 'public.account',
        kind: 'update',
      })
    )
    const direct = await client.query<{ balance: number }>(
      "SELECT balance FROM accounts WHERE id = 'primary'"
    )
    const snapshotResponse = await target.fetch(
      new Request('https://orez-probe.local/snapshot')
    )
    return json({
      direct,
      snapshotStatus: snapshotResponse.status,
      snapshot: await snapshotResponse.json(),
    })
  }

  await target.applicationCancellationMark(action)
  if (action === 'queued') {
    setTimeout(() => cancellationController?.abort(), 40)
  }
  try {
    await client.transaction(compileTransactionQuery, async (tx) => {
      await tx.exec(
        "UPDATE accounts SET balance = balance + 100 WHERE id = 'primary'",
        [],
        {
          table: 'accounts',
          publicTable: 'public.account',
          kind: 'update',
        }
      )
      if (action === 'queued') return
      if (action === 'hold') {
        await target.applicationCancellationWait()
        throw new Error('intentional held transaction rollback')
      }
      await target.applicationCancellationMark('active-active')
      setTimeout(() => cancellationController?.abort(), 25)
      await new Promise((resolve) => setTimeout(resolve, 60_000))
    })
    return json({ ok: true })
  } catch (error) {
    return json({ ok: false, error: String(error) })
  }
}

export class ProbeDurableObject extends ZeroDO {
  readonly #db: SqlStorageSyncDb
  #bootID = crypto.randomUUID()
  #lastRequestAt = 0
  #reinstantiations = 0
  #effects: Array<DeferredEffect & { observedCommitted: boolean }> = []
  #applicationCancellationStages = new Set<string>()
  #applicationCancellationRelease: (() => void) | undefined

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env as never)
    this.#db = new SqlStorageSyncDb(ctx.storage.sql)
    ctx.storage.transactionSync(() => init_probe_schema(this.#db))
  }

  #maybeReinstantiate(now: number): void {
    if (this.#lastRequestAt > 0 && now - this.#lastRequestAt >= IDLE_TEARDOWN_MS) {
      this.#bootID = crypto.randomUUID()
      this.#effects = []
      this.#reinstantiations++
    }
    this.#lastRequestAt = now
  }

  #state(): Record<string, unknown> {
    const row = this.ctx.storage.sql
      .exec(
        `SELECT
          CAST((SELECT lmid FROM probe_state WHERE singleton = 1) AS TEXT) AS lmid,
          (SELECT balance FROM accounts WHERE id = 'primary') AS balance,
          (SELECT COUNT(*) FROM ledger) AS ledgerCount,
          (SELECT COUNT(*) FROM outbox) AS outboxCount,
          (SELECT COUNT(*) FROM mutation_log) AS mutationCount,
          (SELECT COUNT(*) FROM value_probe) AS valueCount`
      )
      .one()
    return {
      ...row,
      sideEffectCount: this.#effects.length,
      sideEffects: this.#effects,
    }
  }

  /** admission state read from inside the object, with no RPC skew */
  applicationAdmissionResidue(): { writer: boolean; readers: number; queued: number } {
    return {
      writer: Reflect.get(this, 'applicationSqlWriter') !== null,
      readers: (Reflect.get(this, 'applicationSqlReaders') as Set<unknown>).size,
      queued: (Reflect.get(this, 'applicationSqlQueue') as unknown[]).length,
    }
  }

  applicationCancellationMark(stage: string): void {
    this.#applicationCancellationStages.add(stage)
  }

  applicationCancellationStatus(): { stages: string[]; activeSession: boolean } {
    return {
      stages: [...this.#applicationCancellationStages],
      activeSession:
        Reflect.get(this, 'applicationSqlWriter') !== null ||
        (Reflect.get(this, 'applicationSqlReaders') as Set<unknown>).size > 0,
    }
  }

  async applicationCancellationWait(): Promise<void> {
    this.#applicationCancellationStages.add('hold-active')
    await new Promise<void>((resolve) => {
      this.#applicationCancellationRelease = resolve
    })
  }

  applicationCancellationRelease(): void {
    this.#applicationCancellationRelease?.()
    this.#applicationCancellationRelease = undefined
  }

  async #mutate(name: MutatorName, deferred: DeferredEffect[]): Promise<void> {
    if (name === 'read-then-write') {
      const { balance } = this.ctx.storage.sql
        .exec("SELECT balance FROM accounts WHERE id = 'primary'")
        .one() as { balance: number }
      await Promise.resolve()
      this.ctx.storage.sql.exec(
        "UPDATE accounts SET balance = ? WHERE id = 'primary'",
        balance + 10
      )
      this.ctx.storage.sql.exec(
        'INSERT INTO ledger (account_id, amount, note) VALUES (?, ?, ?)',
        'primary',
        10,
        'read-then-write'
      )
      deferred.push({ mutationID: '', kind: 'balance-notification' })
      return
    }

    if (name === 'multi-table') {
      this.ctx.storage.sql.exec(
        "UPDATE accounts SET balance = balance - 5 WHERE id = 'primary'"
      )
      await Promise.resolve()
      this.ctx.storage.sql.exec(
        'INSERT INTO ledger (account_id, amount, note) VALUES (?, ?, ?)',
        'primary',
        -5,
        'multi-table'
      )
      this.ctx.storage.sql.exec(
        'INSERT INTO outbox (topic, payload) VALUES (?, ?)',
        'account.changed',
        JSON.stringify({ id: 'primary', delta: -5 })
      )
      deferred.push({ mutationID: '', kind: 'outbox-notification' })
      return
    }

    const { balance } = this.ctx.storage.sql
      .exec("SELECT balance FROM accounts WHERE id = 'primary'")
      .one() as { balance: number }
    this.ctx.storage.sql.exec(
      "UPDATE accounts SET balance = ? WHERE id = 'primary'",
      balance + 777
    )
    await Promise.resolve()
    this.ctx.storage.sql.exec(
      'INSERT INTO ledger (account_id, amount, note) VALUES (?, ?, ?)',
      'primary',
      777,
      'application-error'
    )
    deferred.push({ mutationID: '', kind: 'must-not-run' })
    throw new Error('intentional application mutator error')
  }

  async #push(name: MutatorName, mutationID: string): Promise<Response> {
    this.#db.resetStats()
    const deferred: DeferredEffect[] = []
    const started = performance.now()
    let lmid: string | undefined
    let wasmMs = 0
    try {
      await this.ctx.storage.transaction(async () => {
        let wasmStarted = performance.now()
        const expected = push_preflight(this.#db, mutationID)
        wasmMs += performance.now() - wasmStarted

        // application transaction methods resolve through the microtask queue. Cross that
        // same boundary here without admitting timers or other external work
        // into the storage transaction.
        await Promise.resolve()
        await this.#mutate(name, deferred)

        wasmStarted = performance.now()
        lmid = push_finalize(this.#db, mutationID, expected)
        wasmMs += performance.now() - wasmStarted
      })
    } catch (error) {
      return json(
        {
          ok: false,
          error: String(error),
          awaitedInsideTransaction: true,
          state: this.#state(),
          effectsDeferredButNotRun: deferred.length,
        },
        409
      )
    }

    // External effects are not even inspected until the transaction promise
    // resolves. Each effect independently verifies its mutation is durable.
    for (const effect of deferred) {
      effect.mutationID = mutationID
      const committed =
        this.ctx.storage.sql
          .exec(
            'SELECT 1 AS committed FROM mutation_log WHERE mutation_id = ?',
            mutationID
          )
          .toArray().length === 1
      this.#effects.push({ ...effect, observedCommitted: committed })
    }

    return json({
      ok: true,
      lmid,
      awaitedInsideTransaction: true,
      state: this.#state(),
      timing: {
        elapsedMs: performance.now() - started,
        wasmMs,
        ...this.#db.stats,
      },
    })
  }

  async fetch(request: Request): Promise<Response> {
    this.#maybeReinstantiate(Date.now())
    const url = new URL(request.url)
    if (url.pathname === '/snapshot') return super.fetch(request)
    const [, , ...routeParts] = url.pathname.split('/')
    const route = `/${routeParts.join('/')}`

    if (route === '/status') {
      return json({
        bootID: this.#bootID,
        idleTeardownMs: IDLE_TEARDOWN_MS,
        reinstantiations: this.#reinstantiations,
        state: this.#state(),
      })
    }

    if (route === '/pull') {
      this.#db.resetStats()
      const started = performance.now()
      const snapshot = this.ctx.storage.transactionSync(() => pull_snapshot(this.#db))
      return json({
        snapshot,
        transaction: 'transactionSync',
        timing: { elapsedMs: performance.now() - started, ...this.#db.stats },
      })
    }

    if (route.startsWith('/push/')) {
      const name = route.slice('/push/'.length) as MutatorName
      if (!['read-then-write', 'multi-table', 'application-error'].includes(name)) {
        return json({ error: 'unknown mutator' }, 404)
      }
      const body = (await request.json()) as { mutationID: string }
      return this.#push(name, body.mutationID)
    }

    if (route === '/js-exception') {
      const before = this.#state()
      try {
        await this.ctx.storage.transaction(async () => {
          const expected = push_preflight(this.#db, 'js-exception')
          this.ctx.storage.sql.exec(
            "UPDATE accounts SET balance = balance + 123 WHERE id = 'primary'"
          )
          await Promise.resolve()
          this.ctx.storage.sql.exec(
            'INSERT INTO outbox (topic, payload) VALUES (?, ?)',
            'must.rollback',
            '{}'
          )
          push_finalize(this.#db, 'js-exception', expected)
          throw new Error('intentional JS exception after finalization')
        })
      } catch (error) {
        return json(
          { ok: false, error: String(error), before, after: this.#state() },
          409
        )
      }
      return json({ error: 'JS exception did not escape transaction' }, 500)
    }

    if (route === '/rust-panic') {
      const before = this.#state()
      try {
        this.ctx.storage.transactionSync(() => rust_panic_after_writes(this.#db))
      } catch (error) {
        return json(
          { ok: false, error: String(error), before, after: this.#state() },
          409
        )
      }
      return json({ error: 'Rust panic did not escape transaction' }, 500)
    }

    if (route === '/values') {
      const input = await request.json()
      const output = this.ctx.storage.transactionSync(() =>
        value_round_trip(this.#db, input)
      )
      return json(output)
    }

    // canonical primary-key encoding, asserted against the same fixture the
    // native build asserts. no db needed: the encoder is pure.
    if (route === '/canonical-pk') {
      const input = (await request.json()) as { primaryKey: string[]; pk: unknown }
      return json({ encoded: canonical_pk_probe(input) })
    }

    if (route === '/adapter-guard') {
      const errors: string[] = []
      for (const [sql, params] of [
        ['BEGIN TRANSACTION', []],
        ['SELECT ?1', [{ kind: 'integer', value: '1' }]],
      ] as const) {
        try {
          this.#db.exec(sql, [...params] as never)
        } catch (error) {
          errors.push(String(error))
        }
      }
      return json({ errors })
    }

    if (route === '/transaction-query') {
      let plan: ReturnType<typeof compileTransactionQuery> | undefined
      const tx = new SqlStorageMutatorTransaction(
        new SqlStorageDirect(this.ctx.storage.sql),
        (queryAst, queryFormat) => {
          plan = compileTransactionQuery(queryAst, queryFormat)
          return plan
        }
      )
      const result = await tx.queryAst(
        transactionQueryAst,
        transactionQueryFormat,
        'platformTransactionQuery'
      )
      const execResult = await tx.exec(
        "UPDATE accounts SET balance = balance WHERE id = 'primary'"
      )
      let malformedFormatStatus: unknown
      try {
        await tx.queryAst(transactionQueryAst, undefined as never)
      } catch (error) {
        malformedFormatStatus = (error as { status?: unknown }).status
      }
      return json({ result, execResult, malformedFormatStatus, plan })
    }

    if (route === '/application-transaction-query') {
      const result = await this.runApplicationTransaction(
        compileTransactionQuery,
        (tx) =>
          tx.queryAst(
            transactionQueryAst,
            transactionQueryFormat,
            'applicationTransactionQuery'
          ),
        { maxSelects: 8, maxRows: 20 }
      )
      return json({ result })
    }

    if (route === '/application-transaction-query-budget') {
      try {
        await this.runApplicationTransaction(
          compileTransactionQuery,
          (tx) =>
            tx.queryAst(
              transactionQueryAst,
              transactionQueryFormat,
              'budgetedApplicationTransactionQuery'
            ),
          { maxSelects: 1 }
        )
      } catch (error) {
        return json(
          {
            code: Reflect.get(error as object, 'code'),
            query: Reflect.get(error as object, 'query'),
            selects: Reflect.get(error as object, 'selects'),
          },
          409
        )
      }
      return json({ error: 'query budget did not abort the transaction' }, 500)
    }

    return json({ error: 'not found', route }, 404)
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    const [, first, second, third] = url.pathname.split('/')
    if (first === '_application-rpc') {
      if (
        !second ||
        (third !== 'commit' && third !== 'rollback' && third !== 'overlap')
      ) {
        return json({ error: 'unknown application RPC probe' }, 404)
      }
      if (third === 'overlap') return runApplicationOverlapProbe(env, second)
      return runApplicationRpcProbe(env, second, third)
    }
    if (first === '_application-admission') {
      if (!second) return json({ error: 'unknown application admission probe' }, 404)
      return runApplicationAdmissionProbe(env, second, url)
    }
    if (first === '_application-cancellation') {
      if (!second) return json({ error: 'unknown application cancellation probe' }, 404)
      switch (third) {
        case 'hold':
        case 'queued':
        case 'active':
        case 'status':
        case 'release':
        case 'verify':
          return runApplicationCancellationProbe(env, second, third)
        default:
          return json({ error: 'unknown application cancellation probe' }, 404)
      }
    }
    const namespace = first
    if (!namespace) return new Response('orez rust sync M0 probe')
    return env.PROBE_DO.get(env.PROBE_DO.idFromName(namespace)).fetch(request)
  },
} satisfies ExportedHandler<Env>
