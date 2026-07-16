import { createQueryCompiler } from 'orez-sync-cf-host/query-compiler'

import { ZeroDO } from '../../../src/cf-do/worker.js'
import {
  SqlStorageDirect,
  SqlStorageMutatorTransaction,
  SqlStorageSyncDb,
} from './sql-storage-adapter.js'
import {
  init_probe_schema,
  pull_snapshot,
  push_finalize,
  push_preflight,
  rust_panic_after_writes,
  value_round_trip,
} from './wasm-platform.js'

import type { TransactionQueryFormat } from './transaction-query.js'
import type { ZeroSchemaConfig } from './types.js'

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

export class ProbeDurableObject extends ZeroDO {
  readonly #db: SqlStorageSyncDb
  #bootID = crypto.randomUUID()
  #lastRequestAt = 0
  #reinstantiations = 0
  #effects: Array<DeferredEffect & { observedCommitted: boolean }> = []

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

        // MutatorSql methods resolve through the microtask queue. Cross that
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
      let malformedFormatStatus: unknown
      try {
        await tx.queryAst(transactionQueryAst, undefined as never)
      } catch (error) {
        malformedFormatStatus = (error as { status?: unknown }).status
      }
      return json({ result, malformedFormatStatus, plan })
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
  fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url)
    const [, namespace] = url.pathname.split('/')
    if (!namespace) return Promise.resolve(new Response('orez rust sync M0 probe'))
    return env.PROBE_DO.get(env.PROBE_DO.idFromName(namespace)).fetch(request)
  },
} satisfies ExportedHandler<Env>
