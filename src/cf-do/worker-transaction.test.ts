import { describe, expect, it, vi } from 'vitest'

vi.mock('cloudflare:workers', () => ({ DurableObject: class {}, RpcTarget: class {} }))

type TransactionWork<T> = () => T | Promise<T>

function createSqlStorage() {
  const rows = [{ id: 'row-1', enabled: 1 }]
  let lastSql = ''
  let lastParams: unknown[] = []
  let cursorConsumed = false
  const sql = {
    exec(statement: string, ...params: unknown[]) {
      lastSql = statement
      lastParams = params
      if (/^\s*select changes\(\)/i.test(statement)) {
        return {
          columnNames: ['changes'],
          one: () => ({ changes: 1 }),
          toArray: () => [{ changes: 1 }],
        }
      }
      const selected = /^\s*select/i.test(statement)
      return {
        columnNames: selected ? ['id', 'enabled'] : [],
        rowsWritten: selected ? 0 : 1,
        toArray() {
          cursorConsumed = true
          return selected ? rows : []
        },
      }
    },
  }
  return {
    get cursorConsumed() {
      return cursorConsumed
    },
    get lastParams() {
      return lastParams
    },
    get lastSql() {
      return lastSql
    },
    resetCursor() {
      cursorConsumed = false
    },
    sql,
  }
}

async function createTestZero(transaction: <T>(work: TransactionWork<T>) => Promise<T>) {
  const { ZeroDO } = await import('./worker.js')
  class TestZeroDO extends ZeroDO {
    runTrustedTransaction<T>(
      compileQuery: any,
      work: any,
      queryBudget?: any
    ): Promise<T> {
      return this.runApplicationTransaction(compileQuery, work, queryBudget)
    }
  }
  const storage = createSqlStorage()
  const zero = Object.create(TestZeroDO.prototype) as TestZeroDO & Record<string, any>
  zero.sql = storage.sql
  zero.cdc = {
    active: false,
    beginSchemaChange: () => null,
    capturesTable: () => false,
    drain: () => [],
    ensureTable: vi.fn(() => true),
    finishSchemaChange() {},
    invalidateSchema() {},
    reload() {},
  }
  zero.watermarks = { invalidateCache() {} }
  zero.writeBudget = { recordLogical() {} }
  zero.writeBudgetDisabled = true
  zero.tableSchemas = new Map()
  zero.schemaTables = new Set<string>()
  zero.pendingChangesSchemaReady = false
  zero.activeApplicationSqlSession = null
  zero.applicationSqlDidCommit = () => {}
  zero.ctx = { storage: { transaction } }
  return { storage, zero }
}

const unusedCompiler = () => {
  throw new Error('query compiler should not run')
}

const pluralFormat = { singular: false, relationships: {} }

function flatPlan() {
  return {
    rootTable: 'item',
    planHash: '0123456789abcdef',
    root: {
      table: 'item',
      singular: false,
      sql: 'SELECT id, enabled FROM item WHERE id = ?',
      bindings: [{ kind: 'literal', value: { kind: 'text', value: 'row-1' } }],
      columns: [
        { name: 'id', columnType: 'string' },
        { name: 'enabled', columnType: 'boolean' },
      ],
      relationships: [],
    },
  }
}

function relatedPlan() {
  const plan = flatPlan()
  plan.root.relationships = [
    {
      name: 'children',
      node: {
        table: 'item',
        singular: false,
        sql: 'SELECT id, enabled FROM item WHERE id = ?',
        bindings: [{ kind: 'parent_field', field: 'id' }],
        columns: plan.root.columns,
        relationships: [],
      },
    },
  ]
  return plan
}

describe('ZeroDO trusted application transaction', () => {
  it('binds the private application client to one Durable Object namespace', async () => {
    const { createApplicationSqlClient } = await import('./application-sql.js')
    const calls: unknown[] = []
    const target = {
      applicationSqlSession: async () => ({
        [Symbol.dispose]() {},
        begin: async () => true,
        query: async (sql: string, params: readonly unknown[]) => {
          calls.push(['query', sql, params])
          return [{ id: 'row-1' }]
        },
        exec: async (sql: string, params: readonly unknown[], metadata: unknown) => {
          calls.push(['exec', sql, params, metadata])
          return { changes: 1 }
        },
        queryPlan: async () => [],
        registerTables: async () => {},
        commit: async () => {},
        rollback: async () => {},
      }),
    }
    const client = createApplicationSqlClient(
      {
        idFromName: (namespace) => `id:${namespace}`,
        get: (id) => {
          calls.push(['get', id])
          return target
        },
      },
      'proj-123'
    )

    await client.query('SELECT id FROM item WHERE id = ?', ['row-1'])
    const execResult = await client.exec(
      'UPDATE item SET enabled = ? WHERE id = ?',
      [1, 'row-1'],
      {
        table: 'item',
        publicTable: 'public.item',
        kind: 'update',
      }
    )

    expect(client.namespace).toBe('proj-123')
    expect(execResult).toEqual({ changes: 1 })
    expect(calls).toEqual([
      ['get', 'id:proj-123'],
      ['query', 'SELECT id FROM item WHERE id = ?', ['row-1']],
      [
        'exec',
        'UPDATE item SET enabled = ? WHERE id = ?',
        [1, 'row-1'],
        { table: 'item', publicTable: 'public.item', kind: 'update' },
      ],
    ])
  })

  it('materializes rows before returning a promise and runs effects after commit', async () => {
    const events: string[] = []
    const { storage, zero } = await createTestZero(async (work) => {
      events.push('transaction')
      const value = await work()
      events.push('commit')
      return value
    })

    const result = await zero.runTrustedTransaction(unusedCompiler, async (tx, ctx) => {
      storage.resetCursor()
      const pendingRows = tx.query('SELECT id, enabled FROM item WHERE id = ?', ['row-1'])
      expect(storage.cursorConsumed).toBe(true)
      const rows = await pendingRows
      events.push('work')
      ctx.defer(() => events.push('effect'))
      return rows
    })

    expect(result).toEqual([{ id: 'row-1', enabled: 1 }])
    expect(events).toEqual(['transaction', 'work', 'commit', 'effect'])
  })

  it('serializes application transactions without passing a callback to the Durable Object', async () => {
    const events: string[] = []
    const { createApplicationSqlClient } = await import('./application-sql.js')
    const target = {
      applicationSqlSession: async (sessionID: string) => ({
        [Symbol.dispose]() {},
        begin: async () => {
          events.push(`begin:${sessionID}`)
          return true
        },
        query: async () => [],
        exec: async () => {
          events.push('exec')
          return { changes: 1 }
        },
        queryPlan: async () => {
          events.push('queryAst')
          return [{ id: 'row-1', enabled: true }]
        },
        registerTables: async () => {},
        commit: async () => events.push('commit'),
        rollback: async () => events.push('rollback'),
      }),
    }
    const client = createApplicationSqlClient(
      { idFromName: () => 'id', get: () => target },
      'proj-123'
    )

    const result = await client.transaction(
      () => flatPlan(),
      async (tx, context) => {
        const rows = await tx.queryAst({ table: 'item' }, pluralFormat)
        const execResult = await tx.exec('UPDATE item SET enabled = ?', [1], {
          table: 'item',
          publicTable: 'public.item',
          kind: 'update',
        })
        expect(execResult).toEqual({ changes: 1 })
        context.defer(() => events.push('effect'))
        return rows
      }
    )

    expect(result).toEqual([{ id: 'row-1', enabled: true }])
    expect(events[0]).toMatch(/^begin:/)
    expect(events.slice(1)).toEqual(['queryAst', 'exec', 'commit', 'effect'])
  })

  it('leaves no server ownership behind for a disposed waiting session', async () => {
    const { zero } = await createTestZero(async (work) => await work())
    const owner = await zero.applicationSqlSession('owner')
    const canceled = await zero.applicationSqlSession('canceled')
    const next = await zero.applicationSqlSession('next')
    expect(await owner.begin()).toBe(true)

    expect(await canceled.begin()).toBe(false)
    canceled[Symbol.dispose]()
    expect(await next.begin()).toBe(false)
    zero.releaseApplicationSqlTurn(owner)
    expect(await next.begin()).toBe(true)

    await expect(next.query('SELECT id FROM item')).resolves.toEqual([
      { id: 'row-1', enabled: 1 },
    ])
    zero.releaseApplicationSqlTurn(next)
  })

  it('installs CDC from explicit SQLite write metadata', async () => {
    const { zero } = await createTestZero(async (work) => await work())
    const session = await zero.applicationSqlSession('cdc-metadata')
    await session.begin()

    const result = await session.exec(
      'INSERT INTO item (id, enabled) VALUES (?, ?)',
      ['row-1', 1],
      { table: 'item', publicTable: 'public.item', kind: 'upsert' }
    )

    expect(result).toEqual({ changes: 1 })
    expect(zero.cdc.ensureTable).toHaveBeenCalledWith({
      physicalTableName: 'item',
      tableName: 'public.item',
    })
  })

  it('compiles Zero ASTs and passes decoded bindings to SQLite', async () => {
    const { storage, zero } = await createTestZero(async (work) => await work())
    const ast = { table: 'item' }
    const compiler = vi.fn(() => flatPlan())

    const rows = await zero.runTrustedTransaction(compiler, (tx) =>
      tx.queryAst(ast, pluralFormat)
    )

    expect(rows).toEqual([{ id: 'row-1', enabled: true }])
    expect(compiler).toHaveBeenCalledWith(ast, pluralFormat)
    expect(storage.lastSql).toContain('FROM item')
    expect(storage.lastParams).toEqual(['row-1'])
  })

  it('discards effects from a retried storage-transaction attempt', async () => {
    let attempt = 0
    const ran: number[] = []
    const { zero } = await createTestZero(async (work) => {
      await work()
      return await work()
    })

    await zero.runTrustedTransaction(unusedCompiler, (_tx, ctx) => {
      attempt++
      const currentAttempt = attempt
      ctx.defer(() => ran.push(currentAttempt))
    })

    expect(attempt).toBe(2)
    expect(ran).toEqual([2])
  })

  it('logs a failed effect, continues, and preserves the committed result', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    const ran: string[] = []
    const { zero } = await createTestZero(async (work) => await work())

    const result = await zero.runTrustedTransaction(unusedCompiler, (_tx, ctx) => {
      ctx.defer(() => {
        throw new Error('webhook failed')
      })
      ctx.defer(() => ran.push('continued'))
      return 'committed'
    })

    expect(result).toBe('committed')
    expect(ran).toEqual(['continued'])
    expect(consoleError).toHaveBeenCalledWith(
      JSON.stringify({
        event: 'orez_do_external_effect_error',
        error: 'webhook failed',
      })
    )
    consoleError.mockRestore()
  })

  it('invalidates schema caches and drops effects when the transaction aborts', async () => {
    const ran: string[] = []
    const { zero } = await createTestZero(async (work) => await work())
    const invalidateWatermarks = vi.spyOn(zero.watermarks, 'invalidateCache')
    const reloadCdc = vi.spyOn(zero.cdc, 'reload')

    await expect(
      zero.runTrustedTransaction(unusedCompiler, (_tx, ctx) => {
        ctx.defer(() => ran.push('must not run'))
        throw new Error('abort')
      })
    ).rejects.toThrow('abort')

    expect(ran).toEqual([])
    expect(invalidateWatermarks).toHaveBeenCalledOnce()
    expect(reloadCdc).toHaveBeenCalledOnce()
  })

  it('forbids transaction-control SQL inside the executor', async () => {
    const { zero } = await createTestZero(async (work) => await work())

    await expect(
      zero.runTrustedTransaction(unusedCompiler, (tx) => tx.exec('BEGIN'))
    ).rejects.toThrow('transaction SQL is owned by ZeroDO')
  })

  it('aborts through atomically when a named query exceeds its select budget', async () => {
    const { zero } = await createTestZero(async (work) => await work())
    const invalidateWatermarks = vi.spyOn(zero.watermarks, 'invalidateCache')
    const reloadCdc = vi.spyOn(zero.cdc, 'reload')
    const relatedFormat = {
      singular: false,
      relationships: { children: { singular: false, relationships: {} } },
    }

    await expect(
      zero.runTrustedTransaction(
        () => relatedPlan(),
        (tx) => tx.queryAst({ table: 'item' }, relatedFormat, 'itemsWithChildren'),
        { maxSelects: 1 }
      )
    ).rejects.toMatchObject({
      code: 'transaction_query_budget_exceeded',
      query: 'itemsWithChildren',
      selects: 2,
    })
    expect(invalidateWatermarks).toHaveBeenCalledOnce()
    expect(reloadCdc).toHaveBeenCalledOnce()
  })

  it('does not expose private application SQL on the public fetch surface', async () => {
    const { ZeroDO } = await import('./worker.js')
    const zero = Object.create(ZeroDO.prototype) as ZeroDO

    const response = await zero.fetch(
      new Request('http://zero-do/_orez/application-sql', {
        method: 'POST',
      })
    )

    expect(response.status).toBe(404)
  })
})
