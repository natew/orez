import { describe, expect, it, vi } from 'vitest'

vi.mock('cloudflare:workers', () => ({ DurableObject: class {} }))

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
      const selected = /^\s*select/i.test(statement)
      return {
        columnNames: selected ? ['id', 'enabled'] : [],
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
    runTrustedTransaction<T>(compileQuery: any, work: any): Promise<T> {
      return this.runApplicationTransaction(compileQuery, work)
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
  zero.ctx = { storage: { transaction } }
  return { storage, zero }
}

const unusedCompiler = () => {
  throw new Error('query compiler should not run')
}

describe('ZeroDO trusted application transaction', () => {
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

  it('compiles Zero ASTs and passes decoded bindings to SQLite', async () => {
    const { storage, zero } = await createTestZero(async (work) => await work())
    const ast = { table: 'item' }
    const compiler = vi.fn(() => ({
      sql: 'SELECT id, enabled FROM item WHERE id = ?',
      params: ['row-1'],
    }))

    const rows = await zero.runTrustedTransaction(compiler, (tx) => tx.queryAst(ast))

    expect(rows).toEqual([{ id: 'row-1', enabled: 1 }])
    expect(compiler).toHaveBeenCalledWith(ast)
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

  it('does not expose the executor on the base public fetch surface', async () => {
    const { ZeroDO } = await import('./worker.js')
    const zero = Object.create(ZeroDO.prototype) as ZeroDO

    const response = await zero.fetch(
      new Request('http://zero-do/_orez/run-application-transaction', {
        method: 'POST',
      })
    )

    expect(response.status).toBe(404)
  })
})
