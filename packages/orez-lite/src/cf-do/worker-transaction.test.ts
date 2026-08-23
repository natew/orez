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
  zero.writeBudget = {
    recordLogical() {},
    status: () => ({
      state: 'open',
      budget: 150_000,
      windowMs: 300_000,
      windowRows: 0,
      billableRows: 0,
      logicalRows: 0,
      trippedAt: null,
    }),
  }
  zero.writeBudgetDisabled = true
  zero.writeBudgetAdminToken = 'operator-token'
  zero.writeBudgetTripStatement = undefined
  zero.bootID = 'test-boot'
  zero.bootedAt = Date.now()
  zero.requestsSinceBoot = {
    fetch: 0,
    applicationSqlSessions: 0,
    applicationSqlReadSessions: 0,
    applicationSqlWriteSessions: 0,
    sqlStatements: 0,
  }
  zero.sqlBillingSinceBoot = { rowsRead: 0, rowsWritten: 0 }
  zero.sqlTelemetrySampleRate = 0
  const writeGrantWaitSamples: number[] = []
  zero.writeGrantWaitMs = {
    record: (value: number) => writeGrantWaitSamples.push(value),
    status: () => ({
      observed: writeGrantWaitSamples.length,
      sampled: writeGrantWaitSamples.length,
      capacity: 4_096,
      p50: writeGrantWaitSamples[0] ?? null,
      p99: writeGrantWaitSamples.at(-1) ?? null,
      max: writeGrantWaitSamples.length ? Math.max(...writeGrantWaitSamples) : null,
    }),
  }
  zero.tableSchemas = new Map()
  zero.schemaTables = new Set<string>()
  zero.pendingChangesSchemaReady = false
  zero.applicationSqlWriter = null
  zero.applicationSqlReaders = new Set()
  zero.applicationSqlQueue = []
  zero.applicationSqlDidCommit = () => {}
  zero.ctx = {
    id: { toString: () => 'test-object-id' },
    storage: { sql: { databaseSize: 12_345 }, transaction },
  }
  return { storage, writeGrantWaitSamples, zero }
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
  it('samples named query and transaction timing without logging SQL values', async () => {
    const { zero } = await createTestZero(async (work) => await work())
    zero.sqlTelemetrySampleRate = 1
    const events: Array<Record<string, unknown>> = []
    const log = vi.spyOn(console, 'log').mockImplementation((message) => {
      events.push(JSON.parse(String(message)))
    })
    try {
      const session = await zero.applicationSqlSession('sampled')
      await session.begin()
      await expect(session.queryPlan(flatPlan(), 'item.byId')).resolves.toEqual([
        { id: 'row-1', enabled: true },
      ])
      await session.commit()

      const failed = await zero.applicationSqlSession('sampled-failure')
      await failed.begin()
      await expect(
        failed.queryPlan(relatedPlan(), 'item.tooMany', { maxSelects: 1 })
      ).rejects.toThrow('transaction_query_budget_exceeded')
      await failed.rollback()
    } finally {
      log.mockRestore()
    }

    expect(events).toHaveLength(4)
    expect(events[0]).toMatchObject({
      event: 'orez_sql_query_sample',
      name: 'item.byId',
      outcome: 'success',
      rowsReturned: 1,
      rowsChanged: 0,
      statements: 1,
      sampleRate: 1,
    })
    expect(events[1]).toMatchObject({
      event: 'orez_sql_transaction_sample',
      name: 'application_sql_write',
      outcome: 'committed',
      rowsReturned: 1,
      rowsChanged: 0,
      statements: 1,
      sampleRate: 1,
    })
    expect(events[2]).toMatchObject({
      event: 'orez_sql_query_sample',
      name: 'item.tooMany',
      outcome: 'error',
      errorName: 'TransactionQueryBudgetError',
    })
    expect(events[3]).toMatchObject({
      event: 'orez_sql_transaction_sample',
      outcome: 'rolled_back',
    })
    expect(events[0]?.durationMs).toEqual(expect.any(Number))
    expect(events[1]?.durationMs).toEqual(expect.any(Number))
    expect(events.every((event) => !Object.hasOwn(event, 'sql'))).toBe(true)
    expect(events.every((event) => !Object.hasOwn(event, 'params'))).toBe(true)
    expect(events.every((event) => !Object.hasOwn(event, 'namespace'))).toBe(true)
  })

  it('binds the private application client to one Durable Object namespace', async () => {
    const { createApplicationSqlClient } = await import('./application-sql.js')
    const calls: unknown[] = []
    const target = {
      applicationSqlQuery: async (
        sql: string,
        params: readonly unknown[],
        options: unknown
      ) => {
        calls.push(['query', sql, params, options])
        return [{ id: 'row-1' }]
      },
      applicationSqlSession: async (_sessionID: string, options: unknown) => ({
        [Symbol.dispose]() {},
        begin: async () => {},
        query: async () => [],
        exec: async (sql: string, params: readonly unknown[], metadata: unknown) => {
          calls.push(['exec', sql, params, metadata, options])
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
      'proj-123',
      { priority: 'latency-sensitive' }
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
      [
        'query',
        'SELECT id FROM item WHERE id = ?',
        ['row-1'],
        { priority: 'latency-sensitive' },
      ],
      [
        'exec',
        'UPDATE item SET enabled = ? WHERE id = ?',
        [1, 'row-1'],
        { table: 'item', publicTable: 'public.item', kind: 'update' },
        { priority: 'latency-sensitive' },
      ],
    ])
  })

  it('materializes rows before returning a promise and commits after work', async () => {
    const events: string[] = []
    const { storage, zero } = await createTestZero(async (work) => {
      events.push('transaction')
      const value = await work()
      events.push('commit')
      return value
    })

    const result = await zero.runTrustedTransaction(unusedCompiler, async (tx) => {
      storage.resetCursor()
      const pendingRows = tx.query('SELECT id, enabled FROM item WHERE id = ?', ['row-1'])
      expect(storage.cursorConsumed).toBe(true)
      const rows = await pendingRows
      events.push('work')
      return rows
    })

    expect(result).toEqual([{ id: 'row-1', enabled: 1 }])
    expect(events).toEqual(['transaction', 'work', 'commit'])
  })

  it('turns the structured background RPC outcome into a local preemption error', async () => {
    const { ApplicationSqlSessionPreemptedError, createApplicationSqlClient } =
      await import('./application-sql.js')
    const rollback = vi.fn(async () => undefined)
    const client = createApplicationSqlClient(
      {
        idFromName: () => 'id',
        get: () => ({
          applicationSqlQuery: async () => [],
          applicationSqlSession: async () => ({
            [Symbol.dispose]() {},
            begin: async () => undefined,
            query: async () => [],
            queryPreemptible: async () => ({ outcome: 'preempted' as const }),
            exec: async () => ({ changes: 0 }),
            queryPlan: async () => undefined,
            queryPlanPreemptible: async () => ({ outcome: 'preempted' as const }),
            registerTables: async () => undefined,
            commit: async () => undefined,
            commitPreemptible: async () => ({
              outcome: 'completed' as const,
              value: undefined,
            }),
            rollback,
          }),
        }),
      },
      'proj-123',
      { priority: 'background' }
    )

    await expect(
      client.readTransaction(
        () => flatPlan(),
        (tx) => tx.query('SELECT id FROM item')
      )
    ).rejects.toBeInstanceOf(ApplicationSqlSessionPreemptedError)
    expect(rollback).toHaveBeenCalledOnce()
  })

  it('serializes application transactions without passing a callback to the Durable Object', async () => {
    const events: string[] = []
    const { createApplicationSqlClient } = await import('./application-sql.js')
    const target = {
      applicationSqlQuery: async () => [],
      applicationSqlSession: async (sessionID: string) => ({
        [Symbol.dispose]() {},
        begin: async () => {
          events.push(`begin:${sessionID}`)
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
      async (tx) => {
        const rows = await tx.queryAst({ table: 'item' }, pluralFormat)
        const execResult = await tx.exec('UPDATE item SET enabled = ?', [1], {
          table: 'item',
          publicTable: 'public.item',
          kind: 'update',
        })
        expect(execResult).toEqual({ changes: 1 })
        return rows
      }
    )

    expect(result).toEqual([{ id: 'row-1', enabled: true }])
    expect(events[0]).toMatch(/^begin:/)
    expect(events.slice(1)).toEqual(['queryAst', 'exec', 'commit'])
  })

  it('leaves no server ownership behind for a disposed waiting session', async () => {
    const { zero } = await createTestZero(async (work) => await work())
    const owner = await zero.applicationSqlSession('owner')
    const canceled = await zero.applicationSqlSession('canceled')
    const next = await zero.applicationSqlSession('next')
    await owner.begin()

    let canceledAdmitted = false
    void canceled.begin().then(() => {
      canceledAdmitted = true
    })
    const nextAdmission = next.begin()
    canceled[Symbol.dispose]()
    zero.releaseApplicationSqlTurn(owner)
    await nextAdmission
    expect(canceledAdmitted).toBe(false)
    expect(zero.applicationSqlQueue).toEqual([])

    await expect(next.query('SELECT id FROM item')).resolves.toEqual([
      { id: 'row-1', enabled: 1 },
    ])
    zero.releaseApplicationSqlTurn(next)
  })

  it('admits waiting sessions in arrival order', async () => {
    const { zero } = await createTestZero(async (work) => await work())
    const owner = await zero.applicationSqlSession('owner')
    await owner.begin()

    const admitted: string[] = []
    const waiting = await Promise.all(
      ['first', 'second', 'third'].map((id) => zero.applicationSqlSession(id))
    )
    // deliberately release in reverse order so an unfair queue would show it
    const admissions = waiting.map((session, index) =>
      session.begin().then(() => {
        admitted.push(['first', 'second', 'third'][index])
        zero.releaseApplicationSqlTurn(session)
      })
    )

    zero.releaseApplicationSqlTurn(owner)
    await Promise.all(admissions)

    expect(admitted).toEqual(['first', 'second', 'third'])
  })

  it('reports per-object admission pressure only to an authenticated operator', async () => {
    const { writeGrantWaitSamples, zero } = await createTestZero(
      async (work) => await work()
    )
    const owner = await zero.applicationSqlSession('owner')
    await owner.begin()
    const next = await zero.applicationSqlSession('next')
    const nextAdmission = next.begin()
    await Promise.resolve()

    const forbidden = await zero.fetch(new Request('http://zero-do/_orez/status'))
    expect(forbidden.status).toBe(403)

    zero.releaseApplicationSqlTurn(owner)
    await nextAdmission
    expect(writeGrantWaitSamples).toHaveLength(2)

    const response = await zero.fetch(
      new Request('http://zero-do/_orez/status', {
        headers: {
          'x-orez-admin-token': 'operator-token',
          'x-orez-do-instance': 'ns:user-1',
        },
      })
    )
    expect(response.status).toBe(200)
    expect(await response.json()).toMatchObject({
      bootID: 'test-boot',
      ns: 'ns:user-1',
      objectId: 'test-object-id',
      databaseSizeBytes: 12_345,
      requestsSinceBoot: {
        fetch: 2,
        applicationSqlSessions: 2,
        applicationSqlWriteSessions: 2,
      },
      applicationSql: {
        activeReaders: 0,
        writerActive: true,
        queuedReaders: 0,
        queuedWriters: 0,
        writeGrantWaitMs: { observed: 2, sampled: 2 },
      },
      writeBudget: { enabled: false, state: 'open' },
    })
    zero.releaseApplicationSqlTurn(next)
  })

  it('admits latency-sensitive sessions before queued normal work', async () => {
    const { zero } = await createTestZero(async (work) => await work())
    const owner = await zero.applicationSqlSession('owner')
    await owner.begin()

    const normal = await zero.applicationSqlSession('normal')
    const urgentFirst = await zero.applicationSqlSession('urgent-first', {
      priority: 'latency-sensitive',
    })
    const urgentSecond = await zero.applicationSqlSession('urgent-second', {
      priority: 'latency-sensitive',
    })
    const admitted: string[] = []
    const admissions = [
      normal.begin().then(() => {
        admitted.push('normal')
        zero.releaseApplicationSqlTurn(normal)
      }),
      urgentFirst.begin().then(() => {
        admitted.push('urgent-first')
        zero.releaseApplicationSqlTurn(urgentFirst)
      }),
      urgentSecond.begin().then(() => {
        admitted.push('urgent-second')
        zero.releaseApplicationSqlTurn(urgentSecond)
      }),
    ]

    zero.releaseApplicationSqlTurn(owner)
    await Promise.all(admissions)

    expect(admitted).toEqual(['urgent-first', 'urgent-second', 'normal'])
  })

  it('runs read sessions together and excludes them from a write session', async () => {
    const { zero } = await createTestZero(async (work) => await work())
    const writer = await zero.applicationSqlSession('writer')
    await writer.begin()

    const readers = await Promise.all([
      zero.applicationSqlSession('read-a', { readOnly: true }),
      zero.applicationSqlSession('read-b', { readOnly: true }),
    ])
    const admitted: string[] = []
    const admissions = readers.map((session, index) =>
      session.begin().then(() => admitted.push(['read-a', 'read-b'][index]))
    )
    await Promise.resolve()
    expect(admitted).toEqual([])

    zero.releaseApplicationSqlTurn(writer)
    await Promise.all(admissions)
    expect(admitted).toEqual(['read-a', 'read-b'])

    const laterWriter = await zero.applicationSqlSession('later-writer')
    let laterWriterAdmitted = false
    const laterAdmission = laterWriter.begin().then(() => {
      laterWriterAdmitted = true
    })
    await Promise.resolve()
    expect(laterWriterAdmitted).toBe(false)

    zero.releaseApplicationSqlTurn(readers[0])
    await Promise.resolve()
    expect(laterWriterAdmitted).toBe(false)
    zero.releaseApplicationSqlTurn(readers[1])
    await laterAdmission
    zero.releaseApplicationSqlTurn(laterWriter)
  })

  it('preempts a background read session when a writer arrives', async () => {
    const { zero } = await createTestZero(async (work) => await work())
    const background = await zero.applicationSqlSession('background', {
      readOnly: true,
      priority: 'background',
    })
    await background.begin()

    const writer = await zero.applicationSqlSession('writer')
    void writer.begin().catch(() => {})
    await Promise.resolve()

    try {
      expect(background.state).toBe('preempted')
      expect(writer.state).toBe('active')
      await expect(background.queryPreemptible('SELECT id FROM item')).resolves.toEqual({
        outcome: 'preempted',
      })
    } finally {
      zero.releaseApplicationSqlTurn(background)
      zero.releaseApplicationSqlTurn(writer)
    }
  })

  it('refuses a mutation from a read session instead of escalating it', async () => {
    const { zero } = await createTestZero(async (work) => await work())
    const reader = await zero.applicationSqlSession('reader', { readOnly: true })
    await reader.begin()

    await expect(
      reader.exec("UPDATE item SET enabled = 1 WHERE id = 'row-1'")
    ).rejects.toThrow('read-only application SQLite session cannot execute a mutation')
    await expect(
      reader.registerTables([{ table: 'item', publicTable: 'public.item' }])
    ).rejects.toThrow('read-only application SQLite session cannot register tables')
    await expect(reader.query('SELECT id FROM item')).resolves.toEqual([
      { id: 'row-1', enabled: 1 },
    ])
    zero.releaseApplicationSqlTurn(reader)
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

  it('invalidates schema caches when the transaction aborts', async () => {
    const { zero } = await createTestZero(async (work) => await work())
    const invalidateWatermarks = vi.spyOn(zero.watermarks, 'invalidateCache')
    const reloadCdc = vi.spyOn(zero.cdc, 'reload')

    await expect(
      zero.runTrustedTransaction(unusedCompiler, () => {
        throw new Error('abort')
      })
    ).rejects.toThrow('abort')

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
    const { zero } = await createTestZero(async (work) => await work())

    const response = await zero.fetch(
      new Request('http://zero-do/_orez/application-sql', {
        method: 'POST',
      })
    )

    expect(response.status).toBe(404)
  })
})
