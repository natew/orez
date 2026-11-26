// @vitest-environment jsdom
import { act, StrictMode, useLayoutEffect } from 'react'
import { createRoot } from 'react-dom/client'
import { describe, expect, it, vi } from 'vitest'

;(globalThis as any).IS_REACT_ACT_ENVIRONMENT = true

import {
  createBuilder,
  createClientSchema,
  createSchema,
  extractLocalSchema,
} from '../zero/zero.js'
import { DataProvider, createDataApp } from './DataProvider.js'
import {
  clearSyncClientData,
  isLocalDataDB,
  matchesSyncDB,
} from './helpers/clearZeroClientData.js'
import { observeMutation } from './helpers/useMutation.js'
import {
  LocalDataStore,
  LocalRevisionConflictError,
  classifyQueryAst,
  createMemoryLocalPersistence,
  createSQLiteLocalPersistence,
  type LocalPersistenceAdapter,
  type LocalSQLiteExecutor,
} from './localStore.js'
import { createDataClients } from './multi.js'
import { query, zql } from './zql.js'

import type { OneDataSchema } from './one-schema.js'

function makeTypedOneSchema() {
  return {
    tables: {
      secret: {
        name: 'secret',
        public: false,
        columns: { id: { kind: 'string', optional: false } },
        primaryKey: ['id'],
      },
      session: {
        name: 'session',
        public: true,
        columns: {
          id: { kind: 'string', optional: false },
          title: { kind: 'string', optional: true },
        },
        primaryKey: ['id'],
      },
      uiShell: {
        name: 'uiShell',
        public: false,
        local: 'device',
        columns: {
          id: { kind: 'string', optional: false },
          tab: { kind: 'string', optional: false, default: 'chats' },
          selectedChatId: { kind: 'string', optional: true },
          slug: { kind: 'string', optional: true, unique: true },
          meta: { kind: 'json', optional: true },
        },
        primaryKey: ['id'],
      },
      draft: {
        name: 'draft',
        public: false,
        local: 'device',
        columns: {
          id: { kind: 'string', optional: false },
          shellId: {
            kind: 'string',
            optional: false,
            references: { table: 'uiShell', column: 'id', onDelete: 'cascade' },
          },
          body: { kind: 'string', optional: false, default: '' },
        },
        primaryKey: ['id'],
      },
      palette: {
        name: 'palette',
        public: false,
        local: 'memory',
        columns: {
          id: { kind: 'string', optional: false },
          open: { kind: 'boolean', optional: false, default: false },
        },
        primaryKey: ['id'],
      },
    },
    relationships: {
      uiShell: {
        drafts: [
          {
            sourceField: ['id'],
            destField: ['shellId'],
            destSchema: 'draft',
            cardinality: 'many',
          },
        ],
      },
    },
  } as const satisfies OneDataSchema
}

function makeLocalOnlyOneSchema() {
  return {
    tables: {
      uiShell: {
        name: 'uiShell',
        public: false,
        local: 'device',
        columns: {
          id: { kind: 'string', optional: false },
          tab: { kind: 'string', optional: false, default: 'chats' },
          selectedChatId: { kind: 'string', optional: true },
        },
        primaryKey: ['id'],
      },
    },
    relationships: {},
  } as const satisfies OneDataSchema
}

describe('local tables and query builder', () => {
  it('exports query as the primary builder alias of zql', () => {
    expect(query).toBe(zql)
  })

  it('projects sync schema, client schema, and local metadata with full static types', () => {
    const oneSchema = makeTypedOneSchema()
    const syncSchema = createSchema(oneSchema)
    const clientSchema = createClientSchema(oneSchema)
    const localMeta = extractLocalSchema(oneSchema)

    expect(Object.keys(syncSchema.tables).sort()).toEqual(['session'])
    expect(Object.keys(clientSchema.tables).sort()).toEqual([
      'draft',
      'palette',
      'session',
      'uiShell',
    ])
    expect(Object.keys(localMeta.tables).sort()).toEqual(['draft', 'palette', 'uiShell'])

    // typed builder access on clientSchema (no casts!)
    const clientBuilder = createBuilder(clientSchema)
    const draftQuery = clientBuilder.draft.where('id', 'd1').one()
    expect(draftQuery).toBeDefined()

    // plain Zero schema passed to extractLocalSchema returns empty local metadata
    const plainLocal = extractLocalSchema(syncSchema)
    expect(Object.keys(plainLocal.tables)).toEqual([])
    expect(Object.keys(plainLocal.schema.tables)).toEqual([])
  })

  it('supports local-to-local joins, atomic transactions, rollback, JSON isolation, and persistence across reloads', async () => {
    const oneSchema = makeTypedOneSchema()
    const localMeta = extractLocalSchema(oneSchema)
    const persistence = createMemoryLocalPersistence()

    const store = new LocalDataStore(localMeta, persistence)
    await store.open()

    const view = store.materialize(
      store.query.uiShell.where('id', 'main').related('drafts').one()
    )
    let notifications = 0
    view.addListener(() => {
      notifications++
    })

    // JSON write boundary isolation: mutating caller object after insert cannot mutate live state
    const callerMeta = { nested: { count: 1 } }
    notifications = 0
    await store.transaction(async (tx) => {
      await tx.mutate.uiShell.insert({
        id: 'main',
        tab: 'chats',
        selectedChatId: 's1',
        meta: callerMeta,
      })
      await tx.mutate.draft.insert({ id: 'd1', shellId: 'main', body: 'hello' })
      const inTx = await tx.run(store.query.draft.where('id', 'd1').one())
      expect(inTx?.body).toBe('hello')
    })
    expect(notifications).toBe(1)

    callerMeta.nested.count = 999
    const shell = view.data as any
    expect(shell.meta).toEqual({ nested: { count: 1 } })
    expect(shell.drafts).toHaveLength(1)
    expect(shell.drafts[0].body).toBe('hello')

    // failed transaction rolls back cleanly with zero notifications
    notifications = 0
    await expect(
      store.transaction(async (tx) => {
        await tx.mutate.uiShell.update({ id: 'main', tab: 'code' })
        await tx.mutate.palette.insert({ id: 'p1', open: true })
        throw new Error('abort')
      })
    ).rejects.toThrow('abort')
    expect(notifications).toBe(0)
    expect((view.data as any).tab).toBe('chats')

    // memory table writes do not persist; device table writes survive reload
    await store.transaction(async (tx) => {
      await tx.mutate.palette.upsert({ id: 'p1', open: true })
    })
    expect(await store.run(store.query.palette.where('id', 'p1').one())).toMatchObject({
      id: 'p1',
      open: true,
    })

    view.destroy()
    await store.close()

    // post-close writes reject
    await expect(
      store.transaction(async (tx) => {
        await tx.mutate.palette.upsert({ id: 'p2', open: false })
      })
    ).rejects.toThrow(/closed/)

    const reloaded = new LocalDataStore(localMeta, persistence)
    await reloaded.open()
    const restoredShell = (await reloaded.run(
      reloaded.query.uiShell.where('id', 'main').related('drafts').one()
    )) as any
    expect(restoredShell.selectedChatId).toBe('s1')
    expect(restoredShell.drafts[0].body).toBe('hello')
    expect(
      await reloaded.run(reloaded.query.palette.where('id', 'p1').one())
    ).toBeUndefined()
  })

  it('validates nullability, types, unique constraints, and local FKs before commit', async () => {
    const localMeta = extractLocalSchema(makeTypedOneSchema())
    const store = new LocalDataStore(localMeta, createMemoryLocalPersistence())
    await store.open()

    // null into required column rejected
    await expect(
      store.transaction((tx) => tx.mutate.uiShell.insert({ id: 's1', tab: null as any }))
    ).rejects.toThrow(/not optional/)

    // wrong type rejected
    await expect(
      store.transaction((tx) => tx.mutate.uiShell.insert({ id: 's1', tab: 123 as any }))
    ).rejects.toThrow(/expects string/)

    // valid insert + duplicate unique rejected
    await store.transaction((tx) =>
      tx.mutate.uiShell.insert({ id: 's1', tab: 'chats', slug: 'unique-slug' })
    )
    await expect(
      store.transaction((tx) =>
        tx.mutate.uiShell.insert({ id: 's2', tab: 'code', slug: 'unique-slug' })
      )
    ).rejects.toThrow(/unique constraint/)

    // FK pointing to missing parent rejected
    await expect(
      store.transaction((tx) =>
        tx.mutate.draft.insert({ id: 'd1', shellId: 'missing', body: 'x' })
      )
    ).rejects.toThrow(/foreign key/)

    // valid child insert + cascade delete on parent
    await store.transaction((tx) =>
      tx.mutate.draft.insert({ id: 'd1', shellId: 's1', body: 'x' })
    )
    await store.transaction((tx) => tx.mutate.uiShell.delete({ id: 's1' }))
    expect(await store.run(store.query.draft.where('id', 'd1').one())).toBeUndefined()
  })

  it('does not publish rows when persistence commit rejects and orders writes after hydration', async () => {
    const localMeta = extractLocalSchema(makeLocalOnlyOneSchema())

    // 1. Rejected persistence commit must NOT publish rows to live IVM
    const failingPersistence: LocalPersistenceAdapter = {
      async open() {
        return {}
      },
      async commit() {
        throw new Error('disk full')
      },
    }
    const store1 = new LocalDataStore(localMeta, failingPersistence)
    await store1.open()
    await expect(
      store1.createMutators().uiShell.insert({ id: 'main', tab: 'code' })
    ).rejects.toThrow('disk full')
    expect(
      await store1.run(store1.query.uiShell.where('id', 'main').one())
    ).toBeUndefined()

    // 2. Hydration and delete race: delayed open followed by delete must NOT resurrect deleted row
    let releaseOpen!: () => void
    const openGate = new Promise<void>((resolve) => {
      releaseOpen = resolve
    })
    const backing = createMemoryLocalPersistence()
    const seed = new LocalDataStore(localMeta, backing)
    await seed.open()
    await seed.createMutators().uiShell.insert({ id: 'main', tab: 'persisted' })
    await seed.close()

    const slowPersistence: LocalPersistenceAdapter = {
      async open(tables) {
        await openGate
        return backing.open(tables)
      },
      commit: (ops) => backing.commit(ops),
      clear: () => backing.clear?.() ?? Promise.resolve(),
    }
    const store2 = new LocalDataStore(localMeta, slowPersistence)
    const openPromise = store2.open()
    const delPromise = store2.createMutators().uiShell.delete({ id: 'main' })
    releaseOpen()
    await Promise.all([openPromise, delPromise])
    expect(
      await store2.run(store2.query.uiShell.where('id', 'main').one())
    ).toBeUndefined()
  })

  it('rejects mixed local and synced ASTs across root, related, and EXISTS subqueries', () => {
    const localNames = new Set(['uiShell', 'draft'])
    expect(classifyQueryAst({ table: 'uiShell' } as any, localNames)).toBe('local')
    expect(classifyQueryAst({ table: 'session' } as any, localNames)).toBe('sync')
    expect(() =>
      classifyQueryAst(
        {
          table: 'uiShell',
          related: [{ subquery: { table: 'session' } }],
        } as any,
        localNames
      )
    ).toThrow(/mixed local and synced/)
    expect(() =>
      classifyQueryAst(
        {
          table: 'session',
          where: {
            type: 'correlatedSubquery',
            related: { subquery: { table: 'draft' } },
          },
        } as any,
        localNames
      )
    ).toThrow(/mixed local and synced/)
  })

  it('auto-converges additive columns and rejects incompatible schema changes (including tightening nullability)', async () => {
    const persistence = createMemoryLocalPersistence()
    const v1: OneDataSchema = {
      tables: {
        uiShell: {
          name: 'uiShell',
          public: false,
          local: 'device',
          columns: {
            id: { kind: 'string', optional: false },
            tab: { kind: 'string', optional: false, default: 'chats' },
            note: { kind: 'string', optional: true },
          },
          primaryKey: ['id'],
        },
      },
      relationships: {},
    }
    const s1 = new LocalDataStore(extractLocalSchema(v1), persistence)
    await s1.open()
    await s1.createMutators().uiShell.insert({ id: 'main', tab: 'code' } as any)
    await s1.close()

    // additive column with literal default backfills cleanly
    const v2: OneDataSchema = {
      tables: {
        uiShell: {
          ...v1.tables.uiShell,
          columns: {
            ...v1.tables.uiShell.columns,
            sidebarOpen: { kind: 'boolean', optional: false, default: true },
          },
        },
      },
      relationships: {},
    }
    const s2 = new LocalDataStore(extractLocalSchema(v2), persistence)
    await s2.open()
    const row = (await s2.run(s2.query.uiShell.where('id', 'main').one() as any)) as any
    expect(row.tab).toBe('code')
    expect(row.sidebarOpen).toBe(true)
    await s2.close()

    // tightening nullability from optional -> required rejects while preserving data
    const v3: OneDataSchema = {
      tables: {
        uiShell: {
          ...v2.tables.uiShell,
          columns: {
            ...v2.tables.uiShell.columns,
            note: { kind: 'string', optional: false },
          },
        },
      },
      relationships: {},
    }
    const s3 = new LocalDataStore(extractLocalSchema(v3), persistence)
    await expect(s3.open()).rejects.toThrow(/nullability tightened/)
  })

  it('keeps SQLite reconciliation transactional so a failed backfill rolls back metadata', async () => {
    let meta = new Map<string, string>()
    let rows = new Map<string, Map<string, string>>()
    let txMeta: Map<string, string> | null = null
    let txRows: Map<string, Map<string, string>> | null = null
    let failNextRowInsert = false

    const db: LocalSQLiteExecutor = {
      exec(sql) {
        if (sql === 'BEGIN IMMEDIATE') {
          txMeta = new Map(meta)
          txRows = new Map([...rows.entries()].map(([k, v]) => [k, new Map(v)]))
        } else if (sql === 'COMMIT') {
          if (txMeta) meta = txMeta
          if (txRows) rows = txRows
          txMeta = null
          txRows = null
        } else if (sql === 'ROLLBACK') {
          txMeta = null
          txRows = null
        }
      },
      all(sql, params = []) {
        const curMeta = txMeta ?? meta
        const curRows = txRows ?? rows
        if (sql.includes('limesync_local_meta')) {
          const val = curMeta.get(String(params[0]))
          return val ? [{ schema_json: val }] : []
        }
        const tableMap = curRows.get(String(params[0]))
        return tableMap ? [...tableMap.values()].map((row_json) => ({ row_json })) : []
      },
      run(sql, params = []) {
        const curMeta = txMeta ?? meta
        const curRows = txRows ?? rows
        if (sql.includes('limesync_local_meta')) {
          curMeta.set(String(params[0]), String(params[1]))
        } else if (sql.startsWith('INSERT')) {
          if (failNextRowInsert) {
            failNextRowInsert = false
            throw new Error('injected backfill write error')
          }
          let map = curRows.get(String(params[0]))
          if (!map) {
            map = new Map()
            curRows.set(String(params[0]), map)
          }
          map.set(String(params[1]), String(params[2]))
        } else if (sql.startsWith('DELETE')) {
          curRows.get(String(params[0]))?.delete(String(params[1]))
        }
      },
    }

    const v1 = makeLocalOnlyOneSchema()
    const adapter1 = createSQLiteLocalPersistence(db)
    const store1 = new LocalDataStore(extractLocalSchema(v1), adapter1)
    await store1.open()
    await store1.createMutators().uiShell.upsert({ id: 'main', tab: 'tasks' })
    await store1.close()

    // failed backfill on v2 must roll back metadata so retry still backfills!
    const v2: OneDataSchema = {
      tables: {
        uiShell: {
          ...v1.tables.uiShell,
          columns: {
            ...v1.tables.uiShell.columns,
            pinned: { kind: 'boolean', optional: false, default: true },
          },
        },
      },
      relationships: {},
    }
    failNextRowInsert = true
    const store2a = new LocalDataStore(
      extractLocalSchema(v2),
      createSQLiteLocalPersistence(db)
    )
    await expect(store2a.open()).rejects.toThrow(/injected backfill write error/)

    // retry succeeds and backfills `pinned: true` because metadata rolled back cleanly
    const store2b = new LocalDataStore(
      extractLocalSchema(v2),
      createSQLiteLocalPersistence(db)
    )
    await store2b.open()
    const restored = (await store2b.run(
      store2b.query.uiShell.where('id', 'main').one() as any
    )) as any
    expect(restored.tab).toBe('tasks')
    expect(restored.pinned).toBe(true)
  })

  it('stays live in React StrictMode, handles .one() -> .limit(1) format switch, and boots without auth or server', async () => {
    const oneSchema = makeLocalOnlyOneSchema()
    const syncSchema = createSchema(oneSchema)
    const localMeta = extractLocalSchema(oneSchema)
    const persistence = createMemoryLocalPersistence()

    const app = createDataApp({
      schema: syncSchema,
      localSchema: localMeta,
      models: {},
      groupedQueries: {},
      instanceName: 'local-only-strict-test',
    })

    const { useQuery, mutate, transaction } = app.client
    const q = createBuilder(localMeta.schema)

    function ShellView({ singular }: { singular: boolean }) {
      const expr = singular
        ? q.uiShell.where('id', 'main').one()
        : q.uiShell.where('id', 'main').limit(1)
      const [data] = useQuery(expr as any)
      if (singular) {
        return <div>{`one:${(data as any)?.tab ?? 'empty'}`}</div>
      }
      const list = data as any[]
      return <div>{`many:${Array.isArray(list)}:${list[0]?.tab ?? 'empty'}`}</div>
    }

    const container = document.createElement('div')
    document.body.appendChild(container)
    const root = createRoot(container)
    try {
      await act(async () => {
        root.render(
          <StrictMode>
            <DataProvider app={app} local={{ storageKey: 'tm-strict', persistence }}>
              <ShellView singular={true} />
            </DataProvider>
          </StrictMode>
        )
      })

      expect(container.textContent).toBe('one:empty')

      // typed local mutation works without casts + updates inside StrictMode!
      await act(async () => {
        const res = mutate.uiShell.upsert({ id: 'main', tab: 'old' })
        await observeMutation(res)
        await res
      })
      expect(container.textContent).toBe('one:old')

      // delete + insert after StrictMode remount must still update live
      await act(async () => {
        await transaction(async (tx) => {
          await tx.mutate.uiShell.delete({ id: 'main' })
          await tx.mutate.uiShell.insert({ id: 'main', tab: 'new' })
        })
      })
      expect(container.textContent).toBe('one:new')

      // switching from .one() to .limit(1) must return an array, never a stale object
      await act(async () => {
        root.render(
          <StrictMode>
            <DataProvider app={app} local={{ storageKey: 'tm-strict', persistence }}>
              <ShellView singular={false} />
            </DataProvider>
          </StrictMode>
        )
      })
      expect(container.textContent).toBe('many:true:new')
    } finally {
      await act(async () => {
        root.unmount()
      })
      container.remove()
    }
  })

  it('requires explicit persistence for device tables when IndexedDB is unavailable', () => {
    const oneSchema = makeLocalOnlyOneSchema()
    const app = createDataApp({
      schema: createSchema(oneSchema),
      localSchema: extractLocalSchema(oneSchema),
      models: {},
      groupedQueries: {},
      instanceName: 'missing-idb-test',
    })
    const origIdb = globalThis.indexedDB
    ;(globalThis as any).indexedDB = undefined
    try {
      expect(() => app.client.localStore).toThrow(/requires IndexedDB/)
    } finally {
      ;(globalThis as any).indexedDB = origIdb
    }
  })

  it('clearSyncClientData deletes only sync DBs and never wipes local or unrelated IDBs like grocery-app', async () => {
    expect(isLocalDataDB('limesync-local-team-machine')).toBe(true)
    expect(matchesSyncDB('limesync-local-team-machine')).toBe(false)
    expect(matchesSyncDB('zero-user1-abc')).toBe(true)
    expect(matchesSyncDB('rep:zero-user1:7')).toBe(true)
    expect(matchesSyncDB('replicache-dbs-v0')).toBe(true)
    expect(matchesSyncDB('grocery-app')).toBe(false)
    expect(matchesSyncDB('my-other-app-db')).toBe(false)

    const deleted: string[] = []
    const origIdb = globalThis.indexedDB
    ;(globalThis as any).indexedDB = {
      databases: async () => [
        { name: 'limesync-local-team-machine' },
        { name: 'grocery-app' },
        { name: 'rep:zero-user1:7' },
      ],
      deleteDatabase: (name: string) => {
        deleted.push(name)
        const req: any = {}
        queueMicrotask(() => req.onsuccess?.())
        return req
      },
    }

    try {
      const onCleared = vi.fn()
      await clearSyncClientData({ reload: false, onCleared })
      expect(deleted).toEqual(['rep:zero-user1:7'])
      expect(onCleared).toHaveBeenCalledWith({ count: 1, names: ['rep:zero-user1:7'] })
    } finally {
      ;(globalThis as any).indexedDB = origIdb
    }
  })

  it('drains accepted writes before close, bumps revisions across backfill/clear, enforces partial unique indexes, and supports child useLayoutEffect writes', async () => {
    // 1. insert() followed immediately by close() must drain insert() cleanly
    const schemaWithPartialUnique: OneDataSchema = {
      tables: {
        item: {
          name: 'item',
          public: false,
          local: 'device',
          columns: {
            id: { kind: 'string', optional: false },
            title: { kind: 'string', optional: false },
            active: { kind: 'boolean', optional: false, default: true },
          },
          primaryKey: ['id'],
          indexes: {
            active_title_unique: {
              columns: ['title'],
              unique: true,
              where: 'active = 1',
            },
          },
        },
      },
      relationships: {},
    }

    const memPersist = createMemoryLocalPersistence()
    const s1 = new LocalDataStore(extractLocalSchema(schemaWithPartialUnique), memPersist)
    const ins = s1
      .createMutators()
      .item.insert({ id: '1', title: 'Alpha', active: true } as any)
    const closing = s1.close()
    await expect(ins).resolves.toBeUndefined()
    await closing

    // 2. partial unique index matches SQLite 3-valued logic & numeric affinity
    const s2 = new LocalDataStore(extractLocalSchema(schemaWithPartialUnique), memPersist)
    await s2.open()
    await expect(
      s2.createMutators().item.insert({ id: '2', title: 'Alpha', active: true } as any)
    ).rejects.toThrow(/unique constraint/)
    await expect(
      s2.createMutators().item.insert({ id: '3', title: 'Alpha', active: false } as any)
    ).resolves.toBeUndefined()
    await s2.close()

    const sqliteWhereSchema: OneDataSchema = {
      tables: {
        metric: {
          name: 'metric',
          public: false,
          local: 'device',
          columns: {
            id: { kind: 'string', optional: false },
            name: { kind: 'string', optional: false },
            score: { kind: 'number', optional: true },
            active: { kind: 'boolean', optional: false, default: true },
          },
          primaryKey: ['id'],
          indexes: {
            eq_str_num: { columns: ['name'], unique: true, where: "score = '1'" },
            not_eq_null: { columns: ['name'], unique: true, where: 'NOT (score = 1)' },
            bool_ge_2: { columns: ['name'], unique: true, where: 'active >= 2' },
          },
        },
      },
      relationships: {},
    }
    const sw = new LocalDataStore(
      extractLocalSchema(sqliteWhereSchema),
      createMemoryLocalPersistence()
    )
    await sw.open()
    // score = '1' matches numeric 1 -> duplicate rejected
    await sw
      .createMutators()
      .metric.insert({ id: 'a', name: 'k1', score: 1, active: true } as any)
    await expect(
      sw
        .createMutators()
        .metric.insert({ id: 'b', name: 'k1', score: 1, active: true } as any)
    ).rejects.toThrow(/eq_str_num/)
    // NOT (score = 1) with NULL evaluates to UNKNOWN (excluded) AND active >= 2 with true (1 >= 2 is false) -> duplicate allowed!
    await sw
      .createMutators()
      .metric.insert({ id: 'c', name: 'k2', score: null, active: true } as any)
    await expect(
      sw
        .createMutators()
        .metric.insert({ id: 'd', name: 'k2', score: null, active: true } as any)
    ).resolves.toBeUndefined()
    await sw.close()

    // 2b. close() -> reopen() mid-close does not leave store closed and refreshes external disk updates/deletes
    let releaseClose!: () => void
    const closeGate = new Promise<void>((r) => {
      releaseClose = r
    })
    const backingReopen = createMemoryLocalPersistence()
    const slowCloseAdapter: LocalPersistenceAdapter = {
      open: (t) => backingReopen.open(t),
      commit: (ops) => backingReopen.commit(ops),
      clear: (t) => backingReopen.clear?.(t) ?? Promise.resolve(),
      async close() {
        await closeGate
      },
    }
    const sr = new LocalDataStore(
      extractLocalSchema(makeLocalOnlyOneSchema()),
      slowCloseAdapter
    )
    await sr.open()
    await sr.createMutators().uiShell.insert({ id: 'main', tab: 'before' })
    const closeP = sr.close()
    // simulate external writer modifying backing disk before reopen finishes
    await backingReopen.commit([
      {
        kind: 'put',
        table: 'uiShell',
        key: '["main"]',
        row: { id: 'main', tab: 'external', selectedChatId: null },
      },
    ])
    const reopenP = sr.reopen()
    releaseClose()
    await Promise.all([closeP, reopenP])
    expect(sr.closed).toBe(false)
    const refreshed = await sr.run(sr.query.uiShell.where('id', 'main').one())
    expect(refreshed?.tab).toBe('external')
    await expect(
      sr.createMutators().uiShell.update({ id: 'main', tab: 'after-reopen' })
    ).resolves.toBeUndefined()
    await sr.close()

    // 3. SQLite backfill + clear bump revisions monotonically and preserve post-clear metadata
    let meta = new Map<string, string>()
    let rows = new Map<string, Map<string, string>>()
    let txMeta: Map<string, string> | null = null
    let txRows: Map<string, Map<string, string>> | null = null
    const db: LocalSQLiteExecutor = {
      exec(sql) {
        if (sql === 'BEGIN IMMEDIATE') {
          txMeta = new Map(meta)
          txRows = new Map([...rows.entries()].map(([k, v]) => [k, new Map(v)]))
        } else if (sql === 'COMMIT') {
          if (txMeta) meta = txMeta
          if (txRows) rows = txRows
          txMeta = null
          txRows = null
        } else if (sql === 'ROLLBACK') {
          txMeta = null
          txRows = null
        } else if (sql.includes('DELETE FROM limesync_local_rows')) {
          ;(txRows ?? rows).clear()
          ;(txMeta ?? meta).clear()
        }
      },
      all(sql, params = []) {
        const curMeta = txMeta ?? meta
        const curRows = txRows ?? rows
        if (sql.includes('limesync_local_meta')) {
          const val = curMeta.get(String(params[0]))
          return val ? [{ schema_json: val }] : []
        }
        const tableMap = curRows.get(String(params[0]))
        return tableMap ? [...tableMap.values()].map((row_json) => ({ row_json })) : []
      },
      run(sql, params = []) {
        const curMeta = txMeta ?? meta
        const curRows = txRows ?? rows
        if (sql.includes('limesync_local_meta')) {
          curMeta.set(String(params[0]), String(params[1]))
        } else if (sql.startsWith('INSERT')) {
          let map = curRows.get(String(params[0]))
          if (!map) {
            map = new Map()
            curRows.set(String(params[0]), map)
          }
          map.set(String(params[1]), String(params[2]))
        } else if (sql.startsWith('DELETE')) {
          curRows.get(String(params[0]))?.delete(String(params[1]))
        }
      },
    }

    const v1 = makeLocalOnlyOneSchema()
    const staleWriter = new LocalDataStore(
      extractLocalSchema(v1),
      createSQLiteLocalPersistence(db)
    )
    await staleWriter.open()
    await staleWriter.createMutators().uiShell.insert({ id: 'main', tab: 'v1' })

    // v2 opens and backfills -> must bump revision so staleWriter is rejected
    const v2: OneDataSchema = {
      tables: {
        uiShell: {
          ...v1.tables.uiShell,
          columns: {
            ...v1.tables.uiShell.columns,
            pinned: { kind: 'boolean', optional: false, default: true },
          },
        },
      },
      relationships: {},
    }
    const v2Store = new LocalDataStore(
      extractLocalSchema(v2),
      createSQLiteLocalPersistence(db)
    )
    await v2Store.open()
    await expect(
      staleWriter.createMutators().uiShell.update({ id: 'main', tab: 'clobber' })
    ).rejects.toThrow(/concurrent local SQLite modification/)

    // clear() bumps revision monotonically and reinstalls schema metadata
    const staleV2 = new LocalDataStore(
      extractLocalSchema(v2),
      createSQLiteLocalPersistence(db)
    )
    await staleV2.open()
    await v2Store.clear()
    expect(meta.has('uiShell')).toBe(true)
    await expect(
      staleV2
        .createMutators()
        .uiShell.insert({ id: 'zombie', tab: 'bad', pinned: false } as any)
    ).rejects.toThrow(/concurrent local SQLite modification/)
    await v2Store.close()

    // 4. child useLayoutEffect initial write hits provider store (never headless fallback)
    const layoutPersist = createMemoryLocalPersistence()
    const app = createDataApp({
      schema: createSchema(v1),
      localSchema: extractLocalSchema(v1),
      models: {},
      groupedQueries: {},
      instanceName: 'child-layout-effect-test',
    })
    const q = createBuilder(extractLocalSchema(v1).schema)
    function ChildWriter() {
      const [row] = app.client.useQuery(q.uiShell.where('id', 'main').one() as any)
      useLayoutEffect(() => {
        void app.client.mutate.uiShell.upsert({ id: 'main', tab: 'from-layout' })
      }, [])
      return <div>{(row as any)?.tab ?? 'empty'}</div>
    }
    const container = document.createElement('div')
    document.body.appendChild(container)
    const root = createRoot(container)
    try {
      await act(async () => {
        root.render(
          <DataProvider
            app={app}
            local={{ storageKey: 'layout-key', persistence: layoutPersist }}
          >
            <ChildWriter />
          </DataProvider>
        )
      })
      expect(container.textContent).toBe('from-layout')
    } finally {
      await act(async () => {
        root.unmount()
      })
      container.remove()
    }
  })

  it('rehydrates on peer commit, retries bounded revision conflicts, and routes local tables through createDataClients', async () => {
    const oneSchema = {
      tables: {
        draft: {
          name: 'draft',
          public: false,
          local: 'device' as const,
          columns: {
            id: { kind: 'string' as const, optional: false },
            body: { kind: 'string' as const, optional: false },
          },
          primaryKey: ['id'],
        },
        post: {
          name: 'post',
          public: true,
          columns: {
            id: { kind: 'string' as const, optional: false },
          },
          primaryKey: ['id'],
        },
      },
      relationships: {},
    }
    const localMeta = extractLocalSchema(oneSchema)
    const syncSchema = createSchema(oneSchema)

    // 1. Bounded conflict rebase retry + peer commit notification
    const backing = createMemoryLocalPersistence()
    let peerListener: (() => void) | null = null
    let conflictsLeft = 2
    let notified = 0
    let lockCalls = 0
    const coordinated: LocalPersistenceAdapter = {
      open: (tables) => backing.open(tables),
      async commit(ops) {
        if (conflictsLeft > 0) {
          conflictsLeft -= 1
          await backing.commit([
            {
              kind: 'put',
              table: 'draft',
              key: JSON.stringify(['peer']),
              row: { id: 'peer', body: 'from-peer' },
            },
          ])
          throw new LocalRevisionConflictError(
            '[limesync] concurrent local store modification detected (expected revision 0, found 1)',
            0,
            1
          )
        }
        await backing.commit(ops)
      },
      clear: (t) => backing.clear?.(t) ?? Promise.resolve(),
      withLock: async (work) => {
        lockCalls += 1
        return work()
      },
      onPeerCommit: (cb) => {
        peerListener = cb
        return () => {
          if (peerListener === cb) peerListener = null
        }
      },
      notifyCommit: () => {
        notified += 1
      },
      maxConflictRetries: 3,
    }
    const store = new LocalDataStore(localMeta, coordinated)
    let attempts = 0
    await store.transaction(async (tx) => {
      attempts += 1
      await tx.mutate.draft.upsert({ id: 'mine', body: 'hello' })
    })
    expect(attempts).toBe(3)
    expect(lockCalls).toBeGreaterThan(0)
    expect(notified).toBe(1)
    const rowsAfterRebase = await store.run(store.query.draft)
    expect(rowsAfterRebase.map((r) => r.id).sort()).toEqual(['mine', 'peer'])

    // Peer commit signal triggers fresh open on next transaction
    await backing.commit([
      {
        kind: 'put',
        table: 'draft',
        key: JSON.stringify(['peer-2']),
        row: { id: 'peer-2', body: 'later' },
      },
    ])
    peerListener?.()
    let seenPeer2 = false
    await store.transaction(async (tx) => {
      const all = await tx.run(store.query.draft)
      seenPeer2 = all.some((r) => r.id === 'peer-2')
    })
    expect(seenPeer2).toBe(true)

    // Exhausting maxConflictRetries surfaces the conflict error
    conflictsLeft = 10
    await expect(
      store.transaction(async (tx) => {
        await tx.mutate.draft.upsert({ id: 'fail', body: 'too-many-conflicts' })
      })
    ).rejects.toThrow(LocalRevisionConflictError)
    await store.close()

    // 2. Multi-client createDataClients routes local tables on combined facade
    const localPersist = createMemoryLocalPersistence()
    const multi = createDataClients(
      {
        app: {
          schema: syncSchema,
          queries: {},
          models: {},
          tables: ['post'],
          syncTables: ['post'],
          supportTables: [],
          scope: null,
          defaultVisibility: null,
        },
        usr: {
          schema: syncSchema,
          queries: {},
          models: {},
          tables: [],
          syncTables: [],
          supportTables: [],
          scope: 'userId',
          defaultVisibility: null,
        },
      },
      { inner: 'usr', localSchema: localMeta }
    )
    const q = createBuilder(localMeta.schema)
    function MultiLocalReader() {
      const [row] = multi.combined.useQuery(q.draft.where('id', 'd1').one() as any)
      useLayoutEffect(() => {
        void multi.combined.mutate.draft.upsert({ id: 'd1', body: 'multi-local' })
      }, [])
      return <div>{(row as any)?.body ?? 'empty'}</div>
    }
    const container = document.createElement('div')
    document.body.appendChild(container)
    const root = createRoot(container)
    const UsrProvider = multi.providers.usr
    try {
      await act(async () => {
        root.render(
          <UsrProvider
            userID="u1"
            disable
            local={{ storageKey: 'multi-test', persistence: localPersist }}
          >
            <MultiLocalReader />
          </UsrProvider>
        )
      })
      expect(container.textContent).toBe('multi-local')
      await multi.combined.transaction(async (tx) => {
        await tx.mutate.draft.upsert({ id: 'd2', body: 'tx-multi' })
      })
      const saved = await multi.combined.localStore?.run(q.draft)
      expect(saved?.map((r) => r.id).sort()).toEqual(['d1', 'd2'])
    } finally {
      await act(async () => {
        root.unmount()
      })
      container.remove()
    }
  })
})
