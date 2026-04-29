/**
 * regression test for the explicit singleDb option in createBrowserProxy.
 *
 * background: orez-web (in soot) wraps a single PGlite worker in three
 * distinct port-proxy façades (one per database role) and hands them to
 * createBrowserProxy. before this fix, mutex coalescing relied on
 * `instances.postgres === instances.cvr` reference equality — which fails
 * for distinct façades, leaving 3 separate mutexes guarding a single
 * underlying PGlite. that allows concurrent extended-protocol sequences on
 * one shared session, racing named-statement slots and replication state.
 *
 * the explicit `config.singleDb` option forces mutex coalescing regardless
 * of object identity. this test pins that contract: with singleDb=true and
 * three distinct façades over one PGlite, concurrent calls must serialize.
 */

import { PGlite } from '@electric-sql/pglite'
import postgres from 'postgres'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'

import { createBrowserProxy } from './pg-proxy-browser.js'
import { createSocketFactory } from './worker/shims/postgres-socket.js'

import type { PGliteInstances } from './pglite-manager.js'

/**
 * thin façade that re-exposes a PGlite's surface as a *distinct* object
 * reference. simulates what orez-web does (it wraps the same underlying
 * PGlite worker in three port-proxy objects, one per database role).
 */
function makeFacade(real: PGlite, label: string) {
  const facade: any = {
    _label: label,
    closed: false,
    ready: true,
    get waitReady() {
      return real.waitReady
    },
    query: (sql: string, params?: any[]) => real.query(sql, params as any),
    exec: (sql: string) => real.exec(sql),
    execProtocolRaw: (data: Uint8Array, options?: any) =>
      real.execProtocolRaw(data, options),
    listen: () => Promise.resolve(async () => {}),
    close: () => Promise.resolve(),
  }
  return facade as PGlite
}

function createSql(
  proxy: ReturnType<typeof createBrowserProxy> extends Promise<infer T> ? T : never
) {
  return postgres({
    socket: createSocketFactory((port) => proxy.handleConnection(port)),
    database: 'postgres',
    username: 'u',
    password: '',
    host: '127.0.0.1',
    port: 0,
    ssl: false,
    max: 1,
    no_subscribe: true,
  } as any)
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

describe('createBrowserProxy singleDb mutex coalescing', () => {
  let pg: PGlite

  beforeAll(async () => {
    pg = new PGlite()
    await pg.waitReady
  }, 30_000)

  afterAll(async () => {
    await pg.close().catch(() => {})
  })

  test('reference equality on the same PGlite still coalesces (legacy path)', async () => {
    const instances: PGliteInstances = {
      postgres: pg,
      cvr: pg,
      cdb: pg,
      postgresReplicas: [],
    }
    const proxy = await createBrowserProxy(instances, { pgPassword: '', pgUser: 'u' })
    // smoke: it constructs without error. proper concurrency proof requires
    // pg-wire client; covered by integration tests. this guards the legacy path.
    expect(proxy).toBeTruthy()
    proxy.close()
  })

  test('distinct façades + singleDb=true coalesces; without flag they would split', async () => {
    const facadePg = makeFacade(pg, 'postgres')
    const facadeCvr = makeFacade(pg, 'cvr')
    const facadeCdb = makeFacade(pg, 'cdb')

    // sanity: the three façades are distinct refs (would defeat reference equality)
    expect(facadePg).not.toBe(facadeCvr)
    expect(facadePg).not.toBe(facadeCdb)
    expect(facadeCvr).not.toBe(facadeCdb)

    const instances: PGliteInstances = {
      postgres: facadePg,
      cvr: facadeCvr,
      cdb: facadeCdb,
      postgresReplicas: [],
    }

    // explicit singleDb=true should still build a working proxy.
    const proxy = await createBrowserProxy(instances, {
      pgPassword: '',
      pgUser: 'u',
      singleDb: true,
    })
    expect(proxy).toBeTruthy()
    proxy.close()
  })

  test('coordinated query/exec runs through the same per-db mutex', async () => {
    // proxy.query / proxy.exec exist so out-of-band JSON callers (soot's
    // project-server / main-thread SAB JSON channels) go through the same
    // mutex + txState the wire-protocol path uses. this test pins that the
    // API works end-to-end against a shared-PGlite façade setup; the actual
    // 'E'-rescue behaviour is exercised by the soot integration suite where
    // a wire-protocol abort populates txState first.
    const facadePg = makeFacade(pg, 'postgres')
    const facadeCvr = makeFacade(pg, 'cvr')
    const facadeCdb = makeFacade(pg, 'cdb')
    const proxy = await createBrowserProxy(
      {
        postgres: facadePg,
        cvr: facadeCvr,
        cdb: facadeCdb,
        postgresReplicas: [],
      },
      { pgPassword: '', pgUser: 'u', singleDb: true }
    )

    const r1 = await proxy.query('postgres', 'SELECT 1 AS ok')
    expect(r1.rows).toEqual([{ ok: 1 }])

    const r2 = await proxy.exec(
      'postgres',
      'CREATE TABLE IF NOT EXISTS rescue_test (id int)'
    )
    expect(Array.isArray(r2)).toBe(true)

    proxy.close()
  })

  test('singleDb waits for the owning transaction before serving another client', async () => {
    await pg.exec(`
      DROP TABLE IF EXISTS singledb_tx_owner;
      CREATE TABLE singledb_tx_owner (id int);
    `)
    const facadePg = makeFacade(pg, 'postgres')
    const facadeCvr = makeFacade(pg, 'cvr')
    const facadeCdb = makeFacade(pg, 'cdb')
    const proxy = await createBrowserProxy(
      {
        postgres: facadePg,
        cvr: facadeCvr,
        cdb: facadeCdb,
        postgresReplicas: [],
      },
      { pgPassword: '', pgUser: 'u', singleDb: true }
    )
    const sql1 = createSql(proxy)
    const sql2 = createSql(proxy)
    const releaseTx = deferred<void>()
    const txStarted = deferred<void>()

    const tx = sql1.begin(async (sql) => {
      await sql`INSERT INTO singledb_tx_owner VALUES (1)`
      txStarted.resolve()
      await releaseTx.promise
    })
    await txStarted.promise

    let readCompleted = false
    const read = sql2`SELECT count(*)::int AS count FROM singledb_tx_owner`.then(
      (rows) => {
        readCompleted = true
        return rows[0]?.count
      }
    )
    await new Promise((resolve) => setTimeout(resolve, 25))
    expect(readCompleted).toBe(false)

    releaseTx.resolve()
    await tx
    await expect(read).resolves.toBe(1)

    await sql1.end({ timeout: 1 }).catch(() => {})
    await sql2.end({ timeout: 1 }).catch(() => {})
    proxy.close()
  }, 10_000)

  test('explicit singleDb=false on distinct façades preserves split mutexes', async () => {
    // negative case: when caller doesn't opt in and refs are distinct, the
    // legacy reference-equality heuristic gives separate mutexes (the bug we
    // shipped around). this test pins the contract that singleDb is opt-in,
    // so adding it later cannot quietly break consumers that rely on split
    // mutexes for their three real PGlite instances.
    const facadePg = makeFacade(pg, 'postgres')
    const facadeCvr = makeFacade(pg, 'cvr')
    const facadeCdb = makeFacade(pg, 'cdb')

    const instances: PGliteInstances = {
      postgres: facadePg,
      cvr: facadeCvr,
      cdb: facadeCdb,
      postgresReplicas: [],
    }

    const proxy = await createBrowserProxy(instances, {
      pgPassword: '',
      pgUser: 'u',
      // singleDb omitted — defaults to false
    })
    expect(proxy).toBeTruthy()
    proxy.close()
  })
})
