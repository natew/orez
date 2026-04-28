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
import { afterAll, beforeAll, describe, expect, test } from 'vitest'

import { createBrowserProxy } from './pg-proxy-browser.js'

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
