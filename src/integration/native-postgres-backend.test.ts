/**
 * integration test for the native postgres backend (backend: 'postgres').
 *
 * same sync pipeline as integration.test.ts but with real postgres via the
 * optional embedded-postgres package: zero-cache connects directly and uses
 * real logical replication — no pg-wire proxy, no CDC emulation.
 */

import { describe, expect, test, beforeAll, afterAll, beforeEach } from 'vitest'
import WebSocket from 'ws'

import { startZeroLite } from '../index.js'
import {
  ensureTablesInPublications,
  hasNonNullPermissions,
  installAllowAllPermissions,
} from './test-permissions.js'

const SYNC_PROTOCOL_VERSION = 51
const CLIENT_SCHEMA = {
  tables: {
    foo: {
      columns: {
        id: { type: 'string' },
        value: { type: 'string' },
        num: { type: 'number' },
      },
      primaryKey: ['id'],
    },
  },
}

type Db = {
  exec(sql: string): Promise<unknown>
  query<T>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>
}

function encodeSecProtocols(
  initConnectionMessage: unknown,
  authToken: string | undefined
): string {
  const payload = JSON.stringify({ initConnectionMessage, authToken })
  return encodeURIComponent(Buffer.from(payload, 'utf-8').toString('base64'))
}

class Queue<T> {
  private items: T[] = []
  private waiters: Array<{
    resolve: (v: T) => void
    timer?: ReturnType<typeof setTimeout>
  }> = []

  enqueue(item: T) {
    const waiter = this.waiters.shift()
    if (waiter) {
      if (waiter.timer) clearTimeout(waiter.timer)
      waiter.resolve(item)
    } else {
      this.items.push(item)
    }
  }

  dequeue(fallback?: T, timeoutMs = 10000): Promise<T> {
    if (this.items.length > 0) {
      return Promise.resolve(this.items.shift()!)
    }
    return new Promise<T>((resolve) => {
      const waiter: { resolve: (v: T) => void; timer?: ReturnType<typeof setTimeout> } = {
        resolve,
      }
      if (fallback !== undefined) {
        waiter.timer = setTimeout(() => {
          const idx = this.waiters.indexOf(waiter)
          if (idx >= 0) this.waiters.splice(idx, 1)
          resolve(fallback)
        }, timeoutMs)
      }
      this.waiters.push(waiter)
    })
  }
}

describe('orez native postgres backend', { timeout: 180000 }, () => {
  let db: Db
  let cdb: Db
  let zeroPort: number
  let shutdown: () => Promise<void>
  let restartZero: (() => Promise<void>) | undefined
  let resetZeroFull: (() => Promise<void>) | undefined
  let stopZero: (() => Promise<void>) | undefined
  let dataDir: string

  beforeAll(async () => {
    const testPgPort = 24000 + Math.floor(Math.random() * 1000)
    const testZeroPort = testPgPort + 100

    dataDir = `.orez-native-pg-test-${Date.now()}`
    console.log(
      `[test] starting orez (native pg) on pg:${testPgPort} zero:${testZeroPort}`
    )
    const result = await startZeroLite({
      backend: 'postgres',
      pgPort: testPgPort,
      zeroPort: testZeroPort,
      dataDir,
      logLevel: (process.env.OREZ_TEST_LOG_LEVEL as 'info' | 'debug') || 'info',
      skipZeroCache: false,
    })

    db = result.db
    cdb = result.instances.cdb
    zeroPort = result.zeroPort
    shutdown = result.stop
    restartZero = result.restartZero
    resetZeroFull = result.resetZeroFull
    stopZero = result.stopZero

    // real logical replication artifacts must exist (proves this is not the
    // emulated pipeline): wal_level=logical and a real publication
    const walLevel = await db.query<{ wal_level: string }>(`SHOW wal_level`)
    expect(walLevel.rows[0].wal_level).toBe('logical')

    await db.exec(`
      CREATE TABLE IF NOT EXISTS foo (
        id TEXT PRIMARY KEY,
        value TEXT,
        num INTEGER
      );
    `)
    await ensureTablesInPublications(db, ['foo'])
    await installAllowAllPermissions(db, ['foo'])
    expect(await hasNonNullPermissions(db)).toBe(true)

    // re-snapshot so the replica includes foo + permissions
    await resetZeroFull!()
    await waitForZero(zeroPort, 90000)

    // zero-cache must be running on a REAL replication slot
    const slots = await db.query<{ slot_name: string; plugin: string }>(
      `SELECT slot_name, plugin FROM pg_replication_slots`
    )
    expect(slots.rows.length).toBeGreaterThan(0)
    expect(slots.rows[0].plugin).toBe('pgoutput')
  }, 180000)

  afterAll(async () => {
    if (shutdown) await shutdown()
    if (dataDir) {
      const { rmSync } = await import('node:fs')
      try {
        rmSync(dataDir, { recursive: true, force: true })
      } catch {}
    }
  })

  beforeEach(async () => {
    await db.exec(`DELETE FROM foo;`)
    await waitForReplicationCatchup(db)
    await new Promise((r) => setTimeout(r, 1000))
  }, 30000)

  // wait until zero-cache's slot has consumed all WAL — the real-replication
  // equivalent of draining _orez._zero_changes in the pglite test
  async function waitForReplicationCatchup(db: Db, timeoutMs = 15000): Promise<void> {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      const result = await db.query<{ caught_up: boolean }>(
        `SELECT bool_and(confirmed_flush_lsn >= pg_current_wal_lsn()) AS caught_up
         FROM pg_replication_slots WHERE active`
      )
      if (result.rows[0]?.caught_up) return
      await new Promise((r) => setTimeout(r, 100))
    }
  }

  test('zero-cache starts and accepts websocket connections', async () => {
    const secProtocol = encodeSecProtocols(
      ['initConnection', { desiredQueriesPatch: [] }],
      undefined
    )
    const ws = new WebSocket(
      `ws://localhost:${zeroPort}/sync/v${SYNC_PROTOCOL_VERSION}/connect` +
        `?clientGroupID=cg-${Date.now()}&clientID=c-${Date.now()}&wsid=ws1&schemaVersion=1&baseCookie=&ts=${Date.now()}&lmid=0`,
      secProtocol
    )

    await new Promise<void>((resolve, reject) => {
      ws.on('open', resolve)
      ws.on('error', reject)
      setTimeout(() => reject(new Error('ws connect timeout')), 5000)
    })

    const firstMessage = await new Promise<unknown>((resolve) => {
      ws.on('message', (data) => resolve(JSON.parse(data.toString())))
    })
    expect(firstMessage).toMatchObject(['connected', { wsid: 'ws1' }])
    ws.close()
  })

  test('initial sync delivers existing rows via poke', async () => {
    await db.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
      'row1',
      'hello',
      42,
    ])
    await waitForReplicationCatchup(db)
    await new Promise((r) => setTimeout(r, 1000))

    const downstream = new Queue<unknown>()
    const ws = connectAndSubscribe(zeroPort, downstream, {
      table: 'foo',
      orderBy: [['id', 'asc']],
    })

    await waitForRowPatch(
      downstream,
      (row) =>
        row.op === 'put' &&
        row.tableName === 'foo' &&
        row.value?.id === 'row1' &&
        row.value?.value === 'hello',
      30000,
      'initial row1 put'
    )
    ws.close()
  })

  test('live replication: insert/update/delete trigger pokes', async () => {
    const downstream = new Queue<unknown>()
    const ws = connectAndSubscribe(zeroPort, downstream, {
      table: 'foo',
      orderBy: [['id', 'asc']],
    })
    await drainInitialPokes(downstream)

    await db.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
      'live-row',
      'live-value',
      99,
    ])
    await waitForRowPatch(
      downstream,
      (row) =>
        row.op === 'put' &&
        row.value?.id === 'live-row' &&
        row.value?.value === 'live-value',
      30000,
      'live-row put'
    )

    await db.query(`UPDATE foo SET value = $1 WHERE id = $2`, ['updated', 'live-row'])
    await waitForRowPatch(
      downstream,
      (row) =>
        row.op === 'put' &&
        row.value?.id === 'live-row' &&
        row.value?.value === 'updated',
      30000,
      'live-row update'
    )

    await db.query(`DELETE FROM foo WHERE id = $1`, ['live-row'])
    await waitForRowPatch(
      downstream,
      (row) => row.op === 'del' && row.tableName === 'foo',
      30000,
      'live-row delete'
    )
    ws.close()
  })

  test('concurrent inserts all replicate', { timeout: 60000 }, async () => {
    const downstream = new Queue<unknown>()
    const ws = connectAndSubscribe(zeroPort, downstream, {
      table: 'foo',
      orderBy: [['id', 'asc']],
    })
    await drainInitialPokes(downstream)

    await Promise.all(
      Array.from({ length: 5 }, (_, i) =>
        db.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
          `concurrent-${i}`,
          `value-${i}`,
          i,
        ])
      )
    )

    const allRows = await collectPokeRows(downstream, 30000)
    const ids = allRows
      .filter((r: any) => r.op === 'put' && r.tableName === 'foo')
      .map((r: any) => r.value.id)
      .sort()
    expect(ids).toEqual([
      'concurrent-0',
      'concurrent-1',
      'concurrent-2',
      'concurrent-3',
      'concurrent-4',
    ])
    ws.close()
  })

  test(
    'warm zero-cache restart: reconnect resumes sync',
    { timeout: 90000 },
    async () => {
      expect(restartZero).toBeDefined()

      {
        const downstream = new Queue<unknown>()
        const ws = connectAndSubscribe(zeroPort, downstream, {
          table: 'foo',
          orderBy: [['id', 'asc']],
        })
        await drainInitialPokes(downstream)
        await db.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
          'warm-gen1',
          'before-restart',
          1,
        ])
        await waitForRowPatch(
          downstream,
          (row) => row.op === 'put' && row.value?.id === 'warm-gen1',
          30000,
          'warm-gen1 put'
        )
        ws.close()
      }

      await restartZero!()
      await waitForZero(zeroPort, 60000)

      const downstream = new Queue<unknown>()
      const ws = connectAndSubscribe(zeroPort, downstream, {
        table: 'foo',
        orderBy: [['id', 'asc']],
      })
      await waitForRowPatch(
        downstream,
        (row) => row.op === 'put' && row.value?.id === 'warm-gen1',
        30000,
        'warm-gen1 re-hydrate after restart'
      )

      await db.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
        'warm-gen2',
        'after-restart',
        2,
      ])
      await waitForRowPatch(
        downstream,
        (row) => row.op === 'put' && row.value?.id === 'warm-gen2',
        30000,
        'warm-gen2 put after restart'
      )
      ws.close()
    }
  )

  // the purge-lock self-deadlock (2026-07-06 incident): with rows in the cdc
  // changeLog, the change-streamer takes a purge lock on boot — an open
  // SELECT ... FOR SHARE transaction holding a real xid. if the replica file
  // is gone (unclean shutdown, cache-only reset), initial sync then runs while
  // that lock is held, and CREATE_REPLICATION_SLOT waits on the lock's own
  // xid: a deterministic self-deadlock that crash-loops zero-cache on 55P03
  // lock timeouts. the litestream-patch guard must surface not-found so the
  // caller releases the lock before initial sync.
  test(
    'replica lost with non-empty changeLog: restart resyncs without purge-lock deadlock',
    { timeout: 150000 },
    async () => {
      expect(stopZero).toBeDefined()
      expect(restartZero).toBeDefined()

      // arm: ensure replicated rows exist so the changeLog is non-empty
      await db.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
        'deadlock-arm',
        'pre-crash',
        7,
      ])
      await waitForReplicationCatchup(db)

      const [{ table_schema }] = (
        await cdb.query<{ table_schema: string }>(
          `SELECT table_schema FROM information_schema.tables WHERE table_name = 'changeLog'`
        )
      ).rows
      const changeRows = await cdb.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM "${table_schema}"."changeLog"`
      )
      expect(changeRows.rows[0].n).toBeGreaterThan(0)

      // crash-equivalent: zero-cache gone, replica file gone, changeLog kept
      await stopZero!()
      const { rmSync } = await import('node:fs')
      const { resolve } = await import('node:path')
      for (const suffix of ['', '-wal', '-shm', '-wal2']) {
        try {
          rmSync(resolve(dataDir, `zero-replica.db${suffix}`), { force: true })
        } catch {}
      }

      // without the fix this rejects (zero-cache exits 255 on the slot-create
      // lock timeout ~30s in); with it, initial sync runs lock-free and boots.
      await restartZero!()
      await waitForZero(zeroPort, 60000)

      // resync is real: pre-crash row hydrates and live replication works
      const downstream = new Queue<unknown>()
      const ws = connectAndSubscribe(zeroPort, downstream, {
        table: 'foo',
        orderBy: [['id', 'asc']],
      })
      await waitForRowPatch(
        downstream,
        (row) => row.op === 'put' && row.value?.id === 'deadlock-arm',
        30000,
        'deadlock-arm re-hydrate after resync'
      )
      await db.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
        'deadlock-live',
        'post-resync',
        8,
      ])
      await waitForRowPatch(
        downstream,
        (row) => row.op === 'put' && row.value?.id === 'deadlock-live',
        30000,
        'deadlock-live put after resync'
      )
      ws.close()
    }
  )

  // --- helpers ---

  function connectAndSubscribe(
    port: number,
    downstream: Queue<unknown>,
    query: Record<string, unknown>
  ): WebSocket {
    const secProtocol = encodeSecProtocols(
      [
        'initConnection',
        {
          desiredQueriesPatch: [{ op: 'put', hash: 'q1', ast: query }],
          clientSchema: CLIENT_SCHEMA,
        },
      ],
      undefined
    )
    const ws = new WebSocket(
      `ws://localhost:${port}/sync/v${SYNC_PROTOCOL_VERSION}/connect` +
        `?clientGroupID=cg-${Date.now()}&clientID=c-${Date.now()}&wsid=ws1&schemaVersion=1&baseCookie=&ts=${Date.now()}&lmid=0`,
      secProtocol
    )
    ws.on('message', (data) => {
      downstream.enqueue(JSON.parse(data.toString()))
    })
    return ws
  }

  async function drainInitialPokes(downstream: Queue<unknown>) {
    let settled = false
    const timeout = Date.now() + 30000
    while (!settled && Date.now() < timeout) {
      const msg = (await downstream.dequeue('timeout' as any, 3000)) as any
      if (msg === 'timeout') {
        settled = true
      } else if (Array.isArray(msg) && msg[0] === 'pokeEnd') {
        const next = (await downstream.dequeue('timeout' as any, 2000)) as any
        if (next === 'timeout') {
          settled = true
        }
      }
    }
  }

  async function waitForRowPatch(
    downstream: Queue<unknown>,
    predicate: (row: any) => boolean,
    timeoutMs = 10000,
    label = 'row patch'
  ): Promise<any> {
    const seen: any[] = []
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      const remaining = Math.max(1000, deadline - Date.now())
      const msg = (await downstream.dequeue('timeout' as any, remaining)) as any
      if (msg === 'timeout') break
      if (!Array.isArray(msg) || msg[0] !== 'pokePart' || !msg[1]?.rowsPatch) {
        continue
      }
      for (const row of msg[1].rowsPatch) {
        seen.push(row)
        if (predicate(row)) return row
      }
    }
    throw new Error(
      `timed out waiting for ${label}; recent rows: ${JSON.stringify(seen.slice(-8))}`
    )
  }

  async function collectPokeRows(
    downstream: Queue<unknown>,
    windowMs = 5000
  ): Promise<any[]> {
    const rows: any[] = []
    const deadline = Date.now() + windowMs
    while (Date.now() < deadline) {
      const remaining = Math.max(1000, deadline - Date.now())
      const msg = (await downstream.dequeue('timeout' as any, remaining)) as any
      if (msg === 'timeout') break
      if (Array.isArray(msg) && msg[0] === 'pokePart' && msg[1]?.rowsPatch) {
        rows.push(...msg[1].rowsPatch)
      }
    }
    return rows
  }
})

async function waitForZero(port: number, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://localhost:${port}/`)
      if (res.ok || res.status === 404) return
    } catch {}
    await new Promise((r) => setTimeout(r, 500))
  }
  throw new Error(`zero-cache not ready on port ${port} after ${timeoutMs}ms`)
}
