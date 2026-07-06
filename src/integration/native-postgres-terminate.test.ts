import { rmSync } from 'node:fs'
/**
 * terminateZeroBackends sweep: when a zero-cache generation is killed or crashes,
 * its postgres backends can linger (a walsender waiting out wal_sender_timeout, a
 * backend blocked building a replication-slot snapshot) and hold the replication
 * slot / open transactions that stall the NEXT zero-cache's initial sync in
 * CREATE_REPLICATION_SLOT (canceled by its own SET lock_timeout, 55P03). orez
 * sweeps those orphans before every (re)start. this test proves the sweep kills
 * zero-* backends (including one holding a migrate-schema advisory lock, the exact
 * thing that blocks a restart) while leaving orez's own non-zero connections alone.
 */
import { createRequire } from 'node:module'
import { resolve } from 'node:path'

import { afterAll, beforeAll, describe, expect, test } from 'vitest'

import { getConfig } from '../config.js'
import { startNativePostgres, type NativePostgres } from '../native-postgres.js'

function requirePg(): any {
  const require = createRequire(import.meta.url)
  return createRequire(require.resolve('embedded-postgres'))('pg')
}

describe('terminateZeroBackends orphan sweep', { timeout: 180000 }, () => {
  let np: NativePostgres
  let dataDir: string
  let pgPort: number
  const openClients: Array<{ end: () => Promise<void> }> = []

  beforeAll(async () => {
    pgPort = 26000 + Math.floor(Math.random() * 1000)
    dataDir = resolve(`.orez-terminate-test-${Date.now()}`)
    const config = getConfig({ backend: 'postgres', pgPort, dataDir })
    np = await startNativePostgres(config)
  }, 180000)

  afterAll(async () => {
    await Promise.all(openClients.map((c) => c.end().catch(() => {})))
    if (np) await np.stop().catch(() => {})
    if (dataDir) {
      try {
        rmSync(dataDir, { recursive: true, force: true })
      } catch {}
    }
  })

  async function connect(applicationName: string) {
    const { Client } = requirePg()
    const client = new Client({
      host: '127.0.0.1',
      port: pgPort,
      user: 'user',
      password: 'password',
      database: 'postgres',
      application_name: applicationName,
    })
    // a swept orphan's client emits 'error' (57P01, terminating connection due to
    // administrator command) when its backend is killed — that's the expected
    // outcome here, so swallow it rather than let it surface as an unhandled error.
    client.on('error', () => {})
    await client.connect()
    openClients.push(client)
    return client
  }

  test('kills zero-* orphans (incl. advisory-lock holder), keeps others', async () => {
    const upstream = np.instances.postgres
    const lockName = 'migrate-schema:sweep-test'

    // an orphaned zero-cache backend, mid-transaction, holding the same kind of
    // migrate-schema advisory lock that would block a restart's schema migration.
    const orphan = await connect('zero-change-streamer')
    await orphan.query('BEGIN')
    await orphan.query('SELECT pg_advisory_xact_lock(hashtext($1))', [lockName])

    // a non-zero connection (stands in for orez's own node-pg pools / soot's app).
    const bystander = await connect('soot-app')
    await bystander.query('SELECT 1')

    // precondition: the advisory lock is held, so a fresh session can't take it.
    const contended = await upstream.query<{ locked: boolean }>(
      `SELECT pg_try_advisory_xact_lock(hashtext($1)) AS locked`,
      [lockName]
    )
    expect(contended.rows[0].locked).toBe(false)

    const terminated = await np.terminateZeroBackends()
    expect(terminated).toBeGreaterThanOrEqual(1)

    // the zero-* orphan is gone; its advisory (xact) lock was released on exit,
    // so a fresh session can now acquire it — a restart would no longer stall.
    const stillZero = await upstream.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM pg_stat_activity WHERE application_name = 'zero-change-streamer'`
    )
    expect(stillZero.rows[0].n).toBe(0)

    const freed = await upstream.query<{ locked: boolean }>(
      `SELECT pg_try_advisory_xact_lock(hashtext($1)) AS locked`,
      [lockName]
    )
    expect(freed.rows[0].locked).toBe(true)

    // the non-zero bystander was left untouched.
    const bystanderAlive = await upstream.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM pg_stat_activity WHERE application_name = 'soot-app'`
    )
    expect(bystanderAlive.rows[0].n).toBe(1)
    await expect(bystander.query('SELECT 1')).resolves.toBeTruthy()
  })

  test('is a no-op when there are no orphans', async () => {
    const terminated = await np.terminateZeroBackends()
    expect(terminated).toBe(0)
  })

  // postgres-level backstop for the same orphan class: a SIGKILL'd zero-cache
  // leaves TCP-half-open backends (idle-in-transaction holding the
  // migrate-schema advisory lock, or mid slot-snapshot) that the sweep can
  // miss when it can't run at all (OOM/swap thrash). these flags make the
  // server reap them on its own.
  test('server self-heals: idle-in-tx timeout + tcp keepalives are live', async () => {
    const upstream = np.instances.postgres
    const show = async (name: string) =>
      (await upstream.query<Record<string, string>>(`SHOW ${name}`)).rows[0][name]
    expect(await show('idle_in_transaction_session_timeout')).toBe('2min')
    expect(await show('tcp_keepalives_idle')).toBe('15')
    expect(await show('tcp_keepalives_interval')).toBe('5')
    expect(await show('tcp_keepalives_count')).toBe('3')

    // a stale idle-in-transaction session is reaped by the server without any
    // sweep — prove the mechanism end-to-end with a session-local 1s timeout
    // (the production value is 2min; only the mechanism differs by duration).
    const stale = await connect('zero-stale-tx')
    await stale.query('BEGIN')
    await stale.query(`SET idle_in_transaction_session_timeout = 1000`)
    await stale.query('SELECT txid_current()') // assign a real xid, like DDL would
    await new Promise((r) => setTimeout(r, 2500))
    const gone = await upstream.query<{ n: number }>(
      `SELECT count(*)::int AS n FROM pg_stat_activity
        WHERE application_name = 'zero-stale-tx' AND state = 'idle in transaction'`
    )
    expect(gone.rows[0].n).toBe(0)
  })
})
