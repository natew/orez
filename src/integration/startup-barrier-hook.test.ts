/**
 * End-to-end guard for the startup barrier wiring in startZeroLite (src/index.ts).
 *
 * Unlike the unit test in src/pg-proxy-startup-barrier.test.ts (which constructs
 * PgStartupBarrier and HookContext by hand), this boots the real startZeroLite
 * with a FUNCTION-form onDbReady and exercises three things together:
 *
 *   1. the barrier-creation condition covers function hooks, not just strings,
 *   2. runHook delivers a HookContext with a privileged connection to the
 *      callback (it provisions through ctx.upstreamConnectionString), and
 *   3. an ordinary programmatic client is held at PG startup until the callback
 *      finishes provisioning.
 *
 * If any of those regressed (e.g. the barrier were created only for string
 * hooks again), the ordinary client would query a not-yet-created table and the
 * final assertion would fail.
 */
import { rmSync } from 'node:fs'

import postgres from 'postgres'
import { afterEach, describe, expect, it } from 'vitest'

import { startZeroLite } from '../index.js'

import type { HookContext } from '../config.js'

describe('startZeroLite startup barrier holds ordinary clients during a function onDbReady', () => {
  let stop: (() => Promise<void>) | undefined
  let dataDir: string | undefined
  const clients: Array<ReturnType<typeof postgres>> = []

  afterEach(async () => {
    await Promise.all(clients.splice(0).map((c) => c.end({ timeout: 1 }).catch(() => {})))
    // a benign "worker is closed" can surface while ephemeral pglite workers
    // tear down; it must not fail the test.
    await stop?.().catch(() => {})
    stop = undefined
    if (dataDir) {
      try {
        rmSync(dataDir, { recursive: true, force: true })
      } catch {}
    }
    dataDir = undefined
  })

  it(
    'blocks an ordinary programmatic client until the callback provisions the schema',
    { timeout: 60_000 },
    async () => {
      const pgPort = 24_000 + Math.floor(Math.random() * 1000)
      dataDir = `.orez-hook-barrier-test-${pgPort}`

      // signals the resolved proxy port to the racing ordinary client the moment
      // the callback starts (before it sleeps or creates any table).
      let announcePort!: (port: number) => void
      const portAnnounced = new Promise<number>((resolve) => {
        announcePort = resolve
      })

      let hookProvisioned = false
      const onDbReady = async (ctx: HookContext) => {
        announcePort(ctx.pgPort)
        // widen the race window: a build that skipped the barrier for function
        // hooks would let the ordinary client query a missing table in here.
        await new Promise((resolve) => setTimeout(resolve, 250))
        const sql = postgres(ctx.upstreamConnectionString, { max: 1 })
        try {
          await sql`CREATE TABLE widget (id text PRIMARY KEY)`
          await sql`INSERT INTO widget (id) VALUES ('ready')`
        } finally {
          await sql.end({ timeout: 5 })
        }
        hookProvisioned = true
      }

      const startPromise = startZeroLite({
        pgPort,
        zeroPort: pgPort + 1,
        dataDir,
        ephemeral: true,
        skipZeroCache: true,
        logLevel: 'error',
        onDbReady,
      })

      // an ordinary (untagged) client races the migration as soon as the proxy
      // announces its port from inside the callback.
      const ordinaryPromise = (async () => {
        const realPort = await portAnnounced
        const ordinary = postgres({
          host: '127.0.0.1',
          port: realPort,
          user: 'user',
          password: 'password',
          database: 'postgres',
          max: 1,
        })
        clients.push(ordinary)
        return ordinary<{ id: string }[]>`SELECT id FROM widget ORDER BY id`
      })()

      const started = await startPromise
      stop = started.stop
      expect(hookProvisioned).toBe(true)

      // reachable only if the barrier held the ordinary client until the table
      // existed. Without the barrier this query hits a missing table and rejects.
      await expect(ordinaryPromise).resolves.toEqual([{ id: 'ready' }])
    }
  )
})
