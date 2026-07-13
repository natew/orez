import { PGlite } from '@electric-sql/pglite'
import { Client } from 'pg'
import postgres from 'postgres'
import { afterEach, describe, expect, it } from 'vitest'

import { getConfig } from './config.js'
import { PgStartupBarrier, startPgProxy } from './pg-proxy.js'

import type { AddressInfo, Server } from 'node:net'

describe('PG startup schema barrier', () => {
  let db: PGlite | undefined
  let server: Server | undefined
  const clients: Array<ReturnType<typeof postgres>> = []
  const pgClients: Client[] = []

  afterEach(async () => {
    await Promise.all(clients.splice(0).map((client) => client.end({ timeout: 1 })))
    await Promise.all(pgClients.splice(0).map((client) => client.end()))
    await new Promise<void>((resolve) => {
      if (!server) return resolve()
      server.close(() => resolve())
    })
    server = undefined
    await db?.close()
    db = undefined
  })

  it('lets the tagged migrator provision a cold schema before application queries run', async () => {
    db = new PGlite()
    await db.waitReady
    const applicationName = 'orez-on-db-ready-test-token'
    const barrier = new PgStartupBarrier(applicationName)
    server = await startPgProxy(db, { ...getConfig(), pgPort: 0 }, barrier)
    const port = (server.address() as AddressInfo).port

    const application = postgres({
      host: '127.0.0.1',
      port,
      user: 'user',
      password: 'password',
      database: 'postgres',
      max: 1,
    })
    // Match Chat's migration script exactly: node-postgres receives the tagged
    // connection string from ZERO_UPSTREAM_DB and opens a Client from it.
    const migrator = new Client({
      connectionString: `postgresql://user:password@127.0.0.1:${port}/postgres?application_name=${applicationName}`,
      connectionTimeoutMillis: 2_000,
    })
    clients.push(application)
    pgClients.push(migrator)

    let applicationSettled = false
    const applicationQuery = application<{ id: string }[]>`
      SELECT id FROM file ORDER BY id
    `.then(
      (rows) => {
        applicationSettled = true
        return rows
      },
      (error) => {
        applicationSettled = true
        throw error
      }
    )

    await new Promise((resolve) => setTimeout(resolve, 25))
    expect(applicationSettled).toBe(false)
    await migrator.connect()
    await migrator.query('CREATE TABLE file (id TEXT PRIMARY KEY)')
    await migrator.query("INSERT INTO file (id) VALUES ('ready')")
    expect(applicationSettled).toBe(false)

    barrier.release()
    await expect(applicationQuery).resolves.toEqual([{ id: 'ready' }])
  })

  it('surfaces migration failure to clients waiting at startup', async () => {
    const barrier = new PgStartupBarrier('migration-token')
    const failure = new Error('migration failed')
    const waiting = barrier.wait(undefined)
    barrier.fail(failure)
    await expect(waiting).rejects.toBe(failure)
  })
})
