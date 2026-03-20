/**
 * integration test for zero-cache embedded mode.
 *
 * validates that zero-cache can run in-process with SINGLE_PROCESS=1,
 * connected to PGlite via the TCP proxy. this is the same pipeline as
 * the full integration test but without child_process.fork().
 *
 * test flow:
 * 1. create PGlite instances (postgres, cvr, cdb)
 * 2. start TCP proxy
 * 3. start zero-cache in-process via startZeroCacheEmbed()
 * 4. connect WebSocket client and verify sync
 */

import { describe, test, expect, beforeAll, afterAll } from 'vitest'
import { resolve } from 'node:path'
import { mkdirSync } from 'node:fs'
import WebSocket from 'ws'

import { installChangeTracking } from '../replication/change-tracker.js'
import { startPgProxy } from '../pg-proxy.js'
import {
  createPGliteInstances,
  type PGliteInstances,
} from '../pglite-manager.js'
import { getConfig, getConnectionString } from '../config.js'
import { startZeroCacheEmbed, type ZeroCacheEmbed } from './zero-cache-embed.js'
import {
  ensureTablesInPublications,
  installAllowAllPermissions,
} from '../integration/test-permissions.js'

import type { PGlite } from '@electric-sql/pglite'

const SYNC_PROTOCOL_VERSION = 49

function encodeSecProtocols(
  initConnectionMessage: unknown,
  authToken: string | undefined
): string {
  const payload = JSON.stringify({ initConnectionMessage, authToken })
  return encodeURIComponent(Buffer.from(payload, 'utf-8').toString('base64'))
}

describe(
  'zero-cache embed integration',
  { timeout: 120000 },
  () => {
    let db: PGlite
    let instances: PGliteInstances
    let pgServer: ReturnType<Awaited<ReturnType<typeof startPgProxy>>>
    let embed: ZeroCacheEmbed
    let zeroPort: number
    let pgPort: number
    let dataDir: string

    beforeAll(async () => {
      // use random ports to avoid conflicts with other tests
      pgPort = 24000 + Math.floor(Math.random() * 1000)
      zeroPort = pgPort + 100

      dataDir = `.orez-embed-test-${Date.now()}`

      const config = getConfig({
        pgPort,
        zeroPort,
        dataDir,
        logLevel: 'info',
        useWorkerThreads: false,
        singleDb: false,
      })

      mkdirSync(dataDir, { recursive: true })

      // create PGlite instances
      instances = await createPGliteInstances(config)
      db = instances.postgres

      // create test table
      await db.exec(`
        CREATE TABLE IF NOT EXISTS foo (
          id TEXT PRIMARY KEY,
          value TEXT,
          num INTEGER
        )
      `)

      // set up publications
      const pubName = `orez_zero_public`
      process.env.ZERO_APP_PUBLICATIONS = pubName
      await db.exec(`CREATE PUBLICATION "${pubName}"`).catch(() => {})
      await db.exec(`ALTER PUBLICATION "${pubName}" ADD TABLE "public"."foo"`).catch(() => {})

      // install change tracking
      await installChangeTracking(db)

      // install allow-all permissions for test
      await installAllowAllPermissions(db, ['foo'])
      await ensureTablesInPublications(db, ['foo'])

      // start TCP proxy
      pgServer = await startPgProxy(instances, config)

      // start zero-cache in-process
      const upstreamDb = getConnectionString(config, 'postgres')
      const cvrDb = getConnectionString(config, 'zero_cvr')
      const changeDb = getConnectionString(config, 'zero_cdb')
      const replicaFile = resolve(dataDir, 'zero-replica.db')

      console.log(`[embed-test] starting in-process zero-cache on port ${zeroPort}`)
      console.log(`[embed-test] upstream: ${upstreamDb}`)

      embed = await startZeroCacheEmbed({
        pglite: db,
        upstreamDb,
        cvrDb,
        changeDb,
        replicaFile,
        port: zeroPort,
        publications: [pubName],
        env: {
          ZERO_LOG_LEVEL: 'info',
        },
      })

      console.log(`[embed-test] zero-cache ready on port ${embed.port}`)

      // wait for HTTP health check
      await waitForZero(zeroPort, 30000)
      console.log(`[embed-test] health check passed`)
    }, 120000)

    afterAll(async () => {
      if (embed) await embed.stop()
      if (pgServer) pgServer.close()
      if (instances) {
        await instances.postgres.close().catch(() => {})
        await instances.cvr.close().catch(() => {})
        await instances.cdb.close().catch(() => {})
      }
      if (dataDir) {
        const { rmSync } = await import('node:fs')
        try {
          rmSync(dataDir, { recursive: true, force: true })
        } catch {}
      }
    })

    test('zero-cache is ready', () => {
      expect(embed.ready).toBe(true)
      expect(embed.port).toBe(zeroPort)
    })

    test('accepts WebSocket connections', async () => {
      const cg = `test-cg-${Date.now()}`
      const cid = `test-client-${Date.now()}`
      const secProtocol = encodeSecProtocols(
        [
          'initConnection',
          {
            desiredQueriesPatch: [],
            clientSchema: {
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
            },
          },
        ],
        undefined
      )
      const ws = new WebSocket(
        `ws://localhost:${zeroPort}/sync/v${SYNC_PROTOCOL_VERSION}/connect` +
          `?clientGroupID=${cg}&clientID=${cid}&wsid=ws1&schemaVersion=1&baseCookie=&ts=${Date.now()}&lmid=0`,
        secProtocol
      )

      // collect messages — attach listener before open to catch everything
      const messages: unknown[] = []
      ws.on('message', (data) => {
        messages.push(JSON.parse(data.toString()))
      })

      const connected = new Promise<void>((resolve, reject) => {
        ws.on('open', resolve)
        ws.on('error', reject)
        setTimeout(() => reject(new Error('ws connect timeout')), 10000)
      })

      await connected

      // wait for messages to arrive
      const deadline = Date.now() + 10000
      while (Date.now() < deadline && !messages.some(
        (m) => Array.isArray(m) && m[0] === 'connected'
      )) {
        await new Promise((r) => setTimeout(r, 100))
      }

      const connectedMsg = messages.find(
        (m) => Array.isArray(m) && m[0] === 'connected'
      )
      expect(connectedMsg).toMatchObject(['connected', { wsid: 'ws1' }])
      ws.close()
    })

    test('live replication: insert triggers poke', async () => {
      // insert data
      await db.query(`INSERT INTO foo (id, value, num) VALUES ($1, $2, $3)`, [
        'embed-row',
        'hello-embed',
        42,
      ])

      // wait for replication to deliver
      await waitForReplicationCatchup(db)
      await new Promise((r) => setTimeout(r, 1000))

      // connect and subscribe
      const downstream: unknown[] = []
      const ws = connectAndSubscribe(zeroPort, downstream)

      // wait for poke with our data
      const deadline = Date.now() + 30000
      let found = false
      while (Date.now() < deadline && !found) {
        await new Promise((r) => setTimeout(r, 500))
        for (const msg of downstream) {
          if (
            Array.isArray(msg) &&
            msg[0] === 'pokePart' &&
            msg[1]?.rowsPatch
          ) {
            for (const patch of msg[1].rowsPatch) {
              if (
                patch.op === 'put' &&
                patch.tableName === 'foo' &&
                patch.value?.id === 'embed-row'
              ) {
                found = true
                break
              }
            }
          }
        }
      }

      ws.close()
      expect(found).toBe(true)
    })
  }
)

function connectAndSubscribe(
  port: number,
  downstream: unknown[]
): WebSocket {
  const cg = `test-cg-${Date.now()}`
  const cid = `test-client-${Date.now()}`
  const secProtocol = encodeSecProtocols(
    [
      'initConnection',
      {
        desiredQueriesPatch: [
          {
            op: 'put',
            hash: 'q1',
            ast: {
              table: 'foo',
              orderBy: [['id', 'asc']],
            },
          },
        ],
        clientSchema: {
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
        },
      },
    ],
    undefined
  )
  const ws = new WebSocket(
    `ws://localhost:${port}/sync/v${SYNC_PROTOCOL_VERSION}/connect` +
      `?clientGroupID=${cg}&clientID=${cid}&wsid=ws1&schemaVersion=1&baseCookie=&ts=${Date.now()}&lmid=0`,
    secProtocol
  )

  ws.on('message', (data) => {
    downstream.push(JSON.parse(data.toString()))
  })

  return ws
}

async function waitForReplicationCatchup(
  pglite: PGlite,
  timeoutMs = 15000
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const result = await pglite.query<{ count: string }>(
      `SELECT count(*)::text as count FROM _orez._zero_changes`
    )
    if (Number(result.rows[0]?.count) === 0) return
    await new Promise((r) => setTimeout(r, 100))
  }
}

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
