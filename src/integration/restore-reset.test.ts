/**
 * regression test for restore/reset integration.
 *
 * covers the real integration boundary that previously regressed:
 * - restore data through wire protocol
 * - trigger full zero-state reset via pid-file + SIGUSR1 (same path as pg_restore)
 * - verify zero-cache restarts and live replication still works
 */

import { existsSync, readFileSync, rmSync, unlinkSync, writeFileSync } from 'node:fs'
import { homedir, tmpdir } from 'node:os'
import { join } from 'node:path'

import { loadModule } from 'pgsql-parser'
import postgres from 'postgres'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import WebSocket from 'ws'

import { execDumpFile } from '../cli.js'
import { startZeroLite } from '../index.js'

import type { PGlite } from '@electric-sql/pglite'

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

function generateFallbackDump(): string {
  return [
    'SET statement_timeout = 0;',
    "SET client_encoding = 'UTF8';",
    'SET standard_conforming_strings = on;',
    '',
    'CREATE TABLE IF NOT EXISTS restore_seed (',
    '  id integer PRIMARY KEY,',
    '  note text NOT NULL',
    ');',
    '',
    "INSERT INTO restore_seed (id, note) VALUES (1, 'seeded by fallback dump');",
    '',
  ].join('\n')
}

function resolveDumpFile(): { path: string; cleanup: boolean } {
  const envDump = process.env.OREZ_RESTORE_SQL_DUMP
  if (envDump && existsSync(envDump)) {
    return { path: envDump, cleanup: false }
  }

  const chatCandidates = [
    join(homedir(), 'chat', 'tmp', 'restore.sql'),
    join(homedir(), 'chat', 'tmp', 'backup.sql'),
    join(homedir(), 'chat', 'restore.sql'),
    join(homedir(), 'chat', 'backup.sql'),
  ]
  for (const candidate of chatCandidates) {
    if (existsSync(candidate)) {
      return { path: candidate, cleanup: false }
    }
  }

  const tmpDump = join(tmpdir(), `orez-restore-reset-${Date.now()}.sql`)
  writeFileSync(tmpDump, generateFallbackDump())
  return { path: tmpDump, cleanup: true }
}

describe('restore/reset integration regression', { timeout: 150_000 }, () => {
  let db: PGlite
  let pgPort: number
  let zeroPort: number
  let shutdown: () => Promise<void>
  let dataDir: string
  let dumpFile: string
  let dumpFileIsTemp = false

  beforeAll(async () => {
    await loadModule()

    const dump = resolveDumpFile()
    dumpFile = dump.path
    dumpFileIsTemp = dump.cleanup

    dataDir = `.orez-restore-reset-test-${Date.now()}`

    const started = await startZeroLite({
      pgPort: 27000 + Math.floor(Math.random() * 1000),
      zeroPort: 28000 + Math.floor(Math.random() * 1000),
      dataDir,
      logLevel: 'warn',
      skipZeroCache: false,
    })

    db = started.db
    pgPort = started.pgPort
    zeroPort = started.zeroPort
    shutdown = started.stop

    await waitForZero(zeroPort, 90_000)
  }, 120_000)

  afterAll(async () => {
    if (shutdown) await shutdown()
    if (dataDir) {
      try {
        rmSync(dataDir, { recursive: true, force: true })
      } catch {}
    }
    if (dumpFileIsTemp && dumpFile) {
      try {
        unlinkSync(dumpFile)
      } catch {}
    }
  })

  test('wire restore + pid signal full reset keeps zero-cache healthy', async () => {
    const sql = postgres({
      host: '127.0.0.1',
      port: pgPort,
      user: 'user',
      password: 'password',
      database: 'postgres',
      max: 1,
    })

    try {
      const wireDb = { exec: (query: string) => sql.unsafe(query) as Promise<unknown> }
      await execDumpFile(wireDb, dumpFile)
    } finally {
      await sql.end({ timeout: 1 }).catch(() => {})
    }

    // mirror pg_restore behavior: read pid file and signal SIGUSR1 for full reset
    const pidFile = join(dataDir, 'orez.pid')
    const pid = Number(readFileSync(pidFile, 'utf-8').trim())
    expect(pid).toBeGreaterThan(0)
    process.kill(pid, 'SIGUSR1')

    await waitForZero(zeroPort, 90_000)

    // prove zero-cache is alive after reset and still streams live writes
    await db.exec(`
      CREATE TABLE IF NOT EXISTS reset_probe (
        id text PRIMARY KEY,
        value text NOT NULL
      )
    `)

    const downstream = new Queue<unknown>()
    const ws = connectAndSubscribe(zeroPort, downstream, {
      table: 'reset_probe',
      orderBy: [['id', 'asc']],
    })

    try {
      await drainInitialPokes(downstream)

      await db.query(`INSERT INTO reset_probe (id, value) VALUES ($1, $2)`, [
        `post-reset-${Date.now()}`,
        'ok',
      ])

      const poke = await waitForPokePart(downstream, 30_000)
      expect(poke.rowsPatch).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            op: 'put',
            tableName: 'reset_probe',
            value: expect.objectContaining({
              value: 'ok',
            }),
          }),
        ])
      )
    } finally {
      ws.close()
    }
  })
})

function connectAndSubscribe(
  port: number,
  downstream: Queue<unknown>,
  query: Record<string, unknown>
): WebSocket {
  const ws = new WebSocket(
    `ws://localhost:${port}/sync/v4/connect` +
      `?clientGroupID=restore-reset-cg-${Date.now()}&clientID=restore-reset-client&wsid=ws1&schemaVersion=1&baseCookie=&ts=${Date.now()}&lmid=0`
  )

  ws.on('message', (data) => {
    downstream.enqueue(JSON.parse(data.toString()))
  })

  ws.on('open', () => {
    ws.send(
      JSON.stringify([
        'initConnection',
        {
          desiredQueriesPatch: [{ op: 'put', hash: 'q1', ast: query }],
        },
      ])
    )
  })

  return ws
}

async function drainInitialPokes(downstream: Queue<unknown>) {
  let settled = false
  const timeout = Date.now() + 30_000

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

async function waitForPokePart(
  downstream: Queue<unknown>,
  timeoutMs = 10_000
): Promise<Record<string, any>> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const remaining = Math.max(1000, deadline - Date.now())
    const msg = (await downstream.dequeue('timeout' as any, remaining)) as any
    if (msg === 'timeout') throw new Error('timed out waiting for pokePart')
    if (Array.isArray(msg) && msg[0] === 'pokePart' && msg[1]?.rowsPatch) {
      return msg[1]
    }
  }
  throw new Error('timed out waiting for pokePart')
}

async function waitForZero(port: number, timeoutMs = 30_000) {
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
