import { spawn, type ChildProcess } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { Zero } from '@rocicorp/zero'

import { mutators, schema } from '../fixture.js'
import { observedPullFetch, type HttpPullObservation } from '../observed-fetch.js'
import { ensureHttpPullTransport } from '../vendor/httpPullTransport.js'

import type { Rows, SyncTarget } from '../target.js'

export type OrezLocalProcessTarget = SyncTarget & {
  readonly databaseFile: string
  readonly pid: number
  crashAndRestart(downForMs?: number): Promise<{ before: number; after: number }>
  pull(): Promise<void>
}

async function unusedPort() {
  const probe = createServer()
  await new Promise<void>((resolve) => probe.listen(0, '127.0.0.1', resolve))
  const address = probe.address()
  if (!address || typeof address === 'string') throw new Error('failed to allocate port')
  await new Promise<void>((resolve, reject) =>
    probe.close((error) => (error ? reject(error) : resolve()))
  )
  return address.port
}

async function processExit(child: ChildProcess, timeoutMs = 10_000) {
  if (child.exitCode !== null || child.signalCode !== null) return
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error('child process did not exit')),
      timeoutMs
    )
    child.once('exit', () => {
      clearTimeout(timer)
      resolve()
    })
  })
}

export async function startOrezLocalProcess(opts?: {
  pullIntervalMs?: number
  onPull?: (observation: HttpPullObservation) => void
}): Promise<OrezLocalProcessTarget> {
  const directory = mkdtempSync(join(tmpdir(), 'zharness-process-'))
  const databaseFile = join(directory, 'authority.sqlite')
  const port = await unusedPort()
  const origin = `http://127.0.0.1:${port}`
  const childFile = fileURLToPath(
    new URL('./orez-local-process-child.ts', import.meta.url)
  )
  let child: ChildProcess | undefined
  let childLogs = ''

  const startChild = async () => {
    const next = spawn(process.execPath, [childFile], {
      env: {
        ...process.env,
        ZHARNESS_PROCESS_PORT: String(port),
        ZHARNESS_PROCESS_DB: databaseFile,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    child = next
    next.stdout?.on('data', (chunk) => {
      childLogs += String(chunk)
    })
    next.stderr?.on('data', (chunk) => {
      childLogs += String(chunk)
    })
    const started = Date.now()
    let lastError: unknown
    while (Date.now() - started < 15_000) {
      if (next.exitCode !== null) {
        throw new Error(`sync child exited ${next.exitCode}: ${childLogs}`)
      }
      try {
        const response = await fetch(`${origin}/admin/health`)
        if (response.ok) return
      } catch (error) {
        lastError = error
      }
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
    throw new Error(`sync child did not become ready: ${String(lastError)}\n${childLogs}`)
  }
  await startChild()

  const transport = ensureHttpPullTransport({
    origin,
    fetch: observedPullFetch(opts?.onPull),
    pullIntervalMs: opts?.pullIntervalMs ?? 100,
  })
  const clients: Zero<typeof schema, typeof mutators>[] = []
  let clientN = 0

  async function adminSql(query: string): Promise<Rows> {
    const response = await fetch(`${origin}/admin/sql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ query }),
    })
    if (!response.ok) throw new Error(`process admin/sql ${response.status}`)
    return ((await response.json()) as { rows: Rows }).rows
  }

  return {
    name: 'orez-local-process',
    databaseFile,
    get pid() {
      if (!child?.pid) throw new Error('sync child is not running')
      return child.pid
    },

    createClient(userID: string, storage) {
      const zero = new Zero({
        server: origin,
        userID,
        auth: `token-${userID}`,
        schema,
        mutators,
        kvStore: storage?.kvStore ?? ('mem' as const),
        onClientStateNotFound: storage?.onClientStateNotFound,
        storageKey: storage?.storageKey ?? `zharness-process-${++clientN}`,
      })
      clients.push(zero)
      return zero
    },

    sql: adminSql,
    oracle: adminSql,
    async metrics() {
      return {}
    },
    pull() {
      return transport.pull()
    },

    async crashAndRestart(downForMs = 1_500) {
      const previous = child
      if (!previous?.pid) throw new Error('sync child is not running')
      const before = previous.pid
      child = undefined
      previous.kill('SIGKILL')
      await processExit(previous)
      await new Promise((resolve) => setTimeout(resolve, downForMs))
      await startChild()
      const after = child!.pid!
      if (after === before) throw new Error('sync child PID did not change')
      return { before, after }
    },

    async close() {
      while (clients.length) await clients.pop()?.close()
      transport.uninstall()
      const closing = child
      child = undefined
      if (closing) {
        closing.kill('SIGTERM')
        await processExit(closing).catch(() => {
          closing.kill('SIGKILL')
        })
      }
      rmSync(directory, { recursive: true, force: true })
    },
  }
}
