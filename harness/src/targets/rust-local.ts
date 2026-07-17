// rust-local target: the native sync-native host (axum + rusqlite, one sqlite
// file per namespace, WAL, one serialized writer per namespace) under test.
// modeled on orez-local-process.ts — it builds the release binary once, spawns
// it on a temp data dir, and points stock zero clients at it through the same
// vendored http-pull transport every other target uses. a fresh namespace per
// target instance isolates concurrent lanes on one process.
//
// this is the M2 conformance target: every required lane runs against it
// exactly as it runs against orez-local, so the native host is proved
// differentially against stock zero-cache with no target-specific normalizers.
import { spawn, type ChildProcess } from 'node:child_process'
import { execFile } from 'node:child_process'
import { randomBytes } from 'node:crypto'
import { mkdtempSync, rmSync } from 'node:fs'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'

import { Zero } from '@rocicorp/zero'

import { mutators, queryNameToAst, schema } from '../fixture.js'
import { observedPullFetch, type HttpPullObservation } from '../observed-fetch.js'
import { ensureHttpPullTransport } from '../vendor/httpPullTransport.js'

import type { Rows, SyncTarget } from '../target.js'

const execFileAsync = promisify(execFile)

const REPO_ROOT = fileURLToPath(new URL('../../../', import.meta.url))
const BINARY = join(REPO_ROOT, 'target', 'release', 'sync-native')

export type RustLocalTarget = SyncTarget & {
  readonly baseUrl: string
  readonly origin: string
  readonly namespace: string
  readonly databaseFile: string
  readonly adminKey: string
  readonly pid: number
  // fault hooks shared with the reference targets (see reconnect/eviction lanes)
  pull(): Promise<void>
  crashAndRestart(downForMs?: number): Promise<{ before: number; after: number }>
  restart(downForMs?: number): Promise<void>
  dropNextPushResponse(): Promise<void>
  invalidate(): Promise<void>
  resetCursor(): Promise<void>
}

// build the release binary once per process; concurrent lanes share the build.
let buildPromise: Promise<void> | undefined
function ensureBinaryBuilt(): Promise<void> {
  if (!buildPromise) {
    const cargoBin = join(process.env.HOME ?? '', '.cargo', 'bin')
    const env = {
      ...process.env,
      PATH: `${cargoBin}:${process.env.PATH ?? ''}`,
    }
    buildPromise = execFileAsync('cargo', ['build', '--release', '-p', 'sync-native'], {
      cwd: REPO_ROOT,
      env,
    }).then(() => undefined)
  }
  return buildPromise
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

export async function startRustLocal(opts?: {
  pullIntervalMs?: number
  retainChanges?: number
  // baseline-pull change-row cap (--max-change-rows). small values (1-2) cut a
  // mutation's row effects and its lmid ack onto separate pulls, exercising the
  // capped-diff path a default host never reaches.
  maxChangeRows?: number
  visible?: boolean
  queryAware?: boolean
  onPull?: (observation: HttpPullObservation) => void
  fetch?: typeof fetch
}): Promise<RustLocalTarget> {
  await ensureBinaryBuilt()

  const directory = mkdtempSync(join(tmpdir(), 'zharness-rust-'))
  const namespace = `rust${Date.now().toString(36)}${Math.floor(Math.random() * 1e6).toString(36)}`
  const databaseFile = join(directory, `${namespace}.sqlite`)
  const port = await unusedPort()
  const baseUrl = `http://127.0.0.1:${port}`
  const origin = `${baseUrl}/${namespace}`
  const adminToken = randomBytes(32).toString('hex')

  const spawnArgs = ['--data-dir', directory, '--port', String(port)]
  if (opts?.retainChanges !== undefined)
    spawnArgs.push('--retain-changes', String(opts.retainChanges))
  if (opts?.maxChangeRows !== undefined)
    spawnArgs.push('--max-change-rows', String(opts.maxChangeRows))
  if (opts?.visible) spawnArgs.push('--visible')
  if (opts?.queryAware) spawnArgs.push('--query-aware')

  let child: ChildProcess | undefined
  let childLogs = ''

  const startChild = async () => {
    const next = spawn(BINARY, spawnArgs, {
      env: { ...process.env, SYNC_NATIVE_ADMIN_TOKEN: adminToken },
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
    while (Date.now() - started < 20_000) {
      if (next.exitCode !== null) {
        throw new Error(`sync-native exited ${next.exitCode}: ${childLogs}`)
      }
      try {
        const response = await fetch(`${baseUrl}/admin/health`, {
          headers: { 'x-admin-key': adminToken },
        })
        if (response.ok) return
      } catch (error) {
        lastError = error
      }
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
    throw new Error(
      `sync-native did not become ready: ${String(lastError)}\n${childLogs}`
    )
  }
  await startChild()

  const transport = ensureHttpPullTransport({
    origin,
    fetch: observedPullFetch(opts?.onPull, opts?.fetch),
    pullIntervalMs: opts?.pullIntervalMs ?? 100,
    // subscribe to the native wake channel: a push commit wakes the other
    // clients for a push-shaped pull, with the interval poll as safety net.
    wake: true,
    // query-aware: ship desired queries (name+args resolved to AST) to the
    // host and take got-query acks from the server, matching --query-aware.
    queryTransform: opts?.queryAware ? queryNameToAst : undefined,
  })
  const clients: Zero<typeof schema, typeof mutators>[] = []
  let clientN = 0

  async function adminSql(query: string): Promise<Rows> {
    const response = await fetch(`${origin}/admin/sql`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-admin-key': adminToken,
      },
      body: JSON.stringify({ query }),
    })
    if (!response.ok) throw new Error(`rust-local admin/sql ${response.status}`)
    return ((await response.json()) as { rows: Rows }).rows
  }

  async function adminPost(path: string): Promise<void> {
    const response = await fetch(`${origin}/admin/${path}`, {
      method: 'POST',
      headers: { 'x-admin-key': adminToken },
    })
    if (!response.ok) throw new Error(`rust-local admin/${path} ${response.status}`)
  }

  return {
    name: 'rust-local',
    baseUrl,
    origin,
    namespace,
    databaseFile,
    adminKey: adminToken,
    get pid() {
      if (!child?.pid) throw new Error('sync-native is not running')
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
        storageKey: storage?.storageKey ?? `zharness-rust-${++clientN}`,
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
    dropNextPushResponse() {
      return adminPost('drop-next-push-response')
    },
    invalidate() {
      return adminPost('invalidate')
    },
    resetCursor() {
      return adminPost('reset-cursor')
    },

    // hard kill + reopen on the same data dir + port (eviction lane). the
    // sqlite file persists (WAL + synchronous=FULL) so cookies stay monotonic.
    async crashAndRestart(downForMs = 1_500) {
      const previous = child
      if (!previous?.pid) throw new Error('sync-native is not running')
      const before = previous.pid
      child = undefined
      previous.kill('SIGKILL')
      await processExit(previous)
      await new Promise((resolve) => setTimeout(resolve, downForMs))
      await startChild()
      const after = child!.pid!
      if (after === before) throw new Error('sync-native PID did not change')
      return { before, after }
    },

    // graceful restart on the same files, keeping the change log (reconnect
    // lane's persisted-cookie recovery across an HTTP host restart).
    async restart(downForMs = 100) {
      const previous = child
      child = undefined
      if (previous) {
        previous.kill('SIGTERM')
        await processExit(previous).catch(() => previous.kill('SIGKILL'))
      }
      await new Promise((resolve) => setTimeout(resolve, downForMs))
      await startChild()
    },

    async close() {
      while (clients.length) await clients.pop()?.close()
      transport.uninstall()
      const closing = child
      child = undefined
      if (closing) {
        closing.kill('SIGTERM')
        await processExit(closing).catch(() => closing.kill('SIGKILL'))
      }
      rmSync(directory, { recursive: true, force: true })
    },
  }
}
