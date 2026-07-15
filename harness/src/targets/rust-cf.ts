// rust-cf target: sync-core compiled to wasm inside the production-shaped
// sync-cf-host Durable Object. Each run uses a fresh namespace/DO while the
// client and oracle surface stays identical to rust-local and orez-cf.
import { readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'

import { Zero } from '@rocicorp/zero'

import { mutators, schema } from '../fixture.js'
import { observedPullFetch, type HttpPullObservation } from '../observed-fetch.js'
import { ensureHttpPullTransport } from '../vendor/httpPullTransport.js'

import type { Rows, SyncTarget } from '../target.js'

const WORKER =
  process.env.ZHARNESS_RUST_CF_WORKER ?? 'https://orez-rust-sync.lslcf.workers.dev'

export type RustCfStatus = {
  bootID: string
  idleTeardownMs: number
  hibernations: number
  databaseSizeBytes: number
  wasmMemoryBytes: number
  heapUsedBytes: number | null
  heapTotalBytes: number | null
  heapLimitBytes: number | null
  connectedWakeSockets: number
  engine: { watermark: string; floor: string } | null
  counters: Record<string, number>
}

export type RustCfTarget = SyncTarget & {
  readonly origin: string
  pull(): Promise<void>
  hibernationStatus(): Promise<RustCfStatus>
  dropNextPushResponse(): Promise<void>
  invalidate(): Promise<void>
  resetCursor(): Promise<void>
  restart(): Promise<void>
}

export async function startRustCf(opts?: {
  namespace?: string
  pullIntervalMs?: number
  onPull?: (observation: HttpPullObservation) => void
  visible?: boolean
  retainChanges?: number
  queryAware?: boolean
}): Promise<RustCfTarget> {
  const namespace =
    opts?.namespace ??
    `rust-${Date.now().toString(36)}-${Math.floor(Math.random() * 1e6).toString(36)}`
  const adminKey =
    process.env.ZHARNESS_CF_ADMIN_KEY ??
    readFileSync(join(homedir(), '.zharness-cf-admin-key'), 'utf8').trim()
  const origin = `${WORKER}/${namespace}`

  async function admin<Value>(path: string, body?: unknown): Promise<Value> {
    const response = await fetch(`${origin}${path}`, {
      method: body === undefined ? 'GET' : 'POST',
      headers: {
        'x-admin-key': adminKey,
        ...(body === undefined ? {} : { 'content-type': 'application/json' }),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    })
    if (!response.ok) {
      throw new Error(`rust-cf ${path} ${response.status}: ${await response.text()}`)
    }
    return response.json() as Promise<Value>
  }

  const seeded = await admin<{ rows: Rows }>('/admin/sql', {
    query: 'SELECT COUNT(*) AS n FROM project',
  })
  if (Number(seeded.rows[0]?.n) < 1) throw new Error('rust-cf seed missing')
  if (opts?.visible !== undefined)
    await admin('/admin/visibility', { enabled: opts.visible })
  if (opts?.retainChanges !== undefined)
    await admin('/admin/retention', { retainChanges: opts.retainChanges })
  if (opts?.queryAware !== undefined)
    await admin('/admin/query-aware', { enabled: opts.queryAware })

  const transport = ensureHttpPullTransport({
    origin,
    fetch: opts?.onPull ? observedPullFetch(opts.onPull) : undefined,
    pullIntervalMs: opts?.pullIntervalMs ?? 500,
    wake: true,
    queryForward: opts?.queryAware,
  })
  const clients: Zero<typeof schema, typeof mutators>[] = []
  let clientNumber = 0

  return {
    name: 'rust-cf',
    origin,

    createClient(userID: string, storage) {
      const zero = new Zero({
        server: origin,
        userID,
        auth: `token-${userID}`,
        schema,
        mutators,
        kvStore: storage?.kvStore ?? ('mem' as const),
        onClientStateNotFound: storage?.onClientStateNotFound,
        storageKey: storage?.storageKey ?? `zharness-rust-cf-${++clientNumber}`,
      })
      clients.push(zero)
      return zero
    },

    async sql(query: string): Promise<Rows> {
      return (await admin<{ rows: Rows }>('/admin/sql', { query })).rows
    },

    async oracle(query: string): Promise<Rows> {
      return (await admin<{ rows: Rows }>('/admin/sql', { query })).rows
    },

    async metrics() {
      return {}
    },

    pull() {
      return transport.pull()
    },

    hibernationStatus() {
      return admin<RustCfStatus>('/admin/status')
    },

    async dropNextPushResponse() {
      await admin('/admin/drop-next-push-response', {})
    },

    async invalidate() {
      await admin('/admin/invalidate', {})
    },

    async resetCursor() {
      await admin('/admin/sql', {
        query:
          'DELETE FROM _zsync_changes; UPDATE _zsync_meta SET floor = 0; UPDATE _zsync_watermark SET high = 0',
      })
    },

    async restart() {
      const before = await admin<RustCfStatus>('/admin/status')
      await fetch(`${origin}/admin/restart`, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-admin-key': adminKey,
        },
        body: '{}',
      }).catch(() => null)
      for (let attempt = 0; attempt < 100; attempt++) {
        try {
          const after = await admin<RustCfStatus>('/admin/status')
          if (after.bootID !== before.bootID) return
        } catch {}
        await new Promise((resolve) => setTimeout(resolve, 50))
      }
      throw new Error('rust-cf durable object did not restart')
    },

    async close() {
      while (clients.length) await clients.pop()?.close()
      transport.uninstall()
    },
  }
}
