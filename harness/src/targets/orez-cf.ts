// orez-cf target: the SAME sync-server core as orez-local, hosted in a
// cloudflare durable object over ctx.storage.sql (harness/cf/worker.ts,
// deployed as zharness-sync on the lightstrike account). each harness run
// gets a fresh namespace → its own DO with a fresh seeded dataset. clients
// are the real @rocicorp/zero package through the canonical orez transport,
// exactly like orez-local — only the host differs.
//
// requires ~/.zharness-cf-admin-key (set as the worker's ADMIN_KEY secret)
// for the oracle/upstream-write channel.
import { readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'

import { Zero } from '@rocicorp/zero'

import { mutators, schema } from '../fixture.js'
import { observedPullFetch, type HttpPullObservation } from '../observed-fetch.js'
import { ensureHttpPullTransport } from '../vendor/httpPullTransport.js'

import type { Rows, SyncTarget } from '../target.js'

const WORKER = process.env.ZHARNESS_CF_WORKER ?? 'https://zharness-sync.lslcf.workers.dev'

export type OrezCfTarget = SyncTarget & {
  pull(): Promise<void>
  hibernationStatus(): Promise<{
    bootID: string
    idleTeardownMs: number
    hibernations: number
  }>
}

export async function startOrezCf(opts?: {
  namespace?: string
  pullIntervalMs?: number
  onPull?: (observation: HttpPullObservation) => void
}): Promise<OrezCfTarget> {
  const ns =
    opts?.namespace ??
    `run-${Date.now().toString(36)}-${Math.floor(Math.random() * 1e6).toString(36)}`
  const adminKey =
    process.env.ZHARNESS_CF_ADMIN_KEY ??
    readFileSync(join(homedir(), '.zharness-cf-admin-key'), 'utf8').trim()
  // zero's server option allows at most ONE path component — the namespace
  // is that component
  const origin = `${WORKER}/${ns}`

  async function adminSql(query: string): Promise<Rows> {
    const response = await fetch(`${origin}/admin/sql`, {
      method: 'POST',
      headers: { 'x-admin-key': adminKey, 'content-type': 'application/json' },
      body: JSON.stringify({ query }),
    })
    if (!response.ok) {
      throw new Error(`admin/sql ${response.status}: ${await response.text()}`)
    }
    return ((await response.json()) as { rows: Rows }).rows
  }

  // first touch seeds the DO; verify reachability before handing out clients
  const probe = await adminSql(`SELECT count(*) AS n FROM project`)
  if (Number(probe[0]?.n) < 1) throw new Error('cf DO seed missing')

  const transport = ensureHttpPullTransport({
    origin,
    fetch: opts?.onPull ? observedPullFetch(opts.onPull) : undefined,
    pullIntervalMs: opts?.pullIntervalMs ?? 500,
  })

  const clients: Zero<typeof schema, typeof mutators>[] = []
  let clientN = 0

  return {
    name: 'orez-cf',

    createClient(userID: string, storage) {
      const zero = new Zero({
        server: origin,
        userID,
        auth: `token-${userID}`,
        schema,
        mutators,
        kvStore: storage?.kvStore ?? ('mem' as const),
        onClientStateNotFound: storage?.onClientStateNotFound,
        storageKey: storage?.storageKey ?? `zharness-cf-${++clientN}`,
      })
      clients.push(zero)
      return zero
    },

    async sql(query: string): Promise<Rows> {
      return adminSql(query)
    },

    async oracle(query: string): Promise<Rows> {
      return adminSql(query)
    },

    async metrics() {
      return {}
    },

    pull() {
      return transport.pull()
    },

    async hibernationStatus() {
      const response = await fetch(`${origin}/admin/status`, {
        headers: { 'x-admin-key': adminKey },
      })
      if (!response.ok) {
        throw new Error(`admin/status ${response.status}: ${await response.text()}`)
      }
      return response.json() as Promise<{
        bootID: string
        idleTeardownMs: number
        hibernations: number
      }>
    },

    async close() {
      while (clients.length) await clients.pop()?.close()
      transport.uninstall()
    },
  }
}
