// orez-cf target: the SAME sync-server core as orez-local, hosted in a
// cloudflare durable object over ctx.storage.sql (harness/cf/worker.ts,
// deployed as zharness-sync on the lightstrike account). each harness run
// gets a fresh namespace → its own DO with a fresh seeded dataset. clients
// are stock zero over on-zero's production http-pull transport, exactly like
// orez-local — only the host differs.
//
// requires ~/.zharness-cf-admin-key (set as the worker's ADMIN_KEY secret)
// for the oracle/upstream-write channel.
import { readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { Zero } from '@rocicorp/zero'
import { ensureHttpPullTransport } from '../../../../takeout/packages/on-zero/src/httpPullTransport'
import { mutators, schema } from '../fixture.js'
import type { Rows, SyncTarget } from '../target.js'

const WORKER = 'https://zharness-sync.lslcf.workers.dev'

export async function startOrezCf(opts?: {
  namespace?: string
  pullIntervalMs?: number
}): Promise<SyncTarget> {
  const ns = opts?.namespace ?? `run-${Date.now().toString(36)}-${Math.floor(Math.random() * 1e6).toString(36)}`
  const adminKey = readFileSync(join(homedir(), '.zharness-cf-admin-key'), 'utf8').trim()
  // zero's server option allows at most ONE path component — the namespace
  // is that component
  const origin = `${WORKER}/${ns}`

  async function adminSql(query: string, write: boolean): Promise<Rows> {
    const response = await fetch(`${origin}/admin/sql`, {
      method: 'POST',
      headers: { 'x-admin-key': adminKey, 'content-type': 'application/json' },
      body: JSON.stringify({ query, write }),
    })
    if (!response.ok) {
      throw new Error(`admin/sql ${response.status}: ${await response.text()}`)
    }
    return ((await response.json()) as { rows: Rows }).rows
  }

  // first touch seeds the DO; verify reachability before handing out clients
  const probe = await adminSql(`SELECT count(*) AS n FROM project`, false)
  if (Number(probe[0]?.n) < 1) throw new Error('cf DO seed missing')

  const transport = ensureHttpPullTransport({
    origin,
    pullIntervalMs: opts?.pullIntervalMs ?? 500,
  })

  const clients: Zero<typeof schema, typeof mutators>[] = []
  let clientN = 0

  return {
    name: 'orez-cf',

    createClient(userID: string) {
      const zero = new Zero({
        server: origin,
        userID,
        auth: `token-${userID}`,
        schema,
        mutators,
        kvStore: 'mem' as const,
        storageKey: `zharness-cf-${++clientN}`,
      })
      clients.push(zero)
      return zero
    },

    async sql(query: string): Promise<Rows> {
      return adminSql(query, true)
    },

    async oracle(query: string): Promise<Rows> {
      return adminSql(query, false)
    },

    async metrics() {
      return {}
    },

    async close() {
      while (clients.length) await clients.pop()?.close()
      transport.uninstall()
    },
  }
}
