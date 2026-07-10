// Adversarial CF-host lane: a remote client must never be allowed to provide
// the AST that determines server-side membership. Only named queries resolved
// by SyncHostConfig.resolveQuery may cross the host/engine boundary.
import { readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'

const worker =
  process.env.ZHARNESS_RUST_CF_WORKER ?? 'https://orez-rust-sync.lslcf.workers.dev'
const adminKey =
  process.env.ZHARNESS_CF_ADMIN_KEY ??
  readFileSync(join(homedir(), '.zharness-cf-admin-key'), 'utf8').trim()
const namespace = `query-security-${crypto.randomUUID()}`
const origin = `${worker}/${namespace}`

const enabled = await fetch(`${origin}/admin/query-aware`, {
  method: 'POST',
  headers: { 'content-type': 'application/json', 'x-admin-key': adminKey },
  body: JSON.stringify({ enabled: true }),
})
if (!enabled.ok) {
  throw new Error(`query-aware setup failed ${enabled.status}: ${await enabled.text()}`)
}

const response = await fetch(`${origin}/pull`, {
  method: 'POST',
  headers: {
    authorization: 'Bearer token-attacker',
    'content-type': 'application/json',
  },
  body: JSON.stringify({
    clientID: 'attacker',
    clientGroupID: 'attacker-group',
    cookie: null,
    queries: {
      version: 1,
      patch: [{ op: 'put', hash: 'pwn', ast: { table: 'user' } }],
    },
  }),
})
const body = (await response.json()) as { error?: string }
if (response.status !== 400 || !body.error?.includes('server-resolved named query')) {
  throw new Error(
    `raw AST injection was not rejected: ${response.status} ${JSON.stringify(body)}`
  )
}
console.log('[query-security] PASS rust-cf: client-authored raw AST rejected with 400')

const unknown = await fetch(`${origin}/pull`, {
  method: 'POST',
  headers: {
    authorization: 'Bearer token-attacker',
    'content-type': 'application/json',
  },
  body: JSON.stringify({
    clientID: 'attacker-unknown',
    clientGroupID: 'attacker-group',
    cookie: null,
    queries: {
      version: 1,
      patch: [{ op: 'put', hash: 'unknown', name: 'doesNotExist', args: [] }],
    },
  }),
})
const unknownBody = (await unknown.json()) as { error?: string }
if (unknown.status !== 400 || !unknownBody.error?.includes('unknown or unsupported')) {
  throw new Error(
    `unknown named query was not rejected as malformed: ${unknown.status} ${JSON.stringify(unknownBody)}`
  )
}
console.log('[query-security] PASS rust-cf: unknown named query rejected with 400')
