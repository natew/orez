// Test-infrastructure-only canary/rollback drill. It models one logical
// namespace with isolated old/new physical test namespaces and proves the
// operator sequence never enables both writers. It does not change a
// production route or deploy a worker.
import { readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

const { values: args } = parseArgs({
  options: {
    worker: { type: 'string' },
    'confirm-test-only': { type: 'boolean', default: false },
  },
})

if (!args['confirm-test-only']) {
  throw new Error('refusing to run without --confirm-test-only')
}

const worker = (
  args.worker ??
  process.env.ZHARNESS_RUST_CF_WORKER ??
  'https://orez-rust-sync.lslcf.workers.dev'
).replace(/\/$/, '')
if (!worker.includes('lslcf.workers.dev') && !worker.startsWith('http://127.0.0.1')) {
  throw new Error(`refusing non-test worker: ${worker}`)
}

const adminKey =
  process.env.ZHARNESS_CF_ADMIN_KEY ??
  readFileSync(join(homedir(), '.zharness-cf-admin-key'), 'utf8').trim()
const drillID = `drill-${crypto.randomUUID()}`

type Status = {
  writerEnabled: boolean
  engine: { watermark: string; floor: string } | null
  counters: Record<string, number>
}

function endpoint(label: 'old' | 'new') {
  const origin = `${worker}/${drillID}-${label}`

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
      throw new Error(`${label} ${path} ${response.status}: ${await response.text()}`)
    }
    return response.json() as Promise<Value>
  }

  async function push(clientID: string, id: number, rowID: string) {
    const response = await fetch(`${origin}/push`, {
      method: 'POST',
      headers: {
        authorization: 'Bearer token-drill-operator',
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        clientGroupID: `drill-group-${label}`,
        pushVersion: 1,
        mutations: [
          {
            type: 'custom',
            clientID,
            id,
            name: 'project.create',
            args: [{ id: rowID, ownerId: 'drill-operator', name: 'drill row' }],
          },
        ],
      }),
    })
    return {
      status: response.status,
      body: (await response.json()) as Record<string, unknown>,
    }
  }

  async function queryState() {
    await admin('/admin/query-aware', { enabled: true })
    const response = await fetch(`${origin}/pull`, {
      method: 'POST',
      headers: {
        authorization: 'Bearer token-drill-operator',
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        clientID: `drill-query-${label}`,
        clientGroupID: `drill-query-group-${label}`,
        cookie: null,
        queries: {
          version: 1,
          patch: [
            { op: 'put', hash: 'drill-all-projects', name: 'allProjects', args: [] },
          ],
        },
      }),
    })
    const body = (await response.json()) as {
      gotQueries?: { version?: number; patch?: Array<{ op?: string; hash?: string }> }
    }
    if (
      response.status !== 200 ||
      body.gotQueries?.version !== 1 ||
      body.gotQueries.patch?.[0]?.hash !== 'drill-all-projects'
    ) {
      throw new Error(`${label} query state did not acknowledge: ${response.status}`)
    }
  }

  return {
    admin,
    push,
    queryState,
    status: () => admin<Status>('/admin/status'),
    setWriter: (enabled: boolean) => admin('/admin/writer', { enabled }),
  }
}

const oldHost = endpoint('old')
const newHost = endpoint('new')

async function assertWriterOwnership(expected: 'old' | 'new' | 'none') {
  const [oldStatus, newStatus] = await Promise.all([oldHost.status(), newHost.status()])
  const enabled = [
    oldStatus.writerEnabled ? 'old' : null,
    newStatus.writerEnabled ? 'new' : null,
  ]
    .filter(Boolean)
    .join(',')
  const actual = enabled || 'none'
  if (actual !== expected) {
    throw new Error(`writer ownership mismatch: expected ${expected}, got ${actual}`)
  }
}

function expectPush(result: { status: number }, expected: number, label: string) {
  if (result.status !== expected) {
    throw new Error(`${label}: expected HTTP ${expected}, got ${result.status}`)
  }
}

// Initial route: old owns writes, new is dark.
await oldHost.setWriter(true)
await newHost.setWriter(false)
await assertWriterOwnership('old')
expectPush(
  await oldHost.push('drill-old-client', 1, `${drillID}-old-1`),
  200,
  'old initial write'
)
expectPush(
  await newHost.push('drill-new-client', 1, `${drillID}-new-rejected`),
  503,
  'dark new'
)

// Canary migration: stop old first, then start new.
await oldHost.setWriter(false)
await assertWriterOwnership('none')
await newHost.setWriter(true)
await assertWriterOwnership('new')
expectPush(
  await oldHost.push('drill-old-client', 2, `${drillID}-old-rejected`),
  503,
  'stopped old'
)
expectPush(
  await newHost.push('drill-new-client', 1, `${drillID}-new-1`),
  200,
  'new canary write'
)
await newHost.queryState()

// Rollback: stop new and prove rejection before restoring old.
await newHost.setWriter(false)
await assertWriterOwnership('none')
expectPush(
  await newHost.push('drill-new-client', 2, `${drillID}-new-after-stop`),
  503,
  'new stopped'
)
await oldHost.setWriter(true)
await assertWriterOwnership('old')
expectPush(
  await oldHost.push('drill-old-client', 2, `${drillID}-old-2`),
  200,
  'old restored'
)
await oldHost.queryState()

const [oldStatus, newStatus] = await Promise.all([oldHost.status(), newHost.status()])
if (
  (oldStatus.counters.invariantFailures ?? 0) !== 0 ||
  (newStatus.counters.invariantFailures ?? 0) !== 0
) {
  throw new Error('invariant failure counter increased during rollback drill')
}

console.log(
  JSON.stringify({
    lane: 'rollback-one-writer',
    result: 'PASS',
    drillID,
    phases: ['old-only', 'none', 'new-only', 'none', 'old-only'],
    oldEngine: oldStatus.engine,
    newEngine: newStatus.engine,
  })
)
