// M6 transaction/storage fault lane. Native additionally proves SIGKILL-shaped
// durability; CF uses error/quota injection while real eviction is covered by
// the separate restart/eviction lane.
import { readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

import { startRustCf, type RustCfTarget } from './targets/rust-cf.js'
import { startRustLocal, type RustLocalTarget } from './targets/rust-local.js'

const { values: args } = parseArgs({
  options: { target: { type: 'string', default: 'rust-local' } },
})
const target: RustCfTarget | RustLocalTarget =
  args.target === 'rust-cf'
    ? await startRustCf({ pullIntervalMs: 0 })
    : args.target === 'rust-local'
      ? await startRustLocal({ pullIntervalMs: 0 })
      : (() => {
          throw new Error('target must be rust-local or rust-cf')
        })()
const cf = 'origin' in target
const origin = cf ? target.origin : `${target.baseUrl}/${target.namespace}`
const adminKey = cf
  ? (process.env.ZHARNESS_CF_ADMIN_KEY ??
    readFileSync(join(homedir(), '.zharness-cf-admin-key'), 'utf8').trim())
  : null
const prefix = `fault-${crypto.randomUUID()}`

type FaultPoint =
  | 'push_before_mutation'
  | 'push_after_write_before_commit'
  | 'push_after_commit_before_response'
  | 'pull_during_tx'
  | 'pull_after_commit'
type FaultKind = 'kill' | 'error' | 'quota'

async function arm(point: FaultPoint, kind: FaultKind) {
  const response = await fetch(`${origin}/admin/fault`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(adminKey ? { 'x-admin-key': adminKey } : {}),
    },
    body: JSON.stringify({ point, kind }),
  })
  if (!response.ok) throw new Error(`arm ${point}/${kind} failed ${response.status}`)
  await response.arrayBuffer()
}

function mutation(clientID: string, id: number, rowID: string) {
  return {
    clientGroupID: `group-${clientID}`,
    pushVersion: 1,
    mutations: [
      {
        type: 'custom',
        clientID,
        id,
        name: 'project.create',
        args: [{ id: rowID, ownerId: 'fault-user', name: 'fault probe' }],
      },
    ],
  }
}

async function push(clientID: string, id: number, rowID: string) {
  return fetch(`${origin}/push`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-fault-user',
      'content-type': 'application/json',
    },
    body: JSON.stringify(mutation(clientID, id, rowID)),
    signal: AbortSignal.timeout(5_000),
  })
}

async function pull(clientID: string) {
  return fetch(`${origin}/pull`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-fault-user',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientID,
      clientGroupID: `group-${clientID}`,
      cookie: null,
    }),
    signal: AbortSignal.timeout(5_000),
  })
}

async function counts(rowID: string, clientID: string) {
  const rows = await target.oracle(
    `SELECT (SELECT COUNT(*) FROM project WHERE id = '${rowID}') AS projectCount, ` +
      `(SELECT COUNT(*) FROM _zsync_clients WHERE clientID = '${clientID}') AS clientCount`
  )
  return {
    project: Number(rows[0]?.projectCount),
    client: Number(rows[0]?.clientCount),
  }
}

function expectStatus(response: Response, status: number, label: string) {
  if (response.status !== status) {
    throw new Error(`${label}: expected HTTP ${status}, got ${response.status}`)
  }
}

try {
  const beforeClient = `${prefix}-before`
  const beforeRow = `${prefix}-before-row`
  await arm('push_before_mutation', 'quota')
  let response = await push(beforeClient, 1, beforeRow)
  expectStatus(response, 507, 'push before mutation quota')
  await response.arrayBuffer()
  let state = await counts(beforeRow, beforeClient)
  if (state.project !== 0 || state.client !== 0) {
    throw new Error('pre-mutation quota changed durable state')
  }

  const midClient = `${prefix}-mid`
  const midRow = `${prefix}-mid-row`
  if (cf) {
    await arm('push_after_write_before_commit', 'quota')
    response = await push(midClient, 1, midRow)
    expectStatus(response, 507, 'push before commit quota')
    await response.arrayBuffer()
  } else {
    await arm('push_after_write_before_commit', 'kill')
    await push(midClient, 1, midRow).catch(() => undefined)
    await target.restart()
  }
  state = await counts(midRow, midClient)
  if (state.project !== 0 || state.client !== 0) {
    throw new Error('pre-commit fault failed to roll back row and LMID')
  }

  const afterClient = `${prefix}-after`
  const afterRow = `${prefix}-after-row`
  if (cf) {
    await arm('push_after_commit_before_response', 'error')
    response = await push(afterClient, 1, afterRow)
    expectStatus(response, 500, 'push after commit error')
    await response.arrayBuffer()
  } else {
    await arm('push_after_commit_before_response', 'kill')
    await push(afterClient, 1, afterRow).catch(() => undefined)
    await target.restart()
  }
  state = await counts(afterRow, afterClient)
  if (state.project !== 1 || state.client !== 1) {
    throw new Error('post-commit fault lost durable row or LMID')
  }
  response = await push(afterClient, 1, afterRow)
  expectStatus(response, 200, 'post-commit replay')
  await response.arrayBuffer()

  const pullDuringClient = `${prefix}-pull-during`
  await arm('pull_during_tx', 'error')
  response = await pull(pullDuringClient)
  expectStatus(response, 500, 'pull during transaction error')
  await response.arrayBuffer()
  state = await counts(`${prefix}-none`, pullDuringClient)
  if (state.client !== 0) throw new Error('in-pull fault committed client claim')

  const pullAfterClient = `${prefix}-pull-after`
  await arm('pull_after_commit', 'quota')
  response = await pull(pullAfterClient)
  expectStatus(response, 507, 'pull after commit quota')
  await response.arrayBuffer()
  state = await counts(`${prefix}-none`, pullAfterClient)
  if (state.client !== 1) throw new Error('post-pull fault lost committed client claim')

  const recoveryClient = `${prefix}-recovery`
  const recoveryRow = `${prefix}-recovery-row`
  response = await push(recoveryClient, 1, recoveryRow)
  expectStatus(response, 200, 'valid push after fault corpus')
  await response.arrayBuffer()
  state = await counts(recoveryRow, recoveryClient)
  if (state.project !== 1 || state.client !== 1) {
    throw new Error('host did not recover after fault corpus')
  }

  console.log(
    JSON.stringify({
      lane: 'storage-transaction-faults',
      result: 'PASS',
      target: args.target,
      points: 5,
      nativeKillDurability: !cf,
    })
  )
} finally {
  await target.close()
}
