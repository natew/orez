// Test-only logical namespace backup/restore. The source is quiesced before
// capture, application tables restore into a fresh namespace, and a fresh
// baseline pull verifies the restored snapshot. No production route is changed.
import { readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

import { canonical } from './canonical.js'
import { mutators, queryNameToAst } from './fixture.js'
import { assertServerOutcome } from './server-outcome.js'
import { startRustCf, type RustCfTarget } from './targets/rust-cf.js'
import { startRustLocal, type RustLocalTarget } from './targets/rust-local.js'

const { values: args } = parseArgs({
  options: { target: { type: 'string', default: 'rust-local' } },
})
if (args.target !== 'rust-local' && args.target !== 'rust-cf') {
  throw new Error('target must be rust-local or rust-cf')
}

type Target = RustCfTarget | RustLocalTarget
const start = (): Promise<Target> =>
  args.target === 'rust-cf'
    ? startRustCf({ pullIntervalMs: 0 })
    : startRustLocal({ pullIntervalMs: 0 })

const tables = ['user', 'project', 'member', 'task'] as const
const source = await start()
let sourceClosed = false
let destination: Target | undefined

function sqlValue(value: unknown): string {
  if (value === null || value === undefined) return 'NULL'
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new Error('backup contains non-finite number')
    return String(value)
  }
  if (typeof value === 'boolean') return value ? '1' : '0'
  const text = typeof value === 'string' ? value : JSON.stringify(value)
  return `'${text.replaceAll("'", "''")}'`
}

function origin(target: Target) {
  return 'origin' in target ? target.origin : `${target.baseUrl}/${target.namespace}`
}

async function stopCfWriter(target: RustCfTarget) {
  const adminKey =
    process.env.ZHARNESS_CF_ADMIN_KEY ??
    readFileSync(join(homedir(), '.zharness-cf-admin-key'), 'utf8').trim()
  const response = await fetch(`${target.origin}/admin/writer`, {
    method: 'POST',
    headers: { 'x-admin-key': adminKey, 'content-type': 'application/json' },
    body: JSON.stringify({ enabled: false }),
  })
  if (!response.ok) throw new Error(`failed to stop source writer: ${response.status}`)
  await response.arrayBuffer()
}

try {
  const ownerID = 'backup-user'
  const queryArgs = [{ ownerId: ownerID }]
  const desiredQueries = {
    version: 1,
    patch: [
      {
        op: 'put',
        hash: 'q-backup',
        name: 'projectsOwnedBy',
        args: queryArgs,
        ast: queryNameToAst('projectsOwnedBy', queryArgs),
      },
    ],
  }
  const pullQueryRows = async (target: Target, phase: 'source' | 'destination') => {
    const response = await fetch(`${origin(target)}/pull`, {
      method: 'POST',
      headers: {
        authorization: `Bearer token-${ownerID}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        clientID: `backup-restore-${phase}`,
        clientGroupID: `backup-restore-${phase}-group`,
        cookie: null,
        queries: desiredQueries,
      }),
      signal: AbortSignal.timeout(10_000),
    })
    const body = (await response.json()) as {
      rowsPatch?: Array<{ op?: string; tableName?: unknown; value?: unknown }>
      error?: string
    }
    if (!response.ok) {
      throw new Error(`${phase} query pull failed ${response.status}: ${body.error}`)
    }

    const puts = new Set<string>()
    for (const patch of body.rowsPatch ?? []) {
      if (patch.op !== 'put') continue
      const value = patch.value as Record<string, unknown> | null | undefined
      if (typeof patch.tableName !== 'string' || typeof value?.id !== 'string') {
        throw new Error(
          `${phase} query pull emitted a put without tableName and value.id`
        )
      }
      puts.add(canonical([patch.tableName, value.id]))
    }
    return [...puts].sort()
  }

  const zero = source.createClient(ownerID)
  const projectID = `backup-${crypto.randomUUID()}`
  const taskID = `backup-task-${crypto.randomUUID()}`
  for (const request of [
    zero.mutate(
      mutators.project.create({
        id: projectID,
        ownerId: ownerID,
        name: 'backup project',
      })
    ),
    zero.mutate(
      mutators.task.create({
        id: taskID,
        projectId: projectID,
        title: 'backup task',
        rank: 1,
        done: false,
      })
    ),
  ]) {
    await request.client
    await assertServerOutcome(request.server, 'success', 'backup seed mutation')
  }

  // rust-local now also exposes `origin`; only the CF DO has a writer to stop
  if (args.target === 'rust-cf') await stopCfWriter(source as RustCfTarget)
  const backup = new Map<string, Array<Record<string, unknown>>>()
  for (const table of tables) {
    backup.set(table, await source.oracle(`SELECT * FROM "${table}" ORDER BY id`))
  }
  const baselinePuts = await pullQueryRows(source, 'source')
  if (baselinePuts.length === 0) {
    throw new Error('source baseline query emitted no puts')
  }
  await source.close()
  sourceClosed = true

  destination = await start()
  for (const table of [...tables].reverse())
    await destination.sql(`DELETE FROM "${table}"`)
  for (const table of tables) {
    for (const row of backup.get(table)!) {
      const columns = Object.keys(row)
      await destination.sql(
        `INSERT INTO "${table}" (${columns.map((column) => `"${column}"`).join(', ')}) ` +
          `VALUES (${columns.map((column) => sqlValue(row[column])).join(', ')})`
      )
    }
  }

  for (const table of tables) {
    const restored = await destination.oracle(`SELECT * FROM "${table}" ORDER BY id`)
    if (canonical(restored) !== canonical(backup.get(table))) {
      throw new Error(`${table} diverged after logical restore`)
    }
  }

  const restoredPuts = await pullQueryRows(destination, 'destination')
  if (canonical(restoredPuts) !== canonical(baselinePuts)) {
    throw new Error(
      `restored query rows diverged: source=${canonical(baselinePuts)} ` +
        `destination=${canonical(restoredPuts)}`
    )
  }

  console.log(
    JSON.stringify({
      lane: 'backup-restore',
      result: 'PASS',
      target: args.target,
      tables: tables.length,
      rows: [...backup.values()].reduce((sum, rows) => sum + rows.length, 0),
      baselinePuts: baselinePuts.length,
      restoredPuts: restoredPuts.length,
    })
  )
} finally {
  if (!sourceClosed) await source.close()
  await destination?.close()
}
