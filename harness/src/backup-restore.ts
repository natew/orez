// Test-only logical namespace backup/restore. The source is quiesced before
// capture, application tables restore into a fresh namespace, and a fresh
// baseline pull verifies the restored snapshot. No production route is changed.
import { readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

import { canonical } from './canonical.js'
import { mutators } from './fixture.js'
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
  const zero = source.createClient('backup-user')
  const projectID = `backup-${crypto.randomUUID()}`
  const taskID = `backup-task-${crypto.randomUUID()}`
  for (const request of [
    zero.mutate(
      mutators.project.create({
        id: projectID,
        ownerId: 'backup-user',
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

  if ('origin' in source) await stopCfWriter(source)
  const backup = new Map<string, Array<Record<string, unknown>>>()
  for (const table of tables) {
    backup.set(table, await source.oracle(`SELECT * FROM "${table}" ORDER BY id`))
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

  const pull = await fetch(`${origin(destination)}/pull`, {
    method: 'POST',
    headers: {
      authorization: 'Bearer token-backup-user',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      clientID: 'backup-restore-client',
      clientGroupID: 'backup-restore-group',
      cookie: null,
    }),
  })
  const body = (await pull.json()) as {
    rowsPatch?: Array<{ op?: string }>
    error?: string
  }
  if (!pull.ok)
    throw new Error(`restored snapshot pull failed ${pull.status}: ${body.error}`)
  const expectedRows = [...backup.values()].reduce((sum, rows) => sum + rows.length, 0)
  const puts = body.rowsPatch?.filter(({ op }) => op === 'put').length ?? 0
  if (puts !== expectedRows) {
    throw new Error(`restored snapshot emitted ${puts} puts, expected ${expectedRows}`)
  }

  console.log(
    JSON.stringify({
      lane: 'backup-restore',
      result: 'PASS',
      target: args.target,
      tables: tables.length,
      rows: expectedRows,
      freshSnapshotPuts: puts,
    })
  )
} finally {
  if (!sourceClosed) await source.close()
  await destination?.close()
}
