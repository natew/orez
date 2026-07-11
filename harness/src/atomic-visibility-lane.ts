// Dedicated live workload for the versioned atomic-visibility checker profile.
// It establishes only none-or-all visibility for one authoritative multi-append
// mutation and complete full-scope client observations. It does not claim
// convergence, realtime ordering, or general transaction semantics.
//
//   bun src/atomic-visibility-lane.ts --target orez-local --seed example
import { execFileSync } from 'node:child_process'
import { createHash, randomUUID } from 'node:crypto'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { parseArgs } from 'node:util'

import { writeConsistencyArtifacts } from './consistency/artifacts.js'
import {
  assertAtomicAuthorityRows,
  projectAtomicRead,
  validateAtomicProfileEvidence,
  type AtomicAppendEffect,
  type AtomicTaskRow,
} from './consistency/atomic-visibility-workload.js'
import {
  ATOMIC_VISIBILITY_WORKLOAD_PROFILE,
  checkAtomicVisibility,
} from './consistency/atomic-visibility.js'
import { FAULT_SCHEDULE_SCHEMA_VERSION } from './consistency/fault-schedule.js'
import { HISTORY_SCHEMA_VERSION, type MicroOp } from './consistency/history.js'
import { HistoryRecorder } from './consistency/recorder.js'
import { mutators, queries } from './fixture.js'
import { assertServerOutcome } from './server-outcome.js'

import type { FixtureZero, SyncTarget } from './target.js'

const { values: args } = parseArgs({
  options: {
    target: { type: 'string', default: 'orez-local' },
    seed: { type: 'string' },
    replay: { type: 'boolean', default: false },
    'results-dir': { type: 'string' },
  },
})

const seed = args.seed ?? randomUUID()
if (!/^[A-Za-z0-9._:-]+$/.test(seed)) {
  throw new Error(
    'seed must contain only letters, digits, dot, underscore, colon, or dash'
  )
}
const digest = createHash('sha256').update(seed).digest('hex')
const runId = `atomic-visibility-${digest.slice(0, 16)}`
const idPrefix = `${runId}-task-`
const projectIds = ['p0', 'p1']
const effects: AtomicAppendEffect[] = projectIds.map((projectId, index) => ({
  id: `${idPrefix}${index}`,
  projectId,
  rank: 1_000_000_000 + Number.parseInt(digest.slice(index * 6, index * 6 + 6), 16),
}))
const resultsDir =
  args['results-dir'] ?? join('target', 'consistency', 'atomic-visibility', runId)
const repoRoot = fileURLToPath(new URL('../..', import.meta.url))

function sqlString(value: string): string {
  return `'${value.replaceAll("'", "''")}'`
}

async function startTarget(name: string): Promise<SyncTarget> {
  if (name === 'orez-local') {
    return (await import('./targets/orez-local.js')).startOrezLocal({
      pullIntervalMs: 100,
    })
  }
  throw new Error(`unsupported atomic-visibility target ${name}`)
}

type CompleteWatcher = {
  waitFor(predicate: (rows: AtomicTaskRow[]) => boolean): Promise<AtomicTaskRow[]>
  destroy(): void
}

function watchCompleteScope(zero: FixtureZero): CompleteWatcher {
  const view = zero.materialize(queries.tasksInProjects({ projectIds }))
  const complete: AtomicTaskRow[][] = []
  const waiters = new Set<() => void>()
  view.addListener((data, resultType) => {
    if (resultType !== 'complete') return
    complete.push(
      structuredClone(data).map(({ id, projectId, rank }) => ({ id, projectId, rank }))
    )
    for (const wake of waiters) wake()
  })
  return {
    waitFor(predicate) {
      return new Promise((resolve, reject) => {
        const inspect = () => {
          const match = complete.findLast(predicate)
          if (match === undefined) return
          clearTimeout(deadline)
          waiters.delete(inspect)
          resolve(structuredClone(match))
        }
        const deadline = setTimeout(() => {
          waiters.delete(inspect)
          reject(new Error('timed out waiting for a complete full-scope observation'))
        }, 30_000)
        waiters.add(inspect)
        inspect()
      })
    },
    destroy: () => view.destroy(),
  }
}

function recordRead(
  recorder: HistoryRecorder,
  phase: 'invoke' | 'ok',
  opId: string,
  transaction: MicroOp[]
): void {
  recorder.record({
    opId,
    process: 'atomic-reader',
    clientId: 'atomic-reader',
    phase,
    kind: 'read',
    transaction,
  })
}

const target = await startTarget(args.target!)
const recorder = new HistoryRecorder(() => Math.floor(performance.now() * 1_000))
let watcher: CompleteWatcher | undefined
try {
  const ids = effects.map(({ id }) => sqlString(id)).join(', ')
  const pairs = effects
    .map(
      ({ projectId, rank }) =>
        `("projectId" = ${sqlString(projectId)} AND rank = ${rank})`
    )
    .join(' OR ')
  const preflight = (await target.oracle(
    `SELECT id, "projectId", rank FROM task WHERE id IN (${ids}) OR ${pairs}`
  )) as AtomicTaskRow[]
  validateAtomicProfileEvidence({
    profile: ATOMIC_VISIBILITY_WORKLOAD_PROFILE,
    projectIds,
    idPrefix,
    authorityPreflightRows: preflight,
  })

  const writer = target.createClient('atomic-writer')
  const observer = target.createClient('atomic-reader')
  const beforeOp = `${runId}-read-before`
  recordRead(
    recorder,
    'invoke',
    beforeOp,
    projectIds.map((key) => ({ type: 'read', key, value: null }))
  )
  watcher = watchCompleteScope(observer)
  const before = await watcher.waitFor(() => true)
  recordRead(recorder, 'ok', beforeOp, projectAtomicRead(projectIds, before))

  const mutationOp = `${runId}-mutation`
  const transaction = effects.map(({ projectId: key, rank: value }) => ({
    type: 'append' as const,
    key,
    value,
  }))
  recorder.record({
    opId: mutationOp,
    process: 'atomic-writer',
    clientId: 'atomic-writer',
    phase: 'invoke',
    kind: 'mutation',
    transaction,
  })
  const request = writer.mutate(mutators.atomicVisibility.appendGroup({ effects }))
  try {
    await assertServerOutcome(request.server, 'success', mutationOp)
    recorder.record({
      opId: mutationOp,
      process: 'atomic-writer',
      clientId: 'atomic-writer',
      phase: 'ok',
      kind: 'mutation',
      transaction,
    })
  } catch (error) {
    recorder.record({
      opId: mutationOp,
      process: 'atomic-writer',
      clientId: 'atomic-writer',
      phase: 'info',
      kind: 'mutation',
      transaction,
      error: error instanceof Error ? error.message : String(error),
    })
    throw error
  }

  const afterOp = `${runId}-read-after`
  recordRead(
    recorder,
    'invoke',
    afterOp,
    projectIds.map((key) => ({ type: 'read', key, value: null }))
  )
  const after = await watcher.waitFor((rows) =>
    effects.every((effect) =>
      rows.some(
        (row) =>
          row.id === effect.id &&
          row.projectId === effect.projectId &&
          row.rank === effect.rank
      )
    )
  )
  recordRead(recorder, 'ok', afterOp, projectAtomicRead(projectIds, after))

  const authority = (await target.oracle(
    `SELECT id, "projectId", rank FROM task WHERE id IN (${ids}) ORDER BY id`
  )) as AtomicTaskRow[]
  assertAtomicAuthorityRows(effects, authority)

  const outcome = checkAtomicVisibility(recorder.snapshot())
  const replayDir = join('target', 'consistency', 'atomic-visibility', `${runId}-replay`)
  const replay = `bun src/atomic-visibility-lane.ts --target ${args.target} --seed ${seed} --replay --results-dir ${replayDir}`
  const build = execFileSync('git', ['rev-parse', 'HEAD'], {
    cwd: repoRoot,
    encoding: 'utf8',
  }).trim()
  await writeConsistencyArtifacts({
    resultsDir,
    recorder,
    manifest: {
      schemaVersion: HISTORY_SCHEMA_VERSION,
      kind: 'orez-consistency-history',
      runId,
      seed: {
        value: seed,
        source: args.replay ? 'replay' : args.seed === undefined ? 'random' : 'fixed',
      },
      workload: {
        name: ATOMIC_VISIBILITY_WORKLOAD_PROFILE.name,
        version: ATOMIC_VISIBILITY_WORKLOAD_PROFILE.version,
      },
      target: { name: target.name, build },
      replay: { command: replay, env: {} },
    },
    schedule: {
      schemaVersion: FAULT_SCHEDULE_SCHEMA_VERSION,
      faultsRequired: false,
      plans: [],
      receipts: [],
    },
    checks: {
      schemaVersion: HISTORY_SCHEMA_VERSION,
      kind: 'orez-consistency-checks',
      checks: [
        {
          name: 'atomic-visibility',
          version: String(ATOMIC_VISIBILITY_WORKLOAD_PROFILE.version),
          input: 'history.jsonl',
          ...outcome,
        },
      ],
    },
  })
  console.log(`[atomic-visibility] ${outcome.valid ? 'PASS' : 'FAIL'} ${resultsDir}`)
  console.log(`[atomic-visibility] replay: ${replay}`)
  if (!outcome.valid) {
    throw new Error(`atomic visibility violations:\n${outcome.violations.join('\n')}`)
  }
} finally {
  watcher?.destroy()
  await target.close()
}
