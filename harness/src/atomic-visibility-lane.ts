// Dedicated live workload for the versioned atomic-visibility checker profile.
// It establishes only none-or-all visibility for one authoritative multi-append
// mutation and complete full-scope client observations. It does not claim
// convergence, realtime ordering, or general transaction semantics.
//
//   bun src/atomic-visibility-lane.ts --target rust-local --seed example
import { execFileSync } from 'node:child_process'
import { createHash, randomUUID } from 'node:crypto'
import { basename, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { parseArgs } from 'node:util'

import {
  CHECKS_SCHEMA_VERSION,
  writeConsistencyArtifacts,
} from './consistency/artifacts.js'
import {
  AtomicObservationCollector,
  atomicReplayCommand,
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
    target: { type: 'string', default: 'rust-local' },
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
const scenarioId = `atomic-visibility-${digest.slice(0, 16)}`
const idPrefix = `${scenarioId}-task-`
const projectIds = ['p0', 'p1']
const effects: AtomicAppendEffect[] = projectIds.map((projectId, index) => ({
  id: `${idPrefix}${index}`,
  projectId,
  rank: 1_000_000_000 + Number.parseInt(digest.slice(index * 6, index * 6 + 6), 16),
}))
const defaultResultsName = args.replay
  ? `${scenarioId}-replay-${randomUUID().slice(0, 8)}`
  : scenarioId
const resultsDir =
  args['results-dir'] ??
  join('target', 'consistency', 'atomic-visibility', defaultResultsName)
const runId = basename(resultsDir)
const repoRoot = fileURLToPath(new URL('../..', import.meta.url))
const dirty = execFileSync(
  'git',
  [
    'status',
    '--porcelain',
    '--untracked-files=all',
    '--',
    'src',
    'harness/src',
    'package.json',
    'bun.lock',
    'harness/package.json',
    'harness/bun.lock',
  ],
  { cwd: repoRoot, encoding: 'utf8' }
).trim()
if (dirty !== '') {
  throw new Error(`refusing evidence run with dirty executable inputs:\n${dirty}`)
}
const build = execFileSync('git', ['rev-parse', 'HEAD'], {
  cwd: repoRoot,
  encoding: 'utf8',
}).trim()

function sqlString(value: string): string {
  return `'${value.replaceAll("'", "''")}'`
}

async function startTarget(name: string): Promise<SyncTarget> {
  if (name === 'orez-local') {
    return (await import('./targets/orez-local.js')).startOrezLocal({
      pullIntervalMs: 100,
    })
  }
  if (name === 'rust-local') {
    return (await import('./targets/rust-local.js')).startRustLocal({
      pullIntervalMs: 100,
    })
  }
  throw new Error(`unsupported atomic-visibility target ${name}`)
}

type CompleteWatcher = {
  initial(timeoutMs: number): Promise<AtomicTaskRow[]>
  startCollecting(collector: AtomicObservationCollector): void
  waitForTerminal(timeoutMs: number): Promise<'partial' | 'all'>
  destroy(): void
}

function watchCompleteScope(zero: FixtureZero): CompleteWatcher {
  const view = zero.materialize(queries.tasksInProjects({ projectIds }))
  let initial: AtomicTaskRow[] | undefined
  let resolveInitial: ((rows: AtomicTaskRow[]) => void) | undefined
  let collector: AtomicObservationCollector | undefined
  const pending: AtomicTaskRow[][] = []
  let terminal: 'partial' | 'all' | undefined
  let collectionError: unknown
  let resolveTerminal: ((result: 'partial' | 'all') => void) | undefined
  let rejectTerminal: ((error: unknown) => void) | undefined
  const dispatch = (rows: AtomicTaskRow[]) => {
    if (terminal !== undefined) return
    try {
      const classification = collector!.observe(rows)
      if (classification !== 'none') {
        terminal = classification
        resolveTerminal?.(classification)
      }
    } catch (error) {
      collectionError = error
      rejectTerminal?.(error)
    }
  }
  view.addListener((data, resultType) => {
    if (resultType !== 'complete') return
    const rows = structuredClone(data).map(({ id, projectId, rank }) => ({
      id,
      projectId,
      rank,
    }))
    if (initial === undefined) {
      initial = rows
      resolveInitial?.(structuredClone(rows))
    } else if (collector === undefined) pending.push(rows)
    else dispatch(rows)
  })
  return {
    initial(timeoutMs) {
      if (initial !== undefined) return Promise.resolve(structuredClone(initial))
      return new Promise((resolve, reject) => {
        const deadline = setTimeout(
          () => reject(new Error('timed out waiting for initial complete observation')),
          timeoutMs
        )
        resolveInitial = (rows) => {
          clearTimeout(deadline)
          resolve(rows)
        }
      })
    },
    startCollecting(nextCollector) {
      if (collector !== undefined) throw new Error('complete observer already collecting')
      collector = nextCollector
      for (const rows of pending.splice(0)) dispatch(rows)
    },
    waitForTerminal(timeoutMs) {
      if (collectionError !== undefined) return Promise.reject(collectionError)
      if (terminal !== undefined) return Promise.resolve(terminal)
      return new Promise((resolve, reject) => {
        const deadline = setTimeout(
          () => reject(new Error('timed out waiting for terminal atomic observation')),
          timeoutMs
        )
        resolveTerminal = (result) => {
          clearTimeout(deadline)
          resolve(result)
        }
        rejectTerminal = reject
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
  const before = await watcher.initial(30_000)
  const collector = new AtomicObservationCollector(effects, (rows) => {
    const afterOp = `${runId}-read-after-${observationIndex++}`
    recordRead(
      recorder,
      'invoke',
      afterOp,
      projectIds.map((key) => ({ type: 'read', key, value: null }))
    )
    recordRead(recorder, 'ok', afterOp, projectAtomicRead(projectIds, rows))
  })
  collector.initialize(before)
  recordRead(recorder, 'ok', beforeOp, projectAtomicRead(projectIds, before))
  let observationIndex = 1
  collector.arm()
  watcher.startCollecting(collector)

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

  await watcher.waitForTerminal(30_000)
  watcher.destroy()
  watcher = undefined

  const authority = (await target.oracle(
    `SELECT id, "projectId", rank FROM task WHERE id IN (${ids}) ORDER BY id`
  )) as AtomicTaskRow[]
  assertAtomicAuthorityRows(effects, authority)

  const outcome = checkAtomicVisibility(recorder.snapshot())
  const replay = atomicReplayCommand(args.target!, seed)
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
      schemaVersion: CHECKS_SCHEMA_VERSION,
      kind: 'orez-consistency-checks',
      checks: [
        {
          name: 'atomic-visibility',
          version: String(ATOMIC_VISIBILITY_WORKLOAD_PROFILE.version),
          inputs: ['history.jsonl'],
          status: outcome.valid ? 'pass' : 'fail',
          violations: outcome.violations,
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
