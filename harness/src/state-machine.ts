// Electric-style generated lifecycle model for the Rust hosts. A deterministic
// trace mixes writes, desired-query changes, retention pruning, lost responses,
// server restarts, and client restarts. Every operation compares live client
// views to an authoritative SQL oracle. Failures emit the seed, full trace, and
// a delta-debugged reproducer under harness/regressions/.
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { parseArgs } from 'node:util'

import { canonical } from './canonical.js'
import { mutators, queries } from './fixture.js'
import { persistentKVStoreProvider } from './persistent-kv.js'
import { assertServerOutcome } from './server-outcome.js'

import type { FixtureZero, SyncTarget } from './target.js'

type StateTarget = SyncTarget & {
  dropNextPushResponse(): Promise<void> | void
  pull(): Promise<void>
  restart(downForMs?: number): Promise<void>
}

type Operation =
  | { kind: 'desire'; slot: number; projectIDs: string[] }
  | { kind: 'undesire'; slot: number }
  | { kind: 'write'; id: string; projectID: string; rank: number }
  | { kind: 'responseLoss'; id: string; projectID: string }
  | { kind: 'prune'; epoch: number }
  | { kind: 'serverRestart' }
  | { kind: 'clientRestart' }
  | { kind: 'checkpoint' }

const { values: args } = parseArgs({
  options: {
    against: { type: 'string', default: 'rust-local' },
    seed: { type: 'string', default: '1' },
    steps: { type: 'string', default: '24' },
    replay: { type: 'string' },
    'no-shrink': { type: 'boolean', default: false },
    'shrink-runs': { type: 'string', default: '12' },
  },
})

if (!['rust-local', 'rust-cf'].includes(args.against!))
  throw new Error('--against must be rust-local or rust-cf')

const seed = Number(args.seed)
const steps = Number(args.steps)
const maxShrinkRuns = Number(args['shrink-runs'])
if (
  !Number.isSafeInteger(seed) ||
  !Number.isSafeInteger(steps) ||
  steps < 1 ||
  !Number.isSafeInteger(maxShrinkRuns) ||
  maxShrinkRuns < 1
)
  throw new Error('--seed and --steps must be safe integers; steps must be positive')

function mulberry32(initial: number) {
  let value = initial
  return () => {
    value |= 0
    value = (value + 0x6d2b79f5) | 0
    let mixed = Math.imul(value ^ (value >>> 15), 1 | value)
    mixed = (mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed)) ^ mixed
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4294967296
  }
}

function generateTrace(): Operation[] {
  const required: Operation[] = [
    { kind: 'desire', slot: 0, projectIDs: ['p0', 'p1'] },
    { kind: 'write', id: `sm-${seed}-write`, projectID: 'p0', rank: 4.25 },
    {
      kind: 'responseLoss',
      id: `sm-${seed}-lost-response`,
      projectID: 'p1',
    },
    { kind: 'prune', epoch: 0 },
    { kind: 'serverRestart' },
    { kind: 'clientRestart' },
    { kind: 'desire', slot: 1, projectIDs: ['p2'] },
    { kind: 'undesire', slot: 1 },
    { kind: 'checkpoint' },
  ]
  const rng = mulberry32(seed)
  const generated: Operation[] = []
  const project = () => `p${Math.floor(rng() * 10)}`
  for (let index = required.length; index < steps; index++) {
    const roll = Math.floor(rng() * 8)
    switch (roll) {
      case 0:
        generated.push({
          kind: 'desire',
          slot: 1 + Math.floor(rng() * 2),
          projectIDs: [project(), project()],
        })
        break
      case 1:
        generated.push({ kind: 'undesire', slot: 1 + Math.floor(rng() * 2) })
        break
      case 2:
      case 3:
        generated.push({
          kind: 'write',
          id: `sm-${seed}-${index}`,
          projectID: project(),
          rank: Math.round(rng() * 1000) / 100,
        })
        break
      case 4:
        generated.push({ kind: 'prune', epoch: index })
        break
      case 5:
        generated.push({ kind: 'serverRestart' })
        break
      case 6:
        generated.push({ kind: 'clientRestart' })
        break
      default:
        generated.push({ kind: 'checkpoint' })
    }
  }
  return [...required, ...generated].slice(0, steps)
}

async function startTarget(): Promise<StateTarget> {
  if (args.against === 'rust-local') {
    return (await import('./targets/rust-local.js')).startRustLocal({
      pullIntervalMs: 75,
      queryAware: true,
      retainChanges: 8,
    })
  }
  return (await import('./targets/rust-cf.js')).startRustCf({
    pullIntervalMs: 150,
    queryAware: true,
    retainChanges: 8,
  })
}

type View = {
  projectIDs: string[]
  snapshot(): { complete: boolean; ids: string[] }
  destroy(): void
}

function watch(client: FixtureZero, projectIDs: string[]): View {
  const view = client.materialize(queries.tasksInProjects({ projectIds: projectIDs }), {
    ttl: 0,
  })
  let complete = false
  let rows: { id: string }[] = []
  let destroyed = false
  view.addListener((data, resultType) => {
    rows = JSON.parse(JSON.stringify(data)) as { id: string }[]
    if (resultType === 'complete') complete = true
  })
  return {
    projectIDs,
    snapshot: () => ({ complete, ids: rows.map(({ id }) => id).sort() }),
    destroy() {
      if (destroyed) return
      destroyed = true
      view.destroy()
    },
  }
}

async function eventually(
  check: () => void | Promise<void>,
  label: string,
  timeoutMs = 45_000
) {
  const started = Date.now()
  let lastError: unknown
  while (Date.now() - started < timeoutMs) {
    try {
      await check()
      return
    } catch (error) {
      lastError = error
      await new Promise((resolve) => setTimeout(resolve, 25))
    }
  }
  throw new Error(`timeout waiting for ${label}: ${String(lastError)}`)
}

async function withTimeout<T>(promise: Promise<T>, label: string, ms = 45_000) {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`timeout waiting for ${label}`)), ms)
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}

async function oracleIDs(target: SyncTarget, projectIDs: string[]) {
  const quoted = projectIDs.map((id) => `'${id}'`).join(',')
  const rows = (await target.oracle(
    `SELECT id FROM task WHERE "projectId" IN (${quoted}) ORDER BY id`
  )) as { id: string }[]
  return rows.map(({ id }) => id).sort()
}

async function execute(trace: Operation[]) {
  const target = await startTarget()
  const directory = mkdtempSync(join(tmpdir(), 'orez-state-machine-'))
  const kvStore = persistentKVStoreProvider(directory)
  const storageKey = `state-machine-${seed}`
  let client = target.createClient('state-machine-user', { kvStore, storageKey })
  const views = new Map<number, View>()

  const verify = async (step: number, operation: Operation) => {
    for (const [slot, view] of views) {
      await eventually(async () => {
        const got = view.snapshot()
        if (!got.complete) throw new Error(`slot ${slot} is incomplete`)
        const want = await oracleIDs(target, view.projectIDs)
        if (canonical(got.ids) !== canonical(want)) {
          throw new Error(
            `slot ${slot} diverged: got ${canonical(got.ids)}, want ${canonical(want)}`
          )
        }
      }, `seed ${seed} step ${step} ${operation.kind}`)
    }
  }

  try {
    for (const [step, operation] of trace.entries()) {
      switch (operation.kind) {
        case 'desire': {
          views.get(operation.slot)?.destroy()
          views.set(operation.slot, watch(client, operation.projectIDs))
          break
        }
        case 'undesire': {
          views.get(operation.slot)?.destroy()
          views.delete(operation.slot)
          break
        }
        case 'write': {
          const request = client.mutate(
            mutators.task.create({
              id: operation.id,
              projectId: operation.projectID,
              title: `state machine ${operation.id}`,
              rank: operation.rank,
              done: false,
              meta: { seed, step },
            })
          )
          await withTimeout(request.client, `client write ${operation.id}`)
          await withTimeout(
            assertServerOutcome(request.server, 'success', operation.id),
            `server write ${operation.id}`
          )
          break
        }
        case 'responseLoss': {
          await target.dropNextPushResponse()
          const request = client.mutate(
            mutators.task.create({
              id: operation.id,
              projectId: operation.projectID,
              title: `lost response ${operation.id}`,
              rank: 9.5,
              done: false,
            })
          )
          await withTimeout(request.client, `lost-response client ${operation.id}`)
          await withTimeout(
            assertServerOutcome(request.server, 'success', operation.id),
            `lost-response recovery ${operation.id}`
          )
          const rows = await target.oracle(
            `SELECT id FROM task WHERE id = '${operation.id}'`
          )
          if (rows.length !== 1)
            throw new Error(
              `lost-response write ${operation.id} committed ${rows.length} times`
            )
          break
        }
        case 'prune': {
          for (let index = 0; index < 16; index++) {
            const id = `sm-prune-${seed}-${operation.epoch}-${index}`
            await target.sql(
              `INSERT INTO task (id, "projectId", title, rank, done, meta, "dueAt") VALUES ('${id}', 'p0', '${id}', ${index}, 0, NULL, NULL)`
            )
          }
          // Make pruning self-contained so removing surrounding operations
          // during shrinking cannot create a dependency-only false failure.
          await target.pull()
          break
        }
        case 'serverRestart':
          await target.restart(50)
          break
        case 'clientRestart': {
          const desired = [...views.entries()].map(([slot, view]) => ({
            slot,
            projectIDs: view.projectIDs,
          }))
          // Model a page/process restart, where the client disappears while
          // its subscriptions are still active. Destroying ttl=0 views first
          // is observably different: it persists an undesire and may evict the
          // corresponding rows just before shutdown. Do not destroy the stale
          // handles after close either: Zero's query-manager cleanup can remain
          // live beyond close, while a dead page cannot run that cleanup.
          await withTimeout(client.close(), `client restart at step ${step}`, 10_000)
          views.clear()
          client = target.createClient('state-machine-user', { kvStore, storageKey })
          for (const entry of desired)
            views.set(entry.slot, watch(client, entry.projectIDs))
          break
        }
        case 'checkpoint':
          break
      }
      await verify(step, operation)
      if (operation.kind === 'prune') {
        await eventually(async () => {
          const rows = (await target.oracle(
            'SELECT floor FROM _zsync_meta WHERE lock = 1'
          )) as { floor: number | string }[]
          if (Number(rows[0]?.floor) <= 0)
            throw new Error('retention floor did not advance')
        }, `seed ${seed} step ${step} retention pruning`)
      }
    }
  } finally {
    for (const view of views.values()) view.destroy()
    try {
      // target.close owns every client it created. Bound cleanup as well as the
      // operations: a broken connection must yield an artifact, not hang the
      // CI job before its always() upload step.
      await withTimeout(target.close(), 'state-machine target cleanup', 10_000)
    } finally {
      rmSync(directory, { recursive: true, force: true })
    }
  }
}

function failureFingerprint(error: unknown) {
  const message = String(error)
  const viewFailure = message.match(
    /step \d+ (\w+): Error: slot (\d+) (diverged|is incomplete)/
  )
  if (viewFailure)
    return `view-${viewFailure[3]}:${viewFailure[1]}:slot-${viewFailure[2]}`
  if (message.includes('retention floor did not advance')) return 'retention-floor'
  if (message.includes('lost-response write')) return 'lost-response-cardinality'
  if (message.includes('timeout waiting for server write')) return 'server-write-timeout'
  if (message.includes('timeout waiting for client write')) return 'client-write-timeout'
  if (message.includes('timeout waiting for state-machine target cleanup'))
    return 'target-cleanup-timeout'
  return message.replaceAll(/\d+/g, '#')
}

async function minimize(trace: Operation[], expectedError: unknown) {
  let current = trace
  let granularity = 2
  let runs = 0
  const expectedFingerprint = failureFingerprint(expectedError)
  while (current.length >= 2 && runs < maxShrinkRuns) {
    const chunkSize = Math.ceil(current.length / granularity)
    let reduced = false
    for (let start = 0; start < current.length; start += chunkSize) {
      const candidate = current.slice(0, start).concat(current.slice(start + chunkSize))
      if (candidate.length === 0) continue
      runs++
      try {
        await execute(candidate)
      } catch (error) {
        if (failureFingerprint(error) === expectedFingerprint) {
          current = candidate
          granularity = Math.max(2, granularity - 1)
          reduced = true
          break
        }
      }
      if (runs >= maxShrinkRuns) break
    }
    if (reduced) continue
    if (granularity >= current.length) break
    granularity = Math.min(current.length, granularity * 2)
  }
  console.error(`[state-machine] shrink replays: ${runs}/${maxShrinkRuns}`)
  return current
}

const replay = args.replay
  ? (JSON.parse(await Bun.file(args.replay).text()) as {
      trace: Operation[]
      minimized?: Operation[]
    })
  : undefined
const trace = replay?.minimized ?? replay?.trace ?? generateTrace()

console.log(
  `[state-machine] seed=${seed} target=${args.against} operations=${trace.length}`
)
try {
  await execute(trace)
} catch (error) {
  console.error(`[state-machine] FAIL seed=${seed}: ${String(error)}`)
  const minimized = args['no-shrink'] ? trace : await minimize(trace, error)
  const directory = join(import.meta.dirname, '..', 'regressions')
  mkdirSync(directory, { recursive: true })
  const file = join(directory, `state-machine-${args.against}-seed-${seed}.json`)
  writeFileSync(
    file,
    JSON.stringify(
      {
        seed,
        target: args.against,
        error: String(error),
        replay: `bun src/state-machine.ts --against ${args.against} --seed ${seed} --replay ${file} --no-shrink`,
        trace,
        minimized,
      },
      null,
      2
    )
  )
  console.error(
    `[state-machine] minimized ${trace.length} -> ${minimized.length}: ${file}`
  )
  process.exit(1)
}

console.log(`[state-machine] PASS seed=${seed} target=${args.against}`)
// A deterministic replay supersedes an older failure for the same target and
// seed. Leaving that artifact behind would make a green CI rerun publish stale
// red evidence in its always() upload.
rmSync(
  join(
    import.meta.dirname,
    '..',
    'regressions',
    `state-machine-${args.against}-seed-${seed}.json`
  ),
  { force: true }
)
const resultsDirectory = join(import.meta.dirname, '..', 'results')
mkdirSync(resultsDirectory, { recursive: true })
writeFileSync(
  join(resultsDirectory, `state-machine-${args.against}-seed-${seed}.json`),
  JSON.stringify(
    {
      lane: 'generated-lifecycle-state-machine',
      result: 'PASS',
      seed,
      target: args.against,
      trace,
    },
    null,
    2
  )
)
